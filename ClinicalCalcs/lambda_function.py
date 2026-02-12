"""
AWS Lambda handler: thin orchestration only.
Loads config (drug_classes + goal1/2/3), transforms request, scores, builds response.
All logic lives in config_loader, transform, dosing, glucose, scoring, response, rule_interpreter.
"""
import json
import os
import sys
from datetime import datetime, timezone
try:
    import boto3
except ImportError:
    boto3 = None
from config_loader import load_drug_classes, load_goal1, load_goal2, load_goal3, CONFIG_LOADER_VERSION
from transform import _normalize_request, transform_request_to_patient, normalize_glucose_readings
from scoring import calculate_scores, get_all_drug_weight_details
from dosing import get_recommended_dose
from response import (
    generate_assessment,
    generate_rationale,
    generate_alternatives,
    _drug_display_name,
    _build_drug_classes_from_config,
    find_cheapest_for_index2,
    build_claude_prompt,
    call_claude_api,
)
from deescalation import should_recommend_deescalation, get_deescalation_recommendations


def _log(msg):
    print(msg)
    sys.stdout.flush()


def _no_change_medication_label(patient, config):
    """Return 'No change - Drug1, Drug2' so it's clear which drugs the no-change option covers. (Legacy; prefer _no_change_choices.)"""
    current_ids = patient.get("current_drug_ids", set())
    if not current_ids:
        return "No medication change"
    drugs_config = config.get("drugs", {})
    names = []
    for did in sorted(current_ids):
        d = drugs_config.get(did, {})
        names.append(_drug_display_name({"drug": did, "class": d.get("class", did)}, config))
    return "No change - " + ", ".join(names)


def _single_no_change_label(patient, config, drug_id):
    """Return 'Maintain DrugName' for one drug (matches de-escalation format)."""
    drugs_config = config.get("drugs", {})
    d = drugs_config.get(drug_id, {})
    name = _drug_display_name({"drug": drug_id, "class": d.get("class", drug_id)}, config)
    return f"Maintain {name}"


def _no_change_choices(patient, config, no_change_result):
    """Expand 'No Change' into one choice per current drug. Returns list of {medication, dose, class, drug, clinical_fit, coverage}."""
    current_ids = sorted(patient.get("current_drug_ids", set()))
    if not current_ids:
        return [{"medication": "No medication change", "dose": "Continue current therapy", "class": "No Change", "drug": "No Change",
                 "clinical_fit": no_change_result.get("clinical_fit", 0), "coverage": no_change_result.get("coverage", 0)}]
    current_med_info = patient.get("current_medication_info", {})
    choices = []
    for did in current_ids:
        med_info = current_med_info.get(did, {})
        dose = "Continue current therapy"
        if med_info and med_info.get("dose"):
            freq = med_info.get("frequency", "")
            dose = f"{med_info['dose']} {freq}".strip() if freq else med_info["dose"]
        choices.append({
            "medication": _single_no_change_label(patient, config, did),
            "dose": dose,
            "class": "No Change",
            "drug": did,
            "clinical_fit": no_change_result.get("clinical_fit", 0),
            "coverage": no_change_result.get("coverage", 0),
        })
    return choices


def _get_user_id_from_event(event):
    """Extract userID from JWT passed via API Gateway authorizer. Uses sub (or cognito:username) from:
    - REST API: requestContext.authorizer.claims.sub
    - HTTP API v2: requestContext.authorizer.jwt.claims.sub
    Falls back to body/event only when no authorizer (e.g. direct Lambda invocation for testing).
    """
    authorizer = (event.get("requestContext") or {}).get("authorizer")
    if isinstance(authorizer, dict):
        try:
            # REST API (v1) Cognito: authorizer.claims
            # HTTP API v2: authorizer.jwt.claims
            claims = authorizer.get("claims") or (authorizer.get("jwt") or {}).get("claims") or {}
            if isinstance(claims, dict):
                sub = claims.get("sub") or claims.get("cognito:username")
                if sub:
                    return str(sub)
            sub = authorizer.get("sub")
            if sub:
                return str(sub)
        except Exception:
            pass
        # When authorizer exists, only use JWT - never body (security)
        return None
    # No authorizer: fallback for direct Lambda invocation / testing
    body = event.get("body", event)
    if isinstance(body, str):
        try:
            body = json.loads(body) if body else {}
        except Exception:
            body = {}
    if isinstance(body, dict):
        uid = body.get("userID") or body.get("userId")
        if uid:
            return str(uid)
        pid = (body.get("patientInfo") or {}).get("userID") or (body.get("patientInfo") or {}).get("userId")
        if pid:
            return str(pid)
    return event.get("userID") or event.get("userId")


def _invoke_save_history(event, request_data, response_body, context, recommendation_timestamp):
    """Invoke Save History Lambda synchronously with request + response. userID from JWT. Pass timestamp for feedback matching.
    Returns 'success' or 'fail'."""
    fn_name = (os.environ.get("SAVE_HISTORY_FUNCTION_NAME") or "save_history").strip()
    if not fn_name or not boto3:
        return "fail"
    user_id = _get_user_id_from_event(event)
    if not user_id:
        _log("Save history: skipped (no userID in request)")
        return "fail"
    payload = {
        "body": {
            "userID": str(user_id),
            "timestamp": recommendation_timestamp,
            "request": request_data,
            "response": {
                "statusCode": 200,
                "body": response_body,
                "requestId": (context.aws_request_id if context and getattr(context, "aws_request_id", None) else response_body.get("requestId")),
            },
        }
    }
    try:
        lambda_client = boto3.client("lambda")
        resp = lambda_client.invoke(
            FunctionName=fn_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload, default=str),
        )
        payload_bytes = resp.get("Payload")
        if payload_bytes:
            raw = payload_bytes.read()
            try:
                result = json.loads(raw)
            except json.JSONDecodeError:
                _log(f"Save history: invalid JSON in response: {raw[:500]}")
                return "fail"
            # Check for Lambda invocation error (FunctionError)
            if resp.get("FunctionError"):
                err_msg = result.get("errorMessage", str(result))
                _log(f"Save history: Lambda error ({resp.get('FunctionError')}): {err_msg}")
                return "fail"
            status = result.get("statusCode", 0)
            body_str = result.get("body", "{}")
            body_obj = json.loads(body_str) if isinstance(body_str, str) else body_str if isinstance(body_str, dict) else {}
            if status == 200 and body_obj.get("saved") is True:
                _log("Save history: saved successfully")
                return "success"
            _log(f"Save history: save returned status={status} body={body_obj}")
        else:
            _log("Save history: no payload in invoke response")
        return "fail"
    except Exception as e:
        _log(f"Save history: invoke failed ({e})")
        import traceback
        traceback.print_exc()
        return "fail"


def lambda_handler(event, context):
    """
    AWS Lambda handler. Accepts diabetes-medication (Next.js) app payload.
    Returns: statusCode, body (assessment, bestChoice, lowestCost, rationale, alternatives, allDrugWeights, top3BestOptions).
    """
    try:
        _log("Handler: entered")
        _log(f"Config loader version: {CONFIG_LOADER_VERSION}")
        _log("Handler: loading config...")
        config = load_drug_classes()
        _log("Handler: drug config (classes + drugs) loaded")
        goal1_data = load_goal1()
        goal2_data = load_goal2()
        goal3_data = load_goal3()
        _log("Handler: goal1/2/3 loaded")
        # Drug-level potency: build potency_by_drug from potency_by_class (one entry per drug)
        drugs_config = config.get("drugs", {})
        by_class = (goal3_data or {}).get("potency_by_class") or {}
        on_therapy_by_class = (goal3_data or {}).get("potency_on_therapy_by_class") or {}
        goal3_data["potency_by_drug"] = {did: by_class.get(d.get("class"), {}) for did, d in drugs_config.items() if isinstance(d, dict)}
        goal3_data["potency_on_therapy_by_drug"] = {did: on_therapy_by_class.get(d.get("class"), {}) for did, d in drugs_config.items() if isinstance(d, dict)}

        if isinstance(event.get("body"), str):
            request_data = json.loads(event["body"])
        else:
            request_data = event.get("body", event)
        request_data = _normalize_request(request_data or {})

        patient = transform_request_to_patient(request_data, config, goal2_data)
        normalized_glucose = normalize_glucose_readings(request_data)

        # 1.3 De-escalation: When lows detected, recommend reduce/maintain (regardless of A1C).
        # Hypoglycemia requires dose reduction first; when A1C > goal, add-on can follow after reduction.
        if should_recommend_deescalation(patient, normalized_glucose):
            _log("Handler: de-escalation indicated (lows detected), building de-escalation response")
            reduce_opts, maintain_opts, assessment_suffix = get_deescalation_recommendations(
                patient, normalized_glucose, config
            )
            combined_deesc = reduce_opts + maintain_opts
            top3_deesc = combined_deesc[:3]
            # Slot 3 = cheapest from top 5 (no exclusions); fill when fewer than 3
            lc_deesc = find_cheapest_for_index2(combined_deesc, config, exclude_drug_ids=set()) if combined_deesc else None
            if lc_deesc and len(top3_deesc) < 3:
                top3_deesc.append(lc_deesc)
            elif lc_deesc and len(top3_deesc) >= 3:
                top3_deesc[2] = lc_deesc
            # Slot 3 (lowest cost) only: use drug name, no dose (same as normal flow)
            if len(top3_deesc) >= 3:
                lc_copy = dict(top3_deesc[2])
                med = lc_copy.get("medication", "")
                drug_id = lc_copy.get("drug", "")
                if drug_id and (med.startswith("Reduce ") or med.startswith("Maintain ") or med.startswith("Stop ")):
                    lc_copy["medication"] = _drug_display_name(
                        {"drug": drug_id, "class": lc_copy.get("class", drug_id)}, config
                    )
                lc_copy["dose"] = ""
                top3_deesc[2] = lc_copy
            if not top3_deesc:
                _log("Handler: no de-escalation options (patient may have no current meds), falling back to normal scoring")
            else:
                # Run full scoring for allDrugWeights (trace log) and for Claude alternative_drug_names
                results_deesc = calculate_scores(config, patient, None, normalized_glucose, goal1_data, goal3_data)
                all_drug_weights = get_all_drug_weight_details(
                    config, patient, None, normalized_glucose, goal1_data, goal3_data
                )
                drugs_config = config.get("drugs", {})
                all_drug_weights_payload = []
                current_ids = patient.get("current_drug_ids", set())
                for e in all_drug_weights:
                    payload_entry = dict(e)
                    drug_id = e.get("drug")
                    cls = e.get("class")
                    if drug_id == "No Change" or cls == "No Change":
                        if current_ids:
                            for did in sorted(current_ids):
                                row = dict(payload_entry)
                                row["class"] = _drug_display_name(
                                    {"drug": did, "class": drugs_config.get(did, {}).get("class", did)},
                                    config,
                                ) + " (No change)"
                                all_drug_weights_payload.append(row)
                        else:
                            continue  # Skip when no current meds
                    else:
                        # Skip: current drugs already appear as "X (No change)" above
                        if drug_id in current_ids:
                            continue
                        payload_entry["class"] = _drug_display_name(
                            {"drug": drug_id, "class": cls},
                            config,
                        )
                        all_drug_weights_payload.append(payload_entry)

                base_assessment = generate_assessment(patient, {}, normalized_glucose, goal3_data)
                assessment = (base_assessment.rstrip(".") + assessment_suffix) if assessment_suffix else base_assessment
                original_assessment = assessment

                # Use same Claude call as normal flow - explain why top 2 (reduce/maintain) are recommended
                exclude_deesc = set(r.get("drug", r.get("class")) for r in top3_deesc if r.get("drug") or r.get("class"))
                exclude_deesc.add("No Change")  # Don't include "No Change" in other options - focus on add-on drugs
                alternative_results = [r for r in (results_deesc or []) if r.get("drug", r.get("class")) not in exclude_deesc][:5]
                alternative_drug_names = [_drug_display_name(r, config) for r in alternative_results if r.get("coverage", 0) > 0]
                lowest_cost_deesc = top3_deesc[2] if len(top3_deesc) >= 3 else None

                a1c_val = float(patient.get("a1c") or 0)
                goal_val = float(patient.get("goal") or 7.5)
                a1c_above_goal = a1c_val > 0 and a1c_val > goal_val
                if a1c_above_goal:
                    rationale = [
                        "Documented hypoglycemia requires dose reduction first (e.g., sulfonylurea) per de-escalation guidelines.",
                        "Consider Metformin increase or add-on therapy after sulfonylurea reduction to address A1C above goal.",
                    ]
                    alternatives = ["Add-on therapy deferred until after sulfonylurea reduction; hypoglycemia is the priority."]
                else:
                    rationale = [
                        "A1C at or below goal with hypoglycemia detected.",
                        "Dose reduction is recommended per de-escalation guidelines to reduce hypoglycemia risk.",
                    ]
                    alternatives = ["Add-on therapy was not considered; de-escalation is the priority when A1C at goal with lows."]
                future_considerations = [
                    "Recheck fasting glucose in 1-2 weeks after dose reduction.",
                    "If A1C rises above goal after de-escalation, consider re-escalation.",
                ]

                claude_api_key = os.environ.get("CLAUDE_API_KEY")
                if claude_api_key and claude_api_key.strip():
                    try:
                        drug_classes = _build_drug_classes_from_config(config)
                        top_two_for_prompt = top3_deesc[:2] if len(top3_deesc) >= 2 else (top3_deesc[:1] if top3_deesc else [])
                        system_message, prompt = build_claude_prompt(
                            request_data, results_deesc or [], drug_classes, patient,
                            alternative_drug_names=alternative_drug_names,
                            top_two_results=top_two_for_prompt,
                            lowest_cost_result=lowest_cost_deesc,
                            is_deescalation=True,
                            a1c_above_goal=a1c_above_goal,
                            assessment=assessment,
                        )
                        claude_model = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-5-20250929")
                        claude_temperature = float(os.environ.get("CLAUDE_TEMPERATURE", "0.3"))
                        claude_result = call_claude_api(prompt, claude_api_key, claude_model, claude_temperature, system_message=system_message)
                        rationale = (claude_result.get("rationale") or rationale)[:15]
                        alternatives = claude_result.get("alternatives") or alternatives
                        # Strip "No Change" bullets - not a drug; focus on add-on classes only
                        alternatives = [a for a in alternatives if "no change" not in a.lower()]
                        future_considerations = claude_result.get("future_considerations") or future_considerations
                        # Use Claude's updated assessment if provided
                        updated = claude_result.get("updated_assessment", "")
                        if updated:
                            assessment = updated
                    except Exception as claude_err:
                        _log(f"Claude API call failed for de-escalation: {claude_err}")
                recommendation_timestamp = datetime.now(timezone.utc).isoformat()
                body = {
                    "assessment": str(assessment),
                    "original_assessment": str(original_assessment),
                    "rationale": rationale,
                    "alternatives": alternatives,
                    "futureConsiderations": future_considerations,
                    "allDrugWeights": all_drug_weights_payload,
                    "top3BestOptions": top3_deesc,
                    "recommendationTimestamp": recommendation_timestamp,
                }
                if context and getattr(context, "aws_request_id", None):
                    body["requestId"] = context.aws_request_id
                body["save"] = _invoke_save_history(event, request_data, body, context, recommendation_timestamp)
                body_json = json.dumps(body, ensure_ascii=False)
                _log(f"Handler: de-escalation response: {body_json[:500]}...")
                return {
                    "isBase64Encoded": False,
                    "statusCode": 200,
                    "headers": {
                        "Content-Type": "application/json",
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Headers": "Content-Type",
                        "Access-Control-Allow-Methods": "POST, OPTIONS",
                    },
                    "body": body_json,
                }

        results = calculate_scores(config, patient, None, normalized_glucose, goal1_data, goal3_data)

        if not results:
            raise ValueError("No drug results calculated")

        # Top 2 best choices = by clinical fit, from different drug classes. Lowest cost = separate.
        top_result = results[0] if results else {}
        top_class = top_result["class"]
        top_drug_id = top_result.get("drug", top_class)
        drugs = config.get("drugs", {})
        top_drug_data = drugs.get(top_drug_id, drugs.get(top_class, {}))
        assessment = generate_assessment(patient, top_result, normalized_glucose, goal3_data)
        original_assessment = assessment

        current_drug_ids = patient.get("current_drug_ids", set())
        current_med_info_dict = patient.get("current_medication_info", {})
        is_currently_on = top_drug_id in current_drug_ids
        current_med_info = current_med_info_dict.get(top_drug_id)
        preferred_drug_by_class = request_data.get("preferredDrugByClass") or request_data.get("preferred_drug_by_class") or {}
        preferred_drug = preferred_drug_by_class.get(top_class) or top_drug_id
        best_medication = get_recommended_dose(
            top_class,
            patient.get("eGFR"),
            is_currently_on=is_currently_on,
            current_medication_info=current_med_info,
            goal2_data=goal2_data,
            preferred_drug=preferred_drug,
        )

        # Build 2 best choices by clinical fit, from different drug classes. Expand "No Change" into one per drug.
        # When patient has no current meds, skip "No Change" - recommend add-on therapy instead
        top_two_results = []
        for r in results:
            if len(top_two_results) >= 2:
                break
            drug_id = r.get("drug", r.get("class"))
            if drug_id == "No Change" and not current_drug_ids:
                continue
            cls = r.get("class", r.get("drug"))
            if not top_two_results:
                top_two_results.append(r)
            elif cls != top_two_results[0].get("class", top_two_results[0].get("drug")):
                top_two_results.append(r)
        expanded_choices = []
        for r in top_two_results:
            cls = r["class"]
            drug_id = r.get("drug", cls)
            if drug_id == "No Change" or cls == "No Change":
                expanded_choices.extend(_no_change_choices(patient, config, r))
            else:
                preferred = preferred_drug_by_class.get(cls) or drug_id
                med = get_recommended_dose(
                    cls,
                    patient.get("eGFR"),
                    is_currently_on=(drug_id in patient.get("current_drug_ids", set())),
                    current_medication_info=patient.get("current_medication_info", {}).get(drug_id),
                    goal2_data=goal2_data,
                    preferred_drug=preferred,
                )
                expanded_choices.append({
                    "class": cls,
                    "drug": drug_id,
                    "clinical_fit": round(float(r["clinical_fit"]), 2),
                    "coverage": round(float(r["coverage"]), 2),
                    "medication": str(med["medication"]),
                    "dose": str(med["dose"]),
                })
        top_two_choices_by_fit = expanded_choices[:2]
        lowest_cost_result = find_cheapest_for_index2(results, config, exclude_drug_ids=set())
        lowest_cost_med_name = top_two_choices_by_fit[0]["medication"] if top_two_choices_by_fit else ""
        # 3rd slot = cheapest from top 5 clinical fits (no exclusions)
        cheapest_for_index3 = find_cheapest_for_index2(results, config, exclude_drug_ids=set())
        # Exclude all top 3 choices from "Why Other Options Weren't Preferred" (only explain drugs we did NOT recommend)
        exclude_for_alternatives = set()
        for r in top_two_choices_by_fit:
            did = r.get("drug", r.get("class"))
            if did:
                exclude_for_alternatives.add(did)
        if cheapest_for_index3:
            exclude_for_alternatives.add(cheapest_for_index3.get("drug", cheapest_for_index3.get("class")))
        # 5 alternative drug names for AI-generated "why not preferred"
        alternative_results = [r for r in results if r.get("drug", r.get("class")) not in exclude_for_alternatives][:5]
        alternative_drug_names = [_drug_display_name(r, config) for r in alternative_results if r.get("coverage", 0) > 0]
        if lowest_cost_result and not top_two_choices_by_fit:
            lc_drug = lowest_cost_result.get("drug", lowest_cost_result.get("class"))
            lc_med = get_recommended_dose(
                lowest_cost_result.get("class", "Metformin"),
                patient.get("eGFR"),
                lc_drug in current_drug_ids,
                current_med_info_dict.get(lc_drug),
                goal2_data,
                preferred_drug=lc_drug,
            )
            lowest_cost_med_name = str(lc_med.get("medication", ""))

        claude_api_key = os.environ.get("CLAUDE_API_KEY")
        use_claude = claude_api_key and claude_api_key.strip()
        if use_claude:
            try:
                drug_classes = _build_drug_classes_from_config(config)
                top_two_for_prompt = top_two_choices_by_fit[:2] if top_two_choices_by_fit and len(top_two_choices_by_fit) >= 2 else None
                system_message, prompt = build_claude_prompt(
                    request_data, results, drug_classes, patient,
                    alternative_drug_names=alternative_drug_names,
                    top_two_results=top_two_for_prompt,
                    lowest_cost_result=lowest_cost_result,
                    assessment=assessment,
                )
                claude_model = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-5-20250929")
                claude_temperature = float(os.environ.get("CLAUDE_TEMPERATURE", "0.3"))
                claude_result = call_claude_api(prompt, claude_api_key, claude_model, claude_temperature, system_message=system_message)
                rationale = (claude_result.get("rationale") or [])[:15]
                claude_alternatives = claude_result.get("alternatives") or []
                future_considerations = claude_result.get("future_considerations") or []
                # Use Claude's updated assessment if provided
                updated = claude_result.get("updated_assessment", "")
                if updated:
                    assessment = updated
                if not rationale:
                    rationale = generate_rationale(patient, top_result, top_drug_data)
            except Exception as claude_error:
                print(f"Claude API call failed: {claude_error}")
                rationale = generate_rationale(patient, top_result, top_drug_data)
                claude_alternatives = []
                future_considerations = []
        else:
            rationale = generate_rationale(patient, top_result, top_drug_data)
            claude_alternatives = []
            future_considerations = []

        second_best_choice = None
        if len(top_two_choices_by_fit) >= 2:
            second_best_choice = {"medication": top_two_choices_by_fit[1]["medication"], "dose": top_two_choices_by_fit[1]["dose"]}
        if lowest_cost_result:
            lc_drug = lowest_cost_result.get("drug", lowest_cost_result.get("class"))
            lc_med = get_recommended_dose(
                lowest_cost_result.get("class", "Metformin"),
                patient.get("eGFR"),
                lc_drug in current_drug_ids,
                current_med_info_dict.get(lc_drug),
                goal2_data,
                preferred_drug=lc_drug,
            )
            lowest_cost_med = {"medication": str(lc_med.get("medication", "")), "dose": str(lc_med.get("dose", ""))}
        elif top_two_choices_by_fit:
            lowest_cost_med = {"medication": top_two_choices_by_fit[0]["medication"], "dose": top_two_choices_by_fit[0]["dose"]}
        else:
            lowest_cost_med = best_medication

        # Build drug weight details first so we can pass penalties/boosts into alternatives text
        all_drug_weights = get_all_drug_weight_details(
            config, patient, None, normalized_glucose, goal1_data, goal3_data
        )
        drug_details_map = {
            e.get("drug", e.get("class")): {
                "applied_boosts": e.get("applied_boosts", []),
                "applied_cautions": e.get("applied_cautions", []),
            }
            for e in all_drug_weights
        }
        # Use Claude-generated alternatives (2 sentences per drug) when available; else fallback to generate_alternatives
        alternatives = claude_alternatives if claude_alternatives else generate_alternatives(
            results, top_class, top_drug_id, config,
            exclude_ids=exclude_for_alternatives,
            drug_details_map=drug_details_map,
        )
        # Frontend "Class" column displays "class"; send drug name + brand (e.g. Semaglutide (Ozempic)) when in config
        # "No Change" becomes one row per current drug: "Metformin (No change)", "Sitagliptin (No change)", etc.
        drugs_config = config.get("drugs", {})
        all_drug_weights_payload = []
        for e in all_drug_weights:
            payload_entry = dict(e)
            drug_id = e.get("drug")
            cls = e.get("class")
            if drug_id == "No Change" or cls == "No Change":
                current_ids = patient.get("current_drug_ids", set())
                if current_ids:
                    for did in sorted(current_ids):
                        row = dict(payload_entry)
                        row["class"] = _drug_display_name(
                            {"drug": did, "class": drugs_config.get(did, {}).get("class", did)},
                            config,
                        ) + " (No change)"
                        all_drug_weights_payload.append(row)
                else:
                    # Skip "No Change" when patient has no meds - not relevant to show in log
                    continue
            else:
                payload_entry["class"] = _drug_display_name(
                    {"drug": drug_id, "class": cls},
                    config,
                )
                all_drug_weights_payload.append(payload_entry)

        # top3BestOptions = [0] best clinical fit, [1] 2nd best clinical fit (different class), [2] lowest cost (includes top 2 in pool)
        top3_best_options = list(top_two_choices_by_fit) if top_two_choices_by_fit else []
        if not top3_best_options and top_result:
            med_label = _no_change_medication_label(patient, config) if top_drug_id == "No Change" else str(best_medication.get("medication", ""))
            dose_label = str(best_medication.get("dose", ""))
            top3_best_options = [{
                "class": top_class,
                "drug": top_drug_id,
                "clinical_fit": round(float(top_result.get("clinical_fit", 0)), 2),
                "coverage": round(float(top_result.get("coverage", 0)), 2),
                "medication": med_label,
                "dose": dose_label,
            }]
        # Add 3rd = cheapest from top 5 clinical fits (no exclusions; show drug name, not "No Change")
        if cheapest_for_index3 and len(top3_best_options) < 3:
            lc_drug = cheapest_for_index3.get("drug", cheapest_for_index3.get("class"))
            lc_class = cheapest_for_index3.get("class", "Metformin")
            if lc_drug == "No Change" or lc_class == "No Change":
                no_change_choices = _no_change_choices(patient, config, cheapest_for_index3)
                if no_change_choices:
                    first = no_change_choices[0]
                    lc_med_display = first["medication"]
                    lc_drug = first["drug"]
                    lc_class = first["class"]
                else:
                    lc_med_display = "No medication change"
            else:
                lc_med_display = _drug_display_name(
                    {"drug": lc_drug, "class": lc_class}, config
                )
            top3_best_options.append({
                "class": lc_class,
                "drug": lc_drug,
                "clinical_fit": round(float(cheapest_for_index3.get("clinical_fit", 0)), 2),
                "coverage": round(float(cheapest_for_index3.get("coverage", 0)), 2),
                "medication": lc_med_display,
                "dose": "",
            })

        # Build payload; ensure all values are JSON-serializable for API Gateway
        # top3BestOptions: [0]=best clinical fit, [1]=2nd best clinical fit (different class), [2]=lowest cost (includes top 2)
        recommendation_timestamp = datetime.now(timezone.utc).isoformat()
        body = {
            "assessment": str(assessment) if assessment is not None else "",
            "original_assessment": str(original_assessment),
            "rationale": [str(x) for x in (rationale or [])],
            "alternatives": [str(x) for x in (alternatives or [])],
            "futureConsiderations": [str(x) for x in (future_considerations or [])],
            "allDrugWeights": all_drug_weights_payload,
            "top3BestOptions": top3_best_options,
            "recommendationTimestamp": recommendation_timestamp,
        }
        if context and getattr(context, "aws_request_id", None):
            body["requestId"] = context.aws_request_id
        # Invoke Save History Lambda (request + response to DynamoDB). userID from JWT. Pass timestamp for feedback matching.
        body["save"] = _invoke_save_history(event, request_data, body, context, recommendation_timestamp)
        body_json = json.dumps(body, ensure_ascii=False)
        _log(f"Response payload (to frontend): {body_json}")
        # API Gateway proxy integration requires exactly: statusCode (int), headers (str values), body (string), isBase64Encoded (bool)
        response = {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
            },
            "body": body_json,
        }
        print("Handler: response built, returning 200")
        return response
    except Exception as e:
        _log(f"Error in lambda_handler: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()
        return {
            "isBase64Encoded": False,
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)}),
        }
