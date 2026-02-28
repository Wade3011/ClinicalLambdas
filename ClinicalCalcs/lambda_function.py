"""
AWS Lambda handler: thin orchestration only.
Loads config (drug_classes + goal1/2/3), transforms request, scores, builds response.
All logic lives in config_loader, transform, dosing, glucose, scoring, response, rule_interpreter.
"""
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")
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
    _drug_display_name,
    _build_drug_classes_from_config,
    find_cheapest_for_index2,
    build_claude_prompt,
    call_claude_api,
    retrieve_from_bedrock_kb,
    call_bedrock_claude,
)
from deescalation import should_recommend_deescalation, get_deescalation_recommendations
from rule_interpreter import evaluate_structured_rule
from scoring import _rule_context

# Single source for API response display names. Used for top3BestOptions and allDrugWeights so #1, #2, and lowest cost all match.
RESPONSE_DISPLAY_NAMES = {
    "Bexagliflozin": "Bexagliflozin (Brenzavy)",
    "Canagliflozin": "Canagliflozin (Invokana)",
    "Dapagliflozin": "Dapagliflozin (Farxiga)",
    "Empagliflozin": "Empagliflozin (Jardiance)",
    "Ertugliflozin": "Ertugliflozin (Steglatro)",
    "Metformin IR": "Metformin IR",
    "Metformin SA": "Metformin SA",
    "Glimepiride": "Glimepiride (Amaryl)",
    "Glipizide": "Glipizide (Glucotrol)",
    "Glyburide": "Glyburide (Diabeta)",
    "Dulaglutide": "Dulaglutide (Trulicity)",
    "Semaglutide": "Semaglutide (Ozempic)",
    "Tirzepatide": "Tirzepatide (Mounjaro)",
    "Semaglutide Oral": "Semaglutide (Rybelsus)",
    "Alogliptin": "Alogliptin (Nesina)",
    "Saxagliptin": "Saxagliptin (Onglyza)",
    "Linagliptin": "Linagliptin (Tradjenta)",
    "Sitagliptin": "Sitagliptin (Januvia)",
    "Pioglitazone": "Pioglitazone (Actos)",
    "Glargine": "Glargine (Lantus/Basaglar/Toujeo)",
    "Detemir": "Detemir (Levemir)",
    "Degludec": "Degludec (Tresiba)",
    "NPH": "NPH (Basal principles)",
    "Other": "Other",
    "Lispro": "Lispro (Humalog)",
    "Aspart": "Aspart (Novolog)",
    "Glulisine": "Glulisine (Apidra)",
}


def _response_display_name(drug_id, cls, config):
    """Display name for response payload. Only use RESPONSE_DISPLAY_NAMES; no fallback to config. All drugs must be mapped."""
    return RESPONSE_DISPLAY_NAMES.get(drug_id, drug_id)


def _rule_mentions_egfr(rule):
    """True if rule or any nested and/or sub-rule has field 'eGFR'."""
    if not isinstance(rule, dict):
        return False
    if rule.get("field") == "eGFR":
        return True
    for key in ("and", "or"):
        for r in rule.get(key, []):
            if _rule_mentions_egfr(r):
                return True
    return False


def _eGFR_therapy_warning(patient, config):
    """True when eGFR is below a threshold and a current therapy should be dose-reduced or stopped (deny_if or caution_if with eGFR)."""
    current_ids = patient.get("current_drug_ids", set())
    if not current_ids:
        return False
    drugs = config.get("drugs", {})
    ctx = _rule_context(patient)
    for drug_id in current_ids:
        drug_data = drugs.get(drug_id)
        if not drug_data:
            continue
        for rule in drug_data.get("deny_if", []):
            if isinstance(rule, dict) and _rule_mentions_egfr(rule) and evaluate_structured_rule(rule, ctx):
                return True
        for caution in drug_data.get("caution_if", []):
            r = caution.get("rule", caution) if isinstance(caution, dict) else caution
            if isinstance(r, dict) and _rule_mentions_egfr(r) and evaluate_structured_rule(r, ctx):
                return True
    return False


def _log(msg):
    print(msg)
    sys.stdout.flush()


# Comorbidities that warrant using the full model (no Haiku fallback). PDF: "simple cases" = clinical_fit > 0.9 and no complex comorbidities.
_COMPLEX_COMORBIDITY_SIGNALS = frozenset({"CKD", "Chronic Kidney Disease", "Heart Failure (CHF)", "CHF", "ASCVD", "Cardiovascular disease", "Obesity (BMI > 40)"})


def _use_haiku_fallback(patient, top_results):
    """True when we can use Haiku for cost savings: top option has clinical_fit > 0.9 and no complex comorbidities."""
    if not top_results or len(top_results) == 0:
        return False
    fit = float(top_results[0].get("clinical_fit") or 0)
    if fit <= 0.9:
        return False
    comorbidities = patient.get("comorbidities") or set()
    if len(comorbidities) >= 2:
        return False
    for c in comorbidities:
        if any(s in str(c) for s in _COMPLEX_COMORBIDITY_SIGNALS):
            return False
    return True


def _build_retrieval_query(request_data, top_results, is_deescalation=False):
    """Build a focused retrieval query from patient context + top drug options (PDF-style).
    Extracts key clinical signals for Knowledge Base retrieval; strips 'Other: ' prefixes."""
    patient = request_data.get("patientInfo") or {}
    comorbidities_raw = request_data.get("comorbidities") or []
    medications_raw = request_data.get("currentMedications") or []
    allergies_raw = request_data.get("allergies") or []
    glucose = request_data.get("glucoseReadings") or {}

    # Strip "Other: " prefix from comorbidities
    comorbidities = []
    for c in comorbidities_raw:
        s = c if isinstance(c, str) else str(c)
        cleaned = s.replace("Other: ", "").strip() if s.startswith("Other: ") else s.strip()
        if cleaned:
            comorbidities.append(cleaned)

    # Extract current drug names and classes, strip "Other: "
    current_drugs = []
    for med in medications_raw:
        if not isinstance(med, dict):
            continue
        drug_name = (med.get("drugName") or med.get("drug_name") or "").strip()
        drug_class = (med.get("drugClass") or med.get("drug_class") or "").strip()
        if drug_name.startswith("Other: "):
            drug_name = drug_name.replace("Other: ", "").strip()
        if drug_class.startswith("Other: "):
            drug_class = drug_class.replace("Other: ", "").strip()
        if drug_name:
            current_drugs.append(drug_name)
        elif drug_class:
            current_drugs.append(drug_class)

    # Top 3 recommended drug names and classes
    recommended = []
    for option in (top_results or [])[:3]:
        if not isinstance(option, dict):
            continue
        name = option.get("drug_name") or option.get("drugName") or option.get("medication") or option.get("drug") or ""
        drug_class = option.get("drug_class") or option.get("drugClass") or option.get("class") or ""
        if isinstance(name, str) and name.startswith("Other: "):
            name = name.replace("Other: ", "").strip()
        if name and drug_class:
            recommended.append("{} ({})".format(name, drug_class))
        elif name:
            recommended.append(name)
        elif drug_class:
            recommended.append(drug_class)

    query_parts = []

    # Core patient profile
    age = patient.get("age") or patient.get("Age") or ""
    a1c = patient.get("lastA1c") or patient.get("last_a1c") or patient.get("a1c") or ""
    egfr = patient.get("eGFR") or patient.get("egfr") or ""
    query_parts.append(
        "Type 2 diabetes management for patient age {} with A1C {} and eGFR {}".format(age, a1c, egfr).strip()
    )
    if is_deescalation:
        query_parts.append("De-escalation hypoglycemia dose reduction")

    # Comorbidities
    if comorbidities:
        query_parts.append("Comorbidities: {}".format(", ".join(comorbidities)))

    # Recommended drugs (what guidelines say about these options)
    if recommended:
        query_parts.append("Recommendations for: {}".format(", ".join(recommended)))

    # Current medications
    if current_drugs:
        query_parts.append("Currently taking: {}".format(", ".join(current_drugs)))

    # Glucose
    fasting_avg = None
    postprandial_avg = None
    if isinstance(glucose, dict):
        fasting_obj = glucose.get("fasting")
        if isinstance(fasting_obj, dict):
            fasting_avg = fasting_obj.get("average")
        post_obj = glucose.get("postPrandial") or glucose.get("postprandial")
        if isinstance(post_obj, dict):
            postprandial_avg = post_obj.get("average")
    if fasting_avg is not None or postprandial_avg is not None:
        glucose_parts = []
        if fasting_avg is not None:
            glucose_parts.append("fasting average {}".format(fasting_avg))
        if postprandial_avg is not None:
            glucose_parts.append("postprandial average {}".format(postprandial_avg))
        if glucose_parts:
            query_parts.append("Glucose readings: {}".format(", ".join(glucose_parts)))

    # Allergies
    allergy_parts = []
    for a in allergies_raw:
        if not isinstance(a, dict):
            continue
        allergen = (a.get("allergen") or "").strip()
        specific = a.get("specificDrugs") or a.get("specific_drugs") or []
        if not allergen:
            continue
        if specific and (isinstance(specific, list) and "All" in specific or specific == "All"):
            allergy_parts.append("full class exclusion: {}".format(allergen))
        elif specific and isinstance(specific, list):
            allergy_parts.append("{} ({})".format(allergen, ", ".join(str(s) for s in specific)))
        else:
            allergy_parts.append(allergen)
    if allergy_parts:
        query_parts.append("Allergies: {}".format(", ".join(allergy_parts)))

    # Additional context (free text from clinician)
    additional_context = (request_data.get("additionalContext") or request_data.get("additional_context") or "").strip()
    if additional_context:
        query_parts.append("Additional context: {}".format(additional_context))

    return ". ".join(p for p in query_parts if p)


def _build_targeted_retrieval_query(top_results):
    """Build a short query for the targeted KB (drug/class-specific PDFs). Uses top 2-3 drug and class names."""
    recommended = []
    for option in (top_results or [])[:3]:
        if not isinstance(option, dict):
            continue
        drug = (option.get("drug") or "").strip()
        drug_class = (option.get("class") or "").strip()
        if drug_class and drug and drug != "No Change" and drug_class != "No Change":
            recommended.append("{} {}".format(drug_class, drug))
        elif drug_class and drug_class != "No Change":
            recommended.append(drug_class)
        elif drug and drug != "No Change":
            recommended.append(drug)
    if not recommended:
        return "Type 2 diabetes medication dosing and guidelines"
    return "Dosing, safety, and guidelines for: " + ", ".join(recommended)


def _retrieve_kb_dual_query(kb_id, generic_query, targeted_query, region=None, number_of_results=5):
    """Run two retrievals against the same KB in parallel (generic + targeted query). Returns (merged_refs_string, total_chunk_count)."""
    with ThreadPoolExecutor(max_workers=2) as executor:
        f_gen = executor.submit(
            retrieve_from_bedrock_kb, kb_id, generic_query, region=region, number_of_results=number_of_results
        )
        f_tgt = executor.submit(
            retrieve_from_bedrock_kb, kb_id, targeted_query, region=region, number_of_results=number_of_results
        )
        refs_gen, count_gen = f_gen.result()
        refs_tgt, count_tgt = f_tgt.result()
    parts = []
    if refs_gen and refs_gen.strip():
        parts.append(refs_gen.strip())
    if refs_tgt and refs_tgt.strip():
        parts.append(refs_tgt.strip())
    merged = "\n\n".join(parts) if parts else ""
    return merged, count_gen + count_tgt


def _no_change_medication_label(patient, config):
    """Return drug names only (no literal 'No Change'). Used when single top result is no-change."""
    current_ids = patient.get("current_drug_ids", set())
    if not current_ids:
        return "No medication change"
    drugs_config = config.get("drugs", {})
    names = []
    for did in sorted(current_ids):
        d = drugs_config.get(did, {})
        names.append(_drug_display_name({"drug": did, "class": d.get("class", did)}, config))
    return ", ".join(names)


def _get_current_med_info_for_dose(patient, drug_id, cls, config):
    """Resolve is_currently_on and current_medication_info for get_recommended_dose.
    Only uses info when the patient is on this specific drug (drug_id). No same-class fallback,
    so when we recommend a different drug in the same class (e.g. Dapagliflozin vs Empagliflozin),
    we show that drug's name and its own dose (start or titration), not the other drug's."""
    current_med_info_dict = patient.get("current_medication_info", {})
    current_drug_ids = patient.get("current_drug_ids", set())
    current_med_info = current_med_info_dict.get(drug_id)
    is_on_this_drug = drug_id in current_drug_ids
    return is_on_this_drug, current_med_info


def _single_no_change_label(patient, config, drug_id):
    """Return 'Continue DrugName' for one drug (matches de-escalation and normal flow)."""
    drugs_config = config.get("drugs", {})
    d = drugs_config.get(drug_id, {})
    name = _drug_display_name({"drug": drug_id, "class": d.get("class", drug_id)}, config)
    return f"Continue {name}"


# When uninsured + cannot afford copay, limit to these classes only (affordability gate)
AFFORDABILITY_GATE_CLASSES = frozenset({"Metformin", "TZD", "Sulfonylurea", "Basal Insulin", "Bolus Insulin", "No Change"})


def _filter_config_for_affordability_gate(config):
    """Return a copy of config with only drugs in AFFORDABILITY_GATE_CLASSES (and those classes)."""
    classes = config.get("classes", {})
    drugs = config.get("drugs", {})
    new_drugs = {did: dict(d) for did, d in drugs.items() if isinstance(d, dict) and d.get("class") in AFFORDABILITY_GATE_CLASSES}
    new_classes = {k: dict(v) for k, v in classes.items() if k in AFFORDABILITY_GATE_CLASSES}
    return {"classes": new_classes, "drugs": new_drugs}


def _no_change_choices(patient, config, no_change_result):
    """Expand 'No Change' into one choice per current drug. Returns list of {medication, dose, class, drug, clinical_fit, coverage}.
    medication and class = drug name only (never the literal 'No Change'); dose = current dose."""
    current_ids = sorted(patient.get("current_drug_ids", set()))
    if not current_ids:
        return [{"medication": "No medication change", "dose": "Continue current therapy", "class": "No medication change", "drug": "No Change",
                 "clinical_fit": no_change_result.get("clinical_fit", 0), "coverage": no_change_result.get("coverage", 0)}]
    drugs_config = config.get("drugs", {})
    current_med_info = patient.get("current_medication_info", {})
    choices = []
    for did in current_ids:
        med_info = current_med_info.get(did, {})
        dose = "Continue current therapy"
        if med_info and med_info.get("dose"):
            freq = med_info.get("frequency", "")
            dose = f"{med_info['dose']} {freq}".strip() if freq else med_info["dose"]
        cls = drugs_config.get(did, {}).get("class", did)
        drug_name = _response_display_name(did, cls, config)
        medication = f"Continue {drug_name}"
        choices.append({
            "medication": medication,
            "dose": dose,
            "class": medication,
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
    # No authorizer: no fallback (require auth)
    return None


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

        # Affordability gate: uninsured + cannot afford copay -> limit to Metformin, TZD, Sulfonylurea, Insulin (and No Change)
        if patient.get("insurance") == "No Insurance" and patient.get("can_afford_copay") is False:
            config = _filter_config_for_affordability_gate(config)
            _log("Handler: affordability gate applied (uninsured, cannot afford copay); limited drug set")

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
                if drug_id and (med.startswith("Reduce ") or med.startswith("Continue ") or med.startswith("Stop ")):
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
                for e in all_drug_weights:
                    payload_entry = dict(e)
                    drug_id = e.get("drug")
                    cls = e.get("class")
                    if drug_id == "No Change" or cls == "No Change":
                        continue
                    payload_entry["class"] = _response_display_name(drug_id, cls, config)
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
                bedrock_model = (os.environ.get("BEDROCK_MODEL_ID") or "").strip()
                bedrock_kb_id = (os.environ.get("BEDROCK_KNOWLEDGE_BASE_ID") or "").strip()
                bedrock_region = (os.environ.get("BEDROCK_REGION") or "").strip() or None
                use_bedrock = bool(bedrock_model and bedrock_kb_id)
                if use_bedrock:
                    try:
                        drug_classes = _build_drug_classes_from_config(config)
                        top_two_for_prompt = top3_deesc[:2] if len(top3_deesc) >= 2 else (top3_deesc[:1] if top3_deesc else [])
                        kb_query = _build_retrieval_query(request_data, top3_deesc, is_deescalation=True)
                        targeted_query = _build_targeted_retrieval_query(top3_deesc)
                        kb_refs, kb_chunk_count = _retrieve_kb_dual_query(
                            bedrock_kb_id, kb_query, targeted_query,
                            region=bedrock_region, number_of_results=5,
                        )
                        system_message, prompt = build_claude_prompt(
                            request_data, results_deesc or [], drug_classes, patient,
                            alternative_drug_names=alternative_drug_names,
                            top_two_results=top_two_for_prompt,
                            lowest_cost_result=lowest_cost_deesc,
                            is_deescalation=True,
                            a1c_above_goal=a1c_above_goal,
                            assessment=assessment,
                            kb_references_section=kb_refs if kb_refs else None,
                        )
                        claude_temperature = float(os.environ.get("CLAUDE_TEMPERATURE", "0.3"))
                        _cache_val = (os.environ.get("BEDROCK_PROMPT_CACHE", "") or "").strip().lower()
                        use_cache = _cache_val not in ("0", "false", "no")
                        bedrock_model_to_use = (os.environ.get("BEDROCK_HAIKU_MODEL_ID") or "").strip() if _use_haiku_fallback(patient, top3_deesc) else bedrock_model
                        if bedrock_model_to_use != bedrock_model:
                            _log(f"Claude model (Bedrock): {bedrock_model_to_use} (Haiku fallback)")
                        else:
                            _log(f"Claude model (Bedrock): {bedrock_model}")
                        _log(f"Bedrock: full prompt length={len(prompt)} chars (no truncation)")
                        claude_result = call_bedrock_claude(
                            prompt,
                            bedrock_model_to_use or bedrock_model,
                            temperature=claude_temperature,
                            system_message=system_message,
                            region=bedrock_region,
                            use_cache=use_cache,
                        )
                        _log("Bedrock: succeeded")
                        top3_drugs_deesc = [o.get("medication") or o.get("drug_name") or o.get("drug") or "" for o in top3_deesc[:3]]
                        _log(json.dumps({"event": "bedrock_invocation", "input_tokens": claude_result.get("input_tokens"), "output_tokens": claude_result.get("output_tokens"), "ada_passages_retrieved": kb_chunk_count, "top3_drugs": top3_drugs_deesc}))
                        rationale = (claude_result.get("rationale") or rationale)[:15]
                        alternatives = claude_result.get("alternatives") or alternatives
                        alternatives = [a for a in alternatives if "no change" not in a.lower()]
                        future_considerations = claude_result.get("future_considerations") or future_considerations
                        updated = claude_result.get("updated_assessment", "")
                        if updated:
                            assessment = updated
                    except Exception as claude_err:
                        _log(f"Bedrock API call failed for de-escalation: {claude_err}")
                elif claude_api_key and claude_api_key.strip():
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
                        claude_temperature = float(os.environ.get("CLAUDE_TEMPERATURE", "0.2"))
                        _log(f"Claude model: {claude_model}")
                        claude_result = call_claude_api(prompt, claude_api_key, claude_model, claude_temperature, system_message=system_message)
                        rationale = (claude_result.get("rationale") or rationale)[:15]
                        alternatives = claude_result.get("alternatives") or alternatives
                        alternatives = [a for a in alternatives if "no change" not in a.lower()]
                        future_considerations = claude_result.get("future_considerations") or future_considerations
                        updated = claude_result.get("updated_assessment", "")
                        if updated:
                            assessment = updated
                    except Exception as claude_err:
                        _log(f"Claude API call failed for de-escalation: {claude_err}")
                # Build top3 for response with full display names (e.g. "Reduce Empagliflozin (Jardiance)").
                top3_deesc_for_body = []
                for opt in top3_deesc:
                    out = dict(opt)
                    if opt.get("drug") and opt.get("drug") != "No Change" and opt.get("class"):
                        full_name = _response_display_name(opt["drug"], opt["class"], config)
                        med = (opt.get("medication") or "").strip()
                        if med.startswith("Reduce"):
                            out["medication"] = f"Reduce {full_name}"
                        elif med.startswith("Continue"):
                            out["medication"] = f"Continue {full_name}"
                        elif med.startswith("Stop"):
                            out["medication"] = f"Stop {full_name}"
                        else:
                            out["medication"] = full_name
                    top3_deesc_for_body.append(out)
                # "***see future considerations***": append when Additional Context is not empty (not when AI returns items)
                additional_context = (request_data.get("additionalContext") or request_data.get("additional_context") or "").strip()
                if additional_context and "***see future considerations***" not in (assessment or ""):
                    assessment = (assessment or "").rstrip(".") + " ***see future considerations***."
                recommendation_timestamp = datetime.now(EASTERN).isoformat()
                body = {
                    "assessment": str(assessment),
                    "original_assessment": str(original_assessment),
                    "rationale": rationale,
                    "alternatives": alternatives,
                    "futureConsiderations": future_considerations,
                    "allDrugWeights": all_drug_weights_payload,
                    "top3BestOptions": top3_deesc_for_body,
                    "recommendationTimestamp": recommendation_timestamp,
                    "warning-eGFR": _eGFR_therapy_warning(patient, config),
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
        is_currently_on, current_med_info = _get_current_med_info_for_dose(patient, top_drug_id, top_class, config)
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
                is_on_this_drug, current_med_info = _get_current_med_info_for_dose(patient, drug_id, cls, config)
                med = get_recommended_dose(
                    cls,
                    patient.get("eGFR"),
                    is_currently_on=is_on_this_drug,
                    current_medication_info=current_med_info,
                    goal2_data=goal2_data,
                    preferred_drug=preferred,
                )
                med_display = _response_display_name(drug_id, cls, config)
                medication_label = f"Increase {med_display}" if is_on_this_drug else f"Start {med_display}"
                expanded_choices.append({
                    "class": cls,
                    "drug": drug_id,
                    "clinical_fit": round(float(r["clinical_fit"]), 2),
                    "coverage": round(float(r["coverage"]), 2),
                    "medication": medication_label,
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
            lc_cls = lowest_cost_result.get("class") or lowest_cost_result.get("drug") or ""
            lc_is_on, lc_med_info = _get_current_med_info_for_dose(patient, lc_drug, lc_cls, config)
            lc_med = get_recommended_dose(
                lc_cls,
                patient.get("eGFR"),
                lc_is_on,
                lc_med_info,
                goal2_data,
                preferred_drug=lc_drug,
            )
            lowest_cost_med_name = str(lc_med.get("medication", ""))

        claude_api_key = os.environ.get("CLAUDE_API_KEY")
        bedrock_model = (os.environ.get("BEDROCK_MODEL_ID") or "").strip()
        bedrock_kb_id = (os.environ.get("BEDROCK_KNOWLEDGE_BASE_ID") or "").strip()
        bedrock_region = (os.environ.get("BEDROCK_REGION") or "").strip() or None
        use_bedrock = bool(bedrock_model and bedrock_kb_id)
        use_claude = claude_api_key and claude_api_key.strip()
        if use_bedrock:
            try:
                drug_classes = _build_drug_classes_from_config(config)
                top_two_for_prompt = top_two_choices_by_fit[:2] if top_two_choices_by_fit and len(top_two_choices_by_fit) >= 2 else None
                kb_query = _build_retrieval_query(request_data, top_two_choices_by_fit or [], is_deescalation=False)
                targeted_query = _build_targeted_retrieval_query(top_two_choices_by_fit or [])
                kb_refs, kb_chunk_count = _retrieve_kb_dual_query(
                    bedrock_kb_id, kb_query, targeted_query,
                    region=bedrock_region, number_of_results=5,
                )
                system_message, prompt = build_claude_prompt(
                    request_data, results, drug_classes, patient,
                    alternative_drug_names=alternative_drug_names,
                    top_two_results=top_two_for_prompt,
                    lowest_cost_result=lowest_cost_result,
                    assessment=assessment,
                    kb_references_section=kb_refs if kb_refs else None,
                )
                claude_temperature = float(os.environ.get("CLAUDE_TEMPERATURE", "0.3"))
                _cache_val = (os.environ.get("BEDROCK_PROMPT_CACHE", "") or "").strip().lower()
                use_cache = _cache_val not in ("0", "false", "no")
                top3_for_log = list((top_two_choices_by_fit or [])[:2])
                if lowest_cost_result:
                    top3_for_log.append(lowest_cost_result)
                bedrock_model_to_use = (os.environ.get("BEDROCK_HAIKU_MODEL_ID") or "").strip() if _use_haiku_fallback(patient, top_two_choices_by_fit or []) else bedrock_model
                if bedrock_model_to_use != bedrock_model:
                    _log(f"Claude model (Bedrock): {bedrock_model_to_use} (Haiku fallback)")
                else:
                    _log(f"Claude model (Bedrock): {bedrock_model}")
                _log(f"Bedrock: full prompt length={len(prompt)} chars (no truncation)")
                claude_result = call_bedrock_claude(
                    prompt,
                    bedrock_model_to_use or bedrock_model,
                    temperature=claude_temperature,
                    system_message=system_message,
                    region=bedrock_region,
                    use_cache=use_cache,
                )
                _log("Bedrock: succeeded")
                top3_drugs_names = [o.get("medication") or o.get("drug_name") or o.get("drug") or "" for o in top3_for_log[:3]]
                _log(json.dumps({"event": "bedrock_invocation", "input_tokens": claude_result.get("input_tokens"), "output_tokens": claude_result.get("output_tokens"), "ada_passages_retrieved": kb_chunk_count, "top3_drugs": top3_drugs_names}))
                rationale = (claude_result.get("rationale") or [])[:15]
                claude_alternatives = claude_result.get("alternatives") or []
                future_considerations = claude_result.get("future_considerations") or []
                updated = claude_result.get("updated_assessment", "")
                if updated:
                    assessment = updated
                if not rationale:
                    rationale = generate_rationale(patient, top_result, top_drug_data)
            except Exception as claude_error:
                print(f"Bedrock API call failed: {claude_error}")
                rationale = generate_rationale(patient, top_result, top_drug_data)
                claude_alternatives = []
                future_considerations = []
        elif use_claude:
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
                claude_temperature = float(os.environ.get("CLAUDE_TEMPERATURE", "0.2"))
                _log(f"Claude model: {claude_model}")
                claude_result = call_claude_api(prompt, claude_api_key, claude_model, claude_temperature, system_message=system_message)
                rationale = (claude_result.get("rationale") or [])[:15]
                claude_alternatives = claude_result.get("alternatives") or []
                future_considerations = claude_result.get("future_considerations") or []
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
            lc_cls = lowest_cost_result.get("class") or lowest_cost_result.get("drug") or ""
            lc_is_on, lc_med_info = _get_current_med_info_for_dose(patient, lc_drug, lc_cls, config)
            lc_med = get_recommended_dose(
                lc_cls,
                patient.get("eGFR"),
                lc_is_on,
                lc_med_info,
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
        # Use Claude-generated alternatives only. No fallback to generate_alternatives.
        alternatives = claude_alternatives if claude_alternatives else []
        # allDrugWeights "class" column: full display name (e.g. Empagliflozin (Jardiance)).
        drugs_config = config.get("drugs", {})
        all_drug_weights_payload = []
        for e in all_drug_weights:
            payload_entry = dict(e)
            drug_id = e.get("drug")
            cls = e.get("class")
            if drug_id == "No Change" or cls == "No Change":
                continue
            payload_entry["class"] = _response_display_name(drug_id, cls, config)
            all_drug_weights_payload.append(payload_entry)

        # top3BestOptions = [0] best clinical fit, [1] 2nd best clinical fit (different class), [2] lowest cost (includes top 2 in pool)
        top3_best_options = list(top_two_choices_by_fit) if top_two_choices_by_fit else []
        if not top3_best_options and top_result:
            if top_drug_id == "No Change":
                med_label = "Continue " + _no_change_medication_label(patient, config)
            else:
                med_display = _response_display_name(top_drug_id, top_class, config)
                med_label = f"Increase {med_display}" if is_currently_on else f"Start {med_display}"
            dose_label = str(best_medication.get("dose", ""))
            top3_best_options = [{
                "class": top_class,
                "drug": top_drug_id,
                "clinical_fit": round(float(top_result.get("clinical_fit", 0)), 2),
                "coverage": round(float(top_result.get("coverage", 0)), 2),
                "medication": med_label,
                "dose": dose_label,
            }]
        # Add 3rd = cheapest option among best clinical fit (top 5). Show action prefix (Increase/Start/Continue) + drug name, no dose.
        if cheapest_for_index3 and len(top3_best_options) < 3:
            lc_drug = cheapest_for_index3.get("drug", cheapest_for_index3.get("class"))
            lc_class = cheapest_for_index3.get("class") or cheapest_for_index3.get("drug") or ""
            if lc_drug == "No Change" or lc_class == "No Change":
                no_change_choices = _no_change_choices(patient, config, cheapest_for_index3)
                if no_change_choices:
                    first = no_change_choices[0]
                    lc_drug = first["drug"]
                    lc_class = first["class"]
                    lc_med_display = _response_display_name(lc_drug, lc_class, config)
                    lc_med_display = f"Continue {lc_med_display}"
                else:
                    lc_med_display = "No medication change"
            else:
                lc_med_display = _response_display_name(lc_drug, lc_class, config)
                is_on_lc_drug, _ = _get_current_med_info_for_dose(patient, lc_drug, lc_class, config)
                lc_med_display = f"Increase {lc_med_display}" if is_on_lc_drug else f"Start {lc_med_display}"
            top3_best_options.append({
                "class": lc_class,
                "drug": lc_drug,
                "clinical_fit": round(float(cheapest_for_index3.get("clinical_fit", 0)), 2),
                "coverage": round(float(cheapest_for_index3.get("coverage", 0)), 2),
                "medication": lc_med_display,
                "dose": "",
            })

        # Build top3BestOptions for response: always use full display name (e.g. "Increase Empagliflozin (Jardiance)") via _response_display_name.
        top3_for_body = []
        for opt in top3_best_options:
            out = dict(opt)
            if opt.get("drug") and opt.get("drug") != "No Change" and opt.get("class"):
                full_name = _response_display_name(opt["drug"], opt["class"], config)
                med = (opt.get("medication") or "").strip()
                if med.startswith("Increase"):
                    out["medication"] = f"Increase {full_name}"
                elif med.startswith("Start"):
                    out["medication"] = f"Start {full_name}"
                elif med.startswith("Continue"):
                    out["medication"] = f"Continue {full_name}"
                elif med.startswith("Reduce"):
                    out["medication"] = f"Reduce {full_name}"
                elif med.startswith("Stop"):
                    out["medication"] = f"Stop {full_name}"
                else:
                    out["medication"] = full_name
            top3_for_body.append(out)

        # "***see future considerations***": append when Additional Context is not empty (not when AI returns items)
        additional_context = (request_data.get("additionalContext") or request_data.get("additional_context") or "").strip()
        if additional_context and "***see future considerations***" not in (assessment or ""):
            assessment = (assessment or "").rstrip(".") + " ***see future considerations***."

        # Build payload; ensure all values are JSON-serializable for API Gateway
        # top3BestOptions: [0]=best clinical fit, [1]=..., [2]=lowest cost
        recommendation_timestamp = datetime.now(EASTERN).isoformat()
        body = {
            "assessment": str(assessment) if assessment is not None else "",
            "original_assessment": str(original_assessment),
            "rationale": [str(x) for x in (rationale or [])],
            "alternatives": [str(x) for x in (alternatives or [])],
            "futureConsiderations": [str(x) for x in (future_considerations or [])],
            "allDrugWeights": all_drug_weights_payload,
            "top3BestOptions": top3_for_body,
            "recommendationTimestamp": recommendation_timestamp,
            "warning-eGFR": _eGFR_therapy_warning(patient, config),
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
