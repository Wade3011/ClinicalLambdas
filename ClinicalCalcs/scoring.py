"""
Clinical and coverage scoring. calculate_clinical, calculate_coverage, calculate_scores, get_all_drug_weight_details.
Uses rule_interpreter for structured rules; glucose for Goal 3; goal1/goal3 JSON when provided.
"""
# Generic denied_reason when a drug is excluded due to patient allergy (class or drug-level).
ALLERGY_DENIED_REASON = "Allergy to this drug/class"

from rule_interpreter import evaluate_structured_rule
from dosing import calculate_next_dose
from glucose import (
    calculate_goal3_boost,
    calculate_goal3_on_therapy_max_boost,
    get_goal3_boost_breakdown,
    estimate_fasting_from_a1c,
    estimate_post_prandial_from_a1c,
    get_target_fasting,
    get_target_post_prandial,
    FASTING_LOWERING_POTENTIAL,
    POST_PRANDIAL_LOWERING_POTENTIAL,
)


def _rule_context(patient, normalized_glucose=None, goal3_data=None):
    """Build context dict for rule_interpreter from patient. Optionally add fasting_above_goal, post_prandial_above_goal (mg/dL above target), fasting_avg, lows_detected."""
    ctx = {
        "eGFR": patient.get("eGFR"),
        "a1c": patient.get("a1c"),
        "age": patient.get("age"),
        "goal": patient.get("goal"),
        "comorbidities": patient.get("comorbidities"),
        "allergy_labels_set": patient.get("allergy_labels_set"),
    }
    if normalized_glucose is not None and goal3_data is not None:
        goal = patient.get("goal", 7.5)
        target_fasting = get_target_fasting(goal, goal3_data)
        target_post_prandial = get_target_post_prandial(goal, goal3_data)
        fasting_avg = normalized_glucose.get("fasting_avg")
        if fasting_avg is None and patient.get("a1c") is not None:
            fasting_avg = estimate_fasting_from_a1c(patient.get("a1c"), goal3_data)
        post_pp_avg = normalized_glucose.get("post_pp_avg")
        if post_pp_avg is None and patient.get("a1c") is not None:
            post_pp_avg = estimate_post_prandial_from_a1c(patient.get("a1c"), goal3_data)
        ctx["fasting_above_goal"] = (float(fasting_avg) - target_fasting) if fasting_avg is not None else None
        ctx["post_prandial_above_goal"] = (float(post_pp_avg) - target_post_prandial) if post_pp_avg is not None else None
    # Add fasting_avg and lows_detected for Basal Insulin guardrail (fasting at goal with lows)
    if normalized_glucose is not None:
        fasting_avg = normalized_glucose.get("fasting_avg")
        if fasting_avg is None and patient.get("a1c") is not None:
            fasting_avg = estimate_fasting_from_a1c(patient.get("a1c"), goal3_data)
        ctx["fasting_avg"] = float(fasting_avg) if fasting_avg is not None else None
        lows = normalized_glucose.get("lows_detected") or normalized_glucose.get("lows_overnight") or normalized_glucose.get("lows_after_meals")
        if not lows:
            comorbidities = patient.get("comorbidities") or set()
            if hasattr(comorbidities, "__iter__") and not isinstance(comorbidities, str):
                cm = set(str(x).strip().upper() for x in comorbidities)
            else:
                cm = {str(comorbidities).strip().upper()} if comorbidities else set()
            lows = bool({"FREQUENT HYPOGLYCEMIA", "HISTORY OF HYPOGLYCEMIA"} & cm)
        ctx["lows_detected"] = 1 if lows else 0
    return ctx


HIGH_HYPO_RISK_CLASSES = frozenset({"Sulfonylurea", "Basal Insulin", "Bolus Insulin"})


def _apply_hypoglycemia_penalty(patient, normalized_glucose, drug_class):
    """Apply single penalty when lows detected for high-risk medications (3.1 CGM lows).
    Hierarchy: overnight > any lows > post-meal (Bolus only). Uses CGM data or comorbidities fallback."""
    if drug_class not in HIGH_HYPO_RISK_CLASSES:
        return 0.0, None

    if normalized_glucose is not None:
        lows_overnight = bool(normalized_glucose.get("lows_overnight"))
        lows_detected = bool(
            normalized_glucose.get("lows_detected")
            or normalized_glucose.get("lows_overnight")
            or normalized_glucose.get("lows_after_meals")
        )
        lows_after_meals = bool(normalized_glucose.get("lows_after_meals"))
    else:
        lows_overnight = False
        lows_after_meals = False
        comorbidities = patient.get("comorbidities") or set()
        if hasattr(comorbidities, "__iter__") and not isinstance(comorbidities, str):
            cm = set(str(x).strip().upper() for x in comorbidities)
        else:
            cm = {str(comorbidities).strip().upper()} if comorbidities else set()
        lows_detected = bool({"FREQUENT HYPOGLYCEMIA", "HISTORY OF HYPOGLYCEMIA"} & cm)

    if lows_overnight:
        return -0.20, "Overnight lows detected - high hypoglycemia risk"
    if lows_detected:
        return -0.15, "Hypoglycemia detected - use with caution"
    if lows_after_meals and drug_class == "Bolus Insulin":
        return -0.10, "Post-meal lows detected - review bolus timing/dose"

    return 0.0, None


def _rule_to_description(rule):
    """Human-readable string for a rule dict (for Notes display). Returns short label like 'A1C > 8%', 'CKD'."""
    if not isinstance(rule, dict):
        return str(rule)
    if "and" in rule:
        parts = [_rule_to_description(r) for r in rule["and"]]
        return " and ".join(parts)
    field = rule.get("field", "")
    op = rule.get("op", "")
    val = rule.get("value")
    if field == "fasting_avg" and op == "lt":
        return f"Fasting at goal (<{val} mg/dL)"
    if field == "lows_detected" and op in ("ge", "eq") and val >= 1:
        return "Lows detected"
    if field == "a1c":
        if op == "gt":
            return f"A1C > {val}%"
        if op == "ge":
            return f"A1C ≥ {val}%"
    if field == "comorbidity":
        return str(val) if val else field
    if field == "eGFR":
        if op == "lt":
            return f"eGFR < {val}"
        if op == "ge":
            return f"eGFR ≥ {val}"
        if op == "gt":
            return f"eGFR > {val}"
    if field == "age":
        if op == "lt":
            return f"Age < {val}"
        if op == "ge":
            return f"Age ≥ {val}"
    if field == "fasting_above_goal":
        if op == "ge":
            return f"Fasting glucose ≥{val} mg/dL above goal"
        if op == "gt":
            return f"Fasting glucose >{val} mg/dL above goal"
    if field == "post_prandial_above_goal":
        if op == "ge":
            return f"Post-prandial glucose ≥{val} mg/dL above goal"
        if op == "gt":
            return f"Post-prandial glucose >{val} mg/dL above goal"
    return field or "rule"


def _patient_on_class_at_max_dose(patient, drug_class, drugs_config):
    """True if patient has any current drug in drug_class that is at max dose (entire class should be excluded for add-on)."""
    if not drug_class or drug_class == "No Change" or not drugs_config:
        return False
    current_ids = patient.get("current_drug_ids", set())
    med_info_dict = patient.get("current_medication_info", {})
    for cid in current_ids:
        cdata = drugs_config.get(cid) if isinstance(drugs_config, dict) else None
        if not cdata or cdata.get("class") != drug_class:
            continue
        med_info = med_info_dict.get(cid)
        if not med_info or not med_info.get("dose"):
            continue
        _, is_at_max = calculate_next_dose(
            cdata.get("class", drug_class),
            med_info.get("dose", ""),
            med_info.get("frequency", ""),
            patient.get("eGFR"),
            med_info.get("drugName"),
        )
        if is_at_max:
            return True
    return False


def calculate_clinical(drug_id, drug_data, patient, rules_json=None, goal1_data=None, include_current_therapy_boost=True, normalized_glucose=None, goal3_data=None, drugs_config=None):
    """Clinical fit score using drug-level rules. drug_data has class, clinical_base, deny_if, caution_if, clinical_boost.
    Allergy at drug level: drug_id in allergy_drug_ids; current-therapy boost uses drug_id in current_drug_ids.
    Exclude drug if patient is on it and at max dose (treated like deny_if).
    Exclude entire class if patient is on any drug in same class at max dose (do not add another drug in same class).
    normalized_glucose/goal3_data optional for caution rules that use fasting_above_goal, post_prandial_above_goal."""
    if drug_id in patient.get("allergy_drug_ids", set()):
        return 0.0
    drug_class = drug_data.get("class", drug_id)
    # Exclude if patient is on this drug and already at max dose (no room to titrate)
    if drug_id in patient.get("current_drug_ids", set()):
        med_info = patient.get("current_medication_info", {}).get(drug_id)
        if med_info and med_info.get("dose"):
            _, is_at_max = calculate_next_dose(
                drug_class,
                med_info.get("dose", ""),
                med_info.get("frequency", ""),
                patient.get("eGFR"),
                med_info.get("drugName"),
            )
            if is_at_max:
                return 0.0
    # Exclude entire class if patient is on any drug in this class at max dose (do not add another in same class)
    if _patient_on_class_at_max_dose(patient, drug_class, drugs_config):
        return 0.0
    score = drug_data.get("clinical_base", 0.5)
    context = _rule_context(patient, normalized_glucose, goal3_data)

    for rule in drug_data.get("deny_if", []):
        if isinstance(rule, dict) and evaluate_structured_rule(rule, context):
            return 0.0
    for boost in drug_data.get("clinical_boost", []):
        r = boost.get("rule", boost) if isinstance(boost, dict) else boost
        if isinstance(r, dict) and evaluate_structured_rule(r, context):
            score += boost.get("add", 0)
    for caution in drug_data.get("caution_if", []):
        r = caution.get("rule", caution) if isinstance(caution, dict) else caution
        if isinstance(r, dict) and evaluate_structured_rule(r, context):
            score -= caution.get("penalty", 0)

    # 3.1 CGM lows: penalty for high-risk drugs when lows detected
    hypo_penalty, _ = _apply_hypoglycemia_penalty(patient, normalized_glucose, drug_class)
    score += hypo_penalty

    score += drug_data.get("drug_in_class_bonus", 0)

    if patient["goal"] <= 7.0:
        score += 0.05
    elif patient["goal"] <= 7.5:
        score += 0.03
    if include_current_therapy_boost:
        current_therapy_boost = (goal1_data or {}).get("current_therapy_boost", 0.20)
        if drug_id in patient.get("current_drug_ids", set()):
            score += current_therapy_boost
    score = min(score, 1.0)  # allow 1.0 for No Change; other drugs capped at 0.90 below
    if drug_class != "No Change":
        score = min(score, 0.90)
    score = max(score, 0.0)
    return round(score, 2)


def insurance_adjustment(patient):
    """Insurance-based adjustment to coverage score."""
    return {"va": 0.10, "medicare": 0.05, "medicaid": -0.05, "no insurance": -0.25}.get(patient["insurance"].lower(), 0.0)


def cost_tier_penalty(drug_data):
    """Cost and tier penalties."""
    cost_penalty = {"very_high": -0.10, "high": -0.07, "medium": -0.03, "low": 0.05}.get(drug_data.get("cost", "medium"), 0.0)
    tier_penalty = {4: -0.12, 3: -0.08, 2: -0.03, 1: 0.02}.get(drug_data.get("tier", 2), 0.0)
    return cost_penalty + tier_penalty


def pa_penalty(drug_data):
    """Prior authorization penalty."""
    return -0.20 if drug_data.get("pa_required", False) else 0.0


def va_pdf_boost(drug_data):
    """VA PDF boost by cost tier."""
    if not drug_data.get("va_pdf_exists", False):
        return 0.0
    return {"low": 0.15, "medium": 0.20, "high": 0.25, "very_high": 0.30}.get(drug_data.get("cost", "medium"), 0.20)


def cgm_boost(patient):
    """CGM monitoring boost."""
    return 0.02 if patient.get("monitor", "").lower() == "cgm" else 0.0


def calculate_coverage(drug_or_class_data, patient, drug_deny_if=None):
    """Coverage/access score at drug level (cost, tier, base_access_score, etc.). drug_deny_if optional."""
    context = _rule_context(patient)
    deny_rules = drug_deny_if if drug_deny_if is not None else drug_or_class_data.get("deny_if", [])
    for rule in deny_rules:
        if isinstance(rule, dict) and evaluate_structured_rule(rule, context):
            return 0.0, "Denied by absolute contraindication"
    score = drug_or_class_data.get("base_access_score", 0.6)
    score += insurance_adjustment(patient)
    score += cost_tier_penalty(drug_or_class_data)
    score += pa_penalty(drug_or_class_data)
    score += va_pdf_boost(drug_or_class_data)
    score += cgm_boost(patient)
    score = min(score, 0.90)
    return round(max(score, 0), 2), "Calculated coverage score"


def calculate_scores(config, patient, rules_json=None, normalized_glucose=None, goal1_data=None, goal3_data=None):
    """Clinical and coverage scores per drug. config = {classes, drugs}. #1/#2 ranked by clinical_fit (includes current-therapy boost) to match trace log display."""
    classes = config.get("classes", {})
    drugs = config.get("drugs", {})
    if not drugs and "drug_classes" in config:
        drugs = config["drug_classes"]
        classes = {k: {**v, "allergy_labels": v.get("allergy_labels", [])} for k, v in drugs.items()}
    results = []
    for drug_id, drug_data in drugs.items():
        drug_class = drug_data.get("class", drug_id)
        clinical = calculate_clinical(drug_id, drug_data, patient, None, goal1_data, include_current_therapy_boost=True, normalized_glucose=normalized_glucose, goal3_data=goal3_data, drugs_config=drugs)
        clinical_rank = calculate_clinical(drug_id, drug_data, patient, None, goal1_data, include_current_therapy_boost=False, normalized_glucose=normalized_glucose, goal3_data=goal3_data, drugs_config=drugs)
        coverage, coverage_reason = calculate_coverage(drug_data, patient, drug_deny_if=drug_data.get("deny_if"))
        if clinical == 0.0:
            continue
        if normalized_glucose and drug_id != "No Change" and drug_class != "No Change":
            goal3_boost = calculate_goal3_boost(drug_id, drug_class, patient, normalized_glucose, goal3_data)
            clinical += goal3_boost
            clinical += calculate_goal3_on_therapy_max_boost(drug_id, drug_class, patient, normalized_glucose, goal3_data)
            clinical = max(0.0, min(1.0, clinical))
            clinical_rank += goal3_boost
            clinical_rank += calculate_goal3_on_therapy_max_boost(drug_id, drug_class, patient, normalized_glucose, goal3_data)
            clinical_rank = max(0.0, min(1.0, clinical_rank))
        if clinical <= 0.0:
            continue
        clinical = max(0.0, min(1.0, clinical))
        clinical_rank = max(0.0, min(1.0, clinical_rank))
        results.append({
            "drug": drug_id,
            "class": drug_class,
            "clinical_fit": round(clinical, 2),
            "clinical_fit_rank": round(clinical_rank, 2),
            "coverage": round(coverage, 2),
            "rationale": f"Clinical fit {round(clinical, 2)}; {coverage_reason}",
        })
    return sorted(results, key=lambda x: (x["clinical_fit"], x["coverage"]), reverse=True)


def get_all_drug_weight_details(config, patient, rules_json=None, normalized_glucose=None, goal1_data=None, goal3_data=None):
    """Detailed weight info per drug (denied, boosts, cautions, Goal 3). config = {classes, drugs}. All at drug level."""
    drugs = config.get("drugs", config.get("drug_classes", {}))
    all_details = []
    current_therapy_boost = (goal1_data or {}).get("current_therapy_boost", 0.20)
    for drug_id, drug_data in drugs.items():
        drug_class = drug_data.get("class", drug_id)
        clinical = calculate_clinical(drug_id, drug_data, patient, None, goal1_data, include_current_therapy_boost=True, normalized_glucose=normalized_glucose, goal3_data=goal3_data, drugs_config=drugs)
        clinical_rank = calculate_clinical(drug_id, drug_data, patient, None, goal1_data, include_current_therapy_boost=False, normalized_glucose=normalized_glucose, goal3_data=goal3_data, drugs_config=drugs)
        goal3_boost = 0.0
        goal3_info = None
        if normalized_glucose and clinical > 0.0 and drug_id != "No Change" and drug_class != "No Change":
            goal3_boost = calculate_goal3_boost(drug_id, drug_class, patient, normalized_glucose, goal3_data)
            clinical += goal3_boost
            clinical += calculate_goal3_on_therapy_max_boost(drug_id, drug_class, patient, normalized_glucose, goal3_data)
            clinical_rank += goal3_boost
            clinical_rank += calculate_goal3_on_therapy_max_boost(drug_id, drug_class, patient, normalized_glucose, goal3_data)
            clinical_rank = max(0.0, clinical_rank)
            goal = patient.get("goal", 7.5)
            a1c = patient.get("a1c", 0)
            fasting_current = normalized_glucose.get("fasting_avg") or estimate_fasting_from_a1c(a1c, goal3_data)
            post_pp_current = normalized_glucose.get("post_pp_avg") or estimate_post_prandial_from_a1c(a1c, goal3_data)
            target_fasting = get_target_fasting(goal, goal3_data)
            target_post_prandial = get_target_post_prandial(goal, goal3_data)
            is_currently_on = drug_id in patient.get("current_drug_ids", set())
            by_drug = (goal3_data or {}).get("potency_by_drug") or {}
            by_class = (goal3_data or {}).get("potency_by_class") or {}
            pcls = by_drug.get(drug_id) or by_class.get(drug_class, {})
            fp = pcls.get("fasting") if isinstance(pcls, dict) else FASTING_LOWERING_POTENTIAL.get(drug_class, 0)
            pp = pcls.get("post_prandial") if isinstance(pcls, dict) else POST_PRANDIAL_LOWERING_POTENTIAL.get(drug_class, 0)
            if fp is None:
                fp = FASTING_LOWERING_POTENTIAL.get(drug_class, 0)
            if pp is None:
                pp = POST_PRANDIAL_LOWERING_POTENTIAL.get(drug_class, 0)
            fasting_potential = fp * (0.5 if is_currently_on else 1.0)
            post_pp_potential = pp * (0.5 if is_currently_on else 1.0)
            goal3_info = {
                "boost": goal3_boost,
                "fasting_current": fasting_current,
                "post_prandial_current": post_pp_current,
                "target_fasting": target_fasting,
                "target_post_prandial": target_post_prandial,
                "fasting_potential": fasting_potential,
                "post_prandial_potential": post_pp_potential,
                "is_currently_on": is_currently_on,
            }
        coverage, coverage_reason = calculate_coverage(drug_data, patient, drug_deny_if=drug_data.get("deny_if"))
        context = _rule_context(patient, normalized_glucose, goal3_data)
        denied_reasons = []
        if drug_id in patient.get("allergy_drug_ids", set()):
            denied_reasons.append(ALLERGY_DENIED_REASON)
        if drug_id in patient.get("current_drug_ids", set()):
            med_info = patient.get("current_medication_info", {}).get(drug_id)
            if med_info and med_info.get("dose"):
                _, is_at_max = calculate_next_dose(
                    drug_class,
                    med_info.get("dose", ""),
                    med_info.get("frequency", ""),
                    patient.get("eGFR"),
                    med_info.get("drugName"),
                )
                if is_at_max:
                    denied_reasons.append("Patient at max dose of this drug")
        if drug_id not in patient.get("current_drug_ids", set()) and _patient_on_class_at_max_dose(patient, drug_class, drugs):
            denied_reasons.append("Patient already on this drug class at max dose; do not add another drug in same class")
        for rule in drug_data.get("deny_if", []):
            if isinstance(rule, dict) and evaluate_structured_rule(rule, context):
                denied_reasons.append(_rule_to_description(rule) if isinstance(rule, dict) else str(rule))
        # Clinical log notes: include base score and goal boost, then all boosts and penalties
        clinical_base = drug_data.get("clinical_base", 0.5)
        goal = patient.get("goal", 7.5)
        applied_boosts = [{"condition": f"Clinical base (+{clinical_base:.2f})", "add": clinical_base}]
        if goal <= 7.0:
            applied_boosts.append({"condition": "A1C goal <7% (+0.05)", "add": 0.05})
        elif goal <= 7.5:
            applied_boosts.append({"condition": "A1C goal <7.5% (+0.03)", "add": 0.03})
        for boost in drug_data.get("clinical_boost", []):
            r = boost.get("rule", boost) if isinstance(boost, dict) else boost
            if isinstance(r, dict) and evaluate_structured_rule(r, context):
                desc = _rule_to_description(r)
                add_val = boost.get("add", 0)
                applied_boosts.append({"condition": f"{desc} (+{add_val:.2f})", "add": add_val})
        if drug_id in patient.get("current_drug_ids", set()):
            applied_boosts.append({"condition": f"Current therapy (+{current_therapy_boost:.2f})", "add": current_therapy_boost})
        drug_bonus = drug_data.get("drug_in_class_bonus", 0)
        if drug_bonus != 0:
            applied_boosts.append({"condition": f"Drug in class ({'+' if drug_bonus > 0 else ''}{drug_bonus:.3f})", "add": drug_bonus})
        # Goal 3 glucose breakdown for Notes (fasting, post-prandial, on-therapy only). Skip for No Change.
        if normalized_glucose and clinical > 0.0 and drug_id != "No Change" and drug_class != "No Change":
            g3 = get_goal3_boost_breakdown(drug_id, drug_class, patient, normalized_glucose, goal3_data)
            if g3.get("goal3_fasting"):
                applied_boosts.append({"condition": "Goal 3 fasting (+0.05)", "add": 0.05})
            if g3.get("goal3_post_prandial"):
                applied_boosts.append({"condition": "Goal 3 post-prandial (+0.05)", "add": 0.05})
            if g3.get("goal3_on_therapy"):
                applied_boosts.append({"condition": "Goal 3 on-therapy (+0.05)", "add": 0.05})
        applied_cautions = []
        for caution in drug_data.get("caution_if", []):
            r = caution.get("rule", caution) if isinstance(caution, dict) else caution
            if isinstance(r, dict) and evaluate_structured_rule(r, context):
                desc = _rule_to_description(r)
                penalty_val = caution.get("penalty", 0)
                applied_cautions.append({"condition": f"{desc} (-{penalty_val:.2f})", "penalty": penalty_val})
        # 3.1 CGM lows: show hypoglycemia penalty in applied_cautions when applicable
        hypo_penalty, hypo_reason = _apply_hypoglycemia_penalty(patient, normalized_glucose, drug_class)
        if hypo_penalty != 0 and hypo_reason:
            applied_cautions.append({"condition": f"{hypo_reason} (-{abs(hypo_penalty):.2f})", "penalty": abs(hypo_penalty)})
        denied = bool(denied_reasons) or clinical == 0.0 or (coverage == 0.0 and "Denied" in (coverage_reason or ""))
        final_clinical = round(max(0.0, min(1.0, clinical)), 2)
        # Zero out coverage in the log when clinical fit is zeroed out
        effective_coverage = 0.0 if final_clinical == 0.0 else round(float(coverage), 2)
        detail = {
            "drug": drug_id,
            "class": drug_class,
            "clinical_fit": final_clinical,
            "clinical_fit_rank": round(max(0.0, min(1.0, clinical_rank)), 2),
            "coverage": effective_coverage,
            "denied": denied,
            "denied_reasons": denied_reasons,
            "applied_boosts": applied_boosts,
            "applied_cautions": applied_cautions,
            "coverage_reason": coverage_reason if final_clinical > 0.0 else (coverage_reason or "Clinical fit 0"),
        }
        if goal3_info:
            detail["goal3_potency"] = goal3_info
        all_details.append(detail)
    return all_details
