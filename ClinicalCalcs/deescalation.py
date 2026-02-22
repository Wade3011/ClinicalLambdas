"""
De-escalation logic: when A1C at goal + lows detected, recommend reduce/maintain instead of add.
Uses Diabetes Med De-escalation handout rules. Outputs same structure as top3BestOptions for frontend.
"""
import re


def _lows_detected(patient, normalized_glucose):
    """Same logic as scoring: CGM lows or comorbidities (Frequent/History of Hypoglycemia)."""
    if normalized_glucose:
        lows = (
            normalized_glucose.get("lows_detected")
            or normalized_glucose.get("lows_overnight")
            or normalized_glucose.get("lows_after_meals")
        )
        if lows:
            return True, bool(normalized_glucose.get("lows_overnight")), bool(normalized_glucose.get("lows_after_meals"))
    cm = patient.get("comorbidities") or set()
    if hasattr(cm, "__iter__") and not isinstance(cm, str):
        cm = set(str(x).strip().upper() for x in cm)
    else:
        cm = {str(cm).strip().upper()} if cm else set()
    if {"FREQUENT HYPOGLYCEMIA", "HISTORY OF HYPOGLYCEMIA"} & cm:
        return True, False, False  # Comorbidity: assume any lows; no timing
    return False, False, False


def should_recommend_deescalation(patient, normalized_glucose):
    """Return True when lows detected (CGM or comorbidities).
    Runs regardless of A1C: hypoglycemia requires dose reduction (e.g. sulfonylurea) first.
    When A1C > goal, reduce offending drug first; add-on can be considered after reduction."""
    has_lows, _, _ = _lows_detected(patient, normalized_glucose)
    if not has_lows:
        return False
    # Need current meds to reduce; otherwise falls through to normal add-on flow
    if not patient.get("current_drug_ids"):
        return False
    return True


def _parse_dose_mg(dose_str):
    """Extract first numeric value (mg) from dose string. Returns (value, is_weekly)."""
    if not dose_str or not isinstance(dose_str, str):
        return None, False
    s = str(dose_str).strip().lower()
    # Match "10 mg", "10mg", "500 mg BID", "1 mg weekly"
    m = re.search(r"([\d.]+)\s*(?:mg|milligram)", s)
    if not m:
        m = re.search(r"([\d.]+)\s*(?:g|gram)", s)  # 1g metformin
    if not m:
        m = re.search(r"^([\d.]+)\s", s)  # "10 daily"
    if not m:
        m = re.search(r"([\d.]+)", s)  # Any number
    if m:
        try:
            val = float(m.group(1))
            is_weekly = "week" in s or "weekly" in s
            return val, is_weekly
        except (ValueError, TypeError):
            pass
    return None, False


def _parse_insulin_units(dose_str):
    """Extract total daily units from dose string. Handles '20 units', '10 U daily', '5-5-5'."""
    if not dose_str or not isinstance(dose_str, str):
        return None
    s = str(dose_str).strip().lower()
    # "20 units", "20 u", "10 units at bedtime"
    m = re.search(r"([\d.]+)\s*(?:units?|u)\b", s)
    if m:
        try:
            return float(m.group(1))
        except (ValueError, TypeError):
            pass
    # Sum of multiple: "5-5-5" or "5, 5, 5" = 15
    parts = re.split(r"[-,\s]+", s)
    nums = []
    for p in parts:
        try:
            n = float(p.strip())
            if 0 <= n <= 200:
                nums.append(n)
        except (ValueError, TypeError):
            pass
    if len(nums) >= 2:
        return sum(nums)
    if len(nums) == 1:
        return nums[0]
    m = re.search(r"([\d.]+)", s)
    if m:
        try:
            return float(m.group(1))
        except (ValueError, TypeError):
            pass
    return None


def _sulfonylurea_suggestion(drug_id, dose_str):
    """Handout 1.1/2.3: Glipizide/Glyburide 10+ -> cut in half, <10 -> stop. Glimepiride 4+ -> cut in half, <4 -> stop."""
    val, _ = _parse_dose_mg(dose_str)
    if val is None:
        return "Reduce " + (drug_id or "Sulfonylurea"), "Consult handout for dose reduction"
    drug = (drug_id or "").lower()
    if "glimepiride" in drug:
        if val >= 4:
            return "Reduce " + (drug_id or "Glimepiride"), f"Cut dose in half (from {val} mg daily)"
        return "Stop " + (drug_id or "Glimepiride"), "Less than 4 mg daily per handout"
    # Glipizide, Glyburide
    if val >= 10:
        return "Reduce " + (drug_id or "Sulfonylurea"), f"Cut dose in half (from {val} mg daily)"
    return "Stop " + (drug_id or "Sulfonylurea"), "Less than 10 mg daily per handout"


def _basal_insulin_suggestion(drug_id, dose_str):
    """Handout 1.2/2.5: 21+ -> reduce 20%; 10-20 -> cut in half; <10 -> stop. Uses drug_id for display (e.g. Glargine)."""
    units = _parse_insulin_units(dose_str)
    label = drug_id or "Basal Insulin"
    if units is None:
        return "Reduce " + label, "Consider dose reduction per handout"
    if units >= 21:
        new_val = round(units * 0.8, 0)
        return "Reduce " + label, f"Reduce total daily dose by 20% (e.g. to ~{int(new_val)} units)"
    if units >= 10:
        return "Reduce " + label, f"Cut dose in half (from {units} units)"
    return "Stop " + label, "Less than 10 units per handout"


def _bolus_insulin_suggestion(drug_id, dose_str):
    """Handout 2.1: 15+ -> reduce 20%; 6-14 -> cut in half; ≤5 -> stop. Uses drug_id for display (e.g. Lispro)."""
    units = _parse_insulin_units(dose_str)
    label = drug_id or "Bolus Insulin"
    if units is None:
        return "Reduce " + label, "Consider dose reduction per handout"
    if units >= 15:
        new_val = round(units * 0.8, 0)
        return "Reduce " + label, f"Reduce total daily dose by 20% (e.g. to ~{int(new_val)} units)"
    if units >= 6:
        return "Reduce " + label, f"Cut dose in half (from {units} units)"
    return "Stop " + label, "5 units or less per handout"


def _pioglitazone_suggestion(drug_id, dose_str):
    """Handout 2.2: reduce by 15 mg; 15 mg -> stop."""
    val, _ = _parse_dose_mg(dose_str)
    if val is None:
        return "Reduce Pioglitazone", "Decrease dose by 15 mg daily"
    if val <= 15:
        return "Stop Pioglitazone", "At 15 mg daily per handout"
    return "Reduce Pioglitazone", f"Decrease dose by 15 mg (from {val} mg daily)"


def _metformin_suggestion(drug_id, dose_str):
    """Handout: cut in half or stop if 500 mg. No fallback: use drug_id only (frontend sends required data)."""
    val, _ = _parse_dose_mg(dose_str)
    med = drug_id if drug_id else ""
    if val is None:
        return med, "Cut dose in half or stop"
    if val <= 500:
        return med, "At 500 mg per handout"
    return med, "Cut dose in half or stop"


def _glp1_suggestion(drug_id, dose_str):
    """Handout 2.4: drug-specific stepdown. Simplified: go to next lower dose."""
    drug = (drug_id or "").lower()
    if "semaglutide" in drug or "ozempic" in drug:
        return "Reduce Semaglutide", "Go to next lower dose (e.g. 1 mg -> 0.5 mg weekly)"
    if "dulaglutide" in drug or "trulicity" in drug:
        return "Reduce Dulaglutide", "Stepdown: 4.5 -> 3 -> 1.5 -> 0.75 mg weekly"
    if "tirzepatide" in drug or "mounjaro" in drug:
        return "Reduce Tirzepatide", "Stepdown: 15 -> 12.5 -> 10 -> 7.5 -> 5 -> 2.5 mg weekly"
    if "liraglutide" in drug or "victoza" in drug:
        return "Reduce Liraglutide", "Decrease by 0.6 mg weekly"
    if "rybelsus" in drug:
        return "Reduce Rybelsus", "Stepdown: 14 -> 7 -> 3 mg daily"
    return "Reduce GLP-1", "Go to next lower dose per handout"


def _dpp4_suggestion(drug_id):
    """Handout: Sitagliptin - stop therapy."""
    return "Stop " + (drug_id or "DPP-4"), "Consider stop due to replaceable efficacy"


def _sglt2_suggestion(drug_id, comorbidities):
    """Handout: Empagliflozin - stop unless CHF or CKD; then cut in half."""
    cm = comorbidities or set()
    cm = set(str(x).strip().upper() for x in cm) if hasattr(cm, "__iter__") and not isinstance(cm, str) else {str(cm).upper()}
    if "HEART FAILURE (CHF)" in cm or "CHF" in cm or "CKD" in cm:
        return "Reduce " + (drug_id or "SGLT2"), "Cut dose in half (CHF/CKD present)"
    return "Stop " + (drug_id or "SGLT2"), "Stop unless CHF or CKD; then cut in half"


def _get_reduction_suggestion(drug_id, drug_class, med_info, overnight, daytime, comorbidities):
    """Return (medication, dose) for display in top3BestOptions shape."""
    dose_str = ""
    if med_info and isinstance(med_info, dict):
        dose_str = (med_info.get("dose") or "") + " " + (med_info.get("frequency") or "")
    dose_str = dose_str.strip() or (med_info.get("dose") if isinstance(med_info, dict) else "")

    # Overnight priority: Sulfonylurea, Basal Insulin, then others
    # Daytime priority: Bolus Insulin, Pioglitazone, Sulfonylurea, GLP-1, Basal Insulin, then others
    if drug_class == "Sulfonylurea":
        return _sulfonylurea_suggestion(drug_id, dose_str)
    if drug_class == "Basal Insulin":
        return _basal_insulin_suggestion(drug_id, dose_str)
    if drug_class == "Bolus Insulin":
        return _bolus_insulin_suggestion(drug_id, dose_str)
    if drug_class == "TZD" or (drug_id and "pioglitazone" in str(drug_id).lower()):
        return _pioglitazone_suggestion(drug_id, dose_str)
    if drug_class == "Metformin":
        return _metformin_suggestion(drug_id, dose_str)
    if drug_class == "GLP1":
        return _glp1_suggestion(drug_id, dose_str)
    if drug_class == "DPP4":
        return _dpp4_suggestion(drug_id)
    if drug_class == "SGLT2":
        return _sglt2_suggestion(drug_id, comorbidities)
    return "Review " + (drug_class or "medication"), "Consider dose reduction per handout"


def _get_priority_and_fallback(overnight, daytime):
    """Return (priority_classes, fallback_classes). Priority = reduce first if present.
    Fallback = only when neither priority class is present. Per handout.
    Daytime-only lows: prioritize mealtime/bolus only; exclude basal (lows not overnight)."""
    if overnight:
        priority = ["Sulfonylurea", "Basal Insulin"]
        fallback = ["TZD", "Metformin", "GLP1", "DPP4", "Bolus Insulin", "SGLT2"]
        return priority, fallback
    if daytime:
        # Daytime-only lows: focus on mealtime/bolus (and other daytime drivers). Do not suggest basal reduction.
        priority = ["Bolus Insulin", "TZD", "Sulfonylurea", "GLP1"]
        fallback = ["DPP4", "Metformin", "SGLT2"]
        return priority, fallback
    priority = ["Sulfonylurea", "Basal Insulin", "Bolus Insulin"]
    fallback = ["TZD", "Metformin", "GLP1", "DPP4", "SGLT2"]
    return priority, fallback


def _build_maintain_options(patient, config, reduce_classes):
    """Build maintain options for drugs we're not reducing."""
    reduce_set = set(reduce_classes)
    drugs_config = config.get("drugs", {}) or {}
    maint = []
    for drug_id, med_info in (patient.get("current_medication_info") or {}).items():
        if not med_info or not isinstance(med_info, dict):
            continue
        drug_cfg = drugs_config.get(drug_id, {})
        cls = drug_cfg.get("class", drug_id)
        if cls in reduce_set:
            continue
        dose = med_info.get("dose", "")
        freq = med_info.get("frequency", "")
        dose_display = f"{dose} {freq}".strip() if freq else (dose or "at current dose")
        display_name = drug_cfg.get("display_name") or drug_id
        if "(" in str(display_name):
            display_name = display_name  # e.g. "Glargine (Lantus)"
        else:
            display_name = f"{display_name} ({drug_id})" if display_name != drug_id else drug_id
        maint.append({
            "class": cls,
            "drug": drug_id,
            "clinical_fit": 1.0,
            "coverage": 1.0,
            "medication": f"Continue {display_name}",
            "dose": dose_display or "at current dose",
        })
    return maint


def get_deescalation_recommendations(patient, normalized_glucose, config):
    """
    Build de-escalation options in top3BestOptions shape.
    Returns (reduce_options, maintain_options, assessment_suffix).
    reduce_options and maintain_options are lists of {medication, dose, class, drug, clinical_fit, coverage}.
    """
    has_lows, overnight, daytime = _lows_detected(patient, normalized_glucose)
    if not has_lows:
        return [], [], ""

    comorbidities = patient.get("comorbidities") or set()
    priority_classes, fallback_classes = _get_priority_and_fallback(overnight, daytime)
    drugs_config = config.get("drugs", {}) or {}

    def _patient_has_class(cls):
        for did in patient.get("current_drug_ids") or set():
            cfg = drugs_config.get(did, {})
            if isinstance(cfg, dict) and cfg.get("class") == cls:
                return True
        return False

    # Per handout: reduce priority classes if present. No fallback list when none present.
    has_any_priority = any(_patient_has_class(c) for c in priority_classes)
    classes_to_reduce = priority_classes if has_any_priority else []

    reduce_options = []
    reduce_classes = []

    for cls in classes_to_reduce:
        if cls in reduce_classes:
            continue
        drug_id = None
        med_info = None
        for did in patient.get("current_drug_ids") or set():
            cfg = drugs_config.get(did, {})
            if isinstance(cfg, dict) and cfg.get("class") == cls:
                drug_id = did
                med_info = (patient.get("current_medication_info") or {}).get(did)
                break
        if not drug_id:
            continue
        reduce_classes.append(cls)
        med, dose = _get_reduction_suggestion(drug_id, cls, med_info, overnight, daytime, comorbidities)
        reduce_options.append({
            "class": cls,
            "drug": drug_id,
            "clinical_fit": 1.0,
            "coverage": 1.0,
            "medication": med,
            "dose": dose,
        })

    maintain_options = _build_maintain_options(patient, config, reduce_classes)

    a1c = float(patient.get("a1c") or 0)
    goal = float(patient.get("goal") or 7.5)
    loc = "overnight and/or daytime" if (overnight and daytime) else ("overnight" if overnight else "daytime")
    if a1c > 0 and a1c > goal:
        assessment_suffix = f" A1C {a1c}% above goal with {loc} lows detected. Recommend dose reduction per de-escalation guidelines first; consider add-on therapy after sulfonylurea reduction."
    else:
        assessment_suffix = f" A1C {a1c}% at goal with {loc} lows detected. Recommend dose reduction per de-escalation guidelines."

    return reduce_options, maintain_options, assessment_suffix
