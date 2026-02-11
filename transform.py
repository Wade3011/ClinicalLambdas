"""
Transform raw API request to patient dict and normalize glucose readings.
Uses drug_classes for allergy mapping; goal2_data for form_value -> class mapping and drug name -> drug_id.
"""
import re


def build_drug_name_to_id(goal2_data, config):
    """
    Build mapping from display names / brand names to config drug_id (e.g. 'Jardiance' -> 'Empagliflozin').
    Used so current_drug_ids reflects the specific drug the patient is on.
    Returns (name_to_drug_id dict, class_to_default_drug_id dict).
    """
    name_to_drug_id = {}
    class_to_default_drug_id = {}
    if not config or not isinstance(config, dict):
        return name_to_drug_id, class_to_default_drug_id
    drugs = config.get("drugs", {})
    if not drugs:
        return name_to_drug_id, class_to_default_drug_id
    # drugs by class for default lookups
    drugs_by_class = {}
    for drug_id, data in drugs.items():
        cls = data.get("class", drug_id)
        drugs_by_class.setdefault(cls, []).append(drug_id)
    # From goal2 by_drug: medication string and brand (in parens) -> drug_id
    by_class = (goal2_data or {}).get("starting_dose_by_class") or {}
    default_meds = (goal2_data or {}).get("default_medications") or {}
    for class_name, class_cfg in by_class.items():
        if not isinstance(class_cfg, dict):
            continue
        by_drug = class_cfg.get("by_drug") or {}
        for drug_id, drug_cfg in by_drug.items():
            if drug_id not in drugs:
                continue
            med_str = (drug_cfg or {}).get("medication") or ""
            if med_str:
                name_to_drug_id[med_str.strip()] = drug_id
                # first word (e.g. "Empagliflozin")
                first = med_str.split()[0] if med_str.split() else ""
                if first:
                    name_to_drug_id[first] = drug_id
                # brand in parens (e.g. "Jardiance")
                import re as _re
                paren = _re.search(r"\(([^)]+)\)", med_str)
                if paren:
                    name_to_drug_id[paren.group(1).strip()] = drug_id
            name_to_drug_id[drug_id] = drug_id
    # Default medication -> drug_id for classes without by_drug (Metformin, TZD, Basal, Bolus)
    for class_name, default_cfg in default_meds.items():
        if not isinstance(default_cfg, dict):
            continue
        med_str = (default_cfg.get("medication") or "").strip()
        candidates = drugs_by_class.get(class_name, [])
        if not candidates:
            continue
        # Single drug for this class (e.g. Metformin -> Metformin, TZD -> Pioglitazone)
        if len(candidates) == 1:
            drug_id = candidates[0]
            class_to_default_drug_id[class_name] = drug_id
            if med_str:
                name_to_drug_id[med_str] = drug_id
                first = med_str.split()[0] if med_str.split() else ""
                if first:
                    name_to_drug_id[first] = drug_id
                paren = re.search(r"\(([^)]+)\)", med_str)
                if paren:
                    name_to_drug_id[paren.group(1).strip()] = drug_id
        else:
            # Multiple drugs (e.g. SGLT2): default medication already in name_to_drug_id from by_drug
            class_to_default_drug_id[class_name] = name_to_drug_id.get(med_str, candidates[0] if candidates else None)
    return name_to_drug_id, class_to_default_drug_id


def form_value_to_class_mapping(goal2_data):
    """Build form_value -> class name from goal2 form_value_by_class. Returns None if invalid."""
    if not goal2_data or not isinstance(goal2_data.get("form_value_by_class"), dict):
        return None
    return {v: k for k, v in goal2_data["form_value_by_class"].items()}


DRUG_CLASS_MAPPING_FALLBACK = {
    "biguanides": "Metformin",
    "sglt2": "SGLT2",
    "glp1_gip": "GLP1",
    "tzd": "TZD",
    "dppiv": "DPP4",
    "sulfonylureas": "Sulfonylurea",
    "basal_insulin": "Basal Insulin",
    "bolus_insulin": "Bolus Insulin",
    "pramlintide": "Pramlintide",
}

COMORBIDITY_MAPPING = {
    "Heart Failure (CHF)": "Heart Failure (CHF)",
    "ASCVD": "ASCVD",
    "CKD": "CKD",
    "Obesity (BMI > 40)": "Obesity (BMI > 40)",
    "Dialysis": "Dialysis",
    "DKA History": "DKA History",
    "Pregnant": "Pregnant",
    "Type 1 Diabetes": "Type 1 Diabetes",
    "Severe Genital Infections": "Severe Genital Infections",
    "MEN2/MTC or Family History": ["MEN2/MTC or Family History", "MEN2", "MTC"],
    "Gastroparesis": "Gastroparesis",
    "Acute Pancreatitis": "Acute Pancreatitis",
    "History of Pancreatitis": "History of Pancreatitis",
    "Active Bladder Cancer": "Active Bladder Cancer",
    "Frequent Hypoglycemia": "Frequent Hypoglycemia",
    "History of Hypoglycemia": "History of Hypoglycemia",
    "Elderly with High Fall Risk": "Elderly with High Fall Risk",
    "Mild GI Issues": "Mild GI Issues",
    "Moderate to severe GI issues": "Moderate to severe GI issues",
    "Hypersensitivity": "HYPERSENSITIVITY",
    "Other": None,
}


def _normalize_request(request_data):
    """Accept frontend payload: prefer camelCase, fall back to snake_case so Lambda works with either."""
    if not request_data or not isinstance(request_data, dict):
        return request_data or {}
    out = dict(request_data)
    # Top-level: camelCase (frontend) vs snake_case
    if "patient_info" in out and "patientInfo" not in out:
        out["patientInfo"] = out["patient_info"]
    if "current_medications" in out and "currentMedications" not in out:
        out["currentMedications"] = out["current_medications"]
    if "glucose_readings" in out and "glucoseReadings" not in out:
        out["glucoseReadings"] = out["glucose_readings"]
    if "additional_context" in out and "additionalContext" not in out:
        out["additionalContext"] = out["additional_context"]
    if "preferred_drug_by_class" in out and "preferredDrugByClass" not in out:
        out["preferredDrugByClass"] = out["preferred_drug_by_class"]
    if "allergies_raw" in out and "allergies" not in out:
        out["allergies"] = out.get("allergies_raw", [])
    return out


def transform_request_to_patient(request_data, drug_classes=None, goal2_data=None):
    """
    Transform request to patient dict used by calculations.
    goal2_data used for form_value -> class mapping when provided.
    Accepts frontend payload: patientInfo, currentMedications, comorbidities, glucoseReadings, allergies (optional).
    """
    request_data = _normalize_request(request_data)
    patient = {}
    patient_info = request_data.get("patientInfo", {})

    a1c_goal_str = patient_info.get("a1cGoal", "")
    if a1c_goal_str:
        goal_match = re.search(r"([\d.]+)", a1c_goal_str)
        patient["goal"] = float(goal_match.group(1)) if goal_match else 7.5
    else:
        patient["goal"] = 7.5

    patient["age"] = patient_info.get("age") or 0
    patient["eGFR"] = patient_info.get("eGFR") or 0.0
    last_a1c_str = str(patient_info.get("lastA1c") or "0").strip().rstrip("%").strip()
    patient["a1c"] = float(last_a1c_str) if last_a1c_str else 0.0

    insurance_plan = (patient_info.get("insurancePlan") or "").lower()
    if "va" in insurance_plan or "veteran" in insurance_plan:
        patient["insurance"] = "VA"
    elif "medicare" in insurance_plan:
        patient["insurance"] = "Medicare"
    elif "medicaid" in insurance_plan:
        patient["insurance"] = "Medicaid"
    elif "no insurance" in insurance_plan or "uninsured" in insurance_plan:
        patient["insurance"] = "No Insurance"
    else:
        patient["insurance"] = "Private"

    monitor = (patient_info.get("monitoringMethod") or "").lower()
    patient["monitor"] = "CGM" if "cgm" in monitor else "fingerstick"

    mapping = form_value_to_class_mapping(goal2_data) if goal2_data else None
    mapping = mapping or DRUG_CLASS_MAPPING_FALLBACK
    name_to_drug_id, class_to_default_drug_id = build_drug_name_to_id(goal2_data, drug_classes)
    current_meds = request_data.get("currentMedications", [])
    patient["current_classes"] = []
    patient["current_drugs"] = {}
    patient["current_medication_info"] = {}
    current_drug_ids_set = set()

    for med in current_meds:
        form_class = med.get("drugClass", "").lower()
        drug_class = mapping.get(form_class)
        if drug_class:
            patient["current_classes"].append(drug_class)
            drug_name = (med.get("drugName") or "").strip()
            dose = med.get("dose", "")
            frequency = med.get("frequency", "")
            if drug_name:
                patient["current_drugs"][drug_class] = f"{drug_name} {dose} {frequency}"
            # Resolve to config drug_id (drug-level: current_medication_info and current_drug_ids by drug_id)
            drug_id = None
            if drug_name:
                drug_id = name_to_drug_id.get(drug_name) or name_to_drug_id.get(drug_name.split()[0] if drug_name else "")
                if not drug_id and "(" in drug_name:
                    brand = re.search(r"\(([^)]+)\)", drug_name)
                    if brand:
                        drug_id = name_to_drug_id.get(brand.group(1).strip())
            if not drug_id:
                drug_id = class_to_default_drug_id.get(drug_class)
            if not drug_id and drug_class:
                drugs_cfg = (drug_classes or {}).get("drugs", {}) if isinstance(drug_classes, dict) else {}
                for did, d in drugs_cfg.items():
                    if isinstance(d, dict) and d.get("class") == drug_class:
                        drug_id = did
                        break
            if drug_id:
                current_drug_ids_set.add(drug_id)
                patient["current_medication_info"][drug_id] = {
                    "drugName": drug_name or "",
                    "dose": dose,
                    "frequency": frequency,
                }
    patient["current_drug_ids"] = current_drug_ids_set

    comorbidities = request_data.get("comorbidities", [])
    normalized_comorbidities = set()
    for com in comorbidities:
        if com in COMORBIDITY_MAPPING:
            mapped = COMORBIDITY_MAPPING[com]
            if mapped is None:
                continue
            if isinstance(mapped, list):
                normalized_comorbidities.update(mapped)
            else:
                normalized_comorbidities.add(mapped)
        elif com and com.startswith("Other:"):
            # Handle "Other: custom text" format - extract and preserve custom comorbidity
            custom_text = com.split(":", 1)[1].strip()
            if custom_text:
                normalized_comorbidities.add(custom_text.upper())
        elif com:
            normalized_comorbidities.add(com.upper())
    patient["comorbidities"] = normalized_comorbidities
    if not patient.get("current_drug_ids"):
        patient["comorbidities"].add("No active diabetes therapy")
    patient["bmi"] = 0.0

    # Drug-level allergy: map labels -> drug_ids (from each drug's allergy_labels).
    # Accepts list of strings (legacy) or objects { allergen, specificDrugs?, openToTrial? }.
    # Whole class: no specificDrugs, or specificDrugs ["All"], or openToTrial is False (not open to trialing → deny entire class).
    # Granular: specificDrugs = [specific labels] and openToTrial is True → deny only those drugs; rest of class still scored.
    drugs_for_allergy = drug_classes.get("drugs", {}) if isinstance(drug_classes, dict) else {}
    label_to_drug_ids = {}
    for drug_id, cfg in drugs_for_allergy.items():
        if isinstance(cfg, dict):
            for label in cfg.get("allergy_labels", []):
                label_to_drug_ids.setdefault(label, set()).add(drug_id)
    allergy_drug_ids = set()
    allergy_labels_used = set()
    raw_allergies = request_data.get("allergies", [])
    unknown_allergies = []
    for raw in raw_allergies:
        if isinstance(raw, dict):
            allergen = (raw.get("allergen") or "").strip()
            specific_drugs = raw.get("specificDrugs") or []
            if not allergen:
                continue
            if allergen.startswith("Other:"):
                custom_text = allergen.split(":", 1)[1].strip()
                if custom_text:
                    unknown_allergies.append(custom_text)
                continue
            # Not open to trialing → deny entire class; else whole-class only if no specific drugs or "All"
            is_whole_class = (
                raw.get("openToTrial") is False
                or not specific_drugs
                or (len(specific_drugs) == 1 and (specific_drugs[0] or "").strip() == "All")
            )
            if is_whole_class:
                if allergen in label_to_drug_ids:
                    allergy_drug_ids.update(label_to_drug_ids[allergen])
                    allergy_labels_used.add(allergen)
            else:
                for spec in specific_drugs:
                    spec = (spec or "").strip()
                    if spec and spec in label_to_drug_ids:
                        allergy_drug_ids.update(label_to_drug_ids[spec])
                        allergy_labels_used.add(spec)
        else:
            s = (raw or "").strip()
            if s and s.startswith("Other:"):
                custom_text = s.split(":", 1)[1].strip()
                if custom_text:
                    unknown_allergies.append(custom_text)
            elif s and s in label_to_drug_ids:
                allergy_drug_ids.update(label_to_drug_ids[s])
                allergy_labels_used.add(s)
    patient["allergy_drug_ids"] = allergy_drug_ids
    patient["unknown_allergies"] = unknown_allergies
    patient["allergies"] = {drugs_for_allergy.get(did, {}).get("class") for did in allergy_drug_ids if did in drugs_for_allergy}
    patient["allergies_raw"] = raw_allergies
    patient["allergy_labels_set"] = set((lab or "").lower() for lab in allergy_labels_used if (lab or "").strip())
    return patient


def normalize_glucose_readings(request_data):
    """Normalize glucoseReadings to fasting_avg, post_pp_avg; optional cgm_*."""
    out = {"fasting_avg": None, "post_pp_avg": None}
    gr = request_data.get("glucoseReadings") or {}
    monitor = ((request_data.get("patientInfo") or {}).get("monitoringMethod") or "").lower()

    if "cgm" in monitor or "continuous" in monitor:
        # CGM data: nested under cgmData, or flat at glucoseReadings root
        cgm = gr.get("cgmData") or gr
        if cgm.get("wakeUpAverage") is not None or cgm.get("bedtimeAverage") is not None or cgm.get("gmi") is not None:
            out["gmi"] = cgm.get("gmi")
            out["time_in_range"] = cgm.get("timeInRange")
            out["wake_up_average"] = cgm.get("wakeUpAverage")
            out["bedtime_average"] = cgm.get("bedtimeAverage")
            out["lows_detected"] = cgm.get("lowsDetected")
            out["lows_overnight"] = cgm.get("lowsOvernight")
            out["lows_after_meals"] = cgm.get("lowsAfterMeals")
            out["fasting_avg"] = cgm.get("wakeUpAverage")
            out["post_pp_avg"] = cgm.get("bedtimeAverage")
            return out

    if "fingerPokeData" in gr:
        fp = gr.get("fingerPokeData") or {}
        out["fasting_avg"] = fp.get("fastingAverage")
        out["post_pp_avg"] = fp.get("postPrandialAverage")
        # Fingerstick: lows_detected from request if present; else fallback to comorbidities in deescalation/scoring
        if fp.get("lowsDetected") is not None:
            out["lows_detected"] = fp.get("lowsDetected")
        return out

    # Legacy path: frontend sends fasting/postPrandial (camelCase); accept post_prandial if present
    fasting = gr.get("fasting") or {}
    post_pp = gr.get("postPrandial") or gr.get("post_prandial") or {}
    out["fasting_avg"] = fasting.get("average")
    out["post_pp_avg"] = post_pp.get("average")
    # If average not sent, compute from values (Goal 3 uses: post_prandial_average - potency)
    if out["fasting_avg"] is None and fasting.get("values"):
        nums = [float(x) for x in fasting["values"] if x is not None and str(x).strip() != ""]
        if nums:
            out["fasting_avg"] = round(sum(nums) / len(nums), 1)
    if out["post_pp_avg"] is None and post_pp.get("values"):
        nums = [float(x) for x in post_pp["values"] if x is not None and str(x).strip() != ""]
        if nums:
            out["post_pp_avg"] = round(sum(nums) / len(nums), 1)
    return out
