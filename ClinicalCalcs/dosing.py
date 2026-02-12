"""
Goal 2: Dosing (eGFR-adjusted starting dose + dose increase when already on therapy).
parse_dose, calculate_next_dose, get_recommended_dose. Uses goal2.json when provided.
"""
import re

DEFAULT_MEDICATIONS_FALLBACK = {
    "Metformin": {"medication": "Metformin IR", "dose": "1000mg BID"},
    "SGLT2": {"medication": "Dapagliflozin (Farxiga)", "dose": "10 mg daily"},
    "GLP1": {"medication": "Semaglutide (Ozempic)", "dose": "0.5mg weekly, titrate to 1mg"},
    "TZD": {"medication": "Pioglitazone (Actos)", "dose": "30mg daily"},
    "DPP4": {"medication": "Sitagliptin (Januvia)", "dose": "100mg daily"},
    "Sulfonylurea": {"medication": "Glipizide (Glucotrol)", "dose": "5mg daily"},
    "Basal Insulin": {"medication": "Glargine (Lantus)", "dose": "10 units at bedtime"},
    "Bolus Insulin": {"medication": "Lispro (Humalog)", "dose": "4-6 units with meals"},
    "Pramlintide": {"medication": "Pramlintide (Symlin)", "dose": "15mcg before meals"},
}


def parse_dose(dose_str):
    """Parse dose string to (numeric_value, unit, frequency) or (None, None, None)."""
    if not dose_str:
        return None, None, None
    num_match = re.search(r"(\d+\.?\d*)", dose_str)
    if not num_match:
        return None, None, None
    numeric_value = float(num_match.group(1))
    unit_match = re.search(r"(mg|mcg|units?|g)", dose_str, re.IGNORECASE)
    unit = unit_match.group(1).lower() if unit_match else None
    freq_match = re.search(r"(daily|BID|twice daily|weekly|monthly)", dose_str, re.IGNORECASE)
    frequency = freq_match.group(1).lower() if freq_match else None
    return numeric_value, unit, frequency


def _is_bid(freq_str):
    """True if frequency means twice daily (BID)."""
    if not freq_str:
        return False
    s = (freq_str or "").strip().lower()
    return s in ("bid", "twice daily", "twice a day", "2x daily", "2x/day")


def calculate_next_dose(class_name, current_dose_str, current_frequency, eGFR, drug_name=None):
    """Calculate next dose step. Returns (next_dose_str, is_at_max) or (None, False)."""
    if not current_dose_str:
        return None, False
    current_value, unit, freq = parse_dose(current_dose_str)
    if current_value is None:
        return None, False
    # Use frequency from dose string if present, else from caller (e.g. UI "twice daily")
    effective_bid = _is_bid(freq) or _is_bid(current_frequency)
    egfr = float(eGFR) if eGFR is not None else 0.0
    drug_name_lower = (drug_name or "").lower()
    dose_str_lower = current_dose_str.lower()

    if class_name == "Metformin":
        max_daily = 1000 if 30 <= egfr < 45 else 2000
        steps = [s for s in [500, 1000, 1500, 2000] if s <= max_daily]
        current_daily = current_value * 2 if effective_bid else current_value
        for step in steps:
            if step > current_daily:
                if step == 2000 and max_daily < 2000:
                    return f"Max {max_daily} mg daily (eGFR 30-45)", True
                if step == 1500:
                    return "1500 mg daily (SA) or 1000 mg morning + 500 mg evening (IR)", False
                if step == 2000:
                    return "2000 mg daily (SA) or 1000 mg BID (IR)", False
                return f"{step} mg daily", False
        return f"At max dose ({max_daily} mg daily)", True

    if class_name == "SGLT2":
        if "canagliflozin" in drug_name_lower or "invokana" in drug_name_lower or "canagliflozin" in dose_str_lower or "invokana" in dose_str_lower:
            # eGFR 30-59: max 100 mg daily; eGFR >= 60: max 300 mg daily (per Dosing Based on Kidney Function)
            if 30 <= egfr < 60:
                if current_value < 100:
                    return "100 mg daily (eGFR 30-59 max)", False
                return "At max dose (100 mg daily for eGFR 30-59)", True
            if egfr >= 60:
                if current_value < 100:
                    return "100 mg daily", False
                if current_value < 300:
                    return "300 mg daily (eGFR ≥60)", False
                return "At max dose (300 mg daily)", True
            return "At max dose", True
        # Dapagliflozin (Farxiga): 10 mg daily fixed; eGFR >=25 (Excel: kidney dosing)
        if "dapagliflozin" in drug_name_lower or "farxiga" in drug_name_lower or "dapagliflozin" in dose_str_lower or "farxiga" in dose_str_lower:
            return "At max dose (10 mg daily)", True
        # Ertugliflozin (Steglatro): 15 mg daily fixed; no kidney dose reduction (Excel)
        if "ertugliflozin" in drug_name_lower or "steglatro" in drug_name_lower or "ertugliflozin" in dose_str_lower or "steglatro" in dose_str_lower:
            return "At max dose (15 mg daily)", True
        # Bexagliflozin (Brenzavvy): 20 mg daily fixed; Excel deny if eGFR <30 (class deny 25 covers)
        if "bexagliflozin" in drug_name_lower or "brenzavvy" in drug_name_lower or "bexagliflozin" in dose_str_lower or "brenzavvy" in dose_str_lower:
            return "At max dose (20 mg daily)", True
        return "At max dose (fixed dose medication)", True

    if class_name == "DPP4":
        if "sitagliptin" in drug_name_lower or "januvia" in drug_name_lower or "sitagliptin" in dose_str_lower or "januvia" in dose_str_lower:
            max_dose = 100 if egfr >= 45 else (50 if 30 <= egfr < 45 else 25)
            if current_value < max_dose:
                return f"{max_dose} mg daily (eGFR {int(egfr)})", False
            return f"At max dose ({max_dose} mg daily for eGFR {int(egfr)})", True
        if "alogliptin" in drug_name_lower or "nesina" in drug_name_lower or "alogliptin" in dose_str_lower or "nesina" in dose_str_lower:
            max_dose = 25 if egfr >= 60 else (12.5 if egfr >= 30 else 6.25)
            if current_value < max_dose:
                return f"{max_dose} mg daily (eGFR {int(egfr)})", False
            return f"At max dose ({max_dose} mg daily for eGFR {int(egfr)})", True
        # Saxagliptin (Onglyza): eGFR ≥45 → 5 mg; eGFR <45 → 2.5 mg (ref: kidney dosing)
        if "saxagliptin" in drug_name_lower or "onglyza" in drug_name_lower or "saxagliptin" in dose_str_lower or "onglyza" in dose_str_lower:
            max_dose = 5.0 if egfr >= 45 else 2.5
            if current_value < max_dose:
                return f"{max_dose} mg daily (eGFR-based)", False
            return f"At max dose ({max_dose} mg daily for eGFR {int(egfr)})", True
        # Linagliptin (Tradjenta): 5 mg daily, no kidney dose adjustment (Excel: drugs not impacted)
        if "linagliptin" in drug_name_lower or "tradjenta" in drug_name_lower or "linagliptin" in dose_str_lower or "tradjenta" in dose_str_lower:
            return "At max dose (5 mg daily)", True
        return "At max dose", True

    if class_name == "GLP1":
        if "semaglutide" in drug_name_lower or "ozempic" in drug_name_lower or "semaglutide" in dose_str_lower or "ozempic" in dose_str_lower:
            if "rybelsus" in drug_name_lower or "rybelsus" in dose_str_lower or current_value >= 3:
                for step in [3.0, 7.0, 14.0]:
                    if step > current_value:
                        return f"{step} mg daily (Rybelsus; titrate after 30 days)", False
                return "At max dose (14 mg daily Rybelsus)", True
            for step in [0.25, 0.5, 1.0, 2.0]:
                if step > current_value:
                    return f"{step} mg weekly (titrate every 4 weeks)", False
            return "At max dose (2 mg weekly)", True
        if "dulaglutide" in drug_name_lower or "trulicity" in drug_name_lower or "dulaglutide" in dose_str_lower or "trulicity" in dose_str_lower:
            for step in [0.75, 1.5, 3.0, 4.5]:
                if step > current_value:
                    return f"{step} mg weekly (titrate every 4 weeks)", False
            return "At max dose (4.5 mg weekly)", True
        if "tirzepatide" in drug_name_lower or "mounjaro" in drug_name_lower or "tirzepatide" in dose_str_lower or "mounjaro" in dose_str_lower:
            for step in [2.5, 5.0, 7.5, 10.0, 12.5, 15.0]:
                if step > current_value:
                    return f"{step} mg weekly (titrate every 4 weeks)", False
            return "At max dose (15 mg weekly)", True
        if "exenatide" in drug_name_lower or "byetta" in drug_name_lower:
            if "bydureon" in drug_name_lower or "bydureon" in dose_str_lower or "er " in dose_str_lower or current_value == 2:
                return "At max dose (2 mg weekly)", True
            for step in [5.0, 10.0]:
                if step > current_value:
                    return f"{step} mcg BID (titrate every 4 weeks)", False
            return "At max dose (10 mcg BID)", True
        if "liraglutide" in drug_name_lower or "victoza" in drug_name_lower or "liraglutide" in dose_str_lower or "victoza" in dose_str_lower:
            for step in [0.6, 1.2, 1.8]:
                if step > current_value:
                    return f"{step} mg daily (titrate weekly)", False
            return "At max dose (1.8 mg daily)", True
        return "Consider dose increase per protocol", False

    if class_name == "Sulfonylurea":
        if "glipizide" in drug_name_lower or "glucotrol" in drug_name_lower or "glipizide" in dose_str_lower or "glucotrol" in dose_str_lower:
            # Max 20 mg daily; BID means per-dose * 2 for daily total
            current_daily = current_value * 2 if effective_bid else current_value
            for step in [2.5, 5.0, 10.0, 20.0]:
                if step > current_daily:
                    return f"{step} mg daily (consider BID dosing if >5 mg)" if step > 5 else f"{step} mg daily", False
            return "At max dose (20 mg daily)", True
        if "glimepiride" in drug_name_lower or "amaryl" in drug_name_lower or "glimepiride" in dose_str_lower or "amaryl" in dose_str_lower:
            current_daily = current_value * 2 if effective_bid else current_value
            for step in [1.0, 2.0, 4.0, 8.0]:
                if step > current_daily:
                    return "8 mg daily (consider 4 mg BID)" if step == 8 else f"{int(step)} mg daily", False
            return "At max dose (8 mg daily or 4 mg BID)", True
        if "glyburide" in drug_name_lower or "diabeta" in drug_name_lower or "glyburide" in dose_str_lower or "diabeta" in dose_str_lower:
            current_daily = current_value * 2 if effective_bid else current_value
            for step in [1.25, 2.5, 5.0, 10.0, 20.0]:
                if step > current_daily:
                    return f"{step} mg daily (consider BID if >5 mg)" if step > 5 else f"{step} mg daily", False
            return "At max dose (20 mg daily)", True
        return "Consider dose increase per protocol", False

    if class_name == "TZD":
        if "pioglitazone" in drug_name_lower or "actos" in drug_name_lower or "pioglitazone" in dose_str_lower or "actos" in dose_str_lower:
            for step in [15, 30, 45]:
                if step > current_value:
                    return f"{step} mg daily (titrate every 4-12 weeks)", False
            return "At max dose (45 mg daily)", True
        return "At max dose", True

    if class_name == "Basal Insulin":
        meals = freq and "meal" in (freq or "").lower() or (current_frequency and "meal" in (current_frequency or "").lower())
        current_daily = current_value * 2 if effective_bid else (current_value * 3 if meals else current_value)
        if current_daily <= 20:
            return "Increase by 2-4 units based on fasting glucose (max +10 units/day increase)", False
        return "Increase total daily dose by 10-20% based on fasting glucose (max +10 units/day increase)", False
    if class_name == "Bolus Insulin":
        meals = freq and "meal" in (freq or "").lower() or (current_frequency and "meal" in (current_frequency or "").lower())
        current_daily = current_value * 2 if effective_bid else (current_value * 3 if meals else current_value)
        if 10 <= current_daily <= 20:
            return "Divide dose with each meal; increase 1-2 units per meal (max 4 units per single increase)", False
        if current_daily > 20:
            return "Increase daily dose by 10-15% and divide by number of meals (max +10 units/day increase)", False
        return "Increase by 1-2 units based on post-prandial glucose (max +10 units/day increase)", False
    return None, False


def _dose_from_cfg(cfg, egfr):
    """Given a config dict with eGFR_* keys, return the dose string for this egfr (or None)."""
    if not cfg:
        return None
    if "eGFR_geq_45" in cfg and egfr >= 45:
        return cfg["eGFR_geq_45"]
    if "eGFR_30_44" in cfg and 30 <= egfr < 45:
        return cfg["eGFR_30_44"]
    if "eGFR_30_45" in cfg and 30 <= egfr < 45:
        return cfg["eGFR_30_45"]
    if "eGFR_geq_20" in cfg and egfr >= 20:
        return cfg["eGFR_geq_20"]
    if "eGFR_geq_25" in cfg and egfr >= 25:
        return cfg["eGFR_geq_25"]
    if "eGFR_gt_60" in cfg and egfr > 60:
        return cfg["eGFR_gt_60"]
    if "eGFR_30_60" in cfg and 30 <= egfr <= 60:
        return cfg["eGFR_30_60"]
    if "eGFR_lt_30" in cfg and 0 < egfr < 30:
        return cfg["eGFR_lt_30"]
    if "eGFR_gt_45" in cfg and egfr > 45:
        return cfg["eGFR_gt_45"]
    if "eGFR_lte_45" in cfg and 0 < egfr <= 45:
        return cfg["eGFR_lte_45"]
    if "eGFR_lt_45" in cfg and 0 < egfr < 45:
        return cfg["eGFR_lt_45"]
    if "eGFR_geq_60" in cfg and egfr >= 60:
        return cfg["eGFR_geq_60"]
    if "eGFR_30_59" in cfg and 30 <= egfr < 60:
        return cfg["eGFR_30_59"]
    if "eGFR_geq_30" in cfg and egfr >= 30:
        return cfg["eGFR_geq_30"]
    return cfg.get("default")


def _sglt2_drug_for_egfr(class_cfg, egfr, preferred_drug=None):
    """Return (drug_key, drug_cfg) for SGLT2 allowed at this eGFR per Excel; or (None, None). Uses min_eGFR and drug_order_by_min_eGFR."""
    by_drug = class_cfg.get("by_drug") or {}
    order = class_cfg.get("drug_order_by_min_eGFR") or list(by_drug.keys())
    # Resolve preferred_drug to a key in by_drug
    preferred_key = None
    if preferred_drug:
        preferred_lower = preferred_drug.lower()
        for key in by_drug:
            if key.lower() == preferred_lower or preferred_lower in key.lower():
                preferred_key = key
                break
    if preferred_key and preferred_key in by_drug:
        drug_cfg = by_drug[preferred_key]
        min_g = drug_cfg.get("min_eGFR")
        if min_g is not None and egfr < min_g:
            # Preferred drug not allowed; pick first allowed alternative
            for key in order:
                if key not in by_drug:
                    continue
                cfg = by_drug[key]
                if cfg.get("min_eGFR") is not None and egfr >= cfg["min_eGFR"]:
                    return key, cfg
            return preferred_key, drug_cfg  # return preferred with "not recommended" message
        return preferred_key, drug_cfg
    # No preferred: first drug in order that is allowed at this eGFR
    for key in order:
        if key not in by_drug:
            continue
        cfg = by_drug[key]
        min_g = cfg.get("min_eGFR")
        if min_g is None or egfr >= min_g:
            return key, cfg
    return None, None


def _starting_dose_from_goal2(class_name, egfr, goal2_data, preferred_drug=None):
    """Return (medication, dose) from goal2 starting_dose_by_class, optionally from by_drug[preferred_drug].
    For SGLT2, enforces Excel drug-level min_eGFR (Empagliflozin 20, Dapagliflozin 25, Canagliflozin/Bexagliflozin 30, Ertugliflozin 45)."""
    if not goal2_data:
        return None, None
    by_class = goal2_data.get("starting_dose_by_class") or {}
    default_meds = goal2_data.get("default_medications") or {}
    class_cfg = by_class.get(class_name)
    default = default_meds.get(class_name, {"medication": class_name, "dose": "Consult dosing guidelines"})
    if not class_cfg:
        return default.get("medication"), default.get("dose")

    by_drug = class_cfg.get("by_drug") or {}

    # SGLT2: Excel drug-level eGFR cutoffs – pick drug by min_eGFR and drug_order_by_min_eGFR
    if class_name == "SGLT2" and by_drug and class_cfg.get("drug_order_by_min_eGFR"):
        drug_key, drug_cfg = _sglt2_drug_for_egfr(class_cfg, egfr, preferred_drug)
        if drug_key is None:
            return default.get("medication"), "No SGLT2 recommended for this eGFR (all require higher kidney function)."
        min_g = drug_cfg.get("min_eGFR")
        if min_g is not None and egfr < min_g:
            # Preferred drug not allowed – suggest alternative
            alt_key, alt_cfg = _sglt2_drug_for_egfr(class_cfg, egfr, None)
            alt_med = alt_cfg.get("medication") if alt_cfg else None
            dose = _dose_from_cfg(alt_cfg, egfr) if alt_cfg else None
            msg = f"Not recommended (eGFR <{int(min_g)})."
            if alt_med and dose:
                msg += f" Consider {alt_med}: {dose}."
            else:
                msg += " No alternative SGLT2 suitable for this eGFR."
            return drug_cfg.get("medication") or drug_key, msg
        dose = _dose_from_cfg(drug_cfg, egfr)
        if dose is not None:
            return drug_cfg.get("medication") or default.get("medication"), dose
        return drug_cfg.get("medication") or default.get("medication"), drug_cfg.get("default", default.get("dose"))

    # Prefer drug-level config when preferred_drug is given and present in by_drug
    if preferred_drug:
        preferred_lower = preferred_drug.lower()
        for key in by_drug:
            if key.lower() == preferred_lower or preferred_lower in key.lower():
                drug_cfg = by_drug[key]
                dose = _dose_from_cfg(drug_cfg, egfr)
                if dose is not None:
                    med = drug_cfg.get("medication") or default.get("medication")
                    return med, dose
                med = drug_cfg.get("medication") or default.get("medication")
                return med, drug_cfg.get("default", default.get("dose"))

    # Class-level config
    dose = _dose_from_cfg(class_cfg, egfr)
    if dose is not None:
        return default.get("medication"), dose
    return default.get("medication"), class_cfg.get("default", default.get("dose"))


def get_recommended_dose(class_name, eGFR, is_currently_on=False, current_medication_info=None, goal2_data=None, preferred_drug=None):
    """Return {medication, dose} for class and eGFR. Uses goal2 when provided; dose increase when on therapy.
    preferred_drug: optional drug name (e.g. 'Dapagliflozin', 'Saxagliptin') to use drug-level starting dose from goal2 by_drug."""
    if class_name == "No Change":
        return {"medication": "No medication change", "dose": "Continue current therapy"}
    egfr = float(eGFR) if eGFR is not None else 0.0
    default_meds = (goal2_data or {}).get("default_medications") or DEFAULT_MEDICATIONS_FALLBACK
    default = default_meds.get(class_name, {"medication": class_name, "dose": "Consult dosing guidelines"})

    med_from_goal2, dose_from_goal2 = _starting_dose_from_goal2(class_name, egfr, goal2_data, preferred_drug=preferred_drug)
    if dose_from_goal2 is not None:
        med = med_from_goal2 or default["medication"]
        dose = dose_from_goal2
    else:
        if class_name == "Metformin":
            dose = "500 mg daily week 1, then 1000 mg daily; max 2000 mg daily (titrate per protocol)" if egfr >= 45 else ("Max 1000 mg daily (eGFR 30-45)" if egfr >= 30 else default["dose"])
            med = default["medication"]
        elif class_name == "SGLT2":
            med = "Empagliflozin (Jardiance)"
            dose = "25 mg daily" if egfr >= 20 else "No SGLT2 recommended for this eGFR (eGFR <20)."
        elif class_name == "DPP4":
            med = default["medication"]
            dose = "100 mg daily" if egfr >= 45 else ("50 mg daily (eGFR 30-44)" if 30 <= egfr < 45 else ("25 mg daily (eGFR <30)" if egfr > 0 else default["dose"]))
        elif class_name == "GLP1":
            med, dose = default["medication"], "0.25 mg weekly x4 weeks, then titrate to 0.5–1 mg weekly (max 2 mg weekly)"
        elif class_name == "Sulfonylurea":
            med, dose = default["medication"], "2.5 mg daily; titrate to 5–20 mg daily (BID if >5 mg)"
        elif class_name == "TZD":
            med, dose = default["medication"], "15 mg daily; titrate to 30–45 mg every 4–12 weeks (effect at 4–6 weeks)"
        elif class_name == "Basal Insulin":
            med, dose = default["medication"], "10 units at bedtime; adjust by 1–2 units based on fasting (max +10 units/day increase)"
        elif class_name == "Bolus Insulin":
            med, dose = default["medication"], "4 units 15–20 min before largest meal; titrate per 2-h post-prandial (max +10 units/day)"
        else:
            med, dose = default["medication"], default["dose"]

    has_med_info = current_medication_info and isinstance(current_medication_info, dict) and current_medication_info.get("dose")
    if is_currently_on and has_med_info:
        current_dose_str = current_medication_info.get("dose", "")
        current_frequency = current_medication_info.get("frequency", "")
        drug_name = current_medication_info.get("drugName", "")
        if current_dose_str:
            next_dose, is_at_max = calculate_next_dose(class_name, current_dose_str, current_frequency, egfr, drug_name)
            if next_dose:
                if drug_name:
                    med = drug_name
                dose = f"Currently on {current_dose_str} {current_frequency}. {next_dose}."
                if class_name in ("Basal Insulin", "Bolus Insulin") and goal2_data:
                    by_class = (goal2_data.get("starting_dose_by_class") or {}).get(class_name) or {}
                    rules = by_class.get("dose_adjustment_rules")
                    if rules:
                        dose = f"{dose} Dose adjustments (per protocol): {rules}"
                return {"medication": med, "dose": dose}
    # Append Excel basal/bolus dose-adjustment rules when present in goal2
    if class_name in ("Basal Insulin", "Bolus Insulin") and goal2_data:
        by_class = (goal2_data.get("starting_dose_by_class") or {}).get(class_name) or {}
        rules = by_class.get("dose_adjustment_rules")
        if rules and dose:
            dose = f"{dose} Dose adjustments (per protocol): {rules}"
    return {"medication": med, "dose": dose}
