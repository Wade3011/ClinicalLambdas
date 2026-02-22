"""
Goal 2: Dosing (eGFR-adjusted starting dose + dose increase when already on therapy).
parse_dose, calculate_next_dose, get_recommended_dose. Uses dosing_config.json when provided.
"""
import re

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
        is_sa = " sa" in drug_name_lower or "glumetza" in drug_name_lower or "metformin sa" in dose_str_lower
        for step in steps:
            if step > current_daily:
                if step == 2000 and max_daily < 2000:
                    return f"Max {max_daily} mg daily (eGFR 30-45)", True
                if step == 1500:
                    return "1500 mg daily" if is_sa else "1000 mg morning + 500 mg evening (IR)", False
                if step == 2000:
                    return "2000 mg daily" if is_sa else "1000 mg BID (IR)", False
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
        # Dapagliflozin (Farxiga): eGFR >=25; dose increments 5, 10 mg daily (source table)
        if "dapagliflozin" in drug_name_lower or "farxiga" in drug_name_lower or "dapagliflozin" in dose_str_lower or "farxiga" in dose_str_lower:
            if current_value < 5:
                return "5 mg daily", False
            if current_value < 10:
                return "10 mg daily", False
            return "At max dose (10 mg daily)", True
        # Empagliflozin (Jardiance): eGFR >=20; dose increments 10, 25 mg daily
        if "empagliflozin" in drug_name_lower or "jardiance" in drug_name_lower or "empagliflozin" in dose_str_lower or "jardiance" in dose_str_lower:
            if current_value < 25:
                return "25 mg daily", False
            return "At max dose (25 mg daily)", True
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
        # Glipizide (Glucotrol): max 20 mg daily; dose increments 5, 10, 20 mg (source table)
        if "glipizide" in drug_name_lower or "glucotrol" in drug_name_lower or "glipizide" in dose_str_lower or "glucotrol" in dose_str_lower:
            current_daily = current_value * 2 if effective_bid else current_value
            for step in [5.0, 10.0, 20.0]:
                if step > current_daily:
                    return f"{int(step)} mg daily (consider BID dosing if >5 mg)" if step > 5 else f"{int(step)} mg daily", False
            return "At max dose (20 mg daily)", True
        # Glimepiride (Amaryl): max 8 mg daily; dose increments 1, 2, 4, 8 mg (source table)
        if "glimepiride" in drug_name_lower or "amaryl" in drug_name_lower or "glimepiride" in dose_str_lower or "amaryl" in dose_str_lower:
            current_daily = current_value * 2 if effective_bid else current_value
            for step in [1.0, 2.0, 4.0, 8.0]:
                if step > current_daily:
                    return "8 mg daily (consider 4 mg BID)" if step == 8 else f"{int(step)} mg daily", False
            return "At max dose (8 mg daily or 4 mg BID)", True
        # Glyburide (Diabeta): max 10 mg daily; dose increments 1.25, 2.5, 5, 10 mg (source table)
        if "glyburide" in drug_name_lower or "diabeta" in drug_name_lower or "glyburide" in dose_str_lower or "diabeta" in dose_str_lower:
            current_daily = current_value * 2 if effective_bid else current_value
            for step in [1.25, 2.5, 5.0, 10.0]:
                if step > current_daily:
                    return f"{step} mg daily (consider BID if >5 mg)" if step > 5 else f"{step} mg daily", False
            return "At max dose (10 mg daily)", True
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
    For SGLT2, enforces drug-level min_eGFR (Dapagliflozin 25, Canagliflozin 30)."""
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
        # preferred_drug not in config: do not substitute class-level drug/dose
        return preferred_drug, "Consult dosing guidelines"

    # Class-level config
    dose = _dose_from_cfg(class_cfg, egfr)
    if dose is not None:
        return default.get("medication"), dose
    return default.get("medication"), class_cfg.get("default", default.get("dose"))


def get_insulin_tdd_units(dose_str, frequency, class_name):
    """Return total daily dose in units for basal/bolus insulin, or None if not parseable.
    Handles 'Other: N units' and 'N units' dose strings; uses frequency for BID/TID."""
    if not dose_str or class_name not in ("Basal Insulin", "Bolus Insulin"):
        return None
    value, unit, freq = parse_dose(dose_str)
    # Fallback: "Other: N units" or "N units" when parse_dose missed unit (e.g. odd spacing)
    u = (unit or "").lower()
    if value is None or u not in ("unit", "units"):
        other_match = re.search(r"(?:other\s*:\s*)?(\d+\.?\d*)\s*units?", (dose_str or ""), re.IGNORECASE)
        if other_match:
            value = float(other_match.group(1))
            unit = "units"
            u = "units"
    if value is None:
        return None
    if u not in ("unit", "units"):
        return None
    effective_bid = _is_bid(freq) or _is_bid(frequency)
    dose_str_lower = (dose_str or "").lower()
    freq_lower = (freq or frequency or "").lower()
    if "meal" in freq_lower or "meal" in dose_str_lower or "tid" in freq_lower or "3x" in freq_lower:
        return float(value) * 3  # assume 3 meals
    if effective_bid:
        return float(value) * 2
    return float(value)


def _streamline_basal_rules(full_rules, tdd):
    """Return only the applicable TDD row plus closing sentences (If TDD >50; Unexplained lows)."""
    if not full_rules or not isinstance(full_rules, str):
        return full_rules
    # Normalize: ensure period+space between sentences (some loaders may collapse or use different whitespace)
    normalized = re.sub(r"\.\s*", ". ", full_rules)
    parts = [p.strip() for p in re.split(r"\.\s+", normalized) if p.strip()]
    # If still one segment, try split by period (with optional space) before TDD/If TDD/Unexplained
    if len(parts) == 1 and "TDD 41+" in parts[0]:
        parts = [p.strip() for p in re.split(r"\.\s+(?=TDD |If TDD |Unexplained )", parts[0]) if p.strip()]
        if len(parts) == 1:
            parts = [p.strip() for p in re.split(r"\.\s*(?=TDD |If TDD |Unexplained )", parts[0]) if p.strip()]
    closing, rows = [], []
    for p in parts:
        if p.startswith("If TDD >50") or p.startswith("Unexplained fasting lows"):
            closing.append(p)
        else:
            rows.append(p)
    closing_str = ". ".join(closing) + "." if closing else ""
    # Identify row by TDD: "TDD <20", "TDD 21–40" or "TDD 21-40", "TDD 41+"
    if tdd is not None and tdd > 40:
        row = next((r for r in rows if r.startswith("TDD 41+")), rows[-1] if rows else "")
    elif tdd is not None and tdd > 20:
        row = next((r for r in rows if "TDD 21" in r and "40" in r), rows[1] if len(rows) > 1 else (rows[0] if rows else ""))
    else:
        row = next((r for r in rows if "TDD <20" in r or "TDD &lt;20" in r), rows[0] if rows else "")
    if not row:
        return full_rules
    intro = f"For your total daily dose (TDD={int(tdd)} units): " if tdd is not None else "For TDD <20 units: "
    return intro + row + (". " + closing_str if closing_str else "")


def _streamline_bolus_rules(full_rules, tdd):
    """Return only the applicable TDD segment plus closing (Unexplained postprandial lows)."""
    if not full_rules or not isinstance(full_rules, str):
        return full_rules
    parts = [p.strip() for p in re.split(r"\.\s+", full_rules) if p.strip()]
    # If one long segment (e.g. no ". " between lines), split by next-sentence pattern
    if len(parts) == 1 and ("TDD ≥21" in parts[0] or "TDD >=21" in parts[0]):
        parts = [p.strip() for p in re.split(r"\.\s+(?=If |TDD |Unexplained )", parts[0]) if p.strip()]
    closing, lower_rows, high_row = [], [], ""
    for p in parts:
        if p.startswith("Unexplained postprandial lows"):
            closing.append(p)
        elif p.startswith("TDD ≥21") or p.startswith("TDD >=21"):
            high_row = p
        else:
            lower_rows.append(p)
    closing_str = ". ".join(closing) + "." if closing else ""
    if tdd is not None and tdd >= 21 and high_row:
        row = high_row
        intro = f"For your total daily dose (TDD={int(tdd)} units): "
    else:
        row = ". ".join(lower_rows) if lower_rows else ""
        intro = f"For your total daily dose (TDD={int(tdd)} units): " if tdd is not None else ""
    if not row:
        return full_rules
    return intro + row + (". " + closing_str if closing_str else "")


def _streamline_insulin_dose_rules(class_name, full_rules, tdd):
    """Show only the protocol row that applies to the patient's TDD, plus closing sentences."""
    if class_name == "Basal Insulin":
        return _streamline_basal_rules(full_rules, tdd)
    if class_name == "Bolus Insulin":
        return _streamline_bolus_rules(full_rules, tdd)
    return full_rules


def get_recommended_dose(class_name, eGFR, is_currently_on=False, current_medication_info=None, goal2_data=None, preferred_drug=None):
    """Return {medication, dose} for class and eGFR. Uses goal2 when provided; dose increase when on therapy.
    preferred_drug: optional drug name (e.g. 'Dapagliflozin', 'Saxagliptin') to use drug-level starting dose from goal2 by_drug."""
    if class_name == "No Change":
        return {"medication": "No medication change", "dose": "Continue current therapy"}
    egfr = float(eGFR) if eGFR is not None else 0.0
    # Use only config for drug/dose; no hardcoded fallback that substitutes a different drug or dose
    default_meds = (goal2_data or {}).get("default_medications") or {}
    default = default_meds.get(class_name, {"medication": class_name, "dose": "Consult dosing guidelines"})

    med_from_goal2, dose_from_goal2 = _starting_dose_from_goal2(class_name, egfr, goal2_data, preferred_drug=preferred_drug)
    if dose_from_goal2 is not None:
        med = med_from_goal2 or default["medication"]
        dose = dose_from_goal2
    else:
        med = default["medication"]
        dose = default["dose"]

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
                return {"medication": med, "dose": dose}
    return {"medication": med, "dose": dose}


def get_current_dose_from_input(dose_str, frequency):
    """Parse dose string and frequency into comparable daily or weekly amounts.
    Returns dict: daily_mg (float or None), weekly_mg (float or None).
    Only populates when unit is mg (or no unit) for consistency with max-dose comparison."""
    if not dose_str:
        return {"daily_mg": None, "weekly_mg": None}
    value, unit, freq = parse_dose(dose_str)
    if value is None:
        return {"daily_mg": None, "weekly_mg": None}
    unit = (unit or "").lower()
    if unit and unit not in ("mg", "g"):
        return {"daily_mg": None, "weekly_mg": None}
    if unit == "g":
        value = value * 1000.0
    effective_bid = _is_bid(freq) or _is_bid(frequency)
    dose_str_lower = (dose_str or "").lower()
    freq_lower = (freq or frequency or "").lower()
    if "weekly" in freq_lower or "weekly" in dose_str_lower:
        return {"daily_mg": None, "weekly_mg": float(value)}
    if effective_bid:
        return {"daily_mg": float(value) * 2, "weekly_mg": None}
    return {"daily_mg": float(value), "weekly_mg": None}


def _parse_max_from_dose_string(dose_str):
    """Extract max daily or weekly mg from a dose string from dosing_config (e.g. '100 mg daily', 'Max 1000 mg daily', '2 mg weekly').
    Returns dict: max_daily_mg (float or None), max_weekly_mg (float or None)."""
    if not dose_str or not isinstance(dose_str, str):
        return {"max_daily_mg": None, "max_weekly_mg": None}
    s = dose_str.strip().lower()
    out = {"max_daily_mg": None, "max_weekly_mg": None}
    # Prefer explicit "max N mg daily/weekly"
    max_daily_match = re.search(r"max\s+(\d+\.?\d*)\s*mg\s*daily", s)
    max_weekly_match = re.search(r"max\s+(\d+\.?\d*)\s*mg\s*weekly", s)
    if max_daily_match:
        out["max_daily_mg"] = float(max_daily_match.group(1))
    if max_weekly_match:
        out["max_weekly_mg"] = float(max_weekly_match.group(1))
    # Then any "N mg daily" or "N mg weekly" (take largest if multiple)
    for m in re.finditer(r"(\d+\.?\d*)\s*mg\s*(daily|weekly)", s):
        val = float(m.group(1))
        if m.group(2) == "daily":
            if out["max_daily_mg"] is None or val > out["max_daily_mg"]:
                out["max_daily_mg"] = val
        else:
            if out["max_weekly_mg"] is None or val > out["max_weekly_mg"]:
                out["max_weekly_mg"] = val
    return out


def _dose_string_for_drug_at_egfr(class_name, egfr, drug_id, goal2_data):
    """Get the dose string from dosing_config for this class/drug at this eGFR.
    Resolves by_drug by matching drug_id to config keys. Returns None if no dose in config."""
    if not goal2_data:
        return None
    by_class = goal2_data.get("starting_dose_by_class") or {}
    class_cfg = by_class.get(class_name)
    if not class_cfg:
        return None
    by_drug = class_cfg.get("by_drug") or {}
    drug_id_lower = (drug_id or "").lower()
    cfg = None
    if by_drug:
        # Prefer exact key match, then first word / substring match
        if drug_id in by_drug:
            cfg = by_drug[drug_id]
        else:
            for key in by_drug:
                if key.lower() == drug_id_lower or drug_id_lower in key.lower() or key.lower() in drug_id_lower:
                    cfg = by_drug[key]
                    break
    if cfg is None:
        cfg = class_cfg
    return _dose_from_cfg(cfg, egfr)


def get_max_dose_for_egfr(class_name, eGFR, drug_name=None, goal2_data=None):
    """Return max allowed dose for this drug/class at given eGFR.
    Uses dosing_config (goal2_data) when provided; otherwise returns None (no comparison).
    Returns dict: max_daily_mg (float or None), max_weekly_mg (float or None)."""
    egfr = float(eGFR) if eGFR is not None else 0.0
    out = {"max_daily_mg": None, "max_weekly_mg": None}
    # Resolve drug_id for config lookup (drug_name may be display name like "Empagliflozin (Jardiance)" or drug_id)
    drug_id = (drug_name or "").strip()
    if drug_id and "(" in drug_id:
        drug_id = drug_id.split("(")[0].strip()
    if not drug_id:
        drug_id = drug_name
    dose_str = _dose_string_for_drug_at_egfr(class_name, egfr, drug_id, goal2_data)
    if dose_str:
        out = _parse_max_from_dose_string(dose_str)
    return out


def current_dose_exceeds_max_for_egfr(class_name, current_dose_str, current_frequency, eGFR, drug_name=None, goal2_data=None):
    """True when the current dose (from input) is higher than the max dose allowed for this eGFR.
    Uses dosing_config (goal2_data) when provided. Returns (exceeds: bool, detail: str for assessment)."""
    current = get_current_dose_from_input(current_dose_str, current_frequency)
    max_d = get_max_dose_for_egfr(class_name, eGFR, drug_name, goal2_data)
    if current["daily_mg"] is not None and max_d["max_daily_mg"] is not None:
        if current["daily_mg"] > max_d["max_daily_mg"]:
            egfr_int = int(float(eGFR)) if eGFR is not None else 0
            return True, f"Current dose exceeds maximum recommended for eGFR {egfr_int}; clinician review recommended."
    if current["weekly_mg"] is not None and max_d["max_weekly_mg"] is not None:
        if current["weekly_mg"] > max_d["max_weekly_mg"]:
            egfr_int = int(float(eGFR)) if eGFR is not None else 0
            return True, f"Current dose exceeds maximum recommended for eGFR {egfr_int}; clinician review recommended."
    return False, ""
