"""
Goal 3: Potency check (can drug lower fasting/post-prandial to target?).
goal_bands, A1c estimation tables, potency, finger_poke_interpret, calculate_goal3_boost.
Uses glucose_targets.json when goal3_data provided. No non-S3 fallbacks.
"""
# Aligned with A1c Config CSV (A1c --> Fasting Avg Config). Typo fixes: 6.9 PP 11->151, 9.4 PP 23->223.
FASTING_ESTIMATION_TABLE = {
    6.5: 120.0, 6.6: 123.3, 6.7: 126.7, 6.8: 130.0, 6.9: 133.3,
    7.0: 136.7, 7.1: 140.0, 7.2: 143.3, 7.3: 146.7, 7.4: 150.0,
    7.5: 153.3, 7.6: 156.7, 7.7: 160.0, 7.8: 163.3, 7.9: 166.7,
    8.0: 170.0, 8.1: 173.3, 8.2: 176.7, 8.3: 180.0, 8.4: 183.3,
    8.5: 186.7, 8.6: 190.0, 8.7: 193.3, 8.8: 196.7, 8.9: 200.0,
    9.0: 203.3, 9.1: 206.7, 9.2: 210.0, 9.3: 213.3, 9.4: 216.7,
    9.5: 220.0, 9.6: 223.3, 9.7: 226.7, 9.8: 226.7, 9.9: 226.7,
    10.0: 226.7, 10.1: 226.7, 10.2: 226.7, 10.3: 226.7, 10.4: 226.7,
    10.5: 226.7, 10.6: 226.7, 10.7: 226.7, 10.8: 226.7, 10.9: 226.7,
    11.0: 226.7,
}
POST_PRANDIAL_ESTIMATION_TABLE = {
    6.5: 140.0, 6.6: 143.0, 6.7: 146.0, 6.8: 149.0, 6.9: 151.0,
    7.0: 154.0, 7.1: 157.0, 7.2: 160.0, 7.3: 163.0, 7.4: 166.0,
    7.5: 169.0, 7.6: 171.0, 7.7: 174.0, 7.8: 177.0, 7.9: 180.0,
    8.0: 183.0, 8.1: 186.0, 8.2: 189.0, 8.3: 192.0, 8.4: 194.0,
    8.5: 197.0, 8.6: 200.0, 8.7: 203.0, 8.8: 206.0, 8.9: 209.0,
    9.0: 215.0, 9.1: 215.0, 9.2: 218.0, 9.3: 220.0, 9.4: 223.0,
    9.5: 226.0, 9.6: 229.0, 9.7: 232.0, 9.8: 235.0, 9.9: 237.0,
    10.0: 240.0, 10.1: 243.0, 10.2: 246.0, 10.3: 249.0, 10.4: 252.0,
    10.5: 255.0, 10.6: 258.0, 10.7: 260.0, 10.8: 263.0, 10.9: 266.0,
    11.0: 269.0,
}
FASTING_LOWERING_POTENTIAL = {
    "Basal Insulin": 100, "Sulfonylurea": 70, "Metformin": 60, "SGLT2": 25,
    "GLP1": 15, "TZD": 15, "DPP4": 5, "Bolus Insulin": 0,
}
POST_PRANDIAL_LOWERING_POTENTIAL = {
    "Bolus Insulin": 100, "GLP1": 75, "TZD": 65, "Metformin": 60,
    "DPP4": 50, "Sulfonylurea": 40, "Basal Insulin": 40, "SGLT2": 10,
}


def _get_finger_poke_band(goal):
    """Return band key for patient goal: lt7, lt7_5, or lt8."""
    g = float(goal) if goal is not None else 7.0
    if g <= 7.0:
        return "lt7"
    if g <= 7.5:
        return "lt7_5"
    return "lt8"


def goal3_bands(goal3_data):
    """Return goal_bands from goal3_data. None when missing (no fallback)."""
    return (goal3_data or {}).get("goal_bands")


def finger_poke_interpret(goal, fasting_avg, post_pp_avg, goal3_data=None):
    """Interpret fasting/post-prandial per Finger Poke Rules. Returns {fasting, post_prandial, band}."""
    band_key = _get_finger_poke_band(goal)
    out = {"band": band_key}
    bands_all = goal3_bands(goal3_data)
    if bands_all is None:
        return {**out, "fasting": None, "post_prandial": None}
    bands = bands_all.get(band_key)
    if bands is None:
        return {**out, "fasting": None, "post_prandial": None}

    def _interpret(value, band):
        if value is None:
            return None
        v = float(value)
        if v < band["reduce_below"]:
            return "reduce"
        if band["ok_min"] <= v <= band["ok_max"]:
            return "no_change"
        if v >= band["increase_at"]:
            return "increase"
        return "no_change"

    out["fasting"] = _interpret(fasting_avg, bands["fasting"]) if fasting_avg is not None and bands.get("fasting") else None
    out["post_prandial"] = _interpret(post_pp_avg, bands["post_prandial"]) if post_pp_avg is not None and bands.get("post_prandial") else None
    return out


def estimate_fasting_from_a1c(a1c, goal3_data=None):
    """Estimate fasting glucose (mg/dl) from A1c. Uses goal3 a1c_to_fasting when provided. Aligned with A1c Config CSV."""
    if a1c is None or a1c <= 0:
        return None
    a1c_rounded = round(float(a1c) * 10) / 10
    table = (goal3_data or {}).get("a1c_to_fasting")
    if table is not None:
        val = table.get(str(a1c_rounded))
        if val is not None:
            return float(val)
        if a1c_rounded <= 6.5:
            return 120.0
        max_key = max(float(k) for k in table.keys())
        return float(table.get(str(max_key), 226.7))
    if a1c_rounded <= 6.5:
        return 120.0
    if a1c_rounded >= 9.7:
        return 226.7
    return FASTING_ESTIMATION_TABLE.get(a1c_rounded)


def estimate_post_prandial_from_a1c(a1c, goal3_data=None):
    """Estimate post-prandial glucose (mg/dl) from A1c. Uses goal3 a1c_to_post_prandial when provided. Aligned with A1c Config CSV."""
    if a1c is None or a1c <= 0:
        return None
    a1c_rounded = round(float(a1c) * 10) / 10
    table = (goal3_data or {}).get("a1c_to_post_prandial")
    if table is not None:
        val = table.get(str(a1c_rounded))
        if val is not None:
            return float(val)
        if a1c_rounded <= 6.5:
            return 140.0
        max_key = max(float(k) for k in table.keys())
        return float(table.get(str(max_key), 269.0))
    if a1c_rounded <= 6.5:
        return 140.0
    val = POST_PRANDIAL_ESTIMATION_TABLE.get(a1c_rounded)
    return val if val is not None else 269.0


def get_target_fasting(goal, goal3_data=None):
    """Target fasting (mg/dl) for A1c goal. Uses goal3 goal_bands when provided. None when missing."""
    bands = goal3_bands(goal3_data)
    if bands is None:
        return None
    band_key = _get_finger_poke_band(goal)
    b = bands.get(band_key, {}).get("fasting", {})
    return b.get("ok_max")


def get_target_post_prandial(goal, goal3_data=None):
    """Target post-prandial (mg/dl) for A1c goal. Uses goal3 goal_bands when provided. None when missing."""
    bands = goal3_bands(goal3_data)
    if bands is None:
        return None
    band_key = _get_finger_poke_band(goal)
    b = bands.get(band_key, {}).get("post_prandial", {})
    return b.get("ok_max")


def _potency_for_drug(drug_id, drug_class, goal3_data, on_therapy=False):
    """Get potency (fasting, post_prandial) for a drug from goal3 only. No by_class fallback."""
    g = goal3_data or {}
    if on_therapy:
        by_drug = g.get("potency_on_therapy_by_drug") or {}
        p = by_drug.get(drug_id)
    else:
        by_drug = g.get("potency_by_drug") or {}
        p = by_drug.get(drug_id)
    return p if isinstance(p, dict) else {}


def calculate_goal3_boost(drug_id, drug_class, patient, normalized_glucose, goal3_data=None):
    """Goal 3: Per-axis boost per Goal 3 Rules Start CSV. Drug-level potency lookup."""
    goal = patient.get("goal", 7.5)
    a1c = patient.get("a1c", 0)
    is_currently_on = drug_id in patient.get("current_drug_ids", set())

    fasting_current = normalized_glucose.get("fasting_avg") or estimate_fasting_from_a1c(a1c, goal3_data)
    post_pp_current = normalized_glucose.get("post_pp_avg") or estimate_post_prandial_from_a1c(a1c, goal3_data)
    if fasting_current is None and post_pp_current is None:
        return 0.0

    target_fasting = get_target_fasting(goal, goal3_data)
    target_post_prandial = get_target_post_prandial(goal, goal3_data)
    p = _potency_for_drug(drug_id, drug_class, goal3_data, on_therapy=is_currently_on)
    fasting_potential = p.get("fasting")
    post_pp_potential = p.get("post_prandial")
    # No fallback: use only goal3 potency. When missing, 0 so no boost from that axis.
    if fasting_potential is None:
        fasting_potential = 0
    if post_pp_potential is None:
        post_pp_potential = 0

    # Per Goal 3: Value = (glucose average - potency). IF Value > Target = 0, IF Value <= Target = +0.05
    # Fasting: value = fasting_avg - fasting_potential
    fasting_score = 0.0
    if fasting_current is not None and target_fasting is not None:
        value_after_fasting = fasting_current - (fasting_potential or 0)
        fasting_score = 0.05 if value_after_fasting <= target_fasting else 0.0

    # Post-prandial: value = post_prandial_avg - post_prandial_potential
    post_pp_score = 0.0
    if post_pp_current is not None and target_post_prandial is not None:
        value_after_pp = post_pp_current - (post_pp_potential or 0)  # average - potency
        post_pp_score = 0.05 if value_after_pp <= target_post_prandial else 0.0

    return fasting_score + post_pp_score


def calculate_goal3_on_therapy_max_boost(drug_id, drug_class, patient, normalized_glucose, goal3_data=None):
    """Goal 3 on-therapy: +0.05 when patient is on this drug. Single boost only (no extra for 'max could reach both')."""
    if drug_id not in patient.get("current_drug_ids", set()):
        return 0.0
    return 0.05  # on same med


def get_goal3_boost_breakdown(drug_id, drug_class, patient, normalized_glucose, goal3_data=None):
    """Return per-component Goal 3 boosts for UI Notes: fasting, post_prandial, on_therapy only.
    Each value is 0.0 or 0.05. Used by scoring.get_all_drug_weight_details to populate applied_boosts."""
    out = {"goal3_fasting": 0.0, "goal3_post_prandial": 0.0, "goal3_on_therapy": 0.0}
    if not normalized_glucose:
        return out
    goal = patient.get("goal", 7.5)
    a1c = patient.get("a1c", 0)
    is_currently_on = drug_id in patient.get("current_drug_ids", set())

    fasting_current = normalized_glucose.get("fasting_avg") or estimate_fasting_from_a1c(a1c, goal3_data)
    post_pp_current = normalized_glucose.get("post_pp_avg") or estimate_post_prandial_from_a1c(a1c, goal3_data)
    if fasting_current is None and post_pp_current is None:
        if is_currently_on:
            out["goal3_on_therapy"] = 0.05
        return out

    target_fasting = get_target_fasting(goal, goal3_data)
    target_post_prandial = get_target_post_prandial(goal, goal3_data)
    p = _potency_for_drug(drug_id, drug_class, goal3_data, on_therapy=is_currently_on)
    fasting_potential = p.get("fasting") if p.get("fasting") is not None else 0
    post_pp_potential = p.get("post_prandial") if p.get("post_prandial") is not None else 0

    if fasting_current is not None and target_fasting is not None:
        value_after = fasting_current - (fasting_potential or 0)
        out["goal3_fasting"] = 0.05 if value_after <= target_fasting else 0.0
    # Post-prandial: value = post_prandial_avg - potency; if value <= target → +0.05
    if post_pp_current is not None and target_post_prandial is not None:
        value_after = post_pp_current - (post_pp_potential or 0)
        out["goal3_post_prandial"] = 0.05 if value_after <= target_post_prandial else 0.0

    if is_currently_on:
        out["goal3_on_therapy"] = 0.05
    return out
