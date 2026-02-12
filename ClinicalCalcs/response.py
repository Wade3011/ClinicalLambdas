"""
Build assessment, rationale, alternatives, lowest-cost option; Claude API for rationale.
Uses glucose for finger_poke interpretation; transform for normalize_glucose in Claude prompt.
"""
import json
import os
import re
import time
import urllib.request
import urllib.error
from transform import normalize_glucose_readings
from glucose import finger_poke_interpret, goal3_bands


def generate_assessment(patient, top_result, normalized_glucose, goal3_data=None):
    """Generate clinical assessment text. goal3_data for finger-poke bands when provided."""
    fasting_avg = normalized_glucose.get("fasting_avg") if isinstance(normalized_glucose, dict) else None
    post_pp_avg = normalized_glucose.get("post_pp_avg") if isinstance(normalized_glucose, dict) else None
    if fasting_avg is None and isinstance(normalized_glucose, dict):
        fasting_avg = (normalized_glucose.get("fasting") or {}).get("average")
    if post_pp_avg is None and isinstance(normalized_glucose, dict):
        post_pp_avg = (normalized_glucose.get("postPrandial") or {}).get("average")

    assessment_parts = []
    if patient["a1c"] > 0:
        goal = patient["goal"]
        if patient["a1c"] > goal:
            assessment_parts.append(f"Current A1C of {patient['a1c']}% exceeds goal of <{goal}%")
        else:
            assessment_parts.append(f"Current A1C of {patient['a1c']}% is at or below goal of <{goal}%")

    if isinstance(normalized_glucose, dict) and normalized_glucose.get("time_in_range") is not None:
        tir = normalized_glucose["time_in_range"]
        assessment_parts.append(f"Time in range below target ({tir}%)" if tir < 70 else f"Time in range {tir}%")
        if normalized_glucose.get("gmi") is not None:
            assessment_parts.append(f"Glucose Management Indicator (GMI): {normalized_glucose['gmi']}%")
        if fasting_avg is not None:
            assessment_parts.append(f"Wake-up average: {fasting_avg} mg/dL")
        if post_pp_avg is not None:
            assessment_parts.append(f"Bedtime average: {post_pp_avg} mg/dL")
    else:
        if fasting_avg is not None or post_pp_avg is not None:
            fp = finger_poke_interpret(patient.get("goal"), fasting_avg, post_pp_avg, goal3_data)
            bands = goal3_bands(goal3_data)
            if fp.get("fasting") is not None and fasting_avg is not None:
                favg = float(fasting_avg)
                if fp["fasting"] == "reduce":
                    assessment_parts.append(f"Fasting average {favg:.0f} mg/dL (below target): recommend dose reduction to drug class with highest fasting lowering potential")
                elif fp["fasting"] == "no_change":
                    band = bands[fp["band"]]["fasting"]
                    assessment_parts.append(f"Fasting average {favg:.0f} mg/dL (within {band['ok_min']}-{band['ok_max']} mg/dL): no change needed to lower fasting levels")
                else:
                    assessment_parts.append(f"Fasting average {favg:.0f} mg/dL (above target): recommend dose increase or new start to lower fasting levels")
            if fp.get("post_prandial") is not None and post_pp_avg is not None:
                pavg = float(post_pp_avg)
                if fp["post_prandial"] == "reduce":
                    assessment_parts.append(f"Post-prandial average {pavg:.0f} mg/dL (below 100): recommend dose reduction to drug class with highest post-prandial lowering potential")
                elif fp["post_prandial"] == "no_change":
                    band = bands[fp["band"]]["post_prandial"]
                    assessment_parts.append(f"Post-prandial average {pavg:.0f} mg/dL (within {band['ok_min']}-{band['ok_max']} mg/dL): no change needed to lower post-prandial levels")
                else:
                    assessment_parts.append(f"Post-prandial average {pavg:.0f} mg/dL (above target): recommend dose increase or new start to lower post-prandial level")

    if not assessment_parts:
        assessment_parts.append("Reviewing patient data for medication recommendations")
    return ". ".join(assessment_parts) + "."


def generate_rationale(patient, result, drug_data):
    """Generate rationale for the best choice."""
    rationale = []
    if result["clinical_fit"] >= 0.75:
        rationale.append(f"High clinical fit score ({result['clinical_fit']}) based on patient profile")
    elif result["clinical_fit"] >= 0.60:
        rationale.append(f"Moderate clinical fit score ({result['clinical_fit']})")
    else:
        rationale.append(f"Clinical fit score ({result['clinical_fit']})")
    if result["coverage"] >= 0.75:
        rationale.append(f"Excellent coverage probability ({int(result['coverage'] * 100)}%)")
    elif result["coverage"] >= 0.60:
        rationale.append(f"Good coverage probability ({int(result['coverage'] * 100)}%)")
    comorbidities = patient.get("comorbidities", set())
    if "ASCVD" in comorbidities and result["class"] in ["SGLT2", "GLP1"]:
        rationale.append("Proven cardiovascular benefits in patients with ASCVD")
    if "Heart Failure (CHF)" in comorbidities and result["class"] == "SGLT2":
        rationale.append("Demonstrated heart failure mortality benefit")
    if "Obesity (BMI > 40)" in comorbidities and result["class"] == "GLP1":
        rationale.append("Significant weight loss benefit (10-15% body weight reduction)")
    if "CKD" in comorbidities and result["class"] == "SGLT2":
        rationale.append("Renal protective benefits and slows CKD progression")
    if patient["a1c"] >= 9.0 and result["class"] in ["Basal Insulin", "Bolus Insulin"]:
        rationale.append("A1C >9% warrants aggressive therapy with insulin")
    return rationale


def _drug_display_name(alt, config=None):
    """Return drug name for display; include brand (display_name) when present in config.
    When display_name is a full medication string (e.g. 'Glargine (Lantus)'), return it as-is
    so the ranking table shows drug name instead of class name (e.g. Basal Insulin -> Glargine (Lantus))."""
    display = alt.get("drug") or alt.get("class") or "Other"
    cls = alt.get("class")
    drugs = (config.get("drugs", {}) if config and isinstance(config, dict) else {}) or {}
    # If we have a drug id, check for display_name (e.g. Semaglutide -> Ozempic, or Basal Insulin -> Glargine (Lantus))
    if alt.get("drug") and display in drugs:
        data = drugs.get(display)
        if isinstance(data, dict) and data.get("display_name"):
            dname = data["display_name"]
            # Full medication string (contains parens): use as-is so table shows drug name not class
            if "(" in (dname or ""):
                return dname
            return f"{display} ({dname})"
    if config and cls and (display == cls or not alt.get("drug")):
        for drug_id, data in drugs.items():
            if isinstance(data, dict) and data.get("class") == cls:
                if data.get("display_name"):
                    dname = data["display_name"]
                    if "(" in (dname or ""):
                        return dname
                    return f"{drug_id} ({dname})"
                return drug_id
    return display


def _condition_to_plain_text(condition_str):
    """Turn condition string like 'No active diabetes therapy (-0.20)' into plain text for prose."""
    if not condition_str or not isinstance(condition_str, str):
        return condition_str or ""
    # Remove trailing " (-0.20)" or " (+0.05)" for readability
    s = condition_str.strip()
    if s.endswith(")"):
        i = s.rfind(" (")
        if i > 0 and ("+" in s[i:] or "-" in s[i:]):
            s = s[:i].strip()
    return s


def generate_alternatives(results, top_class, top_drug_id=None, config=None, exclude_ids=None, drug_details_map=None):
    """Generate alternative recommendations (top 3). Exclude best choice, second best, and lowest-cost option from bullets.
    Format as readable text explaining why each option was not preferred."""
    if exclude_ids is None:
        exclude_ids = {top_drug_id or top_class}
    alternatives = []
    for alt in [r for r in results if r.get("drug", r.get("class")) not in exclude_ids][:3]:
        if alt["coverage"] > 0:
            fit = alt.get("clinical_fit_rank", alt["clinical_fit"])
            fit_display = round(float(fit), 2)
            display_name = _drug_display_name(alt, config)
            cov_pct = int(alt["coverage"] * 100)
            drug_id = alt.get("drug", alt.get("class"))
            details = (drug_details_map or {}).get(drug_id) if drug_details_map else None
            cautions = (details or {}).get("applied_cautions") or []
            boosts = (details or {}).get("applied_boosts") or []
            penalty_plain = [_condition_to_plain_text(c.get("condition", str(c))) for c in cautions if isinstance(c, dict)]
            boost_plain = [_condition_to_plain_text(b.get("condition", str(b))) for b in boosts if isinstance(b, dict)]

            # Build text-based explanation
            parts = [f"{display_name} had a clinical fit score of {fit_display} and {cov_pct}% coverage."]
            if penalty_plain:
                if len(penalty_plain) == 1:
                    parts.append(f"It was not preferred because it received a penalty for {penalty_plain[0].lower()}.")
                else:
                    parts.append(f"It was not preferred because it received penalties for {', '.join(p.lower() for p in penalty_plain[:-1])} and {penalty_plain[-1].lower()}.")
            if boost_plain:
                if len(boost_plain) == 1:
                    parts.append(f"It did receive a boost for {boost_plain[0].lower()}.")
                else:
                    parts.append(f"It did receive boosts for {', '.join(p.lower() for p in boost_plain[:-1])} and {boost_plain[-1].lower()}.")
            if alt["coverage"] < 0.5:
                parts.append("Coverage may be limited.")
            alt_text = " ".join(parts)
            alternatives.append(alt_text)
    return alternatives


def _cost_score(result, drugs):
    """Lower is cheaper. drugs = config['drugs'] (drug-level cost/tier)."""
    drug_id = result.get("drug", result.get("class"))
    data = drugs.get(drug_id, {}) if isinstance(drugs, dict) else {}
    tier = data.get("tier", 3)
    cost = data.get("cost", "medium")
    cost_vals = {"low": 1, "medium": 2, "high": 3, "very_high": 4}
    return (tier, cost_vals.get(cost, 2))


def find_lowest_cost_option(results, config_or_classes):
    """Among best clinical-fit options (top 5), pick the one with lowest cost. config = {classes, drugs}."""
    two = find_two_lowest_cost_options(results, config_or_classes, n=1)
    return two[0] if two else None


def find_cheapest_for_index2(results, config_or_classes, exclude_drug_ids):
    """Return the cheapest option that is NOT in exclude_drug_ids.
    Prefer top 5 by clinical fit; if all are excluded, search all results.
    Used for top3BestOptions[2] so index 2 always has the lowest-cost option."""
    config = config_or_classes if isinstance(config_or_classes, dict) else {}
    drugs = config.get("drugs") or {}
    if not isinstance(drugs, dict):
        drugs = {}

    def _viable(pool):
        v = [r for r in pool if r.get("coverage", 0) > 0.5 and r.get("drug", r.get("class")) in drugs]
        if not v:
            v = [r for r in pool if r.get("coverage", 0) > 0]
        return v

    # Try top 5 first, then all results (when top 5 has only 2 drugs, e.g. Metformin + Empagliflozin)
    for pool in [results[:5] if len(results) >= 5 else results, results]:
        viable = _viable(pool)
        if not viable:
            continue
        viable_sorted = sorted(viable, key=lambda r: _cost_score(r, drugs))
        for r in viable_sorted:
            drug_id = r.get("drug", r.get("class"))
            if drug_id and drug_id not in exclude_drug_ids:
                return r
    return None


def find_two_lowest_cost_options(results, config_or_classes, n=2):
    """Among best clinical-fit options (top 5 by clinical_fit_rank), return up to n with lowest cost.
    Returns list of result dicts, cheapest first. Prefer second from different drug class; else use 2nd cheapest."""
    config = config_or_classes if isinstance(config_or_classes, dict) else {}
    drugs = config.get("drugs", config)
    top_by_fit = results[:5] if len(results) >= 5 else results
    viable = [r for r in top_by_fit if r["coverage"] > 0.5 and r.get("drug", r.get("class")) in drugs]
    if not viable:
        viable = [r for r in top_by_fit if r["coverage"] > 0]
    if not viable:
        viable = [r for r in results if r["coverage"] > 0.5 and r.get("drug", r.get("class")) in drugs]
    if not viable:
        viable = [r for r in results if r["coverage"] > 0]
    if not viable:
        return []
    viable_sorted = sorted(viable, key=lambda r: _cost_score(r, drugs))
    if n <= 1:
        return viable_sorted[:n]
    first = viable_sorted[0]
    out = [first]
    first_class = first.get("class")
    for r in viable_sorted[1:]:
        if r.get("class") != first_class:
            out.append(r)
            break
    if len(out) < 2 and len(viable_sorted) >= 2:
        out.append(viable_sorted[1])
    return out[:n]


def _build_drug_classes_from_config(config):
    """Build drug_classes dict (class -> merged data) for prompt. Uses first drug per class."""
    drug_classes = {}
    classes = config.get("classes", {})
    for drug_id, data in config.get("drugs", {}).items():
        cls = data.get("class")
        if cls and cls not in drug_classes:
            merged = dict(classes.get(cls, {}))
            merged.update(data)
            drug_classes[cls] = merged
    return drug_classes


def _load_reference_file(filename):
    """Load a reference MD file for AI context. Returns empty string if not found."""
    base = os.path.dirname(os.path.abspath(__file__))
    for path in [
        os.path.join(base, filename),
        os.path.join("/var/task", filename),
        filename,
    ]:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return f.read()
            except Exception:
                pass
    return ""


def _format_allergy_for_display(entry):
    """Format a single allergy entry (string or { allergen, specificDrugs? }) for prompt/display."""
    if entry is None:
        return ""
    if isinstance(entry, str):
        return (entry or "").strip()
    if isinstance(entry, dict):
        allergen = (entry.get("allergen") or "").strip()
        if not allergen:
            return ""
        specific = entry.get("specificDrugs") or []
        if not specific or (len(specific) == 1 and (specific[0] or "").strip() == "All"):
            return allergen
        drugs_str = ", ".join((s or "").strip() for s in specific if (s or "").strip())
        return f"{allergen}: {drugs_str}" if drugs_str else allergen
    return str(entry).strip()


def _granular_allergy_clarification(allergies_list):
    """Build a one-line note when patient has drug-level allergies (open to trialing others in class).
    Used so the AI does not state the entire class is contraindicated in other_options_not_preferred."""
    if not allergies_list:
        return ""
    parts = []
    for a in allergies_list:
        if not isinstance(a, dict):
            continue
        allergen = (a.get("allergen") or "").strip()
        if not allergen or allergen.startswith("Other:"):
            continue
        specific = a.get("specificDrugs") or []
        if not specific or (len(specific) == 1 and (specific[0] or "").strip() == "All"):
            continue
        if a.get("openToTrial") is not True:
            continue
        drugs_str = ", ".join((s or "").strip() for s in specific if (s or "").strip())
        if drugs_str:
            parts.append(f"{allergen}: only {drugs_str} excluded due to allergy; other drugs in this class may still be recommended.")
    if not parts:
        return ""
    return " ALLERGY CLARIFICATION (for other_options_not_preferred): " + " ".join(parts)


def build_claude_prompt(request_data, results, drug_classes, patient, alternative_drug_names=None, top_two_results=None, lowest_cost_result=None, is_deescalation=False, a1c_above_goal=False, assessment=""):
    """Build system and user prompt for Claude API. Uses top_two_results when provided (the displayed #1 and #2).
    lowest_cost_result is the lowest cost option to determine if best_cost_explanation should be included.
    assessment is the pre-computed clinical assessment text; Claude may return an updated version.
    is_deescalation=True: top options are reduce/maintain; explain why these are recommended per de-escalation rules.
    a1c_above_goal=True: when in de-escalation, A1C is above goal; future_considerations should include Metformin increase or add-on after reduction."""
    patient_info = request_data.get("patientInfo", {})
    if top_two_results and len(top_two_results) >= 2:
        top_result, second_result = top_two_results[0], top_two_results[1]
    elif top_two_results and len(top_two_results) == 1:
        top_result, second_result = top_two_results[0], {}
    else:
        top_result = results[0] if results else {}
        second_result = results[1] if len(results) > 1 else {}
    top_class = top_result.get("class", "N/A")
    second_class = second_result.get("class", "N/A") if second_result else "N/A"
    top_drug_data = drug_classes.get(top_class, {})
    second_drug_data = drug_classes.get(second_class, {}) if second_class != "N/A" else {}
    denied_rules = top_drug_data.get("deny_if", [])
    boost_rules = top_drug_data.get("clinical_boost", [])
    caution_rules = top_drug_data.get("caution_if", [])
    applied_boosts = []
    for boost in boost_rules:
        cond = boost.get("rule", boost)
        if isinstance(cond, dict) and cond.get("field") == "comorbidity" and boost.get("add"):
            applied_boosts.append(f"Comorbidity match (+{boost.get('add', 0):.2f} boost)")
    applied_cautions = [f"Caution (-{c.get('penalty', 0):.2f} penalty)" for c in caution_rules]
    norm_glucose = normalize_glucose_readings(request_data)
    fasting_avg = norm_glucose.get("fasting_avg")
    post_pp_avg = norm_glucose.get("post_pp_avg")
    allergies_list = request_data.get("allergies") or patient.get("allergies_raw") or []
    allergy_parts = [_format_allergy_for_display(a) for a in allergies_list if _format_allergy_for_display(a)]
    allergies_str = "; ".join(allergy_parts) if allergy_parts else "None"
    granular_allergy_note = _granular_allergy_clarification(allergies_list)
    system_message = """You are a board-certified endocrinologist and clinical pharmacologist specializing in Type 2 diabetes management. You explain pre-computed medication recommendations to fellow clinicians using evidence-based reasoning grounded in the provided reference documents.

This output is displayed directly to prescribing clinicians in a clinical decision support tool. Accuracy is critical — unsupported claims could affect patient care. Cite ONLY evidence present in the provided reference documents. If the references do not support a claim, do not make it."""
    calc_ref = _load_reference_file("RECOMMENDATION_CALCULATION_REFERENCE.md")
    pharm_ref = _load_reference_file("Type 2 Diabetes Pharmacotherapy Reference.docx.md")
    deesc_ref = _load_reference_file("Diabetes Med De-escalation handout.docx.md")
    # Reference documents are included in the user prompt with XML tags for clear boundaries
    ref_blocks = []
    if calc_ref:
        ref_blocks.append(f"<reference name=\"calculation\">\n{calc_ref}\n</reference>")
    if pharm_ref:
        ref_blocks.append(f"<reference name=\"pharmacotherapy\">\n{pharm_ref}\n</reference>")
    if deesc_ref:
        ref_blocks.append(f"<reference name=\"de-escalation\">\n{deesc_ref}\n</reference>")
    references_section = "\n\n".join(ref_blocks) if ref_blocks else ""
    cgm_lines = []
    if norm_glucose.get("time_in_range") is not None:
        cgm_lines.append(f"- CGM Time in Range: {norm_glucose['time_in_range']}%")
    if norm_glucose.get("gmi") is not None:
        cgm_lines.append(f"- CGM GMI: {norm_glucose['gmi']}%")
    if norm_glucose.get("wake_up_average") is not None:
        cgm_lines.append(f"- Wake-up Average: {norm_glucose['wake_up_average']} mg/dL")
    if norm_glucose.get("bedtime_average") is not None:
        cgm_lines.append(f"- Bedtime Average: {norm_glucose['bedtime_average']} mg/dL")
    cgm_block = "\n".join(cgm_lines) if cgm_lines else ""
    denied_str = ", ".join(str(r) for r in denied_rules) if denied_rules else "None"
    caution_str = ", ".join(str(c.get("rule", c)) for c in caution_rules) if caution_rules else "None"
    # For de-escalation, use medication/dose for display; otherwise class/drug
    if is_deescalation:
        top_class = top_result.get("medication", top_result.get("class", "N/A"))
        top_drug_id = top_result.get("dose", "")
        second_class = second_result.get("medication", second_result.get("class", "N/A")) if second_result else "N/A"
        second_drug_id = second_result.get("dose", "") if second_result else ""
        top_drug_data = {}
        second_drug_data = {}
    else:
        top_class = top_result.get("class", "N/A")
        top_drug_id = top_result.get("drug", top_result.get("class"))
        second_class = second_result.get("class", "N/A") if second_result else "N/A"
        second_drug_id = second_result.get("drug", second_result.get("class")) if second_result else None
        top_drug_data = drug_classes.get(top_class, {})
        second_drug_data = drug_classes.get(second_class, {}) if second_class != "N/A" else {}
    lowest_cost_drug_id = lowest_cost_result.get("drug", lowest_cost_result.get("class")) if lowest_cost_result else None
    lowest_cost_is_duplicate = lowest_cost_drug_id and (lowest_cost_drug_id == top_result.get("drug") or lowest_cost_drug_id == (second_result.get("drug") if second_result else None))
    lowest_cost_class = lowest_cost_result.get("class", "N/A") if lowest_cost_result else "N/A"
    lowest_cost_drug_data = drug_classes.get(lowest_cost_result.get("class"), {}) if lowest_cost_result and lowest_cost_result.get("class") else {}

    # Build current medications string with doses
    current_meds = request_data.get('currentMedications', [])
    current_meds_str = ', '.join([f"{m.get('drugName', '')} {m.get('dose', '')} {m.get('frequency', '')}".strip() for m in current_meds]) if current_meds else 'None'

    # Get alternative drug classes for "other options not preferred" (expand from 3 to 5)
    alts = (alternative_drug_names or [])[:5]
    alt_drugs_str = ", ".join(alts) if alts else "None"

    # Check for lows detected (for de-escalation context)
    lows_detected = norm_glucose.get("lows_detected") or norm_glucose.get("lows_overnight") or norm_glucose.get("lows_after_meals")

    if is_deescalation:
        rec_parts = [
            f"#1 RECOMMENDATION: {top_class}\n- Suggestion: {top_drug_id}"
        ]
        if second_class != "N/A":
            rec_parts.append(f"#2 RECOMMENDATION: {second_class}\n- Suggestion: {second_drug_id}")
        if lowest_cost_result and not lowest_cost_is_duplicate:
            rec_parts.append(f"#3 OPTION: {lowest_cost_result.get('medication', lowest_cost_class)} - {lowest_cost_result.get('dose', '')}")
        rec_block = "\n\n" + "\n\n".join(rec_parts)
    else:
        rec_block = f"""
#1 BEST CLINICAL FIT: {top_class} (Drug: {top_drug_id})
- Clinical Fit: {top_result.get('clinical_fit', 0):.2f} (scale 0-1)
- Coverage: {int(top_result.get('coverage', 0) * 100)}%
- Cost Tier: {top_drug_data.get('tier', 'N/A')} ({top_drug_data.get('cost', 'N/A')} cost)
- Clinical Boosts: {', '.join(applied_boosts) if applied_boosts else 'None'}
- Cautions: {caution_str}

{f'''#2 BEST CLINICAL FIT: {second_class} (Drug: {second_drug_id})
- Clinical Fit: {(second_result.get('clinical_fit_rank') or second_result.get('clinical_fit', 0)):.2f}
- Coverage: {int(second_result.get('coverage', 0) * 100)}%
- Cost Tier: {second_drug_data.get('tier', 'N/A')} ({second_drug_data.get('cost', 'N/A')} cost)
''' if second_class != 'N/A' else ''}
{f'''LOWEST COST OPTION: {lowest_cost_class} (Drug: {lowest_cost_drug_id})
- Clinical Fit: {lowest_cost_result.get('clinical_fit', 0):.2f}
- Coverage: {int(lowest_cost_result.get('coverage', 0) * 100)}%
- Cost Tier: {lowest_cost_drug_data.get('tier', 'N/A')} ({lowest_cost_drug_data.get('cost', 'N/A')} cost)
- NOTE: This is the same drug as #1 or #2, so DO NOT include best_cost_explanation.
''' if lowest_cost_result and lowest_cost_is_duplicate else (f'''LOWEST COST OPTION: {lowest_cost_class} (Drug: {lowest_cost_drug_id})
- Clinical Fit: {lowest_cost_result.get('clinical_fit', 0):.2f}
- Coverage: {int(lowest_cost_result.get('coverage', 0) * 100)}%
- Cost Tier: {lowest_cost_drug_data.get('tier', 'N/A')} ({lowest_cost_drug_data.get('cost', 'N/A')} cost)
''' if lowest_cost_result and not lowest_cost_is_duplicate else '')}"""

    user_prompt = f"""{references_section}

<patient_data>
PATIENT PROFILE:
- Age: {patient_info.get('age', 'N/A')} years
- Current A1C: {patient_info.get('lastA1c', 'N/A')}%
- A1C Goal: {patient_info.get('a1cGoal', 'N/A')}
- eGFR: {patient_info.get('eGFR', 'N/A')} mL/min/1.73m²
- Insurance: {patient_info.get('insurancePlan', 'N/A')}
- Monitoring: {patient_info.get('monitoringMethod', 'N/A')}
- Current Medications: {current_meds_str}
- Comorbidities: {', '.join(request_data.get('comorbidities', [])) or 'None'}
- Allergies: {allergies_str}
{f'- Fasting Glucose Average: {fasting_avg} mg/dL' if fasting_avg is not None else ''}
{f'- Post-Prandial Glucose Average: {post_pp_avg} mg/dL' if post_pp_avg is not None else ''}
{cgm_block}
{f'- Lows Detected: Yes' if lows_detected else ''}
{f'- Additional Context: {request_data.get("additionalContext", "")}' if request_data.get("additionalContext") else ''}
{f"""
CLINICAL ASSESSMENT (pre-computed from patient data):
{assessment}
""" if assessment else ""}
{rec_block}

OTHER OPTIONS NOT IN TOP 3: {alt_drugs_str}
{granular_allergy_note}
</patient_data>"""
    if is_deescalation:
        top_rec = top_result.get("medication", top_result.get("class", "N/A"))
        top_dose = top_result.get("dose", "")
        second_rec = second_result.get("medication", second_result.get("class", "N/A")) if second_result else "N/A"
        second_dose = second_result.get("dose", "") if second_result else ""
        deesc_context = "A1C is above goal with documented hypoglycemia. Reduce sulfonylurea first; consider Metformin increase or add-on therapy after reduction." if a1c_above_goal else "A1C is at goal with hypoglycemia detected. The recommendations are REDUCE and MAINTAIN actions (not add-on therapy)."
        user_prompt += f"""
<task_instructions>
TASK: DE-ESCALATION MODE. {deesc_context}
Generate a JSON response with the following structure. Use the <reference name="de-escalation"> as the primary source.

REQUIRED FIELDS:

1. "best_choice_explanation" (string): ONE concise sentence explaining why {top_rec} is the #1 recommendation.
   - Cite the specific table from <reference name="de-escalation"> (e.g., table 1.1 for sulfonylurea, table 1.2 for basal insulin).
   - Explain the clinical rationale: hypoglycemia risk, handout guidance, dose reduction benefit.

2. "second_choice_explanation" (string): ONE concise sentence explaining why {second_rec} is the #2 recommendation.
   - For MAINTAIN: explain why continuing at current dose is appropriate (e.g., metformin is foundational, low hypoglycemia risk).
   - For REDUCE: same format as #1.
{f'''
3. "best_cost_explanation" (string): ONE concise sentence for the third option if distinct from #1 or #2. Otherwise OMIT.
''' if lowest_cost_result and not lowest_cost_is_duplicate else '''
3. "best_cost_explanation": OMIT (third option same as #1 or #2).
'''}
4. "other_options_not_preferred" (array of 2-3 strings): One sentence each explaining why other drug CLASSES were not preferred.
   - Group by drug class or common theme — use class name only (e.g. "SGLT2 inhibitors"), not individual drug names.
   - Focus ONLY on the add-on drug classes in the OTHER OPTIONS list: {alt_drugs_str}. Do NOT include "No Change" or "do nothing."
   - If ALLERGY CLARIFICATION appears above: only the specific drug(s) named are excluded due to allergy; do NOT state the entire class is contraindicated.
   - Do NOT mix best-choice rationale (e.g. "reduce sulfonylurea") into this field. Keep each bullet focused on why THAT option was rejected.
   - Maximum 3 bullets.

5. "future_considerations" (array of strings): Monitoring and follow-up per <reference name="de-escalation">.
   - Recheck fasting glucose in 1-2 weeks.
   - If A1C rises after de-escalation, consider re-escalation.
   {f'- IMPORTANT: A1C is above goal. Include: reduce sulfonylurea per handout, then consider Metformin increase or add-on (SGLT2/GLP1/DPP4) to address A1C.' if a1c_above_goal else ''}

6. "updated_assessment" (string): Return an updated version of the CLINICAL ASSESSMENT that incorporates relevant insights from the Additional Context and recommendation results. Preserve all core facts from the original. You may add 1-2 observations based on the patient profile or Additional Context — but do NOT introduce new diagnoses, risk assessments, or clinical claims not present in the patient data. Keep it concise (2-4 sentences total). If no changes are needed, return the original verbatim.

RESPONSE FORMAT — return ONLY this JSON (no markdown, no code blocks, no extra text):
{{
  "best_choice_explanation": "...",
  "second_choice_explanation": "...",{f'''
  "best_cost_explanation": "...",''' if lowest_cost_result and not lowest_cost_is_duplicate else ''}
  "other_options_not_preferred": ["...", "..."],
  "future_considerations": ["..."],
  "updated_assessment": "..."
}}

RULES:
- Use Generic (Brand) format for all drug names: e.g., "Semaglutide (Ozempic)", "Empagliflozin (Jardiance)".
- Keep explanations to 1 sentence each for fields 1-3.
- Cite ONLY evidence from the provided <reference> documents. Do not cite trials or guidelines not present in the references.
- Return ONLY valid JSON.
</task_instructions>"""
    else:
        user_prompt += f"""
<task_instructions>
TASK: Generate a JSON response with the following structure. Use <reference name="pharmacotherapy"> for trial citations and <reference name="de-escalation"> for future considerations.

REQUIRED FIELDS:

1. "best_choice_explanation" (string): ONE concise sentence explaining why {top_class} ({top_drug_id}) is the #1 choice.
   - Mention: comorbidity benefits relevant to this patient, glucose control ability, and cite a supporting clinical trial from <reference name="pharmacotherapy"> (e.g., UKPDS, SUSTAIN, CREDENCE).

2. "second_choice_explanation" (string): ONE concise sentence explaining why {second_class} ({second_drug_id}) is the #2 choice.
   - Same format: comorbidity benefits, glucose control, trial citation from <reference name="pharmacotherapy">.
{f'''
3. "best_cost_explanation" (string): ONE concise sentence explaining why {lowest_cost_class} ({lowest_cost_drug_id}) is the lowest cost option.
   - Focus on formulary tier, cost tier, and similar efficacy to alternatives.
''' if lowest_cost_result and not lowest_cost_is_duplicate else '''
3. "best_cost_explanation": OMIT this field entirely (lowest cost option is same as #1 or #2).
'''}
4. "other_options_not_preferred" (array of 2-3 strings): One sentence each explaining why other drug CLASSES were not preferred.
   - Group by drug class or common theme — use class name only, not individual drug names.
   - If ALLERGY CLARIFICATION appears above: only the specific drug(s) named are excluded due to allergy; do NOT state the entire class is contraindicated.
   - Reference the OTHER OPTIONS list: {alt_drugs_str}
   - Maximum 3 bullets.

5. "future_considerations" (array of strings): Recommendations based on <reference name="de-escalation">.
   - If starting a high-potency drug (GLP-1, Basal Insulin, Bolus Insulin), include relevant de-escalation guidance.
   - If patient has lows detected, include dose reduction recommendations.
   - If no de-escalation applies, return empty array [].

6. "updated_assessment" (string): Return an updated version of the CLINICAL ASSESSMENT that incorporates relevant insights from the Additional Context and recommendation results. Preserve all core facts from the original. You may add 1-2 observations based on the patient profile or Additional Context — but do NOT introduce new diagnoses, risk assessments, or clinical claims not present in the patient data. Keep it concise (2-4 sentences total). If no changes are needed, return the original verbatim.

RESPONSE FORMAT — return ONLY this JSON (no markdown, no code blocks, no extra text):
{{
  "best_choice_explanation": "...",
  "second_choice_explanation": "...",{f'''
  "best_cost_explanation": "...",''' if lowest_cost_result and not lowest_cost_is_duplicate else ''}
  "other_options_not_preferred": ["...", "..."],
  "future_considerations": ["..."],
  "updated_assessment": "..."
}}

RULES:
- Use Generic (Brand) format for all drug names: e.g., "Semaglutide (Ozempic)", "Empagliflozin (Jardiance)".
- Keep explanations to 1 sentence each for fields 1-3.
- Cite ONLY trials and evidence from the provided <reference> documents. Do not cite trials or guidelines not present in the references.
- Return ONLY valid JSON.
</task_instructions>"""
    return system_message, user_prompt


def _parse_claude_rationale(parsed):
    """Map Claude JSON to rationale + alternatives + future_considerations.
    rationale = best/second/cost explanations.
    alternatives = other_options_not_preferred (for frontend 'Why Other Options Weren't Preferred').
    future_considerations = de-escalation guidance.
    Returns dict with 'rationale', 'alternatives', and 'future_considerations' keys, or None."""
    if not isinstance(parsed, dict):
        return None

    # New format: best_choice_explanation, second_choice_explanation, best_cost_explanation, other_options_not_preferred, future_considerations
    if "best_choice_explanation" in parsed:
        rationale = []
        # Collect explanations in order: best, second, cost
        for key in ("best_choice_explanation", "second_choice_explanation", "best_cost_explanation", "lowest_cost_explanation"):
            val = parsed.get(key)
            if val and isinstance(val, str) and val.strip():
                rationale.append(val.strip())

        # Parse alternatives (other_options_not_preferred)
        alternatives = []
        val = parsed.get("other_options_not_preferred")
        if isinstance(val, list):
            for s in val:
                if s and isinstance(s, str) and s.strip() and s.strip().upper() != "N/A":
                    alternatives.append(s.strip())
        elif val and isinstance(val, str) and val.strip():
            alternatives.append(val.strip())

        # Parse future_considerations
        future_considerations = []
        val = parsed.get("future_considerations")
        if isinstance(val, list):
            for s in val:
                if s and isinstance(s, str) and s.strip() and s.strip().upper() != "N/A":
                    future_considerations.append(s.strip())
        elif val and isinstance(val, str) and val.strip():
            future_considerations.append(val.strip())

        # Parse updated_assessment
        updated_assessment = ""
        val = parsed.get("updated_assessment")
        if val and isinstance(val, str) and val.strip():
            updated_assessment = val.strip()

        if rationale:
            return {
                "rationale": rationale[:5],
                "alternatives": alternatives[:3],
                "future_considerations": future_considerations,
                "updated_assessment": updated_assessment,
            }
        return None

    # Legacy sentences format: join into one combined rationale paragraph
    best = parsed.get("rationale_best") or parsed.get("sentences")
    second = parsed.get("rationale_second")
    if isinstance(best, list) and len(best) > 0:
        all_sentences = (best[:12] if best else []) + (second[:4] if isinstance(second, list) else [])
        sentences_clean = [s.strip() for s in all_sentences if s and isinstance(s, str) and len(s.strip()) > 5]
        combined = " ".join(sentences_clean) if sentences_clean else ""
        rationale = [combined] if combined else []
        alternatives = []
        val = parsed.get("other_options_not_preferred")
        if isinstance(val, list):
            for s in val:
                if s and isinstance(s, str) and s.strip() and s.strip().upper() != "N/A":
                    alternatives.append(s.strip())
        elif val and isinstance(val, str) and val.strip():
            alternatives.append(val.strip())
        return {"rationale": rationale, "alternatives": alternatives, "future_considerations": [], "updated_assessment": ""}
    return None


def call_claude_api(prompt, api_key, model="claude-sonnet-4-5-20250929", temperature=0.3, system_message=None, max_retries=3):
    """Call Claude API; parse JSON. Returns dict with 'rationale' (list) and 'alternatives' (list).
    alternatives populates payload key 'alternatives' for frontend 'Why Other Options Weren't Preferred'."""
    api_url = "https://api.anthropic.com/v1/messages"
    headers = {"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"}
    messages = [{"role": "user", "content": prompt}]
    payload = {"model": model, "max_tokens": 1500, "temperature": temperature, "messages": messages}
    if system_message:
        payload["system"] = system_message
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(api_url, data=json.dumps(payload).encode("utf-8"), headers=headers)
            with urllib.request.urlopen(req, timeout=45) as response:
                response_data = json.loads(response.read().decode("utf-8"))
                content = response_data.get("content", [])
                if not content:
                    raise ValueError("No content in Claude response")
                text_content = content[0].get("text", "").strip()
                try:
                    parsed = json.loads(text_content)
                    result = _parse_claude_rationale(parsed)
                    if result:
                        return result
                except json.JSONDecodeError:
                    pass
                for pattern in [r'\{[\s\S]*"best_choice_explanation"[\s\S]*\}', r'\{[\s\S]*"rationale_best"[\s\S]*\}', r'\{[\s\S]*"sentences"[\s\S]*\}']:
                    json_match = re.search(pattern, text_content)
                    if json_match:
                        try:
                            parsed = json.loads(json_match.group(0))
                            result = _parse_claude_rationale(parsed)
                            if result:
                                return result
                        except Exception:
                            continue
                sentences = re.split(r'[.!?]+\s+', text_content)
                sentences = [s.strip() for s in sentences if s.strip() and len(s.strip()) > 20 and not s.startswith("```")]
                return {"rationale": sentences[:9] if sentences else [], "alternatives": [], "future_considerations": [], "updated_assessment": ""}
        except urllib.error.HTTPError as e:
            if 400 <= e.code < 500:
                raise
            if attempt < max_retries - 1:
                time.sleep((2 ** attempt) + (attempt * 0.5))
                continue
            raise
        except urllib.error.URLError as e:
            if attempt < max_retries - 1:
                time.sleep((2 ** attempt) + (attempt * 0.5))
                continue
            raise
    raise Exception(f"Failed after {max_retries} attempts")
