# Recommendation Calculation — High-Level Reference

A short overview of how the diabetes medication recommendation engine works and what the config JSONs contain.

---

## Overall Flow

1. **Transform** — Convert the request (patient info, meds, comorbidities, glucose) into a normalized patient object.
2. **Score** — Compute a clinical fit score and a coverage score for each drug.
3. **Rank** — Sort drugs by clinical fit, then by coverage.
4. **Select** — Choose the top 2 by clinical fit (from different drug classes), plus the lowest-cost option among the top 5.
5. **Dose** — Look up eGFR-adjusted starting or next dose for the chosen drug.
6. **Explain** — Build an assessment and rationale (optionally with AI).

---

## Config JSONs — What They Hold

### drug_classes.json (Goal 1)

- **Classes** — Drug classes (Metformin, SGLT2, GLP1, etc.) with:
  - Cost tier (low/medium/high)
  - Formulary tier (1–4)
  - Base access score
  - Allergy labels for matching
  - VA PDF flag, prior-auth flag
- **Drugs** — Individual drugs with:
  - `clinical_base` — Base clinical fit (0–1)
  - `deny_if` — Rules that exclude the drug (e.g. eGFR &lt; 30, dialysis)
  - `clinical_boost` — Rules that add to fit (e.g. ASCVD, CKD, heart failure)
  - `caution_if` — Rules that subtract from fit (e.g. eGFR 30–45)
  - `drug_in_class_bonus` — Small adjustment for preferred drug in a class
- **current_therapy_boost** — Extra fit when the patient is already on that drug.

### goal2.json (Dosing)

- **form_value_by_class** — Maps form values (e.g. `biguanides`) to class names (e.g. `Metformin`).
- **default_medications** — Default drug name and dose per class when no specific drug is chosen.
- **starting_dose_by_class** — eGFR-based starting doses and titration rules per class.
- **by_drug** — Drug-specific dosing (e.g. SGLT2s with different eGFR cutoffs).

### goal3.json (Glucose / Potency)

- **goal_bands** — Target ranges for fasting and post-prandial glucose by A1C goal (&lt;7%, &lt;7.5%, &lt;8%).
- **a1c_to_fasting** / **a1c_to_post_prandial** — Estimated glucose from A1C when no readings are given.
- **potency_by_class** — How much each class lowers fasting vs post-prandial (0–100).
- **potency_on_therapy_by_class** — Potency when the patient is already on that drug (dose increase scenario).

---

## Clinical Fit Score (0–1)

- Start from `clinical_base` for the drug.
- **Exclude** if any `deny_if` rule matches (allergy, eGFR, dialysis, pregnancy, etc.).
- **Exclude** if the patient is on the drug and at max dose.
- **Add** for each matching `clinical_boost` (e.g. ASCVD +0.10, CKD +0.12).
- **Subtract** for each matching `caution_if` (e.g. eGFR 30–45).
- **Add** current-therapy boost if the patient is already on that drug.
- **Add** small goal-based bonus (e.g. A1C goal &lt;7%).
- **Add** Goal 3 glucose boost if the drug can lower fasting/post-prandial toward target.
- Cap at 0.90 for drugs, 1.0 for “No Change.”

---

## Coverage Score (0–1)

- Start from `base_access_score`.
- Adjust for insurance (VA +0.10, Medicare +0.05, Medicaid −0.05, no insurance −0.25).
- Adjust for cost tier and formulary tier.
- Subtract if prior authorization is required.
- Add VA PDF boost when applicable.
- Add small CGM monitoring boost when applicable.
- Exclude if any `deny_if` rule matches.

---

## Goal 3 (Glucose / Potency)

- Uses fasting and post-prandial averages (from readings or estimated from A1C).
- Compares to target bands based on the patient’s A1C goal.
- Gives a small boost (+0.05 each) when the drug can:
  - Lower fasting toward goal
  - Lower post-prandial toward goal
  - Support dose increase when already on therapy
- Potency values determine which drugs get the boost (e.g. basal insulin for fasting, GLP1 for post-prandial).

---

## Final Selection

- Rank drugs by **clinical_fit** (includes current-therapy boost), then by coverage. Ranking matches the trace log display.
- **#1 Best Choice** = highest clinical fit.
- **#2 Best Choice** = highest clinical fit from a **different drug class** than #1.
- **Lowest Cost Option** = among the top 5 by clinical fit, pick the one with lowest cost (tier, cost level).
- Top 3 = top 2 (from different classes) + 3rd = lowest cost (if different from top 2).
- “No Change” is a valid option when current therapy is appropriate.

---

## Dosing

- Uses eGFR to pick the right starting or next dose.
- Different rules per class (e.g. Metformin max 1000 mg when eGFR 30–45).
- If the patient is on the drug, suggests the next titration step or “at max dose.”
