# Scoring Interpretation (trimmed reference)

**clinical_fit** (0–1): Measures drug suitability based on comorbidity benefit match, contraindication absence, eGFR safety, and A1C lowering potential.

**coverage** (0–1): Reflects insurance formulary match and estimated out-of-pocket cost.

**Selection logic**: Top 2 options are highest clinical_fit from *different drug classes*. Option 3 is the lowest-cost option not already selected. If no change to therapy is optimal, the engine returns "No Change" with clinical_fit=1.0 indicating current regimen is appropriate.
