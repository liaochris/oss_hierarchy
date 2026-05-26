# Verification Report: §5 Flexibly Modeling Organizational Resilience

**Draft file**: `source/paper/main.tex` lines 650–748  
**Code files**: `source/analysis/event_study_forest/fit_event_study_forest.R`, `source/analysis/event_study_forest/predict_forest_resilience.R`, `source/lib/event_study_forest/event_study_forest.R`, `source/lib/event_study_forest/forest_helpers.R`, `source/lib/R/constants.R`

---

## Discrepancies (draft claims that do not match code)

### D1 — Post-period average formula uses simple average; code uses cohort-weighted average
**Claim**: §5.1 Equation 5 (line 687): $\hat{\psi}(X_i) = \frac{1}{\text{NumPostPeriod}} \sum_{k>0}^{\text{NumPostPeriod}} \hat{\psi}_{e,k}(X_i)$. This implies an equal-weight average over all 5 post-periods.  
**Code**: `forest_helpers.R` `AggregateEventStudy(..., type="att")` (lines 158–162). The code weights by `prob_cohort / slots_per_cohort`, where `slots_per_cohort` is the number of valid event times for a cohort and the sum is restricted to `cohort + event_time <= max_time`. For early cohorts with all 5 post-periods valid, this reduces to a simple average and matches the equation. For late cohorts where some post-periods are truncated (cohort + event_time > max_time), the denominator is the number of valid periods, not NumPostPeriod=5, so the equation is imprecise.  
**Suggested fix**: Change the equation to $\hat{\psi}(X_i) = \frac{1}{K_e} \sum_{k=1}^{K_e} \hat{\psi}_{e,k}(X_i)$ where $K_e$ is the number of post-periods available for cohort $e$, or add a footnote noting that the average is over the available post-periods for the cohort.  
**Severity**: LOW
- @CLAUDE: When does this actually ever happen? I tohught all post-periods are valid?
---

## Gaps (code steps not documented in the draft)

### G1 — Propensity score uses marginal cohort probability conditioned on entry period, not on covariates
**Code step**: `forest_helpers.R` `ComputeCohortTimeDist` (lines 257–266) computes $P(G=g \mid \text{first\_time})$ — the probability of being in treatment cohort $g$ conditional on the organization's first observed period, marginalized over covariates. A comment on line 256 explicitly notes: *"computes marginal P(G=g | first_time); the paper's F_G also conditions on X_i — see HANDOFF.md for planned enhancement."*  
**Why it matters**: The paper (line 679 footnote) says propensity scores are "adapted to the panel data and staggered treatment adoption setting" and cites Appendix C of the method note for details, without disclosing this simplification. A reader of the paper would assume the propensity model conditions on covariates $X_i$ as described in the theoretical note; the implementation conditions only on entry period.  
**Suggested fix**: Add a footnote noting that the propensity score implementation conditions on entry period rather than the full covariate vector.  
**Severity**: LOW
- @CLAUDE: leave as is. 

### G2 — Number of cross-fitting folds not stated
**Code step**: `analysis_parameters.json` `n_folds = 10`. `constants.R` `N_FOLDS <- analysis_parameters$n_folds`. The paper describes a "k-fold procedure" throughout §5 but never states $k = 10$.  
**Suggested fix**: Add "using $k=10$ folds" where the $k$-fold procedure is first introduced (line 680).  
**Severity**: LOW
- @CLAUDE: Sure, use an autofill for the number of folds. Make sue that autofill is a global and imported throughout code as well. 

### G3 — Continuous Forest Split training outcome not re-stated
**Code step**: `fit_event_study_forest.R` line 49: `df_data <- CreateDataPanel(panel, FOREST_TRAINING_OUTCOME, ...)`. `FOREST_TRAINING_OUTCOME = "pull_request_merged"` for all covariate types including `pc_score` (continuous).  
**Why it matters**: The paper clearly states at line 708 that the coarse Forest Split "used only the pull requests merged outcome." The continuous split analysis at lines 714–718 does not repeat this, leaving a reader uncertain whether the continuous analysis also uses PR merged as the training outcome.  
**Suggested fix**: Add "using the same pull requests merged training outcome" when introducing the continuous Forest Split at line 714.  
**Severity**: LOW
- @CLAUDE: sure. 
---

## Overall Assessment

The section is well-aligned with the code. The key item to address is G1 (propensity score conditioned on entry period rather than covariates), which is a noted simplification relative to the theoretical paper; the other items are minor presentation gaps.
