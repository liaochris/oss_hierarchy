# Code Review: source/analysis + source/lib/event_study_forest

## Completed fixes (all batches)

### source/analysis/event_study/estimate_resilience.R

- **Double parquet load** (non-aggregated branch): `LoadPreparedSample` was called twice for the same path when both `full_sample` and `pc_score` splits were active. Fixed: load `panel` once, pass to both split functions.
- **`BuildOutcomeSpecs` unnecessary arguments**: `outcome_variables` and `NORM_OPTIONS` were passed as arguments but are already in-scope globals. Fixed: drop both from the signature.

### source/analysis/event_study/combine_estimates.R

- **`AbsAvgPostPeriodEffect` operation order**: Original code computed `abs() %>% mean()` (mean of absolute values per period). Paper reports the absolute value of the average decline, so the correct order is `mean() %>% abs()`. Fixed and renamed from `compute_avg_abs` to `AbsAvgPostPeriodEffect` (CamelCase per convention).

### source/analysis/event_study_forest/fit_event_study_forest.R

- **`SelectFeatureColumns` single-use extraction**: The function was a two-line `switch` called only in `Main()`. Inlined directly.

### source/analysis/event_study_forest/predict_forest_resilience.R

- **`SPLIT_CONFIGS` placement**: Moved above `Main()` with other globals.
- **`UnifyEventTimeColumns` + `AddDRSplitColumns` chain**: Merged into single `AddDoublyRobustSplitColumns`; "DR" expanded throughout.
- **Duplicate coefficient-collection block**: Extracted `CollectForestEstimateRow` helper used by both `RunSplitEventStudies` and `RunAggregatedSplitEventStudies`.
- **`dr` abbreviation**: All `dr_*` column names, local variables, and string labels renamed to `doubly_robust_*` / `DoublyRobust`.

### source/lib/event_study_forest/forest_helpers.R

- **`ParseArmName` → `ParseCohortEventLabel`**: Renamed throughout for domain clarity.
- **`ResidualizeOutcome` → `ResidualizeOutcomeLinearFE`**: Renamed to make explicit that this implements the Appendix §1 linear time FE special case (not the main heterogeneous baseline path).
- **Double `ParseCohortEventLabel` call in `TransformRepoCohortTimeCoefficients`**: Was called once for `$cohort` and once for `$event_time`. Fixed: call once, store as `parsed`, reference `parsed$cohort` and `parsed$event_time`.
- **`AveragePreTreatmentCovariateAcrossPeriods` missing guard**: Silently returned NULL for unsupported `rolling_period` values. Fixed: add `stop()` for values other than 1 or 5.
- **Loop variable names in `ComputeBaselinePropensities`**: `rid` → `repo_id`, `rp` → `prob_cohort_by_repo`. Added vectorization comment.
- **`ComputeCohortTimeDist` covariate conditioning**: Current implementation conditions only on `first_time`, not on covariates `X_i` as the paper's `F_G(g | s, X)` requires. Added comment pointing to HANDOFF.md for the planned enhancement.
- **Probability naming**: Standardized all cohort probability variables to `prob_cohort` (replacing `prob_treatment_cohort`). Data frame holding the distribution renamed `cohort_prob_dist` to avoid collision with the column name.
- **`dr` abbreviation**: `dr_scores` parameter in `AggregateDoublyRobustByRepo` renamed to `doubly_robust_scores`.

### source/lib/event_study_forest/event_study_forest.R

- **`BuildCohortTimeCoefficients` simplified**: Removed unfiltered prediction path and `present` column. Now: assign filtered result to `tau_hat_filt`, pass to `TransformRepoCohortTimeCoefficients`, filter `!is.na(coef)`.
- **`treated_idx` comment**: Added explanation that this selects each treated unit's own cohort/event-time prediction, not predictions for other arms.
- **`dr_fold` / `att_dr` / `att_dr_group`**: Renamed to `doubly_robust_fold`, `att_doubly_robust`, `att_doubly_robust_group` throughout.

### source/analysis/analyze_forest/outcome_diagnostics.R

- **`BuildOutcomeSummaryTablefill` typo**: Renamed to `BuildOutcomeSummaryAutofill`.
- **Hardcoded `outcome_cols`**: `c("pull_request_opened", ...)` replaced with `unlist(lapply(outcome_variables, function(x) x$run))`.

### source/analysis/analyze_forest/pc_score_combinations.R

- **Deprecated `size` in `geom_tile`**: `size = 1.5` → `linewidth = 1.5` (ggplot2 3.4.0+).
- **`att_dr` / `att_dr_mean`**: Renamed to `att_doubly_robust` / `att_doubly_robust_mean`.

### source/analysis/analyze_forest/fold_comparisons.R

- **`att_dr` / `att_dr_mean`**: Renamed to `att_doubly_robust` / `att_doubly_robust_mean`.

### source/analysis/analyze_forest/SConscript

- **Missing fold `.rds` inputs**: `fold_comparisons.R` reads per-fold `.rds` model files from `drive/output/...` but these were not listed as SCons inputs. Fixed: enumerate all fold paths using `N_FOLDS` (read from `analysis_parameters.json`) and add to `fold_sources`.

### source/analysis/summ_stats/create_project_summary.py

- **`TimePeriod = 6` hardcoded**: Now reads `settings["time_period_months"]` from `global_settings.json` (already loaded in `GenerateConfigAutofill`).
- **`LoadPipelineInputs()` missing path**: Now passes explicit path `"source/lib/config/pipeline_inputs.json"` matching the SConscript call.

### source/analysis/summ_stats/SConscript

- **Missing config deps**: Added `pipeline_inputs.json`, `analysis_parameters.json`, `importance_specifications.json`, `paper_settings.json`, `feature_variables.json` — all read by `GenerateConfigAutofill` but absent from `summary_source`.
- **Missing data deps**: Added `exported_graphs_log.csv` and the `org_panel` parquet from drive.

---

## Math/code alignment (source/lib/event_study_forest vs grf_es.tex)

Verified the following match the paper:

- **First-differencing** (`FirstDifferenceOutcome`): subtracts event-time −1 baseline per repo, matching eq. (1).
- **T matrix** (`MakeTimeIndicatorMatrix`): indicator for `time_index` minus indicator for `quasi_treatment_group − 1`, matching the paper's T construction.
- **Baseline propensities** (`ComputeBaselinePropensities`): sets `W_hat` to 1 at the unit's own time index, then subtracts weighted cohort probabilities at each cohort's normalization time, matching the paper's IPW formula.
- **Doubly robust scoring** (`ComputeFoldDoublyRobust`): direct + IPW correction term; `treated_idx` correctly selects each treated unit's own arm prediction.
- **Aggregation** (`AggregateEventStudy`): cohort-probability-weighted average of CATT(x) estimates, matching the paper's ATT aggregation.

**Known simplification**: `ComputeCohortTimeDist` produces `P(G=g | s_i)` — marginal over covariates. The paper's `F_G(g | s_i, X_i)` also conditions on covariates. Documented in HANDOFF.md as a planned enhancement.
