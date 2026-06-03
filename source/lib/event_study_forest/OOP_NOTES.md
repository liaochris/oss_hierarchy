# Event Study Forest OOP Refactor Todo List

This document tracks the work needed to turn `source/lib/event_study_forest`
into a cleaner S3-style R library for fitting, inspecting, and reusing the
event-study forest pipeline.

## Public API and User-Facing Interface

- [ ] Keep the public interface S3-based rather than R6-based.
  - S3 is CRAN-friendly, works naturally with base R generics such as
    `predict`, `summary`, `coef`, and `plot`, and matches the current use of
    `UseMethod` and `structure(..., class = ...)`.
- [ ] Define the core user workflow explicitly:
  - construct an `EventStudyForestPipeline`;
  - prepare the event-study panel;
  - fit out-of-sample forests;
  - assemble event-time and ATT outputs;
  - save or inspect results.
- [ ] Make the required inputs clear:
  - covariates;
  - outcome;
  - time index;
  - treatment indicator and treatment cohort;
  - reference event time, defaulting to `-1`.
- [ ] Document how time-varying covariates are handled. The default should use
  pre-treatment values, with the relevant pre-period determined by the reference
  event time.
- [ ] Make the primary outputs explicit:
  - direct forest predictions of treatment effects by future event time;
  - doubly robust treatment-effect predictions by future event time;
  - aggregate ATT and event-study coefficients.

## Object Model and Lifecycle

- [ ] Replace the explicit string `phase` state machine with checks for required
  fields.
  - Current lifecycle: `initialized -> prepared -> fitted -> assembled`.
  - Preferred package pattern: check that required fields such as
    `pipeline$data`, `pipeline$results`, and `pipeline$assembled` are present
    before each method runs.
- [ ] Keep pipeline objects lightweight and inspectable: configuration, prepared
  data metadata, fitted model outputs, and assembled results should be separate
  named fields.
- [ ] Transform file-system outputs into either R object attributes, or specify directory for output. 

## Constructor and Configuration

- [ ] Extend `NewEventStudyForestPipeline()` with configuration options that
  control model behavior without changing downstream scripts.
- [ ] Add `baseline_type = c("heterogeneous", "time_fe")`.
  - `heterogeneous` uses `RunBaselineHeterogeneityCrossFit` to estimate
    time-varying baseline functions of covariates, `f_t(X_i)`.
  - `time_fe` uses `ResidualizeOutcomeLinearFE` to demean first-differenced
    outcomes by never-treated units in the same quasi-treatment cohort and event
    time.
- [ ] Add `covariate_quasi_dates = FALSE`.
  - The default preserves the current marginal quasi-treatment-date assignment.
  - When `TRUE`, never-treated units receive quasi-treatment dates from an
    out-of-fold covariate-conditional distribution.
- [ ] Keep existing defaults for `outcome`, `rolling_period`, `n_folds`, and
  `seed` unless there is a specific empirical reason to change them.

## Data Contracts and Validation

- [ ] Validate that treated units have nonzero treatment cohorts and
  never-treated units have `treatment_group == 0`.
- [ ] Validate that treatment cohorts are defined on the same time scale as the
  panel time index.
- [ ] Validate that the reference period exists for each unit before first
  differencing.
- [ ] Validate that `feature_cols` exist in the prepared data and contain usable
  numeric covariates for `grf`.
- [ ] Replace ambiguous quasi-event-time assumptions with explicit event-time
  construction wherever possible.
- [ ] Remove or rename any stale "quasi-event time" variables once the code has a
  clearer distinction between observed treatment timing and assigned
  quasi-treatment timing.

## Core Pipeline Methods

- [ ] Keep `PrepareData()` responsible for building the estimation-ready panel,
  assigning folds, creating first differences, and recording feature columns.
- [ ] Keep `FitOutOfSample()` responsible for K-fold out-of-sample prediction and
  doubly robust scoring.
- [ ] Keep `Assemble()` responsible for converting fold-level predictions into
  event-time coefficients, repo-level outputs, and aggregate ATT outputs.
- [ ] Keep `Save()` responsible for writing pipeline outputs to disk through the
  existing `SaveData` conventions.
- [ ] Ensure each method returns the updated pipeline object invisibly or
  predictably, so workflows can be piped or assigned.

## S3 Methods and Diagnostics

- [ ] Implement `predict.EventStudyForestPipeline(object, newdata, type, ...)`.
  - Supported `type` values should include `att`, `event_time`, and
    `doubly_robust`.
  - The method should make the direct estimate and the doubly robust estimate
    visibly distinct.
- [ ] Implement `coef.EventStudyForestPipeline(object, aggregate, ...)`.
  - Supported `aggregate` values should include `att` and `event_time`.
  - Return population-average event-study coefficients in a tidy data frame.
- [ ] Implement `summary.EventStudyForestPipeline(object, ...)`.
  - Include the outcome, rolling period, number of folds, number of repos, number
    of observations, baseline type, and top variable-importance diagnostics when
    available.
- [ ] Implement `plot.EventStudyForestPipeline(object, type, ...)`.
  - Supported plot types should include `event_study`, `importance`, and
    `stability`.
- [ ] Convert analysis outputs that are currently scattered across scripts into
  package methods where appropriate:
  - `variable_importance(object)`;
  - `fold_stability(object)`;
  - `heterogeneity_split(object, by, ...)`.

## Estimation Internals

- [ ] Keep cross-fitting as the default estimation discipline: a unit's held-out
  prediction should not be produced by a forest trained on that unit.
- [ ] Keep direct forest predictions and doubly robust scores as separate
  internal objects until the public API combines or compares them.
- [ ] Expose doubly robust estimates through a named argument or `type`, not
  through ambiguous column naming alone.
- [ ] Preserve the current full-sample model artifact only as an explicit
  persisted model for inspection or reuse after fold-level predictions are
  complete.
- [ ] Check the implementation for time fixed-effect leakage before promoting
  `baseline_type = "time_fe"` as a supported user option.
- [ ] Check the PCA feature construction for leakage. If PCA rotations are fit on
  the full estimation sample, decide whether to cross-fit them or document the
  limitation clearly.

## Covariate-Conditional Quasi-Treatment Dates

- [ ] Replace the current marginal assignment `P(G = g | s_i)` with an optional
  covariate-conditional assignment `P(G = g | s_i, X_i)`.
- [ ] Add `FitQuasiTreatmentDatesFold(df_data, feature_cols, fold_id, seed)`.
  - Train a `probability_forest` on treated units outside `fold_id`.
  - Predict cohort probabilities for never-treated units inside `fold_id`.
  - Set impossible cohorts to zero when `g - 1 < first_time`.
  - Sample a quasi-treatment date from the normalized covariate-conditional
    distribution.
- [ ] Add `CrossFitQuasiTreatmentDates(df_data, feature_cols, n_folds, seed)`.
  - Loop across folds.
  - Bind fold-level assignments.
  - Replace `quasi_treatment_group` for never-treated rows only.
- [ ] Update `CreateDataPanel()` so covariate-conditional quasi dates are assigned
  after folds are created and before first differences are finalized.
- [ ] Re-run first-difference outcome construction after quasi-treatment dates
  change, because the normalization period may change.
- [ ] Preserve the current behavior unless `covariate_quasi_dates = TRUE`.

## Package Structure and Exports

- [ ] Turn `source/lib/event_study_forest` into a proper R package or package-like
  module once the public API stabilizes.
- [ ] Use this package structure as the target:

```text
R/
  event_study_forest.R   # Constructor and pipeline methods
  predict.R              # predict.EventStudyForestPipeline, coef.EventStudyForestPipeline
  diagnostics.R          # summary, plot, variable_importance, fold_stability
  heterogeneity.R        # heterogeneity_split
  internals.R            # cross-fitting, doubly robust scoring, aggregation
```

- [ ] Export only the constructor, S3 methods, and user-facing diagnostics.
- [ ] Keep cross-fitting helpers, doubly robust scoring, aggregation internals,
  and low-level matrix builders unexported.
- [ ] Add package documentation only after the object lifecycle and method names
  are stable.

## Documentation, Leakage Notes, and Code Quality

- [ ] Document that future-period treatment-effect predictions use training data
  containing future periods. This is appropriate for post-hoc descriptive
  heterogeneity analysis, not for causal forecasting.
- [ ] Add function-level documentation for each public method once signatures are
  stable.
- [ ] Use documentation work as a code-quality pass: each documented argument
  should correspond to a clear validation check and a clear place in the object.
- [ ] Record open methodology questions for Jesse before treating them as settled:
  - whether the time fixed-effect residualization path leaks information;
  - whether current PCA feature construction creates meaningful leakage;
  - whether covariate-conditional quasi-treatment assignment is required for the
    current empirical claim or can remain an optional robustness path.

## Analysis-Script Migration

- [ ] Review `source/analysis/event_study_forest` after the library API is
  cleaned.
- [ ] Keep analysis scripts thin: they should load config, choose samples and
  feature sets, call the library API, and write outputs.
- [ ] Move reusable plotting, heterogeneity splitting, fold comparison, and
  variable-importance logic out of analysis scripts and into package methods
  when those outputs are part of the standard forest workflow.
- [ ] Update SCons dependencies whenever helpers move between analysis-local and
  library-level files.
