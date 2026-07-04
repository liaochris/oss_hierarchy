# Code review — model_event_study resilience comparison

Scope: `source/analysis/model_event_study/estimate_model_event_study.R`, `source/analysis/model_event_study/SConscript` (as-is).

## source/analysis/model_event_study

### Issues
- None blocking.

### Notes
- **Goal met & verified.** Produces the 4-series resilience plots (`resilience/{covar_type}/{norm}/{outcome}.png`, 24 files = 3 samples × 2 covar_types × 2 norms × 2 outcomes) and `resilience_band_estimates.csv`. High vs low subsets confirmed distinct (`high == low` → False), and the subset filter shadowing bug was fixed with `.env$resilience_group`.
- **Naming/structure clean.** `Main()` first, helpers in call order; self-documenting names throughout (`resilience_group`, `group_repos`, `bands_by_resilience_group`, `present_groups`, `n_treated_by_subsample`); CamelCase functions / snake_case locals / CAPS globals; no generic `df`/`tmp`; no dead code; minimal comments. `ComputeSampleBands` is factored and shared by the full-sample and resilience paths (no duplication of the band pipeline).
- **SaveData correct.** Two distinct outputs (`band_estimates`, `resilience_band_estimates`), each with its own `.log`, unique keys (`...,split_value,event_time`); not a single-file sequential loop, so `append=FALSE` default is right.
- **Minor duplication (optional).** The model median `CoefMatrix(...)` + `model_bounds` (cbind p2.5/p97.5 + rownames) is built in both `PlotBand` (244–247) and `BuildResilienceGroupMatrices` (304–309). Could extract a small `BandModelMatrices(event_study_band)` returning `list(matrix, bounds)`. ~2 lines; not required.
- **Performance note (inherent).** `DrawPointEstimates` re-fits 500 `feols` per (norm × covar_type × outcome × group × subsample). The draws are identical across covar_types — only the repo subset differs — so re-fitting is unavoidable given different subsets. Built fine; no easy win.
- **Accounting caveat (documented in HANDOFF.md).** The high/low split comes from the period-based forest (`att_doubly_robust_group`) while the plotted outcome is opened-cohort. Model-vs-actual is internally consistent (both opened-cohort); the grouping/outcome accounting mismatch is flagged for later alignment.

### SConscript
- Correct: `covar_types` from `feature_sets.run`; forest att-group parquets added as deps (all present); resilience targets + CSV added; `draw_sources` point at the `drive/output` datastore. `env.R` lists all deps.

## Summary
- Outputs match the stated goal; high/low subsetting verified correct after the `.env$` fix.
- No must-fix issues. Two optional improvements: extract the small band→model-matrix duplication; (already-documented) resilience-split vs opened-cohort accounting mismatch.
- No naming/convention violations.
