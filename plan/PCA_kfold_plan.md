# Plan: Out-of-Sample PCA via Cross-Fold Estimation

## Context

Previously, `ComputeRepoPCScores` in `prepare_panel.py` fit PCA on all repos in the estimation sample at once. Because the forest uses K-fold cross-fitting (K=10), repos in held-out fold k had their PC scores constructed using a PCA rotation that included their own data — a mild form of leakage. The fix is to assign folds in Python and compute PC scores out-of-sample: for each fold k, fit PCA on the 9 training folds, then apply to fold k. This mirrors how the forest itself handles cross-fitting.

## What changes

### 1. `source/derived/analysis_panel/prepare_panel.py`

**Add `AssignFolds(repo_names, n_folds, seed)` function** — assigns a fold (1..K) to each unique repo. Logic: sort repos alphabetically, assign fold as `(rank % n_folds) + 1`. Outcome-agnostic (unlike the current R version which shifts offset by outcome name — that's fine since PCA doesn't use outcomes). Read `n_folds` and `seed` from `LoadAnalysisParameters()`, which already loads `analysis_parameters.json`.

**Replace `ComputeRepoPCScores(panel, pc_groups_cfg, rolling_period)` with a fold-aware version:**
```
fold_map = AssignFolds(repos)
for k in 1..n_folds:
    train_repos = repos where fold != k
    hold_repos  = repos where fold == k
    fit PCA on train_repos baseline means
    apply PCA to hold_repos baseline means (transform only)
    store scores for hold_repos
assemble full repo_pc_scores from all folds
```

The inner PCA logic per group (StandardScaler + PCA(n_components=1) + sign_flip) stays exactly the same. The scaler/PCA is fit on `train_repos` rows and applied to `hold_repos` via `.transform()`, not `.fit_transform()`. Metadata (loadings, variance explained) is taken from a single full-sample fit — used only for autofill tables, not for the scores.

**Save fold assignment in panel output** — add `fold` column to `panel.parquet` and `panel_with_pc_scores.parquet` so R can read it directly.

### 2. `source/lib/event_study_forest/forest_helpers.R`

**Modify `CreateDataPanel`** — remove the `left_join(AssignOutcomeFolds(...))` call at line 85 and replace with a join on the `fold` column already present in the panel:
```r
df_data %>% left_join(panel %>% select(repo_name, fold) %>% distinct(), by = "repo_name")
```
This requires `panel` (the pre-processed panel with `fold` column) to be passed into `CreateDataPanel`. Update the function signature accordingly; `n_folds` and `seed` args become unused.

**Remove `AssignOutcomeFolds`** — now unused.

### 3. `source/analysis/event_study_forest/fit_event_study_forest.R`

`CreateDataPanel` is called at line 49–50. `panel` is already loaded via `LoadPreparedSample(...)` — so it will contain the `fold` column from the updated parquet. Caller signature may need minor adjustment to pass `panel` if it is not already accessible in `CreateDataPanel`'s scope.

## Critical files

- [source/derived/analysis_panel/prepare_panel.py](source/derived/analysis_panel/prepare_panel.py) — main changes
- [source/lib/event_study_forest/forest_helpers.R](source/lib/event_study_forest/forest_helpers.R) — `CreateDataPanel` + remove `AssignOutcomeFolds`
- [source/analysis/event_study_forest/fit_event_study_forest.R](source/analysis/event_study_forest/fit_event_study_forest.R) — caller, minimal change

## Constants

- `n_folds = 10`, `seed = 420` — from `source/lib/config/analysis_parameters.json`, already loaded by `LoadAnalysisParameters()` in Python.

## Verification

1. Run `prepare_panel.py` and confirm `panel.parquet` has a `fold` column (1–10, ~equal distribution).
2. Confirm `panel_with_pc_scores.parquet` PC scores differ slightly from the old in-sample version (expected due to cross-fitting).
3. Run `fit_event_study_forest.R` for one combo and confirm `df_data$fold` is populated correctly.
4. Check fold balance: `table(df_data$fold)`.
