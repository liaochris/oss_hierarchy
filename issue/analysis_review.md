# Code Review: source/analysis/

**Goal:** Estimate event studies of key-member departures on OSS org outcomes, fit/predict a Generalized Random Forest Event Study to model heterogeneous resilience, and produce supplementary analyses (forest characteristics, PC combination plots, diagnostics).

---

## source/analysis/SConscript

### Issues
- **[SConscript:8] Stale reference to deleted directory.** `SConscript('model_prediction/SConscript')` references a directory deleted from the repo (confirmed via git status). This crashes the build.

---

## source/analysis/data_prep/

### Issues
- **[prepare_panel.py:41–42] Debug print statements in production code.**
  ```python
  print([c for c in panel.columns if "issue" in c or "release" in c])
  print(active_outcomes)
  ```
  Delete both.

- **[SConscript] Single `env.Python()` call for all combinations.** All importance_type × rolling_period × sample × control_group combinations are computed in a single Python process with an internal loop. Per conventions, parallelized work should use one `env.Python()` per entity so SCons can run them in parallel. This will slow the pipeline materially as sample counts grow.

- **[Architecture] data_prep belongs in `source/derived/`, not `source/analysis/`.** The script performs panel construction, sample filtering, and PCA computation — these are data-preparation steps, not analysis. Only estimation scripts belong in `source/analysis/`. The user flagged this and it should be moved and renamed to reflect its responsibility (e.g., `source/derived/analysis_panel/`).

### Notes
- `INDIR`, `OUTDIR`, `INDIR_LIB` are correctly named module-level Path constants. ✓
- `SaveData` used correctly for all CSV/parquet outputs with log files. ✓
- All helper functions are well-named and semantically focused. ✓

---

## source/analysis/summ_stats/

### Issues
- **[SConscript] Broken regex `r"exact\\d+"` in raw string.** In a raw string, `\\d` is two characters (backslash + d), not a regex digit class. `re.fullmatch(r"exact\\d+", "exact1")` returns `None`. Replace the entire regex approach with a config-driven filter:
  ```python
  exact_samples = sorted(
      [s for s in pipeline_cfg["qualified_samples"]["run"] if "_" not in s],
      key=lambda s: int(s.removeprefix("exact"))
  )
  ```
  Also remove the now-unused `import re` from the SConscript.
  - @CL: THe solution here is to not use regex for determining these parameters like exact, etc. I define which ones I'm using in source/lib/project_config.json
  - also @project_config.json needs to be less lines. It's way to inefficient right now. 

- **[create_project_summary.py] All `INDIR_*`/`OUTDIR` constants defined inside `Main()`.** These should be module-level constants per conventions (lines 17–27). Move them to module scope with `CAPITALIZED` names.

- The script itself uses `re.fullmatch(r"exact\d+", sample)` correctly (single backslash) in `BuildAnalysisSpec` and `ExportSummaryTable` — only the SConscript has the bug. 
  - @ CL: PLEASE DON"T USE REGEX FOR EXACT MATCHING given that WE KNOW WHAT WE HAVE TO ITERATE THROUGH. 

---

## source/analysis/event_study/

### Issues
- **[SConscript] Single `env.R()` call for all combinations.** All samples/outcomes/PC groups are run in one R process with internal loops. Same parallelization concern as data_prep.
 
---

## source/analysis/event_study_forest/

### Issues
- **[predict_forest_resilience.R] Not referenced in any SConscript — dead code.** The file exists and contains a complete pipeline but is not reachable from the SCons build system. Either wire it into `event_study_forest/SConscript` or delete it. Without a SConscript entry, its outputs are never tracked and users would need to run it manually.

- **[predict_forest_resilience.R:200] `Main()` placed after all helper functions.** Convention requires `Main()` first, helpers following in call order. Move `Main()` to line 1 (after library/source calls).

- **[fit_cate.R:29] Hardcoded magic string `"prop_tests_passed"`.** Used to filter out a specific practice mode:
  ```r
  practice_modes <- practice_modes[
    !sapply(practice_modes, function(x) x$continuous_covariate == "prop_tests_passed")
  ]
  ```
  This should be driven by a config key or at minimum have a comment explaining why this variable is excluded.
  @CL: Can we just remove this from all data construction? No comment needed - just remove it everywhere starting in source/scrape or source/derived. 

- **[SConscript] Single `env.R()` call for all non-aggregated combinations.** Same parallelization concern.

---

## source/analysis/analyze_forest/

### Issues
- **[helpers.R:128–129, 146–147] `write_csv()` used instead of `SaveData`.** `CreatePCScoreComboTables()` writes four CSV files using `write_csv()` directly, bypassing SaveData and producing no log files:
  ```r
  write_csv(bind_rows(combo_rows_all) ..., file.path(outdir_ds, "pc_score_combo_k2_table.csv"))
  write_csv(bind_rows(combo_rows_high) ..., file.path(outdir_ds, "pc_score_combo_k2_high_table.csv"))
  ```
  Replace with `SaveData(df, key_cols, path, log_path)` calls. The key column for the `_table.csv` files is `c("pc_score_subset", "pattern")`; for the `_high_table.csv` files it is `"pc_score_subset"`.

- **[helpers.R] `BuildLatexAndSave()` and `ExportVariableImportanceTex()` write `.tex` files directly with `writeLines()`.** Per Task 2 of the refactor plan, these should be converted to the Tablefill workflow (write `.txt` data files; use templates in `source/tables/`; fill via `env.Tablefill`). Tracked separately.


## Summary

**Goal alignment:** The code achieves the stated goal (event studies, forest fit, supplementary analyses). Outputs match what the paper describes. No economics-sanity flags (correct outcome directions, sensible aggregation logic).

**Top issues to address:**

1. **Build crash:** `source/analysis/SConscript:8` — remove stale `model_prediction/SConscript` reference.
2. **Silent bug:** `source/analysis/summ_stats/SConscript` — broken regex filter produces empty `exact_samples` list, likely causing all panel paths to be missing from the SCons source list; replace with `"_" not in s` filter.
3. **SaveData violation:** `source/analysis/analyze_forest/helpers.R:128–147` — `write_csv()` calls in `CreatePCScoreComboTables()` must be replaced with `SaveData`.
4. **Dead code:** `source/analysis/event_study_forest/predict_forest_resilience.R` — not in any SConscript; wire it in or delete it.
5. **Debug prints:** `source/analysis/data_prep/prepare_panel.py:41–42` — remove.

**Systemic convention violations:**
- All three main SConscripts (`data_prep`, `event_study`, `event_study_forest`) use a single `env.Python/R()` call for all parameter combinations instead of one call per entity. This prevents parallel execution and should be addressed as the pipeline scales.
- `create_project_summary.py` defines path constants inside `Main()` rather than at module scope.

**Architecture note:** `source/analysis/data_prep/` performs data preparation (panel construction, sample filtering, PCA) and should be moved to `source/derived/` and renamed to reflect its role (e.g., `analysis_panel/`).
