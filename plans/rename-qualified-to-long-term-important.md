# Rename "qualified important member" → "long-term important member"

> Execution spec for HANDOFF.md item #6. Decisions below are settled with the user; this is ready to implement in its own issue.

## Context
The codebase uses the word **"qualified"** for the concept of an important member who has been important for several consecutive periods — i.e. a *long-term* important member. This is confusing (it reads like a pass/fail gate rather than a duration criterion). We are renaming the concept to **"long-term important member"** across **both code and the paper**, so the writing and the persisted data stay consistent.

Two distinct code surfaces both use the word "qualified" and both map to "long-term":
1. **The count** `num_important_qualified` = number of long-term important members present ($q_{i,t}$).
2. **The sample-selection config** `qualified_sample` / `qualified_samples` = selection on the exact count at event time (values `exact1`, `exact2`, `exact_1_2`).

This is a **full concept rename** covering both surfaces plus companion identifiers, comments, and paper prose.

## Decisions (settled)
- Count column: `num_important_qualified` → **`num_important_long_term`**.
- Config term: `qualified_sample` / `qualified_samples` → **`long_term_imp_sample` / `long_term_imp_samples`** (the `imp` ties it to the long-term *importance* definition; `_sample` suffix parallels the old term).
- **Value strings and folder names are UNCHANGED**: `exact1`, `exact2`, `exact_1_2` stay; `AGGREGATED_SAMPLES` stays. No directory path contains "qualified", so **no folders are created or removed**. (Corrects the earlier HANDOFF note that claimed the config term was "embedded in 40+ output directory names" — folder names are built from *values* only.)
- **Data sync**: make all code + prose edits only; then **run the SCons regeneration** afterward (see Execution order).

## Scope: what changes

### Group A — the count concept ($q_{i,t}$)
Rename every occurrence (identifiers + comments/docstrings):
- `num_important_qualified` → `num_important_long_term`
- `important_qualified_actors` → `important_long_term_actors`
- `TrackQualified` → `TrackLongTerm` (function name + call site)
- local vars in the tracker: `qualified_by_period` → `long_term_by_period`, `qualified_mask` → `long_term_mask`, `qualified` → `long_term`
- constant `MAX_QUALIFIED` → `MAX_LONG_TERM`
- output column `num_important_qualified_degree_top3_at_treatment` → `num_important_long_term_degree_top3_at_treatment`
- prose in comments/docstrings: "qualified important member(s)" → "long-term important member(s)"

Representative files (pattern repeats; not every line listed):
- `source/derived/graph_structure/calculate_importance.py` — defines the column + `TrackLongTerm`, JSON/list-col handling (lines ~42, 172, 180–181, 270)
- `source/derived/org_outcomes_practices/org_panel.py` — fill-zero + list-col serialization (lines ~80, 157, 165)
- `source/derived/analysis_panel/panel_filters.py` — the `.isin` filter on the count (line ~22)
- `source/derived/model_prediction/compute_mean_reversion.py` — `META_COLUMNS`, `MAX_LONG_TERM`, FE-Poisson formula string `i(num_important_long_term, ref=0)`, grouping keys, coef parsing (lines ~11–96)
- `source/lib/model/staged_count_model.py` — `MeanReversionAdj` param + docstring
- `source/lib/python/config_loaders.py` — `LoadMeanReversionAdj` dict key read + comments (lines ~49–58)
- `source/analysis/model_prediction/fit_model.py`, `source/analysis/model_prediction/predict_model.py` — `num_important_qualified_by_repo` / `_by_period` locals + lookups
- `source/analysis/summ_stats/create_project_summary.py` — column select + rename to `..._degree_top3_at_treatment`
- `issue/compare_lambda_baseline.py` — diagnostic script that duplicates `LoadMeanReversionAdj` / `MeanReversionAdj`; update for consistency (flagged: diagnostic, not in the SCons DAG)

### Group B — the sample-selection config
- `qualified_sample` → `long_term_imp_sample`  (loop vars, params, path f-strings, `combo["qualified_sample"]` keys)
- `qualified_samples` → `long_term_imp_samples`  (config key + locals)
- `QUALIFIED_SAMPLES` → `LONG_TERM_IMP_SAMPLES`  (SConscript config-loader globals)
- `primary_qualified_sample` → `primary_long_term_imp_sample`  (`paper_settings.json` key + readers)
- `CL_QUALIFIED_SAMPLE` → `CL_LONG_TERM_IMP_SAMPLE`  (CLI env var passed to Python and R)
- `ParseQualifiedSample` → `ParseLongTermImpSample`, `FilterQualifiedSample` → `FilterLongTermImpSample`

Config files:
- `source/lib/config/pipeline_inputs.json` — key `qualified_samples` → `long_term_imp_samples` (values `exact*` unchanged)
- `source/lib/config/paper_settings.json` — key `primary_qualified_sample` → `primary_long_term_imp_sample` (value `exact1` unchanged)

Code + SConscript files (~20 files, ~99 occurrences). SConscripts: `derived/analysis_panel`, `derived/model_prediction`, `analysis/model_prediction`, `analysis/event_study`, `analysis/event_study_forest`, `analysis/analyze_forest`, `analysis/summ_stats`, `figures`, `paper`, `tables`, `talk`. Python: `prepare_panel.py`, `panel_filters.py`, `build_event_time_member_panel.py`, `fit_model.py`, `predict_model.py`, `evaluate_predictions.py`, `create_project_summary.py`. Plus the **R** files under `event_study` / `event_study_forest` that read `CL_QUALIFIED_SAMPLE`.

### Group C — paper prose
- `source/paper/main.tex` — 6 occurrences of "qualified important members" (lines ~934, 937, 973, 986, 992, 994) → "long-term important members". (Prose lives in `main.tex`, not `model.tex` as the old HANDOFF note said.)
- `source/tables/mean_reversion.tex` — caption (line ~3) "qualified important members" → "long-term important members". Check the table header/column label too.

## What does NOT change
- Value strings `exact1`, `exact2`, `exact_1_2`, `exact3`; `AGGREGATED_SAMPLES` mapping.
- All on-disk directory names (built from values, never from the config term).
- Any unrelated use of "qualified" (none found for other concepts — confirm via final grep).

## Execution order
1. Group A code edits (start at the definition in `calculate_importance.py`, then follow the column downstream).
2. Group B code + JSON + SConscript + R edits.
3. Group C prose edits.
4. Final grep sweep (below) to confirm no stray "qualified" for this concept remains.
5. Run SCons to bring persisted parquet/CSV columns into sync:
   ```
   source activate org_resilience && export PYTHONPATH=.
   scons output/derived/hashes/important_members.txt
   scons                       # regenerates org_panel → analysis_panel → model_prediction → analysis downstream
   ```
   (Graph_structure recompute is the expensive step; everything downstream keys off it.)

## Verification
- **Static (after edits):**
  - `grep -rin "qualified" source/` returns **no** hits referring to this concept (expect zero; any remaining hit must be intentional and explained).
  - `python -c "import ast, pathlib; [ast.parse(p.read_text()) for p in pathlib.Path('source').rglob('*.py')]"` — all edited Python files parse.
  - Confirm `ParseLongTermImpSample("exact_1_2") == {1, 2}` still holds (logic unchanged, only the name).
- **End-to-end (after SCons run):**
  - New `panel.parquet` files contain column `num_important_long_term` (and not `num_important_qualified`).
  - `mean_reversion_estimates.csv` has column `num_important_long_term`; `LoadMeanReversionAdj` loads it; model_prediction outputs regenerate without KeyError.
  - Paper builds (`scons` for `source/paper`) with the new prose.
- **Code review:** run `/code-review` on the diff (naming, helper placement, comment accuracy).
