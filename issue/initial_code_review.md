# Code Review — model_prediction (source)

Scope: `source/derived/model_prediction/` and `source/analysis/model_prediction/`, examined
as-is on branch `31-org-model` (overlaid from `31-model`, uncommitted). Goal: build event-time
member-stage count panels → fit count distributions (Poisson / Negative Binomial) → estimate
member stage-completion probabilities → Monte-Carlo simulate outcome distributions → compute Q
(variance-ratio) and Z (standardized residual) goodness-of-fit metrics, compared treated vs
control and pre vs post.

---

## source/derived/model_prediction

### Issues
- **[build_event_time_member_panel.py:22-24] Config-global naming (user mandate + convention).** `_g = LoadGlobalSettings()`, `_cfg = LoadPipelineInputs()`, `_ap = LoadAnalysisParameters()` → must be CAPITALIZED globals: `GLOBAL_SETTINGS`, `CONFIG`, `PARAMETERS`.
@CLAUDE: Please fix. 
- **[SConscript] Missing input dependency: action data.** Script reads `drive/output/derived/action_data/repo_actions/{repo}.parquet` (`INDIR_ACTIONS`, line 32/79) but the SConscript `source=` (script_deps + panel_sources) does not list it. SCons won't rebuild when action data changes.
@CLAUDE: Please fix. 
- **[SConscript] Target / output mismatch.** `targets` lists only `…/{ref}.parquet` (one file per (variant,combo) for the *reference* repo via `_g["reference_github_repo"]`), but `ProcessRepo` writes `…/{safe_name}.parquet` for **every** repo in the panel, plus per-repo `.log` files under `output/derived/model_prediction/...` that aren't declared as targets at all.
@CLAUDE: I think now, I want to replace all reference repo things in scons with the hashing approach used in other source/derived directories. 
- **[SConscript] One `env.Python` with internal loop.** A single `env.Python(targets, …)` runs `Main()`, which loops `Combos()` and `joblib.Parallel` over repos. Convention prefers one `env.Python` per entity so SCons tracks/parallelizes; here parallelism is hidden inside joblib and SCons sees one opaque build step.
@CLAUDE: fine to leave as is. 

### Notes
- Header comment explaining the 3 variants (observed / same_period / opened_cohort) is genuinely useful — keep.
- `SaveData` usage is correct: parallel per-repo saves, each with its own `.log`, `append=False` (default).
- Paths all use `pathlib.Path` with `INDIR_*`/`OUTDIR`/`LOG_OUTDIR` naming — good.

---

## source/analysis/model_prediction

### Issues
- **[fit_model.py:13,26 / predict_model.py:20-22,43 / evaluate_predictions.py:11-12,42] Config-global naming (user mandate + convention).** Rename everywhere:
  - `_ap` / `LoadAnalysisParameters()` → `PARAMETERS`
  - `_model_prediction_config` / `LoadModelPredictionConfig()` → `MODEL_PREDICTION_CONFIG`
  - `_cfg` and the local `cfg` inside `Main()` / `LoadPipelineInputs()` → `CONFIG`
  - `_g` / `LoadGlobalSettings()` → `GLOBAL_SETTINGS`
  Also inconsistent: `LoadPipelineInputs()` is a module-level `_cfg` in the derived script but a function-local `cfg` inside `Main()` in fit/predict/evaluate. Make it a module-level `CONFIG` everywhere.
@CLAUDE: please fix 
- **[predict_model.py:149] Reproducibility bug (HIGH).** `rng = np.random.default_rng(abs(hash(repo_name)) % (2**31))` uses Python's built-in `hash()`, which is **salted per process** (PYTHONHASHSEED). Seeds therefore differ across runs and across joblib workers → simulation results are **not reproducible**. Use a stable hash (e.g. `int(hashlib.md5(repo_name.encode()).hexdigest()[:8], 16)`).
@CLAUDE: Make reproducible. 
- **[model_fitting_utils.py] Dead code (verified zero references).** Remove:
  - `FitDistribution` (lines 34-92) — docstring "kept for backward compat"; never called.
  - `FitZIPParams` (327) and `LogLikZIP` (365) — only used by the dead `FitDistribution`.
  - `ComputeModelVariance` (307), `ComputeBinomialProportionStd` (317) — "retained for reference"; never called.
  - `_safe_sub` (297) — never called.
  Removing these also drops the `ZeroInflatedPoisson` import and the entire "zip" code path.
@CLAUDE: do we not use zero inflated poisson as an option for latent flow anymore??? 
- **[model_fitting_utils.py:248] `StageDecomposition` unused parameter `obs_mr`.** Passed by callers but never used in the body.
@CLAUDE: what is obs_mr??? 
- **[predict_model.py:267] Pointless indirection.** `_ComputePiFromDf(df)` just returns `_ComputePi(df)`; call `_ComputePi` directly and delete the alias.
@CLAUDE: ok. Also Pi is unclear, code should never be named based on math notation
- **[fit_model.py:18 / predict_model.py:28 / evaluate_predictions.py: n/a] Hardcoded `ROLLING_LABEL = "rolling5"`.** The derived script derives this from config (`f"rolling{CONFIG['rolling_periods']['run'][0]}"`); the analysis scripts hardcode it. Derive from `CONFIG` to stay consistent and config-driven.
@CLAUDE: fix
- **[predict_model.py:35 / model_fitting_utils.py:187] `K` overloaded + `N_CORES` vs `N_JOBS`.** `K = 1000` (Monte-Carlo draws) collides in meaning with `K = max_event_time` in the derived script; rename the draw count to `N_DRAWS`. `N_CORES` (predict) vs `N_JOBS` (derived) for the same `n_jobs` setting — pick one.
@CLAUDE: USE N_JOBS, and N_MODEL_DRAWS
- **[analysis SConscript] Missing runtime deps — predict step.** `predict_model` imports `LoadGlobalSettings` and `LoadAnalysisParameters`, so it reads `global_settings.json` and `analysis_parameters.json`, but the predict `env.Python` source list includes neither (only `model_prediction.json` + `pipeline_inputs.json` via `shared_deps`).
@CLAUDE: update. 
- **[analysis SConscript] Missing runtime dep — evaluate step.** `evaluate_predictions` imports from `source.lib.python.config_loaders`, but the evaluate `env.Python` source list omits `#source/lib/python/config_loaders.py`.
@CLAUDE: update
- **[analysis SConscript] One `env.Python` per stage with internal loops** (same pattern as derived) — fit/predict/evaluate each run all combos inside `Main()`.
@CLAUDE: update
- **[evaluate_predictions.py:57-313] `RunCombination` is ~256 lines** with `# ---- … ----` section headers — a signal to extract functions (e.g. `MakeQPanels`, `MakeZPanels`, `MakeIndividualPlots`). `predict_model.ProcessRepo` (~120 lines) is a milder case.
@CLAUDE: What's the problem? Are you saying we don't need comments? Also what is Q, Z (rhetorical)? Use words
- **[evaluate_predictions.py:339-340] Dead vars** `z_key_cols` / `merge_cols` computed but never used in `_MakeDecompZPanel`.
@CLAUDE: same comment about "z" cols.... also drop
- **Helper naming inconsistency.** Module-level helpers mix styles: `_MakePanel`, `_AllQ`, `_ComputePi` (leading underscore) vs `PlotPanelSubplot`, `PlotErrorDistribution` (none). Pick one convention.
@CLAUDE: No underscore. 

### Notes
- **Helper placement (CLAUDE.md §6):** `DrawCounts`, `ComputeQ`, `ComputeZ`, `StageDecomposition` in `model_fitting_utils.py` are imported **only** by `predict_model.py` (not `fit_model.py`). Per the ≥2-callers rule they should move into `predict_model.py` or carry a `# SINGLE-USE EXCEPTION` comment. (`FitDistributionForced` and `FitMemberProbabilities` are genuinely shared by fit + predict — those belong in the helper.)
@CLAUDE: ok, don't put them in a helper.. 
- **[predict_model.py:78-80] Dependency beyond the 3 identity columns:** predict reads `dropouts_actors` (JSON string) and `num_dropouts` from `panel.parquet`. Confirm `main`'s `prepare_panel` produces these columns (they're outside the prepare_panel diff, so expected present — worth a build check).
@CLAUDE: please confirm. 
- **Economics sanity:** `_ComputePi` sets `pi_o = df_probs["prob_open"].sum()` (a sum of per-member probabilities, can exceed 1) while `pi_r = 1 - Π(1 - prob_review)` (a proper probability). `DrawCounts` then clips `pi_o` to [0,1]. Confirm this aggregation/clipping is intended rather than a normalization issue.
@CLAUDE: Can you add an assert in the code that `df_probs["prob_open"].sum()` isalways between 0 and 1?? 
- **[fit/predict/evaluate] `QUALIFIED_SAMPLES = {"exact1","exact2","exact_1_2"}`** is a hardcoded subset filter — confirm intended vs config-driven.
@CLAUDE: can u import globals instead of having me define qualified samples. 
- `SaveData` usage is correct throughout (distinct files per combo, `log_file=` set; figures use `fig.savefig`, not `SaveData` — correct).

---

## Summary

**Goal alignment:** The implementation coherently realizes the stated pipeline; outputs (fitted params, member probabilities, predictions, evaluation panels) line up with the SConscript targets.

**Top issues to address (in priority order):**
1. **`hash()` seeding** in `predict_model.py` → non-reproducible simulations. Switch to a stable hash. (Correctness/repro)
2. **Config-global naming** (your mandate): `PARAMETERS` / `MODEL_PREDICTION_CONFIG` / `CONFIG` / `GLOBAL_SETTINGS`, module-level and consistent across all 4 scripts.
3. **Dead code in `model_fitting_utils.py`** — remove `FitDistribution`, `FitZIPParams`, `LogLikZIP`, `ComputeModelVariance`, `ComputeBinomialProportionStd`, `_safe_sub` (and the ZIP path).
4. **SConscript dependency gaps** — predict: add `global_settings.json` + `analysis_parameters.json`; evaluate: add `config_loaders.py`; derived: declare the action-data input and fix the `{ref}`-only target list (and `.log` targets).
5. **Hardcoded `ROLLING_LABEL="rolling5"`** and `QUALIFIED_SAMPLES` subset → derive/justify.

**Economics-sanity flags:** `pi_o` can exceed 1 and is silently clipped; verify `dropouts_actors`/`num_dropouts` exist in `main`'s panel.

**Systemic convention violations:** (a) config-global naming wrong across all 4 scripts; (b) helper-function underscore prefix used inconsistently; (c) "one `env.Python` per entity" not followed (single call + internal joblib loop) in both SConscripts.
