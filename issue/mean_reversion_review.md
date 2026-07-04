# Code review — mean-reversion calculation & diagnostics

Scope (examined as-is, no diff): `source/derived/model_prediction/compute_mean_reversion.py`,
`source/analysis/model_prediction/plot_mean_reversion_diagnostics.py`, and their SConscripts.
Env: org_resilience.

## source/derived/model_prediction/compute_mean_reversion.py

### Issues
- None blocking.

### Notes
- **Goal met.** Computes per-control-org post/pre per-period rate ratios and the three summary
  estimators (`latent_ratio_poisson/_geom_mean/_median`) consumed by predict_model.py, and now also
  saves the per-org bridge `org_reversion_stats.parquet`. Verified the bridge has unique keys
  (1392 rows, keys `variant,importance_type,qualified_sample,control_group,repo_name`), single
  variant `opened_cohort`, 0 NaN opened ratios, 2 NaN merged ratios (orgs with zero pre-period
  merges — handled downstream by the plot's `notna()` filter).
- **Two intentional post windows.** `latent_ratio_median` uses Post = event_time ≥ 0 (`opened_ratio`),
  while `latent_ratio_geom_mean` uses event_time ≥ 1 (`pre_post_ratio`, dropping post-period 0). This
  matches the header comment and the earlier modeling decision — correct, but subtle.
- **Minor — `pre_post_ratio` name is vestigial.** It is specifically the post(≥1)/pre mean ratio that
  feeds the geometric mean, distinct from `opened_ratio` (post≥0). The generic name doesn't convey the
  dropped-period-0 window; a name like `geom_input_ratio` would self-document. (not changed — touches
  the geom-mean path; flagging for your call)
- **Minor — opened ratio duplicates `PrePostRateRatio`.** `opened_ratio` is computed inline
  (line 144) with the same per-period rate-ratio formula as the `PrePostRateRatio` helper (used for
  merged). The inline version intentionally skips the None branch (opened pre/post are already
  validated at line 129), so it isn't a drop-in call, but the formula is repeated. Low priority.
- Style clean: `Main()` first, helpers in call order; CamelCase functions / snake_case locals / CAPS
  globals; `pathlib.Path` paths; no type hints; no unused imports; minimal comments.
- SaveData: two distinct outputs, each with its own `.log`, unique keys, `append=False` default —
  correct (not a sequential loop of one logical output).

## source/analysis/model_prediction/plot_mean_reversion_diagnostics.py

### Issues
- None blocking.

### Notes
- **Goal met.** Reads the bridge and produces per-cell binscatter diagnostics (raw scatter + binned
  dots + CI + `line=(3,3)` binscatter trend, symlog axes, median and ratio=1 reference lines). Built
  green; 3 PNGs (one per qualified_sample).
- **Latent — grouping omits `variant`.** `Main()` groups by `(importance_type, qualified_sample,
  control_group)` and the filename omits variant, though the bridge is keyed on variant. Fine for the
  current single variant (`opened_cohort`); if `variants.run` grows >1, orgs would silently pool
  across variants and filenames would collide. The analysis SConscript bakes in the same assumption
  (its targets use `sample_combinations`, no variant). Add variant to the groupby + filename + targets
  if variants expand.
- `except Exception` in `DrawBinscatter` is broad but justified — binsreg can fail on degenerate
  cells; it prints and continues rather than failing the build. Acceptable for a diagnostic.
- `np.median(ratio)` computed twice (axhline + label) — trivial.
- Style clean: `Main()` first; CamelCase / snake_case / CAPS; `Path` paths; no unused imports;
  no SaveData needed (PNG-only). Correctly lives in `source/analysis` (plotting), reading a
  `source/derived` data bridge — clean derived/analysis separation.

## SConscripts

### Issues
- **[source/analysis/model_prediction/SConscript:87] Fixed.** `outcomes` still listed `merged_total`,
  an unpropagated leftover from the `merged_total → merged` rename (evaluate_predictions.py emits
  `merged`). Harmless today (the list only drives the `individual` figures, gated on
  `evaluation_figures` which runs `['panels']`), but a latent build break if `individual` is enabled.
  Changed to `merged`.

### Notes
- Derived SConscript: `org_reversion_stats.parquet`/`.log` added as targets; the old diagnostics PNG
  targets removed (plotting moved to analysis). Correct.
- Analysis SConscript: new `env.Python` for the plot with `source = [plot script, bridge parquet]`,
  `targets = 3 diagnostic PNGs`. Deps complete (plot script imports only third-party packages).

## Summary
- **Outputs match the stated goal**: per-org ratios + summary estimators + bridge table in derived;
  binscatter diagnostics in analysis. Build green.
- **Top items**:
  1. Fixed the stale `merged_total` in the analysis SConscript (latent build break).
  2. Latent: plot/SConscript assume a single variant — revisit if `variants.run` grows.
  3. Optional naming: `pre_post_ratio` → something conveying the dropped-period-0 / geom-mean role.
- No economics-sanity flags. No systemic convention violations; naming/structure are consistent.
</content>
