# Rewrite the model_prediction pipeline + restructure Section 3 of model.tex

## Context

`source/paper/model.tex` was substantially rewritten and now describes:

1. a baseline stage-decomposition model;
2. a measurement-and-empirical-implementation section that fits objects from treated pre-period data and uses never-treated controls to choose how those objects are fit;
3. diagnostics that separate prediction error, stage-level attribution, and share-level allocation error.

The existing code in `source/derived/model_prediction/` and `source/analysis/model_prediction/` was written against an older draft and now drifts from the paper in three ways:

1. It treats latent-problem aggregation as a fitted object rather than as part of the prediction model itself.
2. Its narrative claims everything is "fit on never-treated," but the intended procedure is: fit treated inputs using treated pre-period data; use never-treated controls only to choose the fitting rule.
3. It does not produce the diagnostics the paper now needs: decomposable share errors, the singleton-plus-other partition diagnostic, known/unknown share-error components, relative count prediction errors, stage-level oracle attribution, and channel decomposition.

The plan below rips out the old code, rebuilds the pipeline with one shared helper library, and rewrites Sections 3.3 and 3.4 of `model.tex`. All fitting metrics, diagnostic metrics, and prediction-error metrics derive from the same three primitives E1, E2, and E3.

## Pause structure

There are now seven pause-and-inspect units. The old Phase 2B and 2C are combined into one joint share-window x partition selection step.

| # | Unit | Outputs at pause | Paper subsection(s) |
|---|---|---|---|
| 0 | Phase 0 - rip out old code | deletions only | none |
| 1 | Phase 1 - derived event_time x contributor panel | `event_time_member_panel/{repo}.parquet` | none |
| 2 | Phase 2A - fit latent problem flow `N` | `latent_n_selection.tex`, `latent_flow_*.png`, partial `recommended_config.json` | Section 3.3.5 |
| 3 | Phase 2B - jointly fit share window and contributor partition | `share_partition_selection.tex`, `share_partition_*.png`, complete `recommended_config.json` | Section 3.3.6 |
| 4 | Phase 3A - count prediction error | `prediction_errors.tex`, `prediction_*.png`, `failure_quantity_errors.png` | Section 3.4.1 |
| 5 | Phase 3B - stage decomposition | `oracle_attribution.tex`, `oracle_attribution_decomposed.tex`, `channel_decomposition.tex`, `oracle_*.png`, `channel_decomposition_*.png` | Section 3.4.2 + Appendix A + Appendix B |
| 6 | Phase 3C - share-level decomposition | `treated_share_decomposition.tex`, `share_decomp_*.png` | Section 3.4.3 |

The pause/checklist material should stay in the implementation plan but be concise. The paper-facing prose should focus on the model, the estimands, and the interpretation of the diagnostics.

---

## Notation

Use this notation throughout both the implementation note and `model.tex`.

- `i`: organization/repository.
- `j`: contributor.
- `j_i^{dep}`: departed contributor in treated organization `i`; pseudo-departed contributor in control organization `i`.
- `k`: event-time period relative to actual or pseudo-departure, `k in {-L,...,-1,0,...,K}`.
- `L = 5`: default pre-period window length. Code is parameterized so candidate share windows can use `ell in {1,...,L}`.
- `K = 5`: post-period evaluation window.
- `s in {o,r,m}`: stage: opened, reviewed, merged.
- `A_{i,j,k}^s`: number of stage-`s` actions taken by contributor `j` in organization `i` at event time `k`.
- `A_{i,k}^s = sum_j A_{i,j,k}^s`.
- `Q_{i,k}^{s,*}`: realized organization-level count at stage `s` and event time `k`.
  - For opens and merges, `Q_{i,k}^{o,*}=A_{i,k}^o` and `Q_{i,k}^{m,*}=A_{i,k}^m`.
  - For reviews, `Q_{i,k}^{r,*}` is the number of unique PRs that received any review action, so generally `Q_{i,k}^{r,*} <= A_{i,k}^r`.
- `hat Q_{i,k}^s`: predicted organization-level count.
- `hat p_{i,j}^{s,ell}`: fitted pre-period contributor share for contributor `j`, stage `s`, share-window candidate `ell`. It satisfies `sum_j hat p_{i,j}^{s,ell}=1` on the fitted contributor universe.
- `p_{i,j,k}^{s,*}`: realized post-period contributor share.
- `hat pi_i^s`, `hat pi_i^{s,(-j_i^{dep})}`: aggregated stage-completion probability with all contributors / with departed contributor removed.
- `hat N_i`: fitted latent problem flow for organization `i`.
- `widehat{obj}`: model prediction.
- `obj^*`: realized truth.

---

## Error primitives used everywhere

All errors are computed per organization `i`. Cross-organization summaries are computed downstream using RMS aggregation.

### E1 - Relative trajectory error

For a generic predicted trajectory `widehat{obj}_{i,k}` and realized target trajectory `obj_{i,k}^*` over an evaluation window `mathcal K`, the default normalized trajectory error is prediction-relative:

$$
\mathcal E_1^{rel}(\widehat{obj}, obj^*; \mathcal K)
=
\left[
\frac{1}{|\mathcal K|}
\sum_{k\in\mathcal K}
\left(
\frac{\widehat{obj}_{i,k}-obj_{i,k}^*}{\widehat{obj}_{i,k}}
\right)^2
\right]^{1/2}.
$$


Interpretation: RMS error as a fraction of the model's own predicted quantity.

No epsilon guard is used. If a prediction is zero, the relative error is undefined and should be treated as missing/invalid for that row rather than silently regularized.

Raw count error is reported only when explicitly useful:

$$
\mathcal E_1^{raw}(\widehat{obj}, obj^*; \mathcal K)
=
\left[
\frac{1}{|\mathcal K|}
\sum_{k\in\mathcal K}
(\widehat{obj}_{i,k}-obj_{i,k}^*)^2
\right]^{1/2}.
$$


Two event-time samples are used:

- `event_time_sample = all`: `mathcal K = {0,...,K}`.
- `event_time_sample = k=t`: `mathcal K = {t}` for each `t in {0,...,K}`.

When the predicted object is a scalar, e.g. `hat N_i`, set `widehat{obj}_{i,k}=hat N_i` for every `k in mathcal K`.

### E2 - Singleton-plus-other share-vector error

The contributor partitions used here always have the form:

1. a set of tracked singleton contributors;
2. one pooled `other` cell containing everyone else.

For organization `i`, stage `s`, share window `ell`, and partition candidate `c`, define:

$$
S_{i,c}^s = \text{tracked singleton contributors},
\qquad
O_{i,c}^s = \mathcal J_i^s \setminus S_{i,c}^s.
$$

For the `all` partition, every contributor is a singleton and `O_{i,c}^s` is empty.

Define the fitted and realized `other` totals:

$$
\hat P_{i,O}^{s,\ell,c} = \sum_{j\in O_{i,c}^s}\hat p_{i,j}^{s,\ell},
\qquad
P_{i,O,k}^{s,*,c} = \sum_{j\in O_{i,c}^s}p_{i,j,k}^{s,*}.
$$
The predicted individual-level share vector induced by the partition is:

$$
\hat q_{i,j}^{s,\ell,c}
=
\begin{cases}
\hat p_{i,j}^{s,\ell}, & j\in S_{i,c}^s,\\
\hat P_{i,O}^{s,\ell,c}/|O_{i,c}^s|, & j\in O_{i,c}^s.
\end{cases}
$$
The total decomposable share error is:

$$
\mathcal E_2^{total}
=
\left[
\frac{1}{|\mathcal K|}
\sum_{k\in\mathcal K}
\sum_j
(\hat q_{i,j}^{s,\ell,c} - p_{i,j,k}^{s,*})^2
\right]^{1/2}.
$$
It decomposes into three squared components:

$$
(\mathcal E_2^{total})^2
=
(\mathcal E_2^{singleton\text{-}cell\text{-}fit})^2
+
(\mathcal E_2^{other\text{-}cell\text{-}fit})^2
+
(\mathcal E_2^{other\text{-}coarsening})^2.
$$
The singleton cell-fit component is:

$$
(\mathcal E_2^{singleton\text{-}cell\text{-}fit})^2
=
\frac{1}{|\mathcal K|}
\sum_{k\in\mathcal K}
\sum_{j\in S_{i,c}^s}
(\hat p_{i,j}^{s,\ell}-p_{i,j,k}^{s,*})^2.
$$
The other-cell fit component is:

$$
(\mathcal E_2^{other\text{-}cell\text{-}fit})^2
=
\frac{1}{|\mathcal K|}
\sum_{k\in\mathcal K}
\frac{
(\hat P_{i,O}^{s,\ell,c}-P_{i,O,k}^{s,*,c})^2
}{|O_{i,c}^s|}.
$$
The other-cell coarsening component is:

$$
(\mathcal E_2^{other\text{-}coarsening})^2
=
\frac{1}{|\mathcal K|}
\sum_{k\in\mathcal K}
\sum_{j\in O_{i,c}^s}
\left(
\frac{P_{i,O,k}^{s,*,c}}{|O_{i,c}^s|}
-
p_{i,j,k}^{s,*}
\right)^2.
$$
If `O_{i,c}^s` is empty, all `other` components are zero.

#### Known/unknown version of E2

Define known and unknown contributors as:

$$
K_i^{s,\ell} = \{j: \hat p_{i,j}^{s,\ell} > 0\},
\qquad
U_i^{s,\ell} = \mathcal J_i^s \setminus K_i^{s,\ell}.
$$
Each of the three E2 components splits into known and unknown pieces. The six squared terms are:

1. `known_singleton_cell_fit`
2. `unknown_singleton_cell_fit`
3. `known_other_cell_fit`
4. `unknown_other_cell_fit`
5. `known_other_coarsening`
6. `unknown_other_coarsening`

They sum exactly to total squared error:

$$
(\mathcal E_2^{total})^2
=
(\mathcal E_2^{known,singleton\text{-}cell\text{-}fit})^2
+
(\mathcal E_2^{unknown,singleton\text{-}cell\text{-}fit})^2
+
(\mathcal E_2^{known,other\text{-}cell\text{-}fit})^2
+
(\mathcal E_2^{unknown,other\text{-}cell\text{-}fit})^2
+
(\mathcal E_2^{known,other\text{-}coarsening})^2
+
(\mathcal E_2^{unknown,other\text{-}coarsening})^2.
$$



For example:

$$
(\mathcal E_2^{known,singleton\text{-}cell\text{-}fit})^2
=
\frac{1}{|\mathcal K|}
\sum_{k\in\mathcal K}
\sum_{j\in S_{i,c}^s\cap K_i^{s,\ell}}
(\hat p_{i,j}^{s,\ell}-p_{i,j,k}^{s,*})^2,
$$

$$
(\mathcal E_2^{known,other\text{-}cell\text{-}fit})^2
=
\frac{1}{|\mathcal K|}
\sum_{k\in\mathcal K}
|O_{i,c}^s\cap K_i^{s,\ell}|
\left(
\frac{\hat P_{i,O}^{s,\ell,c}-P_{i,O,k}^{s,*,c}}
{|O_{i,c}^s|}
\right)^2,
$$


and

$$
(\mathcal E_2^{known,other\text{-}coarsening})^2
=
\frac{1}{|\mathcal K|}
\sum_{k\in\mathcal K}
\sum_{j\in O_{i,c}^s\cap K_i^{s,\ell}}
\left(
\frac{P_{i,O,k}^{s,*,c}}{|O_{i,c}^s|}
-
p_{i,j,k}^{s,*}
\right)^2.
$$


The unknown versions replace `K_i^{s,ell}` with `U_i^{s,ell}`.

The reported component shares are always squared-error shares, e.g.:

$$
share\_singleton\_cell\_fit
=
\frac{(\mathcal E_2^{singleton\text{-}cell\text{-}fit})^2}
{(\mathcal E_2^{total})^2}.
$$


Analogous shares are reported for all three components and all six known/unknown components when total error is positive.

#### Interpretation of the `all` partition

The `all` partition mechanically sets `other_cell_fit` and `other_coarsening` to zero because there is no `other` cell. This does not imply that `all` mechanically minimizes total error. A coarser partition can reduce cell-fit error by allowing over- and under-prediction among contributors to offset within the `other` cell. Partition choice therefore trades off individual-level fit instability against information loss from coarsening.

### E3 - Cross-stage weighted aggregate

For any per-stage error values `{mathcal E^o, mathcal E^r, mathcal E^m}`, define the organization-level cross-stage aggregate:

$$
\mathcal E_3(\{\mathcal E^s\};w)
=
\left[
\sum_{s\in\{o,r,m\}}
w_i^s(\mathcal E^s)^2
\right]^{1/2}.
$$


Report both weight schemes:

$$
w_{i,pre}^s
=
\frac{\sum_{k=-L}^{-1} A_{i,k}^s}
{\sum_{s'}\sum_{k=-L}^{-1} A_{i,k}^{s'}},
\qquad
w_{i,post}^s
=
\frac{\sum_{k=0}^{K} A_{i,k}^s}
{\sum_{s'}\sum_{k=0}^{K} A_{i,k}^{s'}}.
$$


### Cross-organization summary

Use RMS as the headline cross-organization summary because it preserves squared-error decompositions:

$$
\overline{\mathcal R}^{RMS}
=
\left[
\frac{1}{|\mathcal S|}
\sum_{i\in\mathcal S}
(\mathcal R_i)^2
\right]^{1/2},
$$


where `mathcal S` is the control set for fitting-selection tables and the treated set for evaluation tables.

Medians are reported alongside when useful:

$$
\widetilde{\mathcal R} = median_{i\in\mathcal S}\,\mathcal R_i.
$$


Use `overline{mathcal R}^{RMS}` explicitly throughout. Do not use `overline{mathcal R}` as shorthand.

---

## Phase 0 - rip out the old code

Delete stale code and stale outputs so nothing from the old pipeline survives.

Delete:

- `source/derived/model_prediction/prepare_contributor_actions.py`
- `source/derived/model_prediction/SConscript`
- `source/analysis/model_prediction/prepare_contributor_panel.py`
- `source/analysis/model_prediction/estimate_model_probabilities.py`
- `source/analysis/model_prediction/compute_predictions.py`
- `source/analysis/model_prediction/evaluate_fit.py`
- `source/analysis/model_prediction/model_utils.py`
- `source/analysis/model_prediction/SConscript`
- all previously emitted outputs under:
  - `output/derived/model_prediction/`
  - `output/analysis/model_prediction/`
  - `drive/output/derived/model_prediction/`
  - `drive/output/analysis/model_prediction/`

  Update parent `SConscript`s to drop deleted subdirectories from their `SConscript()` calls.

  Pause after listing what will be deleted. After approval, commit as a separate commit: `rip out old model_prediction pipeline #<issue>`.

---

## Phase 1 - Derived dataset: per-repo event_time x contributor panel

Code only. No paper edits.

For every repo with an action-data file under `drive/output/derived/action_data/repo_actions/`, produce a per-repo parquet keyed by `(quasi_event_time, actor_id)`.

### Output columns

| Column | Definition |
|---|---|
| `member_pull_request_opened` | `A_{i,j,k}^o`: unique PRs contributor `j` opened in period `k` |
| `member_pull_request_reviewed` | `A_{i,j,k}^r`: unique PRs contributor `j` reviewed in period `k` |
| `member_pull_request_merged` | `A_{i,j,k}^m`: unique PRs contributor `j` merged in period `k` |
| `repo_pull_request_opened` | `Q_{i,k}^{o,*}` |
| `repo_pull_request_reviewed` | `Q_{i,k}^{r,*}`: unique PRs reviewed |
| `repo_pull_request_merged` | `Q_{i,k}^{m,*}` |

Member universe: any non-bot `actor_id` with at least one PR-stage action in any period from `quasi_event_time=-5` through `+5`. The grid is rectangular over included contributors and event times `{-5,...,+5}`. Missing contributor-period cells get zeros for member counts. Repo-level columns are constant across contributors within event time.

### Inputs

- `drive/output/derived/action_data/repo_actions/{repo}.parquet`
- For each `(importance_type, rolling_period, qualified_sample, control_group)` combo from `pipeline_inputs.json`: `output/analysis/data_prep/{...}/panel.parquet` to recover `quasi_treatment_group` and event-time mapping.

### Outputs

- Data: `drive/output/derived/model_prediction/event_time_member_panel/{importance_type}/{rolling_period}/{qualified_sample}/{control_group}/{repo}.parquet`
- Log: `output/derived/model_prediction/event_time_member_panel/{importance_type}/{rolling_period}/{qualified_sample}/{control_group}/{repo}.log`

### Files to write

- `source/derived/model_prediction/build_event_time_member_panel.py`
- `source/derived/model_prediction/SConscript`

Implementation follows the reference-repo sentinel pattern. The script discovers all repos at runtime and parallelizes with `joblib`.

### Verification at pause

1. SCons builds the reference repo target.
2. For a spot-checked repo, contributor universe equals the union of PR-stage actors in `[-5,+5]` after bot exclusion.
3. `repo_*` columns are constant within event time.
4. `sum_j A_{i,j,k}^o = Q_{i,k}^{o,*}` and `sum_j A_{i,j,k}^m = Q_{i,k}^{m,*}` exactly.
5. For reviews, `sum_j A_{i,j,k}^r >= Q_{i,k}^{r,*}`.
6. Row count equals `#contributors x 11`.

Pause for inspection.

---

## Phase 2 - Fitting counterfactual inputs on controls

All selection in Phase 2 is computed only over never-treated control organizations. Treated organizations are held out from selection and are used only in Phase 3 evaluation.

### Restructured paper outline for Section 3.3

- **Section 3.3.1 Prediction model.** Define the treated prediction:

$$\hat Q_{i,k}^{o,(-j_i^{dep})}=\hat N_i\hat\pi_i^{o,(-j_i^{dep})}, \qquad \hat Q_{i,k}^{r,(-j_i^{dep})}=\hat N_i\hat\pi_i^{o,(-j_i^{dep})}\hat\pi_i^{r,(-j_i^{dep})}, \qquad \hat Q_{i,k}^{m,(-j_i^{dep})}=\hat N_i\hat\pi_i^{o,(-j_i^{dep})}\hat\pi_i^{r,(-j_i^{dep})}\hat\pi_i^{m,(-j_i^{dep})}. $$ 

- **Section 3.3.2 Latent-problem aggregation model.** Opening uses share-normalized opening; review and merge use independent-Bernoulli aggregation.
- **Section 3.3.3 Error primitives.** Define E1, E2, E3 once.
- **Section 3.3.4 How controls are used.** Production inputs are fit on treated pre-period data. Controls choose the fitting rule by applying candidate procedures around pseudo-departure dates.
- **Section 3.3.5 Fitting `hat N_i`.** Select between latent-flow candidates using E1.
- **Section 3.3.6 Jointly fitting contributor shares and partition.** Select `(ell,c)` jointly using E2 + E3.
- **Section 3.3.7 Selection outputs.** Tables and figures produced by Phase 2.

### Phase 2A - Fit latent problem flow `hat N_i`

Candidate rules: $$\hat N_i^{avg_L}=\frac{1}{L}\sum_{k=-L}^{-1}Q_{i,k}^{o,*},
\qquad
\hat N_i^{last}=Q_{i,-1}^{o,*}.$$ 

Selection metric on controls: $$\mathcal R_i^N(\hat N;\mathcal K)
=
\mathcal E_1^{rel}(\hat N_i,Q_{i,k}^{o,*};\mathcal K).$$ 

Raw count error is reported alongside: $$\mathcal R_i^{N,raw}(\hat N;\mathcal K)
=
\mathcal E_1^{raw}(\hat N_i,Q_{i,k}^{o,*};\mathcal K).$$ 

Selection rule: $$\hat N^* =
\arg\min_{\hat N\in\{avg_L,last\}}
\overline{\mathcal R^N}^{RMS}(\hat N;\mathcal K=all).$$ 

#### Scripts

- `source/analysis/model_prediction/fit_latent_n.py`
- `source/analysis/model_prediction/plot_latent_n.py`

#### Outputs

- `drive/output/analysis/model_prediction/{...}/latent_n_selection.parquet`
- `output/analysis/model_prediction/{...}/tables/latent_n_selection.tex`
- `output/analysis/model_prediction/{...}/plots/latent_flow_N_fan.png`
- `output/analysis/model_prediction/{...}/plots/latent_flow_error_dist.png`
- `output/analysis/model_prediction/{...}/plots/latent_flow_error_by_k.png`
- `output/analysis/model_prediction/{...}/recommended_config.json` with `latent_flow_rule` populated.

#### `latent_n_selection.tex`

Columns:

| candidate | event_time_sample | rms_relative_count_error | median_relative_count_error | rms_raw_count_error | median_raw_count_error | selected_on_relative_all |
|---|---|---:|---:|---:|---:|---|

The selected marker appears only on the `event_time_sample=all` row of the winning candidate.

#### Pause checks

1. Table winner has smallest `rms_relative_count_error` on `event_time_sample=all`.
2. `recommended_config.json.latent_flow_rule` matches the table.
3. Relative-error rows with zero predictions are marked invalid/missing rather than regularized.
4. Plots open and are readable.

Pause for inspection.

### Phase 2B - Jointly fit share window and contributor partition

The old separate estimation-sample and contributor-partition phases are replaced by one joint grid search.

Candidate share windows: $$\ell\in\{1,\ldots,L\}.$$ 

The paper can highlight `ell=1` (`last`) and `ell=L=5` (`pooled_L5`), but the code should support the full grid.

For each window: $$\hat p_{i,j}^{s,\ell}
=
\frac{\sum_{k=-\ell}^{-1}A_{i,j,k}^s}
{\sum_{k=-\ell}^{-1}A_{i,k}^s}.$$ 

Candidate partitions:

| Candidate | Definition |
|---|---|
| `all` | every contributor in the pre+post union is a singleton; no `other` cell |
| `top5` | top 5 contributors by pre-period activity across all stages; everyone else in `other` |
| `top5_per_stage` | union of stage-specific top-5 contributors by pre-period activity; everyone else in `other` |
| `cover80p` | smallest top-K set covering 80 percent of all-stage pre-period activity; everyone else in `other` |
| `cover80p_per_stage` | union of stage-specific top-K sets each covering 80 percent of that stage's pre-period activity; everyone else in `other` |

Infrastructure for `top5` and `cover80p` should use parameters, e.g. `top_k=5` and `coverage_threshold=0.80`, so sensitivity checks are easy later.

The contributor universe is the union of pre-period and post-period contributors at the organization. Top-K and coverage ranking use pre-period activity from the candidate window `ell`. New post-period contributors have zero pre-period activity and therefore can appear only as zero-share singletons under `all` or inside `other` under coarser partitions.

Selection metric for a candidate pair `(ell,c)`: $$\mathcal R_i^{p,s,\ell,c,total}(\mathcal K)
=
\mathcal E_2^{total}(\hat p_{i,\cdot}^{s,\ell},p_{i,\cdot,k}^{s,*},c;\mathcal K).$$ 

Aggregate across stages with E3: $$\mathcal R_i^{p,agg,\ell,c,w}(\mathcal K)
=
\mathcal E_3(\{\mathcal R_i^{p,s,\ell,c,total}(\mathcal K)\}_s;w).$$ 

Selection rule: $$(\ell^*,c^*)
=
\arg\min_{\ell,c}
\overline{\mathcal R^{p,agg,\ell,c,w_{pre}}}^{RMS}(\mathcal K=all).$$ 

Post-event weighting is reported as a robustness check, not used for the headline selection.

#### Scripts

- `source/analysis/model_prediction/fit_share_partition.py`
- `source/analysis/model_prediction/plot_share_partition.py`

Remove the old split scripts:

- `fit_estimation_sample.py`
- `plot_estimation_sample.py`
- `fit_contributor_partition.py`
- `plot_contributor_partition.py`

#### Outputs

- `drive/output/analysis/model_prediction/{...}/share_partition_selection.parquet`
- `output/analysis/model_prediction/{...}/tables/share_partition_selection.tex`
- `output/analysis/model_prediction/{...}/plots/share_partition_total_error_dist.png`
- `output/analysis/model_prediction/{...}/plots/share_partition_total_error_by_k.png`
- `output/analysis/model_prediction/{...}/plots/share_partition_total_error_grid.png`
- `output/analysis/model_prediction/{...}/plots/share_partition_three_component_dist.png`
- `output/analysis/model_prediction/{...}/plots/share_partition_three_component_share_bar.png`
- `output/analysis/model_prediction/{...}/plots/share_partition_six_component_dist.png`
- `output/analysis/model_prediction/{...}/plots/share_partition_six_component_share_bar.png`
- `output/analysis/model_prediction/{...}/recommended_config.json` with `individual_share_estimator` and `contributor_partition` populated.

#### `share_partition_selection.tex`

This is a detailed main table, not an appendix table. Use clear and consistent column names across the `.tex`, parquet, plots, and helper outputs.

Panel A - headline selection:

| share_window_candidate | partition_candidate | event_time_sample | rms_total_error_stage_open | rms_total_error_stage_review | rms_total_error_stage_merge | rms_total_error_agg_pre_weight | rms_total_error_agg_post_weight | selected_on_agg_pre_all |
|---|---|---|---:|---:|---:|---:|---:|---|

Panel B - three-component decomposition:

| share_window_candidate | partition_candidate | event_time_sample | stage_or_aggregate | rms_singleton_cell_fit | rms_other_cell_fit | rms_other_coarsening | share_singleton_cell_fit | share_other_cell_fit | share_other_coarsening |
|---|---|---|---|---:|---:|---:|---:|---:|---:|

Panel C - six-component decomposition:

| share_window_candidate | partition_candidate | event_time_sample | stage_or_aggregate | rms_known_singleton_cell_fit | rms_unknown_singleton_cell_fit | rms_known_other_cell_fit | rms_unknown_other_cell_fit | rms_known_other_coarsening | rms_unknown_other_coarsening |
|---|---|---|---|---:|---:|---:|---:|---:|---:|

Panel D - six-component squared-error shares:

| share_window_candidate | partition_candidate | event_time_sample | stage_or_aggregate | share_known_singleton_cell_fit | share_unknown_singleton_cell_fit | share_known_other_cell_fit | share_unknown_other_cell_fit | share_known_other_coarsening | share_unknown_other_coarsening |
|---|---|---|---|---:|---:|---:|---:|---:|---:|

`stage_or_aggregate` takes values: `open`, `review`, `merge`, `agg_pre_weight`, `agg_post_weight`.

#### Recommended config schema

Phase 2 writes:

```json
{
  "latent_flow_rule": "avg_L",
  "individual_share_estimator": {"type": "pooled", "L": 5},
  "contributor_partition": {
    "rule": "top5",
    "top_k": 5,
    "coverage_threshold": 0.80
  },
  "selection_objective": "rms_total_error_agg_pre_weight_all",
  "winner_errors": {}
}
```

The code never writes to `source/lib/config/model_prediction.json`. The user manually promotes the selected values after inspecting the Phase 2 outputs.

#### Pause checks

1. Table winner has smallest `rms_total_error_agg_pre_weight` on `event_time_sample=all`.
2. `recommended_config.json` matches the selected pair.
3. For every `(organization, stage, ell, partition, event_time_sample)`, the three-component identity holds:

```tex
(total)^2 = (singleton\_cell\_fit)^2 + (other\_cell\_fit)^2 + (other\_coarsening)^2.
```

4. The six known/unknown components sum to total squared error.
5. For `all`, `other_cell_fit=0` and `other_coarsening=0` exactly.
6. Plots open and are readable.

Pause for inspection. After approval, the user manually copies the selected config into `source/lib/config/model_prediction.json`.

### Phase 2 SConscript wiring

`source/analysis/model_prediction/SConscript` should include four `env.Python()` calls per input combo:

1. `fit_latent_n.py`
2. `plot_latent_n.py`
3. `fit_share_partition.py`
4. `plot_share_partition.py`

Dependency chain:

```text
fit_latent_n -> plot_latent_n
fit_latent_n -> fit_share_partition -> plot_share_partition
```

This preserves the pause structure while allowing end-to-end builds.

---

## Phase 3 - Predictions on treated organizations and error attribution

Phase 3 reads `source/lib/config/model_prediction.json`, applies the selected configuration to treated organizations, and produces three diagnostics:

1. count prediction error;
2. stage-level oracle attribution;
3. conditional remaining-contributor share allocation error.

### Restructured paper outline for Section 3.4

- **Section 3.4.1 Step 1 - Count prediction error.** How wrong are predicted counts at each stage and overall? Uses E1 + E3.
- **Section 3.4.2 Step 2 - Stage-level oracle attribution.** Which stage-level ingredients account for count prediction error? Uses E1 + E3 across oracle levels and explicitly states the joint `hat N_i` / departed-share identification problem.
- **Section 3.4.3 Step 3 - Conditional remaining-contributor share allocation.** Conditional on the predicted departed share being correct, how well does the model allocate the remaining share mass across non-departed contributors? Uses E2.
- **Appendix A - Component-level oracle attribution.** Decomposes review and merge oracle drops into singleton-cell-fit, other-cell-fit, and other-coarsening substitutions.
- **Appendix B - Channel decomposition of predicted decline.** Predicted versus empirical channel split in count units.

### Phase 3A - Count prediction error

For each treated organization `i`, event time `k`, and stage `s`, compute the selected model prediction:

$$\hat Q_{i,k}^{o,(-j_i^{dep})}
=
\hat N_i\hat\pi_i^{o,(-j_i^{dep})}, \qquad \hat Q_{i,k}^{r,(-j_i^{dep})}
=
\hat N_i\hat\pi_i^{o,(-j_i^{dep})}\hat\pi_i^{r,(-j_i^{dep})}, \hat Q_{i,k}^{m,(-j_i^{dep})}
=
\hat N_i\hat\pi_i^{o,(-j_i^{dep})}\hat\pi_i^{r,(-j_i^{dep})}\hat\pi_i^{m,(-j_i^{dep})}.$$ 

Count error uses E1 relative error by default: $$\mathcal R_i^{s,rel}(\mathcal K)
=
\mathcal E_1^{rel}(\hat Q_{i,k}^{s,(-j_i^{dep})},Q_{i,k}^{s,*};\mathcal K).$$ 

Raw count error is reported alongside when useful.

Aggregate across stages using E3 under both pre- and post-event weights.

Failure quantities reuse E1: $$Q_{i,k}^{o-r,*}=Q_{i,k}^{o,*}-Q_{i,k}^{r,*},
Q_{i,k}^{r-m,*}=Q_{i,k}^{r,*}-Q_{i,k}^{m,*},$$  with predicted analogues constructed from predicted counts.

#### Scripts

- `source/analysis/model_prediction/predict_treated.py`
- `source/analysis/model_prediction/evaluate_count_predictions.py`
- `source/analysis/model_prediction/plot_count_predictions.py`

`predict_treated.py` writes all prediction parquets used by Phase 3A, 3B, and 3C.

#### Prediction parquets

- `drive/output/analysis/model_prediction/{...}/predictions.parquet`
- `drive/output/analysis/model_prediction/{...}/oracle_predictions.parquet`
- `drive/output/analysis/model_prediction/{...}/treated_share_decomposition.parquet`
- `drive/output/analysis/model_prediction/{...}/channel_decomposition.parquet`

#### `prediction_errors.tex`

Panel A - per-stage count error:

| stage | event_time_sample | rms_relative_count_error | median_relative_count_error | rms_raw_count_error | median_raw_count_error |
|---|---|---:|---:|---:|---:|

Panel B - aggregate count error:

| event_time_sample | rms_relative_count_error_agg_pre_weight | median_relative_count_error_agg_pre_weight | rms_relative_count_error_agg_post_weight | median_relative_count_error_agg_post_weight | rms_raw_count_error_agg_pre_weight | rms_raw_count_error_agg_post_weight |
|---|---:|---:|---:|---:|---:|---:|

Panel C - failure quantities:

| failure_quantity | event_time_sample | rms_relative_count_error | median_relative_count_error | rms_raw_count_error | median_raw_count_error |
|---|---|---:|---:|---:|---:|

#### Plots

- `prediction_error_dist_open.png`
- `prediction_error_dist_review.png`
- `prediction_error_dist_merge.png`
- `prediction_error_by_event_time_open.png`
- `prediction_error_by_event_time_review.png`
- `prediction_error_by_event_time_merge.png`
- `prediction_error_agg_dist.png`
- `failure_quantity_errors.png`

#### Pause checks

1. Pipeline runs clean.
2. Relative count errors with zero predictions are marked invalid/missing, not regularized.
3. Aggregates use the selected config from `source/lib/config/model_prediction.json`.
4. Plots open and are readable.

Pause for inspection.

### Phase 3B - Stage-level error decomposition

#### Identification preamble for Section 3.4.2

Opening-stage treated count error is jointly attributable to latent problem flow and the departed contributor's missing opening share.

The realized treated opening count can be written conceptually as: $$Q_{i,k}^{o,*}
=
N_{i,k}^*(1-p_{i,j_i^{dep},k}^{o,*,cf}),$$ 

where $p_{i,j_i^{dep},k}^{o,*,cf}$  is the counterfactual share the departed contributor would have taken had they not left. This quantity is never observed.

The model predicts: $$\hat Q_{i,k}^{o,(-j_i^{dep})}
=
\hat N_i(1-\hat p_{i,j_i^{dep}}^{o,\ell^*}).$$ 

The count error therefore combines two unseparated objects: $\hat N_i$  and $\hat p_{i,j_i^{dep}}^{o,ell*}$. Observed treated counts alone cannot tell whether the model's opening error comes from latent problem flow, the predicted departed share, or both. The oracle attribution should therefore label the first drop as the joint $(\hat N_i, \hat p_{i,j_i^{dep}}^o)$ contribution, not the $\hat N_i$ contribution alone.

This identification issue also motivates Phase 3C. The share diagnostic in Phase 3C does not solve the joint-identification problem. Instead, it asks a conditional question: assuming the predicted departed mass is correct, did the model allocate the remaining mass correctly across non-departed contributors?

#### Main four-level oracle

Define implied realized stage rates: $$\pi_{i,k}^{r,*}=Q_{i,k}^{r,*}/Q_{i,k}^{o,*},
\qquad
\pi_{i,k}^{m,*}=Q_{i,k}^{m,*}/Q_{i,k}^{r,*}.$$ 

Four oracle levels for merge prediction:

| Level | Formula | Drop from previous level |
|---|---|---|
| 0 model | $\hat N_i \hat \pi_i^{o,(-dep)} \hat \pi_i^{r,(-dep)} \hat \pi_i^{m,(-dep)}$ | none |
| 1 opening oracle | $Q_{i,k}^{o,*} \hat \pi_i^{r,(-dep)} \hat \pi_i^{m,(-dep)}$ | joint $(\hat N_i, \hat p_{i,j_i^{dep}}^o)$ |
| 2 review oracle | $Q_{i,k}^{r,*} \hat  \pi_i^{m,(-dep)}$ | review aggregation |
| 3 merge oracle | $Q_{i,k}^{m,*}$ | merge aggregation |

Apply E1 relative error and E3 at each level. Report error levels and drops for both `event_time_sample=all` and per-k rows.

#### Appendix A component-level oracle

The appendix propagates the E2 component logic through the stage aggregation. For a given stage, define the following component-corrected share vectors at each `k`:

1. model exploded vector:
   - singleton contributors use $\hat p_{i,j}^{s,ell*}$;
   - other contributors receive $\hat P_{i,O}^{s,ell*,c*}/|O|$.
2. singleton-cell-fit corrected vector:
   - singleton contributors use realized $p_{i,j,k}^{s,*}$;
   - other contributors still receive $\hat P_{i,O}^{s,ell*,c*}/|O|$.
3. other-cell-fit corrected vector:
   - singleton contributors use realized $p_{i,j,k}^{s,*}$;
   - other contributors receive realized other total $P_{i,O,k}^{s,*,c*}/|O|$.
4. full oracle vector:
   - every contributor uses realized $p_{i,j,k}^{s,*}$.

   Aggregating these vectors through the latent-problem formula yields component-level oracle drops:

- review singleton-cell-fit;
- review other-cell-fit;
- review other-coarsening;
- merge singleton-cell-fit;
- merge other-cell-fit;
- merge other-coarsening.

Appendix A rows should therefore use clear level labels:

```text
model
opening_oracle
review_singleton_cell_fit_oracle
review_other_cell_fit_oracle
review_full_oracle
merge_singleton_cell_fit_oracle
merge_other_cell_fit_oracle
merge_full_oracle
```

#### Scripts

- `source/analysis/model_prediction/evaluate_stage_decomposition.py`
- `source/analysis/model_prediction/plot_stage_decomposition.py`

#### Tables

`oracle_attribution.tex`, main four-level version.

Panel A - error by oracle level:

| oracle_level | event_time_sample | rms_relative_count_error_agg_pre_weight | median_relative_count_error_agg_pre_weight | rms_relative_count_error_agg_post_weight | median_relative_count_error_agg_post_weight |
|---|---|---:|---:|---:|---:|

Panel B - drops:

| drop_label | event_time_sample | rms_relative_error_drop_pre_weight | rms_relative_error_drop_post_weight | share_of_model_error_squared_pre_weight | share_of_model_error_squared_post_weight |
|---|---|---:|---:|---:|---:|

`oracle_attribution_decomposed.tex`, Appendix A component-level version.

Panel A - component oracle levels:

| oracle_level | event_time_sample | rms_relative_count_error_agg_pre_weight | rms_relative_count_error_agg_post_weight | median_relative_count_error_agg_pre_weight | median_relative_count_error_agg_post_weight |
|---|---|---:|---:|---:|---:|

Panel B - component drops:

| drop_label | event_time_sample | rms_relative_error_drop_pre_weight | rms_relative_error_drop_post_weight | share_of_model_error_squared_pre_weight | share_of_model_error_squared_post_weight |
|---|---|---:|---:|---:|---:|

`channel_decomposition.tex`, Appendix B.

Panel A - predicted versus empirical channels:

| channel | event_time_sample | mean_predicted_channel | mean_empirical_channel | mean_absolute_discrepancy | mean_absolute_relative_discrepancy |
|---|---|---:|---:|---:|---:|

Panel B - predicted channel shares:

| channel | event_time_sample | mean_predicted_channel_share_of_total_decline |
|---|---|---:|

#### Plots

Main oracle:

- `oracle_attribution_levels.png`
- `oracle_attribution_drops.png`
- `oracle_attribution_drop_dist.png`
- `oracle_attribution_by_event_time.png`

Appendix A:

- `oracle_component_levels.png`
- `oracle_component_drops.png`
- `oracle_component_drop_dist.png`

Appendix B:

- `channel_decomposition_scatter_open.png`
- `channel_decomposition_scatter_review.png`
- `channel_decomposition_scatter_merge.png`
- `channel_decomposition_disc_dist_open.png`
- `channel_decomposition_disc_dist_review.png`
- `channel_decomposition_disc_dist_merge.png`

#### Pause checks

1. Main oracle is non-increasing by oracle level for each event-time sample; level 3 is zero by construction.
2. Component-level oracle is non-increasing by component level.
3. Within review and merge, component drops sum in squared-error space to the corresponding main-stage drop.
4. Channel contributions sum to predicted decline to numerical precision.
5. Plots open and are readable.

Pause for inspection.

### Phase 3C - Conditional remaining-contributor share allocation

This replaces the earlier treated-share renormalization language.

The model already has predictions for the non-departed contributors' shares, $\hat p_{i,j}^{s,ell*}$ for $j != j_i^{dep}$.  Do not renormalize these predicted non-departed shares.

Instead, scale the realized post-period non-departed share vector so that its total mass equals the model-implied non-departed mass: $$1-\hat p_{i,j_i^{dep}}^{s,\ell^*}.$$ 

For $j != j_i^{dep}$, define the scaled realized target: $$p_{i,j,k}^{s,*,scaled}
=
\left(1-\hat p_{i,j_i^{dep}}^{s,\ell^*}\right)
\frac{A_{i,j,k}^s}
{\sum_{h\neq j_i^{dep}}A_{i,h,k}^s}.$$ 

Then compare: $$\hat p_{i,j}^{s,\ell^*}
\quad\text{to}\quad
p_{i,j,k}^{s,*,scaled}.$$ 

This diagnostic asks:

> Conditional on the model's predicted departed share being correct, does the model allocate the remaining contributor mass correctly?

This is intentionally different from validating the departed effect or latent problem flow. If Phase 3C errors are small but count-prediction errors are large, the error likely lives in the jointly unidentified $(\hat N_i, \hat p_{i,j_i^{dep}})$ margin. If Phase 3C errors are large, then even after conditioning on the departed mass, the model is misallocating activity across remaining contributors.

Apply E2 using:

- prediction: $\hat p_{i,j}^{s,ell*}$ for non-departed contributors;
- target: $p_{i,j,k}^{s,*,scaled}$;
- selected partition $c^*$, restricted to non-departed contributors;
- contributor universe: pre-departure remaining contributors plus post-departure new arrivals.

New post-period contributors have zero predicted share. Under `all`, they are unknown singletons; under coarser partitions, they enter `other`.

Rows with no observed remaining stage activity have undefined realized shares and should be marked missing for the affected `(organization, stage, k)` rather than regularized.

#### Scripts

- `source/analysis/model_prediction/evaluate_share_decomposition.py`
- `source/analysis/model_prediction/plot_share_decomposition.py`

#### `treated_share_decomposition.tex`

Use the same component names as `share_partition_selection.tex`.

Panel A - headline treated share allocation error:

| stage_or_aggregate | event_time_sample | rms_total_error | median_total_error | rms_total_error_agg_pre_weight | rms_total_error_agg_post_weight |
|---|---|---:|---:|---:|---:|

Panel B - three-component decomposition:

| stage_or_aggregate | event_time_sample | rms_singleton_cell_fit | rms_other_cell_fit | rms_other_coarsening | share_singleton_cell_fit | share_other_cell_fit | share_other_coarsening |
|---|---|---:|---:|---:|---:|---:|---:|

Panel C - six-component decomposition:

| stage_or_aggregate | event_time_sample | rms_known_singleton_cell_fit | rms_unknown_singleton_cell_fit | rms_known_other_cell_fit | rms_unknown_other_cell_fit | rms_known_other_coarsening | rms_unknown_other_coarsening |
|---|---|---:|---:|---:|---:|---:|---:|

Panel D - six-component squared-error shares:

| stage_or_aggregate | event_time_sample | share_known_singleton_cell_fit | share_unknown_singleton_cell_fit | share_known_other_cell_fit | share_unknown_other_cell_fit | share_known_other_coarsening | share_unknown_other_coarsening |
|---|---|---:|---:|---:|---:|---:|---:|

#### Plots

- `share_decomp_total_open.png`
- `share_decomp_total_review.png`
- `share_decomp_total_merge.png`
- `share_decomp_three_component_open.png`
- `share_decomp_three_component_review.png`
- `share_decomp_three_component_merge.png`
- `share_decomp_three_component_share_bar.png`
- `share_decomp_six_component_open.png`
- `share_decomp_six_component_review.png`
- `share_decomp_six_component_merge.png`
- `share_decomp_six_component_share_bar.png`
- `share_decomp_by_event_time_open.png`
- `share_decomp_by_event_time_review.png`
- `share_decomp_by_event_time_merge.png`
- `share_decomp_oracle_consistency.png`

#### Pause checks

1. Three-component E2 identity holds for every `(organization, stage, event_time_sample)` and at the cross-organization RMS level.
2. Six-component E2 identity holds analogously.
3. Predicted non-departed shares are not renormalized.
4. Scaled realized non-departed shares sum to `1 - hat p_dep` for each valid `(organization, stage, k)`.
5. Plots open and are readable.

Pause for inspection.

### Phase 3 SConscript wiring

Add seven `env.Python()` calls per input combo:

1. `predict_treated.py`
2. `evaluate_count_predictions.py`
3. `plot_count_predictions.py`
4. `evaluate_stage_decomposition.py`
5. `plot_stage_decomposition.py`
6. `evaluate_share_decomposition.py`
7. `plot_share_decomposition.py`

Dependencies:

```text
predict_treated -> evaluate_count_predictions -> plot_count_predictions
predict_treated -> evaluate_count_predictions -> evaluate_stage_decomposition -> plot_stage_decomposition
predict_treated -> evaluate_stage_decomposition -> evaluate_share_decomposition -> plot_share_decomposition
```

This preserves the 3A -> 3B -> 3C pause order while sharing the prediction parquets.

---

## Shared helper module

Create `source/analysis/model_prediction/helpers.py`. Function names should be descriptive; E1/E2/E3 are paper labels, not necessarily code names.

Required helpers:

### Count errors

- `relative_trajectory_error(pred_traj, obs_traj, k_set)`
  - Implements E1 relative error.
  - Does not use an epsilon guard.
  - Marks zero-prediction rows invalid/missing.
- `raw_trajectory_error(pred_traj, obs_traj, k_set)`
  - Implements E1 raw error.

### Share errors

- `share_partition_error(p_hat, p_obs_traj, singleton_members, other_members, known_members, k_set)`
  - Implements E2.
  - Returns the full field set below.

  Return fields:

```text
total
singleton_cell_fit
other_cell_fit
other_coarsening
known_singleton_cell_fit
unknown_singleton_cell_fit
known_other_cell_fit
unknown_other_cell_fit
known_other_coarsening
unknown_other_coarsening
share_singleton_cell_fit
share_other_cell_fit
share_other_coarsening
share_known_singleton_cell_fit
share_unknown_singleton_cell_fit
share_known_other_cell_fit
share_unknown_other_cell_fit
share_known_other_coarsening
share_unknown_other_coarsening
```

### Aggregation and summaries

- `weighted_stage_aggregate(per_stage_errors, weights)`
  - Implements E3.
- `cross_org_rms(per_org_values)`
  - Implements `overline{mathcal R}^{RMS}`.
- `cross_org_median(per_org_values)`
  - Used for table medians.

### Model helpers

- `compute_individual_shares(panel, ell)`
- `estimate_latent_n(panel, rule, L)`
- `build_singleton_other_partition(panel, rule, ell, stage, member_universe, top_k=5, coverage_threshold=0.80)`
- `apply_partition_mapping(shares, mapping)`
- `independent_bernoulli_aggregation(individual_shares, exclude=None)`
- `scaled_realized_remaining_shares(panel, p_hat_dep, departed_id, stage)`
  - Implements Phase 3C target scaling.
- `component_corrected_shares(p_hat, p_obs, singleton_members, other_members, level)`
  - Levels: `model_exploded`, `singleton_cell_fit_corrected`, `other_cell_fit_corrected`, `full_oracle`.
  - Used by Appendix A oracle decomposition.

### Config helper

Add `LoadModelPredictionConfig()` in `source/lib/python/config_loaders.py`.

Code must not write to `source/lib/config/model_prediction.json`; only the user edits that file after inspecting `recommended_config.json`.

---

## Critical files to be modified or created

### Deletions

- `source/derived/model_prediction/prepare_contributor_actions.py`
- `source/derived/model_prediction/SConscript`
- `source/analysis/model_prediction/prepare_contributor_panel.py`
- `source/analysis/model_prediction/estimate_model_probabilities.py`
- `source/analysis/model_prediction/compute_predictions.py`
- `source/analysis/model_prediction/evaluate_fit.py`
- `source/analysis/model_prediction/model_utils.py`
- `source/analysis/model_prediction/SConscript`

### New Phase 1 files

- `source/derived/model_prediction/build_event_time_member_panel.py`
- `source/derived/model_prediction/SConscript`

### New Phase 2 files

- `source/analysis/model_prediction/helpers.py`
- `source/analysis/model_prediction/fit_latent_n.py`
- `source/analysis/model_prediction/plot_latent_n.py`
- `source/analysis/model_prediction/fit_share_partition.py`
- `source/analysis/model_prediction/plot_share_partition.py`
- `source/analysis/model_prediction/SConscript`
- `source/lib/config/model_prediction.json` (user-edited)
- `source/lib/python/config_loaders.py` update for `LoadModelPredictionConfig()`

### New Phase 3 files

- `source/analysis/model_prediction/predict_treated.py`
- `source/analysis/model_prediction/evaluate_count_predictions.py`
- `source/analysis/model_prediction/plot_count_predictions.py`
- `source/analysis/model_prediction/evaluate_stage_decomposition.py`
- `source/analysis/model_prediction/plot_stage_decomposition.py`
- `source/analysis/model_prediction/evaluate_share_decomposition.py`
- `source/analysis/model_prediction/plot_share_decomposition.py`

### Paper edits

- `source/paper/model.tex`
  - Rewrite Section 3.3 using the new notation and the joint share-window x partition selection.
  - Rewrite E1 as inverse-prediction relative error by default.
  - Rewrite E2 as singleton cell-fit + other cell-fit + other coarsening, with six known/unknown components.
  - Rewrite Section 3.4.3 so treated share diagnostics scale the realized non-departed shares by `1 - hat p_dep` and do not renormalize predicted non-departed shares.
  - Update all table and figure references to the new output file names.
  - Update Appendix A to use component-level oracle labels aligned with the three E2 components.
  - Keep the algebraic proof of the general decomposition in the appendix or implementation note, not in the main text.

---

## Outputs summary

### Phase 2 tables

- `latent_n_selection.tex`
- `share_partition_selection.tex`

### Phase 2 plots

- `latent_flow_N_fan.png`
- `latent_flow_error_dist.png`
- `latent_flow_error_by_k.png`
- `share_partition_total_error_dist.png`
- `share_partition_total_error_by_k.png`
- `share_partition_total_error_grid.png`
- `share_partition_three_component_dist.png`
- `share_partition_three_component_share_bar.png`
- `share_partition_six_component_dist.png`
- `share_partition_six_component_share_bar.png`

### Phase 3 tables

- `prediction_errors.tex`
- `oracle_attribution.tex`
- `oracle_attribution_decomposed.tex`
- `channel_decomposition.tex`
- `treated_share_decomposition.tex`

### Phase 3 plots

- `prediction_error_dist_{open,review,merge}.png`
- `prediction_error_by_event_time_{open,review,merge}.png`
- `prediction_error_agg_dist.png`
- `failure_quantity_errors.png`
- `oracle_attribution_levels.png`
- `oracle_attribution_drops.png`
- `oracle_attribution_drop_dist.png`
- `oracle_attribution_by_event_time.png`
- `oracle_component_levels.png`
- `oracle_component_drops.png`
- `oracle_component_drop_dist.png`
- `channel_decomposition_scatter_{open,review,merge}.png`
- `channel_decomposition_disc_dist_{open,review,merge}.png`
- `share_decomp_total_{open,review,merge}.png`
- `share_decomp_three_component_{open,review,merge}.png`
- `share_decomp_three_component_share_bar.png`
- `share_decomp_six_component_{open,review,merge}.png`
- `share_decomp_six_component_share_bar.png`
- `share_decomp_by_event_time_{open,review,merge}.png`
- `share_decomp_oracle_consistency.png`

---

## End-to-end verification

After all phases:

```bash
scons drive/output/derived/model_prediction/ \
      output/analysis/model_prediction/
```

must build clean for the full Cartesian product of `(importance_type, rolling_period, qualified_sample, control_group)`.

Core checks:

1. Every plot listed above renders.
2. Every table listed above is written with clear, consistent column names.
3. E1 relative errors use inverse prediction by default; raw count error is only reported in explicit raw columns.
4. Zero-prediction relative-error rows are invalid/missing, not epsilon-regularized.
5. Phase 2B jointly selects `(ell,c)` using `rms_total_error_agg_pre_weight` on `event_time_sample=all`.
6. The three E2 components sum to total squared error for every relevant row and at the cross-organization RMS level.
7. The six known/unknown E2 components sum to total squared error for every relevant row and at the cross-organization RMS level.
8. For `all`, `other_cell_fit=0` and `other_coarsening=0` exactly.
9. Phase 3C does not renormalize predicted non-departed shares.
10. Phase 3C scaled realized shares sum to `1 - hat p_dep` for every valid `(organization, stage, k)`.
11. Main oracle attribution is monotone non-increasing by level and ends at zero.
12. Component-level oracle drops sum in squared-error space to their corresponding main-stage drops.
13. Channel contributions sum to predicted decline to numerical precision.
14. Compile `source/paper/model.tex` and confirm that all new table and figure references resolve.
