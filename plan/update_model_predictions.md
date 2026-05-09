# Rewrite the model_prediction pipeline + restructure §3 of model.tex

## Context

`source/paper/model.tex` was substantially rewritten and now describes:
(a) a baseline stage-decomposition model,
(b) a measurement-and-empirical-implementation section that fits objects on never-treated controls and evaluates predictions on treated organizations.

The existing code in `source/derived/model_prediction/` and `source/analysis/model_prediction/` (852 LOC, 5 scripts) was written against an older draft and now drifts from the paper in three ways:

1. It treats the **latent-problem aggregation** as a fitted object rather than as part of the prediction model itself.
2. Its narrative claims everything is "fit on never-treated," but the actual procedure is "use treated pre-period data to predict treated post-period; use controls only to *choose* the procedure."
3. It does not produce the diagnostics the paper describes (per-stage error distributions across event time, decomposable partition errors, latent-flow normalization plots, oracle attribution that respects the joint $(N, p_j)$ identification limit).

The plan rips the old code, restructures §3.3/§3.4 of the paper, and rebuilds the pipeline with **one shared library of error primitives used everywhere** (this is the user's main structural request — all error metrics in the paper and code derive from the same three primitives, so the connections between fitting metrics, diagnostic metrics, and prediction-error metrics are explicit).

### Pause structure (eight pauses total)

The work is broken into pause-and-inspect units. Before each pause the corresponding tables, plots, and parquets are exported, and the relevant paper subsection is updated; the user then inspects all three before the next unit runs.

| # | Unit | Outputs at pause | Paper subsection(s) |
|---|---|---|---|
| 0 | Phase 0 — rip out old code | (deletions only) | none |
| 1 | Phase 1 — derived (event_time × member) panel | `event_time_member_panel/{repo}.parquet` | none |
| 2 | Phase 2A — fit $\hat N_u$ | `latent_n_selection.tex`, `latent_flow_*.png`, `recommended_config.json` partial | §3.3.5 |
| 3 | Phase 2B — fit estimation sample $\ell^*$ | `estimation_sample_selection.tex`, `share_*.png`, `recommended_config.json` updated | §3.3.6a |
| 4 | Phase 2C — fit contributor partition $c^*$ | `contributor_partition_selection.tex`, `partition_*.png`, `recommended_config.json` complete | §3.3.6b |
| 5 | Phase 3A — count prediction error | `prediction_errors.tex`, `prediction_*.png`, `failure_quantity_errors.png` | §3.4.1 |
| 6 | Phase 3B — stage decomposition (oracle + channel) | `oracle_attribution.tex`, `oracle_attribution_decomposed.tex`, `channel_decomposition.tex`, all `oracle_*.png` and `channel_decomposition_*.png` | §3.4.2 + Appendix §A + Appendix §B |
| 7 | Phase 3C — share-level decomposition | `treated_share_decomposition.tex`, all `share_decomp_*.png` | §3.4.3 |

---

## Notation

- $u$: organization (repo). $\mathcal C$ = control orgs, $\mathcal T$ = treated orgs.
- $k$: event-time period (relative to actual or pseudo-departure), $k \in \{-L, \ldots, -1, 0, \ldots, K\}$.
- $L = 5$: pre-period window length (default; code parameterized so $L \in \{1, \ldots, L_{\max}\}$).
- $K = 5$: post-period window length.
- $s \in \{o, r, m\}$: stage (open, review, merge).
- $i \in \mathcal I_u$: member of org $u$.
- $j$: the departed member at a treated org; pseudo-departed at a control org.
- $A_{u,i,k}^s$: # of stage-$s$ actions taken by member $i$ at org $u$ in period $k$. $A_{u,k}^s \equiv \sum_i A_{u,i,k}^s$.
- $Q_{u,k}^s$: org-level count of stage-$s$ outcomes (PRs opened/reviewed/merged) at $u$ in period $k$. For opens **and merges**, $Q_{u,k}^o = A_{u,k}^o$ and $Q_{u,k}^m = A_{u,k}^m$ (each PR has one merger). For reviews, $Q_{u,k}^r$ = # unique PRs that received any review action; in general $Q_{u,k}^r \leq A_{u,k}^r$ since a PR can be reviewed by multiple people.
- $\hat p_{u,i}^s$: pre-period fitted individual share for member $i$ at org $u$, stage $s$. Always satisfies $\sum_i \hat p_{u,i}^s = 1$.
- $\hat\pi_u^s$, $\hat\pi_u^{s,(-j)}$: aggregated stage-completion probability with all members / with $j$ removed.
- $\hat N_u$: fitted latent-problem flow at org $u$.
- $\mathcal G_{u,c}^s$: contributor partition for org $u$ under partition rule $c$ at stage $s$. A cell is denoted $g$; $g(i)$ is the cell containing $i$; $|g|$ is the cell size.

---

## Error primitives used everywhere

All errors in the rest of the document — fitting selection, diagnostics, prediction error, oracle attribution — are built from three primitives. They are listed once here and referred to by name (E1, E2, E3) throughout the plan; in code they get descriptive names (see "Helpers code" below). Each is implemented once in `source/analysis/model_prediction/helpers.py` and reused everywhere.

**All three primitives are computed per organization $u$.** Every formula below carries an implicit "for org $u$" subscript. Cross-org summaries (means, medians, KDEs) are constructed downstream by the cross-org summary block at the end of this section.

**Why both per-stage normalization in E1 and per-stage weights in E3?** They serve orthogonal purposes that both have to be in place for the cross-stage aggregate to mean anything:
- $\rho_u^d$ in E1 (per-org, per-stage) puts each stage's count error in dimensionless units (fraction of own pre-period mean) so that adding errors across stages doesn't mix "PRs opened" with "PRs reviewed."
- $w_u^s$ in E3 (per-org, per-stage) decides how much each (already-dimensionless) stage error contributes to the org's aggregate, based on how the org allocates activity across stages.

Drop $\rho_u^d$ and you can no longer add stage errors. Drop $w_u^s$ and you treat all stages equally regardless of how active the org is at each.

### E1 — Scaled trajectory error

For a generic predicted trajectory $\hat Z_{u,k}$ and a target trajectory $Z_{u,k}$ over an evaluation window $\mathcal K \subseteq \{0, \ldots, K\}$, with a per-org normalizing scale $\rho_u$:

$$
\mathcal E_1(\hat Z, Z; \mathcal K, \rho_u) = \left[\frac{1}{|\mathcal K|}\sum_{k \in \mathcal K}\bigl(\rho_u (\hat Z_{u,k} - Z_{u,k})\bigr)^2\right]^{1/2}.
$$

Two flavors used:
- **Pooled across post-period:** $\mathcal K = \{0, \ldots, K\}$ — labeled `event_time_sample = "all"` in tables.
- **Per single $k$:** $\mathcal K = \{k\}$ — labeled `event_time_sample = "k=0"`, `"k=1"`, …, `"k=K"`.

Two normalizations used:
- **Normalized:** $\rho_u = 1/\bar Q_{u,\mathrm{pre}}^d$ for count-valued targets, where $\bar Q_{u,\mathrm{pre}}^d = \frac{1}{L}\sum_{k=-L}^{-1} Q_{u,k}^d$ is the per-org, per-stage pre-period mean. Makes errors comparable across orgs of different sizes and across stages with different volumes.
- **Raw:** $\rho_u = 1$. Reported alongside as the unnormalized scalar.

When the predicted object is a single scalar (e.g. $\hat N_u$ rather than a trajectory), set $\hat Z_{u,k} = \hat Z_u$ for all $k \in \mathcal K$ in the formula above — the prediction is constant in $k$ but the target $Z_{u,k}$ still varies.

### E2 — Decomposable share-vector error (per `plan/decomposition.md`)

Inputs: a pre-period fitted individual share vector $\hat p_{u,\cdot}^{s,\ell}$, a partition mapping $\mathcal G_{u,c}^s$ with cells indexed by $g$ (cell containing member $i$ is $g(i)$, cell size $|g|$), and an observed individual share trajectory $\tilde p_{u,\cdot,k}^s$ over an evaluation window $\mathcal K$.

The three E2 components are:

$$
\boxed{\,\mathcal E_2^{\mathrm{total}}(\hat p, \tilde p, c; \mathcal K) = \left[\frac{1}{|\mathcal K|}\sum_{k \in \mathcal K}\sum_{i} \left(\frac{\sum_{i' \in g(i)} \hat p_{u,i'}^{s,\ell}}{|g(i)|} - \tilde p_{u,i,k}^s\right)^2\right]^{1/2}\,}
$$

$$
\mathcal E_2^{\mathrm{cell\text{-}fit}}(\hat p, \tilde p, c; \mathcal K) = \left[\frac{1}{|\mathcal K|}\sum_{k \in \mathcal K}\sum_{g} \frac{\bigl(\sum_{i \in g} \hat p_{u,i}^{s,\ell} - \sum_{i \in g}\tilde p_{u,i,k}^s\bigr)^2}{|g|}\right]^{1/2}
$$

$$
\mathcal E_2^{\mathrm{coarsening}}(\hat p, \tilde p, c; \mathcal K) = \left[\frac{1}{|\mathcal K|}\sum_{k \in \mathcal K}\sum_{g}\sum_{i \in g} \left(\frac{\sum_{i' \in g}\tilde p_{u,i',k}^s}{|g|} - \tilde p_{u,i,k}^s\right)^2\right]^{1/2}
$$

**What each piece represents (the two decomposed ones):**

- $\mathcal E_2^{\mathrm{cell\text{-}fit}}$ — *"are the cell totals right?"* For each cell, we compare the fitted total share for that cell (sum of pre-period individual shares of members in the cell) against the observed total share for that cell (sum of post-period individual shares). Squared error is divided by $|g|$ so that each cell's contribution is expressed at the individual scale (it's a per-individual error attributable to the cell-total mistake, then summed over the cell). High cell-fit error means our partition gets the broad allocation of activity wrong.
- $\mathcal E_2^{\mathrm{coarsening}}$ — *"within a cell, how heterogeneous are the actual post-period shares?"* For each cell, compare the within-cell mean of observed individual shares to each individual's observed share. Computed entirely from observed data — no fitted quantity appears. Captures the intrinsic information loss from treating members in a cell as exchangeable. High coarsening error means the partition lumps together members with very different post-period activity.

The exact identity (proven in `plan/decomposition.md`, included as an appendix of `source/paper/model.tex`):

$$(\mathcal E_2^{\mathrm{total}})^2 = (\mathcal E_2^{\mathrm{cell\text{-}fit}})^2 + (\mathcal E_2^{\mathrm{coarsening}})^2.$$

Contribution shares (sum to 1 when total > 0):

$$\mathrm{Share}^{\mathrm{cell\text{-}fit}} = \frac{(\mathcal E_2^{\mathrm{cell\text{-}fit}})^2}{(\mathcal E_2^{\mathrm{total}})^2}, \qquad \mathrm{Share}^{\mathrm{coarsening}} = \frac{(\mathcal E_2^{\mathrm{coarsening}})^2}{(\mathcal E_2^{\mathrm{total}})^2}.$$

**Known-vs-unknown decomposition of E2 total** (used in Phase 3 §3 to surface "we got lucky" vs. "we missed people we didn't know would be there"):

For stage $s$ at org $u$, define member status:
- Knowns $\mathcal I_u^{\text{known}}(s) = \{i \in \mathcal I_u : \hat p_{u,i}^{s,\ell} > 0\}$ — members our pre-period fit assigned positive share to.
- Unknowns $\mathcal I_u^{\text{unknown}}(s) = \mathcal I_u \setminus \mathcal I_u^{\text{known}}(s)$ — members the fit assigned zero share to (typically because they had no pre-period activity at stage $s$, often because they didn't appear in the org pre-departure at all).

Because E2 total is a sum of squared per-member residuals, it splits orthogonally by member status:

$$(\mathcal E_2^{\mathrm{total}})^2 = (\mathcal E_2^{\mathrm{known}})^2 + (\mathcal E_2^{\mathrm{unknown}})^2$$

where

$$\mathcal E_2^{\mathrm{known}} = \left[\frac{1}{|\mathcal K|}\sum_{k \in \mathcal K}\sum_{i \in \mathcal I_u^{\text{known}}(s)} \left(\frac{\sum_{i' \in g(i)} \hat p_{u,i'}^{s,\ell}}{|g(i)|} - \tilde p_{u,i,k}^s\right)^2\right]^{1/2}$$

$$\mathcal E_2^{\mathrm{unknown}} = \left[\frac{1}{|\mathcal K|}\sum_{k \in \mathcal K}\sum_{i \in \mathcal I_u^{\text{unknown}}(s)} \left(\frac{\sum_{i' \in g(i)} \hat p_{u,i'}^{s,\ell}}{|g(i)|} - \tilde p_{u,i,k}^s\right)^2\right]^{1/2}$$

**Interpretation.** A small overall E2 total can hide that we got lucky on knowns but completely missed a cohort of new contributors. The known/unknown split surfaces this: high $(\mathcal E_2^{\mathrm{unknown}})^2 / (\mathcal E_2^{\mathrm{total}})^2$ means most of our share-fit error is from people we didn't know would matter. Low means our error is concentrated on members we tried to predict and got wrong.

(The cell-fit and coarsening pieces individually do *not* split orthogonally by member status — cross terms between knowns and unknowns within a cell don't vanish — so we report known/unknown only on E2 total.)

Special cases:

- **Trivial partition** ($c = $ `all`, every contributor a singleton): $\mathcal E_2^{\mathrm{coarsening}} = 0$ and $\mathcal E_2^{\mathrm{total}} = \mathcal E_2^{\mathrm{cell\text{-}fit}} = [\frac{1}{|\mathcal K|}\sum_k\sum_i (\hat p_i - \tilde p_{i,k})^2]^{1/2}$, the simple individual L2 distance. *Caveat:* the mechanical $\mathcal E_2^{\mathrm{coarsening}} = 0$ for `all` is structural — singletons trivially have zero within-cell variance regardless of which data defined the partition. The known/unknown decomposition above is what actually distinguishes `all`'s genuine cost from its mechanical advantage.
- **Renormalization for treated orgs (Phase 3 §3):** $\hat p_{u,i}^s$ is replaced by the renormalized share $\hat p_{u,i}^s / (1 - \hat p_{u,j}^s)$ over remaining members $i \neq j$, and $\tilde p_{u,i,k}^s = A_{u,i,k}^s / A_{u,k}^s$ is the post-departure observed share over remaining members (which sums to 1 by construction since $j$ contributes zero post-departure). Both vectors live on the support $\mathcal I_u \setminus \{j\}$.

`event_time_sample` flavor (single $k$ vs. pooled across $\{0,\ldots,K\}$) follows the same convention as E1.

### E3 — Cross-stage weighted aggregate

For per-stage errors $\{\mathcal E^o, \mathcal E^r, \mathcal E^m\}$ (any of E1 or E2), aggregate to a single org-level scalar:

$$
\mathcal E_3(\{\mathcal E^s\}; w) = \left[\sum_{s \in \{o,r,m\}} w_u^s (\mathcal E^s)^2\right]^{1/2}.
$$

Two weight schemes, both reported:

- **Pre-event activity weight:** $w_{u,\mathrm{pre}}^s = \dfrac{\sum_{k=-L}^{-1} A_{u,k}^s}{\sum_{s'}\sum_{k=-L}^{-1} A_{u,k}^{s'}}$. Reflects how org $u$ allocated its activity across stages **before** the (real or pseudo) departure. Useful when we want the aggregate to reflect what the org was doing historically.
- **Post-event activity weight:** $w_{u,\mathrm{post}}^s = \dfrac{\sum_{k=0}^{K} A_{u,k}^s}{\sum_{s'}\sum_{k=0}^{K} A_{u,k}^{s'}}$. Reflects what the org is doing **after** the departure. Useful when we want the aggregate to reflect the regime the prediction lives in.

Reporting both lets the reader see whether selection or evaluation conclusions are sensitive to which weight one cares about.

### Cross-org summary

Per-org errors $\mathcal E_u$ are themselves square roots of squared-error sums, so the natural cross-org aggregate is the **root-mean-square (RMS)** rather than the mean — RMS preserves the squared-error decomposition identities (e.g. cell-fit² + coarsening² = total², known² + unknown² = total²) at the cross-org level, which the simple mean does not.

For the relevant org set ($\mathcal C$ for fitting, $\mathcal T$ for evaluation):

$$
\overline{\mathcal E}^{\mathrm{RMS}} = \left[\frac{1}{|\mathcal C|}\sum_{u \in \mathcal C} (\mathcal E_u)^2\right]^{1/2}, \qquad \widetilde{\mathcal E} = \mathrm{median}_{u \in \mathcal C}\, \mathcal E_u.
$$

Selection rules in §3.3.5 / §3.3.6 minimize $\overline{\mathcal E}^{\mathrm{RMS}}$. Median is reported alongside as a robust check (when median and RMS disagree sharply, a few outlier orgs are driving the RMS — diagnostically informative).

**Notation in the rest of this doc.** When tables and selection rules below say "$\overline{\mathcal R}$" they mean $\overline{\mathcal R}^{\mathrm{RMS}}$ unless explicitly stated otherwise.

---

## Phase 0 (one shot): rip out the old code

Delete the following so nothing stale lingers (CLAUDE.md §0: no ghosts):

- `source/derived/model_prediction/prepare_contributor_actions.py`
- `source/derived/model_prediction/SConscript`
- `source/analysis/model_prediction/{prepare_contributor_panel,estimate_model_probabilities,compute_predictions,evaluate_fit,model_utils}.py`
- `source/analysis/model_prediction/SConscript`
- All previously-emitted outputs under `output/derived/model_prediction/`, `output/analysis/model_prediction/`, `drive/output/derived/model_prediction/`, `drive/output/analysis/model_prediction/`.

Update parent `SConscript`s to drop the now-deleted subdirs from their `SConscript()` calls.

**→ PAUSE for user inspection of what's about to be deleted.** After approval, commit as a separate commit: `rip out old model_prediction pipeline #<issue>`.

---

## Phase 1 — Derived dataset: per-repo (event_time × member) panel

**Code only. No paper edits.**

### What to build

For every repo with a file under `drive/output/derived/action_data/repo_actions/`, produce a per-repo parquet keyed by `(quasi_event_time, actor_id)` with these columns:

| Column | Definition |
|---|---|
| `member_pull_request_opened` | $A_{u,i,k}^o$: # unique PRs the actor opened in that period |
| `member_pull_request_reviewed` | $A_{u,i,k}^r$: # unique PRs the actor reviewed (`type ∈ REVIEW_TYPES`) in that period |
| `member_pull_request_merged` | $A_{u,i,k}^m$: # unique PRs the actor merged in that period |
| `repo_pull_request_opened` | $Q_{u,k}^o = A_{u,k}^o$ |
| `repo_pull_request_reviewed` | $Q_{u,k}^r$: # unique PRs reviewed (≤ $\sum_i A_{u,i,k}^r$) |
| `repo_pull_request_merged` | $Q_{u,k}^m = A_{u,k}^m$ |

**Member universe:** any `actor_id` with at least one PR-stage action in any period from `quasi_event_time = -5` through `+5`. Bots excluded via `LoadBotList()`. Grid is rectangular over included members and `quasi_event_time ∈ {-5,...,+5}`; missing actor-period cells get zeros for the three `member_*` columns; the three `repo_*` columns are constant across members within a period.

**Inputs:**
- `drive/output/derived/action_data/repo_actions/{repo}.parquet`
- For each `(importance_type, rolling_period, qualified_sample, control_group)` combo from `pipeline_inputs.json`: `output/analysis/data_prep/{...}/panel.parquet` to recover `quasi_treatment_group` and the `quasi_event_time → time_period` mapping.

**Outputs (one per repo per config combo), saved via `SaveData`:**
- Data: `drive/output/derived/model_prediction/event_time_member_panel/{importance_type}/{rolling_period}/{qualified_sample}/{control_group}/{repo}.parquet`
- Log: `output/derived/model_prediction/event_time_member_panel/{importance_type}/{rolling_period}/{qualified_sample}/{control_group}/{repo}.log`

### Why this lives in `derived/`

Deterministic transformation of action data + the panel's event-time mapping. No model choices. No fitting. Reusable by every downstream model script.

### Files to write

- `source/derived/model_prediction/build_event_time_member_panel.py` — `Main()` discovers all repos × all config combos and parallelizes via `joblib`. Reuses `ImputeTimePeriod`, `LoadBotList`, `MakeRepoNameSafe`. PR-action aggregation logic lifts from the old `AggregateContributorCounts` / `AggregateOrgCounts`.
- `source/derived/model_prediction/SConscript` — follows the **reference-repo sentinel** pattern from `source/derived/org_outcomes_practices/SConscript:8`: SCons targets and sources name only the reference repo's input parquet; the Python script discovers and processes all repos at runtime in parallel. One `env.Python()` call per `(importance × rolling × sample × control)` combo.

### Verification

1. `scons drive/output/derived/model_prediction/event_time_member_panel/...` runs clean for the reference repo.
2. Spot-check one repo's parquet: (a) actor universe matches the union of PR-stage actors in $[-5, +5]$, (b) `repo_*` columns are constant within `quasi_event_time`, (c) $\sum_i A_{u,i,k}^o = Q_{u,k}^o$ and $\sum_i A_{u,i,k}^m = Q_{u,k}^m$ exactly; $\sum_i A_{u,i,k}^r \geq Q_{u,k}^r$.
3. Row count = $|\text{members}| \times 11$.

**→ PAUSE for user inspection.**

---

## Phase 2 — Fitting counterfactual inputs (paper §3.3 + code + outputs)

### Restructured paper outline of §3.3

> **§3.3.1 Prediction model.** The actual prediction we generate for treated org $u$ at post-departure period $k$:
> $$\hat Q_{u,k}^{o,(-j)} = \hat N_u \hat\pi_u^{o,(-j)}, \quad \hat Q_{u,k}^{r,(-j)} = \hat N_u \hat\pi_u^{o,(-j)} \hat\pi_u^{r,(-j)}, \quad \hat Q_{u,k}^{m,(-j)} = \hat N_u \hat\pi_u^{o,(-j)} \hat\pi_u^{r,(-j)} \hat\pi_u^{m,(-j)}.$$
> Two ingredients: (a) $\hat N_u$, (b) $\hat\pi_u^{s,(-j)}$ derived from individual shares $\hat p_{u,i}^s$ via the latent-problem model.
>
> **§3.3.2 Latent-problem model.** Aggregation identity (this is *the model*, not a fitted object):
> - Opening: under share-normalized opening, $\hat\pi_u^o = \sum_i \hat p_{u,i}^o = 1$, with $\hat\pi_u^{o,(-j)} = 1 - \hat p_{u,j}^o$.
> - Review and merge: independent-Bernoulli, $\hat\pi_u^{s,(-j)} = 1 - \prod_{i \neq j}(1 - \hat p_{u,i}^s)$.
>
> **§3.3.3 Error primitives.** Restate E1, E2, E3 from above (with cross-references to fitting and evaluation sections that use them).
>
> **§3.3.4 Procedure for fitting on the treated pre-period.** "In production we fit $\hat N_u$ and $\hat p_{u,i}^s$ from the pre-period of the treated org. To choose *how* to fit, we run candidates on never-treated orgs (which have post-period data under a pseudo-departure date) and pick the candidate with smallest cross-control error."
>
> **§3.3.5 Fitting $\hat N_u$.** Candidates and selection — math below using E1.
>
> **§3.3.6 Fitting $\hat p_{u,i}^s$ (and the partition over which it is defined).** Two coupled choices: (a) the pre-period estimation sample $\ell$, (b) the contributor partition $c$. The partition uses the chosen sample; we discuss them in that order. Math below using E2 + E3.
>
> **§3.3.7 Selection table and figures.** Reference outputs the code produces.

### §3.3.5 — Fitting $\hat N_u$ (math, in terms of E1)

**All selection in §3.3.5 and §3.3.6 is computed across the never-treated control set $\mathcal C$ only.** Treated organizations are held out from the selection procedure and used only at evaluation time (Phase 3).

Two candidates:
- $\hat N_u^{\mathrm{avg}} = \frac{1}{L}\sum_{k=-L}^{-1} Q_{u,k}^o$
- $\hat N_u^{\mathrm{last}} = Q_{u,-1}^o$

**Selection metric.** Apply E1 with target = $Q_{u,k}^o$ over post-periods (under the pseudo-departure date), predicted scalar = $\hat N_u$:
$$\mathcal R_u^N(\hat N; \mathcal K) = \mathcal E_1(\hat N_u, Q_{u,k}^o; \mathcal K, \rho_u = 1/\bar Q_{u,\mathrm{pre}}^o).$$

Reported $\mathcal K$ values: `all` (= $\{0,\ldots,K\}$) and each individual `k=0,...,K`. Raw RMSE ($\rho = 1$) reported alongside the normalized version.

Selection rule: $\hat N^* = \arg\min_{\hat N \in \{\mathrm{avg}, \mathrm{last}\}} \overline{\mathcal R^N}^{\mathrm{RMS}}(\hat N; \mathcal K = \mathrm{all})$, with the RMS taken over $\mathcal C$.

### §3.3.6a — Estimation sample $\hat p_{u,i}^{s,\ell}$ (math, in terms of E2 + E3)

For each window length $\ell \in \{1, \ldots, L\}$:
$$\hat p_{u,i}^{s,\ell} = \frac{\sum_{k=-\ell}^{-1} A_{u,i,k}^s}{\sum_{k=-\ell}^{-1} A_{u,k}^s}, \quad \sum_i \hat p_{u,i}^{s,\ell} = 1.$$

Code is parameterized over arbitrary $\ell$; paper reports $\ell = L = 5$ (`pooled_L5`) and $\ell = 1$ (`last`).

**Selection metric.** Apply E2 with the **trivial `all` partition** (every contributor a singleton). E2 reduces to the simple individual-level L2 distance:
$$\mathcal R_u^{p,s,\ell}(\mathcal K) = \mathcal E_2^{\mathrm{total}}(\hat p_{u,\cdot}^{s,\ell}, \tilde p_{u,\cdot,k}^s, \mathrm{all}; \mathcal K), \qquad \tilde p_{u,i,k}^s = \frac{A_{u,i,k}^s}{A_{u,k}^s}.$$

Aggregate across stages via E3 under both pre- and post-event weights:
$$\mathcal R_u^{p,\mathrm{agg},\ell,w}(\mathcal K) = \mathcal E_3(\{\mathcal R_u^{p,s,\ell}(\mathcal K)\}_s; w).$$

Reported $\mathcal K$ values: `all` plus each `k=0,...,K`.

Selection: $\ell^* = \arg\min_\ell \overline{\mathcal R^{p,\mathrm{agg},\ell, w_{\mathrm{pre}}}}(\mathcal K = \mathrm{all})$, with the post-event weighted version reported as a robustness check.

### §3.3.6b — Contributor partition (math, in terms of E2 + E3)

**Member universe over which the partition is defined: union of pre-period and post-period contributors at the org.** This matters because it ensures new post-period members appear in the partition explicitly — under `all` they show up as their own singleton cell with $\hat p_i = 0$ (so the cell-fit error properly accounts for them); under any partition with an "other" cell they get absorbed there. Defining the partition over pre-period contributors only would silently drop new members from the error metric and understate prediction error for orgs that grow.

Top-$K$ selection rules use **pre-period activity only** to rank (so new post-period members never qualify as top-$K$ — their pre-period activity is 0). The union-with-post-period change above only affects which members appear in "other" (or as zero-share singletons under `all`), not who is individually tracked.

Five candidates (renamed `cov80 → cover80p`): Also note that infrastructure for 80p should be built such that if i want to vary 80 to something else its easy. same with top5/top5_per_stage. 

| Candidate | Definition |
|---|---|
| `all` | every contributor in the pre+post union is their own singleton cell; no "other" pool |
| `top5` | top 5 contributors by pre-period activity *across all stages combined*; rest (incl. new post members) into "other" |
| `top5_per_stage` | union of stage-specific top-5 sets by pre-period activity; rest into "other" |
| `cover80p` | smallest top-$K$ covering 80% of all-stage *pre-period* activity; rest into "other" |
| `cover80p_per_stage` | union of stage-specific top-$K$ sets each covering 80% of that stage's *pre-period* activity; rest into "other" |

**Each candidate inherits the chosen estimation sample $\ell^*$.** Full code/table name: `<partition>__pooled_L{ℓ*}` or `<partition>__last`, e.g. `top5__pooled_L5`. Dependency made explicit.

**Selection metric** = the **decomposable total error** from E2 (per `plan/decomposition.md`, this is the headline change from the prior draft):
$$\mathcal R_u^{p,s,c,\mathrm{total}}(\mathcal K) = \mathcal E_2^{\mathrm{total}}(\hat p_{u,\cdot}^{s,\ell^*}, \tilde p_{u,\cdot,k}^s, c; \mathcal K).$$

Aggregate across stages via E3:
$$\mathcal R_u^{p,\mathrm{agg},c,w}(\mathcal K) = \mathcal E_3(\{\mathcal R_u^{p,s,c,\mathrm{total}}(\mathcal K)\}_s; w).$$

Selection: $c^* = \arg\min_c \overline{\mathcal R^{p,\mathrm{agg},c,w_{\mathrm{pre}}}}^{\mathrm{RMS}}(\mathcal K = \mathrm{all})$, RMS taken over $\mathcal C$.

**Decomposition reported alongside.** For each candidate, also report:

- $\mathcal R_u^{p,s,c,\mathrm{cell\text{-}fit}}(\mathcal K) = \mathcal E_2^{\mathrm{cell\text{-}fit}}(\cdot)$ — error from getting cell totals wrong.
- $\mathcal R_u^{p,s,c,\mathrm{coarsening}}(\mathcal K) = \mathcal E_2^{\mathrm{coarsening}}(\cdot)$ — error from grouping heterogeneous contributors.
- Cross-org mean shares $\overline{\mathrm{Share}^{\mathrm{cell\text{-}fit}}}$ and $\overline{\mathrm{Share}^{\mathrm{coarsening}}}$ — interpretation: "this partition fails 70% because cell totals are off, 30% because of coarsening loss."

The within-"other" individual error from the prior draft is dropped: it's strictly dominated by the global $\mathcal E_2^{\mathrm{coarsening}}$, which sums squared individual-vs-cell-mean residuals across **all** cells (singleton cells contribute 0, so for top-K partitions essentially all coarsening comes from "other" anyway).

The same E2 (with its decomposition) is reused in Phase 3 §2 on treated orgs, with the renormalization noted in the E2 definition above.

**On the `all` partition's mechanical $\mathcal R^{\mathrm{coarsening}} = 0$:** structural — singleton cells have zero within-cell variance regardless of which data defined the partition, so we cannot remove this advantage by reformulating the metric. What we *can* do is rely on the **known/unknown decomposition** (defined in E2) to surface what `all` genuinely costs: under `all`, every new post-period member contributes $\tilde p_i^2$ entirely to $\mathcal E_2^{\text{unknown}}$, since their fitted singleton share is 0. So an org with lots of new post-period members will have a large unknown-share contribution under `all`, even though coarsening is 0. The plots below report total error, the cell-fit/coarsening split, AND the known/unknown split; together they tell a complete story without `all` getting a free pass on novelty.

### Output tables — `output/analysis/model_prediction/.../tables/`

Three separate `.tex` files, one per sub-phase, so the user can inspect each at its own pause point. Cells = $\overline{\mathcal R}^{\mathrm{RMS}}$ over $\mathcal C$; medians and per-$k$ values live in the backing per-sub-phase parquets.

**File 1: `latent_n_selection.tex` (sub-phase 2A) — Panel A.**

| Candidate | event_time_sample | $\overline{\mathcal R^N}_{\mathrm{norm}}$ | $\overline{\mathcal R^N}_{\mathrm{raw}}$ | Selected |
|---|---|---|---|---|
| `avg_L` | all | … | … | ✓/blank |
| `avg_L` | k=0 | … | … | |
| `avg_L` | k=1 | … | … | |
| … | … | … | … | |
| `avg_L` | k=K | … | … | |
| `last` | all | … | … | ✓/blank |
| `last` | k=0 | … | … | |
| … | … | … | … | |

The "Selected" check appears only on the `event_time_sample = all` row of the winning candidate; selection is decided on that row. Per-$k$ rows are diagnostic.

**File 2: `estimation_sample_selection.tex` (sub-phase 2B) — Panel B (uses the `all` partition for selection).**

| Candidate | event_time_sample | $\overline{\mathcal R^{p,o,\ell}}$ | $\overline{\mathcal R^{p,r,\ell}}$ | $\overline{\mathcal R^{p,m,\ell}}$ | $\overline{\mathcal R^{p,\mathrm{agg},\ell,w_\mathrm{pre}}}$ | $\overline{\mathcal R^{p,\mathrm{agg},\ell,w_\mathrm{post}}}$ | Selected |
|---|---|---|---|---|---|---|---|
| `pooled_L5` | all | … | … | … | … | … | ✓/blank |
| `pooled_L5` | k=0 | … | … | … | … | … | |
| … | … | … | … | … | … | … | |
| `last` | all | … | … | … | … | … | ✓/blank |
| … | … | … | … | … | … | … | |

**File 3: `contributor_partition_selection.tex` (sub-phase 2C) — Panel C (inheriting $\ell^*$).** Headline error is decomposable. Per-$k$ rows included for total only (per-$k$ cell-fit / coarsening / known / unknown live in the parquet for plotting; otherwise the panel becomes too wide).

All cells = $\overline{\mathcal R}^{\mathrm{RMS}}$ taken over $\mathcal C$.

| Candidate | event_time_sample | total ($w_\mathrm{pre}$) | total ($w_\mathrm{post}$) | cell-fit | coarsening | Share$^{\mathrm{cell\text{-}fit}}$ | Share$^{\mathrm{coarsening}}$ | known | unknown | Share$^{\mathrm{known}}$ | Share$^{\mathrm{unknown}}$ | Selected |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `all__<ℓ*>` | all | … | … | … | 0.000 | 1.000 | 0.000 | … | … | … | … | ✓/blank |
| `all__<ℓ*>` | k=0 | … | … | … | 0.000 | 1.000 | 0.000 | … | … | … | … | |
| … | … | … | … | … | … | … | … | … | … | … | … | |
| `top5__<ℓ*>` | all | … | … | … | … | … | … | ✓/blank |
| `top5__<ℓ*>` | k=0 | … | … | … | … | … | … | |
| … | … | … | … | … | … | … | … | |

Each sub-phase has its own backing parquet (`latent_n_selection.parquet`, `estimation_sample_selection.parquet`, `contributor_partition_selection.parquet`) keeping every per-org, per-candidate, per-$k$, per-component value for plotting and the recommendation file. These live in `drive/output/analysis/model_prediction/.../`.

### Output plots — `output/analysis/model_prediction/.../plots/`

KDEs by default. `event_time_sample` distinguishes pooled vs. per-$k$ where applicable. Plots grouped by sub-phase.

**Sub-phase 2A:**
1. `latent_flow_N_fan.png` — fan chart of $N$ over event time across all control orgs: bold median + IQR (25-75%) shaded + 10-90% shaded lighter + low-alpha (~0.03) per-org spaghetti behind. Vertical reference line at $k=0$.
2. `latent_flow_error_dist.png` — KDE of $\mathcal R_u^N(\mathrm{all})$ across orgs, one curve per candidate $\hat N \in \{\text{avg}, \text{last}\}$.
3. `latent_flow_error_by_k.png` — KDE of $\mathcal R_u^N(\{k\})$ at each $k$, faceted by candidate.

**Sub-phase 2B:**
4. `share_error_dist.png` — KDE of $\mathcal R_u^{p,\mathrm{agg},\ell,w_\mathrm{pre}}(\mathrm{all})$ across orgs, one curve per candidate $\ell$.
5. `share_error_by_k.png` — KDE per $k$, faceted by candidate $\ell$.

**Sub-phase 2C:**
6. `partition_total_error_dist.png` — KDE of $\mathcal R_u^{p,\mathrm{agg},c,\mathrm{total},w_\mathrm{pre}}(\mathrm{all})$, one curve per candidate $c$.
7. `partition_decomposition_dist.png` — three faceted KDEs (total, cell-fit, coarsening), one curve per candidate.
8. `partition_decomposition_share_bar.png` — stacked bar chart per candidate: cross-org cell-fit share vs. coarsening share (squared shares; sum to 1).
9. `partition_known_unknown_dist.png` — two faceted KDEs (known-share error, unknown-share error), one curve per candidate.
10. `partition_known_unknown_share_bar.png` — stacked bar chart per candidate: cross-org known share vs. unknown share. The headline visual comparing `all` against top-K candidates.
11. `partition_total_error_by_k.png` — KDE of total error per $k$, faceted by candidate.

### Friction-bound chosen-parameters config

Phase 2 code writes `output/analysis/model_prediction/{...}/recommended_config.json` every run with auto-selected winners:
```json
{
  "latent_flow_rule": "avg_L" | "last",
  "individual_share_estimator": {"type": "pooled" | "last", "L": 5},
  "contributor_partition": "all" | "top5" | "top5_per_stage" | "cover80p" | "cover80p_per_stage",
  "winner_errors": { ... per-input cross-control aggregate ... }
}
```

Code **never touches** `source/lib/`. The user reads the recommendation + table + plots, then by hand writes/edits `source/lib/config/model_prediction.json` with the same schema. Phase 3 reads only from that file via a new `LoadModelPredictionConfig()` in `source/lib/python/config_loaders.py`.

### Code structure: split into three pause-and-inspect sub-phases

Phase 2 is split into **2A / 2B / 2C**, one per fitted input. Each sub-phase has its own pair of scripts (analytic outputs + plots), produces its own table file and its own plots, and updates `recommended_config.json` in place. **The user pauses after each sub-phase to inspect outputs and the corresponding paper subsection before the next sub-phase runs.**

Sub-phases inherit upstream choices: 2C reads $\ell^*$ from the (partially-written) `recommended_config.json` produced by 2B; 2B does not depend on 2A but is paused on independently.

### Shared helpers module (lives across all phases)

`source/analysis/model_prediction/helpers.py` — implements the three error primitives, the cross-org RMS aggregator, and partition utilities. **Function names are descriptive — E1/E2/E3 are doc-only labels, not code names.**

- `count_squared_error(pred_traj, obs_traj, K_set, rho)` — implements E1; per-org scalar.
- `share_squared_error(p_hat_individual, p_obs_individual_traj, partition_mapping, K_set)` — implements E2; returns dict `{total, cell_fit, coarsening, share_cell_fit, share_coarsening, known, unknown, share_known, share_unknown}`.
- `weighted_aggregate(per_stage_errors, weights)` — E3.
- `cross_org_rms(per_org_values)` — $\overline{\mathcal E}^{\mathrm{RMS}}$. Used wherever cross-org aggregation appears.
- `build_partition(panel, rule, sample, L, member_universe)` — contributor-to-cell identity mapping; `member_universe` is the pre+post union; top-K uses pre-period activity to rank.
- `apply_partition_mapping(shares, mapping)` — apply existing mapping to a fresh share vector (Phase 3 reuse).
- `compute_individual_shares(panel, sample, L)` — generic over $\ell$.
- `estimate_latent_n(panel, rule, L)` for `rule ∈ {"avg_L", "last"}`.
- `renormalize_for_treated(p_hat, j)` — divides by $(1 - \hat p_j)$ over remaining members.
- `cell_fit_corrected_shares(p_obs_individual, partition_mapping)` — Phase 3 Appendix §A.
- `independent_bernoulli_aggregation(individual_shares, exclude=None)` — wherever $\hat\pi^s$ is computed.

### Sub-phase 2A — Fit $\hat N_u$ (paper §3.3.5)

**Scripts** (one analytic + one plot):
- `source/analysis/model_prediction/fit_latent_n.py` — reads event_time_member panels for $\mathcal C$; computes $\mathcal R_u^N(\hat N; \mathcal K)$ for both candidates × all `event_time_sample` values; writes `latent_n_selection.parquet` (per-org per-candidate per-$k$) and `latent_n_selection.tex` (Panel A from the table spec); updates `recommended_config.json` with `latent_flow_rule`.
- `source/analysis/model_prediction/plot_latent_n.py` — reads the parquet; writes `latent_flow_N_fan.png`, `latent_flow_error_dist.png`, `latent_flow_error_by_k.png`.

**Outputs to inspect at pause:**
- `output/analysis/model_prediction/{...}/tables/latent_n_selection.tex`
- `output/analysis/model_prediction/{...}/plots/latent_flow_*.png`
- `output/analysis/model_prediction/{...}/recommended_config.json` (`latent_flow_rule` populated)
- Paper draft updates to §3.3.5

**Verification:**
1. Pipeline runs clean for one config combo on the reference repo set.
2. The "Selected" row in `latent_n_selection.tex` has the strictly smallest cross-control RMS in its panel.
3. `recommended_config.json.latent_flow_rule` matches the "Selected" row.
4. Plots open and are readable.

**→ PAUSE.** User inspects outputs + §3.3.5 paper draft. Approves before 2B begins.

### Sub-phase 2B — Fit estimation sample $\ell^*$ (paper §3.3.6a)

**Scripts:**
- `source/analysis/model_prediction/fit_estimation_sample.py` — reads event_time_member panels for $\mathcal C$; computes $\mathcal R_u^{p,s,\ell}$ via `share_squared_error` with the trivial `all` partition for every $\ell \in \{1, \ldots, L\}$ × stage × `event_time_sample`; aggregates via `weighted_aggregate` under both weight schemes; writes `estimation_sample_selection.parquet` and `estimation_sample_selection.tex` (Panel B); updates `recommended_config.json` with `individual_share_estimator`.
- `source/analysis/model_prediction/plot_estimation_sample.py` — writes `share_error_dist.png`, `share_error_by_k.png`.

**Outputs to inspect at pause:**
- `output/analysis/model_prediction/{...}/tables/estimation_sample_selection.tex`
- `output/analysis/model_prediction/{...}/plots/share_*.png`
- `output/analysis/model_prediction/{...}/recommended_config.json` (`individual_share_estimator` now populated)
- Paper draft updates to §3.3.6a

**Verification:**
1. Pipeline runs clean.
2. "Selected" row matches the smallest cross-control RMS aggregate.
3. **E2 identities** hold for the trivial partition (cell-fit² = total², coarsening² = 0, known² + unknown² = total²). Hard assertion in code.
4. Plots open.

**→ PAUSE.** User inspects outputs + §3.3.6a paper draft. Approves before 2C begins.

### Sub-phase 2C — Fit contributor partition $c^*$ (paper §3.3.6b)

**Scripts:**
- `source/analysis/model_prediction/fit_contributor_partition.py` — reads `recommended_config.json` to get $\ell^*$; computes `share_squared_error` for all five partition candidates × stage × `event_time_sample`, including all decomposition components (cell-fit, coarsening, known, unknown, shares); writes `contributor_partition_selection.parquet` and `contributor_partition_selection.tex` (Panel C with the wide column set including known/unknown); updates `recommended_config.json` with `contributor_partition`.
- `source/analysis/model_prediction/plot_contributor_partition.py` — writes `partition_total_error_dist.png`, `partition_decomposition_dist.png`, `partition_decomposition_share_bar.png`, `partition_known_unknown_dist.png`, `partition_known_unknown_share_bar.png`, `partition_total_error_by_k.png`.

**Outputs to inspect at pause:**
- `output/analysis/model_prediction/{...}/tables/contributor_partition_selection.tex`
- `output/analysis/model_prediction/{...}/plots/partition_*.png`
- `output/analysis/model_prediction/{...}/recommended_config.json` (fully populated)
- Paper draft updates to §3.3.6b

**Verification:**
1. Pipeline runs clean.
2. "Selected" row matches smallest cross-control RMS total.
3. **E2 cell-fit/coarsening identity** ($\sqrt{(\mathcal E_2^{\mathrm{cell\text{-}fit}})^2 + (\mathcal E_2^{\mathrm{coarsening}})^2} = \mathcal E_2^{\mathrm{total}}$) holds at every (org, candidate, stage) triple AND at the cross-org RMS level.
4. **E2 known/unknown identity** holds analogously.
5. For the `all` candidate, $\mathcal E_2^{\mathrm{coarsening}} = 0$ exactly.
6. Plots open.

**→ PAUSE.** User inspects outputs + §3.3.6b paper draft. User then manually promotes `recommended_config.json` values into `source/lib/config/model_prediction.json` (the friction-bound config Phase 3 reads).

### SConscript wiring (Phase 2)

`source/analysis/model_prediction/SConscript` adds **six** `env.Python()` calls per `(importance × rolling × sample × control)` combo — three analytic + three plotting, one per sub-phase. SCons dependency edges enforce the order: 2C's analytic script depends on 2B's `recommended_config.json`; 2B's depends on 2A's. Tables, plots, parquets, and recommendation JSON all <1MB → `output/`. Reference-repo sentinel pattern.

This means each sub-phase can be invoked independently (`scons output/analysis/model_prediction/.../tables/latent_n_selection.tex`) for inspection, and the natural dependency chain reproduces the workflow when running end-to-end.

---

## Phase 3 — Predictions on treated orgs + error attribution (paper §3.4 + code + outputs)

Three jobs: (a) show how bad the prediction is at an aggregate level at each stage, (b) localize how each ingredient ($\hat N$, $\hat\pi^r$, $\hat\pi^m$) contributes to prediction error, (c) localize how the share-vector inputs to those ingredients are wrong (cell-fit error, coarsening error, and — newly — known-vs-unknown member contributions).

**Restructured paper outline of §3.4** (per the user's request):

> **§3.4.1 Step 1 — Count prediction error.** How wrong is $\hat Q^d$ at each stage and overall? Uses E1 + E3.
>
> **§3.4.2 Step 2 — Stage-level error decomposition (oracle attribution).** Of total $\hat Q^m$ prediction error, how much comes from each stage's $\hat\pi^s$? Uses E1 + E3 across four oracle levels. Includes the joint-$(\hat N, \hat p_j^o)$ identification caveat. **No cell-fit/coarsening sub-decomposition in the main version** — the $\hat\pi^s$ contribution is reported as a single drop per stage.
>
> **§3.4.3 Step 3 — Share-level error decomposition.** How wrong are the underlying $\hat p$ shares, and *why*? Uses E2 (with its cell-fit/coarsening decomposition) on renormalized shares; further sub-decomposes E2 total by **known vs. unknown** member status (see "Why this matters" below).
>
> **Appendix §A — Cell-fit/coarsening sub-decomposition of the oracle attribution.** Per-stage sub-substitutions inside the oracle that propagate the E2 cell-fit/coarsening lens through the latent-problem aggregation. The 6-level version of Step 2; same data, finer slicing.
>
> **Appendix §B — Channel decomposition of predicted decline.** Predicted vs. empirical three-channel split of $\widehat{\Delta Q^m}$. Same data as Step 2, viewed in count units; does not double-count.

**Why the known/unknown sub-decomposition in Step 3 matters.** Cell-fit error from E2 is a single number per cell, but two very different things can drive a high cell-fit error: (i) we knew certain members were active and got their contribution wrong, (ii) entirely new members appeared post-departure that we couldn't have predicted (their fitted share was 0). The aggregate error doesn't distinguish these — and worse, an org can have a *low* total error not because we were genuinely accurate, but because the gains and losses across knowns and unknowns canceled in the aggregate. The known/unknown decomposition surfaces the structural component "we missed people we didn't know would be there" separately from "we got the people we tracked wrong."

### §3.4.1 — Step 1: Count prediction error (uses E1 + E3)

For each treated org $u$, each $k \in \{0, \ldots, K\}$, each stage $d \in \{o, r, m\}$:

$$\mathcal R_{u,k}^{d,\mathrm{single}} = \mathcal E_1(\hat Q_{u,k}^{d,(-j)}, Q_{u,k}^d; \{k\}, \rho = 1/\bar Q_{u,\mathrm{pre}}^d), \qquad \mathcal R_u^{d,\mathrm{all}} = \mathcal E_1(\hat Q^{d,(-j)}, Q^d; \{0,\ldots,K\}, \rho = 1/\bar Q_{u,\mathrm{pre}}^d).$$

Aggregate across stages (E3):
$$\mathcal R_u^{\mathrm{agg}, w}(\mathcal K) = \mathcal E_3(\{\mathcal R_u^{d}(\mathcal K)\}_d; w), \quad w \in \{w_\mathrm{pre}, w_\mathrm{post}\}.$$

**Failure quantities** (reuses E1 + E3 with derived count series): define $Q_{u,k}^{o-r} = Q_{u,k}^o - Q_{u,k}^r$, $Q_{u,k}^{r-m} = Q_{u,k}^r - Q_{u,k}^m$, predicted analogues $\hat Q^{o-r,(-j)}_{u,k} = \hat Q^{o,(-j)}_{u,k} - \hat Q^{r,(-j)}_{u,k}$, etc. Apply E1 with the same normalization scheme.

### §3.4.2 — Step 2: Stage-level error decomposition via oracle attribution (main version, uses E1 + E3)

**Identification preamble (must appear in the paper text):**
- True post-period openings: $Q_{u,k}^o = N_{u,k}(1 - p_{u,j,k}^{o,\mathrm{cf}})$ where $p_{u,j,k}^{o,\mathrm{cf}}$ is what $j$ would have done at $k$ had they not left; **never observed.**
- Predicted: $\hat Q_{u,k}^{o,(-j)} = \hat N_u(1 - \hat p_{u,j}^o)$.
- Algebraically $\hat Q^o - Q^o = (\hat N_u - N_{u,k})(1 - \hat p_{u,j}^o) - N_{u,k}(\hat p_{u,j}^o - p_{u,j,k}^{o,\mathrm{cf}})$, but each term contains an unobserved quantity ($N_{u,k}$ or $p_{u,j,k}^{o,\mathrm{cf}}$). **Opening-stage error is jointly attributable to the pair $(\hat N, \hat p_j^o)$ and not further decomposable from openings alone.** Step 3 below provides parallel, identification-free evidence on share stability among remaining members.

Define implied stage rates: $\tilde\pi_{u,k}^r = Q_{u,k}^r / Q_{u,k}^o$, $\tilde\pi_{u,k}^m = Q_{u,k}^m / Q_{u,k}^r$.

**Four oracle levels for the merge prediction.** Successively replace fitted ingredients with their empirical analogues:

| Level $\lambda$ | $\hat Q_{u,k}^{m,\lambda}$ formula | Drop from previous level pins down |
|---|---|---|
| 0 (model) | $\hat N_u\, \hat\pi_u^{o,(-j)}\, \hat\pi_u^{r,(-j)}\, \hat\pi_u^{m,(-j)}$ | — |
| 1 (opening) | $Q_{u,k}^o\, \hat\pi_u^{r,(-j)}\, \hat\pi_u^{m,(-j)}$ | Joint $(\hat N, \hat p_j^o)$ contribution |
| 2 (+ review) | $Q_{u,k}^r\, \hat\pi_u^{m,(-j)}$ | $\hat\pi_u^{r,(-j)}$ contribution |
| 3 (+ merge) | $Q_{u,k}^m$ | $\hat\pi_u^{m,(-j)}$ contribution |

At each level $\lambda$ apply E1+E3 across stages and weight schemes:
$$\mathcal R_u^{\mathrm{agg}, w, \lambda}(\mathcal K) = \mathcal E_3(\{\mathcal E_1(\hat Q^{d,\lambda}, Q^d; \mathcal K, \rho = 1/\bar Q_{u,\mathrm{pre}}^d)\}_d; w).$$

(The substitution at level $\lambda$ affects only the merge prediction and any downstream stages; the opening and lower-stage predictions stay at their model values for those E1 entries. The aggregate is over all three stages each time.)

**Per-level error drop:**
$$\Delta_\lambda^{w}(\mathcal K) = \mathcal R_u^{\mathrm{agg}, w, \lambda-1}(\mathcal K) - \mathcal R_u^{\mathrm{agg}, w, \lambda}(\mathcal K).$$

The three drops $\{\Delta_1, \Delta_2, \Delta_3\}$ partition total error into joint-$(\hat N, \hat p_j^o)$, review aggregation, and merge aggregation contributions.

**Reported summaries.** Cross-org RMS and median of $\mathcal R_u^{\mathrm{agg}, w, \lambda}$ at each level; RMS and median of $\Delta_\lambda^w$; both pooled (`event_time_sample = all`) and per-$k$.

A finer 6-level version (cell-fit and coarsening sub-substitutions inside each $\hat\pi^s$) lives in Appendix §A.

### §3.4.3 — Step 3: Share-level error decomposition with known/unknown sub-decomposition (uses E2)

The Step 2 oracle gives us "how much each $\hat\pi^s$ contributes to prediction error" but not "how the underlying $\hat p$ shares are wrong." Step 3 fills that gap by applying E2 directly to the share vectors that feed each $\hat\pi^s$.

For each treated org $u$, each stage $s$, apply E2 with the **renormalized** treated share vectors:
- Pre-period fitted, renormalized over remaining members: $\hat p_{u,i}^{s, \mathrm{post\text{-}renorm}} = \hat p_{u,i}^s / (1 - \hat p_{u,j}^s)$, for $i \neq j$. Sums to 1.
- Observed post-period over remaining members: $\tilde p_{u,i,k}^s = A_{u,i,k}^s / A_{u,k}^s$, for $i \neq j$. Sums to 1 by construction since $j$ contributes 0.
- Partition: the pre-period identity mapping $c^*$ from `model_prediction.json`, restricted to remaining members (any singleton cell that contained $j$ drops; $j$ does not get re-absorbed because the post-period observed share already excludes $j$). The member universe is the union of pre-departure remaining members and post-departure new arrivals; new arrivals appear under `all` as zero-share singletons or get absorbed into "other" under coarser rules — exactly as in Phase 2 §3.3.6.

Apply E2 to get the components per stage $s$ at each org $u$ across both `event_time_sample = all` and per-$k$:

$$\{\mathcal R_{u,\mathrm{tr}}^{p,s,\mathrm{total}},\, \mathcal R_{u,\mathrm{tr}}^{p,s,\mathrm{cell\text{-}fit}},\, \mathcal R_{u,\mathrm{tr}}^{p,s,\mathrm{coarsening}},\, \mathcal R_{u,\mathrm{tr}}^{p,s,\mathrm{known}},\, \mathcal R_{u,\mathrm{tr}}^{p,s,\mathrm{unknown}}\}(\mathcal K) = \mathcal E_2(\hat p^{s,\mathrm{post\text{-}renorm}}, \tilde p^{s}, c^*; \mathcal K).$$

By the E2 identities:
- $(\mathcal R^{\mathrm{total}})^2 = (\mathcal R^{\mathrm{cell\text{-}fit}})^2 + (\mathcal R^{\mathrm{coarsening}})^2$ — partition-side decomposition of "where the error lives in the partition."
- $(\mathcal R^{\mathrm{total}})^2 = (\mathcal R^{\mathrm{known}})^2 + (\mathcal R^{\mathrm{unknown}})^2$ — member-side decomposition of "we got tracked members wrong" vs. "we missed novelty."

These are two orthogonal slicings of the same total. Both reported.

Aggregated across stages via E3 (under both pre- and post-event weights). Cross-org summary via RMS.

**Why we report both decompositions.** The cell-fit/coarsening split tells you whether your *partition* is the problem; the known/unknown split tells you whether your *member universe* is the problem. They can disagree (e.g. cell-fit may dominate while unknowns also dominate, if novelty is concentrated in cells that are individually-tracked elsewhere — note: this happens when a member who was zero-pre-period gets absorbed into "other"). Reporting both makes "we got lucky in aggregate" diagnosable.

### Output tables — `output/analysis/model_prediction/.../tables/`

All paneled like Phase 2; cells = $\overline{\mathcal R}^{\mathrm{RMS}}$ over $\mathcal T$ unless stated; medians reported alongside; `event_time_sample` column where applicable.

**`prediction_errors.tex` — three panels (Step 1).**

- Panel A — per-stage error: rows = (stage $d \in \{o,r,m\}$) × `event_time_sample`; columns = $\overline{\mathcal R_u^d}^{\mathrm{RMS}}$, $\widetilde{\mathcal R_u^d}$.
- Panel B — aggregate error: rows = `event_time_sample`; columns = $\overline{\mathcal R_u^{\mathrm{agg}, w_\mathrm{pre}}}^{\mathrm{RMS}}$, $\overline{\mathcal R_u^{\mathrm{agg}, w_\mathrm{post}}}^{\mathrm{RMS}}$, medians.
- Panel C — failure quantities: rows = ($d \in \{o-r, r-m\}$) × `event_time_sample`; columns = $\overline{\mathcal R_u^d}^{\mathrm{RMS}}$, $\widetilde{\mathcal R_u^d}$.

**`oracle_attribution.tex` — two panels (Step 2 main, 4-level version).**

- Panel A — error at each level: rows = oracle level $\lambda \in \{0, 1, 2, 3\}$ × `event_time_sample`; columns = $\overline{\mathcal R_u^{\mathrm{agg}, w_\mathrm{pre}}}^{\mathrm{RMS}}$, $\overline{\mathcal R_u^{\mathrm{agg}, w_\mathrm{post}}}^{\mathrm{RMS}}$, medians.
- Panel B — drop at each level: rows = $\lambda \in \{1, 2, 3\}$ × `event_time_sample`; columns = drop label (joint $(\hat N, \hat p_j^o)$ / $\hat\pi^r$ / $\hat\pi^m$), $\overline{\Delta_\lambda^{w_\mathrm{pre}}}^{\mathrm{RMS}}$, $\overline{\Delta_\lambda^{w_\mathrm{post}}}^{\mathrm{RMS}}$, share of total error explained ($\Delta_\lambda^2 / (\mathcal R^{\lambda=0})^2$).

Footnote on Panel B: "Level-1 drop = joint $(\hat N, \hat p_j^o)$ contribution, **not** $\hat N$ alone — see §3.4.2 identification argument. The cell-fit/coarsening sub-decomposition of the level-2 and level-3 drops appears in Appendix §A."

**`treated_share_decomposition.tex` — three panels (Step 3, one per stage).**

For each stage $s \in \{o, r, m\}$:

| event_time_sample | $\overline{\mathcal R^{\mathrm{total}}}^{\mathrm{RMS}}$ | $\overline{\mathcal R^{\mathrm{cell\text{-}fit}}}^{\mathrm{RMS}}$ | $\overline{\mathcal R^{\mathrm{coarsening}}}^{\mathrm{RMS}}$ | Share$^{\mathrm{cell\text{-}fit}}$ | Share$^{\mathrm{coarsening}}$ | $\overline{\mathcal R^{\mathrm{known}}}^{\mathrm{RMS}}$ | $\overline{\mathcal R^{\mathrm{unknown}}}^{\mathrm{RMS}}$ | Share$^{\mathrm{known}}$ | Share$^{\mathrm{unknown}}$ |
|---|---|---|---|---|---|---|---|---|---|
| all | … | … | … | … | … | … | … | … | … |
| k=0 | … | … | … | … | … | … | … | … | … |
| … | … | … | … | … | … | … | … | … | … |

Plus a fourth panel (D) with the cross-stage E3 aggregate of each component column under both weight schemes, so the reader can see whether the dominant story is consistent across stages.

**Appendix `oracle_attribution_decomposed.tex` — two panels (Appendix §A, 6-level version).** Same shape as the main `oracle_attribution.tex` but with the cell-fit-corrected sub-substitutions inserted for review and merge stages; rows = $\lambda \in \{0, 1, 2a, 2b, 3a, 3b\}$. Spelled-out formulas for the 2a/3a cell-fit-corrected $\hat\pi^s$ live in the appendix prose.

**Appendix `channel_decomposition.tex` — two panels (Appendix §B).**

- Panel A — predicted vs. empirical channel split: rows = channels $\{o, r, m\}$ × `event_time_sample`; columns = $\overline{\widehat{\mathrm{channel}_s}}$, $\overline{\widetilde{\mathrm{channel}_s}}$, $\overline{\mathrm{Disc}^{s,\mathrm{abs}}}$, $\overline{|\mathrm{Disc}^{s,\mathrm{rel}}|}$. (Predicted and empirical channel formulas spelled out in appendix prose, from §2.2.)
- Panel B — channel share of total predicted decline: rows = channels × `event_time_sample`; column = $\overline{\widehat{\mathrm{channel}_s} / \widehat{\Delta Q^m}}$.

### Output plots — `output/analysis/model_prediction/.../plots/`

Plot everything to start. KDEs use cross-org distributions; bars use cross-org RMS.

**Step 1:**
- `prediction_error_dist_{o,r,m}.png` — KDE of $\mathcal R_u^{d,\mathrm{all}}$ across orgs, per stage.
- `prediction_error_by_event_time_{o,r,m}.png` — boxplot over $k$, per stage.
- `prediction_error_agg_dist.png` — two-curve KDE of $\mathcal R_u^{\mathrm{agg}}$ under pre- vs. post-event weights.
- `failure_quantity_errors.png` — KDE of $\mathcal R_u^d$ for $d \in \{o-r, r-m\}$.

**Step 2 (oracle attribution, 4-level main version):**
- `oracle_attribution_levels.png` — bar chart of cross-org RMS $\mathcal R_u^{\mathrm{agg}}$ at the four oracle levels {0, 1, 2, 3}, pre-w bars with post-w faceted alongside.
- `oracle_attribution_drops.png` — stacked bar chart of cross-org RMS $\Delta_\lambda^w$ for the three drops, labeled (joint $(\hat N, \hat p_j^o)$ / $\hat\pi^r$ / $\hat\pi^m$). Squared heights sum to $(\mathcal R^{\lambda=0})^2$. Headline picture of error attribution.
- `oracle_attribution_drop_dist.png` — KDE across orgs of $\Delta_\lambda$ at each of the three drop levels (faceted), to show that the cross-org RMS is not driven by a few outliers.
- `oracle_attribution_by_event_time.png` — line plot, RMS $\mathcal R^{\mathrm{agg}, \lambda}$ vs. $k$ at each of the four levels.

**Step 3 (share-level decomposition + known/unknown):**
- `share_decomp_total_{o,r,m}.png` — KDE of E2 total per stage.
- `share_decomp_cellfit_coarsening_{o,r,m}.png` — three-curve KDE per stage (total, cell-fit, coarsening).
- `share_decomp_cellfit_coarsening_share_bar.png` — bar chart of cross-org cell-fit share vs. coarsening share (squared shares; sum to 1), per stage.
- `share_decomp_known_unknown_{o,r,m}.png` — three-curve KDE per stage (total, known, unknown).
- `share_decomp_known_unknown_share_bar.png` — bar chart of cross-org known share vs. unknown share, per stage. The headline visual of the "we got lucky vs. we missed novelty" story.
- `share_decomp_by_event_time_{o,r,m}.png` — boxplot over $k$, per stage, of the E2 total.
- `share_decomp_oracle_consistency.png` — for each of $s \in \{r, m\}$, scatter per-org of (Step 3 share-level $\overline{\mathrm{Share}^{\mathrm{cell\text{-}fit}}}_s$) vs. (Appendix §A oracle cell-fit share for stage $s$). Identity line. Cross-step sanity check: the share-level and oracle-level cell-fit shares should be positively correlated; large divergence flags a non-linearity in the aggregation worth investigating.

**Appendix §A (cell-fit/coarsening sub-decomposition of oracle):**
- `oracle_appendix_levels.png` — same as `oracle_attribution_levels.png` but with the six sub-levels {0, 1, 2a, 2b, 3a, 3b}.
- `oracle_appendix_drops.png` — stacked bar chart of the five sub-drops {joint $(\hat N, \hat p_j^o)$ / review cell-fit / review coarsening / merge cell-fit / merge coarsening}.
- `oracle_appendix_drop_dist.png` — faceted KDE of each sub-drop across orgs.

**Appendix §B (channel decomposition):**
- `channel_decomposition_scatter_{o,r,m}.png` — scatter predicted vs. empirical channel per org, per stage; identity line.
- `channel_decomposition_disc_dist_{o,r,m}.png` — KDE of absolute discrepancy.

### Code structure: split into three pause-and-inspect sub-phases

Phase 3 is split into **3A / 3B / 3C**, mirroring the Phase 2 sub-phase pattern. Each sub-phase has its own pair of scripts (analytic + plot) and its own outputs. **The user pauses after each sub-phase to inspect outputs and the corresponding paper subsection before the next runs.**

3A produces the count predictions and the per-org oracle predictions (which 3B reuses) so that 3B doesn't recompute predictions; this also keeps the predict step (which actually generates $\hat Q$ values) cleanly separated from the evaluate step (which decomposes errors).

### Sub-phase 3A — Count prediction error (paper §3.4.1)

**Scripts:**
- `source/analysis/model_prediction/predict_treated.py` — reads `source/lib/config/model_prediction.json`; reads each treated org's event_time_member panel; computes ALL prediction objects (count predictions, oracle predictions for all 6 sub-levels, cell-fit-corrected stage probabilities, treated-side renormalized shares); writes the four prediction parquets (used by 3A/3B/3C):
  - `drive/output/analysis/model_prediction/{...}/predictions.parquet` — per (org, $k$, $d$): $\hat Q^{d,(-j)}_{u,k}$, $Q^d_{u,k}$, $\bar Q^d_{u,\mathrm{pre}}$.
  - `drive/output/analysis/model_prediction/{...}/oracle_predictions.parquet` — per (org, $k$, $d$, $\lambda$) for the 6 sub-levels $\lambda \in \{0, 1, 2, 2a, 3, 3a\}$: $\hat Q^{d,\lambda}_{u,k}$, $Q^d_{u,k}$, plus $\hat\pi_u^{s,\mathrm{cell\text{-}fix},(-j)}$ columns for $s \in \{r, m\}$.
  - `drive/output/analysis/model_prediction/{...}/treated_share_decomposition.parquet` — per (org, $k$, $s$): E2 components for the renormalized treated share vector.
  - `drive/output/analysis/model_prediction/{...}/channel_decomposition.parquet` — per (org, $k$, channel): predicted and empirical channel contributions plus discrepancies.
- `source/analysis/model_prediction/evaluate_count_predictions.py` — reads `predictions.parquet`; applies `count_squared_error` + `weighted_aggregate` + `cross_org_rms`; writes `prediction_errors.tex`.
- `source/analysis/model_prediction/plot_count_predictions.py` — reads `predictions.parquet`; writes `prediction_error_dist_*.png`, `prediction_error_by_event_time_*.png`, `prediction_error_agg_dist.png`, `failure_quantity_errors.png`.

**Outputs to inspect at pause:**
- The four prediction parquets (will be reused by 3B and 3C — generated up-front for efficiency, but only `predictions.parquet` is *evaluated* in 3A).
- `output/analysis/model_prediction/{...}/tables/prediction_errors.tex`
- `output/analysis/model_prediction/{...}/plots/prediction_*.png` and `failure_quantity_errors.png`
- Paper draft updates to §3.4.1

**Verification:**
1. Pipeline runs clean.
2. Renormalized treated fitted vector sums to 1 over remaining members for every (org, stage) — sanity check on `predict_treated.py`.
3. Plots open and are readable.

**→ PAUSE.** User inspects outputs + §3.4.1 paper draft. Approves before 3B begins.

### Sub-phase 3B — Stage-level error decomposition: oracle attribution + channel decomposition (paper §3.4.2 + Appendix §A + Appendix §B)

Per the user's grouping: oracle attribution and channel decomposition are two views of the same stage-level decomposition of the merge prediction, so they share a sub-phase.

**Scripts:**
- `source/analysis/model_prediction/evaluate_stage_decomposition.py` — reads `oracle_predictions.parquet` and `channel_decomposition.parquet`; applies `count_squared_error` + `weighted_aggregate` + `cross_org_rms`; writes:
  - `oracle_attribution.tex` (4-level main, §3.4.2)
  - `oracle_attribution_decomposed.tex` (6-level appendix, §A)
  - `channel_decomposition.tex` (appendix §B)
- `source/analysis/model_prediction/plot_stage_decomposition.py` — reads same parquets; writes:
  - Step 2 (main): `oracle_attribution_levels.png`, `oracle_attribution_drops.png`, `oracle_attribution_drop_dist.png`, `oracle_attribution_by_event_time.png`.
  - Appendix §A: `oracle_appendix_levels.png`, `oracle_appendix_drops.png`, `oracle_appendix_drop_dist.png`.
  - Appendix §B: `channel_decomposition_scatter_*.png`, `channel_decomposition_disc_dist_*.png`.

**Outputs to inspect at pause:**
- `output/analysis/model_prediction/{...}/tables/oracle_attribution.tex`, `oracle_attribution_decomposed.tex`, `channel_decomposition.tex`
- `output/analysis/model_prediction/{...}/plots/oracle_*.png`, `channel_decomposition_*.png`
- Paper draft updates to §3.4.2 + Appendix §A + Appendix §B

**Verification:**
1. **Main oracle (4-level)** in `oracle_attribution.tex` monotonically non-increasing per `event_time_sample` row (level 3 = zero by construction).
2. **Appendix oracle (6-level)** monotonically non-increasing, with cell-fit sub-level for each stage between the previous level and the full-oracle sub-level: $\mathcal R^{2a} \in [\mathcal R^{2}, \mathcal R^{1}]$, $\mathcal R^{3a} \in [\mathcal R^{3}, \mathcal R^{2}]$. Hard assertion in code.
3. Appendix §A 6-level sub-drops within each stage sum (in error² space) to that stage's main-version drop, to numerical precision.
4. Channel decomposition: predicted channel contributions sum to $\widehat{\Delta Q^m}$ to numerical precision (assertion in code).
5. Plots open.

**→ PAUSE.** User inspects outputs + §3.4.2 + appendix paper drafts. Approves before 3C begins.

### Sub-phase 3C — Share-level error decomposition (paper §3.4.3)

**Scripts:**
- `source/analysis/model_prediction/evaluate_share_decomposition.py` — reads `treated_share_decomposition.parquet`; applies `share_squared_error` + `weighted_aggregate` + `cross_org_rms`; writes `treated_share_decomposition.tex` (three-stage panels with cell-fit/coarsening AND known/unknown columns).
- `source/analysis/model_prediction/plot_share_decomposition.py` — reads same parquet; writes `share_decomp_total_*.png`, `share_decomp_cellfit_coarsening_*.png`, `share_decomp_cellfit_coarsening_share_bar.png`, `share_decomp_known_unknown_*.png`, `share_decomp_known_unknown_share_bar.png`, `share_decomp_by_event_time_*.png`, `share_decomp_oracle_consistency.png`.

**Outputs to inspect at pause:**
- `output/analysis/model_prediction/{...}/tables/treated_share_decomposition.tex`
- `output/analysis/model_prediction/{...}/plots/share_decomp_*.png`
- Paper draft updates to §3.4.3

**Verification:**
1. **E2 cell-fit/coarsening identity (treated):** $\sqrt{(\mathcal E_2^{\mathrm{cell\text{-}fit}})^2 + (\mathcal E_2^{\mathrm{coarsening}})^2} = \mathcal E_2^{\mathrm{total}}$ on every (org, stage) triple AND at the cross-org RMS level.
2. **E2 known/unknown identity (treated):** $\sqrt{(\mathcal E_2^{\mathrm{known}})^2 + (\mathcal E_2^{\mathrm{unknown}})^2} = \mathcal E_2^{\mathrm{total}}$. Same identity at cross-org RMS.
3. Treated share decomposition on a control org (with the same partition $c^*$) reproduces Phase 2 sub-phase 2C's partition diagnostic values exactly — confirms shared `helpers.py` codepath.
4. Plots open.

**→ PAUSE.** User inspects outputs + §3.4.3 paper draft. Final commit at user's go.

### SConscript wiring (Phase 3)

`source/analysis/model_prediction/SConscript` adds **seven** more `env.Python()` calls per `(importance × rolling × sample × control)` combo:
- One for `predict_treated.py` (writes the four parquets up-front).
- 3A: one analytic + one plot.
- 3B: one analytic + one plot.
- 3C: one analytic + one plot.

SCons dependency edges enforce the order: 3A/3B/3C all depend on `predict_treated.py` outputs; 3B and 3C depend on 3A's table existing (so the user can pause after 3A); 3C depends on 3B's tables existing. Each sub-phase can be invoked independently for inspection. Reference-repo sentinel pattern. Predictions parquets routed to `drive/output/` since per-treated-org prediction tables across all treated orgs may exceed 1MB; `.tex` and `.png` to `output/`.

---

## Critical files to be modified or created

**Deletions (Phase 0):**
- `source/derived/model_prediction/prepare_contributor_actions.py`
- `source/derived/model_prediction/SConscript`
- `source/analysis/model_prediction/{prepare_contributor_panel,estimate_model_probabilities,compute_predictions,evaluate_fit,model_utils}.py`
- `source/analysis/model_prediction/SConscript`

**New files (Phase 1):**
- `source/derived/model_prediction/build_event_time_member_panel.py`
- `source/derived/model_prediction/SConscript`

**New files (Phase 2 — split into three pause-and-inspect sub-phases):**
- `source/analysis/model_prediction/helpers.py` (`count_squared_error`, `share_squared_error`, `weighted_aggregate`, `cross_org_rms`, partition utilities, etc.)
- 2A: `source/analysis/model_prediction/fit_latent_n.py`, `plot_latent_n.py`
- 2B: `source/analysis/model_prediction/fit_estimation_sample.py`, `plot_estimation_sample.py`
- 2C: `source/analysis/model_prediction/fit_contributor_partition.py`, `plot_contributor_partition.py`
- `source/analysis/model_prediction/SConscript` (six `env.Python()` calls, dependency-chained 2A → 2B → 2C)
- New function `LoadModelPredictionConfig()` in `source/lib/python/config_loaders.py`
- (User-edited) `source/lib/config/model_prediction.json`

**New files (Phase 3 — split into three pause-and-inspect sub-phases):**
- `source/analysis/model_prediction/predict_treated.py` (writes the four prediction parquets up-front; reused by all three sub-phases)
- 3A: `source/analysis/model_prediction/evaluate_count_predictions.py`, `plot_count_predictions.py`
- 3B: `source/analysis/model_prediction/evaluate_stage_decomposition.py`, `plot_stage_decomposition.py`
- 3C: `source/analysis/model_prediction/evaluate_share_decomposition.py`, `plot_share_decomposition.py`
- (extends Phase 2's SConscript with seven more `env.Python()` calls)

**Paper edits:**
- `source/paper/model.tex` — Phase 2 restructures §3.3 (new §§3.3.1–3.3.7) and adds an appendix proving the E2 cell-fit/coarsening identity; Phase 3 rewrites §3.4 (Steps 1–3 in the new ordering: count error → oracle attribution → share decomposition with known/unknown sub-decomposition), plus Appendix §A (cell-fit/coarsening sub-decomposition of oracle) and Appendix §B (channel decomposition).

## Helpers to reuse (do not reinvent)

- `source.lib.python.data_utils.ImputeTimePeriod`
- `source.lib.python.config_loaders.{LoadGlobalSettings,LoadPipelineInputs,LoadAnalysisParameters,LoadPlotSettings}`
- `source.lib.python.repo_utils.MakeRepoNameSafe`
- `source.derived.org_outcomes_practices.helpers.LoadBotList`
- `source.lib.JMSLab.SaveData`
- The `REVIEW_TYPES` set + `AggregateContributorCounts` / `AggregateOrgCounts` patterns from the deleted `prepare_contributor_actions.py` (lifted into `build_event_time_member_panel.py`).

## End-to-end verification

After all three phases:
```
scons drive/output/derived/model_prediction/ \
      output/analysis/model_prediction/
```
must build clean for the full Cartesian product of (importance × rolling × sample × control). For the reference repo set:
- Every plot named in §3.3.7 and §3.4 must render.
- `oracle_attribution.tex` must show a non-trivial level-1 drop (otherwise either $(\hat N, \hat p_j^o)$ are jointly perfect or the dataset is wrong — both diagnostically important).
- The Appendix §A 6-level oracle's cell-fit + coarsening sub-drops within each stage must sum (in error² space) to that stage's main-version drop, to numerical precision.
- E2 cell-fit² + coarsening² = total² and known² + unknown² = total² hold at every (org, stage) triple AND at the cross-org RMS level.
- Phase 2 D-equivalent values reproduce when Phase 3's treated-share-drift function is run on a control org with $c^*$ — confirms shared `helpers.py` codepath.
- E2 squared identity holds in every reported triple.
- Compile `source/paper/model.tex` and confirm §3.3 and §3.4 reference the new tables/plots correctly with no LaTeX errors.
