# Rewrite the model_prediction pipeline + restructure §3 of model.tex

## Context

`source/paper/model.tex` was substantially rewritten and now describes:
(a) a baseline stage-decomposition model,
(b) a measurement-and-empirical-implementation section that fits four objects on never-treated controls and evaluates predictions on treated organizations.

The existing code in `source/derived/model_prediction/` and `source/analysis/model_prediction/` (852 LOC, 5 scripts) was written against an older draft and now drifts from the paper in three ways:

1. It treats the **latent-problem aggregation** as a fitted object rather than as the model itself.
2. Its narrative claims everything is "fit on never-treated," but the actual procedure is "use treated pre-period data to predict treated post-period; use controls only to *choose* the procedure."
3. It does not produce the diagnostics the paper describes (per-stage error distributions across event time, partition-share errors split into selection vs. treated-stability, latent-flow normalization plots, progressive oracle attribution localized to the right channels, stage-decomposition vs. actual stage failures).

The plan is to rip the old code out cleanly, restructure §3.3 of the paper to match the user's mental model, and rebuild the pipeline so that the code, the table in the paper, and the plots in the paper are all in one-to-one correspondence.

The work is broken into three pause-and-inspect phases. **The user must approve after each phase before the next begins.**

---

## Phase 0 (one shot, no pause): rip out the old code

Delete the following so nothing stale lingers (CLAUDE.md §0: no ghosts):

- `source/derived/model_prediction/prepare_contributor_actions.py`
- `source/derived/model_prediction/SConscript`
- `source/analysis/model_prediction/prepare_contributor_panel.py`
- `source/analysis/model_prediction/estimate_model_probabilities.py`
- `source/analysis/model_prediction/compute_predictions.py`
- `source/analysis/model_prediction/evaluate_fit.py`
- `source/analysis/model_prediction/model_utils.py`
- `source/analysis/model_prediction/SConscript`
- All previously-emitted outputs under `output/derived/model_prediction/`, `output/analysis/model_prediction/`, `drive/output/derived/model_prediction/`, `drive/output/analysis/model_prediction/`.

  Update parent `SConscript`s to drop the now-deleted subdirs from their `SConscript()` calls.

  Pause here so that a manual inspection can be made. 

  Commit this as a separate commit: `rip out old model_prediction pipeline #<issue>`.

---

## Phase 1 — Derived dataset: per-repo (event_time × member) panel

**Code only. No paper edits.**

### What to build

A new derived script that, for every repo with a file under `drive/output/derived/action_data/repo_actions/`, produces a per-repo parquet keyed by `(quasi_event_time, actor_id)` with these columns:

| Column | Definition |
|---|---|
| `member_pull_request_opened` | # unique PRs the actor opened in that period |
| `member_pull_request_reviewed` | # unique PRs the actor reviewed (`type ∈ REVIEW_TYPES`) in that period |
| `member_pull_request_merged` | # unique PRs the actor merged in that period |
| `repo_pull_request_opened` | # unique PRs opened at the repo in that period |
| `repo_pull_request_reviewed` | # unique PRs reviewed at the repo in that period |
| `repo_pull_request_merged` | # unique PRs merged at the repo in that period |

**Member universe:** any `actor_id` with at least one PR-stage action (open, review, or merge) at this repo in any period from `quasi_event_time = -5` through `+5`. Bots excluded via `LoadBotList()` from `source/derived/org_outcomes_practices/helpers.py`. The (event_time × member) grid is rectangular over included members and `quasi_event_time ∈ {-5,...,+5}` (no per-period dropping); missing actor-period cells get zeros for the three `member_*` columns; the three `repo_*` columns are constant across members within a period.

**Inputs:**
- `drive/output/derived/action_data/repo_actions/{repo}.parquet`
- For each (importance_type, rolling_period, qualified_sample, control_group) combo from `pipeline_inputs.json`: `output/analysis/data_prep/{...}/panel.parquet` to recover `quasi_treatment_group` and the `quasi_event_time → time_period` mapping.

**Outputs (one per repo per config combo):**
- `drive/output/derived/model_prediction/event_time_member_panel/{importance_type}/{rolling_period}/{qualified_sample}/{control_group}/{repo}.parquet`, saved using SaveData
- log to `output/derived/model_prediction/event_time_member_panel/{importance_type}/{rolling_period}/{qualified_sample}/{control_group}/{repo}.log`

### Why this lives in `derived/`

It is a deterministic transformation of action data + the panel's event-time mapping. No model choices. No fitting. Reusable by every downstream model script.

### Files to write

- `source/derived/model_prediction/build_event_time_member_panel.py` — `Main()` discovers all repos × all config combos and parallelizes via `joblib`. Reuses `ImputeTimePeriod`, `LoadBotList`, `MakeRepoNameSafe`. The PR-action aggregation logic lifts directly from the old `AggregateContributorCounts` / `AggregateOrgCounts` so we don't reinvent it.
- `source/derived/model_prediction/SConscript` — one `env.Python()` per (repo × config combo).
- `scons` structure using the existing framework of only including the required data in `source` corresponding to the global reference repo (see line 8 in `source/derived/org_outcomes_practices/SConscript`)

### Verification

1. `scons drive/output/derived/model_prediction/event_time_member_panel/...` runs clean for the reference repo.
2. Spot-check one repo's parquet: verify (a) actor universe matches the union of PR-stage actors in `[-5, +5]`, (b) `repo_*` columns are constant within `quasi_event_time`, (c) sums of `member_*` match `repo_*` (for opens and merges; reviews can have multiple reviewers per PR so `sum(member_reviewed) ≥ repo_reviewed`).
3. Row count = `|members| × 11`.

**→ PAUSE for user inspection.**

---

## Phase 2 — Fitting counterfactual inputs (paper §3.3.2 + code + outputs)

### Paper edits to `source/paper/model.tex`

Restructure §3.3 so that:

1. The first thing that's presented is the prediction model I'm going to use to predict future output. Then, each subsequent subsection will discuss how I fit the two main components of that model - the $N_k$ and the $p_i^s$ for each stage. 

2. **The latent-problem model is presented first as the model itself, not a fitted object.** Move the share-normalized opening identity ($\hat\pi_u^o = \sum_i \hat p_{u,i}^o = 1$, $\hat\pi_u^{o,(-j)} = 1 - \hat p_{u,j}^o$) and the independent-Bernoulli aggregation for review/merge into a new short subsubsection §3.3.2 *Latent-problem model* that comes immediately after the error-metrics block, before any fitting discussion. 

   1. @CLAUDE I wonder if you actually want to discuss the prediction model first, then say to need to fit the params and discuss error metrics I use to pick the params, then discuss latent-problem part and the p-fitting right after that. I think you can probably fit the contributor partition as part of the individual-level shares discussion - there, you have to pick two things - time period ur optimizing over and the contributor partition. Mention that I will first fit time period and then fit contributor partition, using

3. **Reframe the goal of the fitting procedure.** Replace "Each is fit on never-treated organizations using a pseudo-departure date" with the user's framing:
   > To generate a prediction we need three inputs: individual-level shares $\hat p_i^s$, a contributor partition $\mathcal G_{u,c}^s$ over which those shares are defined, and a latent-flow rule for $\hat N_u$. In production, these are computed from the **pre-period of the treated organization itself**. To choose *how* to compute each input from the pre-period, we run all candidate procedures on never-treated organizations (which have both a pre- and a post-period under a pseudo-departure date) and pick the procedure with the smallest cross-control error.

4. **Reorder the candidate-input discussion to match the table** (and the table's order to match what the user wants discussed): individual-level shares → contributor partition → latent flow.

5. **Spell out the selection rule for each input.** Currently the text waves at "selection… by the control-organization fit metrics." Replace with explicit prose for each:

   - **Individual-level shares.** Two candidate estimation samples: pooled across $L=5$ pre-periods vs. last pre-period only. Both are compared against the *post-period* observed shares using the partition-share error $\mathcal R^p$ from §3.3.1, evaluated per stage and aggregated. The candidate with the lower mean cross-control aggregate error wins. We also plot the per-organization distribution of the error for each candidate side-by-side as a visual check. 

     - @CLAUDE: I actually want the code to have the infrastructure such that if I decide I want to compare 5, 4, 3, 2, 1 pre-periods I can easily do so. But for now for ease of interpretation I will just have 2 pre-period options. Also, write out the math to make sure I know what you're calculating.  

   - **Contributor partition.** Five candidates from Table 1. The selection metric is the same partition-share error $\mathcal R^p$ from §3.3.1, computed using the *chosen* estimation sample from the previous step, comparing fitted (pre-period, coarsened) shares to observed (post-period, coarsened) shares on controls. Lowest cross-control mean wins.

     **Add a second, diagnostic-only pair of errors** (not used for selection) that decomposes the total post-period drift into two sources, computed for each control org and each post-pseudo-departure period $k$:

     - **(D-a) Within-partition fit error.** Hold the partition fixed at the *exact contributor-to-cell identity mapping* derived from pre-period data on that org. Apply this fixed mapping to the post-period data and compute $\mathcal R^p$ between (fitted pre-period coarsened shares) and (observed post-period coarsened shares under the same identity mapping). Captures: "given who I decided to track individually, are the shares right post-pseudo-departure?" @CLAUDE: I think there are two subtle points here. The first is among the coarsened group, and non-coarsened individuals, is the aggregate predicted p_s correct? The second is within the coarsened group, how do the individual p_i^s compare pre and post. I think you should  write out the math for this. 

     - **(D-b) Partition-instability error.** For each post-period $k$, re-run the candidate partition rule on data observed at $k$ to derive a "post-period partition." Compute $\mathcal R^p$ between (post-period coarsened shares using pre-period identity mapping) and (post-period coarsened shares using post-period-$k$ identity mapping). Captures: "did the people I decided to track individually still belong in the individually-tracked cells at post-period $k$?" @CLAUDE: No, this is not right. The point here is to understand how I would do in the post-period if I used my pre-period coarsening groups, but had the right post-period p's. This essentialy tells u how bad my pre-period coarening group is in the post-period data - assuming the p's were correct. Then you could compare the difference in the aggregate p_s, how. I think you should  write out the math for this. The first one is more like how much th ep's changed and the impact on coarsened to coarsened. The second one is coarsened to general. 

       The same (D-a) and (D-b) computations are reused in Phase 3 on treated organizations (post-departure rather than post-pseudo-departure); the only thing that changes is the input set of orgs. Plot the (selection) error and each diagnostic error in three separate figures, with one curve per partition rule.

       @CLAUDE: Also, I think the thing that you should make explicitly clear right now is that the coarsening uses the "estimation sample" from individual-level shares. This is a clear dependency, and the candidate name should indicate so 

   - **Latent flow.** Two candidates: pre-period average and last pre-period. Because there is no departure on controls and $\hat\pi_u^o = 1$, the question reduces to how well each rule predicts the next-period opening count. Use the scaled RMSE from §3.3.1 (so each org is normalized by its own pre-period mean opening count) and pick the lower cross-control value. Also plot the cross-organization distribution of $N$ in pre- and post-period (one panel) so the reader can see what variation we're trying to predict through.

     - @CLAUDE: also write the math here so I can make sure you're calculating what I want you to calculate. 

6. **Add a new table** mirroring what the code produces (see below), and a short paragraph naming the three plots produced for §3.3.2.

### Output table specification (will be referenced from the paper)

The code writes `output/analysis/model_prediction/.../tables/fitting_selection.tex`:

@CLAUDE: call it `cover80p` instead of `cov80`

@CLAUDE: I don't understand what each of the per-stage error columns mean. Can you write out the math for each of these, or reference the math for them when you discuss them above, and propose actual column names? I will help you revise the column names. Also indicate for each input which errors will be calculated and which will be missing, because some will be missing... also I wonder if you want to have the columns represent actual error values so that some of the error columns will have missing rows but that's ok. Actually maybe u can use missing rows as an opportunity to separate table into different panels. 

| Input | Candidate | Per-stage error (o) | Per-stage error (r) | Per-stage error (m) | Aggregate (pre-w) | Aggregate (post-w) | Selected |
|---|---|---|---|---|---|---|---|
| Individual share | pooled_L5 | … | … | … | … | … | ✓ |
| Individual share | last | … | … | … | … | … | |
| Partition | all | … | … | … | … | … | |
| Partition | top5 | … | … | … | … | … | ✓ |
| Partition | top5_per_stage | … | … | … | … | … | |
| Partition | topK_cov80 | … | … | … | … | … | |
| Partition | topK_cov80_per_stage | … | … | … | … | … | |
| Latent flow | avg_L | normalized RMSE … | unnorm. RMSE … | | | | ✓ |
| Latent flow | last | normalized RMSE … | unnorm. RMSE … | | | | |

Each cell is the cross-control mean. The "Selected" column is the row with the smallest aggregate error within each input group.

### Output plots (will be referenced from the paper)

In `output/analysis/model_prediction/.../plots/`:

1. `share_error_dist.png` — per-org distribution of $\mathcal R_u^{p,\mathrm{agg}}$ for `pooled_L5` vs. `last` (two histograms or KDEs). @CLAUDE: let's do KDE's to start with. 
2. `partition_selection_error_dist.png` — per-org distribution of the partition-share *selection* error, one curve per partition rule. 
3. `partition_diagnostic_within_error_dist.png` — per-org distribution of the (D-a) within-partition fit error on controls (per post-period $k$), one curve per partition rule.
4. `partition_diagnostic_instability_error_dist.png` — per-org distribution of the (D-b) partition-instability error on controls (per post-period $k$), one curve per partition rule.
5. `latent_flow_error_dist.png` — per-org distribution of normalized RMSE for `avg_L` vs. `last`. @CLAUDE: KDE
6. `latent_flow_N_distribution.png` — pre- and post-period $N$ distribution across orgs (overlay or side-by-side). @CLAUDE: this should be one figure, time series, and the line is median N, and maybe have bars for pct?? Actually, it would be really cool if there was a way to plot the N line for each organization in the control group, but somehow make it readable because plotting 400 lines isn't readable... so you have to somehow condense the graphic distribution somehow - pelase think about whether this is a plot ppl have made and how they do that. 

### Code to write

Two scripts, separating analytic-output from plotting per the user's structural preference:

- `source/analysis/model_prediction/fit_counterfactual_inputs.py` — reads the event_time_member panel for all controls in a config combo, computes every candidate × every error metric, writes `fitting_selection.tex` and a backing `fitting_selection.parquet` with all per-org per-candidate errors.
- `source/analysis/model_prediction/plot_counterfactual_input_fits.py` — reads the parquet from the previous step, writes the five PNGs above.

Shared logic into `source/analysis/model_prediction/helpers.py`:
- `ComputeIndividualShares(panel, sample={"pool","last"}, L)` — returns `{actor_id: share}` for each stage.
- `ApplyPartition(shares, partition_rule)` — returns coarsened share vector + cell labels.
- `PartitionShareError(fitted_shares, observed_shares, partition)` — implements $\mathcal R^p$ from §3.3.1.
- `ScaledRMSE(pred, actual, rho)` and `AggregateWeightedError(per_stage_errors, weights)` — implement §3.3.1.
- `EstimateLatentN(panel, rule={"avg_L","last"}, L)`.

### Friction-bound chosen-parameters config

After Phase 2 is approved by the user, the user picks the winning procedure for each input by reading the table. Their choices land in a new file:

- `source/lib/config/model_prediction.json` — keys: `individual_share_estimator`, `contributor_partition`, `latent_flow_rule`, `L`. Loaded via a new `LoadModelPredictionConfig()` added to `source/lib/python/config_loaders.py`.

  This file is what Phase 3 reads. It is not auto-written by Phase 2; the user edits it manually after looking at the table and plots. (This is the "friction" the user asked for.)

  @CLAUDE: you shuld not have the code update it every time but you shuld still write it for me based on the results. 

### SConscript wiring

`source/analysis/model_prediction/SConscript` adds two `env.Python()` calls per (importance × rolling × sample × control) combo, depending on the Phase 1 outputs and the four config files. Tables and plots are <1MB so they go to `output/`, not `drive/output/`. The backing `fitting_selection.parquet` likely <1MB too — keep alongside.

### Verification

1. Pipeline runs end-to-end for one config combo on the reference repo set without errors.
2. The selected row in `fitting_selection.tex` has the strictly smallest aggregate error in its input group.
3. Open one plot manually in Preview to confirm it is readable (CLAUDE.md §3 forced verification).

**→ PAUSE for user inspection. User edits `model_prediction.json` to encode chosen procedures before Phase 3 begins.**

---

## Phase 3 — Predictions on treated orgs + error attribution (paper §3.3.3 + code + outputs)

### Paper edits to `source/paper/model.tex`

Rewrite §3.3.3 *Evaluating departure predictions* to align with the user's intent:

1. **Step 1 (count prediction error)** — keep mostly as-is; the formulas for $\hat Q_{u,k}^{o,(-j)}, \hat Q_{u,k}^{r,(-j)}, \hat Q_{u,k}^{m,(-j)}$ are correct. Clarify that errors are computed at every $k = 0, \ldots, K$, not just averaged, and that the cross-org distribution of $\mathcal R_u^d$ is plotted per stage and per event-time.

   1. @CLAUDE: Please write out the math for how you're calcluating the error, and the range of statistics you're calculaing (liek the different k, s, etc. Also show how you're aggregating this. )

2. **Step 2 (treated individual-share diagnostic).** Keep the high-level diagnostic, but **decompose its error into two sub-components** so we can tell *why* fitted shares miss post-period shares:

   - **(2a) Within-partition fit error.** Hold the partition *fixed at the exact contributor-to-cell identity mapping* derived from the pre-period (i.e. specific actor IDs go in their pre-period cells, everyone else into "other" — *not* the rule re-applied to post-period data). Apply this fixed mapping to the post-period data and compute the partition-share error between (fitted pre-period coarsened shares) and (observed post-period coarsened shares using the same mapping). This captures: "given who I decided to track individually, are the shares I fitted on those people still right post-departure?" @CLAUDE: Please see comments here about **(D-a) Within-partition fit error.** 

   - **(2b) Partition-instability error.** For each post-period $k$, re-run the chosen partition rule on the data observed at $k$ to get a "post-period partition" specific to that $k$. Compare this to the pre-period partition (the exact identity mapping derived from the chosen rule on pre-period data). Operationalize as the partition-share error between (post-period coarsened shares using pre-period identity mapping) and (post-period coarsened shares using post-period-$k$ identity mapping). Reported per $k$, so we can see when the partition drifts most. This captures: "did the people I decided to track individually still belong in the individually-tracked cells at post-period $k$?"

     New joiners always contribute zero to fitted $\hat p$ (we have no pre-period share for them), so they are absorbed into "other" under the fixed pre-period mapping; their growth shows up in (2b).

     Plot per-event-time cross-org distributions of (2a) and (2b) separately, per stage. Also plot the original combined diagnostic $\mathcal R_{u,\mathrm{treated}}^{p,s}$ as a sanity check that (2a) + (2b) qualitatively account for the total.

     @CLAUDE: Please see comments here about **(D-b) Partition-instability error.** 

3. **Drop the old Step 3 (implied aggregate stage rates) entirely.** The user's note: "I don't think we need implied aggregate stage results that's step 1." (The implied $\tilde\pi$ identities are still needed for the oracle attribution in Step 4 — keep the formulas there, just don't make them their own diagnostic step.) Renumber.

4. **Rewrite Step 4 (progressive oracle attribution)** to make explicit the "decompose error contribution of $p_o$" mechanic. Add prose:
   > Under the share-normalized opening model, $\hat\pi_u^{o,(-j)} = 1 - \hat p_{u,j}^o$ is identified from pre-period shares alone. The opening prediction $\hat Q_u^o = \hat N_u (1 - \hat p_{u,j}^o)$ can therefore be wrong only because $\hat N_u$ is wrong: error in the opening channel is fully attributable to mis-estimation of $N$. Substituting $\tilde\pi_{u,k}^{o\mid\hat N}$ for $\hat\pi_u^{o,(-j)}$ is equivalent to replacing $\hat N_u (1-\hat p_{u,j}^o)$ with the actual opening count $Q_{u,k}^o$, so the drop in opening-stage error from this oracle substitution measures the share of total error coming from the joint $(N, p_o)$ identification problem.

   Keep the current oracle formulas. Add an explicit table mapping each oracle level to which stage's error it localizes.

   @CLAUDE: No, the problem isn't that only one of the two components is wrong. it's that we can't separate the difference between either one of them is wrong - we can only jointly say that $\hat N_u (1 - \hat p_{u,j}^o)$  , our prediction, isn't equal to the ACTUAL COUNT OF PULL REQUESTS OPENED. However, something I can speak to is that our prediction $p_{u,j}^o$ which is an aggregate implies that a certain proportion of pulls  are done by $i$ for example. In the real data we can also say that a certain proportion are done by $i$. And we should calculate this error. Please show the math?

   ​	@CLAUDE: I feel like there's something missing here. Maybe the issue is that I can't decompose how much of it's prediction error in N, versus prediction error in THE P OF THE DEPARTED HAD THEY NOT LEFT?? I think this is it - can you write out the math showing this is true, if it is, and the text for how you would describe it? 

5. **Rewrite Step 5 (stage decomposition)** to add the math the user asked for. Currently the model gives the *predicted* decomposition of $E[Q_{t+1}^m] - E[Q_t^m]$ into three channels (opening, review, merge). Add the *empirical* decomposition we compare against:

   - Empirical aggregate stage rates: $\tilde p_{u,k}^o = Q_{u,k}^o / \hat N_u$, $\tilde p_{u,k}^r = Q_{u,k}^r / Q_{u,k}^o$, $\tilde p_{u,k}^m = Q_{u,k}^m / Q_{u,k}^r$.
   - Empirical per-stage marginal contribution of $j$: $\tilde\Delta_{u,k}^o = \tilde p_{u,-1}^o - \tilde p_{u,k}^o$ (etc. for $r$, $m$), where $\tilde p_{u,-1}^s$ is the pre-period analog (last pre-period or pooled, matching the chosen latent-flow rule).
   - Plug $\tilde\Delta$ into the same three-term decomposition formula to get an empirical channel split. Compare to the model-predicted channel split per org; report distributions of the absolute and relative discrepancy.

     Also keep the failure-quantity comparison ($Q^o - Q^r$, $Q^r - Q^m$) — that part is good as-is.

     @CLAUDE I think you need to provide more detail here about how I'm calculating the empirical analogues based on the data (liek what are the subsets you're filtering to) and also indicate the math for at each channel the things you're differencing. 

### Output tables and plots

Code writes to `output/analysis/model_prediction/.../`:

@CLAUDE Like earlier, you should write out the rough format of the tex tables you will format. Keep the same format as earlier so keep in mind my earlier feedback from the step 2 part. Also based on our discussion above update table output and plots. keep in mind we want to try and plot everything to start with. 

**Tables** (`tables/`):
- `prediction_errors.tex` — per stage $d \in \{o,r,m\}$, mean and median of $\mathcal R_u^d$ across treated orgs; plus aggregate $\mathcal R_u^{\mathrm{agg}}$ under both pre- and post-event weights.
- `oracle_attribution.tex` — for each oracle level (none, oracle-o, oracle-{o,r}, oracle-{o,r,m}=ground truth), report mean $\mathcal R_u^{\mathrm{agg}}$. Adjacent rows' differences localize the dominant misfit.
- `stage_decomposition.tex` — per channel (opening / review / merge), mean predicted vs. mean empirical contribution to total merge decline; plus mean absolute discrepancy.
- `treated_share_drift.tex` — per stage, mean cross-org error for (2a) within-partition fit error and (2b) partition-instability error; helps see at a glance whether drift is dominated by share-fit failure or by the partition itself becoming wrong.

**Plots** (`plots/`):
- `prediction_error_dist_{o,r,m}.png` — three plots, one per stage, each showing the cross-org distribution of $\mathcal R_u^d$.
- `prediction_error_by_event_time_{o,r,m}.png` — three plots, the cross-org distribution of normalized error at each $k = 0, \ldots, K$ (boxplots over $k$).
- `treated_share_drift_within_partition_{o,r,m}.png` — three plots, per-event-time distribution of the (2a) within-partition fit error.
- `treated_share_drift_partition_instability_{o,r,m}.png` — three plots, per-event-time distribution of the (2b) partition-instability error.
- `treated_share_drift_combined_{o,r,m}.png` — three plots, the original combined $\mathcal R_{u,\mathrm{treated}}^{p,s}$ as a sanity check.
- `oracle_attribution.png` — bar chart of mean $\mathcal R_u^{\mathrm{agg}}$ at each oracle level.
- `stage_decomposition.png` — for each channel, scatter of predicted vs. empirical per-org contribution; identity line.
- `failure_quantity_errors.png` — cross-org distribution of $\mathcal R_u^d$ for the failure quantities $Q^o-Q^r$ and $Q^r-Q^m$.

### Code to write

Three scripts (analytic-output / plotting separation + a thin runner):

- `source/analysis/model_prediction/predict_treated.py` — reads `model_prediction.json` to get the chosen procedure, reads each treated org's event_time_member panel, produces a per-org per-event-time prediction parquet `predictions.parquet` and a per-org oracle parquet `oracle_predictions.parquet` containing each oracle level's $\hat Q_u^d$ side-by-side with $Q_u^d$.
- `source/analysis/model_prediction/evaluate_predictions.py` — reads the two parquets above; writes the three `.tex` tables.
- `source/analysis/model_prediction/plot_predictions.py` — reads the two parquets; writes the six `.png` plots.

Reuse `helpers.py` from Phase 2 wherever applicable (the partition, share-aggregation, scaled-RMSE, and weighted-aggregate-error functions are all needed again).

### SConscript wiring

`source/analysis/model_prediction/SConscript` adds three more `env.Python()` calls per config combo. Predictions parquet may be >1MB across all treated orgs and can stay in `output/` if it's small enough; otherwise route to `drive/output/`. Determine empirically before final wiring (per the user's "if multiple 1mb and not absurdly so, keep in same folder" rule).

### Verification

1. Pipeline runs end-to-end on at least one config combo without errors.
2. The four oracle levels in `oracle_attribution.tex` are monotonically non-increasing in error (oracle-{o,r,m} should give zero error by construction).
3. Stage-decomposition channel sums (predicted) should match $\hat Q_u^{m,(-j),\mathrm{pre}} - \hat Q_u^{m,(-j),\mathrm{post}}$ to numerical precision; assert this in the script and print a warning if it drifts.
4. Plots open and are readable.

**→ PAUSE for user inspection. Final commit at user's go.**

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

**New files (Phase 2):**
- `source/analysis/model_prediction/helpers.py`
- `source/analysis/model_prediction/fit_counterfactual_inputs.py`
- `source/analysis/model_prediction/plot_counterfactual_input_fits.py`
- `source/analysis/model_prediction/SConscript`
- `source/lib/config/model_prediction.json` (created at the end of Phase 2 by the user)
- New function `LoadModelPredictionConfig()` in `source/lib/python/config_loaders.py`

**New files (Phase 3):**
- `source/analysis/model_prediction/predict_treated.py`
- `source/analysis/model_prediction/evaluate_predictions.py`
- `source/analysis/model_prediction/plot_predictions.py`
- (extends Phase 2's SConscript)

**Paper edits:**
- `source/paper/model.tex` — restructure §3.3.2 in Phase 2; rewrite §3.3.3 in Phase 3.

## Helpers to reuse (do not reinvent)

- `source.lib.python.data_utils.ImputeTimePeriod`
- `source.lib.python.config_loaders.{LoadGlobalSettings,LoadPipelineInputs,LoadAnalysisParameters,LoadPlotSettings}`
- `source.lib.python.repo_utils.MakeRepoNameSafe`
- `source.derived.org_outcomes_practices.helpers.LoadBotList`
- `source.lib.JMSLab.SaveData`
- The `REVIEW_TYPES` set + the `AggregateContributorCounts` / `AggregateOrgCounts` patterns from the deleted `prepare_contributor_actions.py` (lifted into `build_event_time_member_panel.py`).

## End-to-end verification

After all three phases:
```
scons drive/output/derived/model_prediction/ \
      output/analysis/model_prediction/
```
must build clean for the full Cartesian product of (importance × rolling × sample × control). For the reference repo set:
- All six plots in §3.3.3 must render and visually match the paper's claims.
- `oracle_attribution.tex` must show a non-trivial error drop at the opening-oracle row (otherwise either $\hat N$ is perfect or the dataset is wrong — both diagnostically important).
- Compile `source/paper/main.tex` and confirm §3.3.2 and §3.3.3 reference the new tables/plots correctly, with no LaTeX errors.
