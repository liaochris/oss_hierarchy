# Model-prediction: checks, diagnostics, and predictive-accuracy improvements

This is the working plan for verifying the model-prediction calculations, improving the diagnostics, and refining the model's predictive accuracy. Tasks are ordered so that small, low-risk changes come first and expensive recomputation is batched. Each substantive change is its own commit.

Companion documents:
- `future_exploration.md` — the stage decomposition (and what positive/negative values mean) and the π^r/π^{m|o} constraint.
- `model_improvement_proposal.md` — candidate refinements to the generative model, framed around the fit statistic.

Legend: ✅ done · ◻ pending · ⏸ awaiting decision.

## Conceptual checks (resolved)

- **Sample restriction (exact1 / exact2 / exact_1_2):** ✅ The model-prediction sample is identical to the event-study sample for all three; no change needed. (`exact_1_2` equals the disjoint union of `exact1` and `exact2`.)
- **Intermediate-stage randomness:** ✅ Already handled — each stage is drawn from the previous stage's *draws* (not point estimates), so downstream Z/Q carry the full cascading uncertainty.
- **Alignment with `model.tex`:** ✅ The implementation matches the math except (i) the treated differenced residual $\tilde Q^{treated}=Q^{post}-\bar Q^{LOO}$ (Phase 3b below) and (ii) the departure-effect event-study visualization (deferred).
- **Decomposition units / negative values, and the π^r/π^{m|o} constraint:** ✅ Explained in `future_exploration.md`.
- **In-sample pre-period fit — when is it useful?** ✅ It is the misspecification floor (can the family even cover the training data?). With overdispersed counts a per-org Poisson commonly fails in-sample, so the panel is worth keeping, framed as that floor.
- **Why Q is large for control orgs in pre-period LOO:** ✅ Systemic Poisson overdispersion (≈87% of control orgs overdispersed; median var/mean ≈ 4). This motivated the adaptive distribution below and the simulated-null KS reference.

## Phase 1 — Figure and diagnostic cleanup ✅

- ✅ **1a.** Distinctive negative numbers in the summary-stat boxes (bold wide minus).
- ✅ **1b.** Fixed the truncated-count bars overlapping the last histogram bar (drawn flush outside the range).
- ✅ **1c.** Per-PNG, data-adaptive, whole-number x-axis bound shared across subplots within a figure (so in-sample panels are no longer an unreadable spike).
- ✅ **1d.** Combined the control and treated panels into single two-row figures for `post_period_fit`, `post_period_decomp`, and `pre_period_decomp`; updated `model.tex`; deleted the old separated files.
- ✅ **1e.** Added a global `evaluation_figures` option (`panels` only for now) and removed the per-org `individual/` plots.
- ✅ Refactored `evaluate_predictions.py` into modular primitives (`DrawDistribution`, `StatsText`, `RenderPanelGrid`).
- ✅ Updated the evaluate-stage SConscript targets to the combined panel names, and made the `individual/` targets conditional on the `evaluation_figures` option (so scons no longer recreates `individual/` under panels-only).
- ✅ Forced the non-interactive `Agg` matplotlib backend (was the slow interactive `macosx`) — the main figure-generation slowdown.

## Adaptive latent-count distribution ✅

- ✅ Replaced the Poisson-vs-Negative-Binomial choice with a single **adaptive** per-organization fit: a method-of-moments Negative Binomial when the opened counts are over-dispersed, and a Poisson otherwise (the NB cannot represent under-dispersion, and nests the Poisson). Renamed the fitting routine to `FitLatentDistribution`; the draw step now samples from each org's chosen family. Documented the math in `model.tex`. (Regenerate outputs via scons.)

## Phase 2 — Output folder rename ◻

- ◻ Rename `…/fitted/{dist}/…` → `…/{dist}/parameters/…` and `…/{dist}/predictions/…` → `…/{dist}/residuals/…` for clarity (the `predictions` folder holds per-org residuals, distinct from the `evaluation` figures). Touches the SConscript and the three scripts.

## Phase 3 — Metric and statistic changes ◻ (each handled and committed separately)

- ◻ **3a.** Replace the analytic KS reference (N(0,1) for Z, χ²(1)−1 for Q) with a **simulated-null** reference built from the existing K=1000 draws — standardize each draw by its own org's mean/SD to get the model-implied null, pool across orgs, and KS-compare. This accounts for discreteness, zero-truncation, and cross-org rate heterogeneity, which the analytic references do not.
- ◻ **3b.** Compute the treated **differenced residual** $\tilde Q^{treated}_i(x)=\bar Q^{post}_i(x)-\bar Q^{LOO}_i(x)$ and integrate it into the existing post-period figure (`model.tex` already defines it).
- ⏸ **3c.** A single fit statistic (simulated-null KS on PRs merged) plus a model-selection table over the pooled/per-period estimators.
- ⏸ **3d.** Compare treated-post vs. control-post (and pre-period-LOO vs. control-post) Z distributions to test whether the model is systematically worse for treated orgs.
- ⏸ **3e.** Use the control-post error (pure model error) as the misspecification baseline to separate model error from departure-specific error.
- ⏸ **3f.** Report a per-outcome variance-explained / pseudo-R² measure.

## Phase 4 — Model refinements ◻

- ◻ Beyond the adaptive distribution, evaluate (via the Phase-3 KS statistic) the refinements in `model_improvement_proposal.md`: zero-inflation / hurdle latent counts, regularization or an aggregated "other" member, and modeling churn (entry/exit). Implement the best one or two.

## Phase 5 — Documentation and paper (last) ◻

- ◻ Add a section for `source/derived/model_prediction` to `DATA_APPENDIX.MD`.
- ◻ In `model.tex`, make **PRs merged** the single main-text outcome and move the other four to an appendix; replace the figure stubs with the generated panels and write the interpretation.

## Open decisions

- ⏸ **Event-study exact_1_2:** model_prediction already reads the upstream panel; only the event study aggregates `exact1`+`exact2`. Change the event study to read the upstream panel (changes the estimator), or leave as is?
- ⏸ **π^r/π^{m|o} constraint:** renormalize onto the simplex (then assert, drop the clip) or keep the clip and log when it binds. See `future_exploration.md`.
