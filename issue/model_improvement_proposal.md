# Model improvement proposal: fixing the post-period over-prediction of opens

## Scope and accuracy metric

The goal is to refine the staged count model in `source/paper/model.tex` so its predictive distribution stops systematically over-predicting future opens `N^o`, where "better" is measured by a single calibration fit-statistic.

The accuracy metric is a **two-sample simulated-null Kolmogorov–Smirnov statistic** (`scipy.stats.ks_2samp`) comparing the empirical per-(org,period) standardized residual `Z` to the model's own standardized draws.
Concretely:

- For each org `i` we draw `K=1000` outcome realizations from the fitted model (`DrawCounts` in `predict_model.py`) and standardize each draw by its own draw-implied moments, `Z_i^(k) = (x_i^(k) - mu_i)/sigma_i`; pooling these across orgs gives the model-implied null (this already encodes the right-skew and zero-truncation of small-count orgs, which a textbook `N(0,1)` reference does not).
- The empirical `Z_{i,t} = (x_{i,t}^obs - mu_i)/sigma_i` is pooled across orgs and the statistic is `D = sup|F_emp(z) - F_null(z)|`; lower `D` = better calibration.
- Evaluate on **leave-one-out pre-period** (LOO: refit on `T-1` pre-periods, score the held-out one — pre-period generalization) and **control post-period** (pure out-of-sample model error, since controls have no focal departure), on **both `total_merge` and `N^o`**, since `N^o` is the diagnosed problem and every downstream count is a thinning of it.

The reference PDFs (`issue/reference_pdfs/ad-price-drivers.pdf`, `qjae041.pdf`) were not opened (prior background attempts hung on them); the techniques below — drift/trend modeling, recency weighting, shrinkage toward a baseline, honest out-of-sample calibration, parsimony — are cited as standard method rather than quoted from those papers.

## Diagnosis

**1. The post-period over-prediction is real and large (control orgs, `pooled/important_degree_top3/exact_1_2/nevertreated`).**
For the signed standardized residual on opens, `signed_std_residual_open`:

| sample | n | mean | median | frac < 0 |
|---|---|---|---|---|
| in-sample (pre) | 3480 | +0.001 | −0.171 | 0.561 |
| leave-one-out (pre) | 3473 | +0.209 | −0.195 | 0.564 |
| **control post-period** | 3480 | +0.105 | **−0.576** | **0.666** |

The post-period median `Z` (−0.576) is far more negative than the in-sample (−0.171) or LOO (−0.195) baselines, which use the *same* fitted distributions.
A negative median `Z` is partly mechanical — a right-skewed count distribution has mean > median, so even a perfectly calibrated model gives median `Z < 0` — but that mechanical floor is the −0.18 in-sample/LOO figure; the *excess* post-period negativity (−0.58 vs −0.18) and the right-skewed positive mean are genuine over-prediction with a heavy right tail (a minority of orgs that grow).
`total_merge` inherits the same pattern (post median −0.677 vs LOO −0.282), as expected since merges are thinned opens, so fixing `N^o` is the lever.

**2. Direct test of non-stationarity confirms a systematic decline (1158 control orgs, raw member panels, model-free).**
Per org, mean opened count in post-periods (`quasi_event_time > 0`) vs pre-periods (`< 0`):

- post/pre ratio: **median 0.583**, IQR [0.286, 1.087], **frac < 1 = 0.702**.

Seventy percent of control orgs open fewer PRs post than pre, with the median org dropping ~42%; the mean ratio (1.089) is pulled above 1 only by a right tail of growing orgs — the same right tail seen in the residuals.

**3. The decline is a peak-and-decay around the event boundary, not a trend visible inside the pre-window.**
The control `Z_open` median worsens monotonically across the post-horizon: −0.508, −0.532, −0.579, −0.634, −0.655 for `quasi_event_time` 1–5, i.e. the gap *grows* the further past the event we predict — consistent with an ongoing post-event decay, not a one-time level shift.
Yet within the pre-window the per-org log-linear slope of opens on event-time is mildly *positive* (median +0.025/period, only 47% of orgs declining), so there is **no consistent downward trend inside the pre-period to extrapolate**.
The picture is a sampling/maturity peak near event-time 0 followed by mean-reverting decay — orgs enter the cohort at a high-activity moment, and activity reverts afterward.

**This has a sharp implication for the candidates: a downward decline that is invisible (or reversed) inside the pre-window cannot be captured by naively extrapolating a per-org pre-period trend forward, nor by up-weighting recent pre-periods (which sit near the peak) — both would predict a *rise* and make the over-prediction worse.**
The decline must instead be captured by a correction learned *across* orgs (a global attenuation / mean-reversion) or by shrinking each org's pre-mean toward a lower baseline.
The previously-diagnosed over-dispersion (large positive `Q`) is a separate, variance-side problem already addressed by the adaptive NB/Poisson choice; it does not explain the negative-median *bias*, and inflating variance alone (e.g. a random walk) will not remove the bias.

---

## Candidate A — Global post-period attenuation of the latent rate (RECOMMENDED FIRST)

**Mechanism.** Multiply each org's fitted latent rate by a single global post-period factor `delta in (0,1]` when predicting the post-period: `lambda_i^post = delta * lambda_i^pre` (for NB, scale the mean and re-solve `size`, `prob` at the fitted dispersion).
With horizon-decay, `lambda_{i,t}^post = delta^t * lambda_i^pre`, `t = 1..5`, capturing the monotone worsening in Diagnosis 3 with the same one parameter.
The median post/pre ratio of 0.583 is the model-free target for `delta` (a horizon-0 anchor).

**Which failure it fixes.** Directly removes the negative-median bias in `N^o` (and hence `total_merge`) by lowering the predicted mean to match the post-period level; it is the most parsimonious capture of whatever drives the decline (maturity, mean reversion, natural churn).

**Implementation.** In `predict_model.py`, scale `dist_params` before the post-period `DrawCounts` call (control branch currently reuses the in-sample draws at lines 231–238 — give controls their own attenuated post draws); pre-period in-sample/LOO draws are unchanged.
Add `delta` (and optional per-horizon `delta^t`) to `MODEL_PREDICTION_CONFIG`.
Parameter impact: **+1 global** (or +1 with a horizon exponent).

**Selection via KS.** Sweep `delta` on a grid and pick the value minimizing the control post-period KS `D` on `N^o`; to keep this honest (the control post-period is also the evaluation set), select `delta` by **leave-one-org-out CV across control orgs** so the chosen value is not fit to the same rows it is scored on, and confirm it does not worsen the LOO pre-period KS (pre-period draws are untouched, so it should not).

**Risks.** A single `delta` is a blunt average over heterogeneous orgs (IQR [0.29, 1.09]); it corrects the median but not the right-tail growers, so it lowers but will not zero the KS `D`.
If `delta` is tuned on the control post-period and then reused for treated orgs, the treated departure effect is measured relative to an attenuated baseline — correct in principle (it removes the mechanical decline so the residual is the departure effect), but the CV discipline above is essential to avoid circularity.

## Candidate B — Shrink each org's pre-mean toward a pooled baseline (regression to the mean)

**Mechanism.** Replace the org pre-mean used to fit `F_i` with a shrunk mean `lambda_i = w * mean_i^pre + (1-w) * lambda_bar`, where `lambda_bar` is the cross-org (or size-stratified) pooled mean and `w in [0,1]`.
Because orgs are sampled at an activity peak, the pre-mean is an upward-biased estimate of the post level; shrinking it toward a common baseline is the textbook regression-to-the-mean correction and reduces the over-prediction for high-pre orgs while barely moving typical ones.

**Which failure it fixes.** The same negative-median bias as A, but org-specifically: it attenuates *more* for orgs whose pre-mean is far above baseline (the ones that revert most), which A cannot do with a single multiplier.

**Implementation.** A shrinkage step at the end of `FitLatentDistribution` in `model_fitting_utils.py` (compute `lambda_bar` once per combination, pass it in); recompute NB `size`/`prob` from the shrunk mean at the fitted dispersion.
Parameter impact: **+1 global** (`w`, or the prior strength).

**Selection via KS.** Tune `w` by the same control-post KS `D` on `N^o` under leave-one-org-out CV; unlike A this *also* changes the pre-period (in-sample/LOO) fit, so check both KS numbers and prefer the `w` that improves control-post without degrading LOO.

**Risks.** If the peak is common to all orgs, shrinking toward a cross-org mean that is *itself* inflated under-corrects; consider shrinking toward each org's own longer-run (pre-cohort) level if available.
Couples the latent-rate fit to a pooled quantity, slightly complicating the per-org interpretation.

## Candidate C — Per-org temporal trend / exponential decay fit on the pre-period, extrapolated

**Mechanism.** Replace the constant pre-mean with a fitted trajectory `log lambda_{i,t} = a_i + b_i * t` (Poisson/NB GLM on pre-period opens vs event-time) and extrapolate `b_i` into the post-period; an exponential-decay parameterization is the same idea with `b_i < 0` enforced.

**Which failure it (does not cleanly) fix.** This is the natural "non-stationarity" candidate, but Diagnosis 3 shows it **backfires here**: the median pre-window slope is *positive* and only 47% of orgs decline in-pre, so extrapolating the per-org pre-trend forward predicts a *rise* for the majority and worsens the over-prediction.
It can only help if the slope is sign-constrained to be non-positive or heavily damped — at which point it collapses toward Candidate A (a shared downward attenuation) with far more parameters and noise.

**Implementation.** A GLM fit per org in `FitLatentDistribution` (+1 slope parameter per org, `2J` total) and a time-indexed `lambda_{i,t}` threaded through the post-period `DrawCounts`.

**Selection via KS.** Same protocol, but it must clear the bar that A/B set; given the diagnosis, expect it to *lose* unless damped.

**Risks.** Over-fits with small `T`; per-org slopes are noisy and, as shown, mostly point the wrong way. **Lower priority than A/B.**

## Candidate D — Recency weighting / fewer, more-recent pre-periods

**Mechanism.** Weight recent pre-periods more (exponential weights) or fit on only the last `k` pre-periods, on the premise that recent data better predicts the near future.

**Which failure it (does not) fix.** Because the series peaks near event-time 0, the most-recent pre-periods are the *highest*, so up-weighting them **raises** the predicted rate and worsens the over-prediction — the opposite of what is needed.
The diagnosis instead argues for *down*-weighting the recent (peak) pre-periods or using a longer window that dilutes the peak, i.e. recency weighting with the sign reversed.

**Implementation.** A weight vector in the pre-period aggregation in `fit_model.py`/`FitMemberProbabilities`; no parameter-count change (a config sweep over window length / decay weight).

**Selection via KS.** Cheap config sweep; report but expect standard recency weighting to lose.
**Risks.** Short windows make per-member `p_j^s` and NB dispersion noisy. **Low priority; useful mainly as a sensitivity check confirming the sign.**

## Candidate E — Drift / random-walk latent rate

**Mechanism.** Let `log lambda_{i,t}` follow a random walk (optionally with drift), `log lambda_{i,t} = log lambda_{i,t-1} + mu + eps_t`, and integrate the rate uncertainty into the post-period draws.

**Which failure it fixes.** A driftless random walk widens `sigma_i` (helps the over-*dispersion* / large positive `Q`) but leaves the predicted *mean* unchanged, so it does **not** remove the negative-median bias — the diagnosed problem.
Only the **drift** term `mu` moves the mean, and a negative `mu` is just Candidate A/C in disguise; the random-walk variance is a second-order calibration gain on top.

**Implementation.** Heaviest: state-space rate threaded through `DrawCounts` (+1–2 params/org or +1–2 global).
**Selection via KS.** Could help the `Q`/dispersion side once the bias is fixed; tune jointly with the adaptive distribution to avoid double-counting variance.
**Risks.** Competes with NB for the same overdispersion; complexity-to-payoff is poor for the *bias* problem. **Defer.**

## Re-ranked previously-listed candidates (now secondary to the non-stationarity fix)

- **Over-dispersion (adaptive NB/Poisson)** — *done.* It addresses the variance side (`Q`), not the negative-median bias; keep it, but it is orthogonal to this proposal.
- **Member-parameter regularization / aggregated "other" member** — *medium, secondary.* Reduces LOO refit noise in `pi^s`; helps the `Q` over-dispersion in pre-period LOO but not the post-period bias. Worth doing after A/B for the variance side; aggregating into a focal+"other" bucket is exact for the additive stages (`pi^o, pi^{m|o}, pi^{m|r}`) and needs care only at the nonlinear review stage.
- **Churn / entry–exit (relax fixed membership)** — *promoted: this is the likely structural mechanism behind Candidate A.* The model holds the pre-period member set fixed; if members naturally attrite post-event (controls churn too), summed `prob_open` over a stale member set over-predicts opens. Modeling post-period active-set shrinkage would produce the decline *structurally* rather than via a reduced-form `delta`. Try after confirming A works, as the explanation for *why* `delta < 1`. Note the **member stage-probabilities `p_j^s` may themselves drift** post-event (less reviewing, slower merging), a parallel channel worth checking once opens are fixed.
- **Zero-inflation / hurdle `F_i`** — *low.* Active OSS orgs rarely have zero-open months; unlikely to be the issue. Check the pre-period zero-open share before implementing.

---

## How candidates are selected with the KS statistic (single protocol)

1. Run `fit_model.py` + `predict_model.py` for the candidate to get the `K=1000` draws and per-(org,period) `Z` for `N^o` and `total_merge`.
2. Build the simulated null by pooling the standardized draws across orgs (`ReferenceFrame` already emits these).
3. Compute `ks_2samp(empirical_Z, null_Z)` `D` on **(a) LOO pre-period** and **(b) control post-period**, for **both `N^o` and `total_merge`**.
4. Rank by the **control post-period KS `D` on `N^o`** (the diagnosed failure), subject to not worsening the LOO pre-period `D` (guards against trading generalization for fit); use `total_merge` as the downstream confirmation.
5. Guardrail: any tuning parameter (`delta`, `w`, window length) must be selected by **leave-one-org-out CV across control orgs**, never on the full post-period it is scored against, or the KS `D` is contaminated.

`evaluate_predictions.py` already supports the simulated-null reference; adding `N^o` alongside `total_merge` in the KS table is the only reporting change needed.

## Recommended first thing to try

**Candidate A — a single global post-period attenuation factor `delta` (with an optional horizon decay `delta^t`) on the latent rate, selected by leave-one-org-out CV on the control post-period KS `D` for `N^o`.**
It is the lowest-effort change (one parameter, scale `dist_params` before the control/post `DrawCounts`), it targets the diagnosed failure directly (lowers the predicted mean to kill the negative-median bias), the model-free target `delta ≈ 0.58` is already in hand, and it leaves the pre-period fit untouched so it cannot harm LOO.
If `delta` helps but leaves the high-pre right tail miscalibrated, follow with **Candidate B** (org-specific shrinkage toward a pooled baseline), then investigate **churn** as the structural reason `delta < 1`.

## Summary

The model over-predicts post-period opens for control orgs: post-period median signed `Z` is −0.576 vs a −0.18 in-sample/LOO baseline, and 70% of control orgs open fewer PRs post than pre (median post/pre ratio 0.583), with the gap widening across the post-horizon — a real, non-mechanical decline.
Crucially the decline is a mean-reverting peak around the event boundary, *invisible inside the pre-window* (median pre-trend is mildly positive), so per-org trend extrapolation and recency weighting both backfire and are demoted.
The recommended fix is the most parsimonious capture of the decline: a single global post-period attenuation factor `delta` (≈0.58, optional horizon decay) on the latent rate, selected by leave-one-org-out CV on the control post-period KS `D` for `N^o`, followed by org-specific shrinkage (B) and, structurally, post-event churn — with over-dispersion (NB, done) and member-parameter regularization kept for the separate variance side.
