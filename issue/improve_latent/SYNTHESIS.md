# Improving the latent problem-arrival model — synthesis (issue/improve_latent)

Scope: `plan.md` item 1 only. Replace the latent problem-arrival mean (currently a flat-forward of the recent-5 opened rate reverted by the δ(q) headcount multiplier) with a **simple, interpretable, non-ML, pre-period-outcomes-only** forecaster. Everything is scored on the REAL framework: the cyborg event study (actual treated + modeled control) at 100 draws via `estimate_model_event_study.R`, and the real `plot_prediction_scatter.py` opened panel. All code in `issue/improve_latent/`; `source/` and the paper untouched.

## Reproduction gate (passed)
`build_draws.py --method baseline` reproduces the committed pipeline's `raw_draws.parquet` **byte-for-byte** (3.14M rows, all 5 outcomes, max abs diff 0), and the ES runner reproduces the committed `band_estimates.csv` exact1 opened rows exactly (raw gap 3.338, norm −0.078). So every candidate number below is on the real estimator.

## Baseline to beat (δ model, cyborg opened, k=1..5)
raw gap **+3.88** (cov 0), norm gap **+0.15** (cov 0); scatter control **β 0.58**, R² 0.375.
Positive gap = modeled control stays high while actual control reverts down = the over-prediction in the flagged raw band.

## Phase A diagnostic (control orgs, opened-cohort)
- **Trajectory bins:** 30% declining / 28% stagnant / 42% increasing (pre-period slope).
- **The typical org reverts DOWN.** Median (opened / pre-mean): declining 0.53→0.30, stagnant 0.57→0.40, increasing 0.80→0.71, all-control **0.60→0.45**. (The equal-weighted MEAN rises to 1.44 by k5, but that is 5 riser repos = 42% of the increasing-bin volume — the median is the true central tendency.) Post/anchor ≈ 0.56 for **every** pre-period anchor (recent-5 mean, full-pre mean, last value): the ~44% decay is a common reversion **not visible in any single org's pre-period levels**.
- **"Overdispersion" is unmodeled heterogeneity.** Cross-repo actual variance is *smaller* than the model's predictive variance (ratio 0.56–0.98), but the PIT is U-shaped with mean 0.41 — the flat-forward mean over-predicts most repos and under-predicts the risers, giving fat-tailed residuals. The fix is the conditional **mean** (trajectory-aware reversion), not the dispersion knob.

## The decisive result: two findings

### 1. Reversion must be imported pooled; per-org-only methods fail.
Because the ~44% decay is invisible in an org's own pre-history, every forecaster that uses only the org's own series fails:

| candidate | family | raw gap | norm gap (cov) | ctrl β | R² |
|---|---|---|---|---|---|
| flat (drop δ, no reversion) | — | +4.17 | +0.20 (0) | 0.56 | 0.35 |
| vasicek_shrink | F1 shrinkage | +4.02 | +0.20 (0) | 0.59 | 0.36 |
| perm_trans | F2 permanent–transitory | +4.08 | +0.23 (0) | 0.60 | 0.40 |
| **damped_trend** | F5 damped trend | **+6.76** | +0.88 (0) | **0.33** | 0.28 |
| cre_proj (slope) | F4b CRE on slope | +5.90 | +0.38 (0) | 0.50 | 0.36 |

`damped_trend` is the worst and is the **spiky-upward failure** you warned against: it extrapolates the rising pre-period slope upward, exactly opposite to the observed downward reversion. `cre_proj` fails the same way (its slope coefficient chases the risers). Canonical cross-sectional shrinkage (`vasicek_shrink`) barely moves because the reversion is not cross-sectional heterogeneity — it is a within-org level drop. **Dropping δ with no replacement (`flat`) is worse than the baseline**, confirming δ was doing real (if insufficient) reversion.

### 2. Pooled cross-fitted reversion works — but there is an irreducible Pareto tension.
Learning the reversion pooled from the control group (5-fold cross-fitted; each org contributes only its pre-period level) and applying it to the org's own pre-level:

| candidate | family | raw gap | norm gap (cov) | ctrl β | R² | β@drop10 |
|---|---|---|---|---|---|---|
| **pooled_decay** | F3 multiplicative decay ×pre-mean | +1.78 | **−0.06 (1.00)** | 0.68 | 0.33 | 0.56 |
| bin_decay | F4a trajectory bins | +1.69 | −0.13 (0) | 0.64 | 0.34 | 0.53 |
| decay_last (ML-distilled) | F4b decay ×pre-last | −1.79 | −0.44 (0) | **1.07** | **0.45** | 0.86 |
| ar1_partial | F3 Fama–French/OU | −1.85 | −0.45 (0) | 1.05 | 0.36 | 0.88 |
| pooled_decay_med | F3 median decay | −2.08 | −0.47 (0) | 1.05 | 0.36 | 0.88 |
| cre_last (ML-distilled) | F4b post ~ last+mean | +1.48 | +0.99 (0) | 0.90 | 0.41 | 0.74 |

- **`pooled_decay` is the only candidate that nails the normalized event study** (gap −0.06, coverage 1.00) and halves the raw gap, with **no spiky-upward** and no over-shoot. It is 5 numbers: c = (0.83, 0.73, 0.78, 0.84, 0.81), forecast_k = c_k · (pre-period mean). β improves 0.58→0.68.
- **`decay_last`** (anchor the reversion on the *last* pre-value, per the ML feature importance) gives the best **scatter** (β 1.07, R² 0.45 ≈ the ML ceiling) but over-reverts the ES.

## Phase C: the ML ceiling proves the tension is fundamental
Out-of-sample (repo-level 5-fold), pre-period **outcomes only**, RandomForest reaches R² **0.53** (through-origin) — GBM 0.48. The dominant feature is **`pre_last`** (importance 0.44), not `pre_mean`. Scored through the same cyborg ES + scatter:

| candidate | raw gap | norm gap (cov) | ctrl β | R² |
|---|---|---|---|---|
| ml_rf | +1.19 | +0.19 (0) | 1.02 | 0.53 |
| ml_gbm | +1.36 | +0.20 (0) | 0.98 | 0.48 |

**Even the ML ceiling cannot drive the norm gap to zero** (+0.19, worse than `pooled_decay`'s −0.06), while it does nail β→1 / R²→0.53 / raw→+1.19.

### Distilling the RandomForest into a simple Poisson GLM
Because the RF leans almost entirely on `pre_last`, it distills into a **Poisson GLM with a log link** that directly parameterizes the latent count mean: `μ_{i,k} = exp(β0 + β1·log(1+last_i) + horizon_k)`. Adding `pre_mean`, the last/mean ratio, or `pre_std` *lowers* OOS R² (collinearity), so **the last value alone is the parsimonious choice**. Fitted `β1 = 0.905 ≈ 1` (β0 = 0.146, small horizon terms): the latent mean is ~proportional to the last pre-period value. With **two coefficients** it recovers OOS R² **0.443** (vs RF 0.53) and reproduces the RF's cyborg profile — `glm_pois`: raw +1.32, norm +0.26, β 0.89, R² 0.45. This is the recommended way to *parameterize the Poisson/NB* from pre-period outcomes.

**Follow-ups (Q1 NB dispersion, Q2 leverage pooled for norm, Q3 trend heterogeneity):**
- **Full NB2 GLM** (`glm_nb`): MLE dispersion α=1.19 (~7× Poisson variance at μ=5, confirming Phase A). Mean/β/R² unchanged, but the correctly-widened band lifts **raw coverage 0→0.80**. Dispersion is a band-width fix; it cannot touch the mean-driven norm gap.
- **Pre-period trend does NOT help** (`glm_pois_trend`): adding norm_slope lowers OOS R² (0.436 vs 0.443), coefficient negative (−0.15: risers revert, no momentum); RF slope importance 0.03. The level (last value) already holds the usable heterogeneity.
- The atheoretic Bates–Granger `combo` (½ pooled_decay + ½ glm) is **dropped** — no economic microfoundation.

### Iteration 2 — enriched features + re-estimating the NB mean
**Enriched pre-period-outcome features add little.** GBM importances on the full feature set: `log_last` 0.36, **`log_n_openers` 0.18** (breadth of opener base), everything else (downstream reviewed/merged levels, conversion, backlog, HHI) <0.05. No enriched GLM spec beats last-only OOS (0.443); the multi-indicator permanent anchor *worsens* norm (permanent composite ≠ opened-mean, the quantity the norm ES normalizes by); n_openers-modulated reversion and RF-on-ratio don't help. Downstream/backlog/composition carry little orthogonal signal for opening.

**Re-estimating HOW the NB mean is fit moves the frontier on every axis** (each a single microfounded model, cross-fitted; `glm_nb` = standard to beat, raw +1.14/norm +0.24/β 0.89/R² 0.44/raw-cov 0.80):

| variant | raw (cov) | norm (cov) | β | R² | verdict |
|---|---|---|---|---|---|
| **Q1 nbrf** (NB with RF mean, α=0.92) | +1.13 (1.0) | +0.18 (0.2) | **1.02** | **0.52** | **dominates glm_nb on all axes** |
| **Q4 glm_eqw** (org-equal weights) | **+1.01 (1.0)** | +0.19 (0.4) | 0.95 | 0.43 | best raw; interpretable |
| **Q2 glm_offset** (exposure offset) | +1.26 (1.0) | **−0.10 (1.0)** | 0.73 | 0.41 | hits norm target; beats pooled_decay |
| Q3 glm_twopart (NB/Poisson split) | +1.25 (0.6) | +0.25 (0) | 0.87 | 0.43 | no gain |
| glm_offset+eqw (stacked) | +5.98 | +0.41 | 0.54 | — | over-corrects — fails |
| nbrf_ratio (RF on ratio) | +2.46 | +0.18 | 0.65 | 0.44 | fails on raw |

Key: **Q1 (RF mean → NB) dominates glm_nb everywhere** — it is the new standard. **Q4 confirms the user's weighting hypothesis**: the Poisson MLE's count-weighting caused β<1/norm+; org-equal weighting (what the norm ES uses) gives the **lowest raw (+1.01)** and β 0.95. **Q2 (exposure offset) hits the norm≈0.1 target** (−0.10, full coverage) and strictly beats the old norm-optimal `pooled_decay`. Still, **norm≈0 and β≈1 cannot both be met in one model** (stacking Q2+Q4 over-corrects); the frontier moved in but did not collapse.

Caveat on "pre-period only": every forecaster's per-org INPUTS are pre-period (the GLM uses only `log(1+pre_last)` + horizon), but the pooled LAW (GLM coefficients, δ replacement, decay curve, NB α) is learned from the control group's post-periods, **cross-fitted 5-fold by repo** so no org trains its own prediction — the same structure as the current δ.

So:
- A **level-optimal** forecast (ML, `decay_last`, `cre_last`) → β≈1, R²≈0.5, raw≈+1.2, but norm≈+0.19.
- A **multiplicative-reversion** forecast (`pooled_decay`) → norm≈0, coverage 1.0, but β≈0.68.
- **No pre-period-outcomes-only forecast (ML included) achieves norm≈0 AND β≈1 at once.** The normalized ES rewards proportional (multiplicative) reversion; the level scatter rewards last-value-anchored calibration. The residual raw-gap floor (~+1.2 even at β=1) is largely the [−5,5]-vs-full-panel window artifact, not control error.

### Window (Q1) and largest-projects (Q2) robustness — the raw gap is mostly artifact
- **Q1 window alignment:** the model draws live only on [−5,5], but the actual is SA-fit on the full member panel by default. Aligning both to [−5,5] removes a **~0.37 constant** from every candidate's raw gap (window artifact, not forecast error). Norm barely moves (genuine).
- **Q2 drop the 3 largest projects** (0x-monorepo, tensorflow/models, arenadata/adcm): they dominate the level-weighted raw gap. `glm_eqw` down-weights them by design so removing them cuts its raw gap; `nbrf` fits them and is unaffected.
- **Combined (fair [−5,5] + drop-3):** `glm_eqw` raw gap = **+0.08** (cov 1.0), `glm_offset` +0.20, `glm_nb` +0.35, `nbrf` +0.53. So `glm_eqw`'s apparent raw over-prediction was ~all artifact + 3 giants — on the like-for-like test it is essentially unbiased.

### The single-flat-post spec (`glm_flatpost`) — breaks the norm-vs-β tension
Dropping the per-horizon dummies and using ONE flat post level — `μ=exp(β0+0.918·log(1+last))`, same for k=1..5, org-equal, NB2 α=1.23 — is the best model found: cyborg opened **raw +0.06 / norm +0.06, BOTH coverage 1.0, β 0.985, R² 0.44** (full window); aligned [−5,5] raw −0.31 / norm +0.03 (cov 1.0). It uniquely achieves norm≈0 AND β≈1 with full coverage. **Why:** the per-horizon dummies were fitting control's per-period means (riser-inflated at k4–5 = the source of the norm-vs-β tension); in the cyborg ES the per-period shape is carried by the shared actual-treated arm, so the modeled-control arm only needs the right LEVEL, not per-period fitting. Normalizing the inputs (ratio/z-score + offset) is bad (raw 4.8/5.7); anchor experiments confirm the single last value beats lags/means; weighting `1/n`=count=`glm_nb` while `1/open`=org-equal (IRLS effective weight = var_weight×μ).

## Recommendation (primary + robustness set)
Replace δ with a pre-period-outcomes-only NB2 parameterization. **Primary: `glm_flatpost`** — org-equal-weighted NB-GLM, single flat post level, `μ=exp(β0+0.918·log(1+last))`, α=1.23; norm≈0 and β≈1 with full coverage, 2 coefficients. **Robustness set kept alongside:** `glm_eqw` (per-horizon; great k1–3, k4–5 rise), `nbrf` (RF mean; best β/R², robust to giants), `glm_offset` (norm≈−0.1), `glm_nb` (count-weighted standard). The δ headcount mechanism is dropped — the reversion is captured directly by the control-estimated NB mean.
- **R²≈0.7–0.8 is not attainable** from pre-period outcomes alone (RF ceiling ~0.53).
- Stratifying by size or trend does not help; the flat-post spec is the one that resolves the norm-vs-β tension. See `tex/improve_latent.tex`.

## Files
`harness/`: `build_draws.py` (reuses real cascade; reproduction gate), `forecasters.py` (per-org methods), `fit_cv_forecasters.py` (cross-fitted reversion + CRE), `fit_ml_forecast.py` (ML ceiling as a scorable table), `run_es_exact1.R` + `make_control_isolation.py` (real cyborg ES), `gen_scatter.py` (real scatter), `phase_a_diagnostic.py`, `phase_c_ml_ceiling.py`, `plot_bands.py`, `compile_scoreboard.py`. `results/scoreboard.csv`, `results/es_bands_*.csv`, `results/scatter_opened_*.csv`. `figures/<tag>/`. `tex/improve_latent.tex`.
