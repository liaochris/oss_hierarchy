# model.tex improvements — prediction and evaluation sections

File: `source/paper/model.tex`

---

## Small changes (§1.2)

1. **Rename subsection** "Opening, review, and merge as person-specific Bernoulli actions" → e.g. "Individual contributions to organizational outcomes"
2. **Explicitly define $p_{j,t}^s$**: add one sentence near the top of §1.2 — "For stage $s \in \{o, r, m\}$, let $p_{j,t}^s \in [0,1]$ denote member $j$'s probability of performing a stage-$s$ action on any given unit at time $t$."
3. **Cite Assumptions 4 and 5 explicitly** after each distributional derivation: opening multinomial → Assumption~\ref{asm:problem-invariant}; review binomial → additionally Assumption~\ref{asm:review-independence}; merge multinomial → Assumption~\ref{asm:problem-invariant}.

---

## New §1.3: Fitting model parameters

1. **Identify latent count**: impose $F_{i,t} = F_i$ (time-invariant flow) and normalization $p_0^o = 0$ in pre-periods, so $N_{i,t} = N_{i,t}^o$ is observed directly.
2. **Fit $F_i$**: choose one of four global specifications applied to all orgs (with org-specific parameters estimated by MLE):
   - Poisson or Negative Binomial
   - × pooled $p_j^s$ (closed-form MLE across all pre-periods) or per-period $p_j^s$ (estimated within each period, then averaged)
3. Model selection across the four combinations will be addressed separately.

---

## New §1.4: Evaluating model fit on training data

**Motivation**: understand how well the fitted model reproduces the pre-period data.

### Drawing from the model

Define the simulation procedure:
1. Draw $N$ from $\hat{F}_i$
2. For each problem, draw Bernoulli($\hat{\pi}^o$) for open/no
3. For each opened, draw Bernoulli($\hat{\pi}^r$) for review/no
4. For each reviewed, draw Bernoulli($\hat{\pi}^m$) for merge/no

Repeat $K = 1000$ times per org-period.

*Note*: Use \todo environment - look into whether I can replace drawing with analytically solved distribution. 

### Two evaluation modes (different questions)

**In-sample**: fit model on all 5 pre-periods; compare draws (from that same fitted model) to each of the 5 pre-period observations. Measures in-sample fit — whether the model's distribution covers the data it was fitted on. Not trivially good: a Poisson can genuinely misfit even in-sample.

**LOO**: for each pre-period $t$, fit on the other 4, draw from that LOO model, compare to period $t$. Measures whether the model is correctly specified for the pre-period — i.e., whether parameter estimates from 4 periods generalize to the held-out 5th. More demanding than in-sample.

**Primary metric** $A = Z^2 - 1$ in both cases: use empirical mean $\mu_\text{draws}$ and SD $\sigma_\text{draws}$ from $K$ draws; $Z = (x_\text{obs} - \mu_\text{draws})/\sigma_\text{draws}$; $A = Z^2 - 1$. Under correct specification $\mathbb{E}[A] = 0$.

**Four outcomes evaluated separately** (same procedure in both modes):
1. $N^o$: draw $N$ from $\hat{F}_i$, apply $\hat{\pi}^o$
2. Review rate (cond. on actual $N^o_\text{obs}$): fix at observed opens, draw review stage
3. Merge rate (cond. on actual $N^r_\text{obs}$): fix at observed reviews, draw merge stage
4. $N^m$ total: full chain draw

### Quantitative stage decomposition of $A$

Decompose $A_\text{total}$ into three additive components per org-period:

| Component | Computation | Interpretation |
|---|---|---|
| $\Delta A_\text{open}$ | $A_\text{total} - A_{\text{fix }N^o}$ | Error from opening stage |
| $\Delta A_\text{review}$ | $A_{\text{fix }N^o} - A_{\text{fix }N^o,N^r}$ | Error from review stage, given actual opens |
| $A_\text{merge}$ | $A_{\text{fix }N^o,N^r}$ | Residual from merge stage, given actual reviews |

$A_{\text{fix }N^o}$: condition on actual $N^o_\text{obs}$, draw review and merge.
$A_{\text{fix }N^o,N^r}$: condition on actual $N^o_\text{obs}$ and $N^r_\text{obs}$, draw only merge.
These sum exactly to $A_\text{total}$ by construction.

---

## New §1.5: Evaluating predictions on control and treated data

**Motivation**: the departure prediction requires two things — (a) the model generalizes out-of-sample absent a departure, and (b) the departure-specific effect is predicted correctly. Control orgs evaluate (a); treated orgs evaluate (a)+(b) combined.

### Control organizations (never-treated)

Fit model on all 5 pre-periods; predict post-period using the full membership set; draw $K = 1000$; compute $A^{post}$.

Differenced residual $\tilde{A}_{i,t} = A^{post}_{i,t} - \bar{A}^{pre}_i$ (where $\bar{A}^{pre}$ uses the **in-sample** pre-period A): the differencing removes baseline misfit $\Delta(\hat{F}_i, G_i)$ (proven in `plan/error_decomp.md`). For control orgs, large $\tilde{A}$ indicates the model fails to adapt to temporal changes — i.e., the pre-period parameterization does not generalize to the post-period. This diagnoses both model misspecification and temporal non-stationarity.

### Treated organizations

- $A^{pre}$: **in-sample** pre-period A (from §1.4, using all 5 pre-periods)
- $A^{post}$: post-period counterfactual prediction (remove $j^*$, draw $K = 1000$ from model)
- $\tilde{A}^{treated} = A^{post} - \bar{A}^{pre}$: departure-specific prediction error, net of baseline misfit

**Why in-sample (not LOO) for $\bar{A}^{pre}$**: $A^{post}$ is computed under the model fitted on all 5 pre-periods. For the differencing to cancel the same model's misfit, $\bar{A}^{pre}$ must also use that same model. LOO pre-period A uses different (4-period) parameters, so subtracting it would remove $\Delta(F^{LOO}, G)$ instead of $\Delta(F, G)$ — wrong baseline.

Comparing $\tilde{A}^{treated}$ to $\tilde{A}^{control}$: control $\tilde{A}$ captures temporal generalization error; excess in treated $\tilde{A}$ isolates the departure-specific prediction error. Report both raw $A^{post}$ and $\tilde{A}$.

---

## New §1.6: Visualizing departure effects (event study)

**Motivation**: compare the distribution of TEs the model predicts to the actual TE.

### Procedure
1. For each treated org, draw $K = 1000$ simulated post-departure datasets from the fitted model (with $j^*$ removed).
2. Estimate the event study on each simulated dataset.
3. Plot the distribution of simulated TEs; overlay with actual TE.

### Why many draws, not one draw + Bayesian bootstrap
- **Many draws**: answers "across all datasets the model could generate, what TEs are plausible?" — correct for checking whether actual TE is consistent with model predictions.
- **One draw + bootstrap**: answers "how precisely can we estimate the TE from one simulated dataset?" — captures estimation noise for a single realization, not DGP randomness. For evaluating predictions, many draws is more direct.

---

## Resulting structure

```
§1.1 Organizational workflow                    <- done
§1.2 [renamed]                                  <- small fixes above
§1.3 Fitting model parameters                   <- new
§1.4 Evaluating model fit: training data        <- new (in-sample + LOO, draw-based A)
§1.5 Evaluating predictions: control + treated  <- new (out-of-sample, differenced A)
§1.6 Visualizing departure effects              <- new (many draws, event study)
```
