# Background findings — the decomposition and the π^r/π^{m|o} constraint

## 1. The stage decomposition: what it is and what +/− values mean

For total merges, `model.tex` defines the squared standardized residual
$$Q(N^m) = \left(\frac{N^m_{\mathrm{obs}} - \mu(N^m)}{\sigma(N^m)}\right)^2 - 1,$$
where $\mu,\sigma$ are the mean and SD of the $K=1000$ full-model draws. Scale: $Q=-1$ ⇔ the observation sits at the model mean; $Q=0$ ⇔ squared error equals the model's own variance (well-calibrated); $Q>0$ ⇔ farther from the mean than the model expects. $Q$ is **signed**, in $[-1,\infty)$.

Algorithm StageOutcomes computes three more copies of $Q(N^m)$ from draws that progressively pin the observed upstream counts: $Q(N^m\mid\mathrm{fix}\,N^o)$, then $Q(N^m\mid\mathrm{fix}\,N^o,N^r)$, then $Q(N^m\mid\mathrm{fix}\,N^o,N^r,N^{m|o})$. The decomposition is the telescoping difference
$$\Delta Q_{\mathrm{open}}=Q(N^m)-Q(N^m\mid\mathrm{fix}\,N^o),\ \ \dots,\ \ \Delta Q_{\mathrm{reviewed\,merge}}=Q(N^m\mid\mathrm{fix}\,N^o,N^r,N^{m|o}),$$
and the four pieces sum **exactly** to $Q(N^m)$. Each $\Delta Q_{\mathrm{stage}}$ = how much the total-merge error changes when that stage's count is additionally pinned to its observed value. The figures plot $\Delta Q_{\mathrm{stage}}/Q(N^m)\times100$ — the stage's share of the total-merge error.

**Why +/−:** each $\Delta Q$ is a *difference* of two $Q$-values, and pinning a count to truth changes both the mean and the variance of the conditional draws, so the squared residual can move either way. $\Delta Q_{\mathrm{stage}}>0$ ⇒ that stage's misprediction contributes to the total error; $\Delta Q_{\mathrm{stage}}<0$ ⇒ fixing it to truth made the total *worse*, i.e. that stage's randomness had been partially cancelling an error elsewhere in the cascade. So the bars are **signed contributions to a signed total**, not slices of a non-negative pie — "every component should be positive" does not hold.

**Caveat:** dividing by $Q(N^m)$ explodes when $Q(N^m)\approx0$; prefer plotting the raw additive $\Delta Q$ (in $Q$-units, exactly additive), or restrict the percentage view to org-periods with $|Q(N^m)|$ above a threshold.

## 2. Why π^r + π^{m|o} can exceed 1

In the data, an opened pull is reviewed-before-merge, merged-directly, or neither — mutually exclusive — so the empirical fractions satisfy (reviewed) + (direct-merged) ≤ 1, matching the multinomial in `model.tex` that requires $\pi^r+\pi^{m|o}\le1$.

The member-level building blocks share the **same denominator** (total opened): $p_j^r=(j\text{'s reviews})/(\text{opened})$ and $p_j^{m|o}=(j\text{'s direct merges})/(\text{opened})$. The asymmetry is the **aggregation**, not the denominator:
- $\pi^{m|o}=\sum_j p_j^{m|o}=(\text{direct merges})/(\text{opened})$ — a clean fraction $\le1$ (each direct merge has a unique merger).
- $\pi^r=1-\prod_j(1-p_j^r)$ — the "at least one reviewer" union form (review is multi-member; $\sum_j p_j^r$ can itself exceed 1). $\pi^r$ stays $\le1$ but systematically **overstates** the true reviewed fraction.

So $\pi^{m|o}$ is the exact `/opened` fraction while $\pi^r$ is inflated, and their sum exceeds 1 in **55.7% of repos** (median 1.011, max 1.259) even though the events are mutually exclusive.

**Decision:** keep the binomial / independent-reviewer (union) form for $\pi^r$. The simplex constraint is then not guaranteed, so an assertion `0 ≤ π^r+π^{m|o} ≤ 1` would fire on the majority of repos; the clip on $\pi^{m|o}/(1-\pi^r)$ currently absorbs it. **Open item:** enforce the constraint either by renormalizing $(\pi^r,\pi^{m|o})$ onto the simplex when it binds (then assert and drop the clip), or keep the clip and log how often/by how much it binds.

## 3. Reading the z-score decomposition panels (`panels/z`)

Each `panels/z` decomposition figure shows, per workflow stage and group (top row treated, bottom control), the cross-org distribution of
$$\left(\frac{\Delta Q_{\mathrm{stage}}}{Q(N^m)}\right)\times Z^m,$$
where $Z^m=(N^m_{\mathrm{obs}}-\mu)/\sigma$ is the **signed** total-merge residual and $\Delta Q_{\mathrm{stage}}/Q(N^m)$ is that stage's share of the total-merge squared error (section 1). So it attributes the *signed* total-merge miss to stages by their error share — the signed analogue of the `panels/q` decomposition.

Reading a stage: a distribution centered at 0 means that stage contributes little to the total-merge miss. A stage shifted **positive** means that stage drives the org's observed total merges *above* the model (the model under-predicts at that stage); shifted **negative** means observed *below* the model (the model over-predicts there). The sign therefore inherits both the direction of the total-merge miss ($Z^m$) and the stage's signed error share, so — exactly as in the Q decomposition (section 1) — a value can flip sign when fixing a stage to its observed value *increases* the total error (a negative share). Read the center and spread of the distribution, not individual points: the $\Delta Q/Q(N^m)$ weight is unstable when $Q(N^m)\approx0$, so extreme tails are mostly that instability rather than signal.

## 4. Post-event decline in opened PRs for control orgs (non-stationarity)

### The finding

The model systematically **over-predicts** future opened PRs ($N^o$) for control (never-treated) orgs in the post-period, and this traces to a real, model-free decline in opening activity after event-time 0 — not to anything about the model.
For control orgs the post-period signed standardized residual on opens has median $\approx-0.58$ (66.6% negative), far below the in-sample ($-0.17$) and leave-one-out ($-0.20$) pre-period baselines that use the *same* fitted distributions, so the post-period shortfall is excess over the mechanical right-skew floor.
In the raw counts, the per-org post/pre ratio of mean opens has **median 0.583** (IQR [0.286, 1.087], 70.2% of control orgs below 1).
The shape is a **gradual decline, not a sharp break**: normalizing each org to its own pre-period mean, the median control org rises to a gentle peak around $t=-2$ (≈0.99), then drifts down steadily through the whole post-period — 0.83 at $t=0$, 0.68 at $t=1$, down to 0.57 at $t=5$ — with the single steepest step at the $t{=}0\to1$ boundary but continued downward drift thereafter.
Treated orgs show the same baseline decline plus a much steeper drop (median to ≈0.21 by $t=5$), i.e. the departure effect sits on top of the control decline.
A second feature drives the over-prediction specifically: the cross-org **mean** moves *opposite* to the median post-event (control mean rises to 1.54 at $t=5$), pulled up by a right tail of orgs that explode in activity.
The model fits each org's latent rate to its pre-period **mean**, so it predicts a level the *typical* (median) org falls well below, while a minority grow — exactly the negative-median-plus-heavy-right-tail residual signature.

### How it is calculated

`issue/control_decline_diagnostic.py` (outputs `issue/control_decline_profile.csv` and `issue/control_decline_profile_ci.png`).
Orgs are split into treated/control by `num_dropouts > 0` at $t=0$ in the analysis panel.
For each org we read its member panel, take repo-level opened counts per event-time, and divide every period by that org's mean opens over the pre-period ($t<0$), so each org contributes a trajectory with pre-period level normalized to 1.0.
At each event-time we then take the cross-org **mean** and **median** of these normalized values (one value per org per period — we observe a single realization per org, so the statistic is cross-sectional, across orgs).
The 95% bands are **bootstrap CIs that resample orgs with replacement** (2000 draws), recomputing the cross-org mean/median each time and taking the 2.5/97.5 percentiles; they quantify sampling uncertainty in the cross-org statistic, not within-org variability, and they explain why the mean band balloons in the post-period (the cross-org mean is unstable under the right tail) while the median band stays tight.

### Interpretation

Qualifying the sample on having an important qualified member for several consecutive pre-periods plausibly conditions on orgs at an **artificially high point in their activity lifetime**.
That conditioning effectively selects the *intense* stretch of an org's life, so by mean reversion activity tends to decline afterwards — which is what the gradual post-event decline in control orgs looks like.
For the **event study** this is likely fine: if the selection-induced inflation hits treated and control orgs the same way, it differences out, and the departure effect is identified off the treated-minus-control gap (which is exactly the extra steepness treated orgs show on top of the control decline).
But for the **predictive model** it is a genuine misspecification: the latent-count distribution is fit to the (inflated, peak-period) pre-period mean and held time-invariant, so it over-predicts the mean-reverting post-period, especially for the typical org.
This motivates a non-stationarity correction in the model (e.g. a post-period attenuation/mean-reversion of the latent rate), separate from the over-dispersion handling — see `model_improvement_proposal.md`.
