# Churn & reaction: a control-disciplined framework for the departure counterfactual

Scope: the **review / direct-merge / after-review-merge** stages, conditional on opens (opens and mean-reversion are handled separately).
Evidence: `measure_churn_reaction.py` → `output/analysis/model_prediction/opened_cohort/adaptive/evaluation/pooled/important_degree_top3/exact1/nevertreated/churn_reaction/` (`churn_decomposition.png`, `reaction_by_event_time.png`, `churn_reaction_tables.txt`).
Sample: 562/571 repos after the event-study outlier trim; 141 treated / 430 control; post = quasi event time 1–5.

## 1. The problem

The paper's counterfactual (Assumption 10, "no-reaction departure") fixes the membership 𝒥, removes only j\*, lets no survivor react, and adds no new members.
It is deliberately the **maximal-decline benchmark**, and it over-predicts the decline in pulls reviewed and merged.
Three things break it: **(a) natural dropout**, **(b) natural entry**, **(c) survivor reaction**.
The trouble is that these are easy to conflate, and conflating them leads to fitting the wrong mechanism.

## 2. Four reframings

**Reframe 1 — Condition on opens.**
Study each stage as a per-opener rate over its own denominator (review and the two direct-merge splits per opened pull; the two after-review splits per reviewed-opened pull).
These rates are already separable in estimation, so working at the rate level removes open-count error entirely and sits exactly where reaction lives.

**Reframe 2 — The flow is people, not a frozen roster.**
π^s = Σⱼ (opener-share ⱼ)·pⱼˢ, and the opener population turns over: surviving incumbents (at possibly-changed rates) + entrants (with their own profile) − leavers.
The no-departure counterfactual keeps j\* in but must let the rest of the churn process run as it would anyway.

**Reframe 3 — Controls identify natural churn; treated−control identifies the departure response.**
Controls churn but experience no departure, so their prediction error is *pure* forces (a)+(b); the treated error is that **plus** the departure response.
This is the paper's own open TODO (main.tex:1098: "controls pin down the distributional drift, so the residual gap is attributable to the departure counterfactual").
**This reframing is the whole point of the memo — it is what catches the error in §5.**

**Reframe 4 — Honest identifiability.**
Baseline churn *rates* (exit hazard, entrant arrival, entrant behavior profile) are pre-period/control identifiable.
The **vacancy-response** — how much of j\*'s freed load gets picked up — is **not** pre-period identifiable, because there is no vacancy in the pre-period.
It can only come from control *complete-departure* episodes (analog events) or a steady-state argument.
Likewise entrants have no pre-period row by construction, so their rates can never be estimated per-org from the pre-period; they must come from a pooled entrant profile.

## 3. The three forces

| force | model object | identified from | economic mechanism |
|---|---|---|---|
| (a) natural dropout | incumbent exit hazard / rate decline | pre-period + controls | churning (Davis–Haltiwanger–Schuh) |
| (b) natural entry | entrant arrival rate + conditional-behavior profile | controls (never per-org pre) | vacancy chains / recruitment (White 1970) |
| (c) reaction | Δ conditional stage rates, pickup reallocation | treated−control, post−pre | review-capacity shock ⇒ the shadow price of review rises ⇒ reallocation of who reviews and who merges |

## 4. What the data actually show

### (a) Natural dropout — no cascade
Treated and control incumbent active-fractions track each other almost exactly (`churn_decomposition.png`, left).
Treated sits slightly below control in every post period (0.069/0.051/0.045/0.032/0.027 vs 0.075/0.060/0.056/0.049/0.044), so there is a *mild* extra withdrawal — but nothing resembling a cascade.
**Dropout is essentially natural churn; the departure does not trigger a wave of further exits.**

### (b) Natural entry — large, and the dominant post-period source of work
Entrants supply **1350 of the 2059 treated post-period opens (66%)**, against 709 from all surviving incumbents combined.
Entrant open-share runs 0.58–0.71 for treated vs 0.44–0.60 for control (`churn_decomposition.png`, right) — though part of the treated excess is mechanical, since losing j\* shrinks the incumbent denominator.
Entrants are **review-heavy and direct-light**: P(reviewed|open) = 0.61 (treated) / 0.78 (control), but self-direct-merge only 0.089 / 0.028.
**Entry is the single largest unmodeled inflow, and the model assigns every entrant a predicted rate of exactly zero.**

### (c) Reaction — a reallocation of *who merges*, not a self-merge surge

| conditional rate | treated pre → post | control pre → post | reaction (DiD) |
|---|---|---|---|
| P(reviewed \| open) | 0.627 → 0.592 | 0.608 → 0.546 | **+0.027** |
| self direct-merge \| open | 0.156 → 0.271 | 0.205 → 0.331 | **−0.012** |
| other direct-merge \| open | 0.100 → 0.031 | 0.096 → 0.030 | **−0.002** |
| self after-review | 0.310 → 0.640 | 0.282 → 0.495 | **+0.118** |
| other after-review | 0.464 → 0.202 | 0.453 → 0.323 | **−0.132** |

Read the **after-review** row pair together, because that is the finding.
Treated survivors merge far more of their own reviewed pulls themselves (+0.118) and have far fewer merged by someone else (−0.132), and the two nearly cancel: total after-review merge rate is **0.842 treated vs 0.818 control**.
**The reviewed-pull merge is not destroyed by the departure — it is reallocated from *other*-merged to *self*-merged.** That is the resilience mechanism, and it is exactly what j\*'s exit as a merger of other people's reviewed work would predict.

Review-seeking is roughly parallel (DiD +0.027): survivors do **not** stop seeking review.

### Pickup — who absorbs j\*'s freed load (treated, rate per repo-open)

| channel | j\* | surviving incumbents | entrants |
|---|---|---|---|
| solo review | 0.141 → 0.004 | 0.214 → 0.201 | 0 → **0.224** |
| other-after-review merge | 0.099 → 0.004 | 0.130 → 0.113 | 0 → **0.068** |
| other-direct merge | 0.034 → 0.001 | 0.052 → 0.026 | 0 → 0.020 |

**Entrants, not surviving incumbents, replenish review and after-review merging** — entrants supply more solo review post-departure than j\* supplied pre.
Surviving incumbents hold roughly steady; they do not step up.
**Other-direct merging is the one channel that is not replenished** (total 0.086 → 0.047): it is authority-bound, consistent with a single member doing all direct merges in most orgs.
(Read composition here, not levels: the per-open denominator is thin post-departure.)

## 5. The direct-merge crux, reinterpreted — and a correction

The prior harness (`issue/improve_model_fable/`) measured that surviving incumbents self-direct-merge at ~0.32/own pull post-departure vs ~0.14 pre, attributed it to "no reviewer left, so they push their own work through," and proposed a **self-merge growth channel** (allow z_self > 1, and/or route residual-fill η into the self channel) to close the direct-merge gap.

**The control group says that is the wrong inference.**
Treated self-direct-merge rises 0.156 → 0.271 (+0.115) — but control rises 0.205 → 0.331 (+0.126), *slightly more*.
The DiD is **−0.012**: essentially zero.
The self-direct-merge rise is a **common post-period drift shared by organizations that had no departure at all** — the sample is selected on elevated activity at event 0, and as that reverts, opening thins out and self-merging rises mechanically.
It is a violation of Assumption 9 (**stationarity**), not of Assumption 10 (**no-reaction**).

A z_self > 1 growth channel fitted to that surge would therefore be fitting **selection drift and calling it a departure response**.
It would have to apply to controls too — where by construction there is no departure to respond to.

The genuine departure-specific self-merge growth is in the **after-review** channel (+0.118), not the direct channel.

### The two failures of stationarity, cleanly separated

1. **Common drift (treated *and* control).** Conditional rates are not stationary post-period: self-merge up, other-merge down, review down. This is Assumption 9. It shows up as control β < 1 in the member scatters. It largely **differences out** of the event study, so it is a *prediction-accuracy* problem, not a departure-effect problem.
2. **Departure-specific reallocation (treated only).** After-review merging shifts other→self; entrants replenish review and after-review; direct-other shrinks and is not replenished. This is Assumption 10, and it is what the event study actually identifies.

Conflating these is what produced the self-merge-growth proposal.

## 6. Implications for the model

1. **Entry is the first-order missing piece**, confirming the harness's "entry is the structural residual." Entrants are two-thirds of treated post-period opens, replenish review and after-review merging, and are currently predicted at zero. Any counterfactual that omits them must over-predict the decline.
2. **The reaction worth modeling is the after-review other→self reallocation**, plus entrant pickup of review — not a direct self-merge growth channel.
3. **The model's "no pickup for direct" is roughly right.** Direct-other is authority-bound and genuinely is not replenished. So the direct-merge count mismatch has to be explained at the **count/composition** level (j\* was a dominant direct self-merger; his opens and self-merges vanish; entrants open a lot but barely direct-merge), not by a survivor conditional-rate growth channel.
4. **The common drift should be relaxed for both groups**, or absorbed via control calibration — never as a departure response.

## 7. Caveats

- Treated post-period is **thin**: 709 incumbent opens and only 420 reviewed-opens, so treated conditional rates are noisy. The DiD signs are nonetheless consistent across all five post periods in `reaction_by_event_time.png`.
- Rates are pooled (opens-weighted), so they are sensitive to org composition and to the outlier trim. The rigorous version runs these contrasts through the real event-study estimator (`estimate_model_event_study.R`) rather than pooled pre/post means.
- Entrant share in the pre-window is zero **by construction** (entrants are defined as having no pre-period activity); only the post-period comparison is informative.

## 8. Reproduce

```bash
source activate org_resilience && export PYTHONPATH=.
python source/analysis/model_prediction/measure_churn_reaction.py
```
