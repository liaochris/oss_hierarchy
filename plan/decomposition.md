# Note: A Decomposable Partition-Share Error Metric

## Purpose

The goal is to use a partition-selection error metric that maps cleanly into an interpretable decomposition. Instead of selecting a contributor partition using only cell-level fit error, this note proposes using an **exploded individual-level error** as the headline partition error.

The benefit is that the headline error decomposes exactly into two pieces:

1. **Cell-fit error**: error from predicting the wrong total share for each partition cell.
2. **Coarsening error**: error from grouping heterogeneous contributors together inside a cell.

This makes the diagnostic easy to interpret: once a partition has a large error, we can say how much of that error is due to bad cell-level prediction versus how much is intrinsic coarsening loss.

---

## Setup and notation

Fix an organization \(u\), stage \(s\), partition candidate \(c\), and post-period event time \(k \in \{0,\ldots,K\}\).

Let

\[
\mathcal I_u
\]

be the set of contributors at organization \(u\).

Let

\[
\mathcal G_{u,c}^s
\]

be the contributor partition induced by candidate \(c\) for stage \(s\). A cell in the partition is denoted by \(g\). For individual \(i\), let \(g(i)\) be the cell containing \(i\). Let

\[
|g|
\]

be the number of contributors in cell \(g\).

The pre-period fitted individual share is

\[
\hat p_{u,i}^{s,\ell^*}.
\]

The observed post-period individual share is

\[
\tilde p_{u,i,k}^s.
\]

The fitted pre-period cell share is

\[
\hat p_{u,g}^{s,\ell^*}(c)
=
\sum_{i\in g}\hat p_{u,i}^{s,\ell^*}.
\]

The observed post-period cell share is

\[
\tilde p_{u,g,k}^{s}(c)
=
\sum_{i\in g}\tilde p_{u,i,k}^{s}.
\]

---

## Proposed headline error: decomposable exploded error

For partition candidate \(c\), define the exploded fitted individual share as

\[
\hat p_{u,i}^{s,c\text{-exploded}}
=
\frac{\hat p_{u,g(i)}^{s,\ell^*}(c)}{|g(i)|}.
\]

That is, we take the fitted share of the cell containing contributor \(i\), and spread that cell share equally across all contributors in the cell.

The proposed headline partition error is

\[
\mathcal R_u^{p,s,c,\mathrm{decomp}}
=
\left[
\frac{1}{K+1}
\sum_{k=0}^{K}
\sum_{i\in\mathcal I_u}
\left(
\frac{\hat p_{u,g(i)}^{s,\ell^*}(c)}{|g(i)|}
-
\tilde p_{u,i,k}^{s}
\right)^2
\right]^{1/2}.
\]

Equivalently,

\[
\mathcal R_u^{p,s,c,\mathrm{decomp}}
=
\mathcal R_u^{p,s,c,\mathrm{exploded}}.
\]

### Interpretation

This asks:

> If I use the fitted pre-period cell shares implied by partition \(c\), and then reconstruct individual-level shares by spreading each cell equally across its members, how far am I from the true post-period individual shares?

This is a useful headline error because it measures the total individual-level error associated with using the partition.

---

## Exact decomposition

The squared headline error decomposes exactly as

\[
\left(\mathcal R_u^{p,s,c,\mathrm{decomp}}\right)^2
=
\left(\mathcal R_u^{p,s,c,\mathrm{cell\text{-}fit}}\right)^2
+
\left(\mathcal R_u^{p,s,c,\mathrm{coarsening}}\right)^2.
\]

The first component is the cell-fit component:

\[
\mathcal R_u^{p,s,c,\mathrm{cell\text{-}fit}}
=
\left[
\frac{1}{K+1}
\sum_{k=0}^{K}
\sum_{g\in\mathcal G_{u,c}^s}
\frac{
\left(
\hat p_{u,g}^{s,\ell^*}(c)
-
\tilde p_{u,g,k}^{s}(c)
\right)^2
}{|g|}
\right]^{1/2}.
\]

The second component is the coarsening component:

\[
\mathcal R_u^{p,s,c,\mathrm{coarsening}}
=
\left[
\frac{1}{K+1}
\sum_{k=0}^{K}
\sum_{g\in\mathcal G_{u,c}^s}
\sum_{i\in g}
\left(
\frac{\tilde p_{u,g,k}^{s}(c)}{|g|}
-
\tilde p_{u,i,k}^{s}
\right)^2
\right]^{1/2}.
\]

Therefore, in words:

\[
\boxed{
\text{total decomposable partition error}
=
\text{cell-fit error}
+
\text{coarsening error}
}
\]

where the equality holds in squared-error space.

---

## What each piece means

### 1. Total decomposable error

\[
\mathcal R_u^{p,s,c,\mathrm{decomp}}
\]

This is the total error from using partition \(c\), after reconstructing individual shares from the partition-level fitted shares.

It captures the full individual-level consequence of using the partition.

A high value means:

> This partition does a poor job reproducing the post-period individual share vector.

---

### 2. Cell-fit error

\[
\mathcal R_u^{p,s,c,\mathrm{cell\text{-}fit}}
\]

This measures the part of the error that comes from getting the total share of each cell wrong.

For example, suppose the fitted pre-period share of the `other` cell is 30%, but the observed post-period share of the `other` cell is 45%. That discrepancy contributes to the cell-fit component.

The term is divided by \(|g|\) because the cell-level error is being expressed as part of an individual-level decomposition. If a cell has many contributors, the cell-total error is spread across many people.

A high value means:

> The partition cells themselves are not stable from pre-period to post-period.

---

### 3. Coarsening error

\[
\mathcal R_u^{p,s,c,\mathrm{coarsening}}
\]

This measures the error caused by treating contributors inside the same cell as exchangeable.

It asks:

> Even if I knew the true post-period total share of each cell, how much error would remain because I spread that cell total equally across the contributors inside the cell?

A high value means:

> The partition is hiding important within-cell heterogeneity.

This is the clean version of the coarsening-loss diagnostic.

---

## Why this is preferable as the main partition metric

The previous cell-level selection metric was

\[
\mathcal R_u^{p,s,c,\mathrm{sel}}
=
\left[
\frac{1}{K+1}
\sum_{k=0}^{K}
\sum_{g\in\mathcal G_{u,c}^s}
\left(
\hat p_{u,g}^{s,\ell^*}(c)
-
\tilde p_{u,g,k}^{s}(c)
\right)^2
\right]^{1/2}.
\]

That metric asks whether the partition gets cell totals right. It is useful, but it does not decompose directly into a total individual-level error.

The proposed metric instead uses

\[
\mathcal R_u^{p,s,c,\mathrm{decomp}}
=
\mathcal R_u^{p,s,c,\mathrm{exploded}}.
\]

This metric has a direct decomposition:

\[
\left(\mathcal R^{\mathrm{decomp}}\right)^2
=
\left(\mathcal R^{\mathrm{cell\text{-}fit}}\right)^2
+
\left(\mathcal R^{\mathrm{coarsening}}\right)^2.
\]

So if a candidate partition performs badly, we can immediately say whether the problem is:

1. the fitted cell totals are wrong, or
2. the cells are too coarse and hide heterogeneous contributors.

---

## Contribution shares

For reporting, define the fraction of squared decomposable error due to cell-fit error:

\[
\mathrm{Share}_u^{p,s,c,\mathrm{cell\text{-}fit}}
=
\frac{
\left(\mathcal R_u^{p,s,c,\mathrm{cell\text{-}fit}}\right)^2
}{
\left(\mathcal R_u^{p,s,c,\mathrm{decomp}}\right)^2
}.
\]

Define the fraction due to coarsening error:

\[
\mathrm{Share}_u^{p,s,c,\mathrm{coarsening}}
=
\frac{
\left(\mathcal R_u^{p,s,c,\mathrm{coarsening}}\right)^2
}{
\left(\mathcal R_u^{p,s,c,\mathrm{decomp}}\right)^2
}.
\]

When \(\mathcal R_u^{p,s,c,\mathrm{decomp}}>0\), these two shares add to one:

\[
\mathrm{Share}_u^{p,s,c,\mathrm{cell\text{-}fit}}
+
\mathrm{Share}_u^{p,s,c,\mathrm{coarsening}}
=1.
\]

These shares are useful because they translate the decomposition into a directly interpretable attribution.

For example:

- 80% cell-fit / 20% coarsening means the partition mostly fails because the cell totals are unstable.
- 20% cell-fit / 80% coarsening means the partition mostly fails because it groups unlike contributors together.

---

## Proof of the decomposition

Fix one post-period \(k\) and one cell \(g\). For each \(i\in g\), write

\[
\frac{\hat p_{u,g}^{s,\ell^*}(c)}{|g|}
-
\tilde p_{u,i,k}^{s}
=
\underbrace{
\frac{
\hat p_{u,g}^{s,\ell^*}(c)
-
\tilde p_{u,g,k}^{s}(c)
}{|g|}
}_{\text{cell-total fit error}}
+
\underbrace{
\left(
\frac{\tilde p_{u,g,k}^{s}(c)}{|g|}
-
\tilde p_{u,i,k}^{s}
\right)
}_{\text{within-cell coarsening residual}}.
\]

Squaring and summing over contributors inside cell \(g\) gives two terms plus a cross term. The cross term is zero because

\[
\sum_{i\in g}
\left(
\frac{\tilde p_{u,g,k}^{s}(c)}{|g|}
-
\tilde p_{u,i,k}^{s}
\right)
=
\tilde p_{u,g,k}^{s}(c)
-
\tilde p_{u,g,k}^{s}(c)
=
0.
\]

Therefore,

\[
\sum_{i\in g}
\left(
\frac{\hat p_{u,g}^{s,\ell^*}(c)}{|g|}
-
\tilde p_{u,i,k}^{s}
\right)^2
=
\frac{
\left(
\hat p_{u,g}^{s,\ell^*}(c)
-
\tilde p_{u,g,k}^{s}(c)
\right)^2
}{|g|}
+
\sum_{i\in g}
\left(
\frac{\tilde p_{u,g,k}^{s}(c)}{|g|}
-
\tilde p_{u,i,k}^{s}
\right)^2.
\]

Summing over cells \(g\), then averaging over post-periods \(k=0,\ldots,K\), gives the full decomposition.

---

## Implication for partition selection

The recommended selection rule is:

\[
 c^*
 =
 \arg\min_c
 \overline{\mathcal R^{p,\mathrm{agg},c,\mathrm{decomp}}},
\]

where the aggregate error combines stages using the chosen stage weights, for example pre-event or post-event activity weights.

For a single organization, one can define

\[
\mathcal R_u^{p,\mathrm{agg},c,\mathrm{decomp},w}
=
\left[
\sum_{s\in\{o,r,m\}}
 w_u^s
 \left(
 \mathcal R_u^{p,s,c,\mathrm{decomp}}
 \right)^2
\right]^{1/2}.
\]

Then across controls,

\[
\overline{\mathcal R^{p,\mathrm{agg},c,\mathrm{decomp},w}}
=
\frac{1}{|\mathcal C|}
\sum_{u\in\mathcal C}
\mathcal R_u^{p,\mathrm{agg},c,\mathrm{decomp},w}.
\]

The selected partition is the one with the lowest cross-control aggregate decomposable error.

---

## Recommended table columns

For each candidate partition, report:

| Candidate | Total decomposable error | Cell-fit component | Coarsening component | % cell-fit | % coarsening | Selected |
|---|---:|---:|---:|---:|---:|---|
| `all__<estimator>` | \(\overline{\mathcal R^{\mathrm{decomp}}}\) | \(\overline{\mathcal R^{\mathrm{cell\text{-}fit}}}\) | \(\overline{\mathcal R^{\mathrm{coarsening}}}\) | share | share | ✓/blank |
| `top5__<estimator>` | ... | ... | ... | ... | ... | ... |
| `top5_per_stage__<estimator>` | ... | ... | ... | ... | ... | ... |
| `cover80p__<estimator>` | ... | ... | ... | ... | ... | ... |
| `cover80p_per_stage__<estimator>` | ... | ... | ... | ... | ... | ... |

The headline selection column should be the total decomposable error.

The cell-fit and coarsening columns explain where that error comes from.

---

## Important caveat: the `all` candidate

For the `all` partition, every contributor is their own cell. Therefore, there is no coarsening loss:

\[
\mathcal R_u^{p,s,\mathrm{all},\mathrm{coarsening}}=0.
\]

This means the `all` candidate may have a mechanical advantage under the decomposable exploded metric.

That is not necessarily a problem. It simply means the metric is asking:

> Which partition best reconstructs individual-level post-period shares?

If the goal is individual-level reconstruction, this is the right criterion. If the goal is to prefer simpler or coarser partitions, one could add a complexity penalty or separately report the number of tracked cells.

---

## Suggested wording for the paper or implementation note

> We select contributor partitions using a decomposable individual-level share error. For each candidate partition, we aggregate fitted pre-period individual shares to the partition cells, then reconstruct an individual-level fitted vector by spreading each fitted cell share equally across members of the cell. The resulting L2 distance from the observed post-period individual share vector defines the headline partition error. This error decomposes exactly into a cell-fit component, which captures errors in predicted cell totals, and a coarsening component, which captures the information loss from treating heterogeneous contributors within the same cell as exchangeable. This decomposition allows the selected partition's error to be attributed directly to instability in cell totals versus intrinsic loss from coarsening.
