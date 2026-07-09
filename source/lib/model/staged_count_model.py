import numpy as np
import pandas as pd

PROB_SUM_TOLERANCE = 1e-6
NEGATIVE_BINOMIAL_SIZE_FLOOR = 1e-9
MEMBER_COUNT_COLUMNS = [
    "member_pull_request_opened", "member_pull_request_reviewed",
    "member_pull_request_reviewed_opened", "member_pull_request_merged_self",
    "member_pull_request_merged_direct_opened_other",
    "member_pull_request_merged_after_review_opened_self",
    "member_pull_request_merged_after_review_opened_other",
]

# SCRIPT SINGLE-USE EXCEPTION: cohesive staged-count-model library — ComputeStageProbabilities and
# DrawCounts are called only from predict_model.py, but belong beside the fit functions
# (FitLatentDistribution, FitMemberProbabilities) that both fit_model.py and predict_model.py use.


def MeanReversionAdj(mean_reversion_adj_table, num_important_qualified):
    # Mean-reversion adjustment delta for a period with this many qualified important members present
    # (= rate(q==0)/rate(q)); 1 when none are present or no estimate is available.
    if num_important_qualified <= 0 or not mean_reversion_adj_table:
        return 1.0
    return mean_reversion_adj_table.get(min(int(num_important_qualified), max(mean_reversion_adj_table)), 1.0)


def FitLatentDistribution(repo_name, counts_per_period, member_effects_per_period, distribution_type):
    """Per-org fit at the q=0 baseline. member_effects_per_period[t] = 1/delta_{q_{i,t}} is the period's
    exposure a_t, aligned to counts_per_period. The baseline rate lambda0 is the Poisson-exposure MLE
    sum(N)/sum(a); the family is chosen from the Pearson dispersion of the counts against their period
    means mu_t = lambda0 * a_t (so q-variation is not mistaken for over-dispersion), and a Negative
    Binomial takes that dispersion as its variance-to-mean ratio."""
    if distribution_type != "adaptive":
        raise ValueError(f"Unknown distribution_type: {distribution_type}")

    counts         = np.asarray(counts_per_period, dtype=float)
    member_effects = np.asarray(member_effects_per_period, dtype=float)   # a_t = 1/delta_{q_t}, the exposure
    baseline_rate  = max(float(np.sum(counts)) / max(float(np.sum(member_effects)), 1e-6), 1e-6)  # Poisson-exposure MLE: sum(N) / sum(a)
    period_means   = baseline_rate * member_effects                       # mu_t = lambda0 * a_t = lambda0 / delta_{q_t}
    if len(counts) > 1 and np.all(period_means > 0):
        dispersion = float(np.sum((counts - period_means) ** 2 / period_means) / (len(counts) - 1))
    else:
        dispersion = 1.0

    if dispersion > 1.0:
        return {
            "repo_name": repo_name, "distribution_type": "negative_binomial",
            "expected_latent_count": baseline_rate, "poisson_rate": np.nan,
            "negative_binomial_size": baseline_rate / (dispersion - 1.0),
            "negative_binomial_prob": 1.0 / dispersion,
        }
    return {
        "repo_name": repo_name, "distribution_type": "poisson",
        "expected_latent_count": baseline_rate, "poisson_rate": baseline_rate,
        "negative_binomial_size": np.nan, "negative_binomial_prob": np.nan,
    }


def FitMemberProbabilities(repo_name, df_pre_period, df_repo_counts, estimation_approach="pooled"):
    if estimation_approach == "pooled":
        return FitMemberProbabilitiesPooled(repo_name, df_pre_period, df_repo_counts)
    if estimation_approach == "per_period":
        return FitMemberProbabilitiesPerPeriod(repo_name, df_pre_period, df_repo_counts)
    raise ValueError(f"Unknown estimation_approach: {estimation_approach}")


def FitMemberProbabilitiesPooled(repo_name, df_pre_period, df_repo_counts):
    total_opened   = float(df_repo_counts["repo_pull_request_opened"].sum())
    total_reviewed = float(df_repo_counts["repo_pull_request_reviewed"].sum())
    member_count_sums = df_pre_period.groupby("actor_id")[MEMBER_COUNT_COLUMNS].sum()
    return MemberProbabilityRows(repo_name, member_count_sums, total_opened, total_reviewed)


def MemberProbabilityRows(repo_name, member_count_sums, total_opened, total_reviewed):
    opened_present   = total_opened   > 0
    reviewed_present = total_reviewed > 0
    member_own_opened   = member_count_sums["member_pull_request_opened"].values
    member_own_reviewed = member_count_sums["member_pull_request_reviewed_opened"].values
    # Merges are indexed by the pull's opener, so each merge rate is the fraction of j's OWN opened pulls (or
    # reviewed-opened pulls) reaching that stage -- self and other share the same denominator, so the total
    # merge probability of a pull opened by j is in [0, 1] by construction (no clipping needed). "self" = the
    # opener merges its own pull; "other" = a different member merges j's pull.
    prob_merge_self = np.divide(
        member_count_sums["member_pull_request_merged_self"].values, member_own_opened,
        out=np.zeros(len(member_count_sums)), where=member_own_opened > 0)
    prob_merge_direct_opened_other = np.divide(
        member_count_sums["member_pull_request_merged_direct_opened_other"].values, member_own_opened,
        out=np.zeros(len(member_count_sums)), where=member_own_opened > 0)
    prob_merge_after_review_self = np.divide(
        member_count_sums["member_pull_request_merged_after_review_opened_self"].values, member_own_reviewed,
        out=np.zeros(len(member_count_sums)), where=member_own_reviewed > 0)
    prob_merge_after_review_other = np.divide(
        member_count_sums["member_pull_request_merged_after_review_opened_other"].values, member_own_reviewed,
        out=np.zeros(len(member_count_sums)), where=member_own_reviewed > 0)
    # Review is opener-specific: prob_review_opened is p^{r|o}_j = R^o_j / A^o_j, the fraction of j's opened
    # pulls that get reviewed. Reviewed and directly-merged pulls are disjoint subsets of j's opens, so
    # p^{r|o}_j + pi_direct_j <= 1 by construction (the competing multinomial is always valid).
    prob_review_opened = np.divide(
        member_own_reviewed, member_own_opened,
        out=np.zeros(len(member_count_sums)), where=member_own_opened > 0)
    return pd.DataFrame({
        "repo_name":               repo_name,
        "actor_id":                member_count_sums.index,
        "prob_open":               member_own_opened                                          / total_opened if opened_present else 0.0,
        "prob_review":             member_count_sums["member_pull_request_reviewed"].values   / total_opened if opened_present else 0.0,
        "prob_review_opened":             prob_review_opened,
        "prob_merge_self":                prob_merge_self,
        "prob_merge_direct_opened_other": prob_merge_direct_opened_other,
        "prob_merge_after_review_self":   prob_merge_after_review_self,
        "prob_merge_after_review_other":  prob_merge_after_review_other,
    }).to_dict("records")


def FitMemberProbabilitiesPerPeriod(repo_name, df_pre_period, df_repo_counts):
    periods         = sorted(df_repo_counts["quasi_event_time"].unique())
    member_universe = df_pre_period["actor_id"].unique()

    period_ratios = {actor_id: {"prob_open": [], "prob_review": [], "prob_merge_direct": [], "prob_merge_after_review": []}
                     for actor_id in member_universe}

    for period in periods:
        df_period   = df_pre_period[df_pre_period["quasi_event_time"] == period]
        repo_period = df_repo_counts[df_repo_counts["quasi_event_time"] == period]
        if len(repo_period) == 0:
            continue
        repo_period = repo_period.iloc[0]
        opened_in_period   = float(repo_period["repo_pull_request_opened"])
        reviewed_in_period = float(repo_period["repo_pull_request_reviewed"])

        df_period_agg = df_period.groupby("actor_id").agg(
            sum_opens=("member_pull_request_opened",                      "sum"),
            sum_reviews=("member_pull_request_reviewed",                  "sum"),
            sum_merges_direct=("member_pull_request_merged_direct",       "sum"),
            sum_merges_after_review=("member_pull_request_merged_after_review", "sum"),
        ).reset_index()

        for actor_id in member_universe:
            member_row    = df_period_agg[df_period_agg["actor_id"] == actor_id]
            opens         = float(member_row["sum_opens"].iloc[0])               if len(member_row) > 0 else 0.0
            reviews       = float(member_row["sum_reviews"].iloc[0])             if len(member_row) > 0 else 0.0
            merges_direct = float(member_row["sum_merges_direct"].iloc[0])       if len(member_row) > 0 else 0.0
            merges_after  = float(member_row["sum_merges_after_review"].iloc[0]) if len(member_row) > 0 else 0.0

            if opened_in_period > 0:
                period_ratios[actor_id]["prob_open"].append(opens / opened_in_period)
                period_ratios[actor_id]["prob_review"].append(reviews / opened_in_period)
                period_ratios[actor_id]["prob_merge_direct"].append(merges_direct / opened_in_period)
            if reviewed_in_period > 0:
                period_ratios[actor_id]["prob_merge_after_review"].append(merges_after / reviewed_in_period)

    rows = []
    for actor_id in member_universe:
        ratios = period_ratios[actor_id]
        rows.append({
            "repo_name":               repo_name,
            "actor_id":                actor_id,
            "prob_open":               float(np.mean(ratios["prob_open"]))               if ratios["prob_open"]               else 0.0,
            "prob_review":             float(np.mean(ratios["prob_review"]))             if ratios["prob_review"]             else 0.0,
            "prob_merge_direct":       float(np.mean(ratios["prob_merge_direct"]))       if ratios["prob_merge_direct"]       else 0.0,
            "prob_merge_after_review": float(np.mean(ratios["prob_merge_after_review"])) if ratios["prob_merge_after_review"] else 0.0,
        })
    return rows


def ComputeStageProbabilities(df_member_probs):
    # Returns the per-opener stage inputs the draw needs. Review and both merge stages are opener-specific
    # (a pull's fate depends on who opened it), so all three are per-member arrays whose counts are
    # Poisson-binomial across openers; opening is the opener composition.
    opener_shares = df_member_probs["prob_open"].to_numpy(dtype=float)
    total_open    = float(opener_shares.sum())
    assert -PROB_SUM_TOLERANCE <= total_open <= 1.0 + PROB_SUM_TOLERANCE, f"prob_open sum out of [0, 1]: {total_open}"

    pi_review = df_member_probs["prob_review_opened"].to_numpy(dtype=float)
    # Direct-merge probability of a pull opened by j: self (j merges it) + other (a different member merges it),
    # both over the same denominator (j's opened pulls); likewise after-review over j's reviewed-opened pulls.
    pi_direct = (df_member_probs["prob_merge_self"].to_numpy(dtype=float)
                 + df_member_probs["prob_merge_direct_opened_other"].to_numpy(dtype=float))
    pi_after  = (df_member_probs["prob_merge_after_review_self"].to_numpy(dtype=float)
                 + df_member_probs["prob_merge_after_review_other"].to_numpy(dtype=float))
    # Review and direct merge compete for the same opened pull; both are fractions of j's opened pulls, and
    # reviewed and directly-merged pulls are disjoint, so their per-opener sum is <= 1 by construction (no clip).
    assert pi_after.max(initial=0.0) <= 1.0 + PROB_SUM_TOLERANCE, f"pi_after exceeds 1: {pi_after.max()}"
    assert (pi_review + pi_direct).max(initial=0.0) <= 1.0 + PROB_SUM_TOLERANCE, \
        f"review + direct-merge competing probabilities exceed 1 for some opener: {(pi_review + pi_direct).max()}"
    return opener_shares, pi_review, pi_direct, pi_after


def MultinomialPvals(shares):
    # A valid pvals vector for rng.multinomial: the member shares (clamped to sum <= 1) plus a trailing
    # residual cell (unopened, or reviewed pulls with no member opener) that the caller discards.
    shares = np.asarray(shares, dtype=float)
    total  = float(shares.sum())
    if total > 1.0:
        shares = shares / total
        total  = 1.0
    return np.append(shares, max(0.0, 1.0 - total))


def DrawCounts(distribution_type, dist_params,
               opener_shares, pi_review, pi_direct, pi_after, n_draws, rng,
               latent_rate_multiplier):
    if distribution_type == "poisson":
        latent_problem_count_draw = rng.poisson(dist_params["poisson_rate"] * latent_rate_multiplier, n_draws)
    elif distribution_type == "negative_binomial":
        # negative_binomial requires size > 0, so a sampled reversion ratio of 0 floors to a tiny size (draws ~0)
        negative_binomial_size = np.maximum(
            dist_params["negative_binomial_size"] * latent_rate_multiplier, NEGATIVE_BINOMIAL_SIZE_FLOOR)
        latent_problem_count_draw = rng.negative_binomial(
            negative_binomial_size, dist_params["negative_binomial_prob"], n_draws)
    else:
        raise ValueError(f"Unknown distribution_type for DrawCounts: {distribution_type}")

    opener_shares = np.asarray(opener_shares, dtype=float)
    n_members     = len(opener_shares)

    # Stage 1: opener composition -- who opens each latent problem (last cell = unopened, discarded)
    opens_full      = rng.multinomial(latent_problem_count_draw, MultinomialPvals(opener_shares))
    opens_by_opener = opens_full[:, :n_members]

    # Stage 2: each opened pull is reviewed, direct-merged, or neither -- a per-opener trinomial drawn in one
    # vectorized call via numpy's multinomial with a 2-D pvals array (one row per opener; it broadcasts the
    # per-opener counts against the per-opener probabilities). Both rates are opener-specific, so each stage
    # count is a Poisson-binomial across openers. Reviewed and directly-merged pulls are disjoint subsets of
    # j's opens (R^o + merges <= A^o on the integer counts), so the "nothing" residual is >= 0 exactly; in
    # float it can be a tiny negative. Assert the violation is only float noise (a real one -- a data
    # inconsistency -- is orders of magnitude larger), then clip that residual to 0 and renormalize each row so
    # numpy's multinomial, which rejects any pval < 0 and needs rows summing to 1, accepts the array.
    nothing_residual = 1.0 - pi_review - pi_direct
    assert nothing_residual.min(initial=0.0) >= -PROB_SUM_TOLERANCE, \
        f"review + direct-merge exceed 1 beyond float tolerance for some opener: min residual {nothing_residual.min()}"
    stage_two_pvals = np.clip(np.column_stack([pi_review, pi_direct, nothing_residual]), 0.0, None)
    stage_two_pvals = stage_two_pvals / stage_two_pvals.sum(axis=1, keepdims=True)
    stage_two_draw  = rng.multinomial(opens_by_opener, stage_two_pvals)   # (n_draws, n_members, 3)
    reviewed_by_opener = stage_two_draw[..., 0]

    pull_request_reviewed_draw        = reviewed_by_opener.sum(axis=1)
    pull_request_merged_directly_draw = stage_two_draw[..., 1].sum(axis=1)

    # Stage 3: each reviewed pull, kept with its opener, is merged after review at that opener's rate -- again
    # a Poisson-binomial across openers (no separate reviewed-opener re-draw: review is already per opener).
    pull_request_merged_reviewed_draw = rng.binomial(reviewed_by_opener, np.clip(pi_after, 0.0, 1.0)[None, :]).sum(axis=1)

    pull_request_opened_draw = opens_by_opener.sum(axis=1)
    pull_request_merged_draw = pull_request_merged_directly_draw + pull_request_merged_reviewed_draw
    return (pull_request_opened_draw, pull_request_reviewed_draw,
            pull_request_merged_directly_draw, pull_request_merged_reviewed_draw,
            pull_request_merged_draw)
