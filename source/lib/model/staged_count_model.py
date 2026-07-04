import numpy as np
import pandas as pd

PROB_SUM_TOLERANCE = 1e-6
NEGATIVE_BINOMIAL_SIZE_FLOOR = 1e-9
MEMBER_COUNT_COLUMNS = [
    "member_pull_request_opened", "member_pull_request_reviewed",
    "member_pull_request_merged_direct", "member_pull_request_merged_after_review",
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
    return pd.DataFrame({
        "repo_name":               repo_name,
        "actor_id":                member_count_sums.index,
        "prob_open":               member_count_sums["member_pull_request_opened"].values               / total_opened   if opened_present   else 0.0,
        "prob_review":             member_count_sums["member_pull_request_reviewed"].values             / total_opened   if opened_present   else 0.0,
        "prob_merge_direct":       member_count_sums["member_pull_request_merged_direct"].values         / total_opened   if opened_present   else 0.0,
        "prob_merge_after_review": member_count_sums["member_pull_request_merged_after_review"].values   / total_reviewed if reviewed_present else 0.0,
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
    prob_open = float(df_member_probs["prob_open"].sum())
    assert -PROB_SUM_TOLERANCE <= prob_open <= 1.0 + PROB_SUM_TOLERANCE, f"prob_open sum out of [0, 1]: {prob_open}"
    prob_review             = float(df_member_probs["prob_review"].sum())
    prob_merge_direct       = float(df_member_probs["prob_merge_direct"].sum())
    prob_merge_after_review = float(df_member_probs["prob_merge_after_review"].sum())
    assert prob_review + prob_merge_direct <= 1.0 + PROB_SUM_TOLERANCE, \
        f"review + direct-merge competing-path probabilities exceed 1: {prob_review + prob_merge_direct}"
    return prob_open, prob_review, prob_merge_direct, prob_merge_after_review


def CompetingPathProbabilities(prob_review, prob_merge_direct):
    # Review / direct-merge / neither form a simplex by construction; the clamps only absorb
    # floating-point rounding at the boundary (e.g. a repo whose opened pulls are all reviewed).
    prob_review       = min(1.0, prob_review)
    prob_merge_direct = min(prob_merge_direct, 1.0 - prob_review)
    return [prob_review, prob_merge_direct, 1.0 - prob_review - prob_merge_direct]


def DrawCounts(distribution_type, dist_params,
               prob_open, prob_review, prob_merge_direct, prob_merge_after_review, n_draws, rng,
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

    # Stage 1: opened
    prob_open = np.clip(prob_open, 0.0, 1.0)
    pull_request_opened_draw = rng.binomial(latent_problem_count_draw, prob_open)

    # Stage 2: each opened pull is reviewed, direct-merged, or neither (multinomial over opened)
    stage_two_outcomes = rng.multinomial(
        pull_request_opened_draw, CompetingPathProbabilities(prob_review, prob_merge_direct))
    pull_request_reviewed_draw        = stage_two_outcomes[:, 0]
    pull_request_merged_directly_draw = stage_two_outcomes[:, 1]

    # Stage 3: merge after review
    prob_merge_after_review = np.clip(prob_merge_after_review, 0.0, 1.0)
    pull_request_merged_reviewed_draw = rng.binomial(pull_request_reviewed_draw, prob_merge_after_review)

    pull_request_merged_draw = pull_request_merged_directly_draw + pull_request_merged_reviewed_draw
    return (pull_request_opened_draw, pull_request_reviewed_draw,
            pull_request_merged_directly_draw, pull_request_merged_reviewed_draw,
            pull_request_merged_draw)
