import numpy as np


# ---------------------------------------------------------------------------
# Distribution fitting
# ---------------------------------------------------------------------------

def FitLatentDistribution(repo_name, counts_per_period, distribution_type):
    """Per-org method-of-moments fit: Negative Binomial if over-dispersed, else Poisson."""
    if distribution_type != "adaptive":
        raise ValueError(f"Unknown distribution_type: {distribution_type}")

    counts_per_period = np.asarray(counts_per_period, dtype=float)
    mean_count = max(float(np.mean(counts_per_period)), 1e-6)
    variance   = float(np.var(counts_per_period, ddof=1)) if len(counts_per_period) > 1 else 0.0
    counts_are_overdispersed = variance > mean_count

    if counts_are_overdispersed:
        return {
            "repo_name": repo_name, "distribution_type": "negative_binomial",
            "expected_latent_count": mean_count, "poisson_rate": np.nan,
            "negative_binomial_size": mean_count ** 2 / (variance - mean_count),
            "negative_binomial_prob": mean_count / variance,
        }
    return {
        "repo_name": repo_name, "distribution_type": "poisson",
        "expected_latent_count": mean_count, "poisson_rate": mean_count,
        "negative_binomial_size": np.nan, "negative_binomial_prob": np.nan,
    }


# ---------------------------------------------------------------------------
# Member stage-completion probability fitting
# ---------------------------------------------------------------------------

def FitMemberProbabilities(repo_name, df_pre_period, df_repo_counts, estimation_approach="pooled"):
    if estimation_approach == "pooled":
        return FitMemberProbabilitiesPooled(repo_name, df_pre_period, df_repo_counts)
    if estimation_approach == "per_period":
        return FitMemberProbabilitiesPerPeriod(repo_name, df_pre_period, df_repo_counts)
    raise ValueError(f"Unknown estimation_approach: {estimation_approach}")


def FitMemberProbabilitiesPooled(repo_name, df_pre_period, df_repo_counts):
    total_opened   = float(df_repo_counts["repo_pull_request_opened"].sum())
    total_reviewed = float(df_repo_counts["repo_pull_request_reviewed"].sum())

    df_member_agg = df_pre_period.groupby("actor_id").agg(
        sum_opens=("member_pull_request_opened",                      "sum"),
        sum_reviews=("member_pull_request_reviewed",                  "sum"),
        sum_merges_direct=("member_pull_request_merged_direct",       "sum"),
        sum_merges_after_review=("member_pull_request_merged_after_review", "sum"),
    ).reset_index()

    rows = []
    for _, row in df_member_agg.iterrows():
        rows.append({
            "repo_name":               repo_name,
            "actor_id":                row["actor_id"],
            "prob_open":               row["sum_opens"]               / total_opened   if total_opened   > 0 else 0.0,
            "prob_review":             row["sum_reviews"]             / total_opened   if total_opened   > 0 else 0.0,
            "prob_merge_direct":       row["sum_merges_direct"]       / total_opened   if total_opened   > 0 else 0.0,
            "prob_merge_after_review": row["sum_merges_after_review"] / total_reviewed if total_reviewed > 0 else 0.0,
        })
    return rows


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
