import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.model_selection import GroupKFold

PROB_SUM_TOLERANCE = 1e-6
NEGATIVE_BINOMIAL_SIZE_FLOOR = 1e-9
POST_LATENT_FOLDS = 5
POST_LATENT_MAX_ITERATIONS = 300
PRE_MEAN_WEIGHT_FLOOR = 1e-9
MEMBER_COUNT_COLUMNS = [
    "member_pull_request_opened", "member_pull_request_reviewed",
    "member_pull_request_reviewed_opened", "member_pull_request_merged_self",
    "member_pull_request_merged_direct_opened_other",
    "member_pull_request_merged_after_review_opened_self",
    "member_pull_request_merged_after_review_opened_other",
]


def FitLatentDistribution(repo_name, counts_per_period, distribution_type):
    if distribution_type != "adaptive":
        raise ValueError(f"Unknown distribution_type: {distribution_type}")

    counts        = np.asarray(counts_per_period, dtype=float)
    baseline_rate = max(float(np.mean(counts)), 1e-6)
    if len(counts) > 1 and baseline_rate > 0:
        variance_to_mean_ratio = float(np.sum((counts - baseline_rate) ** 2 / baseline_rate) / (len(counts) - 1))
    else:
        variance_to_mean_ratio = 1.0

    if variance_to_mean_ratio > 1.0:
        return {
            "repo_name": repo_name, "distribution_type": "negative_binomial",
            "expected_latent_count": baseline_rate, "poisson_rate": np.nan,
            "negative_binomial_size": baseline_rate / (variance_to_mean_ratio - 1.0),
            "negative_binomial_prob": 1.0 / variance_to_mean_ratio,
        }
    return {
        "repo_name": repo_name, "distribution_type": "poisson",
        "expected_latent_count": baseline_rate, "poisson_rate": baseline_rate,
        "negative_binomial_size": np.nan, "negative_binomial_prob": np.nan,
    }


def FitPostLatentRegression(control_post_cells, repo_features):
    control_opened             = control_post_cells["post_opened"].to_numpy(dtype=float)
    control_design             = LastOpeningDesignMatrix(control_post_cells["log_last"])
    organization_equal_weights = 1.0 / control_post_cells["open_mean"].to_numpy(dtype=float).clip(min=PRE_MEAN_WEIGHT_FLOOR)
    repo_of_cell               = control_post_cells["repo_name"].to_numpy()

    out_of_fold_mean = np.zeros(len(control_opened))
    fold_of_cell     = np.full(len(control_opened), -1, dtype=int)
    for fold_index, (train, test) in enumerate(GroupKFold(POST_LATENT_FOLDS).split(control_design, control_opened, repo_of_cell)):
        fold_fit = sm.GLM(control_opened[train], control_design[train], family=sm.families.Poisson(),
                          var_weights=organization_equal_weights[train]).fit(maxiter=POST_LATENT_MAX_ITERATIONS)
        out_of_fold_mean[test] = np.exp(control_design[test] @ fold_fit.params)
        fold_of_cell[test]     = fold_index

    full_fit = sm.GLM(control_opened, control_design, family=sm.families.Poisson(),
                      var_weights=organization_equal_weights).fit(maxiter=POST_LATENT_MAX_ITERATIONS)

    out_of_fold_mean_by_repo = (pd.DataFrame({"repo_name": repo_of_cell, "post_latent_mean": out_of_fold_mean, "fold_id": fold_of_cell})
                                .groupby("repo_name", as_index=False).first())
    full_fit_repos = repo_features[~repo_features["repo_name"].isin(set(out_of_fold_mean_by_repo["repo_name"]))]
    full_fit_mean = pd.DataFrame({
        "repo_name":        full_fit_repos["repo_name"].to_numpy(),
        "post_latent_mean": np.exp(LastOpeningDesignMatrix(full_fit_repos["log_last"]) @ full_fit.params),
        "fold_id":          -1,
    })
    per_repo_mean = pd.concat([out_of_fold_mean_by_repo, full_fit_mean], ignore_index=True)

    coefficients = {
        "intercept":  float(full_fit.params[0]),
        "slope":      float(full_fit.params[1]),
        "r_squared":  ThroughOriginRSquared(control_opened, out_of_fold_mean),
        "n_cells":    int(len(control_opened)),
        "n_repos":    int(out_of_fold_mean_by_repo.shape[0]),
    }
    return per_repo_mean, coefficients


def LastOpeningDesignMatrix(log_last):
    log_last = np.asarray(log_last, dtype=float)
    return np.column_stack([np.ones(len(log_last)), log_last])


def ThroughOriginRSquared(observed, predicted):
    observed  = np.asarray(observed, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    total_sum_of_squares = float(np.sum(observed ** 2))
    if total_sum_of_squares <= 0:
        return np.nan
    return float(1.0 - np.sum((observed - predicted) ** 2) / total_sum_of_squares)


def FitMemberProbabilities(repo_name, df_pre_period, df_repo_counts, estimation_approach="pooled"):
    if estimation_approach == "pooled":
        return FitMemberProbabilitiesPooled(repo_name, df_pre_period, df_repo_counts)
    raise ValueError(f"Unknown estimation_approach: {estimation_approach}")


def FitMemberProbabilitiesPooled(repo_name, df_pre_period, df_repo_counts):
    total_opened   = float(df_repo_counts["repo_pull_request_opened"].sum())
    total_reviewed = float(df_repo_counts["repo_pull_request_reviewed"].sum())
    member_count_sums = df_pre_period.groupby("actor_id")[MEMBER_COUNT_COLUMNS].sum()
    return MemberProbabilityRows(repo_name, member_count_sums, total_opened, total_reviewed)


def MemberProbabilityRows(repo_name, member_count_sums, total_opened, total_reviewed):
    opened_present   = total_opened > 0
    member_own_opened   = member_count_sums["member_pull_request_opened"].values
    member_own_reviewed = member_count_sums["member_pull_request_reviewed_opened"].values
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


def ComputeStageProbabilities(df_member_probs):
    opener_shares = df_member_probs["prob_open"].to_numpy(dtype=float)
    total_open    = float(opener_shares.sum())
    assert -PROB_SUM_TOLERANCE <= total_open <= 1.0 + PROB_SUM_TOLERANCE, f"prob_open sum out of [0, 1]: {total_open}"

    prob_reviewed_given_opened = df_member_probs["prob_review_opened"].to_numpy(dtype=float)
    prob_direct_merge_given_opened = (df_member_probs["prob_merge_self"].to_numpy(dtype=float)
                                      + df_member_probs["prob_merge_direct_opened_other"].to_numpy(dtype=float))
    prob_merge_given_reviewed = (df_member_probs["prob_merge_after_review_self"].to_numpy(dtype=float)
                                 + df_member_probs["prob_merge_after_review_other"].to_numpy(dtype=float))
    assert prob_merge_given_reviewed.max(initial=0.0) <= 1.0 + PROB_SUM_TOLERANCE, \
        f"prob_merge_given_reviewed exceeds 1: {prob_merge_given_reviewed.max()}"
    assert (prob_reviewed_given_opened + prob_direct_merge_given_opened).max(initial=0.0) <= 1.0 + PROB_SUM_TOLERANCE, \
        f"review + direct-merge competing probabilities exceed 1 for some opener: {(prob_reviewed_given_opened + prob_direct_merge_given_opened).max()}"
    return opener_shares, prob_reviewed_given_opened, prob_direct_merge_given_opened, prob_merge_given_reviewed


def MultinomialPvals(shares):
    shares = np.asarray(shares, dtype=float)
    total  = float(shares.sum())
    if total > 1.0:
        shares = shares / total
        total  = 1.0
    unopened_share = max(0.0, 1.0 - total)
    return np.append(shares, unopened_share)


def DrawCounts(distribution_type, dist_params,
               opener_shares, prob_reviewed_given_opened, prob_direct_merge_given_opened,
               prob_merge_given_reviewed, n_draws, rng):
    if distribution_type == "poisson":
        latent_problem_count_draw = rng.poisson(dist_params["poisson_rate"], n_draws)
    elif distribution_type == "negative_binomial":
        negative_binomial_size = max(dist_params["negative_binomial_size"], NEGATIVE_BINOMIAL_SIZE_FLOOR)
        latent_problem_count_draw = rng.negative_binomial(
            negative_binomial_size, dist_params["negative_binomial_prob"], n_draws)
    else:
        raise ValueError(f"Unknown distribution_type for DrawCounts: {distribution_type}")

    opener_shares = np.asarray(opener_shares, dtype=float)
    n_members     = len(opener_shares)

    opens_including_unopened = rng.multinomial(latent_problem_count_draw, MultinomialPvals(opener_shares))
    opens_by_opener          = opens_including_unopened[:, :n_members]

    prob_neither_reviewed_nor_direct_merged = 1.0 - prob_reviewed_given_opened - prob_direct_merge_given_opened
    assert prob_neither_reviewed_nor_direct_merged.min(initial=0.0) >= -PROB_SUM_TOLERANCE, \
        f"review + direct-merge exceed 1 beyond float tolerance for some opener: min residual {prob_neither_reviewed_nor_direct_merged.min()}"
    review_direct_neither_pvals = np.clip(np.column_stack(
        [prob_reviewed_given_opened, prob_direct_merge_given_opened, prob_neither_reviewed_nor_direct_merged]), 0.0, None)
    review_direct_neither_pvals = review_direct_neither_pvals / review_direct_neither_pvals.sum(axis=1, keepdims=True)
    review_direct_neither_draw  = rng.multinomial(opens_by_opener, review_direct_neither_pvals)
    reviewed_by_opener = review_direct_neither_draw[..., 0]

    pull_request_reviewed_draw        = reviewed_by_opener.sum(axis=1)
    pull_request_merged_directly_draw = review_direct_neither_draw[..., 1].sum(axis=1)
    pull_request_merged_reviewed_draw = rng.binomial(
        reviewed_by_opener, np.clip(prob_merge_given_reviewed, 0.0, 1.0)[None, :]).sum(axis=1)

    pull_request_opened_draw = opens_by_opener.sum(axis=1)
    pull_request_merged_draw = pull_request_merged_directly_draw + pull_request_merged_reviewed_draw
    return (pull_request_opened_draw, pull_request_reviewed_draw,
            pull_request_merged_directly_draw, pull_request_merged_reviewed_draw,
            pull_request_merged_draw)
