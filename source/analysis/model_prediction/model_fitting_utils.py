import warnings

import numpy as np
from scipy.stats import poisson, nbinom
from statsmodels.discrete.discrete_model import NegativeBinomial
from statsmodels.discrete.count_model import ZeroInflatedPoisson


def FitDistribution(repo_name, counts_per_period):
    n_periods = len(counts_per_period)
    has_zeros = np.any(counts_per_period == 0)
    log_likelihood_poisson_loo = log_likelihood_zip_loo = log_likelihood_nb_loo = 0.0

    for i in range(n_periods):
        train = np.delete(counts_per_period, i)
        held  = counts_per_period[i]

        lam_p = max(float(np.mean(train)), 1e-6)
        log_likelihood_poisson_loo += float(poisson.logpmf(int(held), lam_p))

        if has_zeros and np.any(train == 0):
            pi_z, lam_z = FitZIPParams(train)
            log_likelihood_zip_loo += LogLikZIP(int(held), pi_z, lam_z)
        else:
            log_likelihood_zip_loo += float(poisson.logpmf(int(held), lam_p))

        r_nb, p_nb = FitNBParams(train)
        log_likelihood_nb_loo += float(nbinom.logpmf(int(held), r_nb, p_nb))

    log_likelihood_approach1 = max(log_likelihood_poisson_loo, log_likelihood_zip_loo)
    approach1_type = "poisson" if log_likelihood_poisson_loo >= log_likelihood_zip_loo else "zip"
    winner         = "approach1" if log_likelihood_approach1 >= log_likelihood_nb_loo else "nb"

    comparison_result = {
        "repo_name":                      repo_name,
        "log_likelihood_poisson_loo":     log_likelihood_poisson_loo,
        "log_likelihood_zip_loo":         log_likelihood_zip_loo,
        "log_likelihood_approach1_loo":   log_likelihood_approach1,
        "log_likelihood_approach1_type":  approach1_type,
        "log_likelihood_nb_loo":          log_likelihood_nb_loo,
        "log_likelihood_diff":            log_likelihood_approach1 - log_likelihood_nb_loo,
    }

    if winner == "approach1":
        if approach1_type == "poisson":
            lam = max(float(np.mean(counts_per_period)), 1e-6)
            dist_result = {
                "repo_name": repo_name, "distribution_type": "poisson",
                "N_hat": lam, "lambda": lam, "r": np.nan, "p_nb": np.nan, "pi_zero": np.nan,
            }
        else:
            pi_z, lam_z = FitZIPParams(counts_per_period)
            dist_result = {
                "repo_name": repo_name, "distribution_type": "zip",
                "N_hat": (1.0 - pi_z) * lam_z, "lambda": lam_z,
                "r": np.nan, "p_nb": np.nan, "pi_zero": pi_z,
            }
    else:
        r_nb, p_nb = FitNBParams(counts_per_period)
        dist_result = {
            "repo_name": repo_name, "distribution_type": "negative_binomial",
            "N_hat": r_nb * (1.0 - p_nb) / p_nb,
            "lambda": np.nan, "r": r_nb, "p_nb": p_nb, "pi_zero": np.nan,
        }

    return dist_result, comparison_result


def FitMemberProbabilities(repo_name, df_pre_period, df_repo_counts):
    total_opened   = float(df_repo_counts["repo_pull_request_opened"].sum())
    total_reviewed = float(df_repo_counts["repo_pull_request_reviewed"].sum())

    df_member_agg = df_pre_period.groupby("actor_id").agg(
        sum_opens=("member_pull_request_opened",   "sum"),
        sum_reviews=("member_pull_request_reviewed", "sum"),
        sum_merges=("member_pull_request_merged",   "sum"),
    ).reset_index()

    rows = []
    for _, row in df_member_agg.iterrows():
        rows.append({
            "repo_name":   repo_name,
            "actor_id":    row["actor_id"],
            "prob_open":   row["sum_opens"]   / total_opened   if total_opened   > 0 else 0.0,
            "prob_review": row["sum_reviews"] / total_opened   if total_opened   > 0 else 0.0,
            "prob_merge":  row["sum_merges"]  / total_reviewed if total_reviewed > 0 else 0.0,
        })
    return rows


def ComputeModelVariance(distribution_type, N_hat, r, pi_zero, lambda_param):
    if distribution_type == "poisson":
        return N_hat
    elif distribution_type == "negative_binomial":
        return N_hat + N_hat ** 2 / r
    elif distribution_type == "zip":
        return N_hat * (1 + pi_zero * lambda_param)
    return np.nan


def ComputeBinomialProportionStd(probability, denominator_count):
    if denominator_count <= 0 or np.isnan(denominator_count) or np.isnan(probability):
        return np.nan
    return np.sqrt(probability * (1 - probability) / denominator_count)


def FitZIPParams(counts_per_period):
    counts_per_period = np.asarray(counts_per_period, dtype=float)
    n        = len(counts_per_period)
    X        = np.ones((n, 1))
    lam_init = max(float(counts_per_period[counts_per_period > 0].mean()) if np.any(counts_per_period > 0) else 0.1, 0.1)
    pi_init  = float(np.clip(np.mean(counts_per_period == 0) - np.exp(-lam_init), 0.01, 0.9))
    start    = [np.log(lam_init), np.log(pi_init / (1 - pi_init))]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = ZeroInflatedPoisson(counts_per_period, X, exog_infl=np.ones((n, 1))).fit(
            start_params=start, disp=False
        )

    lam = float(max(np.exp(result.params[0]), 1e-6))
    pi  = float(np.clip(1.0 / (1.0 + np.exp(-result.params[1])), 0.0, 0.99))
    return pi, lam


def FitNBParams(counts_per_period):
    counts_per_period = np.asarray(counts_per_period, dtype=float)
    X   = np.ones((len(counts_per_period), 1))
    mu  = max(float(np.mean(counts_per_period)), 1e-6)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = NegativeBinomial(counts_per_period, X).fit(
            start_params=[np.log(mu), 0.0], disp=False, method="bfgs"
        )

    mu_fit = float(np.exp(result.params[0]))
    # NB2 parameterization: variance = mu + alpha*mu^2, so r = 1/alpha
    alpha  = float(np.exp(result.params[1]))
    r      = float(max(1.0 / alpha, 1e-6))
    p      = float(np.clip(r / (r + mu_fit), 0.01, 0.99))
    return r, p


def LogLikZIP(x, pi, lam):
    if x == 0:
        return float(np.log(pi + (1 - pi) * np.exp(-lam) + 1e-300))
    return float(np.log(1 - pi + 1e-300) + poisson.logpmf(x, lam))
