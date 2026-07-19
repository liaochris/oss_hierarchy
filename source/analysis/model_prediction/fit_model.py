import numpy as np
import pandas as pd
from pathlib import Path
from itertools import product

from joblib import Parallel, delayed

from source.lib.python.config_loaders import (
    LoadGlobalSettings, LoadPipelineInputs, LoadModelPredictionConfig, LoadPaperSettings,
)
from source.lib.python.repo_utils import MakeRepoNameSafe
from source.lib.JMSLab.SaveData import SaveData
from source.lib.model.staged_count_model import (
    FitLatentDistribution, FitPostLatentRegression, FitMemberProbabilities,
)

GLOBAL_SETTINGS         = LoadGlobalSettings()
CONFIG                  = LoadPipelineInputs()
MODEL_PREDICTION_CONFIG = LoadModelPredictionConfig()
PAPER_SETTINGS          = LoadPaperSettings()

INDIR_MEMBER_PANEL   = Path("drive/output/derived/model_prediction/event_time_member_panel")
INDIR_ANALYSIS_PANEL = Path("output/derived/analysis_panel")
OUTDIR               = Path("output/analysis/model_prediction")
ROLLING_LABEL         = f"rolling{CONFIG['rolling_periods']['run'][0]}"
PRE_PERIOD_COUNT      = CONFIG['rolling_periods']['run'][0]
POST_PERIODS          = list(range(1, PRE_PERIOD_COUNT + 1))
VARIANTS              = MODEL_PREDICTION_CONFIG["variants"]["run"]
DISTRIBUTION_TYPES    = MODEL_PREDICTION_CONFIG["distribution_types"]["run"]
ESTIMATION_APPROACHES = MODEL_PREDICTION_CONFIG["member_probability_estimation"]["run"]
N_JOBS                = GLOBAL_SETTINGS["n_jobs"]

DISTRIBUTION_PARAM_COLUMNS = [
    "repo_name", "distribution_type", "expected_latent_count", "poisson_rate",
    "negative_binomial_size", "negative_binomial_prob",
    "post_latent_mean", "fold_id",
]
TRIM_QUANTILE_BOUNDS = (0.01, 0.99)


def Main():
    importance_types  = CONFIG["importance_types"]["run"]
    qualified_samples = CONFIG["qualified_samples"]["run"]
    control_groups    = CONFIG["control_groups"]["run"]

    for variant, distribution_type, estimation_approach, importance_type, qualified_sample, control_group in product(
        VARIANTS, DISTRIBUTION_TYPES, ESTIMATION_APPROACHES,
        importance_types, qualified_samples, control_groups
    ):
        RunCombination(
            variant, distribution_type, estimation_approach,
            importance_type, qualified_sample, control_group
        )


def RunCombination(variant, distribution_type, estimation_approach,
                   importance_type, qualified_sample, control_group):
    outdir = (
        OUTDIR / variant / distribution_type / "parameters" / estimation_approach
        / importance_type / qualified_sample / control_group
    )
    outdir.mkdir(parents=True, exist_ok=True)

    panel_path = (
        INDIR_ANALYSIS_PANEL / importance_type / ROLLING_LABEL
        / qualified_sample / control_group / "panel.parquet"
    )
    df_panel = pd.read_parquet(panel_path)
    df_all_repos = df_panel[df_panel["quasi_event_time"] == 0][["repo_name", "num_dropouts"]]

    repo_fit_results = Parallel(n_jobs=N_JOBS)(
        delayed(FitRepo)(row["repo_name"], row["num_dropouts"] > 0, variant, importance_type,
                         qualified_sample, control_group, distribution_type, estimation_approach)
        for _, row in df_all_repos.iterrows()
    )
    repo_fit_results = [repo_result for repo_result in repo_fit_results if repo_result is not None]

    distribution_param_rows = [repo_result["dist"] for repo_result in repo_fit_results]
    member_prob_rows        = [member_prob_row
                               for repo_result in repo_fit_results
                               for member_prob_row in repo_result["probs"]]
    review_fit_rows         = [repo_result["review"] for repo_result in repo_fit_results]

    latent_inputs = [repo_result["latent"] for repo_result in repo_fit_results]
    post_latent_mean_by_repo, latent_coefficients = FitPostLatent(latent_inputs)

    df_distribution = pd.DataFrame(distribution_param_rows).merge(post_latent_mean_by_repo, on="repo_name", how="left")
    SaveData(
        df_distribution[DISTRIBUTION_PARAM_COLUMNS], ["repo_name"],
        outdir / "distribution_params.parquet", outdir / "distribution_params.log",
    )
    SaveData(
        pd.DataFrame(member_prob_rows), ["repo_name", "actor_id"],
        outdir / "member_probabilities.parquet", outdir / "member_probabilities.log",
    )
    SaveData(
        pd.DataFrame(review_fit_rows), ["repo_name"],
        outdir / "review_fit_check.parquet", outdir / "review_fit_check.log",
    )

    if IsPrimaryCombination(variant, distribution_type, estimation_approach,
                            importance_type, qualified_sample, control_group):
        WriteLatentRegressionTable(latent_coefficients, outdir / "model_latent_negative_binomial_table.txt")


def FitRepo(repo_name, is_treated, variant, importance_type, qualified_sample, control_group,
            distribution_type, estimation_approach):
    member_path = (
        INDIR_MEMBER_PANEL / variant / importance_type / qualified_sample
        / control_group / f"{MakeRepoNameSafe(repo_name)}.parquet"
    )
    if not member_path.exists():
        return None

    df_member = pd.read_parquet(member_path)
    df_counts_by_period = (
        df_member.groupby("quasi_event_time")[
            ["repo_pull_request_opened", "repo_pull_request_reviewed",
             "repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"]
        ].first().reset_index()
    )
    df_pre = df_member[df_member["quasi_event_time"].between(-PRE_PERIOD_COUNT, -1)].copy()
    df_repo_counts = df_counts_by_period[df_counts_by_period["quasi_event_time"].between(-PRE_PERIOD_COUNT, -1)]
    if df_repo_counts.empty:
        return None
    counts_per_period = df_repo_counts["repo_pull_request_opened"].values.astype(float)

    dist_result  = FitLatentDistribution(repo_name, counts_per_period, distribution_type)
    member_probs = FitMemberProbabilities(repo_name, df_pre, df_repo_counts, estimation_approach)
    return {
        "dist":   dist_result,
        "probs":  member_probs,
        "review": ComputeReviewFitCheck(repo_name, df_repo_counts, member_probs),
        "latent": LatentRegressionInputs(repo_name, is_treated, df_counts_by_period),
    }


def LatentRegressionInputs(repo_name, is_treated, df_counts_by_period):
    opened_by_period = dict(zip(df_counts_by_period["quasi_event_time"].astype(int),
                                df_counts_by_period["repo_pull_request_opened"].astype(float)))
    pre_periods_present = [period for period in range(-PRE_PERIOD_COUNT, 0) if period in opened_by_period]
    open_mean = float(np.mean([opened_by_period[period] for period in pre_periods_present]))
    last_pre_period = max(pre_periods_present)
    return {
        "repo_name":   repo_name,
        "is_treated":  bool(is_treated),
        "log_last":    float(np.log1p(opened_by_period[last_pre_period])),
        "open_mean":   open_mean,
        "n_pre":       len(pre_periods_present),
        "post_opened": {period: opened_by_period[period] for period in POST_PERIODS if period in opened_by_period},
    }


def FitPostLatent(latent_inputs):
    pre_period_mean = np.array([repo_inputs["open_mean"] for repo_inputs in latent_inputs], dtype=float)
    lower_bound, upper_bound = np.quantile(pre_period_mean, TRIM_QUANTILE_BOUNDS)
    kept_repos = {repo_inputs["repo_name"] for repo_inputs in latent_inputs
                  if lower_bound <= repo_inputs["open_mean"] <= upper_bound}

    repo_features = pd.DataFrame(
        [{key: repo_inputs[key] for key in ("repo_name", "is_treated", "log_last", "open_mean")}
         for repo_inputs in latent_inputs])
    control_post_cells = pd.DataFrame([
        {"repo_name": repo_inputs["repo_name"], "post_opened": opened,
         "log_last": repo_inputs["log_last"], "open_mean": repo_inputs["open_mean"]}
        for repo_inputs in latent_inputs
        if not repo_inputs["is_treated"] and repo_inputs["open_mean"] > 0 and repo_inputs["n_pre"] >= 2
        and repo_inputs["repo_name"] in kept_repos
        for opened in repo_inputs["post_opened"].values()
    ])
    return FitPostLatentRegression(control_post_cells, repo_features)


def IsPrimaryCombination(variant, distribution_type, estimation_approach,
                         importance_type, qualified_sample, control_group):
    return (variant == VARIANTS[0] and distribution_type == DISTRIBUTION_TYPES[0]
            and estimation_approach == ESTIMATION_APPROACHES[0]
            and importance_type == PAPER_SETTINGS["primary_importance_type"]
            and qualified_sample == PAPER_SETTINGS["primary_qualified_sample"]
            and control_group == PAPER_SETTINGS["primary_control_group"])


def WriteLatentRegressionTable(latent_coefficients, table_path):
    lines = ["<tab:model_latent_negative_binomial>",
             f"{latent_coefficients['intercept']:.3f}",
             f"{latent_coefficients['slope']:.3f}"]
    table_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def ComputeReviewFitCheck(repo_name, df_repo_counts, member_probs):
    prob_review_implied = 1.0 - float(np.prod([1.0 - row["prob_review"] for row in member_probs]))

    observed_review_rates = [
        row["repo_pull_request_reviewed"] / row["repo_pull_request_opened"]
        for _, row in df_repo_counts.iterrows()
        if row["repo_pull_request_opened"] > 0
    ]

    return {
        "repo_name":                   repo_name,
        "prob_review_implied":         prob_review_implied,
        "prob_review_obs_mean":        float(np.mean(observed_review_rates)) if observed_review_rates else np.nan,
        "mean_squared_deviation_review": float(np.mean([(observed_review_rate - prob_review_implied) ** 2 for observed_review_rate in observed_review_rates])) if observed_review_rates else np.nan,
    }


if __name__ == "__main__":
    Main()
