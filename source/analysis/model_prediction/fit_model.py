import numpy as np
import pandas as pd
from pathlib import Path
from itertools import product

from source.lib.python.config_loaders import LoadPipelineInputs, LoadModelPredictionConfig
from source.lib.python.repo_utils import MakeRepoNameSafe
from source.lib.JMSLab.SaveData import SaveData
from source.analysis.model_prediction.model_fitting_utils import (
    FitLatentDistribution, FitMemberProbabilities
)

CONFIG                  = LoadPipelineInputs()
MODEL_PREDICTION_CONFIG = LoadModelPredictionConfig()

INDIR_MEMBER_PANEL   = Path("drive/output/derived/model_prediction/event_time_member_panel")
INDIR_ANALYSIS_PANEL = Path("output/derived/analysis_panel")
OUTDIR               = Path("output/analysis/model_prediction")
ROLLING_LABEL         = f"rolling{CONFIG['rolling_periods']['run'][0]}"
VARIANTS              = MODEL_PREDICTION_CONFIG["variants"]["run"]
DISTRIBUTION_TYPES    = MODEL_PREDICTION_CONFIG["distribution_types"]["run"]
ESTIMATION_APPROACHES = MODEL_PREDICTION_CONFIG["member_probability_estimation"]["run"]


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
    df_all_repos = df_panel[df_panel["quasi_event_time"] == 0][["repo_name"]]

    dist_rows, prob_rows, review_rows = [], [], []

    for _, row in df_all_repos.iterrows():
        repo_name = row["repo_name"]
        member_path = (
            INDIR_MEMBER_PANEL / variant / importance_type / qualified_sample
            / control_group / f"{MakeRepoNameSafe(repo_name)}.parquet"
        )
        if not member_path.exists():
            continue

        df_member = pd.read_parquet(member_path)
        df_pre    = df_member[df_member["quasi_event_time"] < 0].copy()
        df_repo_counts = (
            df_pre.groupby("quasi_event_time")[
                ["repo_pull_request_opened", "repo_pull_request_reviewed",
                 "repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"]
            ].first().reset_index()
        )
        counts_per_period = df_repo_counts["repo_pull_request_opened"].values.astype(float)

        dist_result  = FitLatentDistribution(repo_name, counts_per_period, distribution_type)
        member_probs = FitMemberProbabilities(repo_name, df_pre, df_repo_counts, estimation_approach)

        dist_rows.append(dist_result)
        prob_rows.extend(member_probs)
        review_rows.append(ComputeReviewFitCheck(repo_name, df_repo_counts, member_probs))

    SaveData(
        pd.DataFrame(dist_rows), ["repo_name"],
        outdir / "distribution_params.parquet", outdir / "distribution_params.log",
    )
    SaveData(
        pd.DataFrame(prob_rows), ["repo_name", "actor_id"],
        outdir / "member_probabilities.parquet", outdir / "member_probabilities.log",
    )
    SaveData(
        pd.DataFrame(review_rows), ["repo_name"],
        outdir / "review_fit_check.parquet", outdir / "review_fit_check.log",
    )


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
        "mean_squared_deviation_review": float(np.mean([(rate - prob_review_implied) ** 2 for rate in observed_review_rates])) if observed_review_rates else np.nan,
    }


if __name__ == "__main__":
    Main()
