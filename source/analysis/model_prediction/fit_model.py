import numpy as np
import pandas as pd
from pathlib import Path
from itertools import product

from source.lib.python.config_loaders import LoadPipelineInputs, LoadModelPredictionConfig
from source.lib.python.repo_utils import MakeRepoNameSafe
from source.lib.JMSLab.SaveData import SaveData
from source.analysis.model_prediction.model_fitting_utils import FitDistribution, FitMemberProbabilities

_model_prediction_config = LoadModelPredictionConfig()

INDIR_MEMBER_PANEL   = Path("drive/output/derived/model_prediction/event_time_member_panel")
INDIR_ANALYSIS_PANEL = Path("output/derived/analysis_panel")
OUTDIR               = Path("output/analysis/model_prediction")
ROLLING_LABEL        = "rolling5"
VARIANTS             = _model_prediction_config["variants"]["run"]
ESTIMATION_APPROACHES = _model_prediction_config["member_probability_estimation"]["run"]
QUALIFIED_SAMPLES    = {"exact1", "exact2", "exact_1_2"}


def Main():
    cfg = LoadPipelineInputs()
    importance_types  = cfg["importance_types"]["run"]
    qualified_samples = [s for s in cfg["qualified_samples"]["run"] if s in QUALIFIED_SAMPLES]
    control_groups    = cfg["control_groups"]["run"]

    for variant, importance_type, qualified_sample, control_group, estimation_approach in product(
        VARIANTS, importance_types, qualified_samples, control_groups, ESTIMATION_APPROACHES
    ):
        RunCombination(variant, importance_type, qualified_sample, control_group, estimation_approach)


def RunCombination(variant, importance_type, qualified_sample, control_group, estimation_approach):
    outdir = OUTDIR / variant / "fitted" / estimation_approach / importance_type / qualified_sample / control_group
    outdir.mkdir(parents=True, exist_ok=True)

    panel_path = (
        INDIR_ANALYSIS_PANEL / importance_type / ROLLING_LABEL
        / qualified_sample / control_group / "panel.parquet"
    )
    df_panel = pd.read_parquet(panel_path)

    df_all_repos = df_panel[df_panel["quasi_event_time"] == 0][["repo_name"]]

    dist_rows, comparison_rows, prob_rows, review_rows = [], [], [], []

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
                ["repo_pull_request_opened", "repo_pull_request_reviewed", "repo_pull_request_merged"]
            ].first().reset_index()
        )
        counts_per_period = df_repo_counts["repo_pull_request_opened"].values.astype(float)

        dist_result, comparison_result = FitDistribution(repo_name, counts_per_period)
        member_probs = FitMemberProbabilities(repo_name, df_pre, df_repo_counts)

        dist_rows.append(dist_result)
        comparison_rows.append(comparison_result)
        prob_rows.extend(member_probs)
        review_rows.append(ComputeReviewFitCheck(repo_name, df_repo_counts, member_probs))

    SaveData(
        pd.DataFrame(dist_rows), ["repo_name"],
        outdir / "distribution_params.parquet", outdir / "distribution_params.log",
    )
    SaveData(
        pd.DataFrame(comparison_rows), ["repo_name"],
        outdir / "distribution_comparison.parquet", outdir / "distribution_comparison.log",
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

    obs_rates = [
        row["repo_pull_request_reviewed"] / row["repo_pull_request_opened"]
        for _, row in df_repo_counts.iterrows()
        if row["repo_pull_request_opened"] > 0
    ]

    return {
        "repo_name":            repo_name,
        "prob_review_implied":  prob_review_implied,
        "prob_review_obs_mean": float(np.mean(obs_rates))                                            if obs_rates else np.nan,
        "msd_review":           float(np.mean([(r - prob_review_implied) ** 2 for r in obs_rates])) if obs_rates else np.nan,
    }


if __name__ == "__main__":
    Main()
