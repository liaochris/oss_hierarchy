import json

import numpy as np
import pandas as pd
from pathlib import Path
from itertools import product

from source.lib.python.config_loaders import LoadPipelineInputs, LoadAnalysisParameters, LoadModelPredictionConfig
from source.lib.python.repo_utils import MakeRepoNameSafe
from source.lib.JMSLab.SaveData import SaveData
from source.analysis.model_prediction.model_fitting_utils import (
    FitDistribution, FitMemberProbabilities, ComputeModelVariance, ComputeBinomialProportionStd
)

_ap = LoadAnalysisParameters()
_model_prediction_config = LoadModelPredictionConfig()

INDIR_MEMBER_PANEL   = Path("drive/output/derived/model_prediction/event_time_member_panel")
INDIR_ANALYSIS_PANEL = Path("output/derived/analysis_panel")
INDIR_FITTED         = Path("output/analysis/model_prediction")
OUTDIR               = Path("output/analysis/model_prediction")
ROLLING_LABEL        = "rolling5"
VARIANTS              = _model_prediction_config["variants"]["run"]
ESTIMATION_APPROACHES = _model_prediction_config["member_probability_estimation"]["run"]
QUALIFIED_SAMPLES     = {"exact1", "exact2", "exact_1_2"}
POST_TIMES            = list(range(1, _ap["max_event_time"] + 1))
ACTIVE_ERROR_METRICS  = _model_prediction_config["error_metrics"]["run"]


def Main():
    cfg = LoadPipelineInputs()
    importance_types  = cfg["importance_types"]["run"]
    qualified_samples = [s for s in cfg["qualified_samples"]["run"] if s in QUALIFIED_SAMPLES]
    control_groups    = cfg["control_groups"]["run"]

    for variant, importance_type, qualified_sample, control_group, event_time, estimation_approach in product(
        VARIANTS, importance_types, qualified_samples, control_groups, POST_TIMES, ESTIMATION_APPROACHES
    ):
        RunCombination(variant, importance_type, qualified_sample, control_group, event_time, estimation_approach)

    for variant, importance_type, qualified_sample, control_group, estimation_approach in product(
        VARIANTS, importance_types, qualified_samples, control_groups, ESTIMATION_APPROACHES
    ):
        RunPrePeriodCombination(variant, importance_type, qualified_sample, control_group, estimation_approach)


def RunCombination(variant, importance_type, qualified_sample, control_group, event_time, estimation_approach):
    outdir = OUTDIR / variant / "predictions" / estimation_approach / importance_type / qualified_sample / control_group / f"event_time_{event_time}"
    outdir.mkdir(parents=True, exist_ok=True)

    fitted_dir = INDIR_FITTED / variant / "fitted" / estimation_approach / importance_type / qualified_sample / control_group
    df_dist  = pd.read_parquet(fitted_dir / "distribution_params.parquet")
    df_probs = pd.read_parquet(fitted_dir / "member_probabilities.parquet")

    panel_path = (
        INDIR_ANALYSIS_PANEL / importance_type / ROLLING_LABEL
        / qualified_sample / control_group / "panel.parquet"
    )
    df_panel = pd.read_parquet(panel_path)
    df_all_repos = df_panel[df_panel["quasi_event_time"] == 0][
        ["repo_name", "dropouts_actors", "num_dropouts"]
    ]

    pred_rows = []
    for _, row in df_all_repos.iterrows():
        repo_name  = row["repo_name"]
        is_treated = row["num_dropouts"] > 0
        dropout_set = set(json.loads(row["dropouts_actors"])) if is_treated else set()

        df_dist_repo = df_dist[df_dist["repo_name"] == repo_name]
        if df_dist_repo.empty:
            continue
        N_hat = float(df_dist_repo["N_hat"].iloc[0])

        df_member_probs = df_probs[df_probs["repo_name"] == repo_name]
        df_remaining    = df_member_probs[~df_member_probs["actor_id"].isin(dropout_set)]

        prob_open_counterfactual   = float(df_remaining["prob_open"].sum())
        prob_review_counterfactual = 1.0 - float(np.prod(1.0 - df_remaining["prob_review"].values))
        prob_merge_counterfactual  = float(df_remaining["prob_merge"].sum())

        N_hat_open   = N_hat * prob_open_counterfactual
        N_hat_review = N_hat * prob_open_counterfactual * prob_review_counterfactual
        N_hat_merge  = N_hat * prob_open_counterfactual * prob_review_counterfactual * prob_merge_counterfactual

        dist_row       = df_dist_repo.iloc[0]
        model_var_open = ComputeModelVariance(
            dist_row["distribution_type"], N_hat_open,
            dist_row["r"], dist_row["pi_zero"], dist_row["lambda"]
        )
        model_std_open = np.sqrt(model_var_open) if not np.isnan(model_var_open) else np.nan

        member_path = (
            INDIR_MEMBER_PANEL / variant / importance_type / qualified_sample
            / control_group / f"{MakeRepoNameSafe(repo_name)}.parquet"
        )
        if not member_path.exists():
            continue

        df_member = pd.read_parquet(member_path)

        df_post = df_member[df_member["quasi_event_time"] == event_time]
        if df_post.empty:
            continue

        obs = df_post.groupby("quasi_event_time")[
            ["repo_pull_request_opened", "repo_pull_request_reviewed", "repo_pull_request_merged"]
        ].first().iloc[0]
        N_obs_open   = float(obs["repo_pull_request_opened"])
        N_obs_review = float(obs["repo_pull_request_reviewed"])
        N_obs_merge  = float(obs["repo_pull_request_merged"])

        prob_review_obs = N_obs_review / N_obs_open   if N_obs_open   > 0 else np.nan
        prob_merge_obs  = N_obs_merge  / N_obs_review if N_obs_review > 0 else np.nan

        # Condition on actual observed N for rate stds — exact conditional binomial variance
        model_std_review_rate = ComputeBinomialProportionStd(prob_review_counterfactual, N_obs_open)
        model_std_merge_rate  = ComputeBinomialProportionStd(prob_merge_counterfactual,  N_obs_review)

        pred_rows.append({
            "repo_name":                    repo_name,
            "is_treated":                   is_treated,
            "N_hat":                        N_hat,
            "prob_open_counterfactual":     prob_open_counterfactual,
            "prob_review_counterfactual":   prob_review_counterfactual,
            "prob_merge_counterfactual":    prob_merge_counterfactual,
            "N_hat_open":                   N_hat_open,
            "N_hat_review":                 N_hat_review,
            "N_hat_merge":                  N_hat_merge,
            "N_obs_open":                   N_obs_open,
            "N_obs_review":                 N_obs_review,
            "N_obs_merge":                  N_obs_merge,
            "prob_review_obs":              prob_review_obs,
            "prob_merge_obs":               prob_merge_obs,
            "error_open_pct":               100 * (N_obs_open - N_hat_open) / N_hat_open if N_hat_open > 0 else np.nan,
            "error_review_rate_pp":         100 * (prob_review_obs - prob_review_counterfactual) if not np.isnan(prob_review_obs) else np.nan,
            "error_merge_rate_pp":          100 * (prob_merge_obs  - prob_merge_counterfactual)  if not np.isnan(prob_merge_obs)  else np.nan,
            "error_review_rate_pct":        100 * (prob_review_obs - prob_review_counterfactual) / prob_review_counterfactual if prob_review_counterfactual > 0 else np.nan,
            "error_merge_rate_pct":         100 * (prob_merge_obs  - prob_merge_counterfactual)  / prob_merge_counterfactual  if prob_merge_counterfactual  > 0 else np.nan,
            "signed_std_residual_open":         SignedStandardizedResidual(N_obs_open,      N_hat_open,                 model_std_open),
            "signed_std_residual_review_rate":  SignedStandardizedResidual(prob_review_obs, prob_review_counterfactual, model_std_review_rate),
            "signed_std_residual_merge_rate":   SignedStandardizedResidual(prob_merge_obs,  prob_merge_counterfactual,  model_std_merge_rate),
            "squared_std_residual_open":        SquaredStandardizedResidual(N_obs_open,      N_hat_open,                 model_std_open),
            "squared_std_residual_review_rate": SquaredStandardizedResidual(prob_review_obs, prob_review_counterfactual, model_std_review_rate),
            "squared_std_residual_merge_rate":  SquaredStandardizedResidual(prob_merge_obs,  prob_merge_counterfactual,  model_std_merge_rate),
        })

    SaveData(
        pd.DataFrame(pred_rows), ["repo_name"],
        outdir / "predictions.parquet", outdir / "predictions.log",
    )


def RunPrePeriodCombination(variant, importance_type, qualified_sample, control_group, estimation_approach):
    outdir = OUTDIR / variant / "pre_period" / estimation_approach / importance_type / qualified_sample / control_group
    outdir.mkdir(parents=True, exist_ok=True)

    panel_path = (
        INDIR_ANALYSIS_PANEL / importance_type / ROLLING_LABEL
        / qualified_sample / control_group / "panel.parquet"
    )
    df_panel     = pd.read_parquet(panel_path)
    df_all_repos = df_panel[df_panel["quasi_event_time"] == 0][["repo_name", "num_dropouts"]]

    pre_period_rows = []
    for _, row in df_all_repos.iterrows():
        repo_name  = row["repo_name"]
        is_treated = row["num_dropouts"] > 0

        member_path = (
            INDIR_MEMBER_PANEL / variant / importance_type / qualified_sample
            / control_group / f"{MakeRepoNameSafe(repo_name)}.parquet"
        )
        if not member_path.exists():
            continue

        df_member = pd.read_parquet(member_path)
        df_pre    = df_member[df_member["quasi_event_time"] < 0]
        pre_times = sorted(df_pre["quasi_event_time"].unique())

        if len(pre_times) < 2:
            continue

        for held_out_time in pre_times:
            train_times = [t for t in pre_times if t != held_out_time]
            df_train    = df_pre[df_pre["quasi_event_time"].isin(train_times)]

            df_repo_counts_train    = (
                df_train.groupby("quasi_event_time")[
                    ["repo_pull_request_opened", "repo_pull_request_reviewed", "repo_pull_request_merged"]
                ].first().reset_index()
            )
            counts_per_period_train = df_repo_counts_train["repo_pull_request_opened"].values.astype(float)

            dist_result, _       = FitDistribution(repo_name, counts_per_period_train)
            N_hat_loo            = float(dist_result["N_hat"])
            member_probs_loo     = FitMemberProbabilities(repo_name, df_train, df_repo_counts_train)
            df_member_probs_loo  = pd.DataFrame(member_probs_loo)
            prob_review_full_loo = 1.0 - float(np.prod(1.0 - df_member_probs_loo["prob_review"].values))
            prob_merge_full_loo  = float(df_member_probs_loo["prob_merge"].sum())

            model_var_open_loo = ComputeModelVariance(
                dist_result["distribution_type"], N_hat_loo,
                dist_result["r"], dist_result["pi_zero"], dist_result["lambda"]
            )
            model_std_open_loo = np.sqrt(model_var_open_loo) if not np.isnan(model_var_open_loo) else np.nan

            df_held        = df_pre[df_pre["quasi_event_time"] == held_out_time]
            obs_row        = df_held[["repo_pull_request_opened", "repo_pull_request_reviewed", "repo_pull_request_merged"]].iloc[0]
            N_obs_open_t   = float(obs_row["repo_pull_request_opened"])
            N_obs_review_t = float(obs_row["repo_pull_request_reviewed"])
            N_obs_merge_t  = float(obs_row["repo_pull_request_merged"])

            prob_review_obs_t = N_obs_review_t / N_obs_open_t   if N_obs_open_t   > 0 else np.nan
            prob_merge_obs_t  = N_obs_merge_t  / N_obs_review_t if N_obs_review_t > 0 else np.nan

            model_std_review_rate_t = ComputeBinomialProportionStd(prob_review_full_loo, N_obs_open_t)
            model_std_merge_rate_t  = ComputeBinomialProportionStd(prob_merge_full_loo,  N_obs_review_t)

            pre_period_rows.append({
                "repo_name":                        repo_name,
                "quasi_event_time":                 held_out_time,
                "is_treated":                       is_treated,
                "signed_std_residual_open":         SignedStandardizedResidual(N_obs_open_t,      N_hat_loo,             model_std_open_loo),
                "signed_std_residual_review_rate":  SignedStandardizedResidual(prob_review_obs_t, prob_review_full_loo,  model_std_review_rate_t),
                "signed_std_residual_merge_rate":   SignedStandardizedResidual(prob_merge_obs_t,  prob_merge_full_loo,   model_std_merge_rate_t),
                "squared_std_residual_open":        SquaredStandardizedResidual(N_obs_open_t,      N_hat_loo,            model_std_open_loo),
                "squared_std_residual_review_rate": SquaredStandardizedResidual(prob_review_obs_t, prob_review_full_loo, model_std_review_rate_t),
                "squared_std_residual_merge_rate":  SquaredStandardizedResidual(prob_merge_obs_t,  prob_merge_full_loo,  model_std_merge_rate_t),
            })

    SaveData(
        pd.DataFrame(pre_period_rows), ["repo_name", "quasi_event_time"],
        outdir / "pre_period_evaluation.parquet", outdir / "pre_period_evaluation.log",
    )


def PercentagePointError(obs, pred):
    return 100 * (obs - pred)


def PercentError(obs, pred):
    if pred == 0 or np.isnan(pred):
        return np.nan
    return 100 * (obs - pred) / pred


def SignedStandardizedResidual(obs, mean, std, centered=True):
    if std == 0 or np.isnan(std):
        return np.nan
    numerator = (obs - mean) if centered else obs
    return numerator / std


def SquaredStandardizedResidual(obs, mean, std, centered=True):
    signed_residual = SignedStandardizedResidual(obs, mean, std, centered=True)
    if np.isnan(signed_residual):
        return np.nan
    return (signed_residual ** 2 - 1) if centered else signed_residual ** 2


if __name__ == "__main__":
    Main()
