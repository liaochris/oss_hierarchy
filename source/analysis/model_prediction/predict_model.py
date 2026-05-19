import json

import numpy as np
import pandas as pd
from pathlib import Path
from itertools import product

from source.lib.python.config_loaders import LoadPipelineInputs, LoadAnalysisParameters
from source.lib.python.repo_utils import MakeRepoNameSafe
from source.lib.JMSLab.SaveData import SaveData

_ap = LoadAnalysisParameters()

INDIR_MEMBER_PANEL   = Path("drive/output/derived/model_prediction/event_time_member_panel")
INDIR_ANALYSIS_PANEL = Path("output/derived/analysis_panel")
INDIR_FITTED         = Path("output/analysis/model_prediction")
OUTDIR               = Path("output/analysis/model_prediction")
ROLLING_LABEL        = "rolling5"
VARIANTS             = ["observed", "same_period", "opened_cohort"]
QUALIFIED_SAMPLES    = {"exact1", "exact2", "exact_1_2"}
POST_TIMES           = list(range(1, _ap["max_event_time"] + 1))


def Main():
    cfg = LoadPipelineInputs()
    importance_types  = cfg["importance_types"]["run"]
    qualified_samples = [s for s in cfg["qualified_samples"]["run"] if s in QUALIFIED_SAMPLES]
    control_groups    = cfg["control_groups"]["run"]

    for variant, importance_type, qualified_sample, control_group, event_time in product(
        VARIANTS, importance_types, qualified_samples, control_groups, POST_TIMES
    ):
        RunCombination(variant, importance_type, qualified_sample, control_group, event_time)


def RunCombination(variant, importance_type, qualified_sample, control_group, event_time):
    outdir = OUTDIR / variant / "predictions" / importance_type / qualified_sample / control_group / f"event_time_{event_time}"
    outdir.mkdir(parents=True, exist_ok=True)

    fitted_dir = INDIR_FITTED / variant / "fitted" / importance_type / qualified_sample / control_group
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

        member_path = (
            INDIR_MEMBER_PANEL / variant / importance_type / qualified_sample
            / control_group / f"{MakeRepoNameSafe(repo_name)}.parquet"
        )
        if not member_path.exists():
            continue

        df_member = pd.read_parquet(member_path)
        df_post   = df_member[df_member["quasi_event_time"] == event_time]
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
            "error_open_pct":         100 * (N_obs_open - N_hat_open) / N_hat_open if N_hat_open > 0 else np.nan,
            "error_review_rate_pp":   100 * (prob_review_obs - prob_review_counterfactual) if not np.isnan(prob_review_obs) else np.nan,
            "error_merge_rate_pp":    100 * (prob_merge_obs  - prob_merge_counterfactual)  if not np.isnan(prob_merge_obs)  else np.nan,
        })

    SaveData(
        pd.DataFrame(pred_rows), ["repo_name"],
        outdir / "predictions.parquet", outdir / "predictions.log",
    )


if __name__ == "__main__":
    Main()
