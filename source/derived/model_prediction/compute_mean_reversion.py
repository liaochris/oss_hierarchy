# Control-org opened-count post/pre ratios; predict_model.py samples this per-org distribution
# for the mean-reversion adjustment.

from itertools import product
from pathlib import Path

import pandas as pd
from joblib import Parallel, delayed

from source.lib.JMSLab.SaveData import SaveData
from source.lib.python.config_loaders import (
    LoadGlobalSettings, LoadPipelineInputs, LoadModelPredictionConfig
)
from source.lib.python.repo_utils import MakeRepoNameSafe

GLOBAL_SETTINGS         = LoadGlobalSettings()
CONFIG                  = LoadPipelineInputs()
MODEL_PREDICTION_CONFIG = LoadModelPredictionConfig()

INDIR_MEMBER_PANEL   = Path("drive/output/derived/model_prediction/event_time_member_panel")
INDIR_ANALYSIS_PANEL = Path("output/derived/analysis_panel")
OUTDIR               = Path("output/derived/model_prediction/mean_reversion")
ROLLING_LABEL = f"rolling{CONFIG['rolling_periods']['run'][0]}"
VARIANTS      = MODEL_PREDICTION_CONFIG["variants"]["run"]
N_JOBS        = GLOBAL_SETTINGS["n_jobs"]


def Main():
    importance_types  = CONFIG["importance_types"]["run"]
    qualified_samples = CONFIG["qualified_samples"]["run"]
    control_groups    = CONFIG["control_groups"]["run"]

    rows = []
    for variant, importance_type, qualified_sample, control_group in product(
        VARIANTS, importance_types, qualified_samples, control_groups
    ):
        rows.extend(ComputeCombination(variant, importance_type, qualified_sample, control_group))

    OUTDIR.mkdir(parents=True, exist_ok=True)
    SaveData(
        pd.DataFrame(rows),
        ["variant", "importance_type", "qualified_sample", "control_group", "repo_name"],
        OUTDIR / "mean_reversion_ratio_distribution.parquet",
        OUTDIR / "mean_reversion_ratio_distribution.log",
    )


def ComputeCombination(variant, importance_type, qualified_sample, control_group):
    panel_path = (
        INDIR_ANALYSIS_PANEL / importance_type / ROLLING_LABEL
        / qualified_sample / control_group / "panel.parquet"
    )
    if not panel_path.exists():
        return []

    df_panel = pd.read_parquet(panel_path, columns=["repo_name", "quasi_event_time", "num_dropouts"])
    at_event = df_panel[df_panel["quasi_event_time"] == 0]
    control_repos = at_event.loc[at_event["num_dropouts"] == 0, "repo_name"].unique()

    org_ratios = Parallel(n_jobs=N_JOBS)(
        delayed(ControlOrgRatio)(repo_name, variant, importance_type, qualified_sample, control_group)
        for repo_name in control_repos
    )
    return [
        {"variant": variant, "importance_type": importance_type, "qualified_sample": qualified_sample,
         "control_group": control_group, "repo_name": repo_name, "latent_ratio": ratio}
        for repo_name, ratio in org_ratios if ratio is not None
    ]


def ControlOrgRatio(repo_name, variant, importance_type, qualified_sample, control_group):
    member_path = (
        INDIR_MEMBER_PANEL / variant / importance_type / qualified_sample
        / control_group / f"{MakeRepoNameSafe(repo_name)}.parquet"
    )
    if not member_path.exists():
        return repo_name, None

    df_member = pd.read_parquet(member_path, columns=["quasi_event_time", "repo_pull_request_opened"])
    opened = df_member.groupby("quasi_event_time")["repo_pull_request_opened"].first()
    pre  = opened[opened.index < 0]
    post = opened[opened.index >= 1]
    if pre.empty or post.empty:
        return repo_name, None
    return repo_name, float(post.mean()) / float(pre.mean())


if __name__ == "__main__":
    Main()
