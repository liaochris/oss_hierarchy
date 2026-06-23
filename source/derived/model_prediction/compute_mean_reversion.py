# Control-org opened-count post/pre mean-reversion ratio per cell: the geometric mean (applied by
# predict_model.py to the post-period latent rate) and the median (reference). Both drop silent
# post-period orgs (ratio 0); the geometric mean is the robust multiplicative average of the decline.

import math
from itertools import product
from pathlib import Path

import numpy as np
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

    rows = [
        ComputeCombination(variant, importance_type, qualified_sample, control_group)
        for variant, importance_type, qualified_sample, control_group in product(
            VARIANTS, importance_types, qualified_samples, control_groups
        )
    ]
    rows = [row for row in rows if row is not None]

    OUTDIR.mkdir(parents=True, exist_ok=True)
    SaveData(
        pd.DataFrame(rows),
        ["variant", "importance_type", "qualified_sample", "control_group"],
        OUTDIR / "mean_reversion_ratios.parquet",
        OUTDIR / "mean_reversion_ratios.log",
    )


def ComputeCombination(variant, importance_type, qualified_sample, control_group):
    panel_path = (
        INDIR_ANALYSIS_PANEL / importance_type / ROLLING_LABEL
        / qualified_sample / control_group / "panel.parquet"
    )
    if not panel_path.exists():
        return None

    df_panel = pd.read_parquet(panel_path, columns=["repo_name", "quasi_event_time", "num_dropouts"])
    at_event = df_panel[df_panel["quasi_event_time"] == 0]
    control_repos = at_event.loc[at_event["num_dropouts"] == 0, "repo_name"].unique()

    org_ratios = Parallel(n_jobs=N_JOBS)(
        delayed(ControlOrgRatio)(repo_name, variant, importance_type, qualified_sample, control_group)
        for repo_name in control_repos
    )
    declines = np.array([ratio for ratio in org_ratios if ratio is not None and ratio > 0])
    return {
        "variant":             variant,
        "importance_type":     importance_type,
        "qualified_sample":    qualified_sample,
        "control_group":       control_group,
        "latent_ratio":        RoundToTwoSignificant(float(np.exp(np.log(declines).mean()))),
        "latent_ratio_median": RoundToTwoSignificant(float(np.median(declines))),
    }


def ControlOrgRatio(repo_name, variant, importance_type, qualified_sample, control_group):
    member_path = (
        INDIR_MEMBER_PANEL / variant / importance_type / qualified_sample
        / control_group / f"{MakeRepoNameSafe(repo_name)}.parquet"
    )
    if not member_path.exists():
        return None

    df_member = pd.read_parquet(member_path, columns=["quasi_event_time", "repo_pull_request_opened"])
    opened = df_member.groupby("quasi_event_time")["repo_pull_request_opened"].first()
    pre  = opened[opened.index < 0]
    post = opened[opened.index >= 1]
    if pre.empty or post.empty:
        return None
    return float(post.mean()) / float(pre.mean())


def RoundToTwoSignificant(value):
    return round(value, 1 - int(math.floor(math.log10(abs(value)))))


if __name__ == "__main__":
    Main()
