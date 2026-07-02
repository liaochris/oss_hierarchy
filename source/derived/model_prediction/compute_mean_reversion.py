# Per-cell control-org mean-reversion ratios (post/pre per-period opening rate, Post = event_time >= 0)
# consumed by predict_model.py, plus a per-org bridge table for the source/analysis binscatter.

import math
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from scipy.optimize import brentq

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

    combination_results = [
        ComputeCombination(variant, importance_type, qualified_sample, control_group)
        for variant, importance_type, qualified_sample, control_group in product(
            VARIANTS, importance_types, qualified_samples, control_groups
        )
    ]
    combination_results = [result for result in combination_results if result is not None]
    summary_rows  = [result["summary_row"] for result in combination_results]
    org_stat_rows = [org_row for result in combination_results for org_row in result["org_stat_rows"]]

    OUTDIR.mkdir(parents=True, exist_ok=True)
    SaveData(
        pd.DataFrame(summary_rows),
        ["variant", "importance_type", "qualified_sample", "control_group"],
        OUTDIR / "mean_reversion_ratios.parquet",
        OUTDIR / "mean_reversion_ratios.log",
    )
    SaveData(
        pd.DataFrame(org_stat_rows),
        ["variant", "importance_type", "qualified_sample", "control_group", "repo_name"],
        OUTDIR / "org_reversion_stats.parquet",
        OUTDIR / "org_reversion_stats.log",
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

    org_stats = Parallel(n_jobs=N_JOBS)(
        delayed(ControlOrgReversionStats)(repo_name, variant, importance_type, qualified_sample, control_group)
        for repo_name in control_repos
    )
    org_stats = [stats for stats in org_stats if stats is not None]

    per_org_rate_ratios  = np.array([stats["opened_ratio"] for stats in org_stats])
    positive_rate_ratios = per_org_rate_ratios[per_org_rate_ratios > 0]
    summary_row = {
        "variant":                variant,
        "importance_type":        importance_type,
        "qualified_sample":       qualified_sample,
        "control_group":          control_group,
        "latent_ratio_poisson":   RoundToTwoSignificant(PoissonFixedEffectsReversion(org_stats)),
        "latent_ratio_geom_mean": RoundToTwoSignificant(float(np.exp(np.log(positive_rate_ratios).mean()))),
        "latent_ratio_median":    RoundToTwoSignificant(float(np.median(per_org_rate_ratios))),
    }
    org_stat_rows = [
        {
            "variant":            variant,
            "importance_type":    importance_type,
            "qualified_sample":   qualified_sample,
            "control_group":      control_group,
            "repo_name":          stats["repo_name"],
            "opened_ratio":       stats["opened_ratio"],
            "opened_post_volume": stats["opened_post_volume"],
            "merged_ratio":       stats["merged_ratio"],
            "merged_post_volume": stats["merged_post_volume"],
        }
        for stats in org_stats
    ]
    return {"summary_row": summary_row, "org_stat_rows": org_stat_rows}


def ControlOrgReversionStats(repo_name, variant, importance_type, qualified_sample, control_group):
    member_path = (
        INDIR_MEMBER_PANEL / variant / importance_type / qualified_sample
        / control_group / f"{MakeRepoNameSafe(repo_name)}.parquet"
    )
    if not member_path.exists():
        return None

    by_period = pd.read_parquet(member_path, columns=[
        "quasi_event_time", "repo_pull_request_opened",
        "repo_pull_request_merged_direct", "repo_pull_request_merged_after_review",
    ]).groupby("quasi_event_time").first()
    opened = by_period["repo_pull_request_opened"]
    merged = by_period["repo_pull_request_merged_direct"] + by_period["repo_pull_request_merged_after_review"]

    pre_opened  = opened[opened.index < 0]
    post_opened = opened[opened.index >= 0]
    opened_ratio, opened_post_volume = PrePostRateRatio(opened)
    if opened_ratio is None:
        return None
    merged_ratio, merged_post_volume = PrePostRateRatio(merged)

    return {
        "repo_name":          repo_name,
        "pre_opens":          float(pre_opened.sum()),
        "post_opens":         float(post_opened.sum()),
        "pre_periods":        int(len(pre_opened)),
        "post_periods":       int(len(post_opened)),
        "opened_ratio":       opened_ratio,
        "opened_post_volume": opened_post_volume,
        "merged_ratio":       merged_ratio,
        "merged_post_volume": merged_post_volume,
    }


def PrePostRateRatio(counts):
    pre  = counts[counts.index < 0]
    post = counts[counts.index >= 0]
    post_volume = float(post.sum()) if not post.empty else 0.0
    if pre.empty or post.empty or float(pre.sum()) == 0:
        return None, post_volume
    return (post_volume / len(post)) / (float(pre.sum()) / len(pre)), post_volume


def PoissonFixedEffectsReversion(org_stats):
    # delta solves the profiled Poisson score, with org effects concentrated out.
    total_post_opens = sum(stats["post_opens"] for stats in org_stats)
    total_pre_opens  = sum(stats["pre_opens"]  for stats in org_stats)
    if total_pre_opens == 0:
        return np.nan
    if total_post_opens == 0:
        return 0.0

    def score(delta):
        implied_post_opens = sum(
            stats["post_periods"] * (stats["pre_opens"] + stats["post_opens"])
            / (stats["pre_periods"] + stats["post_periods"] * delta)
            for stats in org_stats
        )
        return total_post_opens - delta * implied_post_opens

    return float(brentq(score, 1e-8, 1e6))


def RoundToTwoSignificant(value):
    if not np.isfinite(value) or value == 0:
        return value
    return round(value, 1 - int(math.floor(math.log10(abs(value)))))


if __name__ == "__main__":
    Main()
