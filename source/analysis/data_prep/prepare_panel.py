import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

from source.lib.helpers import LoadGlobals
from source.lib.JMSLab.SaveData import SaveData

INDIR = Path("drive/output/derived/org_outcomes_practices/org_panel")
OUTDIR = Path("output/analysis/data_prep")
INDIR_CONFIG = Path("source/analysis/config")
INDIR_LIB = Path("source/lib")

_pipeline_cfg = LoadGlobals(INDIR_LIB / "pipeline_config.json")
IMPORTANCE_TYPES = _pipeline_cfg["importance_types"]["run"]
ROLLING_PERIODS = [f"rolling{p}" for p in _pipeline_cfg["rolling_periods"]["run"]]

MAX_BASELINE_NA_COUNT = 200


def Main():
    with open(INDIR_CONFIG / "pc_groups.json") as f:
        pc_groups_cfg = json.load(f)
    for importance_type in IMPORTANCE_TYPES:
        for rolling_period in ROLLING_PERIODS:
            ProcessDataset(importance_type, rolling_period, pc_groups_cfg)


def ProcessDataset(importance_type, rolling_period, pc_groups_cfg):
    panel = pd.read_parquet(INDIR / importance_type / rolling_period / "panel.parquet")
    base = FilterLowDepartures(panel)

    exact_subsets = {n: FilterExactlyNQualified(base, n) for n in [1, 2, 3]}
    any_repos = set().union(*(set(s["repo_name"].unique()) for s in exact_subsets.values()))
    scoring_population = base[base["repo_name"].isin(any_repos)]

    repo_scores, metadata, excluded_vars = ComputeRepoPCScores(scoring_population, pc_groups_cfg)

    base_outdir = OUTDIR / importance_type / rolling_period
    for n, subset in exact_subsets.items():
        SaveSampleOutputs(
            panel, repo_scores, metadata, excluded_vars,
            base_outdir / f"qualified_{n}",
            keep_repos=subset["repo_name"].unique(),
        )
    SaveSampleOutputs(
        panel, repo_scores, metadata, excluded_vars,
        base_outdir / "qualified_any",
        keep_repos=list(any_repos),
    )


def FilterLowDepartures(panel):
    return panel[panel["num_departures"] <= 1].copy()


def FilterExactlyNQualified(panel, n):
    at_event = panel[panel["time_index"] == panel["quasi_treatment_group"]]
    keep = at_event.loc[at_event["num_important_qualified"] == n, "repo_name"].unique()
    return panel[panel["repo_name"].isin(keep)].copy()


def ComputeRepoPCScores(panel, pc_groups_cfg):
    df = panel.copy()
    df["quasi_event_time"] = df["time_index"] - df["quasi_treatment_group"]
    baseline = df[df["quasi_event_time"] == -1].copy()

    all_group_vars = {v for cfg in pc_groups_cfg.values() for v in cfg["vars"]}
    present_vars = [v for v in all_group_vars if v in baseline.columns]
    na_counts = baseline[present_vars].isna().sum()

    valid_vars = set(v for v in present_vars if na_counts[v] < MAX_BASELINE_NA_COUNT)
    excluded_rows = [
        {"group": group_name, "var": v, "na_count": int(na_counts[v])}
        for group_name, cfg in pc_groups_cfg.items()
        for v in cfg["vars"]
        if v in present_vars and na_counts[v] >= MAX_BASELINE_NA_COUNT
    ]
    excluded_vars = (
        pd.DataFrame(excluded_rows)
        if excluded_rows
        else pd.DataFrame(columns=["group", "var", "na_count"])
    )

    repo_means = baseline.groupby("repo_name")[list(valid_vars)].mean()
    result = repo_means.reset_index()[["repo_name"]]
    metadata_rows = []

    for group_name, cfg in pc_groups_cfg.items():
        present = [v for v in cfg["vars"] if v in valid_vars]
        if len(present) < 2:
            continue

        x = repo_means[present].values
        complete_mask = ~np.isnan(x).any(axis=1)
        if complete_mask.sum() < 2:
            continue

        scaler = StandardScaler()
        x_scaled = scaler.fit_transform(x[complete_mask])
        pca = PCA(n_components=1)
        pc1 = pca.fit_transform(x_scaled).ravel()

        if cfg["sign_flip"]:
            pc1 = -pc1

        scores = np.full(len(repo_means), np.nan)
        scores[complete_mask] = pc1

        var_explained = pca.explained_variance_ratio_[0] * 100
        loadings = pca.components_[0]
        if cfg["sign_flip"]:
            loadings = -loadings

        for var, loading in zip(present, loadings):
            metadata_rows.append({
                "group": group_name,
                "var": var,
                "loading": loading,
                "variance_explained_pc1": var_explained,
            })

        result[f"{group_name}_pc1"] = pd.Series(scores, index=result.index)

    metadata = pd.DataFrame(metadata_rows)
    return result, metadata, excluded_vars


def SaveSampleOutputs(panel, repo_scores, metadata, excluded_vars, outdir, keep_repos=None):
    outdir.mkdir(parents=True, exist_ok=True)

    sample = (
        panel[panel["repo_name"].isin(keep_repos)].copy()
        if keep_repos is not None
        else panel.copy()
    )

    continuous_cols = [c for c in repo_scores.columns if c != "repo_name"]
    repo_continuous = repo_scores[["repo_name"] + continuous_cols]
    repo_binary = CoarsenScoresToAboveBelowMedian(repo_scores)

    panel_pc = sample.merge(repo_continuous, on="repo_name", how="left")
    panel_pc_median = sample.merge(repo_binary, on="repo_name", how="left")

    SaveData(panel_pc, ["repo_name", "time_index"],
             outdir / "panel_pc.parquet", outdir / "panel_pc.log")
    SaveData(panel_pc_median, ["repo_name", "time_index"],
             outdir / "panel_pc_median.parquet", outdir / "panel_pc_median.log")
    SaveData(metadata, ["group", "var"],
             outdir / "pc1_metadata.csv", outdir / "pc1_metadata.log")
    SaveData(excluded_vars, ["group", "var"],
             outdir / "pc1_excluded_vars.csv", outdir / "pc1_excluded_vars.log")


def CoarsenScoresToAboveBelowMedian(repo_scores):
    score_cols = [c for c in repo_scores.columns if c != "repo_name"]
    binary = repo_scores[["repo_name"]].copy()
    for col in score_cols:
        median_val = repo_scores[col].median()
        binary[col] = np.where(
            repo_scores[col].isna(), np.nan,
            np.where(repo_scores[col] > median_val, 1.0, 0.0),
        )
    return binary


if __name__ == "__main__":
    Main()
