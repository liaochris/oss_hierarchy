"""
Prepare panel data for causal forest and event-study analyses.

Loads the org panel for a given dataset + rolling period, computes PCA-based
practice scores for each group defined in config/pc_groups.json, and writes:

  panel_pc.parquet        — original panel + continuous PC1 score per category
                            column names: {group}_pc1
  panel_pc_median.parquet — original panel + binary (0/1) median-split indicator
                            column names: {group}_pc1
  pc1_metadata.csv        — per-group PC1 loadings and variance explained

PC scores are computed from each repo's baseline (quasi_event_time == -1) values,
then broadcast to all time periods via a repo-level join. Repos outside the
analysis sample get NaN scores.

Raw panel columns have no _mean suffix; that suffix is added by R's
ComputeCovariateMeans and is not relevant here.

sign_flip in pc_groups.json negates PC1 for groups where a higher raw score
indicates lower organizational capability (e.g. shared_knowledge: higher HHI
means less sharing, so the sign is flipped to match the interpretive direction).
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

from source.lib.JMSLab.SaveData import SaveData

INDIR = Path("drive/output/derived/org_outcomes_practices/org_panel")
OUTDIR = Path("output/analysis/data_prep")
INDIR_CONFIG = Path("source/analysis/config")

DATASETS = ["important_degree_top3"]
ROLLING_LABELS = ["rolling5"]

NA_THRESHOLD = 200


def Main():
    with open(INDIR_CONFIG / "pc_groups.json") as f:
        pc_groups_cfg = json.load(f)

    for dataset in DATASETS:
        for rolling_label in ROLLING_LABELS:
            ProcessDataset(dataset, rolling_label, pc_groups_cfg)


def ProcessDataset(dataset, rolling_label, pc_groups_cfg):
    panel_variant = NormalizeDatasetName(dataset)
    panel_path = INDIR / panel_variant / f"panel_{rolling_label}.parquet"
    panel = pd.read_parquet(panel_path)

    analysis_sample = FilterAnalysisSample(panel, dataset)
    repo_scores, metadata = ComputeRepoPCScores(analysis_sample, pc_groups_cfg)

    outdir = OUTDIR / dataset / rolling_label
    outdir.mkdir(parents=True, exist_ok=True)

    continuous_cols = [c for c in repo_scores.columns if c != "repo_name"]
    repo_continuous = repo_scores[["repo_name"] + continuous_cols]
    repo_binary = repo_scores[["repo_name"]].copy()
    for col in continuous_cols:
        median_val = repo_scores[col].median()
        repo_binary[col] = np.where(
            repo_scores[col].isna(), np.nan,
            np.where(repo_scores[col] > median_val, 1.0, 0.0)
        )

    panel_pc = panel.merge(repo_continuous, on="repo_name", how="left")
    panel_pc_median = panel.merge(repo_binary, on="repo_name", how="left")

    SaveData(panel_pc, ["repo_name", "time_index"],
             outdir / "panel_pc.parquet", outdir / "panel_pc.log")
    SaveData(panel_pc_median, ["repo_name", "time_index"],
             outdir / "panel_pc_median.parquet", outdir / "panel_pc_median.log")
    SaveData(metadata, ["group", "var"],
             outdir / "pc1_metadata.csv", outdir / "pc1_metadata.log")


def FilterAnalysisSample(panel, dataset):
    df = panel.copy()
    df = df[df["num_departures"] <= 1]
    if "_exact1" in dataset:
        at_event = df[df["time_index"] == df["quasi_treatment_group"]]
        keep = at_event.loc[
            (at_event["num_important_qualified"] >= 1) &
            (at_event["num_important_qualified"] <= 1),
            "repo_name"
        ].unique()
        df = df[df["repo_name"].isin(keep)]
    elif "_oneQual" in dataset:
        at_event = df[df["time_index"] == df["quasi_treatment_group"]]
        keep = at_event.loc[at_event["num_important_qualified"] >= 1, "repo_name"].unique()
        df = df[df["repo_name"].isin(keep)]
    return df


def ComputeRepoPCScores(panel, pc_groups_cfg):
    """Compute per-repo PC1 scores using baseline (quasi_event_time == -1) values.

    Returns (repo_scores DataFrame, metadata DataFrame).
    metadata has columns: group, var, loading, variance_explained_pc1.
    """
    df = panel.copy()
    df["quasi_event_time"] = df["time_index"] - df["quasi_treatment_group"]
    baseline = df[df["quasi_event_time"] == -1].copy()

    all_group_vars = {v for cfg in pc_groups_cfg.values() for v in cfg["vars"]}
    present_vars = [v for v in all_group_vars if v in baseline.columns]
    na_counts = baseline[present_vars].isna().sum()
    valid_vars = set(v for v in present_vars if na_counts[v] < NA_THRESHOLD)

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
    return result, metadata


def NormalizeDatasetName(dataset):
    for suffix in ("_exact1", "_nuclearWhat", "_defaultWhat", "_oneQual"):
        dataset = dataset.replace(suffix, "")
    return dataset


if __name__ == "__main__":
    Main()
