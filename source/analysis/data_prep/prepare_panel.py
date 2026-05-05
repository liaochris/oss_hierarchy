import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

from source.lib.helpers import (
    FlattenConfigValues,
    LoadAnalysisParameters,
    LoadOutcomeVariables,
    LoadPipelineInputs,
)
from source.lib.JMSLab.SaveData import SaveData

INDIR = Path("drive/output/derived/org_outcomes_practices/org_panel")
OUTDIR = Path("output/analysis/data_prep")
INDIR_CONFIG = Path("source/analysis/config")
INDIR_LIB = Path("source/lib")

_analysis_params = LoadAnalysisParameters(INDIR_LIB / "project_config.json")

MAX_BASELINE_NA_COUNT = _analysis_params["pc_inclusion_na_threshold"]
MAX_EVENT_TIME = _analysis_params["max_event_time"]
MIN_EVENT_TIME = -MAX_EVENT_TIME

_pipeline_cfg = LoadPipelineInputs(INDIR_LIB / "project_config.json")
IMPORTANCE_TYPES = _pipeline_cfg["importance_types"]["run"]
ROLLING_PERIODS = [f"rolling{p}" for p in _pipeline_cfg["rolling_periods"]["run"]]
QUALIFIED_SAMPLES = _pipeline_cfg["qualified_samples"]["run"]
CONTROL_GROUPS = _pipeline_cfg["control_groups"]["run"]


def Main():
    with open(INDIR_CONFIG / "pc_groups.json", encoding="utf-8") as fh:
        pc_groups_cfg = json.load(fh)
    outcome_cfg = LoadOutcomeVariables(INDIR_LIB / "project_config.json")
    active_outcomes = FlattenConfigValues(outcome_cfg, phases=("run",))
    for importance_type in IMPORTANCE_TYPES:
        for rolling_period in ROLLING_PERIODS:
            ProcessDataset(importance_type, rolling_period, active_outcomes, pc_groups_cfg)


def ProcessDataset(importance_type, rolling_period, active_outcomes, pc_groups_cfg):
    panel = pd.read_parquet(INDIR / importance_type / rolling_period / "panel.parquet")
    print([c for c in panel.columns if "issue" in c or "release" in c])
    print(active_outcomes)

    for qualified_sample in QUALIFIED_SAMPLES:
        for control_group in CONTROL_GROUPS:
            prepared_panel = BuildPreparedSample(panel, active_outcomes, qualified_sample, control_group)
            repo_scores, metadata, excluded_vars = ComputeRepoPCAScores(prepared_panel, pc_groups_cfg)
            SaveSampleOutputs(
                prepared_panel, repo_scores, metadata, excluded_vars,
                OUTDIR / importance_type / rolling_period / qualified_sample / control_group,
            )


def BuildPreparedSample(panel, active_outcomes, qualified_sample, control_group):
    prepared = panel.copy()
    prepared = FilterQualifiedSample(prepared, qualified_sample)
    prepared = FilterControlGroup(prepared, control_group)
    prepared = CreateCompletePanel(prepared, active_outcomes)
    prepared["quasi_event_time"] = prepared["time_index"] - prepared["quasi_treatment_group"]
    return prepared


def FilterQualifiedSample(panel, qualified_sample):
    qualified_counts = ParseQualifiedSample(qualified_sample)
    at_event = panel[panel["time_index"] == panel["quasi_treatment_group"]]
    keep = at_event.loc[at_event["num_important_qualified"].isin(qualified_counts), "repo_name"].unique()
    return panel[panel["repo_name"].isin(keep)].copy()


def FilterControlGroup(panel, control_group):
    if control_group == "nevertreated":
        return panel[panel["num_departures"] <= 1].copy()
    if control_group == "notyettreated":
        return panel[panel["num_departures"] == 1].copy()
    raise ValueError(f"Unsupported control group: {control_group}")


def ParseQualifiedSample(qualified_sample):
    if not qualified_sample.startswith("exact"):
        raise ValueError(f"Unsupported qualified sample: {qualified_sample}")
    parts = qualified_sample.replace("exact", "").lstrip("_").split("_")
    return {int(p) for p in parts}


def CreateCompletePanel(panel, active_outcomes):
    if panel.empty:
        return panel.copy()

    present_outcomes = [o for o in active_outcomes if o in panel.columns]
    keep_repos = set.intersection(*[set(GetOutcomeValidRepos(panel, o)) for o in present_outcomes])
    filtered = panel[panel["repo_name"].isin(keep_repos)].copy()
    event_time = filtered["time_index"] - filtered["quasi_treatment_group"]
    keep_window = (
        filtered.assign(quasi_event_time=event_time)
        .groupby("repo_name")["quasi_event_time"]
        .apply(lambda x: set(range(MIN_EVENT_TIME, MAX_EVENT_TIME + 1)).issubset(set(x.tolist())))
    )
    return filtered[filtered["repo_name"].isin(keep_window[keep_window].index)].copy()


def GetOutcomeValidRepos(panel, outcome):
    baseline_mask = (
        (panel["time_index"] < panel["quasi_treatment_group"])
        & (panel["time_index"] >= panel["quasi_treatment_group"] - MAX_EVENT_TIME)
    )
    baseline_values = panel.loc[baseline_mask, ["repo_name", outcome]]
    baseline_stats = (
        baseline_values
        .groupby("repo_name")[outcome]
        .agg(["mean", "std"])
    )
    df = panel[["repo_name", outcome]].join(baseline_stats, on="repo_name")
    normalized = (df[outcome] - df["mean"]) / df["std"]
    return df.loc[np.isfinite(normalized), "repo_name"].unique()


def ComputeRepoPCAScores(panel, pc_groups_cfg):
    if panel.empty:
        return EmptyPCAOutputs()

    baseline = panel[panel["quasi_event_time"] == -1].copy()
    all_group_vars = {v for cfg in pc_groups_cfg.values() for v in cfg["vars"]}
    present_vars = [v for v in all_group_vars if v in baseline.columns]

    if not present_vars:
        return EmptyPCAOutputs(baseline)

    na_counts = baseline[present_vars].isna().sum()
    valid_vars = {v for v in present_vars if na_counts[v] < MAX_BASELINE_NA_COUNT}
    excluded_rows = [
        {"group": pc_group_name, "var": var_name, "na_count": int(na_counts[var_name])}
        for pc_group_name, cfg in pc_groups_cfg.items()
        for var_name in cfg["vars"]
        if var_name in present_vars and na_counts[var_name] >= MAX_BASELINE_NA_COUNT
    ]
    excluded_vars = (
        pd.DataFrame(excluded_rows)
        if excluded_rows
        else pd.DataFrame(columns=["group", "var", "na_count"])
    )

    if not valid_vars:
        scores, empty_meta, _ = EmptyPCAOutputs(baseline)
        return scores, empty_meta, excluded_vars

    repo_means = baseline.groupby("repo_name")[sorted(valid_vars)].mean()
    result = repo_means.reset_index()[["repo_name"]]
    metadata_rows = []

    for pc_group_name, cfg in pc_groups_cfg.items():
        present = [var_name for var_name in cfg["vars"] if var_name in valid_vars]
        if len(present) < 2:
            continue

        x = repo_means[present].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
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

        loadings = pca.components_[0]
        if cfg["sign_flip"]:
            loadings = -loadings

        for var_name, loading in zip(present, loadings):
            metadata_rows.append({
                "group": pc_group_name, "var": var_name, "loading": loading,
                "variance_explained_principal_component1": pca.explained_variance_ratio_[0] * 100,
            })

        result[f"{pc_group_name}_principal_component1"] = pd.Series(scores, index=result.index)

    metadata = (
        pd.DataFrame(metadata_rows)
        if metadata_rows
        else pd.DataFrame(columns=["group", "var", "loading", "variance_explained_principal_component1"])
    )
    return result, metadata, excluded_vars


def EmptyPCAOutputs(baseline=None):
    scores = (
        baseline[["repo_name"]].drop_duplicates().reset_index(drop=True)
        if baseline is not None
        else pd.DataFrame(columns=["repo_name"])
    )
    return (
        scores,
        pd.DataFrame(columns=["group", "var", "loading", "variance_explained_principal_component1"]),
        pd.DataFrame(columns=["group", "var", "na_count"]),
    )


def SaveSampleOutputs(panel, repo_scores, metadata, excluded_vars, outdir):
    outdir.mkdir(parents=True, exist_ok=True)

    repo_binary        = CoarsenScoresToAboveBelowMedian(repo_scores)
    pc_cols_continuous = [c for c in repo_scores.columns if c != "repo_name"]
    pc_cols_binary     = [c for c in repo_binary.columns  if c != "repo_name"]
    panel_PCA_median   = (
        panel
        .merge(repo_scores, on="repo_name", how="left")
        .merge(repo_binary, on="repo_name", how="left")
    )

    with open(outdir / "pc_columns.json", "w", encoding="utf-8") as fh:
        json.dump({"pc1": pc_cols_continuous, "pc1_binary": pc_cols_binary}, fh, indent=2)

    SaveData(panel, ["repo_name", "time_index"], outdir / "panel.parquet", outdir / "panel.log")
    SaveData(panel_PCA_median, ["repo_name", "time_index"],
             outdir / "panel_PCA_median.parquet", outdir / "panel_PCA_median.log")
    SaveData(metadata, ["group", "var"], outdir / "PCA_metadata.csv", outdir / "PCA_metadata.log")
    SaveData(excluded_vars, ["group", "var"],
             outdir / "PCA_excluded_vars.csv", outdir / "PCA_excluded_vars.log")


def CoarsenScoresToAboveBelowMedian(repo_scores):
    if repo_scores.empty:
        return repo_scores.copy()

    score_cols = [col for col in repo_scores.columns if col != "repo_name"]
    binary = repo_scores[["repo_name"]].copy()
    for col in score_cols:
        median_val = repo_scores[col].median()
        binary[f"{col}_binary"] = np.where(
            repo_scores[col].isna(), np.nan,
            np.where(repo_scores[col] > median_val, 1.0, 0.0),
        )
    return binary


if __name__ == "__main__":
    Main()
