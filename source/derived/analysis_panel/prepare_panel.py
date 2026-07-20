import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.model_selection import KFold
from sklearn.preprocessing import StandardScaler

from source.derived.analysis_panel.panel_filters import CreateCompletePanel, FilterControlGroup, FilterQualifiedSample
from source.lib.python.config_loaders import LoadAnalysisParameters, LoadPipelineInputs
from source.lib.JMSLab.SaveData import SaveData

INDIR = Path("drive/output/derived/org_outcomes_practices/org_panel")
OUTDIR = Path("output/derived/analysis_panel")
OUTLIERS_KEPT_SUBDIR = "outliers_kept"
INDIR_LIB = Path("source/lib")

ANALYSIS_PARAMETERS = LoadAnalysisParameters()

MAX_BASELINE_NA_COUNT = ANALYSIS_PARAMETERS["pc_inclusion_na_threshold"]
MAX_EVENT_TIME = ANALYSIS_PARAMETERS["max_event_time"]
MIN_EVENT_TIME = -MAX_EVENT_TIME
N_FOLDS = ANALYSIS_PARAMETERS["n_folds"]
SEED = ANALYSIS_PARAMETERS["seed"]

PIPELINE_INPUTS = LoadPipelineInputs()
IMPORTANCE_TYPES  = PIPELINE_INPUTS["importance_types"]["run"]
ROLLING_PERIODS   = [f"rolling{p}" for p in PIPELINE_INPUTS["rolling_periods"]["run"]]
QUALIFIED_SAMPLES = PIPELINE_INPUTS["qualified_samples"]["run"]
CONTROL_GROUPS    = PIPELINE_INPUTS["control_groups"]["run"]

_args            = dict(a.split('=', 1) for a in sys.argv[1:])
IMPORTANCE_TYPE  = _args['CL_IMPORTANCE_TYPE']
ROLLING_PERIOD   = _args['CL_ROLLING_PERIOD']


def Main():
    with open(INDIR_LIB / "pc_groups.json", encoding="utf-8") as fh:
        pc_groups_cfg = json.load(fh)
    active_outcomes = ANALYSIS_PARAMETERS["sample_filter_outcomes"]
    ProcessDataset(IMPORTANCE_TYPE, ROLLING_PERIOD, active_outcomes, pc_groups_cfg)


def ProcessDataset(importance_type, rolling_period, active_outcomes, pc_groups_cfg):
    panel = pd.read_parquet(INDIR / importance_type / rolling_period / "panel.parquet")

    for qualified_sample in QUALIFIED_SAMPLES:
        for control_group in CONTROL_GROUPS:
            prepared_panel = BuildPreparedSample(panel, active_outcomes, qualified_sample, control_group)
            repo_fold = AssignFolds(prepared_panel["repo_name"], N_FOLDS, SEED)
            prepared_panel["fold"] = prepared_panel["repo_name"].map(repo_fold).astype("Int64")
            repo_pc_scores, pc_loading_metadata, pc_excluded_vars = ComputeRepoPCScores(
                prepared_panel, pc_groups_cfg, rolling_period, repo_fold)
            SaveSampleOutputs(
                prepared_panel, repo_pc_scores, pc_loading_metadata, pc_excluded_vars,
                OUTDIR / OUTLIERS_KEPT_SUBDIR / importance_type / rolling_period / qualified_sample / control_group,
            )


def BuildPreparedSample(panel, active_outcomes, qualified_sample, control_group):
    prepared = panel.copy()
    prepared = FilterQualifiedSample(prepared, qualified_sample)
    prepared = FilterControlGroup(prepared, control_group)
    prepared = CreateCompletePanel(prepared, active_outcomes, MIN_EVENT_TIME, MAX_EVENT_TIME)
    prepared["quasi_event_time"] = prepared["time_index"] - prepared["quasi_treatment_group"]
    return prepared


def AssignFolds(repo_names, n_folds, seed):
    repos = np.sort(pd.unique(repo_names))
    kfold = KFold(n_splits=n_folds, shuffle=True, random_state=seed)
    repo_fold = {}
    for fold_label, (_, holdout_idx) in enumerate(kfold.split(repos), start=1):
        for idx in holdout_idx:
            repo_fold[repos[idx]] = fold_label
    return repo_fold


def ComputeRepoPCScores(panel, pc_groups_cfg, rolling_period, repo_fold):
    if panel.empty:
        return EmptyPCScoreOutputs()

    rp = int(rolling_period.replace("rolling", ""))
    if rp == 5:
        pre_data = panel[panel["quasi_event_time"] == -1].copy()
    elif rp == 1:
        pre_data = panel[
            (panel["quasi_event_time"] >= MIN_EVENT_TIME) & (panel["quasi_event_time"] <= -1)
        ].copy()
    else:
        raise ValueError(f"Unsupported rolling_period: {rolling_period}")

    all_group_vars = {v for cfg in pc_groups_cfg.values() for v in cfg["vars"]}
    present_vars = [v for v in all_group_vars if v in pre_data.columns]

    if not present_vars:
        return EmptyPCScoreOutputs(pre_data)

    repo_feature_means = pre_data.groupby("repo_name")[sorted(present_vars)].mean()
    na_counts = repo_feature_means.isna().sum()
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
        scores, empty_meta, _ = EmptyPCScoreOutputs(pre_data)
        return scores, empty_meta, excluded_vars

    repo_feature_means = repo_feature_means[sorted(valid_vars)]
    repo_pc_scores = repo_feature_means.reset_index()[["repo_name"]]
    repo_folds = repo_pc_scores["repo_name"].map(repo_fold).to_numpy()
    metadata_rows = []

    for pc_group_name, cfg in pc_groups_cfg.items():
        present = [var_name for var_name in cfg["vars"] if var_name in valid_vars]
        if len(present) < 2:
            continue

        x = repo_feature_means[present].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
        complete_mask = ~np.isnan(x).any(axis=1)
        if complete_mask.sum() < 2:
            continue

        full_sample_scaler, full_sample_pca = FitGroupPCA(x[complete_mask])
        full_sample_loadings = full_sample_pca.components_[0]

        group_scores = CrossFitGroupScores(x, complete_mask, repo_folds, full_sample_loadings, cfg["sign_flip"])

        reported_loadings = -full_sample_loadings if cfg["sign_flip"] else full_sample_loadings
        for var_name, loading in zip(present, reported_loadings):
            metadata_rows.append({
                "group": pc_group_name, "var": var_name, "loading": loading,
                "variance_explained_pc_score": full_sample_pca.explained_variance_ratio_[0] * 100,
            })

        repo_pc_scores[f"{pc_group_name}_pc_score"] = pd.Series(group_scores, index=repo_pc_scores.index)

    pc_loading_metadata = (
        pd.DataFrame(metadata_rows)
        if metadata_rows
        else pd.DataFrame(columns=["group", "var", "loading", "variance_explained_pc_score"])
    )
    return repo_pc_scores, pc_loading_metadata, excluded_vars


def FitGroupPCA(x_complete):
    scaler = StandardScaler()
    pca = PCA(n_components=1)
    pca.fit(scaler.fit_transform(x_complete))
    return scaler, pca


def SignAdjustment(component, reference_loadings):
    return 1.0 if np.dot(component, reference_loadings) >= 0 else -1.0


def CrossFitGroupScores(x, complete_mask, repo_folds, full_sample_loadings, sign_flip):
    group_scores = np.full(x.shape[0], np.nan)
    orientation = -1.0 if sign_flip else 1.0
    for fold_label in np.unique(repo_folds[~pd.isna(repo_folds)]):
        train_mask = complete_mask & (repo_folds != fold_label)
        holdout_mask = complete_mask & (repo_folds == fold_label)
        if train_mask.sum() < 2 or holdout_mask.sum() == 0:
            continue
        scaler, pca = FitGroupPCA(x[train_mask])
        sign_alignment = SignAdjustment(pca.components_[0], full_sample_loadings)
        holdout_scores = pca.transform(scaler.transform(x[holdout_mask])).ravel()
        group_scores[holdout_mask] = orientation * sign_alignment * holdout_scores
    return group_scores


def EmptyPCScoreOutputs(baseline=None):
    scores = (
        baseline[["repo_name"]].drop_duplicates().reset_index(drop=True)
        if baseline is not None
        else pd.DataFrame(columns=["repo_name"])
    )
    return (
        scores,
        pd.DataFrame(columns=["group", "var", "loading", "variance_explained_pc_score"]),
        pd.DataFrame(columns=["group", "var", "na_count"]),
    )


def SaveSampleOutputs(panel, repo_pc_scores, pc_loading_metadata, pc_excluded_vars, outdir):
    outdir.mkdir(parents=True, exist_ok=True)

    repo_pc_score_binary = CoarsenScoresToAboveBelowMedian(repo_pc_scores)
    pc_score_cols = [c for c in repo_pc_scores.columns if c != "repo_name"]
    pc_score_binary_cols = [c for c in repo_pc_score_binary.columns if c != "repo_name"]
    panel_with_pc_scores = (
        panel
        .merge(repo_pc_scores, on="repo_name", how="left")
        .merge(repo_pc_score_binary, on="repo_name", how="left")
    )

    with open(outdir / "pc_score_columns.json", "w", encoding="utf-8") as fh:
        json.dump({"pc_score": pc_score_cols, "pc_score_binary": pc_score_binary_cols}, fh, indent=2)

    SaveData(panel_with_pc_scores, ["repo_name", "time_index"], outdir / "panel.parquet", outdir / "panel.log")
    SaveData(pc_loading_metadata, ["group", "var"], outdir / "pc_score_metadata.csv", outdir / "pc_score_metadata.log")
    SaveData(pc_excluded_vars, ["group", "var"],
             outdir / "pc_score_excluded_vars.csv", outdir / "pc_score_excluded_vars.log")


def CoarsenScoresToAboveBelowMedian(repo_pc_scores):
    if repo_pc_scores.empty:
        return repo_pc_scores.copy()

    pc_score_cols = [col for col in repo_pc_scores.columns if col != "repo_name"]
    repo_pc_score_binary = repo_pc_scores[["repo_name"]].copy()
    for col in pc_score_cols:
        median_val = repo_pc_scores[col].median()
        repo_pc_score_binary[f"{col}_binary"] = np.where(
            repo_pc_scores[col].isna(), np.nan,
            np.where(repo_pc_scores[col] > median_val, 1.0, 0.0),
        )
    return repo_pc_score_binary


if __name__ == "__main__":
    Main()
