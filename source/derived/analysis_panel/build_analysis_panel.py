import json
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from sklearn.decomposition import PCA
from sklearn.model_selection import KFold
from sklearn.preprocessing import StandardScaler

from source.derived.analysis_panel.panel_filters import CreateCompletePanel
from source.derived.org_outcomes_practices.helpers import LoadBotList
from source.lib.JMSLab.SaveData import SaveData
from source.lib.python.config_loaders import LoadAnalysisParameters, LoadGlobalSettings, LoadPipelineInputs
from source.lib.python.pull_request_stages import (
    AssertMergesLeOpened,
    AttributeReviewsMergesToOpeningPeriod,
    CreateRepoStageCounts,
    LoadRepoActions,
    RestrictReviewsMergesToSamePeriod,
    SeparatePullRequestStages,
)
from source.lib.python.repo_utils import MakeRepoNameSafe

GLOBAL_SETTINGS     = LoadGlobalSettings()
CONFIG              = LoadPipelineInputs()
ANALYSIS_PARAMETERS = LoadAnalysisParameters()

TIME_PERIOD    = GLOBAL_SETTINGS["time_period_months"]
N_JOBS         = GLOBAL_SETTINGS["n_jobs"]
ROLLING_PERIOD = f"rolling{CONFIG['rolling_periods']['run'][0]}"
ANALYSIS_PANEL_OUTCOME_SAMPLES = CONFIG["outcome_samples"]["run"]

SAMPLE_FILTER_OUTCOMES = ANALYSIS_PARAMETERS["sample_filter_outcomes"]
TRIM_OUTCOME    = ANALYSIS_PARAMETERS["trim_outcome"]
TRIM_PROBS      = ANALYSIS_PARAMETERS["trim_probs"]
MAX_EVENT_TIME  = ANALYSIS_PARAMETERS["max_event_time"]
MIN_EVENT_TIME  = -MAX_EVENT_TIME
PRE_PERIOD_EVENT_TIMES = list(range(MIN_EVENT_TIME, 0))
MAX_BASELINE_NA_COUNT  = ANALYSIS_PARAMETERS["pc_inclusion_na_threshold"]
N_FOLDS = ANALYSIS_PARAMETERS["n_folds"]
SEED    = ANALYSIS_PARAMETERS["seed"]

CANDIDATE_SUBDIR = "candidate"
SAMPLE_OUTCOME_COLUMNS = [
    "pull_request_opened", "pull_request_reviewed",
    "pull_request_merged_direct", "pull_request_merged_after_review", "pull_request_merged",
]
SAMPLE_RATE_COLUMNS = ["merge_rate", "review_rate", "direct_merge_rate", "reviewed_merge_rate"]

INDIR_ACTIONS = Path("drive/output/derived/action_data/repo_actions")
INDIR_BOT     = Path("output/derived/create_bot_list")
INDIR_PANEL   = Path("output/derived/analysis_panel")
INDIR_LIB     = Path("source/lib")


def Main():
    df_bot_list = LoadBotList(INDIR_BOT)
    with open(INDIR_LIB / "pc_groups.json", encoding="utf-8") as fh:
        pc_groups_cfg = json.load(fh)
    for combo in Combos():
        ProcessCombo(combo, df_bot_list, pc_groups_cfg)


def Combos():
    importance_types  = CONFIG["importance_types"]["run"]
    qualified_samples = CONFIG["qualified_samples"]["run"]
    control_groups    = CONFIG["control_groups"]["run"]
    for importance_type, qualified_sample, control_group in product(importance_types, qualified_samples, control_groups):
        yield {
            "importance_type":  importance_type,
            "qualified_sample": qualified_sample,
            "control_group":    control_group,
        }


def ProcessCombo(combo, df_bot_list, pc_groups_cfg):
    candidate_dir = (
        INDIR_PANEL / CANDIDATE_SUBDIR / combo["importance_type"] / ROLLING_PERIOD
        / combo["qualified_sample"] / combo["control_group"]
    )
    candidate_path = candidate_dir / "panel.parquet"
    if not candidate_path.exists():
        return

    candidate   = pd.read_parquet(candidate_path)
    df_time_map = candidate[["repo_name", "time_period", "quasi_event_time"]].drop_duplicates()
    repo_names  = df_time_map["repo_name"].unique()

    repo_stage_counts = Parallel(n_jobs=N_JOBS)(
        delayed(ProcessRepo)(repo_name, df_time_map, df_bot_list)
        for repo_name in repo_names
    )
    repo_stage_counts = [counts for counts in repo_stage_counts if counts]
    if not repo_stage_counts:
        return

    org_outcomes_by_outcome_sample = {
        outcome_sample: AggregateOrgOutcomes(repo_stage_counts, outcome_sample)
        for outcome_sample in ANALYSIS_PANEL_OUTCOME_SAMPLES
    }
    kept_repos = GateAndTrimRepos(candidate, org_outcomes_by_outcome_sample["opened_cohort"])

    kept_candidate = candidate[candidate["repo_name"].isin(kept_repos)].copy()
    repo_fold = AssignFolds(kept_candidate["repo_name"], N_FOLDS, SEED)
    repo_pc_scores, pc_loading_metadata, pc_excluded_vars = ComputeRepoPCScores(
        kept_candidate, pc_groups_cfg, ROLLING_PERIOD, repo_fold)

    for outcome_sample in ANALYSIS_PANEL_OUTCOME_SAMPLES:
        analysis_panel = AssembleAnalysisPanel(
            candidate, org_outcomes_by_outcome_sample[outcome_sample], kept_repos, repo_fold, repo_pc_scores)
        WriteAnalysisPanel(analysis_panel, outcome_sample, combo, repo_pc_scores, pc_loading_metadata, pc_excluded_vars)


def ProcessRepo(repo_name, df_time_map, df_bot_list):
    safe_name    = MakeRepoNameSafe(repo_name)
    actions_file = INDIR_ACTIONS / f"{safe_name}.parquet"
    if not actions_file.exists():
        return {}

    repo_event_times = sorted(df_time_map.loc[df_time_map["repo_name"] == repo_name, "quasi_event_time"].unique())

    df_actions = LoadRepoActions(actions_file, df_time_map, df_bot_list, repo_name, TIME_PERIOD)
    df_opens, df_reviews, df_merges_direct, df_merges_after_review, _ = SeparatePullRequestStages(df_actions)

    outcome_sample_inputs = {
        "observed":      (df_opens, df_reviews, df_merges_direct, df_merges_after_review),
        "same_period":   RestrictReviewsMergesToSamePeriod(df_opens, df_reviews, df_merges_direct, df_merges_after_review),
        "opened_cohort": AttributeReviewsMergesToOpeningPeriod(df_opens, df_reviews, df_merges_direct, df_merges_after_review),
    }

    repo_stage_counts_by_outcome_sample = {}
    for outcome_sample in ANALYSIS_PANEL_OUTCOME_SAMPLES:
        outcome_sample_opens, outcome_sample_reviews, outcome_sample_merges_direct, outcome_sample_merges_after_review = outcome_sample_inputs[outcome_sample]
        df_repo_counts = CreateRepoStageCounts(
            outcome_sample_opens, outcome_sample_reviews,
            outcome_sample_merges_direct, outcome_sample_merges_after_review, repo_event_times)
        if outcome_sample in {"same_period", "opened_cohort"}:
            AssertMergesLeOpened(df_repo_counts, repo_name, outcome_sample)
        repo_stage_counts_by_outcome_sample[outcome_sample] = df_repo_counts.assign(repo_name=repo_name)

    return repo_stage_counts_by_outcome_sample


def AggregateOrgOutcomes(repo_stage_counts, outcome_sample):
    outcome_sample_frames = [counts[outcome_sample] for counts in repo_stage_counts if outcome_sample in counts]
    org_counts = pd.concat(outcome_sample_frames, ignore_index=True)
    org_outcomes = org_counts.assign(
        pull_request_opened              = org_counts["repo_pull_request_opened"],
        pull_request_reviewed            = org_counts["repo_pull_request_reviewed"],
        pull_request_merged_direct       = org_counts["repo_pull_request_merged_direct"],
        pull_request_merged_after_review = org_counts["repo_pull_request_merged_after_review"],
        pull_request_merged              = org_counts["repo_pull_request_merged_direct"] + org_counts["repo_pull_request_merged_after_review"],
    )
    opened   = org_outcomes["pull_request_opened"].where(org_outcomes["pull_request_opened"] != 0)
    reviewed = org_outcomes["pull_request_reviewed"].where(org_outcomes["pull_request_reviewed"] != 0)
    org_outcomes = org_outcomes.assign(
        merge_rate          = org_outcomes["pull_request_merged"] / opened,
        review_rate         = org_outcomes["pull_request_reviewed"] / opened,
        direct_merge_rate   = org_outcomes["pull_request_merged_direct"] / opened,
        reviewed_merge_rate = org_outcomes["pull_request_merged_after_review"] / reviewed,
    )
    return org_outcomes[["repo_name", "quasi_event_time"] + SAMPLE_OUTCOME_COLUMNS + SAMPLE_RATE_COLUMNS]


def GateAndTrimRepos(candidate, opened_cohort_org_outcomes):
    gate_panel = candidate.merge(
        opened_cohort_org_outcomes[["repo_name", "quasi_event_time", "pull_request_opened", "pull_request_merged"]],
        on=["repo_name", "quasi_event_time"], how="inner")
    gated = CreateCompletePanel(gate_panel, SAMPLE_FILTER_OUTCOMES, MIN_EVENT_TIME, MAX_EVENT_TIME)
    return set(gated["repo_name"].unique()) & ScaleTrimmedRepos(opened_cohort_org_outcomes)


def ScaleTrimmedRepos(opened_cohort_org_outcomes):
    pre_period = opened_cohort_org_outcomes[opened_cohort_org_outcomes["quasi_event_time"].isin(PRE_PERIOD_EVENT_TIMES)]
    pre_period_mean_opened = pre_period.groupby("repo_name")[TRIM_OUTCOME].mean()
    lower_bound, upper_bound = pre_period_mean_opened.quantile(TRIM_PROBS).values
    within_bounds = (pre_period_mean_opened >= lower_bound) & (pre_period_mean_opened <= upper_bound)
    return set(pre_period_mean_opened[within_bounds].index)


def AssembleAnalysisPanel(candidate, org_outcomes, kept_repos, repo_fold, repo_pc_scores):
    repo_pc_score_binary = CoarsenScoresToAboveBelowMedian(repo_pc_scores)
    analysis_panel = (
        candidate.merge(org_outcomes, on=["repo_name", "quasi_event_time"], how="inner")
        .merge(repo_pc_scores, on="repo_name", how="left")
        .merge(repo_pc_score_binary, on="repo_name", how="left")
    )
    analysis_panel = analysis_panel[analysis_panel["repo_name"].isin(kept_repos)].copy()
    analysis_panel["fold"] = analysis_panel["repo_name"].map(repo_fold).astype("Int64")
    assert not analysis_panel[SAMPLE_OUTCOME_COLUMNS].isna().any().any(), "analysis panel has missing outcomes after join"
    return analysis_panel


def WriteAnalysisPanel(analysis_panel, outcome_sample, combo, repo_pc_scores, pc_loading_metadata, pc_excluded_vars):
    outdir = (
        INDIR_PANEL / outcome_sample / combo["importance_type"] / ROLLING_PERIOD
        / combo["qualified_sample"] / combo["control_group"]
    )
    outdir.mkdir(parents=True, exist_ok=True)

    pc_score_cols = [c for c in repo_pc_scores.columns if c != "repo_name"]
    pc_score_binary_cols = [f"{c}_binary" for c in pc_score_cols]
    with open(outdir / "pc_score_columns.json", "w", encoding="utf-8") as fh:
        json.dump({"pc_score": pc_score_cols, "pc_score_binary": pc_score_binary_cols}, fh, indent=2)

    SaveData(analysis_panel, ["repo_name", "time_index"], outdir / "panel.parquet", outdir / "panel.log")
    SaveData(pc_loading_metadata, ["group", "var"], outdir / "pc_score_metadata.csv", outdir / "pc_score_metadata.log")
    SaveData(pc_excluded_vars, ["group", "var"], outdir / "pc_score_excluded_vars.csv", outdir / "pc_score_excluded_vars.log")


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
