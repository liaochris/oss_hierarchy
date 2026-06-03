import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

from source.lib.python.config_loaders import FlattenConfigValues, LoadAnalysisParameters, LoadOutcomeVariables, LoadPaperSettings, LoadPipelineInputs
from source.lib.JMSLab.SaveData import SaveData
from source.lib.JMSLab.autofill import GenerateAutofillMacros

INDIR = Path("drive/output/derived/org_outcomes_practices/org_panel")
OUTDIR = Path("output/derived/analysis_panel")
INDIR_LIB = Path("source/lib")
AUTOFILL_OUTDIR = Path("output/autofill")
TABLES_OUTDIR = Path("output/derived/analysis_panel")

_analysis_params = LoadAnalysisParameters()

MAX_BASELINE_NA_COUNT = _analysis_params["pc_inclusion_na_threshold"]
MAX_EVENT_TIME = _analysis_params["max_event_time"]
MIN_EVENT_TIME = -MAX_EVENT_TIME

_pipeline_cfg = LoadPipelineInputs()
IMPORTANCE_TYPES  = _pipeline_cfg["importance_types"]["run"]
ROLLING_PERIODS   = [f"rolling{p}" for p in _pipeline_cfg["rolling_periods"]["run"]]
QUALIFIED_SAMPLES = _pipeline_cfg["qualified_samples"]["run"]
CONTROL_GROUPS    = _pipeline_cfg["control_groups"]["run"]

_paper_settings          = LoadPaperSettings()
PRIMARY_IMPORTANCE_TYPE  = _paper_settings["primary_importance_type"]
PRIMARY_QUALIFIED_SAMPLE = _paper_settings["primary_qualified_sample"]
PRIMARY_ROLLING_LABEL    = _paper_settings["primary_rolling_label"]
PRIMARY_CONTROL_GROUP    = _paper_settings["primary_control_group"]

_args            = dict(a.split('=', 1) for a in sys.argv[1:])
IMPORTANCE_TYPE  = _args['CL_IMPORTANCE_TYPE']
ROLLING_PERIOD   = _args['CL_ROLLING_PERIOD']


def Main():
    with open(INDIR_LIB / "pc_groups.json", encoding="utf-8") as fh:
        pc_groups_cfg = json.load(fh)
    outcome_cfg = LoadOutcomeVariables()
    active_outcomes = FlattenConfigValues(outcome_cfg, phases=("run",))
    ProcessDataset(IMPORTANCE_TYPE, ROLLING_PERIOD, active_outcomes, pc_groups_cfg)


def ProcessDataset(importance_type, rolling_period, active_outcomes, pc_groups_cfg):
    panel = pd.read_parquet(INDIR / importance_type / rolling_period / "panel.parquet")

    for qualified_sample in QUALIFIED_SAMPLES:
        for control_group in CONTROL_GROUPS:
            prepared_panel = BuildPreparedSample(panel, active_outcomes, qualified_sample, control_group)
            repo_pc_scores, pc_loading_metadata, pc_excluded_vars = ComputeRepoPCScores(prepared_panel, pc_groups_cfg, rolling_period)
            SaveSampleOutputs(
                prepared_panel, repo_pc_scores, pc_loading_metadata, pc_excluded_vars,
                OUTDIR / importance_type / rolling_period / qualified_sample / control_group,
            )
            if (importance_type == PRIMARY_IMPORTANCE_TYPE
                    and rolling_period == PRIMARY_ROLLING_LABEL
                    and qualified_sample == PRIMARY_QUALIFIED_SAMPLE
                    and control_group == PRIMARY_CONTROL_GROUP):
                GenerateCanonicalAutofill(prepared_panel, repo_pc_scores, pc_loading_metadata, pc_groups_cfg)


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


def ComputeRepoPCScores(panel, pc_groups_cfg, rolling_period):
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
    metadata_rows = []

    for pc_group_name, cfg in pc_groups_cfg.items():
        present = [var_name for var_name in cfg["vars"] if var_name in valid_vars]
        if len(present) < 2:
            continue

        x = repo_feature_means[present].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
        complete_mask = ~np.isnan(x).any(axis=1)
        if complete_mask.sum() < 2:
            continue

        scaler = StandardScaler()
        x_scaled = scaler.fit_transform(x[complete_mask])
        pca = PCA(n_components=1)
        pc_score = pca.fit_transform(x_scaled).ravel()

        if cfg["sign_flip"]:
            pc_score = -pc_score

        group_scores = np.full(len(repo_feature_means), np.nan)
        group_scores[complete_mask] = pc_score

        loadings = pca.components_[0]
        if cfg["sign_flip"]:
            loadings = -loadings

        for var_name, loading in zip(present, loadings):
            metadata_rows.append({
                "group": pc_group_name, "var": var_name, "loading": loading,
                "variance_explained_pc_score": pca.explained_variance_ratio_[0] * 100,
            })

        repo_pc_scores[f"{pc_group_name}_pc_score"] = pd.Series(group_scores, index=repo_pc_scores.index)

    pc_loading_metadata = (
        pd.DataFrame(metadata_rows)
        if metadata_rows
        else pd.DataFrame(columns=["group", "var", "loading", "variance_explained_pc_score"])
    )
    return repo_pc_scores, pc_loading_metadata, excluded_vars


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


def GenerateCanonicalAutofill(panel, repo_pc_scores, pc_loading_metadata, pc_groups_cfg):
    AUTOFILL_OUTDIR.mkdir(parents=True, exist_ok=True)

    pc_score_cols = [c for c in repo_pc_scores.columns if c.endswith("_pc_score")]
    NumOrgs = str(repo_pc_scores.dropna(subset=pc_score_cols)["repo_name"].nunique())
    SampleStart = str(int(panel["time_period"].min().year))
    SampleEnd = str(int(panel["time_period"].max().year))
    GenerateAutofillMacros(
        ["NumOrgs", "SampleStart", "SampleEnd"],
        "{}",
        str(AUTOFILL_OUTDIR / "panel_autofill.tex"),
    )

    variance = (
        pc_loading_metadata.drop_duplicates("group")
        .set_index("group")["variance_explained_pc_score"]
    )
    CollabVarianceExplained = variance.get("collaboration", float("nan"))
    KnowledgeVarianceExplained = variance.get("knowledge_level", float("nan"))
    DiscussionVarianceExplained = variance.get("discussion_quality", float("nan"))
    TalentVarianceExplained = variance.get("investment_in_new_talent", float("nan"))
    RoutinesVarianceExplained = variance.get("problem_solving_routines", float("nan"))

    total_repos = len(repo_pc_scores)
    binary_scores = CoarsenScoresToAboveBelowMedian(repo_pc_scores)
    def _excl_pct(group_name):
        col = f"{group_name}_pc_score_binary"
        n_missing = binary_scores[col].isna().sum() if col in binary_scores.columns else 0
        return n_missing / total_repos * 100
    CollabPCExcludedPct     = _excl_pct("collaboration")
    KnowledgePCExcludedPct  = _excl_pct("knowledge_level")
    DiscussionPCExcludedPct = _excl_pct("discussion_quality")
    TalentPCExcludedPct     = _excl_pct("investment_in_new_talent")
    RoutinesPCExcludedPct   = _excl_pct("problem_solving_routines")

    GenerateAutofillMacros(
        ["CollabVarianceExplained", "KnowledgeVarianceExplained",
         "DiscussionVarianceExplained", "TalentVarianceExplained",
         "RoutinesVarianceExplained",
         "CollabPCExcludedPct", "KnowledgePCExcludedPct",
         "DiscussionPCExcludedPct", "TalentPCExcludedPct", "RoutinesPCExcludedPct"],
        "{:.1f}",
        str(AUTOFILL_OUTDIR / "pc_autofill.tex"),
    )

    GeneratePCLoadingTablefills(pc_loading_metadata, pc_groups_cfg, TABLES_OUTDIR)


def GeneratePCLoadingTablefills(pc_loading_metadata, pc_groups_cfg, autofill_outdir):
    valid_groups = {
        "collaboration",
        "knowledge_level",
        "discussion_quality",
        "investment_in_new_talent",
        "problem_solving_routines",
    }
    for group_name, cfg in pc_groups_cfg.items():
        if group_name not in valid_groups:
            continue
        group_meta = pc_loading_metadata[pc_loading_metadata["group"] == group_name].copy()
        if group_meta.empty:
            continue
        var_order = cfg["vars"]
        group_meta["_order"] = pd.Categorical(group_meta["var"], categories=var_order, ordered=True)
        group_meta = group_meta.sort_values("_order").drop(columns="_order")
        loadings = -group_meta["loading"] if cfg["sign_flip"] else group_meta["loading"]
        tab_label = f"{group_name}_metrics"
        lines = [f"<tab:{tab_label}>"] + [f"{v:.3f}" for v in loadings]
        autofill_outdir.mkdir(parents=True, exist_ok=True)
        (autofill_outdir / f"pc_loadings_{group_name}.txt").write_text(
            "\n".join(lines) + "\n", encoding="utf-8"
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
