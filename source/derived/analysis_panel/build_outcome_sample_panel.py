import shutil
from itertools import product
from pathlib import Path

import pandas as pd
from joblib import Parallel, delayed

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

TRIM_OUTCOME    = ANALYSIS_PARAMETERS["trim_outcome"]
TRIM_PROBS      = ANALYSIS_PARAMETERS["trim_probs"]
MAX_EVENT_TIME  = ANALYSIS_PARAMETERS["max_event_time"]
MIN_EVENT_TIME  = -MAX_EVENT_TIME
PRE_PERIOD_EVENT_TIMES = list(range(MIN_EVENT_TIME, 0))

OUTLIERS_KEPT_SUBDIR = "outliers_kept"
BASE_OUTCOMES_REPLACED_BY_SAMPLE = ["pull_request_opened", "pull_request_merged"]
SAMPLE_OUTCOME_COLUMNS = [
    "pull_request_opened", "pull_request_reviewed",
    "pull_request_merged_direct", "pull_request_merged_after_review", "pull_request_merged",
]
SAMPLE_RATE_COLUMNS = ["merge_rate", "review_rate", "direct_merge_rate", "reviewed_merge_rate"]

INDIR_ACTIONS = Path("drive/output/derived/action_data/repo_actions")
INDIR_BOT     = Path("output/derived/create_bot_list")
INDIR_PANEL   = Path("output/derived/analysis_panel")


def Main():
    df_bot_list = LoadBotList(INDIR_BOT)
    for combo in Combos():
        ProcessCombo(combo, df_bot_list)


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


def ProcessCombo(combo, df_bot_list):
    base_panel_dir = (
        INDIR_PANEL / OUTLIERS_KEPT_SUBDIR / combo["importance_type"] / ROLLING_PERIOD
        / combo["qualified_sample"] / combo["control_group"]
    )
    panel_path = base_panel_dir / "panel.parquet"
    if not panel_path.exists():
        return

    base_panel  = pd.read_parquet(panel_path)
    df_time_map = base_panel[["repo_name", "time_period", "quasi_event_time"]].drop_duplicates()
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
    kept_repos = KeptRepos(org_outcomes_by_outcome_sample["opened_cohort"])

    for outcome_sample in ANALYSIS_PANEL_OUTCOME_SAMPLES:
        outcome_sample_panel = BuildOutcomeSamplePanel(base_panel, org_outcomes_by_outcome_sample[outcome_sample], kept_repos)
        WriteOutcomeSamplePanel(outcome_sample_panel, base_panel_dir, outcome_sample, combo)


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


def KeptRepos(opened_cohort_org_outcomes):
    pre_period = opened_cohort_org_outcomes[opened_cohort_org_outcomes["quasi_event_time"].isin(PRE_PERIOD_EVENT_TIMES)]
    pre_period_mean_opened = pre_period.groupby("repo_name")[TRIM_OUTCOME].mean()
    lower_bound, upper_bound = pre_period_mean_opened.quantile(TRIM_PROBS).values
    within_bounds = (pre_period_mean_opened >= lower_bound) & (pre_period_mean_opened <= upper_bound)
    return set(pre_period_mean_opened[within_bounds].index)


def BuildOutcomeSamplePanel(base_panel, org_outcomes, kept_repos):
    skeleton = base_panel.drop(columns=BASE_OUTCOMES_REPLACED_BY_SAMPLE)
    outcome_sample_panel = skeleton.merge(org_outcomes, on=["repo_name", "quasi_event_time"], how="inner")
    outcome_sample_panel = outcome_sample_panel[outcome_sample_panel["repo_name"].isin(kept_repos)].copy()
    assert not outcome_sample_panel[SAMPLE_OUTCOME_COLUMNS].isna().any().any(), "outcome_sample analysis panel has missing outcomes after join"
    return outcome_sample_panel


def WriteOutcomeSamplePanel(outcome_sample_panel, base_panel_dir, outcome_sample, combo):
    outdir = (
        INDIR_PANEL / outcome_sample / combo["importance_type"] / ROLLING_PERIOD
        / combo["qualified_sample"] / combo["control_group"]
    )
    outdir.mkdir(parents=True, exist_ok=True)
    SaveData(outcome_sample_panel, ["repo_name", "time_index"], outdir / "panel.parquet", outdir / "panel.log")
    shutil.copyfile(base_panel_dir / "pc_score_columns.json", outdir / "pc_score_columns.json")


if __name__ == "__main__":
    Main()
