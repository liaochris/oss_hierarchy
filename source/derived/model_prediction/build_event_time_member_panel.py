# Three variants of the event-time member panel are produced:
#   observed:      reviews/merges counted over all pulls active in a given period,
#                  regardless of when those pulls were opened (original construction)
#   same_period:   reviews/merges restricted to pulls opened AND reviewed/merged
#                  within the same period t (consistent denominator, strict same-period match)
#   opened_cohort: reviews/merges attributed to the period the pull was opened,
#                  accumulating activity across any subsequent period
#                  (consistent denominator, full downstream activity captured)

from itertools import product
from pathlib import Path

import pandas as pd
from joblib import Parallel, delayed

from source.derived.org_outcomes_practices.helpers import LoadBotList
from source.lib.JMSLab.SaveData import SaveData
from source.lib.python.config_loaders import LoadGlobalSettings, LoadPipelineInputs, LoadAnalysisParameters
from source.lib.python.data_utils import ImputeTimePeriod
from source.lib.python.repo_utils import MakeRepoNameSafe

_g   = LoadGlobalSettings()
_cfg = LoadPipelineInputs()
_ap  = LoadAnalysisParameters()

TIME_PERIOD    = _g["time_period_months"]
N_JOBS         = _g["n_jobs"]
K              = _ap["max_event_time"]
ROLLING_PERIOD = f"rolling{_cfg['rolling_periods']['run'][0]}"
VARIANTS       = ["observed", "same_period", "opened_cohort"]

INDIR_ACTIONS = Path("drive/output/derived/action_data/repo_actions")
INDIR_BOT     = Path("output/derived/create_bot_list")
INDIR_PANEL   = Path("output/derived/analysis_panel")
OUTDIR        = Path("drive/output/derived/model_prediction/event_time_member_panel")
LOG_OUTDIR    = Path("output/derived/model_prediction/event_time_member_panel")

EVENT_TIMES = list(range(-K, K + 1))


def Main():
    df_bot_list = LoadBotList(INDIR_BOT)
    for combo in Combos():
        ProcessCombo(combo, df_bot_list)


def Combos():
    importance_types  = _cfg["importance_types"]["run"]
    qualified_samples = _cfg["qualified_samples"]["run"]
    control_groups    = _cfg["control_groups"]["run"]
    for importance_type, qualified_sample, control_group in product(importance_types, qualified_samples, control_groups):
        yield {
            "importance_type":  importance_type,
            "qualified_sample": qualified_sample,
            "control_group":    control_group,
        }


def ProcessCombo(combo, df_bot_list):
    panel_path = (
        INDIR_PANEL / combo["importance_type"] / ROLLING_PERIOD
        / combo["qualified_sample"] / combo["control_group"] / "panel.parquet"
    )
    if not panel_path.exists():
        return

    df_panel   = pd.read_parquet(panel_path, columns=["repo_name", "time_period", "quasi_event_time"])
    df_time_map = df_panel.drop_duplicates()
    repo_names = df_time_map["repo_name"].unique()

    Parallel(n_jobs=N_JOBS)(
        delayed(ProcessRepo)(repo_name, df_time_map, df_bot_list, combo)
        for repo_name in repo_names
    )


def ProcessRepo(repo_name, df_time_map, df_bot_list, combo):
    safe_name    = MakeRepoNameSafe(repo_name)
    actions_file = INDIR_ACTIONS / f"{safe_name}.parquet"
    if not actions_file.exists():
        return

    df_actions = CleanAndFilterData(actions_file, df_time_map, df_bot_list, repo_name)
    df_opens, df_reviews, df_merges = SeparateActionTypes(df_actions)

    variant_inputs = {
        "observed":      (df_opens, df_reviews, df_merges),
        "same_period":   RestrictReviewsMergesToSamePeriod(df_opens, df_reviews, df_merges),
        "opened_cohort": AttributeReviewsMergesToOpeningPeriod(df_opens, df_reviews, df_merges),
    }

    for variant, (df_opens_v, df_reviews_v, df_merges_v) in variant_inputs.items():
        df_member_counts = CreateMemberStageCounts(df_opens_v, df_reviews_v, df_merges_v)
        if df_member_counts is None:
            continue
        df_repo_counts = CreateRepoStageCounts(df_opens_v, df_reviews_v, df_merges_v)
        if variant in {"same_period", "opened_cohort"}:
            AssertReviewMergeLeOpened(df_repo_counts, repo_name, variant)
        df_grid = df_member_counts.merge(df_repo_counts, on="quasi_event_time", how="left")
        df_grid.insert(0, "repo_name", repo_name)

        outdir     = OUTDIR     / variant / combo["importance_type"] / combo["qualified_sample"] / combo["control_group"]
        log_outdir = LOG_OUTDIR / variant / combo["importance_type"] / combo["qualified_sample"] / combo["control_group"]
        outdir.mkdir(parents=True, exist_ok=True)
        log_outdir.mkdir(parents=True, exist_ok=True)

        SaveData(
            df_grid,
            ["repo_name", "quasi_event_time", "actor_id"],
            outdir / f"{safe_name}.parquet",
            log_outdir / f"{safe_name}.log",
        )


def CleanAndFilterData(actions_file, df_time_map, df_bot_list, repo_name):
    df_actions = pd.read_parquet(actions_file, columns=["thread_number", "created_at", "type", "actor_id"])
    df_actions["actor_id"] = pd.to_numeric(df_actions["actor_id"])
    df_actions = ImputeTimePeriod(df_actions, TIME_PERIOD)

    df_repo_map = df_time_map[df_time_map["repo_name"] == repo_name][["time_period", "quasi_event_time"]]
    df_with_event_time = df_actions.merge(df_repo_map, on="time_period", how="inner")
    df_within_window   = df_with_event_time[df_with_event_time["quasi_event_time"].between(-K, K)]
    df_without_bots    = df_within_window[~df_within_window["actor_id"].isin(df_bot_list)]

    return df_without_bots


def SeparateActionTypes(df_actions):
    is_open   = df_actions["type"] == "pull request opened"
    is_review = df_actions["type"].str.startswith("pull request review") | (df_actions["type"] == "pull request comment")
    is_merge  = df_actions["type"] == "pull request merged"

    df_opens   = df_actions[is_open  ][["quasi_event_time", "actor_id", "thread_number"]]
    df_reviews = df_actions[is_review][["quasi_event_time", "actor_id", "thread_number"]]
    df_merges  = df_actions[is_merge ][["quasi_event_time", "actor_id", "thread_number"]]
    return df_opens, df_reviews, df_merges


def RestrictReviewsMergesToSamePeriod(df_opens, df_reviews, df_merges):
    df_opened_keys     = df_opens[["quasi_event_time", "thread_number"]]
    df_reviews_filtered = df_reviews.merge(df_opened_keys, on=["quasi_event_time", "thread_number"], how="inner")
    df_merges_filtered  = df_merges.merge(df_opened_keys,  on=["quasi_event_time", "thread_number"], how="inner")
    return df_opens, df_reviews_filtered, df_merges_filtered


def AttributeReviewsMergesToOpeningPeriod(df_opens, df_reviews, df_merges):
    df_opening_period = (
        df_opens[["thread_number", "quasi_event_time"]]
        .drop_duplicates("thread_number")
        .rename(columns={"quasi_event_time": "opening_quasi_event_time"})
    )

    df_reviews_cohort = (
        df_reviews
        .merge(df_opening_period, on="thread_number", how="inner")
        .drop(columns="quasi_event_time")
        .rename(columns={"opening_quasi_event_time": "quasi_event_time"})
    )
    df_merges_cohort = (
        df_merges
        .merge(df_opening_period, on="thread_number", how="inner")
        .drop(columns="quasi_event_time")
        .rename(columns={"opening_quasi_event_time": "quasi_event_time"})
    )
    return df_opens, df_reviews_cohort, df_merges_cohort


def AssertReviewMergeLeOpened(df_repo_counts, repo_name, variant):
    violations_review = df_repo_counts[
        df_repo_counts["repo_pull_request_reviewed"] > df_repo_counts["repo_pull_request_opened"]
    ]
    violations_merge = df_repo_counts[
        df_repo_counts["repo_pull_request_merged"] > df_repo_counts["repo_pull_request_opened"]
    ]
    assert violations_review.empty, (
        f"[{variant}] {repo_name}: repo_pull_request_reviewed > repo_pull_request_opened "
        f"in periods {violations_review['quasi_event_time'].tolist()}"
    )
    assert violations_merge.empty, (
        f"[{variant}] {repo_name}: repo_pull_request_merged > repo_pull_request_opened "
        f"in periods {violations_merge['quasi_event_time'].tolist()}"
    )


def CreateMemberStageCounts(df_opens, df_reviews, df_merges):
    member_universe = pd.concat([df_opens, df_reviews, df_merges])["actor_id"].unique()
    if len(member_universe) == 0:
        return None

    df_grid = pd.DataFrame(
        list(product(EVENT_TIMES, member_universe)),
        columns=["quasi_event_time", "actor_id"],
    )

    for df_stage, col in [
        (df_opens,   "member_pull_request_opened"),
        (df_reviews, "member_pull_request_reviewed"),
        (df_merges,  "member_pull_request_merged"),
    ]:
        df_counts = df_stage.groupby(["quasi_event_time", "actor_id"])["thread_number"].nunique().reset_index(name=col)
        df_grid = df_grid.merge(df_counts, on=["quasi_event_time", "actor_id"], how="left")

    member_cols = ["member_pull_request_opened", "member_pull_request_reviewed", "member_pull_request_merged"]
    df_grid[member_cols] = df_grid[member_cols].fillna(0).astype(int)
    return df_grid


def CreateRepoStageCounts(df_opens, df_reviews, df_merges):
    df_repo_time_grid = pd.DataFrame({"quasi_event_time": EVENT_TIMES})
    for df_stage, col in [
        (df_opens,   "repo_pull_request_opened"),
        (df_reviews, "repo_pull_request_reviewed"),
        (df_merges,  "repo_pull_request_merged"),
    ]:
        df_counts = df_stage.groupby("quasi_event_time")["thread_number"].nunique().reset_index(name=col)
        df_repo_time_grid = df_repo_time_grid.merge(df_counts, on="quasi_event_time", how="left")

    repo_cols = ["repo_pull_request_opened", "repo_pull_request_reviewed", "repo_pull_request_merged"]
    df_repo_time_grid[repo_cols] = df_repo_time_grid[repo_cols].fillna(0).astype(int)
    return df_repo_time_grid


if __name__ == "__main__":
    Main()
