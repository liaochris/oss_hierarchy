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
from source.lib.python.config_loaders import LoadGlobalSettings, LoadPipelineInputs
from source.lib.python.data_utils import ImputeTimePeriod
from source.lib.python.filesystem_utils import WriteContentHash
from source.lib.python.repo_utils import MakeRepoNameSafe

GLOBAL_SETTINGS = LoadGlobalSettings()
CONFIG          = LoadPipelineInputs()

TIME_PERIOD    = GLOBAL_SETTINGS["time_period_months"]
N_JOBS         = GLOBAL_SETTINGS["n_jobs"]
ROLLING_PERIOD = f"rolling{CONFIG['rolling_periods']['run'][0]}"
VARIANTS       = ["observed", "same_period", "opened_cohort"]

INDIR_ACTIONS = Path("drive/output/derived/action_data/repo_actions")
INDIR_BOT     = Path("output/derived/create_bot_list")
INDIR_PANEL   = Path("output/derived/analysis_panel")
OUTDIR        = Path("drive/output/derived/model_prediction/event_time_member_panel")
LOG_OUTDIR    = Path("output/derived/model_prediction/event_time_member_panel")
HASH_FILE     = Path("output/derived/hashes/event_time_member_panel.txt")


def Main():
    df_bot_list = LoadBotList(INDIR_BOT)
    for combo in Combos():
        ProcessCombo(combo, df_bot_list)
    WriteContentHash(LOG_OUTDIR, HASH_FILE)


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

    repo_event_times = sorted(df_time_map.loc[df_time_map["repo_name"] == repo_name, "quasi_event_time"].unique())

    df_actions = CleanAndFilterData(actions_file, df_time_map, df_bot_list, repo_name)
    df_opens, df_reviews, df_merges_direct, df_merges_after_review = SeparateActionTypes(df_actions)

    variant_inputs = {
        "observed":      (df_opens, df_reviews, df_merges_direct, df_merges_after_review),
        "same_period":   RestrictReviewsMergesToSamePeriod(df_opens, df_reviews, df_merges_direct, df_merges_after_review),
        "opened_cohort": AttributeReviewsMergesToOpeningPeriod(df_opens, df_reviews, df_merges_direct, df_merges_after_review),
    }

    for variant, (df_opens_v, df_reviews_v, df_merges_direct_v, df_merges_after_review_v) in variant_inputs.items():
        df_member_counts = CreateMemberStageCounts(df_opens_v, df_reviews_v, df_merges_direct_v, df_merges_after_review_v, repo_event_times)
        if df_member_counts is None:
            continue
        df_repo_counts = CreateRepoStageCounts(df_opens_v, df_reviews_v, df_merges_direct_v, df_merges_after_review_v, repo_event_times)
        if variant in {"same_period", "opened_cohort"}:
            AssertMergesLeOpened(df_repo_counts, repo_name, variant)
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
    df_without_bots    = df_with_event_time[~df_with_event_time["actor_id"].isin(df_bot_list)]

    return df_without_bots


def SeparateActionTypes(df_actions):
    is_open   = df_actions["type"] == "pull request opened"
    is_review = df_actions["type"].str.startswith("pull request review") | (df_actions["type"] == "pull request comment")
    is_merge  = df_actions["type"] == "pull request merged"

    # HOTFIX: some threads carry "pull request opened" events from multiple distinct actors, which
    # double-counts member opens against the repo's distinct-thread count and pushes sum(prob_open) > 1.
    # Keep only the first opener per thread (earliest created_at). The real fix belongs upstream so every
    # consumer of the action data is deduped consistently -- see HANDOFF.md.
    df_opens   = (df_actions[is_open].sort_values("created_at")
                  .drop_duplicates("thread_number")[["quasi_event_time", "actor_id", "thread_number"]])
    df_reviews = df_actions[is_review][["quasi_event_time", "actor_id", "thread_number"]]
    df_merges  = df_actions[is_merge ][["quasi_event_time", "actor_id", "thread_number"]]

    reviewed_threads = set(df_reviews["thread_number"].unique())
    is_direct = ~df_merges["thread_number"].isin(reviewed_threads)
    df_merges_direct       = df_merges[is_direct]
    df_merges_after_review = df_merges[~is_direct]

    return df_opens, df_reviews, df_merges_direct, df_merges_after_review


def RestrictReviewsMergesToSamePeriod(df_opens, df_reviews, df_merges_direct, df_merges_after_review):
    df_opened_keys = df_opens[["quasi_event_time", "thread_number"]]
    df_reviews_filtered       = df_reviews.merge(df_opened_keys,       on=["quasi_event_time", "thread_number"], how="inner")
    df_merges_direct_filtered = df_merges_direct.merge(df_opened_keys, on=["quasi_event_time", "thread_number"], how="inner")
    df_merges_review_filtered = df_merges_after_review.merge(df_opened_keys, on=["quasi_event_time", "thread_number"], how="inner")
    return df_opens, df_reviews_filtered, df_merges_direct_filtered, df_merges_review_filtered


def AttributeReviewsMergesToOpeningPeriod(df_opens, df_reviews, df_merges_direct, df_merges_after_review):
    df_opening_period = (
        df_opens[["thread_number", "quasi_event_time"]]
        .drop_duplicates("thread_number")
        .rename(columns={"quasi_event_time": "opening_quasi_event_time"})
    )

    def _remap(df):
        return (
            df.merge(df_opening_period, on="thread_number", how="inner")
            .drop(columns="quasi_event_time")
            .rename(columns={"opening_quasi_event_time": "quasi_event_time"})
        )

    return df_opens, _remap(df_reviews), _remap(df_merges_direct), _remap(df_merges_after_review)


def AssertMergesLeOpened(df_repo_counts, repo_name, variant):
    for col in ["repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"]:
        violations = df_repo_counts[df_repo_counts[col] > df_repo_counts["repo_pull_request_opened"]]
        assert violations.empty, (
            f"[{variant}] {repo_name}: {col} > repo_pull_request_opened "
            f"in periods {violations['quasi_event_time'].tolist()}"
        )


def CreateMemberStageCounts(df_opens, df_reviews, df_merges_direct, df_merges_after_review, event_times):
    member_universe = pd.concat([df_opens, df_reviews, df_merges_direct, df_merges_after_review])["actor_id"].unique()
    if len(member_universe) == 0:
        return None

    df_grid = pd.DataFrame(
        list(product(event_times, member_universe)),
        columns=["quasi_event_time", "actor_id"],
    )

    for df_stage, col in [
        (df_opens,               "member_pull_request_opened"),
        (df_reviews,             "member_pull_request_reviewed"),
        (df_merges_direct,       "member_pull_request_merged_direct"),
        (df_merges_after_review, "member_pull_request_merged_after_review"),
    ]:
        df_counts = df_stage.groupby(["quasi_event_time", "actor_id"])["thread_number"].nunique().reset_index(name=col)
        df_grid = df_grid.merge(df_counts, on=["quasi_event_time", "actor_id"], how="left")

    member_cols = [
        "member_pull_request_opened", "member_pull_request_reviewed",
        "member_pull_request_merged_direct", "member_pull_request_merged_after_review",
    ]
    df_grid[member_cols] = df_grid[member_cols].fillna(0).astype(int)
    return df_grid


def CreateRepoStageCounts(df_opens, df_reviews, df_merges_direct, df_merges_after_review, event_times):
    df_repo_time_grid = pd.DataFrame({"quasi_event_time": event_times})
    for df_stage, col in [
        (df_opens,               "repo_pull_request_opened"),
        (df_reviews,             "repo_pull_request_reviewed"),
        (df_merges_direct,       "repo_pull_request_merged_direct"),
        (df_merges_after_review, "repo_pull_request_merged_after_review"),
    ]:
        df_counts = df_stage.groupby("quasi_event_time")["thread_number"].nunique().reset_index(name=col)
        df_repo_time_grid = df_repo_time_grid.merge(df_counts, on="quasi_event_time", how="left")

    repo_cols = [
        "repo_pull_request_opened", "repo_pull_request_reviewed",
        "repo_pull_request_merged_direct", "repo_pull_request_merged_after_review",
    ]
    df_repo_time_grid[repo_cols] = df_repo_time_grid[repo_cols].fillna(0).astype(int)
    return df_repo_time_grid


if __name__ == "__main__":
    Main()
