import pandas as pd

from source.lib.python.data_utils import ImputeTimePeriod


def LoadRepoActions(actions_file, df_time_map, df_bot_list, repo_name, time_period):
    df_actions = pd.read_parquet(actions_file, columns=["thread_number", "created_at", "type", "actor_id"])
    df_actions["actor_id"] = pd.to_numeric(df_actions["actor_id"])
    df_actions = ImputeTimePeriod(df_actions, time_period)

    df_repo_time_map = df_time_map[df_time_map["repo_name"] == repo_name][["time_period", "quasi_event_time"]]
    df_actions_with_event_time = df_actions.merge(df_repo_time_map, on="time_period", how="inner")
    df_actions_without_bots    = df_actions_with_event_time[~df_actions_with_event_time["actor_id"].isin(df_bot_list)]

    return df_actions_without_bots


def SeparatePullRequestStages(df_actions):
    is_open   = df_actions["type"] == "pull request opened"
    is_review = df_actions["type"].str.startswith("pull request review") | (df_actions["type"] == "pull request comment")
    is_merge  = df_actions["type"] == "pull request merged"

    first_open_per_thread = (df_actions[is_open].sort_values("created_at")
                             .drop_duplicates("thread_number")[["quasi_event_time", "actor_id", "thread_number"]])
    opener_by_thread = first_open_per_thread.set_index("thread_number")["actor_id"]

    review_actions      = df_actions[is_review]
    reviewer_is_opener  = review_actions["actor_id"] == review_actions["thread_number"].map(opener_by_thread)
    peer_review_actions = review_actions[~reviewer_is_opener]

    first_peer_review_per_thread = (peer_review_actions.sort_values("created_at")
                                    .drop_duplicates("thread_number")[["quasi_event_time", "actor_id", "thread_number"]])
    first_merge_per_thread       = (df_actions[is_merge].sort_values("created_at")
                                    .drop_duplicates("thread_number")[["quasi_event_time", "actor_id", "thread_number"]])

    peer_reviewer_set_by_thread = peer_review_actions.groupby("thread_number")["actor_id"].agg(set)
    solo_review_threads         = peer_reviewer_set_by_thread.index[peer_reviewer_set_by_thread.map(len) == 1]

    peer_reviewed_threads = set(first_peer_review_per_thread["thread_number"].unique())
    is_direct = ~first_merge_per_thread["thread_number"].isin(peer_reviewed_threads)
    df_merges_direct       = first_merge_per_thread[is_direct]
    df_merges_after_review = first_merge_per_thread[~is_direct]

    return first_open_per_thread, first_peer_review_per_thread, df_merges_direct, df_merges_after_review, solo_review_threads


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

    def remap_to_opening_period(df_action_type):
        return (
            df_action_type.merge(df_opening_period, on="thread_number", how="inner")
            .drop(columns="quasi_event_time")
            .rename(columns={"opening_quasi_event_time": "quasi_event_time"})
        )

    return df_opens, remap_to_opening_period(df_reviews), remap_to_opening_period(df_merges_direct), remap_to_opening_period(df_merges_after_review)


def CreateRepoStageCounts(df_opens, df_reviews, df_merges_direct, df_merges_after_review, event_times):
    df_repo_time_grid = pd.DataFrame({"quasi_event_time": event_times})
    for df_action_type, count_column in [
        (df_opens,               "repo_pull_request_opened"),
        (df_reviews,             "repo_pull_request_reviewed"),
        (df_merges_direct,       "repo_pull_request_merged_direct"),
        (df_merges_after_review, "repo_pull_request_merged_after_review"),
    ]:
        stage_counts = df_action_type.groupby("quasi_event_time")["thread_number"].nunique().reset_index(name=count_column)
        df_repo_time_grid = df_repo_time_grid.merge(stage_counts, on="quasi_event_time", how="left")

    repo_count_columns = [
        "repo_pull_request_opened", "repo_pull_request_reviewed",
        "repo_pull_request_merged_direct", "repo_pull_request_merged_after_review",
    ]
    df_repo_time_grid[repo_count_columns] = df_repo_time_grid[repo_count_columns].fillna(0).astype(int)
    return df_repo_time_grid


def AssertMergesLeOpened(df_repo_counts, repo_name, outcome_sample):
    for merge_column in ["repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"]:
        violations = df_repo_counts[df_repo_counts[merge_column] > df_repo_counts["repo_pull_request_opened"]]
        assert violations.empty, (
            f"[{outcome_sample}] {repo_name}: {merge_column} > repo_pull_request_opened "
            f"in periods {violations['quasi_event_time'].tolist()}"
        )
