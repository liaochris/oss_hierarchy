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
# The (opener, other-member) cross-tab lives in its own tree, NOT beside the member panels: the event-study R
# step globs the member-panel directory with open_dataset(), so a foreign-schema parquet there would break it.
CROSS_OUTDIR  = Path("drive/output/derived/model_prediction/event_time_member_crossmerge")
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
    df_opens, df_reviews, df_merges_direct, df_merges_after_review, solo_review_threads = SeparateActionTypes(df_actions)

    variant_inputs = {
        "observed":      (df_opens, df_reviews, df_merges_direct, df_merges_after_review),
        "same_period":   RestrictReviewsMergesToSamePeriod(df_opens, df_reviews, df_merges_direct, df_merges_after_review),
        "opened_cohort": AttributeReviewsMergesToOpeningPeriod(df_opens, df_reviews, df_merges_direct, df_merges_after_review),
    }

    for variant, (variant_opens, variant_reviews, variant_merges_direct, variant_merges_after_review) in variant_inputs.items():
        df_member_counts = CreateMemberStageCounts(variant_opens, variant_reviews, variant_merges_direct, variant_merges_after_review, repo_event_times)
        if df_member_counts is None:
            continue
        df_repo_counts = CreateRepoStageCounts(variant_opens, variant_reviews, variant_merges_direct, variant_merges_after_review, repo_event_times)
        if variant in {"same_period", "opened_cohort"}:
            AssertMergesLeOpened(df_repo_counts, repo_name, variant)
        df_member_panel = df_member_counts.merge(df_repo_counts, on="quasi_event_time", how="left")
        df_member_panel.insert(0, "repo_name", repo_name)

        outdir     = OUTDIR     / variant / combo["importance_type"] / combo["qualified_sample"] / combo["control_group"]
        log_outdir = LOG_OUTDIR / variant / combo["importance_type"] / combo["qualified_sample"] / combo["control_group"]
        outdir.mkdir(parents=True, exist_ok=True)
        log_outdir.mkdir(parents=True, exist_ok=True)

        SaveData(
            df_member_panel,
            ["repo_name", "quasi_event_time", "actor_id"],
            outdir / f"{safe_name}.parquet",
            log_outdir / f"{safe_name}.log",
        )

        df_crosstab = CreateOpenerPairCounts(variant_opens, variant_merges_direct, variant_merges_after_review,
                                             variant_reviews, solo_review_threads)
        df_crosstab.insert(0, "repo_name", repo_name)
        cross_outdir = CROSS_OUTDIR / variant / combo["importance_type"] / combo["qualified_sample"] / combo["control_group"]
        cross_outdir.mkdir(parents=True, exist_ok=True)
        SaveData(
            df_crosstab,
            ["repo_name", "quasi_event_time", "opener_id", "other_id"],
            cross_outdir / f"{safe_name}.parquet",
            log_outdir / f"{safe_name}.crossmerge.log",
        )


def CleanAndFilterData(actions_file, df_time_map, df_bot_list, repo_name):
    df_actions = pd.read_parquet(actions_file, columns=["thread_number", "created_at", "type", "actor_id"])
    df_actions["actor_id"] = pd.to_numeric(df_actions["actor_id"])
    df_actions = ImputeTimePeriod(df_actions, TIME_PERIOD)

    df_repo_time_map = df_time_map[df_time_map["repo_name"] == repo_name][["time_period", "quasi_event_time"]]
    df_actions_with_event_time = df_actions.merge(df_repo_time_map, on="time_period", how="inner")
    df_actions_without_bots    = df_actions_with_event_time[~df_actions_with_event_time["actor_id"].isin(df_bot_list)]

    return df_actions_without_bots


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
    opener_by_thread = df_opens.set_index("thread_number")["actor_id"]
    # Review is peer review: a reviewer is a member other than the pull's opener, so a PR author engaging with
    # their own PR does not count as a review. This keeps opening and reviewing disjoint roles on any pull.
    review_actions_all = df_actions[is_review]
    reviewer_is_opener = review_actions_all["actor_id"] == review_actions_all["thread_number"].map(opener_by_thread)
    review_actions = review_actions_all[~reviewer_is_opener]
    # Attribute each thread to its first reviewer and first merger so per-member review and merge
    # probabilities sum to the repo rates, keeping review + direct-merge a valid multinomial over opens.
    df_reviews = (review_actions.sort_values("created_at")
                  .drop_duplicates("thread_number")[["quasi_event_time", "actor_id", "thread_number"]])
    df_merges  = (df_actions[is_merge].sort_values("created_at")
                  .drop_duplicates("thread_number")[["quasi_event_time", "actor_id", "thread_number"]])

    # Reviewer set per thread (excluding the opener); a solo-reviewed thread (single non-opener reviewer) loses
    # its review when that reviewer departs, whereas co-reviewed threads survive through another reviewer.
    reviewers_by_thread = review_actions.groupby("thread_number")["actor_id"].agg(set)
    solo_review_threads = reviewers_by_thread.index[reviewers_by_thread.map(len) == 1]

    reviewed_threads = set(df_reviews["thread_number"].unique())
    is_direct = ~df_merges["thread_number"].isin(reviewed_threads)
    df_merges_direct       = df_merges[is_direct]
    df_merges_after_review = df_merges[~is_direct]

    return df_opens, df_reviews, df_merges_direct, df_merges_after_review, solo_review_threads


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


def AssertMergesLeOpened(df_repo_counts, repo_name, variant):
    for merge_column in ["repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"]:
        violations = df_repo_counts[df_repo_counts[merge_column] > df_repo_counts["repo_pull_request_opened"]]
        assert violations.empty, (
            f"[{variant}] {repo_name}: {merge_column} > repo_pull_request_opened "
            f"in periods {violations['quasi_event_time'].tolist()}"
        )


def AttributeToOpener(df_action, opener_by_thread, opener_dtype):
    opener = df_action["thread_number"].map(opener_by_thread)
    return df_action.assign(actor_id=opener).dropna(subset=["actor_id"]).astype({"actor_id": opener_dtype})


def SplitMergesByOpener(df_merges, opener_by_thread, opener_dtype):
    # Attribute each merge to the pull's opener, splitting on whether the merger IS the opener (self) or not
    # (other). Both are indexed by the opener so their counts share the opened-pulls denominator.
    opener   = df_merges["thread_number"].map(opener_by_thread)
    is_self  = (df_merges["actor_id"] == opener) & opener.notna()
    is_other = (df_merges["actor_id"] != opener) & opener.notna()
    self_by_opener  = df_merges[is_self].assign(actor_id=opener[is_self]).astype({"actor_id": opener_dtype})
    other_by_opener = df_merges[is_other].assign(actor_id=opener[is_other]).astype({"actor_id": opener_dtype})
    return self_by_opener, other_by_opener


def CreateOpenerPairCounts(df_opens, df_merges_direct, df_merges_after_review, df_reviews, solo_review_threads):
    # (opener, other-member) counts of the things a departing member j* does to a surviving opener j's pulls:
    # direct- and after-review other-merges, and solo reviews. The no-reaction counterfactual subtracts exactly
    # these for j* from each surviving opener, rather than approximating j*'s share.
    opener_by_thread = df_opens.drop_duplicates("thread_number").set_index("thread_number")["actor_id"]

    def other_pairs(df_action, count_name):
        opener   = df_action["thread_number"].map(opener_by_thread)
        is_other = (df_action["actor_id"] != opener) & opener.notna()
        pairs    = (df_action[is_other]
                    .assign(opener_id=opener[is_other])
                    .rename(columns={"actor_id": "other_id"}))
        return (pairs.groupby(["quasi_event_time", "opener_id", "other_id"])["thread_number"]
                .nunique().reset_index(name=count_name))

    df_reviews_solo = df_reviews[df_reviews["thread_number"].isin(solo_review_threads)]
    counts = [
        other_pairs(df_merges_direct,       "n_merged_direct_other"),
        other_pairs(df_merges_after_review, "n_merged_after_review_other"),
        other_pairs(df_reviews_solo,        "n_solo_reviewed"),
    ]
    cross = counts[0]
    for right in counts[1:]:
        cross = cross.merge(right, on=["quasi_event_time", "opener_id", "other_id"], how="outer")
    count_cols = ["n_merged_direct_other", "n_merged_after_review_other", "n_solo_reviewed"]
    cross[count_cols] = cross[count_cols].fillna(0).astype(int)
    return cross


def CreateMemberStageCounts(df_opens, df_reviews, df_merges_direct, df_merges_after_review, event_times):
    member_universe = pd.concat([df_opens, df_reviews, df_merges_direct, df_merges_after_review])["actor_id"].unique()
    if len(member_universe) == 0:
        return None

    df_member_time_grid = pd.DataFrame(
        list(product(event_times, member_universe)),
        columns=["quasi_event_time", "actor_id"],
    )

    opener_by_thread = df_opens.drop_duplicates("thread_number").set_index("thread_number")["actor_id"]
    opener_dtype     = df_opens["actor_id"].dtype

    # Every merge is attributed to the pull's OPENER, so each member's merge (and review) rate is the fraction
    # of its own opened pulls reaching the stage -- a conditional probability in [0,1] over the same denominator
    # (the pulls it opened, or the reviewed pulls it opened). A self-merge is by the opener, an other-merge by a
    # different member. Reviewed pulls are likewise attributed to their opener (R^o_j) so review is opener-specific.
    df_reviews_opened                    = AttributeToOpener(df_reviews, opener_by_thread, opener_dtype)
    df_direct_self,       df_direct_other       = SplitMergesByOpener(df_merges_direct, opener_by_thread, opener_dtype)
    df_after_review_self, df_after_review_other = SplitMergesByOpener(df_merges_after_review, opener_by_thread, opener_dtype)

    for df_action_type, count_column in [
        (df_opens,               "member_pull_request_opened"),
        (df_reviews,             "member_pull_request_reviewed"),
        (df_reviews_opened,      "member_pull_request_reviewed_opened"),
        (df_direct_self,         "member_pull_request_merged_self"),
        (df_direct_other,        "member_pull_request_merged_direct_opened_other"),
        (df_after_review_self,   "member_pull_request_merged_after_review_opened_self"),
        (df_after_review_other,  "member_pull_request_merged_after_review_opened_other"),
    ]:
        stage_counts = df_action_type.groupby(["quasi_event_time", "actor_id"])["thread_number"].nunique().reset_index(name=count_column)
        df_member_time_grid = df_member_time_grid.merge(stage_counts, on=["quasi_event_time", "actor_id"], how="left")

    member_count_columns = [
        "member_pull_request_opened", "member_pull_request_reviewed",
        "member_pull_request_reviewed_opened", "member_pull_request_merged_self",
        "member_pull_request_merged_direct_opened_other",
        "member_pull_request_merged_after_review_opened_self",
        "member_pull_request_merged_after_review_opened_other",
    ]
    df_member_time_grid[member_count_columns] = df_member_time_grid[member_count_columns].fillna(0).astype(int)
    return df_member_time_grid


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


if __name__ == "__main__":
    Main()
