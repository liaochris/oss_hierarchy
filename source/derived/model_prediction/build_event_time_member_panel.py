from itertools import product
from pathlib import Path

import pandas as pd
from joblib import Parallel, delayed

from source.derived.org_outcomes_practices.helpers import LoadBotList
from source.lib.JMSLab.SaveData import SaveData
from source.lib.python.config_loaders import LoadGlobalSettings, LoadPipelineInputs
from source.lib.python.filesystem_utils import CleanDirs, WriteContentHash
from source.lib.python.pull_request_stages import (
    AssertMergesLeOpened,
    AttributeReviewsMergesToOpeningPeriod,
    CreateRepoStageCounts,
    LoadRepoActions,
    RestrictReviewsMergesToSamePeriod,
    SeparatePullRequestStages,
)
from source.lib.python.repo_utils import MakeRepoNameSafe

GLOBAL_SETTINGS = LoadGlobalSettings()
CONFIG          = LoadPipelineInputs()

TIME_PERIOD    = GLOBAL_SETTINGS["time_period_months"]
N_JOBS         = GLOBAL_SETTINGS["n_jobs"]
ROLLING_PERIOD = f"rolling{CONFIG['rolling_periods']['run'][0]}"
ANALYSIS_PANEL_OUTCOME_SAMPLES = CONFIG["outcome_samples"]["run"]

FINAL_SAMPLE_OUTCOME_SAMPLE = "opened_cohort"

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


def CleanComboOutputs(combo):
    for outcome_sample in ANALYSIS_PANEL_OUTCOME_SAMPLES:
        combo_subpath = Path(outcome_sample) / combo["importance_type"] / combo["qualified_sample"] / combo["control_group"]
        CleanDirs([OUTDIR / combo_subpath, CROSS_OUTDIR / combo_subpath, LOG_OUTDIR / combo_subpath])


def ProcessCombo(combo, df_bot_list):
    base_panel_dir = (
        INDIR_PANEL / FINAL_SAMPLE_OUTCOME_SAMPLE / combo["importance_type"] / ROLLING_PERIOD
        / combo["qualified_sample"] / combo["control_group"]
    )
    panel_path = base_panel_dir / "panel.parquet"
    if not panel_path.exists():
        return

    CleanComboOutputs(combo)
    base_panel  = pd.read_parquet(panel_path)
    df_time_map = base_panel[["repo_name", "time_period", "quasi_event_time"]].drop_duplicates()
    repo_names  = df_time_map["repo_name"].unique()

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

    df_actions = LoadRepoActions(actions_file, df_time_map, df_bot_list, repo_name, TIME_PERIOD)
    df_opens, df_reviews, df_merges_direct, df_merges_after_review, solo_review_threads = SeparatePullRequestStages(df_actions)

    outcome_sample_inputs = {
        "observed":      (df_opens, df_reviews, df_merges_direct, df_merges_after_review),
        "same_period":   RestrictReviewsMergesToSamePeriod(df_opens, df_reviews, df_merges_direct, df_merges_after_review),
        "opened_cohort": AttributeReviewsMergesToOpeningPeriod(df_opens, df_reviews, df_merges_direct, df_merges_after_review),
    }

    for outcome_sample in ANALYSIS_PANEL_OUTCOME_SAMPLES:
        outcome_sample_opens, outcome_sample_reviews, outcome_sample_merges_direct, outcome_sample_merges_after_review = outcome_sample_inputs[outcome_sample]
        df_member_counts = CreateMemberStageCounts(outcome_sample_opens, outcome_sample_reviews, outcome_sample_merges_direct, outcome_sample_merges_after_review, repo_event_times)
        if df_member_counts is None:
            continue
        df_repo_counts = CreateRepoStageCounts(outcome_sample_opens, outcome_sample_reviews, outcome_sample_merges_direct, outcome_sample_merges_after_review, repo_event_times)
        if outcome_sample in {"same_period", "opened_cohort"}:
            AssertMergesLeOpened(df_repo_counts, repo_name, outcome_sample)
        df_member_panel = df_member_counts.merge(df_repo_counts, on="quasi_event_time", how="left")
        df_member_panel.insert(0, "repo_name", repo_name)

        outdir     = OUTDIR     / outcome_sample / combo["importance_type"] / combo["qualified_sample"] / combo["control_group"]
        log_outdir = LOG_OUTDIR / outcome_sample / combo["importance_type"] / combo["qualified_sample"] / combo["control_group"]
        outdir.mkdir(parents=True, exist_ok=True)
        log_outdir.mkdir(parents=True, exist_ok=True)

        SaveData(
            df_member_panel,
            ["repo_name", "quasi_event_time", "actor_id"],
            outdir / f"{safe_name}.parquet",
            log_outdir / f"{safe_name}.log",
        )

        df_crosstab = CreateOpenerPairCounts(outcome_sample_opens, outcome_sample_merges_direct, outcome_sample_merges_after_review,
                                             outcome_sample_reviews, solo_review_threads)
        df_crosstab.insert(0, "repo_name", repo_name)
        cross_outdir = CROSS_OUTDIR / outcome_sample / combo["importance_type"] / combo["qualified_sample"] / combo["control_group"]
        cross_outdir.mkdir(parents=True, exist_ok=True)
        SaveData(
            df_crosstab,
            ["repo_name", "quasi_event_time", "opener_id", "other_id"],
            cross_outdir / f"{safe_name}.parquet",
            log_outdir / f"{safe_name}.crossmerge.log",
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


if __name__ == "__main__":
    Main()
