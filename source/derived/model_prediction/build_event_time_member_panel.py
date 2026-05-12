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

TIME_PERIOD = _g["time_period_months"]
N_JOBS      = _g["n_jobs"]
K           = _ap["max_event_time"]
ROLLING_PERIOD = f"rolling{_cfg['rolling_periods']['run'][0]}"

INDIR_ACTIONS = Path("drive/output/derived/action_data/repo_actions")
INDIR_BOT     = Path("output/derived/create_bot_list")
INDIR_PANEL   = Path("output/analysis/data_prep")
OUTDIR        = Path("drive/output/derived/model_prediction/event_time_member_panel")
LOG_OUTDIR    = Path("output/derived/model_prediction/event_time_member_panel")

EVENT_TIMES = list(range(-K, K + 1))


def Main():
    bot_list = LoadBotList(INDIR_BOT)
    for combo in Combos():
        ProcessCombo(combo, bot_list)


def Combos():
    importance_types  = _cfg["importance_types"]["run"]
    qualified_samples = _cfg["qualified_samples"]["run"]
    control_groups    = _cfg["control_groups"]["run"]
    for imp, qs, cg in product(importance_types, qualified_samples, control_groups):
        yield {"importance_type": imp, "qualified_sample": qs, "control_group": cg}


def ProcessCombo(combo, bot_list):
    panel_path = INDIR_PANEL / combo["importance_type"] / ROLLING_PERIOD / combo["qualified_sample"] / combo["control_group"] / "panel.parquet"
    if not panel_path.exists():
        return

    df_panel   = pd.read_parquet(panel_path, columns=["repo_name", "time_period", "quasi_event_time"])
    time_map   = df_panel.drop_duplicates()
    repo_names = time_map["repo_name"].unique()

    outdir     = OUTDIR / combo["importance_type"] / combo["qualified_sample"] / combo["control_group"]
    log_outdir = LOG_OUTDIR / combo["importance_type"] / combo["qualified_sample"] / combo["control_group"]
    outdir.mkdir(parents=True, exist_ok=True)
    log_outdir.mkdir(parents=True, exist_ok=True)

    Parallel(n_jobs=N_JOBS)(
        delayed(ProcessRepo)(repo_name, time_map, bot_list, outdir, log_outdir) 
        for repo_name in repo_names
    )


def ProcessRepo(repo_name, time_map, bot_list, outdir, log_outdir):
    safe_name    = MakeRepoNameSafe(repo_name)
    actions_file = INDIR_ACTIONS / f"{safe_name}.parquet"
    if not actions_file.exists():
        return

    df = CleanAndFilterData(actions_file, time_map, bot_list, repo_name)
    df_opens, df_reviews, df_merges = CreateOpenReviewMergeData(df)

    grid = CreateMemberStageCounts(df_opens, df_reviews, df_merges)
    if grid is None:
        return
    repo_time_grid = CreateRepoStageCounts(df_opens, df_reviews, df_merges)
    grid = grid.merge(repo_time_grid, on="quasi_event_time", how="left")
    grid.insert(0, "repo_name", repo_name)

    SaveData(
        grid,
        ["repo_name", "quasi_event_time", "actor_id"],
        outdir / f"{safe_name}.parquet",
        log_outdir / f"{safe_name}.log",
    )


def CleanAndFilterData(actions_file, time_map, bot_list, repo_name):
    df = pd.read_parquet(actions_file, columns=["thread_number", "created_at", "type", "actor_id"])
    df["actor_id"] = pd.to_numeric(df["actor_id"])
    df = ImputeTimePeriod(df, TIME_PERIOD)

    repo_map = time_map[time_map["repo_name"] == repo_name][["time_period", "quasi_event_time"]]
    df_with_event_time = df.merge(repo_map, on="time_period", how="inner")
    df_within_period = df_with_event_time[df_with_event_time["quasi_event_time"].between(-K, K)]
    df_without_bots = df_within_period[~df_within_period["actor_id"].isin(bot_list)]

    return df_without_bots


def CreateOpenReviewMergeData(df):
    is_open   = df["type"] == "pull request opened"
    is_review = df["type"].str.startswith("pull request review") | (df["type"] == "pull request comment")
    is_merge  = df["type"] == "pull request merged"

    df_opens   = df[is_open][["quasi_event_time", "actor_id", "thread_number"]]
    df_reviews = df[is_review][["quasi_event_time", "actor_id", "thread_number"]]
    df_merges  = df[is_merge][["quasi_event_time", "actor_id", "thread_number"]]
    return df_opens, df_reviews, df_merges
    

def CreateMemberStageCounts(df_opens, df_reviews, df_merges):
    member_universe = pd.concat([df_opens, df_reviews, df_merges])["actor_id"].unique()
    if len(member_universe) == 0:
        return None

    grid = pd.DataFrame(list(product(EVENT_TIMES, member_universe)), columns=["quasi_event_time", "actor_id"])

    for df_stage, col in [
        (df_opens,   "member_pull_request_opened"),
        (df_reviews, "member_pull_request_reviewed"),
        (df_merges,  "member_pull_request_merged"),
    ]:
        counts = df_stage.groupby(["quasi_event_time", "actor_id"])["thread_number"].nunique().reset_index(name=col)
        grid = grid.merge(counts, on=["quasi_event_time", "actor_id"], how="left")

    member_cols = ["member_pull_request_opened", "member_pull_request_reviewed", "member_pull_request_merged"]
    grid[member_cols] = grid[member_cols].fillna(0).astype(int)
    return grid
    

def CreateRepoStageCounts(df_opens, df_reviews, df_merges):
    repo_time_grid = pd.DataFrame({"quasi_event_time": EVENT_TIMES})
    for df_stage, col in [
        (df_opens,   "repo_pull_request_opened"),
        (df_reviews, "repo_pull_request_reviewed"),
        (df_merges,  "repo_pull_request_merged"),
    ]:
        counts = df_stage.groupby("quasi_event_time")["thread_number"].nunique().reset_index(name=col)
        repo_time_grid = repo_time_grid.merge(counts, on="quasi_event_time", how="left")

    repo_cols = ["repo_pull_request_opened", "repo_pull_request_reviewed", "repo_pull_request_merged"]
    repo_time_grid[repo_cols] = repo_time_grid[repo_cols].fillna(0).astype(int)

    return repo_time_grid


if __name__ == "__main__":
    Main()
