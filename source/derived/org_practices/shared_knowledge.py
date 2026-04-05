import json, random, shutil
from pathlib import Path
import pandas as pd
from joblib import Parallel, delayed
from source.lib.helpers import ApplyRolling, ConcatStatsByTimePeriod, FilterOnImportant, ImputeTimePeriod, LoadFilteredImportantMembers, LoadGlobals

_globals        = LoadGlobals("source/lib/globals.json")
TIME_PERIOD     = _globals["time_period_months"]
ROLLING_PERIODS = _globals["rolling_periods"]
N_JOBS          = _globals["n_jobs"]
RUN_EXTENSIONS  = _globals["run_extensions"]

with open(Path("source/lib") / "importance.json") as f:
    _importance_params = json.load(f)
PRIMARY_SUBSET    = "all"
EXTENSION_SUBSETS = list(_importance_params.keys())

INDIR_LIB       = Path("source/lib")
INDIR           = Path("drive/output/derived/problem_level_data/repo_actions")
INDIR_BOT       = Path("output/derived/create_bot_list")
INDIR_IMPORTANT = Path("output/derived/graph_structure/important_members")
OUTDIR          = Path("drive/output/derived/org_practices/repo_shared_knowledge")


def Main():
    CleanOutputs()
    bot_list   = LoadBotList()
    repo_files = [f.stem for f in INDIR.glob("*.parquet")]
    random.shuffle(repo_files)
    subsets = [PRIMARY_SUBSET] + (EXTENSION_SUBSETS if RUN_EXTENSIONS else [])
    for subset in subsets:
        Parallel(n_jobs=N_JOBS)(
            delayed(ProcessRepo)(repo_name, subset, bot_list)
            for repo_name in repo_files
        )


def CleanOutputs():
    if not OUTDIR.exists():
        return
    for subset_dir in OUTDIR.iterdir():
        if subset_dir.is_dir():
            target = subset_dir / f"rolling{ROLLING_PERIODS}"
            if target.exists():
                shutil.rmtree(target)


def LoadBotList():
    return pd.to_numeric(pd.read_csv(INDIR_BOT / "bot_list.csv")["actor_id"]).unique().tolist()


def ProcessRepo(repo_name, contributor_subset, bot_list):
    infile = INDIR / f"{repo_name}.parquet"
    if not infile.exists():
        return

    outdir = OUTDIR / contributor_subset / f"rolling{ROLLING_PERIODS}"
    outdir.mkdir(parents=True, exist_ok=True)

    df_actions = pd.read_parquet(infile)
    df_actions["actor_id"] = pd.to_numeric(df_actions["actor_id"])
    df_actions = ImputeTimePeriod(df_actions, TIME_PERIOD)

    if contributor_subset != PRIMARY_SUBSET:
        if not (INDIR_IMPORTANT / f"{repo_name}.csv").exists():
            return
        df_important = LoadFilteredImportantMembers(repo_name, INDIR_IMPORTANT, INDIR_LIB, contributor_subset)
        df_actions   = FilterOnImportant(df_actions, df_important[["time_period", "important_actors"]])
        if df_actions.empty:
            return

    df_actions["actor_id"] = df_actions["actor_id"].astype("Int64")
    df_actions["type_broad"] = df_actions["type"].apply(
        lambda x: "pull request review"
        if x.startswith("pull request review") and x != "pull request review comment" else x
    )
    df_actions["type_issue_pr"] = df_actions["type"].apply(
        lambda x: "issue" if x.startswith("issue") else "pull request"
    )

    stats = PrimaryCalculations(df_actions, bot_list)
    df_combined = ConcatStatsByTimePeriod(*stats)
    if not df_combined.empty:
        df_combined.to_parquet(outdir / f"{repo_name}.parquet")


def PrimaryCalculations(df_actions, bot_list):
    return [
        ActorIssuePRMix(df_actions, bot_list),
        AverageTypeCount(df_actions, bot_list),
    ]


def _ActorIssuePRMixCore(df_actions, bot_list):
    actor_df = df_actions[~df_actions["actor_id"].isin(bot_list)]
    if actor_df.empty:
        return pd.DataFrame()
    actor_types = (
        actor_df.groupby(["repo_name", "actor_id"])["type_issue_pr"]
        .unique()
        .apply(lambda x: "issue_only" if set(x) == {"issue"}
               else "pr_only" if set(x) == {"pull request"} else "issue_and_pr")
    )
    shares = actor_types.reset_index(name="category").groupby("category").size()
    shares = shares / shares.sum()
    return shares.rename(lambda c: f"share_{c}").to_frame().T.reset_index(drop=True)

def ActorIssuePRMix(df_actions, bot_list):
    return ApplyRolling(df_actions, ROLLING_PERIODS, _ActorIssuePRMixCore, bot_list=bot_list, time_period=TIME_PERIOD)


def _AverageTypeCountCore(df_actions, bot_list):
    avg_types = (
        df_actions[~df_actions["type_broad"].str.endswith("reopened") & ~df_actions["actor_id"].isin(bot_list)]
        .groupby(["repo_name", "actor_id"])["type_broad"].nunique()
        .groupby("repo_name").mean()
    )
    return pd.DataFrame({"avg_unique_types": [avg_types.mean()]})

def AverageTypeCount(df_actions, bot_list):
    return ApplyRolling(df_actions, ROLLING_PERIODS, _AverageTypeCountCore, bot_list=bot_list, time_period=TIME_PERIOD)


if __name__ == "__main__":
    Main()
