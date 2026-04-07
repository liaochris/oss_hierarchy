import json, random
from pathlib import Path
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from source.lib.helpers import ApplyRolling, ConcatStatsByTimePeriod, FilterOnImportant, ImputeTimePeriod, LoadFilteredImportantMembers, LoadGlobals
from source.lib.JMSLab.SaveData import SaveData

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
INDIR_GRAPH     = Path("drive/output/derived/graph_structure/interactions")
INDIR_IMPORTANT = Path("output/derived/graph_structure/important_members")
OUTDIR          = Path("drive/output/derived/org_outcomes_practices/repo_collaboration")
LOG_OUTDIR      = Path("output/derived/org_outcomes_practices/repo_collaboration")


def Main():
    CleanOutputs()
    bot_list   = LoadBotList()
    repo_files = [f.stem for f in INDIR.glob("*.parquet") if not f.stem.startswith("._")]
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
            log_target = LOG_OUTDIR / subset_dir.name / f"rolling{ROLLING_PERIODS}"
            target.mkdir(parents=True, exist_ok=True)
            log_target.mkdir(parents=True, exist_ok=True)
            for f in list(target.glob("*.parquet")) + list(log_target.glob("*.log")):
                f.unlink(missing_ok=True)


def LoadBotList():
    return pd.to_numeric(pd.read_csv(INDIR_BOT / "bot_list.csv")["actor_id"]).unique().tolist()


def ProcessRepo(repo_name, contributor_subset, bot_list):
    infile       = INDIR / f"{repo_name}.parquet"
    infile_graph = INDIR_GRAPH / f"{repo_name}.parquet"

    if not (infile.exists() and infile_graph.exists()):
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

    df_active_members = (
        df_actions[["time_period", "actor_id"]].drop_duplicates()
        .groupby("time_period")["actor_id"].count()
        .reset_index().rename(columns={"actor_id": "num_active_members"})
    )
    log_dir = LOG_OUTDIR / contributor_subset / f"rolling{ROLLING_PERIODS}"
    log_dir.mkdir(parents=True, exist_ok=True)
    members_df = df_actions[["time_period", "actor_id"]].drop_duplicates()
    SaveData(members_df, ["time_period", "actor_id"], outdir / f"{repo_name}_members.parquet", log_dir / f"{repo_name}_members.log")

    stats = PrimaryCalculations(df_actions, bot_list)
    df_combined = ConcatStatsByTimePeriod(*stats, df_active_members)
    if not df_combined.empty:
        SaveData(df_combined, ["time_period"], outdir / f"{repo_name}.parquet", log_dir / f"{repo_name}.log")


def PrimaryCalculations(df_actions, bot_list):
    return [
        CalculateMemberStatsPerProblem(df_actions, bot_list),
        CalculateProjectHHI(df_actions),
        CalculateProjectProblemHHI(df_actions),
    ]


def _MemberStatsPerProblemCore(df_actions, bot_list):
    df_problem = (
        df_actions[~df_actions["actor_id"].isin(bot_list)]
        .groupby("thread_number", as_index=False)
        .agg(member_count=("actor_id", "nunique"))
    )
    return pd.DataFrame({
        "avg_members_per_problem": [df_problem["member_count"].mean()],
        "pct_members_multiple":    [(df_problem["member_count"] > 1).mean()],
    })

def CalculateMemberStatsPerProblem(df_actions, bot_list):
    return ApplyRolling(df_actions, ROLLING_PERIODS, _MemberStatsPerProblemCore, bot_list=bot_list, time_period=TIME_PERIOD)


def _ProjectHHICore(df_actions):
    def _activity_shares(df, type_col="type_broad"):
        counts = df.groupby(["time_period", "actor_id", type_col]).size().rename("count")
        totals = df.groupby(["time_period", type_col]).size().rename("total_count")
        return (counts.reset_index()
                .merge(totals.reset_index(), on=["time_period", type_col])
                .assign(share=lambda d: d["count"] / d["total_count"]))

    df_shares = _activity_shares(df_actions[~df_actions["type_broad"].str.endswith("reopened")])
    df_comments = df_actions[df_actions["type_broad"].str.endswith("comment")].assign(type_broad="discussion comment")
    df_shares = pd.concat([df_shares, _activity_shares(df_comments)])
    df_shares["share_sq"] = df_shares["share"] ** 2
    df_hhi = (df_shares.groupby(["time_period", "type_broad"])
              .agg(hhi=("share_sq", "sum"), total_count=("total_count", "first")).reset_index())
    agg = (df_hhi.groupby("type_broad", group_keys=False)
           .agg(weighted_hhi=("hhi", lambda x: np.average(x, weights=df_hhi.loc[x.index, "total_count"]))))
    return agg.T.rename(columns=lambda c: f"proj_hhi_{c.replace(' ', '_')}").reset_index(drop=True)

def CalculateProjectHHI(df_actions):
    return ApplyRolling(df_actions, ROLLING_PERIODS, _ProjectHHICore, time_period=TIME_PERIOD)


def _ProjectProblemHHICore(df_actions):
    def _problem_shares(df, type_col="type_broad"):
        counts = df.groupby(["repo_name", "thread_number", "actor_id", type_col]).size().rename("count")
        totals = df.groupby(["repo_name", "thread_number", type_col]).size().rename("total_count")
        return (counts.reset_index()
                .merge(totals.reset_index(), on=["repo_name", "thread_number", type_col])
                .assign(share=lambda d: d["count"] / d["total_count"]))

    df_disc = df_actions[df_actions["type_broad"].str.endswith(("comment", "review"))]
    df_shares = _problem_shares(df_disc)
    df_comments = df_actions[df_actions["type_broad"].str.endswith("comment")].assign(type_broad="discussion comment")
    df_shares = pd.concat([df_shares, _problem_shares(df_comments)])
    df_probs = (df_shares.assign(share_sq=df_shares["share"] ** 2)
                .groupby(["repo_name", "thread_number", "type_broad"])
                .agg(problem_hhi=("share_sq", "sum"), total_count=("total_count", "first")).reset_index())
    agg = (df_probs.groupby("type_broad", group_keys=False)
           .agg(weighted_prob_hhi=("problem_hhi",
                                   lambda x: np.average(x, weights=df_probs.loc[x.index, "total_count"]))))
    return agg.T.rename(columns=lambda c: f"proj_prob_hhi_{c.replace(' ', '_')}").reset_index(drop=True)

def CalculateProjectProblemHHI(df_actions):
    return ApplyRolling(df_actions, ROLLING_PERIODS, _ProjectProblemHHICore, time_period=TIME_PERIOD)


if __name__ == "__main__":
    Main()
