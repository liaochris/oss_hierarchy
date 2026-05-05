import random
from pathlib import Path
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from source.lib.helpers import CleanDirs, ImputeTimePeriod, LoadGlobalSettings, LoadImportanceSpecifications, LoadGlobals
from source.derived.org_outcomes_practices.helpers import ApplyRolling, ConcatStatsByTimePeriod, FilterOnImportant, LoadBotList, LoadFilteredImportantMembers
from source.lib.JMSLab.SaveData import SaveData

_globals              = LoadGlobalSettings()
_constants            = LoadGlobals("source/derived/org_outcomes_practices/constants.json")
TIME_PERIOD           = _globals["time_period_months"]
ROLLING_PERIODS       = _globals["rolling_periods"]
N_JOBS                = _globals["n_jobs"]
RUN_EXTENSIONS        = _globals["run_extensions"]
PR_REVIEW_DATA_START  = pd.Timestamp(_constants["pr_review_data_start_date"])

_importance_params = LoadImportanceSpecifications()
PRIMARY_SUBSET    = "all"
EXTENSION_SUBSETS = list(_importance_params.keys())

INDIR_LIB       = Path("source/lib")
INDIR           = Path("drive/output/derived/action_data/repo_actions")
INDIR_BOT       = Path("output/derived/create_bot_list")
INDIR_INTERACTIONS = Path("drive/output/derived/graph_structure/interactions")
INDIR_FILE      = Path("drive/output/scrape/governance_data")
INDIR_IMPORTANT = Path("output/derived/graph_structure/important_members")
OUTDIR          = Path("drive/output/derived/org_outcomes_practices/repo_investment_in_new_talent")
LOG_OUTDIR      = Path("output/derived/org_outcomes_practices/repo_investment_in_new_talent")

_DATA_CUTOFF = pd.Timestamp("2024-12-31")


def Main():
    CleanOutputs()
    bot_list   = LoadBotList(INDIR_BOT)
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
            CleanDirs([
                subset_dir / f"rolling{ROLLING_PERIODS}",
                LOG_OUTDIR / subset_dir.name / f"rolling{ROLLING_PERIODS}",
            ])


def ProcessRepo(repo_name, contributor_subset, bot_list):
    infile       = INDIR / f"{repo_name}.parquet"
    infile_graph = INDIR_INTERACTIONS / f"{repo_name}.parquet"

    if not (infile.exists() and infile_graph.exists()):
        return

    outdir = OUTDIR / contributor_subset / f"rolling{ROLLING_PERIODS}"
    outdir.mkdir(parents=True, exist_ok=True)

    df_actions = pd.read_parquet(infile)
    df_actions["actor_id"] = pd.to_numeric(df_actions["actor_id"])
    df_actions = ImputeTimePeriod(df_actions, TIME_PERIOD)

    file_path = INDIR_FILE / f"{repo_name}.parquet"
    if file_path.exists():
        df_files = pd.read_parquet(file_path)
        df_files["date"] = pd.to_datetime(df_files["date"], utc=True)
        df_files = ImputeTimePeriod(df_files.rename(columns={"date": "created_at"}), TIME_PERIOD)
        df_newcomer_files = NewcomerFiles(df_files)
    else:
        df_newcomer_files = pd.DataFrame()

    # GoodFirstIssues is a repo-level metric (issue labels); compute before contributor filtering
    df_gfi = GoodFirstIssues(df_actions)

    if contributor_subset != PRIMARY_SUBSET:
        if not (INDIR_IMPORTANT / f"{repo_name}.csv").exists():
            return
        df_important = LoadFilteredImportantMembers(repo_name, INDIR_IMPORTANT, INDIR_LIB, contributor_subset)
        df_actions   = FilterOnImportant(df_actions, df_important[["time_period", "important_actors"]])
        if df_actions.empty:
            return

    stats = [df_gfi, df_newcomer_files, CalculateNewcomerAdoption(df_actions, bot_list, past_periods=3)]
    df_combined = ForwardFillNewcomerCols(ConcatStatsByTimePeriod(*stats))
    if not df_combined.empty:
        log_dir = LOG_OUTDIR / contributor_subset / f"rolling{ROLLING_PERIODS}"
        log_dir.mkdir(parents=True, exist_ok=True)
        SaveData(df_combined, ["time_period"], outdir / f"{repo_name}.parquet", log_dir / f"{repo_name}.log")


def ForwardFillNewcomerCols(df):
    cols = ["has_contributing_guide", "has_code_of_conduct"]
    for col in cols:
        if col not in df.columns:
            df[col] = np.nan
    df[cols] = df[cols].ffill().fillna(0)
    return df


def GoodFirstIssues(df_actions):
    df_issue = df_actions[df_actions["type"].str.startswith("issue")].copy()
    df_issue["good_first_issue"] = df_issue["labels"].apply(
        lambda labels: any(label.lower() == "good first issue" for label in labels)
    )
    has_gfi = (
        df_issue[df_issue["type"] == "issue opened"].groupby("time_period")["thread_number"]
        .apply(lambda threads: df_issue[
            df_issue["time_period"].isin([threads.name])
            & df_issue["good_first_issue"] & df_issue["thread_number"].isin(threads)
        ].shape[0] > 0)
        .to_frame("has_good_first_issue")
    )
    pct_gfi = (
        df_issue.groupby(["time_period", "thread_number"])["good_first_issue"]
        .max().groupby("time_period").mean().to_frame("pct_good_first_issue")
    )
    if ROLLING_PERIODS > 1:
        pct_gfi["pct_good_first_issue"] = pct_gfi["pct_good_first_issue"].rolling(ROLLING_PERIODS, min_periods=1).mean()
    return has_gfi.join(pct_gfi, how="outer").reset_index()


def FirstFilePresence(df_files, file_type, col_name):
    first_period = df_files.loc[df_files["file_type"] == file_type, "time_period"].min()
    return (df_files.query("time_period == @first_period and file_type == @file_type")
            .assign(**{col_name: 1})[["time_period", col_name]]
            .set_index("time_period").drop_duplicates())


def NewcomerFiles(df_files):
    df = (FirstFilePresence(df_files, "contributing",    "has_contributing_guide")
          .join([FirstFilePresence(df_files, "code_of_conduct", "has_code_of_conduct")], how="outer"))
    return df[df.index <= _DATA_CUTOFF].reset_index()


def CalculateNewcomerAdoption(df_actions, bot_list, past_periods):
    """
    Measure how often newcomers adopt higher-privilege activities (merging/reviewing PRs)
    in the period after they first appeared. Returns weighted adoption rates.
    """
    df_actions = df_actions[~df_actions["actor_id"].isin(bot_list)].copy()
    review_types = {"pull request review commented", "pull request review approved",
                    "pull request review changes requested", "pull request review dismissed"}
    act_map  = {"pull request merged": "pull_request_merged", **{t: "pull_request_review" for t in review_types}}
    first_seen = df_actions.groupby("actor_id")["time_period"].min()
    df_acts  = df_actions[df_actions["type"].isin(act_map.keys())].copy()
    df_acts["activity"] = df_acts["type"].map(act_map)
    df_acts  = df_acts.merge(first_seen.rename("first_seen"), on="actor_id")

    results, periods = [], sorted(df_actions["time_period"].unique())
    for i, period in enumerate(periods):
        row = {"time_period": period}
        if i == 0:
            for act in ["pull_request_merged", "pull_request_review", "any_activity"]:
                row.update({f"{act}_eligible": np.nan, f"{act}_adopters": np.nan, f"{act}_rate": np.nan})
            results.append(row)
            continue

        newcomers = first_seen[
            (first_seen >= period - pd.DateOffset(months=past_periods * TIME_PERIOD)) & (first_seen < period)
        ].index
        if newcomers.empty:
            results.append(row)
            continue

        df_new = df_acts[df_acts["actor_id"].isin(newcomers)]
        act_list = ["pull_request_merged", "pull_request_review"] if period >= PR_REVIEW_DATA_START \
                   else ["pull_request_merged"]
        for act in act_list:
            did_before = df_new[(df_new["time_period"] < period) & (df_new["activity"] == act)]["actor_id"].unique()
            eligible   = set(newcomers) - set(did_before)
            did_now    = df_new[(df_new["time_period"] == period) & (df_new["activity"] == act)]["actor_id"].unique()
            adopters   = set(did_now) & eligible
            row.update({f"{act}_eligible": len(eligible), f"{act}_adopters": len(adopters),
                        f"{act}_rate": len(adopters) / len(eligible) if eligible else np.nan})

        did_before_any = df_new[(df_new["time_period"] < period) & df_new["activity"].notna()]["actor_id"].unique()
        eligible_any   = set(newcomers) - set(did_before_any)
        did_now_any    = df_new[(df_new["time_period"] == period) & df_new["activity"].notna()]["actor_id"].unique()
        adopters_any   = set(did_now_any) & eligible_any
        row.update({"any_activity_eligible": len(eligible_any), "any_activity_adopters": len(adopters_any),
                    "any_activity_rate": len(adopters_any) / len(eligible_any) if eligible_any else np.nan})
        results.append(row)

    df_elg = pd.DataFrame(results)
    if df_elg.empty:
        return pd.DataFrame(columns=["time_period"])
    rate_cols = [c for c in df_elg if c.endswith("_rate")]
    weighted = pd.DataFrame({
        f"{c}_weighted": (
            (df_elg[c] * df_elg[c.replace("_rate", "_eligible")]).rolling(ROLLING_PERIODS, min_periods=1).sum()
            / df_elg[c.replace("_rate", "_eligible")].rolling(ROLLING_PERIODS, min_periods=1).sum()
        ) for c in rate_cols
    })
    return pd.concat([df_elg[["time_period"]], weighted], axis=1)


if __name__ == "__main__":
    Main()
