import json
from pathlib import Path
import glob
import pandas as pd
import numpy as np
from joblib import Parallel, delayed
from source.lib.JMSLab.SaveData import SaveData
from source.lib.helpers import JsonSerialize

INDIR = Path("drive/output/derived/graph_structure/graph_degrees")
OUTDIR = Path("output/derived/graph_structure/important_members")
LOGDIR = Path("output/derived/graph_structure")


def Main():
    files = sorted(glob.glob(str(INDIR / "*.parquet")))
    configs = json.load(open("source/lib/importance.json"))

    tasks = MakeTasks(files, configs)

    results = Parallel(n_jobs=-1)(delayed(ProcessFile)(task) for task in tasks)

    results = [r for r in results if r is not None]

    OUTDIR.mkdir(parents=True, exist_ok=True)

    results_by_file = {}
    for r in results:
        results_by_file.setdefault(r['project_name'], []).append(r['df'])

    for project_name, dfs in sorted(results_by_file.items()):
        merged = pd.concat(dfs, ignore_index=True)
        merged = DeduplicateWithLists(merged)
        outpath = OUTDIR / f"{project_name}.csv"
        for col in ["important_actors", "important_qualified_actors", "dropouts_actors"]:
            if col in merged.columns:
                merged[col] = merged[col].apply(JsonSerialize)
        SaveData(
            merged,
            ['time_period', 'mode', 'consecutive_req', 'centrality_col'],
            outpath,
            OUTDIR / f"{project_name}.log"
        )

    summary_rows = [
        {
            'file': r['project_name'],
            'mode': r['mode'],
            'z_thresh': r['z_thresh'],
            'top_k': r['top_k'],
            'consecutive_req': r['consecutive_req'],
            'centrality_col': r['centrality_col'],
            'num_dropouts': r['num_dropouts'],
        }
        for r in results
    ]
    summary_df = pd.DataFrame(summary_rows)
    LOGDIR.mkdir(parents=True, exist_ok=True)
    if not summary_df.empty:
        SaveData(
            summary_df,
            ['file', 'mode', 'consecutive_req', 'centrality_col'],
            LOGDIR / "importance_summary.csv",
            LOGDIR / "importance_summary.log"
        )
    print(f"Done. Saved {len(results_by_file)} repo parquets.")


def MakeTasks(files, configs):
    tasks = []
    for f in files:
        for config_name, config in configs.items():
            tasks.append({
                "file": f,
                "mode": config["mode"],
                "z_thresh": config.get("z_thresh"),
                "top_k": config.get("top_k"),
                "consecutive_req": config["consecutive_req"],
                "centrality_col": config["centrality_col"],
            })
    return tasks


def ProcessFile(task):
    try:
        df = pd.read_parquet(task["file"])
        combined_df = AnalyzeImportance(
            df,
            consecutive_req=task["consecutive_req"],
            centrality_col=task["centrality_col"],
            mode=task["mode"],
            z_thresh=task["z_thresh"],
            top_k=task["top_k"],
        )
        if combined_df.empty:
            return None

        combined_df["time_period"] = combined_df["time_period"].dt.strftime("%Y-%m")
        combined_df["mode"] = task["mode"]
        combined_df["z_thresh"] = task["z_thresh"]
        combined_df["top_k"] = task["top_k"]
        combined_df["consecutive_req"] = task["consecutive_req"]
        combined_df["centrality_col"] = task["centrality_col"]

        num_dropouts = combined_df['num_dropouts'].sum()
        project_name = Path(task["file"]).stem

        return {
            "project_name": project_name,
            "df": combined_df,
            "mode": task["mode"],
            "z_thresh": task["z_thresh"],
            "top_k": task["top_k"],
            "consecutive_req": task["consecutive_req"],
            "centrality_col": task["centrality_col"],
            "num_dropouts": num_dropouts,
        }
    except Exception as e:
        print(f"Error processing {task}: {e}")
        return None


def AnalyzeImportance(df, consecutive_req=3, centrality_col="degree_centrality_z",
                      mode="z_thresh", z_thresh=2, top_k=None):
    df = df.copy()
    df = df.dropna(subset=[centrality_col, "time_period"])
    if df.empty:
        return pd.DataFrame()

    df["time_period"] = (
        pd.to_datetime(df["time_period"]).dt.to_period("6M").dt.to_timestamp()
    )

    periods = GeneratePeriods(df)
    if not periods:
        return pd.DataFrame()

    grouped = {p: g for p, g in df.groupby("time_period", sort=False)}
    present_by_period = {
        p: set(grouped[p]["actor_id"]) if p in grouped else set()
        for p in periods
    }

    _empty = pd.DataFrame(columns=df.columns)
    important_by_period = {}
    for p in periods:
        g = grouped.get(p, _empty)
        if mode == "z_thresh":
            important = set(g.loc[g[centrality_col] > z_thresh, "actor_id"]) if not g.empty else set()
            if not important and not g.empty:
                max_val = g[centrality_col].max()
                important = set(g.loc[g[centrality_col] == max_val, "actor_id"])
            important_by_period[p] = important
        elif mode == "top_k":
            important_by_period[p] = GetTopK(g, centrality_col, top_k) if not g.empty else set()

    timelines = BuildTimelinesFromDict(important_by_period, periods)
    qualified_by_period = TrackQualified(timelines, periods, consecutive_req)

    combined_records = []
    for i, p in enumerate(periods):
        record = {
            "time_period": p,
            "num_important": len(important_by_period[p]),
            "important_actors": sorted(list(important_by_period[p])),
            "num_important_qualified": len(qualified_by_period[p] & present_by_period[p]),
            "important_qualified_actors": sorted(list(qualified_by_period[p] & present_by_period[p])),
        }

        if i < len(periods) - 1:
            candidates = qualified_by_period[p] & present_by_period[p]
            gone = [a for a in candidates if not any(a in present_by_period[fut] for fut in periods[i + 1:])]
            record["num_dropouts"] = len(gone)
            record["dropouts_actors"] = sorted(gone)
        else:
            record["num_dropouts"] = 0
            record["dropouts_actors"] = []

        combined_records.append(record)

    return pd.DataFrame(combined_records)


def GeneratePeriods(df):
    if df.empty or df["time_period"].isna().all():
        return []
    start = pd.to_datetime(df["time_period"]).min()
    end = pd.to_datetime(df["time_period"]).max()
    if pd.isna(start) or pd.isna(end):
        return []
    return pd.date_range(start, end, freq="6MS").drop_duplicates().to_pydatetime().tolist()


def GetTopK(period_df, centrality_col, top_k):
    if period_df.empty:
        return set()
    if len(period_df) <= top_k:
        return set(period_df["actor_id"])
    cutoff = period_df[centrality_col].nlargest(top_k).iloc[-1]
    return set(period_df.loc[period_df[centrality_col] >= cutoff, "actor_id"])


def BuildTimelinesFromDict(important_by_period, periods):
    relevant_actors = set().union(*important_by_period.values()) if any(important_by_period.values()) else set()
    if not relevant_actors:
        return pd.DataFrame(False, index=[], columns=periods)
    timelines = pd.DataFrame(False, index=list(relevant_actors), columns=periods)
    for p, actors in important_by_period.items():
        if actors:
            timelines.loc[list(actors), p] = True
    return timelines


def TrackQualified(timelines, periods, consecutive_req):
    qualified = {p: set() for p in periods}
    binary = timelines.astype(int)

    streaks = (
        binary.T
              .rolling(window=consecutive_req, min_periods=consecutive_req)
              .sum()
              .T
    )

    qualified_mask = streaks >= consecutive_req
    for j, p in enumerate(periods):
        actors = qualified_mask.index[qualified_mask.iloc[:, j]].tolist()
        qualified[p] = set(actors)

    return qualified


def DeduplicateWithLists(df):
    list_cols = ["important_actors", "important_qualified_actors", "dropouts_actors"]
    for col in list_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: tuple(sorted(list(x)))
                if isinstance(x, (list, set, np.ndarray)) else x
            )
    df = df.drop_duplicates()
    for col in list_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: list(x) if isinstance(x, tuple) else x
            )
    return df


if __name__ == "__main__":
    pd.set_option('future.no_silent_downcasting', True)
    Main()
