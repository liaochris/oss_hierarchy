
from pathlib import Path
import pandas as pd
import glob
import os
from multiprocessing import Pool

# in a time_period, classify someone as important if they're degree_centrality_z exceeds 2 (call this z_thresh)
# now, identify the number of people in the repo who are "important" for at least 3 consecutive periods (call this consecutive_req)
# note that each time_period is a 6 month interval and it has to be consectuive 6 month intervals (there may be missing periods)
# now, list the number of people in each period who are important, and who have been important according to the consecutive_req (as of that period), and are presen tin that period
# finally, list the number of people who were important (according to the consecutive_req) as of last period, and are no longer in the repo, and will never return
# eople are actor_id
# do this for all periods


def GeneratePeriods(df: pd.DataFrame) -> list:
    """Generate a complete 6-month period list between min and max."""
    start = pd.to_datetime(df['time_period']).min()
    end = pd.to_datetime(df['time_period']).max()
    return pd.date_range(start, end, freq="6MS").to_pydatetime().tolist()

def GetTopK(period_df: pd.DataFrame, centrality_col: str, top_k: int) -> set:
    """Return top_k actors by centrality, including ties at the cutoff."""
    if period_df.empty:
        return set()

    sorted_df = period_df.sort_values(centrality_col, ascending=False)

    # if fewer than top_k rows, just take all
    if len(sorted_df) <= top_k:
        return set(sorted_df['actor_id'])

    # find the cutoff score at rank top_k
    cutoff = sorted_df.iloc[top_k - 1][centrality_col]

    # include everyone >= cutoff (accounts for ties correctly)
    return set(sorted_df.loc[sorted_df[centrality_col] >= cutoff, 'actor_id'])


def BuildTimelines(df: pd.DataFrame, periods: list, centrality_col: str, mode: str, z_thresh: float = None, top_k: int = None) -> pd.DataFrame:
    """Pivot into actor × period table of important flags (bool) depending on mode."""
    df = df.copy()
    df['time_period'] = pd.to_datetime(df['time_period'])

    if mode == "z_thresh":
        df['important'] = df[centrality_col] > z_thresh
        timelines = (
            df.pivot_table(index="actor_id", columns="time_period", values="important", aggfunc="max")
            .reindex(columns=periods)
            .fillna(False)
            .astype(bool)
        )
        return timelines

    elif mode == "top_k":
        important_flags = []
        for p in periods:
            period_df = df[df['time_period'] == p]
            if period_df.empty:
                continue

            top_ids = GetTopK(period_df, centrality_col, top_k)
            flags = pd.DataFrame({"actor_id": list(top_ids), p: True})
            important_flags.append(flags)

        if important_flags:
            flags_df = pd.concat(important_flags).drop_duplicates().set_index("actor_id")
        else:
            flags_df = pd.DataFrame(index=df["actor_id"].unique())

        timelines = (
            flags_df.reindex(index=df["actor_id"].unique(), columns=periods, fill_value=False)
            .astype(bool)
        )
        return timelines

    else:
        raise ValueError("mode must be either 'z_thresh' or 'top_k'")

def TrackQualified(timelines: pd.DataFrame, periods: list, consecutive_req: int) -> dict:
    """Actors are qualified only in periods where their streak ≥ consecutive_req is active."""
    qualified = {p: set() for p in periods}
    binary = timelines.astype(int)

    streaks = (
        binary
        .rolling(window=consecutive_req, axis=1, min_periods=consecutive_req)
        .sum()
    )

    qualified_mask = streaks >= consecutive_req
    for j, p in enumerate(periods):
        actors = qualified_mask.index[qualified_mask.iloc[:, j]].tolist()
        qualified[p] = set(actors)

    return qualified

def AnalyzeImportance(
    df: pd.DataFrame, 
    consecutive_req: int = 3, 
    centrality_col: str = "degree_centrality_z",
    mode: str = "z_thresh", 
    z_thresh: float = 2, 
    top_k: int = None
) -> pd.DataFrame:
    """
    Classify actors as important using one of two modes:
    - mode="z_thresh": z > z_thresh (or top centrality if none).
    - mode="top_k": top_k actors by z-score (ties included).
    Track important_qualified streaks and compute dropouts.
    """
    df = df.copy()
    df = df.dropna(subset=[centrality_col])
    df['time_period'] = pd.to_datetime(df['time_period'])

    periods = GeneratePeriods(df)

    important_by_period = {}
    present_by_period = {}
    for p in periods:
        period_df = df[df['time_period'] == p]
        present_by_period[p] = set(period_df['actor_id'])

        if mode == "z_thresh":
            important = set(period_df.loc[period_df[centrality_col] > z_thresh, 'actor_id'])
            if not important and not period_df.empty:
                max_val = period_df[centrality_col].max()
                important = set(period_df.loc[period_df[centrality_col] == max_val, 'actor_id'])
            important_by_period[p] = important

        elif mode == "top_k":
            if period_df.empty:
                important_by_period[p] = set()
            else:
                important = GetTopK(period_df, centrality_col, top_k)
                important_by_period[p] = important

        else:
            raise ValueError("mode must be either 'z_thresh' or 'top_k'")

    timelines = BuildTimelines(df, periods, centrality_col, mode, z_thresh, top_k)
    qualified_by_period = TrackQualified(timelines, periods, consecutive_req)

    combined_records = []
    for i, p in enumerate(periods):
        record = {
            "time_period": p,
            "num_important": len(important_by_period[p]),
            "important_actors": sorted(list(important_by_period[p])),
            "num_important_qualified": len(qualified_by_period[p] & present_by_period[p]),
            "important_qualified_actors": sorted(list(qualified_by_period[p] & present_by_period[p]))
        }

        if i < len(periods) - 1:
            candidates = qualified_by_period[p] & present_by_period[p]
            gone = [
                a for a in candidates
                if not any(a in present_by_period[fut] for fut in periods[i+1:])
            ]
            record["num_dropouts"] = len(gone)
            record["dropouts_actors"] = sorted(gone)
        else:
            record["num_dropouts"] = 0
            record["dropouts_actors"] = []

        combined_records.append(record)

    return pd.DataFrame(combined_records)

def MakeTasks(files, z_thresholds, top_ks, consecutive_reqs, centrality_cols):
    tasks = []
    for f in files:
        # z-threshold mode
        tasks += [
            {"file": f, "mode": "z_thresh", "z_thresh": z, "top_k": None, "consecutive_req": c, "centrality_col": col}
            for z in z_thresholds
            for c in consecutive_reqs
            for col in centrality_cols
        ]
        # top-k mode
        tasks += [
            {"file": f, "mode": "top_k", "z_thresh": None, "top_k": k, "consecutive_req": c, "centrality_col": col}
            for k in top_ks
            for c in consecutive_reqs
            for col in centrality_cols
        ]
    return tasks

def ProcessFile(task):
    """
    task = {
        "file": filepath,
        "mode": "z_thresh" | "top_k",
        "z_thresh": float | None,
        "top_k": int | None,
        "consecutive_req": int,
        "centrality_col": str
    }
    """
    try:
        df = pd.read_parquet(task["file"])
        combined_df = AnalyzeImportance(
            df,
            consecutive_req=task["consecutive_req"],
            centrality_col=task["centrality_col"],
            mode=task["mode"],
            z_thresh=task["z_thresh"],
            top_k=task["top_k"]
        )
        combined_df["time_period"] = combined_df["time_period"].dt.strftime("%Y-%m")

        total_dropouts = combined_df["num_dropouts"].sum()

        return {
            "file": os.path.basename(task["file"]),
            "mode": task["mode"],
            "z_thresh": task["z_thresh"],
            "top_k": task["top_k"],
            "consecutive_req": task["consecutive_req"],
            "centrality_col": task["centrality_col"],
            "num_dropouts": total_dropouts
        }
    except Exception:
        return None
        
def Main(part: int, nparts: int):
    files = glob.glob("drive/output/derived/graph_structure/graph_degrees/*.parquet")
    outdir = Path("output/derived/graph_structure")

    # parameter sweeps
    z_thresholds = [1.5, 2, 2.5]
    top_ks = [1, 3, 5]
    consecutive_reqs = [2, 3, 4]
    centrality_cols = [
        "degree_centrality_z",
        "betweenness_centrality_z",
        "weighted_degree_centrality_z"
    ]

    tasks = MakeTasks(files, z_thresholds, top_ks, consecutive_reqs, centrality_cols)

    # slice tasks for this machine
    my_tasks = tasks[part - 1 :: nparts]

    with Pool(processes=os.cpu_count() // 2) as pool:
        results = pool.map(ProcessFile, my_tasks, chunksize=2)

    # filter out None (errors)
    results = [r for r in results if r is not None]

    # save results
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / "analysis_summary.csv"

    new_df = pd.DataFrame(results)

    if outpath.exists():
        old_df = pd.read_csv(outpath)
        combined_df = pd.concat([old_df, new_df], ignore_index=True).drop_duplicates()
    else:
        combined_df = new_df

    combined_df.to_csv(outpath, index=False)
    print(f"✅ Part {part}/{nparts} done. Saved {len(combined_df)} rows to {outpath}")

if __name__ == "__main__":
    # args: script.py --part 1 --nparts 6 --outdir results/
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, required=True)
    parser.add_argument("--nparts", type=int, default=6)
    args = parser.parse_args()

    Main(args.part, args.nparts)