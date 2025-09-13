import uuid
from pathlib import Path
import pandas as pd
import glob
import os
from multiprocessing import Pool
import json
import numpy as np


def DeduplicateWithLists(df: pd.DataFrame) -> pd.DataFrame:
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


def GeneratePeriods(df: pd.DataFrame) -> list:
    if df.empty or df["time_period"].isna().all():
        return []
    start = pd.to_datetime(df["time_period"]).min()
    end = pd.to_datetime(df["time_period"]).max()
    if pd.isna(start) or pd.isna(end):
        return []
    return pd.date_range(start, end, freq="6MS").drop_duplicates().to_pydatetime().tolist()


def GetTopK(period_df: pd.DataFrame, centrality_col: str, top_k: int) -> set:
    if period_df.empty:
        return set()

    sorted_df = period_df.sort_values(centrality_col, ascending=False)
    if len(sorted_df) <= top_k:
        return set(sorted_df["actor_id"])

    cutoff = sorted_df.iloc[top_k - 1][centrality_col]
    return set(sorted_df.loc[sorted_df[centrality_col] >= cutoff, "actor_id"])


def BuildTimelines(df: pd.DataFrame, periods: list, centrality_col: str, mode: str,
                   z_thresh: float = None, top_k: int = None) -> pd.DataFrame:
    df = df.copy()
    df["time_period"] = (
        pd.to_datetime(df["time_period"]).dt.to_period("6M").dt.to_timestamp()
    )
    df = df.groupby(["actor_id", "time_period"], as_index=False).agg({centrality_col: "max"})

    if mode == "z_thresh":
        df["important"] = df[centrality_col] > z_thresh
        timelines = (
            df.pivot(index="actor_id", columns="time_period", values="important")
              .reindex(columns=periods)
              .fillna(False)
              .infer_objects(copy=False)
              .astype(bool)
        )
        return timelines

    elif mode == "top_k":
        important_flags = []
        for p in periods:
            period_df = df[df["time_period"] == p]
            if period_df.empty:
                continue
            top_ids = GetTopK(period_df, centrality_col, top_k)
            flags = pd.DataFrame({"actor_id": list(top_ids), p: True})
            important_flags.append(flags)

        if important_flags:
            flags_df = (
                pd.concat(important_flags)
                  .groupby("actor_id")
                  .max()
            )
        else:
            flags_df = pd.DataFrame(index=df["actor_id"].unique())

        timelines = (
            flags_df.reindex(index=df["actor_id"].unique(), columns=periods, fill_value=False)
                    .infer_objects(copy=False)
                    .astype(bool)
        )
        return timelines

    else:
        raise ValueError("mode must be either 'z_thresh' or 'top_k'")


def TrackQualified(timelines: pd.DataFrame, periods: list, consecutive_req: int) -> dict:
    qualified = {p: set() for p in periods}
    binary = timelines.astype(int)

    # Future-proof rolling
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


def AnalyzeImportance(
    df: pd.DataFrame,
    consecutive_req: int = 3,
    centrality_col: str = "degree_centrality_z",
    mode: str = "z_thresh",
    z_thresh: float = 2,
    top_k: int = None,
) -> pd.DataFrame:
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

    important_by_period, present_by_period = {}, {}

    for p in periods:
        period_df = df[df["time_period"] == p]
        present_by_period[p] = set(period_df["actor_id"])

        if mode == "z_thresh":
            important = set(period_df.loc[period_df[centrality_col] > z_thresh, "actor_id"])
            if not important and not period_df.empty:
                max_val = period_df[centrality_col].max()
                important = set(period_df.loc[period_df[centrality_col] == max_val, "actor_id"])
            important_by_period[p] = important
        elif mode == "top_k":
            important_by_period[p] = GetTopK(period_df, centrality_col, top_k) if not period_df.empty else set()

    timelines = BuildTimelines(df, periods, centrality_col, mode, z_thresh, top_k)
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
            gone = [a for a in candidates if not any(a in present_by_period[fut] for fut in periods[i + 1 :])]
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
        tasks += [
            {"file": f, "mode": "z_thresh", "z_thresh": z, "top_k": None,
             "consecutive_req": c, "centrality_col": col}
            for z in z_thresholds for c in consecutive_reqs for col in centrality_cols
        ]
        tasks += [
            {"file": f, "mode": "top_k", "z_thresh": None, "top_k": k,
             "consecutive_req": c, "centrality_col": col}
            for k in top_ks for c in consecutive_reqs for col in centrality_cols
        ]
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

        outdir = Path("drive/output/derived/graph_structure/important_members/tmp")
        outdir.mkdir(parents=True, exist_ok=True)
        project_name = Path(task["file"]).stem
        tmpfile = outdir / f"{project_name}_{uuid.uuid4().hex}.parquet"

        combined_df.to_parquet(tmpfile, index=False)

        return {**task, "file": os.path.basename(task["file"]), "tmpfile": str(tmpfile)}
    except Exception as e:
        print(f"⚠️ Error processing {task}: {e}")
        return None


def Main(part: int, nparts: int):
    files = glob.glob("drive/output/derived/graph_structure/graph_degrees/*.parquet")
    outdir = Path("drive/output/derived/graph_structure")

    z_thresholds, top_ks, consecutive_reqs = [1.5, 2, 2.5], [1, 3, 5], [2, 3, 4]
    centrality_cols = ["degree_centrality_z", "betweenness_centrality_z", "weighted_degree_centrality_z"]

    tasks = MakeTasks(files, z_thresholds, top_ks, consecutive_reqs, centrality_cols)
    my_tasks = tasks[part - 1 :: nparts]

    with Pool(processes=os.cpu_count() // 2) as pool:
        results = [r for r in pool.map(ProcessFile, my_tasks, chunksize=2) if r is not None]

    if not results:
        print(f"⚠️ Part {part}/{nparts} produced no results")
        return

    outdir.mkdir(parents=True, exist_ok=True)
    summary_path = outdir / "analysis_summary.csv"
    summary_df = pd.DataFrame(results)

    if summary_path.exists():
        old_df = pd.read_csv(summary_path)
        summary_df = pd.concat([old_df, summary_df], ignore_index=True).drop_duplicates()

    summary_df.drop(columns=['tmpfile']).to_csv(summary_path, index=False)
    print(f"✅ Part {part}/{nparts} done. Saved {len(summary_df)} rows to {summary_path}")

    # merge tmp files into one parquet per repo
    panel_outdir = Path("drive/output/derived/graph_structure/important_members")
    panel_outdir.mkdir(parents=True, exist_ok=True)

    for project_name, group in summary_df.query('~tmpfile.isna()').groupby("file"):
        tmpfiles = group["tmpfile"].tolist()
        dfs = [pd.read_parquet(f) for f in tmpfiles if Path(f).exists()]
        if not dfs:
            continue
        merged = pd.concat(dfs, ignore_index=True)
        merged = DeduplicateWithLists(merged)
        outpath = panel_outdir / Path(project_name).with_suffix(".parquet")
        merged.to_parquet(outpath, index=False)
        for f in tmpfiles:
            Path(f).unlink(missing_ok=True)

if __name__ == "__main__":
    import argparse
    pd.set_option('future.no_silent_downcasting', True)
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, required=True)
    parser.add_argument("--nparts", type=int, default=6)
    args = parser.parse_args()

    Main(args.part, args.nparts)
