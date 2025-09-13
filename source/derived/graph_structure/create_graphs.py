import random
from filelock import FileLock
import pandas as pd
import os
from pathlib import Path
import networkx as nx
import numpy as np
import itertools
import concurrent.futures
from datetime import datetime
from source.lib.helpers import *

#####################################
# ---------- HELPERS ---------------
#####################################

def ConcatenateAndFilterDiscussions(pr_comments, issue_comments, review_comments, sel_cols):
    all_discussions = pd.concat([pr_comments, issue_comments, review_comments], ignore_index=True)
    all_discussions = all_discussions.sort_values(['action_id', 'created_at'])
    return all_discussions[sel_cols]


def StandardizeId(x, col):
    if pd.isna(x):
        return None
    try:
        f = float(x)
        val = int(f) if f.is_integer() else f
        return str(val)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid value in {col}: {x}")


def BuildInteractionGraph(df, repo_name, time_periods):
    df = df.reset_index(drop=True)
    df = df.sort_values(['discussion_id', 'created_at']).copy()

    def compute_prev_diff(s):
        shifted = s.shift(1)
        candidate = shifted.where(shifted != s).ffill()
        candidate.iloc[0] = None
        return candidate

    df['prev_diff'] = df.groupby('discussion_id')['actor_id'].transform(compute_prev_diff)

    opener_map = df.groupby('thread_number')['opener_id'].first().to_dict()

    def assign_first_responder(row):
        if pd.isna(row['prev_diff']) and row['origin'] == "pr review":
            return opener_map.get(row['thread_number'])
        return row['prev_diff']

    df['prev_diff'] = df.apply(assign_first_responder, axis=1)

    responded_threads = (
        df.groupby('thread_number')['actor_id']
        .nunique()
        .loc[lambda x: x > 1]
        .index
    )

    # 1. only keep prev_diff.null if it's id_number is in responded_threads
    df = df[df["thread_number"].isin(responded_threads)].copy()

    # 2. if actor_id is the same as prev_diff, set prev_diff to be the most recent actor_id (within that id_number) previous to that row where actor_id != prev_diff
    mask_invalid = df["prev_diff"] == df["actor_id"]
    df.loc[mask_invalid, "prev_diff"] = pd.NA
    # forward-fill within each thread
    df["prev_diff"] = df.groupby("discussion_id")["prev_diff"].ffill()

    df = df.drop_duplicates()
    def to_numeric_or_error(x, col):
        try:
            f = float(x)
            return int(f) if f.is_integer() else f
        except (ValueError, TypeError):
            raise ValueError(f"Invalid string value in {col}: {x}")

    df['actor_id'] = df['actor_id'].apply(lambda x: StandardizeId(x, 'actor_id'))
    df['prev_diff'] = df['prev_diff'].apply(lambda x: StandardizeId(x, 'prev_diff'))

    interaction_df = df.assign(
        repo_name=repo_name,
        sender=df['actor_id'],
        receiver=df['prev_diff']
    )[["repo_name", "discussion_id", "created_at", "origin", "sender", "receiver", "text", "opener_id", "time_period","action_id"]]

    # --- Build per-time_period graphs ---
    graphs = {}
    for period in time_periods:
        sub_edges = df[df['time_period'] == period]
        Gp = nx.Graph()
        for _, row in sub_edges.iterrows():
            u, v = row['actor_id'], row['prev_diff']
            if v is None:
                continue
            if Gp.has_edge(u, v):
                Gp[u][v]["weight"] += 1
            else:
                Gp.add_edge(u, v, weight=1)
        graphs[period] = Gp

    return graphs, interaction_df


def ExportData(repo, graphs, interaction_df, outdir):
    log_entry = {"repo": repo, "per_period_exported": {}}

    # Export per-period graphs
    for period, G in graphs.items():
        if G.number_of_nodes() == 0:
            continue

        yearmonth = f"{period.year}{str(period.month).zfill(2)}"
        output_dir = outdir / "graphs" / yearmonth
        os.makedirs(output_dir, exist_ok=True)
        output_base = output_dir / repo.replace('/', '_')

        nx.write_gexf(G, f"{output_base}.gexf")
        log_entry["per_period_exported"][period] = str(output_base) + ".gexf"

    # Export interaction_df (still one parquet per repo)
    os.makedirs(outdir / "interactions", exist_ok=True)
    parquet_path = outdir / "interactions" / f"{repo.replace('/', '_')}.parquet"
    if interaction_df.shape[0]>0:
        interaction_df.to_parquet(parquet_path, index=False)

    return log_entry


#####################################
# ----------- MAIN FLOW ------------
#####################################
def CreateGraph(repo, time_periods, time_period, exported_graphs_log, outdir, indir_data):
    parquet_path = outdir / "interactions" / f"{repo.replace('/', '_')}.parquet"

    # Skip if interaction_df already exists
    if parquet_path.exists():
        print(f"Skipping {repo} (interactions already exist)")
        return exported_graphs_log

    try:
        df_actions = pd.read_parquet(indir_data / f"{repo.replace('/', '_')}.parquet")
        df_actions = ImputeTimePeriod(df_actions, time_period).reset_index()
        pr_comments = df_actions[df_actions['type'].isin(['pull request comment','pull request opened'])]
        issue_comments = df_actions[df_actions['type'].isin(['issue opened','issue comment'])]
        review_comments = df_actions[df_actions['type'].str.startswith('pull request review')]

        if pr_comments.empty and issue_comments.empty and review_comments.empty:
            exported_graphs_log.append({"repo": repo, "per_period_exported": None})
            print(f"Completed {repo} (no comments)")
            return exported_graphs_log

        sel_cols = ['created_at','actor_id','thread_number','discussion_id', 'action_id','type',
            'time_period','origin','text','opener_id']
        discussions = ConcatenateAndFilterDiscussions(
            pr_comments.assign(origin='pr'),
            issue_comments.assign(origin='issue'),
            review_comments.assign(origin='pr review'),
            sel_cols,
        )
        discussions = discussions[discussions['actor_id'].notna()]
        graphs, interaction_df = BuildInteractionGraph(discussions, repo, time_periods)
        log_entry = ExportData(repo, graphs, interaction_df, outdir)
        exported_graphs_log.append(log_entry)

        print(f"Succeeded {repo}")
        return exported_graphs_log

    except Exception as e:
        print(f"Failed {repo}: {e}")
        return exported_graphs_log

def worker(repo, time_periods, time_period, outdir, indir_data):
    return CreateGraph(repo, time_periods, time_period, [], outdir, indir_data)

def Main():
    indir_data = Path('drive/output/derived/repo_level_data/repo_actions')
    logdir = Path('output/derived/graph_structure')
    outdir = Path('drive/output/derived/graph_structure')
    os.makedirs(logdir, exist_ok=True)
    os.makedirs(outdir, exist_ok=True)

    time_period = 6
    repo_list = sorted({
        f.stem.replace("_", "/", 1)
        for f in indir_data.glob("*.parquet")
        if f.is_file() and "_" in f.stem
    })
    time_periods = pd.date_range("2015-01-01", "2024-12-31", freq="6MS").to_list()

    random.shuffle(repo_list) 
    all_logs = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(worker, repo, time_periods, time_period,  outdir, indir_data) for repo in repo_list]
        for future in concurrent.futures.as_completed(futures):
            logs = future.result()
            all_logs.extend(logs)

    pd.DataFrame(all_logs).to_csv(logdir / "exported_graphs_log.csv", index=False)

if __name__ == '__main__':
    Main()
