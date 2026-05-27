import pandas as pd
import os
from pathlib import Path
import networkx as nx
from joblib import Parallel, delayed
from source.lib.python.filesystem_utils import CleanDirs, WriteDirectoryHash
from source.lib.python.repo_utils import MakeRepoNameSafe, MakeRepoNameOriginal
from source.lib.python.data_utils import ImputeTimePeriod, JsonSerialize
from source.lib.python.config_loaders import LoadGlobalSettings
from source.lib.JMSLab.SaveData import SaveData

INDIR_DATA = Path('drive/output/derived/action_data/repo_actions')
OUTDIR    = Path('drive/output/derived/graph_structure')
HASH_FILE = Path('output/derived/hashes/interactions.txt')
LOG_DIR = Path('output/derived/graph_structure')


def Main():
    CleanOutputs()
    globals_data = LoadGlobalSettings()
    time_period = globals_data["time_period_months"]
    repo_list = sorted({
        MakeRepoNameOriginal(f.stem)
        for f in INDIR_DATA.glob("*.parquet")
        if f.is_file() and "___" in f.stem
    })
    time_periods = pd.date_range(globals_data["github_start_date"], globals_data["github_end_date"], freq="6MS").to_list()

    results = Parallel(n_jobs=globals_data["n_jobs"])(
        delayed(worker)(repo, time_periods, time_period, OUTDIR, INDIR_DATA) for repo in repo_list
    )
    all_logs = [log for logs in results for log in logs]

    log_df = pd.DataFrame(all_logs)
    if not log_df.empty:
        log_df['per_period_exported'] = log_df['per_period_exported'].apply(JsonSerialize)
        SaveData(log_df, ['repo'], LOG_DIR / "exported_graphs_log.csv", LOG_DIR / "exported_graphs_log.log")

    WriteDirectoryHash(OUTDIR / "interactions", HASH_FILE)


def CleanOutputs():
    CleanDirs([OUTDIR / "interactions", LOG_DIR / "interactions"])
    for d in (OUTDIR / "graphs").glob("*"):
        if d.is_dir():
            for f in d.glob("*.gexf"):
                f.unlink(missing_ok=True)


def worker(repo, time_periods, time_period, outdir, indir_data):
    return CreateGraph(repo, time_periods, time_period, [], outdir, indir_data)


def CreateGraph(repo, time_periods, time_period, exported_graphs_log, outdir, indir_data):
    parquet_path = outdir / "interactions" / f"{MakeRepoNameSafe(repo)}.parquet"

    if parquet_path.exists():
        print(f"Skipping {repo} (interactions already exist)")
        return exported_graphs_log

    try:
        df_actions = pd.read_parquet(indir_data / f"{MakeRepoNameSafe(repo)}.parquet")
        df_actions = ImputeTimePeriod(df_actions, time_period).reset_index()
        pr_comments = df_actions[df_actions['type'].isin(['pull request comment', 'pull request opened'])]
        issue_comments = df_actions[df_actions['type'].isin(['issue opened', 'issue comment'])]
        review_comments = df_actions[df_actions['type'].str.startswith('pull request review')]

        if pr_comments.empty and issue_comments.empty and review_comments.empty:
            exported_graphs_log.append({"repo": repo, "per_period_exported": None})
            print(f"Completed {repo} (no comments)")
            return exported_graphs_log

        sel_cols = ['created_at', 'actor_id', 'thread_number', 'discussion_id', 'action_id', 'type',
                    'time_period', 'origin', 'text', 'opener_id']
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


def ConcatenateAndFilterDiscussions(pr_comments, issue_comments, review_comments, sel_cols):
    all_discussions = pd.concat([pr_comments, issue_comments, review_comments], ignore_index=True)
    all_discussions = all_discussions.sort_values(['action_id', 'created_at'])
    return all_discussions[sel_cols]


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

    # 1. Exclude discussion threads where discussion didn't occur between members
    df = df[df["thread_number"].isin(responded_threads)].copy()

    # 2. Ensure edges connect only distinct members - responding to yourself does not
    # constitute an interaction between members (nodes)
    mask_invalid = df["prev_diff"] == df["actor_id"]
    df.loc[mask_invalid, "prev_diff"] = pd.NA
    df["prev_diff"] = df.groupby("discussion_id")["prev_diff"].ffill()

    df = df.drop_duplicates()

    df['actor_id'] = df['actor_id'].apply(lambda x: StandardizeId(x, 'actor_id'))
    df['prev_diff'] = df['prev_diff'].apply(lambda x: StandardizeId(x, 'prev_diff'))

    interaction_df = df.assign(
        repo_name=repo_name,
        sender=df['actor_id'],
        receiver=df['prev_diff']
    )[["repo_name", "discussion_id", "created_at", "origin", "sender", "receiver", "text", "opener_id", "time_period", "action_id"]]

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

    for period, G in graphs.items():
        if G.number_of_nodes() == 0:
            continue

        yearmonth = f"{period.year}{str(period.month).zfill(2)}"
        output_dir = outdir / "graphs" / yearmonth
        os.makedirs(output_dir, exist_ok=True)
        output_base = output_dir / MakeRepoNameSafe(repo)

        nx.write_gexf(G, f"{output_base}.gexf")
        log_entry["per_period_exported"][period.isoformat()] = str(output_base) + ".gexf"

    if interaction_df.shape[0] > 0:
        interactions_dir = outdir / "interactions"
        os.makedirs(interactions_dir, exist_ok=True)
        parquet_path = interactions_dir / f"{MakeRepoNameSafe(repo)}.parquet"

        log_out_dir = LOG_DIR / "interactions"
        log_out_dir.mkdir(parents=True, exist_ok=True)

        interaction_df = interaction_df.reset_index(drop=True)
        interaction_df["action_order"] = interaction_df.index
        SaveData(
            interaction_df,
            ['repo_name', 'action_order'],
            parquet_path,
            log_out_dir / f"{MakeRepoNameSafe(repo)}.log"
        )

    return log_entry


def StandardizeId(x, col):
    if pd.isna(x):
        return None
    try:
        f = float(x)
        if pd.isna(f):
            return None
        val = int(f) if f.is_integer() else f
        return str(val)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid value in {col}: {x}")


if __name__ == '__main__':
    Main()
