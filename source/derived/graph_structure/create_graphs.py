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

def SafeReadParquet(path):
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path).dropna(subset=['created_at'])

def ImputeTimeEmptyRobust(df, time_period):
    if df.empty:
        return df
    return ImputeTimePeriod(df, time_period).reset_index()

def ProcessOtherComments(df_pr, df_issue):
    pr_comments = (
        df_pr[df_pr['type'].isin(["PullRequestEvent","IssueCommentEvent"])].rename(columns={'pr_number': 'id_number'})
        if not df_pr.empty else pd.DataFrame()
    )
    issue_comments = (
        df_issue.rename(columns={'issue_number': 'id_number'})
        if not df_issue.empty else pd.DataFrame()
    )
    return pr_comments, issue_comments

def ProcessReviewComments(df_pr):
    if df_pr.empty:
        return pd.DataFrame()
    df_review = df_pr[df_pr['type'] == "PullRequestReviewCommentEvent"].copy()
    df_review['combo'] = list(zip(
        df_review['pr_review_comment_path'],
        df_review['pr_review_comment_original_position'],
        df_review['pr_review_comment_original_commit_id']
    ))
    df_review['id_number'] = df_review.groupby('pr_number')['combo'].transform(lambda x: pd.factorize(x)[0])
    df_review = df_review[df_review['pr_number'].notnull() & (df_review['pr_number'] != np.inf)]
    df_review['id_number'] = (
        "pr_rc" + df_review['pr_number'].astype(int).astype(str) + "_" + df_review['id_number'].astype(str)
    )
    return df_review.drop(columns='combo')

def ConcatenateAndFilterDiscussions(pr_comments, issue_comments, review_comments, sel_cols):
    all_discussions = pd.concat([pr_comments, issue_comments, review_comments], ignore_index=True)
    all_discussions = all_discussions.sort_values(['id_number', 'created_at'])
    if 'issue_action' not in all_discussions.columns:
        all_discussions['issue_action'] = np.nan
    return all_discussions[sel_cols]

### AMEND SUCH THAT IF IT"S PR REVIEW, DIRECTED TOWARDS PR OPENER
### IF IT"S PR, DIRECTED TOWARDS PR OPENER IF IT"S THE FIRST COMMENT IN THE ID BY CREATED_AT
### 

def BuildInteractionGraph(df, repo_name, time_periods):
    df = df.reset_index(drop=True)
    df = df.sort_values(['id_number', 'created_at']).copy()

    def compute_prev_diff(s):
        shifted = s.shift(1)
        candidate = shifted.where(shifted != s).ffill()
        candidate.iloc[0] = None
        return candidate

    df['prev_diff'] = df.groupby('id_number')['actor_id'].transform(compute_prev_diff)

    opener_map = df.groupby('id_number')['opener_id'].first().to_dict()

    def assign_first_responder(row):
        if pd.isna(row['prev_diff']):
            return opener_map.get(row['id_number'])
        return row['prev_diff']

    df['prev_diff'] = df.apply(assign_first_responder, axis=1)

    responded_threads = (
        df.groupby('id_number')['actor_id']
        .nunique()
        .loc[lambda x: x > 1]
        .index
    )

    df_edges = df[
        (df['issue_action'] != 'reopened') &
        (df['prev_diff'].notnull()) &
        (df['actor_id'] != df['prev_diff'])
    ].copy()

    opener_rows = (
        df.loc[df.groupby('id_number').head(1).index]
        .loc[lambda x: x['id_number'].isin(responded_threads)]
        .copy()
    )
    opener_rows['prev_diff'] = None  

    df_edges = pd.concat([df_edges, opener_rows], ignore_index=True)

    interaction_df = df_edges.assign(
        repo_name=repo_name,
        sender=df_edges['actor_id'],
        receiver=df_edges['prev_diff']
    )[["repo_name", "id_number", "created_at", "origin", "sender", "receiver", "text", "opener_id", "time_period"]]

    interaction_df["id_number"] = interaction_df["id_number"].apply(
        lambda x: str(int(x)) if isinstance(x, float) and x.is_integer() else str(x)
    )

    # --- Build per-time_period graphs ---
    graphs = {}
    for period in time_periods:
        sub_edges = df_edges[df_edges['time_period'] == period]
        Gp = nx.Graph()
        edge_dict, node_origins = {}, {}
        for _, row in sub_edges.iterrows():
            u, v, origin = row['actor_id'], row['prev_diff'], row['origin']
            if v is None:
                continue
            node_origins.setdefault(u, set()).add(origin)
            node_origins.setdefault(v, set()).add(origin)
            edge_key = tuple(sorted([u, v]))
            if edge_key not in edge_dict:
                edge_dict[edge_key] = {'weight': 0, 'origin_counts': {}}
            edge_dict[edge_key]['weight'] += 1
            edge_dict[edge_key]['origin_counts'][origin] = (
                edge_dict[edge_key]['origin_counts'].get(origin, 0) + 1
            )

        for (u, v), attr in edge_dict.items():
            Gp.add_edge(u, v, weight=attr['weight'], origin_counts=attr['origin_counts'])
        for node, origins in node_origins.items():
            Gp.add_node(node, origins=" | ".join(list(origins)))

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
        df_issue = SafeReadParquet(indir_data / 'issue' / f"{repo.replace('/', '_')}.parquet")
        df_pr = SafeReadParquet(indir_data / 'pr' / f"{repo.replace('/', '_')}.parquet")

        if df_issue.empty and df_pr.empty:
            print(f"Skipping {repo} (no data)")
            return exported_graphs_log

        # Impute for all periods in time_periods
        df_issue, df_pr = ImputeTimeEmptyRobust(df_issue, time_period), ImputeTimeEmptyRobust(df_pr, time_period)

        if not df_pr.empty and not df_issue.empty:
            pr_index = df_pr[['repo_name', 'pr_number']].drop_duplicates().set_index(['repo_name','pr_number']).index
            df_pr = pd.concat([df_issue.loc[df_issue.set_index(['repo_name','issue_number']).index.isin(pr_index)], df_pr])
            df_issue = df_issue.loc[~df_issue.set_index(['repo_name','issue_number']).index.isin(pr_index)]

        mapping = {
            "PullRequestReviewCommentEvent": "pr_review_comment_body",
            "PullRequestReviewEvent": "pr_review_body",
            "PullRequestEvent": "pr_body",
            "IssueEvent": "issue_body",
            "IssueCommentEvent": "issue_comment_body"
        }

        def coalesce_text(df):
            if df.empty:
                return df
            df = df.copy()
            for event_type, col in mapping.items():
                if col in df.columns:
                    mask = df["type"] == event_type
                    df.loc[mask, "text"] = df.loc[mask, col]
            return df

        if not df_pr.empty:
            df_pr['pr_body'] = df_pr['pr_body'].replace("", np.nan)
            df_pr['pr_body'] = df_pr.groupby('pr_number')['pr_body'].transform(lambda x: x.ffill().bfill())
            indir_linked = Path('drive/output/scrape/link_issue_pull_request/linked_pull_request_to_issue')
            df_pr_link_to_issue = pd.read_parquet(indir_linked / f"{repo.replace('/', '_')}.parquet")
            df_pr = df_pr.merge(df_pr_link_to_issue[['pr_number', 'pull_request_text']], on='pr_number', how='left')
            df_pr['pr_body'] = df_pr['pr_body'].fillna(df_pr['pull_request_text'])

        df_issue = coalesce_text(df_issue).rename(columns={'issue_user_id': 'opener_id'})
        df_pr = coalesce_text(df_pr)
        df_pr_author = df_pr.query('type == "PullRequestEvent" & pr_action == "opened"')[['pr_number','actor_id']] \
            .rename(columns={'actor_id': 'opener_id'})
        df_pr['pr_number'] = df_pr['pr_number'].fillna(df_pr['issue_number'])
        df_pr = pd.merge(df_pr, df_pr_author, on=['pr_number'])

        sel_cols = ['created_at','actor_id','id_number','type','issue_action','time_period','origin','text','opener_id']
        pr_comments, issue_comments = ProcessOtherComments(df_pr, df_issue)
        review_comments = ProcessReviewComments(df_pr)

        if pr_comments.empty and issue_comments.empty and review_comments.empty:
            exported_graphs_log.append({"repo": repo, "per_period_exported": None})
            print(f"Completed {repo} (no comments)")
            return exported_graphs_log

        discussions = ConcatenateAndFilterDiscussions(
            pr_comments.assign(origin='pr'),
            issue_comments.assign(origin='issue'),
            review_comments.assign(origin='pr review'),
            sel_cols,
        )

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
    indir_data = Path('drive/output/derived/data_export')
    logdir = Path('output/derived/graph_structure')
    outdir = Path('drive/output/derived/graph_structure')
    os.makedirs(logdir, exist_ok=True)
    os.makedirs(outdir, exist_ok=True)

    time_period = 6
    repo_list = sorted({
        f.stem.replace("_", "/", 1)
        for d in ["issue", "pr"]
        for f in indir_data.glob(f"{d}/**/*")
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
