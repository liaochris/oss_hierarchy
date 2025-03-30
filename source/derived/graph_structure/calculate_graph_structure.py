import pandas as pd
import os
from pathlib import Path
import matplotlib.pyplot as plt
import random
import networkx as nx
import numpy as np
from source.derived.contributor_stats.calculate_contributions import *
from source.lib.helpers import *
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import concurrent.futures
import itertools
from source.lib.JMSLab.SaveData import SaveData

def Main():
    indir_data = Path('drive/output/derived/data_export')
    indir_committers_info = Path('drive/output/scrape/link_committers_profile')
    outdir = Path('drive/output/derived/graph_structure/graphs')
    logdir = Path('output/derived/graph_structure/graphs')

    commit_cols = ['commits','commit additions','commit deletions','commit changes total','commit files changed count']
    time_period = 6
    author_thresh = 1/3
    
    issue_cols = ["repo_name", "created_at", "issue_number", "issue_action", "actor_id", "type"]
    pr_cols = ["repo_name", "created_at", "pr_number", "actor_id", "actor_login", "pr_action", 
               "pr_merged_by_id", "pr_review_comment_path", "pr_review_comment_original_position", 
               "pr_review_comment_original_commit_id","type"]
    pr_commits_cols = ["repo_name", "commit time", "commit author name", "commit author email", 
                       "pr_number", "commit additions", "commit deletions",
                       "commit changes total", "commit files changed count"]

    committers_match = CleanCommittersInfo(indir_committers_info)
    
    df_issue = pd.read_parquet(indir_data / 'df_issue.parquet', columns=issue_cols)
    df_pr = pd.read_parquet(indir_data / 'df_pr.parquet', columns=pr_cols)
    df_pr_commits = pd.read_parquet(indir_data / 'df_pr_commits.parquet', columns=pr_commits_cols)
    
    df_issue, df_pr, df_pr_commits = ProcessData(df_issue, df_pr, df_pr_commits)
    
    repo_list = sorted(set(df_issue.index).union(set(df_pr.index)).union(set(df_pr_commits.index)))
    exported_graphs_log = []
    
    df_issue['date'] = df_issue['created_at'].dt.to_period('M').dt.to_timestamp()
    time_periods = sorted(ImputeTimePeriod(df_issue.drop_duplicates(['date']), time_period)['time_period'].unique())
    
    all_logs = []
    for repo, time_period_date in list(itertools.product(time_periods, repo_list)):
        all_logs = CreateGraph(repo, time_period_date, all_logs, df_issue, df_pr, df_pr_commits,
                       committers_match, commit_cols, time_period, author_thresh, outdir)
    df_log = pd.DataFrame(exported_graphs_log)
    df_log.to_csv(logdir / "exported_graphs_log.csv", index=False)

def ProcessData(df_issue, df_pr, df_pr_commits):
    df_issue.set_index('repo_name', inplace=True)
    df_pr.set_index('repo_name', inplace=True)
    df_pr_commits.set_index('repo_name', inplace=True)
    
    df_issue['created_at'] = pd.to_datetime(df_issue['created_at'])
    df_pr['created_at'] = pd.to_datetime(df_pr['created_at'])
    df_pr_commits['created_at'] = pd.to_datetime(df_pr_commits['commit time'], unit='s')
    
    df_issue = df_issue[~df_issue['created_at'].isna()]
    df_pr = df_pr[~df_pr['created_at'].isna()]
    df_pr_commits = df_pr_commits[~df_pr_commits['created_at'].isna()]
    
    return df_issue, df_pr, df_pr_commits

def BuildIssueInteractionGraph(df, method):
    """
    Build an undirected graph from the dataframe using one of two methods.
    Additionally, track:
      1. The origins in which a node appears (stored as a node attribute "origins").
      2. The number of interactions (edge weight) attributed to each origin (stored in an edge attribute "origin_counts").
    Also creates subgraphs for each origin: 'pr', 'issue', and 'pr review'.

    Parameters:
      - df: A pandas DataFrame sorted by id_number and created_at.
      - method: 'keep_consecutive' or 'drop_consecutive'
         (Only 'keep_consecutive' is demonstrated here.)
    
    Returns:
      - A dictionary with keys:
          "full"     : The full interaction graph.
          "pr"       : Graph containing only edges with at least one 'pr' interaction.
          "issue"    : Graph containing only edges with at least one 'issue' interaction.
          "pr_review": Graph containing only edges with at least one 'pr review' interaction.
    """

    df = df.reset_index(drop=True)
    df_sorted = df.sort_values(['id_number', 'created_at']).copy()
    
    # Vectorized function to compute previous different actor.
    def compute_prev_diff_vectorized(s):
        shifted = s.shift(1)
        candidate = shifted.where(shifted != s)
        candidate = candidate.ffill()
        candidate.iloc[0] = None
        return candidate
    df_sorted['prev_diff'] = df_sorted.groupby('id_number')['actor_id'].transform(compute_prev_diff_vectorized)
    
    # Filter rows: exclude 'opened' and 'reopened', and rows without a previous different actor.
    df_edges = df_sorted[
        (df_sorted['issue_action'] != 'opened') &
        (df_sorted['issue_action'] != 'reopened') &
        (df_sorted['prev_diff'].notnull())
    ]
    
    # Dictionaries to store node origins and edge attributes.
    node_origins = {}  # Maps node -> set of origins
    edge_dict = {}     # Maps edge (sorted tuple of nodes) -> {"weight": total, "origin_counts": {origin: count}}
    
    for _, row in df_edges.iterrows():
        u = row['actor_id']
        v = row['prev_diff']
        origin = row['origin']
        
        # Record origin for each node.
        node_origins.setdefault(u, set()).add(origin)
        node_origins.setdefault(v, set()).add(origin)
        
        # Use a sorted tuple as the undirected edge key.
        edge_key = tuple(sorted([u, v]))
        if edge_key not in edge_dict:
            edge_dict[edge_key] = {'weight': 0, 'origin_counts': {}}
        edge_dict[edge_key]['weight'] += 1
        edge_dict[edge_key]['origin_counts'][origin] = edge_dict[edge_key]['origin_counts'].get(origin, 0) + 1
    
    G = nx.Graph()
    for edge_key, attr in edge_dict.items():
        u, v = edge_key
        G.add_edge(u, v, weight=attr['weight'], origin_counts=attr['origin_counts'])
    
    for node, origins in node_origins.items():
        if node in G.nodes():
            G.nodes[node]['origins'] = " | ".join(list(origins))
        else:
            G.add_node(node, origins= " | ".join(list(origins)))
    
    # Build subgraphs for each origin.
    G_pr = nx.Graph()
    G_issue = nx.Graph()
    G_pr_review = nx.Graph()
    
    for u, v, data in G.edges(data=True):
        if data['origin_counts'].get('pr', 0) > 0:
            G_pr.add_node(u, **G.nodes[u])
            G_pr.add_node(v, **G.nodes[v])
            G_pr.add_edge(u, v,
                          weight=data['origin_counts']['pr'],
                          origin_counts={'pr': data['origin_counts']['pr']})
        if data['origin_counts'].get('issue', 0) > 0:
            G_issue.add_node(u, **G.nodes[u])
            G_issue.add_node(v, **G.nodes[v])
            G_issue.add_edge(u, v,
                             weight=data['origin_counts']['issue'],
                             origin_counts={'issue': data['origin_counts']['issue']})
        if data['origin_counts'].get('pr review', 0) > 0:
            G_pr_review.add_node(u, **G.nodes[u])
            G_pr_review.add_node(v, **G.nodes[v])
            G_pr_review.add_edge(u, v,
                                 weight=data['origin_counts']['pr review'],
                                 origin_counts={'pr review': data['origin_counts']['pr review']})
    
    return {"full": G, "pr": G_pr, "issue": G_issue, "pr_review": G_pr_review}

def ExportGraphs(repo, time_period_date, graphs, outdir):
    """
    Exports graphs only if they contain nodes. Returns a dictionary log entry
    indicating which graphs were exported and their file paths.
    
    Parameters:
      - repo: Repository name.
      - time_period_date: The time period (datetime object) used to create directories.
      - graphs: A dictionary of graphs with keys "full", "pr", "issue", "pr_review".
    
    Returns:
      A dictionary log entry.
    """
    yearmonth = f"{time_period_date.year}{str(time_period_date.month).zfill(2)}"
    output_dir = outdir / yearmonth
    os.makedirs(output_dir, exist_ok=True)
    output_base = f"{output_dir}/{repo.replace('/', '_')}"
    
    log_entry = {
         "time_period_date": str(time_period_date),
         "repo": repo,
         "full_exported": None,
         "pr_exported": None,
         "issue_exported": None,
         "pr_review_exported": None,
    }
    
    if graphs["full"].number_of_nodes() != 0:
         print(repo, time_period_date)
         full_file = output_base + ".gexf"
         nx.write_gexf(graphs["full"], full_file)
         log_entry["full_exported"] = full_file
         
    if graphs["pr"].number_of_nodes() != 0:
         pr_file = output_base + "_pr.gexf"
         nx.write_gexf(graphs["pr"], pr_file)
         log_entry["pr_exported"] = pr_file
         
    if graphs["issue"].number_of_nodes() != 0:
         issue_file = output_base + "_issue.gexf"
         nx.write_gexf(graphs["issue"], issue_file)
         log_entry["issue_exported"] = issue_file
         
    if graphs["pr_review"].number_of_nodes() != 0:
         pr_review_file = output_base + "_pr_review.gexf"
         nx.write_gexf(graphs["pr_review"], pr_review_file)
         log_entry["pr_review_exported"] = pr_review_file
         
    return log_entry

def SelectRepoData(repo, df_issue, df_pr, df_pr_commits):
    """
    Select and return DataFrames for a given repo.
    Returns a tuple: (df_issue_sel, df_pr_sel, df_pr_commits_sel).
    If a DataFrame is missing for the repo, returns an empty DataFrame.
    """
    df_issue_sel = df_issue.loc[[repo]].copy() if repo in df_issue.index else pd.DataFrame()
    df_pr_sel = df_pr.loc[[repo]].copy() if repo in df_pr.index else pd.DataFrame()
    df_pr_commits_sel = df_pr_commits.loc[[repo]].copy() if repo in df_pr_commits.index else pd.DataFrame()
    return df_issue_sel, df_pr_sel, df_pr_commits_sel

def ProcessReviewComments(df_pr_sel):
    """
    Process Pull Request review comment events from df_pr_sel.
    Returns a processed DataFrame with a standardized 'id_number' column.
    """
    if df_pr_sel.shape[0]==0:
        return pd.DataFrame()
        
    df_review = df_pr_sel[df_pr_sel['type'] == "PullRequestReviewCommentEvent"].copy()
    df_review['combo'] = list(zip(
        df_review['pr_review_comment_path'],
        df_review['pr_review_comment_original_position'],
        df_review['pr_review_comment_original_commit_id']
    ))
    df_review['id_number'] = df_review.groupby('pr_number')['combo']\
        .transform(lambda x: pd.factorize(x)[0])
    valid_mask = df_review['pr_number'].notnull() & (df_review['pr_number'] != np.inf)
    df_review = df_review[valid_mask]
    df_review['id_number'] = (
        "pr_rc" + 
        df_review['pr_number'].astype(int).astype(str) + "_" + 
        df_review['id_number'].astype(str)
    )
    df_review.drop(columns='combo', inplace=True)
    return df_review

def ImputeTimeEmptyRobust(df, time_period):
    """
    Impute the time period into the DataFrame and reset the index.
    Assumes ImputeTimePeriod is defined elsewhere.
    """
    if df.empty:
        return df
    return ImputeTimePeriod(df, time_period).reset_index()


def ProcessOtherComments(df_pr_sel, df_issue_sel):
    """
    Process Pull Request events and Issue events.
    Returns two DataFrames with a standardized 'id_number' column.
    """
    if df_pr_sel.shape[0] != 0:
        df_pr_comments = df_pr_sel[df_pr_sel['type'] == "PullRequestEvent"].copy()
        df_pr_comments = df_pr_comments.rename(columns={'pr_number': 'id_number'})
    else:
        df_pr_comments = pd.DataFrame()

    if df_issue_sel.shape[0] != 0:
        df_issue_comments = df_issue_sel[df_issue_sel['issue_action'] != "closed"].copy()
        df_issue_comments = df_issue_comments.rename(columns={'issue_number': 'id_number'})
    else:
        df_issue_comments = pd.DataFrame()
    

    return df_pr_comments, df_issue_comments

def ConcatenateAndFilterDiscussions(df_pr_comments, df_issue_comments, df_review_comments, sel_cols, target_period):
    """
    Concatenate all discussion interactions, sort them, and filter by time period.
    """
    all_discussions = pd.concat(
        [df_pr_comments, df_issue_comments, df_review_comments],
        ignore_index=True
    ).sort_values(['id_number', 'created_at'])
    if 'issue_action' not in all_discussions.columns:
        all_discussions['issue_action'] = np.nan

    
    return all_discussions[sel_cols]

def CreateGraph(repo, time_period_date, exported_graphs_log, 
                df_issue, df_pr, df_pr_commits,
                committers_match, commit_cols, time_period=6, author_thresh=1/3, outdir = Path('drive/output/derived/graph_structure/graphs')):
    """
    Process repository data to build an issue interaction graph.
    This function:
      1. Selects relevant DataFrames for the repo.
      2. Imputes the time period and resets indices.
      3. Filters each DataFrame to only include rows where the time period matches time_period_date.
      4. Processes PR commits and PR authorship.
      5. Processes review comments and other discussion events.
      6. Checks if all DataFrames are empty; if so, logs the result and returns.
      7. Concatenates interactions and filters by the target time period.
      8. Builds the interaction graph.
      9. Exports the graphs using ExportGraphs and logs the export paths.
    """
    # Step 1: Data selection.
    df_issue_sel, df_pr_sel, df_pr_commits_sel = SelectRepoData(repo, df_issue, df_pr, df_pr_commits)
    if df_issue_sel.empty and df_pr_sel.empty and df_pr_commits_sel.empty:
        return exported_graphs_log

    # Step 2: Impute time period.
    df_issue_sel = ImputeTimeEmptyRobust(df_issue_sel, time_period)
    df_pr_sel = ImputeTimeEmptyRobust(df_pr_sel, time_period)
    df_pr_commits_sel = ImputeTimeEmptyRobust(df_pr_commits_sel, time_period)

    # Step 3: Filter to only include rows where the time period matches time_period_date.
    if not df_issue_sel.empty:
        df_issue_sel = df_issue_sel[df_issue_sel['time_period'] == time_period_date]
    if not df_pr_sel.empty:
        df_pr_sel = df_pr_sel[df_pr_sel['time_period'] == time_period_date]
    if not df_pr_commits_sel.empty:
        df_pr_commits_sel = df_pr_commits_sel[df_pr_commits_sel['time_period'] == time_period_date]

    # Step 4: Process PR commits and authorship (for review counting).
    if df_pr_sel.shape[0] > 0 and df_pr_commits_sel.shape[0] > 0:
        df_pr_commit_stats = LinkCommits(df_pr_sel, df_pr_commits_sel, committers_match, commit_cols, 'pr')
        df_pr_commit_author_stats = AssignPRAuthorship(df_pr_commit_stats, df_pr_sel, author_thresh, commit_cols)
    else:
        df_pr_commit_author_stats = pd.DataFrame()
    
    sel_cols = ['created_at', 'actor_id', 'id_number', 'type', 'issue_action', 'time_period', 'origin']
    
    # Step 5: Process discussion interactions.
    df_pr_comments, df_issue_comments = ProcessOtherComments(df_pr_sel, df_issue_sel)
    df_review_comments = ProcessReviewComments(df_pr_sel)
    
    # Step 6: Check if all DataFrames are empty.
    if df_pr_comments.empty and df_issue_comments.empty and df_review_comments.empty:
        log_entry = {
             "time_period_date": str(time_period_date),
             "repo": repo,
             "full_exported": None,
             "pr_exported": None,
             "issue_exported": None,
             "pr_review_exported": None,
        }
        exported_graphs_log.append(log_entry)    
        return exported_graphs_log

    # Step 7: Concatenate and filter discussions.
    discussion_filtered = ConcatenateAndFilterDiscussions(
        df_pr_comments.assign(origin='pr'), 
        df_issue_comments.assign(origin='issue'), 
        df_review_comments.assign(origin='pr review'), sel_cols, str(time_period_date.date())
    )

    # Step 8: Build the interaction graph.
    graphs = BuildIssueInteractionGraph(discussion_filtered, 'keep_consecutive')

    # Step 9: Export graphs and update the log.
    log_entry = ExportGraphs(repo, time_period_date, graphs, outdir = outdir)
    exported_graphs_log.append(log_entry)    
    return exported_graphs_log

if __name__ == '__main__':
    Main()
