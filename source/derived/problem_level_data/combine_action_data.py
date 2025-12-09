import pandas as pd
from pathlib import Path
import concurrent.futures
import json
import numpy as np
import random

INDIR_REPO = Path('drive/output/scrape/extract_github_data/repo_level_data')
INDIR_LINK = Path('drive/output/scrape/link_issue_pull_request')
OUTDIR = Path('drive/output/derived/problem_level_data/repo_actions')
STATS_PATH = Path("output/derived/problem_level_data/dropped_stats.csv")


def RunAllRepos():
    issue_repos = {p.stem for p in (INDIR_REPO / "issue").glob("*.parquet")}
    pr_repos = {p.stem for p in (INDIR_REPO / "pr").glob("*.parquet")}
    link_repos = {p.stem for p in (INDIR_LINK / "linked_pull_request_to_issue").glob("*.parquet")}
    repos = sorted(issue_repos & pr_repos & link_repos)
    random.shuffle(repos)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for msg in executor.map(ProcessRepo, repos):
            print(msg)

def ProcessRepo(safe_repo_name):
    issue_path = INDIR_REPO / "issue" / f"{safe_repo_name}.parquet"
    pr_path = INDIR_REPO / "pr" / f"{safe_repo_name}.parquet"
    linked_path = INDIR_LINK / "linked_pull_request_to_issue" / f"{safe_repo_name}.parquet"
    out_path = OUTDIR / f"{safe_repo_name}.parquet"

    if not (issue_path.exists() and pr_path.exists() and linked_path.exists()):
        return f"⏭️ missing inputs {safe_repo_name}"
    if out_path.exists():
        return f"⏭️ already exists {safe_repo_name}"

    df_issue = LoadAndProcessIssues(issue_path)
    df_pr = LoadAndProcessPullRequests(pr_path, linked_path)
    df_all = AggregateAndCleanIssuesAndPRs(df_issue, df_pr, STATS_PATH, safe_repo_name)

    if df_all.empty:
        return f"⏭️ empty result {safe_repo_name}"

    OUTDIR.mkdir(parents=True, exist_ok=True)
    df_all.to_parquet(out_path, index=False)
    return f"✅ exported {safe_repo_name}"


def ParseJsonList(x, col):
    if x in ("", None):
        return []
    if isinstance(x, str):
        try:
            parsed = json.loads(x)
            return [ele[col] for ele in parsed] if isinstance(parsed, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    return []

def LoadAndProcessIssues(issue_path):
    required_columns = [
        'repo_name',"issue_number", "created_at", "type",
        "issue_action",
        "issue_title", "issue_comment_body", "issue_body",
        "actor_id","latest_issue_assignees","latest_issue_labels"
    ]

    issues = pd.read_parquet(issue_path)[required_columns].rename(columns={
        "issue_number": "thread_number",
        "issue_title": "title",
        "issue_action": "action",
        "latest_issue_assignees":"assignees",
        "latest_issue_labels":"labels"
    })
    issues['issue_body'] = issues.apply(lambda x: x['issue_body'] if x['type'] == "IssuesEvent" and x['action'] == "opened" else pd.NA, axis = 1)
    issues["text"] = issues["issue_body"].combine_first(issues["issue_comment_body"])
    issues = issues.drop(columns=["issue_body", "issue_comment_body"])

    issues["assignees"] = issues["assignees"].apply(lambda x: ParseJsonList(x,'id'))
    issues["labels"] = issues["labels"].apply(lambda x: ParseJsonList(x, 'name'))

    issues["title"] = (
        issues.groupby("thread_number")["title"]    
        .transform(lambda x: x.ffill().bfill())
    )

    has_action = issues["action"].notna()
    issues.loc[has_action, "action"] = "issue " + issues.loc[has_action, "action"]
    issue_comment_mask = issues["action"].isna() & issues["title"].notna()
    issues.loc[issue_comment_mask, "action"] = "issue comment"

    return issues


def LoadAndProcessPullRequests(pr_path, linked_pr_path):
    pr_events = pd.read_parquet(pr_path)[[
        "repo_name","pr_number","created_at","type",
        "pr_action","pr_title","pr_body","actor_id","pr_merged_by_id",
        "pr_review_body","pr_review_comment_body","pr_review_state",
        'pr_assignees', 'pr_requested_reviewers','pr_review_comment_path',
        "pr_review_id","pr_review_comment_commit_id","pr_label",
        "pr_review_comment_position", "pr_review_comment_original_position",
    ]].rename(columns={"pr_number": "thread_number", "pr_action": "action",
       "pr_assignees":"assignees","pr_requested_reviewers":"requested_reviewers","pr_label":"labels"})

    linked_prs = pd.read_parquet(linked_pr_path).rename(columns={"pr_number": "thread_number"})
    pr_events = pr_events.merge(
        linked_prs[["repo_name","thread_number","pull_request_text","pull_request_title"]],
        on=["repo_name","thread_number"], how="left"
    )

    empty_string_cols = ["pr_title","pr_body","pr_merged_by_id","pr_review_body","pr_review_comment_body"]
    pr_events[empty_string_cols] = pr_events[empty_string_cols].replace("", pd.NA)

    empty_string_cols_list = ["assignees", "requested_reviewers"]
    for col in empty_string_cols_list:
        pr_events[col] = pr_events[col].apply(lambda x: ParseJsonList(x, 'id'))
        
    pr_events["pull_request_text"] = pr_events["pull_request_text"].where(pr_events["type"] == "PullRequestEvent")
    pr_events["pr_review_body"] = pr_events["pr_review_body"].where(pr_events["type"] == "PullRequestReviewEvent")
    pr_events["pr_review_comment_body"] = pr_events["pr_review_comment_body"].where(pr_events["type"] == "PullRequestReviewCommentEvent")

    pr_events["title"] = pr_events["pull_request_title"].combine_first(pr_events["pr_title"])
    pr_events["text"] = pr_events["pull_request_text"].combine_first(pr_events["pr_review_body"]).combine_first(pr_events["pr_review_comment_body"])

    pr_events['discussion_id'] = (pr_events['pr_review_comment_path'].fillna('').astype(str) + "_" + pr_events['pr_review_comment_position'].fillna('').astype(str) + "_" + \
        pr_events['pr_review_comment_original_position'].fillna('').astype(str)).where(pr_events['type'] == "PullRequestReviewCommentEvent")
    pr_events.loc[(pr_events["type"] == "PullRequestReviewEvent"), "discussion_id"] = (
        pr_events[(pr_events["type"] == "PullRequestReviewEvent")] # sort by date??
        .groupby("thread_number")
        .cumcount()
        .add(1)
        .astype(str)
        .radd("review")
    )

    mask = (pr_events["type"] == "PullRequestEvent") & (pr_events["action"] != "closed")
    pr_events.loc[mask, "type"] = "pull request " + pr_events.loc[mask, "action"].astype(str)
    pr_events.loc[(pr_events["type"] == "PullRequestEvent") & (pr_events["action"] == "closed") & pr_events["pr_merged_by_id"].isna(), "type"] = "pull request closed"
    pr_events.loc[(pr_events["type"] == "PullRequestEvent") & (pr_events["action"] == "closed") & pr_events["pr_merged_by_id"].notna(), "type"] = "pull request merged"
    pr_events.loc[pr_events["type"] == "PullRequestReviewEvent", "type"] = "pull request review " + pr_events["pr_review_state"].fillna("").apply(lambda v: v.replace("_", " "))
    pr_events.loc[pr_events["type"] == "PullRequestReviewCommentEvent", "type"] = "pull request review comment"


    pr_events = pr_events.drop(columns=[
        "pull_request_text","pull_request_title",
        "pr_body","pr_review_body","pr_review_comment_body","pr_title",
        "pr_review_id","pr_review_comment_commit_id","pr_merged_by_id","pr_review_state","action",
        "pr_review_comment_path","pr_review_comment_original_position","pr_review_comment_position"
    ])
    return pr_events

def AggregateAndCleanIssuesAndPRs(df_issue, df_pr, STATS_PATH, safe_repo_name):
    def _append_stats(stats_row, stats_path):
        stats_path.parent.mkdir(parents=True, exist_ok=True)
        header = not stats_path.exists() or stats_path.stat().st_size == 0
        stats_row.to_csv(stats_path, mode="a", header=header, index=False)

    df_all = pd.concat([df_issue, df_pr])
    df_all = df_all.reset_index(drop = True)
    df_all = df_all.loc[df_all.astype(str).drop_duplicates().index]

    # IssueCommentEvent with issue title already has action "issue comment"
    df_all.loc[(df_all["type"] == "IssueCommentEvent") & df_all["action"].isna() , "action"] = "pull request comment"
    df_all["type"] = df_all["action"].combine_first(df_all["type"])

    df_all, stats_row = RemoveUnidentifiedThreads(df_all, safe_repo_name)
    _append_stats(stats_row, STATS_PATH)

    # Deduplicate text for issues
    issue_opened = df_all[df_all["type"] == "issue opened"][["thread_number","text"]].rename(columns={"text":"opened_text"})
    df_all = df_all.merge(issue_opened, on="thread_number", how="left")
    df_all.loc[((df_all["type"] == "issue closed") | (df_all["type"] == "issue reopened")) & (df_all["text"] == df_all["opened_text"]), "text"] = pd.NA
    df_all = df_all.drop(columns="opened_text")

    # Deduplicate text for pull requests
    pr_opened = df_all[df_all["type"] == "pull request opened"][["thread_number","text"]].rename(columns={"text":"opened_text"})
    df_all = df_all.merge(pr_opened, on="thread_number", how="left")
    mask = ((df_all["type"] == "pull request closed") | (df_all["type"] == "pull request merged") | (df_all["type"] == "pull request reopened")) & (df_all["text"] == df_all["opened_text"])
    df_all.loc[mask, "text"] = pd.NA
    df_all = df_all.drop(columns="opened_text")

    df_all['opener_id'] = df_all['actor_id'].where(df_all['type'].isin(['issue opened','pull request opened']))
    df_all['opener_id'] = df_all.groupby('thread_number')['opener_id'].transform(lambda x: x.ffill().bfill())
    
    df_all = df_all.sort_values(['thread_number','type','created_at']).copy()
    cols = ['assignees','labels','requested_reviewers']
    for col in cols:
        df_all[col] = (
            df_all[col]
            .apply(lambda x: x if (isinstance(x, list) and len(x)>0) else np.nan)
            .groupby(df_all['thread_number'])
            .ffill()
            .apply(lambda x: x if isinstance(x, list) else [])
        )
    df_all = df_all.sort_values(['thread_number','created_at']).copy()
    df_all["discussion_id"] = (
        df_all["thread_number"].astype(int).astype(str)
        + df_all["discussion_id"].fillna("").radd("___").where(df_all["discussion_id"].notna(), "")
    )   
    action_eq = df_all.groupby(['discussion_id']).cumcount()+1
    df_all['action_id'] = df_all['discussion_id'] + "___" + action_eq.astype(str)

    return df_all.drop(columns=["action"])

def RemoveUnidentifiedThreads(df_all, safe_repo_name):
    mask_na_thread = df_all["thread_number"].isna()
    count_na_thread_actions = mask_na_thread.sum()

    df_all["thread_number"] = df_all["thread_number"].astype(float)
    opened_threads = df_all[df_all["type"].str.endswith(" opened")][["repo_name","thread_number"]].drop_duplicates()
    mask_issue_unopened = ~df_all.set_index(["repo_name","thread_number"]).index.isin(opened_threads.set_index(["repo_name","thread_number"]).index)
    count_unopened_thread_actions = mask_issue_unopened.sum()
    count_threads_unopened = df_all.loc[mask_issue_unopened, ["repo_name","thread_number"]].drop_duplicates().shape[0]

    df_all = df_all.loc[~mask_na_thread & ~mask_issue_unopened]

    stats_row = pd.DataFrame([{
        "latest_repo_name": safe_repo_name.replace("___","/"),
        "count_na_thread_actions": count_na_thread_actions,
        "count_unopened_thread_actions": count_unopened_thread_actions,
        "count_threads_unopened": count_threads_unopened,
        "total_threads_remaining": df_all['thread_number'].nunique()
    }])
    df_all["thread_number"] = df_all["thread_number"].astype(int)
    return df_all, stats_row

if __name__ == "__main__":
    RunAllRepos()