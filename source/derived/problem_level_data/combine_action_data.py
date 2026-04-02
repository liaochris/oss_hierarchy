import pandas as pd
from pathlib import Path
import concurrent.futures
import json
import numpy as np
import random
from source.lib.JMSLab.SaveData import SaveData
from source.lib.helpers import JsonDeserialize, JsonSerialize, MakeRepoNameSafe

INDIR_REPO = Path('drive/output/scrape/extract_github_data/repo_level_data')
INDIR_LINK = Path('drive/output/scrape/link_issue_pull_request')
INDIR_REPO_LIST = Path('output/scrape/extract_github_data')
OUTDIR = Path('drive/output/derived/problem_level_data/repo_actions')
LOG_DIR = Path('output/derived/problem_level_data/repo_actions')
STATS_DIR = Path("output/derived/problem_level_data")


def Main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    for f in list(OUTDIR.glob("*.parquet")) + list(LOG_DIR.glob("*.log")):
        f.unlink(missing_ok=True)

    TrackRepoComponents()

    issue_repos = {p.stem for p in (INDIR_REPO / "issue").glob("*.parquet") if not p.name.startswith("._")}
    pr_repos = {p.stem for p in (INDIR_REPO / "pr").glob("*.parquet") if not p.name.startswith("._")}
    link_repos = {p.stem for p in (INDIR_LINK / "linked_pull_request_to_issue").glob("*.parquet") if not p.name.startswith("._")}
    repos = sorted(issue_repos & pr_repos & link_repos)

    OUTDIR.mkdir(parents=True, exist_ok=True)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(executor.map(ProcessRepo, repos))

    for msg, _ in results:
        print(msg)

    stats_rows = [stats for _, stats in results if stats is not None]
    if stats_rows:
        stats_df = pd.concat(stats_rows, ignore_index=True).sort_values('latest_repo_name')
        STATS_DIR.mkdir(parents=True, exist_ok=True)
        SaveData(stats_df, ['latest_repo_name'],
                 STATS_DIR / "dropped_stats.csv",
                 STATS_DIR / "dropped_stats.log")


def TrackRepoComponents():
    STATS_DIR.mkdir(parents=True, exist_ok=True)
    repo_list_path = INDIR_REPO_LIST / "repo_id_history_final.csv"

    if not repo_list_path.exists():
        print(f"Repo list not found at {repo_list_path}")
        return

    repo_df = pd.read_csv(repo_list_path)
    unique_repos = repo_df[['latest_repo_name']].drop_duplicates()

    tracking_rows = []
    for _, row in unique_repos.iterrows():
        repo_name = row['latest_repo_name']
        safe_name = MakeRepoNameSafe(repo_name)

        has_issues = int((INDIR_REPO / "issue" / f"{safe_name}.parquet").exists())
        has_prs = int((INDIR_REPO / "pr" / f"{safe_name}.parquet").exists())
        has_linked = int((INDIR_LINK / "linked_pull_request_to_issue" / f"{safe_name}.parquet").exists())

        tracking_rows.append({
            'repo_name': repo_name,
            'has_issues': has_issues,
            'has_prs': has_prs,
            'has_linked': has_linked
        })

    tracking_df = pd.DataFrame(tracking_rows)
    SaveData(tracking_df, ['repo_name'], STATS_DIR / "repo_component_tracking.csv", STATS_DIR / "repo_component_tracking.log")
    n_ready = tracking_df[["has_issues", "has_prs", "has_linked"]].all(axis=1).sum()
    print(f"Component tracking exported: {len(tracking_df)} repos total, {n_ready} ready to process (have issues, PRs, and linked data)")


def ProcessRepo(safe_repo_name):
    issue_path = INDIR_REPO / "issue" / f"{safe_repo_name}.parquet"
    pr_path = INDIR_REPO / "pr" / f"{safe_repo_name}.parquet"
    linked_path = INDIR_LINK / "linked_pull_request_to_issue" / f"{safe_repo_name}.parquet"
    out_path = OUTDIR / f"{safe_repo_name}.parquet"

    if not (issue_path.exists() and pr_path.exists() and linked_path.exists()):
        return f"missing inputs {safe_repo_name}", None
    if out_path.exists():
        return f"already exists {safe_repo_name}", None

    df_issue = LoadAndProcessIssues(issue_path)
    df_pr = LoadAndProcessPullRequests(pr_path, linked_path)
    df_all, stats_row = CombineIssuesAndPRs(df_issue, df_pr, safe_repo_name)

    if df_all.empty:
        return f"empty result {safe_repo_name}", stats_row

    for col in ['assignees', 'labels', 'requested_reviewers', 'issue_link']:
        if col in df_all.columns:
            df_all[col] = df_all[col].apply(JsonSerialize)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    SaveData(df_all, ['action_id'], out_path, LOG_DIR / f"{safe_repo_name}.log")
    return f"exported {safe_repo_name}", stats_row


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
    def extract_text_title(df):
        df['issue_body'] = df['issue_body'].where((df['type'] == "IssuesEvent") & (df['action'] == "opened"))
        df["text"] = df["issue_body"].combine_first(df["issue_comment_body"])
        df["title"] = (
            df.groupby("thread_number")["title"]
            .transform(lambda x: x.ffill().bfill())
        )
        return df.drop(columns=["issue_body", "issue_comment_body"])

    def define_action_type(df):
        has_action = df["action"].notna()
        df.loc[has_action, "action"] = "issue " + df.loc[has_action, "action"]
        
        issue_comment_mask = df["action"].isna() & df["title"].notna()
        df.loc[issue_comment_mask, "action"] = "issue comment"
        return df

    required_columns = [
        'repo_name', "issue_number", "created_at", "type",
        "issue_action",
        "issue_title", "issue_comment_body", "issue_body",
        "actor_id", "latest_issue_assignees", "latest_issue_labels"
    ]

    issues = pd.read_parquet(issue_path)[required_columns].rename(columns={
        "issue_number": "thread_number",
        "issue_title": "title",
        "issue_action": "action",
        "latest_issue_assignees": "assignees",
        "latest_issue_labels": "labels"
    })

    issues = extract_text_title(issues)

    issues["assignees"] = issues["assignees"].apply(lambda x: ParseJsonList(x, 'id'))
    issues["labels"] = issues["labels"].apply(lambda x: ParseJsonList(x, 'name'))
    issues = define_action_type(issues)

    return issues


def LoadAndProcessPullRequests(pr_path, linked_pr_path):
    def merge_linked_data(pr_events, linked_path):
        linked_prs = pd.read_parquet(linked_path).rename(columns={"pr_number": "thread_number"})
        linked_prs["issue_link"] = linked_prs["issue_link"].apply(lambda x: JsonDeserialize(x, default=[]))
        return pr_events.merge(
            linked_prs[["repo_name", "thread_number", "pull_request_text", "pull_request_title", "issue_link"]],
            on=["repo_name", "thread_number"], how="left"
        )

    def extract_text_title(df):
        df["pull_request_text"] = df["pull_request_text"].where(df["type"] == "PullRequestEvent")
        df["pr_review_body"] = df["pr_review_body"].where(df["type"] == "PullRequestReviewEvent")
        df["pr_review_comment_body"] = df["pr_review_comment_body"].where(df["type"] == "PullRequestReviewCommentEvent")

        df["title"] = df["pull_request_title"].combine_first(df["pr_title"])
        df["text"] = df["pull_request_text"].combine_first(df["pr_review_body"]).combine_first(df["pr_review_comment_body"])
        return df

    def create_discussion_id(df):
        # Review comment discussion ID based on file path and position, necessary for uniqueness
        df['discussion_id'] = (
            df['pr_review_comment_path'].fillna('').astype(str) + "_" +
            df['pr_review_comment_position'].fillna('').astype(str) + "_" +
            df['pr_review_comment_original_position'].fillna('').astype(str)
        ).where(df['type'] == "PullRequestReviewCommentEvent")

        # Review discussion ID based on chronological order within thread, necessary for uniqueness
        df = df.sort_values(['thread_number', 'created_at'])
        df.loc[(df["type"] == "PullRequestReviewEvent"), "discussion_id"] = (
            df[(df["type"] == "PullRequestReviewEvent")]
            .groupby("thread_number")
            .cumcount()
            .add(1)
            .astype(str)
            .radd("review")
        )
        return df

    def define_action_type(df):
        mask = (df["type"] == "PullRequestEvent") & (df["action"] != "closed")
        df.loc[mask, "type"] = "pull request " + df.loc[mask, "action"].astype(str)

        df.loc[(df["type"] == "PullRequestEvent") & (df["action"] == "closed") & df["pr_merged_by_id"].isna(), "type"] = "pull request closed"
        df.loc[(df["type"] == "PullRequestEvent") & (df["action"] == "closed") & df["pr_merged_by_id"].notna(), "type"] = "pull request merged"
        df.loc[df["type"] == "PullRequestReviewEvent", "type"] = "pull request review " + df["pr_review_state"].fillna("").apply(lambda v: v.replace("_", " "))
        df.loc[df["type"] == "PullRequestReviewCommentEvent", "type"] = "pull request review comment"
        return df

    pr_events = pd.read_parquet(pr_path)[[
        "repo_name", "pr_number", "created_at", "type",
        "pr_action", "pr_title", "pr_body", "actor_id", "pr_merged_by_id",
        "pr_review_body", "pr_review_comment_body", "pr_review_state",
        'pr_assignees', 'pr_requested_reviewers', 'pr_review_comment_path',
        "pr_review_id", "pr_review_comment_commit_id", "pr_label",
        "pr_review_comment_position", "pr_review_comment_original_position",
    ]].rename(columns={
        "pr_number": "thread_number",
        "pr_action": "action",
        "pr_assignees": "assignees",
        "pr_requested_reviewers": "requested_reviewers",
        "pr_label": "labels"
    })

    pr_events = merge_linked_data(pr_events, linked_pr_path)

    empty_string_cols = ["pr_title", "pr_body", "pr_merged_by_id", "pr_review_body", "pr_review_comment_body"]
    pr_events[empty_string_cols] = pr_events[empty_string_cols].replace("", pd.NA)

    for col in ["assignees", "requested_reviewers"]:
        pr_events[col] = pr_events[col].apply(lambda x: ParseJsonList(x, 'id'))

    pr_events = extract_text_title(pr_events)
    pr_events = create_discussion_id(pr_events)
    pr_events = define_action_type(pr_events)

    pr_events = pr_events.drop(columns=[
        "pull_request_text", "pull_request_title",
        "pr_body", "pr_review_body", "pr_review_comment_body", "pr_title",
        "pr_review_id", "pr_review_comment_commit_id", "pr_merged_by_id", "pr_review_state", "action",
        "pr_review_comment_path", "pr_review_comment_original_position", "pr_review_comment_position"
    ])
    return pr_events


def CombineIssuesAndPRs(df_issue, df_pr, safe_repo_name):
    def fix_mislabeled_issue_comments(df):
        # IssueCommentEvent on PRs should be labeled as "pull request comment"
        # GitHub incorrectly labels comments on PRs as IssueCommentEvent
        df.loc[(df["type"] == "IssueCommentEvent") & df["action"].isna(), "action"] = "pull request comment"
        # Replace generic GitHub event types with descriptive action types where available
        df["type"] = df["action"].combine_first(df["type"])
        return df

    def remove_threads_without_opener(df, safe_repo_name):
        mask_na_thread = df["thread_number"].isna()
        count_na_thread_actions = mask_na_thread.sum()

        df["thread_number"] = df["thread_number"].astype(float)
        opened_threads = df[df["type"].str.endswith(" opened")][["repo_name", "thread_number"]].drop_duplicates()

        opened_idx = pd.MultiIndex.from_frame(opened_threads[["repo_name", "thread_number"]])
        mask_without_opener = ~pd.MultiIndex.from_frame(df[["repo_name", "thread_number"]]).isin(opened_idx)

        count_unopened_thread_actions = mask_without_opener.sum()
        count_threads_unopened = df.loc[mask_without_opener, ["repo_name", "thread_number"]].drop_duplicates().shape[0]

        df_filtered = df.loc[~mask_na_thread & ~mask_without_opener].copy()
        df_filtered["thread_number"] = df_filtered["thread_number"].astype(int)

        stats_row = pd.DataFrame([{
            "latest_repo_name": safe_repo_name.replace("___", "/"),
            "count_na_thread_actions": count_na_thread_actions,
            "count_unopened_thread_actions": count_unopened_thread_actions,
            "count_threads_unopened": count_threads_unopened,
            "total_threads_remaining": df_filtered['thread_number'].nunique()
        }])
        return df_filtered, stats_row

    def deduplicate_opening_text(df, opened_type, action_types_to_clean):
        # Keep earliest opener per thread (duplicate PR open events with different repo IDs due to repo ID changes)
        opened_df = (df[df["type"] == opened_type].sort_values("created_at")
                     .drop_duplicates("thread_number", keep="first")
                     [["thread_number", "text"]].rename(columns={"text": "opened_text"}))
        df = df.merge(opened_df, on="thread_number", how="left")
        mask = df["type"].isin(action_types_to_clean) & (df["text"] == df["opened_text"])
        df.loc[mask, "text"] = pd.NA
        return df.drop(columns="opened_text")

    def propagate_opener_id(df):
        df['opener_id'] = df['actor_id'].where(df['type'].isin(['issue opened', 'pull request opened']))
        df['opener_id'] = df.groupby('thread_number')['opener_id'].transform(lambda x: x.ffill().bfill())
        return df

    def forward_fill_list_columns(df, cols):
        df = df.sort_values(['thread_number', 'type', 'created_at']).copy()
        for col in cols:
            df[col] = (
                df[col]
                .apply(lambda x: x if (isinstance(x, list) and len(x) > 0) else np.nan)
                .groupby(df['thread_number'])
                .ffill()
                .apply(lambda x: x if isinstance(x, list) else [])
            )
        return df.sort_values(['thread_number', 'created_at']).copy()

    def create_action_id(df):
        df["discussion_id"] = (
            df["thread_number"].astype(int).astype(str)
            + df["discussion_id"].fillna("").radd("___").where(df["discussion_id"].notna(), "")
        )
        action_seq = df.groupby(['discussion_id']).cumcount() + 1
        df['action_id'] = df['discussion_id'] + "___" + action_seq.astype(str)
        return df

    df_combined = pd.concat([df_issue, df_pr])
    df_combined = df_combined.reset_index(drop=True)
    # Deduplicate rows that are identical
    df_deduped = df_combined.loc[df_combined.astype(str).drop_duplicates().index]

    df_deduped = fix_mislabeled_issue_comments(df_deduped)

    df_filtered, stats_row = remove_threads_without_opener(df_deduped, safe_repo_name)

    # Remove duplicate text from close/reopen events that repeat the opening text
    df_filtered = deduplicate_opening_text(
        df_filtered,
        "issue opened",
        ["issue closed", "issue reopened"]
    )
    df_filtered = deduplicate_opening_text(
        df_filtered,
        "pull request opened",
        ["pull request closed", "pull request merged", "pull request reopened"]
    )

    df_filtered = propagate_opener_id(df_filtered)
    df_filtered = forward_fill_list_columns(df_filtered, ['assignees', 'labels', 'requested_reviewers'])
    df_filtered = create_action_id(df_filtered)

    return df_filtered.drop(columns=["action"]), stats_row


if __name__ == "__main__":
    Main()
