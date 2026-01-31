import os
import glob
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from source.lib.helpers import MakeRepoNameSafe

BATCH_SIZE = 10

def WriteParquetAppend(df_group, out_path, keep_cols):
    new_df = df_group[keep_cols]
    for int_col in ['actor_id', 'repo_id', 'pr_number', 'issue_number', 
                    'issue_user_id', 'issue_comment_id']:
        if int_col in new_df.columns:
            new_df[int_col] = pd.to_numeric(new_df[int_col],errors = 'coerce')

    if out_path.exists():
        existing_df = pd.read_parquet(out_path)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    combined_df.to_parquet(out_path, engine="pyarrow", index=False)


def ReadRawGitHubData(path, keep_cols):
    df = pd.read_csv(path, engine="python", escapechar="\\",      
                     on_bad_lines="skip", encoding="utf-8", keep_default_na=False)
    for c in keep_cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df[keep_cols]

def SanitizeRepoName(repo_name):
    if repo_name is None or pd.isna(repo_name):
        return "unknown_repo"

    s = str(repo_name).strip()
    try:
        pd.to_datetime(s, utc=True)
        return "unknown_repo"
    except Exception:
        return MakeRepoNameSafe(s)

    
def BuildRepoLookup(repo_lookup_path):
    df = pd.read_csv(repo_lookup_path).query('repo_name != "ERROR"')
    return dict(zip(df["repo_name"], df["latest_repo_name"]))

def ProcessCategory(globs_list, repo_lookup, OUTDIR, category_name, keep_cols):
    paths = [p for g in globs_list for p in glob.glob(g)]
    paths = sorted([p for p in paths if Path(p).exists() and Path(p).stat().st_size > 0])

    outdir = Path(OUTDIR) / category_name
    outdir.mkdir(parents=True, exist_ok=True)

    for i in range(0, len(paths), BATCH_SIZE):
        batch_paths = paths[i : i + BATCH_SIZE]
        print(batch_paths)
        dfs = []

        for p in batch_paths:
            try:
                df = ReadRawGitHubData(p, keep_cols)
            except Exception as e:
                print(f"[{category_name}] Failed to read {p}: {e}")
                continue
            if df is None or df.empty:
                continue
            dfs.append(df)

        if not dfs:
            continue

        df = pd.concat(dfs, ignore_index=True, copy=False)
        df["repo_name"] = df["repo_name"].map(repo_lookup).fillna("unknown_repo").astype("string")

        for repo_name_latest, df_group in df.groupby("repo_name"):
            safe_repo_name = SanitizeRepoName(repo_name_latest)
            if safe_repo_name == "unknown_repo":
                continue
            out_path = outdir / f"{safe_repo_name}.parquet"
            WriteParquetAppend(df_group, out_path, keep_cols)

def Main():
    INDIR = Path("output/scrape/extract_github_data")
    OUTDIR = Path("drive/output/scrape/extract_github_data/repo_level_data")
    OUTDIR.mkdir(parents=True, exist_ok=True)

    repo_lookup_path = INDIR / "repo_id_history_final.csv"
    repo_lookup = BuildRepoLookup(repo_lookup_path)

    categories = [
        {
            "name": "pr",
            "globs": [
                "drive/output/scrape/extract_github_data/event_level_data/pull_request_data/*.csv",
                "drive/output/scrape/extract_github_data/event_level_data/pull_request_review_data/*.csv",
                "drive/output/scrape/extract_github_data/event_level_data/pull_request_review_comment_data/*.csv",
            ],
            "keep": [
                "type", "created_at", "repo_id", "repo_name", "actor_id", "actor_login",
                "pr_number", "pr_title", "pr_body", "pr_action",
                "pr_merged_by_id", "pr_merged_by_type", "pr_label",
                "pr_review_action", "pr_review_id", "pr_review_state", "pr_review_body",
                "pr_review_comment_body", "pr_review_comment_position",
                "pr_review_comment_original_position",
                "pr_review_comment_original_commit_id", "pr_review_comment_commit_id",
                "pr_review_comment_path", "pr_assignee", "pr_assignees",
                "pr_requested_reviewers",
            ]
        },
        {
            "name": "issue",
            "globs": [
                "drive/output/scrape/extract_github_data/event_level_data/issue_data/*.csv",
                "drive/output/scrape/extract_github_data/event_level_data/issue_comment_data/*.csv",
            ],
            "keep": [
                "type", "created_at", "repo_id", "repo_name", "actor_id", "actor_login",
                "issue_number", "issue_body", "issue_title", "issue_action", "issue_state",
                "issue_comment_id", "issue_user_id", "issue_comment_body",
                "latest_issue_assignees", "latest_issue_assignee", "latest_issue_labels", "actor_type",
            ]
        },
    ]

    for cfg in categories:
        ProcessCategory(cfg["globs"], repo_lookup, OUTDIR,
                        cfg["name"], cfg["keep"])

if __name__ == "__main__":
    Main()
