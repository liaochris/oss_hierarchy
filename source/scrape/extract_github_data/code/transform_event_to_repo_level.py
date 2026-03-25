import glob
from pathlib import Path
import pandas as pd
from source.lib.helpers import MakeRepoNameSafe
from source.lib.JMSLab.SaveData import SaveData

BATCH_SIZE = 10


def Main():
    INDIR = Path("output/scrape/extract_github_data")
    OUTDIR = Path("drive/output/scrape/extract_github_data/repo_level_data")
    LOG_OUTDIR = Path("output/scrape/extract_github_data/repo_level_data")
    OUTDIR.mkdir(parents=True, exist_ok=True)
    LOG_OUTDIR.mkdir(parents=True, exist_ok=True)

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
            ],
            "keys": ["pr_number", "type", "created_at", "actor_id"],
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
            ],
            "keys": ["issue_number", "type", "created_at", "actor_id"],
        },
    ]

    for cfg in categories:
        ProcessCategory(cfg["globs"], repo_lookup, OUTDIR, LOG_OUTDIR,
                        cfg["name"], cfg["keep"], cfg["keys"])


def BuildRepoLookup(repo_lookup_path):
    df = pd.read_csv(repo_lookup_path).query('repo_name != "ERROR"')
    return dict(zip(df["repo_name"], df["latest_repo_name"]))


def ProcessCategory(globs_list, repo_lookup, OUTDIR, LOG_OUTDIR, category_name, keep_cols, keys):
    paths = [p for g in globs_list for p in glob.glob(g)]
    paths = sorted([p for p in paths if Path(p).exists() and Path(p).stat().st_size > 0])

    outdir = OUTDIR / category_name
    outdir.mkdir(parents=True, exist_ok=True)

    log_dir = LOG_OUTDIR / category_name / "logs"
    flag_dir = OUTDIR / category_name / "flagged"
    log_dir.mkdir(parents=True, exist_ok=True)
    flag_dir.mkdir(parents=True, exist_ok=True)

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
        df["repo_name"] = df["repo_name"].str.lower().map(repo_lookup).fillna("unknown_repo").astype("string")

        for repo_name_latest, df_group in df.groupby("repo_name"):
            safe_repo_name = SanitizeRepoName(repo_name_latest)
            if safe_repo_name == "unknown_repo":
                continue
            first_char = safe_repo_name[0]
            file_prefix = first_char if first_char.isalpha() else "numeric"
            log_file = log_dir / f"{file_prefix}.log"
            out_path = outdir / f"{safe_repo_name}.parquet"
            WriteParquetAppend(df_group, out_path, keep_cols, keys, log_file, flag_dir, file_prefix)


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


def WriteParquetAppend(df_group, out_path, keep_cols, keys, log_file, flag_dir, file_prefix):
    new_df = df_group[keep_cols].copy()
    for int_col in ['actor_id', 'repo_id', 'pr_number', 'issue_number',
                    'issue_user_id', 'issue_comment_id']:
        if int_col in new_df.columns:
            new_df[int_col] = pd.to_numeric(new_df[int_col], errors='coerce')

    if out_path.exists():
        existing_df = pd.read_parquet(out_path)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    filename = out_path.name

    missing_count = DropMissingKeys(combined_df, keys, filename, flag_dir, file_prefix)
    if missing_count > 0:
        combined_df = combined_df[~combined_df[keys].isna().any(axis=1)].copy()

    HandleDuplicates(combined_df, keys, filename, flag_dir, file_prefix)

    # Drop any remaining exact-duplicate rows before key uniqueness check
    combined_df = combined_df.drop_duplicates().reset_index(drop=True)

    SaveData(combined_df, keys, out_path, log_file, append=True)


def DropMissingKeys(df, keys, filename, flag_dir, file_prefix):
    missing_mask = df[keys].isna().any(axis=1)
    missing_count = missing_mask.sum()

    if missing_count > 0:
        missing_df = df[missing_mask].copy()
        missing_df['source_file'] = filename
        missing_csv = flag_dir / f"{file_prefix}_missing_keys.csv"
        missing_df.to_csv(missing_csv, mode='a', header=not missing_csv.exists(), index=False)

    return missing_count


def HandleDuplicates(df, keys, filename, flag_dir, file_prefix):
    dup_mask = df.duplicated(subset=keys, keep=False)

    if not dup_mask.any():
        return

    event_types = df['type'].unique()

    for event_type in event_types:
        event_mask = (df['type'] == event_type) & dup_mask
        if not event_mask.any():
            continue

        event_dups = df[event_mask]

        if event_type == 'IssueCommentEvent':
            HandleIssueCommentDuplicates(df, keys, event_dups, filename, flag_dir, file_prefix)

        elif event_type in ['IssuesEvent', 'PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent']:
            HandleDropAndPerturbDuplicates(df, keys, event_dups, event_type, filename, flag_dir, file_prefix)


def HandleIssueCommentDuplicates(df, keys, event_dups, filename, flag_dir, file_prefix):
    if 'issue_comment_id' not in df.columns:
        return

    dup_groups = event_dups.groupby(keys, dropna=False)
    rows_to_drop = []

    for _, group in dup_groups:
        if len(group) > 1:
            max_id_idx = group['issue_comment_id'].idxmax()
            drop_idx = group.index[group.index != max_id_idx].tolist()
            rows_to_drop.extend(drop_idx)

    if rows_to_drop:
        RecordDroppedRows(df, rows_to_drop, filename, flag_dir, file_prefix,
                         'IssueCommentEvent duplicate - kept latest issue_comment_id')
        df.drop(rows_to_drop, inplace=True)


def HandleDropAndPerturbDuplicates(df, keys, event_dups, event_type, filename, flag_dir, file_prefix):
    dup_groups = event_dups.groupby(keys, dropna=False)

    for _, group in dup_groups:
        if len(group) <= 1:
            continue

        unique_group = group.drop_duplicates()

        if len(unique_group) < len(group):
            rows_to_drop = group.index[~group.index.isin(unique_group.index)]
            RecordDroppedRows(df, rows_to_drop, filename, flag_dir, file_prefix,
                            f'{event_type} duplicate - all columns identical')
            df.drop(rows_to_drop, inplace=True)

        if len(unique_group) > 1:
            sorted_indices = sorted(unique_group.index.tolist())
            for i, row_idx in enumerate(sorted_indices):
                if i > 0:
                    original_time = pd.to_datetime(df.loc[row_idx, 'created_at'])
                    new_time = original_time + pd.Timedelta(milliseconds=i)
                    df.loc[row_idx, 'created_at'] = str(new_time)


def RecordDroppedRows(df, rows_to_drop, filename, flag_dir, file_prefix, reason):
    dropped_df = df.loc[rows_to_drop].copy()
    dropped_df['source_file'] = filename
    dropped_df['reason'] = reason
    dup_csv = flag_dir / f"{file_prefix}_duplicates.csv"
    dropped_df.to_csv(dup_csv, mode='a', header=not dup_csv.exists(), index=False)


if __name__ == "__main__":
    Main()
