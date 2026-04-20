import concurrent.futures
import glob
import re
import shutil
import sys
from pathlib import Path

import pandas as pd

from source.lib.helpers import LoadGlobalSettings, MakeRepoNameSafe
from source.lib.JMSLab.SaveData import SaveData


INTEGER_COLUMNS = ["actor_id", "repo_id", "pr_number", "issue_number", "issue_user_id", "issue_comment_id", "pr_review_id", "pr_merged_by_id"]
DUPLICATE_EVENT_TYPES = {"IssuesEvent", "PullRequestEvent", "PullRequestReviewEvent", "PullRequestReviewCommentEvent"}
INDIR = Path("output/scrape/extract_github_data")
OUTDIR = Path("drive/output/scrape/extract_github_data/repo_level_data")
OUTDIR_LOG = Path("output/scrape/extract_github_data/repo_level_data")
STAGE_ROOT = Path("drive/temp/scrape/extract_github_data/repo_level_data_stage")
CHUNK_SIZE = 100_000
PARQUET_COMPRESSION = "snappy"
CATEGORIES = [
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
            "pr_review_comment_body", "pr_review_comment_position", "pr_review_comment_original_position",
            "pr_review_comment_original_commit_id", "pr_review_comment_commit_id", "pr_review_comment_path",
            "pr_assignee", "pr_assignees", "pr_requested_reviewers",
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
READ_CSV_KWARGS = {
    "escapechar": "\\",
    "on_bad_lines": "skip",
    "encoding": "utf-8",
    "keep_default_na": False,
    "low_memory": False,
}
_REPO_NAME_RE = re.compile(r"^[^/]+/[^/]+$")


def Main():
    for path in [INDIR, OUTDIR, OUTDIR_LOG, STAGE_ROOT]:
        path.mkdir(parents=True, exist_ok=True)

    repo_lookup = BuildRepoLookup(INDIR / "repo_id_history_final.csv")
    max_workers = LoadGlobalSettings()["n_jobs"]
    for cfg in CATEGORIES:
        ProcessCategory(cfg, repo_lookup, max_workers)


def BuildRepoLookup(repo_lookup_path):
    df = pd.read_csv(repo_lookup_path).query('repo_name != "ERROR"')
    repo_names = df["repo_name"].astype("string").str.lower()
    latest_repo_names = df["latest_repo_name"].astype("string")
    return dict(zip(repo_names, latest_repo_names))


def ProcessCategory(cfg, repo_lookup, max_workers):
    category_name = cfg["name"]
    keep_cols = cfg["keep"]
    keys = cfg["keys"]
    paths = DiscoverPaths(cfg["globs"])
    outdir = OUTDIR / category_name
    log_dir = OUTDIR_LOG / category_name / "logs"
    flag_dir = OUTDIR / category_name / "flagged"
    parts_dir = STAGE_ROOT / f"{category_name}_parts"
    export_dir = STAGE_ROOT / f"{category_name}_export"
    missing_parts_dir = flag_dir / "_missing_parts"
    dup_parts_dir = flag_dir / "_dup_parts"

    for path in [outdir, log_dir, flag_dir]:
        path.mkdir(parents=True, exist_ok=True)
    for csv_path in [flag_dir / "missing_keys.csv", flag_dir / "duplicates.csv"]:
        csv_path.unlink(missing_ok=True)
    for temp_dir in [parts_dir, export_dir, missing_parts_dir, dup_parts_dir]:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
    if not paths:
        return

    parts_dir.mkdir(parents=True, exist_ok=True)
    stage_workers = min(max_workers, len(paths))
    print(f"[{category_name}] staging {len(paths)} source files across {stage_workers} workers")
    repo_names = BuildRepoParts(paths, keep_cols, repo_lookup, parts_dir, category_name, stage_workers)
    if not repo_names:
        shutil.rmtree(parts_dir, ignore_errors=True)
        return

    export_dir.mkdir(parents=True, exist_ok=True)
    merge_workers = min(max_workers, len(repo_names))
    print(f"[{category_name}] exporting {len(repo_names)} repos across {merge_workers} workers")
    MergeRepoParts(parts_dir, export_dir, repo_names, keep_cols, category_name, merge_workers)
    shutil.rmtree(parts_dir, ignore_errors=True)

    for path in [missing_parts_dir, dup_parts_dir]:
        path.mkdir(parents=True, exist_ok=True)

    finalize_workers = min(max_workers, len(repo_names))
    finalize_jobs = [
        (
            repo_name,
            str(export_dir / f"{repo_name}.parquet"),
            str(outdir / f"{repo_name}.parquet"),
            str(log_dir / f"{repo_name}.log"),
            keys,
            str(missing_parts_dir),
            str(dup_parts_dir),
        )
        for repo_name in repo_names
    ]
    print(f"[{category_name}] finalizing {len(repo_names)} repos across {finalize_workers} workers")
    RunParallel(
        FinalizeRepoData,
        finalize_jobs,
        finalize_workers,
        progress_items=repo_names,
        progress_label=f"{category_name} finalize",
    )

    CombineFlaggedParts(missing_parts_dir, flag_dir / "missing_keys.csv")
    CombineFlaggedParts(dup_parts_dir, flag_dir / "duplicates.csv")
    shutil.rmtree(missing_parts_dir, ignore_errors=True)
    shutil.rmtree(dup_parts_dir, ignore_errors=True)
    shutil.rmtree(export_dir, ignore_errors=True)
    print(f"[{category_name}] done")


def RunParallel(fn, jobs, max_workers, progress_items=None, progress_label=None):
    if max_workers <= 0 or not jobs:
        return []
    progress_items = progress_items or [""] * len(jobs)

    def Report(futures):
        total = len(futures)
        completed = 0
        results = [None] * total
        for future in concurrent.futures.as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()
            completed += 1
            if progress_label:
                item = progress_items[idx]
                suffix = f": {item}" if item else ""
                print(f"[{progress_label}] {completed}/{total}{suffix}")
        return results

    main = sys.modules.get("__main__")
    use_threads = main is not None and not hasattr(main, "__file__")

    if not use_threads:
        try:
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(fn, *job): i for i, job in enumerate(jobs)}
                return Report(futures)
        except (OSError, PermissionError):
            use_threads = True

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fn, *job): i for i, job in enumerate(jobs)}
        return Report(futures)


def DiscoverPaths(globs_list):
    return sorted(
        path
        for pattern in globs_list
        for path in map(Path, glob.glob(pattern))
        if not path.name.startswith("._") and path.exists() and path.stat().st_size > 0
    )


def BuildRepoParts(paths, keep_cols, repo_lookup, parts_dir, category_name, max_workers):
    results = StageChunkedCategoryData(paths, keep_cols, repo_lookup, parts_dir, category_name, max_workers)
    return sorted({repo_name for repo_names in results for repo_name in repo_names})


def StageChunkedCategoryData(paths, keep_cols, repo_lookup, parts_dir, category_name, max_workers):
    jobs = [
        (str(path), keep_cols, repo_lookup, str(parts_dir), idx, CHUNK_SIZE)
        for idx, path in enumerate(paths, start=1)
    ]
    return RunParallel(
        StageOneChunkedFile,
        jobs,
        max_workers,
        progress_items=[path.name for path in paths],
        progress_label=f"{category_name} stage",
    )


def StageOneChunkedFile(path_str, keep_cols, repo_lookup, parts_dir_str, source_idx, chunk_size):
    return StageSourceFile(Path(path_str), keep_cols, repo_lookup, Path(parts_dir_str), source_idx, chunk_size)


def StageSourceFile(path, keep_cols, repo_lookup, parts_dir, source_idx, chunk_size):
    try:
        rows_written, repo_names, rows_seen = StageSourceFileOnce(
            path, keep_cols, repo_lookup, parts_dir, source_idx, chunk_size, force_python=False,
        )
    except pd.errors.ParserError:
        RemoveSourceParts(parts_dir, source_idx)
        try:
            rows_written, repo_names, _ = StageSourceFileOnce(
                path, keep_cols, repo_lookup, parts_dir, source_idx, chunk_size, force_python=True,
            )
        except Exception:
            RemoveSourceParts(parts_dir, source_idx)
            return []
        return sorted(repo_names)
    if rows_seen == 0 or (rows_seen > 0 and rows_written == 0):
        RemoveSourceParts(parts_dir, source_idx)
        try:
            rows_written, repo_names, _ = StageSourceFileOnce(
                path, keep_cols, repo_lookup, parts_dir, source_idx, chunk_size, force_python=True,
            )
        except Exception:
            RemoveSourceParts(parts_dir, source_idx)
            return []
    return sorted(repo_names)


def RemoveSourceParts(parts_dir, source_idx):
    for old_part in parts_dir.glob(f"*/{source_idx:05d}_*.parquet"):
        old_part.unlink(missing_ok=True)


def StageSourceFileOnce(path, keep_cols, repo_lookup, parts_dir, source_idx, chunk_size, force_python):
    import pyarrow.parquet as pq

    schema = GetArrowSchema(keep_cols)
    rows_seen = 0
    rows_written = 0
    repo_names = set()
    for chunk_idx, chunk in enumerate(ReadRawGitHubChunks(path, keep_cols, chunk_size, force_python), start=1):
        rows_seen += len(chunk)
        staged_chunk = PrepareSourceChunk(chunk, keep_cols, repo_lookup)
        if staged_chunk is None or staged_chunk.empty:
            continue
        for repo_name, repo_df in staged_chunk.groupby("safe_repo_name", sort=False, dropna=False):
            if repo_name is None or pd.isna(repo_name):
                continue
            repo_dir = parts_dir / str(repo_name)
            repo_dir.mkdir(parents=True, exist_ok=True)
            table = MakeArrowTable(repo_df.drop(columns=["safe_repo_name"]).reset_index(drop=True), schema)
            pq.write_table(table, repo_dir / f"{source_idx:05d}_{chunk_idx:05d}.parquet", compression=PARQUET_COMPRESSION)
            repo_names.add(str(repo_name))
        rows_written += len(staged_chunk)
    return rows_written, repo_names, rows_seen


def ReadRawGitHubChunks(path, keep_cols, chunk_size, force_python=False):
    import csv as _csv
    _csv.field_size_limit(10_000_000)
    kwargs = {**READ_CSV_KWARGS, "usecols": lambda col: col in keep_cols, "chunksize": chunk_size}
    python_kwargs = {k: v for k, v in kwargs.items() if k != "low_memory"}
    if force_python:
        reader = pd.read_csv(path, engine="python", **python_kwargs)
    else:
        try:
            reader = pd.read_csv(path, **kwargs)
        except Exception:
            reader = pd.read_csv(path, engine="python", **python_kwargs)
    for chunk in reader:
        for col in keep_cols:
            if col not in chunk.columns:
                chunk[col] = pd.NA
        yield chunk[keep_cols]


def PrepareSourceChunk(df, keep_cols, repo_lookup):
    if df.empty:
        return None

    repo_name = df["repo_name"].astype("string").str.lower().map(repo_lookup).fillna("unknown_repo")
    safe_repo_name = repo_name.map(SanitizeRepoName)
    keep_mask = safe_repo_name != "unknown_repo"
    if not keep_mask.any():
        return None

    out = pd.DataFrame({"safe_repo_name": safe_repo_name[keep_mask].astype("string").reset_index(drop=True)})
    integer_set = set(INTEGER_COLUMNS)
    for col in keep_cols:
        series = repo_name[keep_mask] if col == "repo_name" else df.loc[keep_mask, col]
        if col in integer_set:
            out[col] = pd.to_numeric(series, errors="coerce").astype("Float64").astype("Int64").reset_index(drop=True)
        else:
            out[col] = series.astype("string").reset_index(drop=True)
    return out


def GetArrowSchema(keep_cols):
    import pyarrow as pa

    integer_set = set(INTEGER_COLUMNS)
    fields = []
    for col in keep_cols:
        if col in integer_set:
            fields.append(pa.field(col, pa.int64()))
        else:
            fields.append(pa.field(col, pa.string()))
    return pa.schema(fields)


def MakeArrowTable(df, schema):
    import pyarrow as pa

    return pa.Table.from_pandas(df, schema=schema, preserve_index=False).replace_schema_metadata(None)


def MergeRepoParts(parts_dir, export_dir, repo_names, keep_cols, category_name, max_workers):
    jobs = [(repo_name, str(parts_dir), str(export_dir), keep_cols) for repo_name in repo_names]
    RunParallel(
        MergeOneRepoParts,
        jobs,
        max_workers,
        progress_items=repo_names,
        progress_label=f"{category_name} export",
    )


def MergeOneRepoParts(repo_name, parts_dir_str, export_dir_str, keep_cols):
    import pyarrow.parquet as pq

    repo_dir = Path(parts_dir_str) / repo_name
    part_paths = sorted(repo_dir.glob("*.parquet"))
    if not part_paths:
        return 0

    export_path = Path(export_dir_str) / f"{repo_name}.parquet"
    schema = GetArrowSchema(keep_cols)
    writer = pq.ParquetWriter(str(export_path), schema, compression=PARQUET_COMPRESSION)
    try:
        for part_path in part_paths:
            table = pq.read_table(str(part_path)).cast(schema).replace_schema_metadata(None)
            writer.write_table(table)
    finally:
        writer.close()
    return len(part_paths)


def FinalizeRepoData(safe_repo_name, export_path_str, out_path_str, log_file_str, keys, missing_parts_dir_str, dup_parts_dir_str):
    combined_df = pd.read_parquet(export_path_str)
    if combined_df.empty:
        return 0

    out_path = Path(out_path_str)
    log_file = Path(log_file_str)
    missing_parts_dir = Path(missing_parts_dir_str)
    dup_parts_dir = Path(dup_parts_dir_str)
    filename = out_path.name

    missing_df = DropMissingKeys(combined_df, keys, filename)
    if not missing_df.empty:
        combined_df = combined_df[~combined_df[keys].isna().any(axis=1)].copy()
        missing_df.to_parquet(missing_parts_dir / f"{safe_repo_name}.parquet", index=False)

    duplicate_df = HandleDuplicates(combined_df, keys, filename)
    if not duplicate_df.empty:
        duplicate_df.to_parquet(dup_parts_dir / f"{safe_repo_name}.parquet", index=False)

    combined_df = combined_df.drop_duplicates().reset_index(drop=True)
    SaveData(combined_df, keys, out_path, log_file)
    return len(combined_df)


def CombineFlaggedParts(parts_dir, out_csv):
    part_paths = sorted(parts_dir.glob("*.parquet"))
    if not part_paths:
        return

    with out_csv.open("w", encoding="utf-8", newline="") as fh:
        wrote_header = False
        for part_path in part_paths:
            df = pd.read_parquet(part_path)
            if df.empty:
                continue
            df.to_csv(fh, index=False, header=not wrote_header)
            wrote_header = True


def SanitizeRepoName(repo_name):
    if repo_name is None or pd.isna(repo_name):
        return "unknown_repo"
    s = str(repo_name).strip()
    if not _REPO_NAME_RE.match(s):
        return "unknown_repo"
    return MakeRepoNameSafe(s)


def DropMissingKeys(df, keys, filename):
    missing_mask = df[keys].isna().any(axis=1)
    if not missing_mask.any():
        return pd.DataFrame()
    missing_df = df[missing_mask].copy()
    missing_df["source_file"] = filename
    return missing_df


def HandleDuplicates(df, keys, filename):
    dup_mask = df.duplicated(subset=keys, keep=False)
    if not dup_mask.any():
        return pd.DataFrame()

    dropped_frames = []
    for event_type in df.loc[dup_mask, "type"].unique():
        event_dups = df[(df["type"] == event_type) & dup_mask]
        if event_type == "IssueCommentEvent":
            dropped_df = HandleIssueCommentDuplicates(df, keys, event_dups, filename)
        elif event_type in DUPLICATE_EVENT_TYPES:
            dropped_df = HandleDropAndPerturbDuplicates(df, keys, event_dups, event_type, filename)
        else:
            dropped_df = pd.DataFrame()
        if not dropped_df.empty:
            dropped_frames.append(dropped_df)
    return pd.concat(dropped_frames, ignore_index=True) if dropped_frames else pd.DataFrame()


def HandleIssueCommentDuplicates(df, keys, event_dups, filename):
    if "issue_comment_id" not in df.columns:
        return pd.DataFrame()
    rows_to_drop = []
    for _, group in event_dups.groupby(keys, dropna=False):
        if len(group) > 1:
            keep_idx = group["issue_comment_id"].idxmax()
            rows_to_drop.extend(group.index[group.index != keep_idx].tolist())
    if not rows_to_drop:
        return pd.DataFrame()
    dropped_df = RecordDroppedRows(df, rows_to_drop, filename, "IssueCommentEvent duplicate - kept latest issue_comment_id")
    df.drop(rows_to_drop, inplace=True)
    return dropped_df


def HandleDropAndPerturbDuplicates(df, keys, event_dups, event_type, filename):
    dropped_frames = []
    for _, group in event_dups.groupby(keys, dropna=False):
        if len(group) <= 1:
            continue
        unique_group = group.drop_duplicates()
        if len(unique_group) < len(group):
            rows_to_drop = group.index[~group.index.isin(unique_group.index)]
            dropped_frames.append(RecordDroppedRows(df, rows_to_drop, filename, f"{event_type} duplicate - all columns identical"))
            df.drop(rows_to_drop, inplace=True)
        if len(unique_group) > 1:
            for i, row_idx in enumerate(sorted(unique_group.index.tolist())[1:], start=1):
                df.loc[row_idx, "created_at"] = str(pd.to_datetime(df.loc[row_idx, "created_at"]) + pd.Timedelta(milliseconds=i))
    return pd.concat(dropped_frames, ignore_index=True) if dropped_frames else pd.DataFrame()


def RecordDroppedRows(df, rows_to_drop, filename, reason):
    dropped_df = df.loc[rows_to_drop].copy()
    dropped_df["source_file"] = filename
    dropped_df["reason"] = reason
    return dropped_df


if __name__ == "__main__":
    Main()
