import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from collections import defaultdict

from source.lib.JMSLab.SaveData import SaveData


def Main():
    PYPI_DIR = Path("drive/output/scrape/pypi_version_downloads")
    ISSUE_DIR = Path("drive/output/scrape/extract_github_data/repo_level_data/issue")
    PR_DIR = Path("drive/output/scrape/extract_github_data/repo_level_data/pr")

    PYPI_LOG_DIR = Path("output/scrape/pypi_version_downloads")
    GITHUB_LOG_DIR = Path("output/scrape/extract_github_data/repo_level_data")

    PYPI_LOG_DIR.mkdir(parents=True, exist_ok=True)
    GITHUB_LOG_DIR.mkdir(parents=True, exist_ok=True)

    print("Detecting case-sensitive duplicates...")
    issue_files = list(ISSUE_DIR.glob("*.parquet"))
    pr_files = list(PR_DIR.glob("*.parquet"))

    print("\n" + "="*80)
    print("PROCESSING ISSUES")
    print("="*80)
    ResaveGithubData(
        ISSUE_DIR,
        GITHUB_LOG_DIR / "issue" / "logs",
        GITHUB_LOG_DIR / "issue" / "flagged",
        ["issue_number", "type", "created_at", "actor_id"],
        start_category="p",
    )

    print("\n" + "="*80)
    print("PROCESSING PRs")
    print("="*80)
    ResaveGithubData(
        PR_DIR,
        GITHUB_LOG_DIR / "pr" / "logs",
        GITHUB_LOG_DIR / "pr" / "flagged",
        ["pr_number", "type", "created_at", "actor_id"],
    )


def ResaveGithubData(indir, log_dir, flag_dir, keys, start_category=None):
    files = sorted([f for f in indir.glob("*.parquet") if not f.name.startswith("._")])
    print(f"Processing {len(files)} files in {indir.name}...")

    log_dir.mkdir(parents=True, exist_ok=True)
    flag_dir.mkdir(parents=True, exist_ok=True)

    files_by_category = defaultdict(list)
    for f in files:
        category = GetFirstCharCategory(f.name)
        files_by_category[category].append(f)

    print(f"Grouped into {len(files_by_category)} categories")

    for category in sorted(files_by_category.keys()):
        if start_category and category < start_category:
            print(f"[{category}] Skipping (already processed)")
            continue

        ProcessCategory(category, files_by_category[category], log_dir, flag_dir, keys)

    print(f"Done processing {indir.name}")


def ProcessCategory(category, category_files, log_dir, flag_dir, keys):
    log_file = log_dir / f"{category}.log"

    if log_file.exists():
        log_file.unlink()

    print(f"\n[{category}] Processing {len(category_files)} files...")

    category_total_rows = 0
    category_missing = 0
    category_duplicates = 0
    files_with_issues = []

    for i, f in enumerate(category_files):
        df = pd.read_parquet(f)
        original_rows = len(df)
        category_total_rows += original_rows
        file_issues = []

        missing_count = DropMissingKeys(df, keys, f.name, category, flag_dir)
        if missing_count > 0:
            df = df[~df[keys].isna().any(axis=1)].copy()
            category_missing += missing_count
            file_issues.append(f"missing_keys={missing_count}")

        dup_count = HandleDuplicates(df, keys, f.name, category, flag_dir, file_issues)
        category_duplicates += dup_count

        if file_issues:
            files_with_issues.append(f"{f.name}: {', '.join(file_issues)}")

        SaveData(df, keys, f, log_file, append=True)

        if (i + 1) % 100 == 0:
            print(f"  [{category}] Progress: {i + 1}/{len(category_files)} files")

    print(f"[{category}] Complete: {len(category_files)} files, {category_total_rows} rows, {category_missing} missing keys, {category_duplicates} duplicates")
    if files_with_issues:
        print(f"[{category}] Files with issues: {len(files_with_issues)}")
        for issue_desc in files_with_issues[:5]:
            print(f"  - {issue_desc}")
        if len(files_with_issues) > 5:
            print(f"  ... and {len(files_with_issues) - 5} more (see flagged CSVs)")


def HandleDuplicates(df, keys, filename, category, flag_dir, file_issues):
    total_dropped = 0
    dup_mask = df.duplicated(subset=keys, keep=False)

    if not dup_mask.any():
        return 0

    event_types = df['type'].unique()

    for event_type in event_types:
        event_mask = (df['type'] == event_type) & dup_mask
        if not event_mask.any():
            continue

        event_dups = df[event_mask]

        if event_type == 'IssueCommentEvent':
            dropped = HandleIssueCommentDuplicates(df, keys, event_dups, filename, category, flag_dir)
            if dropped > 0:
                total_dropped += dropped
                file_issues.append(f"IssueCommentEvent_dups={dropped}")

        elif event_type in ['IssuesEvent', 'PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent']:
            dropped, perturbed = HandleDropAndPerturbDuplicates(df, keys, event_dups, event_type, filename, category, flag_dir)
            if dropped > 0:
                total_dropped += dropped
                file_issues.append(f"{event_type}_dups={dropped}")
            if perturbed > 0:
                file_issues.append(f"{event_type}_perturbed={perturbed}")

    return total_dropped


def HandleIssueCommentDuplicates(df, keys, event_dups, filename, category, flag_dir):
    if 'issue_comment_id' not in df.columns:
        return 0

    dup_groups = event_dups.groupby(keys, dropna=False)
    rows_to_drop = []

    for _, group in dup_groups:
        if len(group) > 1:
            max_id_idx = group['issue_comment_id'].idxmax()
            drop_idx = group.index[group.index != max_id_idx].tolist()
            rows_to_drop.extend(drop_idx)

    if rows_to_drop:
        RecordDroppedRows(df, rows_to_drop, filename, category, flag_dir,
                         'IssueCommentEvent duplicate - kept latest issue_comment_id')
        df.drop(rows_to_drop, inplace=True)
        return len(rows_to_drop)

    return 0


def HandleDropAndPerturbDuplicates(df, keys, event_dups, event_type, filename, category, flag_dir):
    dup_groups = event_dups.groupby(keys, dropna=False)
    total_dropped = 0
    total_perturbed = 0

    for _, group in dup_groups:
        if len(group) <= 1:
            continue

        unique_group = group.drop_duplicates()

        if len(unique_group) < len(group):
            rows_to_drop = group.index[~group.index.isin(unique_group.index)]
            RecordDroppedRows(df, rows_to_drop, filename, category, flag_dir,
                            f'{event_type} duplicate - all columns identical')
            df.drop(rows_to_drop, inplace=True)
            total_dropped += len(rows_to_drop)

        if len(unique_group) > 1:
            sorted_indices = sorted(unique_group.index.tolist())
            for i, row_idx in enumerate(sorted_indices):
                if i > 0:
                    original_time = pd.to_datetime(df.loc[row_idx, 'created_at'])
                    new_time = original_time + pd.Timedelta(milliseconds=i)
                    df.loc[row_idx, 'created_at'] = str(new_time)
            total_perturbed += len(sorted_indices) - 1

    return total_dropped, total_perturbed

def DropMissingKeys(df, keys, filename, category, flag_dir):
    missing_mask = df[keys].isna().any(axis=1)
    missing_count = missing_mask.sum()

    if missing_count > 0:
        missing_df = df[missing_mask].copy()
        missing_df['source_file'] = filename
        missing_csv = flag_dir / f"{category}_missing_keys.csv"
        missing_df.to_csv(missing_csv, mode='a', header=not missing_csv.exists(), index=False)

    return missing_count


def RecordDroppedRows(df, rows_to_drop, filename, category, flag_dir, reason):
    dropped_df = df.loc[rows_to_drop].copy()
    dropped_df['source_file'] = filename
    dropped_df['reason'] = reason
    dup_csv = flag_dir / f"{category}_duplicates.csv"
    dropped_df.to_csv(dup_csv, mode='a', header=not dup_csv.exists(), index=False)


def GetFirstCharCategory(filename):
    stem = Path(filename).stem
    first_char = stem[0]
    if first_char.isdigit():
        return "numeric"
    return first_char


if __name__ == "__main__":
    Main()
