import pandas as pd
import numpy as np
import re
import os
import time
from pandarallel import pandarallel
from source.lib.helpers import *
from joblib import Parallel, delayed
from pathlib import Path
import random

pandarallel.initialize(progress_bar=True)

fenced_code_block_pattern = re.compile(r'```[\s\S]*?```')
github_url_pattern = re.compile(r'https:\/\/github\.com\/([^\/\s]+)\/([^\/\s]+)\/(pull|issues)\/(\d+)')
path_ref_pattern = re.compile(r'([a-zA-Z0-9_.-]*[a-zA-Z][a-zA-Z0-9_.-]*/[a-zA-Z0-9_.-]*[a-zA-Z][a-zA-Z0-9_.-]*)#(\d+)\w*')
plain_ref_pattern = re.compile(r' #\d+\b')


# ---------------------------
# Main parallel driver
# ---------------------------
def Main():
    indir = Path("drive/output/scrape/link_issue_pull_request")
    indir_derived = Path("drive/output/derived/data_export")
    outdir = Path("drive/output/derived/construct_problems/strict_links")
    outdir.mkdir(parents=True, exist_ok=True)

    repos = GetRepos(indir, indir_derived)
    random.shuffle(repos)
    repos = [repo for repo in repos if not (outdir / f"{repo}.parquet").exists()]
    print(f"🔎 Found {len(repos)} repos to process")

    # use half the available threads
    n_jobs = max(1, (os.cpu_count() or 1) // 2)
    print(f"⚡ Using {n_jobs} parallel workers")

    results = Parallel(n_jobs=n_jobs)(
        delayed(ProcessRepo)(repo, indir, indir_derived, outdir) for repo in repos
    )

    if not all(results):
        print("❗ Aborting: at least one repo failed")
        raise SystemExit(1)

    print("🎉 All repos processed successfully")

def GetRepos(indir, indir_derived):

    dirs = [
        indir / "linked_issue_to_pull_request",
        indir / "linked_pull_request_to_issue",
        indir_derived / "issue",
        indir_derived / "pr"
    ]

    repo_sets = []
    for d in dirs:
        repos_here = {os.path.splitext(f.name)[0] for f in d.glob("*.parquet")}
        print(f"📂 {d}: {len(repos_here)} repos")
        repo_sets.append(repos_here)

    repos_all = set.intersection(*repo_sets)
    print(f"✅ Repos in all 4 dirs: {len(repos_all)}")

    return sorted(repos_all)

# ---------------------------
# Per-repo processing
# ---------------------------
def ProcessRepo(repo: str, indir: Path, indir_derived: Path, outdir: Path):
    output_file = outdir / f"{repo}.parquet"
    if output_file.exists():
        return True  # success

    print(f"🚀 Starting {repo}...")
    start = time.time()
    indir_repo_match = Path("output/scrape/extract_github_data")
    repo_df = pd.read_csv(indir_repo_match / "repo_id_history_filtered.csv")

    try:
        df_issue_linked_raw = pd.read_parquet(indir / "linked_issue_to_pull_request" / f"{repo}.parquet")
        df_pr_linked_raw = pd.read_parquet(indir / "linked_pull_request_to_issue" / f"{repo}.parquet")

        df_issue_linked = CleanLinkedIssue(df_issue_linked_raw) if not df_issue_linked_raw.empty else pd.DataFrame()
        df_pr_linked = CleanLinkedPR(df_pr_linked_raw) if not df_pr_linked_raw.empty else pd.DataFrame()

        df_issue_raw = pd.read_parquet(
            indir_derived / "issue" / f"{repo}.parquet",
            columns=['repo_name', 'issue_number', 'issue_title', 'issue_body', 'issue_comment_body'],
            engine='pyarrow',
        )
        df_pr = pd.read_parquet(
            indir_derived / "pr" / f"{repo}.parquet",
            columns=['repo_name', 'pr_number', 'pr_title', 'pr_body', 'pr_review_body', 'pr_review_comment_body'],
            engine='pyarrow',
        )

        if df_pr.empty and df_issue_raw.empty:
            # Nothing to process
            output_file.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame().to_parquet(output_file, index=False)
        else:
            repo_names = df_pr['repo_name'].dropna().unique().tolist() + df_issue_raw['repo_name'].dropna().unique().tolist()
            repo_name_dict = {repo_name: GetLatestRepoName(repo_name, repo_df) for repo_name in repo_names}
            if not df_pr.empty:
                df_pr['repo_name_latest'] = df_pr['repo_name'].map(repo_name_dict)
            if not df_issue_raw.empty:
                df_issue_raw['repo_name_latest'] = df_issue_raw['repo_name'].map(repo_name_dict)

            if not df_pr.empty:
                pr_index = (
                    df_pr[['repo_name', 'pr_number', 'repo_name_latest']]
                    .drop_duplicates()
                    .set_index(['repo_name', 'pr_number', 'repo_name_latest'])
                    .index
                )
            else:
                pr_index = pd.Index([])

            df_issue = df_issue_raw.loc[~df_issue_raw.set_index(['repo_name', 'issue_number', 'repo_name_latest']).index.isin(pr_index)] if not df_issue_raw.empty else pd.DataFrame()
            df_pr_comments = df_issue_raw.loc[df_issue_raw.set_index(['repo_name', 'issue_number', 'repo_name_latest']).index.isin(pr_index)] if not df_issue_raw.empty else pd.DataFrame()

            targets = [
                (df_issue, ["issue_title", "issue_body", "issue_comment_body"]),
                (df_pr, ["pr_title", "pr_body", "pr_review_body", "pr_review_comment_body"]),
                (df_pr_comments, ["issue_title", "issue_body", "issue_comment_body"]),
                (df_pr_linked_raw, ["pull_request_title", "pull_request_text"]),
            ]

            for df, cols in targets:
                if df.empty:
                    continue
                df[["github_urls", "issue_pr_refs"]] = ExtractGithubData(df, cols)

            pr_sources = []
            if not df_pr.empty:
                pr_sources.append(df_pr[['repo_name', 'repo_name_latest','pr_number','github_urls','issue_pr_refs']])
            if not df_pr_comments.empty:
                pr_sources.append(df_pr_comments.rename(columns={'issue_number':'pr_number'})[['repo_name', 'repo_name_latest','pr_number','github_urls','issue_pr_refs']])
            if not df_pr_linked_raw.empty:
                pr_sources.append(df_pr_linked_raw[['repo_name', 'repo_name_latest','pr_number','github_urls','issue_pr_refs']])

            df_pr_all = pd.concat(pr_sources, ignore_index=True) if pr_sources else pd.DataFrame()

            df_issue_text_ref = (
                df_issue.groupby(['repo_name', 'repo_name_latest','issue_number'])[['github_urls','issue_pr_refs']]
                .agg(RemoveDuplicatesFlattened)
                .reset_index()
            ) if not df_issue.empty else pd.DataFrame()

            df_pr_text_ref = (
                df_pr_all.groupby(['repo_name', 'repo_name_latest','pr_number'])[['github_urls','issue_pr_refs']]
                .agg(RemoveDuplicatesFlattened)
                .reset_index()
            ) if not df_pr_all.empty else pd.DataFrame()

            df_issue_full_links = SafeMerge(df_issue_linked, df_issue_text_ref, ['repo_name', 'repo_name_latest','issue_number'])
            df_pr_full_links   = SafeMerge(df_pr_linked, df_pr_text_ref, ['repo_name', 'repo_name_latest','pr_number'])

            if not df_issue_full_links.empty:
                df_issue_full_links = NormalizeDiscussionReferences(df_issue_full_links)
                df_issue_full_links['linked_pr'] = df_issue_full_links['linked_pr'].apply(lambda x: x if isinstance(x, list) else [])
                df_issue_full_links['same_repo'] = df_issue_full_links.apply(lambda x: MigrateSameRepo(x, 'linked_pr', 'issue_number'), axis=1)
                df_issue_full_links['other_repo'] = df_issue_full_links.apply(MigrateOtherRepo, axis=1)
                df_issue_full_links.drop('discussion_reference', axis=1, inplace=True)

            if not df_pr_full_links.empty:
                df_pr_full_links = NormalizeDiscussionReferences(df_pr_full_links)
                df_pr_full_links['linked_issue'] = df_pr_full_links['linked_issue'].apply(lambda x: x if isinstance(x, list) else [])
                df_pr_full_links['same_repo'] = df_pr_full_links.apply(lambda x: MigrateSameRepo(x, 'linked_issue', 'pr_number'), axis=1)
                df_pr_full_links['other_repo'] = df_pr_full_links.apply(MigrateOtherRepo, axis=1)
                df_pr_full_links.drop('discussion_reference', axis=1, inplace=True)
                df_pr_full_links = df_pr_full_links[~df_pr_full_links['pr_number'].isna()]

            df_strict_links = BuildStrictLinks(df_issue_full_links, df_pr_full_links) if not df_issue_full_links.empty or not df_pr_full_links.empty else pd.DataFrame()

            output_file.parent.mkdir(parents=True, exist_ok=True)
            df_strict_links.to_parquet(output_file, index=False)

        print(f"✅ Finished {repo} in {time.time()-start:.2f}s")
        return True
    except Exception as e:
        print(f"❌ Failed {repo}: {e}")
        return False

def SafeMerge(left: pd.DataFrame, right: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if not left.empty and not right.empty:
        merged = pd.merge(left, right, how="outer", on=keys)
    elif not left.empty:
        merged = left
    elif not right.empty:
        merged = right
    else:
        return pd.DataFrame()

    # Ensure critical list columns exist and are normalized
    for col in ["github_urls", "issue_pr_refs"]:
        if col not in merged.columns:
            merged[col] = [[] for _ in range(len(merged))]
        else:
            merged[col] = merged[col].apply(lambda x: x if isinstance(x, list) else [])

    return merged

def GetNumberOrNull(link: str):
    """Extract trailing numeric ID from GitHub issue/PR URL, else NaN"""
    try:
        last = link.rsplit("/", 1)[-1].split("#", 1)[0].split("%", 1)[0].split("?", 1)[0]
        return int(last)
    except (ValueError, IndexError, AttributeError):
        return np.nan

def GetRepoNumber(link: str):
    """Extract org/repo/number string if valid GitHub URL, else NaN"""
    if not isinstance(link, str) or "github.com" not in link:
        return np.nan
    try:
        parts = link.split("/")
        # Expect format: https://github.com/{org}/{repo}/{issues|pull}/{num}
        org, repo, num = parts[-4], parts[-3], parts[-1].split("#")[0].split("%")[0].split("?")[0]
        int(num)  # validate numeric suffix
        return f"{org}/{repo}/{num}"
    except (ValueError, IndexError):
        return np.nan


def RemoveNAFromList(seq):
    """Remove NaN/None from list-like"""
    return [val for val in seq if pd.notna(val)] if isinstance(seq, (list, tuple)) else []


def ExtractSameRepoLinks(github_links, pr_links, repo_name, linking_number):
    """Find same-repo links that aren’t already known"""
    links = list(github_links) if isinstance(github_links, (np.ndarray, pd.Series)) else github_links or []
    disc_numbers = set(pr_links) | {linking_number}
    results = {GetNumberOrNull(link) for link in links if isinstance(link, str) and repo_name in link}
    return sorted({num for num in results if pd.notna(num) and num not in disc_numbers})



def ExtractOtherRepoLinks(github_links, repo_name):
    """Find cross-repo references with full org/repo/number"""
    links = list(github_links) if isinstance(github_links, (np.ndarray, pd.Series)) else github_links or []
    results = {
        GetRepoNumber(link)
        for link in links
        if isinstance(link, str) and "github.com" in link and repo_name not in link
    }
    return sorted({r for r in results if pd.notna(r)})


def CleanLinkedIssue(df_issue_linked):
    df_issue_linked['github_links'] = df_issue_linked['linked_pull_request'].apply(lambda x: x['github_links'])
    df_issue_linked['pr_links'] = df_issue_linked['linked_pull_request'].apply(lambda x: x['pr_links'])
    df_issue_linked.drop('linked_pull_request', axis=1, inplace=True)

    # possible to have multiple linked PRs
    df_issue_linked['linked_pr'] = df_issue_linked['pr_links'].apply(lambda links: RemoveNAFromList([GetNumberOrNull(link) for link in links]))
    df_issue_linked['same_repo'] = df_issue_linked.apply(lambda row: ExtractSameRepoLinks(row['github_links'], row['linked_pr'], row['repo_name'], row['issue_number']), axis=1)
    df_issue_linked['other_repo'] = df_issue_linked.apply(lambda row: ExtractOtherRepoLinks(row['github_links'], row['repo_name']), axis=1)

    return df_issue_linked.drop(columns = ['github_links','pr_links'])

def CleanLinkedPR(df_pr_linked):
    df_pr_linked['linked_issue'] = df_pr_linked['issue_link'].apply(lambda links: RemoveNAFromList([GetNumberOrNull(link) for link in links]))
    df_pr_linked['same_repo'] = df_pr_linked.apply(lambda row: ExtractSameRepoLinks(row['other_links'], row['linked_issue'], row['repo_name'], row['pr_number']), axis=1)
    df_pr_linked['other_repo'] = df_pr_linked.apply(lambda row: ExtractOtherRepoLinks(row['other_links'], row['repo_name']), axis=1)

    return df_pr_linked.drop(columns=['issue_link','other_links','pull_request_title','pull_request_text'])

def CleanTextSeries(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.replace(fenced_code_block_pattern, "", regex=True)
    )


def GetCombinedTextSeries(df: pd.DataFrame, columns) -> pd.Series:
    combined = df[columns].astype(str).fillna("").agg(" ".join, axis=1)
    return CleanTextSeries(combined)


def ExtractGithubUrlsVectorized(series: pd.Series) -> pd.Series:
    mask = (
        series.str.contains("github.com")
        & (series.str.contains("pull") | series.str.contains("issues"))
    )
    matches = series.where(mask, "").str.findall(github_url_pattern)
    return matches.apply(
        lambda arr: [f"https://github.com/{a}/{b}/{c}/{d}" for a, b, c, d in arr]
    )


def ExtractIssuePrRefsVectorized(series: pd.Series) -> pd.Series:
    mask = series.str.contains("#")
    s = series.where(mask, "")

    path_refs = s.str.findall(path_ref_pattern).apply(
        lambda arr: {f"{a}#{b}" for a, b in arr}
    )
    plain_refs = s.str.findall(plain_ref_pattern).apply(
        lambda arr: {ref.strip() for ref in arr}  # strip here
    )

    return (path_refs.combine(plain_refs, lambda a, b: a | b)).apply(list)

def ExtractGithubData(df: pd.DataFrame, text_columns: list[str]) -> pd.DataFrame:
    combined = GetCombinedTextSeries(df, text_columns)

    github_urls = ExtractGithubUrlsVectorized(combined)
    issue_pr_refs = ExtractIssuePrRefsVectorized(combined)

    # Ensure lists, never floats/NaN
    github_urls = github_urls.apply(lambda x: x if isinstance(x, list) else [])
    issue_pr_refs = issue_pr_refs.apply(lambda x: x if isinstance(x, list) else [])

    return pd.DataFrame(
        {
            "github_urls": github_urls,
            "issue_pr_refs": issue_pr_refs,
        },
        index=df.index,
    )


def SameRepoCombine(repo, ref):
    return f"{repo}/{ref.lstrip('#')}"
def OtherRepoCombine(ref):
    return ref.replace('#', '/')
    
def NormalizeIssuePrRefs(row):
    repo = row['repo_name']
    refs = [ref.strip() for ref in row['issue_pr_refs']]

    normalized = row['discussion_reference']

    for ref in refs:
        if ref.startswith('#'):  # plain shorthand
            normalized.append(f"{repo}/{ref.lstrip('#')}")
        elif '/' in ref and '#' in ref:  # org/repo#num
            normalized.append(ref.replace('#', '/'))
        else:
            # ignore junk
            continue

    return list(set(normalized))


def NormalizeDiscussionReferences(df):
    """Standardize discussion references into unified column"""
    # Ensure list type
    for col in ['linked_issue','same_repo','other_repo','github_urls','issue_pr_refs']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [])

    # Direct URL → repo/number
    df['discussion_reference'] = df['github_urls'].map(
        lambda urls: ["/".join(u.split("/")[-4:-2] + [u.rsplit("/", 1)[-1]]) for u in urls]
    )

    df['discussion_reference'] = df.apply(NormalizeIssuePrRefs, axis=1)
    return df.drop(columns=['github_urls', 'issue_pr_refs'])

def MigrateSameRepo(row, linked_col, id_col):
    existing_ids = set(row[linked_col]) | set(row['same_repo']) | {row[id_col]}
    additions = []
    for ref in row['discussion_reference']:
        try:
            ref_repo, ref_num = "/".join(ref.split("/")[:-1]), int(ref.rsplit("/", 1)[-1])
            if ref_repo == row['repo_name'] and ref_num not in existing_ids:
                additions.append(ref_num)
        except ValueError:
            continue
    return sorted(set(row['same_repo']) | set(additions))


def MigrateOtherRepo(row):
    """Bring in missing other-repo references"""
    additions = [ref for ref in row['discussion_reference']
                 if row['repo_name'] not in ref and ref not in row['other_repo']]
    return sorted(set(row['other_repo']) | set(additions))


def BuildStrictLinks(df_issue_full_links, df_pr_full_links):
    """
    Construct strict link relationships between issues and PRs within a repo.

    Parameters
    ----------
    df_issue_full_links : pd.DataFrame
        Issues with extracted + normalized links (linked_pr, same_repo, other_repo).
    df_pr_full_links : pd.DataFrame
        PRs with extracted + normalized links (linked_issue, same_repo, other_repo).

    Returns
    -------
    pd.DataFrame
        Strictly linked problems, one row per "problem_id" = repo + min(issue/pr id).
        Columns: [repo_name, problem_id, problem_id_num, issues, prs, same_repo, other_repo, type]
    """

    # ---- Utility closures ---- #
    def make_numeric(seq):
        """Convert iterable to ints, ignoring NaN"""
        return [int(x) for x in seq if pd.notna(x)]

    def flatten_unique(series):
        """Flatten list-like series into sorted unique list"""
        return sorted({x for lst in series.dropna() for x in lst if pd.notna(x)})

    # Pre-normalize numeric ids for faster lookups
    df_issue_full_links = df_issue_full_links.copy()
    df_pr_full_links = df_pr_full_links.copy()
    df_issue_full_links['issue_number'] = df_issue_full_links['issue_number'].astype(int)
    df_pr_full_links['pr_number'] = df_pr_full_links['pr_number'].astype(int)

    # Build lookup tables
    pr_lookup = df_pr_full_links.set_index(['repo_name', 'pr_number'])
    issue_lookup = df_issue_full_links.set_index(['repo_name', 'issue_number'])

    # Reverse map: issue -> PRs that mention it
    reverse_issue_map = (
        df_pr_full_links
        .explode('linked_issue')
        .dropna(subset=['linked_issue'])
        .assign(linked_issue=lambda d: d['linked_issue'].astype(int))
        .groupby(['repo_name', 'linked_issue'])
        .apply(lambda g: g.to_dict(orient='records'))
        .to_dict()
    )

    records = []

    # ---- Process Issues ---- #
    for repo, subdf in df_issue_full_links.groupby("repo_name", sort=False):
        for _, row in subdf.sort_values("issue_number").iterrows():
            issue = int(row['issue_number'])
            linked_prs = make_numeric(row['linked_pr'])

            record = {
                'repo_name': repo,
                'issues': [issue],
                'prs': linked_prs,
                'same_repo': row['same_repo'],
                'other_repo': row['other_repo']
            }

            # Expand with metadata from directly linked PRs
            if linked_prs:
                pr_rows = pr_lookup.loc[[(repo, pr) for pr in linked_prs if (repo, pr) in pr_lookup.index]]
                if not pr_rows.empty:
                    record['same_repo'] = flatten_unique(pd.Series([row['same_repo']] + pr_rows['same_repo'].tolist()))
                    record['other_repo'] = flatten_unique(pd.Series([row['other_repo']] + pr_rows['other_repo'].tolist()))

            # Add reverse links (PRs that point to this issue)
            for r in reverse_issue_map.get((repo, issue), []):
                record['prs'] = sorted(set(record['prs']) | {int(r['pr_number'])})
                record['same_repo'] = flatten_unique(pd.Series([record['same_repo'], r['same_repo']]))
                record['other_repo'] = flatten_unique(pd.Series([record['other_repo'], r['other_repo']]))

            records.append(record)

    # Track which issue sets already linked
    seen_linked_issues = {(tuple(r['issues']), r['repo_name']) for r in records if r['prs']}

    # ---- Process PRs ---- #
    for repo, subdf in df_pr_full_links.groupby("repo_name", sort=False):
        for _, row in subdf.sort_values("pr_number").iterrows():
            pr = int(row['pr_number'])
            linked_issues = make_numeric(row['linked_issue'])

            # Skip duplicates if already covered by issue processing
            if linked_issues:
                key = (tuple(linked_issues), repo)
                if key in seen_linked_issues:
                    continue

            record = {
                'repo_name': repo,
                'prs': [pr],
                'issues': linked_issues,
                'same_repo': row['same_repo'],
                'other_repo': row['other_repo']
            }

            # Expand with metadata from linked issues
            if linked_issues:
                issue_rows = issue_lookup.loc[[(repo, iss) for iss in linked_issues if (repo, iss) in issue_lookup.index]]
                if not issue_rows.empty:
                    record['same_repo'] = flatten_unique(pd.Series([row['same_repo']] + issue_rows['same_repo'].tolist()))
                    record['other_repo'] = flatten_unique(pd.Series([row['other_repo']] + issue_rows['other_repo'].tolist()))
                seen_linked_issues.add(key)

            records.append(record)

    # ---- Build final dataframe ---- #
    df_strict_links = pd.DataFrame.from_records(records)

    # Unique problem identifier = repo + min(issue/pr id)
    df_strict_links['problem_id_num'] = df_strict_links.apply(lambda x: min(x['issues'] + x['prs']), axis=1)
    df_strict_links['problem_id'] = df_strict_links['repo_name'] + "/" + df_strict_links['problem_id_num'].astype(str)

    # Deduplicate refs across rows
    df_strict_links = (
        df_strict_links
        .groupby(['repo_name', 'problem_id', 'problem_id_num'])
        [['issues', 'prs', 'same_repo', 'other_repo']]
        .agg(RemoveDuplicatesFlattened)
        .reset_index()
    )

    # Label type of problem
    def classify(row):
        if row['issues'] and row['prs']:
            return "linked"
        elif row['prs']:
            return "unlinked pr"
        return "unlinked issue"

    df_strict_links['type'] = df_strict_links.apply(classify, axis=1)

    # Remove refs that point to self
    df_strict_links['same_repo'] = df_strict_links.apply(
        lambda x: [ref for ref in x['same_repo'] if ref not in x['issues'] and ref not in x['prs']],
        axis=1
    )

    return df_strict_links.sort_values(['repo_name', 'problem_id_num']).reset_index(drop=True)

Main()