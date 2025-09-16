from source.lib.helpers import ImputeTimePeriod
from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds
from concurrent.futures import ProcessPoolExecutor
from source.lib.helpers import ConcatStatsByTimePeriod
import numpy as np
import random
import re

CHECKLIST_PATTERN = re.compile(r'^\s*[-*+]\s*\[( |x|X)\].*$', re.M)

def Main():
    time_period = 6
    indir = Path("drive/output/derived/problem_level_data/repo_actions")
    indir_file = Path("drive/output/scrape/github_file_data")
    indir_test = Path("drive/output/scrape/pull_request_test_data")

    outdir = Path("drive/output/derived/org_characteristics/repo_routines")

    repos = [f.stem for f in indir.glob("*.parquet")]
    random.shuffle(repos)

    with ProcessPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(
                ProcessRepo,
                repo,
                time_period,
                indir,
                indir_file,
                indir_test,
                outdir,
            )
            for repo in repos
        ]
        for f in futures:
            f.result()


def ProcessRepo(repo, time_period, indir, indir_file, indir_test, outdir):
    outfile_agg = outdir / f"{repo}.parquet"
    if outfile_agg.exists():
        print(f"⏭️ Skipping {repo}, already processed.")
        return

    df_all = pd.read_parquet(indir / f"{repo}.parquet")
    df_all = ImputeTimePeriod(df_all, time_period)

    file_path = indir_file / f"{repo}.parquet"
    if file_path.exists() :
        df_special_files = pd.read_parquet(file_path)
        df_special_files['date'] = pd.to_datetime(df_special_files['date'], utc=True)
        df_special_files = ImputeTimePeriod(
            df_special_files.rename(columns={'date': 'created_at'}), time_period
        )
        df_organizing_files = OrganizingFiles(df_special_files)
        df_contributing_files = ContributorGuides(df_special_files)
    else:
        print(f"⚠️ No special files for {repo}, skipping related stats.")
        df_organizing_files = pd.DataFrame()
        df_contributing_files = pd.DataFrame()

    test_path = indir_test / f"{repo}.parquet"
    if test_path.exists():
        df_test = pd.read_parquet(test_path)
        df_test_stats = ComputePullTestStats(df_all, df_test)
    else:
        print(f"⚠️ No test data for {repo}, skipping related stats.")
        df_test_stats = pd.DataFrame()

    df_problem_organization = OrganizedProblemSolving(df_all)
    df_good_first_issues = GoodFirstIssues(df_all)

    stats = [
        df_problem_organization,
        df_organizing_files,
        df_good_first_issues,
        df_contributing_files,
        df_test_stats,
    ]

    df_combined = ConcatStatsByTimePeriod(*stats)
    df_combined = ForwardFillOrganizingCols(df_combined)

    if not df_combined.empty:
        outdir.mkdir(parents=True, exist_ok=True)
        df_combined.to_parquet(outfile_agg) 
        print(f"✅ Processed {repo} → {outfile_agg}")

def ComputeMeanStatus(df, status_col):
    return (
        df.sort_values(['thread_number', 'time_period', status_col],
                       ascending=[True, True, False])
          .drop_duplicates(['thread_number', 'time_period'], keep='first')
          .groupby('time_period')
          .agg({status_col: 'mean'})
          .rename(columns={status_col: f"mean_{status_col}"})
    )


def ComputeFirstCodeowners(df_special_files):
    return (
        df_special_files.query('file_type == "codeowners"')
                        .sort_values('created_at')
                        .drop_duplicates('file_type', keep='first')
                        .query('time_period <= 20241231')
                        .assign(has_codeowners=1)
                        [['time_period', 'has_codeowners']]
                        .set_index('time_period')
    )


def ComputeFirstTemplate(df_special_files, template_type, col_name):
    first_time_period = (
        df_special_files.loc[df_special_files["file_type"] == template_type, "time_period"].min()
    )
    return (
        df_special_files.query("time_period == @first_time_period and file_type == @template_type")
                        .assign(**{col_name: 1})
                        [["time_period", col_name]]
                        .set_index("time_period")
                        .drop_duplicates()
    )


def ComputeUniqueIssueTemplates(df_special_files):
    df_unique_files = (
        df_special_files.query('file_type == "issue_template"')
                        .groupby('time_period', as_index=False)['file_path'].unique()
    )
    if df_unique_files.empty:
        return pd.DataFrame()
    df_unique_files['issue_template_count'] = (
        df_unique_files['file_path']
        .apply(lambda x: len({file.split("/")[-1].split(".")[0] for file in x}))
    )
    return df_unique_files.set_index('time_period')[['issue_template_count']]


def OrganizedProblemSolving(df_all):
    df_pr = df_all[df_all['type'].str.startswith('pull request')].copy()
    df_pr['has_reviewer'] = df_pr['requested_reviewers'].apply(lambda x: len(x) > 0)

    df_all = df_all.copy()
    df_all['has_tag'] = df_all['labels'].apply(lambda x: len(x) > 0)
    df_all['has_assignee'] = df_all['assignees'].apply(lambda x: len(x) > 0)

    df_problem_organization = (
        ComputeMeanStatus(df_pr, 'has_reviewer')
        .join([
            ComputeMeanStatus(df_all, 'has_tag'),
            ComputeMeanStatus(df_all, 'has_assignee'),
        ], how='outer')
    ).reset_index()
    return df_problem_organization

def OrganizingFiles(df_special_files):
    df_organizing_files = (
        ComputeFirstCodeowners(df_special_files)
        .join([
            ComputeFirstTemplate(df_special_files, "issue_template", "has_issue_template"),
            ComputeFirstTemplate(df_special_files, "pr_template", "has_pr_template"),
            ComputeUniqueIssueTemplates(df_special_files)
        ], how='outer')
    )
    
    return df_organizing_files[df_organizing_files.index <= "2024-12-31"].reset_index()


def GoodFirstIssues(df_all):
    df_issue = df_all[df_all['type'].str.startswith('issue')].copy()
    df_issue['good_first_issue'] = df_issue['labels'].apply(
        lambda labels: any(label.lower() == 'good first issue' for label in labels)
    )

    has_good_first_issue = (
        df_issue[df_issue['type'] == 'issue opened']
        .groupby('time_period')['thread_number']
        .apply(lambda threads: df_issue[
            (df_issue['time_period'].isin([threads.name])) &
            (df_issue['good_first_issue']) &
            (df_issue['thread_number'].isin(threads))
        ].shape[0] > 0)
        .to_frame('has_good_first_issue')
    )

    pct_good_first_issue = (
        df_issue.groupby(['time_period', 'thread_number'])['good_first_issue']
        .max()
        .groupby('time_period')
        .mean()
        .to_frame('pct_good_first_issue')
    )

    return has_good_first_issue.join(pct_good_first_issue, how='outer').reset_index()


def ContributorGuides(df_special_files):
    df_contributing_files = (
        ComputeFirstTemplate(df_special_files, "contributing", "has_contributing_guide")
        .join([
            ComputeFirstTemplate(df_special_files, "code_of_conduct", "has_code_of_conduct"),
        ], how='outer')
    )
    
    return df_contributing_files[df_contributing_files.index <= "2024-12-31"].reset_index()

def ForwardFillOrganizingCols(df):
    cols = ["has_codeowners", "has_issue_template", "has_pr_template", "issue_template_count",
            "has_contributing_guide", "has_code_of_conduct"]
    for col in cols:
        if col not in df.columns:
            df[col] = np.nan
    df[cols] = df[cols].ffill()

    # https://github.blog/news-insights/product-news/introducing-code-owners/
    mask = df.index >= "2017-07-01"
    df.loc[mask, "has_codeowners"] = (
        df.loc[mask, "has_codeowners"].fillna(0)
    )

    # https://github.blog/developer-skills/github/issue-and-pull-request-templates/
    mask = df.index >= "2016-01-01"
    df.loc[mask, ["has_issue_template", "has_pr_template", "issue_template_count"]] = (
        df.loc[mask, ["has_issue_template", "has_pr_template", "issue_template_count"]].fillna(0)
    )

    df[["has_contributing_guide", "has_code_of_conduct"]] = df[["has_contributing_guide", "has_code_of_conduct"]].fillna(0)
    return df

def ComputePullTestStats(df_all, df_test):
    df_pr_uq = (
        df_all.query('type == "pull request merged"')
              .sort_values(['thread_number', 'created_at'])
              .drop_duplicates('thread_number')
    )

    df_pr_tests_uq = (
        pd.merge(
            df_pr_uq[['thread_number', 'time_period']],
            df_test.rename(columns={'number': 'thread_number'})[
                ['thread_number', 'passed', 'total_runs', 'has_more_suites', 'has_more_runs']
            ],
            on='thread_number'
        )
    )

    df_pr_tests_uq['prop_tests_passed'] = (
        df_pr_tests_uq['passed'] / df_pr_tests_uq['total_runs']
    )
    for col in ['has_more_suites','has_more_runs']:
        df_pr_tests_uq[col] = df_pr_tests_uq.apply(lambda x: x[col] if x['total_runs']!=0 else np.nan, axis = 1)

    return  (
        df_pr_tests_uq
        .groupby('time_period', as_index = False)[['has_more_suites', 'has_more_runs', 'prop_tests_passed']]
        .mean()  
    )


if __name__ == "__main__":
    Main()
