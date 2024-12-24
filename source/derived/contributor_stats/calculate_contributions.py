#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from pathlib import Path
import numpy as np
import sys
import glob
import warnings
import random
from pandarallel import pandarallel
from source.lib.JMSLab import autofill
from source.lib.helpers import *
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from glob import glob 
import datetime
import itertools
import time
from multiprocessing import pool
import shutil

warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', None)
pandarallel.initialize(progress_bar = False)

def Main():
    indir_committers_info = Path('drive/output/scrape/link_committers_profile')
    indir_data = Path('drive/output/derived/data_export')
    outdir = Path('drive/output/derived/contributor_stats/contributor_data/intermediary_files')
    outdir_final = Path('drive/output/derived/contributor_stats/contributor_data')

    commit_cols = ['commits','commit additions','commit deletions','commit changes total','commit files changed count']
    author_thresh = 1/3

    time_period = int(sys.argv[1])
    rolling_window = int(sys.argv[2])
    
    df_issue = pd.read_parquet(indir_data / 'df_issue.parquet')
    df_pr = pd.read_parquet(indir_data / 'df_pr.parquet')
    df_pr_commits = pd.read_parquet(indir_data / 'df_pr_commits.parquet')
    df_push_commits = pd.read_parquet(indir_data / 'df_push_commits.parquet')
    df_all_linked_issues = pd.read_parquet('drive/output/scrape/link_issue_pull_request/linked_issue_to_pull_request.parquet')


    df_issue['created_at'] = pd.to_datetime(df_issue['created_at'])
    df_pr['created_at'] = pd.to_datetime(df_pr['created_at'])

    
    all_repos = df_issue[['repo_name']].drop_duplicates()['repo_name']
    chunk_size = 100 if time_period<6 else 300
    chunk_count = int(np.ceil(len(all_repos)/chunk_size))
    repo_chunks = np.array_split(all_repos, chunk_count)

    for chunk in range(chunk_count):
        sample_size = 'full'

        if f'major_contributors_major_months{time_period}_window{rolling_window}D_sample{sample_size}_chunk{chunk+1}.parquet' in os.listdir(outdir):
            continue
        selected_repos = repo_chunks[chunk]
        df_issue_selected_unlinked = df_issue[(df_issue['repo_name'].isin(selected_repos)) & (df_issue['created_at']>='2015-01-01')]
        df_pr_selected = df_pr[(df_pr['repo_name'].isin(selected_repos))  & (df_pr['created_at']>='2015-01-01')]
        df_pr_commits_selected = df_pr_commits[(df_pr_commits['repo_name'].isin(selected_repos))]
        df_push_commits_selected = df_push_commits[(df_push_commits['repo_name'].isin(selected_repos))]
        df_linked_issues = df_all_linked_issues[(df_all_linked_issues['repo_name'].isin(selected_repos))]
        
        committers_match = CleanCommittersInfo(indir_committers_info)
        df_pr_commit_stats = LinkCommits(df_pr_selected, df_pr_commits_selected, committers_match, commit_cols, 'pr')
        df_push_commit_stats = LinkCommits(None, df_push_commits_selected, committers_match, commit_cols, 'push')
        df_issue_selected = LinkIssuePR(df_issue_selected_unlinked, df_linked_issues)
        issue_comments = FilterDuplicateIssues(df_issue_selected, 'type == "IssueCommentEvent"')
        
        major_pct_list = [0.1, 0.25, 0.5, 0.75, 0.9]
        general_pct_list = [0.25, 0.5]
        OutputMajorContributors(committers_match, df_pr_commit_stats, df_pr_selected, df_push_commit_stats, 
                    df_issue_selected, issue_comments, major_pct_list, general_pct_list, time_period, 
                    author_thresh, commit_cols, rolling_window, sample_size, chunk+1, outdir)

    relevant_files = outdir.glob(f'major_contributors_major_months{time_period}_window{rolling_window}D_samplefull_chunk*.parquet')
    df_major_contributors = pd.concat([pd.read_parquet(file) for file in relevant_files])
    df_major_contributors.to_parquet(outdir_final / f'major_contributors_major_months{time_period}_window{rolling_window}D_samplefull.parquet')


def CleanCommittersInfo(indir_committers_info):
    # TODO: edit file so it can handle pushes
    df_committers_info = pd.concat([pd.read_csv(indir_committers_info / 'committers_info_pr.csv', index_col = 0).dropna(),
                                    pd.read_csv(indir_committers_info / 'committers_info_push.csv', index_col = 0).dropna()])
    df_committers_info['committer_info'] = df_committers_info['committer_info'].apply(literal_eval)
    # TODO: handle cleaning so that it can handle the other cases
    df_committers_info = df_committers_info[df_committers_info['committer_info'].apply(lambda x: len(x)==4)]
    df_committers_info['actor_name'] = df_committers_info['committer_info'].apply(lambda x: x[0])
    df_committers_info['actor_id'] = df_committers_info['committer_info'].apply(lambda x: x[1])
    committers_match = df_committers_info[['name','email','user_type','actor_name','actor_id']].drop_duplicates()
    committers_match.rename({'actor_id':'commit_author_id'}, axis = 1, inplace = True)
    return committers_match

def LinkCommits(df_selected, df_commits_selected, committers_match, commit_cols, commit_type):
    # TODO: what % of commits were dropped because nobody could be found
    df_commits_selected['created_at'] = pd.to_datetime(df_commits_selected['commit time'], unit = 's')
    df_commits_selected = df_commits_selected[df_commits_selected['created_at'] >= '2015-01-01']
    matched_commits = pd.merge(df_commits_selected, committers_match,
        how = 'inner', left_on = ['commit author name','commit author email'],
        right_on = ['name','email'])\
        .assign(commits=1)
    selected_id = 'push_id' if commit_type == 'push' else 'pr_number'
    if commit_type == 'pr':
        matched_commits_total = matched_commits.groupby(['repo_name',selected_id])\
            [commit_cols].sum()
        matched_commits_total.columns = [col + ' total' for col in commit_cols]
        matched_commits_share = pd.merge(
            matched_commits,
            matched_commits_total.reset_index(), on = ['repo_name',selected_id])
        for col in commit_cols:
            matched_commits_share[f"{col} share"] = matched_commits_share[col]/matched_commits_share[f"{col} total"]
        final_agg_cols = commit_cols + [f"{col} share" for col in commit_cols]
    else:
        matched_commits_share = matched_commits
        final_agg_cols = commit_cols
        
    commit_stats = matched_commits_share\
        .assign(commits=1)\
        .groupby(['repo_name',selected_id,'commit_author_id','created_at'])\
        [final_agg_cols].sum().reset_index()
    # TODO: what % of commits had truncated information bc 250 max - also, is that push or PR? 
    # TODO: what % of commits could we not get information for
    if commit_type == 'pr':
        merged_commits = df_selected.query('pr_action == "closed" & ~pr_merged_by_id.isna()')
        df_commit_stats = pd.merge(merged_commits.rename({'created_at':'pr_opened_at'}, axis = 1), commit_stats, on = ['repo_name',selected_id])
    else:
        df_commit_stats = commit_stats      
    return df_commit_stats

def LinkIssuePR(df_issue_selected, df_linked_issues):
    df_linked_issues = df_linked_issues.query('linked_pull_request != "list index out of range"')
    df_linked_issues['linked_pr_number'] = df_linked_issues['linked_pull_request'].apply(lambda x: x.split("/")[-1])
    df_issue_pr = df_linked_issues[['repo_name','issue_number', 'linked_pr_number']].drop_duplicates()
    df_issue_selected = pd.merge(df_issue_selected, df_issue_pr, how = 'left', on = ['repo_name','issue_number'])
    return df_issue_selected

def FilterDuplicateIssues(df, query):
    df_sel = df.query(query)\
        .sort_values(['repo_name','issue_number','created_at'])\
        [['repo_name','actor_id', 'issue_user_id','issue_number', 
          'issue_comment_id', 'created_at', 'linked_pr_number']]\
        .dropna(subset = ['issue_number'])\
        .dropna(axis=1, how='all')\
        .drop_duplicates()
    return df_sel

def ImputeTimePeriod(df, time_period_months):
    t = time_period_months
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['year'] = df['created_at'].parallel_apply(lambda x: x.year)
    df['time_period'] = df['created_at'].parallel_apply(lambda x: datetime.date(x.year, int(t*(x.month/t if x.month%t == 0 else np.ceil(x.month/t))-(t-1)), 1))
    df['time_period'] = pd.to_datetime(df['time_period'])
    return df 

def AssignPRAuthorship(df_pr_commit_stats, author_thresh, commit_cols):
    commit_cols_share = [f"{col} share" for col in commit_cols]
    commit_author_bool = df_pr_commit_stats.apply(lambda x: any([x[col]>author_thresh for col in commit_cols_share]), axis = 1)
    df_pr_commit_author_stats = df_pr_commit_stats[commit_author_bool]
    return df_pr_commit_author_stats

def AddPROpener(df_pr_commit_author_stats, df_pr_selected):
    pr_opener = df_pr_selected.query('pr_action == "opened"')[['repo_name','pr_number','actor_id']]\
        .rename({'actor_id':'pr_opener_actor_id'}, axis = 1).drop_duplicates()
    df_pr_commit_author_stats = pd.merge(df_pr_commit_author_stats, pr_opener, how = 'left', on = ['repo_name','pr_number'])
    return df_pr_commit_author_stats

def CalculateCommitAuthorStats(df_commit_author_stats, major_col_list, commit_type):
    if commit_type == 'pr':
        df_commit_author_stats['pr'] = 1/df_commit_author_stats.groupby(['repo_name','pr_number'])['type'].transform('count')
        df_commit_author_stats['pr_opener'] = df_commit_author_stats.parallel_apply(
            lambda x: 0 if pd.isnull(x['pr_opener_actor_id']) else int(x['pr_opener_actor_id'] == x['commit_author_id']), axis = 1)
    df_commit_author_stats['actor_id'] = df_commit_author_stats['commit_author_id']
    ts_authorship = df_commit_author_stats.groupby(['time_period', 'repo_name','actor_id'])\
        [major_col_list].sum()
    return ts_authorship

def CombinePushPR(ts_pr_authorship, ts_push_authorship, commit_cols):
    ts_push_authorship.columns = [f"push_{col}" if col in commit_cols else col for col in ts_push_authorship.columns]
    ts_pr_authorship.columns = [f"pr_{col}" if col in commit_cols else col for col in ts_pr_authorship.columns]
    ts_commit_authorship = ts_pr_authorship.join(ts_push_authorship, how = 'outer')
    for col in commit_cols:
        ts_commit_authorship[col] = ts_commit_authorship[f"push_{col}"].fillna(0) + ts_commit_authorship[f"pr_{col}"].fillna(0)
    return ts_commit_authorship
    
def CalculateIssueCommentStats(issue_comments, df_pr_selected):        
    pr_data = df_pr_selected[['repo_name','pr_number']].drop_duplicates()
    pr_opener_data = df_pr_selected.query('pr_action == "opened"')[['repo_name','pr_number','actor_id']].drop_duplicates()\
        .rename({'actor_id':'pr_opener_id'})
    issue_comments = issue_comments.merge(pr_data, how = 'left', left_on = ['repo_name','issue_number'], right_on = ['repo_name','pr_number'])
    issue_comments = issue_comments.merge(pr_opener_data, how = 'left', left_on = ['repo_name','issue_number'], right_on = ['repo_name','pr_number'])


    issue_comments['comments'] = 1
    issue_comments['issue_comments'] = issue_comments['pr_number'].isna().astype(int)
    issue_comments['pr_comments'] = 1 - issue_comments['issue_comments']
    issue_comments['linked_pr_issue_comments'] = 1 - issue_comments['linked_pr_number'].isna().astype(int)
    issue_comments['linked_pr_issue_number'] = issue_comments.apply(lambda x: x['issue_number'] if not pd.isnull(x['linked_pr_number']) else np.nan, axis = 1)
    issue_comments['own_issue_comments'] = ((issue_comments['issue_user_id'] == issue_comments['actor_id']) & (issue_comments['issue_comments'] == 1)).astype(int)
    issue_comments['helping_issue_comments'] = issue_comments['issue_comments'] - issue_comments['own_issue_comments']
    issue_comments['own_pr_comments'] = ((issue_comments['pr_opener_id'] == issue_comments['actor_id']) & (issue_comments['pr_comments'] == 1)).astype(int)
    issue_comments['helping_pr_comments'] = issue_comments['pr_comments'] - issue_comments['own_pr_comments']

    ts_issue_comments = issue_comments.groupby(['time_period', 'repo_name','actor_id'])\
        .agg({'comments': 'sum', 'issue_comments': 'sum', 'pr_comments': 'sum', 'own_issue_comments': 'sum', 'helping_issue_comments': 'sum',
              'own_pr_comments':'sum','helping_pr_comments':'sum', 'linked_pr_issue_comments': 'sum', 'issue_number': 'nunique', 
              'linked_pr_issue_number':'nunique'}).reset_index()
    return ts_issue_comments


def CalculateColumnPercentile(df, repo, rolling_window, major_col_list, major_pct_list):   
    df_repo = df[df['repo_name'] == repo]
    df_quantile = df_repo.set_index('time_period')\
        [major_col_list].resample("1d")\
        .quantile(major_pct_list).reset_index(level = 1)
    df_quantile_wide = pd.DataFrame(index = df_quantile.index.unique())
    for pct in major_pct_list:
        df_quantile_subset = df_quantile[df_quantile['level_1'] == pct].drop('level_1', axis = 1)
        df_quantile_subset.columns = [f'{major_col}_{int(pct*100)}th_pct' for major_col in major_col_list]
        df_quantile_wide = pd.concat([df_quantile_wide, df_quantile_subset], axis = 1)
    df_all_pct = df_quantile_wide.rolling(window = rolling_window, min_periods = 1)\
        .mean()\
        .reset_index()
    df_repo = pd.merge(df_repo, df_all_pct, on = ['time_period'])
    return df_repo


def CalculateColumnPercentileDF(df, rolling_window, major_col_list, major_pct_list, general_pct_list): 
    repo_list = df['repo_name'].unique().tolist()
    with ThreadPoolExecutor(8) as pool:
        df = pd.concat(pool.map(CalculateColumnPercentile, itertools.repeat(df), repo_list, itertools.repeat(rolling_window), itertools.repeat(major_col_list), itertools.repeat(major_pct_list)))
    df_quantile = df.set_index('time_period')\
        [major_col_list].resample("1d")\
        .quantile(general_pct_list).reset_index(level = 1)
    df_quantile_wide = pd.DataFrame(index = df_quantile.index.unique())
    for pct in general_pct_list:
        df_quantile_subset = df_quantile[df_quantile['level_1'] == pct].drop('level_1', axis = 1)
        df_quantile_subset.columns = [f'general_{major_col}_{int(pct*100)}th_pct' for major_col in major_col_list]
        df_quantile_wide = pd.concat([df_quantile_wide, df_quantile_subset], axis = 1)
    df_all_pct = df_quantile_wide.rolling(window = rolling_window, min_periods = 1)\
        .mean()\
        .reset_index()
    df = pd.merge(df, df_all_pct, on = ['time_period'])
    return df


def GetMajorContributorPostPercentile(ts_data, rolling_window, major_col_list, major_pct_list, general_pct_list, other_cols = []):
    ts_data = ts_data.reset_index()\
        .sort_values(['repo_name','time_period','actor_id'])
    major_pct_quantile_list = [f'{major_col}_{int(major_pct*100)}th_pct' for major_col, major_pct in itertools.product(major_col_list, major_pct_list)]
    general_pct_quantile_list = [f'general_{major_col}_{int(general_pct*100)}th_pct'  for major_col, general_pct in itertools.product(major_col_list, general_pct_list)]
    major_cols = ['time_period', 'repo_name','actor_id']
    for lst in [major_col_list, major_pct_quantile_list, general_pct_quantile_list, other_cols]:
        major_cols.extend(lst)
    ts_data_pct = CalculateColumnPercentileDF(ts_data, rolling_window, major_col_list, major_pct_list, general_pct_list)
    major_contributor_data = ts_data_pct[major_cols]
    return major_contributor_data

def GroupedFill(df, group, fill_cols):
    df[fill_cols] = df.groupby(group)[fill_cols].ffill()
    df[fill_cols] = df.groupby(group)[fill_cols].bfill()
    return df

def GenerateBalancedContributorsPanel(ic_major_contributor_data, commit_major_contributor_data, time_period):
    ic_uq = ic_major_contributor_data[['repo_name','actor_id', 'time_period']].drop_duplicates()
    commit_uq = commit_major_contributor_data[['repo_name','actor_id', 'time_period']].drop_duplicates()
    major_contributors = pd.concat([ic_uq,commit_uq]).drop_duplicates()
    major_contributors_time = major_contributors.groupby(['repo_name','actor_id']).agg({'time_period':['min','max']})
    major_contributors_time.columns = ['earliest','latest']
    major_contributors_time.reset_index(drop = True, inplace = True)
    major_contributors_time['time_period'] = major_contributors_time.parallel_apply(
        lambda x: pd.date_range(x['earliest'],x['latest'], freq = f'{time_period}MS'), axis = 1)
    major_contributors_data_pct = major_contributors.explode('time_period').reset_index(drop = True).set_index(['repo_name','actor_id','time_period'])
    ic_data = ic_major_contributor_data.set_index(['repo_name','actor_id','time_period'])
    commit_data = commit_major_contributor_data.set_index(['repo_name','actor_id','time_period'])
    major_contributors_data_pct = major_contributors_data_pct.join(ic_data, how = 'left')
    major_contributors_data_pct = major_contributors_data_pct.join(commit_data, how = 'left')
    
    return major_contributors_data_pct

def AddMajorContributorNonPercentileActivity(major_contributors_data_pct, df_pr_selected, df_issue_selected, time_period):
    df_pr_data = ImputeTimePeriod(df_pr_selected, time_period)
    df_issue_data = ImputeTimePeriod(df_issue_selected, time_period)
    pr_reviews = df_pr_data.query('type == "PullRequestReviewEvent"').assign(pr_reviews = 1)\
        .groupby(['actor_id','repo_name','time_period'])\
        ['pr_reviews'].sum().reset_index().set_index(['actor_id','repo_name','time_period'])
    pr_review_comments = df_pr_data.query('type == "PullRequestReviewCommentEvent"').assign(pr_review_comments = 1)\
        .groupby(['actor_id','repo_name','time_period'])\
        ['pr_review_comments'].sum().reset_index().set_index(['actor_id','repo_name','time_period'])
    pr_merges = df_pr_data.query('type == "PullRequestEvent"')[['repo_name','pr_number', 'pr_merged_by_id','time_period', 'pr_merged_by_type']]\
        .dropna().sort_values('time_period').drop_duplicates(['repo_name','pr_number', 'time_period'])\
        .rename({'pr_merged_by_id':'actor_id', 'pr_merged_by_type':'user_type'}, axis = 1)\
        .assign(prs_merged = 1)\
        .groupby(['actor_id','user_type','repo_name','time_period'])\
        [['prs_merged']].sum().reset_index().set_index(['actor_id','repo_name','time_period'])
    issue_closers = df_issue_data.query('type == "IssuesEvent" & issue_action == "closed"')[['repo_name','issue_number','time_period','actor_id']]\
        .sort_values('time_period').drop_duplicates(['repo_name','issue_number', 'time_period'])\
        .assign(issues_closed = 1)\
        .groupby(['actor_id','repo_name','time_period'])\
        [['issues_closed']].sum().reset_index().set_index(['actor_id','repo_name','time_period'])
    major_contributors_data = major_contributors_data_pct.join(pr_reviews, how = 'outer')
    major_contributors_data = major_contributors_data.join(pr_review_comments, how = 'outer')
    major_contributors_data = major_contributors_data.join(pr_merges, how = 'outer')
    major_contributors_data = major_contributors_data.join(issue_closers, how = 'outer')
    return major_contributors_data

def FillNaAndReorder(major_contributors_data):
    val_cols =[col for col in major_contributors_data.columns if 'pct' not in col and col != 'user_type']
    pct_cols = [col for col in major_contributors_data.columns if 'pct' in col]
    major_contributors_data_reordered = major_contributors_data[['user_type'] + val_cols + pct_cols]

    major_contributors_data[pct_cols] = \
        major_contributors_data.groupby(['repo_name','time_period'])[pct_cols].transform('bfill')
    major_contributors_data[pct_cols] = \
        major_contributors_data.groupby(['repo_name','time_period'])[pct_cols].transform('ffill')
    major_contributors_data['user_type'] = major_contributors_data.groupby(['actor_id'])['user_type'].transform('bfill')
    major_contributors_data['user_type'] = major_contributors_data.groupby(['actor_id'])['user_type'].transform('ffill')

    return major_contributors_data


def OutputMajorContributors(committers_match, df_pr_commit_stats, df_pr_selected, df_push_commit_stats, df_issue_selected, issue_comments,
                            major_pct_list, general_pct_list, time_period, author_thresh, commit_cols,
                            rolling_window, sample_size, chunk, outdir):
    major_pr_col_list = ['pr', 'pr_opener'] + commit_cols 
    major_push_col_list = commit_cols
    push_commit_cols =  [f"push_{col}" for col in commit_cols]
    pr_commit_cols = [f"pr_{col}" for col in commit_cols]

    # DO I WANT TO USE COMMIT TIME INSTEAD OF MERGE TIME
    df_pr_commit_stats = ImputeTimePeriod(df_pr_commit_stats, time_period)
    df_pr_commit_author_stats = AssignPRAuthorship(df_pr_commit_stats, author_thresh, commit_cols)
    df_pr_commit_author_stats = AddPROpener(df_pr_commit_author_stats, df_pr_selected)
    ts_pr_authorship = CalculateCommitAuthorStats(df_pr_commit_author_stats, major_pr_col_list, 'pr')

    df_push_commit_stats = ImputeTimePeriod(df_push_commit_stats, time_period)
    ts_push_authorship = CalculateCommitAuthorStats(df_push_commit_stats, major_push_col_list, 'push')
    ts_commit_authorship = CombinePushPR(ts_pr_authorship, ts_push_authorship, commit_cols)
    commit_major_contributor_data = GetMajorContributorPostPercentile(ts_commit_authorship, str(rolling_window)+"D", major_pr_col_list, major_pct_list, general_pct_list, other_cols = pr_commit_cols+push_commit_cols) 
    print("percentile for PR + push commits obtained")

    issue_comments = ImputeTimePeriod(issue_comments, time_period)
    ts_issue_comments = CalculateIssueCommentStats(issue_comments, df_pr_selected)
    major_ic_col_list  = ['comments', 'issue_comments', 'pr_comments', 'own_issue_comments', 'helping_issue_comments', 'own_pr_comments','helping_pr_comments',
        'linked_pr_issue_comments', 'issue_number', 'linked_pr_issue_number']
    ic_major_contributor_data = GetMajorContributorPostPercentile(ts_issue_comments, str(rolling_window)+"D", major_ic_col_list, major_pct_list, general_pct_list)
    print("percentile for issues obtained")
    major_contributors_data_pct = GenerateBalancedContributorsPanel(ic_major_contributor_data, commit_major_contributor_data, time_period)
    major_contributors_data = AddMajorContributorNonPercentileActivity(major_contributors_data_pct, df_pr_selected, df_issue_selected, time_period)
    print("supplementary information obtained")
    pct_cols = [col for col in major_contributors_data.columns if 'pct' in col and 'general' not in col]
    general_pct_cols = [col for col in major_contributors_data.columns if 'pct' in col and 'general' in col]
    major_cols = ['pr', 'pr_opener'] + push_commit_cols + pr_commit_cols + commit_cols
    major_cols.extend(major_ic_col_list)
    major_cols.extend(['pr_reviews','pr_review_comments','prs_merged','issues_closed'])

    major_contributors_data.reset_index(inplace = True)
    major_contributors_data = GroupedFill(major_contributors_data, ['repo_name','time_period'], pct_cols)
    major_contributors_data = GroupedFill(major_contributors_data, ['time_period'], general_pct_cols)

    major_contributors_data[major_cols] = major_contributors_data[major_cols].fillna(0)
    print(f"Major PCT: {major_pct_list}, General PCT: {general_pct_list}, Time Period: {time_period} months")
    print(major_contributors_data[['repo_name','actor_id']].drop_duplicates().shape)
    major_contributors_data.drop_duplicates(inplace = True)
    major_contributors_data.columns = [col.replace(" ","_") for col in major_contributors_data.columns]

    major_contributors_data = FillNaAndReorder(major_contributors_data)

    major_contributors_data.to_parquet(outdir / f'major_contributors_major_months{time_period}_window{rolling_window}D_sample{sample_size}_chunk{chunk}.parquet')
    print("major contributors exported")

if __name__ == '__main__':
    Main()


