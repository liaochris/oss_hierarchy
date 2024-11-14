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

warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', None)
pandarallel.initialize(progress_bar = True)

def Main():
    indir_committers_info = Path('drive/output/scrape/link_committers_profile')
    indir_data = Path('drive/output/derived/data_export')
    outdir = Path('drive/output/derived/major_contributor_prospects/intermediary_files')
    
    commit_cols = ['commits','commit additions','commit deletions','commit changes total','commit files changed count']
    author_thresh = 1/3
    rolling_window_list = ['732D','1828D','367D','3654D']
    time_period_months = [6, 3, 12, 2]

    df_issue = pd.read_parquet(indir_data / 'df_issue.parquet')
    df_pr = pd.read_parquet(indir_data / 'df_pr.parquet')
    df_pr_commits = pd.read_parquet(indir_data / 'df_pr_commits.parquet')
    df_all_linked_issues = ReadFileList(glob('drive/output/scrape/link_issue_pull_request/linked_issue/*.csv'))

    df_issue['created_at'] = pd.to_datetime(df_issue['created_at'])
    df_pr['created_at'] = pd.to_datetime(df_pr['created_at'])

    
    all_repos = df_issue[['repo_name']].drop_duplicates()['repo_name']
    chunk_size = 250 if time_period_months==2 else 500
    chunk_count = int(np.ceil(len(all_repos)/chunk_size))
    repo_chunks = np.array_split(all_repos, chunk_count)

    for time_period in time_period_months:
        for rolling_window in rolling_window_list:
            for chunk in range(chunk_count):
                sample_size = 'full'
                selected_repos = repo_chunks[chunk]
                df_issue_selected_unlinked = df_issue[(df_issue['repo_name'].isin(selected_repos)) & (df_issue['created_at']>='2015-01-01')]
                df_pr_selected = df_pr[(df_pr['repo_name'].isin(selected_repos))  & (df_pr['created_at']>='2015-01-01')]
                df_pr_commits_selected = df_pr_commits[(df_pr_commits['repo_name'].isin(selected_repos))]
                df_linked_issues = df_all_linked_issues[(df_all_linked_issues['repo_name'].isin(selected_repos))]
                
                committers_match = CleanCommittersInfo(indir_committers_info)
                df_pr_commit_stats = LinkPRCommits(df_pr_selected, df_pr_commits_selected, committers_match, commit_cols)
                df_issue_selected = LinkIssuePR(df_issue_selected_unlinked, df_linked_issues)
                issue_comments = FilterDuplicateIssues(df_issue_selected, 'type == "IssueCommentEvent"')
                
                major_pct_list = [0.1, 0.25, 0.5, 0.75, 0.9]
                general_pct_list = [0.25, 0.5, 0.75]
                OutputMajorContributors(committers_match, df_pr_commit_stats, df_issue_selected, issue_comments,
                            major_pct_list, general_pct_list, time_period, author_thresh, commit_cols,
                            rolling_window, sample_size, chunk+1, outdir)
        

def CleanCommittersInfo(indir_committers_info):
    # TODO: edit file so it can handle pushes
    df_committers_info = pd.read_csv(indir_committers_info / 'committers_info_pr.csv', index_col = 0).dropna()
    df_committers_info['committer_info'] = df_committers_info['committer_info'].apply(literal_eval)
    # TODO: handle cleaning so that it can handle the other cases
    df_committers_info = df_committers_info[df_committers_info['committer_info'].apply(lambda x: len(x)==4)]
    df_committers_info['actor_name'] = df_committers_info['committer_info'].apply(lambda x: x[0])
    df_committers_info['actor_id'] = df_committers_info['committer_info'].apply(lambda x: x[1])

    committers_match = df_committers_info[['name','email','user_type','actor_name','actor_id']].drop_duplicates()
    committers_match.rename({'actor_id':'commit_author_id'}, axis = 1, inplace = True)

    return committers_match

def LinkPRCommits(df_pr_selected, df_pr_commits_selected, committers_match, commit_cols):
    # TODO: what % of commits were dropped because nobody could be found
    matched_commits = pd.merge(df_pr_commits_selected, committers_match,
                               how = 'inner', left_on = ['commit author name','commit author email'],
                               right_on = ['name','email'])
    matched_commits = matched_commits.assign(commits=1)
    
    matched_commits_total = matched_commits.groupby(['repo_name','pr_number'])\
        [commit_cols].sum()
    matched_commits_total.columns = [col + ' total' for col in commit_cols]
    matched_commits_share = pd.merge(
        matched_commits,
        matched_commits_total.reset_index(), on = ['repo_name','pr_number'])
    
    for col in commit_cols:
        matched_commits_share[f"{col} share"] = matched_commits_share[col]/matched_commits_share[f"{col} total"]

    final_agg_cols = commit_cols + [f"{col} share" for col in commit_cols]
    commit_stats = matched_commits_share\
        .assign(commits=1)\
        .groupby(['repo_name','pr_number','commit_author_id'])\
        [final_agg_cols].sum().reset_index()
    
    merged_commits = df_pr_selected.query('pr_action == "closed" & ~pr_merged_by_id.isna()')
    # TODO: what % of commits had truncated information bc 250 max - also, is that push or PR? 
    # TODO: what % of commits could we not get information for
    df_commit_stats = pd.merge(merged_commits, commit_stats, on = ['repo_name','pr_number'])

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
    df['year'] = df['created_at'].apply(lambda x: x.year)
    
    df['period'] = df['created_at'].apply(lambda x: int(x.month>6))
    df['time_period'] = df['created_at'].apply(lambda x: datetime.date(x.year, int(t*(x.month/t if x.month%t == 0 else np.ceil(x.month/t))-(t-1)), 1))
    df['time_period'] = pd.to_datetime(df['time_period'])
    
    df_period_index = df[['year','period']].drop_duplicates()\
        .sort_values(['year','period'], ascending = True)\
        .reset_index(drop = True)
    df_period_index['index'] = df_period_index.index
    df = pd.merge(df, df_period_index).drop(['year','period'], axis = 1)\
        .rename({'index': 'time_period_index'}, axis = 1)
    
    return df 

def AssignPRAuthorship(df_pr_commit_stats, author_thresh, commit_cols):
    commit_cols_share = [f"{col} share" for col in commit_cols]
    commit_author_bool = df_pr_commit_stats.apply(lambda x: any([x[col]>author_thresh for col in commit_cols_share]), axis = 1)
    df_pr_commit_author_stats = df_pr_commit_stats[commit_author_bool]
    return df_pr_commit_author_stats

def CalculateIssueCommentStats(issue_comments):        
    ts_issue_comments = issue_comments.assign(issue_comments=1)
    ts_issue_comments['linked_pr_issue_comments'] = 1 - ts_issue_comments['linked_pr_number'].isna().astype(int)
    ts_issue_comments['linked_pr_issue_number'] = ts_issue_comments.apply(lambda x: x['issue_number'] if not pd.isnull(x['linked_pr_number']) else np.nan, axis = 1)
    ts_issue_comments = ts_issue_comments.groupby(['time_period','time_period_index', 'repo_name','actor_id'])\
        .agg({'issue_comments': 'sum','linked_pr_issue_comments': 'sum',
              'issue_number': 'nunique', 'linked_pr_issue_number':'nunique'}).reset_index()

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


def GetMajorContributorPostPercentile(ts_data, rolling_window, major_col_list, major_pct_list, general_pct_list):
    ts_data = ts_data.reset_index()\
        .sort_values(['repo_name','time_period_index','actor_id'])

    major_pct_quantile_list = [f'{major_col}_{int(major_pct*100)}th_pct' for major_col, major_pct in itertools.product(major_col_list, major_pct_list)]
    general_pct_quantile_list = [f'general_{major_col}_{int(general_pct*100)}th_pct'  for major_col, general_pct in itertools.product(major_col_list, general_pct_list)]
    
    major_cols = ['time_period','time_period_index', 'repo_name','actor_id']
    for lst in [major_col_list, major_pct_quantile_list, general_pct_quantile_list]:
        major_cols.extend(lst)
    
    ts_data_pct = CalculateColumnPercentileDF(ts_data, rolling_window, major_col_list, major_pct_list, general_pct_list)
    major_contributor_data = ts_data_pct[major_cols]

    return major_contributor_data

def GroupedFill(df, group, fill_cols):
    df[fill_cols] = df.groupby(group)[fill_cols].ffill()
    df[fill_cols] = df.groupby(group)[fill_cols].bfill()

    return df

def GenerateBalancedContributorsPanel(ic_major_contributor_data, pr_major_contributor_data):
    major_contributors = pd.concat([ic_major_contributor_data[['repo_name','actor_id']].drop_duplicates(),
                                    pr_major_contributor_data[['repo_name','actor_id']].drop_duplicates()]).drop_duplicates()
    time_periods = sorted(ic_major_contributor_data['time_period'].unique().tolist())
    major_contributors['time_period'] = [time_periods for i in range(major_contributors.shape[0])]
    major_contributors_data = major_contributors.explode('time_period').reset_index(drop = True)
    major_contributors_data = pd.merge(major_contributors_data, ic_major_contributor_data, how = 'left')
    major_contributors_data = pd.merge(major_contributors_data, pr_major_contributor_data, how = 'left')

    return major_contributors_data

def RemovePeriodsPriorToJoining(major_contributors_data):
    contributor_earliest = major_contributors_data.dropna().sort_values('time_period')\
        [['repo_name','actor_id','time_period']]\
        .drop_duplicates(['repo_name','actor_id'])\
        .rename({'time_period':'earliest_appearance'}, axis = 1)
    major_contributors_data = pd.merge(major_contributors_data, contributor_earliest, how = 'inner', on = ['repo_name','actor_id'])
    major_contributors_data = major_contributors_data.query('time_period>=earliest_appearance')

    return major_contributors_data


def OutputMajorContributors(committers_match, df_pr_commit_stats, df_issue_selected, issue_comments, 
                            major_pct_list, general_pct_list, time_period, author_thresh, commit_cols, 
                            rolling_window, sample_size, chunk, outdir):
    major_pr_col_list = ['pr'] + commit_cols + [f"{col} share" for col in commit_cols]
    df_pr_commit_stats = ImputeTimePeriod(df_pr_commit_stats, time_period)
    df_pr_commit_author_stats = AssignPRAuthorship(df_pr_commit_stats, author_thresh, commit_cols)
    ts_pr_authorship = df_pr_commit_author_stats.assign(pr = 1)\
        .groupby(['time_period', 'time_period_index', 'repo_name','actor_id'])\
        [major_pr_col_list].sum()
    
    pr_major_contributor_data = GetMajorContributorPostPercentile(ts_pr_authorship, rolling_window, major_pr_col_list, major_pct_list, general_pct_list)
    print("percentile for PRs obtained")
    issue_comments = ImputeTimePeriod(issue_comments, time_period)
    ts_issue_comments = CalculateIssueCommentStats(issue_comments)
    
    major_ic_col_list  = ['issue_comments','linked_pr_issue_comments', 'issue_number', 'linked_pr_issue_number']
    ic_major_contributor_data = GetMajorContributorPostPercentile(ts_issue_comments, rolling_window, major_ic_col_list, major_pct_list, general_pct_list)
    print("percentile for issues obtained")
    major_contributors_data = GenerateBalancedContributorsPanel(ic_major_contributor_data, pr_major_contributor_data)

    major_contributors_data = RemovePeriodsPriorToJoining(major_contributors_data)

    pct_cols = [col for col in major_contributors_data.columns if 'pct' in col and 'general' not in col]
    general_pct_cols = [col for col in major_contributors_data.columns if 'pct' in col and 'general' in col]
    major_cols = major_pr_col_list
    major_cols.extend(major_ic_col_list)
    
    major_contributors_data = GroupedFill(major_contributors_data, ['repo_name','time_period'], pct_cols)
    major_contributors_data = GroupedFill(major_contributors_data, ['time_period'], general_pct_cols)
    major_contributors_data = GroupedFill(major_contributors_data, ['time_period'], ['time_period_index'])
    major_contributors_data[major_cols] = major_contributors_data[major_cols].fillna(0)

    print(f"Major PCT: {major_pct_list}, General PCT: {general_pct_list}, Time Period: {time_period} months")
    print(major_contributors_data[['repo_name','actor_id']].drop_duplicates().shape)
    major_contributors_data = major_contributors_data.drop_duplicates()
    major_contributors_data.to_parquet(outdir / f'major_contributors_major_months{time_period}_window{rolling_window}_sample{sample_size}_chunk{chunk}.parquet')
    print("major contributors exported")

    return major_contributors_data
    
if __name__ == '__main__':
    Main()


