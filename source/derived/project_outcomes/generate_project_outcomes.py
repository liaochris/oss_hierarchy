
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
from source.lib.helpers import *

### I want to merge in df_ranked_activity_share

def Main():
    warnings.filterwarnings("ignore")
    pd.set_option('display.max_columns', None)
    pandarallel.initialize(progress_bar = True)

    indir_data = Path('drive/output/derived/data_export')
    indir_contributors = Path('drive/output/derived/contributor_stats/contributor_data')
    outdir_data = Path('drive/output/derived/project_outcomes')
    
    commit_cols = ['commits','commit_additions','commit_deletions','commit_changes_total','commit_files_changed count']
    SECONDS_IN_DAY = 86400
    closing_day_options = [30, 60, 90, 180, 360]

    time_period = int(sys.argv[1])
    df_issue = pd.read_parquet(indir_data / 'df_issue.parquet')
    df_pr = pd.read_parquet(indir_data / 'df_pr.parquet')
    
    df_issue['created_at'] = pd.to_datetime(df_issue['created_at'])
    df_pr['created_at'] = pd.to_datetime(df_pr['created_at'])
    
    df_issue_selected = df_issue[df_issue['created_at']>='2015-01-01']
    df_pr_selected = df_pr[df_pr['created_at']>='2015-01-01']
    
    df_issue_selected = ImputeTimePeriod(df_issue_selected, time_period)
    df_pr_selected = ImputeTimePeriod(df_pr_selected, time_period)

    df_contributor_panel = pd.read_parquet(indir_contributors / f"major_contributors_major_months{time_period}_window732D_samplefull.parquet")
    val_cols = [col for col in df_contributor_panel.columns if 'pct' in col]
    df_contributor_panel = df_contributor_panel.drop(val_cols, axis = 1)

    ConstructRepoPanel(df_contributor_panel, df_issue_selected, df_pr_selected, time_period, SECONDS_IN_DAY, outdir_data, closing_day_options)


def ConstructRepoPanel(df_contributor_panel, df_issue_selected, df_pr_selected, time_period, SECONDS_IN_DAY, outdir_data, closing_day_options):
    df_repo_panel = df_contributor_panel.drop(['pr_opener','user_type','actor_id'], axis = 1)\
        .rename({'issue_number':'issues_opened','linked_pr_issue_number':'issues_opened_with_linked_pr','pr':'prs_opened'}, axis = 1)\
        .groupby(['repo_name','time_period']).sum()\
        .reset_index()
    df_repo_panel['first_period'] = df_repo_panel.groupby(['repo_name'])['time_period'].transform('min')
    df_repo_panel['final_period'] = df_repo_panel.groupby(['repo_name'])['time_period'].transform('max')
    time_periods = df_repo_panel['time_period'].sort_values().unique().tolist()
    df_balanced = df_repo_panel[['repo_name']].drop_duplicates()
    df_balanced['time_period'] = [time_periods for i in range(df_balanced.shape[0])]
    df_balanced = df_balanced.explode('time_period')
    df_repo_panel_full = pd.merge(df_balanced, df_repo_panel, how = 'left')
    df_repo_panel_full[['first_period','final_period']] = df_repo_panel_full.groupby(['repo_name'])[['first_period','final_period']].ffill()
    df_repo_panel_full = df_repo_panel_full.query('time_period >= first_period')
    df_repo_panel_full = df_repo_panel_full.fillna(0)
            
    df_issues_sans_comments = CreateIssueSansCommentsStats(df_issue_selected)
    df_issues = CreateFullIssueDatasetWithComments(df_issue_selected, df_issues_sans_comments, SECONDS_IN_DAY, closing_day_options)
    df_issues_stats = CreateIssueStats(df_issues)

    df_prs_sans_reviews = CreatePRSansReviewsStats(df_pr_selected)
    df_prs_complete = CreateFullPRDatasetWithReviews(df_pr_selected, df_prs_sans_reviews, SECONDS_IN_DAY, closing_day_options)
    df_prs_stats = CreatePRStats(df_prs_complete)
    df_stats = pd.merge(df_issues_stats, df_prs_stats, how = 'outer')
    df_repo_panel_stats = pd.merge(df_repo_panel_full, df_stats, how = 'left')
    df_repo_panel_stats.to_parquet(outdir_data / f'project_outcomes_major_months{time_period}.parquet')

def RemoveDuplicates(df, query, keepcols, duplicatecols, newcolname):
    df_uq = df.query(query).sort_values('created_at', ascending = True)[keepcols]\
        .drop_duplicates(duplicatecols)
    df_uq[newcolname] = 1
    return df_uq

def CreateIssueSansCommentsStats(df_issue_selected):
    issue_keepcols = ['repo_name','issue_number','time_period', 'created_at']
    issue_duplicatecols = ['repo_name','issue_number']
    df_opened_issues = RemoveDuplicates(df_issue_selected, 'issue_action == "opened"', issue_keepcols, issue_duplicatecols, 'opened_issue')
    df_closed_issues = RemoveDuplicates(df_issue_selected, 'issue_action == "closed"', issue_keepcols, issue_duplicatecols, 'closed_issue')\
        .rename({'time_period':'closed_time_period','created_at':'closed_at'}, axis = 1)
    ## TODO: how many closed issues are unlinked
    df_issues_sans_comments = pd.merge(df_opened_issues, df_closed_issues, how = 'left')
    return df_issues_sans_comments

def CreateFullIssueDatasetWithComments(df_issue_selected, df_issues_sans_comments, SECONDS_IN_DAY, closing_day_options):
    ic_keepcols = ['issue_number','issue_comment_id','repo_name','time_period', 'created_at']
    ic_duplicatecols = ['repo_name','issue_number','time_period', 'created_at']
    df_issue_comments = RemoveDuplicates(df_issue_selected, 'type == "IssueCommentEvent"',ic_keepcols, ic_duplicatecols, 'issue_comments')\
        .groupby(['repo_name','issue_number'])['issue_comments'].sum()\
        .reset_index()
    # TODO: how many unlinked issues by issue comments
    df_issues = pd.merge(df_issues_sans_comments, df_issue_comments, how = 'left')
    for col in ['closed_issue','issue_comments']:    
        df_issues[col] = df_issues[col].fillna(0)
    df_issues['days_to_close'] = (df_issues['closed_at'] - df_issues['created_at']).apply(lambda x: x.total_seconds()/SECONDS_IN_DAY)
    for day in closing_day_options:
        df_issues[f'closed_in_{day}_days'] = pd.to_numeric(df_issues['days_to_close']<day).astype(int)
    return df_issues
    
def CreateIssueStats(df_issues):
    df_issues_stats = df_issues.groupby(['repo_name','time_period'])\
        .agg({'closed_issue':'mean', 'closed_in_30_days':'mean', 
              'closed_in_60_days':'mean','closed_in_90_days':'mean',
              'closed_in_180_days':'mean', 'closed_in_360_days':'mean'})
    df_issues_stats.columns = df_issues_stats.columns.to_flat_index()
    df_issues_stats = df_issues_stats.reset_index()\
        .rename(columns = {('closed_issue','mean'): 'p_issues_closed',
            ('closed_in_30_days', 'mean'): 'p_issues_closed_30d',
            ('closed_in_60_days', 'mean'): 'p_issues_closed_60d',
            ('closed_in_90_days', 'mean'): 'p_issues_closed_90d',
            ('closed_in_180_days', 'mean'): 'p_issues_closed_180d',
            ('closed_in_360_days', 'mean'): 'p_issues_closed_360d'})

    return df_issues_stats

def CreatePRSansReviewsStats(df_pr_selected):
    pr_keepcols = ['repo_name','pr_number','time_period', 'created_at']
    pr_merge_keepcols = ['repo_name','pr_number','time_period', 'created_at', 'pr_merged_by_type']
    pr_idcols = ['repo_name','pr_number']
    
    df_opened_prs = RemoveDuplicates(df_pr_selected,'pr_action == "opened"', pr_keepcols, pr_idcols, 'opened_pr')
    df_closed_prs = RemoveDuplicates(df_pr_selected,'pr_action == "closed" & pr_merged_by_id.isna()', pr_keepcols, pr_idcols, 'closed_unmerged_pr')\
        .rename({'time_period':'closed_unmerged_time_period','created_at':'closed_unmerged_at'}, axis = 1)
    df_merged_prs = RemoveDuplicates(df_pr_selected,'pr_action=="closed" & ~pr_merged_by_id.isna()',pr_merge_keepcols, pr_idcols,'merged_pr')\
        .rename({'time_period':'merged_time_period','created_at':'merged_at'}, axis = 1)

    df_prs_sans_reviews = pd.merge(df_opened_prs, df_closed_prs, how = 'left').merge(df_merged_prs, how = 'left')
    return df_prs_sans_reviews

def CreateFullPRDatasetWithReviews(df_pr_selected, df_prs_sans_reviews, SECONDS_IN_DAY, closing_day_options):
    pr_review_keepcols = ['repo_name','pr_number','time_period', 'created_at','pr_review_id','pr_review_state']
    pr_review_idcols = ['repo_name','pr_number','pr_review_id']
    df_pr_reviews = RemoveDuplicates(df_pr_selected,'type == "PullRequestReviewEvent"',pr_review_keepcols,pr_review_idcols, 'pr_review')
    
    for col in ['commented','approved','changes_requested']:
        df_pr_reviews[f'review_state_{col}'] = pd.to_numeric(df_pr_reviews['pr_review_state']==col).astype(int)
    df_pr_review_stats = df_pr_reviews.groupby(['repo_name','pr_number'])\
        [['review_state_commented','review_state_approved','review_state_changes_requested']].sum().reset_index()
    
    df_prs_complete = pd.merge(df_prs_sans_reviews, df_pr_review_stats, how = 'left')
    for col in ['closed_unmerged_pr', 'merged_pr','review_state_commented',
                'review_state_approved','review_state_changes_requested']:    
        df_prs_complete[col] = df_prs_complete[col].fillna(0)
    df_prs_complete['days_to_merge'] = (df_prs_complete['merged_at'] - df_prs_complete['created_at']).apply(lambda x: x.total_seconds()/SECONDS_IN_DAY)
    for day in closing_day_options:
        df_prs_complete[f'merged_in_{day}_days'] = pd.to_numeric(df_prs_complete['days_to_merge']<day).astype(int)
    return df_prs_complete

def CreatePRStats(df_prs_complete):
    df_prs_stats = df_prs_complete.groupby(['repo_name','time_period'])\
        .agg({'closed_unmerged_pr':['sum','mean'], 'merged_pr':'mean',
              'review_state_commented':'mean', 'review_state_approved': 'mean',
              'review_state_changes_requested': 'mean',
              'merged_in_30_days':'mean', 'merged_in_60_days':'mean','merged_in_90_days':'mean',
              'merged_in_180_days':'mean', 'merged_in_360_days':'mean'})
    df_prs_stats.columns = df_prs_stats.columns.to_flat_index()
    df_prs_stats = df_prs_stats.reset_index()\
        .rename(columns = {('closed_unmerged_pr','sum'): 'prs_closed',
                           ('closed_unmerged_pr','mean'): 'p_prs_closed',
                           ('merged_pr','mean'): 'p_prs_merged',
                           ('review_state_commented','mean'):'p_review_state_commented',
                           ('review_state_approved','mean'):'p_review_state_approved',
                           ('review_state_changes_requested','mean'):'p_review_state_changes_requested',
                           ('merged_in_30_days', 'mean'): 'p_prs_merged_30d',
                           ('merged_in_60_days', 'mean'): 'p_prs_merged_60d',
                           ('merged_in_90_days', 'mean'): 'p_prs_merged_90d',
                           ('merged_in_180_days', 'mean'): 'p_prs_merged_180d',
                           ('merged_in_360_days', 'mean'): 'p_prs_merged_360d'})
    return df_prs_stats


if __name__ == '__main__':
    Main()


