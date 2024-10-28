import pandas as pd
from pathlib import Path
import numpy as np
import sys
import glob
import warnings
import random
from pandarallel import pandarallel
from source.lib.JMSLab import autofill
from source.lib.helpers import ExportTable, AddToTableList

warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', None)
pandarallel.initialize(progress_bar = True)

def Main():
    OUTDIR = Path('output/analysis/descriptives/')
    issue_table_list_length = 7
    issue_closing_table_list_length = 12
    issue_comment_table_list_length = 13
    repo_col = 'repo_name'

    pr_data_indir = glob.glob('drive/output/scrape/extract_github_data/pull_request_data/*.csv')
    pr_data_indir.extend(glob.glob('drive/output/scrape/extract_github_data/pull_request_review_data/*.csv'))
    pr_data_indir.extend(glob.glob('drive/output/scrape/extract_github_data/pull_request_review_comment_data/*.csv'))
    pr_cols = ['type','created_at','repo_id','repo_name','actor_id','actor_login','pr_number', 'pr_title',
               'pr_body', 'pr_action','pr_merged_by_id','pr_merged_by_type','pr_label', 'pr_review_action',
               'pr_review_id','pr_review_state', 'pr_review_body', 'pr_review_comment_body']
    df_pr = ReadPrIssueData(pr_data_indir, pr_cols)

    issue_data_indir = glob.glob('drive/output/scrape/extract_github_data/issue_data/*.csv')
    issue_data_indir.extend(glob.glob('drive/output/scrape/extract_github_data/issue_comment_data/*.csv'))
    issue_cols = ['type','created_at','repo_id','repo_name','actor_id','actor_login','issue_number', 'issue_body','issue_title',
                  'issue_action','issue_state', 'issue_comment_id', 'issue_user_id', 'issue_comment_body']
    df_issue = ReadPrIssueData(issue_data_indir, issue_cols)

    issue_stats = GetIssueStats(df_issue, df_pr, issue_table_list_length, repo_col)
    ExportTable(OUTDIR / 'issue_stats.txt', 
                issue_stats, 'issue_stats',
            fmt = "%s")

    issue_closing_stats = GetIssueClosingStats(df_issue, df_pr, issue_closing_table_list_length, repo_col)
    ExportTable(OUTDIR / 'issue_closing_stats.txt', 
                issue_closing_stats, 'issue_closing_stats',
                fmt = "%s")

    issue_comment_stats = GetIssueCommentStats(df_issue, df_pr, issue_comment_table_list_length, repo_col)
    ExportTable(OUTDIR / 'issue_comment_stats.txt', 
                issue_comment_stats, 'issue_comment_stats',
                fmt = "%s")


def ReadPrIssueData(file_dirs, data_cols):
    df_final = pd.DataFrame(columns = data_cols)
    for file in file_dirs:
        df_part = pd.read_csv(file, nrows = 1)
        df_part_cols = [col for col in data_cols if col in df_part.columns]
        df_part = pd.read_csv(file, usecols = df_part_cols)
        df_final = pd.concat([df_final, df_part]).drop_duplicates()

    df_final = AddDates(df_final)

    return df_final

def AddDates(df):
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['date'] = df.parallel_apply(lambda x: f"{x['created_at'].year}-{x['created_at'].month}", axis = 1)

    return df

def ReturnMeanMedStd(pd_series):
    return [pd_series.mean(), np.median(pd_series), np.std(pd_series)]

def GetIssueStats(df_issue_selected, df_pr_selected, table_list_length, repo_col):
    issue_stats = []
    months_active = pd.concat([
        df_issue_selected[[repo_col, 'date']].drop_duplicates(),
        df_pr_selected[[repo_col, 'date']].drop_duplicates()
    ]).drop_duplicates().groupby(repo_col)['date'].count()
    proj_activity = [""]
    proj_activity.extend(ReturnMeanMedStd(months_active))

    issue_stats = AddToTableList(issue_stats, proj_activity, table_list_length)

    opened_activity = OpenCloseStats(df_issue_selected, 'issue_action == "opened"')
    closed_activity = OpenCloseStats(df_issue_selected, 'issue_action == "closed"')
    comment_activity = OpenCloseStats(df_issue_selected, 'type == "IssueCommentEvent"')

    issue_stats = AddToTableList(issue_stats, opened_activity, table_list_length)
    issue_stats = AddToTableList(issue_stats, closed_activity, table_list_length)
    issue_stats = AddToTableList(issue_stats, comment_activity, table_list_length)

    opened_people = PeopleStats(df_issue_selected, 'issue_action == "opened"')
    closed_people = PeopleStats(df_issue_selected, 'issue_action == "closed"')
    comment_people = PeopleStats(df_issue_selected, 'type == "IssueCommentEvent"')

    issue_stats = AddToTableList(issue_stats, opened_people, table_list_length)
    issue_stats = AddToTableList(issue_stats, closed_people, table_list_length)
    issue_stats = AddToTableList(issue_stats, comment_people, table_list_length)
    
    return issue_stats

def OpenCloseStats(df, query_filter):
    df_filtered = df.query(query_filter)
    df_filtered_stats = df_filtered.groupby(repo_col)['type'].count()
    
    df_filtered_month_stats = df_filtered.groupby([repo_col, 'date'])['type'].count()
    df_filtered_activity = [df_filtered.shape[0]]
    df_filtered_activity.extend(ReturnMeanMedStd(df_filtered_stats))
    df_filtered_activity.extend(ReturnMeanMedStd(df_filtered_month_stats))

    return df_filtered_activity

def PeopleStats(df, query_filter):
    df_filtered = df.query(query_filter)
    df_filtered_stats = df_filtered.groupby(repo_col)['actor_id'].nunique()
    
    df_filtered_month_stats = df_filtered.groupby([repo_col, 'date'])['actor_id'].nunique()
    df_filtered_activity = [len(df_filtered['actor_id'].unique())]
    df_filtered_activity.extend(ReturnMeanMedStd(df_filtered_stats))
    df_filtered_activity.extend(ReturnMeanMedStd(df_filtered_month_stats))

    return df_filtered_activity


def GetIssueClosingStats(df_issue_selected, df_pr_selected, table_list_length, repo_col):
    issue_closing_stats = []
    selcols = ['created_at',repo_col,'issue_number']

    opened_issues = df_issue_selected.query('issue_action == "opened"')[
        selcols].dropna().drop_duplicates().rename({'created_at':'opened_date'}, axis = 1)
    closed_issues = df_issue_selected.query('issue_action == "closed"')[
        selcols].dropna().drop_duplicates().rename({'created_at':'closed_date'}, axis = 1)
    df_merged_issues = pd.merge(opened_issues, closed_issues, how = 'left')
    df_merged_issues['closed'] = df_merged_issues['closed_date'].notna()
    num_issues = df_merged_issues[[repo_col,'issue_number']].drop_duplicates().shape[0]
    closed_pct_activity = [num_issues]
    closed_pct = df_merged_issues.groupby(repo_col)['closed'].mean()
    closed_pct_activity.extend(ReturnMeanMedStd(closed_pct))

    issue_closing_stats = AddToTableList(issue_closing_stats, closed_pct_activity, table_list_length)

    df_merged_issues['closing_time'] = df_merged_issues.parallel_apply(
        lambda x: (x['closed_date']-x['opened_date']).total_seconds(), axis = 1)
    closing_time_days = df_merged_issues.groupby(repo_col)['closing_time'].mean().dropna()/86400
    closing_time_activity = [num_issues]
    closing_time_activity.extend(ReturnMeanMedStd(closing_time_days))
    
    issue_closing_stats = AddToTableList(issue_closing_stats, closing_time_activity, table_list_length)

    for days in [30, 60, 180]:
        df_merged_issues[f'closed_{days}_days'] = df_merged_issues['closing_time'] <= days * 86400
        
    closed_cond = []
    closed_uncond = []
    for days in [30, 60, 180]:
        closed_timeline = f'closed_{days}_days'
        df_subset_cond_mean = df_merged_issues.dropna().groupby(repo_col)[closed_timeline].mean()
        num_closed_cond = df_merged_issues.dropna()[closed_timeline].sum()
        closed_cond.extend([num_closed_cond])
        closed_cond.extend(ReturnMeanMedStd(df_subset_cond_mean))
                          
        df_subset_uncond_mean = df_merged_issues.groupby(repo_col)[closed_timeline].mean()
        num_closed_uncond = df_merged_issues.dropna()[closed_timeline].sum()
        closed_uncond.extend([num_closed_uncond])
        closed_uncond.extend(ReturnMeanMedStd(df_subset_uncond_mean))

    issue_closing_stats = AddToTableList(issue_closing_stats, closed_cond, table_list_length)
    issue_closing_stats = AddToTableList(issue_closing_stats, closed_uncond, table_list_length)

    return issue_closing_stats

def GetIssueCommentStats(df_issue_selected, df_pr_selected, table_list_length, repo_col):
    issue_comment_stats = []
    
    df_issue_comments = df_issue_selected.query('type == "IssueCommentEvent"')
    df_issue_time = df_issue_selected.query('issue_action == "opened"')[[repo_col,'issue_number','created_at']].dropna()
    df_issue_time.rename({'created_at': 'opened_date'}, axis = 1, inplace = True)
    df_issue_comments_details = pd.merge(df_issue_time, df_issue_comments, how = 'left', on = [repo_col, 'issue_number'])

    num_comments = df_issue_comments_details[[repo_col,'issue_comment_id']].dropna().drop_duplicates().shape[0]
    num_issues = df_issue_comments_details[[repo_col,'issue_number']].drop_duplicates().shape[0]
    
    df_no_comments = df_issue_comments_details.query('issue_comment_id.isna()')[[repo_col,'issue_number']].assign(
        issue_comment_id = 0).set_index([repo_col,'issue_number'])
    df_has_comments = df_issue_comments_details.query('~issue_comment_id.isna()').groupby(
               [repo_col, 'issue_number'])['issue_comment_id'].nunique()
    issue_comment_counts = pd.concat([df_no_comments,df_has_comments])['issue_comment_id']
    comment_activity = [num_comments]
    comment_activity.extend(ReturnMeanMedStd(issue_comment_counts))
    issue_comment_stats = AddToTableList(issue_comment_stats, comment_activity, table_list_length)
    
    df_issue_comments_details['comment_time'] = df_issue_comments_details.parallel_apply(
        lambda x: (x['created_at']-x['opened_date']).total_seconds(), axis = 1)
    comment_time_days = df_issue_comments_details.groupby(repo_col)['comment_time'].mean().dropna()/86400

    comment_time_activity = [num_comments]
    comment_time_activity.extend(ReturnMeanMedStd(comment_time_days))
    issue_comment_stats = AddToTableList(issue_comment_stats, comment_time_activity, table_list_length)

    for days in [30, 60, 180]:
        df_issue_comments_details[f'comment_{days}_days'] = df_issue_comments_details['comment_time'] <= days * 86400

    comment_days_prop = [num_issues]
    comment_days_mean = [num_issues]
    for days in [30, 60, 180]:
        comment_timeline = f'comment_{days}_days'
        df_closed_prop = df_issue_comments_details.groupby(repo_col)[comment_timeline].mean()
        num_comments_days = df_issue_comments_details.query(f'{comment_timeline} == True')[[repo_col,'issue_comment_id']].drop_duplicates().shape[0]
        
        comment_days_prop.extend([num_comments_days])
        comment_days_prop.extend(ReturnMeanMedStd(df_closed_prop))
    
        df_closed_mean = df_issue_comments_details.query(f'{comment_timeline} == True').groupby(repo_col)['type'].count()
        comment_days_mean.extend([num_comments_days])
        comment_days_mean.extend(ReturnMeanMedStd(df_closed_mean))

    issue_comment_stats = AddToTableList(issue_comment_stats, comment_days_prop, table_list_length)
    issue_comment_stats = AddToTableList(issue_comment_stats, comment_days_mean, table_list_length)

    return issue_comment_stats


if __name__ == '__main__':
    Main()

