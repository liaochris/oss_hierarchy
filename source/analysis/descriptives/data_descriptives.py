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

    repo_col = 'repo_name'
    table_list_length = 6

    df_pr_selected = df_pr
    df_issue_selected = df_issue
    
    project_stats = GetProjectStats(df_issue_selected, df_pr_selected, table_list_length, repo_col)
    ExportTable(OUTDIR / 'project_stats.txt', 
            project_stats, 'project_stats',
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

def GetProjectStats(df_issue_selected, df_pr_selected, table_list_length, repo_col):
    project_stats = []
    months_active = pd.concat([
        df_issue_selected[[repo_col, 'date']].drop_duplicates(),
        df_pr_selected[[repo_col, 'date']].drop_duplicates()
    ]).drop_duplicates().groupby(repo_col)['date'].count()
    proj_activity = [""]
    proj_activity.extend(returnMeanMedStd(months_active))

    project_stats = AddToTableList(project_stats, proj_activity, table_list_length)

    opened_activity = OpenCloseStats(df_issue_selected, 'issue_action == "opened"')
    closed_activity = OpenCloseStats(df_issue_selected, 'issue_action == "closed"')
    comment_activity = OpenCloseStats(df_issue_selected, 'type == "IssueCommentEvent"')

    project_stats = AddToTableList(project_stats, opened_activity, table_list_length)
    project_stats = AddToTableList(project_stats, closed_activity, table_list_length)
    project_stats = AddToTableList(project_stats, comment_activity, table_list_length)

    return project_stats

def OpenCloseStats(df, query_filter):
    df_filtered = df.query(query_filter)
    df_filtered_stats = df_filtered.groupby(repo_col)['type'].count()
    
    df_filtered_month_stats = df_filtered.groupby([repo_col, 'date'])['type'].count()
    df_filtered_activity = [df_filtered.shape[0]]
    df_filtered_activity.extend(returnMeanMedStd(df_filtered_stats))
    df_filtered_activity.extend(returnMeanMedStd(df_filtered_month_stats))

    return df_filtered_activity