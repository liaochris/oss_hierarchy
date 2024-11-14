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
    outdir = Path('drive/output/derived/data_export')

    pr_data_indir = glob.glob('drive/output/scrape/extract_github_data/pull_request_data/*.csv')
    pr_data_indir.extend(glob.glob('drive/output/scrape/extract_github_data/pull_request_review_data/*.csv'))
    pr_data_indir.extend(glob.glob('drive/output/scrape/extract_github_data/pull_request_review_comment_data/*.csv'))
    pr_cols = ['type','created_at','repo_id','repo_name','actor_id','actor_login','pr_number', 'pr_title',
               'pr_body', 'pr_action','pr_merged_by_id','pr_merged_by_type','pr_label', 'pr_review_action',
               'pr_review_id','pr_review_state', 'pr_review_body', 'pr_review_comment_body']
    df_pr = ReadPrIssueData(pr_data_indir, pr_cols)
    df_pr.to_parquet(outdir / 'df_pr.parquet')
    
    issue_data_indir = glob.glob('drive/output/scrape/extract_github_data/issue_data/*.csv')
    issue_data_indir.extend(glob.glob('drive/output/scrape/extract_github_data/issue_comment_data/*.csv'))
    issue_cols = ['type','created_at','repo_id','repo_name','actor_id','actor_login','issue_number', 'issue_body','issue_title',
                  'issue_action','issue_state', 'issue_comment_id', 'issue_user_id', 'issue_comment_body']
    df_issue = ReadPrIssueData(issue_data_indir, issue_cols)
    df_pr.to_parquet(outdir / 'df_issue.parquet')

    
    commit_cols = ['repo_name','commit author name','commit author email', 'commit additions',
                   'commit deletions','commit changes total','commit files changed count', 'commit file changes',
                   'commit time']

    df_pr_commits = pd.concat([read_parquet(filename, commit_cols + ['pr_number']) for filename in 
                               glob.glob('drive/output/scrape/collect_commits/pr/*')])
    df_pr_commits['commit file changes'] = df_pr_commits['commit file changes'].astype(str)
    df_pr_commits.to_parquet(outdir / 'df_pr_commits.parquet')

    
    df_push_commits = pd.concat([read_parquet(filename, commit_cols + ['push_id']) for filename in glob.glob('drive/output/scrape/collect_commits/push/*')])
    df_push_commits['commit file changes'] = df_push_commits['commit file changes'].astype(str)
    df_push_commits.to_parquet(outdir / 'df_push_commits.parquet')
    

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
    df['created_at'] = pd.to_datetime(df['created_at'], errors = 'coerce')
    df = df[~df['created_at'].isna()]
    df['date'] = df.parallel_apply(lambda x: f"{x['created_at'].year}-{x['created_at'].month}", axis = 1)

    return df

def read_parquet(filename, commit_cols):
    try:
        df = pd.read_parquet(filename).drop_duplicates('commit sha')[commit_cols]
        return df
    except:
        return 

def read_csv(filename, commit_cols):
    try:
        df = pd.read_csv(filename, index_col = 0).drop_duplicates('commit sha')[commit_cols]
        return df
    except:
        return 

if __name__ == '__main__':
    Main()
