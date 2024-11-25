import pandas as pd
import os
from ast import literal_eval
import numpy as np
import glob
from pathlib import Path
import multiprocessing
from pandarallel import pandarallel

def Main():
    pandarallel.initialize(progress_bar = True)
    outdir = Path('drive/output/derived/filter_nonunique_push')
    
    df_pr_commits = pd.concat([ReadPrCommits(filename) for filename in glob.glob('drive/output/scrape/push_pr_commit_data/pull_request_data/*')]).reset_index(drop = True)
    df_push_commits = pd.concat([ReadPushCommits(filename) for filename in glob.glob('drive/output/scrape/push_pr_commit_data/push_data/*')]).reset_index(drop = True)
    repo_list = df_push_commits['repo_name'].dropna().unique().tolist()

    df_return = []
    for repo in repo_list:
        df_repo = GetUniquePushes(repo, df_push_commits, df_pr_commits, outdir)
        df_return.append(df_repo)

    df_push = pd.concat(df_return)
    df_push = GetHelperColumns(df_push)
    
    push_cols = ['push_id','push_before','push_head', 'repo_id', 'repo_name', 'actor_id', 'actor_login', 'org_id', 'org_login']
    df_push_before_head = pd.concat([pd.read_csv(filename, usecols = push_cols) for filename in glob.glob('drive/output/scrape/extract_github_data/push_data/*')])
    
    df_push_final = AddPushBeforeHead(df_push, df_push_before_head)
    df_push_final.drop(
        ['commit_urls_parsed','commit_urls_parsed_length','type','first_commit_position'], axis = 1, inplace = True)
    df_push_final.to_csv(outdir / 'non_unique_push_complete.csv')
    

def ReadPrCommits(filename):
    df_pr_commits = pd.read_csv(filename, index_col = 0).drop(
        'Unnamed: 0', axis = 1)
    if 'commit_list' in df_pr_commits.columns:
        df_pr_commits['commit_list'] = df_pr_commits['commit_list'].apply(
            lambda x: literal_eval(x) if x != "'float' object has no attribute 'split'" else np.nan)
        return df_pr_commits.dropna()

def ReadPushCommits(filename):
    df_push_commits = pd.read_csv(filename, index_col = 0)
    df_push_commits['commit_urls_parsed'] = df_push_commits['commit_urls'].apply(literal_eval)
    df_push_commits['commit_urls_parsed'] = df_push_commits['commit_urls_parsed'].apply(
        lambda x: [ele if ele.startswith("https:") else "https:"+ele for ele in x[0].split("https:")][1:] if len(x) == 1 else x)
    df_push_commits['commit_list'] = df_push_commits['commit_urls_parsed'].apply(lambda x: [ele.split("/")[-1] for ele in x])
    df_push_commits['repo_name'] = df_push_commits['commit_urls_parsed'].apply(lambda x: "/".join(x[0].split("/")[4:6]) if len(x)>0 else np.nan)

    return df_push_commits

def SelectFirst(x):
    return x.loc[x.index[0]]

def GetUniquePushes(repo, df_push_commits, df_pr_commits, outdir):
    df_push_commits_repo = df_push_commits.query(f'repo_name == "{repo}"').explode('commit_list')
    df_pr_commits_repo = df_pr_commits.query(f'repo_name == "{repo}"').explode('commit_list')


    df_push_commits_merged = pd.merge(df_push_commits_repo, df_pr_commits_repo, 
                                      how = 'left', on = ['repo_name','commit_list'])
    df_push_commits_merged = df_push_commits_merged[df_push_commits_merged['pr_commits_url'].isna()]
    df_push_commits_uq = df_push_commits_merged.groupby(
        ['repo_name','push_id']).agg({'commit_urls_parsed': SelectFirst, 'commit_list': list}).reset_index()

    print(repo, df_push_commits_uq.shape)
    return df_push_commits_uq


def GetHelperColumns(df_push):
    
    df_push['commit_urls_parsed_length'] = df_push['commit_urls_parsed'].parallel_apply(len)
    df_push['commit_list_length'] = df_push['commit_list'].parallel_apply(len)
    
    df_push.loc[df_push['commit_urls_parsed_length'] != df_push['commit_list_length'], 'type'] = 'handle'
    df_push.loc[df_push['commit_urls_parsed_length'] == df_push['commit_list_length'], 'type'] = 'standard'
    
    df_push['commit_list_full'] = df_push['commit_urls_parsed'].apply(lambda x: [commit_url.split("/")[-1] for commit_url in x])
    df_push['first_commit_position'] = df_push.apply(
        lambda x: x['commit_list_full'].index(x['commit_list'][0]) if x['commit_list'][0] in x['commit_list_full'] else -1, axis = 1)

    return df_push

def AddPushBeforeHead(df_push, df_push_before_head):

    df_push_standard = pd.merge(df_push.query('type == "standard"'), df_push_before_head, how = 'left', on = ['repo_name','push_id'])

    df_push_handle_beforemod = pd.merge(df_push.query('type == "handle" & first_commit_position+commit_list_length==commit_urls_parsed_length'),
                                        df_push_before_head.drop('push_before', axis = 1), how = 'left', on = ['repo_name','push_id'])
    df_push_handle_beforemod['push_before'] = ImputePushBefore(df_push_handle_beforemod)
    
    df_push_handle_headmod = pd.merge(
        df_push.query('type == "handle" & first_commit_position+commit_list_length<commit_urls_parsed_length & first_commit_position == 0'),
        df_push_before_head.drop('push_head', axis = 1), how = 'left', on = ['repo_name','push_id'])
    df_push_handle_headmod['push_head'] = ImputePushHead(df_push_handle_headmod)
    
    df_push_handle_beforeheadmod = pd.merge(
        df_push.query('type == "handle" & first_commit_position+commit_list_length<commit_urls_parsed_length & first_commit_position > 0'),
        df_push_before_head.drop(['push_before','push_head'], axis = 1), how = 'left', on = ['repo_name','push_id'])
    df_push_handle_beforeheadmod['push_before'] = ImputePushBefore(df_push_handle_beforeheadmod)
    df_push_handle_beforeheadmod['push_head'] = ImputePushHead(df_push_handle_beforeheadmod)
    
    df_push_final = pd.concat([df_push_standard, df_push_handle_beforemod, df_push_handle_headmod, df_push_handle_beforemod]).reset_index(drop = True)

    return df_push_final

def ImputePushBefore(df):
    push_before = df.apply(lambda x: x['commit_list_full'][x['first_commit_position']-1], axis = 1)
    return push_before

def ImputePushHead(df):
    push_head = df.apply(lambda x: x['commit_list_full'][x['commit_list_length']+x['first_commit_position']], axis = 1)
    return push_head


if __name__ == '__main__':
    Main()



