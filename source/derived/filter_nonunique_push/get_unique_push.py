import pandas as pd
import os
from ast import literal_eval
import numpy as np
import glob
from pathlib import Path
import multiprocessing

def Main():
    outdir = Path('drive/output/derived/filter_nonunique_push')
    df_pr_commits = pd.concat([ReadPrCommits(filename) for filename in glob.glob('drive/output/scrape/push_pr_commit_data/pull_request_data/*')])
    df_push_commits = pd.concat([ReadPushCommits(filename) for filename in glob.glob('drive/output/scrape/push_pr_commit_data/push_data/*')])
    
    df_push_commits.reset_index(inplace = True, drop = True)
    df_pr_commits.reset_index(inplace = True, drop = True)

    repo_list = df_push_commits['repo_name'].dropna().unique().tolist()
    
    with multiprocessing.Pool(processes=4) as pool:
        results = pool.map(PoolWorker, repo_list)

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

def GetUniquePushes(repo, df_push_commits, df_pr_commits):
    df_push_commits_repo = df_push_commits.query(f'repo_name == "{repo}"').explode('commit_list')
    df_pr_commits_repo = df_pr_commits.query(f'repo_name == "{repo}"').explode('commit_list')


    df_push_commits_merged = pd.merge(df_push_commits_repo, df_pr_commits_repo, 
                                      how = 'left', on = ['repo_name','commit_list'])
    df_push_commits_merged = df_push_commits_merged[df_push_commits_merged['pr_commits_url'].isna()]
    df_push_commits_uq = df_push_commits_merged.groupby(
        ['repo_name','push_id']).agg({'commit_urls_parsed': SelectFirst, 'commit_list': list}).reset_index()

    print(repo, df_push_commits_uq.shape)
    df_push_commits_uq.to_csv(outdir / f'unique_push_{repo.replace("/","___")}')

def PoolWorker(repo):
    return GetUniquePushes(repo, df_push_commits, df_pr_commits)

if __name__ == '__main__':
    Main()



