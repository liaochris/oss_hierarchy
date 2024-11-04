#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
import ast
import pandas as pd
import numpy as np
from pygit2 import Object, Repository, GIT_SORT_TIME
from pygit2 import init_repository, Patch
from tqdm import tqdm
from pandarallel import pandarallel
import subprocess
import warnings
from joblib import Parallel, delayed
import os
import multiprocessing
import time
import random
from source.scrape.collect_commits.docs.get_commit_data_helpers import getCommitData

def Main():
    pandarallel.initialize(progress_bar=True)
    warnings.filterwarnings("ignore")
    tqdm.pandas()

    pr_indir = Path('drive/output/scrape/push_pr_commit_data/pull_request_data')
    commits_outdir = Path('drive/output/scrape/collect_commits/pr')

    df_pull_request = importPullRequestData(pr_indir)
    
    github_repos = df_pull_request['repo_name'].unique().tolist()
    github_repos = [library for library in github_repos if "/" in library]
    random.shuffle(github_repos)

    pr_commit_cols = ['repo_name', 'pr_number', 'pr_commits_url', 'parent_commit', 'commit_groups']
    for library in github_repos:
        print(library)
        df_library = df_pull_request[df_pull_request['repo_name'] == library]
        getCommitData(library, commits_outdir, df_library, pr_commit_cols, 'pr')
            
    print("Done!")

push_commit_cols = ['repo_name', 'push_id', 'commit_list', 'push_before', 'push_head', 'commit_list_length']


def importPullRequestData(pr_indir):
    df_pull_request = pd.DataFrame(columns = ['Unnamed: 0', 'repo_name', 'pr_commits_url', 'commit_list'])
    for subset_year in np.arange(2011, 2024, 1): 
        for subset_month in np.arange(1, 13, 1):
            if (subset_year != 2023) or (subset_year == 2023 and subset_month < 9):
                try:
                    df_pull_request_commits = pd.read_csv(pr_indir / f'pull_request_data_{subset_year}_{subset_month}.csv', index_col = 0)
                    df_pull_request = pd.concat([df_pull_request, df_pull_request_commits])
                except:
                    pass
    df_pull_request.drop('Unnamed: 0', axis = 1, inplace = True)
    df_pull_request['pr_number'] = df_pull_request['pr_commits_url'].parallel_apply(lambda x: x.split("/")[-2] if not pd.isnull(x) else x)

    return df_pull_request
    


if __name__ == '__main__':   
    Main()
    