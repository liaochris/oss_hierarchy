#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from pathlib import Path
import os
import requests
import numpy as np

def Main():
    indir_push = Path('drive/output/scrape/extract_github_data/push_data/')
    outdir_push = Path('drive/output/scrape/push_pr_commit_data/push_data/')

    username = os.environ['PRIMARY_GITHUB_USERNAME']
    token = os.environ['PRIMARY_GITHUB_TOKEN']
    backup_username = os.environ['BACKUP_GITHUB_USERNAME']
    backup_token = os.environ['BACKUP_GITHUB_TOKEN']
    
    for subset_year in np.arange(2015, 2024, 1): # start at 2015 because commit data is unavailable earlier
        for subset_month in np.arange(1, 13, 1):
            if (subset_year != 2023) or (subset_year == 2023 and subset_month < 9):
                df_push = pd.read_csv(indir_push / f"push_{subset_year}_{subset_month}.csv", index_col = 0)
                df_push = df_push[['push_id', 'push_size', 'repo_name','push_before','push_head','commit_urls']].drop_duplicates()

                df_push_query_commits = df_push.query('push_size>20 | commit_urls == "[]"').index
                df_push.loc[df_push_query_commits, 'commit_urls'] = df_push.loc[df_push_query_commits].apply(
                    lambda x: getCommits(x['repo_name'], x['push_before'], x['push_head'], x['commit_urls'],
                                         username, token, backup_username, backup_token), axis = 1)

                print(f"Commits for push_data_{subset_year}_{subset_month}.csv obtained")
                df_push_final = df_push[['push_id', 'commit_urls']].to_csv(outdir_push / f"push_data_{subset_year}_{subset_month}.csv")

def getCommits(repo_info, before, head, original_urls, username, token, backup_username, backup_token, second_try = 0):
    api_url = f"https://api.github.com/repos/{repo_info}/compare/{before}...{head}"
    try:
        with requests.get(api_url, auth=(username,token)) as url:
            data = url.json()
        commits = data['commits']
        return [c['url'] for c in commits]
    except:
        if second_try == 1:
            return original_urls
        return getCommits(repo_info, before, head, original_urls, username, token, backup_username, backup_token, second_try = 1)


if __name__ == '__main__':
    Main()

