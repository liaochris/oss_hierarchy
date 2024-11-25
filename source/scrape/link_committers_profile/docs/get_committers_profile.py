import pandas as pd
import numpy as np
from ast import literal_eval
from collections import OrderedDict, defaultdict
import requests
import time
import re
import random
import glob
import csv
import sys
from pandarallel import pandarallel
import os

csv.field_size_limit(sys.maxsize)
pandarallel.initialize(progress_bar = True)


def Main():
    ncount = 1000
    username = os.environ['PRIMARY_GITHUB_USERNAME']
    token = os.environ['PRIMARY_GITHUB_TOKEN']

    for file_type in ['push']:
        df_committers_full = pd.concat([ReadInputFiles(file, file_type) for file in glob.glob(f'drive/output/scrape/collect_commits/{file_type}/*.parquet')])
        df_committers_match, indices = PrepareMergeComponents(df_committers_full, ncount)
        IterateInBatch(df_committers_match, indices, username, token, file_type, ncount)


def ReadInputFiles(file, file_type):
    if file_type == 'push':
        id_col = 'push_id'        
    if file_type == 'pr':
        id_col = 'pr_number'        
    print(file)
    selcols = [id_col, 'repo_name', 'commit author name', 'commit author email', 'committer name', 'commmitter email', 'commit sha']
    df_committers_full_subset = pd.read_parquet(file, engine = 'fastparquet')[selcols]
    df_committers_full_subset = df_committers_full_subset.drop_duplicates()
    return df_committers_full_subset


def CreateMatchDataset(df_committers_full):
    df_committers_match = pd.concat([
        df_committers_full[['commit author name', 'commit author email']].drop_duplicates().rename(
            {'commit author name': 'name', 'commit author email': 'email'}, axis = 1),
        df_committers_full[['committer name', 'committer email']].drop_duplicates().rename(
        {'committer name': 'name', 'committer email': 'email'}, axis = 1)])[['name', 'email']].drop_duplicates().dropna()
    for indiv_type in ['commit author', 'committer']:
        df_committers_full[f'{indiv_type} details'] = df_committers_full.parallel_apply(
            lambda x: x[f'{indiv_type} name'] + "_" + x[f'{indiv_type} email'] if 
            not pd.isnull(x[f'{indiv_type} name']) and not pd.isnull(x[f'{indiv_type} email']) else np.nan, axis = 1)
        df_committers_full['commit_repo'] = df_committers_full.parallel_apply(lambda x: x['commit sha'] + "_" + x['repo_name'], axis = 1)
        df_emails = df_committers_full[~df_committers_full[f'{indiv_type} name'].isna()].groupby(
            'commit author details')[['commit_repo']].agg(list)
        df_emails['commit_repo'] = df_emails['commit_repo'].parallel_apply(lambda x: random.sample(x, min(5, len(x))))
        dict_emails = df_emails.to_dict()['commit_repo']
        if indiv_type == 'commit author':
            df_committers_match['commit_repo'] = df_committers_match.parallel_apply(
                lambda x: dict_emails.get(x['name']+"_"+x['email'], np.nan), axis = 1)
        else:
            df_committers_match['user_type'] = df_committers_match['commit_repo'].parallel_apply(
                lambda x: 'author' if type(x) == list else 'committer')
            df_committers_match['commit_repo'] = df_committers_match.parallel_apply(
                lambda x: dict_emails.get(x['name']+"_"+x['email'], np.nan) 
                if type(x['commit_repo']) != list else x['commit_repo'], axis = 1)
    return df_committers_full, df_committers_match

def PrepareMergeComponents(df_committers_full, ncount):
    df_committers_full.rename({'commmitter email':'committer email'}, axis = 1, inplace = True)
    df_committers_full, df_committers_match = CreateMatchDataset(df_committers_full)
    df_committers_match.reset_index(drop = True, inplace = True)
    indices = np.array_split(df_committers_match.index, ncount)
    return df_committers_match, indices


def IterateInBatch(df_committers_match, indices, username, token, file_type, ncount):
    for i in np.arange(0, ncount, 1):
        start = time.time()
        print(f"Iter {i}")
        df_committers_match.loc[indices[i], 'committer_info'] = df_committers_match.loc[indices[i]].apply(
            lambda x: GetCommitterData(x['commit_repo'], x['user_type'], username, token) if type(x['commit_repo']) == list else np.nan, axis = 1)
        df_committers_match.to_csv(f'drive/output/scrape/link_committers_profile/committers_info_{file_type}_special.csv')
        end = time.time()
        print(f"Scaled production/hour: {len(indices[i])/(end-start) * 3600}")
        print(f"Number of people linked {df_committers_match.loc[indices[i], 'committer_info'].apply(lambda x: 1 if type(x) == list else 0).sum()}")


def GetCommitterData(commit_repo, user_type, username, token):
    success = False
    i = 0
    while (not success) and i < len(commit_repo):
        repo_info = commit_repo[i].split("_")[1]
        sha = commit_repo[i].split("_")[0]
        api_url = f"https://api.github.com/repos/{repo_info}/commits/{sha}"
        with requests.get(api_url, auth=(username,token)) as url:
            try:
                time.sleep(.25)
                data = url.json()
                if 'message' in data.keys() and 'API rate limit exceeded' in data['message']:
                    username = os.environ['BACKUP_GITHUB_USERNAME']
                    token = os.environ['BACKUP_GITHUB_TOKEN']
                    continue    
                info = data[user_type]
                if info != None:
                    success = True
                    return [info['login'], info['id'], info['type'], info['site_admin']]
                i+=1
            except Exception as e:  
                print(e)  
                print(data)
                i+=1
    return np.nan


if __name__ == '__main__':
    Main()

