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

csv.field_size_limit(sys.maxsize)
pandarallel.initialize(progress_bar = True)


def Main():
    df_committers_full = pd.concat([ReadPullRequests(file) for file in glob.glob('drive/output/scrape/collect_commits/*.csv')])
    df_committers_full.rename({'commmitter email':'committer email'}, axis = 1, inplace = True)
    df_committers_full, df_committers_match = CreateMatchDataset(df_committers_full)


    ncount = 1000
    df_committers_match.reset_index(drop = True, inplace = True)
    indices = np.array_split(df_committers_match[df_committers_match['committer_info'].isna()].index, ncount)

    for i in np.arange(0, ncount, 1):
        print(f"Iter {i}")
        df_committers_match.loc[indices[i], 'committer_info'] = df_committers_match.loc[indices[i]].apply(
            lambda x: GetCommitterData(x['commit_repo'], x['user_type']) if type(x['commit_repo']) == list else np.nan, axis = 1)
        df_committers_match.to_csv('drive/output/scrape/link_committers_profile/committers_info.csv')

def ReadPullRequests(file):
    print(file)
    selcols = ['pr_number', 'repo_name', 'commit author name', 'commit author email', 'committer name', 'commmitter email', 'commit sha']
    try:
        df_committers_full_subset = pd.read_csv(file, usecols = selcols, engine = 'pyarrow')
    except:
        df_committers_full_subset = pd.read_csv(file, usecols = selcols, engine="python", on_bad_lines = 'warn')        
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

def GetCommitterData(commit_repo, user_type):
    success = False
    i = 0
    username = os.environ['PRIMARY_GITHUB_USERNAME']
    token = os.environ['PRIMARY_GITHUB_TOKEN']

    while (not success) or i < len(commit_repo):
        repo_info = commit_repo[i].split("_")[1]
        sha = commit_repo[i].split("_")[0]
        api_url = f"https://api.github.com/repos/{repo_info}/commits/{sha}"
        with requests.get(api_url, auth=(username,token)) as url:
            try:
                data = url.json()
                info = data[user_type]
                time.sleep(.75)
                if info != None:
                    success = True
                    return [info['login'], info['id'], info['type'], info['site_admin']]
                i+=1
            except:
                print(data)
                i+=1
    return np.nan


