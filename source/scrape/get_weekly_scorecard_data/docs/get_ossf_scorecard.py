
import os
import warnings
from pathlib import Path
import pandas as pd
import numpy as np
from pandarallel import pandarallel
import random
import glob
import subprocess
from pygit2 import Object, Repository, GIT_SORT_TIME, enums, init_repository, Patch
import sys
from source.scrape.collect_commits.docs.get_commit_data_pr import importPullRequestData
import time
import multiprocess
from bson import json_util
import json
import concurrent.futures
import shutil


def Main():
    warnings.filterwarnings("ignore")
    pandarallel.initialize(progress_bar = True)

    pr_indir = Path('drive/output/scrape/push_pr_commit_data/pull_request_data')

    df_pull_request = importPullRequestData(pr_indir)

    github_repos = df_pull_request['repo_name'].unique().tolist()
    github_repos = [library for library in github_repos if "/" in library]
    random.shuffle(github_repos)


    with multiprocess.Pool(8) as pool:
        for result in pool.imap(GetScorecard, github_repos):
            print(result)

    print("Done!")


def GetScorecard(library):
    scorecard_outdir = Path('drive/output/scrape/get_weekly_scorecard_data')
    print(library)
    lib_name = library.split("/")[1]
    lib_renamed = library.replace("/","_")
    if f'scorecard_{lib_renamed}.csv' not in os.listdir(scorecard_outdir / 'scorecard'):
        try:
            print(f"Starting {library}")
            start = time.time()
            if lib_renamed not in os.listdir(scorecard_outdir / 'github_repos'):
                df_commits_dict = IterateThroughCommits(library, lib_renamed, scorecard_outdir)
                df_commits = pd.DataFrame(df_commits_dict)
                df_commits.to_csv(scorecard_outdir / f'scorecard/scorecard_{lib_renamed}.csv')
                subprocess.Popen(["rm", "-rf", f"scorecard_{lib_renamed}.json"], cwd = scorecard_outdir / 'scorecard').communicate()
                shutil.rmtree(scorecard_outdir / 'github_repos' / f"{lib_renamed}")
                end = time.time()
                print(f"{library} completed in {start - end}")
            else:
                print(f"skipping {lib_renamed} because it's a concurrent process")
            return "success"
        except Exception as e:
            print(e)
            return f"failure, {str(e)}"
    return 'success'

def IterateThroughCommits(library, lib_renamed, scorecard_outdir):
    start = time.time()
    subprocess.Popen(["git", "clone", f"https://github.com/{library}.git", f"{lib_renamed}"], cwd = scorecard_outdir  / 'github_repos').communicate()

    global repo
    cloned_repo_location = scorecard_outdir / 'github_repos' / lib_renamed
    repo = Repository(cloned_repo_location)
    latest_commit = repo[repo.head.target]

    data_commits = [[commit.commit_time, commit.id] for commit in repo.walk(latest_commit.id, enums.SortMode.TIME)]
    df_commits = pd.DataFrame(data_commits, columns = ['time','commit_sha'])
    
    df_commits['date'] = pd.to_datetime(df_commits['time'], unit = 's')
    df_commits['week'] = df_commits['date'].apply(lambda x: x.week)
    df_commits['year'] = df_commits['date'].apply(lambda x: x.year)
    df_commits['commit_sha'] = df_commits['commit_sha'].apply(lambda x: str(x))
    df_commits_log = df_commits.copy()
    df_commits_log.to_parquet(scorecard_outdir / f'commit_logs/commit_logs_{lib_renamed}.parquet')

    df_commits = df_commits.sort_values('date', ascending = True).drop_duplicates(['week','year'])
    df_commits = df_commits.sort_values('date', ascending = False).reset_index(drop = True)
    df_commits_dict = df_commits.to_dict('index')

    for commit_num in  df_commits.index:
        df_commits_dict[commit_num]['date'] = str(df_commits_dict[commit_num]['date'])

    print(f"Getting {df_commits.shape[0]} commits")
    
    commit_list = df_commits.index
    if f'scorecard_{lib_renamed}.json' in os.listdir(scorecard_outdir / 'scorecard'):
        with open(scorecard_outdir / f'scorecard/scorecard_{lib_renamed}.json', 'r') as file:
            df_commits_dict = json.load(file)
        i=0
        while type(df_commits_dict[commit_num].get('scorecard_data', False) != bool):
            df_commits_dict[commit_num]
            i+=1
        commit_list = commit_list[i:]
    
    for commit_num in commit_list:
        if commit_num % 10 == 0:
            print(commit_num)
        try:
            subprocess.Popen(["git", "reset", "--hard", str(df_commits.loc[commit_num, 'commit_sha'])], 
                cwd = cloned_repo_location, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL).wait()
            scorecard_command = "scorecard --local=. --show-details --format json"
            scorecard_json = json.loads(GetProcessOutput(scorecard_command, cloned_repo_location))
            scorecard_json['repo']['name'] = library
            scorecard_json['repo']['commit'] = str(df_commits.loc[commit_num, 'commit_sha'])
            scorecard_json['scorecard']['commit'] = str(scorecard_json['scorecard']['commit'])
            df_commits_dict[commit_num]['scorecard_data'] = scorecard_json
            
            with open(scorecard_outdir / f'scorecard/scorecard_{lib_renamed}.json', "w") as outfile:
                df_commits_dict[commit_num]['date'] = str(df_commits_dict[commit_num]['date'])
                df_commits_dict_json = json.dumps(df_commits_dict)
                outfile.write(df_commits_dict_json)
        
        except Exception as e:
            print(e)
            continue

    end = time.time()
    print(end - start)
    
    while lib_renamed in os.listdir(scorecard_outdir / 'github_repos'):
        try:
            shutil.rmtree(cloned_repo_location)
        except Exception as e:
            print(e)

    return df_commits_dict

def GetProcessOutput(cmd, cwd):
    process = subprocess.Popen(cmd, cwd = cwd, shell=True, stdout=subprocess.PIPE)
    process.wait()
    data, err = process.communicate()
    if process.returncode == 0:
        return data.decode('utf-8')
    else:
        print("Error:", err)
    return ""


if __name__ == '__main__':
    Main()

