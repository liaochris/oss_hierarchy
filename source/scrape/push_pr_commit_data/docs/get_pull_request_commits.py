from bs4 import BeautifulSoup, SoupStrainer
import requests
from pandarallel import pandarallel
import random
import os
from pathlib import Path
import numpy as np
import pandas as pd
import re
import time
import ast
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

def Main():
    pandarallel.initialize(progress_bar=True)
    
    indir_pull = Path('drive/output/scrape/extract_github_data/pull_request_data/')
    outdir_pull = Path('drive/output/scrape/push_pr_commit_data/pull_request_data/')
    
    pr_dirs = []
    for subset_year in np.arange(2015, 2024, 1): 
        for subset_month in np.arange(1, 13, 1):
            if (subset_year != 2023) or (subset_year == 2023 and subset_month < 9):
                pr_dirs.append(f"pull_request_{subset_year}_{subset_month}.csv")
    random.shuffle(pr_dirs) # enables parallelization

    for pr_data in pr_dirs:
        pr_file_name = pr_data.replace('pull_request','pull_request_data')
        already_scraped_list = os.listdir(outdir_pull)
        if pr_file_name not in already_scraped_list:
            df_pull = pd.read_csv(indir_pull / pr_data)[['repo_name','pr_commits_url']].drop_duplicates()
            df_pull.to_csv(outdir_pull / pr_file_name) # enables parallelization
            df_pull['commit_list'] = np.nan
        else:
            df_pull = pd.read_csv(indir_pull / pr_data)[['repo_name','pr_commits_url']].drop_duplicates()
            df_pull_existing = pd.read_csv(outdir_pull / pr_file_name)
            for col in ['Unnamed: 0', 'Unnamed: 0.1']:
                if col in df_pull_existing.columns:
                    df_pull_existing = df_pull_existing.drop(col, axis = 1)
            df_pull_existing = df_pull_existing.drop_duplicates()

            print(pr_data)
            df_pull = pd.merge(df_pull, df_pull_existing, how = 'left')
            if 'commit_list' not in df_pull.columns:
                df_pull['commit_list'] = np.nan
            else:
                commit_list = ~df_pull['commit_list'].isna() & ~df_pull['commit_list'].apply(lambda x: type(x) == str and x.startswith("HTTP Error 40"))
                df_pull.loc[commit_list, 'commit_list'] = df_pull.loc[commit_list, 'commit_list'].apply(ast.literal_eval)
                df_pull['commit_list'] = df_pull['commit_list'].apply(lambda x: np.nan if type(x) == list and len(x) == 0 else x)
        
        df_pull = df_pull.reset_index(drop = True)
        for i in df_pull.index:
            if (i%100 == 0):
                print(f"{i} commits for {pr_data} obtained")
            if type(df_pull.loc[i, 'commit_list']) != list:
                df_pull.loc[i, 'commit_list'] = GrabCommits(df_pull.loc[i, 'repo_name'], df_pull.loc[i, 'pr_commits_url'])
                
        print(f"Commits for {pr_data} obtained")
        df_pull.to_csv(outdir_pull / pr_file_name)

def GrabCommits(repo, pr_commits_url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.90 Safari/537.36',
        'Origin': 'http://example.com',
        'Referer': 'http://example.com/some_page'
    }
    try:
        pull_info = "/".join(pr_commits_url.split("/")[-3:-1]).replace("pulls","pull")
        scrape_url = f"https://github.com/{repo}/{pull_info}/commits"
        req = Request(scrape_url, headers=headers)
        response = urlopen(req)
        page_content = response.read()
        page_text = page_content.decode('utf-8', errors='replace') 

        if "Please wait a few minutes before you try again" in page_text or "Looks like something went wrong!" in page_text or response == 429:
            print('pausing, rate limit hit')
            time.sleep(120)
            req = Request(scrape_url, headers=headers)
            response = urlopen(req)
            page_content = response.read()

        soup = BeautifulSoup(page_content, features="html.parser")
        commits_pattern = re.compile(r".*/commits/[0-9a-f]+")
        commits = soup.find_all("a", href=commits_pattern)
        commit_urls = [c['href'].split("/")[-1] for c in commits]

        unique_commit_urls = list(dict.fromkeys(commit_urls)) 
        return unique_commit_urls
    except Exception as e:
        error = str(e)
        return error

if __name__ == '__main__':
    Main()

