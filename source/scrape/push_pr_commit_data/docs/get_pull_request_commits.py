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

def Main():
    pandarallel.initialize(progress_bar=True)
    
    indir_pull = Path('drive/output/scrape/extract_github_data/pull_request_data/')
    outdir_pull = Path('drive/output/scrape/push_pr_commit_data/pull_request_data/')
    
    pr_dirs = []
    for subset_year in np.arange(2011, 2024, 1): 
        for subset_month in np.arange(1, 13, 1):
            if (subset_year != 2023) or (subset_year == 2023 and subset_month < 9):
                pr_dirs.append(f"pull_request_{subset_year}_{subset_month}.csv")
    random.shuffle(pr_dirs) # enables parallelization

    for pr_data in pr_dirs:
        already_scraped_list = os.listdir(outdir_pull)
        if pr_data not in already_scraped_list:
            df_pull = pd.read_csv(indir_pull / pr_data)[['Unnamed: 0', 'repo_name','pr_commits_url']]
            df_pull.to_csv(outdir_pull / pr_data) # as a placeholder
            
            df_pull['commit_list'] = df_pull.parallel_apply(lambda x: grabCommits(x['repo_name'], x['pr_commits_url']), axis = 1 )
            print(f"Commits for pull_request_data_{subset_year}_{subset_month}.csv obtained")
            df_pull.to_csv(outdir_pull / pr_data.replace('pull_request','pull_request_data')) # as a placeholder

def grabCommits(repo, pr_commits_url):
    try:
        pull_info = "/".join(pr_commits_url.split("/")[-3:-1]).replace("pulls","pull")
        scrape_url = f"https://github.com/{repo}/{pull_info}/commits"
        product = SoupStrainer('div', {'id': 'commits_bucket'})
        sesh = requests.Session() 
        page = sesh.get(scrape_url)
        page_text = str(page.text)
        if "Please wait a few minutes before you try again" in page_text:
            print('pausing, rate limit hit')
            time.sleep(120)
        soup = BeautifulSoup(page.content,parse_only = product,features="html.parser")
        commits = soup.find_all("a", attrs={"id":re.compile(r'commit-details*')})
        commit_urls = [c['href'].split("/")[-1] for c in commits]
        return commit_urls
    except Exception as e:
        error = str(e)
        return error

if __name__ == '__main__':
    Main()

