import pandas as pd
import numpy as np
import glob
from pandarallel import pandarallel
from bs4 import BeautifulSoup, SoupStrainer
import requests
import random
import os
import time
from pathlib import Path
import warnings

def readIssue(issue_files):
    df_issue = pd.concat([pd.read_csv(issue_file, usecols = ['repo_name','issue_number']) 
        for issue_file in issue_files]).drop_duplicates().dropna()

    return df_issue


def GrabIssueData(repo_name, issue_number):
    try:
        scrape_url = f"https://github.com/{repo_name}/issues/{issue_number}"
        product = SoupStrainer('a')
        sesh = requests.Session() 
        page = sesh.get(scrape_url)
        page_text = str(page.text)
        if "Please wait a few minutes before you try again" in page_text:
            print('pausing, rate limit hit')
            time.sleep(120)
            page = sesh.get(scrape_url)
            page_text = str(page.text)
            
        soup = BeautifulSoup(page.content,parse_only = product,features="html.parser")
        pr_links = soup.find_all("a", attrs={"data-hovercard-type":'pull_request'})
        pr_link = pr_links[0]['href']
        return pr_link
    except Exception as e:
        error = str(e)
        return error

def Main():
    pandarallel.initialize(progress_bar=True)
    warnings.filterwarnings("ignore")

    issue_files = glob.glob('drive/output/scrape/extract_github_data/issue*/*')
    commit_outdir = Path('drive/output/scrape/link_issue_pull_request')

    df_issue = readIssue(issue_files)
    repo_list = df_issue['repo_name'].unique().tolist()

    repo_list = sorted(repo_list)

    df_library_all = pd.DataFrame()
    for repo in repo_list:
        df_library = df_issue[df_issue['repo_name'] == repo]
        lib_name = repo.replace("/","_")
        print(lib_name)
        
        
        df_library['linked_pull_request'] = df_library.parallel_apply(lambda x: 
            GrabIssueData(x['repo_name'], x['issue_number']), axis = 1)
        
        df_library_all = pd.concat([df_library_all, df_library])
    df_library_all.drop('Unnamed: 0', axis = 1).to_parquet(commit_outdir / 'linked_issue_to_pull_request.parquet', index = False)

if __name__ == '__main__':   
    Main()