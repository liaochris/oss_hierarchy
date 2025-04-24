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
import re
def ReadPullRequests(pull_request_files):
    df_pull_request = pd.concat([pd.read_csv(pull_request_file, usecols = ['repo_name','pr_number']) 
                                 for pull_request_file in pull_request_files]).drop_duplicates().dropna()

    return df_pull_request


def GrabPullRequestData(repo_name, pull_request_number):
    try:
        scrape_url = f"https://github.com/{repo_name}/issues/{pull_request_number}"
        product = SoupStrainer('a')
        product_title = SoupStrainer('bdi')
        product_text = SoupStrainer('div')
        sesh = requests.Session() 
        page = sesh.get(scrape_url)
        page_text = str(page.text)
        if "Please wait a few minutes before you try again" in page_text:
            print('pausing, rate limit hit')
            time.sleep(120)
            page = sesh.get(scrape_url)
            page_text = str(page.text)
 
        issue_link = 'missing'
        title = 'missing'
        text = 'missing'

        soup_github = BeautifulSoup(page.content, features="html.parser")
        p = soup_github.find("p", string=lambda t: t and "Successfully merging this pull request may close these issues." in t)
        span = p.find_next_sibling("span") if p else None
        issue_links = [a["href"] for a in span.find_all("a", {"data-hovercard-type": "issue"})] if span else []

        soup = BeautifulSoup(page.content, parse_only = product, features="html.parser")
        other_links = soup.find_all("a")
        links = list(set([link['href'] for link in other_links if 'href' in link.attrs]))
        url_pattern = re.compile(r'^https://github\.com/([^/]+)/([^/]+)/(issues|pull)/([^/]+)$')
        pattern = re.compile(r'^/(.*)/(issues|pull)/(\d+)$')
        github_links = [github_link for github_link in links if pattern.match(github_link) or url_pattern.match(github_link)]

        soup_title = BeautifulSoup(page.content, parse_only = product_title, features="html.parser")        
        title_links = soup_title.find_all("bdi", attrs={"class":'js-issue-title'})
        if len(title_links)>0:
            title = title_links[0].text

        soup_text = BeautifulSoup(page.content, parse_only = product_text, features="html.parser")        
        text_links = soup_text.find_all("div", attrs={"class":'comment-body'})
        if len(text_links)>0:
            text = text_links[0].text

        return [issue_links, github_links, title, text]
    except Exception as e:
        error = str(e)
        return error

def Main():
    pandarallel.initialize(progress_bar=True)
    warnings.filterwarnings("ignore")

    pull_request_files = glob.glob('drive/output/scrape/extract_github_data/pull_request*/*')
    commit_outdir = Path('drive/output/scrape/link_issue_pull_request')

    df_pull_request = ReadPullRequests(pull_request_files)
    repo_list = df_pull_request['repo_name'].unique().tolist()
    repo_list = sorted(repo_list)

    df_library_all = pd.DataFrame()
    for repo in repo_list:
        print(repo)
        print(np.where([ele == repo for ele in repo_list]))
        df_library = df_pull_request[df_pull_request['repo_name'] == repo]
        if df_library.shape[0] == 0: continue 

        df_library['linked_issue'] = df_library.apply(lambda x: 
            GrabPullRequestData(x['repo_name'], x['pr_number']), axis = 1)
        
        df_library['linked_issue'] = df_library.apply(lambda x: 
            GrabPullRequestData(x['repo_name'], x['pr_number']) if 
            type(x['linked_issue']) == str else x['linked_issue'], axis = 1)
        

        df_library['issue_link'] = df_library['linked_issue'].apply(lambda x: x[0] if type(x) == list else x)
        df_library['other_links'] = df_library['linked_issue'].apply(lambda x: x[1] if type(x) == list else x)
        df_library['pull_request_title'] = df_library['linked_issue'].apply(lambda x: x[2] if type(x) == list else x)
        df_library['pull_request_text'] = df_library['linked_issue'].apply(lambda x: x[3] if type(x) == list else x)

        df_library_all = pd.concat([df_library_all, df_library])
        df_library_all.drop('linked_issue', axis = 1).to_parquet(commit_outdir / 'linked_pull_request_to_issue.parquet', index = False)

    df_library_all.drop('linked_issue', axis = 1).to_parquet(commit_outdir / 'linked_pull_request_to_issue.parquet', index = False)


if __name__ == '__main__':   
    Main()