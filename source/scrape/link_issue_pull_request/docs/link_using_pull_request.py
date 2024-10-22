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

def readPullRequests(pull_request_files):
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
        soup = BeautifulSoup(page.content, parse_only = product, features="html.parser")
        issue_links = soup.find_all("a", attrs={"data-hovercard-type":'issue'})
        if len(issue_links)>0:
            issue_link = issue_links[0]['href']

        soup_title = BeautifulSoup(page.content, parse_only = product_title, features="html.parser")        
        title_links = soup_title.find_all("bdi", attrs={"class":'js-issue-title'})
        if len(title_links)>0:
            title = title_links[0].text

        soup_text = BeautifulSoup(page.content, parse_only = product_text, features="html.parser")        
        text_links = soup_text.find_all("div", attrs={"class":'comment-body'})
        if len(text_links)>0:
            text = text_links[0].text

        return [issue_link, title, text]
    except Exception as e:
        error = str(e)
        return error

def Main():
    pandarallel.initialize(progress_bar=True)
    warnings.filterwarnings("ignore")

    pull_request_files = glob.glob('drive/output/scrape/extract_github_data/pull_request*/*')
    commit_outdir = Path('drive/output/scrape/link_issue_pull_request/linked_pull_request')

    df_pull_request = readPullRequests(pull_request_files)
    repo_list = df_pull_request['repo_name'].unique().tolist()

    repo_list = sorted(repo_list)

    for repo in repo_list:
        df_library = df_pull_request[df_pull_request['repo_name'] == repo]
        lib_name = repo.replace("/","_")
        print(lib_name)
        fname = f"{lib_name}_linked_pull_request_to_issue.csv"
        if fname not in os.listdir(commit_outdir):
            df_library['linked_issue'] = df_library.parallel_apply(lambda x: 
                GrabPullRequestData(x['repo_name'], x['pr_number']), axis = 1)
            
            df_library['issue_link'] = df_library['linked_issue'].apply(lambda x: x[0] if type(x) == list else x)
            df_library['pull_request_title'] = df_library['linked_issue'].apply(lambda x: x[1] if type(x) == list else x)
            df_library['pull_request_text'] = df_library['linked_issue'].apply(lambda x: x[2] if type(x) == list else x)

            df_library.to_csv(commit_outdir / fname)

if __name__ == '__main__':   
    Main()