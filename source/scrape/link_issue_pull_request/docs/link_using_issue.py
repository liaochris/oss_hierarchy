import pandas as pd
import glob
from pandarallel import pandarallel
from bs4 import BeautifulSoup, SoupStrainer
import requests
import time
from pathlib import Path
import warnings
import re
from source.scrape.link_issue_pull_request.docs.link_using_pull_request import ReadPullRequests
from source.lib.JMSLab.SaveData import SaveData
import numpy as np

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
        if "Please wait a few minutes before you try again" in page_text or "You Are Not Connected" in page_text:
            print('pausing, rate limit hit')
            time.sleep(120)
            page = sesh.get(scrape_url)
            page_text = str(page.text)
   
        soup = BeautifulSoup(page.content,parse_only = product,features="html.parser")
        required = {"Box-sc-g0xbh4-0", "eqUmOV", "prc-Link-Link-85e08"}
        pr_links_raw = [
            a for a in soup.find_all("a", class_=True)
            if required.issubset(set(a["class"]))
        ]        
        pr_links = list(set([link['href'] for link in pr_links_raw if 'href' in link]))

        other_links = soup.find_all("a")
        links = list(set([link['href'] for link in other_links if 'href' in link]))
        url_pattern = re.compile(r'^https://github\.com/([^/]+)/([^/]+)/(issues|pull)/([^/]+)$')
        pattern = re.compile(r'^/(.*)/(issues|pull)/(\d+)$')
        github_links = [github_link for github_link in links if pattern.match(github_link) or url_pattern.match(github_link)]
        return {'pr_links': pr_links, 'github_links': github_links}
    except Exception as e:
        error = str(e)
        return error

def Main():
    pandarallel.initialize(progress_bar=True)
    warnings.filterwarnings("ignore")

    issue_files = glob.glob('drive/output/scrape/extract_github_data/issue*/*')
    commit_outdir = Path('drive/output/scrape/link_issue_pull_request')
    commit_logdir = Path('output/scrape/link_issue_pull_request')

    df_issue = readIssue(issue_files)
    repo_list = df_issue['repo_name'].unique().tolist()
    repo_list = sorted(repo_list)

    #### CHANGE TO IMPORT PR
    pull_request_files = glob.glob('drive/output/scrape/extract_github_data/pull_request*/*')
    df_pull_request = ReadPullRequests(pull_request_files)
    pr_index = df_pull_request.drop_duplicates().set_index(['repo_name','pr_number']).index
    df_issue = df_issue.loc[~df_issue.set_index(['repo_name','issue_number']).index.isin(pr_index)]

    df_library_all = pd.DataFrame()    
    for repo in repo_list[5603:]:
        print(np.where([ele == repo for ele in repo_list]))
        df_library = df_issue[df_issue['repo_name'] == repo].dropna()
        if df_library.shape[0] == 0: continue 
        df_library['linked_pull_request'] = df_library.parallel_apply(lambda x: 
            GrabIssueData(x['repo_name'], int(x['issue_number'])), axis = 1)
        df_library['linked_pull_request'] = df_library.parallel_apply(lambda x: 
            GrabIssueData(x['repo_name'], int(x['issue_number'])) 
            if type(x['linked_pull_request']) == str else x['linked_pull_request'], axis = 1)

        df_library_all = pd.concat([df_library_all, df_library])
        df_library_all.to_parquet(commit_outdir / 'linked_issue_to_pull_request.parquet', index = False)

    SaveData(df_library_all, ['repo_name','issue_number'],
             commit_outdir / 'linked_issue_to_pull_request.parquet',
             commit_logdir / 'linked_issue_to_pull_request.log')

if __name__ == '__main__':   
    Main()