import os
import pandas as pd
import numpy as np
import glob
from pandarallel import pandarallel
from bs4 import BeautifulSoup, SoupStrainer
import requests
import time
from pathlib import Path
import re
import warnings

from source.scrape.link_issue_pull_request.code.fetch_helpers import FetchGitHubPage
from source.lib.helpers import MakeRepoNameSafe

PROXY_NUM = 1


def ReadPRParquet(path):
    cols = ['repo_name', 'pr_number']
    try:
        df = pd.read_parquet(path, engine="pyarrow", columns=cols)
        return df.drop_duplicates().dropna(subset=cols)
    except Exception as e:
        warnings.warn(f"Failed to read {path!r}: {e}")
        return pd.DataFrame(columns=cols)

def _FetchPullPage(session, repo, pr_number):
    url = f"https://github.com/{repo}/pull/{pr_number}"
    return FetchGitHubPage(session, url, PROXY_NUM)

def _ExtractIssueLinks(content):
    soup = BeautifulSoup(content, features="html.parser")
    p = soup.find("p", string=lambda t: t and "Successfully merging this pull request may close these issues." in t)
    span = p.find_next_sibling("span") if p else None
    return [a["href"] for a in span.find_all("a", {"data-hovercard-type": "issue"})] if span else []

def _ExtractGitHubLinks(content):
    product_links = SoupStrainer('a')
    soup = BeautifulSoup(content, parse_only=product_links, features="html.parser")
    links = [a.get('href') for a in soup.find_all("a") if a.get('href')]
    links = list(set(links))
    url_pattern = re.compile(r'^https://github\.com/([^/]+)/([^/]+)/(issues|pull)/([^/]+)$')
    pattern = re.compile(r'^/(.*)/(issues|pull)/(\d+)$')
    return [g for g in links if pattern.match(g) or url_pattern.match(g)]

def _ExtractPRTitle(content):
    soup = BeautifulSoup(content, "html.parser")
    title_span = soup.find(class_="markdown-title")
    if title_span:
        return title_span.get_text(strip=True)
    header = soup.find("h1", class_=re.compile(r"prc-PageHeader-Title"))
    if header:
        return re.sub(r'#\d+$', '', header.get_text(strip=True)).strip()
    return "missing"

def _ExtractPRText(content):
    product_text = SoupStrainer('div')
    soup = BeautifulSoup(content, parse_only=product_text, features="html.parser")
    text_nodes = soup.find_all("div", attrs={"class": 'comment-body'})
    return text_nodes[0].text if len(text_nodes) > 0 else 'missing'

def GrabPullRequestData(repo_name, pr_number):
    try:
        sesh = requests.Session()
        resp, is_not_found, is_rate_limited = _FetchPullPage(sesh, repo_name, pr_number)
        if is_not_found:
            return [[], [], 'missing', 'missing']
        if is_rate_limited:
            print("RATE LIMITED")
            time.sleep(120)
            resp, is_not_found, is_rate_limited = _FetchPullPage(sesh, repo_name, pr_number)

        issue_links = _ExtractIssueLinks(resp.content)
        github_links = _ExtractGitHubLinks(resp.content)
        title = _ExtractPRTitle(resp.content)
        text = _ExtractPRText(resp.content)

        return [issue_links, github_links, title, text]
    except Exception as e:
        return str(e)

def Main():
    pandarallel.initialize(progress_bar=True, nb_workers=5)
    warnings.filterwarnings("ignore")

    INDIR = Path('drive/output/scrape/extract_github_data/repo_level_data/pr')
    OUTDIR = Path('drive/output/scrape/link_issue_pull_request/linked_pull_request_to_issue')
    OUTDIR.mkdir(parents=True, exist_ok=True)
    
    parquet_files = glob.glob(str(INDIR / '*.parquet'))
    np.random.shuffle(parquet_files)

    for parquet_file in parquet_files:
        df_pr = ReadPRParquet(parquet_file)
        if df_pr.empty:
            continue
        
        repo_name = df_pr['repo_name'].dropna().unique().tolist()[0]
        safe_repo = MakeRepoNameSafe(repo_name)

        assert(len(df_pr['repo_name'].dropna().unique().tolist()) == 1)

        repo_file = OUTDIR / f"{safe_repo}.parquet"
        if repo_file.exists():
            continue

        df_pr['linked_issue'] = df_pr.parallel_apply(
            lambda x: GrabPullRequestData(
                x['repo_name'],
                int(x['pr_number'])
            ),
            axis=1
        )
        for retry in range(10):
            df_pr['linked_issue'] = df_pr.parallel_apply(
                lambda x: GrabPullRequestData(
                    x['repo_name'],
                    int(x['pr_number'])
                ) if isinstance(x['linked_issue'], str) else x['linked_issue'],
                axis=1
            )

        df_pr['issue_link'] = df_pr['linked_issue'].apply(lambda x: x[0] if isinstance(x, list) else x)
        df_pr['other_links'] = df_pr['linked_issue'].apply(lambda x: x[1] if isinstance(x, list) else x)
        df_pr['pull_request_title'] = df_pr['linked_issue'].apply(lambda x: x[2] if isinstance(x, list) else x)
        df_pr['pull_request_text'] = df_pr['linked_issue'].apply(lambda x: x[3] if isinstance(x, list) else x)

        df_pr.drop(columns=['linked_issue']).to_parquet(repo_file)
        print(parquet_file + " done")

if __name__ == '__main__':
    Main()
