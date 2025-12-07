import pandas as pd
import glob
from pandarallel import pandarallel
from bs4 import BeautifulSoup, SoupStrainer
import requests
import time
from pathlib import Path
import warnings
import re
from source.lib.JMSLab.SaveData import SaveData
from source.lib.helpers import GetLatestRepoName
import numpy as np
import os

def ReadIssueParquet(path):
    cols = ['repo_name', 'issue_number']
    try:
        df = pd.read_parquet(path, engine="pyarrow", columns=cols)
        return df.drop_duplicates().dropna(subset=cols)
    except Exception as e:
        warnings.warn(f"Failed to read {path!r}: {e}")
        return pd.DataFrame(columns=cols)

def _FetchIssuePage(session, repo, issue_number):
    url = f"https://github.com/{repo}/issues/{issue_number}"
    resp = session.get(url, allow_redirects=True)
    text = resp.text if hasattr(resp, "text") else ""
    is_not_found = (resp.status_code == 404) or ("This is not the webpage you are looking for" in text)
    is_rate_limited = ("Please wait a few minutes before you try again" in text) or ("You Are Not Connected" in text)
    return url, resp, text, is_not_found, is_rate_limited

def GrabIssueData(repo_name, issue_number, repo_name_latest=None):
    try:
        product = SoupStrainer('a')
        sesh = requests.Session()

        url, resp, page_text, is_not_found, is_rate_limited = _FetchIssuePage(sesh, repo_name, issue_number)
        if is_rate_limited:
            time.sleep(120)
            url, resp, page_text, is_not_found, is_rate_limited = _FetchIssuePage(sesh, repo_name, issue_number)

        if is_not_found and repo_name_latest and repo_name_latest != repo_name:
            url, resp, page_text, is_not_found, is_rate_limited = _FetchIssuePage(sesh, repo_name_latest, issue_number)
            if is_rate_limited:
                time.sleep(120)
                url, resp, page_text, is_not_found, is_rate_limited = _FetchIssuePage(sesh, repo_name_latest, issue_number)

        soup = BeautifulSoup(resp.content, parse_only=product, features="html.parser")

        pr_links_raw = [
            a for a in soup.find_all("a", class_=True)
            if any("LinkedPullRequest" in cls for cls in a.get("class", []))
        ]
        pr_links = list(set([a.get("href") for a in pr_links_raw if a.get("href")]))

        other_links = soup.find_all("a")
        links = list(set([a.get('href') for a in other_links if a.get('href')]))

        url_pattern = re.compile(r'^https://github\.com/([^/]+)/([^/]+)/(issues|pull)/([^/]+)$')
        pattern = re.compile(r'^/(.*)/(issues|pull)/(\d+)$')
        github_links = [g for g in links if pattern.match(g) or url_pattern.match(g)]

        return {'pr_links': pr_links, 'github_links': github_links}
    except Exception as e:
        return str(e)

def Main():
    pandarallel.initialize(progress_bar=True)
    warnings.filterwarnings("ignore")

    indir_repo_match = Path("output/scrape/extract_github_data")
    issue_dir = Path('drive/output/derived/problem_level_data/issue')
    pr_dir = Path('drive/output/derived/problem_level_data/pr')
    linked_outdir = Path('drive/output/scrape/link_issue_pull_request/linked_issue_to_pull_request')
    linked_outdir.mkdir(parents=True, exist_ok=True)

    repo_df = pd.read_csv(indir_repo_match / "repo_id_history_filtered.csv")

    parquet_files = glob.glob(str(issue_dir / '*.parquet'))
    np.random.shuffle(parquet_files)
    len(parquet_files)

    for parquet_file in parquet_files:
        df_library = ReadIssueParquet(parquet_file)
        if df_library.empty:
            continue

        repo_name = df_library['repo_name'].dropna().unique().tolist()[0]
        safe_repo = repo_name.replace('/', '_')

        repo_file = linked_outdir / f"{safe_repo}.parquet"
        if repo_file.exists():
            continue
        
        print(parquet_file)
        # scrape links
        df_library['linked_pull_request'] = df_library.parallel_apply(
            lambda x: GrabIssueData(
                x['repo_name'],
                int(float(x['issue_number']))
            ),
            axis=1
        )
        df_library['linked_pull_request'] = df_library.parallel_apply(
            lambda x: GrabIssueData(
                x['repo_name'],
                int(float(x['issue_number']))
            ) if isinstance(x['linked_pull_request'], str) else x['linked_pull_request'],
            axis=1
        )

        df_library.to_parquet(repo_file)
        print(parquet_file + " done")

if __name__ == '__main__':
    Main()
