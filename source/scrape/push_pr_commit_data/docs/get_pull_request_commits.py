from bs4 import BeautifulSoup
import requests
from urllib.request import Request, urlopen
import re
import time
import os
import random
from pathlib import Path
import numpy as np
import pandas as pd
import ast
from source.lib.helpers import GetLatestRepoName
from source.scrape.push_pr_commit_data.docs.get_push_commits import GitHubCommitFetcher

def FetchPageText(scrape_url, headers, retry_delay=120):
    try:
        req = Request(scrape_url, headers=headers)
        response = urlopen(req)
        page_text = response.read().decode("utf-8", errors="replace")
        if ("Please wait a few minutes" in page_text or "Looks like something went wrong!" in page_text
                or response.getcode() == 429):
            print("Rate limit hit on", scrape_url, "; sleeping for", retry_delay, "seconds.")
            time.sleep(retry_delay)
            return FetchPageText(scrape_url, headers, retry_delay)
        return page_text
    except Exception as e:
        print("Error fetching page text for", scrape_url, ":", e)
        return f"Error: {e}"

def ScrapeCommitsPage(scrape_url, headers, retry_delay=120):
    page_text = FetchPageText(scrape_url, headers, retry_delay)
    if page_text.startswith("Error"):
        return page_text  # This indicates a failure in fetching the page text.
    soup = BeautifulSoup(page_text, "html.parser")
    commits_pattern = re.compile(r".*/commits/[0-9a-f]+")
    commits = soup.find_all("a", href=commits_pattern)
    return list(dict.fromkeys([c["href"].split("/")[-1] for c in commits]))


def GetPRAPIData(api_pr_url, auth):
    try:
        response = requests.get(api_pr_url, auth=auth)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print("Error PR API data from", api_pr_url, ":", e)
        return f"PR API Error: {e}"

def RetrieveAdditionalCommits(latest_repo, pr_commits_url, commit_shas, primary_auth, backup_auth, repo_df, pr_data):
    base_sha = pr_data.get("base", {}).get("sha")
    head_sha = commit_shas[0]
    pr_total_commits = pr_data.get("commits")
    pr_size = pr_total_commits - 250 if pr_total_commits else 0
    if pr_size > 0:
        commit_fetcher = GitHubCommitFetcher(repo_df, primary_auth[0], primary_auth[1],
                                               backup_auth[0], backup_auth[1])
        additional_commits = commit_fetcher.GetCommits(
            latest_repo, base_sha, head_sha, [], pr_size, int(pr_commits_url.split("/")[-2])
        )[:-1]
        failure = ""
        if commit_fetcher.failed_commits:
            failure = "; ".join(fc["failure_status"] for fc in commit_fetcher.failed_commits)
            commit_fetcher.failed_commits = []
        additional_shas = [
            c.split("/")[-1] if isinstance(c, str) and "/" in c else c for c in additional_commits
        ]
        combined = additional_shas + commit_shas
        return combined, failure
    return commit_shas, np.nan


def GrabCommits(repo_name, pr_commits_url, primary_auth, backup_auth, repo_df):
    latest_repo = GetLatestRepoName(repo_name, repo_df) or repo_name
    pull_info = "/".join(pr_commits_url.strip("/").split("/")[-3:-1]).replace("pulls", "pull")
    scrape_url = f"https://github.com/{latest_repo}/{pull_info}/commits"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
        "Origin": "http://example.com",
        "Referer": "http://example.com/some_page"
    }
    commit_shas = ScrapeCommitsPage(scrape_url, headers)
    if type(commit_shas)    == str and commit_shas.startswith("Error"):
        return {"commit_list": [], "failure_status": commit_shas}
    if len(commit_shas) == 250:
        print("250 commits scraped – retrieving additional commits via API.")
        api_pr_url = pr_commits_url.replace("/commits", "")
        pr_data = GetPRAPIData(api_pr_url, primary_auth)
        if type(pr_data) == str and pr_data.startswith("PR API Error"):
            return {"commit_list": commit_shas, "failure_status": pr_data}
        commit_shas, fetcher_fail = RetrieveAdditionalCommits(
            latest_repo, pr_commits_url, commit_shas, primary_auth, backup_auth, repo_df, pr_data
        )
        return {"commit_list": commit_shas, "failure_status": fetcher_fail}
    return {"commit_list": commit_shas, "failure_status": ""}


def LoadPRData(pr_filename, indir):
    return pd.read_csv(indir / pr_filename)[["repo_name", "pr_commits_url"]].drop_duplicates()

def TryASTLiteralEval(value):
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return np.nan

def MergeExistingPRData(df_new, outdir, out_filename):
    existing_path = outdir / out_filename
    if existing_path.exists():
        df_existing = pd.read_csv(existing_path)
        df_existing = df_existing.loc[:, ~df_existing.columns.str.startswith("Unnamed")].drop_duplicates()
        df_merged = pd.merge(df_new, df_existing, how="left")
        if "commit_list" not in df_merged.columns:
            df_merged["commit_list"] = np.nan
        if "failure_status" not in df_merged.columns:
            df_merged["failure_status"] = np.nan
        valid = (~df_merged["commit_list"].isna()) & (
            ~df_merged["commit_list"].apply(lambda x: isinstance(x, str) and x.startswith("HTTP Error"))
        )
        df_merged.loc[valid, "commit_list"] = df_merged.loc[valid, "commit_list"].apply(TryASTLiteralEval)
        df_merged["commit_list"] = df_merged["commit_list"].apply(lambda x: np.nan if isinstance(x, list) and len(x) == 0 else x)
        return df_merged
    df_new["commit_list"] = np.nan
    df_new["failure_status"] = np.nan
    return df_new


def UpdatePRCommitList(df_pull, primary_auth, backup_auth, repo_df):
    for idx in df_pull.index:
        if idx % 1000 == 0:
            print(f"{idx} commits processed")
        current_list = df_pull.at[idx, "commit_list"]
        if not isinstance(current_list, list) or (isinstance(current_list, list) and len(current_list) == 250):
            result = GrabCommits(
                df_pull.at[idx, "repo_name"], df_pull.at[idx, "pr_commits_url"],
                primary_auth, backup_auth, repo_df
            )
            df_pull['commit_list'] = df_pull['commit_list'].astype('object')
            df_pull.at[idx, "commit_list"] = result["commit_list"]
            df_pull.at[idx, "failure_status"] = result["failure_status"]
    return df_pull


def Main():
    primary_auth = (os.environ["PRIMARY_GITHUB_USERNAME"], os.environ["PRIMARY_GITHUB_TOKEN"])
    backup_auth = (os.environ["BACKUP_GITHUB_USERNAME"], os.environ["BACKUP_GITHUB_TOKEN"])
    indir_pull = Path("drive/output/scrape/extract_github_data/pull_request_data/")
    indir_repo = Path("output/scrape/extract_github_data")
    outdir_pull = Path("drive/output/scrape/push_pr_commit_data/pull_request_data/")
    repo_df = pd.read_csv(indir_repo / "repo_id_history_filtered.csv")
    
    pr_filenames = [f"pull_request_{year}_{month}.csv" for year in range(2015, 2025)
                    for month in range(1, 13)]

    for pr_filename in pr_filenames:
        out_filename = pr_filename.replace("pull_request", "pull_request_data")
        df_pull = LoadPRData(pr_filename, indir_pull)
        df_pull = MergeExistingPRData(df_pull, outdir_pull, out_filename)
        df_pull.reset_index(drop=True, inplace=True)
        df_pull = UpdatePRCommitList(df_pull, primary_auth, backup_auth, repo_df)
        print("Commits for", pr_filename, "obtained")
        df_pull.to_csv(outdir_pull / out_filename, index=False)


if __name__ == "__main__":
    Main()
