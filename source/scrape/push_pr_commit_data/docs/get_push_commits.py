import os
import time
import ast
import logging
from pathlib import Path

import pandas as pd
import numpy as np
import requests
from ratelimit import limits, sleep_and_retry
from source.lib.helpers import GetLatestRepoName

ONE_HOUR = 3600
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class GitHubCommitFetcher:
    def __init__(self, repo_df, username, token, backup_username, backup_token):
        self.repo_df = repo_df
        self.username = username
        self.token = token
        self.backup_username = backup_username
        self.backup_token = backup_token
        self.failed_commits = []

    @sleep_and_retry
    @limits(calls=5000, period=ONE_HOUR)
    def GetCommits(self, repo_name, before, head, original_urls, push_size, push_id, second_try=0):
        latest_repo = GetLatestRepoName(repo_name, self.repo_df) or repo_name
        api_url = f"https://api.github.com/repos/{latest_repo}/compare/{before}...{head}"
        logging.info(f"Fetching commits from: {api_url}")
        try:
            response = requests.get(api_url, auth=(self.username, self.token), timeout=10)
            data = response.json()
        except Exception as e:
            logging.error(f"Error parsing API response from {api_url}: {e}")
            return original_urls
            
        if "commits" in data:
            commit_urls = [commit["url"] for commit in data["commits"]]
            if not commit_urls:
                return commit_urls
            new_head = commit_urls[0].split("/")[-1]
            if push_size > len(commit_urls) and new_head != before:
                return self.GetCommits(repo_name, before, new_head, original_urls, push_size - 250, push_id, second_try) + commit_urls
            return commit_urls
        if data.get("message", "").startswith("API rate limit exceeded"):
            if second_try == 1:
                return self.SwapCredentialsAndRetry(latest_repo, before, head, original_urls, push_size, push_id, second_try)
            if second_try == 2:
                time.sleep(120)
                return self.SwapCredentialsAndRetry(latest_repo, before, head, original_urls, push_size, push_id, second_try)
        if (data.get("message") == "Not Found" or data.get("message", "").startswith("No common ancestor")) and second_try < 1:
            if original_urls:
                new_before = original_urls[second_try].split("/")[-1]
                return ([f"https://api.github.com/repos/{latest_repo}/commits/{new_before}"] +
                        self.GetCommits(latest_repo, new_before, head, original_urls, push_size - 1, push_id, second_try=second_try + 1))
            return original_urls
        logging.error(f"Failed with URL: {api_url}. Message: {data.get('message')}")
        self.failed_commits.append({
            "repo_name": repo_name,
            "push_id": push_id,
            "failure_status": data.get("message")
        })
        return original_urls

    def SwapCredentialsAndRetry(self, repo_name, before, head, original_urls, push_size, push_id, second_try):
        return self.GetCommits(repo_name, before, head, original_urls, push_size, push_id, second_try=second_try + 1)


def ParseCommitUrls(url_str):
    if pd.isnull(url_str):
        return []
    safe_str = url_str.replace("'\n '", "', '")
    try:
        urls = ast.literal_eval(safe_str)
        return list(dict.fromkeys(urls)) if isinstance(urls, list) else []
    except Exception as e:
        logging.error(f"Failed to parse commit_urls: {url_str} with error: {e}")
        return []


def ProcessPushData(df_push, fetcher):
    df_push["old_commit_urls"] = df_push["old_commit_urls"].apply(ParseCommitUrls)
    df_push["commit_urls"] = df_push["commit_urls"].apply(ParseCommitUrls)
    df_push["commit_urls"] = df_push.apply(
        lambda row: row["commit_urls"][1:] if row["commit_urls"] and row["commit_urls"][0].split("/")[-1] == row["push_before"] else row["commit_urls"],
        axis=1
    )
    df_push["commit_urls"] = df_push.apply(lambda x: x["commit_urls"] if len(x["commit_urls"])>len(x["old_commit_urls"]) else x["old_commit_urls"], axis = 1)
    df_push.drop('old_commit_urls', axis = 1, inplace = True)

    try_index = df_push[(df_push["commit_urls"].apply(len) != df_push["push_size"])].index
    for idx in try_index:
        row = df_push.loc[idx]
        updated_urls = fetcher.GetCommits(row["repo_name"], row["push_before"], row["push_head"], row["commit_urls"], row["push_size"],
                                          row["push_id"])
        df_push.at[idx, "commit_urls"] = list(dict.fromkeys(updated_urls))
    return df_push

def Main():
    indir_push = Path("drive/output/scrape/extract_github_data/push_data/")
    indir_repo_match = Path("output/scrape/extract_github_data")
    outdir_push = Path("drive/output/scrape/push_pr_commit_data/push_data/")
    repo_df = pd.read_csv(indir_repo_match / "repo_id_history_filtered.csv")
    username = os.environ["PRIMARY_GITHUB_USERNAME"]
    token = os.environ["PRIMARY_GITHUB_TOKEN"]
    backup_username = os.environ["BACKUP_GITHUB_USERNAME"]
    backup_token = os.environ["BACKUP_GITHUB_TOKEN"]
    fetcher = GitHubCommitFetcher(repo_df, username, token, backup_username, backup_token)
    for year in range(2020, 2025):
        for month in range(1, 13):
            if month >= 7 or year >= 2021:
                df_push = pd.read_csv(indir_push /  f"push_{year}_{month}.csv", index_col=0)
                req_cols = ["push_id", "push_size", "repo_name", "push_before", "push_head", "commit_urls"]
                df_push = df_push[req_cols].drop_duplicates()
                output_file = outdir_push / f"push_data_{year}_{month}.csv"
                if output_file.exists():
                    df_existing = pd.read_csv(output_file, index_col=0)
                    on_cols = ["push_id", "push_size"] if "push_size" in df_existing.columns else ["push_id"]
                    df_push = pd.merge(df_push.rename(columns={"commit_urls":"old_commit_urls"}), df_existing, how="left", on=on_cols)
                df_push = df_push.reset_index(drop=True)
                df_push = ProcessPushData(df_push, fetcher)
                if fetcher.failed_commits:
                    df_failures = pd.DataFrame(fetcher.failed_commits)
                    df_push = pd.merge(df_push, df_failures, how="left", on=["repo_name", "push_id"])
                    fetcher.failed_commits = []
                    df_push['commit_urls_length'] = df_push['commit_urls'].apply(lambda x: len(x))
                    df_push['failure_status'] = df_push.apply(lambda x: x['failure_status'] if not pd.isnull(x['failure_status']) else 
                                                            'less commits than push size' if x['commit_urls_length'] < x['push_size'] else np.nan, axis=1)
                logging.info(f"Commits for push_data_{year}_{month}.csv obtained")
                df_push[["push_id", "commit_urls", "push_size", "commit_urls_length", "failure_status"]].to_csv(output_file)


if __name__ == "__main__":
    Main()
