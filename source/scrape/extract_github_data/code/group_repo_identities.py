import pandas as pd
import requests
import re
from pathlib import Path
from source.lib.JMSLab.SaveData import SaveData
import numpy as np

FORK_REGEX = re.compile(r'forked from\s+<[^>]*>([^<]+)</a>', re.IGNORECASE)

def DetectForkSource(html_text):
    match = FORK_REGEX.search(html_text)
    if match:
        return match.group(1).strip(), 1
    return np.nan, 0


def ResolveRepoDetails(repo_name):
    url = f"https://github.com/{repo_name}"

    try:
        r = requests.get(url, allow_redirects=True, timeout=10)
        if r.status_code == 200:
            latest = r.url.replace("https://github.com/", "").rstrip("/")
            forked_from, is_fork = DetectForkSource(r.text)
        else:
            latest, forked_from, is_fork = "ERROR", np.nan, 0
    except Exception:
        latest, forked_from, is_fork = "ERROR", np.nan, 0

    return latest, forked_from, is_fork


def AssignLatestNamesAndGroups(df):
    repos = df["repo_name"].unique().tolist()

    cache_latest = {}
    cache_fork = {}
    cache_flag = {}

    for repo_name in repos:
        latest, forked_from, is_fork = ResolveRepoDetails(repo_name)
        cache_latest[repo_name] = latest if latest == "ERROR" else latest.lower()
        cache_fork[repo_name] = forked_from
        cache_flag[repo_name] = is_fork

    df["latest_repo_name"] = df["repo_name"].map(cache_latest)
    df["forked_from"] = df["repo_name"].map(cache_fork)
    df["is_fork"] = df["repo_name"].map(cache_flag)

    unique_latest = {name: i for i, name in enumerate(df["latest_repo_name"].unique())}
    df["repo_group"] = df["latest_repo_name"].map(unique_latest)
    return df


def Main():
    indir = Path("output/scrape/extract_github_data")
    df_full = pd.read_csv(indir / "repo_id_history.csv")

    df_full = AssignLatestNamesAndGroups(df_full)
    
    SaveData(
        df_full,
        ["repo_group", "repo_id", "repo_name", "latest_repo_name"],
        indir / "repo_id_history_final.csv",
        indir / "repo_id_history_final.log",
    )


if __name__ == "__main__":
    Main()
