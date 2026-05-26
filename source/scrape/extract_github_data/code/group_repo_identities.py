import pandas as pd
import requests
import re
from pathlib import Path
from source.lib.JMSLab.SaveData import SaveData
import numpy as np

FORK_REGEX = re.compile(r'forked from\s+<[^>]*>([^<]+)</a>', re.IGNORECASE)
ARCHIVE_REGEX = re.compile(r'archived by the owner on\s+([A-Za-z]+ \d+, \d{4})', re.IGNORECASE)


def DetectForkSource(html_text):
    match = FORK_REGEX.search(html_text)
    if match:
        return match.group(1).strip(), 1
    return np.nan, 0


def DetectArchiveDate(html_text):
    match = ARCHIVE_REGEX.search(html_text)
    if match:
        return match.group(1).strip()
    return ""


def ResolveRepoDetails(repo_name):
    url = f"https://github.com/{repo_name}"

    try:
        r = requests.get(url, allow_redirects=True, timeout=10)
        if r.status_code == 200:
            latest = r.url.replace("https://github.com/", "").rstrip("/")
            forked_from, is_fork = DetectForkSource(r.text)
            archive_date = DetectArchiveDate(r.text)
        else:
            latest, forked_from, is_fork, archive_date = "ERROR", np.nan, 0, ""
    except Exception:
        latest, forked_from, is_fork, archive_date = "ERROR", np.nan, 0, ""

    return latest, forked_from, is_fork, archive_date


def AssignLatestNamesAndGroups(df):
    repos = df["repo_name"].unique().tolist()

    cache_latest = {}
    cache_fork = {}
    cache_flag = {}
    cache_archive = {}

    for repo_name in repos:
        latest, forked_from, is_fork, archive_date = ResolveRepoDetails(repo_name)
        cache_latest[repo_name] = latest if latest == "ERROR" else latest.lower()
        cache_fork[repo_name] = forked_from
        cache_flag[repo_name] = is_fork
        cache_archive[repo_name] = archive_date

    df["latest_repo_name"] = df["repo_name"].map(cache_latest)
    df["forked_from"] = df["repo_name"].map(cache_fork)
    df["is_fork"] = df["repo_name"].map(cache_flag)
    df["archive_date"] = pd.to_datetime(df["repo_name"].map(cache_archive))

    unique_latest = {name: i for i, name in enumerate(df["latest_repo_name"].unique())}
    df["repo_group"] = df["latest_repo_name"].map(unique_latest)
    return df

def CollapseOverlappingPeriods(df, keys):
    return (
        df
        .groupby(keys, dropna=False)
        .agg(
            first_seen=("first_seen", "min"),
            last_seen=("last_seen", "max"),
            forked_from=("forked_from", "first"),
            is_fork=("is_fork", "max"),
            archive_date=("archive_date", "max"),
        )
        .reset_index()
    )


def Main():
    INDIR = Path("output/scrape/extract_github_data")

    keys = ["repo_group", "repo_id", "repo_name", "latest_repo_name"]

    df_full = pd.read_csv(INDIR / "repo_id_history.csv")

    df_full = AssignLatestNamesAndGroups(df_full)
    df_full = CollapseOverlappingPeriods(df_full, keys)

    SaveData(
        df_full,
        keys,
        INDIR / "repo_id_history_final.csv",
        INDIR / "repo_id_history_final.log",
    )


if __name__ == "__main__":
    Main()
