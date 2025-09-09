import pandas as pd
import requests
import time
from pathlib import Path
from source.lib.JMSLab.SaveData import SaveData


def ResolveLatestRepoName(repo_name):
    url = f"https://github.com/{repo_name}"
    try:
        r = requests.get(url, allow_redirects=True, timeout=10)
        if r.status_code == 200:
            latest = r.url.replace("https://github.com/", "").rstrip("/")
        else:
            latest = "ERROR"
    except Exception:
        latest = "ERROR"

    return latest


def AssignLatestNamesAndGroups(df):
    start = time.time()
    cache = {repo_name: ResolveLatestRepoName(repo_name) for repo_name in df['repo_name'].unique().tolist()}
    end = time.time()
    print(end-start)
    df["latest_repo_name"] = df["repo_name"].map(cache)

    # Assign new repo_group based only on latest_repo_name
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
        indir / "repo_id_history_latest.csv",
        indir / "repo_id_history_latest.log",
    )

if __name__ == "__main__":
    Main()
