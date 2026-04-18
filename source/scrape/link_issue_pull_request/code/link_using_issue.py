# source/scrape/link_issue_pull_request/code/link_using_issue.py
import pandas as pd
import glob
from tqdm import tqdm
from bs4 import BeautifulSoup, SoupStrainer
import requests
import time
from pathlib import Path
import warnings
import re
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from wakepy import keep
import os
import concurrent.futures
import argparse

from source.scrape.link_issue_pull_request.code.create_issue_timeline_json import HtmlTextToTimelineJSON
from source.scrape.link_issue_pull_request.code.create_issue_timeline_dom import HtmlTextToTimelineDOM
from source.scrape.link_issue_pull_request.code.fetch_helpers import FetchGitHubPage
from source.lib.helpers import MakeRepoNameSafe, JsonSerialize
from source.lib.JMSLab.SaveData import SaveData


CHUNK_SIZE = 200


def ReadIssueParquet(path):
    cols = ["repo_name", "issue_number"]
    try:
        df = pd.read_parquet(path, engine="pyarrow", columns=cols)
        return df.drop_duplicates().dropna(subset=cols)
    except Exception as e:
        warnings.warn(f"Failed to read {path!r}: {e}")
        return pd.DataFrame(columns=cols)


def _FetchIssuePage(sesh, repo_name, issue_number, proxy_num: int):
    url = f"https://github.com/{repo_name}/issues/{issue_number}"
    resp, is_not_found, is_rate_limited = FetchGitHubPage(sesh, url, proxy_num)
    is_pull = "/pull/" in resp.url
    return resp, is_not_found, is_rate_limited, is_pull


def _ExpandAllWithSelenium(issue_url):
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")

    html = ""
    for _ in range(5):
        driver = webdriver.Chrome(options=chrome_options)
        try:
            driver.get(issue_url)
            wait = WebDriverWait(driver, 10)
            finished_naturally = False

            try:
                while True:
                    btn = wait.until(
                        EC.element_to_be_clickable(
                            (By.CSS_SELECTOR, 'button[data-testid^="issue-timeline-load-more-load-top"]')
                        )
                    )
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(3)
                    driver.execute_script("arguments[0].scrollIntoView({block:\'center\'});", btn)
                    time.sleep(3)
                    btn.click()
                    time.sleep(10)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(5)

            except TimeoutException:
                finished_naturally = True
            except StaleElementReferenceException:
                pass

            time.sleep(3)
            driver.execute_script("return document.documentElement.outerHTML;")
            time.sleep(10)
            html = driver.page_source

            if finished_naturally:
                return html
        finally:
            driver.quit()

    return html


def GrabIssueData(repo_name, issue_number, proxy_num: int):
    try:
        product = SoupStrainer("a")
        sesh = requests.Session()

        issue_url = f"https://github.com/{repo_name}/issues/{issue_number}"

        resp, is_not_found, is_rate_limited, is_pull = _FetchIssuePage(sesh, repo_name, issue_number, proxy_num)
        if is_not_found:
            return {"pr_links": [], "github_links": [], "timeline_df": None}
        if is_rate_limited:
            print("RATE LIMITED")
            time.sleep(120)
            resp, is_not_found, is_rate_limited, is_pull = _FetchIssuePage(sesh, repo_name, issue_number, proxy_num)
        if is_not_found:
            return {"pr_links": [], "github_links": [], "timeline_df": None}

        html_text = resp.text if hasattr(resp, "text") else ""
        bs = BeautifulSoup(html_text, "html.parser")
        bs_text = bs.find(attrs={"data-testid": "issue-title"})
        issue_title = bs_text.text if bs_text is not None and hasattr(bs_text, "text") else ""

        timeline_cols = ["type", "author", "date", "text", "url"]
        timeline_df = pd.DataFrame()
        if not is_pull:
            maybe_timeline = HtmlTextToTimelineJSON(html_text)
            if isinstance(maybe_timeline, pd.DataFrame) and not maybe_timeline.empty:
                timeline_df = maybe_timeline.copy()
                if re.search(r"\"hasNextPage\":true", html_text):
                    expanded_html = _ExpandAllWithSelenium(issue_url)
                    dom_timeline = HtmlTextToTimelineDOM(expanded_html)
                    opening_issue_row = None
                    if isinstance(timeline_df, pd.DataFrame) and not timeline_df.empty:
                        opening_issue_row = timeline_df.loc[0]
                    if opening_issue_row is not None:
                        opening_issue_df = pd.DataFrame(
                            [["IssueOpened", opening_issue_row.get("author"), opening_issue_row.get("date"), opening_issue_row.get("text"), None]],
                            columns=timeline_cols,
                        )
                        appended = pd.DataFrame()
                        if isinstance(dom_timeline, pd.DataFrame):
                            appended = dom_timeline
                        timeline_df = pd.concat([opening_issue_df, appended], ignore_index=True).reset_index(drop=True)
            else:
                timeline_df = pd.DataFrame()

            if not timeline_df.empty:
                opening_issue_row = timeline_df.loc[0]
                author = opening_issue_row.get("author", None)
                date = opening_issue_row.get("date") if isinstance(opening_issue_row, dict) or hasattr(opening_issue_row, "get") else None
                issue_title_df = pd.DataFrame(
                    [["IssueTitle", author, date, issue_title, issue_url]],
                    columns=timeline_cols,
                )
                timeline_df = pd.concat([issue_title_df, timeline_df], ignore_index=True).reset_index(drop=True)
                timeline_df["repo_name"] = repo_name
                timeline_df["issue_number"] = issue_number
                timeline_df["event_order"] = range(1, len(timeline_df) + 1)
        else:
            timeline_df = pd.DataFrame()

        soup = BeautifulSoup(html_text, parse_only=product, features="html.parser")
        pr_links_raw = [
            a for a in soup.find_all("a", class_=True)
            if any("LinkedPullRequest" in cls for cls in a.get("class", []))
        ]
        pr_links = list({a.get("href") for a in pr_links_raw if a.get("href")})

        other_links = soup.find_all("a")
        links = list({a.get("href") for a in other_links if a.get("href")})

        url_pattern = re.compile(r"^https://github\.com/([^/]+)/([^/]+)/(issues|pull)/([^/]+)$")
        pattern = re.compile(r"^/(.*)/(issues|pull)/(\d+)$")
        github_links = [g for g in links if pattern.match(str(g)) or url_pattern.match(str(g))]

        return {
            "pr_links": pr_links,
            "github_links": github_links,
            "timeline_df": timeline_df,
            "is_pull": is_pull,
        }

    except Exception as e:
        print(f"GrabIssueData error: {e}")
        return f"Error: {e}"


def Main(proxy_num: int):
    warnings.filterwarnings("ignore")

    INDIR = Path("drive/output/scrape/extract_github_data/repo_level_data/issue")
    OUTDIR = Path("drive/output/scrape/link_issue_pull_request/linked_issue_to_pull_request")
    TIMELINE_DIR = Path("drive/output/scrape/link_issue_pull_request/timeline")

    OUTDIR.mkdir(parents=True, exist_ok=True)
    TIMELINE_DIR.mkdir(parents=True, exist_ok=True)

    CHUNK_OUTDIR = OUTDIR / "chunks"
    CHUNK_TIMELINE_DIR = TIMELINE_DIR / "chunks"
    CHUNK_OUTDIR.mkdir(parents=True, exist_ok=True)
    CHUNK_TIMELINE_DIR.mkdir(parents=True, exist_ok=True)

    LOG_ISSUE_DIR    = Path("output/scrape/link_issue_pull_request/linked_issue_to_pull_request/logs")
    LOG_TIMELINE_DIR = Path("output/scrape/link_issue_pull_request/timeline/logs")
    LOG_ISSUE_DIR.mkdir(parents=True, exist_ok=True)
    LOG_TIMELINE_DIR.mkdir(parents=True, exist_ok=True)

    parquet_files = glob.glob(str(INDIR / "*.parquet"))
    np.random.shuffle(parquet_files)

    def _IsAlreadyDone(parquet_file):
        safe_repo = Path(parquet_file).stem
        return (OUTDIR / f"{safe_repo}.parquet").exists() and \
               (TIMELINE_DIR / f"{safe_repo}_issue_timeline.parquet").exists()

    with concurrent.futures.ThreadPoolExecutor() as pool:
        done_flags = list(pool.map(_IsAlreadyDone, parquet_files))

    parquet_files = [f for f, done in zip(parquet_files, done_flags) if not done]
    print(f"{len(parquet_files)} repos remaining to process")

    for parquet_file in parquet_files:
        print(parquet_file)
        df_issue = ReadIssueParquet(parquet_file)
        if df_issue.empty:
            continue

        repo_name = df_issue["repo_name"].iloc[0]
        safe_repo = MakeRepoNameSafe(repo_name)

        assert df_issue["repo_name"].nunique() == 1

        repo_file = OUTDIR / f"{safe_repo}.parquet"
        timeline_file = TIMELINE_DIR / f"{safe_repo}_issue_timeline.parquet"

        if repo_file.exists() and timeline_file.exists():
            continue

        chunks = [df_issue.iloc[i : i + CHUNK_SIZE].copy() for i in range(0, len(df_issue), CHUNK_SIZE)]

        while True:
            unmade = [i for i in range(len(chunks)) if not ((CHUNK_OUTDIR / f"{safe_repo}_chunk_{i:04d}.parquet").exists()
                and (CHUNK_TIMELINE_DIR / f"{safe_repo}_chunk_{i:04d}_timeline.parquet").exists())]
            if not unmade:
                break

            chunk_idx = int(np.random.choice(unmade))
            chunk_df = chunks[chunk_idx]

            chunk_out = CHUNK_OUTDIR / f"{safe_repo}_chunk_{chunk_idx:04d}.parquet"
            chunk_tl  = CHUNK_TIMELINE_DIR / f"{safe_repo}_chunk_{chunk_idx:04d}_timeline.parquet"
            if chunk_out.exists() and chunk_tl.exists():
                continue

            print(f"  Chunk {chunk_idx + 1}/{len(chunks)} ({len(chunk_df)} issues)")

            rows = [(row["repo_name"], int(float(row["issue_number"]))) for _, row in chunk_df.iterrows()]
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
                results = list(
                    tqdm(
                        pool.map(lambda r: GrabIssueData(r[0], r[1], proxy_num), rows),
                        total=len(rows),
                        desc="Fetching issues",
                    )
                )
            chunk_df["linked_pull_request"] = results

            for retry in range(10):
                error_mask = chunk_df["linked_pull_request"].apply(lambda x: isinstance(x, str))
                if not error_mask.any():
                    break
                error_indices = chunk_df[error_mask].index.tolist()
                retry_rows = [
                    (chunk_df.loc[idx, "repo_name"], int(float(chunk_df.loc[idx, "issue_number"])))
                    for idx in error_indices
                ]
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
                    retry_results = list(
                        tqdm(
                            pool.map(lambda r: GrabIssueData(r[0], r[1], proxy_num), retry_rows),
                            total=len(retry_rows),
                            desc=f"Retry {retry + 1}",
                        )
                    )
                for idx, result in zip(error_indices, retry_results):
                    chunk_df.at[idx, "linked_pull_request"] = result
                time.sleep(5)

            timeline_dfs = [
                d["timeline_df"]
                for d in chunk_df["linked_pull_request"]
                if isinstance(d, dict) and isinstance(d.get("timeline_df"), pd.DataFrame)
            ]

            if timeline_dfs:
                pd.concat(timeline_dfs, ignore_index=True).to_parquet(chunk_tl, engine="pyarrow", index=False)
            else:
                pd.DataFrame().to_parquet(chunk_tl, engine="pyarrow", index=False)

            chunk_df["linked_pull_request"] = chunk_df["linked_pull_request"].apply(
                lambda d: {
                    "pr_links": d.get("pr_links", []),
                    "github_links": d.get("github_links", []),
                    "is_pull": d.get("is_pull", False),
                }
                if isinstance(d, dict)
                else d
            )

            chunk_df.to_parquet(chunk_out, engine="pyarrow")

        chunk_dfs = [pd.read_parquet(CHUNK_OUTDIR / f"{safe_repo}_chunk_{i:04d}.parquet") for i in range(len(chunks))]
        repo_df = pd.concat(chunk_dfs, ignore_index=True)
        repo_df["linked_pull_request"] = repo_df["linked_pull_request"].apply(JsonSerialize)
        SaveData(repo_df, ["repo_name", "issue_number"], repo_file,
                 LOG_ISSUE_DIR / f"{safe_repo}.log", append=False)

        tl_dfs = [
            pd.read_parquet(CHUNK_TIMELINE_DIR / f"{safe_repo}_chunk_{i:04d}_timeline.parquet")
            for i in range(len(chunks))
        ]
        non_empty_tls = [t for t in tl_dfs if not t.empty]
        combined_tl = pd.concat(non_empty_tls, ignore_index=True) if non_empty_tls else pd.DataFrame()
        if not combined_tl.empty:
            SaveData(combined_tl, ["repo_name", "issue_number", "event_order"], timeline_file,
                     LOG_TIMELINE_DIR / f"{safe_repo}.log", append=False)
        else:
            combined_tl.to_parquet(timeline_file, engine="pyarrow", index=False)

        for i in range(len(chunks)):
            (CHUNK_OUTDIR / f"{safe_repo}_chunk_{i:04d}.parquet").unlink()
            (CHUNK_TIMELINE_DIR / f"{safe_repo}_chunk_{i:04d}_timeline.parquet").unlink()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy-num", type=int, required=True)
    args = parser.parse_args()
    Main(args.proxy_num)
