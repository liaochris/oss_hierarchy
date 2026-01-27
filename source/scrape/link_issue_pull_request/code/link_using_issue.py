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

from source.scrape.link_issue_pull_request.code.create_issue_timeline_json import HtmlTextToTimelineJSON
from source.scrape.link_issue_pull_request.code.create_issue_timeline_dom import HtmlTextToTimelineDOM
                                                                             
PROXY_NUM = 1
PROXY_FILE = Path("source/lib/proxies.txt")

def ReadIssueParquet(path):
    cols = ["repo_name", "issue_number"]
    try:
        df = pd.read_parquet(path, engine="pyarrow", columns=cols)
        return df.drop_duplicates().dropna(subset=cols)
    except Exception as e:
        warnings.warn(f"Failed to read {path!r}: {e}")
        return pd.DataFrame(columns=cols)


def ReadProxyFromFile(proxy_file, proxy_num):
    if not proxy_file.exists():
        return None
    try:
        with proxy_file.open("r", encoding="utf-8") as fh:
            lines = [ln.strip() for ln in fh.readlines() if ln.strip()]
    except Exception:
        return None
    index = proxy_num - 1
    if proxy_num == 0 or proxy_num - 1 < 0 or proxy_num - 1 >= len(lines):
        return None
    chosen = lines[index]
    parts = chosen.split(":")

    host = parts[0]
    port = parts[1]
    username = parts[2]
    password = ":".join(parts[3:])
    return f"http://{username}:{password}@{host}:{port}"


def _FetchIssuePage(sesh, repo_name, issue_number):
    url = f"https://github.com/{repo_name}/issues/{issue_number}"
    proxy_url = ReadProxyFromFile(PROXY_FILE, PROXY_NUM)
    if proxy_url:
        sesh.proxies = {"http": proxy_url, "https": proxy_url}
    else:
        sesh.proxies = {}
    resp = sesh.get(url, allow_redirects=True)
    if resp.status_code == 404:
        time.sleep(5)
        resp = sesh.get(url, allow_redirects=True)
    text = resp.text if hasattr(resp, "text") else ""
    is_not_found = (resp.status_code == 404) or ("This is not the webpage you are looking for" in text) or ("Not Found" in text)
    is_rate_limited = ("Please wait a few minutes before you try again" in text) or ("You Are Not Connected" in text)
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
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
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


def GrabIssueData(repo_name, issue_number):
    try:
        product = SoupStrainer("a")
        sesh = requests.Session()

        issue_url = f"https://github.com/{repo_name}/issues/{issue_number}"

        resp, is_not_found, is_rate_limited, is_pull = _FetchIssuePage(sesh, repo_name, issue_number)
        if is_not_found:
            return {"pr_links": [], "github_links": [], "timeline_df": None}
        if is_rate_limited:
            print("RATE LIMITED")
            time.sleep(120)
            resp, is_not_found, is_rate_limited, is_pull = _FetchIssuePage(sesh, repo_name, issue_number)
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
                if re.search(r'"hasNextPage":true', html_text):
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
                    columns=timeline_cols
                )
                timeline_df = pd.concat([issue_title_df, timeline_df], ignore_index=True).reset_index(drop=True)
                timeline_df["repo_name"] = repo_name
                timeline_df["issue_number"] = issue_number
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
            "is_pull": is_pull
        }

    except Exception as e:
        print(f"GrabIssueData error: {e}")
        return f"Error: {e}"


def Main():
    warnings.filterwarnings("ignore")

    os.chdir('/Users/chrisliao/Documents/research_temp')
    INDIR = Path("drive/output/scrape/extract_github_data/repo_level_data/issue")
    OUTDIR = Path("drive/output/scrape/link_issue_pull_request/linked_issue_to_pull_request")
    TIMELINE_DIR = Path("drive/output/scrape/link_issue_pull_request/timeline")

    OUTDIR.mkdir(parents=True, exist_ok=True)
    TIMELINE_DIR.mkdir(parents=True, exist_ok=True)

    parquet_files = glob.glob(str(INDIR / "*.parquet"))
    np.random.shuffle(parquet_files)
    with keep.running():
        for parquet_file in parquet_files:
            print(parquet_file)
            df_issue = ReadIssueParquet(parquet_file)
            if df_issue.empty:
                continue

            repo_name = df_issue["repo_name"].iloc[0]
            safe_repo = repo_name.replace("/", "___")

            assert df_issue["repo_name"].nunique() == 1
    
            repo_file = OUTDIR / f"{safe_repo}.parquet"
            timeline_file = TIMELINE_DIR / f"{safe_repo}_issue_timeline.parquet"

            if repo_file.exists() and timeline_file.exists():
                continue

            results = []
            for idx, row in tqdm(df_issue.iterrows(), total=len(df_issue), desc="Fetching issues"):
                results.append(GrabIssueData(row["repo_name"], int(float(row["issue_number"]))))
            df_issue["linked_pull_request"] = results

            for retry in range(10):
                error_mask = df_issue['linked_pull_request'].apply(lambda x: isinstance(x, str))
                if not error_mask.any():
                    break
                error_indices = df_issue[error_mask].index.tolist()
                for idx in tqdm(error_indices, desc=f"Retry {retry + 1}"):
                    row = df_issue.loc[idx]
                    df_issue.at[idx, "linked_pull_request"] = GrabIssueData(
                        row["repo_name"], int(float(row["issue_number"]))
                    )
                time.sleep(5)

            timeline_dfs = [
                d["timeline_df"]
                for d in df_issue["linked_pull_request"]
                if isinstance(d, dict) and isinstance(d.get("timeline_df"), pd.DataFrame)
            ]

            if timeline_dfs:
                repo_timeline_df = pd.concat(timeline_dfs, ignore_index=True)
                repo_timeline_df.to_parquet(timeline_file, engine="pyarrow", index=False)

            df_issue["linked_pull_request"] = df_issue["linked_pull_request"].apply(
                lambda d: {
                    "pr_links": d.get("pr_links", []),
                    "github_links": d.get("github_links", []),
                    "is_pull": d.get("is_pull", False)
                } if isinstance(d, dict) else d
            )

            df_issue.to_parquet(repo_file, engine="pyarrow")
            


if __name__ == "__main__":
    Main()
