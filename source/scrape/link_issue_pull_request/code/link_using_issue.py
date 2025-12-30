import pandas as pd
import glob
from pandarallel import pandarallel
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

from source.scrape.link_issue_pull_request.code.create_issue_timeline_json import HtmlTextToTimelineJSON
from source.scrape.link_issue_pull_request.code.create_issue_timeline_dom import HtmlTextToTimelineDOM


def ReadIssueParquet(path):
    cols = ["repo_name", "issue_number"]
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
    return resp, is_not_found, is_rate_limited


def _ExpandAllWithSelenium(issue_url):
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")

    for _ in range(5):
        driver = webdriver.Chrome(options=chrome_options)
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
        driver.quit()

        if finished_naturally:
            print("FINISHED")
            return html

    return html


def GrabIssueData(repo_name, issue_number):
    try:
        product = SoupStrainer("a")
        sesh = requests.Session()

        issue_url = f"https://github.com/{repo_name}/issues/{issue_number}"

        resp, is_not_found, is_rate_limited = _FetchIssuePage(sesh, repo_name, issue_number)
        if is_not_found:
            return {"pr_links": [], "github_links": [], "timeline_df": None}
        if is_rate_limited:
            time.sleep(120)
            resp, is_not_found, is_rate_limited = _FetchIssuePage(sesh, repo_name, issue_number)
    
        html_text = resp.text if hasattr(resp, "text") else ""

        if re.search(r"LoadMore-module__buttonChildrenWrapper", html_text):
            html_text = _ExpandAllWithSelenium(issue_url)
            timeline_df = HtmlTextToTimelineJSON(html_text)
        else:
            timeline_df = HtmlTextToTimelineDOM(html_text)
            
        if not timeline_df.empty:
            timeline_df["repo_name"] = repo_name
            timeline_df["issue_number"] = issue_number

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
        github_links = [g for g in links if pattern.match(g) or url_pattern.match(g)]

        return {
            "pr_links": pr_links,
            "github_links": github_links,
            "timeline_df": timeline_df
        }

    except Exception as e:
        return str(e)


def Main():
    pandarallel.initialize(progress_bar=True)
    warnings.filterwarnings("ignore")

    INDIR = Path("drive/output/scrape/extract_github_data/repo_level_data/issue")
    OUTDIR = Path("drive/output/scrape/link_issue_pull_request/linked_issue_to_pull_request")
    TIMELINE_DIR = Path("drive/output/scrape/link_issue_pull_request/timeline")

    OUTDIR.mkdir(parents=True, exist_ok=True)
    TIMELINE_DIR.mkdir(parents=True, exist_ok=True)

    parquet_files = glob.glob(str(INDIR / "*.parquet"))
    np.random.shuffle(parquet_files)

    for parquet_file in parquet_files:
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

        df_issue["linked_pull_request"] = df_issue.parallel_apply(
            lambda x: GrabIssueData(x["repo_name"], int(float(x["issue_number"]))),
            axis=1
        )

        df_issue["linked_pull_request"] = df_issue.parallel_apply(
            lambda x: GrabIssueData(x["repo_name"], int(float(x["issue_number"])))
            if isinstance(x["linked_pull_request"], str)
            else x["linked_pull_request"],
            axis=1
        )

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
                "github_links": d.get("github_links", [])
            } if isinstance(d, dict) else d
        )

        df_issue.to_parquet(repo_file, engine="pyarrow")

        print(parquet_file + " done")


if __name__ == "__main__":
    Main()

