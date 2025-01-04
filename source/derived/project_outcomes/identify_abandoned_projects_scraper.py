import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime
from pathlib import Path
import concurrent.futures

def ParseArchiveDate(text):
    # Example pattern: This repository has been archived by the owner on Jun 25, 2020. It is now read-only.
    # Goal: 06-25-2020
    match = re.search(r'on\s+([A-Za-z]{3}\s+\d{1,2},\s+\d{4})', text)
    if match:
        date_str = match.group(1) 
        try:
            dt = datetime.strptime(date_str, "%b %d, %Y")
            return dt.strftime("%m-%d-%Y")  # e.g. '02-25-2024'
        except ValueError:
            return None
    return None

def ScrapeRepoURL(repo_name):
    url = f'https://github.com/{repo_name}'
    print(f"Checking {repo_name} at url...")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error accessing {url}: {e}")
        return (repo_name, "website inaccessible", None, None)
    
    soup = BeautifulSoup(response.text, "html.parser")    
    warning_div = soup.find("div", class_="flash flash-warn flash-full border-top-0 text-center text-bold py-2")

    if warning_div:
        text = warning_div.get_text(strip=True)
        parsed_date = ParseArchiveDate(text)
        return (repo_name, "abandoned", parsed_date, text)
    return (repo_name, "not abandoned", None, None)

if __name__ == "__main__":
    indir = Path('drive/output/derived/contributor_stats/contributor_data')
    outdir = Path('drive/output/derived/project_outcomes/abandoned_projects')
    time_period = 6
    rolling_window = 732

    contribution_histories = pd.read_parquet(indir / f'major_contributors_major_months{time_period}_window{rolling_window}D_samplefull.parquet')
    repo_list = contribution_histories['repo_name'].unique().tolist()

    with concurrent.futures.ThreadPoolExecutor(max_workers = 8) as executor:
        results = executor.map(ScrapeRepoURL, repo_list)
    repo_data = list(results)
    df_repo = pd.DataFrame(repo_data, columns = ['repo_name','status', 'abandoned_date', 'text'])
    df_repo.to_csv(outdir / "scraped_abandoned_repo_data.csv")
