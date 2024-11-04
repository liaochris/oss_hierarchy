import pandas as pd
import os
import glob
from pathlib import Path
import concurrent.futures

def CsvToParquet(file):
    print(f"now handling {file}")
    try:
        df = pd.read_csv(file, index_col = 0, engine = 'pyarrow')
    except:
        df = pd.read_csv(file, index_col = 0, on_bad_lines='warn', engine = 'python')
    df.to_parquet(file.replace('.csv','.parquet'))
    os.remove(file)
    print(f"{file} has been converted to parquet")

e = concurrent.futures.ThreadPoolExecutor(12)
pr_commits_csv = glob.glob('drive/output/scrape/collect_commits/pr/*.csv')
e.map(CsvToParquet, pr_commits_csv)


e = concurrent.futures.ThreadPoolExecutor(12)
push_commits_csv = glob.glob('drive/output/scrape/collect_commits/push/*.csv')
e.map(CsvToParquet, push_commits_csv)


e = concurrent.futures.ThreadPoolExecutor(12)
pypi_package_downloads_csv = glob.glob('drive/output/scrape/pypi_package_downloads/*/*.csv')
e.map(CsvToParquet, pypi_package_downloads_csv)
