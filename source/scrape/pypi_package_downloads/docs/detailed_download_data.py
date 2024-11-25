#!/usr/bin/env python3
import os
from google.cloud import bigquery
from pathlib import Path
import pandas as pd
import numpy as np
from calendar import monthrange
import itertools
import warnings 
import concurrent.futures
import random

def GetProjectDailyDownloadData(client, pypi_project, outdir):
    pypi_downloads_sql = f"""
    SELECT 
        project, 
        DATE(timestamp) as date, 
        file.version as library_version, 
        CASE ARRAY_LENGTH(SPLIT(details.python, ".")) 
            WHEN 1 THEN SPLIT(details.python, ".")[0] 
            ELSE CONCAT(SPLIT(details.python, ".")[0], ".", SPLIT(details.python, ".")[1]) 
        END AS python_version, 
        COUNT(*) AS num_downloads
    FROM `bigquery-public-data.pypi.file_downloads`
    WHERE project = "{pypi_project}" AND DATE(timestamp)
        BETWEEN "2011-01-01" AND "2024-12-31"
    GROUP BY project, date, library_version, python_version
    ORDER BY project, date, library_version
    """
    pypi_downloads_query = client.query(pypi_downloads_sql)
    df_pypi_downloads_query = pypi_downloads_query.to_dataframe()
    df_pypi_downloads_query.to_parquet(outdir / f"{pypi_project}_downloads.parquet")

def GetProjectData(client, pypi_project, outdir):
    if f"{pypi_project}_downloads.parquet" not in os.listdir(outdir):
        print(f"starting {pypi_project}")
        GetProjectDailyDownloadData(client, pypi_project, outdir)
        print(f"{pypi_project} downloads finished!")


def Main():
    warnings.filterwarnings('ignore')
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ.keys():
        print("Need to set up GOOGLE_APPLICATION_CREDENTIALS environment variable")
        return
	
    project_id = 'crested-guru-440700-a6'
    client = bigquery.Client(project=project_id)
    outdir = Path('drive/output/scrape/pypi_package_downloads')
    indir_github_projects = Path('output/derived/collect_github_repos')
    
    df_python_projects = pd.read_csv(indir_github_projects / 'linked_pypi_github.csv', index_col = 0)
    python_projects = list(df_python_projects[df_python_projects['github repository'] != 'Unavailable']['package'].unique())

    random.shuffle(python_projects)

    for project in python_projects:
        GetProjectData(client, project, outdir)
	
	
if __name__ == '__main__':
    Main()


