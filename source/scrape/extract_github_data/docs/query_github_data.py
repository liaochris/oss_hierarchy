#!/usr/bin/env python3
import os
from google.cloud import bigquery
from pathlib import Path
import pandas as pd
import numpy as np

def CreateDataset(client, dataset_name):
    dataset_id = f"{client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, timeout=30)

    print(f"Created dataset {client.project}.{dataset.dataset_id}")

def LoadTableToDataset(client, dataset_name, table_name, df_github_projects, indir_github_projects):
    df_github_projects = pd.read_csv(Path(indir_github_projects) / 'linked_pypi_github.csv', index_col = False)
    df_github_projects = df_github_projects[['github repository']].rename({'github repository':'github_repository'}, axis = 1)
    
    github_project_ref = client.dataset(dataset_name).table(table_name)
    load_data_job = client.load_table_from_dataframe(df_github_projects, github_project_ref)
    load_results = load_data_job.result()

    if load_results.errors == None:
        print("Data loaded successfully")
    else:
        print(f"Error Log: {load_results.errors}")

def GetRawGitHubData(client, github_projects_name, project_name, dataset_name, github_data_name):
    github_data_config = bigquery.QueryJobConfig(destination=f"{project_name}.{dataset_name}.{github_data_name}")
    github_raw_sql = f"""
    SELECT *
    FROM `githubarchive.month.20*`
    WHERE (_TABLE_SUFFIX BETWEEN '1101' AND '2308') AND repo.name in 
    (SELECT github_repository FROM `{project_name}.{dataset_name}.{github_projects_name}`)
    """
    github_data_query = client.query(github_raw_sql, job_config=github_data_config) 
    github_data_query.result()

def GetWatchData(client, project_name, dataset_name, github_data_name):
    watch_data_name = 'watch_data'
    watch_data_config = bigquery.QueryJobConfig(destination=f"{project_name}.{dataset_name}.{watch_data_name}")
    watch_data_sql = f"""
    SELECT
      `type`,
      `created_at`,
      repo.id AS `repo_id`,
      repo.name AS `repo_name`,
      actor.id AS `actor_id`,
      actor.login AS `actor_login`,
      org.id AS `org_id`,
      org.login AS `org_login`,
    FROM
        `{project_name}.{dataset_name}.{github_data_name}`
    WHERE
        type = "WatchEvent"
    """
    watch_data_query = client.query(watch_data_sql, job_config=watch_data_config) 
    watch_data_query.result()

    GetSubsetData(client, project_name, dataset_name, 'watch_data')


def GetSubsetData(client, project_name, dataset_name, subset_data_name):
    outdir = f"drive/output/scrape/extract_github_data/{subset_data_name}"
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    for subset_year in np.arange(2011, 2024, 1):
        for subset_month in np.arange(1, 13, 1):
            if (subset_year != 2023) or (subset_year == 2023 and subset_month < 9):
                subset_date_sql = f"""
                SELECT *
                FROM
                    `{project_name}.{dataset_name}.{subset_data_name}`
                WHERE
                    EXTRACT(MONTH FROM created_at) = {subset_month} AND EXTRACT(YEAR FROM created_at) = {subset_year}
                """
                subset_date_query = client.query(subset_date_sql)
                df_subset = subset_date_query.to_dataframe()
                df_subset.to_csv(f"drive/output/scrape/extract_github_data/{subset_data_name}/{subset_data_name}_{subset_year}_{subset_month}.csv")
                print(f"{subset_data_name}/{subset_data_name}_{subset_year}_{subset_month}.csv extracted")


def GetMonthlyData()

def Main():
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ.keys():
        print("Need to set up GOOGLE_APPLICATION_CREDENTIALS environment variable")
        return

    indir_github_projects = 'output/derived/collect_github_repos'
    project_name = 'oss-hierarchy'
    dataset_name = 'source'
    github_projects_name = 'github_repositories'
    github_data_name = 'github_data'

    client = bigquery.Client(project=project_name)
    CreateDataset(client, dataset_name)
    LoadTableToDataset(client, 'source', github_projects_name, df_github_projects, indir_github_projects)
    GetRawGitHubData(client, github_projects_name, project_name, dataset_name, github_data_name)
    GetWatchData(client, project_name, dataset_name, github_data_name)




if __name__ == '__main__':
    Main()