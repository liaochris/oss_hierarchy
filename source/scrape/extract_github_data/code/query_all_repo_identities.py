#!/usr/bin/env python3

import os
import sys
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from source.lib.JMSLab.SaveData import SaveData
from source.lib.helpers import LoadGlobals

def MapIDToName(bq_client, repo_names):
    names_lower = [n.lower() for n in repo_names]
    query = """
    SELECT DISTINCT repo.id AS repo_id
    FROM `githubarchive.month.20*`
    WHERE (_TABLE_SUFFIX BETWEEN '1501' AND '2412')
      AND LOWER(repo.name) IN UNNEST(@names)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("names", "STRING", names_lower)]
    )
    results = bq_client.query(query, job_config=job_config).result()
    return {row.repo_id for row in results}


def GetNameForID(bq_client, repo_ids):
    query = """
    SELECT DISTINCT repo.name AS repo_name
    FROM `githubarchive.month.20*`
    WHERE (_TABLE_SUFFIX BETWEEN '1501' AND '2412')
      AND repo.id IN UNNEST(@ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", list(repo_ids))]
    )
    results = bq_client.query(query, job_config=job_config).result()
    return {row.repo_name for row in results}


def NormalizeInitialRepos(initial_repos):
    valid = [r for r in initial_repos if r != "Unavailable" and "/" in r]
    names_lower = set([r.lower() for r in valid])
    return valid, names_lower


def FetchIDsForNames(bq_client, names):
    query = """
    SELECT DISTINCT repo.id AS repo_id
    FROM `githubarchive.month.20*`
    WHERE (_TABLE_SUFFIX BETWEEN '1501' AND '2412')
      AND LOWER(repo.name) IN UNNEST(@names)
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("names", "STRING", list(names))])
    return {row.repo_id for row in bq_client.query(query, job_config=job_config).result()}


def FetchNamesForIDs(bq_client, ids):
    query = """
    SELECT DISTINCT repo.name AS repo_name
    FROM `githubarchive.month.20*`
    WHERE (_TABLE_SUFFIX BETWEEN '1501' AND '2412')
      AND repo.id IN UNNEST(@ids)
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", list(ids))])
    return {row.repo_name for row in bq_client.query(query, job_config=job_config).result()}


def FetchFinalMappingForIDs(bq_client, ids):
    final_query = """
        SELECT DISTINCT
            repo.id AS repo_id,
            repo.name AS repo_name,
            MIN(created_at) AS first_seen,
            MAX(created_at) AS last_seen
        FROM `githubarchive.month.20*`
        WHERE (_TABLE_SUFFIX BETWEEN '1501' AND '2412')
        AND repo.id IN UNNEST(@ids)
        GROUP BY repo.id, repo.name
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", list(ids))])
    df = bq_client.query(final_query, job_config=job_config).to_dataframe()
    df["repo_name"] = df["repo_name"].str.lower()
    return df


def LinkRepoNameID(bq_client, initial_repos, max_iterations=10):
    print(f"Starting with {len(initial_repos)} initial repositories")
    initial_repos = [r for r in initial_repos if r != "Unavailable" and "/" in r]
    known_repo_names = set([r.lower() for r in initial_repos])
    print(f"Continuing with {len(initial_repos)} correctly named repositories")
    known_repo_ids = set()
    for iteration in range(1, max_iterations + 1):
        print(f"\nIteration {iteration} — names: {len(known_repo_names)} ids: {len(known_repo_ids)}")
        new_ids = FetchIDsForNames(bq_client, known_repo_names) - known_repo_ids
        if new_ids:
            known_repo_ids.update(new_ids)
            print(f"Found {len(new_ids)} new repository IDs")
        elif iteration > 1:
            print("No new repository IDs found")
            break

        returned_names = FetchNamesForIDs(bq_client, known_repo_ids)
        candidate_names = {n.lower() for n in returned_names if "/" in n}
        candidate_names = {n for n in candidate_names if len(n.split("/")) == 2}
        new_names = candidate_names - known_repo_names
        if new_names:
            known_repo_names.update(new_names)
            print(f"Found {len(new_names)} new repository names")
        else:
            print("No new repository names found")
            break

    mapping_df = FetchFinalMappingForIDs(bq_client, known_repo_ids)
    print(f"\nFinal results: {len(known_repo_ids)} unique repository IDs, {mapping_df['repo_name'].nunique()} unique repository names, {len(mapping_df)} ID-name pairs")
    return mapping_df

def Main():
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("Error: GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
        sys.exit(1)

    INDIR = Path("output/scrape/pypi_site_info")
    OUTDIR = Path("output/scrape/extract_github_data")
    globals_data = LoadGlobals("source/lib/globals.json")

    initial_repo_df = pd.read_csv(INDIR / "linked_pypi_github.csv")
    initial_repos = initial_repo_df["github repository"].drop_duplicates().tolist()
    initial_repos = [repo for repo in initial_repos if repo != "Unavailable" and "/" in repo]

    bq_client = bigquery.Client(project=globals_data['project_id'])
    mapping_df = LinkRepoNameID(bq_client, initial_repos)
    mapping_df = mapping_df[
        mapping_df["repo_name"].apply(lambda x: len([ele for ele in x.split("/") if ele != ""]) == 2)
    ]

    SaveData(
        mapping_df,
        ["repo_id", "repo_name","first_seen", "last_seen"],
        OUTDIR / "repo_id_history.csv",
        OUTDIR / "repo_id_history.log",
    )


if __name__ == "__main__":
    Main()
