#!/usr/bin/env python3

import os
import sys
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from typing import Set, List
import json

def MapIDToName(bq_client: bigquery.Client, repo_names: Set[str]) -> Set[int]:
    query = """
    SELECT DISTINCT repo.id AS repo_id
    FROM `githubarchive.month.20*`
    WHERE (_TABLE_SUFFIX BETWEEN '1501' AND '2412')
      AND repo.name IN UNNEST(@names)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("names", "STRING", list(repo_names))
        ]
    )
    results = bq_client.query(query, job_config=job_config).result()
    return {row.repo_id for row in results}

def GetRepoNamesForIds(bq_client: bigquery.Client, repo_ids: Set[int]) -> Set[str]:
    query = """
    SELECT DISTINCT repo.name AS repo_name
    FROM `githubarchive.month.20*`
    WHERE (_TABLE_SUFFIX BETWEEN '1501' AND '2412')
      AND repo.id IN UNNEST(@ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("ids", "INT64", list(repo_ids))
        ]
    )
    results = bq_client.query(query, job_config=job_config).result()
    return {row.repo_name for row in results}

def FindAllRelatedRepos(bq_client: bigquery.Client, initial_repos: List[str]) -> pd.DataFrame:
    known_repo_names = set(initial_repos)
    known_repo_ids = set()
    iteration = 1

    while True:
        print(f"\nIteration {iteration}")
        print(f"Repository names available: {len(known_repo_names)}; Repository IDs available: {len(known_repo_ids)}")

        new_repo_ids = MapIDToName(bq_client, known_repo_names) - known_repo_ids
        if new_repo_ids:
            print(f"Found {len(new_repo_ids)} new repository IDs")
            known_repo_ids.update(new_repo_ids)
        elif iteration > 1:
            print("No new repository IDs found")
            break

        new_repo_names = {name for name in GetRepoNamesForIds(bq_client, known_repo_ids) if "/" in name} - known_repo_names
        if new_repo_names:
            print(f"Found {len(new_repo_names)} new repository names")
            known_repo_names.update(new_repo_names)
        else:
            print("No new repository names found")
            break

        iteration += 1
        if iteration > 10:
            print("Reached maximum iterations, stopping")
            break

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
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("ids", "INT64", list(known_repo_ids))
        ]
    )
    print("\nGetting final mapping...")
    mapping_df = bq_client.query(final_query, job_config=job_config).to_dataframe()
    print(f"\nFinal results: {len(known_repo_ids)} unique repository IDs, {len(known_repo_names)} unique repository names, {len(mapping_df)} ID-name pairs")
    return mapping_df


def GroupRepos(mapping_df):
    visited_ids = set()
    visited_names = set()
    groups = []
    for _, row in mapping_df.iterrows():
        rid, rname = row['repo_id'], row['repo_name']
        if rid in visited_ids or rname in visited_names:
            continue
        group_ids = set()
        group_names = {rname}
        while True:
            new_ids = set(mapping_df[mapping_df['repo_name'].isin(group_names)]['repo_id'])
            new_names = set(mapping_df[mapping_df['repo_id'].isin(group_ids.union(new_ids))]['repo_name'])
            merged_ids = group_ids.union(new_ids)
            merged_names = group_names.union(new_names)
            if merged_ids == group_ids and merged_names == group_names:
                break
            group_ids, group_names = merged_ids, merged_names
        visited_ids.update(group_ids)
        visited_names.update(group_names)
        groups.append((group_ids, group_names))
    mapping = {}
    for i, (grp_ids, _) in enumerate(groups):
        for rid in grp_ids:
            mapping[rid] = i
    return mapping, groups

def Main():
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("Error: GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
        sys.exit(1)

    input_dir = Path('output/derived/collect_github_repos')
    output_dir = Path("output/scrape/extract_github_data")

    initial_repo_df = pd.read_csv(input_dir / 'linked_pypi_github.csv')
    initial_repos = initial_repo_df['github repository'].drop_duplicates().tolist()
    initial_repos = [repo for repo in initial_repos if repo != "Unavailable" and "/" in repo]

    print(f"Starting with {len(initial_repos)} initial repositories")
    bq_client = bigquery.Client()
    mapping_df = FindAllRelatedRepos(bq_client, initial_repos)
    id_map, full_mapping = GroupRepos(mapping_df)
    mapping_df['repo_group'] = mapping_df['repo_id'].map(id_map)
    mapping_df.sort_values(['repo_group','first_seen'], inplace = True)
    
    full_mapping_dict = {
        str(i): {"repo_ids":  [int(x) for x in group_ids], "repo_names": list(group_names)}
        for i, (group_ids, group_names) in enumerate(full_mapping)
    }
    with open(output_dir / "full_repo_id_mapping.json", "w") as f:
        json.dump(full_mapping_dict, f)

    output_path = output_dir / "repo_id_history.csv"

    
    mapping_df.to_csv(output_path, index=False)
    print(f"\nSaved mapping to {output_path}")

if __name__ == "__main__":
    Main()
