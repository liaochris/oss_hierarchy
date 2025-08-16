#!/usr/bin/env python3

import os
import sys
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from typing import Set, List
import json
from collections import defaultdict, deque
from source.lib.JMSLab.SaveData import SaveData


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


def GetNameForID(bq_client: bigquery.Client, repo_ids: Set[int]) -> Set[str]:
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


def LinkRepoNameID(
    bq_client: bigquery.Client, initial_repos: List[str]
) -> pd.DataFrame:
    print(f"Starting with {len(initial_repos)} initial repositories")
    known_repo_names = set(initial_repos)
    initial_repos = [repo for repo in initial_repos if len(repo.split("/")) == 2]
    print(f"Continuing with {len(initial_repos)} correctly named repositories")
    known_repo_ids = set()
    iteration = 1

    while True:
        print(f"\nIteration {iteration}")
        print(
            f"Repository names available: {len(known_repo_names)}; Repository IDs available: {len(known_repo_ids)}"
        )

        new_repo_ids = MapIDToName(bq_client, known_repo_names) - known_repo_ids
        if new_repo_ids:
            print(f"Found {len(new_repo_ids)} new repository IDs")
            known_repo_ids.update(new_repo_ids)
        elif iteration > 1:
            print("No new repository IDs found")
            break

        new_repo_names = {
            name for name in GetNameForID(bq_client, known_repo_ids) if "/" in name
        } - known_repo_names
        new_repo_names = {name for name in new_repo_names if len(name.split("/")) == 2}
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
    print(
        f"\nFinal results: {len(known_repo_ids)} unique repository IDs, {len(known_repo_names)} unique repository names, {len(mapping_df)} ID-name pairs"
    )
    return mapping_df


def BuildAdjacencyMaps(mapping_df):
    """
    Returns two dictionaries:
      - repo_id_to_names: repo_id → set of associated repo_names
      - repo_name_to_ids: repo_name → set of associated repo_ids
    """
    repo_id_to_names = defaultdict(set)
    repo_name_to_ids = defaultdict(set)
    for _, row in mapping_df.iterrows():
        rid = row["repo_id"]
        rname = row["repo_name"]
        repo_id_to_names[rid].add(rname)
        repo_name_to_ids[rname].add(rid)
    return repo_id_to_names, repo_name_to_ids


def ExploreConnectedComponent(
    start_repo_id, repo_id_to_names, repo_name_to_ids, visited_ids, visited_names
):
    """
    Starting from start_repo_id, perform a breadth‐first search over the bipartite graph
    (repo_ids ↔ repo_names), collecting all connected repo_ids and repo_names.
    Marks visited_ids and visited_names in place.
    Returns a tuple (component_ids, component_names).
    """
    component_ids = set()
    component_names = set()
    queue = deque([(start_repo_id, "id")])

    while queue:
        value, value_type = queue.popleft()
        if value_type == "id":
            if value in visited_ids:
                continue
            visited_ids.add(value)
            component_ids.add(value)
            for connected_name in repo_id_to_names[value]:
                if connected_name not in visited_names:
                    queue.append((connected_name, "name"))
        else:  # value_type == 'name'
            if value in visited_names:
                continue
            visited_names.add(value)
            component_names.add(value)
            for connected_id in repo_name_to_ids[value]:
                if connected_id not in visited_ids:
                    queue.append((connected_id, "id"))

    return component_ids, component_names


def GroupRepos(mapping_df):
    """
    Finds connected components of repo identities (by id or name). Returns:
      - repo_to_group: dict mapping each repo_id to its component index
      - groups: list of tuples (set_of_ids, set_of_names) for each component
    """
    repo_id_to_names, repo_name_to_ids = BuildAdjacencyMaps(mapping_df)
    visited_ids = set()
    visited_names = set()
    groups = []

    for repo_id in repo_id_to_names:
        if repo_id in visited_ids:
            continue
        component_ids, component_names = ExploreConnectedComponent(
            repo_id, repo_id_to_names, repo_name_to_ids, visited_ids, visited_names
        )
        groups.append((component_ids, component_names))

    repo_to_group = {}
    for group_index, (component_ids, _) in enumerate(groups):
        for rid in component_ids:
            repo_to_group[rid] = group_index

    return repo_to_group, groups


def Main():
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("Error: GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
        sys.exit(1)

    input_dir = Path("output/derived/collect_github_repos")
    output_dir = Path("output/scrape/extract_github_data")

    initial_repo_df = pd.read_csv(input_dir / "linked_pypi_github.csv")
    initial_repos = initial_repo_df["github repository"].drop_duplicates().tolist()
    initial_repos = [
        repo for repo in initial_repos if repo != "Unavailable" and "/" in repo
    ]

    bq_client = bigquery.Client(project="my-project-90021")
    mapping_df = LinkRepoNameID(bq_client, initial_repos)
    mapping_df = mapping_df[
        mapping_df["repo_name"].apply(
            lambda x: len([ele for ele in x.split("/") if ele != ""]) == 2
        )
    ]
    id_map, full_mapping = GroupRepos(mapping_df)
    mapping_df["repo_group"] = mapping_df["repo_id"].map(id_map)
    mapping_df.sort_values(["repo_group", "first_seen"], inplace=True)

    full_mapping_dict = {
        str(i): {
            "repo_ids": [int(x) for x in group_ids],
            "repo_names": list(group_names),
        }
        for i, (group_ids, group_names) in enumerate(full_mapping)
    }
    with open(output_dir / "full_repo_id_mapping.json", "w") as f:
        json.dump(full_mapping_dict, f)

    SaveData(
        mapping_df,
        ["repo_group", "first_seen", "last_seen", "repo_id", "repo_name"],
        output_dir / "repo_id_history.csv",
        output_dir / "repo_id_history.log",
    )


if __name__ == "__main__":
    Main()
