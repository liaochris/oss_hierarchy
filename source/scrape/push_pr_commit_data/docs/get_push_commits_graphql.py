import os
import time
import requests
import pandas as pd
from itertools import cycle
from requests.exceptions import ChunkedEncodingError
import concurrent.futures
from tqdm import tqdm
from collections import defaultdict

WORKERS_PER_TOKEN = 2
API_URL = "https://api.github.com/graphql"

TOKENS = [
    (os.environ["BACKUP2_GITHUB_USERNAME"], os.environ["BACKUP2_GITHUB_TOKEN"]),
    (os.environ["BACKUP3_GITHUB_USERNAME"], os.environ["BACKUP3_GITHUB_TOKEN"]),
    (os.environ["BACKUP7_GITHUB_USERNAME"], os.environ["BACKUP7_GITHUB_TOKEN"]),
    (os.environ["BACKUP8_GITHUB_USERNAME"], os.environ["BACKUP8_GITHUB_TOKEN"]),
    (os.environ["BACKUP9_GITHUB_USERNAME"], os.environ["BACKUP9_GITHUB_TOKEN"])
]
token_cycle = cycle(TOKENS)
current_user, current_token = next(token_cycle)

# -------------------------------
# Queries
# -------------------------------

COMMITS_QUERY = """
query($owner: String!, $repo: String!, $after: String) {
  repository(owner: $owner, name: $repo) {
    defaultBranchRef {
      target {
        ... on Commit {
          history(first: 100, after: $after) {
            pageInfo { hasNextPage endCursor }
            nodes {
              oid
              committedDate
              messageHeadline
              message
              changedFiles
              additions
              deletions
              author {
                name email user { login }
              }
              committer {
                name email user { login }
              }
              associatedPullRequests(first: 1) {
                nodes {
                  number
                  title
                  createdAt
                  merged
                  mergedAt
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

# -------------------------------
# Query Counter
# -------------------------------

query_counter = defaultdict(int)

def IncrementQueryCounter(owner, repo):
    key = f"{owner}/{repo}"
    query_counter[key] += 1

# -------------------------------
# Core Query Runner
# -------------------------------
def RunQuery(query, variables):
    global current_user, current_token
    headers = {"Authorization": f"bearer {current_token}"}

    response = requests.post(
        API_URL, json={"query": query, "variables": variables},
        headers=headers, timeout=60
    )
    # --- Rate limit handler (HTTP + GraphQL error cases) ---
    if response.status_code in (200, 403):
        reset_time = response.headers.get("X-RateLimit-Reset")
        try:
            data = response.json()
        except Exception:
            data = {}

        errors = data.get("errors", [])
        is_rate_limited = (
            response.status_code == 403 and "rate limit" in response.text.lower()
        ) or any(err.get("type") == "RATE_LIMITED" for err in errors)

        if is_rate_limited:
            sleep_for = (
                max(0, int(reset_time) - int(time.time())) + 5
                if reset_time else 60
            )
            print(f"⏳ [{current_user}] Rate limit hit, sleeping {sleep_for}s...")
            time.sleep(sleep_for)
            return RunQuery(query, variables)

    # Don’t retry 502/504 here — just raise so caller can shrink
    if response.status_code in (502, 504):
        raise Exception(f"{response.status_code} Gateway error")

    if response.status_code != 200:
        raise Exception(f"Query failed: {response.status_code}, {response.text[:200]}")

    data = response.json()
    if "errors" in data and not any(err.get("type") == "RATE_LIMITED" for err in data["errors"]):
        raise Exception(f"GraphQL Error: {data['errors']}")

    return data


# -------------------------------
# Commit Test Batch Query Builder
# -------------------------------

def BuildCommitTestsQuery(shas):
    parts = []
    for i, sha in enumerate(shas):
        alias = f"c{i}"
        parts.append(f"""
          {alias}: object(oid: "{sha}") {{
            ... on Commit {{
              checkSuites(first: 6) {{
                pageInfo {{ hasNextPage endCursor }}
                nodes {{
                  checkRuns(first: 100) {{
                    pageInfo {{ hasNextPage endCursor }}
                    nodes {{ conclusion }}
                  }}
                }}
              }}
            }}
          }}
        """)
    query = f"""
    query($owner: String!, $repo: String!) {{
      repository(owner: $owner, name: $repo) {{
        {"".join(parts)}
      }}
    }}
    """
    return query

# -------------------------------
# Fetch Commit Tests in Batches
# -------------------------------

def FetchCommitTestsBatchDynamic(owner, repo, shas, max_batch=200, retries=3):
    batch_size = max_batch
    results = {}
    start = 0

    while start < len(shas):
        sub_shas = shas[start:start+batch_size]
        query = BuildCommitTestsQuery(sub_shas)
        variables = {"owner": owner, "repo": repo}
        IncrementQueryCounter(owner, repo)

        try:
            data = RunQuery(query, variables)
        except Exception as e:
            if ("504" in str(e) or "502" in str(e)) and batch_size > 1:
                batch_size = max(1, batch_size // 2)
                print(f"⚠️ [{owner}/{repo}] Reducing alias batch size → {batch_size}")
                continue
            elif retries > 0:
                return FetchCommitTestsBatchDynamic(owner, repo, shas[start:], batch_size, retries-1)
            else:
                raise

        repo_data = data["data"]["repository"]
        for i, sha in enumerate(sub_shas):
            alias = f"c{i}"
            node = repo_data.get(alias)
            passed = failed = skipped = 0
            if node:
                suites = node["checkSuites"]["nodes"]
                for suite in suites:
                    for run in suite.get("checkRuns", {}).get("nodes", []):
                        if run["conclusion"] == "SUCCESS":
                            passed += 1
                        elif run["conclusion"] == "FAILURE":
                            failed += 1
                        elif run["conclusion"] == "SKIPPED":
                            skipped += 1
            results[sha] = (passed, failed, skipped)

        start += batch_size

    return results

# -------------------------------
# Fetch Repo Commits
# -------------------------------

def FetchRepoCommits(owner, repo):
    all_results = []
    cursor = None
    commit_counter = 0

    while True:
        variables = {"owner": owner, "repo": repo, "after": cursor}
        IncrementQueryCounter(owner, repo)
        data = RunQuery(COMMITS_QUERY, variables)

        repo_data = data["data"]["repository"]
        if not repo_data or not repo_data["defaultBranchRef"]:
            break

        history = repo_data["defaultBranchRef"]["target"]["history"]
        commits = history["nodes"]
        shas = [c["oid"] for c in commits]

        # Fetch tests for up to 100 commits at once
        test_results = FetchCommitTestsBatchDynamic(owner, repo, shas, max_batch=200)

        for c in commits:
            commit_counter += 1
            pr = c["associatedPullRequests"]["nodes"]
            passed, failed, skipped = test_results.get(c["oid"], (0, 0, 0))

            row = {
                "repo_name": f"{owner}/{repo}",
                "commit_sha": c["oid"],
                "commit_date": c["committedDate"],
                "commit_message": c["messageHeadline"],
                "commit_message_full": c["message"],
                "files_changed": c["changedFiles"],
                "additions": c["additions"],
                "deletions": c["deletions"],
                "author_name": c["author"]["name"] if c["author"] else None,
                "author_email": c["author"]["email"] if c["author"] else None,
                "author_login": c["author"]["user"]["login"] if c["author"] and c["author"]["user"] else None,
                "committer_name": c["committer"]["name"] if c["committer"] else None,
                "committer_email": c["committer"]["email"] if c["committer"] else None,
                "committer_login": c["committer"]["user"]["login"] if c["committer"] and c["committer"]["user"] else None,
                "pr_number": pr[0]["number"] if pr else None,
                "pr_title": pr[0]["title"] if pr else None,
                "pr_created_at": pr[0]["createdAt"] if pr else None,
                "pr_merged": pr[0]["merged"] if pr else None,
                "pr_merged_at": pr[0]["mergedAt"] if pr else None,
                "tests_passed": passed,
                "tests_failed": failed,
                "tests_skipped": skipped,
            }
            all_results.append(row)

        if not history["pageInfo"]["hasNextPage"]:
            break
        cursor = history["pageInfo"]["endCursor"]

    qcount = query_counter[f"{owner}/{repo}"]
    print(f"🏁 [{owner}/{repo}] Finished {commit_counter} commits → {qcount} queries")
    return all_results

# -------------------------------
# Worker
# -------------------------------

def WorkerProcess(repo_name, token):
    global current_user, current_token
    current_user, current_token = token
    if "/" not in repo_name:
        return pd.DataFrame()

    owner, repo = repo_name.split("/", 1)
    start = time.time()

    try:
        results = FetchRepoCommits(owner, repo)
    except Exception as e:
        print(f"❌ [{current_user}] Failed {repo_name}: {e}")
        return pd.DataFrame()

    end = time.time()
    print(f"[{current_user}] {repo_name} in {end - start:.2f}s, commits: {len(results)}")
    return pd.DataFrame(results)

# -------------------------------
# File Processing
# -------------------------------

def ProcessOneFile(fname, input_dir, out_dir, token):
    in_path = os.path.join(input_dir, fname)
    out_path = os.path.join(out_dir, fname)
    df = pd.read_parquet(in_path)

    if "repo_name" not in df.columns:
        return None
    
    if os.path.exists(out_path):
        print(f"⏭️ Skipping {fname}, already processed")
        return None

    repos = list(df["repo_name"].dropna().unique())
    all_dfs = []

    for repo_name in repos:
        df_repo = WorkerProcess(repo_name, token)
        if not df_repo.empty:
            all_dfs.append(df_repo)

    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        combined_df.to_parquet(out_path, index=False)
        print(f"💾 Wrote commit history for {fname} → {out_path}")

def ProcessRepoFiles(input_dir="drive/output/derived/data_export/pr",
                     out_dir="drive/output/scrape/push_pr_commit_data/push_graphql"):
    os.makedirs(out_dir, exist_ok=True)
    fnames = [f for f in os.listdir(input_dir) if f.endswith(".parquet")]

    with concurrent.futures.ProcessPoolExecutor(max_workers=len(TOKENS) * WORKERS_PER_TOKEN) as executor:
        futures = {
            executor.submit(ProcessOneFile, fname, input_dir, out_dir, TOKENS[i % len(TOKENS)]): fname
            for i, fname in enumerate(fnames)
        }
        with tqdm(total=len(futures), desc="Processing files") as pbar:
            for f in concurrent.futures.as_completed(futures):
                fname = futures[f]
                try:
                    f.result()
                except Exception as e:
                    print(f"❌ File worker crashed on {fname}: {e}")
                pbar.update(1)

if __name__ == "__main__":
    ProcessRepoFiles()
