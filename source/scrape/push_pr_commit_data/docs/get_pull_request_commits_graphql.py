import os
import time
import requests
import pandas as pd
from itertools import cycle
from requests.exceptions import ChunkedEncodingError
import concurrent.futures
from tqdm import tqdm

WORKERS_PER_TOKEN = 2
API_URL = "https://api.github.com/graphql"
TOKENS = [
    (os.environ["PRIMARY_GITHUB_USERNAME"], os.environ["PRIMARY_GITHUB_TOKEN"]),
    (os.environ["BACKUP_GITHUB_USERNAME"], os.environ["BACKUP_GITHUB_TOKEN"]),
    (os.environ["BACKUP4_GITHUB_USERNAME"], os.environ["BACKUP4_GITHUB_TOKEN"]),
    (os.environ["BACKUP5_GITHUB_USERNAME"], os.environ["BACKUP5_GITHUB_TOKEN"]),
    (os.environ["BACKUP6_GITHUB_USERNAME"], os.environ["BACKUP6_GITHUB_TOKEN"])
]
token_cycle = cycle(TOKENS)
current_user, current_token = next(token_cycle)

# -------------------------------
# Queries
# -------------------------------

PR_COMMITS_QUERY = """
query($owner: String!, $repo: String!, $prBatch: Int!, $afterPR: String, $commitBatch: Int!, $afterCommit: String) {
  repository(owner: $owner, name: $repo) {
    pullRequests(first: $prBatch, after: $afterPR, orderBy: {field: CREATED_AT, direction: ASC}) {
      nodes {
        id
        number
        title
        createdAt
        merged
        mergedAt
        commits(first: $commitBatch, after: $afterCommit) {
          nodes {
            commit {
              oid
              committedDate
              messageHeadline
              message
              changedFiles
              additions
              deletions
              author { name email user { login } }
              committer { name email user { login } }
            }
          }
          pageInfo { hasNextPage endCursor }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

COMMIT_TESTS_QUERY_TEMPLATE = """
query($owner: String!, $repo: String!, $suiteBatch: Int!, $runBatch: Int!, $afterSuite: String, $afterRun: String) {
%s
}
"""

COMMIT_TESTS_QUERY_BLOCK = """
  %(alias)s: repository(owner: $owner, name: $repo) {
    obj: object(oid: "%(sha)s") {
      ... on Commit {
        checkSuites(first: $suiteBatch, after: $afterSuite) {
          pageInfo { hasNextPage endCursor }
          nodes {
            checkRuns(first: $runBatch, after: $afterRun) {
              pageInfo { hasNextPage endCursor }
              nodes { conclusion }
            }
          }
        }
      }
    }
  }
"""

# -------------------------------
# Core Query Runner
# -------------------------------

def RunQuery(query, variables, retries=3, **kwargs):
    global current_user, current_token
    headers = {"Authorization": f"bearer {current_token}"}

    try:
        response = requests.post(
            API_URL, json={"query": query, "variables": variables},
            headers=headers, timeout=60
        )
    except ChunkedEncodingError:
        if retries > 0:
            print(f"⚠️ ChunkedEncodingError, retrying ({retries} left)")
            return RunQuery(query, variables, retries - 1, **kwargs)
        raise

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
            return RunQuery(query, variables, retries, **kwargs)

    if response.status_code in (502, 504):
        if retries > 0:
            print(f"⚠️ {response.status_code}, retrying ({retries} left)")
            return RunQuery(query, variables, retries - 1, **kwargs)
        raise Exception(f"❌ {response.status_code}, retries exhausted")

    if response.status_code != 200:
        print("❌ Bad response:", response.status_code, response.text[:200])
        raise Exception(f"Query failed: {response.status_code}")

    data = response.json()
    if "errors" in data and not any(err.get("type") == "RATE_LIMITED" for err in data["errors"]):
        raise Exception(f"GraphQL Error: {data['errors']}")

    return data

# -------------------------------
# Fetch PRs + commits
# -------------------------------

def FetchRepoPRs(owner, repo):
    all_results = []
    final_commits = []  # last commit per PR
    pr_cursor = None
    pr_batch = 50
    commit_batch = 50

    while True:
        variables = {
            "owner": owner,
            "repo": repo,
            "afterPR": pr_cursor,
            "afterCommit": None,
            "prBatch": pr_batch,
            "commitBatch": commit_batch,
        }
        data = RunQuery(PR_COMMITS_QUERY, variables, pr_batch=pr_batch)

        prs = data["data"]["repository"]["pullRequests"]["nodes"]
        for pr in prs:
            pr_meta = {
                "repo_name": f"{owner}/{repo}",
                "pr_number": pr["number"],
                "pr_title": pr["title"],
                "pr_createdAt": pr["createdAt"],
                "pr_merged": pr["merged"],
                "pr_mergedAt": pr["mergedAt"],
            }

            commit_cursor = None
            last_row, last_commit = None, None
            while True:
                commits = pr["commits"]["nodes"]
                for c in commits:
                    row = dict(pr_meta)
                    row.update({
                        "commit_sha": c["commit"]["oid"],
                        "commit_date": c["commit"]["committedDate"],
                        "commit_message": c["commit"]["messageHeadline"],
                        "commit_message_full": c["commit"]["message"],
                        "files_changed": c["commit"]["changedFiles"],
                        "additions": c["commit"]["additions"],
                        "deletions": c["commit"]["deletions"],
                        "author_name": c["commit"]["author"]["name"] if c["commit"]["author"] else None,
                        "author_email": c["commit"]["author"]["email"] if c["commit"]["author"] else None,
                        "author_login": c["commit"]["author"]["user"]["login"] if c["commit"]["author"] and c["commit"]["author"]["user"] else None,
                        "committer_name": c["commit"]["committer"]["name"] if c["commit"]["committer"] else None,
                        "committer_email": c["commit"]["committer"]["email"] if c["commit"]["committer"] else None,
                        "committer_login": c["commit"]["committer"]["user"]["login"] if c["commit"]["committer"] and c["commit"]["committer"]["user"] else None,
                        "tests_passed": None,
                        "tests_failed": None,
                        "tests_skipped": None,
                    })
                    all_results.append(row)
                    last_row, last_commit = row, c["commit"]

                page_info = pr["commits"]["pageInfo"]
                if not page_info["hasNextPage"]:
                    break
                commit_cursor = page_info["endCursor"]
                variables.update({"afterCommit": commit_cursor})
                data = RunQuery(PR_COMMITS_QUERY, variables, pr_batch=pr_batch)
                pr = data["data"]["repository"]["pullRequests"]["nodes"][0]

            if last_row and last_commit:
                final_commits.append((last_row, last_commit["oid"]))

        page_info = data["data"]["repository"]["pullRequests"]["pageInfo"]
        pr_cursor = page_info["endCursor"]
        if not page_info["hasNextPage"]:
            break

        print(f"Processed PRs up to #{max(pr['number'] for pr in prs)} for {owner}/{repo}")

    return all_results, final_commits

# -------------------------------
# Batch fetch test results
# -------------------------------
def FetchCommitTestsSingle(owner, repo, sha, suite_batch=6, run_batch=120, retries=3):
    """
    Fetch test results for a single commit SHA, with retry + batch shrinkage.
    """
    passed = failed = skipped = 0
    suite_cursor = None

    while True:
        variables = {
            "owner": owner,
            "repo": repo,
            "suiteBatch": suite_batch,
            "runBatch": run_batch,
            "afterSuite": suite_cursor,
            "afterRun": None,
        }
        query = COMMIT_TESTS_QUERY_TEMPLATE % (COMMIT_TESTS_QUERY_BLOCK % {"alias": "c", "sha": sha})

        try:
            data = RunQuery(query, variables, pr_batch=None)
        except Exception as e:
            if retries > 0:
                if suite_batch > 1:
                    return FetchCommitTestsSingle(owner, repo, sha, max(1, suite_batch // 2), run_batch, retries-1)
                if run_batch > 10:
                    return FetchCommitTestsSingle(owner, repo, sha, suite_batch, max(10, run_batch // 2), retries-1)
            raise

        node = data["data"]["c"]["obj"]
        if not node:
            return (0, 0, 0)

        suites = node["checkSuites"]["nodes"]
        for suite in suites:
            runs = suite.get("checkRuns", {}).get("nodes", [])
            for run in runs:
                if run["conclusion"] == "SUCCESS":
                    passed += 1
                elif run["conclusion"] == "FAILURE":
                    failed += 1
                elif run["conclusion"] == "SKIPPED":
                    skipped += 1

        page_info = node["checkSuites"]["pageInfo"]
        if not page_info.get("hasNextPage"):
            break
        suite_cursor = page_info["endCursor"]

    return (passed, failed, skipped)


def FetchCommitTestsBatch(owner, repo, shas):
    results = {}
    for sha in shas:
        results[sha] = FetchCommitTestsSingle(owner, repo, sha)
    return results


def WorkerProcess(repo_name, token):
    global current_user, current_token
    current_user, current_token = token
    if "/" not in repo_name:
        print(f"⚠️ Invalid repo_name {repo_name}")
        return pd.DataFrame()

    owner, repo = repo_name.split("/", 1)
    print(f"🔎 [{current_user}] Fetching {owner}/{repo}")
    start = time.time()

    try:
        all_results, final_commits = FetchRepoPRs(owner, repo)

        # Map PR → last commit row + sha
        pr_to_last_commit = {}
        for row, sha in final_commits:
            pr_to_last_commit[row["pr_number"]] = (row, sha)

        if pr_to_last_commit:
            rows, shas = zip(*pr_to_last_commit.values())
            test_results = FetchCommitTestsBatch(owner, repo, list(shas))
            for row, sha in zip(rows, shas):
                p, f, s = test_results.get(sha, (0, 0, 0))
                row.update({"tests_passed": p, "tests_failed": f, "tests_skipped": s})

    except Exception as e:
        print(f"❌ [{current_user}] Failed {repo_name}: {e}")
        return pd.DataFrame()

    end = time.time()
    print(f"[{current_user}] Finished {repo_name} in {end - start:.2f}s, commits: {len(all_results)}")
    return pd.DataFrame(all_results)

# -------------------------------
# File Processing
# -------------------------------
def ProcessOneFile(fname, input_dir, out_dir, token):
    in_path = os.path.join(input_dir, fname)
    df = pd.read_parquet(in_path)

    if "repo_name" not in df.columns:
        print(f"⚠️ Skipping {fname}, no repo_name column")
        return None

    repos = list(df["repo_name"].dropna().unique())
    all_dfs = []

    for repo_name in repos:
        df_repo = WorkerProcess(repo_name, token)
        if not df_repo.empty:
            all_dfs.append(df_repo)

    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        out_path = os.path.join(out_dir, fname)
        combined_df.to_parquet(out_path, index=False)
        print(f"💾 Wrote commit+test results for {fname} → {out_path}")

def ProcessRepoFiles(input_dir="drive/output/derived/data_export/pr",
                     out_dir="drive/output/scrape/push_pr_commit_data/pull_request_graphql"):
    os.makedirs(out_dir, exist_ok=True)
    fnames = [f for f in os.listdir(input_dir) if f.endswith(".parquet")]
    fnames = [f for f in fnames if not os.path.exists(os.path.join(out_dir, f))]

    if not fnames:
        print("✅ No new files to process.")
        return

    start_time = time.time()
    total_files = len(fnames)

    with concurrent.futures.ProcessPoolExecutor(max_workers=len(TOKENS)) as executor:
        futures = {
            executor.submit(ProcessOneFile, fname, input_dir, out_dir, TOKENS[i % len(TOKENS)]): fname
            for i, fname in enumerate(fnames)
        }
        with tqdm(total=total_files, desc="Processing files", unit="file") as pbar:
            completed = 0
            for f in concurrent.futures.as_completed(futures):
                fname = futures[f]
                try:
                    f.result()
                except Exception as e:
                    print(f"❌ File worker crashed on {fname}: {e}")
                completed += 1
                elapsed = time.time() - start_time
                avg_time = elapsed / completed
                remaining = total_files - completed
                eta = avg_time * remaining
                eta_str = time.strftime("%H:%M:%S", time.gmtime(eta))
                elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
                pbar.set_postfix_str(f"Elapsed {elapsed_str} | ETA {eta_str}")
                pbar.update(1)

if __name__ == "__main__":
    ProcessRepoFiles()
