import os
import time
import requests
import pandas as pd
from itertools import cycle
from requests.exceptions import ChunkedEncodingError
import concurrent.futures
from tqdm import tqdm

WORKERS_PER_TOKEN = 1
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

def RunQuery(query, variables, retries=3, backoff=5, **kwargs):
    global current_user, current_token
    headers = {"Authorization": f"bearer {current_token}"}

    try:
        response = requests.post(
            API_URL, json={"query": query, "variables": variables},
            headers=headers, timeout=60
        )
    except (ChunkedEncodingError, requests.exceptions.RequestException) as e:
        if retries > 0:
            wait_time = backoff * (4 - retries)  # exponential-ish backoff
            print(f"⚠️ [{current_user}] {e}, retrying in {wait_time}s ({retries} left)")
            time.sleep(wait_time)
            return RunQuery(query, variables, retries - 1, backoff, **kwargs)
        raise

    # Handle truncated/invalid JSON
    try:
        data = response.json()
    except (ValueError, ChunkedEncodingError, requests.exceptions.RequestException) as e:
        if retries > 0:
            wait_time = backoff * (4 - retries)
            print(f"⚠️ [{current_user}] JSON parse failed ({e}), retrying in {wait_time}s ({retries} left)")
            time.sleep(wait_time)
            return RunQuery(query, variables, retries - 1, backoff, **kwargs)
        raise

    if response.status_code in (200, 403):
        reset_time = response.headers.get("X-RateLimit-Reset")
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
            return RunQuery(query, variables, retries, backoff, **kwargs)

    if response.status_code in (502, 504, 500):
        if retries > 0:
            pr_batch = kwargs.get("pr_batch")
            if pr_batch and pr_batch == 100:
                print(f"⚠️ {response.status_code}, reducing pr_batch permanently to 50")
                variables["prBatch"] = 50
                new_kwargs = dict(kwargs, pr_batch=50)
                return RunQuery(query, variables, retries - 1, backoff, **new_kwargs)
            elif pr_batch and pr_batch > 1:
                smaller = max(1, pr_batch // 2)
                print(f"⚠️ {response.status_code}, retrying once with pr_batch={smaller}")
                variables["prBatch"] = smaller
                new_kwargs = dict(kwargs, pr_batch=smaller)
                return RunQuery(query, variables, retries - 1, backoff, **new_kwargs)
            return RunQuery(query, variables, retries - 1, backoff, **kwargs)

    if response.status_code != 200:
        print("❌ Bad response:", response.status_code, response.text[:200])
        raise Exception(f"Query failed: {response.status_code}")

    if "errors" in data and not any(err.get("type") == "RATE_LIMITED" for err in data["errors"]):
        raise Exception(f"GraphQL Error: {data['errors']}")

    return data

# -------------------------------
# Fetch PRs + commits
# -------------------------------

def FetchRepoPRs(owner, repo):
    all_results = []
    final_commits = []
    pr_cursor = None
    pr_batch = 100
    commit_batch = 100

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
                variables.update({"afterCommit": page_info["endCursor"]})
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
# Batch fetch test results (with pagination)
# -------------------------------

def FetchCommitTestsBatch(owner, repo, shas, batch_size=100):
    results = {}
    i = 0
    while i < len(shas):
        current_batch_size = batch_size
        batch = shas[i:i+current_batch_size]

        blocks = []
        for j, sha in enumerate(batch):
            blocks.append(COMMIT_TESTS_QUERY_BLOCK % {"alias": f"c{j}", "sha": sha})
        query = COMMIT_TESTS_QUERY_TEMPLATE % "\n".join(blocks)

        variables = {
            "owner": owner,
            "repo": repo,
            "suiteBatch": 10,
            "runBatch": 50,
            "afterSuite": None,
            "afterRun": None,
        }

        try:
            data = RunQuery(query, variables)
        except Exception as e:
            if "502" in str(e) or "504" in str(e) or "500" in str(e):
                if batch_size == 100:
                    print(f"⚠️ Test batch 100 too big, reducing permanently to 50 for {owner}/{repo}")
                    batch_size = 50
                    continue
                elif batch_size > 1:
                    smaller = max(1, batch_size // 2)
                    print(f"⚠️ Test batch {batch_size} failed, retrying once with {smaller}")
                    current_batch_size = smaller
                    blocks = []
                    for j, sha in enumerate(shas[i:i+current_batch_size]):
                        blocks.append(COMMIT_TESTS_QUERY_BLOCK % {"alias": f"c{j}", "sha": sha})
                    query = COMMIT_TESTS_QUERY_TEMPLATE % "\n".join(blocks)
                    data = RunQuery(query, variables)
                else:
                    raise
            else:
                raise

        for j, sha in enumerate(batch[:current_batch_size]):
            passed = failed = skipped = 0

            # suite pagination
            suite_cursor = None
            while True:
                node = data["data"].get(f"c{j}", {}).get("obj")
                if not node:
                    results[sha] = (0, 0, 0)
                    break

                suites = node["checkSuites"]["nodes"]
                for suite in suites:
                    # run pagination
                    run_cursor = None
                    while True:
                        runs = suite.get("checkRuns", {}).get("nodes", [])
                        for run in runs:
                            if run["conclusion"] == "SUCCESS":
                                passed += 1
                            elif run["conclusion"] == "FAILURE":
                                failed += 1
                            elif run["conclusion"] == "SKIPPED":
                                skipped += 1

                        run_page_info = suite.get("checkRuns", {}).get("pageInfo", {})
                        if run_page_info.get("hasNextPage"):
                            variables["afterRun"] = run_page_info["endCursor"]
                            data = RunQuery(query, variables)
                            suite = data["data"][f"c{j}"]["obj"]["checkSuites"]["nodes"][0]
                            continue
                        break

                results[sha] = (passed, failed, skipped)

                suite_page_info = node["checkSuites"]["pageInfo"]
                if suite_page_info.get("hasNextPage"):
                    variables["afterSuite"] = suite_page_info["endCursor"]
                    data = RunQuery(query, variables)
                    continue
                break

        i += current_batch_size

    return results

# -------------------------------
# Worker
# -------------------------------

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
        pr_to_last_commit = {row["pr_number"]: (row, sha) for row, sha in final_commits}

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

import os
import pandas as pd
import requests

def ProcessOneFile(fname, input_dir, out_dir, token):
    in_path = os.path.join(input_dir, fname)
    df = pd.read_parquet(in_path)

    if "repo_name" not in df.columns:
        print(f"⚠️ Skipping {fname}, no repo_name column")
        return None

    repos = list(df["repo_name"].dropna().unique())
    all_dfs = []
    visited_repos = set()

    for repo_name in repos:
        resolved_repo = ResolveRepoName(repo_name)
        if resolved_repo in visited_repos:
            print(f"🔁 Skipping {repo_name}, redirects to {resolved_repo} already processed")
            continue

        df_repo = WorkerProcess(resolved_repo, token)
        if not df_repo.empty:
            all_dfs.append(df_repo)
            visited_repos.add(resolved_repo)

    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        out_path = os.path.join(out_dir, fname)
        combined_df.to_parquet(out_path, index=False)
        print(f"💾 Wrote commit+test results for {fname} → {out_path}")
    return fname


def ResolveRepoName(repo_name):
    url = f"https://github.com/{repo_name}"
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        final_url = response.url.rstrip("/")
        return "/".join(final_url.split("/")[-2:])
    except Exception:
        return repo_name


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

    with concurrent.futures.ProcessPoolExecutor(max_workers=len(TOKENS) * WORKERS_PER_TOKEN) as executor:
        futures = {
            executor.submit(ProcessOneFile, fname, input_dir, out_dir, TOKENS[i % len(TOKENS)]): fname
            for i, fname in enumerate(fnames)
        }

        with tqdm(total=total_files, desc="Processing files", unit="file") as pbar:
            completed = 0
            for f in concurrent.futures.as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    print(f"❌ File worker crashed: {e}")
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
