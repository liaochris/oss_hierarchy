import os
import random
import time
import asyncio
import httpx
import pandas as pd
import concurrent.futures
from collections import defaultdict
from tqdm import tqdm

# -------------------------------
# Config
# -------------------------------
API_URL = "https://api.github.com/graphql"

TOKENS = [
    (os.environ["BIG1_GITHUB_USERNAME"], os.environ["BIG1_GITHUB_TOKEN"]),
    (os.environ["BIG2_GITHUB_USERNAME"], os.environ["BIG2_GITHUB_TOKEN"]),
    (os.environ["BACKUP4_GITHUB_USERNAME"], os.environ["BACKUP4_GITHUB_TOKEN"]),
    (os.environ["BACKUP5_GITHUB_USERNAME"], os.environ["BACKUP5_GITHUB_TOKEN"]),
    (os.environ["BACKUP6_GITHUB_USERNAME"], os.environ["BACKUP6_GITHUB_TOKEN"])
]

repo_batch_size = defaultdict(lambda: 100)
repo_alias_batch_size = defaultdict(lambda: 100)
query_counter = defaultdict(int)

# -------------------------------
# Queries
# -------------------------------
COMMITS_QUERY = """
query($owner: String!, $repo: String!, $after: String, $batchSize: Int!) {
  repository(owner: $owner, name: $repo) {
    defaultBranchRef {
      target {
        ... on Commit {
          history(first: $batchSize, after: $after) {
            pageInfo { hasNextPage endCursor }
            nodes {
              oid
              committedDate
              messageHeadline
              message
              changedFiles
              additions
              deletions
              author { name email user { login } }
              committer { name email user { login } }
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

def BuildCommitTestsQuery(shas):
    parts = []
    for i, sha in enumerate(shas):
        alias = f"c{i}"
        parts.append(f"""
          {alias}: object(oid: "{sha}") {{
            ... on Commit {{
              checkSuites(first: 6) {{
                nodes {{
                  checkRuns(first: 100) {{
                    nodes {{ conclusion }}
                  }}
                }}
              }}
            }}
          }}
        """)
    return f"""
    query($owner: String!, $repo: String!) {{
      repository(owner: $owner, name: $repo) {{
        {"".join(parts)}
      }}
    }}
    """

# -------------------------------
# Core Query Runners
# -------------------------------
async def RunQuery(client, user, query, variables,
                   retries=3, delay_seconds=5, owner_repo=None, batch_size=None):
    attempt = 0
    while attempt < retries:
        try:
            resp = await client.post(API_URL, json={"query": query, "variables": variables}, timeout=90)
            try:
                data = resp.json()
            except ValueError:
                raise Exception("TransientError: Empty JSON response")

            # --- Rate limiting ---
            if (resp.status_code == 403 and "rate limit" in resp.text.lower()) or \
               ("errors" in data and any(err.get("type") == "RATE_LIMITED" for err in data["errors"])):
                reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                sleep_for = max(0, reset - int(time.time())) + 5
                print(f"⏳ [{user}] {owner_repo or ''}: Rate limit hit, sleeping {sleep_for}s...")
                await asyncio.sleep(sleep_for)
                continue

            # --- Gateway errors ---
            if resp.status_code in (502, 504):
                raise Exception(f"GatewayError: {resp.status_code} Gateway error")

            # --- Other HTTP errors ---
            if resp.status_code != 200:
                raise Exception(f"TransientError: HTTP {resp.status_code}: {resp.text[:200]}")

            # --- GraphQL errors ---
            if "errors" in data:
                if any(err.get("type") == "NOT_FOUND" for err in data["errors"]):
                    raise Exception(f"FatalError: GraphQL Error: {data['errors']}")
                raise Exception(f"TransientError: GraphQL Error: {data['errors']}")

            if "data" not in data or data["data"] is None:
                raise Exception("TransientError: Missing data block in GraphQL response")

            return data

        except Exception as error:
            msg = str(error)
            if "GatewayError" in msg:
                raise
            if "FatalError" in msg or "NOT_FOUND" in msg:
                print(f"❌ [{user}] {owner_repo or ''}: Fatal error, not retrying: {msg}")
                raise

            attempt += 1
            if attempt < retries:
                # Exponential backoff with jitter
                sleep_for = delay_seconds * (2 ** attempt) + random.uniform(0, 3)
                print(f"⚠️ [{user}] {owner_repo or ''}: {msg}, retrying in {sleep_for:.1f}s... "
                      f"(attempt {attempt}/{retries}, batch={batch_size})")
                await asyncio.sleep(sleep_for)
            else:
                print(f"❌ [{user}] {owner_repo or ''}: Failed after {retries} attempts "
                      f"(batch={batch_size}): {msg}")
                raise


async def SafeRunQuery(client, user, query, variables, batch_size,
                       retries=3, owner_repo=None, is_alias=False):
    gateway_retries = 2
    for attempt in range(retries):
        try:
            return await RunQuery(client, user, query, variables,
                                  owner_repo=owner_repo, batch_size=batch_size)
        except Exception as e:
            msg = str(e)

            if "FatalError" in msg or "NOT_FOUND" in msg:
                raise

            # Shrink batch size on heavy errors
            if ("GatewayError" in msg or "TransientError" in msg) and batch_size > 50:
                # Permanently lock repo to 50
                print(f"⚠️ [{user}] {owner_repo}: {msg} at batch={batch_size}, locking → 50 for remainder of repo")
                if is_alias:
                    repo_alias_batch_size[owner_repo] = 50
                else:
                    repo_batch_size[owner_repo] = 50
                return await SafeRunQuery(client, user, query, variables, 50, retries, owner_repo, is_alias)

            if ("GatewayError" in msg or "TransientError" in msg) and batch_size > 1:
                # Temporary shrink below 50
                new_batch = max(1, batch_size // 2)
                print(f"⚠️ [{user}] {owner_repo}: {msg} at batch={batch_size}, temporarily shrinking → {new_batch}")
                return await SafeRunQuery(client, user, query, variables, new_batch, retries, owner_repo, is_alias)

            # Gateway errors at batch=1
            if "GatewayError" in msg and batch_size == 1:
                if attempt < gateway_retries:
                    sleep_for = 2 * (2 ** attempt) + random.uniform(0, 2)
                    print(f"⚠️ [{user}] {owner_repo}: Gateway error at batch=1, retry {attempt+1}/{gateway_retries}, sleeping {sleep_for:.1f}s...")
                    await asyncio.sleep(sleep_for)
                    continue
                print(f"❌ [{user}] {owner_repo}: Skipped page after repeated gateway errors at batch=1")
                return None

            # Transient errors at batch=1
            if "TransientError" in msg and batch_size == 1:
                if attempt < retries - 1:
                    sleep_for = 5 * (2 ** attempt) + random.uniform(0, 3)
                    print(f"⚠️ [{user}] {owner_repo}: Transient error at batch=1, retry {attempt+1}/{retries}, sleeping {sleep_for:.1f}s...")
                    await asyncio.sleep(sleep_for)
                    continue
                print(f"❌ [{user}] {owner_repo}: Skipped page after repeated transient errors at batch=1")
                return None

            # Generic retry
            if attempt < retries - 1:
                sleep_for = 3 + random.uniform(0, 2)
                print(f"⚠️ [{user}] {owner_repo}: {msg}, retrying in {sleep_for:.1f}s... "
                      f"(attempt {attempt+1}/{retries}, batch={batch_size})")
                await asyncio.sleep(sleep_for)
                continue

            return None
    return None

# -------------------------------
# Fetch Commit Tests
# -------------------------------
async def FetchCommitTestsBatchDynamic(client, user, owner, repo, shas, retries=3):
    batch_size = repo_alias_batch_size[f"{owner}/{repo}"]
    results, start = {}, 0
    while start < len(shas):
        sub_shas = shas[start:start + batch_size]
        query = BuildCommitTestsQuery(sub_shas)
        variables = {"owner": owner, "repo": repo}
        query_counter[f"{owner}/{repo}"] += 1

        data = await SafeRunQuery(client, user, query, variables, batch_size,
                                  retries=retries, owner_repo=f"{owner}/{repo}", is_alias=True)
        if data is None:
            start += batch_size
            continue

        repo_data = data["data"]["repository"]
        for i, sha in enumerate(sub_shas):
            alias = f"c{i}"
            node = repo_data.get(alias)
            passed = failed = skipped = 0
            if node:
                for suite in node["checkSuites"]["nodes"]:
                    for run in suite["checkRuns"]["nodes"]:
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
async def FetchRepoCommits(client, user, owner, repo):
    all_results, cursor, commit_counter = [], None, 0
    batch_size = repo_batch_size[f"{owner}/{repo}"]

    while True:
        variables = {"owner": owner, "repo": repo, "after": cursor, "batchSize": batch_size}
        query_counter[f"{owner}/{repo}"] += 1
        data = await SafeRunQuery(client, user, COMMITS_QUERY, variables, batch_size,
                                  retries=3, owner_repo=f"{owner}/{repo}")

        if data is None:
            break

        repo_data = data["data"]["repository"]
        if not repo_data or not repo_data.get("defaultBranchRef"):
            break

        history = repo_data["defaultBranchRef"]["target"]["history"]
        commits = history["nodes"]
        shas = [c["oid"] for c in commits]

        test_results = await FetchCommitTestsBatchDynamic(client, user, owner, repo, shas)

        for c in commits:
            commit_counter += 1
            pr = c["associatedPullRequests"]["nodes"]
            passed, failed, skipped = test_results.get(c["oid"], (0, 0, 0))
            all_results.append({
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
            })

        if not history["pageInfo"]["hasNextPage"]:
            break
        cursor = history["pageInfo"]["endCursor"]

    print(f"🏁 [{user}] {owner}/{repo}: {commit_counter} commits, queries={query_counter[f'{owner}/{repo}']}")
    return all_results

# -------------------------------
# Worker
# -------------------------------
async def Worker(user, token, repos):
    headers = {"Authorization": f"bearer {token}"}
    async with httpx.AsyncClient(http2=True, headers=headers) as client:
        all_results = []
        for repo_name in repos:
            if "/" not in repo_name:
                continue
            owner, repo = repo_name.split("/", 1)
            try:
                results = await FetchRepoCommits(client, user, owner, repo)
                all_results.extend(results)
            except Exception as e:
                print(f"❌ [{user}] Failed {repo_name}: {e}")
        return all_results

# -------------------------------
# Coordinator
# -------------------------------
async def ProcessReposAsync(all_repos, user, token):
    if not all_repos:
        return pd.DataFrame()
    results = await Worker(user, token, all_repos)
    return pd.DataFrame(results)

# -------------------------------
# Repo Resolver
# -------------------------------
def ResolveRepoName(repo_name):
    import requests
    url = f"https://github.com/{repo_name}"
    try:
        resp = requests.head(url, allow_redirects=True, timeout=5)
        final_url = resp.url.rstrip("/")
        return "/".join(final_url.split("/")[-2:])
    except Exception:
        return repo_name

# -------------------------------
# File Processing
# -------------------------------
def ProcessOneFile(fname, input_dir, out_dir, token_tuple):
    in_path = os.path.join(input_dir, fname)
    out_path = os.path.join(out_dir, fname)

    df = pd.read_parquet(in_path)
    if "repo_name" not in df.columns:
        print(f"⚠️ Skipping {fname}, no repo_name column")
        return None

    repos = list(df["repo_name"].dropna().unique())
    repos = list(set([ResolveRepoName(r) for r in repos]))

    user, token = token_tuple
    print(f"▶️ [{user}] Processing {fname} with {len(repos)} repos...")

    try:
        result_df = asyncio.run(ProcessReposAsync(repos, user, token))
    except Exception as e:
        print(f"❌ [{user}] {fname} failed: {e}")
        return None

    if not result_df.empty:
        result_df.to_parquet(out_path, index=False)
        print(f"💾 Wrote commit history for {fname} → {out_path}")
        return fname
    print(f"⚠️ {fname} produced empty DataFrame, skipping export")
    return None

def ProcessRepoFiles(input_dirs,
                     out_dir="drive/output/scrape/push_pr_commit_data/push_graphql"):
    os.makedirs(out_dir, exist_ok=True)
    fnames = [[f for f in os.listdir(input_dir) if f.endswith(".parquet")] for input_dir in input_dirs]
    fnames = [item for sublist in fnames for item in sublist]
    random.shuffle(fnames)
    fnames = [f for f in fnames if not os.path.exists(os.path.join(out_dir, f))]
    if not fnames:
        print("✅ No new files to process.")
        return

    total_files = len(fnames)
    with concurrent.futures.ProcessPoolExecutor(max_workers=len(TOKENS)) as executor:
        futures = {
            executor.submit(ProcessOneFile, fname, input_dir, out_dir, TOKENS[i % len(TOKENS)]): fname
            for i, fname in enumerate(fnames)
        }

        with tqdm(total=total_files, desc="Processing files", unit="file") as pbar:
            completed, exported = 0, 0
            for f in concurrent.futures.as_completed(futures):
                try:
                    result = f.result()
                    if result:
                        exported += 1
                        pbar.update(1)
                    else:
                        pbar.total -= 1
                        pbar.refresh()
                except Exception as e:
                    print(f"❌ File worker crashed: {e}")
                    pbar.total -= 1
                    pbar.refresh()
                completed += 1
                pbar.set_postfix_str(f"Exported {exported}/{pbar.total}")
    print("✅ Finished processing.")

# -------------------------------
# Entrypoint
# -------------------------------
if __name__ == "__main__":
    ProcessRepoFiles(input_dirs=["drive/output/derived/repo_level_data/pr","drive/output/derived/repo_level_data/issue"])
