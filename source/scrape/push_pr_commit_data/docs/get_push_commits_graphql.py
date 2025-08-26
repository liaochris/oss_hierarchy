import os
import random
import time
import asyncio
import httpx
import pandas as pd
from collections import defaultdict
from tqdm import asyncio as tqdm_asyncio

# -------------------------------
# Config
# -------------------------------
API_URL = "https://api.github.com/graphql"
TOKENS = [
    (os.environ["BACKUP2_GITHUB_USERNAME"], os.environ["BACKUP2_GITHUB_TOKEN"]),
    (os.environ["BACKUP3_GITHUB_USERNAME"], os.environ["BACKUP3_GITHUB_TOKEN"]),
    (os.environ["BACKUP7_GITHUB_USERNAME"], os.environ["BACKUP7_GITHUB_TOKEN"]),
    (os.environ["BACKUP8_GITHUB_USERNAME"], os.environ["BACKUP8_GITHUB_TOKEN"]),
    (os.environ["BACKUP9_GITHUB_USERNAME"], os.environ["BACKUP9_GITHUB_TOKEN"]),
]

repo_batch_size = defaultdict(lambda: 100)   # start at 100, will shrink to 50/25/… if needed
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
                nodes { number title createdAt merged mergedAt }
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
# Exceptions
# -------------------------------
class PermanentGraphQLError(Exception):
    pass

# -------------------------------
# Async Query Runner
# -------------------------------
async def RunQuery(client, user, query, variables):
    headers = {"Authorization": f"bearer {client.headers.get('Authorization', '').replace('bearer ', '')}"}

    while True:
        try:
            resp = await client.post(API_URL, json={"query": query, "variables": variables}, headers=headers)
            text = resp.text
            try:
                data = resp.json()
            except ValueError:
                raise Exception("Expecting value")

            # --- Handle OK / Rate limit ---
            if resp.status_code in (200, 403):
                errors = data.get("errors", [])
                is_rate_limited = (
                    resp.status_code == 403 and "rate limit" in text.lower()
                ) or any(err.get("type") == "RATE_LIMITED" for err in errors)
                if is_rate_limited:
                    reset_time = resp.headers.get("X-RateLimit-Reset")
                    sleep_for = max(0, int(reset_time) - int(time.time()) + 5) if reset_time else 60
                    print(f"⏳ [{user}] Rate limit hit, sleeping {sleep_for}s...")
                    await asyncio.sleep(sleep_for)
                    continue

                if "errors" in data:
                    messages = [err.get("message", "") for err in data["errors"]]
                    if any("Could not resolve to a Repository" in msg for msg in messages):
                        raise PermanentGraphQLError("REPO_NOT_FOUND")
                    if not any(err.get("type") == "RATE_LIMITED" for err in data["errors"]):
                        raise PermanentGraphQLError(f"GraphQL Error: {data['errors']}")

            # --- Transient server errors ---
            if resp.status_code in (502, 504, 500):
                raise Exception(f"{resp.status_code} Server Error")

            resp.raise_for_status()
            return data

        except PermanentGraphQLError:
            raise
        except Exception as e:
            msg = str(e)
            if any(err in msg for err in ["502", "504", "500", "Expecting value", "StreamReset", "Timeout"]):
                if "batchSize" in variables and variables["batchSize"] > 1:
                    old = variables["batchSize"]; new = max(1, old // 2)
                    variables["batchSize"] = new
                    print(f"🔽 [{user}] Reduced batchSize {old} → {new}")
                    continue
                print(f"❌ [{user}] Giving up — batchSize already at 1")
                raise
            else:
                raise

# -------------------------------
# Fetch Commit Tests
# -------------------------------
def BuildCommitTestsQuery(shas):
    parts = []
    for i, sha in enumerate(shas):
        alias = f"c{i}"
        parts.append(f"""
          {alias}: object(oid: "{sha}") {{
            ... on Commit {{
              checkSuites(first: 6) {{
                nodes {{
                  checkRuns(first: 100) {{ nodes {{ conclusion }} }}
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

async def FetchCommitTestsBatchDynamic(client, user, owner, repo, shas):
    batch_size = repo_alias_batch_size[f"{owner}/{repo}"]
    results, start = {}, 0

    while start < len(shas):
        sub_shas = shas[start:start + batch_size]
        query = BuildCommitTestsQuery(sub_shas)
        variables = {"owner": owner, "repo": repo}
        query_counter[f"{owner}/{repo}"] += 1

        try:
            data = await RunQuery(client, user, query, variables)
        except Exception as e:
            if any(x in str(e) for x in ("504", "502", "Premature")):
                if batch_size == 100:
                    print(f"⚠️ [{user}] Reducing alias batch size to 50 for {owner}/{repo}")
                    batch_size = repo_alias_batch_size[f"{owner}/{repo}"] = 50
                    continue
                elif batch_size > 1:
                    smaller = max(1, batch_size // 2)
                    print(f"⚠️ [{user}] Retry with alias batch size={smaller} for {owner}/{repo}")
                    batch_size = smaller
                    continue
            raise

        repo_data = data["data"]["repository"]
        for i, sha in enumerate(sub_shas):
            alias = f"c{i}"
            node = repo_data.get(alias)
            passed = failed = skipped = 0
            if node:
                for suite in node["checkSuites"]["nodes"]:
                    for run in suite["checkRuns"]["nodes"]:
                        if run["conclusion"] == "SUCCESS": passed += 1
                        elif run["conclusion"] == "FAILURE": failed += 1
                        elif run["conclusion"] == "SKIPPED": skipped += 1
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

        try:
            data = await RunQuery(client, user, COMMITS_QUERY, variables)
        except Exception as e:
            print(f"❌ [{user}] Failed fetching commits for {owner}/{repo}: {e}")
            break

        repo_data = data["data"]["repository"]
        if not repo_data or not repo_data["defaultBranchRef"]:
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
                "tests_passed": passed, "tests_failed": failed, "tests_skipped": skipped,
            })

        if not history["pageInfo"]["hasNextPage"]:
            break
        cursor = history["pageInfo"]["endCursor"]

    qcount = query_counter[f"{owner}/{repo}"]
    print(f"🏁 [{user}] {owner}/{repo}: {commit_counter} commits → {qcount} queries")
    return all_results

# -------------------------------
# Worker
# -------------------------------
async def Worker(user, token, repo_name):
    async with httpx.AsyncClient(http2=True, headers={"Authorization": f"bearer {token}"}) as client:
        try:
            if "/" not in repo_name:
                return []
            owner, repo = repo_name.split("/", 1)
            return await FetchRepoCommits(client, user, owner, repo)
        except Exception as e:
            print(f"❌ [{user}] Failed {repo_name}: {e}")
            return []

# -------------------------------
# Coordinator
# -------------------------------
async def ProcessReposAsync(all_repos):
    tasks = []
    for i, repo in enumerate(all_repos):
        user, tok = TOKENS[i % len(TOKENS)]
        tasks.append(asyncio.create_task(Worker(user, tok, repo)))

    results = []
    processed = 0
    for result in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Processing repos", unit="repo"):
        repo_results = await result
        results.append(repo_results)
        processed += 1
        if processed % 50 == 0:
            print(f"✔ {processed}/{len(tasks)} repos processed")

    print(f"🏁 Finished {processed}/{len(tasks)} repos total")
    flat = [r for sub in results for r in sub]
    return pd.DataFrame(flat)

# -------------------------------
# File Processing
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

def ProcessRepoFiles(input_dir="drive/output/derived/data_export/pr",
                     out_dir="drive/output/scrape/push_pr_commit_data/push_graphql"):
    os.makedirs(out_dir, exist_ok=True)
    fnames = [f for f in os.listdir(input_dir) if f.endswith(".parquet")]
    random.shuffle(fnames)
    fnames = [f for f in fnames if not os.path.exists(os.path.join(out_dir, f))]
    if not fnames:
        print("✅ No new files to process."); return

    for fname in fnames:
        in_path, out_path = os.path.join(input_dir, fname), os.path.join(out_dir, fname)
        df = pd.read_parquet(in_path)
        if "repo_name" not in df.columns:
            continue

        repos = [ResolveRepoName(r) for r in df["repo_name"].dropna().unique()]
        print(f"▶️ Processing {fname} with {len(repos)} repos...")
        result_df = asyncio.run(ProcessReposAsync(repos))

        if not result_df.empty:
            result_df.to_parquet(out_path, index=False)
            print(f"💾 Wrote commit history for {fname} → {out_path}")

# -------------------------------
# Entrypoint
# -------------------------------
if __name__ == "__main__":
    ProcessRepoFiles()
