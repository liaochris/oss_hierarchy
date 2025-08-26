import random
import os
import time
import asyncio
import httpx
import pandas as pd
from itertools import cycle
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

# -------------------------------
# Queries
# -------------------------------
PR_COMMITS_QUERY_WITH_CHANGEDFILES = """
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

PR_COMMITS_QUERY_NO_CHANGEDFILES = PR_COMMITS_QUERY_WITH_CHANGEDFILES.replace("changedFiles", "")
PR_COMMITS_QUERY = PR_COMMITS_QUERY_WITH_CHANGEDFILES

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
# Helpers
# -------------------------------
def SafeGet(d, *keys, default=None):
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
    return d if d is not None else default

class PermanentGraphQLError(Exception):
    pass

# -------------------------------
# Core Query Runner (async)
# -------------------------------
async def RunQueryAsync(query, variables, user, token, client, **kwargs):
    headers = {"Authorization": f"bearer {token}"}
    while True:
        try:
            response = await client.post(API_URL, json={"query": query, "variables": variables}, headers=headers)
            text = response.text
            try:
                data = response.json()
            except ValueError:
                raise Exception("Expecting value")

            if response.status_code in (200, 403):
                errors = data.get("errors", [])
                is_rate_limited = (
                    response.status_code == 403 and "rate limit" in text.lower()
                ) or any(err.get("type") == "RATE_LIMITED" for err in errors)
                if is_rate_limited:
                    reset_time = response.headers.get("X-RateLimit-Reset")
                    sleep_for = max(0, int(reset_time) - int(time.time()) + 5) if reset_time else 60
                    print(f"⏳ [{user}] Rate limit hit, sleeping {sleep_for}s...")
                    await asyncio.sleep(sleep_for)
                    continue
                if "errors" in data:
                    messages = [err.get("message", "") for err in data["errors"]]
                    if any("Could not resolve to a Repository" in msg for msg in messages):
                        raise PermanentGraphQLError("REPO_NOT_FOUND")
                    if any("changedFiles count" in msg and "unavailable" in msg for msg in messages):
                        print(f"⚠️ [{user}] changedFiles unavailable, retrying without it")
                        response2 = await client.post(
                            API_URL,
                            json={"query": PR_COMMITS_QUERY_NO_CHANGEDFILES, "variables": variables},
                            headers=headers
                        )
                        return response2.json()
                    if not any(err.get("type") == "RATE_LIMITED" for err in data["errors"]):
                        raise PermanentGraphQLError(f"GraphQL Error: {data['errors']}")
            if response.status_code in (502, 504, 500):
                raise Exception(f"{response.status_code} Server Error")

            response.raise_for_status()
            return data

        except PermanentGraphQLError:
            raise
        except Exception as e:
            msg = str(e)
            if any(err in msg for err in ["502", "504", "500", "Expecting value", "StreamReset", "Timeout"]):
                if "prBatch" in variables and variables["prBatch"] > 1:
                    old = variables["prBatch"]; new = max(1, old // 2)
                    variables["prBatch"] = new
                    print(f"🔽 [{user}] Reduced prBatch {old} → {new}")
                if "commitBatch" in variables and variables["commitBatch"] > 1:
                    old = variables["commitBatch"]; new = max(1, old // 2)
                    variables["commitBatch"] = new
                    print(f"🔽 [{user}] Reduced commitBatch {old} → {new}")
                if variables.get("prBatch", 1) == 1 and variables.get("commitBatch", 1) == 1:
                    print(f"❌ [{user}] Giving up — prBatch and commitBatch already at 1")
                    raise
                print(f"⚠️ [{user}] {e}, retrying in 5s...")
                await asyncio.sleep(5)
                continue
            else:
                raise

def make_runquery(user, token, client, loop):
    def _inner(query, variables, **kwargs):
        return loop.run_until_complete(RunQueryAsync(query, variables, user, token, client, **kwargs))
    return _inner

# -------------------------------
# Fetch PRs + commits
# -------------------------------
def FetchRepoPRs(owner, repo, user, token, RunQuery):
    all_results, final_commits, pr_cursor = [], [], None
    session_pr_batch, session_commit_batch = 100, 100

    while True:
        variables = {"owner": owner, "repo": repo, "afterPR": pr_cursor,
                     "afterCommit": None, "prBatch": session_pr_batch, "commitBatch": session_commit_batch}
        data = RunQuery(PR_COMMITS_QUERY, variables, pr_batch=session_pr_batch, commit_batch=session_commit_batch)
        session_pr_batch = 50 if variables["prBatch"] <= 50 else variables["prBatch"]
        session_commit_batch = 50 if variables["commitBatch"] <= 50 else variables["commitBatch"]

        try:
            repo_data = SafeGet(data, "data", "repository", default={})
            pull_requests = SafeGet(repo_data, "pullRequests", default={})
            prs = pull_requests.get("nodes") or []
        except Exception:
            print(f"❌ [{user}] Debug: failed parsing PR data for {owner}/{repo}, data={data}")
            raise

        for pr in prs:
            pr_meta = {"repo_name": f"{owner}/{repo}", "pr_number": pr.get("number"),
                       "pr_title": pr.get("title"), "pr_createdAt": pr.get("createdAt"),
                       "pr_merged": pr.get("merged"), "pr_mergedAt": pr.get("mergedAt")}
            last_row, last_commit = None, None

            while True:
                try:
                    commits = SafeGet(pr, "commits", "nodes", default=[])
                except Exception:
                    print(f"❌ [{user}] Debug: failed parsing commit data for {owner}/{repo}, data={data}")
                    raise
                for c in commits:
                    commit_data = c.get("commit") or {}
                    if not commit_data:
                        print(f"⚠️ [{user}] Skipping null commit in {owner}/{repo}")
                        continue
                    author, committer = commit_data.get("author") or {}, commit_data.get("committer") or {}
                    author_user, committer_user = author.get("user") or {}, committer.get("user") or {}
                    row = dict(pr_meta)
                    row.update({
                        "commit_sha": commit_data.get("oid"),
                        "commit_date": commit_data.get("committedDate"),
                        "commit_message": commit_data.get("messageHeadline"),
                        "commit_message_full": commit_data.get("message"),
                        "files_changed": commit_data.get("changedFiles"),
                        "additions": commit_data.get("additions"),
                        "deletions": commit_data.get("deletions"),
                        "author_name": author.get("name"), "author_email": author.get("email"),
                        "author_login": author_user.get("login"),
                        "committer_name": committer.get("name"), "committer_email": committer.get("email"),
                        "committer_login": committer_user.get("login"),
                        "tests_passed": None, "tests_failed": None, "tests_skipped": None,
                    })
                    all_results.append(row)
                    last_row, last_commit = row, commit_data
                page_info = SafeGet(pr, "commits", "pageInfo", default={})
                if not page_info.get("hasNextPage"): break
                variables.update({"afterCommit": page_info.get("endCursor")})
                data = RunQuery(PR_COMMITS_QUERY, variables, pr_batch=session_pr_batch, commit_batch=session_commit_batch)
                repo_data = SafeGet(data, "data", "repository", default={})
                pull_requests = SafeGet(repo_data, "pullRequests", default={})
                nodes = pull_requests.get("nodes") or []
                if not nodes: break
                pr = nodes[0]
            if last_row and last_commit:
                final_commits.append((last_row, last_commit.get("oid")))

        page_info = SafeGet(pull_requests, "pageInfo", default={})
        pr_cursor = page_info.get("endCursor")
        if not page_info.get("hasNextPage"): break
        if prs:
            print(f"Processed PRs up to #{max(pr['number'] for pr in prs if pr.get('number'))} for {owner}/{repo}")
    return all_results, final_commits

# -------------------------------
# Fetch commit test results
# -------------------------------
def FetchCommitTestsBatch(owner, repo, shas, user, token, RunQuery, batch_size=100, test_batch=None):
    results, i = {}, 0
    if test_batch is not None: batch_size = test_batch

    while i < len(shas):
        current_batch_size = batch_size
        batch = shas[i:i+current_batch_size]
        blocks = [COMMIT_TESTS_QUERY_BLOCK % {"alias": f"c{j}", "sha": sha} for j, sha in enumerate(batch)]
        query = COMMIT_TESTS_QUERY_TEMPLATE % "\n".join(blocks)
        variables = {"owner": owner, "repo": repo, "suiteBatch": 10, "runBatch": 50,
                     "afterSuite": None, "afterRun": None}
        try:
            data = RunQuery(query, variables)
        except Exception as e:
            if any(err in str(e) for err in ["502", "504", "500", "Expecting value", "StreamReset", "Timeout"]):
                if batch_size > 1:
                    smaller = max(1, batch_size // 2)
                    print(f"🔽 [{user}] Test batch {batch_size} failed ({e}), retrying with {smaller}")
                    batch_size = smaller
                    if batch_size <= 50: test_batch = 50
                    time.sleep(5); continue
                else: raise
            else: raise

        for j, sha in enumerate(batch[:current_batch_size]):
            try:
                passed = failed = skipped = 0
                c_entry = SafeGet(data, "data", f"c{j}", default={})
                node = c_entry.get("obj") if isinstance(c_entry, dict) else None
            except Exception:
                print(f"❌ [{user}] Debug: failed parsing test data for {owner}/{repo} sha={sha}, data={data}")
                raise
            if not node:
                results[sha] = (0, 0, 0); continue
            suites = SafeGet(node, "checkSuites", "nodes", default=[])
            for suite in suites:
                runs = SafeGet(suite, "checkRuns", "nodes", default=[])
                for run in runs:
                    conc = run.get("conclusion")
                    if conc == "SUCCESS": passed += 1
                    elif conc == "FAILURE": failed += 1
                    elif conc == "SKIPPED": skipped += 1
            results[sha] = (passed, failed, skipped)
        i += current_batch_size
    return results, test_batch

# -------------------------------
# Worker
# -------------------------------
def WorkerProcess(repo_name, token_tuple):
    user, token = token_tuple
    if "/" not in repo_name:
        print(f"⚠️ Invalid repo_name {repo_name}")
        return pd.DataFrame()

    owner, repo = repo_name.split("/", 1)
    print(f"🔎 [{user}] Fetching {owner}/{repo}")
    start = time.time()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    client = httpx.AsyncClient(http2=True, timeout=90, headers={"User-Agent": "commit-scraper"})
    RunQuery = make_runquery(user, token, client, loop)

    try:
        all_results, final_commits = FetchRepoPRs(owner, repo, user, token, RunQuery)
        pr_to_last_commit = {row["pr_number"]: (row, sha) for row, sha in final_commits}
        if pr_to_last_commit:
            rows, shas = zip(*pr_to_last_commit.values())
            test_results, test_batch = FetchCommitTestsBatch(owner, repo, list(shas), user, token, RunQuery)
            for row, sha in zip(rows, shas):
                p, f, s = test_results.get(sha, (0, 0, 0))
                row.update({"tests_passed": p, "tests_failed": f, "tests_skipped": s})
    except PermanentGraphQLError as e:
        if str(e) == "REPO_NOT_FOUND": return pd.DataFrame()
        else:
            print(f"❌ [{user}] {e}"); return pd.DataFrame()
    except Exception as e:
        print(f"❌ [{user}] Failed {repo_name}: {e}"); return pd.DataFrame()
    finally:
        loop.run_until_complete(client.aclose())

    end = time.time()
    print(f"[{user}] Finished {repo_name} in {end - start:.2f}s, commits: {len(all_results)}")
    return pd.DataFrame(all_results)

# -------------------------------
# File Processing
# -------------------------------
def ProcessOneFile(fname, input_dir, out_dir, token_tuple):
    in_path = os.path.join(input_dir, fname)
    df = pd.read_parquet(in_path)
    if "repo_name" not in df.columns:
        print(f"⚠️ Skipping {fname}, no repo_name column"); return None

    repos = list(df["repo_name"].dropna().unique())
    all_dfs, visited_repos = [], set()
    for repo_name in repos:
        resolved_repo = ResolveRepoName(repo_name)
        if resolved_repo in visited_repos:
            print(f"🔁 Skipping {repo_name}, redirects to {resolved_repo} already processed"); continue
        df_repo = WorkerProcess(resolved_repo, token_tuple)
        if not df_repo.empty:
            all_dfs.append(df_repo); visited_repos.add(resolved_repo)

    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        out_path = os.path.join(out_dir, fname)
        combined_df.to_parquet(out_path, index=False)
        print(f"💾 Wrote commit+test results for {fname} → {out_path}")
        return fname
    return None

def ResolveRepoName(repo_name):
    url = f"https://github.com/{repo_name}"
    try:
        response = httpx.head(url, follow_redirects=True, timeout=5)
        final_url = str(response.url).rstrip("/")
        return "/".join(final_url.split("/")[-2:])
    except Exception:
        return repo_name

def ProcessRepoFiles(input_dir="drive/output/derived/data_export/pr",
                     out_dir="drive/output/scrape/push_pr_commit_data/pull_request_graphql"):
    os.makedirs(out_dir, exist_ok=True)
    fnames = [f for f in os.listdir(input_dir) if f.endswith(".parquet")]
    random.shuffle(fnames)
    fnames = [f for f in fnames if not os.path.exists(os.path.join(out_dir, f))]
    if not fnames:
        print("✅ No new files to process."); return

    start_time, total_files = time.time(), len(fnames)
    with concurrent.futures.ProcessPoolExecutor(max_workers=len(TOKENS) * WORKERS_PER_TOKEN) as executor:
        futures = {executor.submit(ProcessOneFile, fname, input_dir, out_dir, TOKENS[i % len(TOKENS)]): fname
                   for i, fname in enumerate(fnames)}
        exported, completed = 0, 0
        with tqdm(total=total_files, desc="Processing files", unit="file") as pbar:
            for f in concurrent.futures.as_completed(futures):
                try:
                    result = f.result()
                    if result: exported += 1; pbar.update(1)
                    else: pbar.total -= 1; pbar.refresh()
                except Exception as e:
                    print(f"❌ File worker crashed: {e}"); pbar.total -= 1; pbar.refresh()
                completed += 1
                elapsed = time.time() - start_time
                avg_time = elapsed / completed
                remaining = pbar.total - exported
                eta = avg_time * remaining if remaining > 0 else 0
                eta_str, elapsed_str = time.strftime("%H:%M:%S", time.gmtime(eta)), time.strftime("%H:%M:%S", time.gmtime(elapsed))
                pbar.set_postfix_str(f"Exported {exported}/{pbar.total} | Elapsed {elapsed_str} | ETA {eta_str}")
    print(f"✅ Finished processing. Exported {exported} out of {pbar.total} files.")

if __name__ == "__main__":
    ProcessRepoFiles()
