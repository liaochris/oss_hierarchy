import os, time, random, asyncio, concurrent.futures, traceback
import httpx, pandas as pd
from tqdm import tqdm

API_URL = "https://api.github.com/graphql"
WORKERS_PER_TOKEN = 1
MAX_PATHS_PER_QUERY = 40  # safety guard to avoid GraphQL node limits

TOKENS = [
    (os.environ["BACKUP2_GITHUB_USERNAME"], os.environ["BACKUP2_GITHUB_TOKEN"]),
    (os.environ["BACKUP3_GITHUB_USERNAME"], os.environ["BACKUP3_GITHUB_TOKEN"]),
    (os.environ["BACKUP7_GITHUB_USERNAME"], os.environ["BACKUP7_GITHUB_TOKEN"]),
    (os.environ["BACKUP8_GITHUB_USERNAME"], os.environ["BACKUP8_GITHUB_TOKEN"]),
    (os.environ["BACKUP9_GITHUB_USERNAME"], os.environ["BACKUP9_GITHUB_TOKEN"])
]

# -------------------------------
# Governance files of interest
# -------------------------------
COMMON_EXTS = ["", ".md", ".rst", ".txt", ".adoc"]
YAML_EXTS = [".yml", ".yaml"]

INTERESTING_PATHS = []
PATH_CATEGORY_MAP = {}

# CODEOWNERS
for p in ["CODEOWNERS", ".github/CODEOWNERS", "docs/CODEOWNERS"]:
    INTERESTING_PATHS.append(p)
    PATH_CATEGORY_MAP[p] = "codeowners_doc"

# CONTRIBUTING.*
for base in ["CONTRIBUTING", ".github/CONTRIBUTING", "docs/CONTRIBUTING"]:
    for ext in COMMON_EXTS:
        p = base + ext
        INTERESTING_PATHS.append(p)
        PATH_CATEGORY_MAP[p] = "contributing_doc"

# PR templates
for base in ["PULL_REQUEST_TEMPLATE", ".github/PULL_REQUEST_TEMPLATE", "docs/PULL_REQUEST_TEMPLATE"]:
    for ext in COMMON_EXTS:
        p = base + ext
        INTERESTING_PATHS.append(p)
        PATH_CATEGORY_MAP[p] = "pr_template"
PATH_CATEGORY_MAP[".github/PULL_REQUEST_TEMPLATE/"] = "pr_template"

# Issue templates
for base in ["ISSUE_TEMPLATE", ".github/ISSUE_TEMPLATE", "docs/ISSUE_TEMPLATE"]:
    for ext in COMMON_EXTS:
        p = base + ext
        INTERESTING_PATHS.append(p)
        PATH_CATEGORY_MAP[p] = "issue_template"
PATH_CATEGORY_MAP[".github/ISSUE_TEMPLATE/"] = "issue_template"
for ext in YAML_EXTS:
    for base in ["bug", "feature_request"]:
        p = ".github/ISSUE_TEMPLATE/" + base + ext
        INTERESTING_PATHS.append(p)
        PATH_CATEGORY_MAP[p] = "issue_template"

# -------------------------------
# Error class
# -------------------------------
class PermanentGraphQLError(Exception):
    pass

# -------------------------------
# Build Query with Aliases
# -------------------------------
def BuildHistoryQuery(paths):
    alias_map = {p: f"p{i}" for i, p in enumerate(paths)}
    histories = []
    for path, alias in alias_map.items():
        histories.append(f"""
          {alias}: history(path: "{path}", first: $commitBatch, after: $afterCommit) {{
            pageInfo {{ hasNextPage endCursor }}
            nodes {{
              oid
              committedDate
              messageHeadline
              author {{ name email user {{ login }} }}
              committer {{ name email user {{ login }} }}
              changedFiles
              additions
              deletions
            }}
          }}
        """)
    query = f"""
    query($owner: String!, $repo: String!, $commitBatch: Int!, $afterCommit: String) {{
      repository(owner: $owner, name: $repo) {{
        defaultBranchRef {{
          target {{
            ... on Commit {{
              {''.join(histories)}
            }}
          }}
        }}
      }}
    }}
    """
    return query, alias_map

# -------------------------------
# Core Query Runner (async)
# -------------------------------
async def RunQueryAsync(query, variables, user, token, client, **kwargs):
    headers = {"Authorization": f"bearer {token}"}
    retries_at_one, max_retries_at_one = 0, 3

    while True:
        try:
            response = await client.post(API_URL, json={"query": query, "variables": variables}, headers=headers)
            text = response.text

            try:
                data = response.json()
            except Exception:
                raise Exception("Invalid JSON")

            if not isinstance(data, dict):
                raise Exception("Non-dict JSON")

            if response.status_code in (200, 403):
                errors = data.get("errors") or []
                is_rate_limited = (
                    response.status_code == 403 and "rate limit" in text.lower()
                ) or any((err or {}).get("type") == "RATE_LIMITED" for err in errors)

                if is_rate_limited:
                    reset_time = response.headers.get("X-RateLimit-Reset")
                    sleep_for = max(0, int(reset_time) - int(time.time()) + 5) if reset_time else 60
                    print(f"⏳ [{user}] Rate limit hit, sleeping {sleep_for}s…")
                    await asyncio.sleep(sleep_for)
                    continue

                if errors:
                    messages = [err.get("message", "") for err in errors if isinstance(err, dict)]
                    if any("Could not resolve to a Repository" in msg for msg in messages):
                        raise PermanentGraphQLError("REPO_NOT_FOUND")
                    raise PermanentGraphQLError(f"GraphQL Error: {errors}")

            if response.status_code in (502, 504, 500):
                raise Exception(f"{response.status_code} Server Error")

            response.raise_for_status()
            return data or {}

        except PermanentGraphQLError:
            raise
        except Exception as e:
            retries_at_one += 1
            if retries_at_one >= max_retries_at_one:
                print(f"❌ [{user}] {e} after {retries_at_one} retries → returning empty result")
                return {}
            print(f"⚠️ [{user}] {e}, retrying in 5s (attempt {retries_at_one}/{max_retries_at_one})…")
            await asyncio.sleep(5)

# -------------------------------
# Fetch commit history for repo (all paths)
# -------------------------------
def FetchRepoHistories(owner, repo, user, token, RunQuery, paths, session_commit_batch=100):
    all_rows = []
    paths_chunks = [paths[i:i+MAX_PATHS_PER_QUERY] for i in range(0, len(paths), MAX_PATHS_PER_QUERY)]

    for chunk in paths_chunks:
        commit_cursor = None
        query, alias_map = BuildHistoryQuery(chunk)

        while True:
            variables = {
                "owner": owner,
                "repo": repo,
                "commitBatch": session_commit_batch,
                "afterCommit": commit_cursor,
            }
            data = RunQuery(query, variables)
            if data == {}:
                break

            repo_data = (((data.get("data") or {})
                             .get("repository") or {})
                             .get("defaultBranchRef") or {}
                         ).get("target") or {}
            if not repo_data:
                break

            still_has_pages = False
            for path, alias in alias_map.items():
                history = repo_data.get(alias)
                if not history:
                    continue

                for commit_node in history.get("nodes", []):
                    row = {
                        "repo_name": f"{owner}/{repo}",
                        "tracked_path": path,
                        "category": PATH_CATEGORY_MAP.get(path, "other"),
                        "commit_sha": commit_node["oid"],
                        "commit_date": commit_node["committedDate"],
                        "commit_message": commit_node["messageHeadline"],
                        "author_name": (commit_node.get("author") or {}).get("name"),
                        "author_email": (commit_node.get("author") or {}).get("email"),
                        "author_login": ((commit_node.get("author") or {}).get("user") or {}).get("login"),
                        "committer_name": (commit_node.get("committer") or {}).get("name"),
                        "committer_email": (commit_node.get("committer") or {}).get("email"),
                        "committer_login": ((commit_node.get("committer") or {}).get("user") or {}).get("login"),
                        "changed_files_count": commit_node.get("changedFiles"),
                        "additions": commit_node.get("additions"),
                        "deletions": commit_node.get("deletions"),
                        "error": None,
                    }
                    all_rows.append(row)

                page_info = history.get("pageInfo") or {}
                if page_info.get("hasNextPage"):
                    commit_cursor = page_info.get("endCursor")
                    still_has_pages = True

            if not still_has_pages:
                break

    return all_rows

# -------------------------------
# Worker for one repo
# -------------------------------
def WorkerProcess(repo_name, token_tuple):
    user, token = token_tuple
    if "/" not in repo_name:
        print(f"⚠️ [{user}] Invalid repo name: {repo_name}")
        return pd.DataFrame()

    owner, repo = repo_name.split("/", 1)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    client = httpx.AsyncClient(http2=True, timeout=90,
                               headers={"User-Agent": "contrib-scraper"})
    RunQuery = lambda q, v, **kwargs: loop.run_until_complete(
        RunQueryAsync(q, v, user, token, client, **kwargs))

    try:
        print(f"🔎 [{user}] Starting {owner}/{repo}")
        rows = FetchRepoHistories(owner, repo, user, token, RunQuery, INTERESTING_PATHS)
        if rows:
            print(f"✅ [{user}] Finished {owner}/{repo}, rows={len(rows)}")
            return pd.DataFrame(rows)
        else:
            print(f"⚠️ [{user}] Finished {owner}/{repo}, no rows")
            return pd.DataFrame()
    except PermanentGraphQLError as e:
        if str(e) == "REPO_NOT_FOUND":
            print(f"❌ [{user}] Repo not found: {owner}/{repo}, skipping entirely")
            return pd.DataFrame()
        else:
            raise
    except Exception as e:
        print(f"❌ [{user}] Error fetching {owner}/{repo} → {e}")
        traceback.print_exc()
        return pd.DataFrame()
    finally:
        loop.run_until_complete(client.aclose())

# -------------------------------
# File Processing
# -------------------------------
def ProcessOneFile(fname, input_dirs, out_dir, token_tuple):
    try:
        dfs = [pd.read_parquet(os.path.join(d, fname))
               for d in input_dirs if os.path.exists(os.path.join(d, fname))]
        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        if "repo_name" not in df.columns:
            print(f"⚠️ Skipping {fname}, no repo_name column")
            return None

        repos = list(df["repo_name"].dropna().unique())
        print(f"📂 Processing {fname}, repos={len(repos)}")

        all_dfs = [WorkerProcess(repo, token_tuple) for repo in repos]

        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            if not combined_df.empty:
                out_path = os.path.join(out_dir, fname)
                combined_df.to_parquet(out_path, index=False)
                print(f"💾 Wrote commit history for {fname} → {out_path}")
                return fname
            else:
                return None
        return None

    except Exception as e:
        print(f"💥 File-level crash in {fname}: {e}")
        traceback.print_exc()
        return None

def ProcessRepoFiles(input_dirs, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    fnames = [[f for f in os.listdir(input_dir) if f.endswith(".parquet")] for input_dir in input_dirs]
    fnames = [item for sublist in fnames for item in sublist]
    random.shuffle(fnames)
    fnames = [f for f in fnames if not os.path.exists(os.path.join(out_dir, f))]
    if not fnames:
        print("✅ No new files to process.")
        return
    with concurrent.futures.ProcessPoolExecutor(max_workers=len(TOKENS) * WORKERS_PER_TOKEN) as executor:
        futures = {executor.submit(ProcessOneFile, fname, input_dirs, out_dir, TOKENS[i % len(TOKENS)]): fname
                   for i, fname in enumerate(fnames)}
        for f in concurrent.futures.as_completed(futures):
            try:
                f.result()
            except Exception as e:
                print(f"❌ File worker crashed: {e}")

if __name__ == "__main__":
    ProcessRepoFiles(
        input_dirs=["drive/output/derived/problem_level_data/pr","drive/output/derived/problem_level_data/issue"],
        out_dir="drive/output/scrape/push_pr_commit_data/special_files_graphql"
    )
