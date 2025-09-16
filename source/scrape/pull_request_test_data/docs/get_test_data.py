import os
import sys
import argparse
import requests
import pandas as pd
from pathlib import Path
import logging
import time
import requests
from requests.exceptions import ChunkedEncodingError
import random

PULL_REQUEST_LIMIT = 100
SUITES_LIMIT = 5
RUNS_LIMIT = 100

GITHUB_API_URL = "https://api.github.com/graphql"

# 10 tokens = 5 primary + 5 backup
TOKENS = [
    (os.environ["BACKUP4_GITHUB_USERNAME"], os.environ["BACKUP4_GITHUB_TOKEN"]),
    (os.environ["BACKUP5_GITHUB_USERNAME"], os.environ["BACKUP5_GITHUB_TOKEN"]),
    (os.environ["BACKUP6_GITHUB_USERNAME"], os.environ["BACKUP6_GITHUB_TOKEN"]),
    (os.environ["BACKUP2_GITHUB_USERNAME"], os.environ["BACKUP2_GITHUB_TOKEN"]),
    (os.environ["BACKUP3_GITHUB_USERNAME"], os.environ["BACKUP3_GITHUB_TOKEN"]),
    (os.environ["BACKUP7_GITHUB_USERNAME"], os.environ["BACKUP7_GITHUB_TOKEN"]),
    (os.environ["BACKUP8_GITHUB_USERNAME"], os.environ["BACKUP8_GITHUB_TOKEN"]),
    (os.environ["BACKUP9_GITHUB_USERNAME"], os.environ["BACKUP9_GITHUB_TOKEN"]),
    (os.environ["BIG1_GITHUB_USERNAME"], os.environ["BIG1_GITHUB_TOKEN"]),
    (os.environ["BIG2_GITHUB_USERNAME"], os.environ["BIG2_GITHUB_TOKEN"]),
]

# Group into 5 primary/backup pairs
TOKEN_PAIRS = [(TOKENS[i], TOKENS[i + 5]) for i in range(5)]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def RunGraphQLQuery(query_template, variables, token_pair):
    primary, backup = token_pair
    pull_limit = PULL_REQUEST_LIMIT

    def reduce_or_skip(reason):
        nonlocal pull_limit
        logging.warning(f"{reason} with limit={pull_limit}, reducing...")
        if pull_limit == 1:
            logging.error("Skipping this page after repeated errors (limit=1).")
            return None
        pull_limit = max(1, pull_limit // 2)
        return "retry"

    def make_request(username, token):
        """Try a single request. Returns (result, exhausted, reset_wait)."""
        nonlocal pull_limit
        variables["limit"] = pull_limit
        headers = {"Authorization": f"Bearer {token}"}

        response = None
        try:
            response = requests.post(
                GITHUB_API_URL,
                json={"query": query_template, "variables": variables},
                headers=headers,
            )
        except ChunkedEncodingError:
            if reduce_or_skip("ChunkedEncodingError") is None:
                return None, False, None
            return None, False, None

        if response.status_code in (502, 504):
            if reduce_or_skip(str(response.status_code)) is None:
                return None, False, None
            return None, False, None

        if response.status_code == 403:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                wait = int(retry_after)
                logging.warning(f"Abuse detection for {username}. Sleeping {wait} sec...")
                time.sleep(wait)
                return None, False, None

            remaining = int(response.headers.get("X-RateLimit-Remaining", "1"))
            if remaining == 0:
                reset_time = int(response.headers.get("X-RateLimit-Reset", "0"))
                now = int(time.time())
                wait = max(1, reset_time - now)
                logging.warning(f"Rate limit hit for {username}. Exhausted until reset.")
                return None, True, wait

            raise RuntimeError(f"Forbidden: {response.text}")

        response.raise_for_status()
        result = response.json()

        time.sleep(1)

        if "errors" in result:
            errors = result["errors"]
            logging.error(f"GraphQL errors: {errors}")
            if any(err.get("type") == "RESOURCE_LIMITS_EXCEEDED" for err in errors):
                if reduce_or_skip("RESOURCE_LIMITS_EXCEEDED") is None:
                    return None, False, None
                return None, False, None
        
            if any(err.get("type") == "RATE_LIMITED" for err in errors):
                reset_time = int(response.headers.get("X-RateLimit-Reset", "0"))
                now = int(time.time())
                wait = max(1, reset_time - now)
                logging.warning(f"GraphQL rate limit for {username}. Exhausted until reset.")
                return None, True, wait
            if not result.get("data"):
                raise RuntimeError(errors)

        return result, False, None

    # Main loop
    while True:
        waits = []
        for username, token in (primary, backup):
            result, exhausted, reset_wait = make_request(username, token)
            if result is not None:
                return result
            if exhausted and reset_wait:
                waits.append(reset_wait)

        if waits:
            # Pick the shortest reset time
            wait = min(waits)
            logging.warning(f"Both tokens exhausted. Sleeping {wait} sec before retry...")
            time.sleep(wait)
        else:
            # If no reset info, default backoff
            logging.warning("Transient errors, sleeping 60 sec before retry...")
            time.sleep(60)



def BuildPRQuery():
    return f"""
    query MergedPRsWithPassingRuns($owner: String!, $repo: String!, $limit: Int!, $cursor: String) {{
      repository(owner: $owner, name: $repo) {{
        pullRequests(
          first: $limit,
          after: $cursor,
          orderBy: {{field: CREATED_AT, direction: ASC}},
          states: MERGED
        ) {{
          edges {{
            cursor
            node {{
              number
              title
              mergedAt
              mergeCommit {{
                checkSuites(first: {SUITES_LIMIT}) {{
                  nodes {{
                    checkRuns(first: {RUNS_LIMIT}) {{
                      nodes {{ conclusion }}
                      pageInfo {{ hasNextPage }}
                    }}
                  }}
                  pageInfo {{ hasNextPage }}
                }}
              }}
            }}
          }}
          pageInfo {{ hasNextPage endCursor }}
        }}
      }}
    }}
    """


def ParseCheckRuns(merge_commit):
    passed = 0
    total_runs = 0
    has_more_suites = False
    has_more_runs = False

    if not merge_commit:
        return passed, total_runs, has_more_suites, has_more_runs

    suites = merge_commit.get("checkSuites", {})
    suite_nodes = suites.get("nodes", [])
    has_more_suites = suites.get("pageInfo", {}).get("hasNextPage", False)

    for suite in suite_nodes:
        runs = suite.get("checkRuns", {})
        run_nodes = runs.get("nodes", [])
        total_runs += len(run_nodes)
        passed += sum(1 for run in run_nodes if run.get("conclusion") == "SUCCESS")
        if runs.get("pageInfo", {}).get("hasNextPage"):
            has_more_runs = True

    return passed, total_runs, has_more_suites, has_more_runs


def ExtractPRResults(edges):
    results = []
    for edge in edges:
        cursor = edge["cursor"]
        pr = edge["node"]
        pr_number = pr["number"]
        pr_title = pr["title"]

        passed, total_runs, has_more_suites, has_more_runs = ParseCheckRuns(
            pr.get("mergeCommit")
        )

        results.append({
            "number": pr_number,
            "title": pr_title,
            "mergedAt": pr.get("mergedAt"),
            "passed": passed,
            "total_runs": total_runs,
            "has_more_suites": has_more_suites,
            "has_more_runs": has_more_runs,
            "cursor": cursor,
        })
    return results


def SaveProgress(outfile, existing_df, new_results, final=False):
    new_df = pd.DataFrame(new_results)

    if existing_df is not None and not existing_df.empty:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=["number", "mergedAt"], keep="last")
    else:
        combined_df = new_df

    if final and "cursor" in combined_df.columns:
        combined_df = combined_df.drop(columns=["cursor"])

    if not combined_df.empty:
        combined_df.to_parquet(outfile, index=False)
    return combined_df


def CountPassingTests(owner, repo, outdir, token, backup_token):
    outfile = os.path.join(outdir, f"{owner}_{repo}.parquet")

    existing_df = None
    last_cursor = None
    if os.path.exists(outfile):
        existing_df = pd.read_parquet(outfile)
        if existing_df.empty:
            last_cursor = None
        elif "cursor" in existing_df.columns:
            last_cursor = existing_df["cursor"].iloc[-1]
        else:
            logging.info(f"{outfile} has no cursor column — assuming complete. Skipping.")
            return existing_df  # short-circuit if no cursor column

    has_next_page = True
    end_cursor = last_cursor
    query = BuildPRQuery()

    while has_next_page:
        variables = {
            "owner": owner,
            "repo": repo,
            "limit": PULL_REQUEST_LIMIT,
            "cursor": end_cursor,
        }

        data = RunGraphQLQuery(query, variables, (token, backup_token))

        repo_data = data.get("data", {}).get("repository", {}).get("pullRequests")
        if not repo_data:
            logging.warning(f"No pull request data found for {owner}/{repo}.")
            break

        edges = repo_data.get("edges", [])
        new_results = ExtractPRResults(edges)

        existing_df = SaveProgress(outfile, existing_df, new_results, final=False)
        logging.info(f"Saved progress: {len(existing_df)} PRs to {outfile}")

        page_info = repo_data["pageInfo"]
        has_next_page = page_info["hasNextPage"]
        end_cursor = page_info["endCursor"]

    existing_df = SaveProgress(outfile, existing_df, [], final=True)
    logging.info(f"All PRs processed for {owner}/{repo}. Final saved file without cursor column.")

    return existing_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("worker", type=int, help="Worker ID (1-5)")
    args = parser.parse_args()

    if not (1 <= args.worker <= 5):
        sys.exit("Worker ID must be between 1 and 5")

    INDIR = Path("drive/output/derived/problem_level_data/repo_actions")
    repo_files = [f.stem for f in INDIR.glob("*.parquet")]

    repo_files.sort()  # deterministic order
    n_shards = 5
    shards = [repo_files[i::n_shards] for i in range(n_shards)]

    worker_id = args.worker - 1  # shift to 0-based index
    repos_for_this_worker = shards[worker_id]
    
    random.shuffle(repos_for_this_worker)
    token, backup_token = TOKEN_PAIRS[worker_id]
    outdir = Path("drive/output/scrape/pull_request_test_data")
    os.makedirs(outdir, exist_ok=True)

    for owner_repo in repos_for_this_worker:
        owner, repo = owner_repo.split("_", 1)
        try:
            df = CountPassingTests(owner, repo, outdir, token, backup_token)
        except Exception as e:
            logging.error(f"Failed on {repo}: {e}")
