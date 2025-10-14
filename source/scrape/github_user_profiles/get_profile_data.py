import os
import time
import requests
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

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

def FetchUserProfile(login, user, token, retries=3):
    headers = {"Accept": "application/vnd.github.v3+json", "Authorization": f"token {token}", "User-Agent": user}
    url = f"https://api.github.com/users/{login}"
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 404:
                print(f"User {login} not found")
                return {"actor_login": login, "error": "Not found"}
            if r.status_code == 403 and r.headers.get("X-RateLimit-Remaining") == "0":
                reset_time = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
                time.sleep(max(0, reset_time - int(time.time())) + 1)
                continue
            if r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            print(f"Successfully fetched user {login}")
            return r.json() | {"actor_login": login}
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            print(f"Failed to fetch user {login}: {e}")
            return {"actor_login": login, "error": str(e)}
    print(f"Failed to fetch user {login} after retries")
    return {"actor_login": login, "error": "Failed after retries"}

def Worker(pairs, user, token, outdir, worker_id):
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(FetchUserProfile, login, user, token): (login, actor_id) for login, actor_id in pairs}
        for f in as_completed(futures):
            login, actor_id = futures[f]
            try:
                res = f.result()
                res["actor_id"] = actor_id
                results.append(res)
            except Exception as e:
                results.append({"actor_login": login, "actor_id": actor_id, "error": str(e)})
    df = pd.DataFrame(results)
    outfile = outdir / f"user_profiles_worker{worker_id}.parquet"
    df.to_parquet(outfile, index=False)
    return str(outfile)

def Main():
    input_dirs = [Path("drive/output/derived/problem_level_data/issue"), Path("drive/output/derived/problem_level_data/pr")]
    outdir = Path("drive/output/scrape/github_user_profiles")
    outdir.mkdir(parents=True, exist_ok=True)

    files = [f for d in input_dirs for f in d.glob("*.parquet")]
    dfs = [pd.read_parquet(f, columns=["actor_login", "actor_id"]) for f in files]
    all_df = pd.concat(dfs, ignore_index=True)
    all_df['actor_id'] = pd.to_numeric(all_df['actor_id'])
    all_df = all_df.drop_duplicates(subset=["actor_login", "actor_id"])
    pairs = all_df.dropna(subset=["actor_login", "actor_id"]).to_records(index=False).tolist()

    chunksize = (len(pairs) + len(TOKENS) - 1) // len(TOKENS)
    pair_chunks = [pairs[i:i+chunksize] for i in range(0, len(pairs), chunksize)]

    with ProcessPoolExecutor(max_workers=len(pair_chunks)) as pool:
        futures = [pool.submit(Worker, chunk, user, token, outdir, i) for i, (chunk, (user, token)) in enumerate(zip(pair_chunks, TOKENS))]
        files = [f.result() for f in futures]

    merged = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    outfile = outdir / "detailed_github_user_profiles.parquet"
    merged.to_parquet(outfile, index=False)
    print(f"Saved {len(merged)} profiles to {outfile}")

if __name__ == "__main__":
    Main()
