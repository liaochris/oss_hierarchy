import subprocess
import datetime
from pathlib import Path
import pandas as pd
import shutil
import concurrent.futures
import random
from source.lib.helpers import LoadGlobals

def RunGit(repo_path, args):
    result = subprocess.run(
        ["git", "-C", str(repo_path)] + args,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def CloneRepo(owner, repo, TEMPDIR):
    repo_path = Path(TEMPDIR) / owner / repo
    if repo_path.exists():
        return None
    repo_path.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://github.com/{owner}/{repo}.git"
    try:
        subprocess.run(["git", "clone", url, str(repo_path)], check=True)
    except subprocess.CalledProcessError:
        print(f"⚠️ Could not clone {owner}/{repo}")
        return None
    return repo_path


def GetCommitsTouchingGovernance(repo_path, START_DATE):
    cutoff = START_DATE
    patterns = [
        "CODEOWNERS",
        "CONTRIBUTING.*",
        "SECURITY.*",
        "CODE_OF_CONDUCT.*",
        ".github/CODEOWNERS",
        ".github/CONTRIBUTING.*",
        ".github/SECURITY.*",
        ".github/CODE_OF_CONDUCT.*",
        ".github/ISSUE_TEMPLATE*",
        ".github/PULL_REQUEST_TEMPLATE*",
        "docs/CODEOWNERS",
        "docs/CONTRIBUTING.*",
    ]
    log_output = RunGit(
        repo_path,
        ["log", "--since", cutoff, "--pretty=format:%H %cI", "--", *patterns]
    )
    commits = []
    for line in log_output.splitlines():
        sha, date = line.split(" ", 1)
        commits.append((sha, datetime.datetime.fromisoformat(date)))
    return list(reversed(commits))


def ExtractGovernanceFiles(repo_path, sha):
    files = {}
    governance_paths = [
        "CODEOWNERS",
        "CONTRIBUTING.md", "CONTRIBUTING.rst", "CONTRIBUTING.txt",
        "SECURITY.md", "CODE_OF_CONDUCT.md",
        ".github/CODEOWNERS",
        ".github/CONTRIBUTING.md", ".github/SECURITY.md", ".github/CODE_OF_CONDUCT.md",
        ".github/ISSUE_TEMPLATE.md", ".github/PULL_REQUEST_TEMPLATE.md",
        "docs/CODEOWNERS", "docs/CONTRIBUTING.md",
    ]
    template_dirs = [".github/ISSUE_TEMPLATE", ".github/PULL_REQUEST_TEMPLATE"]

    for path in governance_paths:
        try:
            text = RunGit(repo_path, ["show", f"{sha}:{path}"])
            if text:
                files[path] = text
        except RuntimeError:
            continue

    for d in template_dirs:
        try:
            ls = RunGit(repo_path, ["ls-tree", "-r", "--name-only", sha, d])
            for f in ls.splitlines():
                try:
                    text = RunGit(repo_path, ["show", f"{sha}:{f}"])
                    if text:
                        files[f] = text
                except RuntimeError:
                    continue
        except RuntimeError:
            continue

    return files


def ClassifyGovernanceFile(path):
    p = path.lower()
    if "codeowners" in p:
        return "codeowners"
    if "contributing" in p:
        return "contributing"
    if "security" in p:
        return "security"
    if "code_of_conduct" in p:
        return "code_of_conduct"
    if "issue_template" in p:
        return "issue_template"
    if "pull_request_template" in p:
        return "pr_template"
    return "other"


def RepoOrgGovernanceHistory(owner, repo, TEMPDIR, OUTDIR, START_DATE):
    OUTDIR = Path(OUTDIR)
    OUTDIR.mkdir(parents=True, exist_ok=True)
    outfile = OUTDIR / f"{owner}_{repo}.parquet"

    if outfile.exists():
        print(f"⏩ Skipping {owner}/{repo}, output already exists: {outfile}")
        return str(outfile)

    rows = []

    repo_path = CloneRepo(owner, repo, TEMPDIR)
    if repo_path:
        commits = GetCommitsTouchingGovernance(repo_path, START_DATE)
        for sha, date in commits:
            files = ExtractGovernanceFiles(repo_path, sha)
            if files:
                for fpath, text in files.items():
                    rows.append({
                        "repo": f"{owner}/{repo}",
                        "source": "repo",
                        "commit": sha,
                        "date": date.isoformat(),
                        "file_path": fpath,
                        "file_type": ClassifyGovernanceFile(fpath),
                        "text": text
                    })
        shutil.rmtree(repo_path)
        repo_path.mkdir(parents=True, exist_ok=True)

    org_path = CloneRepo(owner, ".github", TEMPDIR)
    if org_path:
        commits = GetCommitsTouchingGovernance(org_path, START_DATE)
        for sha, date in commits:
            files = ExtractGovernanceFiles(org_path, sha)
            if files:
                for fpath, text in files.items():
                    rows.append({
                        "repo": f"{owner}/{repo}",
                        "source": "org_default",
                        "commit": sha,
                        "date": date.isoformat(),
                        "file_path": fpath,
                        "file_type": ClassifyGovernanceFile(fpath),
                        "text": text
                    })
        shutil.rmtree(org_path)
        org_path.mkdir(parents=True, exist_ok=True)

    if rows:
        df = pd.DataFrame(rows)
        df.to_parquet(outfile, index=False)
        print(f"✅ Exported {len(df)} rows (repo + org defaults) to {outfile}")
        return str(outfile)
    else:
        print(f"⚠️ No governance files found in {owner}/{repo} (repo or org defaults)")
        return None


def Worker(repo, TEMPDIR, OUTDIR, START_DATE):
    owner, repo_name = repo.split("/", 1)
    return RepoOrgGovernanceHistory(owner, repo_name, TEMPDIR, OUTDIR, START_DATE)

def Main():
    INDIR = Path('output/scrape/extract_github_data')
    OUTDIR = Path("drive/output/scrape/github_file_data")
    TEMPDIR = Path("drive/temp/github_file_data/repos")

    df_repo_list = pd.read_csv(INDIR / 'repo_id_history_final.csv')
    repo_list = df_repo_list.query('latest_repo_name != "ERROR"')['repo_name'].unique().tolist()
    random.shuffle(repo_list)
    START_DATE = LoadGlobals("source/lib/globals.json")['github_start_date']
    all_outputs = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(Worker, repo, TEMPDIR, OUTDIR, START_DATE) for repo in repo_list]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                all_outputs.append(result)

    print(f"✅ Finished {len(all_outputs)} repos")


if __name__ == "__main__":
    Main()
