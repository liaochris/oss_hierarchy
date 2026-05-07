import os
import itertools
import subprocess
import datetime
import shutil
import concurrent.futures
import random
from pathlib import Path

import pandas as pd

from source.lib.python.config_loaders import LoadGlobalSettings
from source.lib.JMSLab.SaveData import SaveData


def Main():
    INDIR = Path('output/scrape/extract_github_data')
    OUTDIR = Path("drive/output/scrape/governance_data")
    LOG_OUTDIR = Path("output/scrape/governance_data")
    LOG_DIR = LOG_OUTDIR / "logs"
    TEMPDIR = Path("drive/temp/governance_data/repos")
    OUTDIR.mkdir(exist_ok=True, parents=True)
    LOG_OUTDIR.mkdir(exist_ok=True, parents=True)
    LOG_DIR.mkdir(exist_ok=True, parents=True)
    TEMPDIR.mkdir(exist_ok=True, parents=True)

    df_repo_list = pd.read_csv(INDIR / 'repo_id_history_final.csv')
    repo_list = df_repo_list.query('latest_repo_name != "ERROR" & is_fork == 0')['repo_name'].unique().tolist()

    random.shuffle(repo_list)
    globals_data = LoadGlobalSettings()
    START_DATE = globals_data['github_start_date']
    n_jobs = globals_data["n_jobs"]

    gh_tokens = LoadTokens()

    letter_groups = {}
    for repo in repo_list:
        first_char = repo[0].lower() if repo[0].isalpha() else "numeric"
        letter_groups.setdefault(first_char, []).append(repo)

    all_outputs = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=n_jobs) as executor:
        futures = [
            executor.submit(ProcessLetterGroup, letter, repos, TEMPDIR, OUTDIR, LOG_DIR, START_DATE, gh_tokens)
            for letter, repos in letter_groups.items()
        ]
        for future in concurrent.futures.as_completed(futures):
            all_outputs.extend(future.result())

    print(f"Finished {len(all_outputs)} repos")


def LoadTokens():
    token_names = (
        ["PRIMARY_GITHUB_TOKEN", "BACKUP_GITHUB_TOKEN"]
        + [f"BACKUP{i}_GITHUB_TOKEN" for i in range(2, 10)]
    )
    tokens = [os.environ[name] for name in token_names if os.environ.get(name)]
    if not tokens:
        raise RuntimeError(
            "No GitHub tokens found. Set PRIMARY_GITHUB_TOKEN and optionally "
            "BACKUP_GITHUB_TOKEN through BACKUP9_GITHUB_TOKEN in your environment."
        )
    print(f"Loaded {len(tokens)} GitHub token(s)")
    return tokens


def ProcessLetterGroup(letter, repos, TEMPDIR, OUTDIR, LOG_DIR, START_DATE, gh_tokens):
    token_cycle = itertools.cycle(gh_tokens)
    results = []
    for repo in repos:
        owner, repo_name = repo.split("/", 1)
        token = next(token_cycle)
        result = RepoOrgGovernanceHistory(owner, repo_name, TEMPDIR, OUTDIR, LOG_DIR, START_DATE, token)
        if result:
            results.append(result)
    return results


def RepoOrgGovernanceHistory(owner, repo, TEMPDIR, OUTDIR, LOG_DIR, START_DATE, GH_TOKEN):
    outfile = OUTDIR / f"{owner}___{repo}.parquet"
    logfile = LOG_DIR / f"{outfile.stem}.log"

    if outfile.exists():
        print(f"Skipping {owner}/{repo}, output already exists: {outfile}")
        return str(outfile)

    rows = []

    repo_path = CloneRepo(owner, repo, TEMPDIR, GH_TOKEN)
    if repo_path:
        commits = GetCommitsTouchingGovernance(repo_path, START_DATE)
        for sha, date in commits:
            files = ExtractGovernanceFiles(repo_path, sha)
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
        shutil.rmtree(repo_path, ignore_errors=True)

    org_path = CloneRepo(owner, ".github", TEMPDIR, GH_TOKEN)
    if org_path:
        commits = GetCommitsTouchingGovernance(org_path, START_DATE)
        for sha, date in commits:
            files = ExtractGovernanceFiles(org_path, sha)
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
        shutil.rmtree(org_path, ignore_errors=True)

    if rows:
        df = pd.DataFrame(rows)
        SaveData(df, ["repo", "source", "date", "commit", "file_path"], outfile, logfile, append=False)
        print(f"Exported {len(df)} rows (repo + org defaults) to {outfile}")
        return str(outfile)
    else:
        print(f"No governance files found in {owner}/{repo} (repo or org defaults)")
        return None


def CloneRepo(owner, repo, TEMPDIR, GH_TOKEN):
    repo_path = Path(TEMPDIR) / owner / repo
    if repo_path.exists():
        return None
    repo_path.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://{GH_TOKEN}@github.com/{owner}/{repo}.git"
    try:
        subprocess.run(["git", "clone", url, str(repo_path)], check=True)
    except subprocess.CalledProcessError:
        print(f"Could not clone {owner}/{repo}")
        return None
    return repo_path


def GetCommitsTouchingGovernance(repo_path, START_DATE):
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
    try:
        log_output = RunGit(
            repo_path,
            ["log", "--since", START_DATE, "--pretty=format:%H %cI", "--", *patterns]
        )
    except RuntimeError as e:
        if "does not have any commits yet" in str(e):
            return []
        raise
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


def RunGit(repo_path, args):
    result = subprocess.run(
        ["git", "-C", str(repo_path)] + args,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="replace").strip())
    return result.stdout.decode("utf-8", errors="replace").strip()


if __name__ == "__main__":
    Main()
