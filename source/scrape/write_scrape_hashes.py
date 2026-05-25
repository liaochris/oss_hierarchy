from pathlib import Path
from source.lib.python.filesystem_utils import WriteDirectoryHash

HASH_DIR = Path("output/derived/hashes")

DIRS = {
    "scrape_issue":       Path("drive/output/scrape/extract_github_data/repo_level_data/issue"),
    "scrape_pr":          Path("drive/output/scrape/extract_github_data/repo_level_data/pr"),
    "scrape_linked_prs":  Path("drive/output/scrape/link_issue_pull_request/linked_pull_request_to_issue"),
}


def Main():
    for name, directory in DIRS.items():
        WriteDirectoryHash(directory, HASH_DIR / f"{name}.txt")


if __name__ == "__main__":
    Main()
