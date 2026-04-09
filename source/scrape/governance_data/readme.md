### Overview
This folder contains code to scrape github repositories to check whether they had particular governance files.

### Source
Chris Liao queried this data using `git` on INSERT DATE.

### When/where obtained & original form of files
Scraped by cloning repositories using `git clone` and then querying the logs.

### Description
- `code/collect_governance_data.py` clones each repo and its org-level `.github` repo, walks commit history to extract governance files (CODEOWNERS, CONTRIBUTING, SECURITY, CODE_OF_CONDUCT, issue/PR templates) at each change, and saves results as parquet files
- `docs/Github_TOU.md` contains the terms of usage for GitHub.

### GitHub Token Setup

`collect_governance_data.py` clones repos directly via HTTPS and requires GitHub personal access tokens (PATs) to avoid rate limits and credential prompts during long runs.

Set the following in your shell profile (`~/.bash_profile` or `~/.zshrc`):

```bash
export PRIMARY_GITHUB_TOKEN="ghp_..."
# Optional backup tokens, rotated across repos to spread rate limit usage:
export BACKUP_GITHUB_TOKEN="ghp_..."
export BACKUP2_GITHUB_TOKEN="ghp_..."
# ... up to BACKUP9_GITHUB_TOKEN
```

Only `PRIMARY_GITHUB_TOKEN` is required. Any backup tokens present are automatically picked up and rotated. Tokens need at least `public_repo` scope.

### Terms of Use
Per the docs, don't use the data to spam people.

### Run Order
1. Run `source/scrape/extract_governance_data/code/collect_governance_data.py`
