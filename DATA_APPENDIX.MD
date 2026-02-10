# Data Appendix: Action-Level Data Construction

This document describes the full data pipeline from raw data collection through transformation into action-level datasets with cleaned text and sentiment scores.

---

## Data Collection Pipeline (`source/scrape/`)

The scraping pipeline is orchestrated by `run_scrape.sh` and executes in the following order:

### Step 1.1: Identify Popular Packages (`source/scrape/pypi_downloads/`)

| Script | Output | Description |
|--------|--------|-------------|
| `query_monthly_pip_downloads.py` | `drive/output/scrape/pypi_downloads/pypi_monthly_downloads.parquet` | BigQuery query for monthly PyPI download counts (2018-2025) |
| `get_popular_packages.py` | `output/scrape/pypi_downloads/popular_python_packages.csv` | Filter to packages with ≥10K monthly downloads |

I begin by identifying Python packages with meaningful adoption. Using Google BigQuery's public PyPI dataset, I collect monthly download counts for all packages between 2018 and 2025. I then filter to packages that achieved at least 10,000 downloads in any single month during this period, ensuring I focus on projects with a meaningful user base.

### Step 1.2: Map Packages to GitHub Repos (`source/scrape/pypi_site_info/`)

| Script | Output | Description |
|--------|--------|-------------|
| `get_library_json.sh` | `drive/output/scrape/pypi_site_info/*.json` | Fetch each package's PyPI API JSON metadata |
| `link_pypi_github.py` | `output/scrape/pypi_site_info/linked_pypi_github.csv` | Extract GitHub repository URLs from JSON |

Each Python PyPI package page links to its source repository. I fetch the JSON metadata from PyPI's public API for each popular package and extract the GitHub repository URL, which provides me with the GitHub repository name. Packages without a linked GitHub repository or with ambiguous links are listed as `Unavailable` in the output excluded.

### Step 1.3: Build Canonical Repo List (`source/scrape/extract_github_data/`)

| Script | Output | Description |
|--------|--------|-------------|
| `query_all_repo_identities.py` | `output/scrape/extract_github_data/repo_id_history.csv` | Map repo names ↔ IDs across time via BigQuery (handles renames) |
| `group_repo_identities.py` | `output/scrape/extract_github_data/repo_id_history_final.csv` | Web scrape to detect forks and resolve to current names |

GitHub repositories can be renamed over time, and many are forks of other projects. I trace the full history of repository names using an iterative process: starting with the initial repo names from PyPI, I query GitHub Archive to find all repository IDs associated with those names, then query again to find all names associated with those IDs, repeating until no new names or IDs are discovered. This captures renamed repositories because the same ID appears with different names over time.

I then scrape each repository's GitHub page to (1) detect whether it is a fork by searching the HTML for "forked from" text, and (2) obtain the current canonical name by following HTTP redirects to the final URL. Forks are excluded, and all historical names are grouped under the current canonical name for consistent downstream processing.

### Step 1.4: Extract GitHub Events (`source/scrape/extract_github_data/`)

| Script | Output | Description |
|--------|--------|-------------|
| `query_github_data.py` | `drive/output/scrape/extract_github_data/event_level_data/*.csv` | BigQuery extraction of 5 event types from GitHub Archive |
| `transform_event_to_repo_level.py` | `drive/output/scrape/extract_github_data/repo_level_data/{issue,pr}/*.parquet` | Transform to one parquet per repo, renaming to current repo name |

**Event types collected from GitHub Archive**:
- `IssuesEvent` - Issue opened/closed/reopened
- `IssueCommentEvent` - Comments on issues (and PRs, due to GitHub quirk)
- `PullRequestEvent` - PR opened/closed/merged
- `PullRequestReviewEvent` - Review approved/changes requested
- `PullRequestReviewCommentEvent` - Inline code review comments

Using all historical repository names from Step 1.3 (not just current names), I query GitHub Archive via BigQuery to extract all issue and pull request events. This ensures I capture events that occurred when a repository had an older name. The transformation step then maps each event's historical repository name to its current canonical name (using the lookup table from Step 1.3) and organizes the data into one parquet file per repository.

**Duplicate handling in repo-level transformation**: When combining events into repo-level parquet files, I apply event-type-specific deduplication using the key columns `[issue_number|pr_number, type, created_at, actor_id]`:

- **Missing keys**: Rows with null values in any key column are dropped and logged to `{category}/flagged/missing_keys.csv`
- **IssueCommentEvent duplicates**: When multiple rows share the same keys, I keep only the row with the highest `issue_comment_id` (representing the most recent version)
- **Other event types** (IssuesEvent, PullRequestEvent, PullRequestReviewEvent, PullRequestReviewCommentEvent): I first drop rows where ALL columns are identical (true duplicates). For remaining duplicates with different values in non-key columns, I perturb the `created_at` timestamp by adding 1ms, 2ms, etc. to preserve all distinct records while maintaining key uniqueness
- **Perturbation rationale**: GitHub timestamps have second-level granularity (.000 milliseconds). Adding milliseconds ensures perturbed timestamps never collide with original timestamps

Dropped and perturbed rows are logged to `{category}/flagged/duplicates.csv` for audit purposes.

### Step 1.5: Link Issues to Pull Requests (`source/scrape/link_issue_pull_request/`)

| Script | Output | Description |
|--------|--------|-------------|
| `link_using_pull_request.py` | `drive/output/scrape/link_issue_pull_request/linked_pull_request_to_issue/*.parquet` | Scrape PR pages for linked issues and full PR text |
| `link_using_issue.py` | `drive/output/scrape/link_issue_pull_request/linked_issue_to_pull_request/*.parquet` | Scrape issue pages for linked PRs and full timeline |

GitHub Archive does not include the links between pull requests and issues, nor does it include the full text of PR descriptions. I fill these gaps with two complementary web scraping scripts:

- **For pull requests**: I scrape each PR page to extract (1) issue links from "Successfully merging this pull request may close these issues" references, and (2) the full PR title and body text.
- **For issues**: I scrape each issue page using Selenium (to expand paginated timelines) to extract (1) linked pull requests, and (2) the complete issue timeline including all comments and cross-references.

### Step 1.6: Governance Data (`source/scrape/governance_data/`)

| Script | Output | Description |
|--------|--------|-------------|
| `collect_governance_data.py` | `drive/output/scrape/governance_data/*.parquet` | Clone repos, query git log for CODEOWNERS, CONTRIBUTING.md, etc. |

To track how projects formalize their development processes, I clone each repository and extract the git history for governance-related files: contribution guidelines, codes of conduct, security policies, issue templates, and code ownership files.

### Step 1.7: Version-Level Downloads (`source/scrape/pypi_version_downloads/`)

| Script | Output | Description |
|--------|--------|-------------|
| `query_daily_pip_version_downloads.py` | `drive/output/scrape/pypi_version_downloads/pypi_version_downloads.parquet` | Monthly downloads by package version |

For analyses requiring version-specific adoption metrics, I separately collect download counts broken down by package version and month.

---

## Data Flow Diagram

```
PyPI BigQuery ──► output/scrape/pypi_downloads/popular_python_packages.csv
                          │
                          ▼
PyPI API JSON ──► output/scrape/pypi_site_info/linked_pypi_github.csv
                          │
                          ▼
GitHub Archive ──► output/scrape/extract_github_data/repo_id_history_final.csv
   BigQuery               │
                          ▼
          drive/output/scrape/extract_github_data/event_level_data/*.csv
                          │
                          ▼
          drive/output/scrape/extract_github_data/repo_level_data/issue/*.parquet ──┐
          drive/output/scrape/extract_github_data/repo_level_data/pr/*.parquet  ────┤
                          │                                                         │
                          ▼                                                         │
Web Scraping ──► drive/output/scrape/link_issue_pull_request/linked_pull_request_to_issue/*.parquet
                          │                                                         │
                          └─────────────────────────────────────────────────────────┘
                                     │
                                     ▼
                          ┌─────────────────────────┐
                          │   DERIVED PIPELINE      │
                          │   (see steps below)     │
                          └─────────────────────────┘
```

---

## Step 2: Derived Pipeline (`source/derived/problem_level_data/`)

This step transforms the raw scraped data into analysis-ready action-level datasets. I create a unified timeline of all activity within each repository, treating issues and pull requests as "threads" with a common schema. This allows analysis of collaboration patterns regardless of whether work happened in an issue discussion or a code review.

### Input Data Sources (from scraping)

- **Issue Events**: `drive/output/scrape/extract_github_data/repo_level_data/issue/`
- **Pull Request Events**: `drive/output/scrape/extract_github_data/repo_level_data/pr/`
- **Linked PR-Issue Data**: `drive/output/scrape/link_issue_pull_request/linked_pull_request_to_issue/`

---

### Step 2.1: Combine Action Data (`source/derived/problem_level_data/combine_action_data.py`)

I combine issue and pull request events into a unified action-level dataset with consistent identifiers and forward-filled metadata.

**Unifying issues and pull requests**: I rename issue and PR identifiers to a common schema (`thread_number` for both issue numbers and PR numbers) so that downstream analysis can treat them uniformly as "threads" of discussion.

**Extracting text content**: For issues, I use the issue body only for "opened" events; subsequent events use the comment body. For pull requests, I merge in the full PR description text from the web-scraped linked data (since GitHub Archive truncates PR bodies). PR review events use their respective review or comment body fields.

**Handling PR review sub-threads**: Pull request reviews can have multiple comment threads on different parts of the code. I create a `discussion_id` to distinguish these: review comments are grouped by file path and line position; reviews without inline comments are ordered chronologically within the thread.

**Correcting GitHub's event labeling**: GitHub Archive labels comments on issue and pull requests as `IssueCommentEvent` rather than a PR-specific event type. I detect these cases (by checking if the thread number corresponds to a PR) and relabel them as "pull request comment".

**Data quality filtering**: I remove threads that lack an opener event (indicating incomplete data). I also deduplicate cases where close/reopen events repeat the opening text verbatim.

**Creating unique identifiers**: Each action receives a unique `action_id`. For PR review events (which have sub-threads), the format is `{thread_number}___{discussion_id}___{sequence_number}`. For regular issue and PR events, the format is simply `{thread_number}___{sequence_number}`.

**Output**:
- **`drive/output/derived/problem_level_data/repo_actions/`**: One parquet file per repository
- **`output/derived/problem_level_data/dropped_stats.csv`**: Statistics on dropped threads
- **`output/derived/problem_level_data/repo_component_tracking.csv`**: Tracking which repos have which data components

---

### Step 2.2: Clean Text and Add Sentiment (`source/derived/problem_level_data/clean_text.py`)

Developer discussions contain substantial non-natural-language content that must be removed before text analysis. I apply regex-based cleaning before computing text sentiment scores.

**Text cleaning**: I remove content that would distort text analysis:
- Code blocks (fenced and inline) and indented code/log lines
- Log prefixes (INFO, ERROR, WARN, DEBUG, TRACE)
- Quoted text (lines starting with >)
- Email headers (From:, Sent:, To:, etc.)
- HTML tags and entities
- Markdown formatting characters

**Token replacement**: I replace certain content with standardized tokens to preserve semantic meaning while normalizing the text:
- URLs and markdown links → [URL]
- Email addresses → [EMAIL]
- @mentions → [USER]
- Issue/PR references (#123) → [ISSUE]
- File paths → [PATH]
- Version numbers → [VERSION]
- Commit hashes → [HASH]
- Emoji codes (:smile:) → [EMOJI:smile]

**Sentiment analysis**: I compute sentiment scores using the VADER lexicon, which is calibrated for social media text and handles the informal tone common in GitHub discussions. I split text into sentences and average scores across sentences to produce `pos`, `neg`, `neu`, and `compound` scores. Empty or unparseable text results in NA values.

**Output**:
- **`drive/output/derived/problem_level_data/cleaned_text/`**: One parquet file per repository
- Columns include: action_id, thread_number, time_period, actor_id, type, text, cleaned_text, labels, assignees, pos, neg, neu, compound

---

## Key Identifiers

| Column | Description |
|--------|-------------|
| `action_id` | Unique identifier: `{thread_number}___{seq}` for most events, or `{thread_number}___{discussion_id}___{seq}` for PR review events |
| `thread_number` | Issue or PR number (unified term) |
| `discussion_id` | Sub-thread identifier (only for PR review events; based on file path/position for comments, chronological order for reviews) |
| `opener_id` | Actor ID of the user who opened the thread |

---

## File Naming Conventions

- **Safe repo names**: Use `MakeRepoNameSafe()` from `source.lib.helpers` to convert repo names to safe filenames
  - Converts `/` to `___` (triple underscore)
  - Example: `pandas-dev/pandas` → `pandas-dev___pandas.parquet`
- **All repo-level parquets** use this naming convention consistently across scrape and derived directories

---

## Downstream Usage Notes

- **Sentiment columns may contain NAs** for empty or unparseable text. When aggregating, pandas `.mean()` ignores NAs by default, but explicit `.dropna()` may be needed for other operations.
- **List columns** (assignees, labels, requested_reviewers) are stored as Python lists in parquet files.
- **Action types** are human-readable strings (e.g., "issue opened", "pull request review approved").
