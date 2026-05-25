# Data Appendix: Data Construction Pipeline

This document describes the full data pipeline from raw data collection through the construction of the analysis panel used in estimation.

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

- **Missing keys**: Rows with null values in any key column are dropped and logged to `{category}/flagged/{letter,numeric}_missing_keys.csv`. Category is one of `issue`,`pr`. 
- **IssueCommentEvent duplicates**: When multiple rows share the same keys, I keep only the row with the highest `issue_comment_id` (representing the most recent version)
- **Other event types** (IssuesEvent, PullRequestEvent, PullRequestReviewEvent, PullRequestReviewCommentEvent): I first drop rows where ALL columns are identical (true duplicates). These can arise from duplicate events in the raw GitHub Archive data (e.g., the same event recorded under multiple historical names for a renamed repository). For remaining duplicates with different values in non-key columns, I perturb the `created_at` timestamp by adding 1ms, 2ms, etc. to preserve all distinct records while maintaining key uniqueness. Since GitHub timestamps have second-level granularity (.000 milliseconds), different evens can have the same key, so adding milliseconds ensures them to be separated. 
- **PullRequestEvent opened duplicates**: After the above step, I deduplicate `PullRequestEvent` rows with `pr_action=opened` on `(pr_number, actor_id)`, keeping the earliest `created_at`. This handles the case where a repo transfer/rename causes the same PR open event to appear twice under different repo IDs with slightly different timestamps, bypassing the key-based deduplication above.

Dropped and perturbed rows are logged to `{category}/flagged/{letter,numeric}_duplicates.csv` for audit purposes.

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

## Step 2: Derived Pipeline (`source/derived/action_data/`)

This step transforms the raw scraped data into analysis-ready action-level datasets. I create a unified timeline of all activity within each repository, treating issues and pull requests as "threads" with a common schema. This allows analysis of collaboration patterns regardless of whether work happened in an issue discussion or a code review.

### Input Data Sources (from scraping)

- **Issue Events**: `drive/output/scrape/extract_github_data/repo_level_data/issue/`
- **Pull Request Events**: `drive/output/scrape/extract_github_data/repo_level_data/pr/`
- **Linked PR-Issue Data**: `drive/output/scrape/link_issue_pull_request/linked_pull_request_to_issue/`

---

### Step 2.1: Combine Action Data (`source/derived/action_data/combine_action_data.py`)

I combine issue and pull request events into a unified action-level dataset with consistent identifiers and forward-filled metadata.

**Unifying issues and pull requests**: I rename issue and PR identifiers to a common schema (`thread_number` for both issue numbers and PR numbers) so that downstream analysis can treat them uniformly as "threads" of discussion.

**Extracting text content**: For issues, I use the issue body only for "opened" events; subsequent events use the comment body. For pull requests, I merge in the full PR description text from the web-scraped linked data (since GitHub Archive truncates PR bodies). PR review events use their respective review or comment body fields.

**Handling PR review sub-threads**: Pull request reviews can have multiple comment threads on different parts of the code. I create a `discussion_id` to distinguish these: review comments are grouped by file path and line position; reviews without inline comments are ordered chronologically within the thread.

**Correcting GitHub's event labeling**: GitHub Archive labels comments on issue and pull requests as `IssueCommentEvent` rather than a PR-specific event type. I detect these cases (by checking whether the thread has no recorded issue-opened event) and relabel them as "pull request comment".

**Data quality filtering**: I remove threads that lack an opener event (indicating incomplete data). I also deduplicate cases where close/reopen events repeat the opening text verbatim.

**Creating unique identifiers**: Each action receives a unique `action_id`. For PR review events (which have sub-threads), the format is `{thread_number}___{discussion_id}___{sequence_number}`. For regular issue and PR events, the format is simply `{thread_number}___{sequence_number}`.

**Output**:
- **`drive/output/derived/action_data/repo_actions/`**: One parquet file per repository
- **`output/derived/action_data/dropped_stats.csv`**: Statistics on dropped threads
- **`output/derived/action_data/repo_component_tracking.csv`**: Tracking which repos have which data components

---

### Step 2.2: Clean Text and Add Sentiment (`source/derived/action_data/clean_text.py`)

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
- **`drive/output/derived/action_data/cleaned_text/`**: One parquet file per repository
- Columns include: action_id, thread_number, time_period, actor_id, type, text, cleaned_text, labels, assignees, pos, neg, neu, compound

---

## Step 3: Graph Structure Pipeline (`source/derived/graph_structure/`)

This step constructs collaboration networks from the action-level data and identifies the most central ("important") members in each repository over time.

### Step 3.1: Build Interaction Graphs (`source/derived/graph_structure/create_graphs.py`)

I construct an undirected weighted interaction network for each repository using the action-level data. Each member is a node; edge weights capture interaction quantity between two distinct members.

An interaction occurs when one member responds to another. Within each discussion thread, each post is assigned a receiver: the most recent prior post by a *different* member. Consecutive posts by the same member are skipped over when identifying the receiver, so uninterrupted self-posting does not dilute who a member is actually responding to. For pull request review threads where a post has no prior post by a different member, the thread opener is used as the default receiver. Threads with only one participant are excluded entirely.

**Actor ID standardization**: Actor IDs are cast to integers. Rows where the sender ID is NaN are dropped entirely. Edges where the receiver ID is NaN — including string `"nan"` values — are excluded.

**Graph construction**: Each graph covers all interactions whose post occurred within a 6-month period (regardless of when the thread was opened). Edges are undirected and weighted by the total number of interactions between the two members in that period.

**Outputs**:
- **`drive/output/derived/graph_structure/graphs/{YYYYMM}/{repo}.gexf`**: One GEXF-format graph per repository per 6-month period
- **`drive/output/derived/graph_structure/interactions/{repo}.parquet`**: Interaction-level edge records (sender, receiver, discussion context, time period); one row per interaction, ordered by discussion then time within discussion
- **`output/derived/graph_structure/exported_graphs_log.csv`**: Summary of which repos had graphs exported

---

### Step 3.2: Calculate Centrality Metrics (`source/derived/graph_structure/calculate_degree.py`)

For each repository and each 6-month time period, I compute three centrality metrics for every actor in the collaboration network.

**Metrics computed**:
- **`degree`**: Raw count of distinct collaborators (node degree)
- **`degree_centrality`**: Degree normalized by the maximum possible degree in the graph
- **`weighted_degree_centrality`**: Sum of edge weights (total interactions), normalized by the maximum observed strength in the graph
- **`betweenness_centrality`**: Fraction of shortest paths (weighted) passing through the actor, normalized

For each metric, I also compute a **z-score** (`_z`) and a **dense rank** (`_rank`) based on the z-score (descending; higher centrality → lower rank number). Ties receive the same rank — e.g., two actors both ranked 1st are followed by rank 2, not rank 3.


**Output**:
- **`drive/output/derived/graph_structure/graph_degrees/{repo}.parquet`**: One row per member per 6-month period

---

### Step 3.3: Identify Important Members (`source/derived/graph_structure/calculate_importance.py`)

I identify "important" members — those who are persistently among the most central actors in the collaboration network across consecutive time periods. Two definitions of importance are used:

Both definitions use degree centrality. An actor qualifies as important in a given period if they meet the threshold, and persistently important if they qualify for 3 or more consecutive 6-month periods.

| Definition | Rule |
|------------|------|
| Z-score threshold | Degree centrality z-score above 2.0 |
| Top-3 | Top 3 actors by degree centrality |

**Algorithm**:
1. For each 6-month period, identify members who exceed the centrality threshold
2. A member is considered persistently important if they meet the threshold for 3 or more consecutive periods ending at the current period
3. Dropouts are persistently important members who never appear in any subsequent period
4. **Action-data verification**: Each dropout candidate is cross-checked against raw action data, and any candidate with actions of any type post-dropout period is removed from the dropout list. This ensures departures are total and complete.

**Output**:
- **`output/derived/graph_structure/important_members/{repo}.csv`**: One row per time period per importance definition, containing lists of important, persistent, and dropout member IDs
- **`output/derived/graph_structure/importance_summary.csv`**: Aggregate summary of dropout counts per repo per definition

---

## Step 4: Organizational Practices Pipeline (`source/derived/org_outcomes_practices/`)

Five organizational practice categories are measured using the action-level data and graph structure from Steps 2–3, then combined into a single panel. All rolling measures use data from a **30-month rolling window** (5 rolling periods × 6-month period length). This means that they are calculated using data pooled from 30 months. 

**Primary vs. extension variables**: Each practice script distinguishes *primary* practices — the core measures used in the main analysis — from *extension* practices that may be used eventually.

**Activity types**: Nine activity types appear throughout the pipeline — *issue opened*, *issue comment*, *issue closed*; *pull request opened*, *pull request comment*, *pull request review comment*, *pull request review*, *pull request merged*, *pull request closed*. Where PR review sub-type detail is not needed, a `type_broad` field collapses all review sub-types (approved, changes requested, etc.) into a single `pull request review` category, and a derived `discussion comment` grouping combines all comment types. This collapsing is used in collaboration, shared knowledge, and discussion quality; investment in new talent and organizational routines filter on the raw `type` field directly.

---

### Step 4.1: Outcome Variables (`source/derived/org_outcomes_practices/org_outcomes.py`)

**Activity**: Counts of issues opened/closed and pull requests opened/closed/merged per 6-month period.

**Downloads**: Each repository is linked to its PyPI package name(s) via the mapping from Step 1.2. Monthly download counts (Step 1.1) are summed across all associated packages per period, restricted to periods from July 2018 onward.

**Releases**: Using version-level download data (Step 1.7), each version's first observed download date is used to assign a release period. Releases are classified as: *major* (X.0 or X.0.0), *minor* (X.Y or X.Y.0, Y ≠ 0), *patch* (X.Y.Z, Z ≠ 0), or *other* (non-numeric components). New releases of each type are counted per period.

**Primary release outcomes**: `overall_new_release_count` (all new versions), `major_minor_release_count` (major and minor versions only).

**Extension release outcomes**: `major_new_release_count`, `minor_new_release_count`, `patch_new_release_count`, `other_new_release_count`, `major_minor_patch_release_count`.

**Output**: `drive/output/derived/org_outcomes_practices/org_outcomes/{repo}.parquet`

---

### Step 4.2: Collaboration (`source/derived/org_outcomes_practices/collaboration.py`)

I measure how broadly project members share responsibility for work — whether collaboration is distributed across many members or dominated by a few.

**Primary**

**Member participation per thread** (`avg_members_per_problem`, `pct_members_multiple`): For each issue/PR thread, I count distinct non-bot members. `avg_members_per_problem` is the mean across threads; `pct_members_multiple` is the fraction of threads with more than one non-bot member.

**Project-level HHI** (`proj_hhi_*`): For each activity type $a$, let $s_i$ be member $i$'s share of all events of type $a$ within the rolling window. The project HHI is $\text{HHI}_a = \sum_i s_i^2$, averaged across time periods in the window weighted by total activity volume. Computed for each of the nine activity types plus the composite `discussion_comment` grouping.

**Problem-level HHI** (`proj_prob_hhi_*`): Within each thread $p$, let $s_{ip}$ be member $i$'s share of events. The thread HHI is $\text{HHI}_p = \sum_i s_{ip}^2$. The project statistic is the thread-size-weighted mean: $\sum_p n_p \cdot \text{HHI}_p / \sum_p n_p$, where $n_p$ is the total number of discussion events in thread $p$.

**Output**: `drive/output/derived/org_outcomes_practices/repo_collaboration/{member_subset}/rolling5/{repo}.parquet`

---

### Step 4.3: Shared Knowledge (`source/derived/org_outcomes_practices/shared_knowledge.py`)

I measure how broadly team members engage across different types of development work — whether activity is spread across issue-tracking and code contributions, or concentrated in a single domain.

**Primary**

**Issue/PR mix** (`share_issue_only`, `share_pr_only`, `share_issue_and_pr`): Among all non-bot members in the rolling window, I compute the share who participated exclusively in issues, exclusively in pull requests, or both.

**Activity type diversity** (`avg_unique_types`): For each non-bot member, I count the number of distinct activity types (from the nine listed in Step 4) engaged in during the rolling window. `avg_unique_types` is the mean across members.

**Output**: `drive/output/derived/org_outcomes_practices/repo_shared_knowledge/{member_subset}/rolling5/{repo}.parquet`

---

### Step 4.4: Discussion Quality (`source/derived/org_outcomes_practices/discussion_quality.py`)

I measure member responsiveness and tone of project discussions, using the interaction graph (Step 3.1) and VADER sentiment scores (Step 2.2).

**Primary**

**Response rate** (`response_rate`): Fraction of non-bot posts — excluding PR review events, which do not receive direct replies — that received a reply from a different member within the same thread.

**Response time** (`mean_days_to_respond`): Among posts that received a reply, the mean elapsed days between the end of the sender's message block and the start of the reply. `mean_days_to_respond_to_last_comment` restricts to the final post in each thread.

**Sentiment** (`pos_sentiment_avg`, `neg_sentiment_avg`, `ov_sentiment_avg`): Mean VADER sentence-level scores across all non-bot posts in the period.

**Extension**

By-type variants of response rate, response time, and sentiment, decomposed across the nine activity types.

**Network clustering** (`clustering_coefficient`): Weighted average clustering coefficient of the rolling cumulative collaboration graph (Step 3.1), restricted to non-bot members (and to important members for importance-filtered subsets).

**PR review thoroughness** (`pct_merged_with_review`, `pct_merged_with_approved`): Among merged PRs, the fraction with at least one review or approval before merge. Restricted to July 2020 onward, when structured review data became available in GitHub Archive.

**Discussion quantity** (`avg_all_disc_count`, `avg_pull_request_*_count`): Across all PR threads, the average count of PR comments, review comments, and review events per thread (the original PR opening post is always excluded). `exclude_opener` variants additionally remove all posts by the PR author.

**Output**: `drive/output/derived/org_outcomes_practices/repo_discussion_quality/{member_subset}/rolling5/{repo}.parquet`

---

### Step 4.5: Investment in New Talent (`source/derived/org_outcomes_practices/investment_in_new_talent.py`)

I measure how projects signal openness to newcomers and whether newcomers advance to higher-responsibility roles.

**Primary**

**Newcomer guidelines** (`has_good_first_issue`, `has_contributing_guide`, `has_code_of_conduct`): `has_good_first_issue` is 1 in any period with at least one issue labeled "good first issue." `has_contributing_guide` and `has_code_of_conduct` are set to 1 from the first period the file appears in the git history and are forward filled(Step 1.6).

**Extension**

`pct_good_first_issue`: Fraction of issues carrying the "good first issue" label, rolling-averaged.

**Newcomer adoption** (`pull_request_merged_rate_weighted`, `pull_request_review_rate_weighted`): In each period, "newcomers" are members whose first activity occurred in the 3 periods immediately preceding the current one (i.e., between 18 and 0 months before the current period, exclusive). Among newcomers who had not yet performed a given high-responsibility action, I compute the fraction who do so for the first time — weighted by the number of eligible newcomers using a rolling weighted mean. PR review rates are restricted to July 2020 onward due to data availability.

`any_activity_rate_weighted` (**Extension**): Same newcomer definition and weighting, but the action is performing *any* high-responsibility action (PR merge or PR review) for the first time.

**Output**: `drive/output/derived/org_outcomes_practices/repo_investment_in_new_talent/{member_subset}/rolling5/{repo}.parquet`

---

### Step 4.6: Organizational Routines (`source/derived/org_outcomes_practices/organizational_routines.py`)

I measure organizational use of formal routines that structure how work is assigned and how contributions are solicited. Because these routines are properties of the repository rather than of a member subset, no importance-filtered variant is computed.

**Primary**

**Problem assignment** (`mean_has_reviewer`, `mean_has_tag`, `mean_has_assignee`): For each thread in the rolling window, I record whether it had at least one requested reviewer (PRs only), label, or assignee. Each `mean_has_*` is the fraction of threads with that feature.

**Governance files** (`has_codeowners`, `has_issue_template`, `has_pr_template`, `issue_template_count`): Forward-filled binary indicators from the period each file first appears in the git history (Step 1.6), treated as absorbing. Zero-filled for periods before GitHub introduced CODEOWNERS (July 2017) and issue/PR templates (January 2016). `issue_template_count` is the rolling-mean count of distinct issue template filenames over the window, reflecting how persistently the project has used this feature rather than just whether templates currently exist.

**Output**: `drive/output/derived/org_outcomes_practices/repo_organizational_routines/rolling5/{repo}.parquet`

---

### Step 4.7: Panel Construction (`source/derived/org_outcomes_practices/org_panel.py`)

I join all five practice datasets with the outcome variables into a single panel.

**Member subsets**: Each practice dataset is joined in two variants — all members (`all`) and importance-filtered (Step 3.3) — with `_imp` column suffixes on the filtered measures. Importance-filtered measures recompute each practice using only important members' contributions.

**Treatment**: A repository is *treated* if it has exactly one member dropout across its full history; the treatment date is the period of that dropout. Repositories with zero dropouts are never-treated controls.

**Quasi-treatment**: For never-treated repositories, a pseudo-treatment date is drawn from the empirical distribution of actual treatment dates, stratified by the repository's entry period, to support event-study comparisons.

**Output**: `drive/output/derived/org_outcomes_practices/org_panel/{importance_type}/{rolling_period}/panel.parquet`

---

## Step 5: Analysis Panel Preparation (`source/derived/analysis_panel/`)

This step creates the panel of organizations used in estimation. Composite covariate scores summarizing each repository's pre-treatment organizational practices are constructed, using principal component analysis. 

### Step 5.1: Prepare Panel (`source/derived/analysis_panel/prepare_panel.py`)

| Script | Output | Description |
|--------|--------|-------------|
| `prepare_panel.py` | `output/derived/analysis_panel/{importance_type}/{rolling_period}/{qualified_sample}/{control_group}/panel.parquet` | Filter org panel to estimation sample and construct PC covariate scores |

**Quasi-treatment and event time**: `quasi_treatment_group` from Step 4.7 serves as the reference period for treated and never-treated repositories alike. Event time is defined as `time_index − quasi_treatment_group`.

**Sample qualification** (`FilterQualifiedSample`): Restricts to repositories where the count of `num_important_qualified` members at the quasi-treatment period equals a target value. Primary specifications are `exact1` (exactly 1 qualified important member) and `exact2` (exactly 2); `exact3` is an extension. This ensures each unit has a well-defined important member whose departure can be studied.

**Control group** (`FilterControlGroup`): Two variants are used. The **primary** variant (`nevertreated`) keeps repositories with at most 1 total departure, using zero-departure repositories as never-treated controls. The **extension** variant (`notyettreated`) keeps only treated repositories and uses units not yet treated at a given event time as controls.

**Complete-panel filter** (`CreateCompletePanel`): Drops repositories that (a) have a non-computable or zero-variance baseline for any active outcome — meaning all pre-period values are missing, or all pre-period values are identical (e.g. zero releases in every pre-period), either of which prevents normalization — or (b) lack observations spanning the full required event-time window relative to quasi-treatment. A repository is dropped if *any* active outcome fails these checks.

**PC score construction** (`ComputeRepoPCScores`): For each of five covariate groups defined in `source/lib/pc_groups.json` — *collaboration*, *shared_knowledge*, *discussion_quality*, *investment_in_new_talent*, *problem_solving_routines* — I compute one principal component score per repository. The PCA uses per-repository means of each group variable over the immediate pre-treatment period (quasi_event_time == −1 for the primary rolling5 specification). Variables missing for 200 or more repositories in the estimation sample are excluded. Variables are standardized before PCA; the first PC direction may sometimes be sign-flipped for groups to ensure that a higher raw score indicates increased practice adoption. A binary variant (above vs. below the cross-repository median) is also produced.


**Output**:
- **`output/derived/analysis_panel/{importance_type}/{rolling_period}/{qualified_sample}/{control_group}/panel.parquet`**: Filtered panel with all practice measures and PC covariate scores
- **`pc_score_metadata.csv`**: PCA loadings and variance explained per group
- **`pc_score_excluded_vars.csv`**: Variables excluded due to missing-value threshold
- **`pc_score_columns.json`**: Column names for continuous and binary PC score variants
- For the primary specification only: `output/autofill/panel_autofill.tex` (sample-size macros), `output/autofill/pc_autofill.tex` (variance-explained macros), `output/derived/analysis_panel/pc_loadings_{group}.txt` (PC loading vectors for paper tables)

The script runs over all combinations of importance type and rolling period defined in `source/lib/config/pipeline_inputs.json`. The primary specification is defined in `source/lib/config/paper_settings.json`.

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

- **Safe repo names**: Slashes in repository names are replaced with triple underscores to produce safe filenames
  - Example: `pandas-dev/pandas` → `pandas-dev___pandas.parquet`
- **All repo-level parquets** use this naming convention consistently across scrape and derived directories

---

## Downstream Usage Notes

- **Sentiment columns may contain NAs** for empty or unparseable text. When aggregating, pandas `.mean()` ignores NAs by default, but explicit `.dropna()` may be needed for other operations.
- **List columns** (assignees, labels, requested_reviewers) are stored as Python lists in parquet files.
- **Action types** are human-readable strings (e.g., "issue opened", "pull request review approved").

