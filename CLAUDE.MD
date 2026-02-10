## Related Documentation
- `QUALITY.MD` - Code conventions (naming, organization, data saving)
- `DATA_APPENDIX.MD` - Detailed documentation of action-level data construction

## Project Structure

- `source/` - Source code for data processing and analysis
  - `source/lib/` - Shared libraries and helpers
    - `source/lib/helpers.py` - Common helper functions (MakeRepoNameSafe, LoadGlobals, SaveData, etc.)
    - `source/lib/JMSLab/` - JMSLab framework (SaveData, builders for SCons)
    - `source/lib/globals.json` - Global configuration (reference_github_repo, dates, etc.)
  - `source/derived/` - Scripts that create derived datasets
  - `source/scrape/` - Data scraping scripts
  - `source/analysis/` - Analysis scripts (R and Python)
- `drive/output/` - Large data files (parquet, csv)
  - `drive/output/scrape/` - Raw scraped data
  - `drive/output/derived/` - Processed/derived data
- `output/` - Smaller output files and logs
- `issue_scrape/` - Issue scraping utilities

## Environment

- Activate conda environment: `source activate org_resilience`

## Key Conventions

### Repo Name Handling
GitHub repo names like `pandas-dev/pandas` are converted to safe filenames using triple underscores:
- Use `MakeRepoNameSafe(repo_name)` from `source.lib.helpers`
- Converts `/` to `___` (e.g., `pandas-dev/pandas` → `pandas-dev___pandas`)
- All parquet files use this naming convention

### SConscript Patterns
- Use `LoadGlobals("source/lib/globals.json")` to get global config
- `reference_github_repo` (pandas-dev/pandas) is the canonical example repo for dependencies
- Use `MakeRepoNameSafe(reference_repo)` for file paths in SConscript dependencies
- Pattern: `env.Python(target, source)` for build rules

### Data Saving
- Always use `SaveData(df, keys, outfile, logfile)` from `source.lib.JMSLab.SaveData`
- Keys must uniquely identify rows
- Creates both parquet file and log file with summary stats

## Common Data Locations

- Issue data: `drive/output/scrape/extract_github_data/repo_level_data/issue/`
- PR data: `drive/output/scrape/extract_github_data/repo_level_data/pr/`
- Linked PR/Issue: `drive/output/scrape/link_issue_pull_request/`
- Repo actions (combined): `drive/output/derived/problem_level_data/repo_actions/`
- Cleaned text (with sentiment): `drive/output/derived/problem_level_data/cleaned_text/`
- Component tracking: `output/derived/problem_level_data/repo_component_tracking.csv`

## Processing Patterns

### Parallel Repo Processing
```python
with concurrent.futures.ProcessPoolExecutor() as executor:
    for msg in executor.map(ProcessRepo, repos):
        print(msg)
```

### Component Tracking
Scripts should update `repo_component_tracking.csv` to track which repos have which data components. This enables incremental processing across scripts.
