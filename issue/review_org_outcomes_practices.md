## source/derived/org_outcomes_practices

Goal: measure outcomes (event counts, downloads, releases) and organizational practices (collaboration, shared knowledge, discussion quality, newcomer investment, routines) used to predict resilience.

---

### Issues

#### `CleanOutputs` in collaboration/shared_knowledge/discussion_quality/investment — partial cleaning on rolling-period change

All four scripts clean only `rolling{ROLLING_PERIODS}` (the current period). If `ROLLING_PERIODS` is changed, stale outputs under old rolling period directories persist. Contrast with `org_outcomes.py` which calls `CleanDirs([OUTDIR, LOG_OUTDIR])` (full clean). Low severity if rolling period is stable, but worth noting.
 - is there not a rollingperiod folder to make this explicit? 
---

### Notes

- `org_outcomes.py`: `PreloadAllDownloads` uses DuckDB to load downloads for all repos in one pass before the joblib parallel loop — good performance pattern.
- `org_panel.py` `_AssignQuasiTreatment`: draws pseudo-treatment times from the empirical treatment distribution conditioned on repo entry time. Well-documented with a docstring.
- `org_panel.py` `_FilterDeparturesByActionData`: correctly removes actors from dropouts_actors if they have post-departure activity. The inline `try/except` silently swallows errors; at minimum the except could `pass` with a comment, but there is already an `except Exception: pass` — acceptable for robustness in parallel loops.
- The "reference repo" target pattern in SConscript (using `{ref}.parquet` as a sentinel for the whole per-repo batch) is idiomatic for dynamic repo sets. Not a bug.
- `organizational_routines.py`: `_CODEOWNERS_LAUNCH` and `_TEMPLATES_LAUNCH` source URLs in comments are valuable context; keep them.
- `discussion_quality.py` `BuildConversationBlocks`: the `sender_block` cumsum approach for segmenting reply chains is non-trivial; the comment correctly explains the intent.

---

## Summary

**Output alignment**: The pipeline correctly produces per-repo parquet files for outcomes (event counts, downloads, release counts), four practice dimensions (collaboration HHI, shared-knowledge mix, discussion quality, newcomer adoption), and one routines dimension (organizing files, reviewer/label/assignee usage). These are assembled into a panel by `org_panel.py` with treatment assignment logic. The outputs match the stated goal.

**Remaining open items:**

- `CleanOutputs` partial-cleaning behavior on rolling-period change (low severity — outputs are namespaced under `rolling{N}/` folders so old periods don't collide).
