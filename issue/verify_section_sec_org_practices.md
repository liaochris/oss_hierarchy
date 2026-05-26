# Verification Report: §3 Organizational Practices

**Draft file**: `source/paper/main.tex` lines 461–520  
**Reference config**: `source/lib/pc_groups.json`  
**Code files**: `source/derived/org_outcomes_practices/collaboration.py`, `shared_knowledge.py`, `discussion_quality.py`, `investment_in_new_talent.py`, `organizational_routines.py`, `source/derived/analysis_panel/prepare_panel.py`

---

## Discrepancies (draft claims that do not match code)

### D1 — HHI measures mix two different HHI types without disclosure
**Claim**: §3.1 (line 475): *"the degree to which participation is concentrated…measured using the Herfindahl–Hirschman Index (HHI) for all discussions, issue threads, and pull request threads respectively."*  
**Code**: `pc_groups.json` shows the three HHI variables in the PC are:
- `proj_hhi_discussion_comment` — **project-level** HHI (share of all discussion-comment events across the whole repo)
- `proj_prob_hhi_issue_comment` — **problem/thread-level** HHI (average HHI within each issue thread)
- `proj_prob_hhi_pull_request_comment` — **problem/thread-level** HHI (average HHI within each PR thread)

The paper implies all three are the same type of HHI. They are not: the first is computed at the project level (who dominates overall discussion activity?), while the latter two are computed at the thread level (who dominates within each individual thread?). DATA_APPENDIX Step 4.2 defines both `proj_hhi_*` and `proj_prob_hhi_*` as distinct constructs.  
**Suggested fix**: Revise §3.1 to: "…using the project-level HHI across all discussions, and the average thread-level HHI within issue threads and pull request threads respectively."  
**Severity**: LOW

- @CLAUDE: Is the distinction made clear in the table that descrbies it? output/tables? If yes, then can ignore. if not, then should clarify table description in source/tables. 

### D2 — PC score uses a single pre-period observation, not five
**Claim**: §3 intro (line 467): *"For each organization, I compute each measure over its five most recent pre-periods and standardize the resulting values. I then aggregate the standardized measures…using the first principal component."*  
**Code**: `prepare_panel.py` `ComputeRepoPCScores` (lines 137–138): `pre_data = panel[panel["quasi_event_time"] == -1]`. The PCA is run on one observation per repo — the value of each rolling measure at quasi_event_time == -1. The "five most recent pre-periods" are baked into the rolling window of that single measure value, but the PCA itself uses only a single data point per repository, not five.  
**Suggested fix**: Clarify: "For each organization, I compute each measure using a rolling window over the five periods ending at the pre-treatment period. I standardize the resulting cross-organizational values and aggregate them using the first principal component."  
**Severity**: LOW
- @CLAUDE: change to "For each organization, I compute each measure over its five most recent pre-periods and standardize the resulting pooled values". Do you agree this fixes it?
- Also add a footnote that I flip the sign of all coefficients of variables in the PC when necessary so that increased score -> higher adoption of the practice. 
---

## Gaps (code steps not documented in the draft)

### G1 — Newcomer adoption measures not mentioned
**Code step**: `investment_in_new_talent.py` `CalculateNewcomerAdoption` (lines 145–204). Two measures are computed: `pull_request_merged_rate_weighted` and `pull_request_review_rate_weighted` — the weighted fraction of newcomers (members whose first activity was in the 3 prior periods) who adopt high-responsibility actions for the first time. PR review rates are restricted to July 2020 onward.  
**Why it matters**: Paper §3.4 lists only 3 measures (good first issues, contributing guide, code of conduct). These newcomer-adoption measures are computed as primary outputs and described in DATA_APPENDIX Step 4.5 as "Primary," but are absent from both the paper and `pc_groups.json`. A reader of DATA_APPENDIX would expect them to be used in the analysis.  
**Suggested fix**: Either mention them in §3.4 (noting they are computed but not included in the PC score), or downgrade them to "Extension" in DATA_APPENDIX Step 4.5.  
**Severity**: LOW
- @CLAUDE: Update data appendix to note that even though they are primary, they don't enter because too many missing (ALSO CHECK THIS IS THE REASON THEY"RE MISSING)

### G2 — Issue template and PR template are two separate measures
**Code step**: `pc_groups.json` lists `has_issue_template` and `has_pr_template` as distinct entries in the problem-solving-routines PC.  
**Why it matters**: Paper §3.5 (line 515) says *"whether the organization uses templates to systematize issues and pull requests"* — one description for what are actually two separate PC inputs. The sentence should distinguish them.  
**Suggested fix**: Change to: "whether the organization uses issue templates and pull request templates to systematize contributions."  
**Severity**: LOW
- @CLAUDE: ok, clarify

### G3 — `issue_template_count` is computed but not in PC or paper
**Code step**: `organizational_routines.py` `UniqueIssueTemplateCount` (lines 95–106) computes `issue_template_count` (rolling-mean count of distinct issue template filenames). It is in the panel but absent from `pc_groups.json`.  
**Why it matters**: No paper change needed; the paper's count of six measures matches the six in `pc_groups.json`. But a reader of the code would wonder why this measure is computed and not used in the primary analysis.  
**Severity**: LOW (code/DATA_APPENDIX concern only)
- @CLAUDE: ok, don't treat it as a primary one then. 
---

## Overall Assessment

The section is mostly aligned with the code. The most actionable fixes are: clarifying the mixed HHI types in §3.1 (D1), splitting "templates" into issue and PR templates in §3.5 (G2), and resolving the DATA_APPENDIX description of newcomer adoption measures as "Primary" when they don't enter the PC score (G1).
