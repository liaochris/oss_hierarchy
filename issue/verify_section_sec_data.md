# Verification Report: §2 Background and Data

**Draft file**: `source/paper/main.tex` lines 327–457  
**DATA_APPENDIX sections**: Steps 1–3.3, 4.7, 5.1  
**Code files**: `source/derived/graph_structure/calculate_importance.py`, `source/derived/org_outcomes_practices/org_panel.py`, `source/derived/analysis_panel/prepare_panel.py`

---

## Discrepancies (draft claims that do not match code)

### D1 — DATA_APPENDIX primary specification mismatch
**Claim**: DATA_APPENDIX Step 5.1: *"Primary specifications are `exact1` (exactly 1 qualified important member) and `exact2` (exactly 2); `exact3` is an extension."*  
**Code**: `source/lib/config/paper_settings.json`: `"primary_qualified_sample": "exact_1_2"`. The actual primary used to generate all autofill macros and canonical figures is the combined `exact_1_2` spec (exact1 + exact2 pooled via inverse-variance weighting), not exact1 or exact2 individually.  
**Suggested fix**: Update DATA_APPENDIX Step 5.1 to say: "The primary specification is `exact_1_2` (organizations with 1 or 2 qualified important members, estimated separately and combined via inverse-variance weighting); `exact3` is an extension."  
**Severity**: HIGH
- @CLAUDE: Yes, please fix . 

---

## Gaps (code steps not documented in the draft)

### G1 — Undocumented baseline validity drop
**Code step**: `prepare_panel.py` `CreateCompletePanel` → `GetOutcomeValidRepos` (lines 100–129). Before enforcing the event-window requirement, this function drops any repository where the baseline outcome is all-missing or has zero variance (i.e., the computed standardized value is non-finite). A repository is dropped if *any* active outcome fails these checks.  
**Why it matters**: This is a substantive sample restriction separate from the two the paper enumerates in §2.5 (key-member count and observation window). It silently removes organizations with, e.g., zero releases in every pre-period. Readers expecting only two stated restrictions would not know this exclusion exists.  
**Suggested fix**: Add a third restriction to §2.5: "Third, each organization must have a non-degenerate baseline for all outcomes — organizations where any outcome has an all-missing or zero-variance pre-period baseline are excluded, as standardization is not feasible."  
**Severity**: HIGH
- @CLAUDE: Definitely include the zero-variance point. Can you define more explicitly what all-missing is?? Also I think orgs must have all outcomes with zero variance right? 
  - **Response**: "All-missing" is practically the same as zero-variance here — outcome counts are zero-filled in `org_panel.py`, so a repo with no activity of a given type just has all-zero pre-period values, which is a zero-variance case (std=0). The code (`set.intersection` over all outcomes, line 105 of `prepare_panel.py`) drops a repo if **any** outcome has a degenerate baseline, meaning each org must pass the non-zero-variance check for **all** outcomes. Paper fix uses: "all pre-period values are identical (e.g., all zero)."

### G2 — Interaction graph construction details omitted
**Code step**: `source/derived/graph_structure/create_graphs.py` and DATA_APPENDIX Step 3.1. The receiver assignment rule has two undocumented details: (a) consecutive posts by the same author are skipped when finding the receiver (so self-posting chains don't count as self-interactions), and (b) for PR review threads where no prior post by a different member exists, the thread opener is used as the default receiver.  
**Why it matters**: The paper footnote at §2.3 says only "An interaction occurs when one member's comment in a discussion thread follows another member's post or comment." This description would mislead a reader trying to replicate the interaction counts.  
**Suggested fix**: Expand the footnote or add a sentence in §2.3 to note both rules.  
**Severity**: LOW
- @CLAUDE. I think the footnote already describes the cumulative part, u just need to add the (b). 

### G3 — Importance-filtered practice variants not mentioned
**Code step**: `org_panel.py` lines 100–119, `DATASETS` dict. Each practice dataset is joined in two variants: `all` members and `_imp` (importance-filtered), where importance-filtered measures recompute each practice using only the important members' contributions. The panel therefore contains both `collab_score` and `collab_score_imp` columns.  
**Why it matters**: §2 and §3 describe practices as measured over all members without acknowledging the importance-filtered variant. A reader does not know which variant the main event study uses.  
**Suggested fix**: Add a clarifying sentence noting that each practice is computed for both all members and important members, and state which is used in the primary analysis.  
**Severity**: LOW
- Note: We don't use the importance-filtered variant so we can ignore. 

### G4 — z-score importance definition not disclosed
**Code step**: `source/lib/config/importance_specifications.json` and DATA_APPENDIX Step 3.3. Two key-member identification methods are computed: `important_degree_top3` (top 3 by degree centrality, primary) and `important_degree_z2` (degree centrality z-score > 2.0, extension). The paper describes only the top-3 rule.  
**Why it matters**: A reader would not know the z-score variant exists or that it serves as a robustness check.  
**Suggested fix**: Add a brief note in §2.3 or a footnote: "As a robustness check, I also apply a z-score threshold (degree centrality z-score > 2.0) to identify single-period key members; results are similar."  
**Severity**: LOW
- Note: We don't use the the z>2 variant so we can skip. 

---

## Overall Assessment

The section is largely well-aligned with the code. The critical fix is updating DATA_APPENDIX Step 5.1 to name `exact_1_2` as the primary specification (D1), and documenting the baseline validity drop as a third sample restriction in §2.5 (G1). The LOW items (G2–G4) are minor documentation gaps.
