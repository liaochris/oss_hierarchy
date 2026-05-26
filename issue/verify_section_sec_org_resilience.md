# Verification Report: §4 Estimating Organizational Resilience

**Draft file**: `source/paper/main.tex` lines 522–649  
**DATA_APPENDIX sections**: Step 5.1  
**Code files**: `source/analysis/event_study/estimate_resilience.R`, `source/lib/R/event_study_helpers.R`, `source/lib/R/constants.R`, `source/derived/analysis_panel/prepare_panel.py` (CoarsenScoresToAboveBelowMedian)

---

## Discrepancies (draft claims that do not match code)

### D1 — Pre-trend Wald test uses four estimated pre-periods, not five
**Claim**: §4.1 (line 578): *"the p-value from the Wald test of the pre-trend null hypothesis $\delta_k = 0$ for the five plotted pre-periods."*  
**Code**: `event_study_helpers.R` `TestPretrends` (lines 31–49). The function tests `as.character(MIN_EVENT_TIME:-1)` = {-5, -4, -3, -2, -1}, but drops rows where `sd == 0`. Period -1 is the reference period — its coefficient is forced to zero by `AddRefPeriodRow`, so its sd = 0 and it is excluded from the Wald statistic. The test uses only four estimated pre-periods: -5, -4, -3, -2.  
**Suggested fix**: Change "five plotted pre-periods" to "four estimated pre-periods (the fifth pre-period serves as the reference)" or drop the count: "…the p-value from the Wald test of the pre-trend null hypothesis $\delta_k = 0$ for all pre-periods prior to the reference."  
**Severity**: LOW

- @CLAUDE: change to "four estimated pre-period (the reference period is excluded)"
---

## Gaps (code steps not documented in the draft)

### G1 — `major_minor_release_count` is a primary outcome but never shown
**Code step**: `source/lib/config/outcome_variables.json` lists `major_minor_release_count` (major and minor releases only) alongside `overall_new_release_count` in the `run` (primary) outcomes. DATA_APPENDIX Step 4.7 confirms both are primary. `estimate_resilience.R` runs event studies for both.  
**Why it matters**: The paper (lines 537, 570, 574) describes exactly three outcomes — pull requests opened, pull requests merged, and "new software releases" — with no mention of a fourth. Figure~\ref{fig:event_study} shows only `overall_new_release_count`. A reader of the DATA_APPENDIX would expect `major_minor_release_count` to appear somewhere in the main results.  
**Suggested fix**: Either show the fourth outcome in an appendix figure with a note, or add a sentence noting that results for major/minor releases specifically are available but not shown because they closely track the overall release count.  
**Severity**: LOW
- @CLAUDE: update the code and output directory to treat major_minor_releases as a secondary outcome not primary

### G2 — Median-split tie-breaking rule not disclosed
**Code step**: `prepare_panel.py` `CoarsenScoresToAboveBelowMedian` (lines 315–318): `score > median_val` is classified as high (1); scores equal to or below the median are classified as low (0).  
**Why it matters**: The paper (line 587) says organizations are split "depending on whether $X_{ij}$ lies above or below the cross-organizational median," which leaves ambiguous what happens at exactly the median. In the code, ties go to the low group.  
**Suggested fix**: Add "organizations at the median are assigned to the low group" (one clause, possibly in a footnote).  
**Severity**: LOW
- @CLAUDE: fine to leave as is

### G3 — Organizations with missing PC scores silently excluded from PC-split analyses
**Code step**: `prepare_panel.py` `CoarsenScoresToAboveBelowMedian` (line 317): NaN scores are assigned NaN binary values. `estimate_resilience.R` `BuildPCScoreGroups` (lines 200–201): groups are built by filtering for binary value == 0 or == 1; NaN repos are in neither group and are excluded from both sub-sample event studies.  
**Why it matters**: If an organization lacks enough non-missing variables to form a PC for some practice (e.g., fewer than 2 vars pass the NA threshold), it is silently dropped from that practice's median-split comparison. The paper does not disclose this exclusion.  
**Suggested fix**: Add a footnote noting that organizations without a valid PC score for a given practice are excluded from the corresponding median-split comparison.  
**Severity**: LOW
- @CLAUDE: are there any organizations that are actually dropped because their practices aren't defined? 

---

## Overall Assessment

The section is well-aligned with the code. All methodological claims (Sun–Abraham estimator, fixed effects, clustering, IVW aggregation, pre-trend Wald test) match their implementations. The only substantive gap is the undisclosed fourth primary outcome (`major_minor_release_count`, G1); the remaining items are minor documentation details.
