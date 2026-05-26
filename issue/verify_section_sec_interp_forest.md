# Verification Report: §6 Interpreting the Random Forest

**Draft file**: `source/paper/main.tex` lines 749–863  
**Code files**: `source/analysis/analyze_forest/` (all scripts), `source/lib/R/constants.R`, `source/figures/forest_characteristics.tex`, `source/figures/pc_score_combo.tex`

---

## Discrepancies (draft claims that do not match code)

### D1 — `forest_characteristics.tex` references wrong filename for correlation heatmap
**Claim**: `source/figures/forest_characteristics.tex` line 48: `\includegraphics{...heatmap_corr.png}`.  
**Code**: `source/analysis/analyze_forest/SConscript` line 70 and `pc_correlation_heatmap.R` declare and generate `pc_score_correlation_heatmap.png` at the same path prefix. The file `heatmap_corr.png` does not exist as a build target.  
**Suggested fix**: Change the `\includegraphics` path in `forest_characteristics.tex` to `pc_score_correlation_heatmap.png`.  
**Severity**: HIGH (figure will fail to compile with the correct output)

- @CLAUDE: change to the right filepath

---

### D2 — `pc_score_combo.tex` references wrong filename for practice combo figure
**Claim**: `source/figures/pc_score_combo.tex` line 5: `\includegraphics{...pc_score_combo.png}`.  
**Code**: `source/analysis/analyze_forest/SConscript` line 75 and `pc_score_combinations.R` generate `pc_score_combo_high_low_grid.png` at the same path prefix. The file `pc_score_combo.png` does not exist as a build target.  
**Suggested fix**: Change the `\includegraphics` path in `pc_score_combo.tex` to `pc_score_combo_high_low_grid.png`.  
**Severity**: HIGH (figure will fail to compile with the correct output)

- @CLAUDE: change to the right filepath

---

### D3 — Typo in figure: "mew talent" instead of "new talent"
**Claim**: `source/figures/forest_characteristics.tex` line 35: "Investment in mew talent score".  
**Code**: N/A — pure text issue.  
**Suggested fix**: Change "mew" to "new".  
**Severity**: LOW

- @CLAUDE: change to the right name and check throughout if there are typos
---

### D4 — Variable importance values are hardcoded, not autofilled from generated output
**Claim**: `source/figures/forest_characteristics.tex` Panel B (lines 28–37) hardcodes: Collaboration 0.21, Knowledge redundancy 0.35, Discussion quality 0.26, Investment in new talent 0.02, Problem-solving routines 0.16.  
**Code**: `source/analysis/analyze_forest/fold_comparisons.R` `ExportVariableImportanceTex` (lines 169–188) generates `variable_importance.tex` at `output/analysis/analyze_forest/.../variable_importance.tex` with the same data. The figure file ignores this output and hardcodes values instead.  
**Suggested fix**: Replace the hardcoded tabular with `\input{output/analysis/analyze_forest/important_degree_top3/rolling5/exact_1_2/nevertreated/norm/variable_importance.tex}`, or use tablefill markers and `env.Tablefill`.  
**Severity**: LOW (values may become stale if forest is retrained)

@CLAUDE: Please use the correct values and autofill. 

---

### D5 — Tree depth distribution values are hardcoded with no generating code
**Claim**: `source/figures/forest_characteristics.tex` Panel A (lines 14–21): depth 1 → 0.334, depth 2 → 0.088, depth 3 → 0.265, depth 4 → 0.280, depth 5 → 0.032.  
**Code**: No script in `source/analysis/analyze_forest/` or `source/analysis/event_study_forest/` computes or exports tree depth distributions. The fold comparisons script reads `.rds` forest objects only for `variable_importance` and fold predictions — no `get_tree()` call or depth calculation exists.  
**Suggested fix**: Either add a code step that computes and exports the depth distribution, or note in the paper/figure that values are from a specific run.  
**Severity**: LOW (values will not auto-update if forest is retrained)

- Add to handoff as todo. 

---

## Gaps (code steps not documented in the draft)

### G1 — Coarsened median split uses combined (exact_1_2) sample medians
**Code step**: `source/analysis/analyze_forest/helpers.R` `BinarizePCScores` (lines 48–52) computes medians across all rows of the combined `exact_1_2` dataframe. The binarization threshold is thus the median across both the exact1 and exact2 sub-samples pooled together.  
**Why it matters**: The paper (line 829–830) says organizations are classified into high/low using "a median split" of continuous practice scores, but does not say the median is computed over the pooled exact_1_2 sample. A reader might assume the split is computed within each sub-sample separately.  
**Suggested fix**: Add a brief clause: "where the median is taken over the full pooled sample."  
**Severity**: LOW
- @CLAUDE: You should still be using the exact1 and exact2 -specific median, just like everywhere else.... THis should be made clear in the text and fix the code. 

---

### G2 — `pc_score_combo.tex` figure notes say "TBD"
**Code step**: `source/figures/pc_score_combo.tex` line 11: `\textbf{Figure notes:} TBD`.  
**Why it matters**: The figure notes for Figure~\ref{fig:practice_combo} are missing. Readers will see "TBD" in the compiled PDF.  
**Suggested fix**: Replace with proper notes describing the figure (e.g., "Each row represents one of the 32 combinations of five binary organizational practice scores (high/low). Rows are ordered by descending mean doubly robust ATT across all post-periods. The red line separates the top 16 from the bottom 16 combinations.").  
**Severity**: LOW
- @CLAUDE: Please add your figure notes. 

---

### G3 — Collaboration count claim (9/16, 6/8) uses hardcoded numbers, no tablefill
**Code step**: Lines 837, 841, 845 of `main.tex` state specific counts: "9 of the 16 combinations with high collaboration," "Six of the eight combinations," etc. These are data-driven claims but are hardcoded in the text.  
**Why it matters**: If the forest is retrained or the sample changes, these counts will silently go stale.  
**Suggested fix**: Consider exporting these counts from `pc_score_combinations.R` and using autofill macros, or at minimum flagging them as values to verify after any rerun.  
**Severity**: LOW
- @CLAUDE: add as a todo in HANDOF.md
---

## Overall Assessment

The section's narrative is well-aligned with the code logic. The critical issues are two filename mismatches in figure `.tex` files (D1, D2) that will prevent the paper from compiling with the correct figures. The remaining items are data-currency risks from hardcoded figure values and a missing figure note.
