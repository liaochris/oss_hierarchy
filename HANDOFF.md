
1. In paper - important exact can be either 1 or 2... so estimate separately and combine. 
2. Make sure paper and DATA_APPENDIX.md are aligned
3. update Python dependencies in `source/lib/requirements.txt` 
4. upate R dependencies in `source/lib/requirements.r`
5. Add code to compute and export tree depth distribution from trained forest RDS objects (currently hardcoded in `source/figures/forest_characteristics.tex` Panel A — values will not auto-update if the forest is retrained). See `fold_comparisons.R` as a model for reading RDS files.
6. Export hardcoded collaboration count claims in §6.2 (e.g., "9 of the 16 combinations with high collaboration") from `pc_score_combinations.R` as autofill macros so they stay in sync with the data.