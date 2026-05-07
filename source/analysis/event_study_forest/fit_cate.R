library(tidyverse)
library(arrow)
library(fs)
library(jsonlite)
library(SaveData)

source("source/lib/helpers.R")
source("source/lib/forest_helpers.R")
source("source/lib/constants.R")
source("source/lib/event_study_forest.R")

INDIR_PREP       <- "output/analysis/data_prep"
OUTDIR           <- "output/analysis/event_study_forest"
OUTDIR_DATASTORE <- "drive/output/analysis/event_study_forest"

Main <- function() {
  project_cfg      <- LoadProjectConfig(PROJECT_CONFIG_PATH)
  outcome_cfg      <- project_cfg$outcome_variables
  org_practice_cfg <- project_cfg$feature_variables

  for (importance_type in IMPORTANCE_TYPES) {
    for (qualified_sample in QUALIFIED_SAMPLES) {
      if (qualified_sample %in% names(AGGREGATED_SAMPLES)) next
      for (control_group in CONTROL_GROUPS) {
        for (rolling_panel in ROLLING_LABELS) {
          rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
          practice_modes <- BuildOrgPracticeModes(org_practice_cfg, control_group,
                                                   outdir = NULL, build_dir = FALSE)
          practice_modes <- practice_modes[
            !sapply(practice_modes, function(x) x$continuous_covariate == "prop_tests_passed")
          ]
          outcome_modes  <- BuildOutcomeModes(outcome_cfg, control_group, outdir = NULL,
                                               NORM_OPTIONS, build_dir = FALSE)

          covars <- unlist(lapply(practice_modes, function(x) x$continuous_covariate))

          pc_score_columns_path <- file.path(INDIR_PREP, importance_type, rolling_panel,
                                             qualified_sample, control_group, "pc_score_columns.json")
          pc_score_columns <- fromJSON(pc_score_columns_path)

          for (covar_type in COVAR_TYPES) {
            needs_pc_scores <- covar_type %in% c("pc_score", "pc_score_binary")
            panel <- LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel,
                                        qualified_sample, control_group, with_pc_scores = needs_pc_scores)
            marg_dist <- ComputeCohortTimeDist(panel)

            outdir_datastore <- file.path(OUTDIR_DATASTORE, importance_type, rolling_panel,
                                   qualified_sample, control_group, covar_type)
            outdir    <- file.path(OUTDIR,            importance_type, rolling_panel,
                                   qualified_sample, control_group, covar_type)

            forest_outcome_modes <- Filter(function(m) m$outcome == FOREST_TRAINING_OUTCOME, outcome_modes)
            for (outcome_mode in forest_outcome_modes) {
              df_data           <- CreateDataPanel(panel, outcome_mode$outcome, covars,
                                                   rolling_period, N_FOLDS, SEED)
              base_feature_cols <- intersect(paste0(covars, "_mean"), colnames(df_data))
              na_counts         <- colSums(is.na(df_data %>% select(all_of(base_feature_cols))))
              base_feature_cols <- base_feature_cols[na_counts < NA_THRESHOLD]
              df_data           <- df_data[complete.cases(df_data %>% select(all_of(base_feature_cols))), ]

              feature_cols <- SelectFeatureColumns(df_data, base_feature_cols, covar_type, pc_score_columns)

              pipeline <- NewEventStudyForestPipeline(outcome_mode$outcome, rolling_period)
              pipeline <- PrepareData(pipeline, df_data, feature_cols, marg_dist)
              pipeline <- FitOutOfSample(pipeline, outdir_datastore, use_existing = TRUE)
              pipeline <- Assemble(pipeline)
              Save(pipeline, outdir)
            }
          }
        }
      }
    }
  }
}


SelectFeatureColumns <- function(df_data, base_feature_cols, covar_type, pc_score_columns) {
  switch(covar_type,
    all_covariates = base_feature_cols,
    pc_score       = intersect(pc_score_columns$pc_score,               colnames(df_data)),
    pc_score_binary = intersect(pc_score_columns$pc_score_binary, colnames(df_data))
  )
}


Main()
