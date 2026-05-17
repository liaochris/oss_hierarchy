library(tidyverse)
library(arrow)
library(fs)
library(jsonlite)
library(SaveData)

source("source/lib/R/config_loaders.R")
source("source/lib/R/analysis_utils.R")
source("source/lib/event_study_forest/forest_helpers.R")
source("source/lib/R/constants.R")
source("source/lib/event_study_forest/event_study_forest.R")

INDIR_PREP       <- "output/derived/analysis_panel"
OUTDIR           <- "output/analysis/event_study_forest"
OUTDIR_DATASTORE <- "drive/output/analysis/event_study_forest"

.cl_raw          <- commandArgs(trailingOnly = TRUE)
.cl              <- setNames(sub("^[^=]+=", "", .cl_raw), sub("=.*$", "", .cl_raw))
IMPORTANCE_TYPE  <- .cl[["CL_IMPORTANCE_TYPE"]]
ROLLING_PERIOD   <- .cl[["CL_ROLLING_PERIOD"]]
QUALIFIED_SAMPLE <- .cl[["CL_QUALIFIED_SAMPLE"]]
CONTROL_GROUP    <- .cl[["CL_CONTROL_GROUP"]]

Main <- function() {
  org_practice_cfg <- feature_variables
  rolling_period   <- as.numeric(str_extract(ROLLING_PERIOD, "\\d+$"))

  practice_modes <- BuildOrgPracticeModes(org_practice_cfg, CONTROL_GROUP,
                                           outdir = NULL, build_dir = FALSE)
  covars <- unlist(lapply(practice_modes, function(x) x$continuous_covariate))

  pc_score_columns_path <- file.path(INDIR_PREP, IMPORTANCE_TYPE, ROLLING_PERIOD,
                                     QUALIFIED_SAMPLE, CONTROL_GROUP, "pc_score_columns.json")
  pc_score_columns <- fromJSON(pc_score_columns_path)

  for (covar_type in COVAR_TYPES) {
    needs_pc_scores <- covar_type %in% c("pc_score", "pc_score_binary")
    panel <- LoadPreparedSample(INDIR_PREP, IMPORTANCE_TYPE, ROLLING_PERIOD,
                                QUALIFIED_SAMPLE, CONTROL_GROUP, with_pc_scores = needs_pc_scores)
    marg_dist <- ComputeCohortTimeDist(panel)

    for (normalize in NORM_OPTIONS) {
      norm_label       <- ifelse(normalize, "norm", "raw")
      outdir_datastore <- file.path(OUTDIR_DATASTORE, IMPORTANCE_TYPE, ROLLING_PERIOD,
                             QUALIFIED_SAMPLE, CONTROL_GROUP, covar_type, norm_label)
      outdir           <- file.path(OUTDIR, IMPORTANCE_TYPE, ROLLING_PERIOD,
                             QUALIFIED_SAMPLE, CONTROL_GROUP, covar_type, norm_label)

      df_data           <- CreateDataPanel(panel, FOREST_TRAINING_OUTCOME, covars,
                                           rolling_period, N_FOLDS, SEED, normalize = normalize)
      base_feature_cols <- intersect(paste0(covars, "_mean"), colnames(df_data))
      na_counts         <- colSums(is.na(df_data %>% select(all_of(base_feature_cols))))
      base_feature_cols <- base_feature_cols[na_counts < NA_THRESHOLD]
      df_data           <- df_data[complete.cases(df_data %>% select(all_of(base_feature_cols))), ]

      feature_cols <- SelectFeatureColumns(df_data, base_feature_cols, covar_type, pc_score_columns)

      pipeline <- NewEventStudyForestPipeline(FOREST_TRAINING_OUTCOME, rolling_period)
      pipeline <- PrepareData(pipeline, df_data, feature_cols, marg_dist)
      pipeline <- FitOutOfSample(pipeline, outdir_datastore, use_existing = TRUE)
      pipeline <- Assemble(pipeline)
      Save(pipeline, outdir)
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
