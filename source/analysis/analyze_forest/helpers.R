library(tidyverse)
library(arrow)
library(fs)
library(jsonlite)
library(SaveData)

source("source/lib/R/config_loaders.R")
source("source/lib/R/analysis_utils.R")
source("source/lib/event_study_forest/forest_helpers.R")
source("source/lib/R/constants.R")

INDIR_PREP <- "output/derived/analysis_panel"

PC_LABELS <- setNames(
  vapply(names(PC_GROUPS), function(g) sub(" score$", "", PCGroupFriendlyLabel(g)), character(1)),
  paste0(names(PC_GROUPS), "_pc_score")
)

LoadForestResults <- function(indir_forest, importance_type, rolling_panel,
                              qualified_sample, control_group, norm_label,
                              covar_type_dir = "pc_score") {
  split_var <- FOREST_TRAINING_OUTCOME

  if (qualified_sample %in% names(AGGREGATED_SAMPLES)) {
    sub_sample_names <- AGGREGATED_SAMPLES[[qualified_sample]]
    sub_dfs <- setNames(
      lapply(sub_sample_names, function(s) {
        path <- file.path(indir_forest, importance_type, rolling_panel, s, control_group,
                          covar_type_dir, norm_label, paste0(split_var, "_repo_att_event_study_forest.parquet"))
        tryCatch(read_parquet(path), error = function(e) NULL)
      }),
      sub_sample_names
    )
    sub_dfs <- Filter(Negate(is.null), sub_dfs)
    if (length(sub_dfs) == 0) return(NULL)
    list(df = bind_rows(sub_dfs), sub_dfs = sub_dfs)
  } else {
    path <- file.path(indir_forest, importance_type, rolling_panel, qualified_sample,
                      control_group, covar_type_dir, norm_label,
                      paste0(split_var, "_repo_att_event_study_forest.parquet"))
    df <- tryCatch(read_parquet(path), error = function(e) NULL)
    if (is.null(df)) return(NULL)
    list(df = df, sub_dfs = setNames(list(df), qualified_sample))
  }
}

LoadAnalysisPanel <- function(importance_type, rolling_panel, qualified_sample, control_group) {
  if (qualified_sample %in% names(AGGREGATED_SAMPLES)) {
    sub_panels <- lapply(AGGREGATED_SAMPLES[[qualified_sample]], function(s)
      LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel, s, control_group))
    sub_panels <- Filter(function(p) nrow(p) > 0, sub_panels)
    if (length(sub_panels) == 0) return(tibble())
    bind_rows(sub_panels)
  } else {
    LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel, qualified_sample, control_group)
  }
}

BinarizePCScores <- function(sub_dfs, pc_cols) {
  binarized_list <- lapply(sub_dfs, function(sub_df) {
    medians <- sapply(pc_cols, function(col) median(sub_df[[col]], na.rm = TRUE))
    sub_df %>% mutate(across(all_of(pc_cols),
                             ~ ifelse(.x > medians[cur_column()], "high", "low"),
                             .names = "{.col}"))
  })
  list(df = bind_rows(binarized_list), sub_dfs = binarized_list)
}

