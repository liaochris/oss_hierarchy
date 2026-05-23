library(tidyverse)
library(fs)
library(SaveData)

source("source/lib/R/config_loaders.R")
source("source/lib/R/constants.R")

INDIR  <- "output/analysis/event_study"
OUTDIR <- "output/analysis/event_study"

Main <- function() {
  all_dfs <- list()
  for (importance_type in IMPORTANCE_TYPES) {
    for (rolling_panel in ROLLING_LABELS) {
      for (qualified_sample in QUALIFIED_SAMPLES) {
        for (control_group in CONTROL_GROUPS) {
          path <- file.path(INDIR, importance_type, rolling_panel,
                            qualified_sample, control_group, "estimates.csv")
          if (file.exists(path)) {
            all_dfs <- c(all_dfs, list(read_csv(path, show_col_types = FALSE)))
          }
        }
      }
    }
  }

  coeffs_df <- bind_rows(all_dfs) %>%
    mutate(event_time = as.numeric(event_time),
           split_covar = replace_na(split_covar, ""),
           split_value = replace_na(as.character(split_value), "")) %>%
    distinct()

  dir_create(OUTDIR)
  SaveData(
    coeffs_df,
    c("importance_type", "rolling", "qualified_sample", "control_group", "split_type",
      "split_covar", "split_value", "category", "outcome", "normalize", "method", "event_time"),
    file.path(OUTDIR, "all_estimates.csv"),
    file.path(OUTDIR, "all_estimates.log"),
    sortbykey = FALSE
  )

  GenerateEventStudyAutofill(coeffs_df)
}

GenerateEventStudyAutofill <- function(coeffs_df) {
  canonical <- coeffs_df %>%
    filter(
      importance_type  == PRIMARY_IMPORTANCE_TYPE,
      rolling          == PRIMARY_ROLLING_LABEL,
      qualified_sample == PRIMARY_QUALIFIED_SAMPLE,
      control_group    == PRIMARY_CONTROL_GROUP,
      normalize        == TRUE,
      method           == "sa",
      split_type       == "full_sample"
    )

  AbsAvgPostPeriodEffect <- function(outcome) {
    canonical %>%
      filter(.data$outcome == !!outcome, event_time %in% 1:5) %>%
      pull(estimate) %>%
      mean() %>%
      abs()
  }

  AvgPRsOpenedDecline <- AbsAvgPostPeriodEffect("pull_request_opened")
  AvgPRsMergedDecline <- AbsAvgPostPeriodEffect("pull_request_merged")
  AvgReleasesDecline  <- AbsAvgPostPeriodEffect("overall_new_release_count")
  AvgMinDecline       <- min(AvgPRsOpenedDecline, AvgPRsMergedDecline, AvgReleasesDecline)
  AvgMaxDecline       <- max(AvgPRsOpenedDecline, AvgPRsMergedDecline, AvgReleasesDecline)

  autofill_path <- "output/autofill/event_study_autofill.tex"
  dir_create(dirname(autofill_path))
  writeLines(c(
    sprintf("\\newcommand{\\AvgPRsOpenedDecline}{%.2f}", AvgPRsOpenedDecline),
    sprintf("\\newcommand{\\AvgPRsMergedDecline}{%.2f}", AvgPRsMergedDecline),
    sprintf("\\newcommand{\\AvgReleasesDecline}{%.2f}", AvgReleasesDecline),
    sprintf("\\newcommand{\\AvgMinDecline}{%.2f}", AvgMinDecline),
    sprintf("\\newcommand{\\AvgMaxDecline}{%.2f}", AvgMaxDecline)
  ), autofill_path)
}

Main()
