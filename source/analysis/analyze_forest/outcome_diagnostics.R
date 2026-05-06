library(tidyverse)
library(arrow)
library(fs)
library(knitr)

source("source/analysis/analyze_forest/helpers.R")

INDIR_CF <- "output/analysis/event_study_forest"
OUTDIR   <- "output/analysis/event_study_forest"

Main <- function() {
  for (importance_type in IMPORTANCE_TYPES) {
    for (qualified_sample in QUALIFIED_SAMPLES) {
      for (control_group in CONTROL_GROUPS) {
        for (rolling_panel in ROLLING_LABELS) {
          outdir_ds <- file.path(OUTDIR, importance_type, rolling_panel, qualified_sample, control_group)
          dir_create(outdir_ds, recurse = TRUE)

          forest_data <- LoadForestData(INDIR_CF, importance_type, rolling_panel, qualified_sample, control_group)
          if (is.null(forest_data)) next

          panel <- LoadPanelData(importance_type, rolling_panel, qualified_sample, control_group)
          if (nrow(panel) == 0) next

          # Restrict the panel to repos that appear in the causal forest output
          analysis_panel <- panel %>% filter(repo_name %in% forest_data$df$repo_name)

          outcome_stats <- ComputeOutcomeStats(analysis_panel)
          pct_treated   <- mean(analysis_panel$treatment_group != 0, na.rm = TRUE)
          num_projects  <- n_distinct(analysis_panel %>%
                             filter(time_index == quasi_treatment_group) %>%
                             pull(repo_name))
          num_periods   <- nrow(analysis_panel)

          BuildLatexAndSave(outcome_stats, pct_treated, num_projects, num_periods, outdir_ds)
        }
      }
    }
  }
  invisible(NULL)
}

LoadPanelData <- function(importance_type, rolling_panel, qualified_sample, control_group) {
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

Main()
