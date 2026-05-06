library(tidyverse)
library(arrow)
library(fs)

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

Main()
