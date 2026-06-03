library(tidyverse)
library(arrow)
library(fs)

source("source/analysis/summ_stats/helpers.R")

OUTDIR <- "output/analysis/summ_stats"

Main <- function() {
  for (importance_type in IMPORTANCE_TYPES) {
    for (qualified_sample in QUALIFIED_SAMPLES) {
      for (control_group in CONTROL_GROUPS) {
        for (rolling_panel in ROLLING_LABELS) {
          outdir_ds <- file.path(OUTDIR, importance_type, rolling_panel, qualified_sample, control_group, "raw")
          dir_create(outdir_ds, recurse = TRUE)

          analysis_panel <- LoadAnalysisPanel(importance_type, rolling_panel, qualified_sample, control_group)
          if (nrow(analysis_panel) == 0) next

          outcome_stats <- ComputeOutcomeStats(analysis_panel)
          pct_treated   <- mean(analysis_panel$treatment_group != 0, na.rm = TRUE)
          num_repos     <- n_distinct(analysis_panel %>%
                            filter(time_index == quasi_treatment_group) %>%
                            pull(repo_name))
          num_periods   <- nrow(analysis_panel)

          BuildOutcomeSummaryAutofill(outcome_stats, pct_treated, num_repos, num_periods, outdir_ds)
        }
      }
    }
  }
  invisible(NULL)
}

ComputeOutcomeStats <- function(panel_df, outcome_cols = unlist(lapply(outcome_variables, function(x) x$run))) {
  map_df(outcome_cols, function(col) {
    vec <- panel_df[[col]]
    tibble(outcome = col, median = median(vec, na.rm = TRUE), mean = mean(vec, na.rm = TRUE), sd = sd(vec, na.rm = TRUE))
  })
}

BuildOutcomeSummaryAutofill <- function(outcome_stats, pct_treated, num_repos, num_periods, outdir_ds) {
  avg_periods <- if (num_repos > 0) as.numeric(num_periods) / as.numeric(num_repos) else NA_real_

  outcome_rows <- apply(outcome_stats, 1, function(r) {
    paste(
      formatC(as.numeric(r["mean"]),   format = "f", digits = 3),
      formatC(as.numeric(r["median"]), format = "f", digits = 3),
      formatC(as.numeric(r["sd"]),     format = "f", digits = 3),
      sep = "\t"
    )
  })

  summary_rows <- c(
    formatC(pct_treated, format = "f", digits = 3),
    as.character(num_repos),
    formatC(avg_periods, format = "f", digits = 2)
  )

  writeLines(
    c("<tab:outcome_summary>", outcome_rows, summary_rows),
    file.path(outdir_ds, "outcome_summary.txt")
  )
}

Main()
