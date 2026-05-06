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
          PlotOrgTimeline(analysis_panel, outdir_ds)
        }
      }
    }
  }
  invisible(NULL)
}

PlotOrgTimeline <- function(panel, outdir_ds) {
  quasi_dates    <- panel %>%
    group_by(repo_name) %>%
    mutate(quasi_treatment_date = time_period[time_index == quasi_treatment_group][1]) %>%
    ungroup() %>%
    select(repo_name, quasi_treatment_date) %>%
    distinct() %>%
    pull(quasi_treatment_date)

  treatment_dates <- panel %>%
    filter(treatment_group != 0) %>%
    select(repo_name, treatment_date) %>%
    distinct() %>%
    pull(treatment_date)

  counts <- bind_rows(
    tibble(date = quasi_dates,     type = "Quasi-Treatment Date"),
    tibble(date = treatment_dates, type = "Treatment Date")
  ) %>%
    group_by(date, type) %>%
    summarise(count = n(), .groups = "drop") %>%
    mutate(type = factor(type, levels = c("Quasi-Treatment Date", "Treatment Date")))

  ggplot(counts, aes(x = date, y = count, fill = type)) +
    geom_col(color = "black", linewidth = 0.3, position = "identity", alpha = 1) +
    scale_x_date(date_breaks = "1 year", date_labels = "%Y", expand = c(0, 0)) +
    labs(x = "Date", y = "# of Organizations", fill = "Appearance Type") +
    theme_minimal(base_size = 21) +
    theme(text        = element_text(size = 21),
          axis.text.x = element_text(angle = 45, hjust = 1),
          legend.position = "top")

  ggsave(file.path(outdir_ds, "hist_org_count.png"), width = 10, height = 5)
}

Main()
