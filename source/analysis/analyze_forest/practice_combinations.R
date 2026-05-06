library(tidyverse)
library(arrow)
library(fs)

source("source/analysis/analyze_forest/helpers.R")

INDIR_CF <- "output/analysis/event_study_forest"
OUTDIR   <- "output/analysis/event_study_forest"

PRACTICE_VARS <- c(
  "collaboration_principal_component1",
  "shared_knowledge_principal_component1",
  "discussion_quality_principal_component1",
  "investment_in_new_talent_principal_component1",
  "problem_solving_routines_principal_component1"
)

PRACTICE_LABELS <- c(
  "collaboration_principal_component1"            = "Collaboration",
  "shared_knowledge_principal_component1"         = "Knowledge level",
  "discussion_quality_principal_component1"       = "Discussion quality",
  "investment_in_new_talent_principal_component1" = "Investment in new talent",
  "problem_solving_routines_principal_component1" = "Problem-solving routines"
)

Main <- function() {
  for (importance_type in IMPORTANCE_TYPES) {
    for (qualified_sample in QUALIFIED_SAMPLES) {
      for (control_group in CONTROL_GROUPS) {
        for (rolling_panel in ROLLING_LABELS) {
          outdir_ds <- file.path(OUTDIR, importance_type, rolling_panel, qualified_sample, control_group)
          dir_create(outdir_ds, recurse = TRUE)

          forest_data <- LoadForestData(INDIR_CF, importance_type, rolling_panel, qualified_sample, control_group)
          if (is.null(forest_data)) next

          pc_cols  <- colnames(forest_data$df)[grepl("_principal_component1$", colnames(forest_data$df))]
          pc_medians <- sapply(pc_cols, function(col) median(forest_data$df[[col]], na.rm = TRUE))

          df_bins <- forest_data$df %>%
            mutate(across(all_of(pc_cols),
                          ~ ifelse(.x > pc_medians[cur_column()], "high", "low"),
                          .names = "{.col}"))

          df_summary <- df_bins %>%
            group_by(across(all_of(pc_cols))) %>%
            summarize(att_dr_mean      = mean(att_dr, na.rm = TRUE),
                      high_resilience  = mean(att_dr_group == "high", na.rm = TRUE),
                      count            = n(),
                      .groups          = "drop") %>%
            arrange(-att_dr_mean) %>%
            mutate(rank = row_number())

          PlotHighLowGrid(df_summary, pc_cols)
          ggsave(file.path(outdir_ds, "practice_combo.png"), width = 9, height = 16, dpi = 300)

          CreatePracticeComboTables(df_bins, pc_cols, outdir_ds)

          p_gradient <- PlotGradientGrid(df_summary, pc_cols)
          ggsave(file.path(outdir_ds, "practice_combo_gradient.png"), p_gradient, width = 3, height = 12, dpi = 300)
        }
      }
    }
  }
  invisible(NULL)
}

PlotHighLowGrid <- function(df_summary, practice_vars) {
  rank_levels <- as.character(1:32)

  df_tiles <- df_summary %>%
    select(rank, all_of(practice_vars)) %>%
    mutate(rank = as.character(rank)) %>%
    pivot_longer(cols = all_of(practice_vars), names_to = "practice", values_to = "raw_value") %>%
    mutate(rank     = factor(rank, levels = rank_levels),
           practice = factor(practice, levels = practice_vars)) %>%
    group_by(practice) %>%
    mutate(level = {
      if (all(is.na(raw_value))) {
        factor("low", levels = c("low", "high"))
      } else if (all(raw_value %in% c("low", "high", NA))) {
        factor(as.character(raw_value), levels = c("low", "high"))
      } else {
        med <- median(as.numeric(raw_value), na.rm = TRUE)
        factor(ifelse(as.numeric(raw_value) > med, "high", "low"), levels = c("low", "high"))
      }
    }) %>%
    ungroup() %>%
    mutate(level = ifelse(level == "high", "High", "Low"))

  n_practices <- length(practice_vars)

  ggplot(df_tiles, aes(x = practice, y = rank, fill = level)) +
    geom_tile(color = "white", size = 1.5) +
    geom_segment(inherit.aes = FALSE,
                 aes(x = 0.5, xend = n_practices + 0.5, y = 16.5, yend = 16.5),
                 color = "red", linewidth = 1.5, alpha = 0.7) +
    annotate("text", x = n_practices + 1, y = 32, label = "Largest\nDoubly-Robust ATT",
             hjust = 0, vjust = 0.5, size = 8.5, lineheight = 0.9) +
    annotate("text", x = n_practices + 1, y = 2,  label = "Smallest\nDoubly-Robust ATT",
             hjust = 0, vjust = 0.5, size = 8.5, lineheight = 0.9) +
    annotate("segment", x = n_practices + 0.95, xend = n_practices + 0.6, y = 32, yend = 32,
             arrow = arrow(length = unit(0.3, "cm"), type = "open"), linewidth = 0.5) +
    annotate("segment", x = n_practices + 0.95, xend = n_practices + 0.6, y = 2,  yend = 2,
             arrow = arrow(length = unit(0.3, "cm"), type = "open"), linewidth = 0.5) +
    scale_fill_manual(values = c("Low" = "lightblue", "High" = "darkblue"),
                      name = "Coarsened\nPractice Score") +
    labs(x = NULL, y = NULL) +
    theme_minimal(base_size = 14) +
    theme(axis.text.y         = element_blank(),
          axis.text.x         = element_text(angle = 30, hjust = 1, vjust = 1, size = 24),
          axis.ticks          = element_blank(),
          axis.ticks.length.x.bottom = unit(-0.4, "cm"),
          panel.grid          = element_blank(),
          legend.title        = element_text(size = 24),
          legend.text         = element_text(size = 24),
          plot.margin         = unit(c(3.0, 4.5, 4.5, 7), "lines"),
          legend.margin       = margin(0, 0, 0, -20),
          legend.box.spacing  = unit(0.01, "cm")) +
    coord_fixed(ratio = 0.45, clip = "off") +
    scale_y_discrete(limits = rev(rank_levels)) +
    scale_x_discrete(
      labels  = function(x) vapply(x, function(xx) {
        lbl <- PRACTICE_LABELS[xx]; if (!is.na(lbl)) lbl else xx
      }, FUN.VALUE = character(1)),
      expand  = expansion(add = c(0.2, 0.2))
    )
}

Main()
