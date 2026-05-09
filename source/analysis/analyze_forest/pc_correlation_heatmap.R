library(tidyverse)
library(arrow)
library(fs)

source("source/analysis/analyze_forest/helpers.R")

INDIR_FOREST <- "output/analysis/event_study_forest"
OUTDIR       <- "output/analysis/analyze_forest"

Main <- function() {
  for (importance_type in IMPORTANCE_TYPES) {
    for (qualified_sample in QUALIFIED_SAMPLES) {
      for (control_group in CONTROL_GROUPS) {
        for (rolling_panel in ROLLING_LABELS) {
          for (norm_option in NORM_OPTIONS) {
            norm_label <- ifelse(norm_option, "norm", "raw")
            outdir_ds <- file.path(OUTDIR, importance_type, rolling_panel, qualified_sample, control_group, norm_label)
            dir_create(outdir_ds, recurse = TRUE)

            forest_results_data <- LoadForestResults(INDIR_FOREST, importance_type, rolling_panel, qualified_sample, control_group, norm_label)
            PlotPCScoreCorrelationHeatmap(forest_results_data$df, outdir_ds)
          }
        }
      }
    }
  }
  invisible(NULL)
}

PlotPCScoreCorrelationHeatmap <- function(forest_results, outdir_ds) {
  pc_score_cols <- colnames(forest_results)[grepl("_pc_score$", colnames(forest_results))]

  corr_tidy <- forest_results %>%
    select(all_of(pc_score_cols)) %>%
    rename(!!!setNames(pc_score_cols, PC_LABELS[pc_score_cols])) %>%
    cor(use = "pairwise.complete.obs") %>%
    as.data.frame() %>%
    tibble::rownames_to_column("var1") %>%
    pivot_longer(-var1, names_to = "var2", values_to = "correlation") %>%
    mutate(label      = sprintf("%.2f", correlation),
           text_color = ifelse(abs(correlation) > 0.4, "white", "black"))

  ggplot(corr_tidy, aes(x = var1, y = var2, fill = correlation)) +
    geom_tile() +
    geom_text(aes(label = label, color = text_color), size = 5) +
    scale_color_identity() +
    scale_fill_gradient2(limits = c(-1, 1), midpoint = 0) +
    coord_fixed(0.8) +
    theme_minimal(base_size = 4) +
    theme(plot.margin  = margin(0, 0, 0, 0),
          axis.text.x  = element_text(angle = 30, hjust = 1, size = 16),
          axis.text.y  = element_text(size = 16),
          legend.title = element_text(size = 16),
          legend.text  = element_text(size = 10)) +
    labs(x = "", y = "", fill = "Correlation\n")

  ggsave(file.path(outdir_ds, "heatmap_corr.png"), width = 10, height = 6, dpi = 300)
}

Main()
