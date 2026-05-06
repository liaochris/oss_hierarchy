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

          PlotPCCorrelationHeatmap(forest_data$df, outdir_ds)
        }
      }
    }
  }
  invisible(NULL)
}

PlotPCCorrelationHeatmap <- function(df_cf, outdir_ds) {
  pc_cols <- colnames(df_cf)[grepl("_principal_component1$", colnames(df_cf))]

  corr_tidy <- df_cf %>%
    select(all_of(pc_cols)) %>%
    rename(!!!setNames(pc_cols, PC_LABELS[pc_cols])) %>%
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
