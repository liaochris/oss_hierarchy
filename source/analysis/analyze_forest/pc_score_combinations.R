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

            pc_score_cols <- colnames(forest_results_data$df)[grepl("_pc_score$", colnames(forest_results_data$df))]
            binarized <- BinarizePCScores(forest_results_data$sub_dfs, pc_score_cols)
            df_bins   <- binarized$df %>% drop_na(all_of(pc_score_cols))

            combo_summary <- df_bins %>%
              group_by(across(all_of(pc_score_cols))) %>%
              summarize(att_doubly_robust_mean      = mean(att_doubly_robust, na.rm = TRUE),
                        high_resilience  = mean(att_doubly_robust_group == "high", na.rm = TRUE),
                        count            = n(),
                        .groups          = "drop") %>%
              arrange(-att_doubly_robust_mean) %>%
              mutate(rank = row_number())

            PlotHighLowGrid(combo_summary, pc_score_cols)
            ggsave(file.path(outdir_ds, "pc_score_combo_high_low_grid.png"), width = 9, height = 16, dpi = 300)

            CreatePCScoreComboTables(df_bins, pc_score_cols, outdir_ds)

            p_gradient <- PlotPCScoreGradientGrid(combo_summary, pc_score_cols)
            ggsave(file.path(outdir_ds, "pc_score_combo_att_gradient.png"), p_gradient, width = 3, height = 12, dpi = 300)
          }
        }
      }
    }
  }
  invisible(NULL)
}

PlotHighLowGrid <- function(combo_summary, pc_score_cols) {
  rank_levels <- as.character(1:32)

  pc_tile_plot_data <- combo_summary %>%
    select(rank, all_of(pc_score_cols)) %>%
    mutate(rank = as.character(rank)) %>%
    pivot_longer(cols = all_of(pc_score_cols), names_to = "pc_score_group", values_to = "raw_value") %>%
    mutate(rank     = factor(rank, levels = rank_levels),
           pc_score_group = factor(pc_score_group, levels = pc_score_cols)) %>%
    group_by(pc_score_group) %>%
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

  n_pc_score_groups <- length(pc_score_cols)

  ggplot(pc_tile_plot_data, aes(x = pc_score_group, y = rank, fill = level)) +
    geom_tile(color = "white", linewidth = 1.5) +
    geom_segment(inherit.aes = FALSE,
                 aes(x = 0.5, xend = n_pc_score_groups + 0.5, y = 16.5, yend = 16.5),
                 color = "red", linewidth = 1.5, alpha = 0.7) +
    annotate("text", x = n_pc_score_groups + 1, y = 32, label = "Largest\nDoubly-Robust ATT",
             hjust = 0, vjust = 0.5, size = 8.5, lineheight = 0.9) +
    annotate("text", x = n_pc_score_groups + 1, y = 2,  label = "Smallest\nDoubly-Robust ATT",
             hjust = 0, vjust = 0.5, size = 8.5, lineheight = 0.9) +
    annotate("segment", x = n_pc_score_groups + 0.95, xend = n_pc_score_groups + 0.6, y = 32, yend = 32,
             arrow = arrow(length = unit(0.3, "cm"), type = "open"), linewidth = 0.5) +
    annotate("segment", x = n_pc_score_groups + 0.95, xend = n_pc_score_groups + 0.6, y = 2,  yend = 2,
             arrow = arrow(length = unit(0.3, "cm"), type = "open"), linewidth = 0.5) +
    scale_fill_manual(values = c("Low" = "lightblue", "High" = "darkblue"),
                      name = "Coarsened\nPC Score") +
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
        lbl <- PC_LABELS[xx]; if (!is.na(lbl)) lbl else xx
      }, FUN.VALUE = character(1)),
      expand  = expansion(add = c(0.2, 0.2))
    )
}

CreatePCScoreComboTables <- function(df_bins, pc_split_cols, outdir_ds) {
  for (k in c(2, 3)) {
    combos <- combn(pc_split_cols, k, simplify = FALSE)

    combo_rows_all <- lapply(combos, function(vars) {
      df_bins %>%
        group_by(across(all_of(vars))) %>%
        summarize(att_doubly_robust_mean = mean(att_doubly_robust, na.rm = TRUE), count = n(), .groups = "drop") %>%
        arrange(-att_doubly_robust_mean) %>%
        mutate(rank = row_number(),
               pattern = apply(as.matrix(pick(all_of(vars))), 1, paste, collapse = "-"),
               pc_score_subset = paste(vars, collapse = " x ")) %>%
        select(pc_score_subset, pattern, rank, att_doubly_robust_mean, count)
    })
    combo_all_path <- file.path(outdir_ds, paste0("pc_score_combo_k", k, "_table.csv"))
    SaveData(bind_rows(combo_rows_all) %>% arrange(-att_doubly_robust_mean),
             c("pc_score_subset", "pattern"),
             combo_all_path,
             sub("\\.csv$", ".log", combo_all_path))

    combo_rows_high <- lapply(combos, function(vars) {
      grp      <- df_bins %>%
        group_by(across(all_of(vars))) %>%
        summarize(att_doubly_robust_mean = mean(att_doubly_robust, na.rm = TRUE), count = n(), .groups = "drop")
      all_high <- grp %>% filter(if_all(all_of(vars), ~ .x == "high"))
      others   <- grp %>% filter(!if_all(all_of(vars), ~ .x == "high"))
      if (nrow(all_high) == 0) return(NULL)
      att_others <- if (nrow(others) > 0 && sum(others$count) > 0)
        sum(others$att_doubly_robust_mean * others$count) / sum(others$count) else NA_real_
      tibble(pc_score_subset = paste(vars, collapse = " x "),
             pc_score_label = paste(PC_LABELS[vars], collapse = " $\\times$ "),
             att_high = all_high$att_doubly_robust_mean,
             att_others_wavg = att_others,
             difference = all_high$att_doubly_robust_mean - att_others,
             count_high = all_high$count)
    })
    combo_high_path <- file.path(outdir_ds, paste0("pc_score_combo_k", k, "_high_table.csv"))
    SaveData(bind_rows(combo_rows_high) %>% arrange(-difference),
             "pc_score_subset",
             combo_high_path,
             sub("\\.csv$", ".log", combo_high_path))

    top3_pc_combos <- bind_rows(combo_rows_high) %>% arrange(-difference) %>% head(3)
    txt_lines <- c(
      paste0("<tab:pc_score_combo_k", k, "_top3>"),
      apply(top3_pc_combos, 1, function(r) {
        paste(
          trimws(r["pc_score_label"]),
          formatC(as.numeric(r["difference"]),      format = "f", digits = 2),
          formatC(as.numeric(r["att_high"]),        format = "f", digits = 2),
          formatC(as.numeric(r["att_others_wavg"]), format = "f", digits = 2),
          sep = "\t"
        )
      })
    )
    writeLines(txt_lines, file.path(outdir_ds, paste0("pc_score_combo_k", k, "_top3.txt")))
  }
}

PlotPCScoreGradientGrid <- function(combo_summary, pc_split_cols, fixed_rank_order = NULL) {
  mat <- as.matrix(combo_summary[pc_split_cols])
  combo_summary <- combo_summary %>%
    mutate(combo_key  = apply(mat, 1, function(r) paste(toupper(substr(r, 1, 1)), collapse = "-")),
           combo_rank = row_number(desc(att_doubly_robust_mean)))

  df_plot <- if (!is.null(fixed_rank_order)) {
    combo_summary %>% mutate(row_pos = match(combo_key, fixed_rank_order)) %>% filter(!is.na(row_pos))
  } else {
    combo_summary %>% mutate(row_pos = combo_rank)
  }

  ggplot(df_plot, aes(x = 1, y = row_pos, fill = combo_rank)) +
    geom_tile(color = "grey80", linewidth = 0.3, width = 1, height = 1) +
    scale_fill_gradient(low = "white", high = "black", limits = c(1, 32), guide = "none") +
    scale_y_reverse(breaks = c(1, 8, 16, 24, 32), expand = c(0, 0)) +
    scale_x_continuous(breaks = NULL, expand = c(0, 0)) +
    coord_fixed(xlim = c(0.5, 1.5), ylim = c(32.5, 0.5)) +
    labs(x = NULL, y = "Rank") +
    theme_minimal(base_size = 7) +
    theme(axis.text.y  = element_text(size = 6),
          axis.title.y = element_text(size = 7, angle = 90),
          panel.grid   = element_blank(),
          axis.ticks   = element_blank(),
          plot.margin  = margin(2, 2, 2, 2, "pt"))
}

Main()
