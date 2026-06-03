library(tidyverse)
library(arrow)
library(fs)

source("source/analysis/analyze_forest/helpers.R")

INDIR_FOREST    <- "output/analysis/event_study_forest"
INDIR_FOREST_DS <- "drive/output/analysis/event_study_forest"
OUTDIR          <- "output/analysis/analyze_forest"
TREE_DEPTH_MAX  <- 6

Main <- function() {
  for (importance_type in IMPORTANCE_TYPES) {
    for (qualified_sample in QUALIFIED_SAMPLES) {
      for (control_group in CONTROL_GROUPS) {
        for (rolling_panel in ROLLING_LABELS) {
          for (norm_option in NORM_OPTIONS) {
            norm_label <- ifelse(norm_option, "norm", "raw")
            outdir_ds      <- file.path(OUTDIR, importance_type, rolling_panel, qualified_sample, control_group, norm_label)
            dir_create(outdir_ds, recurse = TRUE)

            forest_results_data <- LoadForestResults(INDIR_FOREST, importance_type, rolling_panel, qualified_sample, control_group, norm_label)
            if (is.null(forest_results_data)) next

            pc_score_cols <- colnames(forest_results_data$df)[grepl("_pc_score$", colnames(forest_results_data$df))]
            binarized              <- BinarizePCScores(forest_results_data$sub_dfs, pc_score_cols)
            df_binarized_pc_scores <- binarized$df
            sub_bins               <- binarized$sub_dfs

            pc_combo_att_summary <- df_binarized_pc_scores %>%
              group_by(across(all_of(pc_score_cols))) %>%
              summarize(att_doubly_robust_mean = mean(att_doubly_robust, na.rm = TRUE), count = n(), .groups = "drop") %>%
              arrange(-att_doubly_robust_mean) %>%
              mutate(rank = row_number())

            fold_summaries <- BuildFoldSummaries(
              forest_results_data$sub_dfs, sub_bins, pc_score_cols,
              importance_type, rolling_panel, control_group, norm_label
            )
            if (length(fold_summaries) == 0) next

            PlotCrossForestGrid(pc_combo_att_summary, fold_summaries, pc_score_cols, outdir_ds)
            ComputeFoldCorrelations(pc_combo_att_summary, fold_summaries, pc_score_cols, outdir_ds)

            agg_fold_paths <- unlist(lapply(names(forest_results_data$sub_dfs), function(s)
              FoldRdsPaths(importance_type, rolling_panel, s, control_group, norm_label)))
            ExportVariableImportanceTex(agg_fold_paths, outdir_ds)

            binary_forest_paths <- vapply(names(forest_results_data$sub_dfs), function(s)
              FullSampleBinaryRdsPath(importance_type, rolling_panel, s, control_group, norm_label), character(1))
            ExportTreeDepthTablefill(binary_forest_paths, outdir_ds)
          }
        }
      }
    }
  }
  invisible(NULL)
}

BuildFoldSummaries <- function(sub_dfs, sub_bins, pc_score_cols,
                               importance_type, rolling_panel, control_group, norm_label) {
  lapply(seq_len(N_FOLDS), function(fold_i) {
    fold_rows <- Map(function(s, sub_cont, sub_bin) {
      path   <- FoldRdsPaths(importance_type, rolling_panel, s, control_group, norm_label)[fold_i]
      forest <- tryCatch(readRDS(path), error = function(e) NULL)
      if (is.null(forest)) return(NULL)

      x_all      <- as.matrix(sub_cont %>% select(all_of(colnames(forest$X.orig))))
      tau_hat    <- predict(forest, newdata = x_all, drop = TRUE)$predictions
      tau_scalar <- if (is.matrix(tau_hat)) rowMeans(tau_hat, na.rm = TRUE) else as.numeric(tau_hat)
      sub_bin %>% mutate(fold_att = tau_scalar)
    }, names(sub_dfs), sub_dfs, sub_bins)

    fold_rows <- Filter(Negate(is.null), fold_rows)
    if (length(fold_rows) == 0) return(NULL)

    bind_rows(fold_rows) %>%
      group_by(across(all_of(pc_score_cols))) %>%
      summarize(att_doubly_robust_mean = mean(fold_att, na.rm = TRUE), count = n(), .groups = "drop") %>%
      arrange(-att_doubly_robust_mean) %>%
      mutate(rank = row_number())
  }) %>% Filter(Negate(is.null), .)
}

FoldRdsPaths <- function(importance_type, rolling_panel, sample, control_group,
                         norm_label, covar_type_dir = "pc_score") {
  file.path(INDIR_FOREST_DS, importance_type, rolling_panel, sample, control_group, covar_type_dir,
            norm_label, paste0("event_study_forest_", FOREST_TRAINING_OUTCOME,
                   "_fold", seq_len(N_FOLDS), ".rds"))
}

FullSampleBinaryRdsPath <- function(importance_type, rolling_panel, sample, control_group, norm_label) {
  file.path(INDIR_FOREST_DS, importance_type, rolling_panel, sample, control_group, "pc_score_binary",
            norm_label, paste0("event_study_forest_", FOREST_TRAINING_OUTCOME, ".rds"))
}

TreeDepth <- function(nodes, idx = 1) {
  node <- nodes[[idx]]
  if (isTRUE(node$is_leaf)) return(0L)
  1L + max(TreeDepth(nodes, node$left_child), TreeDepth(nodes, node$right_child))
}

ExportTreeDepthTablefill <- function(binary_forest_paths, outdir_ds) {
  existing <- binary_forest_paths[file.exists(binary_forest_paths)]
  if (length(existing) == 0) return(invisible(NULL))

  depths <- unlist(lapply(existing, function(path) {
    forest <- readRDS(path)
    vapply(seq_len(forest[["_num_trees"]]), function(i) TreeDepth(get_tree(forest, i)$nodes), integer(1))
  }))

  depth_levels <- 1:TREE_DEPTH_MAX
  proportion   <- tabulate(depths, nbins = TREE_DEPTH_MAX)[depth_levels] / length(depths)
  writeLines(c("<tab:tree_depth>", sprintf("%.2f", proportion)),
             file.path(outdir_ds, "tree_depth.txt"))
  invisible(proportion)
}

PlotCrossForestGrid <- function(combo_summary, fold_summaries, pc_split_cols, outdir_ds) {
  ComboKeys <- function(df) {
    mat <- as.matrix(df[pc_split_cols])
    apply(mat, 1, function(r) paste(toupper(substr(r, 1, 1)), collapse = "-"))
  }
  full_keys <- ComboKeys(fold_summaries[[1]] %>% arrange(-att_doubly_robust_mean))

  df_all <- bind_rows(lapply(seq_along(fold_summaries), function(i) {
    fs <- fold_summaries[[i]]
    fs %>%
      mutate(combo_key  = ComboKeys(.),
             combo_rank = rank(-att_doubly_robust_mean, ties.method = "first"),
             row_pos    = match(combo_key, full_keys),
             x_pos      = i) %>%
      filter(!is.na(row_pos)) %>%
      select(x_pos, row_pos, combo_rank)
  }))

  n_folds <- length(fold_summaries)
  cross_forest_grid_plot <- ggplot(df_all, aes(x = x_pos, y = row_pos, fill = combo_rank)) +
    geom_tile(color = "grey80", linewidth = 0.2, width = 1, height = 1) +
    scale_fill_gradient(low = "white", high = "black", limits = c(1, 32), guide = "none") +
    scale_x_continuous(breaks = seq_len(n_folds), labels = paste0("F", seq_len(n_folds)), expand = c(0, 0)) +
    scale_y_reverse(breaks = c(1, 8, 16, 24, 32), expand = c(0, 0)) +
    coord_fixed(xlim = c(0.5, n_folds + 0.5), ylim = c(32.5, 0.5)) +
    labs(x = NULL, y = "Rank") +
    theme_minimal(base_size = 7) +
    theme(axis.text.x  = element_text(size = 6),
          axis.text.y  = element_text(size = 6),
          axis.title.y = element_text(size = 7, angle = 90),
          panel.grid   = element_blank(),
          axis.ticks   = element_blank(),
          plot.margin  = margin(2, 2, 2, 2, "pt"))

  tile_in <- 0.18
  ggsave(file.path(outdir_ds, "fold_rank_stability_grid.png"), cross_forest_grid_plot,
         width  = n_folds * tile_in + 0.6,
         height = 32 * tile_in + 0.4,
         dpi = 300)
}

ComputeFoldCorrelations <- function(combo_summary, fold_summaries, pc_split_cols, outdir_ds) {
  ComboKeys <- function(df) {
    mat <- as.matrix(df[pc_split_cols])
    apply(mat, 1, function(r) paste(toupper(substr(r, 1, 1)), collapse = "-"))
  }
  full_keys <- ComboKeys(combo_summary %>% arrange(-att_doubly_robust_mean))

  att_mat  <- sapply(fold_summaries, function(fs) {
    fs$combo_key <- ComboKeys(fs)
    fs[match(full_keys, fs$combo_key), ]$att_doubly_robust_mean
  })
  rank_mat <- apply(att_mat, 2, rank, na.last = "keep")

  corr_att  <- cor(att_mat,  method = "pearson",  use = "pairwise.complete.obs")
  corr_rank <- cor(rank_mat, method = "spearman", use = "pairwise.complete.obs")

  PlotCorrHeatmap <- function(corr_mat, title) {
    n_folds <- nrow(corr_mat)
    rownames(corr_mat) <- colnames(corr_mat) <- paste0("F", seq_len(n_folds))
    corr_mat %>%
      as.data.frame() %>%
      tibble::rownames_to_column("fold1") %>%
      pivot_longer(-fold1, names_to = "fold2", values_to = "corr") %>%
      mutate(label = sprintf("%.2f", corr)) %>%
      ggplot(aes(x = fold1, y = fold2, fill = corr)) +
      geom_tile(color = "white") +
      geom_text(aes(label = label), size = 3) +
      scale_fill_gradient2(limits = c(-1, 1), midpoint = 0,
                           low = "blue", mid = "white", high = "red", name = "r") +
      labs(title = title, x = NULL, y = NULL) +
      theme_minimal(base_size = 10) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
      coord_fixed()
  }

  ggsave(file.path(outdir_ds, "fold_correlation_att.png"),  PlotCorrHeatmap(corr_att,  "Fold Correlation (ATT)"),  width = 8, height = 7, dpi = 300)
  ggsave(file.path(outdir_ds, "fold_correlation_rank.png"), PlotCorrHeatmap(corr_rank, "Fold Correlation (Rank)"), width = 8, height = 7, dpi = 300)
}

ExportVariableImportanceTex <- function(fold_rds_paths, outdir_ds) {
  existing <- fold_rds_paths[file.exists(fold_rds_paths)]
  if (length(existing) == 0) return(invisible(NULL))

  varimp_df <- bind_rows(lapply(existing, function(path) {
    forest <- readRDS(path)
    vi     <- variable_importance(forest)
    tibble(variable = colnames(forest$X.orig), importance = as.numeric(vi))
  })) %>%
    group_by(variable) %>%
    summarize(importance = mean(importance, na.rm = TRUE), .groups = "drop") %>%
    arrange(-importance) %>%
    mutate(rank  = row_number(),
           label = ifelse(variable %in% names(PC_LABELS),
                          paste0(PC_LABELS[variable], " (PC Score)"), variable))

  out_tbl    <- varimp_df %>% transmute(Rank = rank, Variable = label, `Mean Split Share` = sprintf("%.2f", importance))
  latex_body <- as.character(knitr::kable(out_tbl, format = "latex", booktabs = TRUE, linesep = ""))
  writeLines(latex_body, file.path(outdir_ds, "variable_importance.tex"))
  invisible(varimp_df)
}

Main()
