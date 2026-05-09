library(tidyverse)
library(arrow)
library(fs)

source("source/analysis/analyze_forest/helpers.R")

INDIR_FOREST    <- "output/analysis/event_study_forest"
INDIR_FOREST_DS <- "drive/output/analysis/event_study_forest"
OUTDIR          <- "output/analysis/analyze_forest"

Main <- function() {
  for (importance_type in IMPORTANCE_TYPES) {
    # Only aggregated samples have multiple folds to compare across
    for (qualified_sample in names(AGGREGATED_SAMPLES)) {
      for (control_group in CONTROL_GROUPS) {
        for (rolling_panel in ROLLING_LABELS) {
          for (norm_option in NORM_OPTIONS) {
            norm_label     <- ifelse(norm_option, "norm", "raw")
            rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
            outdir_ds      <- file.path(OUTDIR, importance_type, rolling_panel, qualified_sample, control_group, norm_label)
            dir_create(outdir_ds, recurse = TRUE)

            forest_results_data <- LoadForestResults(INDIR_FOREST, importance_type, rolling_panel, qualified_sample, control_group, norm_label)
            if (is.null(forest_results_data)) next

            pc_score_cols <- colnames(forest_results_data$df)[grepl("_pc_score$", colnames(forest_results_data$df))]
            binarized <- BinarizePCScores(forest_results_data$df, pc_score_cols)
            df_bins   <- binarized$df
            sub_bins  <- lapply(forest_results_data$sub_dfs, function(sub_df)
              sub_df %>% mutate(across(all_of(pc_score_cols),
                                       ~ ifelse(.x > binarized$medians[cur_column()], "high", "low"))))

            combo_summary <- df_bins %>%
              group_by(across(all_of(pc_score_cols))) %>%
              summarize(att_dr_mean = mean(att_dr, na.rm = TRUE), count = n(), .groups = "drop") %>%
              arrange(-att_dr_mean) %>%
              mutate(rank = row_number())

            fold_summaries <- BuildFoldSummaries(
              forest_results_data$sub_dfs, sub_bins, pc_score_cols, rolling_period,
              importance_type, rolling_panel, control_group, norm_label
            )
            if (length(fold_summaries) == 0) next

            PlotCrossForestGrid(combo_summary, fold_summaries, pc_score_cols, outdir_ds)
            ComputeFoldCorrelations(combo_summary, fold_summaries, pc_score_cols, outdir_ds)

            agg_fold_paths <- unlist(lapply(names(forest_results_data$sub_dfs), function(s)
              FoldRdsPaths(importance_type, rolling_panel, s, control_group, rolling_period, norm_label)))
            ExportVariableImportanceTex(agg_fold_paths, outdir_ds)
          }
        }
      }
    }
  }
  invisible(NULL)
}

BuildFoldSummaries <- function(sub_dfs, sub_bins, pc_score_cols, rolling_period,
                               importance_type, rolling_panel, control_group, norm_label) {
  lapply(seq_len(N_FOLDS), function(fold_i) {
    fold_rows <- Map(function(s, sub_cont, sub_bin) {
      path   <- FoldRdsPaths(importance_type, rolling_panel, s, control_group, rolling_period, norm_label)[fold_i]
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
      summarize(att_dr_mean = mean(fold_att, na.rm = TRUE), count = n(), .groups = "drop") %>%
      arrange(-att_dr_mean) %>%
      mutate(rank = row_number())
  }) %>% Filter(Negate(is.null), .)
}

FoldRdsPaths <- function(importance_type, rolling_panel, sample, control_group, rolling_period,
                         norm_label, covar_type_dir = "pc_score") {
  file.path(INDIR_FOREST_DS, importance_type, rolling_panel, sample, control_group, covar_type_dir,
            norm_label, paste0("event_study_forest_", FOREST_TRAINING_OUTCOME, "_rolling", rolling_period,
                   "_fold", seq_len(N_FOLDS), ".rds"))
}

Main()
