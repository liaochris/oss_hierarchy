library(tidyverse)
library(arrow)
library(fs)
library(grf)

source("source/analysis/analyze_forest/helpers.R")

INDIR_CF    <- "output/analysis/event_study_forest"
INDIR_CF_DS <- "drive/output/analysis/event_study_forest"
OUTDIR      <- "output/analysis/event_study_forest"

Main <- function() {
  for (importance_type in IMPORTANCE_TYPES) {
    # Only aggregated samples have multiple folds to compare across
    for (qualified_sample in names(AGGREGATED_SAMPLES)) {
      for (control_group in CONTROL_GROUPS) {
        for (rolling_panel in ROLLING_LABELS) {
          rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
          outdir_ds      <- file.path(OUTDIR, importance_type, rolling_panel, qualified_sample, control_group)
          dir_create(outdir_ds, recurse = TRUE)

          forest_data <- LoadForestData(INDIR_CF, importance_type, rolling_panel, qualified_sample, control_group)
          if (is.null(forest_data)) next

          pc_cols    <- colnames(forest_data$df)[grepl("_principal_component1$", colnames(forest_data$df))]
          pc_medians <- sapply(pc_cols, function(col) median(forest_data$df[[col]], na.rm = TRUE))

          # Binarize continuous PC scores using the pooled median so that
          # high/low thresholds are consistent across folds
          sub_bins <- lapply(forest_data$sub_dfs, function(sub_df)
            sub_df %>% mutate(across(all_of(pc_cols),
                                     ~ ifelse(.x > pc_medians[cur_column()], "high", "low"))))

          df_bins <- forest_data$df %>%
            mutate(across(all_of(pc_cols),
                          ~ ifelse(.x > pc_medians[cur_column()], "high", "low"),
                          .names = "{.col}"))

          df_summary <- df_bins %>%
            group_by(across(all_of(pc_cols))) %>%
            summarize(att_dr_mean = mean(att_dr, na.rm = TRUE), count = n(), .groups = "drop") %>%
            arrange(-att_dr_mean) %>%
            mutate(rank = row_number())

          fold_summaries <- BuildFoldSummaries(
            forest_data$sub_dfs, sub_bins, pc_cols, rolling_period,
            importance_type, rolling_panel, control_group
          )
          if (length(fold_summaries) == 0) next

          PlotCrossForestGrid(df_summary, fold_summaries, pc_cols, outdir_ds)
          ComputeFoldCorrelations(df_summary, fold_summaries, pc_cols, outdir_ds)

          agg_fold_paths <- unlist(lapply(names(forest_data$sub_dfs), function(s)
            FoldRdsPaths(importance_type, rolling_panel, s, control_group, rolling_period)))
          ExportVariableImportanceTex(agg_fold_paths, outdir_ds)
        }
      }
    }
  }
  invisible(NULL)
}

FoldRdsPaths <- function(importance_type, rolling_panel, sample, control_group, rolling_period,
                         covar_type_dir = "pc1") {
  file.path(INDIR_CF_DS, importance_type, rolling_panel, sample, control_group, covar_type_dir,
            paste0("event_study_forest_", FOREST_TRAINING_OUTCOME, "_rolling", rolling_period,
                   "_fold", seq_len(N_FOLDS), ".rds"))
}

BuildFoldSummaries <- function(sub_dfs, sub_bins, pc_cols, rolling_period,
                               importance_type, rolling_panel, control_group) {
  lapply(seq_len(N_FOLDS), function(fold_i) {
    fold_rows <- Map(function(s, sub_cont, sub_bin) {
      path <- FoldRdsPaths(importance_type, rolling_panel, s, control_group, rolling_period)[fold_i]
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
      group_by(across(all_of(pc_cols))) %>%
      summarize(att_dr_mean = mean(fold_att, na.rm = TRUE), count = n(), .groups = "drop") %>%
      arrange(-att_dr_mean) %>%
      mutate(rank = row_number())
  }) %>% Filter(Negate(is.null), .)
}

Main()
