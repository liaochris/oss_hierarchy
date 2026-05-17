library(tidyverse)
library(arrow)
library(fs)
library(jsonlite)
library(SaveData)

source("source/lib/R/config_loaders.R")
source("source/lib/R/analysis_utils.R")
source("source/lib/R/event_study_helpers.R")
source("source/lib/event_study_forest/forest_helpers.R")
source("source/lib/R/constants.R")

INDIR_PREP   <- "output/derived/analysis_panel"
INDIR_FOREST <- "output/analysis/event_study_forest"
OUTDIR       <- "output/analysis/event_study_forest"

Main <- function() {
  dir_create(OUTDIR)

  coeffs_all  <- list()

  for (importance_type in IMPORTANCE_TYPES) {
    for (qualified_sample in QUALIFIED_SAMPLES) {
      for (control_group in CONTROL_GROUPS) {
        for (rolling_panel in ROLLING_LABELS) {
          outcome_modes <- BuildOutcomeModes(outcome_variables, control_group, outdir = NULL,
                                              NORM_OPTIONS, build_dir = FALSE)

          if (qualified_sample %in% names(AGGREGATED_SAMPLES)) {
            sub_samples <- AGGREGATED_SAMPLES[[qualified_sample]]
            sub_panels  <- lapply(sub_samples, function(s)
              LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel, s, control_group))

            for (covar_type in c("all_covariates", "pc_score", "pc_score_binary")) {
              for (normalize in NORM_OPTIONS) {
                outcome_modes_n    <- Filter(function(m) m$normalize == normalize, outcome_modes)
                sub_forest_results <- lapply(sub_samples, function(s)
                  LoadForestResultsIfExists(importance_type, rolling_panel, s, control_group, covar_type, normalize))
                if (all(vapply(sub_forest_results, is.null, logical(1)))) next

                base_outdir <- file.path(OUTDIR, importance_type, rolling_panel,
                                          qualified_sample, control_group, covar_type)

                for (split_name in names(SPLIT_CONFIGS)) {
                  outdir_split <- file.path(base_outdir, split_name)
                  coeffs_all   <- RunAggregatedSplitEventStudies(
                    sub_panels, sub_forest_results, SPLIT_CONFIGS[[split_name]],
                    outcome_modes_n, control_group,
                    qualified_sample, covar_type,
                    outdir_split, normalize, coeffs_all
                  )
                }
              }
            }
          } else {
            panel <- LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel,
                                        qualified_sample, control_group)
            if (nrow(panel) == 0) next

            for (covar_type in c("all_covariates", "pc_score", "pc_score_binary")) {
              for (normalize in NORM_OPTIONS) {
                outcome_modes_n <- Filter(function(m) m$normalize == normalize, outcome_modes)
                forest_results  <- LoadForestResultsIfExists(importance_type, rolling_panel,
                                                             qualified_sample, control_group, covar_type, normalize)
                if (is.null(forest_results)) next

                base_outdir <- file.path(OUTDIR, importance_type, rolling_panel,
                                          qualified_sample, control_group, covar_type)

                for (split_name in names(SPLIT_CONFIGS)) {
                  outdir_split <- file.path(base_outdir, split_name)
                  coeffs_all   <- RunSplitEventStudies(
                    panel, forest_results, SPLIT_CONFIGS[[split_name]],
                    outcome_modes_n, control_group,
                    qualified_sample, covar_type,
                    outdir_split, normalize, coeffs_all
                  )
                }
              }
            }
          }
        }
      }
    }
  }

  if (length(coeffs_all) > 0) {
    coeffs_df <- bind_rows(coeffs_all) %>% mutate(event_time = as.numeric(event_time))
    SaveData(coeffs_df,
      c("qualified_sample", "covar_type", "outcome", "category", "normalize",
        "split_folder", "split_value", "estimation_type", "event_time"),
      file.path(OUTDIR, "personalized_event_study_coefficients.csv"),
      file.path(OUTDIR, "personalized_event_study_coefficients.log"),
      sortbykey = FALSE)
    GenerateForestAutofill(coeffs_df)
  }
}

SPLIT_CONFIGS <- list(
  split_full_avg = list(col = "att_dr_group",   label = "Average DR ATT (all periods)"),
  split_period1  = list(col = "att_dr_1_group", label = "DR ATT at Period 1"),
  split_period5  = list(col = "att_dr_5_group", label = "DR ATT at Period 5")
)

UnifyEventTimeColumns <- function(df) {
  cols <- names(df)[str_detect(names(df), "^cohort\\d+event_time\\.?\\d+$")]
  if (length(cols) == 0) return(tibble())
  meta <- tibble(col = cols) %>%
    mutate(
      num   = str_extract(col, "[0-9]+$"),
      dot   = str_detect(col, "event_time\\."),
      label = if_else(dot, paste0("event_time.", num), paste0("event_time", num))
    )
  df %>%
    mutate(.row = row_number()) %>%
    pivot_longer(all_of(cols), names_to = "col", values_to = "val") %>%
    left_join(meta, by = "col") %>%
    group_by(.row, label) %>%
    summarize(val = val[which(!is.na(val))[1]], .groups = "drop") %>%
    pivot_wider(names_from = label, values_from = val) %>%
    arrange(.row) %>%
    select(-.row)
}

AddDRSplitColumns <- function(df_cf) {
  dr_cols <- UnifyEventTimeColumns(df_cf)
  if (ncol(dr_cols) == 0) return(df_cf)
  colnames(dr_cols) <- paste0("dr_", colnames(dr_cols))

  df_cf %>%
    bind_cols(dr_cols) %>%
    mutate(
      att_dr_1_group = case_when(
        !is.na(dr_event_time1) & dr_event_time1 >= median(dr_event_time1, na.rm = TRUE) ~ "high",
        !is.na(dr_event_time1) & dr_event_time1 <  median(dr_event_time1, na.rm = TRUE) ~ "low",
        TRUE ~ NA_character_
      ),
      att_dr_5_group = case_when(
        !is.na(dr_event_time5) & dr_event_time5 >= median(dr_event_time5, na.rm = TRUE) ~ "high",
        !is.na(dr_event_time5) & dr_event_time5 <  median(dr_event_time5, na.rm = TRUE) ~ "low",
        TRUE ~ NA_character_
      )
    )
}

LoadForestResultsIfExists <- function(importance_type, rolling_panel, qualified_sample, control_group, covar_type, normalize) {
  norm_label <- ifelse(normalize, "norm", "raw")
  forest_results_path <- file.path(INDIR_FOREST, importance_type, rolling_panel,
                                   qualified_sample, control_group, covar_type, norm_label,
                                   paste0(FOREST_TRAINING_OUTCOME, "_repo_att_event_study_forest.parquet"))
  if (!file.exists(forest_results_path)) return(NULL)
  read_parquet(forest_results_path) %>% AddDRSplitColumns()
}

RunSplitEventStudies <- function(panel, forest_results, split_cfg, outcome_modes, control_group,
                                 qualified_sample, covar_type,
                                 outdir_split, normalize, coeffs_acc) {
  split_col <- split_cfg$col
  if (!split_col %in% colnames(forest_results)) return(coeffs_acc)

  norm_label  <- ifelse(normalize, "norm", "raw")
  repo_splits <- forest_results %>% select(repo_name, !!split_col)
  plot_specs  <- list()

  for (outcome_mode in outcome_modes) {
    base_df <- panel %>% left_join(repo_splits, by = "repo_name")
    es_list <- lapply(c("high", "low"), function(grp) {
      tryCatch(
        FitEventStudy(base_df %>% filter(.data[[split_col]] == grp),
                      outcome_mode$outcome, control_group,
                      method = "sa", normalize = normalize, title = ""),
        error = function(e) NULL
      )
    })

    valid   <- !sapply(es_list, is.null)
    es_list <- es_list[valid]
    labels  <- c("High adoption", "Low adoption")[valid]
    if (length(es_list) == 0) next

    out_path <- file.path(outdir_split, norm_label,
      paste0(outcome_mode$outcome, "_observed.png"))
    dir_create(dirname(out_path), recurse = TRUE)
    plot_specs[[length(plot_specs) + 1]] <- list(
      es_list = es_list,
      out_path = out_path,
      legend_labels = labels,
      legend_title = split_cfg$label,
      png_args = list(width = 800, height = 500)
    )

    for (j in seq_along(es_list)) {
      coeffs_acc[[length(coeffs_acc) + 1]] <- as_tibble(es_list[[j]]$results, rownames = "event_time") %>%
        mutate(
          outcome          = outcome_mode$outcome,
          category         = outcome_mode$category,
          normalize        = normalize,
          qualified_sample = qualified_sample,
          covar_type       = covar_type,
          split_folder     = basename(outdir_split),
          split_value      = c("high", "low")[valid][j],
          estimation_type  = "observed"
        )
    }
  }

  PlotEventStudyBatch(plot_specs)
  coeffs_acc
}

RunAggregatedSplitEventStudies <- function(sub_panels, sub_forest_results, split_cfg,
                                           outcome_modes, control_group,
                                           qualified_sample, covar_type,
                                           outdir_split, normalize, coeffs_acc) {
  split_col             <- split_cfg$col
  has_split_col         <- vapply(sub_forest_results, function(df) split_col %in% colnames(df), logical(1))
  usable_panels         <- sub_panels[has_split_col]
  usable_forest_results <- sub_forest_results[has_split_col]
  if (length(usable_panels) == 0) return(coeffs_acc)

  norm_label <- ifelse(normalize, "norm", "raw")
  plot_specs <- list()

  for (outcome_mode in outcome_modes) {
    agg_results_by_group <- lapply(c("high", "low"), function(grp) {
      group_filtered_panels <- mapply(function(panel, forest_results) {
        forest_results %>%
          select(repo_name, !!split_col) %>%
          { left_join(panel, ., by = "repo_name") } %>%
          filter(.data[[split_col]] == grp)
      }, usable_panels, usable_forest_results, SIMPLIFY = FALSE)

      sub_results <- lapply(group_filtered_panels, function(p) {
        tryCatch(
          FitEventStudy(p, outcome_mode$outcome, control_group,
                        method = "sa", normalize = normalize, title = "")$results,
          error = function(e) NULL
        )
      })

      is_valid      <- !vapply(sub_results, is.null, logical(1))
      valid_results <- sub_results[is_valid]
      valid_counts  <- vapply(group_filtered_panels[is_valid], nrow, integer(1))
      if (length(valid_results) == 0) return(NULL)
      WeightedAggregateCoefMatrix(valid_results, valid_counts)
    })
    names(agg_results_by_group) <- c("high", "low")

    non_null_groups <- Filter(Negate(is.null), agg_results_by_group)
    if (length(non_null_groups) == 0) next

    out_path <- file.path(outdir_split, norm_label,
      paste0(outcome_mode$outcome, "_observed.png"))
    dir_create(dirname(out_path), recurse = TRUE)
    plot_specs[[length(plot_specs) + 1]] <- list(
      es_list = lapply(non_null_groups, function(m) list(results = m)),
      out_path = out_path,
      legend_labels = tools::toTitleCase(names(non_null_groups)),
      legend_title = split_cfg$label,
      png_args = list(width = 800, height = 500)
    )

    for (grp in names(non_null_groups)) {
      coeffs_acc[[length(coeffs_acc) + 1]] <- as_tibble(non_null_groups[[grp]], rownames = "event_time") %>%
        mutate(
          outcome          = outcome_mode$outcome,
          category         = outcome_mode$category,
          normalize        = normalize,
          qualified_sample = qualified_sample,
          covar_type       = covar_type,
          split_folder     = basename(outdir_split),
          split_value      = grp,
          estimation_type  = "observed"
        )
    }
  }

  PlotEventStudyBatch(plot_specs)
  coeffs_acc
}

GenerateForestAutofill <- function(coeffs_df) {
  compute_diff <- function(outcome, covar_type_val) {
    sub <- coeffs_df %>%
      filter(
        qualified_sample == PRIMARY_QUALIFIED_SAMPLE,
        normalize        == TRUE,
        estimation_type  == "observed",
        event_time %in%  1:5,
        .data$outcome    == !!outcome,
        covar_type       == covar_type_val,
        split_folder     == "split_full_avg"
      )
    high_avg <- sub %>% filter(split_value == "high") %>% pull(estimate) %>% mean()
    low_avg  <- sub %>% filter(split_value == "low")  %>% pull(estimate) %>% mean()
    high_avg - low_avg
  }

  ForestCoarsePRsOpenedDiff <- compute_diff("pull_request_opened",        "pc_score_binary")
  ForestCoarsePRsMergedDiff <- compute_diff("pull_request_merged",        "pc_score_binary")
  ForestCoarseReleasesDiff  <- compute_diff("overall_new_release_count",  "pc_score_binary")
  ForestContPRsOpenedDiff   <- compute_diff("pull_request_opened",        "pc_score")
  ForestContPRsMergedDiff   <- compute_diff("pull_request_merged",        "pc_score")
  ForestContReleasesDiff    <- compute_diff("overall_new_release_count",  "pc_score")

  autofill_path <- file.path("output/autofill/forest_autofill.tex")
  dir_create(dirname(autofill_path))
  writeLines(c(
    sprintf("\\newcommand{\\ForestCoarsePRsOpenedDiff}{%.2f}", ForestCoarsePRsOpenedDiff),
    sprintf("\\newcommand{\\ForestCoarsePRsMergedDiff}{%.2f}", ForestCoarsePRsMergedDiff),
    sprintf("\\newcommand{\\ForestCoarseReleasesDiff}{%.2f}",  ForestCoarseReleasesDiff),
    sprintf("\\newcommand{\\ForestContPRsOpenedDiff}{%.2f}",   ForestContPRsOpenedDiff),
    sprintf("\\newcommand{\\ForestContPRsMergedDiff}{%.2f}",   ForestContPRsMergedDiff),
    sprintf("\\newcommand{\\ForestContReleasesDiff}{%.2f}",    ForestContReleasesDiff)
  ), autofill_path)
}


Main()
