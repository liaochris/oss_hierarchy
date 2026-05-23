library(tidyverse)
library(arrow)
library(fs)
library(jsonlite)
library(SaveData)

source("source/lib/R/config_loaders.R")
source("source/lib/R/pc_groups.R")
source("source/lib/R/analysis_utils.R")
source("source/lib/R/event_study_helpers.R")
source("source/lib/R/constants.R")

INDIR_PREP <- "output/derived/analysis_panel"
OUTDIR     <- "output/analysis/event_study"

.cl_raw          <- commandArgs(trailingOnly = TRUE)
.cl              <- setNames(sub("^[^=]+=", "", .cl_raw), sub("=.*$", "", .cl_raw))
IMPORTANCE_TYPE  <- .cl[["CL_IMPORTANCE_TYPE"]]
ROLLING_PERIOD   <- .cl[["CL_ROLLING_PERIOD"]]
QUALIFIED_SAMPLE <- .cl[["CL_QUALIFIED_SAMPLE"]]
CONTROL_GROUP    <- .cl[["CL_CONTROL_GROUP"]]

Main <- function() {
  outcome_specs <- BuildOutcomeSpecs()
  outdir_slice  <- file.path(OUTDIR, IMPORTANCE_TYPE, ROLLING_PERIOD, QUALIFIED_SAMPLE, CONTROL_GROUP)

  coeffs_all <- list()

  if (QUALIFIED_SAMPLE %in% names(AGGREGATED_SAMPLES)) {
    sub_samples <- AGGREGATED_SAMPLES[[QUALIFIED_SAMPLE]]

    if ("full_sample" %in% EVENT_STUDY_SPLITS) {
      coeffs_all <- c(coeffs_all,
        RunFullSampleEventStudies(outcome_specs, outdir_slice,
          IMPORTANCE_TYPE, ROLLING_PERIOD, QUALIFIED_SAMPLE, CONTROL_GROUP,
          sub_samples = sub_samples))
    }

    if ("pc_score" %in% EVENT_STUDY_SPLITS) {
      coeffs_all <- c(coeffs_all,
        RunPCScoreEventStudies(outcome_specs, pc_groups_cfg, outdir_slice,
          IMPORTANCE_TYPE, ROLLING_PERIOD, QUALIFIED_SAMPLE, CONTROL_GROUP,
          sub_samples = sub_samples))
    }
  } else {
    panel <- LoadPreparedSample(INDIR_PREP, IMPORTANCE_TYPE, ROLLING_PERIOD, QUALIFIED_SAMPLE, CONTROL_GROUP)

    if ("full_sample" %in% EVENT_STUDY_SPLITS) {
      coeffs_all <- c(coeffs_all,
        RunFullSampleEventStudies(outcome_specs, outdir_slice,
          IMPORTANCE_TYPE, ROLLING_PERIOD, QUALIFIED_SAMPLE, CONTROL_GROUP,
          panel = panel))
    }

    if ("pc_score" %in% EVENT_STUDY_SPLITS) {
      coeffs_all <- c(coeffs_all,
        RunPCScoreEventStudies(outcome_specs, pc_groups_cfg, outdir_slice,
          IMPORTANCE_TYPE, ROLLING_PERIOD, QUALIFIED_SAMPLE, CONTROL_GROUP,
          panel = panel))
    }
  }

  coeffs_df <- bind_rows(coeffs_all) %>%
    mutate(event_time = as.numeric(event_time),
           split_covar = replace_na(split_covar, ""),
           split_value = replace_na(as.character(split_value), "")) %>%
    distinct()

  dir_create(outdir_slice, recurse = TRUE)
  SaveData(
    coeffs_df,
    c("importance_type", "rolling", "qualified_sample", "control_group", "split_type",
      "split_covar", "split_value", "category", "outcome", "normalize", "method", "event_time"),
    file.path(outdir_slice, "estimates.csv"),
    file.path(outdir_slice, "estimates.log"),
    sortbykey = FALSE
  )
}

BuildOutcomeSpecs <- function() {
  purrr::flatten(lapply(names(outcome_variables), function(category)
    purrr::flatten(lapply(outcome_variables[[category]]$run, function(outcome)
      lapply(NORM_OPTIONS, function(normalize)
        list(category = category, outcome = outcome, normalize = normalize))))))
}

RunFullSampleEventStudies <- function(outcome_specs, outdir_slice,
                                      importance_type, rolling_panel, qualified_sample, control_group,
                                      panel = NULL, sub_samples = NULL) {
  aggregated <- !is.null(sub_samples)
  if (aggregated) {
    sub_panels <- lapply(sub_samples, function(s)
      LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel, s, control_group))
    obs_counts <- vapply(sub_panels, nrow, integer(1))
  }

  coeffs             <- list()
  outdir_base        <- file.path(outdir_slice, "full_sample")
  plot_specs_by_norm <- list(norm = list(), raw = list())

  for (spec in outcome_specs) {
    if (aggregated) {
      sub_results <- lapply(sub_panels, function(p)
        FitEventStudy(p, spec$outcome, control_group, "sa", normalize = spec$normalize, title = "")$results)
      results <- WeightedAggregateCoefMatrix(sub_results, obs_counts)
      es_list <- list(list(results = results))
    } else {
      es      <- FitEventStudy(panel, spec$outcome, control_group, "sa", title = "", normalize = spec$normalize)
      results <- es$results
      es_list <- list(es)
    }
    norm_label <- ifelse(spec$normalize, "norm", "raw")
    out_path   <- file.path(outdir_base, norm_label, paste0(spec$outcome, ".png"))
    dir_create(dirname(out_path), recurse = TRUE)
    plot_specs_by_norm[[norm_label]][[length(plot_specs_by_norm[[norm_label]]) + 1]] <- list(
      es_list = es_list, out_path = out_path,
      legend_labels = NULL, legend_title = NULL, add_comparison = FALSE
    )
    coeffs[[length(coeffs) + 1]] <- CollectEstimateRows(
      results, importance_type, rolling_panel, qualified_sample, control_group,
      split_type = "full_sample", split_covar = "", split_value = "",
      category = spec$category, outcome = spec$outcome, normalize = spec$normalize)
  }

  lapply(plot_specs_by_norm, PlotEventStudyBatch)
  coeffs
}

RunPCScoreEventStudies <- function(outcome_specs, pc_group_cfg, outdir_slice,
                                   importance_type, rolling_panel, qualified_sample, control_group,
                                   panel = NULL, sub_samples = NULL) {
  aggregated <- !is.null(sub_samples)
  if (aggregated) {
    sub_pc_score_panels <- lapply(sub_samples, function(s)
      LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel, s, control_group))
    sub_pc_score_groups <- lapply(sub_pc_score_panels, BuildPCScoreGroups, pc_group_cfg)
  } else {
    pc_groups <- BuildPCScoreGroups(panel, pc_group_cfg)
  }

  coeffs             <- list()
  outdir_pc_score    <- file.path(outdir_slice, "pc_score")
  plot_specs_by_norm <- list(norm = list(), raw = list())

  for (spec in outcome_specs) {
    for (pc_group_name in names(pc_group_cfg)) {
      if (aggregated) {
        friendly_label <- pc_group_cfg[[pc_group_name]]$friendly_label
        agg_by_side <- setNames(lapply(c("low", "high"), function(split_side) {
          repo_field      <- paste0("repo_", split_side)
          sub_side_panels <- mapply(
            function(sub_panel, pc_groups) sub_panel %>% filter(repo_name %in% pc_groups[[pc_group_name]][[repo_field]]),
            sub_pc_score_panels, sub_pc_score_groups, SIMPLIFY = FALSE
          )
          side_obs_counts <- vapply(sub_side_panels, nrow, integer(1))
          side_results    <- lapply(sub_side_panels, function(p)
            FitEventStudy(p, spec$outcome, control_group, "sa", normalize = spec$normalize, title = "")$results)
          WeightedAggregateCoefMatrix(side_results, side_obs_counts)
        }), c("low", "high"))
        es_list <- lapply(agg_by_side, function(m) list(results = m))
      } else {
        split_group    <- pc_groups[[pc_group_name]]
        friendly_label <- split_group$friendly_label
        es_low  <- FitEventStudy(panel %>% filter(repo_name %in% split_group$repo_low),
                                 spec$outcome, control_group, "sa", title = "", normalize = spec$normalize)
        es_high <- FitEventStudy(panel %>% filter(repo_name %in% split_group$repo_high),
                                 spec$outcome, control_group, "sa", title = "", normalize = spec$normalize)
        agg_by_side <- list(low = es_low$results, high = es_high$results)
        es_list     <- list(es_low, es_high)
      }

      norm_label <- ifelse(spec$normalize, "norm", "raw")
      out_path   <- file.path(outdir_pc_score, norm_label,
        paste0(pc_group_name, "_", spec$outcome, "_pc_score_split.png"))
      dir_create(dirname(out_path), recurse = TRUE)
      plot_specs_by_norm[[norm_label]][[length(plot_specs_by_norm[[norm_label]]) + 1]] <- list(
        es_list = es_list, out_path = out_path,
        legend_labels = c("Low", "High"), legend_title = friendly_label, add_comparison = TRUE
      )

      for (split_side in c("low", "high")) {
        coeffs[[length(coeffs) + 1]] <- CollectEstimateRows(
          agg_by_side[[split_side]], importance_type, rolling_panel, qualified_sample, control_group,
          split_type = "pc_score", split_covar = paste0(pc_group_name, "_pc_score"),
          split_value = split_side,
          category = spec$category, outcome = spec$outcome, normalize = spec$normalize)
      }
    }
  }

  lapply(plot_specs_by_norm, PlotEventStudyBatch)
  coeffs
}

BuildPCScoreGroups <- function(panel_with_pc_scores, pc_group_cfg) {
  groups <- lapply(names(pc_group_cfg), function(group_name) {
    col_name <- paste0(group_name, "_pc_score_binary")
    scores <- panel_with_pc_scores %>% distinct(repo_name, .data[[col_name]])
    list(
      repo_low  = scores %>% filter(.data[[col_name]] == 0) %>% pull(repo_name),
      repo_high = scores %>% filter(.data[[col_name]] == 1) %>% pull(repo_name),
      friendly_label = pc_group_cfg[[group_name]]$friendly_label
    )
  })
  names(groups) <- names(pc_group_cfg)
  groups
}

CollectEstimateRows <- function(results, importance_type, rolling_panel, qualified_sample, control_group,
                                split_type, split_covar, split_value, category, outcome, normalize) {
  as_tibble(results, rownames = "event_time") %>%
    mutate(importance_type = importance_type, rolling = rolling_panel,
           qualified_sample = qualified_sample, control_group = control_group,
           split_type = split_type, split_covar = split_covar, split_value = split_value,
           category = category, outcome = outcome, normalize = normalize, method = "sa")
}

Main()
