library(tidyverse)
library(arrow)
library(fs)
library(jsonlite)
library(SaveData)

source("source/lib/helpers.R")
source("source/lib/event_study_helpers.R")
source("source/lib/constants.R")

INDIR_PREP <- "output/analysis/data_prep"

Main <- function() {
  OUTDIR <- "output/analysis/event_study"
  dir_create(OUTDIR)

  project_cfg <- LoadProjectConfig(PROJECT_CONFIG_PATH)
  outcome_cfg <- project_cfg$outcome_variables
  pc_group_cfg <- PCGroupsConfig(project_cfg$feature_variables)

  coeffs_all <- list()

  for (importance_type in IMPORTANCE_TYPES) {
    for (rolling_panel in ROLLING_LABELS) {
      outcome_specs <- BuildOutcomeSpecs(outcome_cfg, NORM_OPTIONS)

      for (qualified_sample in QUALIFIED_SAMPLES) {
        for (control_group in CONTROL_GROUPS) {
          message("Processing ", importance_type, " / ", rolling_panel, " / ",
            qualified_sample, " / ", control_group)

          outdir_slice <- file.path(OUTDIR, importance_type, rolling_panel, qualified_sample, control_group)

          if (qualified_sample %in% names(AGGREGATED_SAMPLES)) {
            sub_samples <- AGGREGATED_SAMPLES[[qualified_sample]]

            if ("full_sample" %in% EVENT_STUDY_SPLITS) {
              coeffs_all <- c(coeffs_all,
                RunAggregatedFullSampleEventStudies(sub_samples, outcome_specs, outdir_slice,
                  importance_type, rolling_panel, qualified_sample, control_group))
            }

            if ("pc_score" %in% EVENT_STUDY_SPLITS) {
              coeffs_all <- c(coeffs_all,
                RunAggregatedPCScoreEventStudies(sub_samples, outcome_specs, pc_group_cfg, outdir_slice,
                  importance_type, rolling_panel, qualified_sample, control_group))
            }
          } else {
            panel <- LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel, qualified_sample, control_group)

            if ("full_sample" %in% EVENT_STUDY_SPLITS) {
              coeffs_all <- c(coeffs_all,
                RunFullSampleEventStudies(panel, outcome_specs, outdir_slice,
                  importance_type, rolling_panel, qualified_sample, control_group))
            }

            if ("pc_score" %in% EVENT_STUDY_SPLITS) {
              panel_with_pc_scores <- LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel,
                qualified_sample, control_group, with_pc_scores = TRUE)
              coeffs_all <- c(coeffs_all,
                RunPCScoreEventStudies(panel_with_pc_scores, outcome_specs, pc_group_cfg, outdir_slice,
                  importance_type, rolling_panel, qualified_sample, control_group))
            }
          }
        }
      }
    }
  }

  coeffs_df <- bind_rows(coeffs_all) %>%
    mutate(event_time = as.numeric(event_time),
           split_covar = replace_na(split_covar, ""),
           split_value = replace_na(as.character(split_value), "")) %>%
    distinct()

  SaveData(
    coeffs_df,
    c("importance_type", "rolling", "qualified_sample", "control_group", "split_type",
      "split_covar", "split_value", "category", "outcome", "normalize", "method", "event_time"),
    file.path(OUTDIR, "all_estimates.csv"),
    file.path(OUTDIR, "all_estimates.log"),
    sortbykey = FALSE
  )
}

BuildOutcomeSpecs <- function(outcome_cfg, norm_options) {
  purrr::flatten(lapply(names(outcome_cfg), function(category)
    purrr::flatten(lapply(outcome_cfg[[category]]$run, function(outcome)
      lapply(norm_options, function(normalize)
        list(category = category, outcome = outcome, normalize = normalize))))))
}

RunFullSampleEventStudies <- function(panel, outcome_specs, outdir_slice,
                                      importance_type, rolling_panel, qualified_sample, control_group) {
  coeffs <- list()
  outdir_base <- file.path(outdir_slice, "full_sample")

  for (spec in outcome_specs) {
    es <- FitEventStudy(panel, spec$outcome, control_group, "sa", title = "", normalize = spec$normalize)
    out_path <- file.path(outdir_base, spec$category, paste0(spec$outcome, ".png"))
    dir_create(dirname(out_path), recurse = TRUE)
    png(out_path)
    PlotEventStudyComparison(list(es), title = "", legend_labels = NULL,
      add_comparison = FALSE, ylim = YLIM_DEFAULT)
    dev.off()

    coeffs[[length(coeffs) + 1]] <- CollectEstimateRows(
      es$results, importance_type, rolling_panel, qualified_sample, control_group,
      split_type = "full_sample", split_covar = "", split_value = "",
      category = spec$category, outcome = spec$outcome, normalize = spec$normalize)
  }

  coeffs
}

RunAggregatedFullSampleEventStudies <- function(sub_samples, outcome_specs, outdir_slice,
                                                importance_type, rolling_panel,
                                                aggregated_sample, control_group) {
  sub_panels  <- lapply(sub_samples, function(s)
    LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel, s, control_group))
  obs_counts  <- vapply(sub_panels, nrow, integer(1))
  outdir_base <- file.path(outdir_slice, "full_sample")

  lapply(outcome_specs, function(spec) {
    sub_results <- lapply(sub_panels, function(panel)
      FitEventStudy(panel, spec$outcome, control_group, "sa",
                    normalize = spec$normalize, title = "")$results)
    agg_mat  <- WeightedAggregateCoefMatrix(sub_results, obs_counts)
    out_path <- file.path(outdir_base, spec$category, paste0(spec$outcome, ".png"))
    dir_create(dirname(out_path), recurse = TRUE)
    png(out_path)
    PlotEventStudyComparison(list(list(results = agg_mat)), title = "",
      legend_labels = NULL, add_comparison = FALSE, ylim = YLIM_DEFAULT)
    dev.off()
    CollectEstimateRows(agg_mat, importance_type, rolling_panel, aggregated_sample,
      control_group, split_type = "full_sample", split_covar = "", split_value = "",
      category = spec$category, outcome = spec$outcome, normalize = spec$normalize)
  })
}

RunPCScoreEventStudies <- function(panel_with_pc_scores, outcome_specs, pc_group_cfg, outdir_slice,
                                   importance_type, rolling_panel, qualified_sample, control_group) {
  coeffs <- list()
  outdir_pc_score <- file.path(outdir_slice, "pc_score")
  pc_groups <- BuildPCScoreGroups(panel_with_pc_scores, pc_group_cfg)

  for (spec in outcome_specs) {
    for (pc_group_name in names(pc_groups)) {
      split_group <- pc_groups[[pc_group_name]]
      panel_low  <- panel_with_pc_scores %>% filter(repo_name %in% split_group$repo_low)
      panel_high <- panel_with_pc_scores %>% filter(repo_name %in% split_group$repo_high)

      es_low  <- FitEventStudy(panel_low,  spec$outcome, control_group, "sa", title = "", normalize = spec$normalize)
      es_high <- FitEventStudy(panel_high, spec$outcome, control_group, "sa", title = "", normalize = spec$normalize)

      out_path <- file.path(outdir_pc_score, spec$category,
        paste0(pc_group_name, "_", spec$outcome, "_pc_score_split.png"))
      dir_create(dirname(out_path), recurse = TRUE)
      png(out_path)
      PlotEventStudyComparison(list(es_low, es_high),
        legend_labels = c("Low", "High"), legend_title = split_group$friendly_label,
        ylim = c(-2.25, 1.5))
      dev.off()

      for (split_side in list(list(label = "low", es = es_low), list(label = "high", es = es_high))) {
        coeffs[[length(coeffs) + 1]] <- CollectEstimateRows(
          split_side$es$results, importance_type, rolling_panel, qualified_sample, control_group,
          split_type = "pc_score", split_covar = paste0(pc_group_name, "_pc_score"), split_value = split_side$label,
          category = spec$category, outcome = spec$outcome, normalize = spec$normalize)
      }
    }
  }

  coeffs
}

RunAggregatedPCScoreEventStudies <- function(sub_samples, outcome_specs, pc_group_cfg, outdir_slice,
                                             importance_type, rolling_panel,
                                             aggregated_sample, control_group) {
  sub_pc_score_panels <- lapply(sub_samples, function(s)
    LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel, s, control_group, with_pc_scores = TRUE))
  sub_pc_score_groups <- lapply(sub_pc_score_panels, BuildPCScoreGroups, pc_group_cfg)
  outdir_pc_score     <- file.path(outdir_slice, "pc_score")

  coeffs <- list()
  for (spec in outcome_specs) {
    for (pc_group_name in names(pc_group_cfg)) {
      friendly_label <- pc_group_cfg[[pc_group_name]]$friendly_label

      agg_by_side <- setNames(lapply(c("low", "high"), function(split_side) {
        repo_field      <- paste0("repo_", split_side)
        sub_side_panels <- mapply(
          function(panel_with_pc_scores, pc_groups) {
            panel_with_pc_scores %>% filter(repo_name %in% pc_groups[[pc_group_name]][[repo_field]])
          },
          sub_pc_score_panels, sub_pc_score_groups, SIMPLIFY = FALSE
        )
        side_obs_counts <- vapply(sub_side_panels, nrow, integer(1))
        side_results    <- lapply(sub_side_panels, function(panel)
          FitEventStudy(panel, spec$outcome, control_group, "sa",
                        normalize = spec$normalize, title = "")$results)
        WeightedAggregateCoefMatrix(side_results, side_obs_counts)
      }), c("low", "high"))

      out_path <- file.path(outdir_pc_score, spec$category,
        paste0(pc_group_name, "_", spec$outcome, "_pc_score_split.png"))
      dir_create(dirname(out_path), recurse = TRUE)
      png(out_path)
      PlotEventStudyComparison(
        lapply(agg_by_side, function(m) list(results = m)),
        legend_labels = c("Low", "High"), legend_title = friendly_label,
        ylim = c(-2.25, 1.5))
      dev.off()

      for (split_side in c("low", "high")) {
        coeffs[[length(coeffs) + 1]] <- CollectEstimateRows(
          agg_by_side[[split_side]], importance_type, rolling_panel, aggregated_sample, control_group,
          split_type = "pc_score", split_covar = paste0(pc_group_name, "_pc_score"),
          split_value = split_side,
          category = spec$category, outcome = spec$outcome, normalize = spec$normalize)
      }
    }
  }

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
