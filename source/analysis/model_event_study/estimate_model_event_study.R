library(tidyverse)
library(arrow)
library(fs)
library(fixest)
library(parallel)
library(SaveData)

source("source/lib/R/config_loaders.R")
source("source/lib/R/analysis_utils.R")
source("source/lib/R/event_study_helpers.R")
source("source/lib/R/constants.R")

INDIR_PANEL  <- "output/derived/analysis_panel"
INDIR_MEMBER <- "drive/output/derived/model_prediction/event_time_member_panel"
INDIR_DRAWS  <- "drive/output/analysis/model_prediction"
INDIR_FOREST <- "output/analysis/event_study_forest"
OUTDIR       <- "output/analysis/model_event_study"

VARIANT      <- "opened_cohort"
DISTRIBUTION <- "adaptive"
ESTIMATION   <- "pooled"
IMPORTANCE_TYPE <- PRIMARY_IMPORTANCE_TYPE
ROLLING_LABEL   <- PRIMARY_ROLLING_LABEL
CONTROL_GROUP   <- PRIMARY_CONTROL_GROUP
SUB_SAMPLES     <- AGGREGATED_SAMPLES[["exact_1_2"]]
FIGURE_SAMPLES  <- c(SUB_SAMPLES, "exact_1_2")
RESILIENCE_GROUPS <- c("high", "low")

GLOBAL_SETTINGS <- LoadProjectConfig("source/lib/config/global_settings.json")
N_CORES         <- GLOBAL_SETTINGS$n_jobs
setFixest_nthreads(1)   # parallelism comes from mclapply over draw chunks; keep each fit single-threaded so the forks do not oversubscribe
setFixest_nthreads(1)   # parallelism comes from mclapply over draws; keep each fit single-threaded so the forks do not oversubscribe

ES_OUTCOMES <- c("pull_request_opened", "pull_request_reviewed",
                 "pull_request_merged_direct", "pull_request_merged_after_review", "pull_request_merged")
LATENT_FLOW_OUTCOME <- "pull_request_opened"   # the outcome governed by the fitted latent-flow model
KSTAR       <- 0

# Drop scale-outlier orgs: a handful of very high-volume repos (all controls) dominate the count-weighted
# raw event study and manufacture a pretrend that vanishes under normalization. Trim orgs whose pre-period
# (event time [-5,-1]) mean of TRIM_OUTCOME falls outside the pooled [1,99] percentile, pooled across
# treated and control within each subsample.
TRIM_OUTCOME <- "pull_request_opened"
TRIM_PROBS   <- c(0.01, 0.99)


Main <- function() {
  samples_data <- setNames(lapply(SUB_SAMPLES, LoadSampleData), SUB_SAMPLES)

  band_rows                       <- list()
  actual_treated_model_control_rows <- list()
  resilience_band_rows             <- list()
  for (normalize in NORM_OPTIONS) {
    for (outcome in ES_OUTCOMES) {
      full_sample_bands <- ComputeSampleBands(samples_data, outcome, normalize)
      for (sample_name in FIGURE_SAMPLES) {
        PlotBand(full_sample_bands$band_by_sample[[sample_name]], full_sample_bands$actual_by_sample[[sample_name]],
                 sample_name, outcome, normalize)
        band_rows[[length(band_rows) + 1]] <- full_sample_bands$band_by_sample[[sample_name]] %>%
          mutate(sample = sample_name, outcome = outcome, normalize = normalize)
      }
    }

    model_control_samples <- setNames(lapply(SUB_SAMPLES, function(sub_sample)
      BuildActualTreatedModelControlSample(samples_data[[sub_sample]], LATENT_FLOW_OUTCOME)), SUB_SAMPLES)
    model_control_bands <- ComputeSampleBands(model_control_samples, LATENT_FLOW_OUTCOME, normalize)
    for (sample_name in FIGURE_SAMPLES) {
      PlotActualTreatedModelControl(model_control_bands$band_by_sample[[sample_name]],
                                    model_control_bands$actual_by_sample[[sample_name]],
                                    sample_name, LATENT_FLOW_OUTCOME, normalize)
      actual_treated_model_control_rows[[length(actual_treated_model_control_rows) + 1]] <-
        model_control_bands$band_by_sample[[sample_name]] %>%
        mutate(sample = sample_name, outcome = LATENT_FLOW_OUTCOME, normalize = normalize)
    }

    for (covar_type in COVAR_TYPES) {
      resilience_groups_by_subsample <- setNames(
        lapply(SUB_SAMPLES, function(sub_sample) LoadResilienceGroups(sub_sample, covar_type, normalize)), SUB_SAMPLES)
      for (outcome in ES_OUTCOMES) {
        bands_by_resilience_group <- setNames(
          lapply(RESILIENCE_GROUPS, function(resilience_group)
            ComputeResilienceGroupBands(samples_data, resilience_groups_by_subsample, resilience_group, outcome, normalize)),
          RESILIENCE_GROUPS)
        for (sample_name in FIGURE_SAMPLES) {
          PlotResilienceComparison(bands_by_resilience_group, sample_name, outcome, normalize, covar_type)
          resilience_band_rows <- AppendResilienceBandRows(resilience_band_rows, bands_by_resilience_group,
                                                           sample_name, outcome, normalize, covar_type)
        }
      }
    }
  }

  band_table <- bind_rows(band_rows) %>%
    select(sample, outcome, normalize, event_time, p2.5, p50, p97.5, draw_kstar, actual)
  dir_create(OUTDIR, recurse = TRUE)
  SaveData(band_table, c("sample", "outcome", "normalize", "event_time"),
           file.path(OUTDIR, "band_estimates.csv"), file.path(OUTDIR, "band_estimates.log"), sortbykey = FALSE)

  actual_treated_model_control_table <- bind_rows(actual_treated_model_control_rows) %>%
    select(sample, outcome, normalize, event_time, p2.5, p50, p97.5, draw_kstar, actual)
  SaveData(actual_treated_model_control_table, c("sample", "outcome", "normalize", "event_time"),
           file.path(OUTDIR, "actual_treated_model_control_band_estimates.csv"),
           file.path(OUTDIR, "actual_treated_model_control_band_estimates.log"), sortbykey = FALSE)

  resilience_band_table <- bind_rows(resilience_band_rows) %>%
    select(sample, covar_type, outcome, normalize, split_value, event_time, p2.5, p50, p97.5, draw_kstar, actual)
  SaveData(resilience_band_table,
           c("sample", "covar_type", "outcome", "normalize", "split_value", "event_time"),
           file.path(OUTDIR, "resilience_band_estimates.csv"), file.path(OUTDIR, "resilience_band_estimates.log"),
           sortbykey = FALSE)
}


ComputeSampleBands <- function(samples_data_by_subsample, outcome, normalize) {
  n_treated_by_subsample <- vapply(samples_data_by_subsample, function(sample_data) sample_data$n_treated, integer(1))
  subsample_weights      <- n_treated_by_subsample / sum(n_treated_by_subsample)

  actual_by_subsample <- lapply(samples_data_by_subsample, function(sample_data)
    FitEventStudy(sample_data$actual, outcome, CONTROL_GROUP, "sa", normalize = normalize, make_plot = FALSE)$results)
  actual_by_sample <- ResultsBySample(actual_by_subsample, n_treated_by_subsample)

  draw_estimates_by_subsample <- setNames(lapply(SUB_SAMPLES, function(sub_sample)
    DrawPointEstimates(samples_data_by_subsample[[sub_sample]], outcome, normalize)), SUB_SAMPLES)
  draw_estimates_by_sample <- c(draw_estimates_by_subsample,
                                list(exact_1_2 = AggregateDrawEstimates(draw_estimates_by_subsample, subsample_weights)))

  band_by_sample <- setNames(lapply(FIGURE_SAMPLES, function(sample_name)
    SummariseBand(draw_estimates_by_sample[[sample_name]], actual_by_sample[[sample_name]], KSTAR)), FIGURE_SAMPLES)

  list(actual_by_sample = actual_by_sample, band_by_sample = band_by_sample)
}


LoadSampleData <- function(sub_sample) {
  panel    <- LoadPreparedSample(INDIR_PANEL, IMPORTANCE_TYPE, ROLLING_LABEL, sub_sample, CONTROL_GROUP)
  observed <- LoadObservedOpenedCohort(sub_sample)
  draws    <- read_parquet(file.path(INDIR_DRAWS, VARIANT, DISTRIBUTION, "draws", ESTIMATION,
                                      IMPORTANCE_TYPE, sub_sample, CONTROL_GROUP, "raw_draws.parquet"))

  common_repos <- Reduce(intersect, list(unique(panel$repo_name), unique(observed$repo_name), unique(draws$repo_name)))
  common_repos <- TrimOutcomeOutlierRepos(observed, common_repos)
  skeleton     <- panel %>% filter(repo_name %in% common_repos) %>% select(-any_of(ES_OUTCOMES))
  draws        <- draws %>% filter(repo_name %in% common_repos) %>%
    select(repo_name, quasi_event_time, draw_id, all_of(ES_OUTCOMES))

  actual_panel <- skeleton %>% inner_join(observed, by = c("repo_name", "quasi_event_time"))
  stopifnot(!anyNA(actual_panel[ES_OUTCOMES]))

  list(
    skeleton  = skeleton,
    actual    = actual_panel,
    draw_list = split(draws, draws$draw_id),
    n_treated = length(unique(skeleton$repo_name[skeleton$treatment_group != 0]))
  )
}


# SINGLE-USE EXCEPTION: kept beside LoadSampleData; the eventual home for this restriction is the
# data-prep sample construction (panel_filters.py), shared across event_study / event_study_forest.
TrimOutcomeOutlierRepos <- function(observed, common_repos) {
  pre_period_mean <- observed %>%
    filter(repo_name %in% common_repos, quasi_event_time >= MIN_EVENT_TIME, quasi_event_time <= -1) %>%
    group_by(repo_name) %>%
    summarise(pre_mean_outcome = mean(.data[[TRIM_OUTCOME]]), .groups = "drop")
  bounds <- quantile(pre_period_mean$pre_mean_outcome, TRIM_PROBS, names = FALSE)
  pre_period_mean %>%
    filter(pre_mean_outcome >= bounds[1], pre_mean_outcome <= bounds[2]) %>%
    pull(repo_name)
}


LoadObservedOpenedCohort <- function(sub_sample) {
  member_dir <- file.path(INDIR_MEMBER, VARIANT, IMPORTANCE_TYPE, sub_sample, CONTROL_GROUP)
  open_dataset(member_dir) %>%
    select(repo_name, quasi_event_time, repo_pull_request_opened, repo_pull_request_reviewed,
           repo_pull_request_merged_direct, repo_pull_request_merged_after_review) %>%
    distinct() %>%
    collect() %>%
    transmute(repo_name, quasi_event_time,
              pull_request_opened              = repo_pull_request_opened,
              pull_request_reviewed            = repo_pull_request_reviewed,
              pull_request_merged_direct       = repo_pull_request_merged_direct,
              pull_request_merged_after_review = repo_pull_request_merged_after_review,
              pull_request_merged              = repo_pull_request_merged_direct + repo_pull_request_merged_after_review)
}


LoadResilienceGroups <- function(sub_sample, covar_type, normalize) {
  normalization_label <- ifelse(normalize, "norm", "raw")
  forest_path <- file.path(INDIR_FOREST, IMPORTANCE_TYPE, ROLLING_LABEL, sub_sample, CONTROL_GROUP,
                           covar_type, normalization_label,
                           paste0(FOREST_TRAINING_OUTCOME, "_repo_att_event_study_forest.parquet"))
  read_parquet(forest_path) %>% select(repo_name, resilience_group = att_doubly_robust_group)
}


ComputeResilienceGroupBands <- function(samples_data, resilience_groups_by_subsample, resilience_group, outcome, normalize) {
  samples_data_in_group <- setNames(lapply(SUB_SAMPLES, function(sub_sample) {
    group_repos <- resilience_groups_by_subsample[[sub_sample]] %>%
      filter(.data$resilience_group == .env$resilience_group) %>% pull(repo_name)
    SubsetSampleData(samples_data[[sub_sample]], group_repos)
  }), SUB_SAMPLES)
  tryCatch(ComputeSampleBands(samples_data_in_group, outcome, normalize), error = function(e) NULL)
}


SubsetSampleData <- function(sample_data, group_repos) {
  skeleton_in_group <- sample_data$skeleton %>% filter(repo_name %in% group_repos)
  list(
    skeleton  = skeleton_in_group,
    actual    = sample_data$actual %>% filter(repo_name %in% group_repos),
    draw_list = lapply(sample_data$draw_list, function(draw) draw %>% filter(repo_name %in% group_repos)),
    n_treated = length(unique(skeleton_in_group$repo_name[skeleton_in_group$treatment_group != 0]))
  )
}


DrawPointEstimates <- function(sample_data, outcome, normalize) {
  # One multi-LHS feols per chunk demeans the fixed effects once for every draw in the chunk instead
  # of refitting per draw; chunks run in parallel across cores. Coefficients match a per-draw fit.
  draw_id_chunks <- split(names(sample_data$draw_list), cut(seq_along(sample_data$draw_list), N_CORES, labels = FALSE))
  chunk_estimates <- mclapply(draw_id_chunks, function(chunk_draw_ids)
    ChunkPointEstimates(sample_data, outcome, normalize, chunk_draw_ids), mc.cores = N_CORES)
  bind_rows(chunk_estimates)
}


ChunkPointEstimates <- function(sample_data, outcome, normalize, chunk_draw_ids) {
  draw_columns <- paste0("draw_", chunk_draw_ids)
  wide_panel   <- WideDrawPanel(sample_data, outcome, normalize, chunk_draw_ids, draw_columns)
  multi_fit    <- feols(as.formula(sprintf(
    "c(%s) ~ sunab(treatment_group, time_index, ref.p=-1) | repo_name + time_index",
    paste(draw_columns, collapse = ", "))), wide_panel)
  imap_dfr(chunk_draw_ids, function(draw_id, chunk_index) {
    fit <- if (length(chunk_draw_ids) > 1) multi_fit[[chunk_index]] else multi_fit
    EventTimeCoefficients(coef(fit)) %>% mutate(draw_id = as.integer(draw_id))
  })
}


WideDrawPanel <- function(sample_data, outcome, normalize, chunk_draw_ids, draw_columns) {
  if (normalize) {
    # Normalizing drops rows with zero pre-period dispersion, so each draw carries its own sample; a
    # full join leaves NA where a draw dropped a row, and feols fits each column on its own rows.
    normalized_draws <- map2(chunk_draw_ids, draw_columns, function(draw_id, draw_column) {
      draw_panel <- sample_data$skeleton %>%
        inner_join(sample_data$draw_list[[draw_id]] %>% select(repo_name, quasi_event_time, all_of(outcome)),
                   by = c("repo_name", "quasi_event_time"))
      NormalizeOutcome(draw_panel, outcome) %>%
        transmute(repo_name, quasi_event_time, !!draw_column := .data[[paste0(outcome, "_norm")]])
    })
    wide_draws <- reduce(normalized_draws, full_join, by = c("repo_name", "quasi_event_time"))
  } else {
    wide_draws <- bind_rows(sample_data$draw_list[chunk_draw_ids]) %>%
      mutate(draw_column = paste0("draw_", draw_id)) %>%
      select(repo_name, quasi_event_time, draw_column, all_of(outcome)) %>%
      pivot_wider(names_from = draw_column, values_from = all_of(outcome))
  }
  sample_data$skeleton %>% inner_join(wide_draws, by = c("repo_name", "quasi_event_time"))
}


EventTimeCoefficients <- function(model_coefficients) {
  event_time_coefficients <- model_coefficients[grepl("^time_index::", names(model_coefficients))]
  bind_rows(
    tibble(event_time = as.numeric(sub("^time_index::", "", names(event_time_coefficients))), estimate = unname(event_time_coefficients)),
    tibble(event_time = -1, estimate = 0)
  )
}


AggregateDrawEstimates <- function(draw_estimates_by_subsample, weights) {
  subsample_1 <- SUB_SAMPLES[1]
  subsample_2 <- SUB_SAMPLES[2]
  subsample_1_estimates <- draw_estimates_by_subsample[[subsample_1]] %>% rename(subsample_1_estimate = estimate)
  subsample_2_estimates <- draw_estimates_by_subsample[[subsample_2]] %>% rename(subsample_2_estimate = estimate)
  inner_join(subsample_1_estimates, subsample_2_estimates, by = c("draw_id", "event_time")) %>%
    mutate(estimate = weights[subsample_1] * subsample_1_estimate +
                      weights[subsample_2] * subsample_2_estimate) %>%
    select(draw_id, event_time, estimate)
}


ResultsBySample <- function(sub_results, n_treated) {
  by_sample <- setNames(sub_results, SUB_SAMPLES)
  by_sample[["exact_1_2"]] <- WeightedAggregateCoefMatrix(sub_results, n_treated)
  by_sample
}


SummariseBand <- function(draw_est_long, actual_results, kstar) {
  actual_results_tbl <- tibble(event_time = as.numeric(rownames(actual_results)), actual = actual_results[, "estimate"])
  kstar_draw_tbl     <- draw_est_long %>% filter(draw_id == kstar) %>% transmute(event_time, draw_kstar = estimate)
  draw_est_long %>%
    group_by(event_time) %>%
    summarise(p2.5 = quantile(estimate, 0.025), p50 = median(estimate), p97.5 = quantile(estimate, 0.975), .groups = "drop") %>%
    left_join(kstar_draw_tbl,     by = "event_time") %>%
    left_join(actual_results_tbl, by = "event_time") %>%
    filter(event_time >= MIN_EVENT_TIME, event_time <= MAX_EVENT_TIME) %>%
    arrange(event_time)
}


CoefMatrix <- function(event_time, estimate, ci_low, ci_high) {
  sd_from_ci_width <- (ci_high - ci_low) / (2 * 1.96)
  matrix(c(estimate, sd_from_ci_width, ci_low, ci_high), ncol = 4,
         dimnames = list(as.character(event_time), c("estimate", "sd", "ci_low", "ci_high")))
}


BandModelMatrices <- function(event_study_band) {
  model_bounds <- cbind(ci_low = event_study_band$p2.5, ci_high = event_study_band$p97.5)
  rownames(model_bounds) <- as.character(event_study_band$event_time)
  list(
    matrix = CoefMatrix(event_study_band$event_time, event_study_band$p50,
                        event_study_band$p2.5, event_study_band$p97.5),
    bounds = model_bounds
  )
}


PlotBand <- function(event_study_band, actual_results, sample_name, outcome, normalize) {
  normalization_label <- ifelse(normalize, "norm", "raw")
  out_path            <- file.path(OUTDIR, VARIANT, DISTRIBUTION, ESTIMATION, IMPORTANCE_TYPE,
                                   sample_name, CONTROL_GROUP, "bands", normalization_label,
                                   paste0(outcome, ".png"))
  dir_create(dirname(out_path), recurse = TRUE)

  event_labels  <- as.character(event_study_band$event_time)
  actual_matrix <- actual_results[rownames(actual_results) %in% event_labels, , drop = FALSE]
  model         <- BandModelMatrices(event_study_band)

  png(out_path, width = 1000, height = 700, res = 110)
  PlotEventStudyComparison(
    es_list       = list(list(results = actual_matrix), list(results = model$matrix)),
    legend_labels = c("Actual", "Model"),
    legend_title  = NULL,
    add_comparison = FALSE, add_pretrends = TRUE,
    pt_pch        = c(20, 20),
    ci_bounds     = list(NULL, model$bounds),
    ylim          = ComputeSharedYLim(list(actual_matrix, model$matrix))
  )
  dev.off()
}


BuildActualTreatedModelControlSample <- function(sample_data, outcome) {
  treated_repos  <- unique(sample_data$skeleton$repo_name[sample_data$skeleton$treatment_group != 0])
  actual_treated <- sample_data$actual %>%
    filter(repo_name %in% treated_repos) %>%
    transmute(repo_name, quasi_event_time, actual_treated_value = .data[[outcome]])
  sample_data$draw_list <- lapply(sample_data$draw_list, function(draw)
    draw %>%
      left_join(actual_treated, by = c("repo_name", "quasi_event_time")) %>%
      mutate(!!outcome := if_else(is.na(actual_treated_value), .data[[outcome]], actual_treated_value)) %>%
      select(-actual_treated_value))
  sample_data
}


PlotActualTreatedModelControl <- function(event_study_band, actual_results, sample_name, outcome, normalize) {
  normalization_label <- ifelse(normalize, "norm", "raw")
  out_path            <- file.path(OUTDIR, VARIANT, DISTRIBUTION, ESTIMATION, IMPORTANCE_TYPE,
                                   sample_name, CONTROL_GROUP, "actual_treated_model_control", normalization_label,
                                   paste0(outcome, ".png"))
  dir_create(dirname(out_path), recurse = TRUE)

  event_labels  <- as.character(event_study_band$event_time)
  actual_matrix <- actual_results[rownames(actual_results) %in% event_labels, , drop = FALSE]
  model         <- BandModelMatrices(event_study_band)

  png(out_path, width = 1000, height = 700, res = 110)
  PlotEventStudyComparison(
    es_list       = list(list(results = actual_matrix), list(results = model$matrix)),
    legend_labels = c("Actual (treated + control)", "Modeled control"),
    legend_title  = NULL,
    add_comparison = FALSE, add_pretrends = TRUE,
    pt_pch        = c(20, 20),
    ci_bounds     = list(NULL, model$bounds),
    ylim          = ComputeSharedYLim(list(actual_matrix, model$matrix))
  )
  dev.off()
}


PlotResilienceComparison <- function(bands_by_resilience_group, sample_name, outcome, normalize, covar_type) {
  group_matrices <- BuildResilienceGroupMatrices(bands_by_resilience_group, sample_name)
  present_groups <- names(group_matrices)
  if (length(present_groups) == 0) return(invisible())

  actual_series <- lapply(present_groups, function(resilience_group) list(results = group_matrices[[resilience_group]]$actual))
  model_series  <- lapply(present_groups, function(resilience_group) list(results = group_matrices[[resilience_group]]$model))
  model_bounds  <- lapply(present_groups, function(resilience_group) group_matrices[[resilience_group]]$model_bounds)

  event_study_series <- c(actual_series, model_series)
  ci_bounds          <- c(rep(list(NULL), length(present_groups)), model_bounds)
  legend_labels      <- c(paste("Actual", present_groups), paste("Model", present_groups))
  point_symbols      <- c(rep(20, length(present_groups)), rep(4, length(present_groups)))
  shared_ylim        <- ComputeSharedYLim(lapply(event_study_series, function(series) series$results))

  normalization_label <- ifelse(normalize, "norm", "raw")
  out_path <- file.path(OUTDIR, VARIANT, DISTRIBUTION, ESTIMATION, IMPORTANCE_TYPE, sample_name, CONTROL_GROUP,
                        "resilience", covar_type, normalization_label, paste0(outcome, ".png"))
  dir_create(dirname(out_path), recurse = TRUE)
  png(out_path, width = 1000, height = 700, res = 110)
  PlotEventStudyComparison(
    es_list       = event_study_series,
    legend_labels = legend_labels, legend_title = "Predicted resilience",
    add_comparison = FALSE, add_pretrends = TRUE,
    pt_pch        = point_symbols,
    ci_bounds     = ci_bounds,
    ylim          = shared_ylim
  )
  dev.off()
}


BuildResilienceGroupMatrices <- function(bands_by_resilience_group, sample_name) {
  group_matrices <- list()
  for (resilience_group in RESILIENCE_GROUPS) {
    group_bands <- bands_by_resilience_group[[resilience_group]]
    if (is.null(group_bands)) next
    event_study_band <- group_bands$band_by_sample[[sample_name]]
    actual_results   <- group_bands$actual_by_sample[[sample_name]]
    if (is.null(event_study_band) || is.null(actual_results)) next
    event_labels <- as.character(event_study_band$event_time)
    model        <- BandModelMatrices(event_study_band)
    group_matrices[[resilience_group]] <- list(
      actual       = actual_results[rownames(actual_results) %in% event_labels, , drop = FALSE],
      model        = model$matrix,
      model_bounds = model$bounds
    )
  }
  group_matrices
}


AppendResilienceBandRows <- function(resilience_band_rows, bands_by_resilience_group, sample_name, outcome, normalize, covar_type) {
  for (resilience_group in RESILIENCE_GROUPS) {
    group_bands <- bands_by_resilience_group[[resilience_group]]
    if (is.null(group_bands)) next
    event_study_band <- group_bands$band_by_sample[[sample_name]]
    if (is.null(event_study_band)) next
    resilience_band_rows[[length(resilience_band_rows) + 1]] <- event_study_band %>%
      mutate(sample = sample_name, covar_type = covar_type, outcome = outcome,
             normalize = normalize, split_value = resilience_group)
  }
  resilience_band_rows
}


Main()
