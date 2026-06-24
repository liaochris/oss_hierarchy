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
INDIR_DRAWS  <- "output/analysis/model_prediction"
OUTDIR       <- "output/analysis/model_event_study"

VARIANT      <- "opened_cohort"
DISTRIBUTION <- "adaptive"
ESTIMATION   <- "pooled"
IMPORTANCE_TYPE <- PRIMARY_IMPORTANCE_TYPE
ROLLING_LABEL   <- PRIMARY_ROLLING_LABEL
CONTROL_GROUP   <- PRIMARY_CONTROL_GROUP
SUB_SAMPLES     <- AGGREGATED_SAMPLES[["exact_1_2"]]

GLOBAL_SETTINGS <- LoadProjectConfig("source/lib/config/global_settings.json")
N_CORES         <- GLOBAL_SETTINGS$n_jobs

ES_OUTCOMES <- c("pull_request_merged_total", "pull_request_opened")
KSTAR       <- 0


Main <- function() {
  samples_data <- setNames(lapply(SUB_SAMPLES, LoadSampleData), SUB_SAMPLES)
  n_treated    <- vapply(samples_data, function(sample_data) sample_data$n_treated, integer(1))
  weights      <- n_treated / sum(n_treated)

  band_rows <- list()
  for (normalize in NORM_OPTIONS) {
    for (outcome in ES_OUTCOMES) {
      actual_by_subsample <- lapply(samples_data, function(sample_data) FitEventStudy(sample_data$actual, outcome, CONTROL_GROUP, "sa", normalize = normalize, make_plot = FALSE)$results)
      actual_by_sample    <- ResultsBySample(actual_by_subsample, n_treated)

      draw_estimates_by_subsample <- setNames(lapply(SUB_SAMPLES, function(sub_sample) DrawPointEstimates(samples_data[[sub_sample]], outcome, normalize)), SUB_SAMPLES)
      draw_estimates_by_sample    <- c(draw_estimates_by_subsample,
                                       list(exact_1_2 = AggregateDrawEstimates(draw_estimates_by_subsample, weights)))

      for (sample_name in names(draw_estimates_by_sample)) {
        event_study_band <- SummariseBand(draw_estimates_by_sample[[sample_name]], actual_by_sample[[sample_name]], KSTAR)
        PlotBand(event_study_band, actual_by_sample[[sample_name]], sample_name, outcome, normalize)
        band_rows[[length(band_rows) + 1]] <- event_study_band %>% mutate(sample = sample_name, outcome = outcome, normalize = normalize)
      }
    }
  }

  band_table <- bind_rows(band_rows) %>%
    select(sample, outcome, normalize, event_time, p2.5, p50, p97.5, draw_kstar, actual)
  dir_create(OUTDIR, recurse = TRUE)
  SaveData(band_table, c("sample", "outcome", "normalize", "event_time"),
           file.path(OUTDIR, "band_estimates.csv"), file.path(OUTDIR, "band_estimates.log"),
           sortbykey = FALSE)
}


LoadSampleData <- function(sub_sample) {
  panel    <- LoadPreparedSample(INDIR_PANEL, IMPORTANCE_TYPE, ROLLING_LABEL, sub_sample, CONTROL_GROUP)
  observed <- LoadObservedOpenedCohort(sub_sample)
  draws    <- read_parquet(file.path(INDIR_DRAWS, VARIANT, DISTRIBUTION, "draws", ESTIMATION,
                                      IMPORTANCE_TYPE, sub_sample, CONTROL_GROUP, "raw_draws.parquet"))

  common_repos <- Reduce(intersect, list(unique(panel$repo_name), unique(observed$repo_name), unique(draws$repo_name)))
  skeleton     <- panel %>% filter(repo_name %in% common_repos) %>% select(-all_of(ES_OUTCOMES))
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


LoadObservedOpenedCohort <- function(sub_sample) {
  member_dir <- file.path(INDIR_MEMBER, VARIANT, IMPORTANCE_TYPE, sub_sample, CONTROL_GROUP)
  open_dataset(member_dir) %>%
    select(repo_name, quasi_event_time, repo_pull_request_opened,
           repo_pull_request_merged_direct, repo_pull_request_merged_after_review) %>%
    distinct() %>%
    collect() %>%
    transmute(repo_name, quasi_event_time,
              pull_request_opened = repo_pull_request_opened,
              pull_request_merged_total = repo_pull_request_merged_direct + repo_pull_request_merged_after_review)
}


DrawPointEstimates <- function(sample_data, outcome, normalize) {
  draw_estimates <- mclapply(sample_data$draw_list, function(draw) {
    draw_panel <- sample_data$skeleton %>%
      inner_join(draw %>% select(repo_name, quasi_event_time, all_of(outcome)), by = c("repo_name", "quasi_event_time"))
    EventStudyPointEstimates(draw_panel, outcome, normalize) %>% mutate(draw_id = draw$draw_id[1])
  }, mc.cores = N_CORES)
  bind_rows(draw_estimates)
}


EventStudyPointEstimates <- function(panel_data, outcome, normalize) {
  if (normalize) {
    panel_data       <- NormalizeOutcome(panel_data, outcome)
    outcome_variable <- paste0(outcome, "_norm")
  } else {
    outcome_variable <- outcome
  }
  model_fit               <- feols(as.formula(sprintf("%s ~ sunab(treatment_group, time_index, ref.p=-1) | repo_name + time_index", outcome_variable)), panel_data)
  event_time_coefficients <- coef(model_fit)
  event_time_coefficients <- event_time_coefficients[grepl("^time_index::", names(event_time_coefficients))]
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


PlotBand <- function(event_study_band, actual_results, sample_name, outcome, normalize) {
  normalization_label <- ifelse(normalize, "norm", "raw")
  out_path            <- file.path(OUTDIR, VARIANT, DISTRIBUTION, ESTIMATION, IMPORTANCE_TYPE,
                                   sample_name, CONTROL_GROUP, "bands", normalization_label, paste0(outcome, ".png"))
  dir_create(dirname(out_path), recurse = TRUE)

  event_labels        <- as.character(event_study_band$event_time)
  actual_matrix       <- actual_results[rownames(actual_results) %in% event_labels, , drop = FALSE]
  model_median_matrix <- CoefMatrix(event_study_band$event_time, event_study_band$p50,        event_study_band$p2.5,      event_study_band$p97.5)
  kstar_matrix        <- CoefMatrix(event_study_band$event_time, event_study_band$draw_kstar, event_study_band$draw_kstar, event_study_band$draw_kstar)
  model_bounds        <- cbind(ci_low = event_study_band$p2.5, ci_high = event_study_band$p97.5)
  rownames(model_bounds) <- event_labels

  png(out_path, width = 1000, height = 700, res = 110)
  PlotEventStudyComparison(
    es_list       = list(list(results = actual_matrix), list(results = model_median_matrix), list(results = kstar_matrix)),
    legend_labels = c("Actual", "Model", "Model draw 0"),
    legend_title  = NULL,
    add_comparison = FALSE, add_pretrends = TRUE,
    pt_pch        = c(20, 20, 4),
    ci_bounds     = list(NULL, model_bounds, NULL),
    ylim          = ComputeSharedYLim(list(actual_matrix, model_median_matrix, kstar_matrix))
  )
  dev.off()
}


Main()
