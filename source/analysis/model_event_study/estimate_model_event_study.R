library(tidyverse)
library(arrow)
library(fs)

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

ES_OUTCOMES         <- c("pull_request_merged", "pull_request_opened")
KSTAR               <- 0


Main <- function() {
  samples_data <- setNames(lapply(SUB_SAMPLES, LoadSampleData), SUB_SAMPLES)
  n_treated    <- vapply(samples_data, function(s) s$n_treated, integer(1))

  for (outcome in ES_OUTCOMES) {
    actual_sub <- lapply(samples_data, function(s) FitEventStudy(s$actual, outcome, CONTROL_GROUP, "sa", normalize = TRUE, make_plot = FALSE)$results)
    model_sub  <- lapply(samples_data, function(s) FitEventStudy(s$model,  outcome, CONTROL_GROUP, "sa", normalize = TRUE, make_plot = FALSE)$results)

    results_by_sample <- list()
    for (i in seq_along(SUB_SAMPLES)) {
      results_by_sample[[SUB_SAMPLES[i]]] <- list(actual = actual_sub[[i]], model = model_sub[[i]])
    }
    results_by_sample[["exact_1_2"]] <- list(
      actual = WeightedAggregateCoefMatrix(actual_sub, n_treated),
      model  = WeightedAggregateCoefMatrix(model_sub,  n_treated)
    )

    for (sample_name in names(results_by_sample)) {
      PlotActualVsModel(results_by_sample[[sample_name]]$actual,
                        results_by_sample[[sample_name]]$model,
                        sample_name, outcome)
    }
  }
}


LoadSampleData <- function(sub_sample) {
  panel    <- LoadPreparedSample(INDIR_PANEL, IMPORTANCE_TYPE, ROLLING_LABEL, sub_sample, CONTROL_GROUP)
  observed <- LoadObservedOpenedCohort(sub_sample)
  model    <- LoadModelDraw(sub_sample, KSTAR)

  common_repos <- Reduce(intersect, list(unique(panel$repo_name), unique(observed$repo_name), unique(model$repo_name)))
  skeleton     <- panel %>% filter(repo_name %in% common_repos) %>% select(-all_of(ES_OUTCOMES))

  actual_panel <- skeleton %>% inner_join(observed, by = c("repo_name", "quasi_event_time"))
  model_panel  <- skeleton %>% inner_join(model,    by = c("repo_name", "quasi_event_time"))
  stopifnot(!anyNA(actual_panel[ES_OUTCOMES]), !anyNA(model_panel[ES_OUTCOMES]))

  list(
    actual    = actual_panel,
    model     = model_panel,
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
              pull_request_merged = repo_pull_request_merged_direct + repo_pull_request_merged_after_review)
}


LoadModelDraw <- function(sub_sample, draw_id) {
  draws_path <- file.path(INDIR_DRAWS, VARIANT, DISTRIBUTION, "draws", ESTIMATION,
                          IMPORTANCE_TYPE, sub_sample, CONTROL_GROUP, "raw_draws.parquet")
  read_parquet(draws_path) %>%
    filter(draw_id == !!draw_id) %>%
    select(repo_name, quasi_event_time, pull_request_opened, pull_request_merged)
}


PlotActualVsModel <- function(actual_results, model_results, sample_name, outcome) {
  out_path <- file.path(OUTDIR, VARIANT, DISTRIBUTION, ESTIMATION, IMPORTANCE_TYPE,
                        sample_name, CONTROL_GROUP, "onedraw", paste0(outcome, ".png"))
  dir_create(dirname(out_path), recurse = TRUE)
  png(out_path, width = 1000, height = 700, res = 110)
  PlotEventStudyComparison(
    es_list       = list(list(results = actual_results), list(results = model_results)),
    legend_labels = c("Actual (opened-cohort)", paste0("Model draw ", KSTAR)),
    legend_title  = paste(sample_name, outcome, sep = " | "),
    add_comparison = FALSE, add_pretrends = TRUE
  )
  dev.off()
}


Main()
