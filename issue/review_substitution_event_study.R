# TEMPORARY EXPERIMENT (issue/): re-run the model event study for the exact1 sample on the ORIGINAL
# draws and on the counterfactual draws (post-period merge-after-review rebuilt off the ACTUAL review
# count; see review_substitution_experiment.py), then overlay the two Model bands against the fixed
# Actual line for pull_request_merged and pull_request_merged_after_review, norm + raw.
#
# Reuses the real event-study code path (DrawPointEstimates / feols sunab / SummariseBand) so the
# comparison is apples-to-apples with output/analysis/model_event_study.

library(tidyverse)
library(arrow)
library(fs)
library(fixest)
library(parallel)

source("source/lib/R/config_loaders.R")
source("source/lib/R/analysis_utils.R")
source("source/lib/R/event_study_helpers.R")
source("source/lib/R/constants.R")

INDIR_PANEL  <- "output/derived/analysis_panel"
INDIR_MEMBER <- "drive/output/derived/model_prediction/event_time_member_panel"
ORIG_DRAWS   <- "drive/output/analysis/model_prediction"
CF_DRAWS      <- "issue/review_substitution/draws"   # counterfactual raw_draws for exact1
OUTDIR        <- "issue/review_substitution"

VARIANT <- "opened_cohort"; DISTRIBUTION <- "adaptive"; ESTIMATION <- "pooled"
IMPORTANCE_TYPE <- PRIMARY_IMPORTANCE_TYPE
ROLLING_LABEL   <- PRIMARY_ROLLING_LABEL
CONTROL_GROUP   <- PRIMARY_CONTROL_GROUP
SAMPLE          <- "exact1"
OUTCOMES        <- c("pull_request_merged", "pull_request_merged_after_review")
ES_OUTCOMES     <- c("pull_request_opened", "pull_request_reviewed",
                     "pull_request_merged_direct", "pull_request_merged_after_review", "pull_request_merged")
KSTAR           <- 0
N_CORES         <- LoadProjectConfig("source/lib/config/global_settings.json")$n_jobs
setFixest_nthreads(1)


Main <- function() {
  dir_create(OUTDIR, recurse = TRUE)
  orig_sample <- LoadSampleData(ORIG_DRAWS)
  cf_sample   <- LoadSampleData(CF_DRAWS, cf = TRUE)

  band_rows <- list()
  for (normalize in NORM_OPTIONS) {
    for (outcome in OUTCOMES) {
      actual_band <- FitEventStudy(orig_sample$actual, outcome, CONTROL_GROUP, "sa",
                                   normalize = normalize, make_plot = FALSE)$results
      actual_tbl  <- tibble(event_time = as.numeric(rownames(actual_band)), actual = actual_band[, "estimate"])

      orig_band <- SummariseBand(DrawPointEstimates(orig_sample, outcome, normalize), KSTAR)
      cf_band   <- SummariseBand(DrawPointEstimates(cf_sample,   outcome, normalize), KSTAR)

      PlotOverlay(actual_tbl, orig_band, cf_band, outcome, normalize)
      band_rows[[length(band_rows) + 1]] <- orig_band %>% left_join(actual_tbl, by = "event_time") %>%
        transmute(outcome, normalize, event_time, actual,
                  orig_p2.5 = p2.5, orig_p50 = p50, orig_p97.5 = p97.5)
      band_rows[[length(band_rows)]] <- band_rows[[length(band_rows)]] %>%
        left_join(cf_band %>% transmute(event_time, cf_p2.5 = p2.5, cf_p50 = p50, cf_p97.5 = p97.5),
                  by = "event_time")
    }
  }
  comparison <- bind_rows(band_rows) %>%
    mutate(orig_gap_to_actual = orig_p50 - actual, cf_gap_to_actual = cf_p50 - actual,
           gap_closed_frac = 1 - cf_gap_to_actual / orig_gap_to_actual)
  write_csv(comparison, file.path(OUTDIR, "band_comparison.csv"))
  print(comparison %>% filter(event_time >= 0) %>%
          select(outcome, normalize, event_time, actual, orig_p50, cf_p50,
                 orig_gap_to_actual, cf_gap_to_actual, gap_closed_frac) %>%
          mutate(across(where(is.numeric), ~round(.x, 3))), n = 100)
}


LoadSampleData <- function(draws_root, cf = FALSE) {
  panel    <- LoadPreparedSample(INDIR_PANEL, IMPORTANCE_TYPE, ROLLING_LABEL, SAMPLE, CONTROL_GROUP)
  observed <- LoadObservedOpenedCohort()
  draws_path <- if (cf) file.path(draws_root, SAMPLE, "raw_draws.parquet")
                else file.path(draws_root, VARIANT, DISTRIBUTION, "draws", ESTIMATION,
                               IMPORTANCE_TYPE, SAMPLE, CONTROL_GROUP, "raw_draws.parquet")
  draws <- read_parquet(draws_path)

  common_repos <- Reduce(intersect, list(unique(panel$repo_name), unique(observed$repo_name), unique(draws$repo_name)))
  skeleton     <- panel %>% filter(repo_name %in% common_repos) %>% select(-any_of(ES_OUTCOMES))
  draws        <- draws %>% filter(repo_name %in% common_repos) %>%
    select(repo_name, quasi_event_time, draw_id, all_of(ES_OUTCOMES))
  actual_panel <- skeleton %>% inner_join(observed, by = c("repo_name", "quasi_event_time"))
  stopifnot(!anyNA(actual_panel[ES_OUTCOMES]))
  list(skeleton = skeleton, actual = actual_panel, draw_list = split(draws, draws$draw_id))
}


LoadObservedOpenedCohort <- function() {
  member_dir <- file.path(INDIR_MEMBER, VARIANT, IMPORTANCE_TYPE, SAMPLE, CONTROL_GROUP)
  open_dataset(member_dir) %>%
    select(repo_name, quasi_event_time, repo_pull_request_opened, repo_pull_request_reviewed,
           repo_pull_request_merged_direct, repo_pull_request_merged_after_review) %>%
    distinct() %>% collect() %>%
    transmute(repo_name, quasi_event_time,
              pull_request_opened              = repo_pull_request_opened,
              pull_request_reviewed            = repo_pull_request_reviewed,
              pull_request_merged_direct       = repo_pull_request_merged_direct,
              pull_request_merged_after_review = repo_pull_request_merged_after_review,
              pull_request_merged              = repo_pull_request_merged_direct + repo_pull_request_merged_after_review)
}


DrawPointEstimates <- function(sample_data, outcome, normalize) {
  draw_id_chunks <- split(names(sample_data$draw_list), cut(seq_along(sample_data$draw_list), N_CORES, labels = FALSE))
  bind_rows(mclapply(draw_id_chunks, function(chunk_draw_ids)
    ChunkPointEstimates(sample_data, outcome, normalize, chunk_draw_ids), mc.cores = N_CORES))
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
    tibble(event_time = -1, estimate = 0))
}


SummariseBand <- function(draw_est_long, kstar) {
  draw_est_long %>%
    group_by(event_time) %>%
    summarise(p2.5 = quantile(estimate, 0.025), p50 = median(estimate), p97.5 = quantile(estimate, 0.975), .groups = "drop") %>%
    filter(event_time >= MIN_EVENT_TIME, event_time <= MAX_EVENT_TIME) %>%
    arrange(event_time)
}


PlotOverlay <- function(actual_tbl, orig_band, cf_band, outcome, normalize) {
  normalization_label <- ifelse(normalize, "norm", "raw")
  actual_tbl <- actual_tbl %>% filter(event_time >= MIN_EVENT_TIME, event_time <= MAX_EVENT_TIME)
  plot_df <- bind_rows(
    orig_band %>% transmute(event_time, p50, lo = p2.5, hi = p97.5, series = "Model (simulated review)"),
    cf_band   %>% transmute(event_time, p50, lo = p2.5, hi = p97.5, series = "Model (actual review)"))
  g <- ggplot() +
    geom_ribbon(data = plot_df, aes(event_time, ymin = lo, ymax = hi, fill = series), alpha = 0.15) +
    geom_line(data = plot_df, aes(event_time, p50, color = series), linewidth = 0.8) +
    geom_point(data = plot_df, aes(event_time, p50, color = series)) +
    geom_line(data = actual_tbl, aes(event_time, actual), color = "black", linewidth = 0.9) +
    geom_point(data = actual_tbl, aes(event_time, actual), color = "black") +
    geom_vline(xintercept = -1, linetype = "dashed", color = "grey50") +
    geom_hline(yintercept = 0, linetype = "dotted", color = "grey50") +
    labs(title = sprintf("%s  (%s, exact1)  — black = Actual", outcome, normalization_label),
         x = "event time", y = "event-study coefficient", color = NULL, fill = NULL) +
    theme_minimal() + theme(legend.position = "bottom")
  ggsave(file.path(OUTDIR, sprintf("%s_%s.png", outcome, normalization_label)), g, width = 9, height = 6, dpi = 110)
}


Main()
