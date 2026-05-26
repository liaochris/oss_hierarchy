library(tidyverse)
library(arrow)
library(fs)
library(SaveData)
library(grf)
library(zeallot)

`%ni%` <- Negate(`%in%`)

# ---- Constructor ------------------------------------------------------------------

NewEventStudyForestPipeline <- function(outcome, rolling_period, n_folds = N_FOLDS, seed = SEED) {
  structure(
    list(
      cfg       = list(outcome = outcome, rolling_period = rolling_period,
                       n_folds = n_folds, seed = seed),
      data      = NULL,
      results   = NULL,
      assembled = NULL,
      phase     = "initialized"
    ),
    class = "EventStudyForestPipeline"
  )
}

# ---- Public API -------------------------------------------------------------------

PrepareData <- function(pipeline, ...) UseMethod("PrepareData")
PrepareData.EventStudyForestPipeline <- function(pipeline, df_data, feature_cols, marg_dist) {
  stopifnot(pipeline$phase == "initialized")
  pipeline$data  <- list(df_data = df_data, feature_cols = feature_cols, marg_dist = marg_dist)
  pipeline$phase <- "prepared"
  pipeline
}

FitOutOfSample <- function(pipeline, ...) UseMethod("FitOutOfSample")
FitOutOfSample.EventStudyForestPipeline <- function(pipeline, outdir_ds, use_existing = FALSE) {
  stopifnot(pipeline$phase == "prepared")
  pipeline$results <- FitHeldOutPredictions(pipeline$cfg, pipeline$data, outdir_ds, use_existing)
  pipeline$phase   <- "fitted"
  pipeline
}

Assemble <- function(pipeline, ...) UseMethod("Assemble")
Assemble.EventStudyForestPipeline <- function(pipeline) {
  stopifnot(pipeline$phase == "fitted")
  results <- pipeline$results
  c(cohort_time_repo, df_out) %<-% list(
    BuildCohortTimeCoefficients(results$tau_hat, results$df_data, pipeline$cfg$outcome),
    BuildRepoLevelOutput(results$tau_hat, results$doubly_robust_scores,
                         results$df_data, pipeline$cfg, results$feature_cols, results$max_time_val)
  )
  pipeline$assembled <- list(cohort_time_repo = cohort_time_repo, df_out = df_out)
  pipeline$phase     <- "assembled"
  pipeline
}

Save <- function(pipeline, ...) UseMethod("Save")
Save.EventStudyForestPipeline <- function(pipeline, outdir) {
  stopifnot(pipeline$phase == "assembled")
  WritePipelineOutputs(pipeline$cfg, pipeline$assembled, outdir)
  invisible(pipeline)
}

# ---- Cross-fitting ----------------------------------------------------------------
# FitHeldOutPredictions runs K-fold out-of-sample prediction (double ML / cross-fitting).
# Each fold trains on K-1 folds and predicts the held-out fold.
# A full-sample model is persisted as an intermediate artifact after all folds complete.

ResidualizeOutcome <- function(cfg, data, outdir_ds) {
  RunBaselineHeterogeneityCrossFit(
    data$df_data, data$feature_cols, data$marg_dist, outdir_ds,
    outcome   = cfg$outcome,
    n_folds   = cfg$n_folds,
    num_trees = N_TREES
  )
}

FitHeldOutPredictions <- function(cfg, data, outdir_ds, use_existing) {
  df_data      <- ResidualizeOutcome(cfg, data, outdir_ds)
  feature_cols <- data$feature_cols
  n_obs        <- nrow(df_data)
  max_time_val <- max(df_data$time_index, na.rm = TRUE)

  tau_hat              <- NULL
  doubly_robust_scores <- NULL

  for (fold_id in seq_len(cfg$n_folds)) {
    message("fold ", fold_id, " / ", cfg$n_folds)
    c(tau_fold, doubly_robust_fold) %<-% FitOneFold(cfg, fold_id, df_data, feature_cols, outdir_ds, use_existing)

    if (is.null(tau_hat)) {
      tau_hat              <- matrix(NA_real_, nrow = n_obs, ncol = ncol(tau_fold),
                                     dimnames = list(NULL, colnames(tau_fold)))
      doubly_robust_scores <- matrix(NA_real_, nrow = n_obs, ncol = ncol(doubly_robust_fold),
                                     dimnames = list(NULL, colnames(doubly_robust_fold)))
    }
    hold_idx                         <- which(df_data$fold == fold_id)
    tau_hat[hold_idx, ]              <- tau_fold
    doubly_robust_scores[hold_idx, ] <- doubly_robust_fold
  }

  # Persist full-sample model as intermediate artifact
  df_all <- df_data %>%
    group_by(repo_id) %>% filter(!any(is.na(resid_outcome))) %>% ungroup()
  FitForestModel(
    df_all, as.matrix(df_all %>% select(all_of(feature_cols))), df_all$resid_outcome,
    model_path   = file.path(outdir_ds, paste0("event_study_forest_", cfg$outcome, ".rds")),
    use_existing = use_existing,
    seed         = cfg$seed
  )

  list(tau_hat = tau_hat, doubly_robust_scores = doubly_robust_scores, df_data = df_data,
       feature_cols = feature_cols, max_time_val = max_time_val)
}

FitOneFold <- function(cfg, fold_id, df_data, feature_cols, outdir_ds, use_existing) {
  df_train <- df_data %>% filter(fold != fold_id) %>%
    group_by(repo_id) %>% filter(!any(is.na(resid_outcome))) %>% ungroup()
  df_hold  <- df_data %>% filter(fold == fold_id)
  x_train  <- as.matrix(df_train %>% select(all_of(feature_cols)))
  y_train  <- df_train$resid_outcome
  x_hold   <- as.matrix(df_hold %>% select(all_of(feature_cols)))
  y_hold   <- df_hold$resid_outcome

  model_path <- file.path(outdir_ds,
    paste0("event_study_forest_", cfg$outcome, "_fold", fold_id, ".rds"))
  forest_obj <- FitForestModel(df_train, x_train, y_train, model_path,
                               use_existing = use_existing, seed = cfg$seed)

  list(
    predict(forest_obj$model, newdata = x_hold, drop = TRUE)$predictions,
    ComputeFoldDoublyRobust(forest_obj, df_hold, x_hold, y_hold)
  )
}

# ---- Forest model -----------------------------------------------------------------

TrainForestModel <- function(df_train, x_train, y_train, seed) {
  set.seed(seed)
  W                   <- BuildTreatmentMatrix(df_train)
  treatment_model     <- probability_forest(x_train, as.factor(df_train$treatment_group),
                                            clusters = df_train$repo_id)
  prob_cohort_given_x <- predict(treatment_model)$predictions
  W_hat               <- AdjustPropensityMatrix(prob_cohort_given_x, W, df_train)
  outcome_model       <- regression_forest(x_train, y_train,
                                           clusters = df_train$repo_id, num.trees = N_TREES)
  model               <- lm_forest(X = x_train, Y = y_train, W = W,
                                   clusters = df_train$repo_id, num.trees = N_TREES, W.hat = W_hat)
  list(model = model, treatment_model = treatment_model, outcome_model = outcome_model)
}

PersistForestModel <- function(forest_obj, model_path) {
  dir_create(dirname(model_path))
  saveRDS(forest_obj$model,           model_path)
  saveRDS(forest_obj$treatment_model, sub("\\.rds$", "_treatment.rds", model_path))
  saveRDS(forest_obj$outcome_model,   sub("\\.rds$", "_outcome.rds",   model_path))
}

FitForestModel <- function(df_train, x_train, y_train, model_path, use_existing, seed = SEED) {
  treatment_file <- sub("\\.rds$", "_treatment.rds", model_path)
  outcome_file   <- sub("\\.rds$", "_outcome.rds",   model_path)
  if (file.exists(model_path) && file.exists(treatment_file) && file.exists(outcome_file) && isTRUE(use_existing)) {
    return(list(
      model           = readRDS(model_path),
      treatment_model = readRDS(treatment_file),
      outcome_model   = readRDS(outcome_file)
    ))
  }
  forest_obj <- TrainForestModel(df_train, x_train, y_train, seed)
  PersistForestModel(forest_obj, model_path)
  forest_obj
}

# ---- Doubly robust scoring --------------------------------------------------------

ComputeIPW <- function(W, W_hat, treated_rows, treated_idx) {
  IPW                  <- matrix(0, nrow(W), ncol(W))
  IPW[-treated_rows, ] <- -1 / (1 - rowSums(W_hat[-treated_rows, , drop = FALSE]))
  IPW[treated_idx]     <- 1 / W_hat[treated_idx]
  IPW
}

ComputeFoldDoublyRobust <- function(forest_obj, df_hold, x_hold, y_hold) {
  y_hat               <- predict(forest_obj$outcome_model,   x_hold)$predictions
  W                   <- BuildTreatmentMatrix(df_hold)
  prob_cohort_given_x <- predict(forest_obj$treatment_model, x_hold)$predictions
  W_hat               <- AdjustPropensityMatrix(prob_cohort_given_x, W, df_hold)

  preds        <- predict(forest_obj$model, newdata = x_hold)$predictions[, , "Y.1"]
  treated_rows <- which(rowSums(W) == 1)
  treated_cols <- max.col(W[treated_rows, , drop = FALSE])
  treated_idx  <- cbind(treated_rows, treated_cols) # for each treated unit, selects its own treatment cohort and event time

  y_hat_baseline                                <- y_hat - rowSums(W_hat * preds)
  predicted_baseline_outcome                    <- y_hat_baseline
  predicted_baseline_outcome[treated_rows]      <- y_hat_baseline[treated_rows] + preds[treated_idx]

  y_residual <- y_hold - predicted_baseline_outcome
  IPW        <- ComputeIPW(W, W_hat, treated_rows, treated_idx)
  preds + IPW * as.vector(y_residual)
}

# ---- Baseline heterogeneity cross-fit ---------------------------------------------

FitBaselineControlsFold <- function(df_data, feature_cols, fold_id, marg_dist,
                                     outdir_ds, outcome, num_trees = N_TREES) {
  df_fold     <- df_data %>% filter(fold != fold_id)
  df_hold     <- df_data %>% filter(fold == fold_id)
  x_hold      <- df_hold %>% select(all_of(feature_cols))
  df_panel_nt <- df_fold %>% filter(treatment_arm == "never-treated")

  if (nrow(df_panel_nt) == 0)
    return(list(integer(0), numeric(0), numeric(0)))

  quasi_times    <- as.numeric(names(table(df_fold$quasi_treatment_group))) - 1
  df_nt_in_quasi <- df_panel_nt %>% filter(time_index %in% quasi_times)

  W_nt  <- MakeTimeIndicatorMatrix(df_nt_in_quasi)
  W_hat <- ComputeBaselinePropensities(W_nt, df_nt_in_quasi, marg_dist)
  model <- lm_forest(df_nt_in_quasi %>% select(all_of(feature_cols)),
                     df_nt_in_quasi$fd_outcome,
                     W_nt, num.trees = num_trees, W.hat = W_hat,
                     clusters = df_nt_in_quasi$repo_id)

  time_varying_controls_dir <- file.path(outdir_ds, "time_varying_controls")
  dir_create(time_varying_controls_dir)
  saveRDS(model, file.path(time_varying_controls_dir,
    paste0("event_study_forest_", outcome, "_fold", fold_id, ".rds")))

  hold_idx <- which(df_data$fold == fold_id)
  pred_raw <- predict(model, x_hold)$predictions
  pred_mat <- if (length(dim(pred_raw)) == 3) as.matrix(pred_raw[, , "Y.1"]) else as.matrix(pred_raw)

  non_quasi_times <- df_panel_nt %>%
    filter(time_index %ni% quasi_times) %>% pull(time_index) %>% unique() %>% sort()
  for (ti in non_quasi_times) {
    df_nt_ti <- df_panel_nt %>% filter(time_index == ti)
    model_ti <- regression_forest(df_nt_ti %>% select(all_of(feature_cols)),
                                  df_nt_ti$fd_outcome,
                                  num.trees = num_trees, clusters = df_nt_ti$repo_id)
    saveRDS(model_ti, file.path(time_varying_controls_dir,
      paste0("event_study_forest_", outcome, "_fold", fold_id, "_time_index", ti, ".rds")))
    pred_mat <- cbind(pred_mat, predict(model_ti, x_hold)$predictions)
    colnames(pred_mat)[ncol(pred_mat)] <- paste0("factor.time_index.", ti)
  }

  time_idx  <- match(paste0("factor.time_index.", df_hold$time_index),               colnames(pred_mat))
  quasi_idx <- match(paste0("factor.time_index.", df_hold$quasi_treatment_group - 1), colnames(pred_mat))
  list(
    hold_idx,
    vapply(seq_len(nrow(pred_mat)), function(i) pred_mat[i, time_idx[i]],  numeric(1)),
    vapply(seq_len(nrow(pred_mat)), function(i) pred_mat[i, quasi_idx[i]], numeric(1))
  )
}

RunBaselineHeterogeneityCrossFit <- function(df_data, feature_cols, marg_dist, outdir_ds,
                                              outcome, n_folds = N_FOLDS, num_trees = N_TREES) {
  n_obs                <- nrow(df_data)
  time_baseline        <- numeric(n_obs)
  norm_period_baseline <- numeric(n_obs)

  for (fold_id in seq_len(n_folds)) {
    c(hold_idx, time_vals, quasi_vals) %<-% FitBaselineControlsFold(
      df_data, feature_cols, fold_id, marg_dist, outdir_ds, outcome, num_trees
    )
    if (length(hold_idx) > 0) {
      time_baseline[hold_idx]        <- time_vals
      norm_period_baseline[hold_idx] <- quasi_vals
    }
  }

  df_data %>% mutate(
    time_baseline_heterogeneity        = time_baseline,
    norm_period_baseline_heterogeneity = norm_period_baseline,
    resid_outcome = fd_outcome - time_baseline_heterogeneity + norm_period_baseline_heterogeneity
  )
}

# ---- Output assembly --------------------------------------------------------------

BuildCohortTimeCoefficients <- function(tau_hat, df_data, outcome) {
  tau_hat_filt <- FilterPredictions(tau_hat, df_data)
  TransformRepoCohortTimeCoefficients(tau_hat_filt, df_data, outcome, "coef") %>%
    filter(!is.na(coef))
}

BuildRepoLevelOutput <- function(tau_hat, doubly_robust_scores, df_data, cfg, feature_cols, max_time_val) {
  df_repo_data               <- df_data %>% select(repo_id, repo_name, quasi_treatment_group, treatment_group) %>% distinct()
  tau_hat_filt               <- FilterPredictions(tau_hat, df_data)
  doubly_robust_scores_repo  <- AggregateDoublyRobustByRepo(doubly_robust_scores, df_data)
  doubly_robust_repo_filt    <- FilterDoublyRobustPredictions(doubly_robust_scores_repo, df_data, df_repo_data)
  doubly_robust_repo_filt_df <- data.frame(
    doubly_robust_repo_filt,
    repo_id = as.numeric(rownames(doubly_robust_repo_filt))
  )

  att_repo               <- AggregateRepoATT(tau_hat_filt, df_data, max_time_val)
  doubly_robust_att_repo <- AggregateRepoATT(doubly_robust_repo_filt_df, df_repo_data, max_time_val) %>%
    rename(att_doubly_robust = att)
  event_time_repo        <- AggregateRepoEventStudy(tau_hat_filt, df_data, max_time_val)

  repo_level <- att_repo %>%
    left_join(doubly_robust_att_repo, by = c("repo_name", "repo_id")) %>%
    left_join(event_time_repo,        by = c("repo_name", "repo_id")) %>%
    mutate(
      outcome                  = cfg$outcome,
      att_group                = case_when(
        !is.na(att)                & att                >= median(att,                na.rm = TRUE) ~ "high",
        !is.na(att)                & att                <  median(att,                na.rm = TRUE) ~ "low",
        TRUE ~ NA_character_
      ),
      att_doubly_robust_group  = case_when(
        !is.na(att_doubly_robust) & att_doubly_robust >= median(att_doubly_robust, na.rm = TRUE) ~ "high",
        !is.na(att_doubly_robust) & att_doubly_robust <  median(att_doubly_robust, na.rm = TRUE) ~ "low",
        TRUE ~ NA_character_
      )
    ) %>%
    select(outcome, repo_name, repo_id, att, att_group, att_doubly_robust, att_doubly_robust_group, everything())

  repo_level %>%
    left_join(doubly_robust_repo_filt_df, by = "repo_id") %>%
    left_join(df_data %>% select(all_of(feature_cols), repo_name) %>% distinct(), by = "repo_name")
}

WritePipelineOutputs <- function(cfg, assembled, outdir) {
  dir_create(outdir)
  write_parquet(assembled$cohort_time_repo,
    file.path(outdir, paste0(cfg$outcome, "_cohort_time_repo_coeffs_event_study_forest.parquet")))
  SaveData(assembled$df_out,
    c("outcome", "repo_name", "repo_id"),
    file.path(outdir, paste0(cfg$outcome, "_repo_att_event_study_forest.parquet")),
    file.path(outdir, paste0(cfg$outcome, "_repo_att_event_study_forest.log")))
}
