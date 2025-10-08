library(tidyverse)
library(grf)
library(arrow)
library(yaml)
library(fs)
library(zeallot)
library(rlang)

source("source/lib/helpers.R")
source("source/analysis/causal_forest_helpers.R")

#######################################
# 2. Global Settings
#######################################
SEED <- 420
set.seed(SEED)

INDIR       <- "drive/output/derived/org_characteristics/org_panel"
OUTDIR      <- "output/analysis/causal_forest_personalization"
OUTDIR_DATASTORE <- "drive/output/analysis/causal_forest_personalization"
INDIR_YAML  <- "source/derived/org_characteristics"

# ensure output directory exists and is writable
dir_create(OUTDIR)

#######################################
# 4. Core Analysis (cross-fit per outcome, export cohort-time & event-time)
#######################################
AssignOutcomeFolds <- function(repo_names, outcome, n_folds = 10, seed = SEED) {
  repo_sorted <- sort(unique(repo_names))
  if (length(repo_sorted) == 0) stop("No repositories provided to AssignOutcomeFolds for outcome=", outcome)
  outcome_bytes <- as.integer(charToRaw(as.character(outcome)))
  offset <- (sum(outcome_bytes) + as.integer(seed)) %% as.integer(n_folds)
  n_repos <- length(repo_sorted)
  df <- tibble(repo_name = repo_sorted, rank = seq_len(n_repos))
  df <- df %>% mutate(fold = ((rank - 1) + offset) %% n_folds + 1)
  df %>% select(repo_name, fold)
}

CalculateDoublyRobust <- function(df_hold, method, treatment_model_fold, outcome_model_fold, x_hold,
                                  y_hold, preds) {
  W <- CalculateTreatment(df_hold, method)$W
  W_og_form_hat <- predict(treatment_model_fold, newdata = x_hold)$predictions
  W_hat <- TransformWHat(W_og_form_hat, W, df_hold)
  
  y_hat <- predict(outcome_model_fold, newdata = x_hold)$predictions
  Y_residual <- y_hold - (y_hat + preds * (W - W_hat))
  debiasing_weights <- (W - W_hat) / (W_hat * (1 - W_hat))
  
  return(preds + debiasing_weights * Y_residual)
}

TransformRepoCohortTimeCoefficients <- function(preds_all, df_data, outcome, values_to_col) {
  cohort_time_repo_long <- tibble::as_tibble(preds_all) %>%
    dplyr::bind_cols(df_data %>% dplyr::select(repo_name, repo_id)) %>%
    tidyr::pivot_longer(-c(repo_name, repo_id), names_to = "arm", values_to = values_to_col) %>%
    dplyr::distinct() %>%
    dplyr::mutate(
      arm = sub("^treatment_arm", "", arm),
      cohort = as.integer(stringr::str_extract(arm, "(?<=cohort)\\d+")),
      event_time_sep = dplyr::case_when(
        grepl("event_time\\.", arm) ~ ".",
        grepl("event_time-", arm)  ~ "-",
        TRUE                       ~ ""
      ),
      event_time_num = as.integer(stringr::str_extract(arm, "\\d+$")),
      event_time = dplyr::case_when(
        event_time_sep == "." ~ -event_time_num,
        event_time_sep == "-" ~ -event_time_num,
        TRUE                  ~ event_time_num
      ),
      outcome = outcome
    ) %>%
    dplyr::rename(coef = !!rlang::sym(values_to_col)) %>%
    dplyr::select(outcome, repo_name, repo_id, cohort, event_time, coef)
  
  return(cohort_time_repo_long)
}

AggregateRepoEventStudy <- function(preds_all, df_data, max_time_val) {
  aggregated_repo <- AggregateEventStudy(preds_all, df_data, max_time_val, "event_time") %>%
    as_tibble() %>%
    select(-obs_id) %>%
    bind_cols(df_data %>% select(repo_name, repo_id)) %>%
    distinct()
  return(aggregated_repo)
}

AggregateRepoATT <- function(preds_all, df_data, max_time_val) {
  aggregated_repo_att <- bind_cols(
    df_data %>% select(repo_name, repo_id),
    AggregateEventStudy(preds_all, df_data, max_time_val, "att")
  ) %>%
    distinct()
  return(aggregated_repo_att)
}

RunForestEventStudy_CrossFit <- function(outcome, df_panel_common, org_practice_modes, outdir, outdir_ds, method, rolling_period, n_folds = 10, seed = SEED, use_existing = FALSE) {
  message("Cross-fit (no plotting) for outcome=", outcome, " method=", method, " rolling=", rolling_period, " folds=", n_folds)
  
  if (method == "lm_forest" | method == "multi_arm") {
    df_panel_outcome <- ResidualizeOutcome(df_panel_common, outcome)
  } else if (method == "lm_forest_nonlinear") {
    df_panel_outcome <- NormalizeOutcome(df_panel_common, outcome)
    norm_outcome_col <- paste(outcome, "norm", sep = "_")
    df_panel_outcome <- FirstDifferenceOutcome(df_panel_outcome, norm_outcome_col)
    df_panel_outcome$resid_outcome <- df_panel_outcome$fd_outcome
  } else {
    stop("Unsupported method: ", method)
  }
  
  covars     <- unlist(lapply(org_practice_modes, \(x) x$continuous_covariate))
  covars_imp <- unlist(lapply(org_practice_modes, function(x) {
    if (!str_detect(x$legend_title, "organizational_routines")) paste0(x$continuous_covariate, "_imp")
  }))
  
  df_data <- ComputeCovariateMeans(df_panel_outcome, c(covars, covars_imp), rolling_period) %>%
    filter(quasi_event_time != -1) %>%
    mutate(treatment_arm = factor(ifelse(treatment_group == 0, "never-treated", paste0("cohort", treatment_group, "event_time", quasi_event_time))),
           treatment_arm = relevel(treatment_arm, ref = "never-treated"))
  
  y <- df_data$resid_outcome
  keep_names <- paste0(covars, "_mean")
  max_time_val <- max(df_data$time_index, na.rm = TRUE)
  
  fold_map <- AssignOutcomeFolds(df_data$repo_name, outcome, n_folds = n_folds, seed = seed)
  df_data <- df_data %>% left_join(fold_map, by = "repo_name")
  
  n_obs <- nrow(df_data)
  preds_all <- NULL

  for (fold_id in seq_len(n_folds)) {
    message(" fold ", fold_id, " / ", n_folds)
    train_idx <- which(df_data$fold != fold_id)
    hold_idx  <- which(df_data$fold == fold_id)
    if (length(hold_idx) == 0) {
      message("  no held obs for fold ", fold_id, "; skipping")
      next
    }
    
    df_train <- df_data[train_idx, , drop = FALSE]
    x_train <- as.matrix(df_train %>% select(all_of(keep_names)))
    y_train <- df_train$resid_outcome
    
    df_hold  <- df_data[hold_idx, , drop = FALSE]
    x_hold <- as.matrix(df_hold %>% select(all_of(keep_names)))
    y_hold <- df_hold$resid_outcome
    
    outfile_fold <- file.path(
      outdir_ds, paste0(method, "_", outcome, "_rolling", rolling_period, "_fold", fold_id, ".rds"))
    forest_model_obj <- FitForestModel(
      df_train, x_train, y_train, method, outfile_fold, use_existing = use_existing, seed = SEED)
    model_fold <- forest_model_obj$model
    outcome_model_fold <- forest_model_obj$outcome_model
    treatment_model_fold <- forest_model_obj$treatment_model
    
    pred_obj <- predict(model_fold, newdata = x_hold, drop = TRUE)
    preds <- pred_obj$predictions
    if (is.null(preds_all)) {
      preds_all <- matrix(NA_real_, nrow = n_obs, ncol = ncol(preds))
      colnames(preds_all) <- colnames(preds)
    }
    
    preds_all[hold_idx, ] <- preds
  }
  if (is.null(preds_all) || any(is.na(preds_all))) {
    message("Warning: some observations have missing predicted CATEs (preds_all contains NA). These will propagate to repo-level averages.")
  }

  cohort_time_repo_long <- TransformRepoCohortTimeCoefficients(preds_all, df_data, outcome, "coef")

  
  dir_create(outdir)
  cohort_outfile <- file.path(outdir, paste0(outcome, "_cohort_time_repo_coeffs_", method, ".parquet"))
  write_parquet(cohort_time_repo_long, cohort_outfile)
  
  event_time_repo <- AggregateRepoEventStudy(preds_all, df_data, max_time_val)
  
  att_repo <- AggregateRepoATT(preds_all, df_data, max_time_val)
  
  repo_level <- att_repo %>%
    left_join(event_time_repo, by = c("repo_name", "repo_id")) %>%
    mutate(outcome = outcome) %>%
    select(outcome, everything())
  
  med_att <- median(repo_level$att, na.rm = TRUE)
  repo_level <- repo_level %>%
    mutate(
      att_group = ifelse(!is.na(att) & att >= med_att, "high",
        ifelse(!is.na(att) & att < med_att, "low", NA)
      )
    ) %>%
    select(outcome, repo_name, repo_id, att, att_group, everything())
  
  out_file <- file.path(outdir, paste0(outcome, "_repo_att_", method, ".parquet"))
  write_parquet(repo_level, out_file)
  
  invisible(list(cohort_time = cohort_time_repo_long, repo_level = repo_level))
}

#######################################
# 5. Main Entry Point (driver)
#######################################
outcome_cfg <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))
org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariate_organization.yaml"))

for (variant in c("important_topk", "important_thresh")) {
  for (rolling_panel in c("rolling5")) {
    rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
    
    outdir_ds <- file.path(OUTDIR_DATASTORE, variant, rolling_panel)
    
    df_panel <- read_parquet(file.path(INDIR, variant, paste0("panel_", rolling_panel, ".parquet")))
    all_outcomes <- unlist(lapply(outcome_cfg, function(x) x$main))
    df_panel_common <- BuildCommonSample(df_panel, all_outcomes) %>%
      filter(num_departures <= 1) %>%
      mutate(quasi_event_time = time_index - quasi_treatment_group)
    df_panel_common <- KeepSustainedImportant(df_panel_common)
    
    outdir_base <- file.path(OUTDIR, variant, rolling_panel)
    org_practice_modes <- BuildOrgPracticeModes(org_practice_cfg, "nevertreated", outdir_base, 
                                                build_dir = FALSE)
    org_practice_modes <- org_practice_modes[
      unlist(lapply(org_practice_modes, function(x) x$continuous_covariate != "prop_tests_passed"))]
    outcome_modes <- BuildOutcomeModes(outcome_cfg, "nevertreated", outdir_base, c(TRUE),
                                       build_dir = FALSE)
    
    for (method in c("lm_forest", "multi_arm")) {
      outdir_for_spec <- outdir_base
      dir_create(outdir_for_spec)
      
      for (outcome_mode in list(outcome_modes[[2]])) {
        RunForestEventStudy_CrossFit(
          outcome_mode$outcome,
          df_panel_common,
          org_practice_modes,
          outdir_for_spec,
          outdir_ds,
          method = method,
          rolling_period = rolling_period,
          n_folds = 10,
          seed = SEED,
          use_existing = TRUE
        )
      }
    }
  }
}
