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
# Helpers - Organized
#######################################


########## W / design matrix helpers ##########

BuildTimeInvariantIndicator <- function(df_panel_nt) {
  W_og <- model.matrix(~ factor(time_index) - 1, data = df_panel_nt)
  W_nt <- matrix(0, nrow = nrow(W_og), ncol = ncol(W_og), dimnames = list(NULL, colnames(W_og)))
  time_idx <- match(paste0("factor(time_index)", df_panel_nt$time_index), colnames(W_nt))
  norm_period_idx <- match(paste0("factor(time_index)", df_panel_nt$quasi_treatment_group - 1), colnames(W_nt))
  W_nt[cbind(seq_len(nrow(W_nt)), time_idx)] <- 1
  W_nt[cbind(seq_len(nrow(W_nt)), norm_period_idx)] <- -1
  W_nt
}

BuildWHatForBaselineHeterogeneity <- function(W_nt, df_panel_nt, marg_dist_treatment_group) {
  W_hat <- matrix(0, nrow = nrow(W_nt), ncol = ncol(W_nt), dimnames = list(NULL, colnames(W_nt)))
  repo_times <- tapply(df_panel_nt$time_index, df_panel_nt$repo_id, unique)
  for (i in seq_len(nrow(df_panel_nt))) {
    times_i <- repo_times[[as.character(df_panel_nt$repo_id[i])]]
    W_hat[i, paste0("factor(time_index)", times_i)] <- 1 / length(times_i)
  }
  first_time_by_repo <- df_panel_nt %>%
    group_by(repo_id) %>%
    summarize(first_time = min(time_index), .groups = "drop")
  repo_probs <- first_time_by_repo %>%
    left_join(marg_dist_treatment_group, by = "first_time") %>%
    select(repo_id, first_time, quasi_treatment_group, prob)
  
  repos <- unique(repo_probs$repo_id)
  for (r in repos) {
    rows_idx <- which(as.character(df_panel_nt$repo_id) == as.character(r))
    rp <- repo_probs[repo_probs$repo_id == r, , drop = FALSE]
    if (nrow(rp) == 0 || length(rows_idx) == 0) next
    for (k in seq_len(nrow(rp))) {
      col_name <- paste0("factor(time_index)", rp$quasi_treatment_group[k] - 1)
      W_hat[rows_idx, col_name] <- W_hat[rows_idx, col_name] - rp$prob[k]
    }
  }
  W_hat
}

########## Treatment estimation & transformations ##########

CalculateTreatment <- function(df_data, method) {
  if (method == "lm_forest" | method == "lm_forest_nonlinear") {
    W <- model.matrix(~ treatment_arm - 1, data = df_data)
    W <- W[, setdiff(colnames(W), colnames(W)[grepl("never-treated", colnames(W))])]
    return(list(W = W))
  } else {
    stop("Unknown method: ", method)
  }
}

EstimateForestsByCohort <- function(x_train, df_train, never_treated_value = 0) {
  cohorts_all <- sort(unique(df_train$treatment_group))
  treated_cohorts <- setdiff(cohorts_all, never_treated_value)
  forests_by_cohort <- list()
  preds_by_cohort <- list()
  
  for (c_val in treated_cohorts) {
    train_mask <- df_train$quasi_treatment_group == c_val
    x_train_sub <- x_train[train_mask, , drop = FALSE]
    W_binary <- as.factor(ifelse(df_train$treatment_group[train_mask] == c_val, "1", "0"))
    
    forest_c <- probability_forest(x_train_sub, W_binary)
    preds_all <- predict(forest_c, newdata = x_train)$predictions
    prob1 <- as.numeric(preds_all[, "1"])
    
    forests_by_cohort[[as.character(c_val)]] <- forest_c
    preds_by_cohort[[as.character(c_val)]] <- prob1
  }
  
  list(forests_by_cohort = forests_by_cohort,
       preds_by_cohort = preds_by_cohort)
}

TransformWHat <- function(W_og_form_hat, W, df_data, drop_never_treated = TRUE) {
  if (drop_never_treated) {
    W_og_form_hat <- W_og_form_hat[, setdiff(colnames(W_og_form_hat), "0"), drop = FALSE]
  }
  cols <- colnames(W)
  cohort <- as.integer(str_extract(cols, "(?<=cohort)\\d+"))
  event_time <- as.integer(str_replace(str_extract(cols, "(?<=event_time)[.-]?\\d+"), "[.-]", "-"))
  if (!drop_never_treated) {
    cohort[is.na(cohort)] <- 0
    event_time[is.na(event_time)] <- 0
  }
  expected_time <- cohort + event_time
  W_hat <- matrix(0, nrow = nrow(W), ncol = ncol(W), dimnames = list(NULL, cols))
  for (c in unique(cohort)) {
    if (!is.na(c) && as.character(c) %in% colnames(W_og_form_hat)) {
      W_hat[, cohort == c] <- W_og_form_hat[, as.character(c)]
    }
  }
  
  keep_mask <- outer(df_data$time_index, expected_time, `==`)
  if (!drop_never_treated) {
    keep_mask[, 1] <- TRUE
  }
  W_hat <- W_hat * keep_mask
  W_hat
}

TransformWHatNuclear <- function(preds_by_cohort, W, df_data, drop_never_treated = TRUE) {
  cols <- colnames(W)
  cohort_vals <- as.integer(str_extract(cols, "(?<=cohort)\\d+"))
  event_time <- as.integer(str_replace(str_extract(cols, "(?<=event_time)[.-]?\\d+"), "[.-]", "-"))
  if (!drop_never_treated) {
    cohort_vals[is.na(cohort_vals)] <- 0
    event_time[is.na(event_time)] <- 0
  }
  expected_time <- cohort_vals + event_time
  W_hat <- matrix(0, nrow = nrow(W), ncol = ncol(W), dimnames = list(NULL, cols))
  
  for (j in seq_along(cols)) {
    c_val <- cohort_vals[j]
    preds_for_cohort <- preds_by_cohort[[as.character(c_val)]]
    use_mask <- (df_data$quasi_treatment_group == c_val)
    W_hat[use_mask, j] <- preds_for_cohort[use_mask]
  }
  
  keep_mask <- outer(df_data$time_index, expected_time, `==`)
  if (!drop_never_treated) keep_mask[, 1] <- TRUE
  W_hat * keep_mask
}

########## Forest fitting & prediction ##########

FitForestModel <- function(df_train, x_train, y_train, method, outfile_fold,
                           use_existing, seed = SEED, use_default_What = FALSE,
                           use_nuclear_What = FALSE) {
  set.seed(seed)
  treatment_file <- sub("\\.rds$", "_treatment.rds", outfile_fold)
  
  drop_never_treated <- TRUE
  if (method == "lm_forest" | method == "lm_forest_nonlinear") {
    W <- CalculateTreatment(df_train, method)$W
    W_mat <- W
  } else if (method == "multi_arm") {
    W <- df_train$treatment_arm
    W_mat <- model.matrix(~ treatment_arm - 1, data = df_train)
    drop_never_treated <- FALSE
  } else {
    stop("Unknown method: ", method)
  }
  
  message("Model outfile: ", outfile_fold)
  if (file.exists(outfile_fold) && file.exists(treatment_file) && isTRUE(use_existing)) {
    message("Loading existing model from disk: ", outfile_fold)
    loaded_model <- readRDS(outfile_fold)
    loaded_treat   <- readRDS(treatment_file)
    return(list(model = loaded_model, treatment_model = loaded_treat))
  }
  
  if (use_default_What) {
    W_hat <- NULL
    treatment_model <- NULL
  } else if (use_nuclear_What) {
    est_res <- EstimateForestsByCohort(x_train, df_train, never_treated_value = 0)
    preds_by_cohort <- est_res$preds_by_cohort
    treatment_model <- est_res$forests_by_cohort
    W_hat <- TransformWHatNuclear(preds_by_cohort, W_mat, df_train, drop_never_treated)
  } else {
    W_og_form <- as.factor(df_train$treatment_group)
    treatment_model <- probability_forest(x_train, W_og_form, clusters = df_train$repo_id)
    W_og_form_hat <- predict(treatment_model)$predictions
    W_hat <- TransformWHat(W_og_form_hat, W_mat, df_train, drop_never_treated)
  }
  
  if (method == "lm_forest" | method == "lm_forest_nonlinear") {
    model <- lm_forest(X = x_train, Y = y_train, W = W, clusters = df_train$repo_id, num.trees = 2000,
                       W.hat = W_hat)
  } else if (method == "multi_arm") {
    if (!is.null(W_hat)) colnames(W_hat) <- gsub("treatment_arm", "", colnames(W_hat))
    model <- multi_arm_causal_forest(X = x_train, Y = y_train, W = W, clusters = df_train$repo_id, num.trees = 2000,
                                     W.hat = W_hat)
  } else {
    stop("Unknown method (should not reach here): ", method)
  }
  
  dir_create(dirname(outfile_fold))
  saveRDS(model, outfile_fold)
  saveRDS(treatment_model, treatment_file)
  message("Saved model and treatment_model to: ", dirname(outfile_fold))
  
  list(model = model, treatment_model = treatment_model)
}

FitAndPredictBaselineFold <- function(df_data, keep_names, fold_id, time_levels, marg_dist_treatment_group, outdir_ds, method, outcome, num_trees = 200) {
  df_fold <- df_data %>% filter(fold != fold_id)
  df_hold_out <- df_data %>% filter(fold == fold_id)
  x_hold_out <- df_hold_out %>% select(all_of(keep_names))
  
  df_panel_nt <- df_fold %>% filter(treatment_arm == "never-treated")
  if (nrow(df_panel_nt) == 0) return(list(hold_idx = integer(0), time_vals = numeric(0), quasi_vals = numeric(0)))
  
  y_nt <- df_panel_nt$fd_outcome
  x_nt <- df_panel_nt %>% select(all_of(keep_names))
  W_nt <- BuildTimeInvariantIndicator(df_panel_nt)
  W_hat <- BuildWHatForBaselineHeterogeneity(W_nt, df_panel_nt, marg_dist_treatment_group)
  
  model <- lm_forest(x_nt, y_nt, W_nt, num.trees = num_trees, W.hat = W_hat, clusters = df_panel_nt$repo_id)
  
  dir_baseline <- file.path(outdir_ds, "baseline_heterogeneity")
  dir_create(dir_baseline)
  saveRDS(model, file.path(dir_baseline, paste0(method, "_", outcome, "_fold", fold_id, ".rds")))
  
  hold_idx <- which(df_data$fold == fold_id)
  pred_obj <- predict(model, x_hold_out)
  pred_raw <- if (is.list(pred_obj) && !is.null(pred_obj$predictions)) pred_obj$predictions else as.matrix(pred_obj)
  if (length(dim(pred_raw)) == 3) {
    pred_mat <- as.matrix(pred_raw[,, "Y.1"])
  } else {
    pred_mat <- as.matrix(pred_raw)
  }
  
  time_col_names_hold <- paste0("factor.time_index.", df_hold_out$time_index)
  quasi_col_names_hold <- paste0("factor.time_index.", df_hold_out$quasi_treatment_group - 1)
  time_idx_hold <- match(time_col_names_hold, colnames(pred_mat))
  quasi_idx_hold <- match(quasi_col_names_hold, colnames(pred_mat))
  
  pred_time_vals <- vapply(seq_len(nrow(pred_mat)), function(i) pred_mat[i, time_idx_hold[i]], numeric(1))
  pred_quasi_vals <- vapply(seq_len(nrow(pred_mat)), function(i) pred_mat[i, quasi_idx_hold[i]], numeric(1))
  
  list(hold_idx = hold_idx, time_vals = pred_time_vals, quasi_vals = pred_quasi_vals)
}

RunBaselineHeterogeneityCrossFit <- function(df_data, keep_names, marg_dist_treatment_group, outdir_ds, method, outcome, n_folds = 10, num_trees = 200) {
  time_levels <- sort(unique(df_data$time_index))
  n_obs <- nrow(df_data)
  time_baseline_all <- numeric(n_obs)
  norm_period_baseline_all <- numeric(n_obs)
  
  for (fold_id in seq_len(n_folds)) {
    res <- FitAndPredictBaselineFold(df_data, keep_names, fold_id, time_levels, marg_dist_treatment_group, outdir_ds, method, outcome, num_trees = num_trees)
    if (length(res$hold_idx)) {
      time_baseline_all[res$hold_idx] <- res$time_vals
      norm_period_baseline_all[res$hold_idx] <- res$quasi_vals
    }
  }
  
  df_data <- df_data %>%
    mutate(time_baseline_heterogeneity = time_baseline_all,
           norm_period_baseline_heterogeneity = norm_period_baseline_all,
           resid_outcome = fd_outcome - time_baseline_heterogeneity + norm_period_baseline_heterogeneity)
  
  df_data
}

########## Aggregation & post-processing ##########
TransformRepoCohortTimeCoefficients <- function(preds_all, df_data, outcome, values_to_col) {
  cohort_time_repo_long <- tibble::as_tibble(preds_all) %>%
    bind_cols(df_data %>% select(repo_name, repo_id)) %>%
    tidyr::pivot_longer(-c(repo_name, repo_id), names_to = "arm", values_to = values_to_col) %>%
    distinct() %>%
    mutate(
      arm = sub("^treatment_arm", "", arm),
      cohort = as.integer(stringr::str_extract(arm, "(?<=cohort)\\d+")),
      event_time_sep = case_when(
        grepl("event_time\\.", arm) ~ ".",
        grepl("event_time-", arm)  ~ "-",
        TRUE                       ~ ""
      ),
      event_time_num = as.integer(stringr::str_extract(arm, "\\d+$")),
      event_time = case_when(
        event_time_sep == "." ~ -event_time_num,
        event_time_sep == "-" ~ -event_time_num,
        TRUE                  ~ event_time_num
      ),
      outcome = outcome
    ) %>%
    rename(coef = !!rlang::sym(values_to_col)) %>%
    select(outcome, repo_name, repo_id, cohort, event_time, coef)
  
  return(cohort_time_repo_long)
}

FilterRepoCohortEventPairs <- function(cohort_time_repo_long, df_data) {
  valid_pairs <- df_data %>%
    transmute(repo_id, cohort = quasi_treatment_group, event_time = time_index - quasi_treatment_group) %>%
    distinct()
  
  cohort_time_repo_long %>%
    mutate(cohort = as.integer(cohort), event_time = as.integer(event_time)) %>%
    semi_join(valid_pairs, by = c("repo_id", "cohort", "event_time"))
}

########## Cross-fit utilities & misc ##########

QuasiTreatmentGroupFirstDateMarginalDistribution <- function(df_data) {
  df_data %>% group_by(repo_id) %>%
    mutate(first_time = min(time_index)) %>%
    ungroup() %>%
    group_by(first_time, quasi_treatment_group) %>%
    summarize(n = n_distinct(repo_id), .groups = "drop_last") %>%
    mutate(prob = n / sum(n)) %>%
    ungroup()
}

CalculateDoublyRobustScore <- function(model) {
  x <- model$X.orig
  y_hat <- model$Y.hat
  y <- model$Y.orig
  W <- model$W.orig
  W_hat <- model$W.hat
  
  preds <- predict(model, newdata = x)$predictions[, , "Y.1"]
  
  treated_rows <- which(rowSums(W) == 1)
  cols <- max.col(W[treated_rows, , drop = FALSE])
  treated_rows_cols <- cbind(treated_rows, cols)
  
  y_hat_baseline <- y_hat - rowSums(W_hat * preds)
  mu_hat <- y_hat_baseline
  mu_hat[treated_rows] <- y_hat_baseline[treated_rows] + preds[treated_rows_cols]
  
  y_residual <- y - mu_hat
  
  IPW <- matrix(0, dim(W)[1], dim(W)[2])
  prob_no_treat <- (1-rowSums(W_hat))
  IPW[-treated_rows,] <- -1 * 1/prob_no_treat[-treated_rows] 
  IPW[treated_rows_cols] <- 1/W_hat[treated_rows_cols]
  
  return(preds + IPW * as.vector(y_residual))
}


ModifyDoublyRobustTime <- function(dr_scores, df_data) {
  colnames(dr_scores) <- gsub("treatment_arm", "", colnames(dr_scores))
  arm_info <- data.frame(
    arm    = colnames(dr_scores),
    cohort = as.integer(sub("cohort(\\d+).*", "\\1", colnames(dr_scores))),
    e      = ifelse(grepl("event_time[-\\.]", colnames(dr_scores)),
                    -as.integer(sub(".*event_time[-\\.](\\d+)", "\\1", colnames(dr_scores))),
                    as.integer(sub(".*event_time(\\d+)", "\\1", colnames(dr_scores))))
  )
  arm_info$expected_time <- arm_info$cohort + arm_info$e
  match_mask <- outer(df_data$time_index, arm_info$expected_time, "==")
  dr_scores[!match_mask] <- NA
  
  repo <- df_data$repo_id
  groups <- split(seq_along(repo), repo)
  dr_scores_repo <- do.call(rbind, lapply(groups, function(ii) {
    apply(dr_scores[ii, , drop = FALSE], 2, function(x) {
      v <- x[!is.na(x)]
      if (length(v) > 0) v[1] else NA_real_
    })
  }))
  dr_scores_repo
}

CalculateDoublyRobust <- function(model, df_data) {
  dr_scores <- CalculateDoublyRobustScore(model)
  dr_scores_repo <- ModifyDoublyRobustTime(dr_scores, df_data)
  return(dr_scores_repo)
}

RunForestEventStudy_CrossFit <- function(
    outcome, df_panel_common, org_practice_modes, outdir, outdir_ds, method, rolling_period, 
    n_folds = 10, seed = SEED, use_existing = FALSE, use_default_What = FALSE, use_nuclear_What = FALSE) {
  message("Cross-fit (no plotting) for outcome=", outcome, " method=", method, " rolling=", rolling_period, " folds=", n_folds)
  covars     <- unlist(lapply(org_practice_modes, \(x) x$continuous_covariate))
  covars_imp <- unlist(lapply(org_practice_modes, function(x) {
    if (!str_detect(x$legend_title, "organizational_routines")) paste0(x$continuous_covariate, "_imp")
  }))
  
  df_data <- CreateDataPanel(df_panel_common, method, outcome, covars, rolling_period, n_folds, seed)
  df_repo_data <- df_data %>% select(repo_id, repo_name, quasi_treatment_group, treatment_group) %>% unique()
  marg_dist_treatment_group <- QuasiTreatmentGroupFirstDateMarginalDistribution(df_data)
  keep_names <- paste0(covars, "_mean")
  x <- df_data %>% select(all_of(keep_names))
  
  if (method == "lm_forest_nonlinear") {
      df_data <- RunBaselineHeterogeneityCrossFit(
        df_data, keep_names, marg_dist_treatment_group, outdir_ds, method, outcome,
        n_folds = 10, num_trees = 500)
  }
  
  y <- df_data$resid_outcome
  max_time_val <- max(df_data$time_index, na.rm = TRUE)
  
  n_obs <- nrow(df_data)
  preds_all <- NULL

  for (fold_id in seq_len(n_folds)) {
    message(" fold ", fold_id, " / ", n_folds)
    train_idx <- which(df_data$fold != fold_id)
    hold_idx  <- which(df_data$fold == fold_id)

    df_train <- df_data[train_idx, , drop = FALSE]
    df_train <- df_train %>% group_by(repo_id) %>%
      filter(!any(is.na(resid_outcome))) %>%
      ungroup()
    x_train <- as.matrix(df_train %>% select(all_of(keep_names)))
    y_train <- df_train$resid_outcome

    df_hold  <- df_data[hold_idx, , drop = FALSE]
    x_hold <- as.matrix(df_hold %>% select(all_of(keep_names)))
    y_hold <- df_hold$resid_outcome

    outfile_fold <- file.path(
      outdir_ds, paste0(method, "_", outcome, "_rolling", rolling_period, "_fold", fold_id, ".rds"))
    forest_model_obj <- FitForestModel(
      df_train, x_train, y_train, method, outfile_fold, use_existing = use_existing,
      seed = SEED, use_default_What = use_default_What)
    model_fold <- forest_model_obj$model
    treatment_model_fold <- forest_model_obj$treatment_model

    pred_obj <- predict(model_fold, newdata = x_hold, drop = TRUE)
    preds <- pred_obj$predictions
    if (is.null(preds_all)) {
      preds_all <- matrix(NA_real_, nrow = n_obs, ncol = ncol(preds))
      colnames(preds_all) <- colnames(preds)
    }

    preds_all[hold_idx, ] <- preds
  }
  outfile <- file.path(outdir_ds, paste0(method, "_", outcome, "_rolling", rolling_period, ".rds"))
  if (!(use_existing & file.exists(outfile))) {
    x_data <- as.matrix(df_data %>% select(all_of(keep_names)))
    y_data <- df_data$resid_outcome
    forest_model_obj <- FitForestModel(
      df_data, x_data, y_data, method, outfile, use_existing = use_existing, 
      seed = SEED, use_default_What = use_default_What)
  }
  outfile_dr <- file.path(outdir_ds, paste0("dr_scores_", method, "_", outcome, "_rolling", rolling_period, ".csv"))
  if (!(use_existing & file.exists(outfile_dr))) {
    model <- readRDS(outfile)
    dr_scores_repo <- CalculateDoublyRobust(model, df_data)
    write.csv(dr_scores_repo, outfile_dr, row.names = FALSE)
  } else {
    dr_scores_repo <- read.csv(outfile_dr)
  }
  if (is.null(preds_all) || any(is.na(preds_all))) {
    message("Warning: some observations have missing predicted CATEs (preds_all contains NA). These will propagate to repo-level averages.")
  }
  dr_scores_repo_filt <- FilterDoublyRobustPredictions(dr_scores_repo, df_data, df_repo_data)

  preds_all_filt <- FilterPredictions(preds_all, df_data)
  
  cohort_time_repo_long <- TransformRepoCohortTimeCoefficients(preds_all, df_data, outcome, "coef") %>%
    filter(!is.na(coef))
  cohort_time_repo_long_filt <- TransformRepoCohortTimeCoefficients(preds_all_filt, df_data, outcome, "coef")%>%
    filter(!is.na(coef))
  cohort_time_repo_long <- cohort_time_repo_long %>% 
    left_join(cohort_time_repo_long_filt %>% mutate(present=1))
  
  dir_create(outdir)
  cohort_outfile <- file.path(outdir, paste0(outcome, "_cohort_time_repo_coeffs_", method, ".parquet"))
  write_parquet(cohort_time_repo_long, cohort_outfile)
  
  event_time_repo <- rbind(
    AggregateRepoEventStudy(preds_all, df_data, max_time_val) %>% mutate(type = "all"),
    AggregateRepoEventStudy(preds_all_filt, df_data, max_time_val) %>% mutate(type = "observed")
  )
    
  att_repo <- rbind(
    AggregateRepoATT(preds_all, df_data, max_time_val) %>% mutate(type = "all"),
    AggregateRepoATT(preds_all_filt, df_data, max_time_val) %>% mutate(type = "observed")
  )
  
  dr_scores_repo_all <- rbind(
    AggregateRepoATT(dr_scores_repo, df_repo_data, max_time_val) %>% mutate(type = "all"),
    AggregateRepoATT(dr_scores_repo_filt, df_repo_data, max_time_val) %>% mutate(type = "observed")
  ) %>% rename(att_dr = att)
  
  repo_level <- att_repo %>%
    left_join(dr_scores_repo_all, by = c("repo_name", "repo_id", "type")) %>%
    left_join(event_time_repo, by = c("repo_name", "repo_id", "type")) %>%
    mutate(outcome = outcome) %>%
    select(outcome, everything())
  
  
  repo_level <- repo_level %>% group_by(type) %>%
    mutate(
      med_att = median(att, na.rm = TRUE),
      att_group = case_when(
        !is.na(att) & att >= med_att ~ "high",
        !is.na(att) & att <  med_att ~ "low",
        TRUE ~ NA_character_
      ),
      med_att_dr = median(att_dr, na.rm = TRUE),
      att_dr_group = case_when(
        !is.na(att_dr) & att_dr >= med_att_dr ~ "high",
        !is.na(att_dr) & att_dr <  med_att_dr ~ "low",
        TRUE ~ NA_character_
      ),
    ) %>%
    ungroup() %>%
    select(outcome, repo_name, repo_id, type, att, att_group, att_dr, att_dr_group, everything())
  out_file <- file.path(outdir, paste0(outcome, "_repo_att_", method, ".parquet"))
  write_parquet(repo_level, out_file)
  
  invisible(list(cohort_time = cohort_time_repo_long, repo_level = repo_level))
}

#######################################
# 5. Main Entry Point (driver)
#######################################
outcome_cfg <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))
org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariate_organization.yaml"))

for (variant in c( "important_topk", "important_topk_exact1","important_topk_oneQual",
                   "important_topk_defaultWhat", "important_topk_exact1_defaultWhat","important_topk_oneQual_defaultWhat",
                   "important_topk_nuclearWhat", "important_topk_exact1_nuclearWhat","important_topk_oneQual_nuclearWhat")) {
  for (rolling_panel in c("rolling5")) {
    panel_variant <- gsub("_exact1", "", variant)
    panel_variant <- gsub("_nuclearWhat", "", panel_variant)
    panel_variant <- gsub("_defaultWhat", "", panel_variant)
    panel_variant <- gsub("_oneQual", "", panel_variant)
    
    rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
    outdir_ds <- file.path(OUTDIR_DATASTORE, variant, rolling_panel)
    df_panel <- read_parquet(file.path(INDIR, panel_variant, paste0("panel_", rolling_panel, ".parquet")))
    all_outcomes <- unlist(lapply(outcome_cfg, function(x) x$main))

    df_panel_common <- BuildCommonSample(df_panel, all_outcomes) %>%
      filter(num_departures <= 1) %>%
      mutate(quasi_event_time = time_index - quasi_treatment_group)
    if (grepl("_exact1", variant)) {
      df_panel_common <- KeepSustainedImportant(df_panel_common, lb = 1, ub = 1)
    } else if (grepl("_oneQual", variant)) {
      df_panel_common <- KeepSustainedImportant(df_panel_common)
    } 

    use_default_What <- grepl("_defaultWhat", variant)
    use_nuclear_What <- grepl("_nuclearWhat", variant)
    outdir_base <- file.path(OUTDIR, variant, rolling_panel)
    org_practice_modes <- BuildOrgPracticeModes(org_practice_cfg, "nevertreated", outdir_base, 
                                                build_dir = FALSE)
    org_practice_modes <- org_practice_modes[
      unlist(lapply(org_practice_modes, function(x) x$continuous_covariate != "prop_tests_passed"))]
    outcome_modes <- BuildOutcomeModes(outcome_cfg, "nevertreated", outdir_base, c(TRUE),
                                       build_dir = FALSE)
    
    for (method in c("lm_forest")) { # , "lm_forest_nonlinear"
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
          use_existing = TRUE,
          use_default_What = use_default_What,
          use_nuclear_What = use_nuclear_What
        )
      }
    }
  }
}
