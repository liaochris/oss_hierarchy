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
  outcome_bytes <- as.integer(charToRaw(as.character(outcome)))
  offset <- (sum(outcome_bytes) + as.integer(seed)) %% as.integer(n_folds)
  n_repos <- length(repo_sorted)
  df <- tibble(repo_name = repo_sorted, rank = seq_len(n_repos))
  df <- df %>% mutate(fold = ((rank - 1) + offset) %% n_folds + 1)
  df %>% select(repo_name, fold)
}

# CalculateDoublyRobust <- function(df_hold, method, treatment_model_fold, outcome_model_fold, x_hold,
#                                   y_hold, preds) {
#   W <- CalculateTreatment(df_hold, method)$W
#   W_og_form_hat <- predict(treatment_model_fold, newdata = x_hold)$predictions
#   W_hat <- TransformWHat(W_og_form_hat, W, df_hold)
#   
#   y_hat <- predict(outcome_model_fold, newdata = x_hold)$predictions
#   Y_residual <- y_hold - (y_hat + preds * (W - W_hat))
#   debiasing_weights <- (W - W_hat) / (W_hat * (1 - W_hat))
#   
#   return(preds + debiasing_weights * Y_residual)
# }

FilterPredictions <- function(preds_all, df_data) {
  arm_names <- colnames(preds_all)
  quasi_arm <- paste0("treatment_armcohort", df_data$quasi_treatment_group, "event_time", 
                      df_data$time_index - df_data$quasi_treatment_group) |>
    sub("event_time-", "event_time.", x = _, fixed = TRUE)
  valid_arms <- tapply(quasi_arm, df_data$repo_id, unique)
  for (i in seq_len(nrow(preds_all))) {
    keep <- arm_names %in% valid_arms[[as.character(df_data$repo_id[i])]]
    preds_all[i, !keep] <- NA
  }
  preds_all
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

FilterRepoCohortEventPairs <- function(cohort_time_repo_long, df_data) {
  valid_pairs <- df_data %>%
    dplyr::transmute(repo_id, cohort = quasi_treatment_group, event_time = time_index - quasi_treatment_group) %>%
    dplyr::distinct()
  
  cohort_time_repo_long %>%
    dplyr::mutate(cohort = as.integer(cohort), event_time = as.integer(event_time)) %>%
    dplyr::semi_join(valid_pairs, by = c("repo_id", "cohort", "event_time"))
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

QuasiTreatmentGroupFirstDateMarginalDistribution <- function(df_data) {
  df_data %>% group_by(repo_id) %>%
    mutate(first_time = min(time_index)) %>%
    ungroup() %>%
    group_by(first_time, quasi_treatment_group) %>%
    summarize(n = n_distinct(repo_id), .groups = "drop_last") %>%
    mutate(prob = n / sum(n)) %>%
    ungroup() 
}

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
  # P(t = col time | X_i) condition on group, marginal distribution of number of time obs
  W_hat <- matrix(0, nrow = nrow(W_nt), ncol = ncol(W_nt), dimnames = list(NULL, colnames(W_nt)))
  repo_times <- tapply(df_panel_nt$time_index, df_panel_nt$repo_id, unique)
  for (i in seq_len(nrow(df_panel_nt))) {
    times_i <- repo_times[[as.character(df_panel_nt$repo_id[i])]]
    W_hat[i, paste0("factor(time_index)", times_i)] <- 1 / length(times_i)
  }
  first_time_by_repo <- df_panel_nt %>%
    group_by(repo_id) %>%
    summarize(first_time = min(time_index), .groups = "drop")
  # P(quasi_treamtent_group = col time + 1)
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
  # handle 3D arrays (common lm_forest shape): extract Y.1 slice if present
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

RunForestEventStudy_CrossFit <- function(
    outcome, df_panel_common, org_practice_modes, outdir, outdir_ds, method, rolling_period, 
    n_folds = 10, seed = SEED, use_existing = FALSE, use_default_What = FALSE, use_nuclear_What = FALSE) {
  message("Cross-fit (no plotting) for outcome=", outcome, " method=", method, " rolling=", rolling_period, " folds=", n_folds)
  covars     <- unlist(lapply(org_practice_modes, \(x) x$continuous_covariate))
  covars_imp <- unlist(lapply(org_practice_modes, function(x) {
    if (!str_detect(x$legend_title, "organizational_routines")) paste0(x$continuous_covariate, "_imp")
  }))
  if (method == "lm_forest" | method == "multi_arm") {
    df_panel_outcome <- ResidualizeOutcome(df_panel_common, outcome)
  } else if (method == "lm_forest_nonlinear") {
    df_panel_outcome <- NormalizeOutcome(df_panel_common, outcome)
    norm_outcome_col <- paste(outcome, "norm", sep = "_")
    df_panel_outcome <- FirstDifferenceOutcome(df_panel_outcome, norm_outcome_col)
  } else {
    stop("Unsupported method: ", method)
  }
  df_data <- ComputeCovariateMeans(df_panel_outcome, covars, rolling_period) %>%
    filter(quasi_event_time != -1) %>%
    mutate(treatment_arm = factor(ifelse(treatment_group == 0, "never-treated", paste0("cohort", treatment_group, "event_time", quasi_event_time))),
           treatment_arm = relevel(treatment_arm, ref = "never-treated"))
  fold_map <- AssignOutcomeFolds(df_data$repo_name, outcome, n_folds = n_folds, seed = seed)
  df_data <- df_data %>% left_join(fold_map, by = "repo_name")
  
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
  if (is.null(preds_all) || any(is.na(preds_all))) {
    message("Warning: some observations have missing predicted CATEs (preds_all contains NA). These will propagate to repo-level averages.")
  }

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
  
  repo_level <- att_repo %>%
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
      )
    ) %>%
    ungroup() %>%
    select(outcome, repo_name, repo_id, type, att, att_group, everything())
  out_file <- file.path(outdir, paste0(outcome, "_repo_att_", method, ".parquet"))
  write_parquet(repo_level, out_file)
  
  invisible(list(cohort_time = cohort_time_repo_long, repo_level = repo_level))
}

#######################################
# 5. Main Entry Point (driver)
#######################################
outcome_cfg <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))
org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariate_organization.yaml"))

for (variant in c("important_topk_defaultWhat", "important_topk_nuclearWhat", "important_topk", 
                  "important_topk_oneQual_defaultWhat", "important_topk_oneQual_nuclearWhat","important_topk_oneQual",
                  "important_topk_exact1_defaultWhat", "important_topk_exact1_nuclearWhat")) {
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
