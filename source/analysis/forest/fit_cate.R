library(tidyverse)
library(grf)
library(arrow)
library(yaml)
library(fs)
library(zeallot)
library(rlang)

source("source/lib/helpers.R")
source("source/analysis/forest/helpers.R")
source("source/analysis/constants.R")
`%ni%` <- Negate(`%in%`)

INDIR       <- "drive/output/derived/org_outcomes_practices/org_panel"
OUTDIR      <- "output/analysis/causal_forest_personalization"
OUTDIR_DATASTORE <- "drive/output/analysis/causal_forest_personalization"
INDIR_YAML  <- "source/analysis/config"

# ensure output directory exists and is writable
dir_create(OUTDIR)


FitPropensityForests <- function(x_train, df_train, never_treated_value = 0) {
  cohorts_all <- sort(unique(df_train$treatment_group))
  treated_cohorts <- setdiff(cohorts_all, never_treated_value)
  forests_by_cohort <- list()
  preds_by_cohort <- list()
  
  for (c_val in treated_cohorts) {
    train_mask <- df_train$quasi_treatment_group == c_val
    x_train_sub <- x_train[train_mask, , drop = FALSE]
    W_binary <- as.factor(ifelse(df_train$treatment_group[train_mask] == c_val, "1", "0"))
    
    forest_c <- probability_forest(x_train_sub, W_binary)
    tau_hat <- predict(forest_c, newdata = x_train)$predictions
    prob1 <- as.numeric(tau_hat[, "1"])
    
    forests_by_cohort[[as.character(c_val)]] <- forest_c
    preds_by_cohort[[as.character(c_val)]] <- prob1
  }
  
  list(forests_by_cohort = forests_by_cohort,
       preds_by_cohort = preds_by_cohort)
}

########## Forest fitting & prediction ##########

FitForestModel <- function(df_train, x_train, y_train, method, model_path,
                           use_existing, seed = SEED, use_default_propensity = FALSE,
                           use_cohort_propensity = FALSE) {
  set.seed(seed)
  treatment_file <- sub("\\.rds$", "_treatment.rds", model_path)
  outcome_file   <- sub("\\.rds$", "_outcome.rds", model_path)
  
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
  
  message("Model outfile: ", model_path)
  if (file.exists(model_path) && file.exists(treatment_file) && file.exists(outcome_file) && isTRUE(use_existing)) {
    message("Loading existing model from disk: ", model_path)
    loaded_model <- readRDS(model_path)
    loaded_treat <- readRDS(treatment_file)
    loaded_outcome <- readRDS(outcome_file)
    return(list(model = loaded_model, treatment_model = loaded_treat, outcome_model = loaded_outcome))
  }
  
  if (use_default_propensity) {
    W_hat <- NULL
    treatment_model <- NULL
  } else if (use_cohort_propensity) {
    est_res <- FitPropensityForests(x_train, df_train, never_treated_value = 0)
    preds_by_cohort <- est_res$preds_by_cohort
    treatment_model <- est_res$forests_by_cohort
    W_hat <- AdjustPropensityMatrixCohort(preds_by_cohort, W_mat, df_train, drop_never_treated)
  } else {
    W_og_form <- as.factor(df_train$treatment_group)
    treatment_model <- probability_forest(x_train, W_og_form, clusters = df_train$repo_id)
    cohort_propensities <- predict(treatment_model)$predictions
    W_hat <- AdjustPropensityMatrix(cohort_propensities, W_mat, df_train, drop_never_treated)
  }
  
  outcome_model <- regression_forest(x_train, y_train, clusters = df_train$repo_id, num.trees = 2000)
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
  
  dir_create(dirname(model_path))
  saveRDS(model, model_path)
  saveRDS(treatment_model, treatment_file)
  saveRDS(outcome_model, outcome_file)
  message("Saved model, treatment_model, and outcome_model to: ", dirname(model_path))
  
  list(model = model, treatment_model = treatment_model, outcome_model = outcome_model)
}

ComputeFoldDoublyRobust <- function(model_fold, outcome_model_fold, treatment_model_fold, df_hold, x_hold, y_hold, method, use_cohort_propensity = FALSE,
                                      drop_never_treated) { 
  y_hat <- predict(outcome_model_fold, x_hold)$predictions 
  W <- CalculateTreatment(df_hold, method)$W 
  
  # presumes its not nuclear or default What
  cohort_propensities <- predict(treatment_model_fold, x_hold)$predictions 
  W_hat <- AdjustPropensityMatrix(cohort_propensities, W, df_hold, drop_never_treated)
  
  preds <- predict(model_fold, newdata = x_hold)$predictions[, , "Y.1"]
  
  treated_rows <- which(rowSums(W) == 1) 
  cols <- max.col(W[treated_rows, , drop = FALSE]) 
  treated_rows_cols <- cbind(treated_rows, cols)
  
  y_hat_baseline <- y_hat - rowSums(W_hat * preds) 
  mu_hat <- y_hat_baseline 
  mu_hat[treated_rows] <- y_hat_baseline[treated_rows] + preds[treated_rows_cols]
  
  y_residual <- y_hold - mu_hat
  
  IPW <- matrix(0, dim(W)[1], dim(W)[2]) 
  prob_no_treat <- (1 - rowSums(W_hat)) 
  IPW[-treated_rows, ] <- -1 * 1 / prob_no_treat[-treated_rows] 
  IPW[treated_rows_cols] <- 1 / W_hat[treated_rows_cols]
  
  preds + IPW * as.vector(y_residual) 
}


FitAndPredictBaselineFold <- function(df_data, feature_cols, fold_id, time_levels, marg_dist_treatment_group, outdir_ds, 
                                      method, outcome, num_trees = 2000) {
  df_fold <- df_data %>% filter(fold != fold_id)
  df_hold_out <- df_data %>% filter(fold == fold_id)
  x_hold_out <- df_hold_out %>% select(all_of(feature_cols))
  
  df_panel_nt <- df_fold %>% filter(treatment_arm == "never-treated")
  if (nrow(df_panel_nt) == 0) return(list(hold_idx = integer(0), time_vals = numeric(0), quasi_vals = numeric(0)))

  include_quasi_treatment_group <- as.numeric(names(table(df_fold$quasi_treatment_group))) -  1
  df_panel_nt_in_quasi <- df_panel_nt %>% filter(time_index %in% include_quasi_treatment_group)
  
  # time_index in include_quasi_treatment_group 
  y_nt <- df_panel_nt_in_quasi$fd_outcome
  x_nt <- df_panel_nt_in_quasi %>% select(all_of(feature_cols))
  W_nt <- MakeTimeIndicatorMatrix(df_panel_nt_in_quasi)
  W_hat <- ComputeBaselinePropensities(W_nt, df_panel_nt_in_quasi, marg_dist_treatment_group)
  model <- lm_forest(x_nt, y_nt, W_nt, num.trees = num_trees, W.hat = W_hat, clusters = df_panel_nt_in_quasi$repo_id)

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
  
  # time_index not in include_quasi_treatment_group 
  non_quasi_time_index <- df_panel_nt %>% filter(time_index %ni% include_quasi_treatment_group) %>% pull(time_index) %>% unique()
  for (time_index in sort(non_quasi_time_index)) {
    df_panel_nt_time_index <- df_panel_nt %>% filter(time_index == !!time_index)
    y_nt_time <- df_panel_nt_time_index %>% pull(fd_outcome)
    x_nt_time <- df_panel_nt_time_index %>% select(all_of(feature_cols))
    model <- regression_forest(x_nt_time, y_nt_time,num.trees = num_trees,clusters = df_panel_nt_time_index$repo_id)
    saveRDS(model, file.path(dir_baseline, paste0(method, "_", outcome, "_fold", fold_id, "_time_index", time_index, ".rds")))
    pred_obj <- predict(model, x_hold_out)
    pred_mat <- cbind(pred_mat, pred_obj$predictions)
    colnames(pred_mat)[ncol(pred_mat)] <- paste0("factor.time_index.", time_index)
  }
  
  time_col_names_hold <- paste0("factor.time_index.", df_hold_out$time_index)
  quasi_col_names_hold <- paste0("factor.time_index.", df_hold_out$quasi_treatment_group - 1)
  time_idx_hold <- match(time_col_names_hold, colnames(pred_mat))
  quasi_idx_hold <- match(quasi_col_names_hold, colnames(pred_mat))
  
  pred_time_vals <- vapply(seq_len(nrow(pred_mat)), function(i) pred_mat[i, time_idx_hold[i]], numeric(1))
  pred_quasi_vals <- vapply(seq_len(nrow(pred_mat)), function(i) pred_mat[i, quasi_idx_hold[i]], numeric(1))
  
  list(hold_idx = hold_idx, time_vals = pred_time_vals, quasi_vals = pred_quasi_vals)
}

RunBaselineHeterogeneityCrossFit <- function(df_data, feature_cols, marg_dist_treatment_group, outdir_ds, method, outcome,
                                             n_folds = 10, num_trees = 2000) {
  time_levels <- sort(unique(df_data$time_index))
  n_obs <- nrow(df_data)
  time_baseline_all <- numeric(n_obs)
  norm_period_baseline_all <- numeric(n_obs)
  
  for (fold_id in seq_len(n_folds)) {
    res <- FitAndPredictBaselineFold(df_data, feature_cols, fold_id, time_levels, marg_dist_treatment_group, outdir_ds, 
                                     method, outcome, num_trees = num_trees)
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

RunForestCrossFit <- function(
    outcome, panel, org_practice_modes, outdir, outdir_ds, method, rolling_period, marg_dist_treatment_group,
    n_folds = 10, seed = SEED, use_existing = FALSE, use_default_propensity = FALSE, use_cohort_propensity = FALSE,
    use_imputed = FALSE, use_pc_features = FALSE) {
  message("Cross-fit (no plotting) for outcome=", outcome, " method=", method, " rolling=", rolling_period, " folds=", n_folds, " use_pc_features=", use_pc_features)
  covars     <- unlist(lapply(org_practice_modes, \(x) x$continuous_covariate))
  covars_imp <- unlist(lapply(org_practice_modes, function(x) {
    paste0(x$continuous_covariate, "_imp")
  }))
  
  df_data <- CreateDataPanel(panel, method, outcome, c(covars, covars_imp), rolling_period, n_folds, seed)

  # initial feature_cols based on use_imputed (but do not yet compute PCs)
  if (use_imputed) {
    feature_cols <- c(paste0(covars, "_mean"), paste0(covars_imp, "_mean"))
  } else {
    feature_cols <- paste0(covars, "_mean")
  }
  feature_cols <- intersect(feature_cols, colnames(df_data))
  x <- df_data %>% select(all_of(feature_cols))

  # apply the NA-threshold on columns and drop rows with missing values on feature_cols
  feature_cols <- feature_cols[colSums(is.na(x)) < 200]
  feature_cols <- setdiff(feature_cols, c("share_issue_only_mean", "share_pr_only_mean"))
  df_data <- df_data[complete.cases(df_data %>% select(all_of(feature_cols))),]
    
  # At this point df_data is filtered for observations/columns. If requested, compute PC1s
  # from these filtered rows and **replace** feature_cols with the PC1 columns.
  if (use_pc_features == TRUE | use_pc_features == "median") {
    # define pc groups (same as event-study)
    pc_groups <- PC_GROUPS
    pc_colnames <- c()
    # compute PCs using the already filtered df_data
    for (group_name in names(pc_groups)) {
      vars <- pc_groups[[group_name]]
      message("  Attempt PC group: ", group_name, " vars present: ", paste(vars, collapse = ", "))

      group_x <- df_data %>% select(all_of(vars))
      pc_res <- prcomp(group_x, center = TRUE, scale. = TRUE)
      pc1_scores_complete <- pc_res$x[, 1]
      if (group_name == "shared_knowledge") pc1_scores_complete <- -pc1_scores_complete
      pc_col <- paste0(group_name, "_pc1_mean")
      if (use_pc_features == "median") {
        med_score <- median(pc1_scores_complete)
        pc1_scores_complete <- ifelse(pc1_scores_complete>med_score, 1, 0)
      }
      df_data[[pc_col]] <- pc1_scores_complete
      pc_colnames <- c(pc_colnames, pc_col)
      # log loadings
      loadings_vec <- pc_res$rotation[vars, 1]
      loadings_str <- paste(paste0(vars, " ", sprintf("%+.3f", loadings_vec)), collapse = ", ")
      message("    PC1 (", group_name, ") var loadings: ", loadings_str)
    }
    # Replace feature_cols with pc_colnames if any were created
    feature_cols <- intersect(pc_colnames, colnames(df_data))
    message("  Using PC features as feature_cols: ", paste(feature_cols, collapse = ", "))
  }

  x <- df_data %>% select(all_of(feature_cols))
  df_repo_data <- df_data %>% select(repo_id, repo_name, quasi_treatment_group, treatment_group) %>% unique()
  
  if (method == "lm_forest_nonlinear") {
    df_data <- RunBaselineHeterogeneityCrossFit(
      df_data, feature_cols, marg_dist_treatment_group, outdir_ds, method, outcome,
      n_folds = 10, num_trees = 2000)
  }
  
  y <- df_data$resid_outcome
  max_time_val <- max(df_data$time_index, na.rm = TRUE)
  
  n_obs <- nrow(df_data)
  tau_hat <- NULL
  
  dr_scores <- NULL
  for (fold_id in seq_len(n_folds)) {
    message(" fold ", fold_id, " / ", n_folds)
    train_idx <- which(df_data$fold != fold_id)
    hold_idx  <- which(df_data$fold == fold_id)
    
    df_train <- df_data[train_idx, , drop = FALSE]
    df_train <- df_train %>% group_by(repo_id) %>%
      filter(!any(is.na(resid_outcome))) %>%
      ungroup()
    x_train <- as.matrix(df_train %>% select(all_of(feature_cols)))
    y_train <- df_train$resid_outcome
    
    df_hold  <- df_data[hold_idx, , drop = FALSE]
    x_hold <- as.matrix(df_hold %>% select(all_of(feature_cols)))
    y_hold <- df_hold$resid_outcome
    
    model_path <- file.path(
      outdir_ds, paste0(method, "_", outcome, "_rolling", rolling_period, "_fold", fold_id, ".rds"))
    forest_model_obj <- FitForestModel(
      df_train, x_train, y_train, method, model_path, use_existing = use_existing,
      seed = SEED, use_default_propensity = use_default_propensity, use_cohort_propensity = use_cohort_propensity)
    model_fold <- forest_model_obj$model
    treatment_model_fold <- forest_model_obj$treatment_model
    outcome_model_fold <- forest_model_obj$outcome_model
    
    pred_obj <- predict(model_fold, newdata = x_hold, drop = TRUE)
    preds <- pred_obj$predictions
    if (is.null(tau_hat)) {
      tau_hat <- matrix(NA_real_, nrow = n_obs, ncol = ncol(preds))
      colnames(tau_hat) <- colnames(preds)
    }
    
    tau_hat[hold_idx, ] <- preds
    
    dr_hold <- ComputeFoldDoublyRobust(model_fold, outcome_model_fold, treatment_model_fold, df_hold, x_hold, y_hold, method, use_cohort_propensity = use_cohort_propensity,
                                         drop_never_treated = TRUE)
    if (is.null(dr_scores)) {
      dr_scores <- matrix(NA_real_, nrow = n_obs, ncol = ncol(dr_hold))
      colnames(dr_scores) <- colnames(dr_hold)
    }
    dr_scores[hold_idx, ] <- dr_hold
  }
  
  df_data_all <- df_data %>% group_by(repo_id) %>%
    filter(!any(is.na(resid_outcome))) %>%
    ungroup()
  x_all <- df_data_all %>% select(all_of(feature_cols))
  y_all <- df_data_all$resid_outcome
  outfile_model <- file.path(
    outdir_ds, paste0(method, "_", outcome, "_rolling", rolling_period, ".rds"))
  forest_model_obj <- FitForestModel(
    df_data_all, x_all, y_all, method, outfile_model, use_existing = use_existing,
    seed = SEED, use_default_propensity = use_default_propensity, use_cohort_propensity = use_cohort_propensity)
  
  
  outfile_dr <- file.path(outdir_ds, paste0("dr_scores_", method, "_", outcome, "_rolling", rolling_period, ".csv"))
  dr_scores_repo <- AggregateDoublyRobustByRepo(dr_scores, df_data)
  write_csv(as.data.frame(dr_scores_repo), outfile_dr)
  
  dr_scores_repo_filt <- FilterDoublyRobustPredictions(dr_scores_repo, df_data, df_repo_data)
  dr_scores_repo_filt <- data.frame(dr_scores_repo_filt)
  dr_scores_repo_filt$repo_id <- as.numeric(rownames(dr_scores_repo_filt))
  
  if (is.null(tau_hat) || any(is.na(tau_hat))) {
    message("Warning: some observations have missing predicted CATEs (tau_hat contains NA). These will propagate to repo-level averages.")
  }
  tau_hat_filt <- FilterPredictions(tau_hat, df_data)
  
  cohort_time_repo_long <- TransformRepoCohortTimeCoefficients(tau_hat, df_data, outcome, "coef") %>%
    filter(!is.na(coef))
  cohort_time_repo_long_filt <- TransformRepoCohortTimeCoefficients(tau_hat_filt, df_data, outcome, "coef")%>%
    filter(!is.na(coef))
  cohort_time_repo_long <- cohort_time_repo_long %>% 
    left_join(cohort_time_repo_long_filt %>% mutate(present=1))
  
  dir_create(outdir)
  cohort_outfile <- file.path(outdir, paste0(outcome, "_cohort_time_repo_coeffs_", method, ".parquet"))
  write_parquet(cohort_time_repo_long, cohort_outfile)
  
  event_time_repo <- rbind(
    AggregateRepoEventStudy(tau_hat, df_data, max_time_val) %>% mutate(type = "all"),
    AggregateRepoEventStudy(tau_hat_filt, df_data, max_time_val) %>% mutate(type = "observed")
  )
  
  att_repo <- rbind(
    AggregateRepoATT(tau_hat, df_data, max_time_val) %>% mutate(type = "all"),
    AggregateRepoATT(tau_hat_filt, df_data, max_time_val) %>% mutate(type = "observed")
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
  
  df_full_robust_covar <- repo_level %>% left_join(dr_scores_repo_filt) %>% left_join(df_data %>% select(feature_cols, repo_name) %>% unique())
  
  out_file <- file.path(outdir, paste0(outcome, "_repo_att_", method, ".parquet"))
  write_parquet(df_full_robust_covar, out_file)
  
  invisible(list(cohort_time = cohort_time_repo_long, repo_level = repo_level))
}


#######################################
# 5. Main Entry Point (driver)
#######################################
outcome_cfg <- yaml.load_file(file.path(INDIR_YAML, "outcomes.yaml"))
org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariates.yaml"))

# loop variants
for (variant in c("important_degree_top3")) {
  for (use_imputed in c(FALSE)) {
    for (use_pc_features in list("median", TRUE, FALSE)) {
      for (rolling_panel in c("rolling5")) {
        panel_variant <- NormalizeDatasetName(variant)
        
        rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
        use_imputed_suffix <- ifelse(use_imputed, "_imp", "")
        use_pc_features_suffix  <- ifelse(use_pc_features == TRUE, "_pc", ifelse(use_pc_features == "median", "_pc_median", ""))
        rolling_panel_imp <- paste0(rolling_panel, use_imputed_suffix, use_pc_features_suffix)
        outdir_ds <- file.path(OUTDIR_DATASTORE, variant, rolling_panel_imp)
        df_panel <- read_parquet(file.path(INDIR, panel_variant, paste0("panel_", rolling_panel, ".parquet")))
        
        marg_dist_treatment_group <- ComputeCohortTimeDist(df_panel)
        all_outcomes <- unlist(lapply(outcome_cfg, function(x) x$main))
        
        panel <- BuildCommonSample(df_panel, all_outcomes) %>%
          filter(num_departures <= 1) %>%
          mutate(quasi_event_time = time_index - quasi_treatment_group)
        if (grepl("_exact1", variant)) {
          panel <- KeepSustainedImportant(panel, lb = 1, ub = 1)
        } else if (grepl("_oneQual", variant)) {
          panel <- KeepSustainedImportant(panel)
        } 
        
        use_default_propensity <- grepl("_defaultWhat", variant)
        use_cohort_propensity <- grepl("_nuclearWhat", variant)
        outdir_base <- file.path(OUTDIR, variant, rolling_panel_imp)
        org_practice_modes <- BuildOrgPracticeModes(org_practice_cfg, "nevertreated", outdir_base, 
                                                    build_dir = FALSE)
        org_practice_modes <- org_practice_modes[
          unlist(lapply(org_practice_modes, function(x) x$continuous_covariate != "prop_tests_passed"))]
        outcome_modes <- BuildOutcomeModes(outcome_cfg, "nevertreated", outdir_base, c(TRUE),
                                           build_dir = FALSE)
        
        for (method in c("lm_forest_nonlinear")) { 
          outdir_for_spec <- outdir_base
          dir_create(outdir_for_spec)
          
          for (outcome_mode in outcome_modes[2]) {
            RunForestCrossFit(
              outcome_mode$outcome,
              panel,
              org_practice_modes,
              outdir_for_spec,
              outdir_ds,
              method = method,
              rolling_period = rolling_period,
              marg_dist_treatment_group = marg_dist_treatment_group, 
              n_folds = 10,
              seed = SEED,
              use_existing = TRUE,
              use_default_propensity = use_default_propensity,
              use_cohort_propensity = use_cohort_propensity,
              use_imputed = use_imputed,
              use_pc_features = use_pc_features
            )
          }
        }
      }
    }
  }
}