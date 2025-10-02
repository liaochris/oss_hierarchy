#######################################
# 1. Libraries & Sources
#######################################
library(tidyverse)
library(grf)
library(arrow)
library(yaml)
library(fs)

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

dir_create(OUTDIR)

#######################################
# 4. Core Analysis (cross-fit per outcome, export cohort-time & event-time)
#######################################
AssignOutcomeFolds <- function(repo_names, outcome, n_folds = 10, seed = SEED) {
  outcome_bytes <- as.integer(charToRaw(as.character(outcome)))
  offset <- (sum(outcome_bytes) + as.integer(seed)) %% as.integer(n_folds)
  repo_sorted <- sort(unique(repo_names))
  n_repos <- length(repo_sorted)
  df <- tibble(repo_name = repo_sorted, rank = seq_len(n_repos))
  df <- df %>% mutate(fold = ((rank - 1) + offset) %% n_folds + 1)
  df %>% select(repo_name, fold)
}

RunForestEventStudy_CrossFit <- function(outcome, df_panel_common, org_practice_modes,
                                         outdir, outdir_ds, method, rolling_period,
                                         top = "all", n_folds = 10, seed = SEED, use_existing = FALSE) {
  mode_suffix <- if (top == "all") "all" else paste0("top", top)
  message("Cross-fit (no plotting) for outcome=", outcome, " method=", method, " rolling=", rolling_period, " folds=", n_folds)
  
  if (method == "lm_forest") {
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
    mutate(treatment_arm = factor(ifelse(treatment_group == 0, "never-treated",
                                         paste0("cohort", treatment_group, "event_time", quasi_event_time))),
           treatment_arm = relevel(treatment_arm, ref = "never-treated"))
  
  # Covariate selection
  if (top == "all") {
    keep_names <- paste0(covars, "_mean")
  } else {
    Y_reg_forest <- df_data %>% filter(quasi_event_time >= 0) %>% pull(resid_outcome)
    X_reg_forest <- as.matrix(df_data %>% filter(quasi_event_time >= 0) %>% select(all_of(paste0(covars, "_mean"))))
    varimp.Y <- variable_importance(regression_forest(X_reg_forest, Y_reg_forest))
    ranked_vars <- colnames(X_reg_forest)[order(varimp.Y, decreasing = TRUE)]
    keep <- ranked_vars[!duplicated(sub("_imp_mean$", "_mean", ranked_vars))][1:top]
    keep_names <- keep
  }
  
  Y <- df_data$resid_outcome
  max_time_val <- max(df_data$time_index, na.rm = TRUE)
  
  # assign folds
  fold_map <- AssignOutcomeFolds(df_data$repo_name, outcome, n_folds = n_folds, seed = seed)
  df_data <- df_data %>% left_join(fold_map, by = "repo_name")
  
  # prepare container for cross-fitted predictions (one row per observation)
  n_obs <- nrow(df_data)
  preds_all <- NULL   # will become matrix n_obs x k_arms once we know k
  
  # loop folds and produce out-of-sample preds
  for (fold_id in seq_len(n_folds)) {
    message(" fold ", fold_id, " / ", n_folds)
    train_idx <- which(df_data$fold != fold_id)
    hold_idx  <- which(df_data$fold == fold_id)
    if (length(hold_idx) == 0) {
      message("  no held obs for fold ", fold_id, "; skipping")
      next
    }
    
    df_train <- df_data[train_idx, , drop = FALSE]
    df_hold  <- df_data[hold_idx, , drop = FALSE]
    
    X_train <- as.matrix(df_train %>% select(all_of(keep_names)))
    Y_train <- df_train$resid_outcome
    
    outfile_fold <- file.path(outdir_ds, paste0(method, "_", outcome, "_rolling", rolling_period, "_", mode_suffix, "_fold", fold_id, ".rds"))
    model_fold <- FitForestModel(df_train, X_train, Y_train, method, outfile_fold, use_existing = use_existing)
    
    X_hold <- as.matrix(df_hold %>% select(all_of(keep_names)))
    pred_obj <- tryCatch(predict(model_fold, newdata = X_hold, drop = TRUE),
                         error = function(e) stop("predict(..., newdata=...) failed: ", e$message))
    preds <- pred_obj$predictions
    # initialize preds_all if first time
    if (is.null(preds_all)) {
      preds_all <- matrix(NA_real_, nrow = n_obs, ncol = ncol(preds))
      colnames(preds_all) <- colnames(preds)
    }
    # store held predictions into full matrix
    preds_all[hold_idx, ] <- preds
  }
  
  # Check we filled preds_all for all rows (if some folds had no held rows, some rows may be NA)
  if (any(is.na(preds_all))) {
    message("Warning: some observations have missing predicted CATEs (preds_all contains NA). These will propagate to repo-level averages.")
  }
  
  # 1) repo Cohort-time coefficients 
  cohort_time_df <- as_tibble(preds_all)
  cohort_time_df <- bind_cols(df_data %>% select(repo_name, repo_id), cohort_time_df)
  cohort_time_repo_long <- as_tibble(preds_all) %>%
    bind_cols(df_data %>% select(repo_name, repo_id)) %>%
    pivot_longer(-c(repo_name, repo_id), names_to = "arm", values_to = "coef") %>%
    distinct() %>%   # ensure unique rows first
    mutate(
      arm = sub("^treatment_arm", "", arm),
      cohort = as.integer(sub("cohort(\\d+).*", "\\1", arm)),
      event_time = ifelse(
        grepl("event_time[-\\.]", arm),
        -as.integer(sub(".*event_time[-\\.](\\d+)", "\\1", arm)),
        as.integer(sub(".*event_time(\\d+)", "\\1", arm))
      ),
      outcome = outcome
    ) %>%
    select(outcome, repo_name, repo_id, cohort, event_time, coef)
  
  # write cohort-time per-repo CSV (separate file)
  dir_create(outdir)
  cohort_outfile <- file.path(outdir, paste0(outcome, "_cohort_time_repo_coeffs_", method, "_rolling", rolling_period, "_", mode_suffix, ".csv"))
  readr::write_csv(cohort_time_repo_long, cohort_outfile)
  message(" wrote cohort-time (repo-level) CSV: ", cohort_outfile)
  
  # 2) Event-time coefficients per observation (wide)
  event_time_repo <- AggregateEventStudy(preds_all, df_data, max_time_val, "event_time") %>%
    as_tibble() %>%
    select(-obs_id) %>%
    bind_cols(df_data %>% select(repo_name, repo_id)) %>%
    distinct()
  
  # 3) ATT per observation, then averaged to repo
  att_obs <- AggregateEventStudy(preds_all, df_data, max_time_val, "att") %>%
    rename(att = att)
  att_repo <- bind_cols(df_data %>% select(repo_name, repo_id), att_obs) %>%
    distinct()
  
  # combine att_repo (wide ATT) with event_time_repo (wide event-time coeffs)
  repo_level <- att_repo %>%
    left_join(event_time_repo, by = c("repo_name", "repo_id")) %>%
    mutate(outcome = outcome) %>%
    select(outcome, everything())
  
  # median split per outcome (att-based) and attach att_group
  med_att <- median(repo_level$att, na.rm = TRUE)
  repo_level <- repo_level %>%
    mutate(att_group = ifelse(!is.na(att) & att >= med_att, "high", "low")) %>%
    select(outcome, repo_name, repo_id, att, att_group, everything())
  
  # write repo-level CSV (append to per-spec file that contains all outcomes)
  out_csv_file <- file.path(outdir, paste0(outcome, "_repo_att_", method, "_rolling", rolling_period, "_", mode_suffix, ".csv"))
  readr::write_csv(repo_level, out_csv_file)
  message(" wrote/updated repo-level CSV (att + event-time): ", out_csv_file)
  
}

#######################################
# 5. Main Entry Point (driver)
#######################################
outcome_cfg <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))
org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariate_organization.yaml"))

for (variant in c("important_topk")) {
  for (rolling_panel in c("rolling5", "rolling1")) {
    rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
    
    for (top in c("all")) {
      OUTDIR_DS <- file.path(OUTDIR_DATASTORE, variant)
      
      df_panel <- read_parquet(file.path(INDIR, variant, paste0("panel_", rolling_panel, ".parquet")))
      all_outcomes <- unlist(lapply(outcome_cfg, function(x) x$main))
      df_panel_common <- BuildCommonSample(df_panel, all_outcomes) %>%
        filter(num_departures <= 1) %>%
        mutate(quasi_event_time = time_index - quasi_treatment_group)
      
      org_practice_modes <- BuildOrgPracticeModes(org_practice_cfg, "nevertreated",
                                                  file.path(OUTDIR, variant, paste0(rolling_panel, "_", if (top == "all") "all" else paste0("top", top))))
      outcome_modes <- BuildOutcomeModes(outcome_cfg, "nevertreated",
                                         file.path(OUTDIR, variant, paste0(rolling_panel, "_", if (top == "all") "all" else paste0("top", top))),
                                         c(TRUE))
      
      for (method in c("lm_forest", "lm_forest_nonlinear")) {
        outdir_for_spec <- file.path(OUTDIR, variant, paste0(rolling_panel, "_", if (top == "all") "all" else paste0("top", top)))
        dir_create(outdir_for_spec)
        out_csv_file_spec <- file.path(outdir_for_spec, paste0("repo_att_all_outcomes_", method, "_rolling", rolling_period, "_", if (top == "all") "all" else paste0("top", top), ".csv"))
        if (file.exists(out_csv_file_spec)) file.remove(out_csv_file_spec)
        # cohort-time file also cleared if exists
        cohort_outfile_spec <- file.path(outdir_for_spec, paste0("cohort_time_repo_coeffs_", method, "_rolling", rolling_period, "_", if (top == "all") "all" else paste0("top", top), ".csv"))
        if (file.exists(cohort_outfile_spec)) file.remove(cohort_outfile_spec)
        
        for (outcome_mode in list(outcome_modes[[2]])) {
          RunForestEventStudy_CrossFit(
            outcome_mode$outcome,
            df_panel_common,
            org_practice_modes,
            outdir_for_spec,
            OUTDIR_DS,
            method = method,
            rolling_period = rolling_period,
            top = top,
            n_folds = 10,
            seed = SEED,
            use_existing = TRUE
          )
        }
      }
    }
  }
}
