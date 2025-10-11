ResidualizeOutcome <- function(df, outcome) {
  df_norm <- NormalizeOutcome(df, outcome)
  norm_outcome_col <- paste(outcome, "norm", sep = "_")
  df_norm <- FirstDifferenceOutcome(df_norm, norm_outcome_col)
  
  avg_adj <- df_norm %>%
    filter(treatment_group == 0) %>%
    group_by(quasi_treatment_group, quasi_event_time) %>%
    summarise(avg_fd = mean(fd_outcome, na.rm = TRUE), .groups = "drop")
  
  df_norm %>%
    left_join(avg_adj, by = c("quasi_treatment_group", "quasi_event_time")) %>%
    mutate(resid_outcome = fd_outcome - avg_fd) %>%
    select(-baseline, -avg_fd, -fd_outcome)
}


ComputeCovariateMeans <- function(df, covars, rolling_period = 1) {
  if (rolling_period == 1) {
    means <- df %>%
      filter(quasi_event_time >= -5 & quasi_event_time < -1) %>%
      group_by(repo_name) %>%
      summarise(across(all_of(covars), ~ mean(.x, na.rm = TRUE), .names = "{.col}_mean"),
                .groups = "drop")
    
    df %>% filter(quasi_event_time >= -5 & quasi_event_time <= 5) %>%
      left_join(means, by = "repo_name")
    
  } else if (rolling_period == 5) {
    means <- df %>%
      filter(quasi_event_time == -1) %>%
      select(repo_name, all_of(covars)) %>%
      rename_with(~ paste0(.x, "_mean"), all_of(covars))
    
    df %>%
      filter(quasi_event_time >= -5 & quasi_event_time <= 5) %>%
      left_join(means, by = "repo_name")
  }
}

FirstDifferenceOutcome <- function(df, norm_outcome_col) {
  df <- df %>%
    group_by(repo_name) %>%
    mutate(baseline = ifelse(quasi_event_time == -1, !!sym(norm_outcome_col), NA)) %>%
    fill(baseline, .direction = "downup") %>%
    mutate(fd_outcome = !!sym(norm_outcome_col) - baseline) %>%
    ungroup() %>%
    filter(!is.na(fd_outcome))
  return(df)
}

GetWMatrix <- function(df, time_levels) {
  n <- nrow(df)
  col_names <- paste0("factor(time_index)", time_levels)
  W <- matrix(0, nrow = n, ncol = length(col_names), dimnames = list(NULL, col_names))
  
  time_col_names <- paste0("factor(time_index)", df$time_index)
  time_idx <- match(time_col_names, colnames(W))
  W[cbind(seq_len(n), time_idx)] <- 1
  
  # quasi_treatment_group contribution uses (quasi_treatment_group - 1) as in your original code
  norm_col_names <- paste0("factor(time_index)", df$quasi_treatment_group - 1)
  # only set when that column exists in W (safety)
  norm_idx <- match(norm_col_names, colnames(W))
  valid_rows <- which(!is.na(norm_idx))
  if (length(valid_rows) > 0) {
    W[cbind(valid_rows, norm_idx[valid_rows])] <- -1
  }
  
  return(W)
}

CalculateTreatment <- function(df_data, method) {
  if (method == "lm_forest" | method == "lm_forest_nonlinear") {
    W <- model.matrix(~ treatment_arm - 1, data = df_data)
    W <- W[, setdiff(colnames(W), colnames(W)[grepl("never-treated", colnames(W))])]
    return(list(W = W))
  } else {
    stop("Unknown method: ", method)
  }
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

AggregateEventStudy <- function(tau_hat, df, max_time, type = c("event_time", "att", "cumulative"), cumulative_cutoff = 4) {
  type <- match.arg(type)
  colnames(tau_hat) <- gsub("treatment_arm", "", colnames(tau_hat))
  colnames(tau_hat) <- gsub(" - never-treated", "", colnames(tau_hat))
  
  arm_info <- data.frame(
    arm    = colnames(tau_hat),
    cohort = as.integer(sub("cohort(\\d+).*", "\\1", colnames(tau_hat))),
    e      = ifelse(grepl("event_time[-\\.]", colnames(tau_hat)),
                    -as.integer(sub(".*event_time[-\\.](\\d+)", "\\1", colnames(tau_hat))),
                    as.integer(sub(".*event_time(\\d+)", "\\1", colnames(tau_hat))))
  )
  
  cohort_probs <- df %>%
    filter(quasi_treatment_group > 0) %>%
    count(quasi_treatment_group, name = "n") %>%
    mutate(p_g = n / sum(n))
  
  weighted_avg <- function(valid) {
    tau_sub <- tau_hat[, valid$arm, drop = FALSE]
    w_vec <- valid$p / sum(valid$p, na.rm = TRUE)
    w_mat <- matrix(rep(w_vec, each = nrow(tau_sub)), nrow = nrow(tau_sub), ncol = length(w_vec))
    w_mat[is.na(tau_sub)] <- 0
    w_mat <- w_mat / rowSums(w_mat, na.rm = TRUE)
    num <- rowSums(tau_sub * w_mat, na.rm = TRUE)
    num[apply(tau_sub, 1, function(x) all(is.na(x)))] <- NA  # ensure all-NA rows stay NA
    num
  }
  
  if (type == "event_time") {
    est <- lapply(sort(unique(arm_info$e)), function(ev) {
      valid <- subset(arm_info, e == ev & cohort + ev <= max_time)
      if (!nrow(valid)) return(NULL)
      valid$p <- cohort_probs$p_g[match(valid$cohort, cohort_probs$quasi_treatment_group)]
      weighted_avg(valid)
    })
    out <- as.data.frame(do.call(cbind, est))
    colnames(out) <- paste0("event_time", sort(unique(arm_info$e)))
    cbind(obs_id = seq_len(nrow(out)), out)
    
  } else if (type == "att") {
    valid <- subset(arm_info, e >= 0 & cohort + e <= max_time)
    slots_per_cohort <- ave(valid$cohort, valid$cohort, FUN = length)
    valid$p <- cohort_probs$p_g[match(valid$cohort, cohort_probs$quasi_treatment_group)] / slots_per_cohort
    data.frame(att = weighted_avg(valid))
    
  } else if (type == "cumulative") {
    valid <- subset(arm_info, e >= 0 & cohort <= cumulative_cutoff & cohort + e <= cumulative_cutoff)
    if (!nrow(valid)) return(data.frame(cumu_t4 = NA))
    valid$p <- cohort_probs$p_g[match(valid$cohort, cohort_probs$quasi_treatment_group)]
    data.frame(cumu_t4 = weighted_avg(valid))
  }
}
