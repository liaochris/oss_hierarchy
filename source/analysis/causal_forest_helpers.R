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
    
    df %>%
      filter(quasi_event_time >= -5 & quasi_event_time <= 5) %>%
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

CalculateTreatment <- function(df_data, method) {
  if (method == "lm_forest") {
    W <- model.matrix(~ treatment_arm - 1, data = df_data)
    W <- W[, setdiff(colnames(W), colnames(W)[grepl("never-treated", colnames(W))])]
    return(list(W = W))
  } else if (method == "lm_forest_nonlinear") {
    W_treat <- model.matrix(~ treatment_arm - 1, data = df_data)
    W_treat <- W_treat[, setdiff(colnames(W_treat), colnames(W_treat)[grepl("never-treated", colnames(W_treat))])]
    # time dummies
    W_time <- model.matrix(~ factor(time_index) - 1, data = df_data)
    
    # place -1 in the time-dummy column corresponding to baseline (cohort - 1)
    # cohort here is the row's treatment_group (quasi_treatment_group)
    baseline_time_index <- as.integer(df_data$quasi_treatment_group) - 1
    baseline_col_names <- paste0("factor(time_index)", baseline_time_index)
    col_idx_map <- match(baseline_col_names, colnames(W_time))   # NA if that time col doesn't exist
    rows_with_baseline <- which(!is.na(col_idx_map))
    if (length(rows_with_baseline) > 0) {
      row_idx <- rows_with_baseline
      col_idx <- col_idx_map[rows_with_baseline]
      W_time[cbind(row_idx, col_idx)] <- -1
    }
    W <- cbind(W_treat, W_time)
    grad_vec <- c(rep(1, ncol(W_treat)), rep(0, ncol(W_time)))
    return(list(W = W, grad_vec = grad_vec))
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

FitForestModel <- function(df_train, x_train, y_train, method, outfile_fold, use_existing, seed = SEED) {
  set.seed(seed)
  treatment_file <- sub("\\.rds$", "_treatment.rds", outfile_fold)
  outcome_file   <- sub("\\.rds$", "_outcome.rds", outfile_fold)
  
  drop_never_treated <- TRUE
  if (method == "lm_forest") {
    W <- CalculateTreatment(df_train, method)$W
    W_mat <- W
    grad_vec <- NULL
  } else if (method == "multi_arm") {
    W <- df_train$treatment_arm
    W_mat <- model.matrix(~ treatment_arm - 1, data = df_train)
    drop_never_treated <- FALSE
  } else if (method == "lm_forest_nonlinear") {
    calc_treatment <- CalculateTreatment(df_train, method)
    W <- calc_treatment$W %||% calc_treatment[[1]]
    grad_vec <- calc_treatment$grad_vec %||% calc_treatment[[2]] %||% NULL
  } else {
    stop("Unknown method: ", method)
  }
  
  message("Model outfile: ", outfile_fold)
  if (file.exists(outfile_fold) && file.exists(outcome_file) && file.exists(treatment_file) && isTRUE(use_existing)) {
    message("Loading existing model from disk: ", outfile_fold)
    loaded_model <- readRDS(outfile_fold)
    loaded_outcome <- readRDS(outcome_file)
    loaded_treat   <- readRDS(treatment_file)
    return(list(model = loaded_model, outcome_model = loaded_outcome, treatment_model = loaded_treat))
  }
  
  outcome_model <- regression_forest(x_train, y_train)
  y_hat <- predict(outcome_model, x_train)

  W_og_form <- as.factor(df_train$treatment_group)
  treatment_model <- probability_forest(x_train, W_og_form)
  W_og_form_hat <- predict(treatment_model)$predictions
  W_hat <- TransformWHat(W_og_form_hat, W_mat, df_train, drop_never_treated)
  
  # fit main causal model depending on method
  if (method == "lm_forest") {
    model <- lm_forest(X = x_train, Y = y_train, W = W, clusters = df_train$repo_id, num.trees = 2000,
                       Y.hat = y_hat, W.hat = W_hat)
  } else if (method == "multi_arm") {
    colnames(W_hat) <- gsub("treatment_arm", "", colnames(W_hat))
    model <- multi_arm_causal_forest(X = x_train, Y = y_train, W = W, clusters = df_train$repo_id, num.trees = 2000,
                                     Y.hat = y_hat, W.hat = W_hat)
  } else if (method == "lm_forest_nonlinear") {
    # pass gradient.weights if available
    if (is.null(grad_vec)) {
      warning("No gradient weights returned by CalculateTreatment; proceeding without gradient.weights")
      model <- lm_forest(X = x_train, Y = y_train, W = W, clusters = df_train$repo_id, num.trees = 2000)
    } else {
      model <- lm_forest(X = x_train, Y = y_train, W = W, clusters = df_train$repo_id, num.trees = 2000, gradient.weights = grad_vec)
    }
  } else {
    stop("Unknown method (should not reach here): ", method)
  }
  
  # persist safely
  dir_create(dirname(outfile_fold))
  tryCatch({
    saveRDS(model, outfile_fold)
    saveRDS(outcome_model, outcome_file)
    saveRDS(treatment_model, treatment_file)
    message("Saved model, outcome_model, and treatment_model to: ", dirname(outfile_fold))
  }, error = function(e) {
    warning("Failed to write model files: ", e$message)
  })
  
  return(list(model = model, outcome_model = outcome_model, treatment_model = treatment_model))
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
