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

FitForestModel <- function(df_data, X_keep, Y, method, outfile, use_existing) {
  print(outfile)
  if (file.exists(outfile) && use_existing) {
    message("Loading existing model: ", outfile)
    return(readRDS(outfile))
  }
  
  set.seed(SEED)
  if (method == "lm_forest") {
    W <- model.matrix(~ treatment_arm - 1, data = df_data)
    W <- W[, setdiff(colnames(W), colnames(W)[grepl("never-treated", colnames(W))])]
    model <- lm_forest(X = X_keep, Y = Y, W = W, clusters = df_data$repo_id, num.trees = 2000)
  } else if (method == "multi_arm") {
    model <- multi_arm_causal_forest(X = X_keep, Y = Y, W = df_data$treatment_arm, clusters = df_data$repo_id, num.trees = 2000)
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
    model <- lm_forest(X = X_keep, Y = Y, W = W, clusters = df_data$repo_id, num.trees = 2000, gradient.weights = grad_vec)
  } else {
    stop("Unknown method: ", method)
  }
  
  dir_create(dirname(outfile))
  saveRDS(model, outfile)
  model
}

AggregateEventStudy <- function(tau_hat, df, max_time, type = c("event_time", "att", "cumulative"), cumulative_cutoff = 4) {
  type <- match.arg(type)
  colnames(tau_hat) <- gsub("treatment_arm", "", colnames(tau_hat))
  
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
    w_mat <- w_mat/rowSums(w_mat, na.rm = T)
    num <- rowSums(tau_sub * w_mat, na.rm = TRUE)
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
    att <- weighted_avg(valid)
    data.frame(att = att)
    
  } else if (type == "cumulative") {
    valid <- subset(arm_info, e >= 0 & cohort <= cumulative_cutoff & cohort + e <= cumulative_cutoff)
    if (!nrow(valid)) return(data.frame(cumu = NA))
    valid$p <- cohort_probs$p_g[match(valid$cohort, cohort_probs$quasi_treatment_group)]
    cumu <- weighted_avg(valid)
    data.frame(cumu_t4 = cumu)
  }
}
