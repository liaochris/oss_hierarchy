
CreateDataPanel <- function(df_panel_common, method, outcome, covars, rolling_period, n_folds, seed) {
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
    mutate(treatment_arm = factor(ifelse(treatment_group == 0, "never-treated",
                                         paste0("cohort", treatment_group, "event_time", quasi_event_time))),
           treatment_arm = relevel(treatment_arm, ref = "never-treated"))
  fold_map <- AssignOutcomeFolds(df_data$repo_name, outcome, n_folds = n_folds, seed = seed)
  df_data <- df_data %>% left_join(fold_map, by = "repo_name")
  return(df_data)
}


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

AssignOutcomeFolds <- function(repo_names, outcome, n_folds = 10, seed = SEED) {
  repo_sorted <- sort(unique(repo_names))
  outcome_bytes <- as.integer(charToRaw(as.character(outcome)))
  offset <- (sum(outcome_bytes) + as.integer(seed)) %% as.integer(n_folds)
  n_repos <- length(repo_sorted)
  df <- tibble(repo_name = repo_sorted, rank = seq_len(n_repos))
  df <- df %>% mutate(fold = ((rank - 1) + offset) %% n_folds + 1)
  df %>% select(repo_name, fold)
}

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

FilterDoublyRobustPredictions <- function(preds_all, df_data, df_repo_data) {
  arm_names <- colnames(preds_all)
  quasi_arm <- paste0("cohort", df_data$quasi_treatment_group,
                      "event_time", df_data$time_index - df_data$quasi_treatment_group) |>
    sub("event_time-", "event_time.", x = _, fixed = TRUE)
  
  valid_arms <- tapply(quasi_arm, df_data$repo_id, unique)
  
  for (rid in names(valid_arms)) {
    rows <- df_repo_data$repo_id == as.integer(rid)
    keep <- arm_names %in% valid_arms[[rid]]
    preds_all[rows, !keep] <- NA
  }
  
  preds_all
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
    num[apply(tau_sub, 1, function(x) all(is.na(x)))] <- NA
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
