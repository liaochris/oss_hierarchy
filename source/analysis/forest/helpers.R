NormalizeDatasetName <- function(dataset) {
  dataset |>
    gsub("_exact1", "", x = _) |>
    gsub("_nuclearWhat", "", x = _) |>
    gsub("_defaultWhat", "", x = _) |>
    gsub("_oneQual", "", x = _)
}

MakeDatasetLabels <- function(dataset) {
  list(
    num_qualified = ifelse(grepl("_exact1", dataset), "num-qualified=1",
                    ifelse(grepl("_oneQual", dataset), "num-qualified>=1", "all obs")),
    what_estimation = ifelse(grepl("_nuclearWhat", dataset), "only cohort + exact time",
                      ifelse(grepl("_defaultWhat", dataset), "default",
                             "all treated cohorts + exact time matches"))
  )
}

FirstDifferenceOutcome <- function(df, norm_outcome_col) {
  df %>%
    group_by(repo_name) %>%
    mutate(baseline = ifelse(quasi_event_time == -1, !!sym(norm_outcome_col), NA)) %>%
    fill(baseline, .direction = "downup") %>%
    mutate(fd_outcome = !!sym(norm_outcome_col) - baseline) %>%
    ungroup() %>%
    filter(!is.na(fd_outcome))
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

ComputeCovariateMeans <- function(df, covars, rolling_period = 1) {
  if (rolling_period == 1) {
    means <- df %>%
      filter(quasi_event_time >= -5 & quasi_event_time < -1) %>%
      group_by(repo_name) %>%
      summarise(across(any_of(covars), ~ mean(.x, na.rm = TRUE), .names = "{.col}_mean"),
                .groups = "drop")
    df %>% filter(quasi_event_time >= -5 & quasi_event_time <= 5) %>%
      left_join(means, by = "repo_name")
  } else if (rolling_period == 5) {
    means <- df %>%
      filter(quasi_event_time == -1) %>%
      select(repo_name, any_of(covars)) %>%
      rename_with(~ paste0(.x, "_mean"), any_of(covars))
    df %>%
      filter(quasi_event_time >= -5 & quasi_event_time <= 5) %>%
      left_join(means, by = "repo_name")
  }
}

AssignOutcomeFolds <- function(repo_names, outcome, n_folds = N_FOLDS, seed = SEED) {
  repo_sorted <- sort(unique(repo_names))
  outcome_bytes <- as.integer(charToRaw(as.character(outcome)))
  offset <- (sum(outcome_bytes) + as.integer(seed)) %% as.integer(n_folds)
  n_repos <- length(repo_sorted)
  df <- tibble(repo_name = repo_sorted, rank = seq_len(n_repos))
  df %>%
    mutate(fold = ((rank - 1) + offset) %% n_folds + 1) %>%
    select(repo_name, fold)
}

CreateDataPanel <- function(panel, method, outcome, covars, rolling_period, n_folds, seed) {
  if (method == "lm_forest" | method == "multi_arm") {
    df_panel_outcome <- ResidualizeOutcome(panel, outcome)
  } else if (method == "lm_forest_nonlinear") {
    df_panel_outcome <- NormalizeOutcome(panel, outcome)
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
  df_data %>% left_join(fold_map, by = "repo_name")
}

FilterPredictions <- function(tau_hat, df_data) {
  arm_names <- colnames(tau_hat)
  quasi_arm <- paste0("treatment_armcohort", df_data$quasi_treatment_group, "event_time",
                      df_data$time_index - df_data$quasi_treatment_group) |>
    sub("event_time-", "event_time.", x = _, fixed = TRUE)

  valid_arms <- tapply(quasi_arm, df_data$repo_id, unique)
  for (i in seq_len(nrow(tau_hat))) {
    keep <- arm_names %in% valid_arms[[as.character(df_data$repo_id[i])]]
    tau_hat[i, !keep] <- NA
  }
  tau_hat
}

FilterDoublyRobustPredictions <- function(tau_hat, df_data, df_repo_data) {
  arm_names <- colnames(tau_hat)
  quasi_arm <- paste0("cohort", df_data$quasi_treatment_group,
                      "event_time", df_data$time_index - df_data$quasi_treatment_group) |>
    sub("event_time-", "event_time.", x = _, fixed = TRUE)

  valid_arms <- tapply(quasi_arm, df_data$repo_id, unique)

  for (rid in names(valid_arms)) {
    rows <- df_repo_data$repo_id == as.integer(rid)
    keep <- arm_names %in% valid_arms[[rid]]
    tau_hat[rows][!keep] <- NA
  }
  tau_hat
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
    valid <- subset(arm_info, e > 0 & cohort + e <= max_time)
    slots_per_cohort <- ave(valid$cohort, valid$cohort, FUN = length)
    valid$p <- cohort_probs$p_g[match(valid$cohort, cohort_probs$quasi_treatment_group)] / slots_per_cohort
    data.frame(att = weighted_avg(valid))
  } else if (type == "cumulative") {
    valid <- subset(arm_info, e > 0 & cohort <= cumulative_cutoff & cohort + e <= cumulative_cutoff)
    if (!nrow(valid)) return(data.frame(cumu_t4 = NA))
    valid$p <- cohort_probs$p_g[match(valid$cohort, cohort_probs$quasi_treatment_group)]
    data.frame(cumu_t4 = weighted_avg(valid))
  }
}

AggregateRepoEventStudy <- function(tau_hat, df_data, max_time_val) {
  AggregateEventStudy(tau_hat, df_data, max_time_val, "event_time") %>%
    as_tibble() %>%
    select(-obs_id) %>%
    bind_cols(df_data %>% select(repo_name, repo_id)) %>%
    distinct()
}

AggregateRepoATT <- function(tau_hat, df_data, max_time_val) {
  bind_cols(
    df_data %>% select(repo_name, repo_id),
    AggregateEventStudy(tau_hat, df_data, max_time_val, "att")
  ) %>%
    distinct()
}

########## W / design matrix helpers ##########

MakeTimeIndicatorMatrix <- function(df_panel_nt) {
  W_og <- model.matrix(~ factor(time_index) - 1, data = df_panel_nt)
  W_nt <- matrix(0, nrow = nrow(W_og), ncol = ncol(W_og), dimnames = list(NULL, colnames(W_og)))
  time_idx <- match(paste0("factor(time_index)", df_panel_nt$time_index), colnames(W_nt))
  norm_period_idx <- match(paste0("factor(time_index)", df_panel_nt$quasi_treatment_group - 1), colnames(W_nt))
  W_nt[cbind(seq_len(nrow(W_nt)), time_idx)] <- 1
  W_nt[cbind(seq_len(nrow(W_nt)), norm_period_idx)] <- -1
  W_nt
}

ComputeBaselinePropensities <- function(W_nt, df_panel_nt, marg_dist_treatment_group) {
  W_hat <- matrix(0, nrow = nrow(W_nt), ncol = ncol(W_nt), dimnames = list(NULL, colnames(W_nt)))
  for (i in seq_len(nrow(df_panel_nt))) {
    col_name <- paste0("factor(time_index)", df_panel_nt$time_index[i])
    if (col_name %in% colnames(W_hat)) {
      W_hat[i, col_name] <- 1
    }
  }

  marg_dist_treatment_group <- marg_dist_treatment_group %>%
    filter(quasi_treatment_group <= max(df_panel_nt$quasi_treatment_group) &
             quasi_treatment_group >= min(df_panel_nt$quasi_treatment_group)) %>%
    group_by(first_time) %>%
    mutate(prob = prob / sum(prob))

  first_time_by_repo <- df_panel_nt %>%
    group_by(repo_id) %>%
    summarize(first_time = min(time_index), .groups = "drop")
  repo_probs <- first_time_by_repo %>%
    left_join(marg_dist_treatment_group, by = "first_time", relationship = "many-to-many") %>%
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

CalculateTreatment <- function(df_data, method) {
  if (method == "lm_forest" | method == "lm_forest_nonlinear") {
    W <- model.matrix(~ treatment_arm - 1, data = df_data)
    W <- W[, setdiff(colnames(W), colnames(W)[grepl("never-treated", colnames(W))])]
    return(list(W = W))
  } else {
    stop("Unknown method: ", method)
  }
}

AdjustPropensityMatrix <- function(cohort_propensities, W, df_data, drop_never_treated = TRUE) {
  if (drop_never_treated) {
    cohort_propensities <- cohort_propensities[, setdiff(colnames(cohort_propensities), "0"), drop = FALSE]
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
    if (!is.na(c) && as.character(c) %in% colnames(cohort_propensities)) {
      W_hat[, cohort == c] <- cohort_propensities[, as.character(c)]
    }
  }

  keep_mask <- outer(df_data$time_index, expected_time, `==`)
  if (!drop_never_treated) {
    keep_mask[, 1] <- TRUE
  }
  W_hat <- W_hat * keep_mask
  W_hat
}

AdjustPropensityMatrixCohort <- function(preds_by_cohort, W, df_data, drop_never_treated = TRUE) {
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

########## Cohort/time distribution ##########

ComputeCohortTimeDist <- function(df_data) {
  df_data %>% group_by(repo_id) %>%
    mutate(first_time = min(time_index)) %>%
    ungroup() %>%
    group_by(first_time, quasi_treatment_group) %>%
    summarize(n = n_distinct(repo_id), .groups = "drop_last") %>%
    mutate(prob = n / sum(n)) %>%
    ungroup()
}

########## Doubly-robust score computation ##########

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
  prob_no_treat <- (1 - rowSums(W_hat))
  IPW[-treated_rows, ] <- -1 * 1 / prob_no_treat[-treated_rows]
  IPW[treated_rows_cols] <- 1 / W_hat[treated_rows_cols]

  preds + IPW * as.vector(y_residual)
}

AggregateDoublyRobustByRepo <- function(dr_scores, df_data) {
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
  do.call(rbind, lapply(groups, function(ii) {
    apply(dr_scores[ii, , drop = FALSE], 2, function(x) {
      v <- x[!is.na(x)]
      if (length(v) > 0) v[1] else NA_real_
    })
  }))
}

CalculateDoublyRobust <- function(model, df_data) {
  dr_scores <- CalculateDoublyRobustScore(model)
  AggregateDoublyRobustByRepo(dr_scores, df_data)
}

########## Cohort-time coefficient transformation ##########

TransformRepoCohortTimeCoefficients <- function(tau_hat, df_data, outcome, values_to_col) {
  tibble::as_tibble(tau_hat) %>%
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
}

FilterRepoCohortEventPairs <- function(cohort_time_repo_long, df_data) {
  valid_pairs <- df_data %>%
    transmute(repo_id, cohort = quasi_treatment_group, event_time = time_index - quasi_treatment_group) %>%
    distinct()

  cohort_time_repo_long %>%
    mutate(cohort = as.integer(cohort), event_time = as.integer(event_time)) %>%
    semi_join(valid_pairs, by = c("repo_id", "cohort", "event_time"))
}
