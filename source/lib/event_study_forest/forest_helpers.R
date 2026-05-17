library(tidyverse)
library(grf)

ParseArmName <- function(arm) {
  cohort         <- as.integer(stringr::str_extract(arm, "(?<=cohort)\\d+"))
  event_time_num <- as.integer(stringr::str_extract(arm, "\\d+$"))
  is_negative    <- grepl("event_time[-\\.]", arm)
  event_time     <- ifelse(is_negative, -event_time_num, event_time_num)
  list(cohort = cohort, event_time = event_time)
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

AveragePreTreatmentCovariateAcrossPeriods <- function(df, covars, rolling_period = 1) {
  if (rolling_period == 1) {
    means <- df %>%
      filter(quasi_event_time >= MIN_EVENT_TIME & quasi_event_time < -1) %>%
      group_by(repo_name) %>%
      summarise(across(any_of(covars), ~ mean(.x, na.rm = TRUE), .names = "{.col}_mean"),
                .groups = "drop")
    df %>% filter(quasi_event_time >= MIN_EVENT_TIME & quasi_event_time <= MAX_EVENT_TIME) %>%
      left_join(means, by = "repo_name")
  } else if (rolling_period == 5) {
    means <- df %>%
      filter(quasi_event_time == -1) %>%
      select(repo_name, any_of(covars)) %>%
      rename_with(~ paste0(.x, "_mean"), any_of(covars))
    df %>%
      filter(quasi_event_time >= MIN_EVENT_TIME & quasi_event_time <= MAX_EVENT_TIME) %>%
      left_join(means, by = "repo_name")
  }
}

AssignOutcomeFolds <- function(repo_names, outcome, n_folds = N_FOLDS, seed = SEED) {
  repo_sorted   <- sort(unique(repo_names))
  outcome_bytes <- as.integer(charToRaw(as.character(outcome)))
  offset        <- (sum(outcome_bytes) + as.integer(seed)) %% as.integer(n_folds)
  tibble(repo_name = repo_sorted, rank = seq_along(repo_sorted)) %>%
    mutate(fold = ((rank - 1) + offset) %% n_folds + 1) %>%
    select(repo_name, fold)
}

CreateDataPanel <- function(panel, outcome, covars, rolling_period, n_folds, seed, normalize = TRUE) {
  if (normalize) {
    df_in       <- NormalizeOutcome(panel, outcome)
    outcome_col <- paste0(outcome, "_norm")
  } else {
    df_in       <- panel
    outcome_col <- outcome
  }
  df_fd   <- FirstDifferenceOutcome(df_in, outcome_col)
  df_data <- AveragePreTreatmentCovariateAcrossPeriods(df_fd, covars, rolling_period) %>%
    filter(quasi_event_time != -1) %>%
    mutate(
      treatment_arm = factor(
        ifelse(treatment_group == 0, "never-treated",
               paste0("cohort", treatment_group, "event_time", quasi_event_time))
      ),
      treatment_arm = relevel(treatment_arm, ref = "never-treated")
    )
  df_data %>% left_join(AssignOutcomeFolds(df_data$repo_name, outcome, n_folds, seed), by = "repo_name")
}

FilterPredictions <- function(tau_hat, df_data) {
  arm_names <- colnames(tau_hat)
  quasi_arm <- paste0("treatment_armcohort", df_data$quasi_treatment_group, "event_time",
                      df_data$time_index - df_data$quasi_treatment_group) |>
    sub("event_time-", "event_time.", x = _, fixed = TRUE)

  valid_arms_by_repo <- tapply(quasi_arm, df_data$repo_id, unique)
  valid_mat <- outer(df_data$repo_id, seq_along(arm_names), function(rid, j) {
    mapply(function(r, jj) arm_names[jj] %in% valid_arms_by_repo[[as.character(r)]], rid, j)
  })
  tau_hat[!valid_mat] <- NA
  tau_hat
}

FilterDoublyRobustPredictions <- function(tau_hat, df_data, df_repo_data) {
  arm_names <- colnames(tau_hat)
  quasi_arm <- paste0("cohort", df_data$quasi_treatment_group,
                      "event_time", df_data$time_index - df_data$quasi_treatment_group) |>
    sub("event_time-", "event_time.", x = _, fixed = TRUE)

  valid_arms_by_repo <- tapply(quasi_arm, df_data$repo_id, unique)
  for (rid in names(valid_arms_by_repo)) {
    rows <- df_repo_data$repo_id == as.integer(rid)
    tau_hat[rows, !(arm_names %in% valid_arms_by_repo[[rid]])] <- NA
  }
  tau_hat
}

AggregateEventStudy <- function(tau_hat, df, max_time,
                                type = c("event_time", "att", "cumulative"),
                                cumulative_cutoff = 4) {
  type <- match.arg(type)
  colnames(tau_hat) <- gsub("treatment_arm", "", colnames(tau_hat))
  colnames(tau_hat) <- gsub(" - never-treated", "", colnames(tau_hat))

  parsed   <- ParseArmName(colnames(tau_hat))
  arm_info <- data.frame(
    arm        = colnames(tau_hat),
    cohort     = parsed$cohort,
    event_time = parsed$event_time
  )

  cohort_probs <- df %>%
    filter(quasi_treatment_group > 0) %>%
    count(quasi_treatment_group, name = "n_obs") %>%
    mutate(prob_treatment_cohort = n_obs / sum(n_obs))

  weighted_avg <- function(valid) {
    tau_sub    <- tau_hat[, valid$arm, drop = FALSE]
    weight_vec <- valid$p / sum(valid$p, na.rm = TRUE)
    weight_mat <- matrix(rep(weight_vec, each = nrow(tau_sub)), nrow = nrow(tau_sub))
    weight_mat[is.na(tau_sub)] <- 0
    weight_mat <- weight_mat / rowSums(weight_mat, na.rm = TRUE)
    num        <- rowSums(tau_sub * weight_mat, na.rm = TRUE)
    num[apply(tau_sub, 1, function(x) all(is.na(x)))] <- NA
    num
  }

  if (type == "event_time") {
    est <- lapply(sort(unique(arm_info$event_time)), function(ev) {
      valid <- subset(arm_info, event_time == ev & cohort + ev <= max_time)
      if (!nrow(valid)) return(NULL)
      valid$p <- cohort_probs$prob_treatment_cohort[match(valid$cohort, cohort_probs$quasi_treatment_group)]
      weighted_avg(valid)
    })
    out <- as.data.frame(do.call(cbind, est))
    colnames(out) <- paste0("event_time", sort(unique(arm_info$event_time)))
    cbind(obs_id = seq_len(nrow(out)), out)
  } else if (type == "att") {
    valid            <- subset(arm_info, event_time > 0 & cohort + event_time <= max_time)
    slots_per_cohort <- ave(valid$cohort, valid$cohort, FUN = length)
    valid$p          <- cohort_probs$prob_treatment_cohort[match(valid$cohort, cohort_probs$quasi_treatment_group)] / slots_per_cohort
    data.frame(att = weighted_avg(valid))
  } else if (type == "cumulative") {
    valid <- subset(arm_info, event_time > 0 & cohort <= cumulative_cutoff & cohort + event_time <= cumulative_cutoff)
    if (!nrow(valid)) return(data.frame(cumu_t4 = NA))
    valid$p <- cohort_probs$prob_treatment_cohort[match(valid$cohort, cohort_probs$quasi_treatment_group)]
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

MakeTimeIndicatorMatrix <- function(df_panel_nt) {
  W_og         <- model.matrix(~ factor(time_index) - 1, data = df_panel_nt)
  W_nt         <- matrix(0, nrow = nrow(W_og), ncol = ncol(W_og), dimnames = list(NULL, colnames(W_og)))
  time_idx     <- match(paste0("factor(time_index)", df_panel_nt$time_index), colnames(W_nt))
  norm_idx     <- match(paste0("factor(time_index)", df_panel_nt$quasi_treatment_group - 1), colnames(W_nt))
  W_nt[cbind(seq_len(nrow(W_nt)), time_idx)] <- 1
  W_nt[cbind(seq_len(nrow(W_nt)), norm_idx)] <- -1
  W_nt
}

ComputeBaselinePropensities <- function(W_nt, df_panel_nt, marg_dist_treatment_group) {
  W_hat <- matrix(0, nrow = nrow(W_nt), ncol = ncol(W_nt), dimnames = list(NULL, colnames(W_nt)))

  time_col_names <- paste0("factor(time_index)", df_panel_nt$time_index)
  time_col_idx   <- match(time_col_names, colnames(W_hat))
  valid          <- !is.na(time_col_idx)
  W_hat[cbind(which(valid), time_col_idx[valid])] <- 1

  marg_dist_treatment_group <- marg_dist_treatment_group %>%
    filter(quasi_treatment_group >= min(df_panel_nt$quasi_treatment_group),
           quasi_treatment_group <= max(df_panel_nt$quasi_treatment_group)) %>%
    group_by(first_time) %>%
    mutate(prob_treatment_cohort = prob_treatment_cohort / sum(prob_treatment_cohort)) %>%
    ungroup()

  first_time_by_repo <- df_panel_nt %>%
    group_by(repo_id) %>%
    summarize(first_time = min(time_index), .groups = "drop")

  repo_probs <- first_time_by_repo %>%
    left_join(marg_dist_treatment_group, by = "first_time", relationship = "many-to-many") %>%
    select(repo_id, quasi_treatment_group, prob_treatment_cohort)

  for (rid in unique(repo_probs$repo_id)) {
    rows_idx <- which(df_panel_nt$repo_id == rid)
    rp       <- repo_probs[repo_probs$repo_id == rid, , drop = FALSE]
    col_names <- paste0("factor(time_index)", rp$quasi_treatment_group - 1)
    col_idx   <- match(col_names, colnames(W_hat))
    valid_k   <- !is.na(col_idx)
    for (k in which(valid_k)) {
      W_hat[rows_idx, col_idx[k]] <- W_hat[rows_idx, col_idx[k]] - rp$prob_treatment_cohort[k]
    }
  }
  W_hat
}

BuildTreatmentMatrix <- function(df_data) {
  W <- model.matrix(~ treatment_arm - 1, data = df_data)
  W[, !grepl("never-treated", colnames(W)), drop = FALSE]
}

AdjustPropensityMatrix <- function(cohort_propensities, W, df_data) {
  cohort_propensities <- cohort_propensities[, setdiff(colnames(cohort_propensities), "0"), drop = FALSE]
  cols          <- colnames(W)
  parsed        <- ParseArmName(cols)
  cohort        <- parsed$cohort
  event_time    <- parsed$event_time
  expected_time <- cohort + event_time

  W_hat <- matrix(0, nrow = nrow(W), ncol = ncol(W), dimnames = list(NULL, cols))
  for (c in unique(cohort[!is.na(cohort)])) {
    if (as.character(c) %in% colnames(cohort_propensities)) {
      W_hat[, cohort == c] <- cohort_propensities[, as.character(c)]
    }
  }
  W_hat * outer(df_data$time_index, expected_time, `==`)
}

ComputeCohortTimeDist <- function(df_data) {
  df_data %>%
    group_by(repo_id) %>%
    mutate(first_time = min(time_index)) %>%
    ungroup() %>%
    group_by(first_time, quasi_treatment_group) %>%
    summarize(n_obs = n_distinct(repo_id), .groups = "drop_last") %>%
    mutate(prob_treatment_cohort = n_obs / sum(n_obs)) %>%
    ungroup()
}

AggregateDoublyRobustByRepo <- function(dr_scores, df_data) {
  colnames(dr_scores) <- gsub("treatment_arm", "", colnames(dr_scores))
  parsed   <- ParseArmName(colnames(dr_scores))
  arm_info <- data.frame(
    arm        = colnames(dr_scores),
    cohort     = parsed$cohort,
    event_time = parsed$event_time
  )
  arm_info$expected_time <- arm_info$cohort + arm_info$event_time
  match_mask <- outer(df_data$time_index, arm_info$expected_time, "==")
  dr_scores[!match_mask] <- NA

  groups <- split(seq_len(nrow(df_data)), df_data$repo_id)
  do.call(rbind, lapply(groups, function(ii) {
    apply(dr_scores[ii, , drop = FALSE], 2, function(x) {
      v <- x[!is.na(x)]
      if (length(v) > 0) v[1] else NA_real_
    })
  }))
}

TransformRepoCohortTimeCoefficients <- function(tau_hat, df_data, outcome, values_to_col) {
  tibble::as_tibble(tau_hat) %>%
    bind_cols(df_data %>% select(repo_name, repo_id)) %>%
    tidyr::pivot_longer(-c(repo_name, repo_id), names_to = "arm", values_to = values_to_col) %>%
    distinct() %>%
    mutate(
      arm        = sub("^treatment_arm", "", arm),
      cohort     = ParseArmName(arm)$cohort,
      event_time = ParseArmName(arm)$event_time,
      outcome    = outcome
    ) %>%
    rename(coef = !!rlang::sym(values_to_col)) %>%
    select(outcome, repo_name, repo_id, cohort, event_time, coef)
}
