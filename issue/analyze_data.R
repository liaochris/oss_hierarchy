library(tidyverse)
library(did)
library(arrow)
library(gridExtra)
library(ggplot2)
library(egg)
library(eventstudyr)
library(SaveData)
library(future)
library(future.apply)
library(dplyr)
library(purrr)
library(stringr)
library(fixest)
library(didimputation)
library(did2s)
library(dplyr)
library(rlang)
library(aod)


NormalizeOutcome <- function(df, outcome, default_outcome = "prs_opened") {
  outcome_norm <- paste(outcome, "norm", sep = "_")
  sd_outcome_var <- ifelse(grepl("count", outcome), default_outcome, outcome)
  if (grepl("_predep", outcome)) {
    sd_outcome_var <- sub("_predep", "", outcome)
  } else if (grepl("_nondep", outcome)) {
    sd_outcome_var <- sub("_nondep", "", outcome)
  }
  if (grepl("n_avg_", outcome) & startsWith(outcome, "n_avg_")) {
    sd_outcome_var <- sub("n_avg_", "", sd_outcome_var)
  } else if (grepl("avg_", outcome) & startsWith(outcome, "avg_")) {
    sd_outcome_var <- sub("avg_", "", sd_outcome_var)
  }
  if (grepl("_dept_", outcome)) {
    sd_outcome_var <- gsub("_dept.*", "", sd_outcome_var)
  } else {
    sd_outcome_var <- sd_outcome_var
  }
  
  df_norm <- df %>%
    group_by(repo_name) %>%
    mutate(mean_outcome = mean(get(outcome)[time_index < treatment_group & 
                                              time_index >= (treatment_group - 4)], na.rm = TRUE),
           sd_outcome = sd(get(sd_outcome_var)[time_index < treatment_group & 
                                                 time_index >= (treatment_group - 4)], na.rm = TRUE)) %>%
    ungroup()
  df_norm[[outcome_norm]] <- (df_norm[[outcome]] - df_norm$mean_outcome)/df_norm$sd_outcome
  print(weighted.mean(df_norm$sd_outcome/df_norm$mean_outcome, df_norm$mean_outcome))
  print(mean(df_norm$sd_outcome/df_norm$mean_outcome))
  df_norm %>% filter(is.finite(get(outcome_norm)))
}

EventStudy <- function(df, outcome, method = c("cs", "2s", "bjs", "es"), normalize = FALSE, title = NULL) {
  method <- match.arg(method)
  df_est <- if (normalize) NormalizeOutcome(df, outcome) else df %>% mutate("{outcome}_norm" := .data[[outcome]])
  yvar <- paste0(outcome, "_norm")
  
  # compute coefficients & std errors
  res_mat <- switch(method,
                    bjs = {
                      est <- did_imputation(df_est, yname = yvar, gname = "treatment_group", 
                                            tname = "time_index", idname = "repo_name", horizon = T, pretrends = -4:-1) %>% 
                        rename(sd = std.error, ci_low = conf.low, ci_high = conf.high) %>%
                        select(term, estimate, sd, ci_low, ci_high) %>%
                        drop_na() %>%
                        mutate(term = as.numeric(term)) %>%
                        column_to_rownames("term") %>% 
                        as.matrix() 
                      est
                    },
                    cs = {
                      set.seed(420)
                      att <- att_gt(yname = yvar, tname = "time_index", idname = "repo_name_id",
                                    gname = "treatment_group", xformla = ~1, data = df_est,
                                    control_group = "notyettreated", allow_unbalanced_panel = T,
                                    clustervars = "repo_name", est_method = "dr")
                      est <- aggte(att, type = "dynamic", na.rm = TRUE) %>% 
                        tidy() %>% 
                        rename(sd = std.error, ci_low = conf.low, ci_high = conf.high) %>%
                        select(event.time, estimate, sd, ci_low, ci_high) %>%
                        mutate(event.time = as.numeric(event.time)) %>%
                        column_to_rownames("event.time") %>% 
                        as.matrix() 
                      est
                    },
                    `2s` = {
                      rel_df <- df_est %>% mutate(rel_time = time_index - treatment_group)
                      es2s_obj <- did2s(rel_df, yname = yvar,
                                        first_stage = ~ 0 | repo_name + time_index,
                                        second_stage = ~ i(rel_time, ref = -1),
                                        treatment = "treatment", cluster_var = "repo_name")
                      CleanFixEst(es2s_obj, "rel_time::")
                    },
                    es = {
                      est <- eventstudyr::EventStudy(estimator="OLS",data=df_est,outcomevar=yvar,
                                                     policyvar="treatment",idvar="repo_name",
                                                     timevar="time_index",post=4,pre=0)$output %>%
                        tidy() %>%
                        rename(sd=std.error, ci_low=conf.low, ci_high=conf.high) %>%
                        select(term, estimate, sd, ci_low, ci_high) %>%
                        mutate(term = case_when(
                          str_detect(term, "fd_lead") ~ -as.numeric(str_extract(term, "\\d+$")),
                          str_detect(term, "lead") ~ -(as.numeric(str_extract(term, "\\d+$")) + 1),
                          str_detect(term, "lag") ~ as.numeric(str_extract(term, "\\d+$")),
                          TRUE ~ 0
                        )) %>%
                        column_to_rownames("term") %>%
                        as.matrix()
                      est
                    }
  )
  res_mat <- EnsureMinusOneRow(res_mat)
  res_mat[is.na(res_mat)] <- 0
  res_mat <- res_mat[order(as.numeric(rownames(res_mat))), , drop = FALSE]
  plot_fun <- fixest::coefplot
  plot_obj <- plot_fun(res_mat,
                       main = title,
                       xlab = "Event time (k)",
                       keep = "^-[1-4]|[0-4]",
                       drop = "[[:digit:]]{2}",
                       ref.line = 0)
  list(plot = plot_obj, results = res_mat)
}

CleanFixEst <- function(est_obj, term_prefix) {
  cleaned_tbl <- est_obj %>% 
    tidy() %>%
    mutate(term = as.numeric(str_remove(term, paste0("^", term_prefix))),
           sd = std.error) %>%
    select(-std.error, -statistic, -p.value) %>%
    mutate(ci_low = estimate - 1.96 * sd, 
           ci_high = estimate + 1.96 * sd) %>%
    column_to_rownames("term") %>%
    as.matrix()
  return(cleaned_tbl)
}

EnsureMinusOneRow <- function(est_mat) {
  if (!("-1" %in% rownames(est_mat))) {
    blank_row <- matrix(c(0, 0, 0, 0), nrow = 1)
    colnames(blank_row) <- colnames(est_mat)
    rownames(blank_row) <- "-1"
    est_mat <- rbind(est_mat, blank_row)
  }
  est_mat
}

TestPretrends <- function(res_mat, terms = as.character(-4:-1)) {
  available <- intersect(rownames(res_mat), terms)
  if (length(available) == 0) return(NA_real_)
  sorted_terms <- available[order(as.numeric(available))]
  estimates <- res_mat[sorted_terms, "estimate"]
  variances <- res_mat[sorted_terms, "sd"]^2
  sigma <- diag(variances, nrow = length(variances))
  p_value <- tryCatch({
    wald.test(b = estimates, Sigma = sigma, Terms = seq_along(estimates))$result$chi2[["P"]]
  }, error = function(e) NA_real_)
  p_value
}

CompareES <- function(...,
                      legend_title = NULL,
                      legend_labels = NULL,
                      title = "",
                      add_p = TRUE,
                      ylim = NULL) {
  old_par <- par(no.readonly = TRUE)
  on.exit(par(old_par), add = TRUE)
  par(bg = "white")
  
  es_list <- list(...)
  results <- lapply(es_list, `[[`, "results")
  if (length(results) == 0) {
    warning("CompareES: no results to plot")
    return(invisible(NULL))
  }
  
  par(mar = c(8, 4, 2, 2) + 0.1)
  
  plot_args <- list(
    object = results,
    xlab = "Event time (k)",
    ylab = "",
    main = title,
    keep = "^-[1-4]|[0-4]",
    drop = "[[:digit:]]{2}",
    order = c("-4", "-3", "-2", "-1", "0", "1", "2", "3", "4"),
    xaxt = "n", # suppress the x‐axis
    yaxt = "n",
    grid = FALSE
  )
  if (!is.null(ylim)) plot_args$ylim <- ylim
  do.call(fixest::coefplot, plot_args)
  
  if (!is.null(legend_labels)) {
    par(xpd = NA)
    legend(
      "top", legend = legend_labels, title = legend_title,
      horiz = TRUE, 
      bty = "o", box.lwd = 0.8, box.col = "grey40",
      bg = "white", xjust = 0.5,
      col = seq_along(results), pch = 20,
      lwd = 1, lty = seq_along(results)
    )
    par(xpd = FALSE)
  }
  
  if (add_p) {
    # Wald p-value
    if (length(results) == 2) {
      wp <- CompareEventCoefsWald(results, terms = 0:4)
      wp_str <- sprintf("%.3f", wp)
      wald_lbl <- paste0("Wald test p-value: ", wp_str)
    } else if (length(results)>2) {
      combos <- combn(length(results), 2)
      wald_lbl <- paste(
        apply(combos, 2, function(idx) {
          p_val <- CompareEventCoefsWald(results[idx], terms = 0:4)
          p_str <- sprintf("%.3f", p_val)
          paste0(legend_labels[idx[1]], " vs ", legend_labels[idx[2]],
                 ": p=", p_str)
        }),
        collapse = " | "
      )
    }
    
    # Pretrend p-values
    pre_p <- vapply(results, TestPretrends, numeric(1))
    pre_strs <- ifelse(pre_p < 0.001,
                       sprintf("p-value < %.3f", 0.001),
                       sprintf("p-value: %.3f", pre_p))
    
    pre_lbl <- paste(
      paste0("Pretrend (", legend_labels, ") ", pre_strs),
      collapse = "\n"
    )
    if (length(results) == 1) {     pre_lbl <- paste(
      paste0("Pretrend p-value: ", pre_strs),
      collapse = "\n"
    ) ; wald_lbl <- "" }
    
    # position
    usr <- par("usr")
    x_min <- usr[1]; x_max <- usr[2]
    buffer <- 0.02 * (x_max - x_min)
    x_left <- x_min + buffer
    
    # draw inside plot, left-aligned
    mtext(wald_lbl, side = 1, line = -1, at = x_left, adj = 0, cex = 0.8)
    mtext(pre_lbl, side = 1, line = -1.8, at = x_left, adj = 0, cex = 0.8)
  }
}

CompareEventCoefsWald <- function(tidy_list, terms = 0:4) {
  sel <- as.character(terms)
  m1 <- as.matrix(tidy_list[[1]])
  m2 <- as.matrix(tidy_list[[2]])
  available <- intersect(intersect(rownames(m1), rownames(m2)), sel)
  if (length(available) == 0) {
    return(NA_real_)
  }
  avail_sorted <- available[order(as.numeric(available))]
  if (length(avail_sorted) < 4) {
    return(NA)
  }
  t1 <- tidy_list[[1]][avail_sorted, ]
  t2 <- tidy_list[[2]][avail_sorted, ]
  delta <- t1[,"estimate"] - t2[,"estimate"]
  var_sum <- t1[,"sd"]^2 + t2[,"sd"]^2
  
  Sigma <- diag(var_sum, nrow = length(var_sum))
  p_val <- p_val <- tryCatch({
    wald.test(b = delta, Sigma = Sigma, Terms = seq_along(delta))$result$chi2[["P"]]
  }, error = function(e) NA)
  return(as.numeric(p_val))
}

CheckPreTreatmentThreshold <- function(df_panel_nyt, periods_count, outcome, count_thresh) {
  notsmall_preperiod <- df_panel_nyt %>%
    filter(time_index >= (treatment_group - periods_count),
           time_index < treatment_group) %>%
    group_by(repo_name) %>%
    summarise(meets_threshold = all(.data[[outcome]] >= count_thresh),
              .groups = "drop") %>%
    filter(meets_threshold) %>% pull(repo_name) 
  notsmall_preperiod
}

HasMinPreTreatmentPeriods <- function(df_panel_nyt, periods_count) {
  projects <- df_panel_nyt %>%
    group_by(repo_name) %>%
    summarise(
      n_pre = sum(time_index < treatment_group),
      .groups = "drop"
    ) %>%
    filter(n_pre >= periods_count) %>%
    pull(repo_name)
  df_panel_nyt %>% filter(repo_name %in% projects)
}

group_defs <- list(
  # 1. Collaborativeness
  list(
    filters = list(list(col = "ind_key_collab_2bin", vals = c(1, 0))),
    fname_prefix = "prs_opened_collab_",
    legend_title = "Departed collaborativeness",
    legend_labels = c("High", "Low")
  ),
  list(
    filters = list(
      list(col = "departed_involved_2bin", vals = c(1, 0)),
      list(col = "departed_opened_2bin", vals = c(1, 0))
    ),
    fname_prefix = "prs_opened_involved_departed_opened_",
    legend_title = "Departed Involvement & PR Activity",
    legend_labels = c(
      "High Involvement + Many PRs",
      "Low Involvement + Many PRs",
      "High Involvement + Few PRs",
      "Low Involvement + Few PRs"
    )
  ),
  list(
    filters = list(list(col = "departed_involved_2bin", vals = c(1, 0))),
    fname_prefix = "prs_opened_involved_",
    legend_title = "Departed contributor involvement",
    legend_labels = c("High", "Low")
  ),
  list(
    filters = list(list(col = "departed_opened_2bin", vals = c(1, 0))),
    fname_prefix = "prs_opened_departed_opened_",
    legend_title = "Departed contributor PR involvement",
    legend_labels = c("High", "Low")
  ),
  list(
    filters = list(list(col = "ind_other_collab_2bin", vals = c(1, 0))),
    fname_prefix = "prs_opened_other_collab_",
    legend_title = "Other contributor collaborativeness",
    legend_labels = c("High", "Low")
  ),
  list(
    filters = list(list(col = "ind_other_collab_2bin", vals = c(1, 0)),
                   list(col = "ind_key_collab_2bin", vals = c(1, 0))),
    fname_prefix = "prs_opened_other_key_collab_",
    legend_title = "Other + departed contributor collaborativeness",
    legend_labels = c(
      "High + High",
      "Low + High",
      "High + Low",
      "Low + Low"
    )
  )
)
metrics <- c("cs") # BJS HAS KNOWN ISSUE
metrics_fn <- c("Callaway and Sant'Anna 2020")



# ----- Panels & ID Assignment -----
df_panel_nyt <- read_parquet('issue/df_panel_nyt.parquet')
df_panel_nyt_all <- read_parquet('issue/df_panel_nyt_all.parquet')
df_panel_nyt_alltime <- read_parquet('issue/df_panel_nyt_alltime.parquet')

df_predep_cc <- df_panel_nyt_all %>% select(repo_name, time_period, total_contributor_count) %>% 
  rename(predep_contributor_count = total_contributor_count)
df_nodep_cc <- df_panel_nyt_alltime %>% select(repo_name, time_period, total_contributor_count) %>% 
  rename(nodep_contributor_count = total_contributor_count)
df_panel_nyt <- df_panel_nyt %>% left_join(df_predep_cc) %>% left_join(df_nodep_cc) %>%
  group_by(repo_name) %>%
  mutate(
    nodep_contributor_count_neg1 = nodep_contributor_count[time_index - treatment_group == -1][1],
    predep_contributor_count_neg1 = predep_contributor_count[time_index - treatment_group == -1][1],
    contributors_dept_comm_neg1 = contributors_dept_comm[time_index - treatment_group == -1][1],
    contributors_dept_never_comm_predep_neg1 = contributors_dept_never_comm_predep[time_index - treatment_group == -1][1],
    contributors_dept_never_comm_neg1 = contributors_dept_never_comm[time_index - treatment_group == -1][1],
    contributors_dept_comm_avg_above_neg1 = contributors_dept_comm_avg_above[time_index - treatment_group == -1][1],
    contributors_dept_comm_avg_below_neg1 = contributors_dept_comm_avg_below[time_index - treatment_group == -1][1]
  ) %>%
  ungroup()

SafeDivide <- function(numerator, denominator) {
  result <- numerator / denominator
  ifelse(is.finite(result), result, 0)
}
df_panel_nyt <- df_panel_nyt %>%
  mutate(
    avg_prs_opened_nondep = prs_opened_nondep / nodep_contributor_count,
    avg_prs_opened_predep = prs_opened_predep / predep_contributor_count,
    avg_prs_opened_dept_comm = prs_opened_dept_comm/contributors_dept_comm,
    avg_prs_opened_dept_never_comm = prs_opened_dept_never_comm / contributors_dept_never_comm,
    n_avg_prs_opened_nondep = nodep_contributor_count_neg1 * avg_prs_opened_nondep,
    n_avg_prs_opened_predep = predep_contributor_count_neg1 * avg_prs_opened_predep,
    avg_prs_opened_dept_comm_avg_above = SafeDivide(prs_opened_dept_comm_avg_above, contributors_dept_comm_avg_above),
    avg_prs_opened_dept_comm_avg_below = SafeDivide(prs_opened_dept_comm_avg_below, contributors_dept_comm_avg_below),
    n_avg_prs_opened_dept_comm_avg_above = contributors_dept_comm_avg_above_neg1 * avg_prs_opened_dept_comm_avg_above,
    n_avg_prs_opened_dept_comm_avg_below = contributors_dept_comm_avg_below_neg1 * avg_prs_opened_dept_comm_avg_below,
    avg_prs_opened_dept_never_comm_predep = prs_opened_dept_never_comm_predep / contributors_dept_never_comm_predep,
    n_avg_prs_opened_dept_comm = contributors_dept_comm_neg1 * avg_prs_opened_dept_comm,
    n_avg_prs_opened_dept_never_comm_predep = contributors_dept_never_comm_predep_neg1 * avg_prs_opened_dept_never_comm_predep,
    n_avg_prs_opened_dept_never_comm = contributors_dept_never_comm_neg1 * avg_prs_opened_dept_never_comm_predep,
    avg_prs_opened_dept_comm_per_problem_avg_above = SafeDivide(prs_opened_dept_comm_per_problem_avg_above, contributors_dept_comm_per_problem_avg_above),
    avg_prs_opened_dept_comm_per_problem_avg_below = SafeDivide(prs_opened_dept_comm_per_problem_avg_below, contributors_dept_comm_per_problem_avg_below),
    avg_prs_opened_dept_comm_per_problem_min_avg_above = SafeDivide(prs_opened_dept_comm_per_problem_min_avg_above, contributors_dept_comm_per_problem_min_avg_above),
    avg_prs_opened_dept_comm_per_problem_min_avg_below = SafeDivide(prs_opened_dept_comm_per_problem_min_avg_below, contributors_dept_comm_per_problem_min_avg_below),
  ) %>%
  mutate(prs_opened = prs_opened_prob)

comm_label_map <- c(
  "prs_opened_dept_comm" = "Yes",
  "prs_opened_dept_never_comm" = "No",
  "prs_opened_dept_never_comm_predep" = "No",
  "prs_opened_dept_comm_avg_above" = "Above average",
  "prs_opened_dept_comm_avg_below" = "Below average",
  "avg_prs_opened_dept_comm" = "Yes",
  "avg_prs_opened_dept_never_comm" = "No",
  "avg_prs_opened_dept_never_comm_predep" = "No",
  "avg_prs_opened_dept_comm_avg_above" = "Above avg. comm.",
  "avg_prs_opened_dept_comm_avg_below" = "Below avg. comm.",
  "prs_opened_dept_comm_per_problem_avg_above" = "Above average",
  "prs_opened_dept_comm_per_problem_avg_below" = "Below average",
  "prs_opened_dept_comm_per_problem_min_avg_above" = "Above average",
  "prs_opened_dept_comm_per_problem_min_avg_below" = "Below average",
  "avg_prs_opened_dept_comm_per_problem_avg_above"= "Above average",
  "avg_prs_opened_dept_comm_per_problem_avg_below"= "Below average",
  "avg_prs_opened_dept_comm_per_problem_min_avg_above"= "Above average",
  "avg_prs_opened_dept_comm_per_problem_min_avg_below"= "Below average",
  "n_avg_prs_opened_dept_comm" = "Yes", 
  "n_avg_prs_opened_dept_never_comm_predep" = "No",
  "n_avg_prs_opened_dept_comm_avg_above" = "Above average",
  "n_avg_prs_opened_dept_comm_avg_below" = "Below average",
  "prs_opened_dept_comm_2avg_above" = "Above 2xaverage",
  "prs_opened_dept_comm_2avg_below" = "Below 2xaverage"
)

length(unique(df_panel_nyt$repo_name))

# --- helpers (CamelCase) -----------------------------------------------------

## Replace your helpers with these

StoreOutcomes <- c("n_avg_prs_opened_nondep",
                   "n_avg_prs_opened_predep",
                   "prs_opened_nondep",
                   "prs_opened",
                   "prs_opened_predep",
                   "n_avg_prs_opened_dept_comm",
                   "n_avg_prs_opened_dept_never_comm_predep",
                   "prs_opened_dept_comm",
                   "prs_opened_dept_never_comm_predep",
                   "n_avg_prs_opened_dept_never_comm",
                   "n_avg_prs_opened_dept_comm_avg_below",
                   "n_avg_prs_opened_dept_comm_avg_above",
                   "prs_opened_dept_comm_avg_above",
                   "prs_opened_dept_comm_avg_below")

ShouldStoreOutcome <- function(outcome) outcome %in% StoreOutcomes

NormalizeEstimateCols <- function(df) {
  df <- as.data.frame(df, stringsAsFactors = FALSE)
  nm <- names(df)
  if ("std.error" %in% nm) names(df)[nm == "std.error"] <- "sd"
  if ("std_error" %in% nm) names(df)[nm == "std_error"] <- "sd"
  if ("att" %in% nm && !("estimate" %in% nm)) names(df)[nm == "att"] <- "estimate"
  if ("coef" %in% nm && !("estimate" %in% nm)) names(df)[nm == "coef"] <- "estimate"
  df
}

MatrixToDf <- function(mat) {
  if (!is.matrix(mat) || nrow(mat) == 0) return(NULL)
  df <- as.data.frame(mat, stringsAsFactors = FALSE)
  df$event_time <- suppressWarnings(as.numeric(rownames(mat)))
  rownames(df) <- NULL
  NormalizeEstimateCols(df)
}

ExtractFromAggte <- function(x) {
  if (!inherits(x, "aggte")) return(NULL)
  if (!is.null(x$att.egt) && !is.null(x$egt)) {
    se <- if (!is.null(x$se.egt)) x$se.egt else rep(NA_real_, length(x$att.egt))
    cv <- if (!is.null(x$crit.val.egt)) x$crit.val.egt else rep(NA_real_, length(x$att.egt))
    return(NormalizeEstimateCols(data.frame(
      type = "dynamic",
      event_time = x$egt,
      estimate = x$att.egt,
      sd = se,
      ci_low = x$att.egt - cv * se,
      ci_high = x$att.egt + cv * se,
      check.names = FALSE
    )))
  }
  if (!is.null(x$att)) {
    se <- if (!is.null(x$se)) x$se else NA_real_
    return(NormalizeEstimateCols(data.frame(
      type = if (!is.null(x$type)) x$type else "overall",
      estimate = x$att,
      sd = se,
      check.names = FALSE
    )))
  }
  NULL
}

ExtractFromAttGt <- function(x) {
  if (!inherits(x, "att_gt")) return(NULL)
  NormalizeEstimateCols(data.frame(
    type = "gt",
    group = x$group,
    time = x$time,
    estimate = x$att,
    check.names = FALSE
  ))
}

ExtractFromPlot <- function(p) {
  if (inherits(p, "ggplot") && is.data.frame(p$data) && nrow(p$data)) {
    return(NormalizeEstimateCols(p$data))
  }
  NULL
}

ExtractFromModel <- function(mod) {
  td <- tryCatch(broom::tidy(mod), error = function(e) NULL)
  if (is.data.frame(td) && nrow(td)) return(NormalizeEstimateCols(td))
  NULL
}

ExtractEstimates <- function(x) {
  if (is.null(x)) return(NULL)
  
  # direct object types
  if (is.matrix(x)) return(MatrixToDf(x))
  if (is.data.frame(x) && nrow(x)) return(NormalizeEstimateCols(x))
  if (inherits(x, "aggte")) return(ExtractFromAggte(x))
  if (inherits(x, "att_gt")) return(ExtractFromAttGt(x))
  
  # list containers (your EventStudy returns list(plot=..., results=matrix))
  if (is.list(x)) {
    if (!is.null(x$results)) {
      out <- MatrixToDf(x$results)
      if (!is.null(out)) return(out)
    }
    if (!is.null(x$aggte)) {
      out <- ExtractFromAggte(x$aggte)
      if (!is.null(out)) return(out)
    }
    if (!is.null(x$es)) {
      out <- ExtractFromAggte(x$es)
      if (!is.null(out)) return(out)
    }
    cand <- list(x$estimates, x$event_df, x$coefficients, x$df,
                 ExtractFromPlot(x$plot), ExtractFromModel(x$model))
    for (c in cand) if (is.data.frame(c) && nrow(c)) return(NormalizeEstimateCols(c))
    if (!is.null(x$coef) && is.numeric(x$coef)) {
      return(NormalizeEstimateCols(data.frame(term = names(x$coef), estimate = as.numeric(x$coef), check.names = FALSE)))
    }
  }
  
  NULL
}

BuildMetaFrame <- function(df_name, metric, normalize, outcome, legend_label, g, vals_row) {
  meta <- data.frame(df_name = df_name, metric = metric, normalize = normalize,
                     outcome = outcome, legend_label = legend_label, stringsAsFactors = FALSE)
  for (j in seq_along(g$filters)) {
    col_name <- g$filters[[j]]$col
    meta[[col_name]] <- vals_row[[j]]
  }
  meta
}

AppendEstimateRow <- function(store_list, es_result, df_name, metric, normalize, outcome, legend_label, g, vals_row) {
  est <- ExtractEstimates(es_result)
  if (is.null(est)) return(store_list)
  meta <- BuildMetaFrame(df_name, metric, normalize, outcome, legend_label, g, vals_row)
  store_list[[length(store_list) + 1]] <- cbind(meta, est)
  store_list
}


SaveCompare <- function(path, es_list, legend_labels, legend_title, ylim = NULL, title = "") {
  png(path)
  args <- c(es_list, list(legend_labels = legend_labels, legend_title = legend_title, title = title, ylim = ylim))
  do.call(CompareES, args)
  dev.off()
}

# -----------------------------------------------------------------------------

for (df_name in c('df_panel_nyt')) {
  output_root <- if (df_name == 'df_panel_nyt') 'issue/output' else ""
  all_collab_ylim <- c(-4, 4.5)
  pre_nondep_ylim <- c(-4, 4.5)
  comm_nondep_ylim <- c(-4, 4.5)
  comm_predep_ylim <- c(-4, 4.5)
  spec_ylim <-  c(-4, 4.5)
  modes <- list(
    list(normalize = TRUE, file = file.path(output_root, "prs_opened_norm.png"), outcome = "prs_opened")
  )
  panel <- get(df_name)
  panel <- panel %>% 
    mutate(repo_name_id = as.integer(factor(repo_name, levels = sort(unique(repo_name)))))
  panel <- panel %>%
   filter(repo_name %in% CheckPreTreatmentThreshold(df_panel_nyt, 3, "prs_opened", 5))
  print(length(unique(panel$repo_name)))
  for (mode in modes) {
    es_list <- lapply(metrics, function(m) {
      EventStudy(panel, mode$outcome, m, title = "", normalize = mode$normalize)
    })
    png(mode$file)
    do.call(CompareES, c(es_list, list(ylim = all_collab_ylim)))
    dev.off()
  }
  
  for (norm in c(TRUE)) {
    norm_str <- ifelse(norm, "_norm", "")
    for (met in metrics) {
      for (g in group_defs) {
        combo_grid <- expand.grid(lapply(g$filters, `[[`, "vals"), 
                                  KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
        es_list <- apply(combo_grid, 1, function(vals_row){
          df_sub <- panel
          for (i in seq_along(vals_row)) {
            col_name <- g$filters[[i]]$col
            df_sub <- df_sub %>% dplyr::filter(.data[[col_name]] == vals_row[[i]])
          }
          tryCatch(EventStudy(df_sub, "prs_opened", method = met, title = "", normalize = norm),
                   error = function(e) NULL)
        }, simplify = FALSE)
        success_idx <- which(!sapply(es_list, is.null))
        es_list <- es_list[success_idx]
        labels <- g$legend_labels[success_idx]
        legend_title <- g$legend_title
        
        out_path <- paste0(file.path(output_root, "collab/"), g$fname_prefix, met, norm_str, ".png")
        png(out_path)
        do.call(CompareES, c(es_list, list(legend_labels = labels, ylim = all_collab_ylim, legend_title = legend_title)))
        dev.off()
      }
    }
  }
  
  paired_outcomes <- list(
    prs_opened_dept_never_comm = list(
      partner = "prs_opened_dept_comm",
      name = "Communicated w/ departed"
    ),
    prs_opened_dept_comm_avg_above = list(
      partner = "prs_opened_dept_comm_avg_below",
      name = "Communication intensity"
    ),
    prs_opened_dept_comm_2avg_above = list(
      partner = "prs_opened_dept_comm_2avg_below",
      name = "Communication intensity"
    ),
    avg_prs_opened_dept_comm_avg_above = list(
      partner = "avg_prs_opened_dept_comm_avg_below",
      name = "Communication intensity"
    ),
    avg_prs_opened_dept_never_comm = list(
      partner = "avg_prs_opened_dept_comm",
      name = "Communicated w/ Departed"
    ),
    prs_opened_dept_never_comm_predep = list(
      partner = "prs_opened_dept_comm",
      name = "Communicated w/ Departed"
    ),
    n_avg_prs_opened_dept_never_comm_predep = list(
      partner = "n_avg_prs_opened_dept_comm",
      name = "Communicated w/ Departed"
    ),
    avg_prs_opened_dept_never_comm_predep = list(
      partner = "avg_prs_opened_dept_comm",
      name = "Communicated w/ Departed"
    ),
    prs_opened_dept_comm_per_problem_avg_above = list(
      partner = "prs_opened_dept_comm_per_problem_avg_below",
      name = "Per-problem communication intensity"
    ),
    prs_opened_dept_comm_per_problem_min_avg_above = list(
      partner = "prs_opened_dept_comm_per_problem_min_avg_below",
      name = "Per-problem communication intensity"
    ),
    avg_prs_opened_dept_comm_per_problem_avg_above = list(
      partner = "avg_prs_opened_dept_comm_per_problem_avg_below",
      name = "Per-problem communication intensity"
    ),
    avg_prs_opened_dept_comm_per_problem_min_avg_above = list(
      partner = "avg_prs_opened_dept_comm_per_problem_min_avg_below",
      name = "Per-problem communication intensity"
    ),
    n_avg_prs_opened_dept_comm_avg_above = list(
      partner = "n_avg_prs_opened_dept_comm_avg_below",
      name = "Communication intensity"
    )
  )
  
  g <- list(
    filters = list(list(col = "ind_key_collab_2bin", vals = c(1, 0))),
    fname_prefix = "prs_opened_collab_",
    legend_title = "Departed collaborativeness",
    legend_labels = c("High", "Low")
  )
  
  coef_predep_rows <- list()
  coef_nondep_rows <- list()
  
  for (norm in TRUE) {
    norm_str <- if (norm) "_norm" else ""
    for (met in metrics) {
      outcome_vec <- c("prs_opened", "total_contributor_count", "prs_opened_nondep", "prs_opened_predep")
      if (df_name == 'df_panel_nyt') {
        outcome_vec <- c(outcome_vec,
                         "avg_prs_opened_nondep", "avg_prs_opened_predep",
                         "n_avg_prs_opened_nondep", "n_avg_prs_opened_predep",
                         names(paired_outcomes))
      }
      
      for (outcome in outcome_vec) {
        combo_grid <- expand.grid(lapply(g$filters, `[[`, "vals"),
                                  KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
        es_list <- list()
        label_list <- list()
        
        for (row_idx in seq_len(nrow(combo_grid))) {
          vals_row <- combo_grid[row_idx, ]
          df_sub <- panel
          for (j in seq_along(vals_row)) {
            col_name <- g$filters[[j]]$col
            df_sub <- df_sub %>% dplyr::filter(.data[[col_name]] == vals_row[[j]])
          }
          
          if (outcome %in% names(paired_outcomes)) {
            paired <- c(outcome, paired_outcomes[[outcome]]$partner)
            for (oc in paired) {
              res <- tryCatch(EventStudy(df_sub, oc, method = met, title = "", normalize = norm), error = function(e) NULL)
              if (!is.null(res) && "plot" %in% names(res)) {
                es_list[[length(es_list) + 1]] <- res
                pretty_name <- ifelse(oc %in% names(comm_label_map), comm_label_map[[oc]], oc)
                label_list[[length(label_list) + 1]] <- paste0(g$legend_labels[[row_idx]], " + ", pretty_name)
              }
              if (!is.null(res) && ShouldStoreOutcome(oc)) {
                if (grepl("_predep$", oc) || grepl("_comm$", oc) || grepl("_above$",oc) || grepl("_below$", oc)) {
                  coef_predep_rows <- AppendEstimateRow(coef_predep_rows, res, df_name, met, norm, oc, g$legend_labels[[row_idx]], g, vals_row)
                } else {
                  coef_nondep_rows <- AppendEstimateRow(coef_nondep_rows, res, df_name, met, norm, oc, g$legend_labels[[row_idx]], g, vals_row)
                }
              }
            }
          } else {
            res <- tryCatch(EventStudy(df_sub, outcome, method = met, title = "", normalize = norm), error = function(e) NULL)
            if (!is.null(res) && "plot" %in% names(res)) {
              es_list[[length(es_list) + 1]] <- res
              label_list[[length(label_list) + 1]] <- g$legend_labels[[row_idx]]
            }
            if (!is.null(res) && ShouldStoreOutcome(outcome)) {
              if (grepl("_predep$", outcome) || grepl("_comm$", outcome) || grepl("_above$",outcome) || grepl("_below$", outcome)) {
                coef_predep_rows <- AppendEstimateRow(coef_predep_rows, res, df_name, met, norm, outcome, g$legend_labels[[row_idx]], g, vals_row)
              } else {
                coef_nondep_rows <- AppendEstimateRow(coef_nondep_rows, res, df_name, met, norm, outcome, g$legend_labels[[row_idx]], g, vals_row)
              }
            }
          }
        }
        
        if (outcome %in% names(paired_outcomes)) {
          info <- paired_outcomes[[outcome]]
          this_title <- paste(g$legend_title, "+", info$name)
          for (lvl in g$legend_labels) {
            idx <- vapply(label_list, function(lbl) grepl(lvl, lbl, fixed = TRUE), logical(1))
            if (!any(idx)) next
            out_path <- file.path(output_root,
                                  sprintf("collab/%s%s_%s_%s.png", met, norm_str, outcome, gsub("\\s+", "", lvl)))
            ylim_arg <- switch(outcome,
                               prs_opened_dept_never_comm = comm_nondep_ylim,
                               prs_opened_dept_never_comm_predep = comm_predep_ylim,
                               n_avg_prs_opened_dept_never_comm_predep = comm_predep_ylim,
                               prs_opened_dept_comm_avg_above = comm_predep_ylim,
                               n_avg_prs_opened_dept_comm_avg_above = comm_predep_ylim,
                               NULL)
            SaveCompare(out_path, es_list[idx], label_list[idx], this_title, ylim = ylim_arg, title = "")
          }
        } else {
          out_path <- file.path(output_root, sprintf("collab/%s%s_%s.png", met, norm_str, outcome))
          ylim_arg <- if (outcome %in% c("prs_opened_nondep", "prs_opened_predep","n_avg_prs_opened_predep")) pre_nondep_ylim else NULL
          SaveCompare(out_path, es_list, label_list, g$legend_title, ylim = ylim_arg, title = "")
        }
      }
    }
  }
  
  g <- list(
    filters = list(list(col = "ind_key_collab_2bin", vals = c(1, 0))),
    fname_prefix = "prs_opened_collab_",
    legend_title = "Departed collaborativeness",
    legend_labels = c("High", "Low")
  )
  
  for (norm in c(TRUE)) { 
    norm_str <- ifelse(norm, "_norm", "") 
    for (met in metrics) { 
      for (i in c(0, 1)) { 
        outcome_vec <- c("prs_opened", "total_contributor_count", "prs_opened_nondep", "prs_opened_predep")
        if (df_name == 'df_panel_nyt') {
          outcome_vec <- c(outcome_vec, "avg_prs_opened_nondep", "avg_prs_opened_predep",
                           "n_avg_prs_opened_nondep", "n_avg_prs_opened_predep", 
                           "n_avg_prs_opened_dept_comm", "n_avg_prs_opened_dept_never_comm_predep",
                           names(paired_outcomes))
        }
        
        for (outcome in outcome_vec) {
          combo_grid <- expand.grid(lapply(g$filters, `[[`, "vals"), 
                                    KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
          es_list <- list()
          label_list <- list()
          
          for (row_idx in seq_len(nrow(combo_grid))) {
            vals_row <- combo_grid[row_idx, ]
            df_sub <- panel %>% dplyr::filter(departed_involved_2bin == i)
            for (j in seq_along(vals_row)) {
              col_name <- g$filters[[j]]$col
              df_sub <- df_sub %>% dplyr::filter(.data[[col_name]] == vals_row[[j]])
            }
            
            if (outcome %in% names(paired_outcomes)) {
              paired <- c(outcome, paired_outcomes[[outcome]]$partner)
              for (oc in paired) {
                res <- tryCatch(EventStudy(df_sub, oc, method = met, title = "", normalize = norm), error = function(e) NULL)
                if (!is.null(res) && "plot" %in% names(res)) {
                  es_list[[length(es_list) + 1]] <- res
                  pretty_name <- ifelse(oc %in% names(comm_label_map), comm_label_map[[oc]], oc)
                  label_list[[length(label_list) + 1]] <- paste0(g$legend_labels[[row_idx]], " + ", pretty_name)
                }
                if (!is.null(res) && ShouldStoreOutcome(oc)) {
                  if (grepl("_predep$", oc) || grepl("_comm$", oc)) {
                    #coef_predep_rows <- AppendEstimateRow(coef_predep_rows, res, df_name, met, norm, outcome, g$legend_labels[[row_idx]], g, vals_row)
                  } else {
                    #coef_nondep_rows <- AppendEstimateRow(coef_nondep_rows, res, df_name, met, norm, outcome, g$legend_labels[[row_idx]], g, vals_row)
                  }
                }
              }
            } else {
              res <- tryCatch(EventStudy(df_sub, outcome, method = met, title = "", normalize = norm), error = function(e) NULL)
              if (!is.null(res) && "plot" %in% names(res)) {
                es_list[[length(es_list) + 1]] <- res
                label_list[[length(label_list) + 1]] <- g$legend_labels[[row_idx]]
              }
              if (!is.null(res) && ShouldStoreOutcome(outcome)) {
                if (grepl("_predep$", outcome) || grepl("_comm$", outcome)) {
                  #coef_predep_rows <- AppendEstimateRow(coef_predep_rows, res, df_name, met, norm, outcome, g$legend_labels[[row_idx]], g, vals_row)
                } else {
                  #coef_nondep_rows <- AppendEstimateRow(coef_nondep_rows, res, df_name, met, norm, outcome, g$legend_labels[[row_idx]], g, vals_row)
                }
              }
            }
          }
          
          if (outcome %in% names(paired_outcomes)) {
            info <- paired_outcomes[[outcome]]
            this_title <- paste(g$legend_title, "+", info$name)
            for (k in seq_along(g$legend_labels)) {
              legend_label <- g$legend_labels[[k]]
              label_mask <- vapply(label_list, function(lbl) grepl(legend_label, lbl, fixed = TRUE), logical(1))
              
              es_split <- es_list[label_mask]
              label_split <- label_list[label_mask]
              
              if (length(es_split) > 0) {
                out_path_split <- file.path(output_root, paste0("collab_imp/inv", i, "_", met, norm_str, "_", outcome, "_", gsub("\\s+", "", legend_label), ".png"))
                if (outcome == "n_avg_prs_opened_dept_never_comm_predep") {
                  SaveCompare(out_path_split, es_split, label_split,this_title, ylim = spec_ylim , title = "")
                } else {
                  SaveCompare(out_path_split, es_split, label_split, this_title, ylim = pre_nondep_ylim, title = "")
                }
              }
            }
          } else {
            out_path <- file.path(output_root, paste0("collab_imp/inv", i, "_", met, norm_str, "_", outcome, ".png"))
            SaveCompare(out_path, es_list, label_list, g$legend_title, ylim = pre_nondep_ylim, title = "")
          }
        }
      } 
    } 
  }
  
  
  for (norm in c(TRUE)) { 
    norm_str <- ifelse(norm, "_norm", "") 
    for (met in metrics) { 
      for (i in c(0, 1)) { 
        outcome_vec <- c("prs_opened", "total_contributor_count", "prs_opened_nondep", "prs_opened_predep")
        if (df_name == 'df_panel_nyt') {
          outcome_vec <- c(outcome_vec, "avg_prs_opened_nondep", "avg_prs_opened_predep",
                           "n_avg_prs_opened_nondep", "n_avg_prs_opened_predep", 
                           "n_avg_prs_opened_dept_comm", "n_avg_prs_opened_dept_never_comm_predep",
                           names(paired_outcomes))
        }
        
        for (outcome in outcome_vec) {
          combo_grid <- expand.grid(lapply(g$filters, `[[`, "vals"), 
                                    KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
          es_list <- list()
          label_list <- list()
          
          for (row_idx in seq_len(nrow(combo_grid))) {
            vals_row <- combo_grid[row_idx, ]
            df_sub <- panel %>% dplyr::filter(departed_authored_2bin == i)
            for (j in seq_along(vals_row)) {
              col_name <- g$filters[[j]]$col
              df_sub <- df_sub %>% dplyr::filter(.data[[col_name]] == vals_row[[j]])
            }
            
            if (outcome %in% names(paired_outcomes)) {
              paired <- c(outcome, paired_outcomes[[outcome]]$partner)
              for (oc in paired) {
                res <- tryCatch(EventStudy(df_sub, oc, method = met, title = "", normalize = norm), error = function(e) NULL)
                if (!is.null(res) && "plot" %in% names(res)) {
                  es_list[[length(es_list) + 1]] <- res
                  pretty_name <- ifelse(oc %in% names(comm_label_map), comm_label_map[[oc]], oc)
                  label_list[[length(label_list) + 1]] <- paste0(g$legend_labels[[row_idx]], " + ", pretty_name)
                }
                if (!is.null(res) && ShouldStoreOutcome(oc)) {
                  if (grepl("_predep$", oc) || grepl("_comm$", oc)) {
                    #coef_predep_rows <- AppendEstimateRow(coef_predep_rows, res, df_name, met, norm, outcome, g$legend_labels[[row_idx]], g, vals_row)
                  } else {
                    #coef_nondep_rows <- AppendEstimateRow(coef_nondep_rows, res, df_name, met, norm, outcome, g$legend_labels[[row_idx]], g, vals_row)
                  }
                }
              }
            } else {
              res <- tryCatch(EventStudy(df_sub, outcome, method = met, title = "", normalize = norm), error = function(e) NULL)
              if (!is.null(res) && "plot" %in% names(res)) {
                es_list[[length(es_list) + 1]] <- res
                label_list[[length(label_list) + 1]] <- g$legend_labels[[row_idx]]
              }
              if (!is.null(res) && ShouldStoreOutcome(outcome)) {
                if (grepl("_predep$", outcome) || grepl("_comm$", outcome)) {
                  #coef_predep_rows <- AppendEstimateRow(coef_predep_rows, res, df_name, met, norm, outcome, g$legend_labels[[row_idx]], g, vals_row)
                } else {
                  #coef_nondep_rows <- AppendEstimateRow(coef_nondep_rows, res, df_name, met, norm, outcome, g$legend_labels[[row_idx]], g, vals_row)
                }
              }
            }
          }
          
          if (outcome %in% names(paired_outcomes)) {
            info <- paired_outcomes[[outcome]]
            this_title <- paste(g$legend_title, "+", info$name)
            for (k in seq_along(g$legend_labels)) {
              legend_label <- g$legend_labels[[k]]
              label_mask <- vapply(label_list, function(lbl) grepl(legend_label, lbl, fixed = TRUE), logical(1))
              
              es_split <- es_list[label_mask]
              label_split <- label_list[label_mask]
              
              if (length(es_split) > 0) {
                out_path_split <- file.path(output_root, paste0("collab_imp/pr_inv", i, "_", met, norm_str, "_", outcome, "_", gsub("\\s+", "", legend_label), ".png"))
                if (outcome == "n_avg_prs_opened_dept_never_comm_predep") {
                  SaveCompare(out_path_split, es_split, label_split,this_title, ylim = spec_ylim , title = "")
                } else {
                  SaveCompare(out_path_split, es_split, label_split, this_title, ylim = pre_nondep_ylim, title = "")
                }
              }
            }
          } else {
            out_path <- file.path(output_root, paste0("collab_imp/pr_inv", i, "_", met, norm_str, "_", outcome, ".png"))
            SaveCompare(out_path, es_list, label_list, g$legend_title, ylim = pre_nondep_ylim, title = "")
          }
        }
      } 
    } 
  }
  
  coef_predep_df <- dplyr::bind_rows(coef_predep_rows)
  coef_nondep_df <- dplyr::bind_rows(coef_nondep_rows)
}


CalculateEstimateDifference <- function(df, suffix = "") {
  df %>%
    filter(event_time > 0 & event_time < 5) %>%
    select(event_time, ind_key_collab_2bin, estimate) %>%
    pivot_wider(
      names_from = ind_key_collab_2bin,
      values_from = estimate,
      names_prefix = paste0("estimate_", suffix, "_")
    ) %>%
    mutate(!!paste0("estimate_difference_", suffix) :=
             .[[paste0("estimate_", suffix, "_1")]] -
             .[[paste0("estimate_", suffix, "_0")]])
}

collab_diff_df <- CalculateEstimateDifference(coef_nondep_df %>% filter(outcome == "prs_opened"))
predep_collab_diff_df <- CalculateEstimateDifference(coef_predep_df %>% filter(outcome == "prs_opened_predep"),"predep")
collab_diff_df_merged <- collab_diff_df %>% 
  left_join(predep_collab_diff_df) %>%
  mutate(predep_ratio = estimate_difference_predep/estimate_difference_)
print(paste("average treatment effect difference predep", mean(collab_diff_df_merged$estimate_difference_predep)))
print(paste("average treatment effect difference predep ratio", mean(collab_diff_df_merged$predep_ratio)))

nondep_collab_diff_df <- CalculateEstimateDifference(coef_nondep_df %>% filter(outcome == "prs_opened_nondep"),"nondep")
collab_diff_df_merged <- collab_diff_df_merged %>% 
  left_join(nondep_collab_diff_df) %>%
  mutate(nondep_ratio = estimate_difference_nondep/estimate_difference_)
print(paste("average treatment effect difference nondep", mean(collab_diff_df_merged$estimate_difference_nondep)))
print(paste("average treatment effect difference nondep ratio", mean(collab_diff_df_merged$nondep_ratio)))


library(dplyr)
library(tidyr)
library(ggplot2)

NormalizeBreakSpecs <- function(break_specs, default_disp = 1.5) {
  specs <- lapply(break_specs, function(v) {
    v <- as.numeric(v)
    if (length(v) == 2) list(low = v[1], high = v[2], disp = default_disp) else list(low = v[1], high = v[2], disp = v[3])
  })
  specs[order(vapply(specs, function(b) b$low, numeric(1)))]
}

MapYCompressed <- function(y, specs) {
  s <- 0
  for (br in specs) {
    gap <- (br$high - br$low) - br$disp
    if (y < br$low) return(y - s)  # exclusive low
    if (y <= br$high) return(br$low - s + (y - br$low) * (br$disp / (br$high - br$low)))  # inclusive high
    s <- s + gap
  }
  y - s
}

DrawAxisWithSevenStrokeJags <- function(p, y_limits, specs, axis_x = 0.5, axis_size = 1.0, jag_amp_x = 0.12, n_strokes = 7, y_offset = 0) {
  segs <- BuildSegments(y_limits, specs)
  for (seg in segs) {
    y0 <- MapYCompressed(seg[1], specs) - y_offset
    y1 <- MapYCompressed(seg[2], specs) - y_offset
    p <- p + annotate("segment", x = axis_x, xend = axis_x, y = y0, yend = y1, colour = "black", linewidth = axis_size)
  }
  for (br in specs) {
    y_bot <- MapYCompressed(br$low, specs) - y_offset
    y_top <- y_bot + br$disp
    h <- (y_top - y_bot) / n_strokes
    for (i in seq_len(n_strokes)) {
      x0 <- if (i %% 2 == 1) axis_x else axis_x - jag_amp_x
      x1 <- if (i %% 2 == 1) axis_x - jag_amp_x else axis_x + jag_amp_x
      y0 <- y_bot + (i - 1) * h
      y1 <- y_bot + i * h
      p <- p + annotate("segment", x = x0, xend = x1, y = y0, yend = y1, colour = "black", linewidth = axis_size)
    }
  }
  p
}

DrawPanelBoxNoLeft <- function(p, y_limits, specs, n_x, box_size = 0.8, y_offset = 0) {
  x_min <- 0.5; x_max <- n_x + 0.5
  y_min <- MapYCompressed(y_limits[1], specs) - y_offset
  y_max <- MapYCompressed(y_limits[2], specs) - y_offset
  p +
    annotate("segment", x = x_min, xend = x_max, y = y_min, yend = y_min, colour = "black", linewidth = box_size) +
    annotate("segment", x = x_max, xend = x_max, y = y_min, yend = y_max, colour = "black", linewidth = box_size) +
    annotate("segment", x = x_min, xend = x_max, y = y_max, yend = y_max, colour = "black", linewidth = box_size)
}



BuildSegments <- function(y_limits, specs) {
  segs <- list(); cur <- y_limits[1]
  if (length(specs)) for (br in specs) { if (is.finite(cur) && is.finite(br$low) && br$low > cur) segs[[length(segs)+1]] <- c(cur, br$low); cur <- br$high }
  if (is.finite(cur) && is.finite(y_limits[2]) && y_limits[2] > cur) segs[[length(segs)+1]] <- c(cur, y_limits[2])
  Filter(function(s) length(s)==2 && is.finite(s[1]) && is.finite(s[2]) && s[2]>s[1], segs)
}

BuildTicksCompressed <- function(y_limits, specs, step = 0.5) {
  segs <- BuildSegments(y_limits, specs)
  tick_df <- do.call(rbind, lapply(segs, function(seg) {
    vals <- seq(seg[1], seg[2], by = step)
    data.frame(breaks = vapply(vals, MapYCompressed, numeric(1), specs = specs), labels = vals)
  }))
  tick_df <- tick_df[!duplicated(round(tick_df$breaks, 10)), ]
  list(breaks = tick_df$breaks, labels = tick_df$labels)
}
BuildDualRatioPlotFromCoefs <- function(coef_df,
                                        metric,
                                        normalize = TRUE,
                                        outcome_num_a,
                                        outcome_den_a,
                                        outcome_num_b,
                                        outcome_den_b,
                                        label = c("High"),
                                        type_labels = c("DeptComm","NeverCommPredep"),
                                        legend_title = "Ratio comparison",
                                        y_limits = c(-6, 16),
                                        break_specs,
                                        step = 1,
                                        axis_size = 1.0,
                                        jag_amp_x = 0.12,
                                        bar_alpha = 0.6,
                                        contrast = c("ratio","difference"),
                                        y_label = NULL,
                                        dashed_ref = NULL) {
  contrast <- match.arg(contrast)
  specs <- NormalizeBreakSpecs(break_specs, default_disp = 1.5)
  
  BuildContrastDf <- function(outcome_num, outcome_den, contrast_type) {
    base <- coef_df %>%
      dplyr::filter(metric == !!metric,
                    normalize == !!normalize,
                    outcome %in% c(outcome_num, outcome_den),
                    legend_label %in% label) %>%
      dplyr::mutate(event_time = suppressWarnings(as.numeric(event_time)),
                    estimate = suppressWarnings(as.numeric(estimate))) %>%
      dplyr::group_by(event_time, outcome) %>%
      dplyr::summarise(estimate = mean(estimate, na.rm = TRUE), .groups = "drop") %>%
      tidyr::pivot_wider(names_from = outcome, values_from = estimate) %>%
      dplyr::filter(event_time %in% -4:4)
    
    numer <- base[[outcome_num]]
    denom <- base[[outcome_den]]
    if (contrast == "ratio") {
      out <- as.numeric(numer) / as.numeric(denom) * sign(denom)
      out[!is.finite(out)] <- NA_real_
    } else {
      out <- as.numeric(numer) - as.numeric(denom)
    }
    
    dplyr::mutate(base, contrast = out, contrast_type = contrast_type)
  }
  
  df_a <- BuildContrastDf(outcome_num_a, outcome_den_a, type_labels[1])
  df_b <- BuildContrastDf(outcome_num_b, outcome_den_b, type_labels[2])
  plot_df <- dplyr::bind_rows(df_a, df_b) |> dplyr::filter(is.finite(contrast))
  print(plot_df)
  plot_df <- dplyr::mutate(plot_df,
                           contrast_shifted = vapply(contrast, MapYCompressed, numeric(1),
                                                     specs = specs))
  y0 <- MapYCompressed(0, specs)
  plot_df <- dplyr::mutate(plot_df, y_draw = contrast_shifted - y0)
  
  ticks <- BuildTicksCompressed(y_limits, specs, step = step)
  ticks$breaks <- ticks$breaks - y0
  y_lim_draw <- c(MapYCompressed(y_limits[1], specs) - y0,
                  MapYCompressed(y_limits[2], specs) - y0)
  
  if (is.null(dashed_ref)) {
    dashed_ref <- if (contrast == "ratio") 1 else 0
  }
  dashed_y_draw <- if (is.finite(dashed_ref)) MapYCompressed(dashed_ref, specs) - y0 else NA_real_
  dashed_y_draw + MapYCompressed(-dashed_ref, specs) - y0
  if (is.null(y_label)) {
    y_label <- if (contrast == "ratio") {
      "Negative effect of decline in average"
    } else {
      "Difference in effect driven by changes in average"
    }
  }
  
  x_levels <- as.character(-4:4)
  n_x <- length(x_levels)
  
  p <- ggplot2::ggplot(
    plot_df,
    ggplot2::aes(x = factor(event_time, levels = x_levels, ordered = TRUE),
                 y = y_draw, fill = contrast_type)
  ) +
    ggplot2::geom_col(position = ggplot2::position_dodge(width = 0.8),
                      width = 0.72, alpha = bar_alpha, na.rm = TRUE) +
    ggplot2::geom_hline(yintercept = 0, linewidth = 0.4) +
    ggplot2::scale_x_discrete(drop = FALSE, limits = x_levels,
                              expand = ggplot2::expansion(mult = c(0, 0))) +
    ggplot2::scale_y_continuous(limits = y_lim_draw, breaks = ticks$breaks,
                                labels = ticks$labels,
                                expand = ggplot2::expansion(mult = c(0, 0))) +
    ggplot2::scale_fill_manual(values = setNames(c("black", "red"), type_labels),
                               breaks = type_labels, drop = FALSE) +
    ggplot2::guides(fill = ggplot2::guide_legend(nrow = 1, byrow = TRUE,
                                                 title.position = "top")) +
    ggplot2::labs(x = "Event time (k)", y = y_label, fill = legend_title) +
    ggplot2::coord_cartesian(clip = "off") +
    ggplot2::theme_minimal(base_size = 11) +
    ggplot2::theme(
      aspect.ratio = 1,
      panel.grid.major = ggplot2::element_blank(),
      panel.grid.minor = ggplot2::element_blank(),
      panel.background = ggplot2::element_rect(fill = "white", colour = NA),
      plot.background = ggplot2::element_rect(fill = "white", colour = NA),
      axis.text.x = ggplot2::element_text(size = 10),
      axis.text.y = ggplot2::element_text(size = 10),
      axis.title.x = ggplot2::element_text(size = 11,
                                           margin = ggplot2::margin(t = 6)),
      axis.title.y = ggplot2::element_text(size = 11,
                                           margin = ggplot2::margin(r = 6)),
      axis.ticks.y = ggplot2::element_line(colour = "black"),
      axis.ticks.x = ggplot2::element_line(colour = "black"),
      axis.line.y = ggplot2::element_blank(),
      legend.position = c(0.5, 0.98),
      legend.justification = c(0.5, 1.0),
      legend.direction = "horizontal",
      legend.background = ggplot2::element_rect(fill = "white", colour = NA),
      legend.box.background = ggplot2::element_rect(fill = "white",
                                                    colour = "grey40",
                                                    linewidth = 0.8),
      legend.margin = ggplot2::margin(2, 6, 2, 6),
      legend.box.margin = ggplot2::margin(0, 0, 0, 0),
      plot.margin = ggplot2::margin(20, 20, 26, 40)
    )
  
  if (is.finite(dashed_y_draw)) {
    p <- p + ggplot2::geom_hline(yintercept = dashed_y_draw,
                                 linetype = "dashed", linewidth = 0.5)
  }
  
  p <- DrawAxisWithSevenStrokeJags(p, y_limits, specs, axis_x = 0.5,
                                   axis_size = axis_size, jag_amp_x = jag_amp_x,
                                   n_strokes = 7, y_offset = y0)
  p <- DrawPanelBoxNoLeft(p, y_limits, specs, n_x, box_size = 0.8, y_offset = y0)
  p
}

p_high_ratio <- BuildDualRatioPlotFromCoefs(
  coef_df = coef_predep_df,
  metric = "cs",
  normalize = TRUE,
  outcome_num_b = "n_avg_prs_opened_dept_comm",
  outcome_den_b = "prs_opened_dept_comm",
  outcome_num_a = "n_avg_prs_opened_dept_never_comm_predep",
  outcome_den_a = "prs_opened_dept_never_comm_predep",
  label = "High",
  type_labels = c("No","Yes"),
  legend_title = "Departed collaborativeness + Communicated w/ Departed",
  y_limits = c(-4, 6),
  break_specs = NULL,
  step = 1,
  axis_size = 1.0,
  jag_amp_x = 0.12,
  contrast = "ratio",
  y_label = "Proportion in effect driven by changes in average",
  dashed_ref = -1
)
p_high_ratio <- p_high_ratio + theme(aspect.ratio = NULL)
ggsave(file.path(output_root, sprintf("ratio_high_dept_vs_never_comm_predep_%s_norm.png", "cs")),
       plot = p_high_ratio, width = 960, height = 320, units = "px", dpi = 72)

p_low_ratio <- BuildDualRatioPlotFromCoefs(
  coef_df = coef_predep_df,
  metric = "cs",
  normalize = TRUE,
  outcome_num_b = "n_avg_prs_opened_dept_comm",
  outcome_den_b = "prs_opened_dept_comm",
  outcome_num_a = "n_avg_prs_opened_dept_never_comm_predep",
  outcome_den_a = "prs_opened_dept_never_comm_predep",
  label = "Low",
  type_labels = c("Low + No","Low + Yes"),
  legend_title = "Departed collaborativeness + Communicated w/ Departed",
  y_limits = c(-354, 5),
  break_specs = list(c(-352, -2)),
  step = 1,
  axis_size = 1.0,
  jag_amp_x = 0.12,
  contrast = "ratio",
  y_label = "Proportion in effect driven by changes in average",
  dashed_ref = -1
)
p_low_ratio <- p_low_ratio + theme(aspect.ratio = NULL)
ggsave(file.path(output_root, sprintf("ratio_low_dept_vs_never_comm_predep_%s_norm.png", "cs")),
       plot = p_low_ratio, width = 960, height = 320, units = "px", dpi = 72)

p_high_ratio_comm_int <- BuildDualRatioPlotFromCoefs(
  coef_df = coef_predep_df,
  metric = "cs",
  normalize = TRUE,
  outcome_num_a = "n_avg_prs_opened_dept_comm_avg_above",
  outcome_den_a = "prs_opened_dept_comm_avg_above",
  outcome_num_b = "n_avg_prs_opened_dept_comm_avg_below",
  outcome_den_b = "prs_opened_dept_comm_avg_below",
  label = "High",
  type_labels = c("High + Above average","High + Below average"),
  legend_title = "Departed collaborativeness + Communication intensity",
  y_limits = c(-3, 11),
  break_specs = list(c(5, 8)),
  step = 1,
  axis_size = 1.0,
  jag_amp_x = 0.12,
  contrast = "ratio",
  y_label = "Proportion in effect driven by changes in average",
  dashed_ref = -1
)
p_high_ratio_comm_int <- p_high_ratio_comm_int + theme(aspect.ratio = NULL)
ggsave(file.path(output_root, sprintf("ratio_high_comm_int_%s_norm.png", "cs")),
       plot = p_high_ratio_comm_int, width = 960, height = 320, units = "px", dpi = 72)

p_low_ratio_comm_int <- BuildDualRatioPlotFromCoefs(
  coef_df = coef_predep_df,
  metric = "cs",
  normalize = TRUE,
  outcome_num_a = "n_avg_prs_opened_dept_comm_avg_above",
  outcome_den_a = "prs_opened_dept_comm_avg_above",
  outcome_num_b = "n_avg_prs_opened_dept_comm_avg_below",
  outcome_den_b = "prs_opened_dept_comm_avg_below",
  label = "Low",
  type_labels = c("Low + Above average","Low + Below average"),
  legend_title = "Departed collaborativeness + Communicated intensity",
  y_limits = c(-10,4),
  break_specs = list(c(-9,-4)),
  step = 1,
  axis_size = 1.0,
  jag_amp_x = 0.12,
  contrast = "ratio",
  y_label = "Proportion in effect driven by changes in average",
  dashed_ref = -1
)
p_low_ratio_comm_int <- p_low_ratio_comm_int + theme(aspect.ratio = NULL)
ggsave(file.path(output_root, sprintf("ratio_low_comm_int_%s_norm.png", "cs")),
       plot = p_low_ratio_comm_int, width = 960, height = 320, units = "px", dpi = 72)

BuildRatioPlotFromCoefs <- function(coef_df,
                                    metric,
                                    normalize = TRUE,
                                    outcome_num = "n_avg_prs_opened_predep",
                                    outcome_den = "prs_opened_predep",
                                    labels = c("High","Low"),
                                    legend_title = "Departed collaborativeness",
                                    y_limits = c(-3.5, 20),
                                    break_specs,
                                    step = 0.5,
                                    axis_size = 1.0,
                                    jag_amp_x = 0.12,
                                    bar_alpha = 0.6) {
  specs <- NormalizeBreakSpecs(break_specs, default_disp = 1.5)
  
  plot_df <- coef_df %>%
    filter(metric == !!metric, normalize == !!normalize, outcome %in% c(outcome_num, outcome_den), legend_label %in% labels) %>%
    mutate(event_time = suppressWarnings(as.numeric(event_time)), estimate = suppressWarnings(as.numeric(estimate))) %>%
    group_by(legend_label, event_time, outcome) %>% summarise(estimate = mean(estimate, na.rm = TRUE), .groups = "drop") %>%
    pivot_wider(names_from = outcome, values_from = estimate) %>%
    filter(event_time %in% -4:4) %>%
    mutate(ratio = { numer <- .data[[outcome_num]]; denom <- .data[[outcome_den]]; out <- as.numeric(numer) / as.numeric(denom) * sign(denom);  out[!is.finite(out)] <- NA_real_; out },
           ratio_shifted = vapply(ratio, MapYCompressed, numeric(1), specs = specs))
  
  print(plot_df)
  ticks <- BuildTicksCompressed(y_limits, specs, step = step)
  dashed_y_shifted <- MapYCompressed(-1, specs)
  
  x_levels <- as.character(-4:4)
  n_x <- length(x_levels)
  
  p <- ggplot(plot_df, aes(x = factor(event_time, levels = x_levels, ordered = TRUE), y = ratio_shifted, fill = legend_label)) +
    geom_col(position = position_dodge(width = 0.8), width = 0.72, alpha = bar_alpha) +
    geom_hline(yintercept = MapYCompressed(0, specs), linewidth = 0.4) +
    geom_hline(yintercept = dashed_y_shifted, linetype = "dashed", linewidth = 0.5) +
    scale_x_discrete(drop = FALSE, limits = x_levels, expand = expansion(mult = c(0, 0))) +
    scale_y_continuous(limits = c(MapYCompressed(y_limits[1], specs), MapYCompressed(y_limits[2], specs)),
                       breaks = ticks$breaks, labels = ticks$labels, expand = expansion(mult = c(0, 0))) +
    scale_fill_manual(values = c("High" = "black", "Low" = "red"), breaks = labels, drop = FALSE) +
    guides(fill = guide_legend(nrow = 1, byrow = TRUE, title.position = "top")) +
    labs(x = "Event time (k)", y = "Proportion of effect driven by changes in average", fill = legend_title) +
    theme_minimal(base_size = 11) +
    theme(
      aspect.ratio = 1,
      panel.grid.major = element_blank(),
      panel.grid.minor = element_blank(),
      panel.background = element_rect(fill = "white", colour = NA),
      plot.background = element_rect(fill = "white", colour = NA),
      axis.text.x = element_text(size = 10),
      axis.text.y = element_text(size = 10),
      axis.title.x = element_text(size = 11, margin = margin(t = 6)),
      axis.title.y = element_text(size = 11, margin = margin(r = 6)),
      axis.ticks.y = element_line(colour = "black"),
      axis.ticks.x = element_line(colour = "black"),
      axis.line.y = element_blank(),
      legend.position = c(0.5, 0.98),
      legend.justification = c(0.5, 1.0),
      legend.direction = "horizontal",
      legend.background = element_rect(fill = "white", colour = NA),
      legend.box.background = element_rect(fill = "white", colour = "grey40", linewidth = 0.8),
      legend.margin = margin(2, 6, 2, 6),
      legend.box.margin = margin(0, 0, 0, 0),
      plot.margin = margin(20, 20, 26, 40) # aligns top/bottom with ES when saved 480x480
    )
  
  p <- DrawAxisWithSevenStrokeJags(p, y_limits, specs, axis_x = 0.5, axis_size = axis_size, jag_amp_x = jag_amp_x, n_strokes = 7)
  p <- DrawPanelBoxNoLeft(p, y_limits, specs, n_x, box_size = 0.8)
  p
}

p_predep <- BuildRatioPlotFromCoefs(
  coef_df = coef_predep_df,
  metric = "cs",
  normalize = TRUE,
  outcome_num = "n_avg_prs_opened_predep",
  outcome_den = "prs_opened_predep",
  labels = c("High","Low"),
  y_limits = c(-4, 7),
  break_specs = NULL,
  step = 1,
  axis_size = 1.0,
  jag_amp_x = 0.12
)

p_predep <- p_predep + theme(aspect.ratio = NULL)

ggsave(file.path(output_root, sprintf("ratio_predep_%s_norm.png", "cs")),
       plot = p_predep, width = 960, height = 320, units = "px", dpi = 72)


p_nondep <- BuildRatioPlotFromCoefs(
  coef_df = coef_nondep_df,
  metric = "cs",
  normalize = TRUE,
  outcome_num = "n_avg_prs_opened_nondep",
  outcome_den = "prs_opened_nondep",
  labels = c("High","Low"),
  y_limits = c(-3, 14),
  break_specs = list(c(6, 13)),
  step = 1,
  axis_size = 1.0,
  jag_amp_x = 0.12
)

p_nondep <- p_nondep + theme(aspect.ratio = NULL)

ggsave(file.path(output_root, sprintf("ratio_nondep_%s_norm.png", "cs")),
       plot = p_nondep, width = 960, height = 320, units = "px", dpi = 72)

