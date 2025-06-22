library(did)
library(tidyverse)
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

NormalizeOutcome <- function(df, outcome) {
  outcome_norm <- paste(outcome, "norm", sep = "_")
  df_norm <- df %>%
    group_by(repo_name) %>%
    mutate(mean_outcome = mean(get(outcome)[ time_index < treatment_group & 
                                               time_index >= (treatment_group - 5)], na.rm = TRUE)) %>%
    ungroup()
  df_norm[[outcome_norm]] <- df_norm[[outcome]] / df_norm$mean_outcome
  df_norm
}


EventStudy <- function(df, outcome, method = c("bjs", "sa", "cs", "2s"), normalize = FALSE, title = NULL) {
  method <- match.arg(method)
  df_est <- if (normalize) NormalizeOutcome(df, outcome) else df %>% mutate("{outcome}_norm" := .data[[outcome]])
  yvar <- paste0(outcome, "_norm")
  
  # compute coefficients & std errors
  res_mat <- switch(method,
                    bjs = {
                      est <- did_imputation(df_est, yname = yvar, gname = "treatment_group", 
                                            tname = "time_index", idname = "repo_name", horizon = T, pretrends = -5:-1) %>% 
                        rename(sd = std.error, ci_low = conf.low, ci_high = conf.high) %>%
                        select(term, estimate, sd, ci_low, ci_high) %>%
                        drop_na() %>%
                        mutate(term = as.numeric(term)) %>%
                        column_to_rownames("term") %>% 
                        as.matrix() 
                      est
                    },
                    sa = {
                      est_formula <- as.formula(sprintf("%s ~ sunab(treatment_group, time_index) | repo_name + time_index",
                                                        yvar))
                      est_obj <- feols(est_formula, df_est, vcov = ~ repo_name)
                      CleanFixEst(est_obj, "time_index::")
                    },
                    cs = {
                      att <- att_gt(yname = yvar, tname = "time_index", idname = "repo_name_id",
                                    gname = "treatment_group", xformla = ~1, data = df_est,
                                    control_group = "notyettreated", allow_unbalanced_panel = TRUE,
                                    clustervars = "repo_name")
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
                                    first_stage  = ~ 1 | repo_name + time_index,
                                    second_stage = ~ i(rel_time, ref = -1),
                                    treatment    = "treatment", cluster_var = "repo_name")
                      CleanFixEst(es2s_obj, "rel_time::")
                    }
  )
  res_mat <- EnsureMinusOneRow(res_mat)
  res_mat <- res_mat[order(as.numeric(rownames(res_mat))), , drop = FALSE]
  plot_fun <- fixest::coefplot
  plot_obj <- plot_fun(res_mat,
                       main     = title,
                       xlab     = "Time to treatment",
                       keep     = "^-[1-4]|[0-4]",
                       drop     = "[[:digit:]]{2}",
                       ref.line = 0)
  list(plot = plot_obj, results = res_mat)
}

CleanFixEst <- function(est_obj, term_prefix) {
  cleaned_tbl <- est_obj %>% 
    tidy() %>%
    mutate(term = as.numeric(str_remove(term, paste0("^", term_prefix))),
           sd = std.error) %>%
    select(-std.error, -statistic, -p.value) %>%
    mutate(ci_low  = estimate - 1.96 * sd, 
           ci_high = estimate + 1.96 * sd) %>%
    column_to_rownames("term") %>%
    as.matrix()
  return(cleaned_tbl)
}

EnsureMinusOneRow <- function(est_mat) {
  if (!("-1" %in% rownames(est_mat))) {
    blank_row <- matrix(
      c(0,          # estimate
        0,   # sd
        0,   # ci_low
        0),  # ci_high
      nrow = 1
    )
    colnames(blank_row) <- colnames(est_mat)
    rownames(blank_row) <- "-1"
    est_mat <- rbind(est_mat, blank_row)
  }
  est_mat
}
CompareES <- function(..., collab_type, legend_labels, file_path = NULL) {
  es_list <- list(...)
  results  <- lapply(es_list, `[[`, "results")
  plot_fn  <- fixest::coefplot
  
  if (!is.null(file_path)) {
    png(filename = file_path)
  }
  
  plot_fn(
    results,
    xlab     = "Time to treatment",
    keep     = "^-[1-5]|[0-5]",
    drop     = "[[:digit:]]{2}",
    ref.line = 0
  )
  
  n <- length(results)
  legend_labels <- if (n == 2) {
    c(
      paste(collab_type, "Collaborative"),
      paste(collab_type, "Uncollaborative")
    )
  } else if (n == 3) {
    c(
      paste(collab_type, "Most Collaborative"),
      paste(collab_type, "Moderately Collaborative"),
      paste(collab_type, "Least Collaborative")
    )
  } else {
    stopifnot(length(legend_labels) == n)
    legend_labels
  }
  
  legend(
    "topright",
    col    = seq_len(n),
    pch    = 20,
    lwd    = 1,
    lty    = seq_len(n),
    legend = legend_labels
  )
  
  if (!is.null(file_path)) {
    dev.off()
  }
}

RemoveOutliers <- function(df_panel_nyt, col, lower = 0.01, upper = 0.99) {
  mean_prs_df <- df_panel_nyt %>%
    group_by(repo_name) %>%
    summarise(
      mean_outcome = mean({{ col }}, na.rm = TRUE),
      .groups = "drop"
    )
  
  quantile_values <- quantile(
    mean_prs_df$mean_outcome,
    probs = c(lower, upper),
    na.rm = TRUE
  )
  nonextreme_repos <- mean_prs_df %>%
    filter(mean_outcome %in% quantile_values) %>%
    pull(repo_name)
  df_panel_nyt %>% filter(repo_name %in% nonextreme_repos)
}


df_panel_nyt <- df_panel_nyt %>%
  mutate(repo_name_id = as.integer(factor(repo_name, levels = sort(unique(repo_name)))))

group_defs <- list(
  list(name = "Departed", col = "ind_key_collab_2bin", bins = c(1, 0)),
  list(name = "Departed", col = "ind_key_collab_3bin", bins = c(2, 1, 0)),
  list(name = "Other",   col = "ind_other_collab_2bin", bins = c(1, 0)),
  list(name = "Other",   col = "ind_other_collab_3bin", bins = c(2, 1, 0))
)
metrics <- c("sa", "cs", "2s", "bjs") # BJS HAS KNOWN ISSUE
metrics_fn <- c("Sun and Abraham 2020", "Callaway and Sant'Anna 2020", "Gardner 2021", "Borusyak et. al 2024")

modes <- list(
  list(normalize = FALSE, file = "issue/output/prs_opened.png"),
  list(normalize = TRUE,  file = "issue/output/prs_opened_norm.png")
)

for (mode in modes) {
    es_list <- lapply(metrics, function(m) {
    EventStudy(df_panel_nyt, "prs_opened", m, title = "", normalize = mode$normalize)})
  
  do.call(CompareES, c(es_list, list(collab_type = "", legend_labels = metrics_fn, file_path = mode$file)))
}

png("issue/output/naive_prs_opened.png")
df_panel_nyt %>% mutate(relative_time = time_index - treatment_group) %>% group_by(relative_time) %>% 
  summarize(mean(prs_opened)) %>% filter(abs(relative_time) <= 5) %>% plot()
dev.off()

for (norm in c(TRUE, FALSE)) {
  for (m in metrics[[2]]) {
    norm_str <- ifelse(norm, "_norm", "")
    png(paste0("issue/output/prs_opened_collab_",m,".png"))
    for (g in list(group_defs[[1]])) {
      subsets <- lapply(g$bins, function(b) {
        RemoveOutliers(df_panel_nyt, "prs_opened") %>% filter(.data[[g$col]] == b)
      })
      
      es_list <- lapply(subsets, function(df_sub) {
        EventStudy(df_sub, "prs_opened", m, title = "", normalize = norm)
      })
      
      do.call(
        CompareES,
        c(es_list, list(collab_type = g$name))
      )
    }
    dev.off()
  }
}

