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


EventStudy <- function(df, outcome, method = c("cs", "2s", "bjs", "es"), normalize = FALSE, title = NULL) {
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
                    cs = {
                      att <- att_gt(yname = yvar, tname = "time_index", idname = "repo_name_id",
                                    gname = "treatment_group", xformla = ~1, data = df_est,
                                    control_group = "notyettreated", allow_unbalanced_panel = T,
                                    clustervars = "repo_name", est_method =  "dr")
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
                          str_detect(term, "lead")    ~ -(as.numeric(str_extract(term, "\\d+$")) + 1),
                          str_detect(term, "lag")     ~  as.numeric(str_extract(term, "\\d+$")),
                          TRUE                         ~ 0
                        )) %>%
                        column_to_rownames("term") %>%
                        as.matrix()
                      est
                    }
  )
  res_mat <- EnsureMinusOneRow(res_mat)
  res_mat <- res_mat[order(as.numeric(rownames(res_mat))), , drop = FALSE]
  plot_fun <- fixest::coefplot
  plot_obj <- plot_fun(res_mat,
                       main     = title,
                       xlab     = "Time to treatment",
                       keep     = "^-[1-5]|[0-5]",
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
    blank_row <- matrix(c(0, 0, 0, 0), nrow = 1)
    colnames(blank_row) <- colnames(est_mat)
    rownames(blank_row) <- "-1"
    est_mat <- rbind(est_mat, blank_row)
  }
  est_mat
}
CompareES <- function(..., legend_labels) {
  es_list <- list(...)
  results  <- lapply(es_list, `[[`, "results")
  plot_fn  <- fixest::coefplot
  
  plot_fn(results, xlab = "Time to treatment", keep = "^-[1-5]|[0-5]", drop = "[[:digit:]]{2}")
  
  stopifnot(length(legend_labels) == length(results))
  legend("topleft", col = seq_along(results), pch = 20,
         lwd = 1, lty = seq_along(results), legend = legend_labels)
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
  list(filters = list(list(col = "ind_key_collab_2bin", vals = c(1, 0))),
       fname_prefix  = "prs_opened_collab_",
       legend_labels = c("Dept. Collaborative", "Dept. Uncollaborative")),
  list(filters = list(list(col = "ind_key_collab_2bin", vals = c(1, 0)),
                      list(col = "departed_involved_2bin", vals = c(1, 0))),
       fname_prefix  = "prs_opened_collab_involved_",
       legend_labels = c("Dept. Collaborative + Involved", "Dept. Uncollaborative + Involved",
                         "Dept. Collaborative + Uninvolved", "Dept. Uncollaborative + Uninvolved")),
  list(filters = list(list(col = "ind_key_collab_2bin", vals = c(1, 0)),
                      list(col = "departed_involved_3bin", vals = c(2, 1, 0))),
       fname_prefix  = "prs_opened_collab_involved_3b_",
       legend_labels = c("Dept. Collaborative + Involved", "Dept. Uncollaborative + Involved",
                         "Dept. Collaborative + Less involved", "Dept. Uncollaborative + Less involved",
                         "Dept. Collaborative + Uninvolved", "Dept. Uncollaborative + Uninvolved")),
  list(filters = list(list(col = "departed_involved_2bin", vals = c(1, 0))),
       fname_prefix  = "prs_opened_involved_",
       legend_labels = c("Dept. Involved", "Dept. Uninvolved")),
  list(filters = list(list(col = "departed_involved_3bin", vals = c(2, 1, 0))),
     fname_prefix  = "prs_opened_involved_3b_",
     legend_labels = c("Dept. Involved", "Dept. Less involved","Dept. Uninvolved")),
  list(filters = list(list(col = "ind_key_collab_2bin", vals = c(1, 0)),
                         list(col = "key_contributor_count_2bin", vals = c(1, 0))),
          fname_prefix  = "prs_opened_collab_key_",
          legend_labels = c("Dept. Collaborative + Many Key", "Dept. Uncollaborative + Many Key",
                            "Dept. Collaborative + Few Key", "Dept. Uncollaborative + Few Key")),
  list(filters = list(list(col = "key_contributor_count_2bin", vals = c(1, 0))),
       fname_prefix  = "prs_opened_key_",
       legend_labels = c("Many Key", "Few Key")),
  list(filters = list(list(col = "ind_key_collab_2bin", vals = c(1, 0)),
                      list(col = "total_contributor_count_2bin", vals = c(1, 0))),
          fname_prefix  = "prs_opened_collab_total_",
          legend_labels = c("Dept. Collaborative + Many Contributors", "Dept. Uncollaborative + Many Contributors",
                            "Dept. Collaborative + Few Contributors", "Dept. Uncollaborative + Few Contributors")),
  list(filters = list(list(col = "total_contributor_count_2bin", vals = c(1, 0))),
       fname_prefix  = "prs_opened_total_",
       legend_labels = c("Many Contributors", "Few Contributors")),
  list(filters = list(list(col = "ind_key_collab_2bin", vals = c(1, 0)),
                      list(col = "problem_count_2bin", vals = c(1, 0))),
          fname_prefix  = "prs_opened_collab_problem_",
          legend_labels = c("Dept. Collaborative + Many Problems", "Dept. Uncollaborative + Many Problems",
                            "Dept. Collaborative + Few Problems", "Dept. Uncollaborative + Few Problems")),
  list(filters = list(list(col = "problem_count_2bin", vals = c(1, 0))),
       fname_prefix  = "prs_opened_problem_",
       legend_labels = c("Many Problems", "Few Problems")))

metrics <- c("cs", "2s", "es") # BJS HAS KNOWN ISSUE
metrics_fn <- c("Callaway and Sant'Anna 2020", "Gardner 2021", "Freyaldenhoven et. al 2021")

modes <- list(
  list(normalize = FALSE, file = "issue/output/prs_opened.png", outcome = "prs_opened"),
  list(normalize = TRUE,  file = "issue/output/prs_opened_norm.png", outcome = "prs_opened"),
  list(normalize = FALSE, file = "issue/output/commits.png", outcome = "commits"),
  list(normalize = TRUE,  file = "issue/output/commits_norm.png", outcome = "commits")
)

for (mode in modes) {
  es_list <- lapply(metrics, function(m) {
    EventStudy(df_panel_nyt, mode$outcome, m, title = "", normalize = mode$normalize)
  })
  png(mode$file)
  do.call(CompareES, c(es_list, list(legend_labels = metrics_fn)))
  dev.off()
}


for(norm in c(TRUE,FALSE)) {
  norm_str <- ifelse(norm, "_norm", "")
  for(m in metrics[1:3]) {
    for(g in group_defs[1]) {
      combo_grid <- expand.grid(lapply(g$filters, `[[`, "vals"), 
                                KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
      print(combo_grid)
      print(g$legend_labels)
      es_list <- apply(combo_grid, 1, function(vals_row){
        df_sub<-df_panel_nyt %>% filter(repo_name != "pantsbuild/pants")
        for(i in seq_along(vals_row)){
          col_name<-g$filters[[i]]$col
          df_sub<-df_sub %>% filter(.data[[col_name]]==vals_row[[i]])
        }
        tryCatch(EventStudy(df_sub,"prs_opened",method=m,title="",normalize=norm),
                 error=function(e) NULL)
        },
        simplify=FALSE)
      success_idx <- which(!sapply(es_list,is.null))
      es_list     <- es_list[success_idx]
      labels      <- g$legend_labels[success_idx]
      
      out_path <- paste0("issue/output/collab/", g$fname_prefix, m, norm_str, "wo.png")
      png(out_path)
      do.call(CompareES, c(es_list, list(legend_labels = labels)))
      dev.off()
    }
  }
}
