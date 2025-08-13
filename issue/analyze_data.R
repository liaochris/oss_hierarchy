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
  if (grepl("n_avg_", outcome) & startsWith(outcome, "n_avg_")) {
    sd_outcome_var <- sub("n_avg_", "", outcome)
  } else if (grepl("avg_", outcome) & startsWith(outcome, "avg_")) {
    sd_outcome_var <- sub("avg_", "", outcome)
  } else if (grepl("_dept_", outcome)) {
    sd_outcome_var <- gsub("_dept.*", "", outcome)
  } else {
    sd_outcome_var <- sd_outcome_var
  }
  
  df_norm <- df %>%
    group_by(repo_name) %>%
    mutate(mean_outcome = mean(get(outcome)[time_index < treatment_group & 
                                              time_index >= (treatment_group - 5)], na.rm = TRUE),
           sd_outcome = sd(get(sd_outcome_var)[time_index < treatment_group & 
                                                 time_index >= (treatment_group - 5)], na.rm = TRUE)) %>%
    ungroup()
  df_norm[[outcome_norm]] <- (df_norm[[outcome]] - df_norm$mean_outcome)/df_norm$sd_outcome
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
                                        first_stage  = ~ 0 | repo_name + time_index,
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
  res_mat[is.na(res_mat)] <- 0
  res_mat <- res_mat[order(as.numeric(rownames(res_mat))), , drop = FALSE]
  plot_fun <- fixest::coefplot
  plot_obj <- plot_fun(res_mat,
                       main     = title,
                       xlab     = "Event time (k)",
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
                      legend_title  = NULL,
                      legend_labels = NULL,
                      title         = "",
                      add_p         = TRUE,
                      ylim          = NULL) {
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
    xlab   = "Event time (k)",
    ylab   = "",
    main   = title,
    keep   = "^-[1-4]|[0-4]",
    drop   = "[[:digit:]]{2}",
    order  = c("-4", "-3", "-2", "-1", "0", "1", "2", "3", "4"),
    xaxt   = "n",       # suppress the x‐axis
    yaxt   = "n",
    grid   = FALSE
  )
  if (!is.null(ylim)) plot_args$ylim <- ylim
  do.call(fixest::coefplot, plot_args)

  if (!is.null(legend_labels)) {
    par(xpd = NA)
    legend(
      "top", legend  = legend_labels, title   = legend_title,
      horiz  = TRUE, 
      bty    = "o",    box.lwd = 0.8,    box.col = "grey40",
      bg     = "white", xjust   = 0.5,
      col    = seq_along(results), pch     = 20,
      lwd    = 1,      lty     = seq_along(results)
    )
    par(xpd = FALSE)
  }

  if (add_p && length(results) > 1) {
    # Wald p-value
    if (length(results) == 2) {
      wp <- CompareEventCoefsWald(results, terms = 0:4)
      wp_str <- sprintf("%.3f", wp)
      wald_lbl <- paste0("Wald test p-value: ", wp_str)
    } else {
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
    pre_strs <- sprintf("%.3f", pre_p)
    pre_lbl <- paste(
      paste0("Pretrend (", legend_labels, ") p-value: ", pre_strs),
      collapse = "\n"
    )

    # position
    usr   <- par("usr")
    x_min <- usr[1]; x_max <- usr[2]
    buffer <- 0.02 * (x_max - x_min)
    x_left <- x_min + buffer

    # draw inside plot, left-aligned
    mtext(wald_lbl, side = 1, line = -1, at = x_left, adj = 0, cex = 0.8)
    mtext(pre_lbl,  side = 1, line = -1.8, at = x_left, adj = 0, cex = 0.8)
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
  t1  <- tidy_list[[1]][avail_sorted, ]
  t2  <- tidy_list[[2]][avail_sorted, ]
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
           time_index <  treatment_group) %>%
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
    filters      = list(list(col = "ind_key_collab_2bin", vals = c(1, 0))),
    fname_prefix = "prs_opened_collab_",
    legend_title = "Departed collaborativeness",
    legend_labels = c("High", "Low")
  ),
  list(
    filters      = list(
      list(col = "departed_involved_2bin", vals = c(1, 0)),
      list(col = "departed_opened_2bin",  vals = c(1, 0))
    ),
    fname_prefix = "prs_opened_involved_departed_opened_",
    legend_title = "Departed Involvement & PR Activity",
    legend_labels = c(
      "High Involvement + Many PRs",
      "Low Involvement  + Many PRs",
      "High Involvement + Few PRs",
      "Low Involvement  + Few PRs"
    )
  ),
  list(
    filters      = list(list(col = "departed_involved_2bin", vals = c(1, 0))),
    fname_prefix = "prs_opened_involved_",
    legend_title = "Departed contributor involvement",
    legend_labels = c("High", "Low")
  ),
  list(
    filters      = list(list(col = "departed_opened_2bin", vals = c(1, 0))),
    fname_prefix = "prs_opened_departed_opened_",
    legend_title = "Departed contributor PR involvement",
    legend_labels = c("High", "Low")
  )
)
metrics <- c("cs") # BJS HAS KNOWN ISSUE
metrics_fn <- c("Callaway and Sant'Anna 2020")



# ----- Panels & ID Assignment -----
df_panel_nyt     <- read_parquet('issue/df_panel_nyt.parquet')
df_panel_nyt_all <- read_parquet('issue/df_panel_nyt_all.parquet')
df_panel_nyt_alltime <- read_parquet('issue/df_panel_nyt_alltime.parquet')

df_predep_cc <- df_panel_nyt_all %>% select(repo_name, time_period, total_contributor_count) %>% 
  rename(predep_contributor_count = total_contributor_count)
df_nodep_cc <- df_panel_nyt_alltime %>% select(repo_name, time_period, total_contributor_count) %>% 
  rename(nodep_contributor_count = total_contributor_count)
df_panel_nyt <- df_panel_nyt %>% left_join(df_predep_cc) %>% left_join(df_nodep_cc)  %>%
  group_by(repo_name) %>%
  mutate(
    nodep_contributor_count_neg1  = nodep_contributor_count[time_index - treatment_group == -1][1],
    predep_contributor_count_neg1 = predep_contributor_count[time_index - treatment_group == -1][1]
  ) %>%
  ungroup()
SafeDivide <- function(numerator, denominator) {
  result <- numerator / denominator
  ifelse(is.finite(result), result, 0)
}

df_panel_nyt <- df_panel_nyt %>%
  mutate(
    avg_prs_opened_nondep = SafeDivide(prs_opened_nondep, nodep_contributor_count),
    avg_prs_opened_predep = SafeDivide(prs_opened_predep, predep_contributor_count),
    avg_prs_opened_dept_comm = SafeDivide(prs_opened_dept_comm, contributors_dept_comm),
    avg_prs_opened_dept_never_comm = SafeDivide(prs_opened_dept_never_comm, contributors_dept_never_comm),
    n_avg_prs_opened_nondep = nodep_contributor_count_neg1*avg_prs_opened_nondep,
    n_avg_prs_opened_predep = predep_contributor_count_neg1 * avg_prs_opened_predep,
    avg_prs_opened_dept_comm_avg_above = SafeDivide(prs_opened_dept_comm_avg_above, contributors_dept_comm_avg_above),
    avg_prs_opened_dept_comm_avg_below = SafeDivide(prs_opened_dept_comm_avg_below, contributors_dept_comm_avg_below),
    avg_prs_opened_dept_never_comm_predep = SafeDivide(prs_opened_dept_never_comm_predep, contributors_dept_never_comm_predep),
    avg_prs_opened_dept_comm_per_problem_avg_above = SafeDivide(prs_opened_dept_comm_per_problem_avg_above, contributors_dept_comm_per_problem_avg_above),
    avg_prs_opened_dept_comm_per_problem_avg_below = SafeDivide(prs_opened_dept_comm_per_problem_avg_below, contributors_dept_comm_per_problem_avg_below),
    avg_prs_opened_dept_comm_per_problem_min_avg_above = SafeDivide(prs_opened_dept_comm_per_problem_min_avg_above, contributors_dept_comm_per_problem_min_avg_above),
    avg_prs_opened_dept_comm_per_problem_min_avg_below = SafeDivide(prs_opened_dept_comm_per_problem_min_avg_below, contributors_dept_comm_per_problem_min_avg_below),
  ) %>%
  mutate(prs_opened = prs_opened_prob)
  
comm_label_map <- c(
  "prs_opened_dept_comm"    = "Yes",
  "prs_opened_dept_never_comm"    = "No",
  "prs_opened_dept_never_comm_predep"    = "No",
  "prs_opened_dept_comm_avg_above"    = "Above average",
  "prs_opened_dept_comm_avg_below"    = "Below average",
  "avg_prs_opened_dept_comm"    = "Yes",
  "avg_prs_opened_dept_never_comm"    = "No",
  "avg_prs_opened_dept_never_comm_predep"    = "No",
  "avg_prs_opened_dept_comm_avg_above"    = "Above avg. comm.",
  "avg_prs_opened_dept_comm_avg_below"    = "Below avg. comm.",
  "prs_opened_dept_comm_per_problem_avg_above" = "Above average",
  "prs_opened_dept_comm_per_problem_avg_below" = "Below average",
  "prs_opened_dept_comm_per_problem_min_avg_above" = "Above average",
  "prs_opened_dept_comm_per_problem_min_avg_below" = "Below average",
  "avg_prs_opened_dept_comm_per_problem_avg_above"= "Above average",
  "avg_prs_opened_dept_comm_per_problem_avg_below"= "Below average",
  "avg_prs_opened_dept_comm_per_problem_min_avg_above"= "Above average",
  "avg_prs_opened_dept_comm_per_problem_min_avg_below"= "Below average"
)

length(unique(df_panel_nyt$repo_name))

for (df_name in c('df_panel_nyt')) {
  output_root <- if (df_name == 'df_panel_nyt') 'issue/output' else ""
  all_collab_ylim <- c(-3.5, 3.5)
  pre_nondep_ylim <- c(-4, 4.5)
  comm_nondep_ylim <- c(-2, 4)
  comm_predep_ylim <- c(-2.5, 3)

  modes <- list(
    list(normalize = TRUE,  file = file.path(output_root, "prs_opened_norm.png"), outcome = "prs_opened")
  )
  panel <- get(df_name)
  panel <- panel %>% 
    mutate(repo_name_id = as.integer(factor(repo_name, levels = sort(unique(repo_name)))))
  panel <- panel %>%
    filter(repo_name %in% CheckPreTreatmentThreshold(df_panel_nyt, 3, "prs_opened", 5))

  for (mode in modes) {
    es_list <- lapply(metrics, function(m) {
      EventStudy(panel, mode$outcome, m, title = "", normalize = mode$normalize)
    })
    png(mode$file)
    do.call(CompareES, c(es_list, list(ylim = all_collab_ylim)))
    dev.off()
  }
  
  for(norm in c(TRUE)) {
    norm_str <- ifelse(norm, "_norm", "")
    for(m in metrics) {
      for(g in group_defs) {
        combo_grid <- expand.grid(lapply(g$filters, `[[`, "vals"), 
                                  KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
        es_list <- apply(combo_grid, 1, function(vals_row){
          df_sub<-panel
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
        legend_title      <- g$legend_title

        out_path <- paste0(file.path(output_root, "collab/"), g$fname_prefix, m, norm_str, ".png")
        png(out_path)
        if (g$fname_prefix == "prs_opened_collab_") {
          do.call(CompareES, c(es_list, list(legend_labels = labels, ylim = all_collab_ylim, legend_title = legend_title)))
        } else {
          do.call(CompareES, c(es_list, list(legend_labels = labels, legend_title = legend_title)))
        }
        dev.off()
      }
    }
  }
    # 1) Define each pair with its partner key and a short name:
  paired_outcomes <- list(
    prs_opened_dept_never_comm = list(
      partner = "prs_opened_dept_comm",
      name    = "Communicated w/ departed"
    ),
    prs_opened_dept_comm_avg_above = list(
      partner = "prs_opened_dept_comm_avg_below",
      name    = "Communication intensity"
    ),
    avg_prs_opened_dept_comm_avg_above = list(
      partner = "avg_prs_opened_dept_comm_avg_below",
      name    = "Communication intensity"
    ),
    avg_prs_opened_dept_never_comm = list(
      partner = "avg_prs_opened_dept_comm",
      name    = "Communicated w/ Departed"
    ),
    prs_opened_dept_never_comm_predep = list(
      partner = "prs_opened_dept_comm",
      name    = "Communicated w/ Departed"
    ),
    avg_prs_opened_dept_never_comm_predep = list(
      partner = "avg_prs_opened_dept_comm",
      name    = "Communicated w/ Departed"
    ),
    prs_opened_dept_comm_per_problem_avg_above = list(
      partner = "prs_opened_dept_comm_per_problem_avg_below",
      name    = "Per-problem communication intensity"
    ),
    prs_opened_dept_comm_per_problem_min_avg_above = list(
      partner = "prs_opened_dept_comm_per_problem_min_avg_below",
      name    = "Per-problem communication intensity"
    ),
    avg_prs_opened_dept_comm_per_problem_avg_above = list(
      partner = "avg_prs_opened_dept_comm_per_problem_avg_below",
      name    = "Per-problem communication intensity"
    ),
    avg_prs_opened_dept_comm_per_problem_min_avg_above = list(
      partner = "avg_prs_opened_dept_comm_per_problem_min_avg_below",
      name    = "Per-problem communication intensity"
    )
  )

  # 2) Your g list stays the same:
  g <- list(
    filters       = list(list(col = "ind_key_collab_2bin", vals = c(1, 0))),
    fname_prefix  = "prs_opened_collab_",
    legend_title  = "Departed collaborativeness",
    legend_labels = c("High", "Low")
  )

  # Helper to open a PNG, call CompareES, then close
  save_compare <- function(path, es_list, labels, legend_title, ylim = NULL) {
    png(path)
    args <- c(es_list,
              list(legend_labels = labels,
                  legend_title  = legend_title,
                  title         = "",
                  ylim          = ylim))
    do.call(CompareES, args)
    dev.off()
  }

  # Main loops
  for (norm in TRUE) {
    norm_str <- if (norm) "_norm" else ""
    for (met in metrics) {
      # build outcome vector
      outcome_vec <- c("prs_opened", "total_contributor_count",
                      "prs_opened_nondep", "prs_opened_predep")
      if (df_name == 'df_panel_nyt') {
        outcome_vec <- c(outcome_vec,
                        "avg_prs_opened_nondep", "avg_prs_opened_predep",
                        "n_avg_prs_opened_nondep", "n_avg_prs_opened_predep",
                        names(paired_outcomes))
      }
      
      for (outcome in outcome_vec) {
        # build es_list & label_list …
        combo_grid <- expand.grid(lapply(g$filters, `[[`, "vals"),
                                  KEEP.OUT.ATTRS = FALSE,
                                  stringsAsFactors = FALSE)
        es_list    <- list()
        label_list <- list()
        for (row_idx in seq_len(nrow(combo_grid))) {
          # … your existing EventStudy + label_list code …
        }
        
        # choose branch
        if (outcome %in% names(paired_outcomes)) {
          info <- paired_outcomes[[outcome]]
          this_title <- paste(g$legend_title, "+", info$name)
          for (lvl in g$legend_labels) {
            idx <- grepl(lvl, label_list, fixed = TRUE)
            if (!any(idx)) next
            out_path <- file.path(output_root,
                                  sprintf("collab/%s%s_%s_%s.png",
                                          met, norm_str,
                                          outcome, gsub("\\s+", "", lvl)))
            save_compare(
              out_path,
              es_list[idx],
              label_list[idx],
              this_title,
              ylim = switch(
                outcome,
                prs_opened_dept_never_comm           = comm_nondep_ylim,
                prs_opened_dept_never_comm_predep    = comm_predep_ylim,
                NULL
              )
            )
          }
          
        } else {
          out_path <- file.path(output_root,
                                sprintf("collab/%s%s_%s.png",
                                        met, norm_str, outcome))
          ylim_arg <- if (outcome %in% c("prs_opened_nondep", "prs_opened_predep"))
                      pre_nondep_ylim else NULL
          save_compare(out_path, es_list, label_list, g$legend_title, ylim = ylim_arg)
        }
      }
    }
  }

  g <- list(
    filters      = list(list(col = "ind_key_collab_2bin", vals = c(1, 0))),
    fname_prefix = "prs_opened_collab_",
    legend_title = "Departed collaborativeness",
    legend_labels = c("High", "Low")
  )
  for(norm in c(TRUE)) {  
    norm_str <- ifelse(norm, "_norm", "")  
  for(met in metrics) {  
      for(i in c(0,1)) {  
        outcome_vec <- c("prs_opened", "total_contributor_count", "prs_opened_nondep", "prs_opened_predep")
        if (df_name == 'df_panel_nyt') {
          outcome_vec <- c(outcome_vec, "avg_prs_opened_nondep", "avg_prs_opened_predep",
                            "n_avg_prs_opened_nondep", "n_avg_prs_opened_predep", 
                            names(paired_outcomes))  # only include the "above"/"comm" side
        }
        
        for (outcome in outcome_vec) {
          combo_grid <- expand.grid(lapply(g$filters, `[[`, "vals"), 
                                    KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
          
          es_list <- list()
          label_list <- list()
          
          for (row_idx in seq_len(nrow(combo_grid))) {
            vals_row <- combo_grid[row_idx, ]
            
            df_sub <- panel %>%  
              filter(departed_involved_2bin == i)
            
            for (j in seq_along(vals_row)) {
              col_name <- g$filters[[j]]$col
              df_sub <- df_sub %>% filter(.data[[col_name]] == vals_row[[j]])
            }
            
            if (outcome %in% names(paired_outcomes)) {
              paired <- c(outcome, paired_outcomes[[outcome]])
              for (oc in paired) {
                res <- tryCatch(EventStudy(df_sub, oc, method = m, title = "", normalize = norm), error = function(e) e)
                if (!is.null(res) && "plot" %in% names(res)) {
                  es_list[[length(es_list) + 1]] <- res
                  pretty_name <- ifelse(oc %in% names(comm_label_map), comm_label_map[[oc]], oc)
                  label_list[[length(label_list) + 1]] <- paste0(g$legend_labels[[row_idx]], " + ", pretty_name)
                }
              }
            } else {
              res <- tryCatch(EventStudy(df_sub, outcome, method = m, title = "", normalize = norm), error = function(e) e)
              if (!is.null(res) && "plot" %in% names(res)) {
                es_list[[length(es_list) + 1]] <- res
                label_list[[length(label_list) + 1]] <- g$legend_labels[[row_idx]]
              }
            }
          }
          
          if (outcome %in% c("prs_opened_dept_never_comm", "avg_prs_opened_dept_never_comm",
                             "prs_opened_dept_never_comm_predep","avg_prs_opened_dept_never_comm_predep",
                             "prs_opened_dept_comm_avg_above",
                             "prs_opened_dept_comm_per_problem_avg_above","prs_opened_dept_comm_per_problem_min_avg_above",
                             "avg_prs_opened_dept_comm_per_problem_avg_above","avg_prs_opened_dept_comm_per_problem_min_avg_above")) {
            for (k in seq_along(g$legend_labels)) {
              legend_label <- g$legend_labels[[k]]
              label_mask <- vapply(label_list, function(lbl) grepl(legend_label, lbl, fixed = TRUE), logical(1))
              
              es_split <- es_list[label_mask]
              label_split <- label_list[label_mask]
              
              if (length(es_split) > 0) {
                plot_title <- paste0("Departed Inv.: ", i, "\n", outcome, df_name, " — ", legend_label)
                out_path_split <- file.path(output_root, paste0("collab_imp/inv", i, "_", met, norm_str, "_", outcome, "_", gsub("\\s+", "", legend_label), ".png"))
                
                png(out_path_split)
                do.call(CompareES, c(es_split, list(legend_labels = label_split, title = "")))
                dev.off()
              }
            }
          } else {
            out_path <- file.path(output_root, paste0("collab_imp/inv", i, "_", met, norm_str, "_", outcome, ".png"))
            png(out_path)
            do.call(CompareES, c(es_list, list(legend_labels = label_list, title = "")))
            dev.off()
          }
        }
      }  
    }  
  }
}  

