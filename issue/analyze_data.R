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
library(aod)


NormalizeOutcome <- function(df, outcome, default_outcome = "prs_opened") {
  outcome_norm <- paste(outcome, "norm", sep = "_")
  sd_outcome <- ifelse(grepl("count", outcome), default_outcome, outcome)
  sd_outcome <- ifelse(grepl("avg_", outcome), sub("avg_", "", outcome), sd_outcome)
  
  df_norm <- df %>%
    group_by(repo_name) %>%
    mutate(mean_outcome = mean(get(outcome)[ time_index < treatment_group & 
                                               time_index >= (treatment_group - 5)], na.rm = TRUE),
           sd_outcome = sd(get(outcome)[ time_index < treatment_group & 
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
CompareES <- function(..., legend_labels, title = "") {
  es_list <- list(...)
  results  <- lapply(es_list, `[[`, "results")
  plot_fn  <- fixest::coefplot
  
  plot_fn(results, xlab = "Time to treatment", ylab = "", keep = "^-[1-5]|[0-5]", drop = "[[:digit:]]{2}",
          main = title)
  
  stopifnot(length(legend_labels) == length(results))
  legend("topleft", col = seq_along(results), pch = 20,
         lwd = 1, lty = seq_along(results), legend = legend_labels)
  
  if (length(results)>=2) {
    combos <- combn(length(results), 2)
    p_vals <- apply(combos, 2, function(idx) {
      p <- CompareEventCoefsWald(results[idx], terms = 0:5)
      paste0(legend_labels[idx[1]], " vs ", legend_labels[idx[2]], ": p=", signif(p, 3))
    })
    
    # add them as a second legend
    legend("bottomleft",
           legend = p_vals,
           bty    = "n",
           cex    = 0.8)
  }
}

CompareEventCoefsWald <- function(tidy_list, terms = 0:5) {
  sel <- as.character(terms)
  m1 <- as.matrix(tidy_list[[1]])
  m2 <- as.matrix(tidy_list[[2]])
  available <- intersect(intersect(rownames(m1), rownames(m2)), sel)
  if (length(available) == 0) {
    return(NA_real_)
  }
  avail_sorted <- available[order(as.numeric(available))]
  t1  <- tidy_list[[1]][avail_sorted, ]
  t2  <- tidy_list[[2]][avail_sorted, ]
  delta <- t1[,"estimate"] - t2[,"estimate"]
  var_sum <- t1[,"sd"]^2 + t2[,"sd"]^2
  
  Sigma <- diag(var_sum, nrow = length(var_sum))
  p_val <- wald.test(b = delta, Sigma = Sigma, Terms = seq_along(delta))$result$chi2[["P"]]
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
  list(filters = list(list(col = "ind_key_collab_2bin", vals = c(1, 0))),
       fname_prefix  = "prs_opened_collab_",
       legend_labels = c("Collaborative Departed Contributor", "Uncollaborative Departed Contributor")),
  list(filters = list(list(col = "departed_involved_2bin", vals = c(1, 0)),
                      list(col = "departed_opened_2bin", vals = c(1, 0))),
       fname_prefix  = "prs_opened_involved_departed_opened_",
       legend_labels = c("Dept. Inv. + Opened Many", "Dept. Uninv. + Opened Many",
                         "Dept. Inv. + Opened Few", "Dept. Uninv. + Opened Few")),
  list(filters = list(list(col = "departed_involved_2bin", vals = c(1, 0))),
       fname_prefix  = "prs_opened_involved_",
       legend_labels = c("More Involved Departed Contributor", "Less Involved Departed Contributor")),
  list(filters = list(list(col = "departed_opened_2bin", vals = c(1, 0))),
       fname_prefix  = "prs_opened_departed_opened_",
       legend_labels = c("Departed Opened Many PRs", "Departed Opened Few PRs")))

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
df_panel_nyt <- df_panel_nyt %>% left_join(df_predep_cc) %>% left_join(df_nodep_cc) %>%
  mutate(avg_prs_opened_nondep = prs_opened_nondep/nodep_contributor_count,
         avg_prs_opened_predep = prs_opened_predep/predep_contributor_count)
for (df_name in c('df_panel_nyt')) {
  output_root <- if (df_name == 'df_panel_nyt') 'issue/output' else ""
  output_root <- if (df_name == 'df_panel_nyt_all') 'issue/output_all' else output_root
  output_root <- if (df_name == 'df_panel_nyt_alltime') 'issue/output_alltime'  else output_root

  modes <- list(
    list(normalize = FALSE, file = file.path(output_root, "prs_opened.png"), outcome = "prs_opened"),
    list(normalize = TRUE,  file = file.path(output_root, "prs_opened_norm.png"), outcome = "prs_opened"),
    list(normalize = FALSE, file = file.path(output_root, "commits.png"), outcome = "commits"),
    list(normalize = TRUE,  file = file.path(output_root, "commits_norm.png"), outcome = "commits")
  )
  panel <- get(df_name)
  panel <- panel %>% mutate(repo_name_id = as.integer(factor(repo_name, levels = sort(unique(repo_name)))))
  panel <- panel %>% filter(repo_name %in% CheckPreTreatmentThreshold(df_panel_nyt, 3, "prs_opened", 5))
  for (mode in modes) {
    es_list <- lapply(metrics, function(m) {
      EventStudy(panel, mode$outcome, m, title = "", normalize = mode$normalize)
    })
    png(mode$file)
    do.call(CompareES, c(es_list, list(legend_labels = metrics_fn)))
    dev.off()
  }
  
  
  for(norm in c(TRUE,FALSE)) {
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
        
        out_path <- paste0(file.path(output_root, "collab/"), g$fname_prefix, m, norm_str, ".png")
        png(out_path)
        do.call(CompareES, c(es_list, list(legend_labels = labels)))
        dev.off()
      }
    }
  }
  
  g <- list(filters = list(list(col = "ind_key_collab_2bin", vals = c(1, 0))),
         fname_prefix  = "prs_opened_collab_",
         legend_labels = c("Dept. Collab", "Dept. Uncollab"))
  # loop over normalization, method, and the four (authored, involved) combos  
  for(norm in c(TRUE, FALSE)) {  
    norm_str <- ifelse(norm, "_norm", "")  
    for(met in metrics) {  
      for(a in c(0,1)) {  
        for(i in c(0,1)) {  
          outcome_vec <- c("prs_opened", "total_contributor_count")
          if (df_name == 'df_panel_nyt') {
            outcome_vec <- c(outcome_vec, "avg_prs_opened_nondep", "avg_prs_opened_predep")
          }
          for (outcome in outcome_vec) {
            combo_grid <- expand.grid(lapply(g$filters, `[[`, "vals"), 
                                      KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
            es_list <- apply(combo_grid, 1, function(vals_row){
              df_sub <- panel %>%  
                filter(departed_authored_2bin == a, departed_involved_2bin == i)  
              for(i in seq_along(vals_row)){
                col_name<-g$filters[[i]]$col
                df_sub<-df_sub %>% filter(.data[[col_name]]==vals_row[[i]])
              }
              tryCatch(EventStudy(df_sub,outcome,method=m,title = "",normalize=norm),
                       error=function(e) e)
            },
            simplify=FALSE)
            success_idx <- which(!sapply(es_list,is.null))
            es_list     <- es_list[success_idx]
            labels      <- g$legend_labels[success_idx]
            
            # output path  
            out_path <-file.path(output_root,  paste0("collab_imp/auth", a, "_inv", i, "_", met, norm_str, "_",outcome,".png"))  
            # plot  
            png(out_path) 
            do.call(CompareES, c(es_list, list(legend_labels = labels), title=paste0("Departed Inv.: ", i, ", Departed Authored: ", a,"\n", outcome, df_name)))
            dev.off()  
          }
        }  
      }  
    }  
  }
  
  for (norm in c(TRUE, FALSE)) {
    norm_str <- ifelse(norm, "_norm", "")  
    combo_grid <- expand.grid(lapply(g$filters, `[[`, "vals"), 
                              KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
    outcome_vec <- c("avg_prs_opened", "prs_opened", "total_contributor_count", "key_contributor_count", "contributor_count")
    if (df_name == 'df_panel_nyt') {
      outcome_vec <- c(outcome_vec, "avg_prs_opened_nondep", "avg_prs_opened_predep")
    }
    for (outcome in outcome_vec) {
      es_list <- apply(combo_grid, 1, function(vals_row){
        df_sub <- panel %>%  
          filter(departed_authored_2bin != 1 | departed_involved_2bin != 1)  
        for(i in seq_along(vals_row)){
          col_name<-g$filters[[i]]$col
          df_sub<-df_sub %>% filter(.data[[col_name]]==vals_row[[i]])
        }
        tryCatch(EventStudy(df_sub,outcome,method=m,title = "",normalize=norm),
                 error=function(e) NULL)
      },
      simplify=FALSE)
      success_idx <- which(!sapply(es_list,is.null))
      es_list     <- es_list[success_idx]
      labels      <- g$legend_labels[success_idx]
      
      # output path  
      out_path <- paste0(  
        file.path(output_root, "collab_imp/auth_n1_inv_n1_"), met, norm_str, "_",outcome, ".png"  
      )  
      # plot  
      png(out_path) 
      do.call(CompareES, c(es_list, list(legend_labels = labels), title=paste0("Departed Inv != 1, Departed Authored != 1")))
      dev.off()  
    }
  }
}  

