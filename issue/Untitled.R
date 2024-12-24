library(eventstudyr)
library(tidyverse)
library(ggplot2)
library(egg)
library(gtable)
library(foreach)
library(data.table)

set.seed(1234)

cl <- parallel::makeCluster(4)
doParallel::registerDoParallel(cl)

CleanData <- function() {
  df_unbalanced <- read.csv("~/Downloads/panel_major_months6_window1828D_criteria_issue_comments_75pct_general25pct_consecutive3_post_period2threshold_mean_0.2.csv") %>%
    filter(time_period <= as.Date("2022-01-01") & time_period >= as.Date("2017-01-01"))
  df <- expand.grid(repo_name = unique(df_unbalanced$repo_name), time_period = unique(df_unbalanced$time_period)) %>%
    left_join(df_unbalanced) %>%
    arrange(repo_name, time_period)
  df_time_index <-  df %>% select(time_period, time_index) %>% unique() %>% filter(!is.na(time_index))
  df <- df %>% 
    select(-time_index) %>%
    left_join(df_time_index)
  treated_projects <- df %>% filter(treatment == 1) %>% pull(repo_name) %>% unique()
  untreated_projects <- df %>% filter(!(repo_name %in% treated_projects)) %>% pull(repo_name) %>% unique()
  last_preperiod <- df %>%
    filter(repo_name %in% treated_projects & treatment == 0) %>%
    group_by(repo_name) %>%
    summarise(period_before_treatment = max(time_period))
  df <- df %>% left_join(last_preperiod) %>%
    mutate(treated_project = ifelse(repo_name %in% treated_projects, 1, 0))
  
  df_full_untreated <- df %>%
    filter(`X2017_sample` == 1 & repo_name %in% untreated_projects)
  active_treated_repos <- df %>%
    filter(repo_name %in% treated_projects & treatment == 0) %>%
    group_by(repo_name) %>%
    summarise(mean_active_pre_treatment = mean(active_all)) %>% 
    filter(mean_active_pre_treatment == 1) %>%
    pull(repo_name) %>%
    unique()
  df_full_treated <- df %>%
    filter(repo_name %in% active_treated_repos)
  df_full <- rbind(df_full_untreated, df_full_treated) %>%
    mutate(hierarchy_stat = project_hierarchy_rank)
  
  return(list(df = df, df_full = df_full))
}

df <- CleanData()$df
df_full <- CleanData()$df_full

print(paste("Project Count:", length(df_full %>% pull(repo_name) %>% unique())))
print(paste("Treated Projects:", length(df_full %>% filter(treatment == 1) %>% pull(repo_name) %>% unique())))

EventStudyAnalysis <- function(df, outcome, post, pre, title, norm_outcome, fillna)  {
  results <- EstimateEventStudy(df, outcome, post, pre, norm_outcome, fillna)
  plot <- PlotEventStudy(results, title)
  return(list(plot = plot, results = results))
}
EstimateEventStudy <- function(df, outcome, post, pre, norm_outcome, fillna) {
  if (fillna == T) {
    df[[outcome]] <- ifelse(is.na(df[[outcome]]), 0,  df[[outcome]])
  }
  if (norm_outcome == T) {
    norm_outcome <- df %>% 
      filter(time_period <= period_before_treatment) %>% 
      summarise(mean(get(outcome), na.rm = T)) %>% 
      pull()
    df[[outcome]] <- df[[outcome]]/norm_outcome
  }
  results <- eventstudyr::EventStudy(estimator = "OLS",
                        data = df,
                        outcomevar = outcome,
                        policyvar = "treatment",
                        idvar = "repo_name",
                        timevar = "time_index",
                        post = post,
                        pre = pre)
  return(results)
}
PlotEventStudy <- function(results, title) {
  plot <- EventStudyPlot(estimates = results,
                         ytitle = "Coefficient",
                         xtitle = "Event time") + 
    ggtitle(label = title)
  return(plot)
}

EventStudyGrid <- function(df, df_full,outcome, post, pre, title, fillna = T) {
  full_samp <- EventStudyAnalysis(df, outcome, post, pre, paste(title, "\nFull Sample:", dim(df)[1],"obs"),
                                  norm_outcome = T, fillna = fillna)
  bal_samp <- EventStudyAnalysis(df_full, outcome, post, pre, paste(title, "\n Active Panel:", dim(df_full)[1],"obs"), 
                                 norm_outcome = T, fillna = fillna)
  
  full_samp_plot <- full_samp$plot
  bal_samp_plot <- bal_samp$plot
  
  lim <- range(c(layer_scales(full_samp_plot)$y$range$range, layer_scales(bal_samp_plot)$y$range$range))
  
  full_samp_plot <- ggplotGrob(full_samp_plot + ylim(lim))
  bal_samp_plot <- ggplotGrob(bal_samp_plot + ylim(lim))
  panel <- cbind(full_samp_plot, bal_samp_plot, size = "first")
  panel$heights <- unit.pmax(full_samp_plot$heights, bal_samp_plot$heights)
  grid.newpage()
  grid.draw(panel)
}

ic <- EventStudyGrid(
  df, df_full, "issue_comments", 2, 1,
  "Issue Comments on Opened Issues", fillna = T)

opened_i <- EventStudyGrid(
  df, df_full, "opened_issues", 2, 1,
  "# of issues opened that period", fillna = T)

closed_i <- EventStudyGrid(
  df, df_full, "closed_issues", 2, 1,
  "# of issues closed that period", fillna = T)

opened_prs <- EventStudyGrid(
  df, df_full, "opened_prs", 2, 1,
  "# of PRs opened", fillna = T)

merged_prs <- EventStudyGrid(
  df, df_full, "merged_prs", 2, 1,
  "# of PRs merged", fillna = T)


RandomTreatmentTime <- function(df, outcome_list, post, pre, fillna) {
  df_random_time <- df %>% filter(treated_project == 1) %>%
    group_by(repo_name) %>%
    slice_sample(n = 1) %>%
    select(time_period) %>%
    rename(period_before_treatment = time_period)
  
  df_random_treatment_time <- df %>% filter(treated_project == 1) %>%
    select(-period_before_treatment) %>% 
    left_join(df_random_time) %>%
    mutate(treatment = ifelse(time_period > period_before_treatment, 1, 0))
  
  df_random <- rbind(df_random_treatment_time, df %>% filter(treated_project == 0))
  
  bal_samp_list <- list()
  for (outcome in outcome_list) {
    bal_samp <- EstimateEventStudy(df_random, outcome, post, pre, norm_outcome = T, fillna = fillna)
    bal_samp_coefs <- bal_samp$output$coefficients
    bal_samp_list[[length(bal_samp_list)+1]] <- bal_samp_coefs
  }
  df_bal_samp <- data.frame(do.call(rbind, bal_samp_list))
  df_bal_samp$outcome <- outcome_list
  
  return(df_bal_samp)
}

SummaryPermutedTreatmentTime <- function(df, outcome_list, post, pre, fillna, iter) {
  random_coefs <- foreach(i=1:iter, .combine = "rbind", .packages = c("dplyr"),
                          .export = c("RandomTreatmentTime", "EstimateEventStudy")) %dopar% {
    RandomTreatmentTime(df, outcome_list, post, pre, fillna)
  }
  random_coefs <- data.frame(random_coefs)
  val_cols <- colnames(random_coefs)[colnames(random_coefs) != "outcome"]
  random_coefs_summ_mean <- random_coefs %>% group_by(outcome) %>%
    summarise_at(val_cols, mean)
  random_coefs_summ_ci_lb <- random_coefs %>% group_by(outcome) %>%
    summarise_at(val_cols, function (x) quantile(x, 0.025))
  random_coefs_summ_ci_ub <- random_coefs %>% group_by(outcome) %>%
    summarise_at(val_cols, function (x) quantile(x, 0.975))
  random_coefs_summ_ci_sd <- random_coefs %>% group_by(outcome) %>%
    summarise_at(val_cols, sd)
  
  random_coefs_summ_list <- list()
  for (outcome_var in outcome_list) {
    random_coefs_summ <- rbind(random_coefs_summ_mean %>% filter(outcome == outcome_var) %>% as.numeric(),
                               random_coefs_summ_ci_lb %>% filter(outcome == outcome_var) %>% as.numeric(),
                               random_coefs_summ_ci_ub %>% filter(outcome == outcome_var) %>% as.numeric(),
                               random_coefs_summ_ci_sd %>% filter(outcome == outcome_var) %>% as.numeric())
    random_coefs_summ <- t(random_coefs_summ)[-1,]
    colnames(random_coefs_summ) <- c("mean", "LB", "UB", "sd")
    random_coefs_summ_list[[outcome_var]] <- random_coefs_summ
  }
  return(random_coefs_summ_list)
}

PlotPermutedTreatmentTime <- function(random_summ, title) {
  xlabels <- c("-5+", -4:2, "3+") 
  names(xlabels) <- -5:3
  
  random_plot <- rbind(data.frame(random_summ) %>% mutate(index = c(-5, -4, -3, -1, 0, 1, 2, 3)),
        treatment_fd_lead2 = c(0, 0, 0,0, -2)) %>%
    arrange(index) %>%
    ggplot(aes(x = index, y = mean)) +
    geom_point(color = "darkgreen") + 
    geom_linerange(aes(ymin=LB, ymax=UB), colour="black") +
    geom_errorbar(aes(ymin=mean-1.96*sd, ymax=mean+1.96*sd), colour="black", width=.1) +
    geom_hline(yintercept = 0, linetype = 'dashed', color = 'green') +
    labs(title = title, x = "Event time", y = "Coefficient") +
    theme_bw() + 
    theme(panel.grid.minor = element_blank(), 
          panel.grid.major = element_blank(), axis.line = element_line(colour = "black"),
          panel.border = element_rect(colour = "black", fill=NA)) + 
    scale_x_continuous(breaks = -5:3, labels = xlabels)
  
  return(random_plot)
}

df_random_summ <- SummaryPermutedTreatmentTime(
  df_full, c("issue_comments","opened_issues","closed_issues","opened_prs","merged_prs"), 2, 1, T, 500) 

PlotPermutedTreatmentTime(df_random_summ$opened_issues, "# of issues opened that period")
PlotPermutedTreatmentTime(df_random_summ$issue_comments, "Issue Comments on Opened Issues")
