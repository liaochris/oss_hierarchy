
library(eventstudyr)
library(tidyverse)
library(ggplot2)
library(egg)
library(gtable)
library(foreach)
library(data.table)
library(grid)
library(arrow)
library(scales)

set.seed(1234)
cl <- parallel::makeCluster(6)
doParallel::registerDoParallel(cl)
`%ni%` = Negate(`%in%`)

# import outcomes?
indir <- "drive/output/derived/project_outcomes"
indir_departed <- "drive/output/derived/contributor_stats/filtered_departed_contributors"
outdir <- "issue/event_study"

time_period <- 6
rolling_window <- 732
criteria_pct <- 75
consecutive_periods <- 3
post_periods <- 2

df_project_outcomes <- read_parquet(file.path(indir, paste0("project_outcomes_major_months",time_period,".parquet")))
df_departed_contributors <- read_parquet(
  file.path(indir_departed, paste0("filtered_departed_contributors_major_months",time_period,"_window",
                                   rolling_window,"D_criteria_commits_",criteria_pct,"pct_consecutive",consecutive_periods,
                                   "_post_period",post_periods,"_threshold_gap_qty_0.parquet")))
df_project_covariates <- read_parquet(file.path(indir, paste0("project_covariates_major_months",time_period,".parquet")))
df_contributor_covariates <- read_parquet(file.path(indir, paste0("contributor_covariates_major_months",time_period,".parquet"))) %>%
  mutate(departed_actor_id = actor_id,
         time_period = as.Date(time_period)) %>%
  select(-actor_id)

treated_more_than_once <- df_departed_contributors %>% filter(repo_count>1) %>% pull(repo_name) %>% unique()

df_project_covariates <- df_project_covariates %>%
  mutate(time_period = as.Date(time_period))

df_departed <- df_departed_contributors %>% 
  filter(repo_count == 1) %>%
  filter(!abandoned_scraped & !abandoned_consecutive_req3_permanentTrue) %>%
  mutate(departed_actor_id = actor_id,
         abandoned_date = as.Date(abandoned_date_consecutive_req2_permanentTrue)) %>%
  select(repo_name,departed_actor_id,last_pre_period,treatment_period,abandoned_date)

df_project <- df_project_outcomes %>% 
  mutate(time_period = as.Date(time_period)) %>%
  filter(year(time_period)<2023 | (year(time_period) == 2023 & month(time_period) == 1)) %>%
  filter(repo_name %ni% treated_more_than_once) %>%
  mutate(time_index = dense_rank(time_period))

df_project_departed <- df_project %>%
  left_join(df_departed) %>%
  mutate(treated_project = 1-as.numeric(is.na(last_pre_period)),
         treatment = as.numeric(treated_project & time_period>=treatment_period)) %>%
  left_join(df_project_covariates) %>%
  mutate(final_period = as.Date(final_period),
         last_pre_period = as.Date(last_pre_period)) %>%
  left_join(df_contributor_covariates)
  

print(paste("Project Count:", length(df_project_departed %>% pull(repo_name) %>% unique())))
print(paste("Treated Projects:", length(df_project_departed %>% filter(treatment == 1) %>% pull(repo_name) %>% unique())))

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
      filter(time_period <= last_pre_period) %>% 
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
                                     pre = pre,
                                     overidpost = 3,
                                     overidpre = 3)
  return(results)
}

PlotEventStudy <- function(results, title) {
  plot <- EventStudyPlot(estimates = results,
                         ytitle = "Coefficient",
                         xtitle = "Event time") + 
    ggtitle(label = title)
  return(plot)
}

EventStudyGrid <- function(df,outcome, post, pre, title, fillna = T,
                           ymin, ymax, num_breaks) {
  full_samp <- EventStudyAnalysis(df, outcome, post, pre, MakeTitle(title, "Full Sample", df),
                                  norm_outcome = T, fillna = fillna)
  df_early <- df %>% filter(time_period <= final_period)
  early_samp <- EventStudyAnalysis(df_early, outcome, post, pre, MakeTitle(title, "Dropout Sample", df_early),
                                   norm_outcome = T, fillna = fillna)
  
  median_contributors <- 10
    #df %>% 
    #filter(time_period < treatment_period | is.na(treatment_period)) %>% 
    #summarise(median(contributor_count, na.rm = T)) %>% 
    #pull() + 7
  
  mean_sid_all <- df %>% 
    filter(time_period < treatment_period | is.na(treatment_period)) %>% 
    summarise(mean(solve_and_incorporate_and_discuss, na.rm = T)) %>% 
    pull() %>%
    round(2)
  
  mean_hhi_all <- 0.5
    #df %>% 
    #filter(time_period < treatment_period | is.na(treatment_period)) %>% 
    #summarise(mean(commits_hhi, na.rm = T)) %>% 
    #pull() %>%
    #round(2) - 0.2
  
  mean_commits_share_all <- df %>% 
    filter(time_period < treatment_period | is.na(treatment_period)) %>% 
    summarise(mean(commits_share, na.rm = T)) %>% 
    pull() %>%
    round(2) 

  df_summary <- df %>% group_by(repo_name) %>%
    filter(time_period<treatment_period | is.na(treatment_period)) %>%
    summarise(mean_contributor_count = mean(contributor_count),
              mean_sid = mean(solve_and_incorporate_and_discuss),
              mean_hhi = mean(commits_hhi,na.rm = TRUE),
              mean_commits_share = mean(commits_share,na.rm = TRUE),
              median_contributor_count = median(contributor_count),
              median_sid = median(solve_and_incorporate_and_discuss),
              median_hhi = median(commits_hhi, na.rm = TRUE),
              contributor_count_pre_period = sum(contributor_count * as.numeric(time_period == last_pre_period)),
              sid_pre_period = sum(solve_and_incorporate_and_discuss * as.numeric(time_period == last_pre_period)),
              hhi_pre_period = sum(commits_hhi * as.numeric(time_period == last_pre_period)))
  
  
  df <- df %>% 
    left_join(df_summary)
  df_big <- df %>% filter(mean_contributor_count>median_contributors)
  above_samp <- EventStudyAnalysis(
    df_big, outcome, post, pre, 
    MakeTitle(title, paste0("Mean Size>",median_contributors), df_big),    
    norm_outcome = T, fillna = fillna)
  df_small <- df %>% filter(mean_contributor_count<=median_contributors)
  below_samp <- EventStudyAnalysis(
    df_small, outcome, post, pre, 
    MakeTitle(title, paste0("Mean Size<=",median_contributors), df_small),
    norm_outcome = T, fillna = fillna)
  
  
  df_high_hhi <- df %>% filter(mean_hhi>mean_hhi_all)
  high_hhi <- EventStudyAnalysis(
    df_high_hhi, outcome, post, pre, 
    MakeTitle(title, paste0("Mean Commit HHI>",mean_hhi_all), df_high_hhi),
    norm_outcome = T, fillna = fillna)
  df_low_hhi <- df %>% filter(mean_hhi<=mean_hhi_all)
  low_hhi <- EventStudyAnalysis(
    df_low_hhi, outcome, post, pre, 
    MakeTitle(title, paste0("Mean Commit HHI <=",mean_hhi_all), df_low_hhi),    
    norm_outcome = T, fillna = fillna)
  
  df_high_spread <- df %>% filter(mean_sid>mean_sid_all)
  high_spread <- EventStudyAnalysis(
    df_high_spread, outcome, post, pre, 
    MakeTitle(title, paste0("Prop. S,I,D>",mean_sid_all), df_high_spread),
    norm_outcome = T, fillna = fillna)
  df_low_spread <- df %>% filter(mean_sid<=mean_sid_all)
  low_spread <- EventStudyAnalysis(
    df_low_spread, outcome, post, pre, 
    MakeTitle(title, paste0("Prop. S,I,D<=",mean_sid_all), df_low_spread),    
    norm_outcome = T, fillna = fillna)

  
  df_high_commit_share <- df %>% filter(mean_commits_share>mean_commits_share_all | is.na(mean_commits_share))
  high_commit_share <- EventStudyAnalysis(
    df_high_commit_share, outcome, post, pre, 
    MakeTitle(title, paste0("Indiv. Commit Share>",mean_commits_share_all), df_high_commit_share),
    norm_outcome = T, fillna = fillna)
  df_low_commit_share <- df %>% filter(mean_commits_share<=mean_commits_share_all | is.na(mean_commits_share))
  low_commit_share <- EventStudyAnalysis(
    df_low_commit_share, outcome, post, pre, 
    MakeTitle(title, paste0("Indiv. Commit Share<=",mean_commits_share_all), df_low_commit_share),    
    norm_outcome = T, fillna = fillna)
  
  p1 <- full_samp$plot
  p2 <- early_samp$plot
  p3 <- above_samp$plot
  p4 <- below_samp$plot
  p5 <- high_spread$plot
  p6 <- low_spread$plot
  p7 <- high_hhi$plot
  p8 <- low_hhi$plot
  p9 <- high_commit_share$plot
  p10 <- low_commit_share$plot
  
  # Update each plot to have the same y-axis limits
  p1 <- p1 + scale_y_continuous(breaks = pretty_breaks(n = num_breaks)) + 
    coord_cartesian(ylim = c(ymin, ymax))
  p2 <- p2 + scale_y_continuous(breaks = pretty_breaks(n = num_breaks)) + 
    coord_cartesian(ylim = c(ymin, ymax))
  p3 <- p3 + scale_y_continuous(breaks = pretty_breaks(n = num_breaks)) + 
    coord_cartesian(ylim = c(ymin, ymax))
  p4 <- p4 + scale_y_continuous(breaks = pretty_breaks(n = num_breaks)) + 
    coord_cartesian(ylim = c(ymin, ymax))
  p5 <- p5 + scale_y_continuous(breaks = pretty_breaks(n = num_breaks)) + 
    coord_cartesian(ylim = c(ymin, ymax))
  p6 <- p6 + scale_y_continuous(breaks = pretty_breaks(n = num_breaks)) + 
    coord_cartesian(ylim = c(ymin, ymax))
  p7 <- p7 + scale_y_continuous(breaks = pretty_breaks(n = num_breaks)) + 
    coord_cartesian(ylim = c(ymin, ymax))
  p8 <- p8 + scale_y_continuous(breaks = pretty_breaks(n = num_breaks)) + 
    coord_cartesian(ylim = c(ymin, ymax))
  p9 <- p9 + scale_y_continuous(breaks = pretty_breaks(n = num_breaks)) + 
    coord_cartesian(ylim = c(ymin, ymax))
  p10 <- p10 + scale_y_continuous(breaks = pretty_breaks(n = num_breaks)) + 
    coord_cartesian(ylim = c(ymin, ymax))
  
  plot_grid <- grid.arrange(p1, p3, p5, p7, p9, p2, p4, p6, p8, p10, ncol = 5)
  ggsave(plot = plot_grid, filename = file.path(outdir,paste0(outcome,".png")), w = 24, h = 8)
}

MakeTitle <- function(title, title_str, df) {
  full_title_str <- paste(title, paste0("(",title_str,")"), 
        "\n",dim(df)[1],"obs, PC:",length(unique(df$repo_name)),
        "T:",length(unique(df[df$treatment==1,]$repo_name)))
  return(full_title_str)
}


# Problem identification
EventStudyGrid(
  df_project_departed, "issues_opened", 2, 1,
  "Opened Issues ", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

EventStudyGrid(
  df_project_departed, "issue_comments", 2, 1,
  "Issue Comments", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

df_project_departed <- df_project_departed %>%
  mutate(avg_issue_comments = issue_comments/issues_opened)

a_ic <- EventStudyGrid(
  df_project_departed, "avg_issue_comments", 2, 1,
  "Average Issue Comments", fillna = F, 
  ymin = -.75, ymax = 0.5, num_breaks = 5)

o_ic <- EventStudyGrid(
  df_project_departed, "own_issue_comments", 2, 1,
  "Issue Comments (Own)", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

# Problem solving - discussion
h_ic <- EventStudyGrid(
  df_project_departed, "helping_issue_comments", 2, 1,
  "Issue Comments (helping)", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

# why is activity particularly elevated???
pr_c <- EventStudyGrid(
  df_project_departed, "pr_comments", 2, 1,
  "PR Comments", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

# Problem solving - code
opened_prs <- EventStudyGrid(
  df_project_departed, "prs_opened", 2, 1,
  "PRs opened", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

commits <- EventStudyGrid(
  df_project_departed, "commits", 2, 1,
  "Commit Count", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

# Solution Incorporation
merged_prs <- EventStudyGrid(
  df_project_departed, "prs_merged", 2, 1,
  "PRs merged", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

closed_i <- EventStudyGrid(
  df_project_departed, "closed_issue", 2, 1,
  "Closed Issues", fillna = T, 
  ymin = -1, ymax = 0.5, num_breaks = 5)


EventStudyAnalysis <- function(df, outcome, post, pre, title, norm_outcome, fillna)  {
  results <- EstimateEventStudy(df, outcome, post, pre, norm_outcome, fillna)
  plot <- PlotEventStudy(results, title)
  return(list(plot = plot, results = results))
}




RandomTreatmentTime <- function(df, outcome_list, post, pre, fillna) {
  df_random_time <- df %>% filter(treated_project == 1) %>%
    group_by(repo_name) %>%
    slice_sample(n = 1) %>%
    select(time_period) %>%
    rename(last_pre_period = time_period)
  
  df_random_treatment_time <- df %>% filter(treated_project == 1) %>%
    select(-last_pre_period) %>% 
    left_join(df_random_time) %>%
    mutate(treatment = ifelse(time_period > last_pre_period, 1, 0))
  
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
  df_project_departed, c("issue_comments","opened_issues","closed_issues","opened_prs","merged_prs"), 2, 1, T, 100) 

PlotPermutedTreatmentTime(df_random_summ$opened_issues, "# of issues opened that period")
PlotPermutedTreatmentTime(df_random_summ$issue_comments, "Issue Comments on Opened Issues")
