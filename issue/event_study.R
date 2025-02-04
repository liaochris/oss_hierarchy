
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
library(grf)
library(doParallel)

set.seed(1234)
num_cores <- 10
cl <- makeCluster(num_cores)
registerDoParallel(cl)

`%ni%` = Negate(`%in%`)

# import outcomes?
indir <- "drive/output/derived/project_outcomes"
indir_departed <- "drive/output/derived/contributor_stats/filtered_departed_contributors"
issue_tempdir <- "issue"
outdir <- "issue/event_study"

time_period <- 6
rolling_window <- 732
criteria_pct <- 75
consecutive_periods <- 3
post_periods <- 2

df_project_outcomes <- read_parquet(file.path(indir, paste0("project_outcomes_major_months",time_period,".parquet"))) %>% 
  mutate(time_period = as.Date(time_period)) 
df_departed_contributors <- read_parquet(
  file.path(indir_departed, paste0("filtered_departed_contributors_major_months",time_period,"_window",
                                   rolling_window,"D_criteria_commits_",criteria_pct,"pct_consecutive",consecutive_periods,
                                   "_post_period",post_periods,"_threshold_gap_qty_0.parquet")))
df_project_covariates <- read_parquet(file.path(issue_tempdir, "project_covariates.parquet")) %>%
  mutate(time_period = as.Date(time_period))
         
df_contributor_covariates <- read_parquet(file.path(issue_tempdir, "contributor_covariates.parquet")) %>%
  mutate(departed_actor_id = actor_id,
         time_period = as.Date(time_period)) %>%
  select(-actor_id)

df_departed <- df_departed_contributors %>% 
  filter(year(treatment_period)<2023 & !abandoned_scraped & !abandoned_consecutive_req3_permanentTrue) %>%
  group_by(repo_name) %>% 
  mutate(repo_count = n()) %>%
  ungroup() %>%
  mutate(departed_actor_id = actor_id,
         abandoned_date = as.Date(abandoned_date_consecutive_req2_permanentTrue)) %>%
  filter(repo_count == 1) %>%
  select(repo_name,departed_actor_id,last_pre_period,treatment_period,abandoned_date)

treatment_inelg <- df_departed_contributors %>% 
  filter(repo_name %ni% df_departed$repo_name)
treatment_elg <- unique(df_departed$repo_name)

df_project <- df_project_outcomes %>%
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

df_project_departed <- df_project_departed %>% 
  mutate(project_age = (181 + as.numeric(time_period - as.Date(created_at)))) %>%
  group_by(repo_name) %>%
  mutate(smallest_age = min(project_age)) %>%
  ungroup() %>%
  mutate(project_age = ifelse(smallest_age < 0, NA, project_age)) %>%
  mutate(project_id = dense_rank(repo_name))

df_project_departed <- df_project_departed %>%
  group_by(repo_name) %>%
  mutate(treatment_group = min(treatment*time_index),
         treatment_group = ifelse(treatment_group == 0, NA, treatment_group)) %>%
  ungroup()

df_project_departed <- df_project_departed %>%
  rename(
    problem_id_layer_contr_pct              = problem_identification_layer_contributor_pct,
    problem_disc_layer_contr_pct            = problem_discussion_layer_contributor_pct,
    coding_layer_contr_pct                  = coding_layer_contributor_pct,
    problem_app_layer_contr_pct             = problem_approval_layer_contributor_pct,
    problem_id_layer_contr_cnt              = problem_identification_layer_contributor_count,
    problem_disc_layer_contr_cnt            = problem_discussion_layer_contributor_count,
    conding_layer_contr_cnt                 = coding_layer_contributor_count,
    problem_app_layer_contr_cnt             = problem_approval_layer_contributor_count,
    problem_id_hl_contr_ov        = problem_identification_higher_layer_contributor_overlap,
    problem_id_hl_work            = problem_identification_higher_layer_work,
    problem_disc_hl_work          = problem_discussion_higher_layer_work,
    coding_hl_work = coding_higher_layer_work,
    problem_disc_hl_contr_ov      = problem_discussion_higher_layer_contributor_overlap,
    coding_hl_contr_ov            = coding_higher_layer_contributor_overlap,
    pct_coop_commits_100_count              = pct_cooperation_commits_100_count
  )

all_projects <- df_project_departed %>% pull(repo_name) %>% unique()
treated_projects <- df_project_departed %>% filter(treatment == 1) %>% pull(repo_name) %>% unique()
treated_projects_count <- length(treated_projects)
control_projects <- df_project_departed %>%
  filter(repo_name %ni% treated_projects) %>% pull(repo_name) %>% unique()
control_projects_count <- length(control_projects)

print(paste("Project Count:", length(all_projects)))
print(paste("Control Projects:", control_projects_count))
print(paste("Treated Projects:", treated_projects_count))

# training_projects <- 
#   c(sample(treated_projects, size = floor(treated_projects_count/2)), 
#     sample(control_projects, size = floor(control_projects_count/2)))
# testing_projects <- all_projects[all_projects %ni% training_projects]
# 
# training_data <- df_project_departed %>% filter(repo_name %in% training_projects)
# testing_data <- df_project_departed %>% filter(repo_name %in% testing_projects)

project_covars <- c("repo_name","time_period","treated_project","treatment","time_index","treatment_group")
org_covars <- c(
  "stars_accumulated",
  "forks_gained",
  "truckfactor",
  "contributors",
  "org_status",
  "project_age"
)
org_covars_long <- c(
  org_covars, 
  "corporate_pct",
  "educational_pct",
  "apache_software_license",
  "bsd_license",
  "gnu_general_public_license",
  "mit_license"
)

contributor_covars <- c(
  "truckfactor_member",
  "max_rank",
  "total_share",
  "comments_share_avg_wt",
  "comments_hhi_avg_wt",
  "pct_cooperation_comments"
)
contributor_covars_long <- c(
  contributor_covars,
  "contributor_email_educational",
  "contributor_email_corporate",
  "problem_identification_share",
  "problem_discussion_share",
  "coding_share",
  "problem_approval_share",
  "comments_share_avg_wt",
  "comments_hhi_avg_wt",
  "pct_cooperation_comments"
) 

org_structure <- c(
  "min_layer_count",
  "problem_disc_hl_work",
  "coding_hl_work",
  "total_HHI",
  "contributors_comments_wt",
  "comments_cooperation_pct"
)

org_structure_long <- c(
  org_structure,
  "layer_count",
  "problem_id_layer_contr_pct",
  "problem_disc_layer_contr_pct",
  "coding_layer_contr_pct",
  "problem_app_layer_contr_pct",
  "problem_disc_layer_contr_cnt",
  "coding_hl_contr_ov",
  "problem_identification_HHI",
  "problem_discussion_HHI",
  "coding_HHI",
  "hhi_comments",
  "problem_approval_HHI"
)

outcomes <- c("issues_opened","issue_comments","own_issue_comments",
              "helping_issue_comments","pr_comments","prs_opened", "commits", "prs_merged",
              "closed_issue")



median_var_list <- c(org_structure[org_structure!="min_layer_count"],
                     contributor_covars[contributor_covars %ni% c("truckfactor_member","max_rank")])

df_combined <- bind_cols(
  df_project_departed %>% filter(treatment == 0) %>%
    summarise(across(all_of(median_var_list), ~ median(.x, na.rm = TRUE), .names = "{.col}_nyt_med")),
  df_project_departed %>%
    filter(treated_project == 1, treatment == 0) %>%
    summarise(across(all_of(median_var_list), ~ median(.x, na.rm = TRUE), .names = "{.col}_all_med")))

df_project_departed <- cbind(df_project_departed, df_combined) %>%
  select(all_of(c(project_covars, org_covars_long, org_structure_long, outcomes)))
write_dta(df_project_departed, "issue/df_project_departed.dta")

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

EventStudyGrid <- function(df,outcome, post, pre, title, fillna = T,
                           ymin, ymax, num_breaks, policyvar) {
  full_samp <- EventStudyAnalysis(df, outcome, post, pre, MakeTitle(title, "Never-treated", df),
                                  norm_outcome = T, fillna = fillna)
  df_nyt <- df %>% filter(treated_project == 1)
  nyt_samp <- EventStudyAnalysis(df_nyt, outcome, post, pre, MakeTitle(title, "Not-yet treated", df_nyt),
                                   norm_outcome = T, fillna = fillna)
  
  p1 <- full_samp$plot
  p2 <- nyt_samp$plot
  p1 <- p1 + scale_y_continuous(breaks = pretty_breaks(n = num_breaks)) + 
    coord_cartesian(ylim = c(ymin, ymax))
  p2 <- p2 + scale_y_continuous(breaks = pretty_breaks(n = num_breaks)) + 
    coord_cartesian(ylim = c(ymin, ymax))
  plot_grid <- grid.arrange(p1, p2, ncol = 2)
}

MakeTitle <- function(title, title_str, df) {
  full_title_str <- paste(title, paste0("(",title_str,")"), 
                          "\n",dim(df)[1],"obs, PC:",length(unique(df$repo_name)),
                          "T:",length(unique(df[df$treatment==1,]$repo_name)))
  return(full_title_str)
}



EventStudyGrid(
  df_project_departed, "issues_opened", 3, 0,
  "Opened Issues ", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

EventStudyGrid(
  df_project_departed, "issue_comments", 3, 0,
  "Issue Comments", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

df_project_departed <- df_project_departed %>%
  mutate(avg_issue_comments = issue_comments/issues_opened)

a_ic <- EventStudyGrid(
  df_project_departed, "avg_issue_comments", 3, 0,
  "Average Issue Comments", fillna = F, 
  ymin = -.75, ymax = 0.5, num_breaks = 5)

o_ic <- EventStudyGrid(
  df_project_departed, "own_issue_comments", 3, 0,
  "Issue Comments (Own)", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

# Problem solving - discussion
h_ic <- EventStudyGrid(
  df_project_departed, "helping_issue_comments", 3, 0,
  "Issue Comments (helping)", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

# why is activity particularly elevated???
pr_c <- EventStudyGrid(
  df_project_departed, "pr_comments", 3, 0,
  "PR Comments", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

# Problem solving - code
opened_prs <- EventStudyGrid(
  df_project_departed, "prs_opened", 3, 0,
  "PRs opened", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

commits <- EventStudyGrid(
  df_project_departed, "commits", 3, 0,
  "Commit Count", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

# Solution Incorporation
merged_prs <- EventStudyGrid(
  df_project_departed, "prs_merged", 3, 0,
  "PRs merged", fillna = T, 
  ymin = -1.5, ymax = 0.75, num_breaks = 5)

closed_i <- EventStudyGrid(
  df_project_departed, "closed_issue", 3, 0,
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
