library(eventstudyr)
library(dplyr)


df <- read.csv("~/Downloads/panel_major_months6_window732D.csv") 

df_full <- df %>%
  filter(time_period <= as.Date("2022-01-01")) %>%
  filter(time_period >= as.Date("2017-01-01")) %>%
  filter(`X2017_sample` == 1) %>%
  mutate(hierarchy_stat = project_hierarchy_rank)

treated_projects <- c(df_full %>% filter(treatment == 1) %>% select(repo_name) %>% unique())$repo_name
untreated_projects <- c(df_full %>% filter(!(repo_name %in% treated_projects)) %>% select(repo_name) %>% unique())$repo_name

print(paste("Project Count:", dim(df_full %>% select(repo_name) %>% unique())[1]))
print(paste("Treated Projects:", length(treated_projects)))


df_hierarchy_untreated <- df_full %>% 
  mutate(med_hierarchy = median(hierarchy_stat, na.rm = T),
         period_prior_treatment = NA) %>% 
  filter(repo_name %in% untreated_projects) 

df_hierarchy_untreated_low <- df_hierarchy_untreated %>% 
  group_by(repo_name) %>% filter(mean(hierarchy_stat) < med_hierarchy)
df_hierarchy_untreated_high <- df_hierarchy_untreated %>% 
  group_by(repo_name) %>% filter(mean(hierarchy_stat) >= med_hierarchy)

hierarchy_thresh <- unique(df_hierarchy_untreated$med_hierarchy)
df_hierarchy_treated <- df_full %>% 
  filter(treatment==1 & repo_name %in% treated_projects) %>% 
  group_by(repo_name) %>% 
  mutate(time_period = as.Date(time_period), 
         period_prior_treatment = min(time_period) %m-% months(6),
         med_hierarchy = hierarchy_thresh)

repos_hierarchy_treated_low <- c(
  df_full %>% 
    left_join(df_hierarchy_treated %>% select(repo_name, period_prior_treatment, med_hierarchy) %>% unique()) %>%
    filter(time_period == period_prior_treatment & hierarchy_stat<med_hierarchy) %>%
    select(repo_name) %>% unique())$repo_name
repos_hierarchy_treated_high <- c(
  df_full %>% 
    left_join(df_hierarchy_treated %>% select(repo_name, period_prior_treatment, med_hierarchy) %>% unique()) %>%
    filter(time_period == period_prior_treatment & hierarchy_stat>=med_hierarchy) %>%
    select(repo_name) %>% unique())$repo_name

df_full <- df_full %>% mutate(med_hierarchy = hierarchy_thresh)
df_full <- df_full %>% 
  left_join(df_hierarchy_treated %>% select(period_prior_treatment, repo_name) %>% unique()) 

df_hierarchy_high <- rbind(
  df_full %>% filter(repo_name %in% repos_hierarchy_treated_high),
  df_hierarchy_untreated_high)

df_hierarchy_low <- rbind(
  df_full %>% filter(repo_name %in% repos_hierarchy_treated_low),
  df_hierarchy_untreated_low)

repos_hierarchy_high_maintainer <- 
  c(df_full %>% filter(repo_name %in% repos_hierarchy_treated_high & time_period == period_prior_treatment & rolling_3period_rank == "maintainer") %>%
  select(repo_name))$repo_name
repos_hierarchy_high_not_maintainer <- 
  c(df_full %>% filter(repo_name %in% repos_hierarchy_treated_high & time_period == period_prior_treatment & rolling_3period_rank != "maintainer") %>%
      select(repo_name))$repo_name

repos_hierarchy_low_maintainer <- 
  c(df_full %>% filter(repo_name %in% repos_hierarchy_treated_low & time_period == period_prior_treatment & rolling_3period_rank == "maintainer")%>%
      select(repo_name))$repo_name
repos_hierarchy_low_not_maintainer <- 
  c(df_full %>% filter(repo_name %in% repos_hierarchy_treated_low & time_period == period_prior_treatment & rolling_3period_rank != "maintainer")%>%
      select(repo_name))$repo_name


df_hierarchy_high_maintainer <- rbind(
  df_full %>% filter(repo_name %in% repos_hierarchy_high_maintainer),
  df_hierarchy_untreated_high)
df_hierarchy_high_not_maintainer <- rbind(
  df_full %>% filter(repo_name %in% repos_hierarchy_high_not_maintainer),
  df_hierarchy_untreated_high)

df_hierarchy_low_maintainer <- rbind(
  df_full %>% filter(repo_name %in% repos_hierarchy_low_maintainer),
  df_hierarchy_untreated_low)
df_hierarchy_low_not_maintainer <- rbind(
  df_full %>% filter(repo_name %in% repos_hierarchy_low_not_maintainer),
  df_hierarchy_untreated_low)


EstimateEventStudy<- function(df, outcome, post, pre, title, norm_outcome = F)  {
  if (norm_outcome == T) {
    norm_outcome <- df %>% 
      filter(time_period < period_prior_treatment) %>% 
      summarise(mean(get(outcome))) %>% 
      pull()
    df[[outcome]] <- df[[outcome]]/norm_outcome
  }
  
  results <- EventStudy(estimator = "OLS",
                        data = df,
                        outcomevar = outcome,
                        policyvar = "treatment",
                        idvar = "repo_name",
                        timevar = "time_index",
                        post = post,
                        pre = pre)

  plot <- EventStudyPlot(estimates = results,
                         ytitle = "Coefficient",
                         xtitle = "Event time") + 
    ggtitle(label = title)
  return(plot)
}
EventStudyGrid <- function(df, df_full, df_hierarchy_high, df_hierarchy_low, 
                           outcome, post, pre, title) {
  full_samp <- EstimateEventStudy(df, outcome, post, pre, paste(title, "\nFull Sample"))
  bal_samp <- EstimateEventStudy(df_full, outcome, post, pre, paste(title, "\nBalanced Panel"), T)
  unhierarchical <- EstimateEventStudy(df_hierarchy_high, outcome, post, pre, paste(title, "\nUnhierarchical"), T)
  hierarchical <- EstimateEventStudy(df_hierarchy_low, outcome, post, pre, paste(title, "\nHierarchical"), T)
  grid.arrange(full_samp, bal_samp, unhierarchical, hierarchical,
                 ncol=2)
}


EventStudyGridSplit <- function(df_hierarchy_high_maintainer, df_hierarchy_high_not_maintainer,
                           df_hierarchy_low_maintainer, df_hierarchy_low_not_maintainer,
                           outcome, post, pre, title) {
  unhierarchical_maintainer <- EstimateEventStudy(df_hierarchy_high_maintainer, outcome, post, pre, paste(title, "\nUnhierarchical Maintainer"), T)
  unhierarchical_not_maintainer <- EstimateEventStudy(df_hierarchy_high_not_maintainer, outcome, post, pre, paste(title, "\nUnhierarchical Non-Maintainers"), T)
  hierarchical_maintainer <- EstimateEventStudy(df_hierarchy_low_maintainer, outcome, post, pre, paste(title, "\nHierarchical Maintainer"), T)
  hierarchical_not_maintainer <- EstimateEventStudy(df_hierarchy_low_not_maintainer, outcome, post, pre, paste(title, "\nHierarchical Non-Mainainers"), T)
  grid.arrange(unhierarchical_maintainer, unhierarchical_not_maintainer, 
               hierarchical_maintainer, hierarchical_not_maintainer, 
               ncol=2)
}

ic <- EventStudyGrid(
  df, df_full, df_hierarchy_high, df_hierarchy_low,
  "issue_comments", 2, 0,
  "Issue Comments on Opened Issues")

ic_split <- EventStudyGridSplit(
  df_hierarchy_high_maintainer, df_hierarchy_high_not_maintainer,
  df_hierarchy_low_maintainer, df_hierarchy_low_not_maintainer,
  "issue_comments", 2, 0,
  "Issue Comments on Opened Issues")
  
avg_ic <- EventStudyGrid(
  df, df_full, df_hierarchy_high, df_hierarchy_low, "avg_issue_commments", 2, 0,
  "Avg # of Issue Comments on Opened Issues")

avg_ic_split <- EventStudyGridSplit(
  df_hierarchy_high_maintainer, df_hierarchy_high_not_maintainer,
  df_hierarchy_low_maintainer, df_hierarchy_low_not_maintainer,
  "avg_issue_commments", 2, 0,
  "Avg # of Issue Comments on Opened Issues")

p30 <- EventStudyGrid(
  df, df_full, df_hierarchy_high, df_hierarchy_low, "p_issues_closed_30d", 2, 0,
  "Prop. Opened Issues Closed <= 30D")

p90 <- EventStudyGrid(
  df, df_full, df_hierarchy_high, df_hierarchy_low, "p_issues_closed_90d", 2, 0,
  "Prop. Opened Issues Closed <= 90D")

p360 <- EventStudyGrid(
  df, df_full, df_hierarchy_high, df_hierarchy_low, "p_issues_closed_360d", 2, 0,
  "Prop. Opened Issues Closed <= 360D")

#EstimateEventStudy(df_full, "opened_issues", 2, 0, "# of issues opened that period") #NTS investigate 
#EstimateEventStudy(df_full %>% filter(repo_name != "Rapptz/discord.py"), "opened_issues", 2, 0, "# of issues opened that period") #NTS investigate 

opened_i <- EventStudyGrid(
  df, df_full, df_hierarchy_high, df_hierarchy_low, "opened_issues", 2, 0,
  "# of issues opened that period") #NTS investigate 

merged_prs <- EventStudyGrid(
  df, df_full, df_hierarchy_high, df_hierarchy_low, "p_prs_merged", 2, 0,
  "Prop. of PRs merged")

merged_prs_30 <- EventStudyGrid(
  df, df_full, df_hierarchy_high, df_hierarchy_low, "p_prs_merged_30d", 2, 0,
  "Prop. of PRs merged within 30 days")

merged_prs_90 <- EventStudyGrid(
  df, df_full, df_hierarchy_high, df_hierarchy_low, "p_prs_merged_90d", 2, 0,
  "Prop. of PRs merged within 90 days")

merged_prs_360 <- EventStudyGrid(
  df, df_full, df_hierarchy_high, df_hierarchy_low, "p_prs_merged_360d", 2, 0,
  "Prop. of PRs merged within 360 days")

opened_prs <- EventStudyGrid(
  df, df_full, df_hierarchy_high, df_hierarchy_low, "opened_prs", 2, 0,
  "Prop. of PRs opened")

merged_prs <- EventStudyGrid(
  df, df_full, df_hierarchy_high, df_hierarchy_low, "merged_prs", 2, 0,
  "# of PRs merged")

pr_reviews <- EventStudyGrid(
  df, df_full, df_hierarchy_high, df_hierarchy_low, "mean_reviews_per_pr", 2, 0,
  "Mean # of reviews per PR")

pr_review_comments <- EventStudyGrid(
  df, df_full, df_hierarchy_high, df_hierarchy_low, "mean_review_comments_per_pr", 2, 0,
  "Mean # of review comments per PR")
