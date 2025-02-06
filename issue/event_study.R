
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

df_treatment_group <- df_project_departed %>%
  filter(treatment == 1) %>%
  group_by(repo_name) %>%
  summarize(treatment_group = min(treatment*time_index))
df_project_departed <- df_project_departed %>% left_join(df_treatment_group)

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
    pct_coop_commits_100_count              = pct_cooperation_commits_100_count,
    pct_coop_commits_count              = pct_cooperation_commits_count,
    pct_coop_comments = pct_cooperation_comments,
    problem_id_HHI = problem_identification_HHI
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
  "problem_identification_share",
  "problem_discussion_share",
  "coding_share",
  "problem_approval_share",
  "comments_hhi_avg_wt",
  "pct_coop_comments",
  "pct_coop_commits_count"
)

contributor_covars_long <- c(
  contributor_covars,
  "contributor_email_educational",
  "contributor_email_corporate"
) 

org_structure <- c(
  "min_layer_count",
  "problem_disc_hl_work",
  "coding_hl_work",
  "total_HHI",
  "contributors_comments_wt",
  "comments_cooperation_pct",
  "problem_disc_hl_contr_ov",
  "coding_hl_contr_ov",
  "problem_id_HHI",
  "problem_discussion_HHI",
  "problem_approval_HHI",
  "coding_HHI",
  "hhi_comments",
  "hhi_commits_count"
)

org_structure_long <- c(
  org_structure,
  "layer_count",
  "problem_id_layer_contr_pct",
  "problem_disc_layer_contr_pct",
  "coding_layer_contr_pct",
  "problem_app_layer_contr_pct",
  "problem_disc_layer_contr_cnt"
)

outcomes <- c("issues_opened","issue_comments","own_issue_comments",
              "helping_issue_comments","pr_comments","prs_opened", "commits", "prs_merged",
              "closed_issue")



avg_var_list <- c(org_structure,
                     contributor_covars[contributor_covars %ni% c("truckfactor_member","max_rank")])

df_combined <- bind_cols(
  df_project_departed %>% filter(treated_project == 1 & treatment == 0) %>%
    summarise(across(all_of(avg_var_list), ~ mean(.x, na.rm = TRUE), .names = "{.col}_nyt_avg")),
  df_project_departed %>%
    filter(treatment == 0) %>%
    summarise(across(all_of(avg_var_list), ~ mean(.x, na.rm = TRUE), .names = "{.col}_all_avg")))

for (outcome in outcomes) {
  norm_outcome <- df_project_departed %>% 
    filter(time_index < treatment_group) %>% 
    summarise(mean(get(outcome), na.rm = T)) %>% 
    pull()
  df_project_departed[[outcome]] <- df_project_departed[[outcome]]/norm_outcome
}

df_project_departed <- cbind(df_project_departed, df_combined) %>%
  select(all_of(c(project_covars, contributor_covars_long, org_covars_long, org_structure_long, outcomes,
                  paste0(avg_var_list,"_nyt_avg"))))

make_bins <- c("total_share","comments_hhi_avg_wt","pct_coop_comments","pct_coop_commits_count",
               "min_layer_count","problem_disc_hl_contr_ov","coding_hl_contr_ov","problem_disc_hl_work",
               "coding_hl_work","total_HHI","problem_discussion_HHI","coding_HHI","problem_approval_HHI",
               "hhi_comments","hhi_commits_count", "contributors", "problem_identification_share",
               "problem_discussion_share", "coding_share","problem_approval_share")

df_bin2 <- df_project_departed %>%
  filter(time_index >= treatment_group - 2 & time_index < treatment_group) %>%
  group_by(repo_name) %>%
  summarize(across(all_of(make_bins), list(
           bin_2 = ~ if_else(mean(.x, na.rm=TRUE) > get(paste0(cur_column(), "_nyt_avg")), 1, 0)
         ), .names = "{.col}_{.fn}")) %>%
  unique()
df_bin3 <- df_project_departed %>%
  filter(time_index >= treatment_group - 3 & time_index < treatment_group) %>%
  group_by(repo_name) %>%
  summarize(across(all_of(make_bins), list(
    bin_3 = ~ if_else(mean(.x, na.rm=TRUE) > get(paste0(cur_column(), "_nyt_avg")), 1, 0)
  ), .names = "{.col}_{.fn}")) %>%
  unique()
df_project_departed <- df_project_departed %>% left_join(df_bin2) %>% left_join(df_bin3)

df_project_departed %>%
  write_dta("issue/df_project_departed.dta")


summarized_share_situation <- summarized_data <- df_project_departed %>%
  filter(time_index >= (treatment_group - 2) & time_index < treatment_group) %>%
  group_by(total_share_bin_2) %>%
  summarize(across(c(project_age, contributors, commits, min_layer_count, treatment_group),
                   ~ mean(.x, na.rm = TRUE), .names = "avg_{.col}")) %>%
  arrange(total_share_bin_2)

kable_output <- summarized_data %>%
  mutate(across(starts_with("avg_"), ~ round(.x, 1)) ) %>%
  kable(caption = "Summary of Projects by Share Bin", 
        col.names = c("Above Avg. Total Share", "Average Project Age", "Average Contributors", "Average Commits", "Average Layers", "Average Treatment Date"),
        format = "html", table.attr = "style='width:50%;'") %>%
  kable_styling(bootstrap_options = c("striped", "hover", "condensed", "responsive"), full_width = FALSE, position = "left")

df_project_departed
df_project_departed %>% filter(time_period == final_period)
