library(tidyverse)
library(arrow)
library(lubridate)

set.seed(1234)
`%notin%` <- Negate(`%in%`)

# Helper Functions ----------------------------------------------------------
ReadParquetDate <- function(file, date_cols = c("time_period")) {
  read_parquet(file) %>%
    mutate(across(all_of(date_cols), as.Date))
}

WritePanel <- function(df, dir, filename) {
  write_parquet(df, file.path(dir, filename))
}

# Data Cleaning Functions ---------------------------------------------------
LoadProjectOutcomes <- function(dir, period, suffix = "") {
  path <- file.path(dir, sprintf("project_outcomes_major_months%d%s.parquet", period, suffix))
  df <- ReadParquetDate(path, c("time_period","treatment_period")) %>%
    filter(year(time_period) < 2023 | (year(time_period) == 2023 & month(time_period) == 1)) %>%
    mutate(time_index = dense_rank(time_period))
}


LoadCovariates <- function(dir, nyt = FALSE) {
  covariates <- ReadParquetDate(file.path(dir, "contributor_characteristics.parquet")) %>%
    mutate(actor_id = as.numeric(actor_id))
  if (!nyt) {
    covariates <- covariates %>%
      select(-actor_id, -imp_to_other_avg_edge_weight, -imp_to_imp_avg_edge_weight, -overall_overlap) %>%
      distinct(repo_name, time_period, .keep_all = TRUE) %>%
      rename(graph_importance = normalized_degree)
  } else {
    covariates <- rename(covariates, departed_actor_id = actor_id)
  }
  covariates
}

LoadGithubMetrics <- function(dir, type) {
  df_metrics <- ReadParquetDate(file.path(dir, sprintf("github_%s.parquet", type)))
  if (type == "downloads") {
    df_metrics <- df_metrics %>%  
      select(repo_name, time_period, total_downloads, total_downloads_one_project) %>%
      mutate(total_downloads_period = sum(total_downloads),
             total_downloads_one_project_period = sum(total_downloads_one_project)) %>% ungroup() %>%
      mutate(total_downloads_rel = total_downloads / total_downloads_period,
             total_downloads_one_project_rel = total_downloads_one_project / total_downloads_one_project_period)
  } else if (type == "detailed_downloads") {
    df_metrics <- df_metrics %>%  
      select(-ends_with("downloads")) %>%
      group_by(repo_name, time_period)  %>% summarise(across(everything(), sum, na.rm = TRUE))
  } else if (type == "scorecard_scores") {
    df_metrics <- df_metrics %>%
      select(repo_name, time_period, overall_score, overall_increase, overall_decrease, overall_stable, vulnerabilities_score)
  }
  return(df_metrics)
}

# Panel Creation Function ---------------------------------------------------
CreateEventStudyPanel <- function(project_outcomes, covariate_data, github_downloads, github_forks_stars, downloads_detailed, software_score, not_yet_treated = FALSE) {
  panel <- project_outcomes %>%
    mutate(treated = !is.na(treatment_group),
           treatment = as.integer(treated & time_index >= treatment_group)) %>%
    filter(is.na(abandoned_date) | time_period < abandoned_date) %>%
    left_join(github_forks_stars, by = c("repo_name", "time_period")) %>%
    left_join(github_downloads, by = c("repo_name", "time_period")) %>%
    left_join(covariate_data, by = if (not_yet_treated) c("repo_name", "departed_actor_id", "time_period") else c("repo_name", "time_period")) %>%
    left_join(downloads_detailed, by = c("repo_name", "time_period")) %>%
    left_join(software_score, by = c("repo_name", "time_period"))
  panel
}

# Main Process --------------------------------------------------------------
Main <- function() {
  # Set paths and parameters for loading data and generating event study panels
  dirs <- list(
    outcomes = "drive/output/derived/project_outcomes",
    departed = "drive/output/derived/contributor_stats/filtered_departed_contributors",
    temp = "issue",
    covariates = "drive/output/derived/graph_structure"
  )
  params <- list(time_period = 6, rolling_window = 732, criteria_pct = 75, consecutive_periods = 3, post_periods = 2)
  sample_restrictions <- c("", "_imp", "_all", "_unimp", "_new", "_alltime")  # Different contributor subsets  # Different contributor subsets
  
  project_outcomes <- LoadProjectOutcomes(dirs$outcomes, params$time_period)
  nyt_covariates <- LoadCovariates(dirs$covariates, nyt = TRUE)
  downloads <- LoadGithubMetrics(dirs$temp, "downloads")
  forks_stars <- LoadGithubMetrics(dirs$temp, "forks_stars")
  downloads_detailed <- LoadGithubMetrics(dirs$temp, "downloads_detailed")
  software_score <- LoadGithubMetrics(dirs$temp, "scorecard_scores")
  
  ### LOOK INTO WHY THEY ARE DUPLICATES AND RESOLVE
  duplicates <- downloads_detailed %>% group_by(repo_name, time_period) %>% 
    filter(n() > 1) %>% arrange(repo_name, time_period)
  downloads_detailed <- downloads_detailed %>%
    anti_join(duplicates, by = c("repo_name", "time_period"))
  
  df_panel_nyt <- CreateEventStudyPanel(project_outcomes, nyt_covariates, downloads, forks_stars, downloads_detailed, software_score, not_yet_treated = TRUE)
  df_panel_nyt <- df_panel_nyt  %>%
    inner_join(ReadParquetDate("issue/project_collaboration.parquet", c("time_period", "treatment_period")))
  
  
  
}

Main()
