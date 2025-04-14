library(tidyverse)
library(arrow)
library(lubridate)

set.seed(1234)
`%ni%` <- Negate(`%in%`)

CleanProjectOutcomes <- function(indir, time_period, suffix) {
  df_outcomes_full <- read_parquet(file.path(indir, paste0("project_outcomes_major_months", time_period, ".parquet"))) %>% 
    mutate(time_period = as.Date(time_period)) %>%
    filter(year(time_period) < 2023 | (year(time_period) == 2023 & month(time_period) == 1)) %>%
    mutate(time_index = dense_rank(time_period))
  if (suffix != "") {
    df_outcomes_suffix <- read_parquet(file.path(indir, paste0("project_outcomes_major_months", time_period, suffix, ".parquet"))) %>% 
      mutate(time_period = as.Date(time_period)) %>%
      filter(year(time_period) < 2023 | (year(time_period) == 2023 & month(time_period) == 1))
    df_outcomes_full <- df_outcomes_full %>% 
      select(repo_name, time_period, time_index) %>%
      left_join(df_outcomes_suffix)
  }
  return(df_outcomes_full)
}

CleanDepartedContributors <- function(indir_departed, time_period, rolling_window, criteria_pct, consecutive_periods, post_periods) {
  df_dep <- read_parquet(file.path(indir_departed, paste0(
    "filtered_departed_contributors_major_months", time_period, "_window",
    rolling_window, "D_criteria_commits_", criteria_pct, "pct_consecutive", consecutive_periods,
    "_post_period", post_periods, "_threshold_gap_qty_0.parquet"
  )))
  df_clean <- df_dep %>% 
    filter(present_one_after == 0) %>% 
    filter(year(treatment_period) < 2023 & !abandoned_scraped & !abandoned_consecutive_req3_permanentTrue) %>%
    group_by(repo_name) %>% mutate(repo_count = n()) %>% ungroup() %>% 
    filter(repo_count == 1) %>% 
    mutate(departed_actor_id = actor_id, abandoned_date = as.Date(abandoned_date_consecutive_req2_permanentTrue)) %>% 
    select(repo_name, departed_actor_id, last_pre_period, treatment_period, abandoned_date)
  treatment_inelg <- df_dep %>% filter(repo_name %ni% df_clean$repo_name) %>% pull(repo_name)
  list(df_departed = df_clean, treatment_inelg = treatment_inelg)
}

CleanDepartedContributorsGraph <- function(issue_tempdir) {
  df_dep <- read_parquet(file.path(issue_tempdir, "graph_departures.parquet"))
  df_clean <- df_dep %>% group_by(repo_name) %>% mutate(repo_count = n()) %>% ungroup() %>% 
    filter(repo_count == 1) %>% mutate(departed_actor_id = as.numeric(actor_id)) %>% 
    rename(treatment_period = max_time_period) %>% select(repo_name, departed_actor_id, last_pre_period, treatment_period, abandoned_date)
  treatment_inelg <- df_dep %>% filter(repo_name %ni% df_clean$repo_name) %>% pull(repo_name)
  list(df_departed = df_clean, treatment_inelg = treatment_inelg)
}

CleanCovariates <- function(indir) {
  read_parquet(file.path(indir, "contributor_characteristics.parquet")) %>%
    mutate(time_period = as.Date(time_period)) %>%
    mutate(actor_id = as.numeric(actor_id)) %>%
    rename(departed_actor_id = actor_id)
}

CleanDownloads <- function(issue_tempdir) {
  read_parquet(file.path(issue_tempdir, "github_downloads.parquet")) %>%
    mutate(time_period = as.Date(time_period)) %>%
    group_by(time_period) %>%
    select(repo_name, time_period, total_downloads, total_downloads_one_project)
}

CleanForkStars <- function(issue_tempdir) {
  read_parquet(file.path(issue_tempdir, "github_forks_stars.parquet")) %>%
    mutate(time_period = as.Date(time_period))
}

CreateDeparturePanel <- function(df_project, treatment_inelg, df_departed, df_covars, df_downloads, df_fork_stars) {
  df_panel <- df_project %>% 
    left_join(df_departed, by = "repo_name") %>% 
    mutate(treated_project = 1 - as.numeric(is.na(last_pre_period)),
           treatment = as.numeric(treated_project & time_period >= treatment_period),
           final_period = as.Date(final_period),
           last_pre_period = as.Date(last_pre_period))
  df_panel <- df_panel %>% filter(time_period < abandoned_date | is.na(abandoned_date))
  treatment_group <- df_panel %>% filter(treatment == 1) %>% group_by(repo_name) %>% summarize(treatment_group = min(treatment * time_index))
  df_panel %>% 
    left_join(treatment_group, by = "repo_name") %>% 
    left_join(df_covars, by = c("repo_name", "departed_actor_id", "time_period")) %>% 
    left_join(df_downloads, by = c("repo_name", "time_period")) %>% 
    left_join(df_fork_stars, by = c("repo_name", "time_period"))
}

CleanOutcomesPost <- function(issue_tempdir, df_panel_nyt) {
  treated_projects <- df_panel_nyt$repo_name
  df_downloads <- read_parquet(file.path(issue_tempdir, "github_downloads.parquet")) %>%
    select(repo_name, time_period, total_downloads, total_downloads_one_project) %>%
    filter(repo_name %in% treated_projects) %>%
    mutate(total_downloads_period = sum(total_downloads),
           total_downloads_one_project_period = sum(total_downloads_one_project)) %>% ungroup() %>%
    mutate(total_downloads_rel = total_downloads / total_downloads_period,
           total_downloads_one_project_rel = total_downloads_one_project / total_downloads_one_project_period) %>%
    select(repo_name, time_period, total_downloads_rel, total_downloads_one_project_rel)
  df_downloads_det <- read_parquet(file.path(issue_tempdir, "github_downloads_detailed.parquet")) %>%
    filter(repo_name %in% treated_projects) %>%
    select(-ends_with("downloads")) %>%
    group_by(repo_name, time_period)  %>% summarise(across(everything(), sum, na.rm = TRUE))
  df_scorecard <- read_parquet(file.path(issue_tempdir, "github_scorecard_scores.parquet")) %>%
    filter(repo_name %in% treated_projects) %>% 
    select(repo_name, time_period, overall_score, overall_increase, overall_decrease, overall_stable, vulnerabilities_score)
  df_downloads %>% 
    full_join(df_downloads_det) %>% 
    full_join(df_scorecard) %>% 
    mutate(time_period = as.Date(time_period))
}

CreateCovariateBins <- function(df, covariates, time_period_col = "time_index", k_values = 1:3) {
  df_k_avg <- data.frame(repo_name = unique(df$repo_name))
  for (k in k_values) {
    df_k_mean <- df %>% 
      filter(!!sym(time_period_col) < treatment_group,
             time_index < treatment_group,
             time_index >= treatment_group - k) %>%
      group_by(repo_name) %>% 
      summarize(across(all_of(covariates), ~ mean(.x, na.rm = TRUE), .names = "{.col}_{k}p_back"))
    pattern <- paste0("_", k, "p_back$")
    target_cols <- names(df_k_mean)[grepl(pattern, names(df_k_mean))]
    for (col in target_cols) {
      overall_mean <- mean(df_k_mean[[col]], na.rm = TRUE)
      overall_median <- median(df_k_mean[[col]], na.rm = TRUE)
      thirds_bounds <- quantile(df_k_mean[[col]], probs = c(0.3333, 0.6667), na.rm = TRUE)
      quartile_bounds <- quantile(df_k_mean[[col]], probs = c(0.25, 0.5, 0.75), na.rm = TRUE)
      df_k_mean[[paste0(col, "_bin_mean")]] <- ifelse(df_k_mean[[col]] >= overall_mean, 1L, 0L)
      df_k_mean[[paste0(col, "_bin_median")]] <- ifelse(df_k_mean[[col]] > overall_median, 1L, 0L)
      df_k_mean[[paste0(col, "_bin_third")]] <- tryCatch({
        as.integer(cut(df_k_mean[[col]], breaks = c(-Inf, thirds_bounds[1], thirds_bounds[2], Inf),
                       labels = c(3L, 2L, 1L), include.lowest = TRUE))
      }, error = function(e) rep(NA_integer_, nrow(df_k_mean)))
      df_k_mean[[paste0(col, "_bin_quartile")]] <- tryCatch({
        as.integer(cut(df_k_mean[[col]], breaks = c(-Inf, quartile_bounds[1], quartile_bounds[2], quartile_bounds[3], Inf),
                       labels = c(4L, 3L, 2L, 1L), include.lowest = TRUE))
      }, error = function(e) rep(NA_integer_, nrow(df_k_mean)))
    }
    df_k_avg <- df_k_avg %>% left_join(df_k_mean)
  }
  df_k_avg
}

PreparePanelData <- function(indir, indir_departed, issue_tempdir, indir_contributors,
                             time_period, rolling_window, criteria_pct, consecutive_periods, post_periods, graph_flag, suffix) {
  df_project <- CleanProjectOutcomes(indir, time_period, suffix)
  if (!graph_flag) {
    departed <- CleanDepartedContributors(indir_departed, time_period, rolling_window, criteria_pct, consecutive_periods, post_periods)
    outdir <- "issue/event_study/graphs"
  } else {
    departed <- CleanDepartedContributorsGraph(issue_tempdir)
    outdir <- "issue/event_study/graph_departures"
  }
  df_covars <- CleanCovariates(indir_contributors)
  df_downloads <- CleanDownloads(issue_tempdir)
  df_fork_stars <- CleanForkStars(issue_tempdir)
  df_panel <- CreateDeparturePanel(df_project, departed$treatment_inelg, departed$df_departed, df_covars, df_downloads, df_fork_stars)
  df_panel_nyt <- unique(df_panel %>% filter(treated_project == 1))
  df_outcomes <- CleanOutcomesPost(issue_tempdir, df_panel_nyt)
  df_panel_nyt <- df_panel_nyt %>% left_join(df_outcomes)
  list(panel = df_panel_nyt, outdir = outdir)
}

spec_covariates <- list(
  imp_contr = c("total_important"),
  more_imp = c("normalized_degree"),
  imp_contr_more_imp = c("total_important", "normalized_degree"),
  imp_ratio = c("prop_important"),
  indiv_clus = c("overall_overlap"),
  project_clus_ov = c("mean_cluster_overlap"),
  project_clus_node = c("avg_clusters_per_node"),
  project_clus_pct_one = c("pct_nodes_one_cluster"),
  indiv_cluster_size = c("overall_overlap", "total_important"),
  indiv_cluster_impo = c("overall_overlap", "normalized_degree"),
  indiv_cluster_ov_cluster = c("overall_overlap", "mean_cluster_overlap"),
  imp_imp_comm = c("imp_to_imp_avg_edge_weight"),
  imp_other_comm = c("imp_to_other_avg_edge_weight"),
  both_comm = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight"),
  comm_imp_more_imp = c("normalized_degree", "imp_to_imp_avg_edge_weight"),
  comm_within_more_imp = c("normalized_degree", "imp_to_other_avg_edge_weight"),
  both_comm_cluster = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight", "overall_overlap"),
  both_comm_ov_cluster = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight", "mean_cluster_overlap"),
  comm_cluster = c("imp_to_imp_avg_edge_weight", "overall_overlap"),
  comm_within_cluster = c("imp_to_other_avg_edge_weight", "overall_overlap")
)

Main <- function() {
  params <- list(
    time_period = 6,
    rolling_window = 732,
    criteria_pct = 75,
    consecutive_periods = 3,
    post_periods = 2,
    graph_flag = FALSE
  )
  
  indir <- "drive/output/derived/project_outcomes"
  indir_departed <- "drive/output/derived/contributor_stats/filtered_departed_contributors"
  issue_tempdir <- "issue"
  indir_contributors <- "drive/output/derived/graph_structure"
  
  prep <- PreparePanelData(indir, indir_departed, issue_tempdir, indir_contributors,
                           params$time_period, params$rolling_window, params$criteria_pct, params$consecutive_periods, params$post_periods, params$graph_flag,
                           suffix = "")
  df_panel <- prep$panel
  SaveSampleData(df_panel, df_panel %>% filter(time_period <= final_period), outdir)
  write_parquet(df_panel, file.path(issue_tempdir, "event_study_panel.parquet")) # NTS repetitive, delete
  
  
  covars_to_split <- unique(unlist(spec_covariates))
  df_cov_panel <- CreateCovariateBins(df_panel, covars_to_split)
  write_parquet(df_cov_panel, file.path("issue", "covariate_bins.parquet"))
  
  for (suffix in paste0("_", c("imp","all","unimp","new"))) { # add departed later on?
    df_panel_suffix <- PreparePanelData(indir, indir_departed, issue_tempdir, indir_contributors,
                             params$time_period, params$rolling_window, params$criteria_pct, params$consecutive_periods, params$post_periods, params$graph_flag,
                             suffix = suffix)$panel
    write_parquet(df_panel_suffix, file.path(issue_tempdir, paste0("event_study_panel", suffix, ".parquet")))
  }
}

Main()
