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
  ReadParquetDate(path) %>%
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

CreateCovariateBinsTreated <- function(df, covars_to_split, time_period_col = "time_index", k_values = 2:4) {
  df_k_avg <- data.frame(repo_name = unique(df$repo_name))
  for (k in k_values) {
    df_k_mean <- df %>% 
      filter(!!sym(time_period_col) < treatment_group,
             time_index < treatment_group,
             time_index >= treatment_group - k) %>%
      group_by(repo_name) %>% 
      summarize(across(all_of(covars_to_split), ~ mean(.x, na.rm = TRUE), .names = "{.col}_{k}p_back"))
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

CreateCovariateBinsControl <- function(df, covars_to_split, df_covariate_bin_nyt, 
                                       time_period_col = "time_index", 
                                       k_values = 2:3) {
  df_k_avg <- data.frame(repo_name = unique(df$repo_name))
  for (k in k_values) {
    df_k_mean <- df %>% 
      filter(is.na(treatment_group)) %>%
      filter(!!sym(time_period_col) < all_treatment_group,
             time_index < all_treatment_group,
             time_index >= all_treatment_group - k) %>%
      group_by(repo_name) %>% 
      summarize(across(all_of(covars_to_split), ~ mean(.x, na.rm = TRUE), .names = "{.col}_{k}p_back"))
    pattern <- paste0("_", k, "p_back$")
    target_cols <- names(df_k_mean)[grepl(pattern, names(df_k_mean))]
    # For each covariate, calculate thresholds using the rolling data,
    # then create binned values. Note that we follow the original approach:
    #   - overall_mean: mean(value)
    #   - overall_median: median(value)
    #   - thirds_bounds: 33.33% and 66.67% quantiles
    #   - quartile_bounds: 25%, 50%, and 75% quantiles
    # In addition, values lower than the minimum of the window are labeled "OOLB"
    # and those above the maximum are labeled "OOUB".
      for (col in target_cols) {
        overall_mean   <- mean(df_covariate_bin_nyt[[col]], na.rm = TRUE)
        overall_median <- median(df_covariate_bin_nyt[[col]], na.rm = TRUE)
        thirds_bounds  <- quantile(df_covariate_bin_nyt[[col]], 
                                   probs = c(0.3333, 0.6667), na.rm = TRUE)
        quartile_bounds <- quantile(df_covariate_bin_nyt[[col]], 
                                    probs = c(0.25, 0.5, 0.75), na.rm = TRUE)
        
        # Define the valid range as the min and max of the rolling window.
        lower_bound <- min(df_covariate_bin_nyt[[col]], na.rm = TRUE)
        upper_bound <- max(df_covariate_bin_nyt[[col]], na.rm = TRUE)
        
        
        df_k_mean[[paste0(col, "_bin_mean")]] <- ifelse(df_k_mean[[col]] < lower_bound, "OOLB",
                                                  ifelse(df_k_mean[[col]] > upper_bound, "OOUB",
                                                         ifelse(df_k_mean[[col]] >= overall_mean, 1L, 0L)))
        df_k_mean[[paste0(col, "_bin_median")]] <- ifelse(df_k_mean[[col]] < lower_bound, "OOLB",
                                                    ifelse(df_k_mean[[col]] > upper_bound, "OOUB",
                                                           ifelse(df_k_mean[[col]] > overall_median, 1L, 0L)))
        temp_third <- tryCatch({
          as.integer(cut(df_k_mean[[col]], 
                         breaks = c(-Inf, thirds_bounds[1], thirds_bounds[2], Inf),
                         labels = c(3L, 2L, 1L), include.lowest = TRUE))
        }, error = function(e) rep(NA_integer_, nrow(df_k_mean)))
        
        third_bin_col <- paste0(col, "_bin_third")
        df_k_mean[[third_bin_col]] <- ifelse(df_k_mean[[col]] < lower_bound, "OOLB",
                                                   ifelse(df_k_mean[[col]] > upper_bound, "OOUB", temp_third))
        
        # Categorical binning for quartiles.
        quart_lower <- min(quartile_bounds, na.rm = TRUE)
        quart_upper <- max(quartile_bounds, na.rm = TRUE)
        temp_quart <- tryCatch({
          as.integer(cut(df_k_mean[[col]], 
                         breaks = c(-Inf, quartile_bounds[1], quartile_bounds[2], quartile_bounds[3], Inf),
                         labels = c(4L, 3L, 2L, 1L), include.lowest = TRUE))
        }, error = function(e) rep(NA_integer_, nrow(df_k_mean)))
        
        quart_bin_col <- paste0(col, "_bin_quartile")
        df_k_mean[[quart_bin_col]] <- ifelse(df_k_mean[[col]] < lower_bound, "OOLB",
                                                   ifelse(df_k_mean[[col]] > upper_bound, "OOUB", temp_quart))
      }
      df_k_avg <- df_k_avg %>% left_join(df_k_mean)
    }
  df_k_avg  
}
  
spec_covariates <- list(
  imp_contr = c("total_important"),
  more_imp = c("normalized_degree"),
  imp_contr_more_imp = c("total_important", "normalized_degree"),
  imp_ratio = c("prop_important"),
  indiv_clus = c("overall_overlap"),
  indiv_clus_more_imp = c("overall_overlap", "normalized_degree"),
  project_clus_ov = c("mean_cluster_overlap"),
  project_clus_ov_more_imp  = c("mean_cluster_overlap","normalized_degree"),
  project_clus_ov_imp_contr = c("mean_cluster_overlap","total_important"),
  project_clus_ov_more_imp_imp_contr = c("mean_cluster_overlap","normalized_degree", "total_important"),
  project_clus_node = c("avg_clusters_per_node"),
  project_clus_pct_one = c("pct_nodes_one_cluster"),
  indiv_cluster_size = c("overall_overlap", "total_important"),
  indiv_cluster_ov_cluster = c("overall_overlap", "mean_cluster_overlap"),
  imp_imp_comm_dept = c("imp_to_imp_avg_edge_weight"),
  imp_other_comm_dept = c("imp_to_other_avg_edge_weight"),
  both_comm_dept = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight"),
  comm_imp_more_imp_dept = c("normalized_degree", "imp_to_imp_avg_edge_weight"),
  comm_within_more_imp_dept = c("normalized_degree", "imp_to_other_avg_edge_weight"),
  both_comm_cluster_dept = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight", "overall_overlap"),
  both_comm_ov_cluster_dept = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight", "mean_cluster_overlap"),
  comm_cluster_dept = c("imp_to_imp_avg_edge_weight", "overall_overlap"),
  comm_within_cluster_dept = c("imp_to_other_avg_edge_weight", "overall_overlap"),
  imp_imp_comm = c("imp_to_imp_overall"),
  imp_other_comm = c("imp_to_other_overall"),
  both_comm = c("imp_to_imp_overall", "imp_to_other_overall"),
  comm_imp_more_imp = c("normalized_degree", "imp_to_imp_overall"),
  comm_within_more_imp = c("normalized_degree", "imp_to_other_overall"),
  project_clus_ov_imp_imp_comm = c("mean_cluster_overlap","imp_to_imp_overall"),
  project_clus_ov_imp_other_comm = c("mean_cluster_overlap","imp_to_other_overall"),
  project_clus_ov_imp_imp_comm_more_imp = c("mean_cluster_overlap","imp_to_imp_overall", "normalized_degree"),
  project_clus_ov_imp_other_comm_more_imp = c("mean_cluster_overlap","imp_to_other_overall", "normalized_degree"),
  both_comm_cluster = c("imp_to_imp_overall", "imp_to_other_overall", "overall_overlap"),
  both_comm_ov_cluster = c("imp_to_imp_overall", "imp_to_other_overall", "mean_cluster_overlap"),
  comm_cluster = c("imp_to_imp_overall", "overall_overlap"),
  comm_within_cluster = c("imp_to_other_overall", "overall_overlap")
)


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
  nyt_covariates <- LoadCovariates(dirs$covariates, TRUE)
  covariates <- LoadCovariates(dirs$covariates, FALSE)
  downloads <- LoadGithubMetrics(dirs$temp, "downloads")
  forks_stars <- LoadGithubMetrics(dirs$temp, "forks_stars")
  downloads_detailed <- LoadGithubMetrics(dirs$temp, "downloads_detailed")
  duplicates <- downloads_detailed %>% group_by(repo_name, time_period) %>% 
    filter(n() > 1) %>% arrange(repo_name, time_period)
  downloads_detailed <- downloads_detailed %>%
    anti_join(duplicates, by = c("repo_name", "time_period"))
  
  ### LOOK INTO WHY THEY ARE DUPLICATES AND RESOLVE
  software_score <- LoadGithubMetrics(dirs$temp, "scorecard_scores")
  
  df_panel_nyt <- CreateEventStudyPanel(project_outcomes, nyt_covariates, downloads, forks_stars, downloads_detailed, software_score, not_yet_treated = TRUE)
  df_panel <- CreateEventStudyPanel(project_outcomes, covariates, downloads, forks_stars, downloads_detailed, software_score, not_yet_treated = FALSE)
  covars_to_split <- unique(unlist(spec_covariates))
  df_cov_panel_treated <- CreateCovariateBinsTreated(df_panel_nyt, covars_to_split)
  covars_to_split_sel <- intersect(colnames(df_panel), covars_to_split)
  df_cov_panel_control <- CreateCovariateBinsControl(df_panel, covars_to_split_sel, df_cov_panel_treated)
  df_cov_panel_control <- df_cov_panel_control %>%
    mutate(across(.cols = -repo_name, .fns = ~ as.numeric(.)))
  WritePanel(df_panel, dirs$temp, paste0("event_study_panel.parquet"))
  WritePanel(df_cov_panel_treated, dirs$temp, paste0("nyt_covariate_bins.parquet"))
  WritePanel(df_cov_panel_control, dirs$temp, paste0("covariate_bins.parquet"))
    
  for (sample_restriction in sample_restrictions[-1]) {
    project_outcomes_suffix <- LoadProjectOutcomes(dirs$outcomes, params$time_period, sample_restriction)
    panel_sample_restrict <- CreateEventStudyPanel(project_outcomes_suffix, covariates, downloads, forks_stars,  downloads_detailed, software_score, not_yet_treated = FALSE)
    WritePanel(panel_sample_restrict, dirs$temp, paste0("event_study_panel", sample_restriction, ".parquet"))
  }
}

Main()
