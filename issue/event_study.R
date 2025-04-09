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
library(purrr)
library(future.apply)
library(gridExtra)
library(SaveData)
library(ragg)
library(zoo)
library(patchwork)
library(lmtest)
library(multiwayvcov)

set.seed(1234)
`%ni%` <- Negate(`%in%`)

# SEPARATE INTO CLEANING, EVENT STUDY AND BIN SCATETR
Main <- function() {
  set.seed(1234)
  indir <- "drive/output/derived/project_outcomes"
  indir_departed <- "drive/output/derived/contributor_stats/filtered_departed_contributors"
  issue_tempdir <- "issue"
  indir_contributors <- "drive/output/derived/graph_structure"
  outdir_commit_departures <- "issue/event_study/graphs"
  outdir_graph_departures <- "issue/event_study/graph_departures"
  outdir_binscatter <- "issue/event_study/binscatter"
  time_period <- 6
  rolling_window <- 732
  criteria_pct <- 75
  consecutive_periods <- 3
  post_periods <- 2
  
  graph_departures <- FALSE
  specification_covariates <- list(
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
    indiv_cluster_ov_cluster = c("overall_overlap","mean_cluster_overlap"),
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
  
  outcomes <- c(
    "prs_opened", "commits", "commits_lt100", "comments", "issue_comments", "pr_comments",
    "issues_opened", "issues_closed", "prs_merged", "p_prs_merged", "closed_issue",
    "p_prs_merged_30d", "p_prs_merged_60d", "p_prs_merged_90d", "p_prs_merged_180d",
    "p_prs_merged_360d", "closed_in_30_days", "closed_in_60_days", "closed_in_90_days",
    "total_downloads","total_downloads_one_project", "total_downloads_rel", "total_downloads_one_project_rel",
    "closed_in_180_days", "closed_in_360_days",  
    "overall_new_release_count", "major_new_release_count",
    "minor_new_release_count", "patch_new_release_count", "other_new_release_count", "major_minor_release_count",
    "major_minor_patch_release_count", "overall_score", "overall_increase",
    "overall_decrease", "overall_stable", "vulnerabilities_score",
    "total_nodes", "forks_gained","stars_gained")
  
  fillna <- outcomes[
    !(grepl("downloads", outcomes, ignore.case = TRUE) |
      grepl("^p_", outcomes) |
      grepl("^closed_in_", outcomes) |
      outcomes %in% c("overall_score", "overall_increase", "overall_decrease", "overall_stable", "vulnerabilities_score"))
  ]


  if (graph_departures) {
    na_keep_cols <- c()
  } else {
    na_keep_cols <- c("overall_overlap")
  }
  
  df_project_outcomes <- CleanProjectOutcomes(indir, time_period)
  
  if (!graph_departures) {
    departed_list <- CleanDepartedContributors(indir_departed, time_period, rolling_window, criteria_pct, consecutive_periods, post_periods)
    outdir <- outdir_commit_departures
  } else {
    departed_list <- CleanDepartedContributorsGraph(issue_tempdir)
    outdir <- outdir_graph_departures
  }
  
  df_departed <- departed_list$df_departed
  treatment_inelg <- departed_list$treatment_inelg
  df_covariates <- CleanCovariates(indir_contributors)
  df_downloads <- CleanDownloads(issue_tempdir)
  df_fork_stars <- CleanForkStars(issue_tempdir)
  departure_panel <- CreateDeparturePanel(df_project_outcomes, treatment_inelg, df_departed, df_covariates, df_downloads, df_fork_stars)
  
  all_projects <- unique(departure_panel$repo_name)
  treated_projects <- unique(departure_panel %>% filter(treatment == 1) %>% pull(repo_name))
  treated_projects_count <- length(treated_projects)
  control_projects <- unique(departure_panel %>% filter(repo_name %ni% treated_projects) %>% pull(repo_name))
  control_projects_count <- length(control_projects)

  departure_panel_nyt <- unique(departure_panel %>% filter(treated_project == 1))
  project_covars <- c("repo_name", "time_period", "treated_project", "treatment", "time_index", "treatment_group", "departed_actor_id", "final_period")
  departure_panel_na <- departure_panel_nyt %>% 
    filter(treatment_group > time_index & treatment_group <= time_index + 3) %>% 
    select(project_covars, colnames(df_covariates))
  
  print(paste("Project Count:", length(all_projects)))
  print(paste("Control Projects:", control_projects_count))
  print(paste("Treated Projects:", treated_projects_count))
  print("Distribution of missing observations for `total_important`")
  print(c(departure_panel_na %>% 
            group_by(repo_name) %>% 
            summarize(num_obs = sum(is.na(total_important))) %>% pull(num_obs) %>% table(), 
          "all" = length(unique(departure_panel_na$repo_name))))
  
  df_valuable_outcomes <- CleanOutcomesPost(issue_tempdir, departure_panel_nyt)
  departure_panel_nyt <- departure_panel_nyt %>% left_join(df_valuable_outcomes)

  covariates_to_split <- unique(unlist(specification_covariates))
  covariate_panel_nyt <- CreateCovariateBins(departure_panel_nyt, covariates_to_split)
  
  for (outcome in fillna) {
    departure_panel_nyt[[outcome]] <- ifelse(is.na(departure_panel_nyt[[outcome]]), 0, departure_panel_nyt[[outcome]])
  }
  GenerateEventStudyGrids(departure_panel_nyt, covariate_panel_nyt, outcomes, specification_covariates, 
                          post = 4, pre = 0, outdir = outdir, na_keep_cols = na_keep_cols,
                          fillna = fillna, plot = TRUE)
  
  
  release_vars <- colnames(departure_panel_pre_treatment)[grepl("release",colnames(departure_panel_pre_treatment))]
  binscatter_outcomes <- c("overall_score", "vulnerabilities_score", "forks_gained","stars_gained",
                           release_vars)
  departure_panel_pre_treatment <- departure_panel_nyt %>%
    dplyr::filter(time_index < treatment_group)
  BinScatter(data = departure_panel_pre_treatment, y_vars = binscatter_outcomes, 
             outdir = outdir_binscatter, leads = c(0, 1, 2))
  
  # pr_columns <- c("prs_opened", "prs_merged")
  # outcomes <- c("overall_new_release_count","overall_score", "total_downloads")
  # pre_values <- 1:4
  # post_values <- 1:4
  # 
  # df_effect <- ComputeEffectGrid(
  #   departure_panel_nyt, pr_columns = c("prs_opened", "prs_merged"), 
  #   outcomes = c("overall_new_release_count", "total_downloads", "overall_score"),
  #   pre_values = 1:4, post_values = 1:4, leading = TRUE)
  # 
  # ExportPlots(df_effect, pr_columns = c("prs_opened", "prs_merged"), post_values = 1:4)
  # 
}

CleanProjectOutcomes <- function(indir, time_period) {
  df_project_outcomes <- read_parquet(file.path(indir, paste0("project_outcomes_major_months", time_period, ".parquet"))) %>% 
    mutate(time_period = as.Date(time_period)) %>%
    filter(year(time_period) < 2023 | (year(time_period) == 2023 & month(time_period) == 1)) %>%
    mutate(time_index = dense_rank(time_period))
  return(df_project_outcomes)
}

CleanDepartedContributors <- function(indir_departed, time_period, rolling_window, criteria_pct, consecutive_periods, post_periods) {
  df_departed_contributors <- read_parquet(
    file.path(indir_departed, paste0("filtered_departed_contributors_major_months", time_period, "_window",
                                     rolling_window, "D_criteria_commits_", criteria_pct, "pct_consecutive", consecutive_periods,
                                     "_post_period", post_periods, "_threshold_gap_qty_0.parquet"))
  )
  df_departed <- df_departed_contributors %>% 
    filter(present_one_after == 0) %>% 
    filter(year(treatment_period) < 2023 & !abandoned_scraped & !abandoned_consecutive_req3_permanentTrue) %>%
    group_by(repo_name) %>% 
    mutate(repo_count = n()) %>%
    ungroup() %>%
    filter(repo_count == 1) %>%
    mutate(departed_actor_id = actor_id,
           abandoned_date = as.Date(abandoned_date_consecutive_req2_permanentTrue)) %>%
    select(repo_name, departed_actor_id, last_pre_period, treatment_period, abandoned_date)
  treatment_inelg <- df_departed_contributors %>% 
    filter(repo_name %ni% df_departed$repo_name) %>%
    pull(repo_name)
  treatment_elg <- unique(df_departed$repo_name)
  return(list(df_departed = df_departed, treatment_inelg = treatment_inelg, treatment_elg = treatment_elg))
}

CleanDepartedContributorsGraph <- function(issue_tempdir) {
  df_departed_contributors <- read_parquet(file.path(issue_tempdir, "graph_departures.parquet"))
  df_departed <- df_departed_contributors %>% 
    group_by(repo_name) %>% 
    mutate(repo_count = n()) %>%
    ungroup() %>%
    filter(repo_count == 1) %>%
    mutate(departed_actor_id = as.numeric(actor_id)) %>%
    rename(treatment_period = max_time_period) %>%
    select(repo_name, departed_actor_id, last_pre_period, treatment_period, abandoned_date)
  treatment_inelg <- df_departed_contributors %>% 
    filter(repo_name %ni% df_departed$repo_name) %>%
    pull(repo_name)
  treatment_elg <- unique(df_departed$repo_name)
  return(list(df_departed = df_departed, treatment_inelg = treatment_inelg, treatment_elg = treatment_elg))
}

CleanCovariates <- function(indir) {
  df_covariates <- read_parquet(file.path(indir, "contributor_characteristics.parquet")) %>%
    mutate(time_period = as.Date(time_period)) %>%
    mutate(actor_id = as.numeric(actor_id)) %>%
    rename(departed_actor_id = actor_id)
  return(df_covariates)
}

CleanDownloads <- function(issue_tempdir) {
  df_project_downloads <- read_parquet(file.path(issue_tempdir, "github_downloads.parquet")) %>%
    mutate(time_period = as.Date(time_period)) %>%
    group_by(time_period) %>%
    select(repo_name, time_period, total_downloads, total_downloads_one_project)
    return(df_project_downloads)
}
    
CleanForkStars <- function(issue_tempdir) {
  df_fork_stars <- read_parquet(file.path(issue_tempdir, "github_forks_stars.parquet")) %>%
    mutate(time_period = as.Date(time_period))
  return(df_fork_stars)
}


CreateDeparturePanel <- function(df_project_outcomes, treatment_inelg, df_departed, df_covariates, df_downloads, df_fork_stars, trim_abandoned = TRUE) {
  df_project_departed <- df_project_outcomes %>%
    left_join(df_departed, by = "repo_name") %>%
    mutate(treated_project = 1 - as.numeric(is.na(last_pre_period)),
           treatment = as.numeric(treated_project & time_period >= treatment_period)) %>%
    mutate(final_period = as.Date(final_period),
           last_pre_period = as.Date(last_pre_period))
  if (trim_abandoned) {
    df_project_departed <- df_project_departed %>%
      filter(time_period < abandoned_date | is.na(abandoned_date))
  }
  df_treatment_group <- df_project_departed %>%
    filter(treatment == 1) %>%
    group_by(repo_name) %>%
    summarize(treatment_group = min(treatment * time_index))
  df_project_departed <- df_project_departed %>% 
    left_join(df_treatment_group, by = "repo_name") %>% 
    left_join(df_covariates, by = c("repo_name","departed_actor_id", "time_period")) %>%
    left_join(df_downloads, by = c("repo_name","time_period")) %>%
    left_join(df_fork_stars, by = c("repo_name","time_period")) %>%
  return(df_project_departed)
}

CleanOutcomesPost <- function(issue_tempdir, departure_panel_nyt) {
  treated_projects <- departure_panel_nyt$repo_name
  
  df_downloads <- read_parquet(file.path(issue_tempdir, "github_downloads.parquet")) %>%
    select(repo_name, time_period, total_downloads, total_downloads_one_project) %>%
    filter(repo_name %in% treated_projects) %>%
    mutate(total_downloads_period = sum(total_downloads),
           total_downloads_one_project_period = sum(total_downloads_one_project)) %>%
    ungroup() %>%
    mutate(total_downloads_rel = total_downloads/total_downloads_period,
           total_downloads_one_project_rel = total_downloads_one_project/total_downloads_one_project_period) %>%
    select(repo_name, time_period, total_downloads_rel, total_downloads_one_project_rel)
  
  df_downloads_detailed <- read_parquet(file.path(issue_tempdir, "github_downloads_detailed.parquet")) %>%
    filter(repo_name %in% treated_projects) %>%
    select(-ends_with("downloads")) %>%
    group_by(repo_name, time_period)  %>%
    summarise(across(everything(), sum, na.rm = TRUE))

  df_scorecard <- read_parquet(file.path(issue_tempdir, "github_scorecard_scores.parquet")) %>%
    filter(repo_name %in% treated_projects) %>%
    select(c(repo_name,time_period, overall_score, overall_increase, overall_decrease, overall_stable,
             vulnerabilities_score))

  df_valuable_outcomes <- df_downloads %>%
    full_join(df_downloads_detailed) %>%
    full_join(df_scorecard) %>%
    mutate(time_period = as.Date(time_period)) %>%

  return(df_valuable_outcomes)
}

NormalizeOutcome <- function(df, outcome, outcome_norm) {
  df_norm <- copy(df)
  if (grepl("download", outcome, ignore.case = TRUE)) {
    df_norm[[outcome]] <- df_norm %>% group_by(time_index) %>%  
      mutate(norm_outcome = !!sym(outcome) / sum(!!sym(outcome))) %>% 
      pull(norm_outcome)
  }
  # if (grepl("download", outcome, ignore.case = TRUE)) {
  #     df_norm[[outcome_norm]] <- log(df_norm[[outcome]] / df_norm$mean_outcome )
  # } else {}
  df_norm <- df_norm %>% group_by(repo_name) %>%
    mutate(mean_outcome = mean(get(outcome), na.rm = T)) %>%
    ungroup()
  df_norm[[outcome_norm]] <- df_norm[[outcome]] / df_norm$mean_outcome 
  return(df_norm)
}

CreateCovariateBins <- function(df, covariates, time_period_col = "time_index", k_values = 1:3) {
  df_k_averages <- data.frame(repo_name = unique(df$repo_name))
  for (k in k_values) {
    
    df_k_periods_mean <- df %>%
      filter(!!sym(time_period_col) < treatment_group,
             time_index < treatment_group,
             time_index >= treatment_group - k) %>%
      group_by(repo_name) %>%
      summarize(across(all_of(covariates), ~ mean(.x, na.rm = TRUE), .names = "{.col}_{k}p_back"))
    pattern <- paste0("_", k, "p_back$")
    target_cols <- names(df_k_periods_mean)[grepl(pattern, names(df_k_periods_mean))]
    print(target_cols)
    for (col in target_cols) {
      overall_mean <- mean(df_k_periods_mean[[col]], na.rm = TRUE)
      overall_median <- median(df_k_periods_mean[[col]], na.rm = TRUE)
      thirds_bounds <- quantile(df_k_periods_mean[[col]], probs = c(0.3333, 0.6667), na.rm = TRUE)
      quartile_bounds <- quantile(df_k_periods_mean[[col]], probs = c(0.25, 0.5, 0.75), na.rm = TRUE)
      df_k_periods_mean[[paste0(col, "_bin_mean")]] <- ifelse(df_k_periods_mean[[col]] >= overall_mean, 1L, 0L)
      df_k_periods_mean[[paste0(col, "_bin_median")]] <- ifelse(df_k_periods_mean[[col]] > overall_median, 1L, 0L)
      tryCatch({
        df_k_periods_mean[[paste0(col, "_bin_third")]] <- as.integer(
          cut(df_k_periods_mean[[col]],
              breaks = c(-Inf, thirds_bounds[1], thirds_bounds[2], Inf),
              labels = c(3L, 2L, 1L),
              include.lowest = TRUE))
      }, error = function(e) {
        message("thirds can't be made for ", col)
        df_k_periods_mean[[paste0(col, "_bin_third")]] <- rep(NA_integer_, nrow(df_k_periods_mean))
      })
      tryCatch({
        df_k_periods_mean[[paste0(col, "_bin_quartile")]] <- as.integer(
          cut(df_k_periods_mean[[col]],
              breaks = c(-Inf, quartile_bounds[1], quartile_bounds[2], quartile_bounds[3], Inf),
              labels = c(4L, 3L, 2L, 1L),
              include.lowest = TRUE))
      }, error = function(e) {
        message("quartiles can't be made for ", col)
        df_k_periods_mean[[paste0(col, "_bin_quartile")]] <- rep(NA_integer_, nrow(df_k_periods_mean))
      })
    }
    df_k_averages <- df_k_averages %>% left_join(df_k_periods_mean)
  }
  return(df_k_averages)
}

GenerateEventStudyGrids <- function(departure_panel_nyt, covariate_panel_nyt, outcomes, specification_covariates, post, pre, outdir, na_keep_cols, fillna, plot = FALSE) {
  specs <- names(specification_covariates)
  for (spec in specs) {
    for (outcome in outcomes) {
      # Try running the full and early event studies; if error, skip to next outcome
      event_result <- tryCatch({
        print("full")
        full_samp <- EventStudyAnalysis(departure_panel_nyt, outcome, post, pre, 
                                        MakeTitle(outcome, paste0(outcome, " - All Time Periods"), departure_panel_nyt))
        print("early")
        df_early <- departure_panel_nyt %>% filter(time_period <= final_period)
        early_samp <- EventStudyAnalysis(df_early, outcome, post, pre, 
                                         MakeTitle(outcome, paste0(outcome, " - Up to Last Active Period"), df_early))
        list(full = full_samp, early = early_samp, df_early = df_early)
      }, error = function(e) {
        message("Error in EventStudyAnalysis for outcome: ", outcome, spec, ". Skipping this outcome.")
        NULL
      })
      
      if (is.null(event_result)) next
      
      # Save data for the first outcome only
      if (outcome == outcomes[1]) {
        SaveData(departure_panel_nyt, c("repo_name", "time_period"), file.path(outdir, "full_sample.csv"), file.path(outdir, "full_sample.log"))
        SaveData(event_result$df_early, c("repo_name", "time_period"), file.path(outdir, "early_sample.csv"), file.path(outdir, "early_sample.log"))
      }
      
      outcome_str <- ifelse(grepl("^avg_", outcome), sub("^avg_", "", outcome), outcome)
      outdir_outcome_spec <- file.path(outdir, spec, outcome_str)
      outdir_spec <- file.path(outdir, spec)
      dir.create(outdir_outcome_spec, recursive = TRUE, showWarnings = FALSE)
      
      k_vector <- 2 #c(2, 3 ,4)
      bin_types <- c("bin_median") #c("bin_third")
      combos <- expand.grid(k = k_vector, bin = bin_types, stringsAsFactors = FALSE) %>% 
        mutate(split_specs = paste0(k, "p_back_", bin))
      
      for (split_spec in combos$split_specs) {
        split_vars <- specification_covariates[[spec]]
        split_spec_vars <- paste(split_vars, split_spec, sep = '_')
        if (!all(split_spec_vars %in% names(covariate_panel_nyt))) {
          print(paste(split_spec_vars, collapse = ", "))
          print("not available")
          next
        }
        na_drop_cols <- names(covariate_panel_nyt[split_spec_vars])[split_vars %ni% na_keep_cols]
        combinations <- expand.grid(lapply(covariate_panel_nyt[split_spec_vars], unique)) %>% 
          drop_na(all_of(na_drop_cols)) %>% 
          arrange(across(everything()))
        plot_list <- list(event_result$full$plot, event_result$early$plot)
        for (row in 1:nrow(combinations)) {
          combo <- combinations %>% slice(row)
          print(combo)
          descs <- c()
          for (colname in names(combo)) {
            bin_val <- as.numeric(combo[[colname]])
            descs <- c(descs, DescribeBin(colname, bin_val))
          }
          filtered_repos <- covariate_panel_nyt %>% inner_join(combo) %>% select(repo_name)
          df_selected <- event_result$df_early %>% inner_join(filtered_repos, by = "repo_name")
          selected_samp <- tryCatch({
            if (outcome == outcomes[1]) {
              combo_filename <- paste0(names(combo), combo, collapse = "_")
              combo_filename <- paste0(combo_filename, ".csv")
              SaveData(df_selected, c("repo_name", "time_period"), file.path(outdir_spec, combo_filename))
            }
            EventStudyAnalysis(df_selected, outcome, post, pre, 
                               MakeTitle(outcome, paste(descs, collapse = "\n"), df_selected))
          }, error = function(e) {
            message("Error in EventStudyAnalysis for combo: ", paste(combo, e, collapse = " "), ". Skipping.")
            NULL
          })
          if (is.null(selected_samp)) next
          plot_list[[row + 2]] <- selected_samp$plot
        }
        plot_list <- Filter(Negate(is.null), plot_list)
        if (length(plot_list) == 0) next
        plot_list <- AdjustYScaleUniformly(plot_list)
        final_plot <- grid.arrange(grobs = plot_list, ncol = 2)
        split_spec_minus_bin <- gsub("_back_bin", "", split_spec)
        filename_saved <- file.path(outdir_outcome_spec, sprintf("%s_%s.pdf", spec, split_spec_minus_bin))
        if (plot) {
          ggsave(plot = final_plot, filename = filename_saved, width = 9, height = 3 * (2 + nrow(combinations)), limitsize = FALSE,
                 dpi = 60)
        }
        message("Saved file: ", filename_saved)
        flush.console()
      }
    }
  }
  plan(sequential)
}

EventStudyAnalysis <- function(df, outcome, post, pre, title, normalize = TRUE) {
  if (normalize) {
    outcome_norm <- paste0(outcome, "_norm")
    df_norm <- NormalizeOutcome(df, outcome, outcome_norm)
  }
  results <- EstimateEventStudy(df_norm, outcome_norm, post, pre)
  plot <- PlotEventStudy(results, title)
  return(list(plot = plot, results = results))
}

EstimateEventStudy <- function(df, outcome, post, pre) {
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
  plot <- EventStudyPlot(estimates = results, ytitle = "Coefficient", xtitle = "Event time") + 
    ggtitle(label = title) + 
    theme(plot.title = element_text(size = 12))
  return(plot)
}

MakeTitle <- function(title, title_str, df) {
  full_title_str <- paste0(str_replace_all(title, "_", " "), "\n",
                           title_str, "\n",
                           dim(df)[1], " obs, PC: ", length(unique(df$repo_name)),
                           " T: ", length(unique(df[df$treatment == 1,]$repo_name)),
                           "\n# important:", length(unique(df[df$important == 1,]$repo_name)))
  return(full_title_str)
}

DescribeBin <- function(colname, bin_val) {
  pattern <- "^(.*)_([0-9]+p)_back_bin_([a-z]+)$"
  match <- regexec(pattern, colname, perl = TRUE)
  parts <- regmatches(colname, match)[[1]]
  if (length(parts) != 4) {
    stop("colname does not match expected pattern.")
  }
  prefix <- parts[2]
  time_ind <- parts[3]
  bin_type <- parts[4]
  desc <- paste0(prefix, " (", time_ind, ") ", bin_val, " ", bin_type)
  return(desc)
}

AdjustYScaleUniformly <- function(plot_list, num_breaks = 5) {
  if (length(plot_list) == 0) return(plot_list)
  yr <- do.call(rbind, lapply(plot_list, function(p) ggplot_build(p)$layout$panel_params[[1]]$y.range))
  y_min <- min(yr[,1], na.rm = TRUE)
  y_max <- max(yr[,2], na.rm = TRUE)
  y_breaks <- pretty(c(y_min, y_max), n = num_breaks)
  lapply(plot_list, function(p) p +
           coord_cartesian(ylim = c(y_min, y_max)) +
           scale_y_continuous(breaks = y_breaks))
}

BinScatter <- function(data, y_vars, x_var="prs_opened", group_var="repo_name", time_var="time_period", 
                       leads=c(0,1,2), sort_var="time_index", outdir) {
  lead_within_group <- function(x, grp, sort_val, n=1) { 
    res <- rep(NA, length(x))
    for(g in unique(grp)) { 
      idx <- which(grp == g)
      ord <- order(sort_val[idx])
      res[idx[ord]] <- dplyr::lead(x[idx[ord]], n=n)
    }
    res
  }
  label_lookup <- function(var){ 
    str_to_title(str_replace_all(var, "_", " ")) %>% 
      str_replace_all("Release Count", "Releases") %>% 
      str_replace_all("Minor Patch", "Minor + Patch") %>% 
      str_replace_all("Major Minor", "Major + Minor") %>% 
      str_replace_all("Major \\+ Minor \\+ Patch", "Major + Minor + Patch") %>% 
      str_replace_all("Overall Score", "OSSF Scorecard Score") %>% 
      str_replace_all("Vulnerabilities Score", "OSSF Scorecard Vulnerability Score")
  }
  fixed_effect_labels <- c("No Fixed Effects","Project Fixed Effects","Project + Time Fixed Effects")
  for(y in y_vars){
    plots <- list()
    for(lead in leads){
      outcome <- if(lead == 0) data[[y]] else 
        lead_within_group(data[[y]], data[[group_var]], data[[sort_var]], n=lead)
      for(i in seq_along(fixed_effect_labels)){
        w_formula <- if(i==1) NULL else if(i==2) 
          as.formula(paste0("~factor(",group_var,")")) else 
            as.formula(paste0("~factor(",group_var,") + factor(",time_var,")"))
        if (i==1) {
          est <- binsreg(y=outcome, x=log(data[[x_var]]+1), data=data,
                         polyreg=3, ci=TRUE, cb=TRUE, nbins=8, nsims=20000, simsgrid=100)
        } else {
          est <- binsreg(y=outcome, x=log(data[[x_var]]+1), w=w_formula, data=data,
                        polyreg=3, ci=TRUE, cb=TRUE, nbins=8, nsims=20000, simsgrid=100)
        }
        cell_title <- paste(fixed_effect_labels[i], "- Lead", lead)
        p <- est$bins_plot + labs(x = "log(PRs Opened + 1)", y = label_lookup(y), title = cell_title) +
          theme_minimal(base_size = 14) + theme(plot.title = element_text(hjust = 0.5, face = "bold"),
                                                axis.title = element_text(face = "bold"))
        plots[[length(plots)+1]] <- p
      }
    }
    grid_plot <- wrap_plots(plots, ncol = length(fixed_effect_labels)) +
      plot_annotation(title = paste("Binscatter:", label_lookup(y), "vs PRs Opened"),
                      theme = theme(plot.title = element_text(hjust = 0.5, size = 16, face = "bold")))
    ggsave(filename = file.path(outdir, paste0(y, "_binsreg_grid.png")),
           plot = grid_plot, width = 18, height = 6 * length(leads), dpi = 90)
  }
}


Main()
