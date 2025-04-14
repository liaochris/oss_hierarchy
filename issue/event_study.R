LibrarySetup <- function() {
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
  `%ni%` <<- Negate(`%in%`)
}

SaveSampleData <- function(full_panel, early_panel, outdir) {
  SaveData(full_panel, c("repo_name", "time_period"), file.path(outdir, "full_sample.csv"), file.path(outdir, "full_sample.log"))
  SaveData(early_panel, c("repo_name", "time_period"), file.path(outdir, "early_sample.csv"), file.path(outdir, "early_sample.log"))
}

PlotSpecCombo <- function(split_spec, spec, outcome, event_result, df_cov_panel, spec_covariates, na_keep_cols, outdir_spec, post, pre, outcomes, plot_flag) {
  split_vars <- spec_covariates[[spec]]
  split_spec_vars <- paste(split_vars, split_spec, sep = "_")
  if (!all(split_spec_vars %in% names(df_cov_panel))) {
    message(paste(split_spec_vars, collapse = ", "), " not available")
    return(NULL)
  }
  na_drop_cols <- names(df_cov_panel[split_spec_vars])[!(split_vars %in% na_keep_cols)]
  combos <- expand.grid(lapply(df_cov_panel[split_spec_vars], unique)) %>% 
    drop_na(all_of(na_drop_cols)) %>% arrange(across(everything()))
  plots <- list(event_result$full$plot, event_result$early$plot)
  for (i in seq_len(nrow(combos))) {
    combo <- combos[i, , drop = FALSE]
    descs <- sapply(names(combo), function(col) DescribeBin(col, as.numeric(combo[[col]])))
    filtered <- df_cov_panel %>% inner_join(combo, by = names(combo)) %>% select(repo_name)
    df_selected <- event_result$df_early %>% inner_join(filtered, by = "repo_name")
    sample_out <- tryCatch({
      if (outcome == outcomes[1]) {
        fname <- paste0(paste(names(combo), combo, collapse = "_"), ".csv")
        SaveData(df_selected, c("repo_name", "time_period"), file.path(outdir_spec, fname))
      }
      EventStudyAnalysis(df_selected, outcome, post, pre, MakeTitle(outcome, paste(descs, collapse = "\n"), df_selected))
    }, error = function(e) {
      message("Error in EventStudyAnalysis for combo: ", paste(combo, collapse = " "), ". Skipping.")
      NULL
    })
    if (!is.null(sample_out)) plots[[length(plots) + 1]] <- sample_out$plot
  }
  Filter(Negate(is.null), plots)
}

GenerateEventStudies <- function(df_panel, df_cov_panel, outcomes, spec_covariates, post, pre, outdir, na_keep_cols, fill_na, plot_flag = FALSE) {
  specs <- names(spec_covariates)
  for (spec in specs) {
    for (outcome in outcomes) {
      event_result <- tryCatch({
        full_sample <- EventStudyAnalysis(df_panel, outcome, post, pre, MakeTitle(outcome, paste0(outcome, " - All Time Periods"), df_panel))
        early_panel <- df_panel %>% filter(time_period <= final_period)
        early_sample <- EventStudyAnalysis(early_panel, outcome, post, pre, MakeTitle(outcome, paste0(outcome, " - Up to Last Active Period"), early_panel))
        list(full = full_sample, early = early_sample, df_early = early_panel)
      }, error = function(e) {
        message("Error for outcome ", outcome, " with spec ", spec, ". Skipping.")
        NULL
      })
      if (is.null(event_result)) next
      outcome_str <- ifelse(grepl("^avg_", outcome), sub("^avg_", "", outcome), outcome)
      outdir_combo <- file.path(outdir, spec, outcome_str)
      outdir_spec <- file.path(outdir, spec)
      dir.create(outdir_combo, recursive = TRUE, showWarnings = FALSE)
      
      combos <- expand.grid(k = 2, bin = c("bin_median"), stringsAsFactors = FALSE) %>%
        mutate(split_specs = paste0(k, "p_back_", bin))
      for (s_spec in combos$split_specs) {
        plot_list <- PlotSpecCombo(s_spec, spec, outcome, event_result, df_cov_panel, spec_covariates, na_keep_cols, outdir_spec, post, pre, outcomes, plot_flag)
        if (length(plot_list) == 0) next
        plot_list <- AdjustYScaleUniformly(plot_list)
        final_plot <- grid.arrange(grobs = plot_list, ncol = 2)
        fname <- file.path(outdir_combo, sprintf("%s_%s.png", spec, gsub("_back_bin", "", s_spec)))
        if (plot_flag) ggsave(plot = final_plot, filename = fname, width = 9, height = 3 * (2 + nrow(combos)), limitsize = FALSE, dpi = 60)
        message("Saved file: ", fname)
        flush.console()
      }
    }
  }
  plan(sequential)
}

PreparePanelData <- function(indir, indir_departed, issue_tempdir, indir_contributors,
                             time_period, rolling_window, criteria_pct, consecutive_periods, post_periods, graph_flag) {
  df_project <- CleanProjectOutcomes(indir, time_period)
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

Main <- function() {
  LibrarySetup()
  
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
  outdir_binscatter <- "issue/event_study/binscatter"
  
  spec_covariates <- list(
    # imp_contr = c("total_important"),
    # more_imp = c("normalized_degree"),
    # imp_contr_more_imp = c("total_important", "normalized_degree"),
    # imp_ratio = c("prop_important"),
    indiv_clus = c("overall_overlap"),
    # project_clus_ov = c("mean_cluster_overlap"),
    # project_clus_node = c("avg_clusters_per_node"),
    # project_clus_pct_one = c("pct_nodes_one_cluster"),
    # indiv_cluster_size = c("overall_overlap", "total_important"),
    # indiv_cluster_impo = c("overall_overlap", "normalized_degree"),
    # indiv_cluster_ov_cluster = c("overall_overlap", "mean_cluster_overlap"),
    # imp_imp_comm = c("imp_to_imp_avg_edge_weight"),
    # imp_other_comm = c("imp_to_other_avg_edge_weight"),
    # both_comm = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight"),
    # comm_imp_more_imp = c("normalized_degree", "imp_to_imp_avg_edge_weight"),
    # comm_within_more_imp = c("normalized_degree", "imp_to_other_avg_edge_weight"),
    # both_comm_cluster = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight", "overall_overlap"),
    # both_comm_ov_cluster = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight", "mean_cluster_overlap"),
    # comm_cluster = c("imp_to_imp_avg_edge_weight", "overall_overlap"),
    comm_within_cluster = c("imp_to_other_avg_edge_weight", "overall_overlap")
  )
  
  outcomes <- c(
    "prs_opened", "prs_merged", "commits", "commits_lt100", "comments", "issue_comments", "pr_comments",
    "issues_opened", "issues_closed",
    # "p_prs_merged", "closed_issue",
    # "p_prs_merged_30d", "p_prs_merged_60d", "p_prs_merged_90d", "p_prs_merged_180d",
    # "p_prs_merged_360d", "closed_in_30_days", "closed_in_60_days", "closed_in_90_days",
    # "total_downloads", "total_downloads_one_project", "total_downloads_rel", "total_downloads_one_project_rel",
    # "closed_in_180_days", "closed_in_360_days",  
    "overall_new_release_count", "major_new_release_count",
    "minor_new_release_count", "patch_new_release_count", "other_new_release_count", "major_minor_release_count",
    "major_minor_patch_release_count", "total_nodes", "forks_gained", "stars_gained"
    # "overall_score", "overall_increase",
    # "overall_decrease", "overall_stable", "vulnerabilities_score",
  )
  
  fill_na <- outcomes[!(grepl("downloads", outcomes, ignore.case = TRUE) |
                          grepl("^p_", outcomes) |
                          grepl("^closed_in_", outcomes) |
                          outcomes %in% c("overall_score", "overall_increase", "overall_decrease", "overall_stable", "vulnerabilities_score"))]
  
  na_keep <- if (params$graph_flag) c() else c("overall_overlap")
  
  prep <- PreparePanelData(indir, indir_departed, issue_tempdir, indir_contributors,
                           params$time_period, params$rolling_window, params$criteria_pct, params$consecutive_periods, params$post_periods, params$graph_flag)
  df_panel <- prep$panel
  outdir <- prep$outdir
  
  df_panel <- df_panel %>% mutate(across(all_of(fill_na), ~ ifelse(is.na(.), 0, .)))
  covars_to_split <- unique(unlist(spec_covariates))
  df_cov_panel <- CreateCovariateBins(df_panel, covars_to_split)
  
  SaveSampleData(df_panel, df_panel %>% filter(time_period <= final_period), outdir)
  GenerateEventStudies(df_panel, df_cov_panel, outcomes, spec_covariates, post = 4, pre = 0, outdir = outdir, na_keep_cols = na_keep, fill_na = fill_na, plot_flag = TRUE)
  
  df_pre <- df_panel %>% filter(time_index < treatment_group)
  release_vars <- names(df_pre)[grepl("release", names(df_pre))]
  bs_outcomes <- c("overall_score", "vulnerabilities_score", "forks_gained", "stars_gained", release_vars)
  BinScatter(data = df_pre, y_vars = bs_outcomes, outdir = "issue/event_study/binscatter", leads = c(0, 1, 2))
  
  print(summary(df_pre$overall_new_release_count))
  print(summary(df_pre$major_new_release_count))
  print(summary(df_pre$minor_new_release_count))
  print(summary(df_pre$patch_new_release_count))
  print(summary(df_pre$other_new_release_count))
}

Main()
