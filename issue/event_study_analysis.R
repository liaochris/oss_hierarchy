library(tidyverse)
library(arrow)
library(gridExtra)
library(ggplot2)
library(egg)
library(eventstudyr)
library(SaveData)

set.seed(1234)
`%ni%` <- Negate(`%in%`)

NormalizeOutcome <- function(df, outcome, outcome_norm) {
  df_norm <- df
  if (grepl("download", outcome, ignore.case = TRUE)) {
    df_norm[[outcome]] <- df_norm %>% group_by(time_index) %>%  
      mutate(norm_outcome = !!sym(outcome) / sum(!!sym(outcome))) %>% pull(norm_outcome)
  }
  df_norm <- df_norm %>% group_by(repo_name) %>% mutate(mean_outcome = mean(get(outcome), na.rm = TRUE)) %>% ungroup()
  df_norm[[outcome_norm]] <- df_norm[[outcome]] / df_norm$mean_outcome
  df_norm
}

EventStudyAnalysis <- function(df, outcome, post, pre, title, normalize = TRUE) {
  df_norm <- if (normalize) NormalizeOutcome(df, outcome, paste0(outcome, "_norm")) else df
  results <- eventstudyr::EventStudy(estimator = "OLS",
                                     data = df_norm,
                                     outcomevar = paste0(outcome, "_norm"),
                                     policyvar = "treatment",
                                     idvar = "repo_name",
                                     timevar = "time_index",
                                     post = post,
                                     pre = pre)
  plot <- EventStudyPlot(estimates = results, ytitle = "Coefficient", xtitle = "Event time") + 
    ggtitle(title) + theme(plot.title = element_text(size = 12))
  list(plot = plot, results = results)
}

MakeTitle <- function(title, title_str, df) {
  paste0(str_replace_all(title, "_", " "), "\n", title_str, "\n", 
         nrow(df), " obs, PC: ", length(unique(df$repo_name)),
         " T: ", length(unique(df %>% filter(treatment == 1) %>% pull(repo_name))),
         "\n# important:", length(unique(df %>% filter(time_index < treatment_group & important == 1) %>% pull(repo_name))))
}

DescribeBin <- function(colname, bin_val) {
  pattern <- "^(.*)_([0-9]+p)_back_bin_([a-z]+)$"
  match <- regexec(pattern, colname, perl = TRUE)
  parts <- regmatches(colname, match)[[1]]
  if (length(parts) != 4) stop("colname does not match expected pattern.")
  paste0(parts[2], " (", parts[3], ") ", bin_val, " ", parts[4])
}

AdjustYScaleUniformly <- function(plot_list, num_breaks = 5) {
  if (length(plot_list) == 0) return(plot_list)
  yr <- do.call(rbind, lapply(plot_list, function(p) ggplot_build(p)$layout$panel_params[[1]]$y.range))
  y_min <- max(-10, min(yr[,1], na.rm = TRUE))
  y_max <- min(max(yr[,2], na.rm = TRUE), 10)
  y_breaks <- pretty(c(y_min, y_max), n = num_breaks)
  lapply(plot_list, function(p) p + coord_cartesian(ylim = c(y_min, y_max)) + scale_y_continuous(breaks = y_breaks))
}

PlotSpecCombo <- function(split_spec, spec, outcome, event_result, df_cov_panel, spec_covariates, na_keep_cols, outdir_spec, post, pre, outcomes, plot_flag) {
  split_vars <- spec_covariates[[spec]]
  if (length(split_vars)>1) {
    split_spec_vars <- c(paste(split_vars[1], split_spec, sep = "_"),
                         paste(split_vars[2:length(split_vars)], str_replace(split_spec, "bin_third","bin_median"), sep = "_"))
    print(split_spec_vars)
  } else {
    split_spec_vars <- paste(split_vars, split_spec, sep = "_")
  }
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

GenerateEventStudies <- function(df_panel, df_cov_panel, outcomes, spec_covariates, post, pre, outdir,
                                 na_keep_cols, fill_na, plot_flag = FALSE) {
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
      outdir_combo <- file.path(outdir, spec, outcome)
      outdir_spec <- file.path(outdir, spec)
      dir.create(outdir_combo, recursive = TRUE, showWarnings = FALSE)
      
      combos <- expand.grid(k = c(2), bin = c("bin_median","bin_third"), stringsAsFactors = FALSE) %>%
        mutate(split_specs = paste0(k, "p_back_", bin))
      for (s_spec in combos$split_specs) {
        plot_list <- PlotSpecCombo(s_spec, spec, outcome, event_result, df_cov_panel, spec_covariates, na_keep_cols, outdir_spec, post, pre, outcomes, plot_flag)
        if (length(plot_list) == 0) next
        plot_list <- AdjustYScaleUniformly(plot_list)
        final_plot <- grid.arrange(grobs = plot_list, ncol = 2)
        fname <- file.path(outdir_combo, sprintf("%s_%s.png", spec, gsub("_back_bin", "", s_spec)))
        if (plot_flag) ggsave(plot = final_plot, filename = fname, width = 9, height = 4 * (2 + nrow(combos)), limitsize = FALSE, dpi = 150)
        message("Saved file: ", fname)
        flush.console()
      }
    }
  }
  plan(sequential)
}

sel_outcomes <- c("prs_opened", "prs_merged", "commits", "commits_lt100", "comments", "issue_comments", "pr_comments",
                  "issues_opened", "issues_closed")

outcomes <- c(
  "prs_opened", "prs_merged", "commits", "commits_lt100", "comments", "issue_comments", "pr_comments",
  "issues_opened", "issues_closed",
  "p_prs_merged", "closed_issue",
  "p_prs_merged_30d", "p_prs_merged_60d", "p_prs_merged_90d", "p_prs_merged_180d",
  "p_prs_merged_360d", "closed_in_30_days", "closed_in_60_days", "closed_in_90_days",
  "total_downloads", "total_downloads_one_project", "total_downloads_rel", "total_downloads_one_project_rel",
  "closed_in_180_days", "closed_in_360_days",
  "overall_new_release_count", "major_new_release_count",
  "minor_new_release_count", "patch_new_release_count", "other_new_release_count", "major_minor_release_count",
  "major_minor_patch_release_count", "total_nodes", "forks_gained", "stars_gained",
  "overall_score", "overall_increase",
  "overall_decrease", "overall_stable", "vulnerabilities_score"
)

spec_covariates <- list(
  # imp_contr = c("total_important"),
  more_imp = c("normalized_degree"),
  # imp_contr_more_imp = c("total_important", "normalized_degree"),
  # imp_ratio = c("prop_important"),
  #indiv_clus = c("overall_overlap"),
  #indiv_clus_more_imp = c("overall_overlap", "normalized_degree")
  project_clus_ov = c("mean_cluster_overlap"),
  project_clus_ov_more_imp  = c("mean_cluster_overlap","normalized_degree")
  #project_clus_node = c("avg_clusters_per_node"),
  #project_clus_pct_one = c("pct_nodes_one_cluster"),
  # indiv_cluster_size = c("overall_overlap", "total_important"),
  # indiv_cluster_ov_cluster = c("overall_overlap", "mean_cluster_overlap"),
  # imp_imp_comm = c("imp_to_imp_avg_edge_weight"),
  # imp_other_comm = c("imp_to_other_avg_edge_weight"),
  # both_comm = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight"),
  # comm_imp_more_imp = c("normalized_degree", "imp_to_imp_avg_edge_weight"),
  # comm_within_more_imp = c("normalized_degree", "imp_to_other_avg_edge_weight"),
  # both_comm_cluster = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight", "overall_overlap"),
  # both_comm_ov_cluster = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight", "mean_cluster_overlap"),
  # comm_cluster = c("imp_to_imp_avg_edge_weight", "overall_overlap"),
  #comm_within_cluster = c("imp_to_other_avg_edge_weight", "overall_overlap")
)

Main <- function() {
  issue_tempdir <- "issue"
  outdir <- "issue/event_study/graphs"
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  
  df_panel <- read_parquet(file.path(issue_tempdir, "event_study_panel.parquet"))
  df_cov_panel <- read_parquet(file.path(issue_tempdir, "covariate_bins.parquet"))
  
  fill_na <- outcomes[!(grepl("downloads", outcomes, ignore.case = TRUE) |
                          grepl("^p_", outcomes) |
                          grepl("^closed_in_", outcomes) |
                          outcomes %in% c("overall_score", "overall_increase", "overall_decrease", "overall_stable", "vulnerabilities_score"))]
  na_keep <- if (params$graph_flag) c() else c("overall_overlap")
  
  post_period <- 4
  pre_period <- 0
  GenerateEventStudies(df_panel, df_cov_panel, outcomes, spec_covariates, post = post_period, pre = pre_period,
                       outdir = outdir, na_keep_cols = na_keep, fill_na = fill_na, plot_flag = TRUE)
  GenerateEventStudies(df_panel, df_cov_panel, 
                       c(paste("avg", sel_outcomes, sep = '_'),paste("cc", sel_outcomes, sep = '_')), 
                       spec_covariates, post = post_period, pre = pre_period,
                       outdir = outdir, na_keep_cols = na_keep, fill_na = fill_na, 
                       plot_flag = TRUE)
  
  ### DISABLE TFE FOR NEW...
  for (suffix in paste0("_", c("imp","all","unimp","new"))) { # add departed later on?
    outdir_suffix <- paste0("issue/event_study/graphs",suffix)
    dir.create(outdir_suffix, recursive = TRUE, showWarnings = FALSE)
    df_panel_suffix <- read_parquet(file.path(issue_tempdir, paste0("event_study_panel",suffix,".parquet")))
    GenerateEventStudies(df_panel_suffix, df_cov_panel, outcomes, spec_covariates, post = post_period, pre = pre_period,
                         outdir = outdir_suffix, na_keep_cols = na_keep, fill_na = fill_na, plot_flag = TRUE)
    GenerateEventStudies(df_panel_suffix, df_cov_panel, 
                         c(paste("avg", sel_outcomes, sep = '_'),paste("cc", sel_outcomes, sep = '_')), 
                         spec_covariates, post = post_period, pre = pre_period,
                         outdir = outdir_suffix, na_keep_cols = na_keep, fill_na = fill_na, 
                         plot_flag = TRUE)
  }
}

Main()