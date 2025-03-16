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

set.seed(1234)
`%ni%` = Negate(`%in%`)


indir <- "drive/output/derived/project_outcomes"
indir_departed <- "drive/output/derived/contributor_stats/filtered_departed_contributors"
issue_tempdir <- "issue"
outdir <- "issue/event_study/graphs/"
outdir_graph_departures <- "issue/event_study/graph_departures/"

time_period <- 6
rolling_window <- 732
criteria_pct <- 75
consecutive_periods <- 3
post_periods <- 2

CleanProjectOutcomes <- function(indir, time_period) {
  df_project_outcomes <- read_parquet(file.path(indir, paste0("project_outcomes_major_months",time_period,".parquet"))) %>% 
    mutate(time_period = as.Date(time_period)) %>%
    filter(year(time_period)<2023 | (year(time_period) == 2023 & month(time_period) == 1))  %>%
    mutate(time_index = dense_rank(time_period))
  return(df_project_outcomes)
}

CleanDepartedContributors <- function(indir_departed, time_period, rollign_window, criteria_pct, consecutive_periods,
                                      post_periods) {
  df_departed_contributors <- read_parquet(
    file.path(indir_departed, paste0("filtered_departed_contributors_major_months",time_period,"_window",
                                     rolling_window,"D_criteria_commits_",criteria_pct,"pct_consecutive",consecutive_periods,
                                     "_post_period",post_periods,"_threshold_gap_qty_0.parquet")))
  
  df_departed <- df_departed_contributors %>% 
    filter(present_one_after == 0) %>% 
    filter(year(treatment_period)<2023 & !abandoned_scraped & !abandoned_consecutive_req3_permanentTrue) %>%
    group_by(repo_name) %>% 
    mutate(repo_count = n()) %>%
    ungroup() %>%
    filter(repo_count == 1) %>%
    mutate(departed_actor_id = actor_id,
           abandoned_date = as.Date(abandoned_date_consecutive_req2_permanentTrue)) %>%
    select(repo_name,departed_actor_id,last_pre_period,treatment_period,abandoned_date)
  
  treatment_inelg <- df_departed_contributors %>% 
    filter(repo_name %ni% df_departed$repo_name) %>%
    pull(repo_name)
  treatment_elg <- unique(df_departed$repo_name)
  
  return(list(df_departed = df_departed, treatment_inelg = treatment_inelg, treatment_elg = treatment_elg))
}

CleanCovariates  <- function(issue_tempdir) {
  df_covariates <- read_parquet(file.path(issue_tempdir, "graph_important.parquet")) %>%
    mutate(time_period = as.Date(time_period)) %>%
    mutate(actor_id = as.numeric(actor_id)) %>%
    rename(departed_actor_id = actor_id)
  return(df_covariates)
}

CreateDeparturePanel <- function(df_project_outcomes, treatment_inelg, df_covariates, 
                                 trim_abandoned = T) {
  df_project <- df_project_outcomes %>%
    filter(repo_name %ni% treatment_inelg)
  
  df_project_departed <- df_project %>%
    left_join(df_departed) %>%
    mutate(treated_project = 1-as.numeric(is.na(last_pre_period)),
           treatment = as.numeric(treated_project & time_period>=treatment_period)) %>%
    mutate(final_period = as.Date(final_period),
           last_pre_period = as.Date(last_pre_period))
  
  if (trim_abandoned) {
    df_project_departed <- df_project_departed %>%
      filter(time_period < abandoned_date | is.na(abandoned_date))
  }
  df_treatment_group <- df_project_departed %>%
    filter(treatment == 1) %>%
    group_by(repo_name) %>%
    summarize(treatment_group = min(treatment*time_index))
  
  df_project_departed <- df_project_departed %>% 
    left_join(df_treatment_group) %>% 
    left_join(df_covariates)

  return(df_project_departed)
}

df_project_outcomes <- CleanProjectOutcomes(indir, time_period)
departed_list <- CleanDepartedContributors(indir_departed, time_period, rolling_window, criteria_pct, consecutive_periods, post_periods)
df_departed <- departed_list$df_departed
treatment_inelg <- departed_list$treatment_inelg
df_covariates <- CleanCovariates(issue_tempdir)
departure_panel <- CreateDeparturePanel(df_project_outcomes, treatment_inelg, df_covariates)

all_projects <- departure_panel %>% pull(repo_name) %>% unique()
treated_projects <- departure_panel %>% filter(treatment == 1) %>% pull(repo_name) %>% unique()
treated_projects_count <- length(treated_projects)
control_projects <- departure_panel %>%
  filter(repo_name %ni% treated_projects) %>% pull(repo_name) %>% unique()
control_projects_count <- length(control_projects)

departure_panel_nyt <- departure_panel %>%  
  filter(treated_project==1) %>%
  unique()
project_covars <- c("repo_name","time_period","treated_project","treatment",
                    "time_index","treatment_group","departed_actor_id", "final_period")
departure_panel_na <- departure_panel_nyt %>% 
  filter(treatment_group > time_index & treatment_group <= time_index + 3) %>% 
  select(project_covars, colnames(df_covariates))


print(paste("Project Count:", length(all_projects)))
print(paste("Control Projects:", control_projects_count))
print(paste("Treated Projects:", treated_projects_count))
print("Distribution of missing observations for `total_important`")
c(departure_panel_na %>% 
    group_by(repo_name) %>% 
    summarize(num_obs = sum(is.na(total_important))) %>% pull(num_obs) %>% table(), 
  "all" = length(unique(departure_panel_na$repo_name)))



outcomes <- c("prs_opened", "commits","commits_lt100","comments","issue_comments","pr_comments",
              "issues_opened","issues_closed","prs_merged","p_prs_merged","closed_issue",
              "p_prs_merged_30d","p_prs_merged_60d","p_prs_merged_90d","p_prs_merged_180d",
              "p_prs_merged_360d","closed_in_30_days","closed_in_60_days","closed_in_90_days",
              "closed_in_180_days","closed_in_360_days")

# AverageOutcomes <- function(df, outcomes, total_nodes_col = "total_nodes") {
#   for (outcome in outcomes) {
#     if (outcome != total_nodes_col) {
#       new_col <- paste0("avg_", outcome)
#       df[[new_col]] <- df[[outcome]] / df[[total_nodes_col]]
#     }
#   }
#   return(df)
# }
# 
# departure_panel <- AverageOutcomes(departure_panel, outcomes)
# outcomes <- c(outcomes, paste0("avg_", outcomes))

NormalizeOutcome <- function(df, outcome, outcome_norm) {
  df_norm <- copy(df)
  pretreatment_mean <- df_norm %>% filter(time_index < treatment_group) %>% 
    summarize(mean(get(outcome))) %>% pull()
  df_norm[[outcome_norm]] <- df_norm[[outcome]]/pretreatment_mean
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
      summarize(across(all_of(covariates), 
                       ~ mean(.x, na.rm = TRUE), 
                       .names = "{.col}_{k}p_back"))
    
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
              include.lowest = TRUE))}, error = function(e) {
        message("thirds can't be made for ", col)
        df_k_periods_mean[[paste0(col, "_bin_third")]] <- rep(NA_integer_, nrow(df_k_periods_mean))
      })
      
      tryCatch({
        df_k_periods_mean[[paste0(col, "_bin_quartile")]] <- as.integer(
          cut(df_k_periods_mean[[col]],
              breaks = c(-Inf, quartile_bounds[1], quartile_bounds[2], quartile_bounds[3], Inf),
              labels = c(4L, 3L, 2L, 1L),
              include.lowest = TRUE))}, error = function(e) {
        message("quartiles can't be made for ", col)
        df_k_periods_mean[[paste0(col, "_bin_quartile")]] <- rep(NA_integer_, nrow(df_k_periods_mean))
      })
    }
    df_k_averages <- df_k_averages %>% left_join(df_k_periods_mean)
  }
  
  return(df_k_averages)
}

specification_covariates <- list(
  imp_contr = c("total_important"),
  more_imp = c("normalized_degree"),
  imp_ratio = c("prop_important"),
  indiv_clus = c("overall_overlap"),
  project_clus_ov = c("mean_cluster_overlap"),
  project_clus_node = c("avg_clusters_per_node"),
  project_clus_pct_one = c("pct_nodes_one_cluster"),
  indiv_cluster_size = c("overall_overlap", "total_important"),
  indiv_cluster_impo = c("overall_overlap", "normalized_degree"),
  imp_imp_comm = c("imp_to_imp_avg_edge_weight"),
  imp_other_comm = c("imp_to_other_avg_edge_weight"),
  both_comm = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight"),
  comm_imp_more_imp =  c("normalized_degree", "imp_to_imp_avg_edge_weight"),
  comm_within_more_imp =  c("normalized_degree", "imp_to_other_avg_edge_weight"),
  comm_cluster = c("imp_to_imp_avg_edge_weight","overall_overlap"),
  comm_within_cluster = c("imp_to_other_avg_edge_weight","overall_overlap")
)

na_keep_cols <- c("overall_overlap")
covariates_to_split <- unique(unlist(specification_covariates))
covariate_panel_nyt <- CreateCovariateBins(departure_panel_nyt, covariates_to_split)

GenerateEventStudyGrids <- function(df, df_covariates, outcomes, specification_covariates, post, pre, fillna = TRUE) {
  plan(multisession)
  specs <- names(specification_covariates)
  future_lapply(specs, function(spec) {
    for (outcome in outcomes) {
      if (fillna) {
        df[[outcome]] <- ifelse(is.na(df[[outcome]]), 0, df[[outcome]])
      }

      full_samp <- EventStudyAnalysis(df, outcome, post, pre, 
                                      MakeTitle(outcome, paste0(outcome, " - All Time Periods"), df))
      
      df_early <- df %>% filter(time_period <= final_period)
      early_samp <- EventStudyAnalysis(df_early, outcome, post, pre, 
                                       MakeTitle(outcome, paste0(outcome, " - Up to Last Active Period"), df_early))
      
      outcome_str <- ifelse(grepl("^avg_", outcome), sub("^avg_", "", outcome), outcome)
      outdir_outcome_spec <- file.path(outdir, spec, outcome_str)
      dir.create(outdir_outcome_spec, recursive = TRUE, showWarnings = FALSE)
      
      k <- 2 #k <- 1:3
      bin_types <- c("bin_median", "bin_third") #bin_types <- c("bin_median", "bin_mean", "bin_third", "bin_quartile")
      combos <- expand.grid(k = k, bin = bin_types, stringsAsFactors = FALSE) %>% 
        mutate(split_specs = paste0(k, "p_back_", bin))
      
      for (split_spec in combos$split_specs) {
        split_vars <- specification_covariates[[spec]]
        split_spec_vars <- paste(split_vars, split_spec, sep = '_')
        if (!all(split_spec_vars %in% names(df_covariates))) {
          print(paste(split_spec_vars, collapse = ", "))
          print("not available")
          next
        }
        
        na_drop_cols <- names(df_covariates[split_spec_vars])[split_vars %ni% na_keep_cols]
        combinations <- expand.grid(lapply(df_covariates[split_spec_vars], unique)) %>% 
          drop_na(all_of(na_drop_cols)) %>% 
          arrange(across(everything()))
        
        plot_list <- list(full_samp$plot, early_samp$plot)
        for (row in 1:nrow(combinations)) {
          combo <- combinations %>% slice(row)
          print(combo)
          descs <- c()
          for (colname in names(combo)) {
            bin_val <- as.numeric(combo[[colname]])
            descs <- c(descs, DescribeBin(colname, bin_val))
          }
          filtered_repos <- df_covariates %>% inner_join(combo) %>% select(repo_name)
          df_selected <- df_early %>% inner_join(filtered_repos, by = "repo_name")
          
          selected_samp <- tryCatch({
            EventStudyAnalysis(df_selected, outcome, post, pre, 
                               MakeTitle(outcome, paste(descs, collapse = "\n"), df_selected))
          }, error = function(e) {
            message("Error in EventStudyAnalysis for combo: ", paste(combo, collapse = " "))
            NULL
          })
          if (is.null(selected_samp)) next
          plot_list[[row+2]] <- selected_samp$plot
        }
        plot_list <- Filter(Negate(is.null), plot_list)
        if (length(plot_list) == 0) next
        plot_list <- AdjustYScaleUniformly(plot_list)
        final_plot <- grid.arrange(grobs = plot_list, ncol = 2)
        split_spec_minus_bin <- gsub("_back_bin", "", split_spec)
        
        filename_saved <- file.path(outdir_outcome_spec, sprintf("%s_%s.png", spec, split_spec_minus_bin))
        ggsave(plot = final_plot, filename = filename_saved, width = 9, height = 3 * (2+nrow(combinations)))
        message("Saved file: ", filename_saved)
        flush.console()
      }
    }
  })
}


EventStudyAnalysis <- function(df, outcome, post, pre, title, normalize = T)  {
  if (normalize) {
    outcome_norm <- paste0(outcome,"_norm")
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
                          title_str,
                          "\n",dim(df)[1]," obs, PC: ",length(unique(df$repo_name)),
                          " T: ",length(unique(df[df$treatment==1,]$repo_name)),
                          "\n# important:", length(unique(df[df$important==1,]$repo_name)))
  return(full_title_str)
}

DescribeBin <- function(colname, bin_val) {
  pattern <- "^(.*)_([0-9]+p)_back_bin_([a-z]+)$"
  match <- regexec(pattern, colname, perl = TRUE)
  parts <- regmatches(colname, match)[[1]]
  
  if(length(parts) != 4) {
    stop("colname does not match expected pattern.")
  }
  
  prefix <- parts[2]  
  time_ind <- parts[3] 
  bin_type <- parts[4] 
  
  desc <- paste0(prefix, " (", time_ind, ") ", bin_val, " ", bin_type)
  return(desc)
}

AdjustYScaleUniformly <- function(plot_list, num_breaks = 5) {
  if(length(plot_list) == 0) return(plot_list)
  
  # Get global y-range.
  yr <- do.call(rbind, lapply(plot_list, function(p) ggplot_build(p)$layout$panel_params[[1]]$y.range))
  y_min <- min(yr[,1], na.rm = TRUE)
  y_max <- max(yr[,2], na.rm = TRUE)
  y_breaks <- pretty(c(y_min, y_max), n = num_breaks)
  
  lapply(plot_list, function(p) p +
           coord_cartesian(ylim = c(y_min, y_max)) +
           scale_y_continuous(breaks = y_breaks))
}

GenerateEventStudyGrids(departure_panel_nyt, covariate_panel_nyt, 
                        outcomes, specification_covariates, 2, 0, fillna = T)


