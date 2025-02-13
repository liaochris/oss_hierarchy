
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
outdir <- "issue/event_study/linear_panel"

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
                                   "_post_period",post_periods,"_threshold_gap_qty_0.parquet")))  %>% 
  filter(present_one_after == 0)
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
    problem_id_HHI = problem_identification_HHI,
    problem_id_share = problem_identification_share,
    problem_disc_share = problem_discussion_share
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

project_covars <- c("repo_name","time_period","treated_project","treatment",
                    "time_index","treatment_group","departed_actor_id", "final_period")

org_covars <- c("stars_accumulated", "forks_gained", "truckfactor", "contributors", "org_status", "project_age")
org_covars_long <- c(org_covars, "corporate_pct", "educational_pct", "apache_software_license", "bsd_license",
                     "gnu_general_public_license", "mit_license")
contributor_covars <- c("truckfactor_member", "max_rank", "total_share", "comments_share_avg_wt", "problem_id_share", 
                        "problem_disc_share", "coding_share", "problem_approval_share", "comments_hhi_avg_wt", "pct_coop_comments", "pct_coop_commits_count")
contributor_covars_long <- c(contributor_covars, "contributor_email_educational", "contributor_email_corporate")
org_structure <- c("min_layer_count", "problem_disc_hl_work", "coding_hl_work", "total_HHI", "contributors_comments_wt",
                   "comments_cooperation_pct", "problem_disc_hl_contr_ov", "coding_hl_contr_ov", "problem_id_HHI", "problem_discussion_HHI",
                   "problem_approval_HHI", "coding_HHI", "hhi_comments", "hhi_commits_count")
org_structure_long <- c(org_structure, "layer_count", "problem_id_layer_contr_pct", "problem_disc_layer_contr_pct", 
                        "coding_layer_contr_pct", "problem_app_layer_contr_pct", "problem_disc_layer_contr_cnt")

outcomes <- c("issues_opened","issue_comments","own_issue_comments", "helping_issue_comments","pr_comments",
              "prs_opened", "commits", "prs_merged", "closed_issue")
make_bins <- c("total_share","comments_hhi_avg_wt","pct_coop_comments","pct_coop_commits_count",
               "min_layer_count","problem_disc_hl_contr_ov","coding_hl_contr_ov","problem_disc_hl_work",
               "coding_hl_work","total_HHI","problem_discussion_HHI","coding_HHI","problem_approval_HHI",
               "hhi_comments","hhi_commits_count", "contributors", "problem_id_share",
               "problem_disc_share", "coding_share","problem_approval_share",
               "comments_cooperation_pct", "contributors_comments_wt","comments_share_avg_wt")

avg_var_list <- c(org_structure, contributor_covars[contributor_covars %ni% c("truckfactor_member","max_rank")], "contributors")

df_combined <- bind_cols(
  df_project_departed %>% filter(treated_project == 1 & treatment == 0 & time_index >= treatment_group - 2 & time_index < treatment_group) %>%
    summarise(across(all_of(avg_var_list), ~ mean(.x, na.rm = TRUE), .names = "{.col}_nyt_avg")),
  df_project_departed %>%
    filter(treatment == 0) %>%
    summarise(across(all_of(avg_var_list), ~ mean(.x, na.rm = TRUE), .names = "{.col}_all_avg")))

df_project_departed <- cbind(df_project_departed, df_combined) %>%
  select(all_of(c(project_covars, contributor_covars_long, org_covars_long, org_structure_long, outcomes,
                  paste0(avg_var_list,"_nyt_avg"))))

df_bin2 <- df_project_departed %>%
  filter(time_index >= treatment_group - 2 & time_index < treatment_group) %>%
  group_by(repo_name) %>%
  summarize(across(all_of(make_bins), list(
           bin_2 = ~ if_else(mean(.x, na.rm=TRUE) > get(paste0(cur_column(), "_nyt_avg")), 1, 0)), .names = "{.col}_{.fn}")) %>%
  unique()
df_bin3 <- df_project_departed %>%
  filter(time_index >= treatment_group - 3 & time_index < treatment_group) %>%
  group_by(repo_name) %>%
  summarize(across(all_of(make_bins), list(
    bin_3 = ~ if_else(mean(.x, na.rm=TRUE) > get(paste0(cur_column(), "_nyt_avg")), 1, 0)
  ), .names = "{.col}_{.fn}")) %>%
  unique()
df_project_departed <- df_project_departed %>% left_join(df_bin2) %>% left_join(df_bin3)

NormalizeOutcome <- function(df, outcome, outcome_norm) {
  pretreatment_mean <- df %>% filter(time_index < treatment_group) %>% 
    summarize(mean(get(outcome))) %>% pull()
  df[[outcome_norm]] <- df[[outcome]]/pretreatment_mean
  return(df)
}

GenerateEventStudyGrids <- function(df, outcomes, bin_vars, post, pre, fillna = T, plots_per_grid = 8, num_breaks) {
  bins_per_grid <- (plots_per_grid - 2) 
  total_bins <- length(bin_vars)  
  num_grids <- ceiling(total_bins / bins_per_grid)  
  
  for (outcome in outcomes) { 
    print(outcome)
    outdir_outcome <- file.path(outdir, outcome)
    dir.create(outdir_outcome)
    outcome_norm <- paste0(outcome,"_norm")
    
    if (fillna == T) {
      df[[outcome]] <- ifelse(is.na(df[[outcome]]), 0,  df[[outcome]])
    }
    df <- NormalizeOutcome(df, outcome, outcome_norm)
    full_samp <- EventStudyAnalysis(df, outcome_norm, post, pre, MakeTitle(outcome, paste0(outcome, " - Full Sample"), df))
    df_early <- NormalizeOutcome(df %>% filter(time_period <= final_period), outcome, outcome_norm)
    early_samp <- EventStudyAnalysis(df_early, outcome_norm, post, pre, MakeTitle(outcome, paste0(outcome, " - Dropout Sample"), df_early))
    


    for (i in seq_len(num_grids)) {
      print(i)
      start_idx <- (i - 1) * bins_per_grid + 1
      end_idx <- min(i * bins_per_grid, total_bins)
      subset_bins <- make_bins[start_idx:end_idx] 
     
      desired_order <- 1:(2+2*length(subset_bins)) #AlternateOrder(1:(2+2*length(subset_bins)))

      plot_list <- list()
      plot_list[[which(desired_order==1)]] <- full_samp$plot
      plot_list[[which(desired_order==2)]] <- early_samp$plot
      
      counter <- 3
      for (bin_var in subset_bins) {
        
        
        df_high <- NormalizeOutcome(df %>% filter(get(paste0(bin_var, "_bin_2")) == 1), outcome, outcome_norm)
        df_low <- NormalizeOutcome(df %>% filter(get(paste0(bin_var, "_bin_2")) == 0), outcome, outcome_norm)
        
        high_bin <- EventStudyAnalysis(df_high, outcome_norm, post, pre, MakeTitle(outcome, paste0(bin_var, " > mean"), df_high))
        low_bin <- EventStudyAnalysis(df_low, outcome_norm, post, pre, MakeTitle(outcome, paste0(bin_var, " <= mean"), df_low))
        
        df_plot <- rbind(
          tidy(high_bin$results$output) %>% mutate(group = paste(bin_var, "> mean")),
          tidy(low_bin$results$output) %>% mutate(group = paste(bin_var, "<= mean"))
        ) %>%
          mutate(event_time = case_when(
            term == "treatment_lead3" ~ -4,  # Ensure lead3 maps to -4
            str_detect(term, "lead") ~ -as.numeric(str_extract(term, "\\d+")),
            term == "treatment_fd" ~ 0,
            str_detect(term, "lag") ~ as.numeric(str_extract(term, "\\d+"))
          )) %>%
          mutate(event_time_label = case_when(
            event_time == min(event_time) ~ paste0(event_time, "+"),
            event_time == max(event_time) ~ paste0(event_time, "+"),
            TRUE ~ as.character(event_time)
          ))
        df_plot <- data.table::rbindlist(list(df_plot, 
                                              data.frame(list(estimate = 0, event_time = -1, event_time_label = "-1"))), 
                                         fill = T)
        ggplot(df_plot, aes(x = event_time, y = estimate, color = group)) +
          geom_pointrange(aes(ymin = conf.low, ymax = conf.high), 
                          position = position_dodge(width = 0.5)) +
          geom_errorbar(aes(ymin = estimate - 1.96 * std.error, ymax = estimate + 1.96 * std.error), 
                        width = 0.2, size = 1, position = position_dodge(width = 0.5)) +
          geom_hline(yintercept = 0, linetype = "dashed", color = "green") + 
          labs(
            x = "Event time",
            y = "Coefficient",
            caption = paste0("Large ", bin_var, " (", length(unique(df_high$repo_name)), " projects): ", high_bin$plot$labels$caption, "\n",
                             "Small ", bin_var, " (", length(unique(df_low$repo_name)), " projects): ", low_bin$plot$labels$caption) 
          ) +
          theme_minimal() +
          theme(legend.title = element_blank()) + 
          labs(
            title = outcome,  # Title is now the outcome variable
            x = "Event time",
            y = "Coefficient",
            caption = paste0(
              "Large ", bin_var, " (", length(unique(df_high$repo_name)), " projects): ", high_bin$plot$labels$caption, "\n",
              "Small ", bin_var, " (", length(unique(df_low$repo_name)), " projects): ", low_bin$plot$labels$caption
            ) 
          ) +
          theme_minimal() +
          theme(
            legend.title = element_blank(),
            plot.caption = element_text(hjust = 0)  # Left-align caption
          )
        
        
      }
      plot_list <- AdjustYScaleByRow(plot_list)
      final_plot <- grid.arrange(grobs = plot_list, ncol = 2)
      
      # Save grid plot separately
      ggsave(plot = final_plot, filename = file.path(outdir_outcome, paste0(outcome, "_Grid_", i, ".png")), width = 10, height = 16)
    }
  }
}

EventStudyAnalysis <- function(df, outcome, post, pre, title)  {
  results <- EstimateEventStudy(df, outcome, post, pre)
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
  full_title_str <- paste(str_replace_all(title, "_", " "), paste0("(",str_replace_all(title_str, "_", " "),")"), 
                          "\n",dim(df)[1],"obs, PC:",length(unique(df$repo_name)),
                          "T:",length(unique(df[df$treatment==1,]$repo_name)))
  return(full_title_str)
}

AdjustYScaleByRow <- function(plot_list, ncol = 2, num_breaks = 5) {
  num_plots <- length(plot_list)
  nrow <- ceiling(num_plots / ncol)
  
  plot_matrix <- split(plot_list, rep(1:nrow, each = ncol, length.out = num_plots))
  
  adjust_row <- function(row_plots) {
    row_plots <- Filter(Negate(is.null), row_plots) # Remove NULL plots
    if (length(row_plots) == 0) return(NULL)
    
    y_ranges <- lapply(row_plots, function(p) ggplot_build(p)$layout$panel_params[[1]]$y.range)
    y_ranges <- do.call(rbind, y_ranges)
    
    y_min <- min(y_ranges[, 1], na.rm = TRUE)
    y_max <- max(y_ranges[, 2], na.rm = TRUE)
    y_breaks <- pretty(seq(y_min, y_max, length.out = num_breaks))
    
    lapply(row_plots, function(p) p + 
             coord_cartesian(ylim = c(y_min, y_max)) + 
             scale_y_continuous(breaks = y_breaks))
  }
  
  adjusted_rows <- lapply(plot_matrix, adjust_row)
  adjusted_plots <- unlist(adjusted_rows, recursive = FALSE, use.names = FALSE)
  
  return(adjusted_plots)
}

AlternateOrder <- function(vec) {
  n <- length(vec)
  odd_indices <- seq(1, n, by = 2) 
  even_indices <- seq(2, n, by = 2)  
  
  new_order <- c(odd_indices, even_indices)  
  return(vec[new_order])
}

GenerateEventStudyGrids(df = df_project_departed %>% filter(treated_project == 1), 
                        outcomes = outcomes, bin_vars = make_bins,
                        post = 3, pre = 0, fillna = T, plots_per_grid = 8, num_breaks = 7)



summarized_share_situation <- summarized_data <- df_project_departed %>%
  filter(time_index >= (treatment_group - 2) & time_index < treatment_group) %>%
  group_by(total_share_bin_2) %>%
  summarize(across(c(project_age, contributors, prs_opened, min_layer_count, treatment_group),
                   ~ mean(.x, na.rm = TRUE), .names = "avg_{.col}")) %>%
  arrange(total_share_bin_2)

kable_output <- summarized_data %>%
  mutate(across(starts_with("avg_"), ~ round(.x, 1)) ) %>%
  kable(caption = "Summary of Projects by Share Bin", 
        col.names = c("Above Avg. Total Share", "Average Project Age", "Average Contributors", "Average PRs", "Average Layers", "Average Treatment Date"),
        format = "html", table.attr = "style='width:50%;'") %>%
  kable_styling(bootstrap_options = c("striped", "hover", "condensed", "responsive"), full_width = FALSE, position = "left")

df_project_departed %>% filter(treated_project == 1) %>%
  write_dta("issue/df_project_departed.dta")

df_project_departed %>% filter(treated_project == 1) %>%
  write_parquet("issue/df_project_departed.parquet")
