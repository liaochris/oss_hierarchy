#######################################
# 0. Shebang / script header (optional)
#######################################
# run with: Rscript path/to/this_script.R

#######################################
# 1. Libraries
#######################################
library(tidyverse)
library(arrow)
library(yaml)
library(fs)
library(eventstudyr)
library(gridExtra)
library(fixest)
library(png)
library(grid)
library(patchwork)
library(jsonlite)

source("source/lib/helpers.R")
source("source/analysis/event_study/helpers.R")
source("source/analysis/forest/helpers.R")
source("source/analysis/constants.R")

#######################################
# 2. Self-contained helpers
#    (CamelCase functions, snake_case variables)
#######################################
ProcessRepoTreatment <- function(repo_name_raw, time_period_of_treatment, important_members_dir = "output/derived/graph_structure/important_members") {
  NormalizeToUTCMonthStart <- function(x) {
    ts <- as.POSIXct(x, tz = "America/New_York")       # interpret the string correctly
    ts_utc <- format(lubridate::with_tz(ts, "UTC"), "%Y-%m-%d")
    return(ts_utc)
  }
  time_period_of_treatment_utc = NormalizeToUTCMonthStart(time_period_of_treatment)
  
  repo_name_safe <- gsub("/", "_", repo_name_raw, fixed = TRUE)
  
  df_imp <- read.csv(file.path(important_members_dir, paste0(repo_name_safe,".csv"))) %>%
    mutate(
      time_period = as.Date(paste0(time_period, "-01"), tz = "UTC"),
      important_actors = lapply(important_actors, jsonlite::fromJSON),
      important_qualified_actors = lapply(important_qualified_actors, jsonlite::fromJSON),
      dropouts_actors = lapply(dropouts_actors, jsonlite::fromJSON)
    )

  df_filt <- df_imp %>% filter(top_k == 3, consecutive_req == 3, centrality_col == "degree_centrality_z")
  row_treat <- df_filt %>% filter(time_period == time_period_of_treatment_utc)

  focal_person <- row_treat$important_qualified_actors[[1]][1]
  
  presence_flags <- df_filt %>%
    mutate(.has = map_lgl(important_actors, ~ focal_person %in% .x)) %>%
    filter(.has) %>%
    pull(time_period) %>%
    unique()
  
  tibble(repo_name = repo_name_raw,
         time_period = time_period_of_treatment,
         important_file = file.path(important_members_dir, paste0(repo_name_safe, ".csv")),
         found_person = focal_person,
         num_periods_person_in_important_actors = length(presence_flags),
         note = "ok")
}

ComputeOutcomeStats <- function(panel_df, outcome_cols = c("pull_request_opened", "pull_request_merged", "overall_new_release_count")) {
  map_df(outcome_cols, function(col) {
    vec <- panel_df[[col]]
    tibble(outcome = col, median = median(vec, na.rm = T), mean = mean(vec, na.rm = TRUE), sd = sd(vec, na.rm = TRUE))
  })
}

BuildLatexAndSave <- function(outcome_stats, pct_treated, num_projects, num_periods, outdir_ds) {
  outcome_display <- outcome_stats %>%
    transmute(Outcome = outcome,
              Mean = formatC(mean, format = "f", digits = 3),
              Median = ifelse(is.na(median), "", formatC(median, format = "f", digits = 3)),
              SD = formatC(sd, format = "f", digits = 3))

  pct_str <- formatC(pct_treated, format = "f", digits = 3)
  num_projects_str <- as.character(num_projects)
  avg_periods <- ifelse(num_projects > 0, as.numeric(num_periods) / as.numeric(num_projects), NA_real_)
  avg_periods_str <- if (!is.na(avg_periods)) sprintf("%s/%s = %s", as.character(num_periods), as.character(num_projects), formatC(avg_periods, format = "f", digits = 2)) else sprintf("%s/%s", as.character(num_periods), as.character(num_projects))
  summary_df <- tibble(Outcome = c("PctTreated", "NumProjects", "AvgPeriods"),
                       Mean = c(pct_str, num_projects_str, avg_periods_str),
                       Median = c("", "", ""),
                       SD = c("", "", ""))

  outcome_latex <- as.character(knitr::kable(outcome_display, format = "latex", booktabs = TRUE, linesep = ""))
  out_lines <- strsplit(outcome_latex, "\n")[[1]]
  bot_idx <- max(which(grepl("^\\\\bottomrule", out_lines)))
  if (is.na(bot_idx) || length(bot_idx) == 0) bot_idx <- length(out_lines)
  head_lines <- out_lines[1:(bot_idx-1)]  # keep up to row before bottomrule
  # build latex lines for summary rows
  # convert each summary row to a LaTeX table row (escape content)
  make_row <- function(r) {
    # ensure any & or % etc are escaped minimally
    esc <- function(s) gsub("([&%#$_{}~^\\\\])", "\\\\\\1", s)
    paste0(esc(r["Outcome"]), " & ", esc(r["Mean"]), " & ", esc(r["Median"]), " & ", esc(r["SD"]), " \\\\")
  }
  summary_lines <- apply(summary_df, 1, make_row)
  # assemble full latex
  full_lines <- c(
    head_lines,
    "\\midrule",
    summary_lines,
    "\\bottomrule",
    "\\end{tabular}"
  )
  # print result to console (LaTeX)
  cat("\n% ----- Combined outcome + summary table (LaTeX) -----\n")
  cat(paste0(full_lines, collapse = "\n"), "\n")
  invisible(TRUE)
}


#######################################
# 9. Main execution
#######################################
Main <- function() {
  INDIR  <- "drive/output/derived/org_outcomes_practices/org_panel"
  INDIR_CF  <- "output/analysis/causal_forest_personalization"
  INDIR_CF_DS <- "drive/output/analysis/causal_forest_personalization"
  INDIR_YAML <- "source/analysis/config"
  OUTDIR <- "output/analysis/event_study_personalization"
  dir_create(OUTDIR)

  DATASETS <- c("important_degree_top3")
  exclude_outcomes <- c("num_downloads")
  outcome_cfg <- yaml.load_file(file.path(INDIR_YAML, "outcomes.yaml"))
  
  png_ordered <- character(0)
  
  for (dataset in DATASETS) {
    for (rolling_panel in c("rolling5")) {
      rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
      for (method in c("lm_forest_nonlinear")) {
        message("Processing dataset: ", dataset, " (", rolling_panel, ")")
        outdir_ds <- file.path(OUTDIR, dataset)
        dir_create(outdir_ds, recurse = TRUE)
        
        panel_dataset <- NormalizeDatasetName(dataset)
        dataset_labels <- MakeDatasetLabels(dataset)
        num_qualified_label   <- dataset_labels$num_qualified
        What_estimation_label <- dataset_labels$what_estimation
        
        df_panel <- read_parquet(file.path(INDIR, panel_dataset, paste0("panel_", rolling_panel, ".parquet")))
        all_outcomes <- unlist(lapply(outcome_cfg, function(x) x$main))
        panel <- BuildCommonSample(df_panel, all_outcomes)
        
        if (grepl("_exact1", dataset)) {
          panel <- KeepSustainedImportant(panel, lb = 1, ub = 1)
        } else if (grepl("_oneQual", dataset)) {
          panel <- KeepSustainedImportant(panel)
        }
        
        panel_notyettreated <- panel %>% filter(num_departures == 1)
        panel_nevertreated  <- panel %>% filter(num_departures <= 1)
        panels <- list(
          nevertreated  = panel_nevertreated,
          notyettreated = panel_notyettreated
        )

        outcome_modes <- BuildOutcomeModes(outcome_cfg, "nevertreated", outdir_ds,
                                           NORM_OPTIONS, build_dir = FALSE)
        coeffs_all <- list()
        
        for (use_imp in c(FALSE)) {
          for (has_pc in c(TRUE)) {
            rolling_panel_imp <- ifelse(use_imp, paste0(rolling_panel, "_imp"), rolling_panel)
            has_pc_suffix  <- ifelse(has_pc == TRUE, "_pc", ifelse(has_pc == "median", "_pc_median", ""))
            rolling_panel_imp <- paste0(rolling_panel_imp, has_pc_suffix)
            
            split_var <- "pull_request_merged"
            estimation_type <- "observed"

            df_causal_forest_bins <- read_parquet(
              file.path(INDIR_CF, dataset, rolling_panel_imp,
                        paste0(split_var, "_repo_att_", method, ".parquet"))
            ) %>%
              filter(type == estimation_type)
            
            ## identify pc1 score columns (used in place of former 'bin_vars')
            pc1_scores <- colnames(df_causal_forest_bins)[grepl("pc1_mean", colnames(df_causal_forest_bins))]
            
            ## build labelled pc1_df for correlation heatmap
            pc1_df <- df_causal_forest_bins %>%
              select(all_of(pc1_scores)) %>%
              rename(
                `Collaboration` = collaboration_pc1_mean,
                `Knowledge level` = shared_knowledge_pc1_mean,
                `Discussion quality` = discussion_quality_pc1_mean,
                `Investment in new talent` = investment_in_new_talent_pc1_mean,
                `Problem-solving routines` = problem_solving_routines_pc1_mean
              )
            
            corr_mat <- cor(pc1_df, use = "pairwise.complete.obs")
            
            corr_tidy <- corr_mat %>%
              as.data.frame() %>%
              tibble::rownames_to_column("var1") %>%
              pivot_longer(-var1, names_to = "var2", values_to = "correlation") %>%
              mutate(
                label = sprintf("%.2f", correlation),
                text_color = ifelse(abs(correlation) > 0.4, "white", "black")
              )
            
            ## heatmap (saved)
            ggplot(corr_tidy, aes(x = var1, y = var2, fill = correlation)) +
              geom_tile() +
              geom_text(aes(label = label, color = text_color), size = 5) +
              scale_color_identity() +
              scale_fill_gradient2(limits = c(-1, 1), midpoint = 0) +
              coord_fixed(0.8) +
              theme_minimal(base_size = 4) +
              theme(
                plot.margin = margin(0, 0, 0, 0),
                axis.text.x = element_text(angle = 30, hjust = 1, size = 16),
                axis.text.y = element_text(size = 16),
                legend.title = element_text(size = 16),
                legend.text = element_text(size = 10)
              ) +
              labs(x = "", y = "", fill = "Correlation\n")
            
            ggsave(file.path(outdir_ds, "heatmap_corr.png"), width = 10, height = 6, dpi = 300)
            
            df_causal_forest_bins <- df_causal_forest_bins %>%
              mutate(across(all_of(pc1_scores),
                            ~ ifelse(.x > median(.x, na.rm = TRUE), "high", "low"),
                            .names = "{.col}"))
            
            # df_summary computation: group by pc1_scores (instead of bin_vars)
            df_summary <- df_causal_forest_bins %>%
              group_by(across(all_of(pc1_scores))) %>%
              summarize(att_dr_mean = mean(att_dr, na.rm = TRUE),
                        high_resilience = mean(att_dr_group == "high", na.rm = TRUE),
                        count = n(),
                        .groups = "drop") %>%
              arrange(-att_dr_mean) %>%
              ungroup() %>%
              mutate(rank = row_number())
            
            PlotHighLowGrid <- function(df_summary) {
              library(ggplot2)
              library(dplyr)
              library(tidyr)
              practice_vars <- c("collaboration_pc1_mean", "shared_knowledge_pc1_mean",
                                 "discussion_quality_pc1_mean", "investment_in_new_talent_pc1_mean",
                                 "problem_solving_routines_pc1_mean")
              
              pretty_labels <- c(
                "collaboration_pc1_mean"             = "Collaboration",
                "shared_knowledge_pc1_mean"          = "Knowledge level",
                "discussion_quality_pc1_mean"        = "Discussion quality",
                "investment_in_new_talent_pc1_mean"  = "Investment in new talent",
                "problem_solving_routines_pc1_mean"  = "Problem-solving routines",
                "info"                               = ""
              )
              
              rank_levels <- as.character(1:32)
              practice_vars_extended <- c(practice_vars, "info")
              
              df_tiles <- df_summary %>%
                select(rank, all_of(practice_vars), att_dr_mean, count) %>%
                mutate(rank = as.character(rank)) %>%
                pivot_longer(cols = all_of(practice_vars), names_to = "practice", values_to = "raw_value") %>%
                mutate(rank = factor(rank, levels = rank_levels),
                       practice = factor(practice, levels = practice_vars_extended))
              
              df_tiles <- df_tiles %>%
                group_by(practice) %>%
                mutate(level = {
                  if (all(is.na(raw_value))) {
                    factor("low", levels = c("low", "high"))
                  } else if (all(raw_value %in% c("low", "high", NA))) {
                    factor(as.character(raw_value), levels = c("low", "high"))
                  } else {
                    med <- median(as.numeric(raw_value), na.rm = TRUE)
                    factor(ifelse(as.numeric(raw_value) > med, "high", "low"), levels = c("low", "high"))
                  }
                }) %>%
                ungroup()
              
              df_info <- df_summary %>%
                transmute(rank = as.character(rank),
                          att_dr_mean = round(as.numeric(att_dr_mean), 2),
                          count = as.integer(count)) %>%
                mutate(rank = factor(rank, levels = rank_levels),
                       practice = factor("info", levels = practice_vars_extended),
                       num_str = sprintf("%.2f", att_dr_mean),
                       pre_digits = nchar(sub("\\..*$", "", num_str)),
                       max_pre = max(pre_digits),
                       left_pad = pmax(0, max_pre - pre_digits),
                       aligned_num = paste0(strrep(" ", left_pad), num_str),
                       label = paste0(aligned_num, " | n=", count))
              
              df_plot <- bind_rows(df_tiles %>% select(rank, practice, level, raw_value),
                                   df_info %>% mutate(level = NA_character_, raw_value = NA) %>% select(rank, practice, level, raw_value)) %>%
                mutate(level = ifelse(level == "high", "High", "Low"))
              x_info_pos <- length(practice_vars_extended)
              y_header_pos <- 0.4
              
              ggplot(df_plot, aes(x = practice, y = rank, fill = level)) +
                geom_tile(data = filter(df_plot, practice != "info"), color = "white", size = 1.5) +
                geom_text(data = df_info, mapping = aes(x = practice, y = rank, label = label), inherit.aes = FALSE, hjust = 0, size = 5) +
                geom_segment(inherit.aes = FALSE, aes(x = 0.5, xend = length(practice_vars_extended) - 0.5, y = 16.5, yend = 16.5),
                             color = "red", linewidth = 1.5, alpha = .7) +
                scale_fill_manual(values = c("Low" = "lightblue", "High" = "darkblue"),
                                  name = "Coarsened\nPractice Score") +
                annotate("text", x = x_info_pos, y = y_header_pos, label = "Mean DR ATT | Organization Count", vjust = -56.5, hjust = .25, size = 6) +
                labs(x = NULL, y = "Rank") +
                theme_minimal(base_size = 14) +
                theme(
                  axis.text.y = element_text(hjust = 0, size = 22),
                  axis.text.x = element_text(angle = 30, hjust = 1, size = 22),
                  axis.title.y = element_text(size = 22, vjust = .5, angle = 0),
                  axis.ticks = element_blank(),
                  panel.grid = element_blank(),
                  legend.title = element_text(size = 18),
                  legend.text = element_text(size = 18),
                  plot.margin = unit(c(1.5, 1.5, 0.5, 0.5), "lines")
                ) +
                coord_fixed(ratio = 0.25, clip = "off") +
                scale_y_discrete(limits = rev(rank_levels)) +
                scale_x_discrete(
                  labels = function(x) {
                    vapply(x, function(xx) if (!is.null(pretty_labels[[xx]])) pretty_labels[[xx]] else xx, FUN.VALUE = character(1))
                  },
                  expand = expansion(add = c(0.2, 1.2))
                )
            }
            PlotHighLowGrid(df_summary)
            ggsave(file.path(outdir_ds, "practice_combo.png"),  dpi = 600)
            
            final_panel <- panel_nevertreated %>%
              filter(repo_name %in% df_causal_forest_bins$repo_name)
            
            # ---------------------------
            # New block: compute outcomes + treatment diagnostics
            # ---------------------------
            outcome_cols_local <- c("pull_request_opened", "pull_request_merged", "overall_new_release_count")
            outcome_stats <- ComputeOutcomeStats(final_panel, outcome_cols_local)
            pct_treated <- mean(final_panel %>% select(repo_name, treatment_group) %>% pull(treatment_group) != 0, na.rm = TRUE)
            df_treatment_date <- final_panel %>% filter(time_index == quasi_treatment_group) %>% select(repo_name, time_period) %>% distinct() %>% mutate(repo_name_safe = gsub("/", "_", repo_name, fixed = TRUE))
            important_members_dir <- "output/derived/graph_structure/important_members"
            num_projects <- n_distinct(df_treatment_date$repo_name)
            num_periods <- nrow(final_panel)
            BuildLatexAndSave(outcome_stats, pct_treated, num_projects, num_periods, outdir_ds)
            # ---------------------------
            # end new block
            # ---------------------------

            final_panel <- final_panel %>%
              group_by(repo_name) %>%
              mutate(
                quasi_treatment_date = time_period[time_index == quasi_treatment_group][1]
              ) %>%
              ungroup()
            
            quasi_treatment_dates <- final_panel %>% select(repo_name, quasi_treatment_date) %>% unique() %>% pull(quasi_treatment_date)
            treatment_dates <- final_panel %>% filter(treatment_group != 0) %>% select(repo_name, treatment_date) %>% unique() %>% pull(treatment_date)
            dates <- final_panel %>% pull(time_period)
            
            date_df <- bind_rows(
              tibble(date = quasi_treatment_dates, type = "Quasi-Treatment Date"),
              tibble(date = treatment_dates, type = "Treatment Date"),
              tibble(date = dates, type = "Observed")
            )
            
            counts <- date_df %>%
              group_by(date, type) %>%
              summarise(count = n(), .groups = "drop")
            # Ensure the plotting order: Observed → Quasi → Treatment
            counts$type <- factor(
              counts$type,
              levels = c("Observed", "Quasi-Treatment Date", "Treatment Date")
            )
            
            ggplot(counts, aes(x = date, y = count, fill = type)) +
              # Overlapping columns, not stacked
              geom_col(color = "black", linewidth = 0.3, position = "identity", alpha = 1) +
              
              scale_x_date(
                date_breaks = "1 year",
                date_labels = "%Y",
                expand = c(0, 0)
              ) +
              
              labs(
                x = "Date",
                y = "# of Organizations",
                fill = "Appearance Type"
              ) +
              
              theme_minimal(base_size = 21) +
              theme(
                text = element_text(size = 21),
                axis.text.x = element_text(angle = 45, hjust = 1),
                legend.position = "top"
              )
            
            
            ggsave(file.path(outdir_ds, "hist_org_count.png"), width = 10, height = 5)
            
            
            # how long they had been a single period key member based on parquet
          } # end has_pc loop
        } # end use_imp loop
        
      } # end method loop
    } # end rolling_panel loop
  } # end dataset loop
  
  invisible(NULL)
} # end main

Main()
