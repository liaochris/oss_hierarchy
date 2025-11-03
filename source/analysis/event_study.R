#######################################
# Event-study main script (uses helpers from separate files)
#######################################

# Libraries
library(tidyverse)
library(arrow)
library(yaml)
library(fs)
library(eventstudyr)
library(gridExtra)
library(png)
library(grid)
library(future.apply)
library(SaveData)

# Other packages (used by helpers and plotting)
library(dplyr)
library(tidyr)
library(broom)
library(fixest)
library(did)
library(aod)
library(stringr)
library(tibble)
library(purrr)
library(ggplot2)

source("source/lib/helpers.R")
source("source/analysis/event_study_helpers.R")

norm_options <- c(TRUE)

main <- function() {
  SEED <- 420
  set.seed(SEED)
  
  INDIR  <- "drive/output/derived/org_characteristics/org_panel"
  INDIR_YAML <- "source/derived/org_characteristics"
  OUTDIR <- "output/analysis/event_study"
  dir_create(OUTDIR)
  
  DATASETS <- c("important_topk_exact1")
  exclude_outcomes <- c("num_downloads")
  
  outcome_cfg      <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))
  org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariate_organization.yaml"))
  
  plan(multisession, workers = availableCores() - 1)
  
  for (dataset in DATASETS) {
    for (rolling_panel in c("rolling5")) {
      message("Processing dataset: ", dataset, " (", rolling_panel, ")")
      
      outdir_dataset <- file.path(OUTDIR, dataset, rolling_panel)
      dir_create(outdir_dataset, recurse = TRUE)
      
      panel_dataset <- gsub("_exact1", "", dataset)
      panel_dataset <- gsub("_oneQual", "", panel_dataset)
      num_qualified_label <- ifelse(grepl("_exact1", dataset), "num-qualified=1",
                                    ifelse(grepl("_oneQual", dataset), "num-qualified>=1", "all obs"))
      df_panel <- read_parquet(file.path(INDIR, panel_dataset, paste0("panel_", rolling_panel, ".parquet")))
      
      all_outcomes <- unlist(lapply(outcome_cfg, function(x) x$main))
      df_panel_common <- BuildCommonSample(df_panel, all_outcomes)
      if (endsWith(dataset, "exact1")) {
        df_panel_common <- KeepSustainedImportant(df_panel_common, lb = 1, ub = 1)
      } else {
        df_panel_common <- KeepSustainedImportant(df_panel_common)
      }
      
      df_panel_notyettreated <- df_panel_common %>% filter(num_departures == 1)
      df_panel_nevertreated  <- df_panel_common %>% filter(num_departures <= 1)
      
      assign("df_panel_notyettreated", df_panel_notyettreated, envir = .GlobalEnv)
      assign("df_panel_nevertreated",  df_panel_nevertreated,  envir = .GlobalEnv)
      
      outcome_modes      <- BuildOutcomeModes(outcome_cfg, "nevertreated", outdir_dataset,
                                              norm_options, build_dir = TRUE)
      org_practice_modes <- BuildOrgPracticeModes(org_practice_cfg, "nevertreated", outdir_dataset, build_dir = TRUE)
      
      coeffs_all <- list()
      df_bin_change <- data.frame()
      att_diffs_all <- list()
      
      #######################################
      # Baseline outcome plots + collect coeffs
      #######################################
      metrics    <- c("sa")
      metrics_fn <- c("Sun and Abraham 2020")
      
      for (outcome_mode in outcome_modes) {
        es_list <- lapply(metrics, function(m) {
          panel <- get(outcome_mode$data)
          EventStudy(panel, outcome_mode$outcome, outcome_mode$control_group,
                     m, title = "", normalize = outcome_mode$normalize)
        })
        png(outcome_mode$file)
        CompareES(es_list, title = "", legend_labels = metrics_fn, add_comparison = F,
                  ylim = c(-1.75, .75))
        dev.off()
        
        for (i in seq_along(es_list)) {
          res <- es_list[[i]]$results
          coeffs_all[[length(coeffs_all) + 1]] <- as_tibble(res, rownames = "event_time") %>%
            mutate(
              dataset = dataset,
              rolling = rolling_panel,
              category = outcome_mode$category,
              outcome = outcome_mode$outcome,
              normalize = outcome_mode$normalize,
              method = metrics[i],
              covar = NA_character_,
              split_value = NA_real_
            )
        }
      }
      
      #######################################
      # Org practice splits + ATT-diff computation
      #######################################
      metrics    <- c("sa")
      
      for (practice_mode in org_practice_modes) {
        covar <- practice_mode$continuous_covariate
        base_df <- get(outcome_mode$data)
        rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
        
        if (rolling_period == 1) {
          base_df <- CreateOrgPracticeBin(base_df, covar, past_periods = 5)
        } else {
          base_df <- base_df %>%
            group_by(repo_name) %>%
            mutate(
              !!paste0(covar, "_2bin") :=
                if_else(time_index == (quasi_treatment_group - 1) & !is.na(.data[[covar]]),
                        if_else(.data[[covar]] > median(.data[[covar]], na.rm = TRUE), 1, 0),
                        NA_real_)
            ) %>%
            fill(!!paste0(covar, "_2bin"), .direction = "downup") %>%
            ungroup()
        }
        
        df_bin_change <- rbind(df_bin_change,
                               CompareBinsFast(base_df, covar, 5, 1) %>%
                                 mutate(covar = covar,
                                        dataset = dataset,
                                        rolling = rolling_panel))
        
        # compute ATT differences using first filter's first two vals as low/high
        if (length(practice_mode$filters) >= 1) {
          split_filter <- practice_mode$filters[[1]]
          split_col <- split_filter$col
          split_vals <- split_filter$vals
          if (length(split_vals) >= 2) {
            val_low <- split_vals[[1]]
            val_high <- split_vals[[2]]
            for (outcome_mode in outcome_modes) {
              for (norm in norm_options) {
                df_low  <- base_df %>% filter(.data[[split_col]] == val_low)
                df_high <- base_df %>% filter(.data[[split_col]] == val_high)
                att_low <- tryCatch(
                  GetEventAtt(df_low, outcome_mode$outcome, outcome_mode$control_group, method = "sa", normalize = norm, periods = 1:5),
                  error = function(e) NULL
                )
                
                att_high <- tryCatch(
                  GetEventAtt(df_high, outcome_mode$outcome, outcome_mode$control_group, method = "sa", normalize = norm, periods = 1:5),
                  error = function(e) NULL
                )
                
                # Skip if either estimation failed or returned NULL
                if (is.null(att_low) || is.null(att_high)) next
                
                att_diff <- ComputeDiffHighLow(att_high, att_low) %>%
                  mutate(dataset = dataset,
                         rolling = rolling_panel,
                         category = outcome_mode$category,
                         outcome = outcome_mode$outcome,
                         normalize = norm,
                         covar = covar,
                         split_col = split_col,
                         low_value = as.character(val_low),
                         high_value = as.character(val_high)) %>%
                  select(dataset, rolling, category, outcome, normalize, covar, split_col, low_value, high_value,
                         period, high_estimate, high_sd, high_ci_low, high_ci_high,
                         low_estimate, low_sd, low_ci_low, low_ci_high,
                         diff_estimate, diff_se, diff_ci_low, diff_ci_high)
                
                att_diffs_all[[length(att_diffs_all) + 1]] <- att_diff
              }
            }
          }
        }
        
        # existing split-based event study (keeps previous behavior)
        for (outcome_mode in outcome_modes) {
          for (norm in norm_options) {
            norm_str <- ifelse(norm, "_norm", "")
            combo_grid <- expand.grid(lapply(practice_mode$filters, `[[`, "vals"),
                                      KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
            
            es_list <- apply(combo_grid, 1, function(vals_row) {
              df_sub <- base_df
              for (i in seq_along(vals_row)) {
                col_name <- practice_mode$filters[[i]]$col
                df_sub <- df_sub %>% filter(.data[[col_name]] == vals_row[[i]])
              }
              tryCatch(EventStudy(df_sub,
                                  outcome_mode$outcome,
                                  practice_mode$control_group,
                                  method = metrics,
                                  title = "",
                                  normalize = norm),
                       error = function(e) NULL)
            }, simplify = FALSE)
            
            success_idx <- which(!sapply(es_list, is.null))
            es_list <- es_list[success_idx]
            labels <- practice_mode$legend_labels[success_idx]
            
            if (length(es_list) > 0) {
              out_path <- file.path(practice_mode$folder,
                                    paste0(outcome_mode$outcome, norm_str, ".png"))
              png(out_path)
              title_suffix <- paste0("\nSample: ", num_qualified_label)
              ylim <- NULL
              legend_title <- practice_mode$legend_title
              if (covar %in% c("mean_has_assignee", "ov_sentiment_avg", "issue_share_only")) {
                ylim <- c(-3, 1.5)
                legend_title <- NULL
              }
              do.call(CompareES, list(es_list,
                                      legend_labels = labels,
                                      legend_title  = legend_title,
                                      title = "",
                                      ylim = ylim))
              dev.off()
              for (j in seq_along(es_list)) {
                res <- es_list[[j]]$results
                split_val <- practice_mode$filters[[1]]$vals[j]
                coeffs_all[[length(coeffs_all) + 1]] <- as_tibble(res, rownames = "event_time") %>%
                  mutate(
                    dataset = dataset,
                    rolling = rolling_panel,
                    category = outcome_mode$category,
                    outcome = outcome_mode$outcome,
                    normalize = norm,
                    method = metrics[1],
                    covar = covar,
                    split_value = split_val
                  )
              }
            }
          }
        }
      }
      
      #######################################
      # Export results
      #######################################
      coeffs_df <- bind_rows(coeffs_all) %>%
        mutate(event_time = as.numeric(event_time),
               covar = ifelse(is.na(covar), "", covar),
               split_value = ifelse(is.na(split_value), "", split_value))
      SaveData(coeffs_df,
               c("dataset","rolling",
                 "category","outcome","normalize","method","covar","split_value", "event_time"),
               file.path(outdir_dataset,  paste0("all_coefficients","_",rolling_panel,".csv")),
               file.path(outdir_dataset,  paste0("all_coefficients","_",rolling_panel,".log")),
               sortbykey = FALSE)
      
      SaveData(df_bin_change,
               c("covar","dataset","rolling","past_periods"),
               file.path(outdir_dataset, paste0("bin_change","_",rolling_panel,".csv")),
               file.path(outdir_dataset, paste0("bin_change","_",rolling_panel,".log")),
               sortbykey = FALSE)
      
      if (length(att_diffs_all) > 0) {
        att_diffs_df <- bind_rows(att_diffs_all) %>% mutate(period = as.numeric(period))
        SaveData(att_diffs_df,
                 c("dataset","rolling","category","outcome","normalize","covar","split_col","low_value","high_value","period"),
                 file.path(outdir_dataset, paste0("att_diff_high_low_", rolling_panel, ".csv")),
                 file.path(outdir_dataset, paste0("att_diff_high_low_", rolling_panel, ".log")),
                 sortbykey = FALSE)
        
        practice_map_file <- "source/analysis/organizational_practice_map.csv"
        outcome_map <- c(
          "major_minor_release_count" = "Major and minor software releases",
          "overall_new_release_count" = "New software releases",
          "pull_request_opened" = "Pull requests opened",
          "pull_request_merged" = "Pull requests merged"
        )
        
        map_df <- ReadPracticeMap(practice_map_file)
        
        PlotPracticeOutcomeHorizontal(att_diffs_df,
                                      out_file = file.path(outdir_dataset, "att_by_practice_outcomes_grouped.png"),
                                      practice_map = map_df,
                                      outcome_map = outcome_map,
                                      periods = 1:5,
                                      label_offset = 0.43,    # fraction of x_range: labels distance left of plotted area
                                      bracket_offset = 0.45,  # fraction of x_range: bracket further left than labels
                                      right_pad = 0.18,       # fraction of x_range to pad right side of axis
                                      left_margin_pt = 0,
                                      cap_height = 0.12,
                                      cap_thickness = 1.0,
                                      point_size = 3,
                                      point_alpha = 0.6,
                                      width = 10,
                                      height = 8)
      }
      
      #######################################
      # Combine PNGs into one PDF (per rolling period)
      #######################################
      png_files <- list.files(outdir_dataset, pattern = "\\.png$", full.names = TRUE, recursive = TRUE)
      pdf(file.path(outdir_dataset, paste0("all_results","_",rolling_panel,".pdf")))
      if (length(png_files) > 0) {
        imgs <- lapply(png_files[1:min(4, length(png_files))], function(f) grid::rasterGrob(readPNG(f), interpolate = TRUE))
        do.call(grid.arrange, c(imgs, ncol = 2))
        if (length(png_files) > 4) {
          folder_groups <- split(png_files[-(1:4)], dirname(png_files[-(1:4)]))
          for (g in seq_along(folder_groups)) {
            folder_files <- folder_groups[[g]]
            for (i in seq(1, length(folder_files), by = 4)) {
              group_files <- folder_files[i:min(i+3, length(folder_files))]
              imgs <- lapply(group_files, function(f) grid::rasterGrob(readPNG(f), interpolate = TRUE))
              do.call(grid.arrange, c(imgs, ncol = 2))
            }
          }
        }
      }
      dev.off()
    } # end rolling_panel for
  } # end dataset for
} # end main

# run main
main()
