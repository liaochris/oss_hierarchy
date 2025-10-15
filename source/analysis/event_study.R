#######################################
# 1. Libraries
#######################################
library(future.apply)
library(tidyverse)
library(did)
library(arrow)
library(gridExtra)
library(ggplot2)
library(egg)
library(eventstudyr)
library(SaveData)
library(future)
library(dplyr)
library(purrr)
library(stringr)
library(fixest)
library(didimputation)
library(did2s)
library(rlang)
library(aod)
library(yaml)
library(fs)
library(png)
library(grid)

source("source/lib/helpers.R")
source("source/analysis/event_study_helpers.R")
#######################################
# 2. Normalization helpers
#######################################
norm_options <- c(TRUE)

#######################################
# 6. Sample restriction helpers
#######################################
CompareBinsFast <- function(df, covar, max_periods = 5, ref_period = 1) {
  bins <- map_dfc(1:max_periods, function(p) {
    per_repo <- df %>%
      group_by(repo_name) %>%
      summarize(
        mean_pre = mean(
          if_else(time_index < quasi_treatment_group & time_index >= (quasi_treatment_group - p),
                  .data[[covar]], NA_real_),
          na.rm = TRUE
        ),
        .groups = "drop"
      )
    
    threshold <- median(per_repo$mean_pre, na.rm = TRUE)
    
    per_repo %>%
      mutate(val = as.integer(mean_pre > threshold)) %>%
      pull(val)
  })
  
  names(bins) <- paste0("bin_", 1:max_periods)
  ref <- bins[[paste0("bin_", ref_period)]]
  
  tibble(
    past_periods = setdiff(1:max_periods, ref_period),
    percent_changed_from_0 = sapply(setdiff(1:max_periods, ref_period), function(p) {
      mean(ref == 0 & bins[[paste0("bin_", p)]] == 1, na.rm = TRUE) * 100
    }),
    percent_changed_from_1 = sapply(setdiff(1:max_periods, ref_period), function(p) {
      mean(ref == 1 & bins[[paste0("bin_", p)]] == 0, na.rm = TRUE) * 100
    })
  )
}

#######################################
# 8. Org practice bin helpers
#######################################
CreateOrgPracticeBin <- function(df, covar, past_periods = 1) {
  col_bin <- paste0(covar, "_2bin")
  
  df %>%
    group_by(repo_name) %>%
    mutate(
      pre_vals   = if_else(time_index < quasi_treatment_group & time_index >= (quasi_treatment_group - past_periods),
                           .data[[covar]], NA_real_),
      mean_pre   = mean(pre_vals, na.rm = TRUE)
    ) %>%
    ungroup() -> tmp
  
  threshold <- median(tmp$mean_pre, na.rm = TRUE)
  
  tmp %>%
    mutate(!!col_bin := if_else(mean_pre > threshold, 1, 0)) %>%
    select(-pre_vals, -mean_pre)
}

#######################################
# 9. Main execution
#######################################
main <- function() {
  SEED <- 420
  set.seed(SEED)
  
  INDIR  <- "drive/output/derived/org_characteristics/org_panel"
  INDIR_YAML <- "source/derived/org_characteristics"
  OUTDIR <- "output/analysis/event_study"
  dir_create(OUTDIR)
  

  DATASETS <- c("important_topk_exact1", "important_topk", "important_topk_oneQual")
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
      
      #######################################
      # Outcomes
      #######################################
      metrics    <- c("cs", "sa")
      metrics_fn <- c("Callaway and Sant'anna 2020", "Sun and Abraham 2020")
      
      for (outcome_mode in outcome_modes) {
        es_list <- lapply(metrics, function(m) {
          panel <- get(outcome_mode$data)
          EventStudy(panel, outcome_mode$outcome, outcome_mode$control_group,
                     m, title = "", normalize = outcome_mode$normalize)
        })
        # PNG
        png(outcome_mode$file)
        CompareES(es_list, title = outcome_mode$outcome, legend_labels = metrics_fn, add_comparison = F)
        dev.off()
        
        # PDF
        CompareES(es_list, title = outcome_mode$outcome, legend_labels = metrics_fn, add_comparison = F)
        
        # Collect coefficients
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
      # Org practice splits
      #######################################
      metrics    <- c("sa")
      metrics_fn <- c("Sun and Abraham 2020")
      
      for (practice_mode in org_practice_modes) {
        covar <- practice_mode$continuous_covariate
        base_df <-  get(outcome_mode$data)
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
        
        # update df_bin_change
        df_bin_change <- rbind(df_bin_change,
                               CompareBinsFast(base_df, covar, 5, 1) %>%
                                 mutate(covar = covar,
                                        dataset = dataset,
                                        rolling = rolling_panel))
        
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
              do.call(CompareES, list(es_list,
                                      legend_labels = labels,
                                      legend_title  = practice_mode$legend_title,
                                      title = paste(outcome_mode$outcome, rolling_panel, title_suffix)))
              dev.off()
              # Collect coefficients
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
                    method = metrics[1],   # split uses cs
                    covar = covar,
                    split_value = split_val
                  )
              }
            }
          }
        }
      }
      
      #######################################
      # Export results (per rolling period)
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
    }
  }
}

main()
