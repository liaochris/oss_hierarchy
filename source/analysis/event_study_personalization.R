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
# 9. Main execution
#######################################
main <- function() {
  SEED <- 420
  set.seed(SEED)
  
  INDIR  <- "drive/output/derived/org_characteristics/org_panel"
  INDIR_CF  <- "output/analysis/causal_forest_personalization"
  INDIR_YAML <- "source/derived/org_characteristics"
  OUTDIR <- "output/analysis/event_study_personalization"
  dir_create(OUTDIR)
  
  DATASETS <- c("important_topk", "important_thresh")
  exclude_outcomes <- c("num_downloads")
  norm_options <- c(TRUE)
  outcome_cfg      <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))

  plan(multisession, workers = availableCores() - 1)
  
  for (dataset in DATASETS) {
    for (rolling_panel in c("rolling5", "rolling1")) {
      rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
      for (top in c("all")) {
        for (method in c("lm_forest", "lm_forest_nonlinear")) {
          message("Processing dataset: ", dataset, " (", rolling_panel, ")")
          top_str <- ifelse(top == "all", top, paste0("top", top))
          rolling_top_str <- paste(rolling_panel, top_str, sep = "_")
          outdir_dataset <- file.path(OUTDIR, dataset)
          dir_create(outdir_dataset, recurse = TRUE)
          
          df_panel <- read_parquet(file.path(INDIR, dataset, paste0("panel_", rolling_panel, ".parquet")))
          all_outcomes <- unlist(lapply(outcome_cfg, function(x) x$main))
          df_panel_common <- BuildCommonSample(df_panel, all_outcomes)
          
          df_panel_notyettreated <- df_panel_common %>% filter(num_departures == 1)
          df_panel_nevertreated  <- df_panel_common %>% filter(num_departures <= 1)
          
          assign("df_panel_notyettreated", df_panel_notyettreated, envir = .GlobalEnv)
          assign("df_panel_nevertreated",  df_panel_nevertreated,  envir = .GlobalEnv)
          
          outcome_modes      <- BuildOutcomeModes(outcome_cfg, "nevertreated", outdir_dataset,
                                                  norm_options)
          coeffs_all <- list()
    
          #######################################
          # Org practice splits
          #######################################
          metrics    <- c("cs")
          metrics_fn <- c("Callaway and Sant'anna 2020")
          
          for (split_mode in list(outcome_modes[[2]])) {
            split_var <- split_mode$outcome
            control_group <- "nevertreated"
            practice_mode <- list(
              continuous_covariate = "att_group",
              filters = list(list(col = "att_group", vals = c("high", "low"))),
              legend_labels = c("High", "Low"),
              legend_title = paste0("by Causal Forest ATT for ", split_var, 
                                    "\nUsing ", top_str, " covariates"),
              control_group = control_group,
              data = paste0("df_panel_",control_group),
              folder = file.path("output/analysis/event_study_personalization", 
                                 dataset, rolling_top_str, control_group)
            )
            
            df_causal_forest_bins <- read.csv(
              file.path(INDIR_CF, dataset, rolling_top_str, 
                        paste0(split_var, "_repo_att_", method, "_", rolling_top_str, ".csv")))
            
            covar <- practice_mode$continuous_covariate
            for (outcome_mode in outcome_modes) {
              base_df <-  get(outcome_mode$data)
              base_df <- base_df %>%
                left_join(df_causal_forest_bins %>% select(repo_name, att_group))
              
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
                  dir.create(practice_mode$folder, recursive = TRUE)
                  out_path <- file.path(practice_mode$folder,
                                        paste0(outcome_mode$outcome, norm_str, "_split_",
                                        split_mode$outcome, "_", method, ".png"))
                  png(out_path)
                  do.call(CompareES, list(es_list,
                                          legend_labels = labels,
                                          legend_title  = practice_mode$legend_title,
                                          title = paste(outcome_mode$outcome, rolling_panel)))
                  dev.off()
                  
                  # PDF
                  do.call(CompareES, list(es_list,
                                          legend_labels = labels,
                                          legend_title  = practice_mode$legend_title,
                                          title = paste(outcome_mode$outcome, rolling_panel)))
                  
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
        }
      }
    }
    
    #######################################
    # Export results (per rolling period)
    #######################################
    coeffs_df <- bind_rows(coeffs_all) %>%
      mutate(event_time = as.numeric(event_time))
    write_csv(coeffs_df, file.path(outdir_dataset,  paste0("all_coefficients.csv")))
    
    #######################################
    # Combine PNGs into one PDF (per rolling period)
    #   -> event study plots only (exclude histograms)
    #######################################
    png_files <- list.files(outdir_dataset, 
                            pattern = "\\.png$", 
                            full.names = TRUE, recursive = TRUE)
    
    # keep only PNGs that are NOT histograms
    png_files <- png_files[!grepl("_hist_", png_files)]
    
    pdf(file.path(outdir_dataset, paste0("all_results.pdf")))
    if (length(png_files) > 0) {
      imgs <- lapply(png_files[1:min(4, length(png_files))],
                     function(f) grid::rasterGrob(readPNG(f), interpolate = TRUE))
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


#######################################
# Run
#######################################
if (sys.nframe() == 0) {
  main()
}
