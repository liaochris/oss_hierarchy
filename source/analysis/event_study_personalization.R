#######################################
# 1. Libraries
#######################################
library(tidyverse)  
library(arrow)       
library(yaml)      
library(fs)          
library(eventstudyr) 
library(gridExtra)  
library(dplyr)
library(fixest)
library(png)       
library(grid)        

source("source/lib/helpers.R")
source("source/analysis/event_study_helpers.R")

CoalesceEventTimes <- function(df) {
  cols <- names(df)[str_detect(names(df), "^cohort\\d+event_time\\.?\\d+$")]
  if (length(cols) == 0) return(tibble::as_tibble(df[0])[rep(1, nrow(df)),][FALSE])
  meta <- tibble::tibble(col = cols) %>%
    mutate(num = str_extract(col, "[0-9]+$"),
           dot = str_detect(col, "event_time\\."),
           label = if_else(dot, paste0("event_time.", num), paste0("event_time", num)))
  df %>%
    mutate(.row = row_number()) %>%
    pivot_longer(all_of(cols), names_to = "col", values_to = "val") %>%
    left_join(meta, by = "col") %>%
    group_by(.row, label) %>%
    summarize(val = val[which(!is.na(val))[1]], .groups = "drop") %>%
    pivot_wider(names_from = label, values_from = val) %>%
    arrange(.row) %>%
    select(-.row)
}

#######################################
# 9. Main execution
#######################################
main <- function() {
  SEED <- 420
  set.seed(SEED)
  
  INDIR  <- "drive/output/derived/org_characteristics/org_panel"
  INDIR_CF  <- "output/analysis/causal_forest_personalization"
  INDIR_CF_DS <- "drive/output/analysis/causal_forest_personalization"
  INDIR_YAML <- "source/derived/org_characteristics"
  OUTDIR <- "output/analysis/event_study_personalization"
  dir_create(OUTDIR)
  

  # "important_topk_defaultWhat", "important_topk_exact1_defaultWhat","important_topk_oneQual_defaultWhat",
  # "important_topk_nuclearWhat", "important_topk_exact1_nuclearWhat","important_topk_oneQual_nuclearWhat"
  # "important_topk", ,"important_topk_oneQual", "important_thresh","important_thresh_oneQual", "important_thresh_exact1"
  DATASETS <- c( "important_topk_exact1")
  exclude_outcomes <- c("num_downloads")
  norm_options <- c(TRUE)
  outcome_cfg      <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))
  
  png_ordered <- character(0)  
  for (dataset in DATASETS) {
    for (rolling_panel in c("rolling5")) {
      rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
      for (method in c("lm_forest_nonlinear")) {  
        message("Processing dataset: ", dataset, " (", rolling_panel, ")")
        outdir_dataset <- file.path(OUTDIR, dataset)
        dir_create(outdir_dataset, recurse = TRUE)
        
        panel_dataset <- gsub("_exact1", "", dataset)
        panel_dataset <- gsub("_nuclearWhat", "", panel_dataset)
        panel_dataset <- gsub("_defaultWhat", "", panel_dataset)
        panel_dataset <- gsub("_oneQual", "", panel_dataset)
        
        num_qualified_label <- ifelse(grepl("_exact1", dataset), "num-qualified=1",
                                      ifelse(grepl("_oneQual", dataset), "num-qualified>=1", "all obs"))
        What_estimation_label <- ifelse(grepl("_nuclearWhat", dataset), "only cohort + exact time", 
                                        ifelse(grepl("_defaultWhat", dataset), "default", "all treated cohorts + exact time matches"))
        
        df_panel <- read_parquet(file.path(INDIR, panel_dataset, paste0("panel_", rolling_panel, ".parquet")))
        all_outcomes <- unlist(lapply(outcome_cfg, function(x) x$main))
        df_panel_common <- BuildCommonSample(df_panel, all_outcomes)
        if (grepl("_exact1", dataset)) {
          df_panel_common <- KeepSustainedImportant(df_panel_common, lb = 1, ub = 1)
        } else if (grepl("_oneQual", dataset)) {
          df_panel_common <- KeepSustainedImportant(df_panel_common)
        } 
        df_panel_notyettreated <- df_panel_common %>% filter(num_departures == 1)
        df_panel_nevertreated  <- df_panel_common %>% filter(num_departures <= 1)
        
        assign("df_panel_notyettreated", df_panel_notyettreated, envir = .GlobalEnv)
        assign("df_panel_nevertreated",  df_panel_nevertreated,  envir = .GlobalEnv)
        
        outcome_modes      <- BuildOutcomeModes(outcome_cfg, "nevertreated", outdir_dataset,
                                                norm_options, build_dir = FALSE)
        coeffs_all <- list()
        
        #######################################
        # Org practice splits (including optional _imp and optional _pc suffix)
        #######################################
        metrics    <- c("sa")
        metrics_fn <- c("Sun and Abraham 2020")
        
        for (use_imp in c(FALSE)) {
          for (has_pc in c(FALSE, TRUE, "median")) {
            # build combined suffix: _imp and optionally _pc
            rolling_panel_imp <- ifelse(use_imp, paste0(rolling_panel, "_imp"), rolling_panel)
            has_pc_suffix  <- ifelse(has_pc == TRUE, "_pc", ifelse(has_pc == "median", "_pc_median", ""))
            rolling_panel_imp <- paste0(rolling_panel_imp, has_pc_suffix)
            
            # read CF bins from the matching folder (now includes _pc when requested)
            for (split_mode in outcome_modes[2]) {
              for (estimation_type in c("observed")) {
                split_var <- split_mode$outcome
                control_group <- "nevertreated"
                df_causal_forest_bins <- read_parquet(
                  file.path(INDIR_CF, dataset, rolling_panel_imp, 
                            paste0(split_var, "_repo_att_", method, ".parquet"))
                ) %>%
                  filter(type == estimation_type) 
                dr_cf <- read.csv(file.path(INDIR_CF_DS, dataset, rolling_panel_imp, 
                                   "filt_dr_scores_lm_forest_nonlinear_pull_request_merged_rolling5.csv"))
                dr_cf <- CoalesceEventTimes(dr_cf)
                colnames(dr_cf) <- paste0("dr_", colnames(dr_cf))
                dr_cf <- dr_cf  %>%
                  mutate(att_dr_5_group = ifelse(dr_event_time5>median(dr_event_time5, na.rm = T), "high", "low"),
                         att_dr_1_group = ifelse(dr_event_time1>median(dr_event_time1, na.rm = T), "high", "low"))
                df_causal_forest_bins  <- cbind(df_causal_forest_bins, dr_cf)

                
                
                # for (split_estimate in c("att_dr_group", "att_dr_5_group", "att_dr_1_group")) {
                #   split_text <- dplyr::case_when(
                #     split_estimate == "att_dr_group"   ~ "Causal Forest DR ATT",
                #     split_estimate == "att_dr_5_group" ~ "Causal Forest DR ATT (k=5)",
                #     split_estimate == "att_dr_1_group" ~ "Causal Forest DR ATT (k=1)",
                #     TRUE ~ split_estimate
                #   )
                #   practice_mode <- list(
                #     continuous_covariate = split_estimate,
                #     filters = list(list(col = split_estimate, vals = c("high",  "low"))),
                #     legend_labels = c("High",  "Low"),
                #     legend_title = paste0("by ", split_text, " for ", split_var, " on ", estimation_type, " estimates"),
                #     control_group = control_group,
                #     data = paste0("df_panel_",control_group),
                #     folder = file.path("output/analysis/event_study_personalization", 
                #                        dataset, rolling_panel_imp, control_group)
                #   )
                #   covar <- practice_mode$continuous_covariate
                #   for (outcome_mode in outcome_modes) {
                #     base_df <-  get(outcome_mode$data)
                #     base_df <- base_df %>%
                #       left_join(df_causal_forest_bins %>% select(repo_name, !!covar))
                #     
                #     for (norm in norm_options) {
                #       norm_str <- ifelse(norm, "_norm", "")
                #       
                #       combo_grid <- expand.grid(lapply(practice_mode$filters, `[[`, "vals"),
                #                                 KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
                #       
                #       es_list <- apply(combo_grid, 1, function(vals_row) {
                #         df_sub <- base_df
                #         for (i in seq_along(vals_row)) {
                #           col_name <- practice_mode$filters[[i]]$col
                #           df_sub <- df_sub %>% filter(.data[[col_name]] == vals_row[[i]])
                #         }
                #         tryCatch(EventStudy(df_sub,
                #                             outcome_mode$outcome,
                #                             practice_mode$control_group,
                #                             method = metrics,
                #                             title = "",
                #                             normalize = norm),
                #                  error = function(e) NULL)
                #       }, simplify = FALSE)
                #       
                #       success_idx <- which(!sapply(es_list, is.null))
                #       es_list <- es_list[success_idx]
                #       labels <- practice_mode$legend_labels[success_idx]
                #       
                #       if (length(es_list) > 0) {
                #         dir.create(practice_mode$folder, recursive = TRUE, showWarnings = FALSE)
                #         out_path <- file.path(practice_mode$folder,
                #                               paste0(outcome_mode$outcome, norm_str, "_split_",
                #                                      split_mode$outcome, "_", method, "_", estimation_type, "_", split_estimate, has_pc_suffix, ".png"))
                #         
                #         # write PNG (this is the canonical creation point; append to png_ordered here)
                #         png(out_path)
                #         title_suffix <- paste0("\nSample: ", num_qualified_label, ", What method: ", What_estimation_label)
                #         do.call(CompareES, list(es_list,
                #                                 legend_labels = labels,
                #                                 legend_title  = NULL,
                #                                 title = "",
                #                                 ylim = c(-4, 2)))
                #         dev.off()
                #         
                #         # record creation order immediately (so final PDF follows this chronological order)
                #         png_ordered <- c(png_ordered, out_path)
                #         # Collect coefficients
                #         for (j in seq_along(es_list)) {
                #           res <- es_list[[j]]$results
                #           split_val <- practice_mode$filters[[1]]$vals[j]
                #           coeffs_all[[length(coeffs_all) + 1]] <- as_tibble(res, rownames = "event_time") %>%
                #             mutate(
                #               dataset = dataset,
                #               rolling = rolling_panel_imp,
                #               category = outcome_mode$category,
                #               outcome = outcome_mode$outcome,
                #               normalize = norm,
                #               method = metrics[1], 
                #               covar = covar,
                #               split_value = split_val,
                #               estimation_type = estimation_type,
                #               split_estimate = split_estimate,
                #               has_pc = has_pc
                #             )
                #         }
                #       }
                #     }
                #   }
                # }
                # ------------------ REPLACEMENT: handle att_dr_group (unchanged) + combined k=5/k=1 ------------------
                split_modes <- c("att_dr_group", "k_combo")  # k_combo handles att_dr_5_group + att_dr_1_group together
                
                for (split_mode_val in split_modes) {
                  if (split_mode_val == "att_dr_group") {
                    # keep the original single-column behavior (almost identical to your original block)
                    split_estimate <- "att_dr_group"
                    split_text <- "Causal Forest DR ATT"
                    practice_mode <- list(
                      continuous_covariate = split_estimate,
                      filters = list(list(col = split_estimate, vals = c("high", "low"))),
                      legend_labels = c("High", "Low"),
                      legend_title = paste0("by ", split_text, " for ", split_var, " on ", estimation_type, " estimates"),
                      control_group = control_group,
                      data = paste0("df_panel_", control_group),
                      folder = file.path("output/analysis/event_study_personalization", dataset, rolling_panel_imp, control_group)
                    )
                    covar <- practice_mode$continuous_covariate
                    
                    for (outcome_mode in outcome_modes) {
                      base_df <- get(outcome_mode$data)
                      base_df <- base_df %>% left_join(df_causal_forest_bins %>% select(repo_name, !!covar))
                      
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
                          dir.create(practice_mode$folder, recursive = TRUE, showWarnings = FALSE)
                          out_path <- file.path(practice_mode$folder,
                                                paste0(outcome_mode$outcome, norm_str, "_split_",
                                                       split_mode$outcome, "_", method, "_", estimation_type, "_", split_estimate, has_pc_suffix, ".png"))
                          
                          png(out_path)
                          title_suffix <- paste0("\nSample: ", num_qualified_label, ", What method: ", What_estimation_label)
                          do.call(CompareES, list(es_list,
                                                  legend_labels = labels,
                                                  legend_title  = "Resilience Subset",
                                                  title = "",
                                                  ylim = c(-4, 2)))
                          dev.off()
                          
                          png_ordered <- c(png_ordered, out_path)
                          for (j in seq_along(es_list)) {
                            res <- es_list[[j]]$results
                            split_val <- practice_mode$filters[[1]]$vals[j]
                            coeffs_all[[length(coeffs_all) + 1]] <- as_tibble(res, rownames = "event_time") %>%
                              mutate(
                                dataset = dataset,
                                rolling = rolling_panel_imp,
                                category = outcome_mode$category,
                                outcome = outcome_mode$outcome,
                                normalize = norm,
                                method = metrics[1],
                                covar = covar,
                                split_value = split_val,
                                estimation_type = estimation_type,
                                split_estimate = split_estimate,
                                has_pc = has_pc
                              )
                          }
                        }
                      }
                    }
                  } else if (split_mode_val == "k_combo") {
                    # NEW: combine att_dr_5_group (early) and att_dr_1_group (late) into one plot with 4 series:
                    # High (early), Low (early), High (late), Low (late)
                    covar_early <- "att_dr_5_group"
                    covar_late  <- "att_dr_1_group"
                    split_text  <- "Causal Forest DR ATT (early vs late)"
                    practice_mode <- list(
                      filters = list(
                        list(col = covar_early, vals = c("high", "low")),
                        list(col = covar_late,  vals = c("high", "low"))
                      ),
                      control_group = control_group,
                      data = paste0("df_panel_", control_group),
                      folder = file.path("output/analysis/event_study_personalization", dataset, rolling_panel_imp, control_group)
                    )
                    
                    # labels in the order we will collect them: early-high, early-low, late-high, late-low
                    legend_labels_combo <- c("High (early)", "Low (early)", "High (late)", "Low (late)")
                    
                    for (outcome_mode in outcome_modes) {
                      base_df <- get(outcome_mode$data)
                      # ensure both covar columns are present for filtering
                      base_df <- base_df %>% left_join(df_causal_forest_bins %>% select(repo_name, !!covar_early, !!covar_late))
                      
                      for (norm in norm_options) {
                        norm_str <- ifelse(norm, "_norm", "")
                        
                        # we'll create four separate filtered dfs and run EventStudy on each
                        filter_cases <- list(
                          list(col = covar_early, val = "high", label = "High (early)"),
                          list(col = covar_late,  val = "high", label = "High (late)"),
                          list(col = covar_early, val = "low",  label = "Low (early)"),
                          list(col = covar_late,  val = "low",  label = "Low (late)")
                        )
                        
                        es_list <- lapply(filter_cases, function(fc) {
                          df_sub <- base_df %>% filter(.data[[fc$col]] == fc$val)
                          tryCatch(EventStudy(df_sub,
                                              outcome_mode$outcome,
                                              practice_mode$control_group,
                                              method = metrics,
                                              title = "",
                                              normalize = norm),
                                   error = function(e) NULL)
                        })
                        
                        success_idx <- which(!sapply(es_list, is.null))
                        if (length(success_idx) == 0) next
                        es_list <- es_list[success_idx]
                        labels <- legend_labels_combo[success_idx]
                        
                        dir.create(practice_mode$folder, recursive = TRUE, showWarnings = FALSE)
                        out_path <- file.path(practice_mode$folder,
                                              paste0(outcome_mode$outcome, norm_str, "_split_",
                                                     split_mode$outcome, "_", method, "_", estimation_type, "_att_dr_kcombo", has_pc_suffix, ".png"))
                        
                        png(out_path)
                        title_suffix <- paste0("\nSample: ", num_qualified_label, ", What method: ", What_estimation_label)
                        do.call(CompareES, list(es_list,
                                                legend_labels = labels,
                                                legend_title  = "Resilience Subset",
                                                title = "",
                                                ylim = c(-4, 2),
                                                add_comparison = F))
                        dev.off()
                        
                        png_ordered <- c(png_ordered, out_path)
                        
                        # collect coefficients: map success_idx back to filter_cases to label split_value/covar properly
                        for (idx in success_idx) {
                          res <- es_list[[which(success_idx == idx)]]$results
                          fc <- filter_cases[[idx]]
                          coeffs_all[[length(coeffs_all) + 1]] <- as_tibble(res, rownames = "event_time") %>%
                            mutate(
                              dataset = dataset,
                              rolling = rolling_panel_imp,
                              category = outcome_mode$category,
                              outcome = outcome_mode$outcome,
                              normalize = norm,
                              method = metrics[1],
                              covar = fc$col,
                              split_value = fc$val,
                              estimation_type = estimation_type,
                              split_estimate = paste0(fc$col, "_", fc$val),
                              has_pc = has_pc
                            )
                        }
                      }
                    }
                  }
                }
                # ------------------ END REPLACEMENT ------------------
                
              }
            }
          } # end has_pc loop
        } # end use_imp loop
      }
    }
    #######################################
    # Export results (per rolling period)
    #######################################
    coeffs_df <- bind_rows(coeffs_all) %>%
      mutate(event_time = as.numeric(event_time))
    write_csv(coeffs_df, file.path(OUTDIR,  paste0("personalized_event_study_coefficients.csv")))
    
    #######################################
    # Combine PNGs into one PDF (per rolling period)
    #   -> event study plots only (exclude histograms)
    # The order follows png_ordered (first-created to last-created)
    #######################################
    # filter out any hist PNGs just in case
    png_files <- png_ordered[!grepl("_hist_", png_ordered)]
    
    pdf(file.path(OUTDIR, "personalized_event_study_results.pdf"), width = 11, height = 8.5)
    if (length(png_files) > 0) {
      # Print up to first 4 on first page (if fewer than 4, pad in layout)
      first_group <- png_files[1:min(4, length(png_files))]
      imgs <- lapply(first_group, function(f) grid::rasterGrob(readPNG(f), interpolate = TRUE))
      # pad with blanks if fewer than 4 for stable layout
      if (length(imgs) < 4) {
        for (i in seq_len(4 - length(imgs))) imgs <- c(imgs, list(textGrob("")))
      }
      do.call(grid.arrange, c(imgs, ncol = 2))
      
      # remaining groups in order, 4 per page
      if (length(png_files) > 4) {
        remaining <- png_files[-(1:4)]
        for (i in seq(1, length(remaining), by = 4)) {
          group_files <- remaining[i:min(i+3, length(remaining))]
          imgs <- lapply(group_files, function(f) grid::rasterGrob(readPNG(f), interpolate = TRUE))
          # pad page if <4
          if (length(imgs) < 4) {
            for (p in seq_len(4 - length(imgs))) imgs <- c(imgs, list(textGrob("")))
          }
          do.call(grid.arrange, c(imgs, ncol = 2))
        }
      }
    }
  }
  dev.off()
} # end main

#######################################
# Run
#######################################
main()
