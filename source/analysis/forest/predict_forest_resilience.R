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
library(SaveData)        

source("source/lib/helpers.R")
source("source/analysis/event_study/helpers.R")
source("source/analysis/forest/helpers.R")
source("source/analysis/constants.R")

UnifyEventTimeColumns <- function(df) {
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
Main <- function() {
  INDIR  <- "drive/output/derived/org_outcomes_practices/org_panel"
  INDIR_CF  <- "output/analysis/causal_forest_personalization"
  INDIR_CF_DS <- "drive/output/analysis/causal_forest_personalization"
  INDIR_YAML <- "source/analysis/config"
  OUTDIR <- "output/analysis/event_study_personalization"
  dir_create(OUTDIR)

  DATASETS <- c("important_degree_top3")
  exclude_outcomes <- c("num_downloads")
  outcome_cfg      <- yaml.load_file(file.path(INDIR_YAML, "outcomes.yaml"))
  
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

        outcome_modes      <- BuildOutcomeModes(outcome_cfg, "nevertreated", outdir_ds,
                                                NORM_OPTIONS, build_dir = FALSE)
        coeffs_all <- list()
        
        #######################################
        # Org practice splits (including optional _imp and optional _pc suffix)
        #######################################
        metrics    <- c("sa")
        metrics_fn <- c("Sun and Abraham 2020")
        
        for (use_imp in c(FALSE)) {
          for (has_pc in c(TRUE, "median")) {
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
                dr_cf <- UnifyEventTimeColumns(dr_cf)
                colnames(dr_cf) <- paste0("dr_", colnames(dr_cf))
                dr_cf <- dr_cf  %>%
                  mutate(att_dr_5_group = ifelse(dr_event_time5>median(dr_event_time5, na.rm = T), "high", "low"),
                         att_dr_1_group = ifelse(dr_event_time1>median(dr_event_time1, na.rm = T), "high", "low"))
                df_causal_forest_bins  <- cbind(df_causal_forest_bins, dr_cf)
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
                      data = control_group,
                      folder = file.path("output/analysis/event_study_personalization", dataset, rolling_panel_imp, control_group)
                    )
                    covar <- practice_mode$continuous_covariate
                    
                    for (outcome_mode in outcome_modes) {
                      base_df <- panels[[outcome_mode$data]]
                      base_df <- base_df %>% left_join(df_causal_forest_bins %>% select(repo_name, !!covar))
                      
                      for (norm in NORM_OPTIONS) {
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
                          do.call(PlotEventStudyComparison, list(es_list,
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
                      data = control_group,
                      folder = file.path("output/analysis/event_study_personalization", dataset, rolling_panel_imp, control_group)
                    )
                    
                    # labels in the order we will collect them: early-high, early-low, late-high, late-low
                    legend_labels_combo <- c("High (early)", "Low (early)", "High (late)", "Low (late)")
                    
                    for (outcome_mode in outcome_modes) {
                      base_df <- panels[[outcome_mode$data]]
                      # ensure both covar columns are present for filtering
                      base_df <- base_df %>% left_join(df_causal_forest_bins %>% select(repo_name, !!covar_early, !!covar_late))
                      
                      for (norm in NORM_OPTIONS) {
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
                        do.call(PlotEventStudyComparison, list(es_list,
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
    SaveData(coeffs_df,
             c("dataset", "rolling", "category", "outcome", "normalize", "method", "covar",
               "split_value", "estimation_type", "split_estimate", "has_pc", "event_time"),
             file.path(OUTDIR, "personalized_event_study_coefficients.csv"),
             file.path(OUTDIR, "personalized_event_study_coefficients.log"),
             sortbykey = FALSE)

    png_files <- png_ordered[!grepl("_hist_", png_ordered)]
    AggregatePngsToPdf(png_files, file.path(OUTDIR, "personalized_event_study_results.pdf"))
  }
} # end Main

Main()
