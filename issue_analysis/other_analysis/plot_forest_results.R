#######################################
# 1. Libraries
#######################################
library(tidyverse)     
library(arrow)       
library(fs)       
library(gridExtra)    
library(png)       
library(grid)    
source("source/lib/helpers.R")
source("source/lib/event_study_helpers.R")
source("source/lib/forest_helpers.R")
source("source/lib/constants.R")

INDIR_PREP <- "output/analysis/data_prep"

winsorize <- function(x, probs) {
  limits <- quantile(x, probs, na.rm = TRUE)
  pmin(pmax(x, limits[1]), limits[2])
}

#######################################
# 9. Main execution
#######################################
Main <- function() {
  INDIR_FOREST <- "output/analysis/event_study_forest"
  OUTDIR <- "output/analysis/event_study_forest"
  dir_create(OUTDIR)

  ROLLING_PANELS <- ROLLING_LABELS
  METHODS <- c("event_study_forest")
  exclude_outcomes <- c("num_downloads")

  project_cfg <- LoadProjectConfig(PROJECT_CONFIG_PATH)
  outcome_cfg <- project_cfg$outcome_variables
  
  # GLOBAL collector for all histogram PNGs created during the whole run
  hist_files_global <- character(0)
  
  for (importance_type in IMPORTANCE_TYPES) {
    for (qualified_sample in QUALIFIED_SAMPLES) {
      num_qualified_label <- QualifiedSampleLabel(qualified_sample)
      for (control_group in CONTROL_GROUPS) {
        outdir_ds <- file.path(OUTDIR, importance_type, qualified_sample, control_group)
        dir_create(outdir_ds, recurse = TRUE)
        for (rolling_panel in ROLLING_PANELS) {
          rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
          for (method in METHODS) {
            message("Processing dataset: ", importance_type, " (", rolling_panel, "/", qualified_sample, "/", control_group, ") method=", method)
            
            panel <- LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel, qualified_sample, control_group)
            if (nrow(panel) == 0) {
              next
            }
            panels <- list()
            panels[[control_group]] <- panel

            outcome_modes <- BuildOutcomeModes(outcome_cfg, control_group, outdir_ds, NORM_OPTIONS,
                                               build_dir = FALSE)
        
        coeffs_all <- list()
        
        forest_outcome_modes <- Filter(function(m) m$outcome == FOREST_TRAINING_OUTCOME, outcome_modes)
        for (split_mode in forest_outcome_modes) {
          split_var <- split_mode$outcome
          practice_mode <- list(
            continuous_covariate = "att_group",
            filters = list(list(col = "att_group", vals = c("high", "low"))),
            legend_labels = c("High", "Low"),
            legend_title = paste0("by Causal Forest ATT for ", split_var),
            control_group = control_group,
            data = control_group,
            folder = file.path(outdir_ds, rolling_panel, "splits")  # put split outputs under outdir_ds
          )
          
          # read causal forest outputs (repo-level)
          df_causal_forest_bins <- read_parquet(
            file.path(INDIR_FOREST, importance_type, rolling_panel, qualified_sample, control_group, "all_covariates",
                      paste0(split_var, "_repo_att_event_study_forest.parquet")),
            stringsAsFactors = FALSE)
          
          covar <- practice_mode$continuous_covariate
          dir_create(practice_mode$folder, recurse = TRUE)
          
          for (estimation_type in c("observed")) {
            # -------------------------
            # Diagnostics: ATT + ES plots using ggsave()
            # -------------------------
            title <- paste0("Causal Forest ATT ", split_var, "\n", rolling_panel, " (", method, ") estimated on ", estimation_type,
                            "\nSample: ", num_qualified_label, "\nControl group: ", control_group)
            # 1) ATT distribution (column 'att') with both groups, legend title "Causal Forest ATT Group"
            hist_att_path <- file.path(
              practice_mode$folder, paste0("split_", split_mode$outcome, "_", method, "_", estimation_type, "_hist_att.png"))
            df_causal_forest_bins_type <- df_causal_forest_bins %>%
              mutate(att_wins = winsorize(att, c(0, 1)))
            p_att <- ggplot(df_causal_forest_bins_type, aes(x = att_wins, fill = att_group, color = att_group)) +
              geom_histogram(position = "stack", alpha = 0.5, bins = 40) +
              labs(title = title,
                   x = "ATT (winsorized)", y = "Count",
                   fill = "ATT Group", color = "ATT Group") +
              theme_minimal()
            ggsave(filename = hist_att_path, plot = p_att, width = 9, height = 7, dpi = 150, units = "in")
            message("Wrote ATT hist: ", hist_att_path)
            hist_files_global <- c(hist_files_global, hist_att_path)
            
            # 2) Event-study estimates: one facet per event time, high/low overlaid on same plot
            es_cols <- grep("^event_time", names(df_causal_forest_bins_type), value = TRUE)
            
            coef_hist_path <- file.path(practice_mode$folder,
                                        paste0("split_", split_mode$outcome, "_", method, "_", estimation_type, "_hist_es.png"))
            coef_long <- df_causal_forest_bins_type %>%
              select(repo_name, att_group, all_of(es_cols)) %>%
              pivot_longer(cols = all_of(es_cols), names_to = "term", values_to = "estimate") %>%
              filter(!is.na(estimate)) %>%
              mutate(
                term_num = as.numeric(stringr::str_extract(term, "-?\\d+$")),
                term_label = paste0("k=", term_num)
              ) %>%
              arrange(term_num) %>%
              mutate(
                term_label = factor(term_label, levels = paste0("k=", sort(unique(term_num))))
              ) %>%
              mutate(estimate_wins = winsorize(estimate, c(0, 1)))
            
            p_coef <- ggplot(coef_long, aes(x = estimate_wins, fill = att_group, color = att_group)) +
              geom_density(alpha = 0.35) +
              facet_wrap(~ term_label, scales = "free_y", ncol = 3) +
              labs(title = title,
                   x = "Estimate (winsorized)", y = "Density",
                   fill = "ATT Group", color = "ATT Group") +
              theme_minimal()
            ggsave(filename = coef_hist_path, plot = p_coef, width = 12, height = 9, dpi = 150, units = "in")
            message("Wrote ES hist: ", coef_hist_path)
            hist_files_global <- c(hist_files_global, coef_hist_path)
          }
        } 
      } 
    }  
	  }
		}
	}
	AggregatePngsToPdf(hist_files_global, file.path(OUTDIR, "all_histograms_all_runs.pdf"))
}

Main()
