#######################################
# 1. Libraries
#######################################
library(tidyverse)     
library(arrow)       
library(yaml)   
library(fs)       
library(gridExtra)    
library(png)       
library(grid)    
source("source/lib/helpers.R")
source("source/analysis/event_study/helpers.R")
source("source/analysis/forest/helpers.R")
source("source/analysis/constants.R")

winsorize <- function(x, probs) {
  limits <- quantile(x, probs, na.rm = TRUE)
  pmin(pmax(x, limits[1]), limits[2])
}

#######################################
# 9. Main execution
#######################################
Main <- function() {
  INDIR_CF  <- "output/analysis/causal_forest_personalization"
  INDIR_YAML <- "source/analysis/config"
  OUTDIR <- "output/analysis/event_study_personalization"
  dir_create(OUTDIR)

  DATASETS <- c("important_topk_exact1")
  ROLLING_PANELS <- c("rolling5")
  METHODS <- c("lm_forest", "lm_forest_nonlinear")
  exclude_outcomes <- c("num_downloads")

  outcome_cfg <- yaml.load_file(file.path(INDIR_YAML, "outcomes.yaml"))
  
  # GLOBAL collector for all histogram PNGs created during the whole run
  hist_files_global <- character(0)
  
  for (dataset in DATASETS) {
    outdir_ds <- file.path(OUTDIR, dataset)
    dir_create(outdir_ds, recurse = TRUE)
    for (rolling_panel in ROLLING_PANELS) {
      rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
      for (method in METHODS) {
        # ensure outdir_ds exists early so aggregators can find files
        message("Processing dataset: ", dataset, " (", rolling_panel, ") method=", method)
        
        panel_dataset <- NormalizeDatasetName(dataset)
        dataset_labels <- MakeDatasetLabels(dataset)
        num_qualified_label   <- dataset_labels$num_qualified
        What_estimation_label <- dataset_labels$what_estimation
        
        df_panel <- read_parquet(file.path("drive/output/derived/org_outcomes_practices/org_panel", 
                                           panel_dataset, paste0("panel_", rolling_panel, ".parquet")))
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

        outcome_modes <- BuildOutcomeModes(outcome_cfg, "nevertreated", outdir_ds, NORM_OPTIONS,
                                           build_dir = FALSE)
        
        coeffs_all <- list()
        
        # Use the second outcome_mode as split_mode (as you were doing)
        for (split_mode in list(outcome_modes[[2]])) {
          split_var <- split_mode$outcome
          control_group <- "nevertreated"
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
            file.path(INDIR_CF, dataset, rolling_panel, 
                      paste0(split_var, "_repo_att_", method, ".parquet")),
            stringsAsFactors = FALSE)
          
          covar <- practice_mode$continuous_covariate
          dir_create(practice_mode$folder, recurse = TRUE)
          
          for (estimation_type in c("observed")) {
            # -------------------------
            # Diagnostics: ATT + ES plots using ggsave()
            # -------------------------
            title <- paste0("Causal Forest ATT ", split_var, "\n", rolling_panel, " (", method, ") estimated on ", estimation_type,
                            "\nSample: ", num_qualified_label, "\nWhat method: ", What_estimation_label)
            # 1) ATT distribution (column 'att') with both groups, legend title "Causal Forest ATT Group"
            hist_att_path <- file.path(
              practice_mode$folder, paste0("split_", split_mode$outcome, "_", method, "_", estimation_type, "_hist_att.png"))
            df_causal_forest_bins_type <- df_causal_forest_bins %>%
              filter(type == estimation_type) %>%
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
  AggregatePngsToPdf(hist_files_global, file.path(OUTDIR, "all_histograms_all_runs.pdf"))
}

Main()