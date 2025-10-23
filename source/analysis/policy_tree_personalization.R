#######################################
# 1. Libraries
#######################################
library(tidyverse)
library(arrow)
library(yaml)
library(fs)
library(eventstudyr)
library(gridExtra)
library(png)
library(grid)
library(grf)
library(policytree)

source("source/lib/helpers.R")
source("source/analysis/event_study_helpers.R")
source("source/analysis/causal_forest_helpers.R")

# https://github.com/grf-labs/grf/blob/master/r-package/grf/R/get_scores.R#L238

#######################################
# 9. Main execution
#######################################
main <- function() {
  SEED <- 420
  set.seed(SEED)
  n_folds <- 10
  
  INDIR  <- "drive/output/derived/org_characteristics/org_panel"
  INDIR_CF  <- "output/analysis/causal_forest_personalization"
  INDIR_YAML <- "source/derived/org_characteristics"
  OUTDIR <- "output/analysis/event_study_personalization"
  OUTDIR_DATASTORE <- "drive/output/analysis/causal_forest_personalization"
  OUTDIR_POLICYTREE_DATASTORE <- "drive/output/analysis/event_study_personalization"
  dir_create(OUTDIR)
  
  # "important_topk_defaultWhat", "important_topk_exact1_defaultWhat","important_topk_oneQual_defaultWhat",
  # "important_topk_nuclearWhat", "important_topk_exact1_nuclearWhat","important_topk_oneQual_nuclearWhat"
  # "important_topk", ,"important_topk_oneQual", "important_thresh","important_thresh_oneQual"
  
  DATASETS <-  c( "important_topk_exact1", "important_thresh_exact1")
  ESTIMATION_TYPES <- c("all", "observed")
  SPLIT_CRITERION <- c("dr_scores")
  exclude_outcomes <- c("num_downloads")
  norm_options <- c(TRUE)
  outcome_cfg      <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))
  org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariate_organization.yaml"))
  
  png_ordered <- character(0)  
  for (dataset in DATASETS) {
    for (rolling_panel in c("rolling5")) {
      rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
      for (method in c("lm_forest", "lm_forest_nonlinear")) {  
        outdir_cf_ds <- file.path(OUTDIR_DATASTORE, dataset, rolling_panel)
        outdir_policy_tree_ds <- file.path(OUTDIR_POLICYTREE_DATASTORE, dataset, rolling_panel)
        
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
        df_panel_common <- BuildCommonSample(df_panel, all_outcomes) %>%
          filter(num_departures <= 1) %>%
          mutate(quasi_event_time = time_index - quasi_treatment_group)        
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
        org_practice_modes <- BuildOrgPracticeModes(org_practice_cfg, "nevertreated", outdir_dataset, 
                                                    build_dir = FALSE)
        org_practice_modes <- org_practice_modes[
          unlist(lapply(org_practice_modes, function(x) x$continuous_covariate != "prop_tests_passed"))]
        coeffs_all <- list()
        
        #######################################
        # Org practice splits
        #######################################
        metrics    <- c("sa")
        metrics_fn <- c("Sun and Abraham 2020")
        
        for (split_mode in list(outcome_modes[[2]], outcome_modes[[3]])) {
          split_var <- split_mode$outcome
          control_group <- "nevertreated"
          
          model <- readRDS(file.path(outdir_cf_ds, paste0(method, "_", split_var, "_rolling", rolling_period, ".rds")))
          covars     <- unlist(lapply(org_practice_modes, \(x) x$continuous_covariate))
          keep_names <- paste0(covars, "_mean")
          
          df_data <- CreateDataPanel(df_panel_common, method, split_var, covars, rolling_period, n_folds, SEED)
          x <- df_data %>% select(all_of(c(keep_names, "repo_name"))) %>% 
            unique() %>% select(-repo_name)
          df_repo_data <- df_data %>% select(repo_id, repo_name, quasi_treatment_group, treatment_group) %>% unique()
          
          
          for (estimation_type in ESTIMATION_TYPES) {
            df_causal_forest_bins <- read_parquet(
              file.path(INDIR_CF, dataset, rolling_panel, 
                        paste0(split_var, "_repo_att_", method, ".parquet"))) %>%
              filter(type == estimation_type)
            
            
            x_num <- x %>%
              mutate(across(where(is.logical), ~as.integer(.)),
                     across(where(is.factor),  ~as.numeric(as.character(.)),
                            across(where(is.character), ~as.numeric(.)))) # optional; remove if you have text columns
            x_num[is.na(x_num)] <- -1
            x_complete <- as.matrix(x_num)

            score_criterion <- as.matrix(df_causal_forest_bins %>% pull(att_dr))
            score_criterion_clean <- ifelse(is.na(score_criterion), 0, score_criterion)
            gamma <- data.frame(control = 0, treated = score_criterion_clean)
            #min_size <- as.integer(dim(gamma)[1]*0.05) , min.node.size = min_size
            tree <- policy_tree(x_complete, gamma, depth = 3, split.step = 50, verbose = TRUE)
            plot(tree, leaf.labels = c("dont treat", "treat"))
            treatment_assignment <- predict(tree, x_complete) - 1
            df_causal_forest_bins$policy_tree_group <- treatment_assignment
            
            
            dir_create(outdir_policy_tree_ds)
            out_path <- file.path(
              outdir_policy_tree_ds,
              paste0(method, "_", split_mode$outcome, "_", rolling_panel, "_", 
                     estimation_type, ".rds"))
            saveRDS(tree, out_path)
            
            split_text <- "Policy Tree Split"
            practice_mode <- list(
              continuous_covariate = "policy_tree_group",
              filters = list(list(col = "policy_tree_group", vals = c(1, 0))),
              legend_labels = c("High", "Low"),
              legend_title = paste0("by ", split_text, " for ", split_var, " on ", estimation_type, " estimates"),
              control_group = control_group,
              data = paste0("df_panel_",control_group),
              folder = file.path("output/analysis/event_study_personalization", 
                                 dataset, rolling_panel, paste(control_group, "policy_tree", sep = "_"))
            )
            for (outcome_mode in outcome_modes) {
              base_df <-  get(outcome_mode$data)
              base_df <- base_df %>%
                left_join(df_causal_forest_bins %>% 
                            filter(type == estimation_type) %>%
                            select(repo_name, practice_mode$continuous_covariate))
              
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
                  out_path <- file.path(
                    practice_mode$folder,
                    paste0(outcome_mode$outcome, norm_str, "_split_", split_mode$outcome, "_", method, "_", 
                           estimation_type, ".png"))
                  
                  # write PNG (this is the canonical creation point; append to png_ordered here)
                  png(out_path)
                  title_suffix <- paste0("\nSample: ", num_qualified_label, ", What method: ", What_estimation_label)
                  do.call(CompareES, list(es_list,
                                          legend_labels = labels,
                                          legend_title  = practice_mode$legend_title,
                                          title = paste(outcome_mode$outcome, rolling_panel, title_suffix)))
                  dev.off()
                  
                  # Collect coefficients
                  png_ordered <- c(png_ordered, out_path)
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
                        covar = practice_mode$continuous_covariate,
                        split_value = split_val,
                        estimation_type = estimation_type,
                      )
                  }
                }
              }
            }
          }
        }
      }
    }
  } # end for dataset in DATASETS
  
  #######################################
  # Export results (per rolling period)
  #######################################
  coeffs_df <- bind_rows(coeffs_all) %>%
    mutate(event_time = as.numeric(event_time))
  write_csv(coeffs_df, file.path(OUTDIR,  paste0("policy_tree_all_coefficients.csv")))
  
  #######################################
  # Combine PNGs into one PDF (per rolling period)
  #   -> event study plots only (exclude histograms)
  # The order follows png_ordered (first-created to last-created)
  #######################################
  # filter out any hist PNGs just in case
  png_files <- png_ordered[!grepl("_hist_", png_ordered)]
  
  pdf(file.path(OUTDIR, paste0("policy_tree_results.pdf")), width = 11, height = 8.5)
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
  dev.off()
} # end main

#######################################
# Run
#######################################
main()
