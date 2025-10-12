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
library(grf)
library(yaml)
library(fs)
library(png)
library(grid)
library(policytree)

source("source/lib/helpers.R")
source("source/analysis/event_study_helpers.R")
source("source/analysis/causal_forest_helpers.R")

# https://github.com/grf-labs/grf/blob/master/r-package/grf/R/get_scores.R#L238
CalculateDoublyRobustScore <- function(model) {
  x <- model$X.orig
  y_hat <- model$Y.hat
  y <- model$Y.orig
  W <- model$W.orig
  W_hat <- model$W.hat
  
  preds <- predict(model, newdata = x)$predictions[, , "Y.1"]
  
  treated_rows <- which(rowSums(W) == 1)
  cols <- max.col(W[treated_rows, , drop = FALSE])
  treated_rows_cols <- cbind(treated_rows, cols)
  
  y_hat_baseline <- y_hat - rowSums(W_hat * preds)
  mu_hat <- y_hat_baseline
  mu_hat[treated_rows] <- y_hat_baseline[treated_rows] + preds[treated_rows_cols]
  
  y_residual <- y - mu_hat
  
  IPW <- matrix(0, dim(W)[1], dim(W)[2])
  prob_no_treat <- (1-rowSums(W_hat))
  IPW[-treated_rows,] <- -1 * 1/prob_no_treat[-treated_rows] 
  IPW[treated_rows_cols] <- 1/W_hat[treated_rows_cols]
  
  return(preds + IPW * as.vector(y_residual))
}

ModifyDoublyRobustTime <- function(dr_scores, df_data) {
  colnames(dr_scores) <- gsub("treatment_arm", "", colnames(dr_scores))
  arm_info <- data.frame(
    arm    = colnames(dr_scores),
    cohort = as.integer(sub("cohort(\\d+).*", "\\1", colnames(dr_scores))),
    e      = ifelse(grepl("event_time[-\\.]", colnames(dr_scores)),
                    -as.integer(sub(".*event_time[-\\.](\\d+)", "\\1", colnames(dr_scores))),
                    as.integer(sub(".*event_time(\\d+)", "\\1", colnames(dr_scores))))
  )
  arm_info$expected_time <- arm_info$cohort + arm_info$e
  match_mask <- outer(df_data$time_index, arm_info$expected_time, "==")
  dr_scores[!match_mask] <- NA
  
  repo <- df_data$repo_id
  groups <- split(seq_along(repo), repo)
  dr_scores_repo <- do.call(rbind, lapply(groups, function(ii) {
    apply(dr_scores[ii, , drop = FALSE], 2, function(x) {
      v <- x[!is.na(x)]
      if (length(v) > 0) v[1] else NA_real_
    })
  }))
  dr_scores_repo
}

CalculateDoublyRobust <- function(model, df_data) {
  dr_scores <- CalculateDoublyRobustScore(model)
  dr_scores_repo <- ModifyDoublyRobustTime(dr_scores, df_data)
  return(dr_scores_repo)
}

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
  OUTDIR_POLICYTREE_DATASTORE <- "drive/output/analysis/policy_tree_personalization"
  dir_create(OUTDIR)
  
  DATASETS <- c("important_topk_defaultWhat", "important_topk_nuclearWhat", "important_topk", 
                "important_topk_oneQual_defaultWhat")
  exclude_outcomes <- c("num_downloads")
  norm_options <- c(TRUE)
  outcome_cfg      <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))
  org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariate_organization.yaml"))
  
  png_ordered <- character(0)  
  for (dataset in DATASETS) {
    for (rolling_panel in c("rolling5")) {
      rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
      for (method in c("lm_forest")) {  # , "lm_forest_nonlinear"
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
        
        for (split_mode in list(outcome_modes[[2]])) {
          split_var <- split_mode$outcome
          control_group <- "nevertreated"
          
          model <- readRDS(file.path(outdir_cf_ds, paste0(method, "_", split_var, "_rolling", rolling_period, ".rds")))
          covars     <- unlist(lapply(org_practice_modes, \(x) x$continuous_covariate))
          keep_names <- paste0(covars, "_mean")

          df_data <- CreateDataPanel(df_panel_common, method, split_var, covars, rolling_period, n_folds, SEED)
          x <- df_data %>% select(all_of(c(keep_names, "repo_name"))) %>% 
            unique() %>% select(-repo_name)
          df_repo_data <- df_data %>% select(repo_id, repo_name, quasi_treatment_group, treatment_group) %>% unique()

          dr_scores_repo <- CalculateDoublyRobust(model, df_data)
          dr_scores_repo_filt <- FilterPredictions(dr_scores_repo, df_data, df_repo_data)

          max_time_val <- max(df_data$time_index, na.rm = TRUE)
          att_repo <- rbind(
            AggregateRepoATT(dr_scores_repo, df_repo_data, max_time_val) %>% mutate(type = "all"),
            AggregateRepoATT(dr_scores_repo_filt, df_repo_data, max_time_val) %>% mutate(type = "observed")
          )
          att_repo <- rbind(att_repo %>% mutate(policy_tree = "dr_score"), 
                            att_repo %>% mutate(policy_tree = "att_split"))
          for (estimation_type in c("all", "observed")) {
            for (policy_tree_criterion in c("dr_score", "att_split")) {
              df_causal_forest_bins <- read_parquet(
                file.path(INDIR_CF, dataset, rolling_panel, paste0(split_var, "_repo_att_", method, ".parquet"))) %>%
                filter(type == estimation_type)
              x_complete <- as.matrix(replace(x, is.na(x), 0))
              if (policy_tree_criterion == "dr_score") {
                score_criterion <- as.matrix(att_repo %>% 
                                        filter(type == estimation_type & policy_tree == policy_tree_criterion) %>% 
                                        pull(att))
              } else {
                repo_att_bins <- df_causal_forest_bins %>% select(repo_id, att_group) %>%
                  rename(att_group_median = att_group)
                score_criterion <- as.matrix(att_repo %>% 
                                        filter(type == estimation_type & policy_tree == policy_tree_criterion) %>% 
                                        left_join(repo_att_bins) %>%
                                        mutate(att_above_median = ifelse(att_group_median == "high", 1, 0)) %>%
                                        pull(att_above_median))
              }
              gamma <- data.frame(
                control = 0, 
                treated = score_criterion)
              tree <- policy_tree(x_complete, gamma, depth = 2, split.step = 1, min.node.size = 1, verbose = TRUE)
              plot(tree, leaf.labels = c("dont treat", "treat"))
              treatment_assignment <-  predict(tree, x_complete) - 1
              
              group_filter <- att_repo$type == estimation_type & att_repo$policy_tree == policy_tree_criterion
              att_repo[group_filter,"att_group"] <- ifelse(treatment_assignment==1, "high", "low")
              
              practice_mode <- list(
                continuous_covariate = "att_group",
                filters = list(list(col = "att_group", vals = c("high", "low"))),
                legend_labels = c("High", "Low"),
                legend_title = paste0("by Causal Forest ATT for ", split_var, " on ", estimation_type, " estimates"),
                control_group = control_group,
                data = paste0("df_panel_",control_group),
                folder = file.path("output/analysis/policy_tree_personalization", 
                                   dataset, rolling_panel, paste0(control_group,"_policy_tree"))
              )
              dir_create(outdir_policy_tree_ds)
              out_path <- file.path(
                outdir_policy_tree_ds,
                paste0(method, "_", split_mode$outcome, "_", rolling_panel, "_", 
                       estimation_type, "_", policy_tree_criterion,".rds"))
              saveRDS(tree, out_path)
  
              for (outcome_mode in outcome_modes) {
                base_df <-  get(outcome_mode$data)
                base_df <- base_df %>%
                  left_join(att_repo %>% 
                              filter(type == estimation_type & policy_tree == policy_tree_criterion) %>%
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
                             estimation_type, "_", policy_tree_criterion,".png"))
                    
                    # write PNG (this is the canonical creation point; append to png_ordered here)
                    png(out_path)
                    title_suffix <- paste0("\nSample: ", num_qualified_label, ", What method: ", What_estimation_label)
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
                          method = metrics[1],  
                          covar = practice_mode$continuous_covariate,
                          split_value = split_val,
                          estimation_type = estimation_type,
                          policy_tree_criterion = policy_tree_criterion
                        )
                    }
                  }
                }
              }
            }
          }
          dir_create(outdir)
          cohort_outfile <- file.path(outdir, rolling_panel,
                                      paste0(split_var, "_policy_tree_groups.csv"))
          write_parquet(att_repo, cohort_outfile)
        }
      }
    }
    #######################################
    # Export results (per rolling period)
    #######################################
    coeffs_df <- bind_rows(coeffs_all) %>%
      mutate(event_time = as.numeric(event_time))
    write_csv(coeffs_df, file.path(outdir,  paste0("policy_treeall_coefficients.csv")))

    #######################################
    # Combine PNGs into one PDF (per rolling period)
    #   -> event study plots only (exclude histograms)
    # The order follows png_ordered (first-created to last-created)
    #######################################
    # filter out any hist PNGs just in case
    png_files <- png_ordered[!grepl("_hist_", png_ordered)]

    pdf(file.path(outdir_dataset, paste0("all_results.pdf")), width = 11, height = 8.5)
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
  } # end for dataset in DATASETS
} # end main

#######################################
# Run
#######################################
main()
