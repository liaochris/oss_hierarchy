#######################################
# 0. Setup
#######################################
library(tidyverse)
library(arrow)
library(fs)
library(eventstudyr)
library(gridExtra)
library(png)
library(grid)
library(grf)
library(policytree)
library(SaveData)

source("source/lib/helpers.R")
source("source/lib/event_study_helpers.R")
source("source/lib/forest_helpers.R")
source("source/lib/constants.R")

INDIR_PREP <- "output/analysis/data_prep"

#######################################
# 1. Major helpers (unchanged signatures)
#######################################
TrainPolicyTree <- function(x_matrix, gamma_matrix, depth = 2, split_step = 1, verbose = FALSE) {
  policy_tree(x_matrix, gamma_matrix, depth = depth, split.step = split_step, verbose = verbose)
}

RunFullSamplePolicyTree <- function(x_matrix, gamma_matrix, depth = 2, split_step = 1, verbose = FALSE,
                                    outdir_base, method, split_outcome, rolling_label, estimation_type, tag) {
  tree <- TrainPolicyTree(x_matrix, gamma_matrix, depth = depth, split_step = split_step, verbose = verbose)
  dir_create(outdir_base)
  saveRDS(tree,
          file.path(outdir_base,
                    paste0(method, "_", split_outcome, "_", rolling_label, "_", estimation_type, "_", tag, ".rds")))
  tree
}

RunKFoldPredictAndAggregate <- function(df_bins,
                                        df_repo_level,
                                        x_complete,
                                        n_folds,
                                        outdir_base,
                                        method,
                                        split_outcome,
                                        rolling_label,
                                        estimation_type,
                                        depth = 2,
                                        split_step = 1,
                                        tag = "depth2",
                                        seed = 420) {
  repo_map <- df_repo_level %>% select(repo_id, repo_name, fold) %>% distinct()
  if (!"fold" %in% colnames(repo_map) || any(is.na(repo_map$fold))) {
    stop("RunKFoldPredictAndAggregate requires df_repo_level to include a non-missing 'fold' column.")
  }
  repo_names_all <- df_repo_level$repo_name
  n_repos <- length(repo_names_all)
  kfold_preds <- rep(NA_integer_, n_repos)
  
  gamma_vec <- df_bins %>% pull(att_dr)
  gamma_clean <- ifelse(is.na(gamma_vec), 0, gamma_vec)
  gamma_mat <- data.frame(control = -gamma_clean, treated = gamma_clean)
  
  unique_folds <- sort(unique(repo_map$fold))
  for (fold in unique_folds) {
    train_repo_names <- repo_map %>% filter(fold != !!fold) %>% pull(repo_name)
    held_repo_names  <- repo_map %>% filter(fold == !!fold)  %>% pull(repo_name)
    
    train_idx <- which(repo_names_all %in% train_repo_names)
    held_idx  <- which(repo_names_all %in% held_repo_names)
    
    
    x_train <- x_complete[train_idx, , drop = FALSE]
    x_held  <- x_complete[held_idx,  , drop = FALSE]
    gamma_train <- gamma_mat[train_idx, , drop = FALSE]
    
    tree <- TrainPolicyTree(x_train, gamma_train, depth = depth, split_step = split_step, verbose = FALSE)
    
    dir_create(outdir_base)
    saveRDS(tree,
            file.path(outdir_base,
                      paste0(method, "_", split_outcome, "_", rolling_label, "_",
                             estimation_type, "_fold", fold, "_", tag, ".rds")))
    
    held_preds <- as.integer(predict(tree, x_held) - 1)
    kfold_preds[held_idx] <- held_preds
  }
  
  if (any(is.na(kfold_preds))) {
    warning("Some repos did not receive a kfold-held prediction; missing count: ",
            sum(is.na(kfold_preds)))
  }
  
  df_bins <- df_bins %>%
    left_join(tibble(repo_name = repo_names_all,
                     !!sym(paste0("policy_tree_group_kfold_", tag)) := kfold_preds),
              by = "repo_name")
  df_bins
}


RunEventStudyAndCollect <- function(base_df, df_bins, practice_mode, outcome_modes, norm_options,
                                    rolling_label, method_label, estimation_type,
                                    png_collector, coeffs_collector, split_mode) {
  for (outcome_mode in outcome_modes) {
    
    df_with_split <- base_df %>%
      left_join(df_bins %>% select(repo_name, all_of(practice_mode$continuous_covariate)),
                by = "repo_name")
    
    for (norm in norm_options) {
      norm_str <- ifelse(norm, "_norm", "")
      combo_grid <- expand.grid(lapply(practice_mode$filters, `[[`, "vals"),
                                KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
      
      es_list <- apply(combo_grid, 1, function(vals_row) {
        df_sub <- df_with_split
        for (i in seq_along(vals_row)) {
          col_name <- practice_mode$filters[[i]]$col
          df_sub <- df_sub %>% filter(.data[[col_name]] == vals_row[[i]])
        }
        tryCatch(EventStudy(df_sub,
                            outcome_mode$outcome,
                            practice_mode$control_group,
                            method = c("sa"),
                            title = "",
                            normalize = norm),
                 error = function(e) NULL)
      }, simplify = FALSE)
      
      success_idx <- which(!sapply(es_list, is.null))
      es_list <- es_list[success_idx]
      labels <- practice_mode$legend_labels[success_idx]
      
      if (length(es_list) > 0) {
        dir_create(practice_mode$folder, recurse = TRUE)
        out_path <- file.path(practice_mode$folder,
                              paste0(outcome_mode$outcome, norm_str, "_", rolling_label, "_",
                                     method_label, "_", estimation_type, ".png"))
        
        png(out_path)
        title_suffix <- paste0("\nSample: ", practice_mode$legend_title)
        do.call(PlotEventStudyComparison, list(es_list,
                                legend_labels = labels,
                                legend_title  = "Resilience Subset",
                                title = "",
                                ylim = c(-3, 1.5)))
        dev.off()
        
        png_collector <- c(png_collector, out_path)
        for (j in seq_along(es_list)) {
          res <- es_list[[j]]$results
          split_val <- practice_mode$filters[[1]]$vals[j]
          coeffs_collector[[length(coeffs_collector) + 1]] <- as_tibble(res, rownames = "event_time") %>%
            mutate(dataset = NA,
                   rolling = rolling_label,
                   category = outcome_mode$category,
                   outcome = outcome_mode$outcome,
                   normalize = norm,
                   method = "sa",
                   covar = practice_mode$continuous_covariate,
                   split_value = split_val,
                   estimation_type = estimation_type)
        }
      }
    }
  }
  
  # Return the updated collectors after processing all outcome_modes
  list(pngs = png_collector, coeffs = coeffs_collector)
}

#######################################
# 2. New helper: process one tree/input combo
#######################################
ProcessTreeCombo <- function(x_for_tree,
                             df_causal_forest_bins,
                             x_repo,
                             input_choice,
                             depth_val,
                             split_step_val,
                             tag_input,
                             method,
                             split_mode,
                             rolling_panel_imp2,
                             estimation_type,
                             outcome_modes,
                             panels,
                             png_ordered,
                             coeffs_all,
                             OUTDIR_POLICYTREE_DATASTORE) {
  
  gamma_vec <- df_causal_forest_bins %>% pull(att_dr)
  gamma_clean <- ifelse(is.na(gamma_vec), 0, gamma_vec)
  
  tree <- RunFullSamplePolicyTree(x_matrix = x_for_tree,
                                  gamma_matrix = data.frame(control = -gamma_clean, treated = gamma_clean),
                                  depth = depth_val,
                                  split_step = split_step_val,
                                  verbose = TRUE,
                                  outdir_base = file.path(OUTDIR_POLICYTREE_DATASTORE,
                                                          dataset_output, rolling_panel_imp2),
                                  method = method,
                                  split_outcome = split_mode$outcome,
                                  rolling_label = paste0(rolling_panel_imp2, "_", input_choice),
                                  estimation_type = estimation_type,
                                  tag = tag_input)
  
  assignments <- as.integer(predict(tree, x_for_tree) - 1)
  df_causal_forest_bins <- df_causal_forest_bins %>%
    left_join(tibble(repo_name = df_causal_forest_bins$repo_name,
                     !!sym(paste0("policy_tree_group_", tag_input)) := assignments),
              by = "repo_name")
  
  practice_mode <- list(
    continuous_covariate = paste0("policy_tree_group_", tag_input),
    filters = list(list(col = paste0("policy_tree_group_", tag_input), vals = c(1, 0))),
    legend_labels = c("High", "Low"),
    legend_title = NULL,
    control_group = control_group,
    data = control_group,
    folder = file.path("output/analysis/event_study_forest", dataset_output,
                       rolling_panel_imp2, paste(control_group, "policy_tree", tag_input, sep = "_"))
  )
  
  tmp_full <- RunEventStudyAndCollect(base_df = panels[[practice_mode$data]],
                                      df_bins = df_causal_forest_bins,
                                      practice_mode = practice_mode,
                                      outcome_modes = outcome_modes,
                                      norm_options = NORM_OPTIONS,
                                      rolling_label = paste0(rolling_panel_imp2, "_", tag_input),
                                      method_label = paste0(method, "_", tag_input),
                                      estimation_type = estimation_type,
                                      png_collector = png_ordered,
                                      coeffs_collector = coeffs_all,
                                      split_mode = split_mode)
  png_ordered <- tmp_full$pngs
  coeffs_all <- tmp_full$coeffs
  
  df_causal_forest_bins <- RunKFoldPredictAndAggregate(
    df_bins = df_causal_forest_bins,
    df_repo_level = x_repo,
    x_complete = x_for_tree,
    n_folds = 10,
    outdir_base = file.path(OUTDIR_POLICYTREE_DATASTORE, dataset_output, rolling_panel_imp2),
    method = method,
    split_outcome = split_mode$outcome,
    rolling_label = paste0(rolling_panel_imp2, "_", input_choice),
    estimation_type = estimation_type,
    depth = depth_val,
    split_step = split_step_val,
    tag = tag_input,
    seed = SEED
  )

  practice_mode_kfold <- list(
    continuous_covariate = paste0("policy_tree_group_kfold_", tag_input),
    filters = list(list(col = paste0("policy_tree_group_kfold_", tag_input), vals = c(1, 0))),
    legend_labels = c("High", "Low"),
    legend_title = NULL,
    control_group = control_group,
    data = control_group,
    folder = file.path("output/analysis/event_study_forest", dataset_output,
                       rolling_panel_imp2, paste(control_group, "policy_tree_kfold", tag_input, sep = "_"))
  )
  
  tmp_kfold <- RunEventStudyAndCollect(
    base_df = panels[[practice_mode_kfold$data]],
    df_bins = df_causal_forest_bins,
    practice_mode = practice_mode_kfold,
    outcome_modes = outcome_modes,
    norm_options = NORM_OPTIONS,
    rolling_label = paste0(rolling_panel_imp2, "_kfold_", tag_input),
    method_label = paste0(method, "_kfold_", tag_input),
    estimation_type = estimation_type,
    png_collector = png_ordered,
    coeffs_collector = coeffs_all,
    split_mode = split_mode
  )
  png_ordered <- tmp_kfold$pngs
  coeffs_all <- tmp_kfold$coeffs
  
  list(df_bins = df_causal_forest_bins, pngs = png_ordered, coeffs = coeffs_all)
}

#######################################
# 3. Main execution (cleaned)
#######################################

INDIR  <- "drive/output/derived/org_outcomes_practices/org_panel"
INDIR_CF  <- "output/analysis/event_study_forest"
OUTDIR <- "output/analysis/event_study_forest"
OUTDIR_DATASTORE <- "drive/output/analysis/event_study_forest"
OUTDIR_POLICYTREE_DATASTORE <- "drive/output/analysis/policy_forest_personalization"
dir_create(OUTDIR)

ESTIMATION_TYPES <- c("observed")
exclude_outcomes <- c("num_downloads")

project_cfg <- LoadProjectConfig(PROJECT_CONFIG_PATH)
outcome_cfg <- project_cfg$outcome_variables
org_practice_cfg <- project_cfg$feature_variables

png_ordered <- character(0)
coeffs_all <- list()

tree_configs <- list(
  list(depth = 1, split_step = 1, tag = "depth1"),
  list(depth = 2, split_step = 1, tag = "depth2"),
  list(depth = 3, split_step = 10, tag = "depth3_split10")
)

for (importance_type in IMPORTANCE_TYPES) {
  for (qualified_sample in QUALIFIED_SAMPLES) {
    for (control_group in CONTROL_GROUPS) {
      for (use_imputed in c(FALSE)) {
        for (rolling_panel in ROLLING_LABELS) {
          rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
          rolling_panel_imp <- ifelse(use_imputed, paste0(rolling_panel, "_imp"), rolling_panel)
          
          for (method in c("lm_forest")) {
            message("Processing dataset: ", importance_type, " (", rolling_panel, "/", qualified_sample, "/", control_group, ")")
            panel <- LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel, qualified_sample, control_group)
            if (nrow(panel) == 0) {
              next
            }
            dataset_output <- file.path(importance_type, rolling_panel, qualified_sample, control_group)
            outdir_ds <- file.path(OUTDIR, dataset_output)
            dir_create(outdir_ds, recurse = TRUE)
            
            panels <- list()
            panels[[control_group]] <- panel

            outcome_modes <- BuildOutcomeModes(outcome_cfg, control_group, outdir_ds, NORM_OPTIONS, build_dir = FALSE)
            org_practice_modes <- BuildOrgPracticeModes(org_practice_cfg, control_group, outdir_ds, build_dir = FALSE)
        org_practice_modes <- org_practice_modes[
          unlist(lapply(org_practice_modes, function(x) x$continuous_covariate != "prop_tests_passed"))]
        
        forest_outcome_modes <- Filter(function(m) m$outcome == FOREST_TRAINING_OUTCOME, outcome_modes)
        for (split_mode in forest_outcome_modes) {
          split_var <- split_mode$outcome
          
          covars <- unlist(lapply(org_practice_modes, \(x) x$continuous_covariate))
          covars_imp <- paste0(covars, "_imp")
          feature_cols <- if (use_imputed) paste0(c(covars, covars_imp), "_mean") else paste0(covars, "_mean")
          
          df_data <- CreateDataPanel(panel, method, split_var,
                                     covars, rolling_period, N_FOLDS, SEED)
          
          candidate_covars <- if (use_imputed) paste0(c(covars, covars_imp), "_mean") else paste0(covars, "_mean")
          candidate_covars <- intersect(colnames(df_data), candidate_covars)
          
          keep_cols <- candidate_covars[colSums(is.na(df_data %>% select(all_of(candidate_covars)))) < 200]
          if (length(keep_cols) == 0) stop("No covariate columns remain after NA filtering; aborting")
          
          n_before <- nrow(df_data)
          df_data <- df_data[complete.cases(df_data %>% select(all_of(keep_cols))), ]
          n_after <- nrow(df_data)
          message("  df_data rows: before=", n_before, " after=", n_after, " (dropped=", n_before - n_after, ")")
          
          x_repo <- df_data %>%
            distinct(repo_id, repo_name, .keep_all = TRUE) %>%
            select(all_of(keep_cols), repo_id, repo_name, fold)
          
          x_num <- x_repo %>% select(all_of(keep_cols)) %>%
            mutate(across(where(is.logical), ~as.integer(.)),
                   across(where(is.factor), ~as.numeric(as.character(.)),
                          across(where(is.character), ~as.numeric(.))))
          x_num[is.na(x_num)] <- -1
          
          pc_groups_cfg <- PCGroupsConfig()
          
          principal_component_cols <- c()
          for (pc_group_name in names(pc_groups_cfg)) {
            vars <- pc_groups_cfg[[pc_group_name]]$vars
            group_x <- x_repo %>% select(all_of(vars))
            pc_res <- prcomp(group_x, center = TRUE, scale. = TRUE)
            pc1_scores <- pc_res$x[, 1]
            principal_component_col <- paste0(pc_group_name, "_principal_component1")
            if (isTRUE(pc_groups_cfg[[pc_group_name]]$sign_flip)) { pc1_scores <- -pc1_scores }
            x_repo[[principal_component_col]] <- pc1_scores
            principal_component_cols <- c(principal_component_cols, principal_component_col)
            loadings_vec <- pc_res$rotation[vars, 1]
            loadings_str <- paste(paste0(vars, sprintf("%+.3f", loadings_vec)), collapse = ", ")
            message("PC1 ", pc_group_name, " loadings: ", substr(loadings_str, 1, 200))
          }
          x_policy_num <- x_repo %>% select(all_of(principal_component_cols)) %>% mutate(across(everything(), ~as.numeric(.)))
          x_policy_mat <- as.matrix(x_policy_num)
          x_default_mat <- as.matrix(x_num)
          
          df_repo_data <- df_data %>% select(repo_id, repo_name, quasi_treatment_group, treatment_group, fold) %>% distinct()
          
          for (estimation_type in ESTIMATION_TYPES) {
            for (with_PCA in list(TRUE,  "median")) {

              covar_type_dir <- ifelse(identical(with_PCA, "median"),
                                       "pc1_binary",
                                       ifelse(isTRUE(with_PCA), "pc1", "all_covariates"))

              rolling_panel_imp2 <- file.path(rolling_panel_imp, covar_type_dir)
              
              df_causal_forest_bins <- read_parquet(
                file.path(INDIR_CF, dataset_output, rolling_panel_imp2,
                          paste0(split_var, "_repo_att_", method, ".parquet")))
              
              for (tc in tree_configs) {
                depth_val <- tc$depth
                split_step_val <- tc$split_step
                tag_val <- tc$tag
                
                x_for_tree <- if (covar_type_dir == "pc1") {
                  x_policy_mat
                } else if (covar_type_dir == "pc1_binary") {
                  apply(x_policy_mat, 2, \(v) as.integer(v > median(v, na.rm = TRUE)))
                } else {
                  x_default_mat
                }
                print(dim(x_for_tree))
                
                tag_input <- paste0(tag_val, "_", covar_type_dir)


                x_for_tree <- x_for_tree[x_repo$repo_id %in% df_causal_forest_bins$repo_id,]
                tmp_res <- ProcessTreeCombo(x_for_tree = x_for_tree,
                                            df_causal_forest_bins = df_causal_forest_bins,
                                            x_repo = x_repo[x_repo$repo_id %in% df_causal_forest_bins$repo_id,],
                                            input_choice = covar_type_dir,
                                            depth_val = depth_val,
                                            split_step_val = split_step_val,
                                            tag_input = tag_input,
                                            method = method,
                                            split_mode = split_mode,
                                            rolling_panel_imp2 = rolling_panel_imp2,
                                            estimation_type = estimation_type,
                                            outcome_modes = outcome_modes,
                                            panels = panels,
                                            png_ordered = png_ordered,
                                            coeffs_all = coeffs_all,
                                            OUTDIR_POLICYTREE_DATASTORE = OUTDIR_POLICYTREE_DATASTORE)
                
                df_causal_forest_bins <- tmp_res$df_bins
                png_ordered <- tmp_res$pngs
                coeffs_all <- tmp_res$coeffs
              }
            }
          }
        }
      }
    }
  }
}
}
}

coeffs_df <- bind_rows(coeffs_all) %>% mutate(event_time = as.numeric(event_time))
SaveData(coeffs_df,
         c("dataset", "rolling", "category", "outcome", "normalize", "method",
           "covar", "split_value", "estimation_type", "event_time"),
         file.path(OUTDIR, "policy_tree_all_coefficients.csv"),
         file.path(OUTDIR, "policy_tree_all_coefficients.log"),
         sortbykey = FALSE)

png_files <- png_ordered[!grepl("_hist_", png_ordered)]
AggregatePngsToPdf(png_files, file.path(OUTDIR, "policy_tree_results.pdf"))
