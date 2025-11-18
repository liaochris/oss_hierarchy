#######################################
# 0. Setup
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

#######################################
# 1. Major helpers (unchanged signatures)
#######################################
TrainPolicyTree <- function(x_matrix, gamma_matrix, depth = 2, split_step = 1, verbose = FALSE) {
  policy_tree(x_matrix, gamma_matrix, depth = depth, split.step = split_step, verbose = verbose, min.node.size=30)
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
  gamma_mat <- data.frame(control = 0, treated = gamma_clean)
  
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
  outcome_mode <- split_mode
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
      do.call(CompareES, list(es_list,
                              legend_labels = labels,
                              legend_title  = NULL,
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
                             norm_options,
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
                                                          dataset, rolling_panel_imp2),
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
    control_group = "nevertreated",
    data = "df_panel_nevertreated",
    folder = file.path("output/analysis/event_study_personalization", dataset,
                       rolling_panel_imp2, paste("nevertreated", "policy_tree", tag_input, sep = "_"))
  )
  
  tmp_full <- RunEventStudyAndCollect(base_df = get(practice_mode$data),
                                      df_bins = df_causal_forest_bins,
                                      practice_mode = practice_mode,
                                      outcome_modes = outcome_modes,
                                      norm_options = norm_options,
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
    outdir_base = file.path(OUTDIR_POLICYTREE_DATASTORE, dataset, rolling_panel_imp2),
    method = method,
    split_outcome = split_mode$outcome,
    rolling_label = paste0(rolling_panel_imp2, "_", input_choice),
    estimation_type = estimation_type,
    depth = depth_val,
    split_step = split_step_val,
    tag = tag_input,
    seed = 420
  )
  
  practice_mode_kfold <- list(
    continuous_covariate = paste0("policy_tree_group_kfold_", tag_input),
    filters = list(list(col = paste0("policy_tree_group_kfold_", tag_input), vals = c(1, 0))),
    legend_labels = c("High", "Low"),
    legend_title = NULL,
    control_group = "nevertreated",
    data = "df_panel_nevertreated",
    folder = file.path("output/analysis/event_study_personalization", dataset,
                       rolling_panel_imp2, paste("nevertreated", "policy_tree_kfold", tag_input, sep = "_"))
  )
  
  tmp_kfold <- RunEventStudyAndCollect(
    base_df = get(practice_mode_kfold$data),
    df_bins = df_causal_forest_bins,
    practice_mode = practice_mode_kfold,
    outcome_modes = outcome_modes,
    norm_options = norm_options,
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

SEED <- 420
set.seed(SEED)
n_folds <- 10

INDIR  <- "drive/output/derived/org_characteristics/org_panel"
INDIR_CF  <- "output/analysis/causal_forest_personalization"
INDIR_YAML <- "source/derived/org_characteristics"
OUTDIR <- "output/analysis/event_study_personalization"
OUTDIR_DATASTORE <- "drive/output/analysis/causal_forest_personalization"
OUTDIR_POLICYTREE_DATASTORE <- "drive/output/analysis/policy_forest_personalization"
dir_create(OUTDIR)

DATASETS <- c("important_topk_exact1")
ESTIMATION_TYPES <- c("observed")
exclude_outcomes <- c("num_downloads")
norm_options <- c(TRUE)
outcome_cfg <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))
org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariate_organization.yaml"))

png_ordered <- character(0)
coeffs_all <- list()

tree_configs <- list(
  list(depth = 1, split_step = 1, tag = "depth1"),
  list(depth = 2, split_step = 1, tag = "depth2"),
  list(depth = 3, split_step = 50, tag = "depth3_split50")
)

for (dataset in DATASETS) {
  for (use_imp in c(FALSE)) {
    for (rolling_panel in c("rolling5")) {
      rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
      rolling_panel_imp <- ifelse(use_imp, paste0(rolling_panel, "_imp"), rolling_panel)
      
      for (method in c("lm_forest_nonlinear")) {
        message("Processing dataset: ", dataset, " (", rolling_panel, ")")
        outdir_dataset <- file.path(OUTDIR, dataset)
        dir_create(outdir_dataset, recurse = TRUE)
        
        panel_dataset <- dataset %>%
          gsub("_exact1", "", .) %>% gsub("_nuclearWhat", "", .) %>% gsub("_defaultWhat", "", .) %>%
          gsub("_oneQual", "", .)
        
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
        
        outcome_modes <- BuildOutcomeModes(outcome_cfg, "nevertreated", outdir_dataset, norm_options, build_dir = FALSE)
        org_practice_modes <- BuildOrgPracticeModes(org_practice_cfg, "nevertreated", outdir_dataset, build_dir = FALSE)
        org_practice_modes <- org_practice_modes[
          unlist(lapply(org_practice_modes, function(x) x$continuous_covariate != "prop_tests_passed"))]
        
        for (split_mode in outcome_modes) {
          split_var <- split_mode$outcome
          control_group <- "nevertreated"
          
          covars <- unlist(lapply(org_practice_modes, \(x) x$continuous_covariate))
          covars_imp <- paste0(covars, "_imp")
          keep_names <- if (use_imp) paste0(c(covars, covars_imp), "_mean") else paste0(covars, "_mean")
          
          df_data <- CreateDataPanel(df_panel_common, method, split_var,
                                     covars, rolling_period, n_folds, SEED)
          
          candidate_covars <- if (use_imp) paste0(c(covars, covars_imp), "_mean") else paste0(covars, "_mean")
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
          
          pc_groups <- list(
            collaboration = c("avg_members_per_problem_mean", "pct_members_multiple_mean",
                              "proj_hhi_discussion_comment_mean", "proj_prob_hhi_issue_comment_mean",
                              "proj_prob_hhi_pull_request_comment_mean"),
            shared_knowledge = c("share_issue_and_pr_mean", "avg_unique_types_mean"),
            discussion_vibes = c("response_rate_mean", "mean_days_to_respond_mean", "ov_sentiment_avg_mean",
                                 "pos_sentiment_avg_mean", "neg_sentiment_avg_mean"),
            welcoming = c("has_good_first_issue_mean", "has_contributing_guide_mean", "has_code_of_conduct_mean"),
            routines = c("mean_has_reviewer_mean", "mean_has_tag_mean", "mean_has_assignee_mean",
                         "has_codeowners_mean", "has_issue_template_mean", "has_pr_template_mean")
          )
          
          pc_cols <- c()
          for (group_name in names(pc_groups)) {
            vars <- pc_groups[[group_name]]
            group_x <- x_repo %>% select(all_of(vars))
            pc_res <- prcomp(group_x, center = TRUE, scale. = TRUE)
            pc1_scores <- pc_res$x[, 1]
            pc_col <- paste0(group_name, "_pc1")
            if (group_name == "shared_knowledge") { pc1_scores <- -pc1_scores}
            x_repo[[pc_col]] <- pc1_scores
            pc_cols <- c(pc_cols, pc_col)
            loadings_vec <- pc_res$rotation[vars, 1]
            loadings_str <- paste(paste0(vars, sprintf("%+.3f", loadings_vec)), collapse = ", ")
            message("PC1 ", group_name, " loadings: ", substr(loadings_str, 1, 200))
          }
          x_policy_num <- x_repo %>% select(all_of(pc_cols)) %>% mutate(across(everything(), ~as.numeric(.)))
          x_policy_mat <- as.matrix(x_policy_num)
          x_default_mat <- as.matrix(x_num)
          
          df_repo_data <- df_data %>% select(repo_id, repo_name, quasi_treatment_group, treatment_group, fold) %>% distinct()
          
          for (estimation_type in ESTIMATION_TYPES) {
            for (has_pc in c(FALSE, TRUE)) {
              rolling_panel_imp2 <- paste0(rolling_panel_imp, ifelse(has_pc, "_pc", ""))
              
              df_causal_forest_bins <- read_parquet(
                file.path(INDIR_CF, dataset, rolling_panel_imp2,
                          paste0(split_var, "_repo_att_", method, ".parquet"))) %>%
                filter(type == estimation_type) 
              
              for (tc in tree_configs) {
                depth_val <- tc$depth
                split_step_val <- tc$split_step
                tag_val <- tc$tag
                
                # map has_pc -> input_choice (ONLY one choice per has_pc)
                input_choice <- if (has_pc) "pc" else "default"
                x_for_tree <- if (input_choice == "pc") x_policy_mat else x_default_mat
                tag_input <- paste0(tag_val, "_", input_choice)
                
                x_for_tree <- x_for_tree[x_repo$repo_id %in% df_causal_forest_bins$repo_id,]
                tmp_res <- ProcessTreeCombo(x_for_tree = x_for_tree,
                                            df_causal_forest_bins = df_causal_forest_bins,
                                            x_repo = x_repo[x_repo$repo_id %in% df_causal_forest_bins$repo_id,],
                                            input_choice = input_choice,
                                            depth_val = depth_val,
                                            split_step_val = split_step_val,
                                            tag_input = tag_input,
                                            method = method,
                                            split_mode = split_mode,
                                            rolling_panel_imp2 = rolling_panel_imp2,
                                            estimation_type = estimation_type,
                                            outcome_modes = outcome_modes,
                                            norm_options = norm_options,
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

coeffs_df <- bind_rows(coeffs_all) %>% mutate(event_time = as.numeric(event_time))
write_csv(coeffs_df, file.path(OUTDIR, paste0("policy_tree_all_coefficients.csv")))

png_files <- png_ordered[!grepl("_hist_", png_ordered)]
pdf(file.path(OUTDIR, paste0("policy_tree_results.pdf")), width = 11, height = 8.5)
if (length(png_files) > 0) {
  first_group <- png_files[1:min(4, length(png_files))]
  imgs <- lapply(first_group, function(f) grid::rasterGrob(readPNG(f), interpolate = TRUE))
  if (length(imgs) < 4) {
    for (i in seq_len(4 - length(imgs))) imgs <- c(imgs, list(textGrob("")))
  }
  do.call(grid.arrange, c(imgs, ncol = 2))
  
  if (length(png_files) > 4) {
    remaining <- png_files[-(1:4)]
    for (i in seq(1, length(remaining), by = 4)) {
      group_files <- remaining[i:min(i+3, length(remaining))]
      imgs <- lapply(group_files, function(f) grid::rasterGrob(readPNG(f), interpolate = TRUE))
      if (length(imgs) < 4) {
        for (p in seq_len(4 - length(imgs))) imgs <- c(imgs, list(textGrob("")))
      }
      do.call(grid.arrange, c(imgs, ncol = 2))
    }
  }
}
dev.off()
