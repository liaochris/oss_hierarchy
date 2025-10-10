#######################################
# 0. Libraries & Sources
#######################################
library(tidyverse)
library(grf)
library(arrow)
library(yaml)
library(fs)
library(zeallot)
library(policytree)
library(gridExtra)
library(png)
library(grid)

source("source/lib/helpers.R")
source("source/analysis/causal_forest_helpers.R")

#######################################
# 1. Globals
#######################################
SEED <- 420
set.seed(SEED)
train_frac <- 0.5

INDIR_CF <- "output/analysis/causal_forest_personalization"
INDIR_YAML <- "source/derived/org_characteristics"
INDIR <- "drive/output/derived/org_characteristics/org_panel"
OUTDIR <- "output/analysis/causal_forest_personalization"

dir_create(OUTDIR)

#######################################
# 2. Helpers
#######################################
AssignTrainingTest <- function(df_causal_forest_bins, lb_prob, ub_prob, seed = SEED) {
  df_dr_scores <- df_causal_forest_bins %>%
    filter(type == "CATE", !is.na(att))
  
  lb_val <- quantile(df_dr_scores$att, probs = lb_prob, na.rm = TRUE)
  ub_val <- quantile(df_dr_scores$att, probs = ub_prob, na.rm = TRUE)
  
  df_dr_scores <- df_dr_scores %>% filter(att >= lb_val & att <= ub_val)
  
  unique_repo_ids <- unique(df_dr_scores$repo_id)
  set.seed(seed)
  n_train <- ceiling(length(unique_repo_ids) * train_frac)
  repo_train_ids <- sample(unique_repo_ids, size = n_train, replace = FALSE)
  
  df_dr_scores %>% mutate(split_group = if_else(repo_id %in% repo_train_ids, "train", "test"))
}

#######################################
# 3. Core Function
#######################################
RunPolicyTreeForOutcome <- function(outcome, df_panel_common, org_practice_modes,
                                    df_causal_forest_bins, outdir_for_spec,
                                    method, rolling_period, top = "all",
                                    min_node_sizes = seq(100, 500, by = 100),
                                    lbub_choices = list(c(0.01, 0.99), c(0, 1)),
                                    seed = SEED) {
  message("Policy-tree (inline plotting) for outcome=", outcome, " method=", method, " rolling=", rolling_period)
  
  if (method == "lm_forest" | method == "multi_arm") {
    df_panel_outcome <- ResidualizeOutcome(df_panel_common, outcome)
  } else if (method == "lm_forest_nonlinear") {
    df_panel_outcome <- NormalizeOutcome(df_panel_common, outcome)
    norm_outcome_col <- paste(outcome, "norm", sep = "_")
    df_panel_outcome <- FirstDifferenceOutcome(df_panel_outcome, norm_outcome_col)
    df_panel_outcome$resid_outcome <- df_panel_outcome$fd_outcome
  } else {
    stop("Unsupported method: ", method)
  }
  
  covars     <- unlist(lapply(org_practice_modes, \(x) x$continuous_covariate))
  covars_imp <- unlist(lapply(org_practice_modes, function(x) {
    if (!str_detect(x$legend_title, "organizational_routines")) paste0(x$continuous_covariate, "_imp")
  }))
  
  df_data <- ComputeCovariateMeans(df_panel_outcome, c(covars, covars_imp), rolling_period) %>%
    filter(quasi_event_time != -1) %>%
    mutate(treatment_arm = factor(ifelse(treatment_group == 0, "never-treated",
                                         paste0("cohort", treatment_group, "event_time", quasi_event_time))),
           treatment_arm = relevel(treatment_arm, ref = "never-treated"))
  
  # Covariate selection
  if (top == "all") {
    keep_names <- paste0(covars, "_mean")
  } else {
    Y_reg_forest <- df_data %>% filter(quasi_event_time >= 0) %>% pull(resid_outcome)
    X_reg_forest <- as.matrix(df_data %>% filter(quasi_event_time >= 0) %>% select(all_of(paste0(covars, "_mean"))))
    varimp.Y <- variable_importance(regression_forest(X_reg_forest, Y_reg_forest))
    ranked_vars <- colnames(X_reg_forest)[order(varimp.Y, decreasing = TRUE)]
    keep <- ranked_vars[!duplicated(sub("_imp_mean$", "_mean", ranked_vars))][1:top]
    keep_names <- keep
  }
  
  Y <- df_data$resid_outcome
  max_time_val <- max(df_data$time_index, na.rm = TRUE)
  X_all <- df_data %>% select(repo_id, all_of(keep_names)) %>%
    select(-prop_tests_passed_mean) %>% 
    arrange(repo_id) %>% 
    mutate(across(everything(), ~replace_na(.x, 0))) %>%
    unique()

  bins_accum <- list()
  summary_accum <- list()
  plot_files <- c()
  
  for (lbub in lbub_choices) {
    lb <- lbub[1]
    ub <- lbub[2]
    message(" lb/ub = ", lb, "/", ub)
    df_dr_scores <- AssignTrainingTest(df_causal_forest_bins, lb, ub, seed = seed)
    
    repo_train_ids <- df_dr_scores %>% filter(split_group == "train") %>% pull(repo_id) %>% unique()
    train_df <- df_dr_scores %>% filter(repo_id %in% repo_train_ids) %>% distinct(repo_id, .keep_all = TRUE)
    
    X_train <- X_all %>% 
      filter(repo_id %in% repo_train_ids) %>% 
      unique() %>%
      arrange(repo_id) %>% 
      select(-repo_id)  
    
    train_order <- X_all %>% filter(repo_id %in% repo_train_ids) %>% arrange(repo_id) %>% pull(repo_id)
    train_df_ordered <- train_df %>% distinct(repo_id, .keep_all = TRUE) %>% arrange(match(repo_id, train_order))
    dr_rewards_matrix <- as.matrix(cbind(control = -1 * train_df_ordered$att, treat = train_df_ordered$att))
    
    for (mns in min_node_sizes) {
      message("  min.node.size = ", mns)
      tree <- policy_tree(X_train, dr_rewards_matrix, depth = 2, min.node.size = mns)
      tree_file <- file.path(outdir_for_spec, paste0(outcome, "_", method, "_rolling", rolling_period,
                                             "_lb", lb, "_ub", ub, "_mns", mns, ".rds"))
      saveRDS(tree, tree_file)
      
      actions_all <- predict(tree, X_all %>% select(-repo_id)) - 1L
      leaves_all <- predict(tree, X_all %>% select(-repo_id), type = "node")
      
      bins_df <- tibble(
        repo_id = X_all %>% arrange(repo_id) %>% pull(repo_id),
        outcome = outcome, method = method, lb = lb, ub = ub,
        min_node_size = mns, leaf = leaves_all, treatment_policytree = actions_all
      ) %>%
        left_join(df_causal_forest_bins %>% select(repo_id, repo_name, att, type), by = "repo_id") %>%
        left_join(df_dr_scores %>% select(repo_id, split_group) %>% distinct(), by = "repo_id")
      
      summary_df <- bins_df %>%
        group_by(outcome, method, lb, ub, min_node_size, leaf, treatment_policytree) %>%
        summarise(n = n(), mean_att = if (all(is.na(att))) NA_real_ else mean(att, na.rm = TRUE), .groups = "drop")
      
      bins_accum[[length(bins_accum) + 1]] <- bins_df
      summary_accum[[length(summary_accum) + 1]] <- summary_df
    }
  }
  
  df_bins_by_size <- bind_rows(bins_accum)
  df_summary_by_size <- bind_rows(summary_accum)
  
  top_str <- ifelse(top == "all", "all", paste0("top", top))
  rolling_top_str <- paste(rolling_panel, top_str, sep = "_")
  
  summary_outfile <- file.path(outdir_for_spec, paste0(outcome, "_summary_bins_", method, "_rolling", rolling_period, ".parquet"))
  bins_outfile <- file.path(outdir_for_spec, paste0(outcome, "_repo_att_", method, "_", rolling_top_str, ".parquet"))
  write_parquet(df_bins_by_size, bins_outfile)
  write_parquet(df_summary_by_size, summary_outfile)

  
  message(" wrote bins -> ", bins_outfile)
  message(" wrote summary -> ", summary_outfile)
  
  list(bins = df_bins_by_size, summary = df_summary_by_size)
}

#######################################
# 4. Driver
#######################################
outcome_cfg <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))
org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariate_organization.yaml"))

variants <- c("important_topk")
rolling_panels <- c("rolling5", "rolling1")
tops <- c("all")
methods <- c("lm_forest", "lm_forest_nonlinear", "multi_arm")

for (variant in variants) {
  for (rolling_panel in rolling_panels) {
    rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
    for (top in tops) {
      top_str <- ifelse(top == "all", "all", paste0("top", top))
      rolling_top_str <- paste(rolling_panel, top_str, sep = "_")
      
      df_panel <- read_parquet(file.path(INDIR, variant, paste0("panel_", rolling_panel, ".parquet")))
      all_outcomes <- unlist(lapply(outcome_cfg, function(x) x$main))
      df_panel_common <- BuildCommonSample(df_panel, all_outcomes) %>%
        filter(num_departures <= 1) %>%
        mutate(quasi_event_time = time_index - quasi_treatment_group)
      
      org_practice_modes <- BuildOrgPracticeModes(org_practice_cfg, "nevertreated",
                                                  file.path(OUTDIR, variant, paste0(rolling_panel, "_", top_str)))
      outcome_modes <- BuildOutcomeModes(outcome_cfg, "nevertreated",
                                         file.path(OUTDIR, variant, paste0(rolling_panel, "_", top_str)),
                                         c(TRUE))
      
      for (method in methods) {
        outdir_for_spec <- file.path(OUTDIR, variant, paste0(rolling_panel, "_", top_str, "_policytree"))
        dir_create(outdir_for_spec)
        
        for (outcome_mode in list(outcome_modes[[2]])) {
          split_var <- outcome_mode$outcome
          infile <- file.path(INDIR_CF, variant, rolling_top_str,
                              paste0(split_var, "_repo_att_", method, "_", rolling_top_str, ".parquet"))
          df_causal_forest_bins <- read_parquet(infile)
          
          RunPolicyTreeForOutcome(
            outcome = split_var,
            df_panel_common = df_panel_common,
            org_practice_modes = org_practice_modes,
            df_causal_forest_bins = df_causal_forest_bins,
            outdir_for_spec = outdir_for_spec,
            method = method,
            rolling_period = rolling_period,
            top = top,
            min_node_sizes = seq(100, 500, by = 200),
            lbub_choices = list(c(0.01, 0.99), c(0, 1)),
            seed = SEED
          )
        }
      }
    }
  }
}
