#######################################
# Event-study main script (uses helpers from separate files)
#######################################

# Libraries
library(tidyverse)
library(arrow)
library(yaml)
library(fs)
library(eventstudyr)
library(gridExtra)
library(png)
library(grid)
library(future.apply)
library(SaveData)

# Other packages (used by helpers and plotting)
library(dplyr)
library(tidyr)
library(broom)
library(fixest)
library(did)
library(aod)
library(stringr)
library(tibble)
library(purrr)
library(ggplot2)

# Additional packages needed for PCA/data prep
library(reshape2)
library(zeallot)
library(rlang)
library(grf)

source("source/lib/helpers.R")
source("source/analysis/event_study_helpers.R")
source("source/analysis/causal_forest_helpers.R")

norm_options <- c(TRUE)

PreparePCAFilter <- function(df_data_raw, covars, na_threshold = 200) {
  keep_names <- intersect(paste0(covars, "_mean"), colnames(df_data_raw))
  if (length(keep_names) == 0) {
    return(list(df_data_filtered = df_data_raw[0, ], keep_names_nonna = character(0)))
  }
  x_all <- df_data_raw %>% select(all_of(keep_names))
  keep_names_nonna <- keep_names[colSums(is.na(x_all)) < na_threshold]
  keep_names_nonna <- setdiff(keep_names_nonna, c("share_issue_only_mean", "share_pr_only_mean"))
  if (length(keep_names_nonna) == 0) {
    return(list(df_data_filtered = df_data_raw[0, ], keep_names_nonna = character(0)))
  }
  df_data_filtered <- df_data_raw %>% filter(complete.cases(select(., all_of(keep_names_nonna))))
  list(df_data_filtered = df_data_filtered, keep_names_nonna = keep_names_nonna)
}


BuildPCAGroups <- function(df_data, pc_groups, score_name_map = list()) {
  results <- list()
  for (group_name in names(pc_groups)) {
    vars <- pc_groups[[group_name]]
    vars_present <- intersect(vars, colnames(df_data))
    group_matrix <- df_data %>% select(all_of(vars_present))
    pc_res <- prcomp(group_matrix, center = TRUE, scale. = TRUE)
    pc1_scores <- pc_res$x[, 1]
    
    # correct sign for shared_knowledge
    if (group_name == "shared_knowledge") pc1_scores <- -pc1_scores
    score_col <- paste0(group_name, "_score")
    
    df_scores <- df_data %>% slice(which(complete.cases(group_matrix))) %>% mutate(!!score_col := pc1_scores)
    repo_scores <- df_scores %>% group_by(repo_name) %>% summarize(group_score = mean(!!rlang::sym(score_col), na.rm = TRUE), .groups = "drop")
    median_score <- median(repo_scores$group_score, na.rm = TRUE)
    repo_low <- repo_scores %>% filter(group_score <= median_score) %>% pull(repo_name) %>% unique()
    repo_high <- repo_scores %>% filter(group_score >  median_score) %>% pull(repo_name) %>% unique()
    
    loadings <- pc_res$rotation[vars_present, 1]
    variance_explained <- (pc_res$sdev[1]^2) / sum(pc_res$sdev^2) * 100
    friendly_label <- score_name_map[[group_name]]
    results[[group_name]] <- list(repo_low = repo_low,
                                  repo_high = repo_high,
                                  loadings = loadings,
                                  variance_explained = as.numeric(variance_explained),
                                  repo_scores = repo_scores,
                                  friendly_label = friendly_label,
                                  vars_present = vars_present)
  }
  results
}

main <- function() {
  SEED <- 420
  set.seed(SEED)
  
  INDIR  <- "drive/output/derived/org_characteristics/org_panel"
  INDIR_YAML <- "source/derived/org_characteristics"
  OUTDIR <- "output/analysis/event_study"
  dir_create(OUTDIR)
  
  DATASETS <- c("important_topk_exact1")
  exclude_outcomes <- c("num_downloads")
  
  outcome_cfg      <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))
  org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariate_organization.yaml"))
  
  plan(multisession, workers = availableCores() - 1)
  
  for (dataset in DATASETS) {
    for (rolling_panel in c("rolling5")) {
      message("Processing dataset: ", dataset, " (", rolling_panel, ")")
      
      outdir_dataset <- file.path(OUTDIR, dataset, rolling_panel)
      dir_create(outdir_dataset, recurse = TRUE)
      
      panel_dataset <- gsub("_exact1", "", dataset)
      panel_dataset <- gsub("_oneQual", "", panel_dataset)
      num_qualified_label <- ifelse(grepl("_exact1", dataset), "num-qualified=1",
                                    ifelse(grepl("_oneQual", dataset), "num-qualified>=1", "all obs"))
      df_panel <- read_parquet(file.path(INDIR, panel_dataset, paste0("panel_", rolling_panel, ".parquet")))
      
      all_outcomes <- unlist(lapply(outcome_cfg, function(x) x$main))
      df_panel_common <- BuildCommonSample(df_panel, all_outcomes)
      if (endsWith(dataset, "exact1")) {
        df_panel_common <- KeepSustainedImportant(df_panel_common, lb = 1, ub = 1)
      } else {
        df_panel_common <- KeepSustainedImportant(df_panel_common)
      }
      
      df_panel_notyettreated <- df_panel_common %>% filter(num_departures == 1)
      df_panel_nevertreated  <- df_panel_common %>% filter(num_departures <= 1)
      
      assign("df_panel_notyettreated", df_panel_notyettreated, envir = .GlobalEnv)
      assign("df_panel_nevertreated",  df_panel_nevertreated,  envir = .GlobalEnv)
      
      outcome_modes      <- BuildOutcomeModes(outcome_cfg, "nevertreated", outdir_dataset,
                                              norm_options, build_dir = TRUE)
      org_practice_modes <- BuildOrgPracticeModes(org_practice_cfg, "nevertreated", outdir_dataset, build_dir = TRUE)
      
      coeffs_all <- list()
      
      covars     <- unlist(lapply(org_practice_modes, function(x) x$continuous_covariate))
      covars_imp <- unlist(lapply(org_practice_modes, function(x) paste0(x$continuous_covariate, "_imp")))
      
      pc_groups <- list(
        collaboration = c("avg_members_per_problem_mean", "pct_members_multiple_mean",
                          "proj_hhi_discussion_comment_mean", "proj_prob_hhi_issue_comment_mean",
                          "proj_prob_hhi_pull_request_comment_mean"),
        shared_knowledge = c("share_issue_and_pr_mean", "avg_unique_types_mean"),
        discussion_quality = c("response_rate_mean", "mean_days_to_respond_mean", "ov_sentiment_avg_mean",
                               "pos_sentiment_avg_mean", "neg_sentiment_avg_mean"),
        investment_in_new_talent = c("has_good_first_issue_mean", "has_contributing_guide_mean", "has_code_of_conduct_mean"),
        problem_solving_routines = c("mean_has_reviewer_mean", "mean_has_tag_mean", "mean_has_assignee_mean",
                                     "has_codeowners_mean", "has_issue_template_mean", "has_pr_template_mean")
      )
      
      score_name_map <- list(
        "collaboration" = "Collaboration score",
        "shared_knowledge" = "Shared knowledge score",
        "discussion_quality" = "Discussion quality score",
        "investment_in_new_talent" = "Investment in new talent score",
        "problem_solving_routines" = "Problem-solving routines score"
      )
      
      first_outcome <- outcome_modes[[1]]$outcome
      rolling_period_local <- as.numeric(str_extract(rolling_panel, "\\d+$"))
      df_data_raw_pca <- CreateDataPanel(df_panel_nevertreated, "lm_forest_nonlinear", first_outcome,
                                         c(covars, covars_imp), rolling_period_local, 10, SEED)
      pca_prep <- PreparePCAFilter(df_data_raw_pca, covars, na_threshold = 200)
      df_data_filtered <- pca_prep$df_data_filtered
      pc_group_results <- BuildPCAGroups(df_data_filtered, pc_groups, score_name_map)
    

      pc_outdir <- file.path(outdir_dataset, "pc_event_studies")
      dir_create(pc_outdir, recurse = TRUE)
      pc_summary_list <- list()
      if (length(pc_group_results) > 0) {
        for (g in names(pc_group_results)) {
          r <- pc_group_results[[g]]
          pc_loadings_df <- tibble(dataset = dataset,
                                   rolling = rolling_panel,
                                   category = "pca", 
                                   pc_group = g,
                                   var = names(r$loadings),
                                   loading = as.numeric(r$loadings),
                                   variance_explained_pc1 = as.numeric(r$variance_explained))
          pc_summary_list[[length(pc_summary_list) + 1]] <- pc_loadings_df
        }
        pc_summary_df <- bind_rows(pc_summary_list)
        SaveData(pc_summary_df,
                 c("dataset","rolling","category","pc_group","var"),
                 file.path(pc_outdir, paste0("pc1_variance_and_loadings_", rolling_panel, ".csv")),
                 file.path(pc_outdir, paste0("pc1_variance_and_loadings_", rolling_panel, ".log")),
                 sortbykey = FALSE)
      }
      
      #######################################
      # Baseline outcome plots + collect coeffs
      #######################################
      metrics    <- c("sa")
      metrics_fn <- c("Sun and Abraham 2020")
      
      for (outcome_mode in outcome_modes) {
        es_list <- lapply(metrics, function(m) {
          panel <- get(outcome_mode$data) %>%
            filter(repo_name %in% df_data_filtered$repo_name)
          EventStudy(panel, outcome_mode$outcome, outcome_mode$control_group,
                     m, title = "", normalize = outcome_mode$normalize)
        })
        png(outcome_mode$file)
        CompareES(es_list, title = "", legend_labels = NULL, add_comparison = F,
                  ylim = c(-1.75, .75))
        dev.off()
        
        for (i in seq_along(es_list)) {
          res <- es_list[[i]]$results
          coeffs_all[[length(coeffs_all) + 1]] <- as_tibble(res, rownames = "event_time") %>%
            mutate(
              dataset = dataset,
              rolling = rolling_panel,
              category = outcome_mode$category,
              outcome = outcome_mode$outcome,
              normalize = outcome_mode$normalize,
              method = metrics[i],
              covar = NA_character_,
              split_value = NA_real_
            )
        }
      }
      
      #######################################
      # Org practice splits (unchanged)
      #######################################
      metrics    <- c("sa")
      
      for (practice_mode in org_practice_modes) {
        covar <- practice_mode$continuous_covariate
        base_df <- get(outcome_mode$data)  %>%
          filter(repo_name %in% df_data_filtered$repo_name)
        rolling_period <- as.numeric(str_extract(rolling_panel, "\\d+$"))
        
        if (rolling_period == 1) {
          base_df <- CreateOrgPracticeBin(base_df, covar, past_periods = 5)
        } else {
          base_df <- base_df %>%
            group_by(repo_name) %>%
            mutate(
              !!paste0(covar, "_2bin") :=
                if_else(time_index == (quasi_treatment_group - 1) & !is.na(.data[[covar]]),
                        if_else(.data[[covar]] > median(.data[[covar]], na.rm = TRUE), 1, 0),
                        NA_real_)
            ) %>%
            fill(!!paste0(covar, "_2bin"), .direction = "downup") %>%
            ungroup()
        }
        
        for (outcome_mode in outcome_modes) {
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
              out_path <- file.path(practice_mode$folder,
                                    paste0(outcome_mode$outcome, norm_str, ".png"))
              png(out_path)
              title_suffix <- paste0("\nSample: ", num_qualified_label)
              ylim <- NULL
              legend_title <- practice_mode$legend_title
              if (covar %in% c("mean_has_assignee", "ov_sentiment_avg", "issue_share_only")) {
                ylim <- c(-3, 1.5)
                legend_title <- NULL
              }
              do.call(CompareES, list(es_list,
                                      legend_labels = labels,
                                      legend_title  = legend_title,
                                      title = "",
                                      ylim = ylim))
              dev.off()
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
                    covar = covar,
                    split_value = split_val
                  )
              }
            }
          }
        }
      }
      
      #######################################
      # Use precomputed PC groups to run event studies across outcomes
      #######################################
      for (outcome_mode in outcome_modes) {
        outcome_local <- outcome_mode$outcome
        for (g in names(pc_group_results)) {
          r <- pc_group_results[[g]]
          panel_low  <- get(outcome_mode$data) %>% filter(repo_name %in% r$repo_low)
          panel_high <- get(outcome_mode$data) %>% filter(repo_name %in% r$repo_high)
          
          friendly_label <- r$friendly_label
          for (norm in norm_options) {
            es_low <- tryCatch(EventStudy(panel_low, outcome_local, outcome_mode$control_group,
                                          "sa", title = paste0(friendly_label, " <= median"), normalize = norm),
                               error = function(e) { message("EventStudy low error: ", e$message); NULL })
            es_high <- tryCatch(EventStudy(panel_high, outcome_local, outcome_mode$control_group,
                                           "sa", title = paste0(friendly_label, " > median"), normalize = norm),
                                error = function(e) { message("EventStudy high error: ", e$message); NULL })
            if (is.null(es_low) || is.null(es_high)) next
            out_png <- file.path(pc_outdir, paste0(g, "_", outcome_local, "_pc1_split", ifelse(norm, "_norm",""), ".png"))
            png(out_png)
            CompareES(list(es_low, es_high),
                      legend_labels = c("Below median","Above median") ,
                      legend_title  = friendly_label,
                      ylim = c(-2, 1.5))
            dev.off()

            coeffs_pc_all[[length(coeffs_pc_all) + 1]] <- as_tibble(es_low$results, rownames = "event_time") %>%
              mutate(dataset = dataset, rolling = rolling_panel, category = outcome_mode$category,
                     outcome = outcome_local, normalize = norm, method = "sa", covar = paste0(g, "_score"), split_value = "low")
            coeffs_pc_all[[length(coeffs_pc_all) + 1]] <- as_tibble(es_high$results, rownames = "event_time") %>%
              mutate(dataset = dataset, rolling = rolling_panel, category = outcome_mode$category,
                     outcome = outcome_local, normalize = norm, method = "sa", covar = paste0(g, "_score"), split_value = "high")
          } # norm
        } # groups
      } # outcomes

      # Save PC-based coefficients (aggregate across outcomes)
      if (length(coeffs_pc_all) > 0) {
        coeffs_pc_df <- bind_rows(coeffs_pc_all) %>% mutate(event_time = as.numeric(event_time)) %>% unique()
        SaveData(coeffs_pc_df,
                 c("dataset","rolling","category","outcome","normalize","method","covar","split_value","event_time"),
                 file.path(pc_outdir, paste0("pc1_split_coefficients_", rolling_panel, ".csv")),
                 file.path(pc_outdir, paste0("pc1_split_coefficients_", rolling_panel, ".log")),
                 sortbykey = FALSE)
        coeffs_all <- c(coeffs_all, as.list(split(coeffs_pc_df, seq_len(nrow(coeffs_pc_df)))))
      }
      
      #######################################
      # Export results
      #######################################
      coeffs_df <- lapply(coeffs_all, function(x) {
        x$event_time <- as.numeric(x$event_time)
        x$split_value <- as.character(x$split_value)
        x
      }) |> bind_rows() %>%
        mutate(event_time = as.numeric(event_time),
               covar = ifelse(is.na(covar), "", covar),
               split_value = ifelse(is.na(split_value), "", split_value)) 
      SaveData(coeffs_df,
               c("rolling",
                 "category","outcome","normalize","method","covar","split_value", "event_time"),
               file.path(outdir_dataset,  paste0("all_coefficients","_",rolling_panel,".csv")),
               file.path(outdir_dataset,  paste0("all_coefficients","_",rolling_panel,".log")),
               sortbykey = FALSE)
      
      #######################################
      # Combine PNGs into one PDF (per rolling period)
      #######################################
      png_files <- list.files(outdir_dataset, pattern = "\\.png$", full.names = TRUE, recursive = TRUE)
      pdf(file.path(outdir_dataset, paste0("all_results","_",rolling_panel,".pdf")))
      if (length(png_files) > 0) {
        imgs <- lapply(png_files[1:min(4, length(png_files))], function(f) grid::rasterGrob(readPNG(f), interpolate = TRUE))
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
    } # end rolling_panel for
  } # end dataset for
} # end main

# run main
main()
