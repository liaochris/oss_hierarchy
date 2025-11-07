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
# CreateDataPanel is expected in causal_forest_helpers.R; keep this if available.
# If CreateDataPanel already lives in another helper, remove or adjust this line.
source("source/analysis/causal_forest_helpers.R")

norm_options <- c(TRUE)

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
      att_diffs_all <- list()
      
      #######################################
      # Baseline outcome plots + collect coeffs
      #######################################
      metrics    <- c("sa")
      metrics_fn <- c("Sun and Abraham 2020")
      
      for (outcome_mode in outcome_modes) {
        es_list <- lapply(metrics, function(m) {
          panel <- get(outcome_mode$data)
          EventStudy(panel, outcome_mode$outcome, outcome_mode$control_group,
                     m, title = "", normalize = outcome_mode$normalize)
        })
        png(outcome_mode$file)
        CompareES(es_list, title = "", legend_labels = metrics_fn, add_comparison = F,
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
      # Org practice splits + ATT-diff computation
      #######################################
      metrics    <- c("sa")
      
      for (practice_mode in org_practice_modes) {
        covar <- practice_mode$continuous_covariate
        base_df <- get(outcome_mode$data)
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
 
        # compute ATT differences using first filter's first two vals as low/high
        if (length(practice_mode$filters) >= 1) {
          split_filter <- practice_mode$filters[[1]]
          split_col <- split_filter$col
          split_vals <- split_filter$vals
          if (length(split_vals) >= 2) {
            val_low <- split_vals[[1]]
            val_high <- split_vals[[2]]
            for (outcome_mode in outcome_modes) {
              for (norm in norm_options) {
                df_low  <- base_df %>% filter(.data[[split_col]] == val_low)
                df_high <- base_df %>% filter(.data[[split_col]] == val_high)
                att_low <- tryCatch(
                  GetEventAtt(df_low, outcome_mode$outcome, outcome_mode$control_group, method = "sa", normalize = norm, periods = 1:5),
                  error = function(e) NULL
                )
                
                att_high <- tryCatch(
                  GetEventAtt(df_high, outcome_mode$outcome, outcome_mode$control_group, method = "sa", normalize = norm, periods = 1:5),
                  error = function(e) NULL
                )
                
                # Skip if either estimation failed or returned NULL
                if (is.null(att_low) || is.null(att_high)) next
                
                att_diff <- ComputeDiffHighLow(att_high, att_low) %>%
                  mutate(dataset = dataset,
                         rolling = rolling_panel,
                         category = outcome_mode$category,
                         outcome = outcome_mode$outcome,
                         normalize = norm,
                         covar = covar,
                         split_col = split_col,
                         low_value = as.character(val_low),
                         high_value = as.character(val_high)) %>%
                  select(dataset, rolling, category, outcome, normalize, covar, split_col, low_value, high_value,
                         period, high_estimate, high_sd, high_ci_low, high_ci_high,
                         low_estimate, low_sd, low_ci_low, low_ci_high,
                         diff_estimate, diff_se, diff_ci_low, diff_ci_high)
                
                att_diffs_all[[length(att_diffs_all) + 1]] <- att_diff
              }
            }
          }
        }
        
        # existing split-based event study (keeps previous behavior)
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
      # NEW: PCA-based PC1 splits for groups + ATT diffs
      # Use looping outcome_mode$outcome and rolling_period from the surrounding loops
      #######################################
      covars     <- unlist(lapply(org_practice_modes, function(x) x$continuous_covariate))
      covars_imp <- unlist(lapply(org_practice_modes, function(x) paste0(x$continuous_covariate, "_imp")))
      
      # Define PCA groups (adjust as desired)
      pc_groups <- list(
        collaboration = c("avg_members_per_problem_mean", "pct_members_multiple_mean",
                          "proj_hhi_discussion_comment_mean", "proj_prob_hhi_issue_comment_mean",
                          "proj_prob_hhi_pull_request_comment_mean"),
        shared_knowledge = c("share_issue_and_pr_mean", "avg_unique_types_mean", "text_sim_ratio_mean"),
        discussion_vibes = c("response_rate_mean", "mean_days_to_respond_mean", "ov_sentiment_avg_mean",
                             "pos_sentiment_avg_mean", "neg_sentiment_avg_mean"),
        welcoming = c("has_good_first_issue_mean", "has_contributing_guide_mean", "has_code_of_conduct_mean"),
        routines = c("mean_has_reviewer_mean", "mean_has_tag_mean", "mean_has_assignee_mean",
                     "has_codeowners_mean", "has_issue_template_mean", "has_pr_template_mean")
      )
      
      pc_outdir <- file.path(outdir_dataset, "pc_event_studies")
      dir_create(pc_outdir, recurse = TRUE)
      
      coeffs_pc_all <- list()
      att_diffs_pc_groups <- list()
      
      # For each outcome_mode (use the exact outcome and rolling_period currently in loop)
      for (outcome_mode in outcome_modes) {
        outcome_local <- outcome_mode$outcome
        rolling_period_local <- as.numeric(str_extract(rolling_panel, "\\d+$"))
        message("PCA: building panel for outcome=", outcome_local, " rolling=", rolling_period_local)
        
        # Standard event-study panel (the same data used elsewhere)
        panel <- get(outcome_mode$data)
        if (nrow(panel) == 0) {
          message("  skipping outcome ", outcome_local, " because panel is empty")
          next
        }
        
        # Create df_data for this outcome + rolling_period (used only to compute repo-level covariates / PC1)
        df_data <- CreateDataPanel(df_panel_nevertreated, "lm_forest_nonlinear", outcome_local,
                                    c(covars, covars_imp), rolling_period_local, 10, SEED)
        df_repo_data <- df_data %>% select(repo_id, repo_name, quasi_treatment_group, treatment_group) %>% unique()
        
        # Build covariate matrix for PCA (use same keep_names filtering you wanted)
        keep_names <- intersect(paste0(covars, "_mean"), colnames(df_data))
        x <- df_data %>% select(all_of(keep_names))
        keep_names_nonna <- keep_names[colSums(is.na(x)) < 200]
        keep_names_nonna <- setdiff(keep_names_nonna, c("share_issue_only_mean", "share_pr_only_mean"))
        df_data_nonna <- df_data[complete.cases(df_data %>% select(all_of(keep_names_nonna))),]
        x_na <- df_data_nonna %>% select(all_of(keep_names_nonna))
        
        # Save correlation heatmap if >1 var
        if (ncol(x_na) > 1) {
          cormat <- round(cor(x_na), 2)
          melted_cormat <- reshape2::melt(cormat)
          png(file.path(pc_outdir, paste0("covariate_cormat_", outcome_local, ".png")), width = 800, height = 800)
          print(ggplot(data = melted_cormat, aes(x = Var1, y = Var2, fill = value)) +
                  geom_tile() +
                  scale_fill_gradient2(low = "blue", mid = "white", high = "red", midpoint = 0, limits = c(-1, 1)) +
                  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1)))
          dev.off()
        }
        
        # Loop over PC groups and run PCA / PC1 split EventStudy using this outcome_local
        for (group_name in names(pc_groups)) {
          vars <- pc_groups[[group_name]]
          vars_present <- intersect(vars, colnames(x_na))
          message("PC group: ", group_name, " vars present: ", paste(vars_present, collapse = ", "))
          if (length(vars_present) < 2) {
            message("  skipping ", group_name, " (<2 vars present)")
            next
          }
          
          group_x <- x_na %>% select(all_of(vars_present))
          # require some minimum complete rows for stable PCA
          complete_idx <- complete.cases(group_x)
          if (sum(complete_idx) < 50) {
            message("  skipping ", group_name, " (only ", sum(complete_idx), " complete rows)")
            next
          }
          
          pc_res <- prcomp(group_x[complete_idx, ], center = TRUE, scale. = TRUE)
          pc1_scores <- pc_res$x[, 1]
          # format PC1 loadings (sign + 3 decimals) for vars_present
          loadings_vec <- pc_res$rotation[vars_present, 1]
          loading_labels <- paste0(vars_present, " ", sprintf("%+.3f", loadings_vec))

          # break long vector into lines of ~3 vars each
          line_size <- 3
          loading_lines <- split(loading_labels, ceiling(seq_along(loading_labels) / line_size))
          loading_multiline <- paste(sapply(loading_lines, paste, collapse = ", "), collapse = "\n")

          legend_title_pc <- paste0("PC1 loadings:\n", loading_multiline)

          df_group <- df_data_nonna %>% slice(which(complete.cases(group_x)))
          df_group <- df_group %>% mutate(!!paste0(group_name, "_pc1") := pc1_scores)
          median_pc1 <- median(pc1_scores, na.rm = TRUE)
          
          # Identify low/high repo_names from df_group
          low_repo_names  <- df_group %>% filter((!!rlang::sym(paste0(group_name, "_pc1"))) <= median_pc1) %>% pull(repo_name) %>% unique()
          high_repo_names <- df_group %>% filter((!!rlang::sym(paste0(group_name, "_pc1"))) >  median_pc1) %>% pull(repo_name) %>% unique()
          message("  low n_repos=", length(low_repo_names), " high n_repos=", length(high_repo_names))
          if (length(low_repo_names) < 10 || length(high_repo_names) < 10) {
            message("  skipping event study for ", group_name, " due to small repo counts")
            next
          }
          
          # Filter the standard panel (time-series) to the repos in each group
          panel_low  <- panel %>% filter(repo_name %in% low_repo_names)
          panel_high <- panel %>% filter(repo_name %in% high_repo_names)
          if (nrow(panel_low) < 20 || nrow(panel_high) < 20) {
            message("  skipping event study for ", group_name, " because filtered panels are small (rows): low=", nrow(panel_low), " high=", nrow(panel_high))
            next
          }
          
          # Run event studies for each normalization option to match existing behavior
          for (norm in norm_options) {
            es_low <- tryCatch(EventStudy(panel_low, outcome_local, outcome_mode$control_group,
                                          "sa", title = paste0(group_name, " PC1 <= median"), normalize = norm),
                                error = function(e) { message("    EventStudy low error: ", e$message); NULL })
            es_high <- tryCatch(EventStudy(panel_high, outcome_local, outcome_mode$control_group,
                                            "sa", title = paste0(group_name, " PC1 > median"), normalize = norm),
                                error = function(e) { message("    EventStudy high error: ", e$message); NULL })
            
            if (!is.null(es_low) && !is.null(es_high)) {
              out_png <- file.path(pc_outdir, paste0(group_name, "_", outcome_local, "_pc1_split", ifelse(norm, "_norm",""), ".png"))
              png(out_png, width = 900, height = 600)
              # pass legend_title_pc into CompareES so the plot shows variables+loadings
              CompareES(list(es_low, es_high),
                        legend_labels = c("PC1 <= median", "PC1 > median"),
                        legend_title  = legend_title_pc,
                        title = paste0("PC1 split: ", group_name, " - ", outcome_local, ifelse(norm, " (norm)","")))
              dev.off()
              
              res_low <- es_low$results
              res_high <- es_high$results
              coeffs_pc_all[[length(coeffs_pc_all) + 1]] <- as_tibble(res_low, rownames = "event_time") %>%
                mutate(dataset = dataset, rolling = rolling_panel, category = outcome_mode$category,
                        outcome = outcome_local, normalize = norm,
                        method = "sa", covar = paste0(group_name, "_pc1"), split_value = "low")
              coeffs_pc_all[[length(coeffs_pc_all) + 1]] <- as_tibble(res_high, rownames = "event_time") %>%
                mutate(dataset = dataset, rolling = rolling_panel, category = outcome_mode$category,
                        outcome = outcome_local, normalize = norm,
                        method = "sa", covar = paste0(group_name, "_pc1"), split_value = "high")
            } else {
              message("    one of EventStudy runs failed (low or high) for norm=", norm)
            }
            
            # ATT diffs using GetEventAtt + ComputeDiffHighLow (periods 1:5) — operate on the same filtered panels
            att_low <- tryCatch(GetEventAtt(panel_low, outcome_local, outcome_mode$control_group, method = "sa", normalize = TRUE, periods = 1:5),
                                error = function(e) NULL)
            att_high <- tryCatch(GetEventAtt(panel_high, outcome_local, outcome_mode$control_group, method = "sa", normalize = TRUE, periods = 1:5),
                                  error = function(e) NULL)
            if (!is.null(att_low) && !is.null(att_high)) {
              att_diff <- ComputeDiffHighLow(att_high, att_low) %>%
                mutate(dataset = dataset,
                        rolling = rolling_panel,
                        category = outcome_mode$category,
                        outcome = outcome_local,
                        covar = paste0(group_name, "_pc1"))
              att_diffs_pc_groups[[length(att_diffs_pc_groups) + 1]] <- att_diff
            }
          } # end norm loop
        } # end pc_groups loop for this outcome
      } # end outcome_mode loop (PCA per outcome)

      # Save PC-based coefficients and att diffs (aggregate across outcomes)
      if (length(coeffs_pc_all) > 0) {
        coeffs_pc_df <- bind_rows(coeffs_pc_all) %>% mutate(event_time = as.numeric(event_time))
        SaveData(coeffs_pc_df,
                 c("dataset","rolling","category","outcome","normalize","method","covar","split_value","event_time"),
                 file.path(pc_outdir, paste0("pc1_split_coefficients_", rolling_panel, ".csv")),
                 file.path(pc_outdir, paste0("pc1_split_coefficients_", rolling_panel, ".log")),
                 sortbykey = FALSE)
        # append to main coeffs_all so they appear in all_coefficients output
        coeffs_all <- c(coeffs_all, as.list(split(coeffs_pc_df, seq_len(nrow(coeffs_pc_df)))))
      }
      if (length(att_diffs_pc_groups) > 0) {
        att_diffs_pc_df <- bind_rows(att_diffs_pc_groups) %>% mutate(period = as.numeric(period))
        SaveData(att_diffs_pc_df,
                 c("dataset","rolling","category","outcome","covar","period"),
                 file.path(pc_outdir, paste0("att_diff_pc1_high_low_", rolling_panel, ".csv")),
                 file.path(pc_outdir, paste0("att_diff_pc1_high_low_", rolling_panel, ".log")),
                 sortbykey = FALSE)
      }
      
      #######################################
      # Export results
      #######################################
      coeffs_df <- bind_rows(coeffs_all) %>%
        mutate(event_time = as.numeric(event_time),
               covar = ifelse(is.na(covar), "", covar),
               split_value = ifelse(is.na(split_value), "", split_value))
      SaveData(coeffs_df,
               c("dataset","rolling",
                 "category","outcome","normalize","method","covar","split_value", "event_time"),
               file.path(outdir_dataset,  paste0("all_coefficients","_",rolling_panel,".csv")),
               file.path(outdir_dataset,  paste0("all_coefficients","_",rolling_panel,".log")),
               sortbykey = FALSE)
      
      
      if (length(att_diffs_all) > 0) {
        att_diffs_df <- bind_rows(att_diffs_all) %>% mutate(period = as.numeric(period))
        SaveData(att_diffs_df,
                 c("dataset","rolling","category","outcome","normalize","covar","split_col","low_value","high_value","period"),
                 file.path(outdir_dataset, paste0("att_diff_high_low_", rolling_panel, ".csv")),
                 file.path(outdir_dataset, paste0("att_diff_high_low_", rolling_panel, ".log")),
                 sortbykey = FALSE)
        
        practice_map_file <- "source/analysis/organizational_practice_map.csv"
        outcome_map <- c(
          "major_minor_release_count" = "Major and minor software releases",
          "overall_new_release_count" = "New software releases",
          "pull_request_opened" = "Pull requests opened",
          "pull_request_merged" = "Pull requests merged"
        )
        
        map_df <- ReadPracticeMap(practice_map_file)
        
        PlotPracticeOutcomeHorizontal(att_diffs_df,
                                      out_file = file.path(outdir_dataset, "att_by_practice_outcomes_grouped.png"),
                                      practice_map = map_df,
                                      outcome_map = outcome_map,
                                      periods = 1:5,
                                      label_offset = 0.43,    # fraction of x_range: labels distance left of plotted area
                                      bracket_offset = 0.45,  # fraction of x_range: bracket further left than labels
                                      right_pad = 0.18,       # fraction of x_range to pad right side of axis
                                      left_margin_pt = 0,
                                      cap_height = 0.12,
                                      cap_thickness = 1.0,
                                      point_size = 3,
                                      point_alpha = 0.6,
                                      width = 10,
                                      height = 8)
      }
      
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
