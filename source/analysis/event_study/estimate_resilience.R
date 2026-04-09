#######################################
# Event-study main script
#######################################

library(tidyverse)
library(arrow)
library(yaml)
library(fs)
library(jsonlite)
library(eventstudyr)
library(gridExtra)
library(png)
library(grid)
library(future.apply)
library(SaveData)
library(fixest)
library(did)
library(aod)
library(broom)
library(reshape2)
library(zeallot)
library(rlang)
library(grf)

source("source/lib/helpers.R")
source("source/analysis/event_study/helpers.R")
source("source/analysis/forest/helpers.R")
source("source/analysis/constants.R")

Main <- function() {
  INDIR      <- "drive/output/derived/org_outcomes_practices/org_panel"
  INDIR_YAML <- "source/analysis/config"
  OUTDIR     <- "output/analysis/event_study"
  dir_create(OUTDIR)

  exclude_outcomes <- c("num_downloads")

  outcome_cfg      <- yaml.load_file(file.path(INDIR_YAML, "outcomes.yaml"))
  org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariates.yaml"))
  pc_cfg           <- read_json(file.path(INDIR_YAML, "pc_groups.json"))

  plan(multisession, workers = availableCores() - 1)

  for (dataset in DATASETS) {
    for (rolling_panel in ROLLING_LABELS) {
      message("Processing dataset: ", dataset, " (", rolling_panel, ")")

      outdir_ds <- file.path(OUTDIR, dataset, rolling_panel)
      dir_create(outdir_ds, recurse = TRUE)

      panel_dataset <- NormalizeDatasetName(dataset)
      num_qualified_label <- MakeDatasetLabels(dataset)$num_qualified
      df_panel <- read_parquet(file.path(INDIR, panel_dataset, paste0("panel_", rolling_panel, ".parquet")))

      all_outcomes <- unlist(lapply(outcome_cfg, function(x) x$main))
      panel <- BuildCommonSample(df_panel, all_outcomes)
      if (endsWith(dataset, "exact1")) {
        panel <- KeepSustainedImportant(panel, lb = 1, ub = 1)
      } else {
        panel <- KeepSustainedImportant(panel)
      }

      panel_nevertreated  <- panel %>% filter(num_departures <= 1)
      panel_notyettreated <- panel %>% filter(num_departures == 1)
      panels <- list(
        nevertreated  = panel_nevertreated,
        notyettreated = panel_notyettreated
      )

      outcome_modes      <- BuildOutcomeModes(outcome_cfg, "nevertreated", outdir_ds,
                                              NORM_OPTIONS, build_dir = TRUE)
      org_practice_modes <- BuildOrgPracticeModes(org_practice_cfg, "nevertreated", outdir_ds, build_dir = TRUE)

      coeffs_all    <- list()
      coeffs_pc_all <- list()

      covars     <- unlist(lapply(org_practice_modes, function(x) x$continuous_covariate))
      covars_imp <- unlist(lapply(org_practice_modes, function(x) paste0(x$continuous_covariate, "_imp")))

      #######################################
      # Load precomputed PC scores from data_prep
      #######################################
      panel_pc_median <- read_parquet(file.path(INDIR_PC, dataset, rolling_panel, "panel_pc_median.parquet"))

      pca_groups <- lapply(names(pc_cfg), function(g) {
        col    <- paste0(g, "_pc1")
        scores <- panel_pc_median %>% distinct(repo_name, .data[[col]])
        list(
          repo_low       = scores %>% filter(.data[[col]] == 0) %>% pull(repo_name),
          repo_high      = scores %>% filter(.data[[col]] == 1) %>% pull(repo_name),
          friendly_label = pc_cfg[[g]]$friendly_label
        )
      })
      names(pca_groups) <- names(pc_cfg)

      valid_repos <- panel_pc_median %>%
        filter(if_any(ends_with("_pc1"), ~ !is.na(.))) %>%
        pull(repo_name) %>%
        unique()

      #######################################
      # Save PC loadings from precomputed metadata
      #######################################
      pc_outdir <- file.path(outdir_ds, "pc_event_studies")
      dir_create(pc_outdir, recurse = TRUE)

      pc_meta <- read.csv(file.path(INDIR_PC, dataset, rolling_panel, "pc1_metadata.csv"))
      pc_meta <- pc_meta %>% mutate(dataset = dataset, rolling = rolling_panel)
      SaveData(pc_meta,
               c("dataset", "rolling", "group", "var"),
               file.path(pc_outdir, paste0("pc1_variance_and_loadings_", rolling_panel, ".csv")),
               file.path(pc_outdir, paste0("pc1_variance_and_loadings_", rolling_panel, ".log")),
               sortbykey = FALSE)

      #######################################
      # Baseline outcome plots + collect coeffs
      #######################################
      metrics <- c("sa")

      for (outcome_mode in outcome_modes) {
        es_list <- lapply(metrics, function(m) {
          panel_sub <- panels[[outcome_mode$data]] %>% filter(repo_name %in% valid_repos)
          EventStudy(panel_sub, outcome_mode$outcome, outcome_mode$control_group,
                     m, title = "", normalize = outcome_mode$normalize)
        })
        png(outcome_mode$file)
        PlotEventStudyComparison(es_list, title = "", legend_labels = NULL, add_comparison = FALSE,
                                 ylim = YLIM_DEFAULT)
        dev.off()

        for (i in seq_along(es_list)) {
          res <- es_list[[i]]$results
          coeffs_all[[length(coeffs_all) + 1]] <- as_tibble(res, rownames = "event_time") %>%
            mutate(
              dataset     = dataset,
              rolling     = rolling_panel,
              category    = outcome_mode$category,
              outcome     = outcome_mode$outcome,
              normalize   = outcome_mode$normalize,
              method      = metrics[i],
              covar       = NA_character_,
              split_value = NA_real_
            )
        }
      }

      pairs <- list(c(1, 2), c(3, 4))
      for (p in pairs) {
        oms <- outcome_modes[p]

        es_list <- lapply(oms, function(item) {
          panel_sub <- panels[[item$data]] %>% filter(repo_name %in% valid_repos)
          EventStudy(panel_sub, item$outcome, item$control_group, metrics,
                     title = "", normalize = item$normalize)
        })

        legend_labels <- sapply(oms, function(item) FormatLabel(item$outcome))

        png(paste0(tools::file_path_sans_ext(oms[[1]]$file), "_vs_", basename(oms[[2]]$file)))
        PlotEventStudyComparison(es_list, title = "", legend_labels = legend_labels,
                                 add_comparison = FALSE, ylim = YLIM_DEFAULT)
        dev.off()
      }

      #######################################
      # Org practice splits
      #######################################
      metrics <- c("sa")

      for (practice_mode in org_practice_modes) {
        covar   <- practice_mode$continuous_covariate
        base_df <- panels[["nevertreated"]] %>% filter(repo_name %in% valid_repos)
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
          for (norm in NORM_OPTIONS) {
            norm_str   <- ifelse(norm, "_norm", "")
            combo_grid <- expand.grid(lapply(practice_mode$filters, `[[`, "vals"),
                                      KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)

            es_list <- apply(combo_grid, 1, function(vals_row) {
              df_sub <- base_df
              for (i in seq_along(vals_row)) {
                col_name <- practice_mode$filters[[i]]$col
                df_sub   <- df_sub %>% filter(.data[[col_name]] == vals_row[[i]])
              }
              tryCatch(
                EventStudy(df_sub, outcome_mode$outcome, practice_mode$control_group,
                           method = metrics, title = "", normalize = norm),
                error = function(e) {
                  message("WARN: EventStudy failed for ", covar, "/", outcome_mode$outcome,
                          ": ", e$message)
                  NULL
                }
              )
            }, simplify = FALSE)

            success_idx <- which(!sapply(es_list, is.null))
            es_list     <- es_list[success_idx]
            labels      <- practice_mode$legend_labels[success_idx]

            if (length(es_list) > 0) {
              out_path <- file.path(practice_mode$folder,
                                    paste0(outcome_mode$outcome, norm_str, ".png"))
              png(out_path)
              ylim         <- NULL
              legend_title <- practice_mode$legend_title
              if (covar %in% c("mean_has_assignee", "ov_sentiment_avg", "issue_share_only")) {
                ylim         <- YLIM_WIDE
                legend_title <- NULL
              }
              do.call(PlotEventStudyComparison, list(es_list,
                                                     legend_labels = labels,
                                                     legend_title  = legend_title,
                                                     title         = "",
                                                     ylim          = ylim))
              dev.off()

              for (j in seq_along(es_list)) {
                res       <- es_list[[j]]$results
                split_val <- practice_mode$filters[[1]]$vals[j]
                coeffs_all[[length(coeffs_all) + 1]] <- as_tibble(res, rownames = "event_time") %>%
                  mutate(
                    dataset     = dataset,
                    rolling     = rolling_panel,
                    category    = outcome_mode$category,
                    outcome     = outcome_mode$outcome,
                    normalize   = norm,
                    method      = metrics[1],
                    covar       = covar,
                    split_value = split_val
                  )
              }
            }
          }
        }
      }

      #######################################
      # PC-split event studies (precomputed groups)
      #######################################
      for (outcome_mode in outcome_modes) {
        outcome_local <- outcome_mode$outcome
        for (g in names(pca_groups)) {
          r <- pca_groups[[g]]
          panel_low  <- panels[[outcome_mode$data]] %>% filter(repo_name %in% r$repo_low)
          panel_high <- panels[[outcome_mode$data]] %>% filter(repo_name %in% r$repo_high)

          for (norm in NORM_OPTIONS) {
            es_low <- tryCatch(
              EventStudy(panel_low, outcome_local, outcome_mode$control_group,
                         "sa", title = paste0(r$friendly_label, " <= median"), normalize = norm),
              error = function(e) { message("WARN: EventStudy low failed (", g, "/", outcome_local, "): ", e$message); NULL }
            )
            es_high <- tryCatch(
              EventStudy(panel_high, outcome_local, outcome_mode$control_group,
                         "sa", title = paste0(r$friendly_label, " > median"), normalize = norm),
              error = function(e) { message("WARN: EventStudy high failed (", g, "/", outcome_local, "): ", e$message); NULL }
            )
            if (is.null(es_low) || is.null(es_high)) next

            out_png <- file.path(pc_outdir,
                                 paste0(g, "_", outcome_local, "_pc1_split", ifelse(norm, "_norm", ""), ".png"))
            png(out_png)
            PlotEventStudyComparison(list(es_low, es_high),
                                     legend_labels = c("Low", "High"),
                                     legend_title  = r$friendly_label,
                                     ylim          = c(-2.25, 1.5))
            dev.off()

            coeffs_pc_all[[length(coeffs_pc_all) + 1]] <- as_tibble(es_low$results, rownames = "event_time") %>%
              mutate(dataset = dataset, rolling = rolling_panel, category = outcome_mode$category,
                     outcome = outcome_local, normalize = norm, method = "sa",
                     covar = paste0(g, "_score"), split_value = "low")
            coeffs_pc_all[[length(coeffs_pc_all) + 1]] <- as_tibble(es_high$results, rownames = "event_time") %>%
              mutate(dataset = dataset, rolling = rolling_panel, category = outcome_mode$category,
                     outcome = outcome_local, normalize = norm, method = "sa",
                     covar = paste0(g, "_score"), split_value = "high")
          }
        }
      }

      if (length(coeffs_pc_all) > 0) {
        coeffs_pc_df <- bind_rows(coeffs_pc_all) %>% mutate(event_time = as.numeric(event_time)) %>% unique()
        SaveData(coeffs_pc_df,
                 c("dataset", "rolling", "category", "outcome", "normalize", "method", "covar", "split_value", "event_time"),
                 file.path(pc_outdir, paste0("pc1_split_coefficients_", rolling_panel, ".csv")),
                 file.path(pc_outdir, paste0("pc1_split_coefficients_", rolling_panel, ".log")),
                 sortbykey = FALSE)
      }

      #######################################
      # Export results
      #######################################
      coeffs_df <- lapply(coeffs_all, function(x) {
        x$event_time  <- as.numeric(x$event_time)
        x$split_value <- as.character(x$split_value)
        x
      }) |> bind_rows() %>%
        mutate(event_time  = as.numeric(event_time),
               covar       = ifelse(is.na(covar), "", covar),
               split_value = ifelse(is.na(split_value), "", split_value))
      SaveData(coeffs_df,
               c("rolling", "category", "outcome", "normalize", "method", "covar", "split_value", "event_time"),
               file.path(outdir_ds, paste0("all_coefficients_", rolling_panel, ".csv")),
               file.path(outdir_ds, paste0("all_coefficients_", rolling_panel, ".log")),
               sortbykey = FALSE)

      #######################################
      # Combine PNGs into one PDF
      #######################################
      png_files <- list.files(outdir_ds, pattern = "\\.png$", full.names = TRUE, recursive = TRUE)
      AggregatePngsToPdf(png_files, file.path(outdir_ds, paste0("all_results_", rolling_panel, ".pdf")))
    }
  }
}

Main()
