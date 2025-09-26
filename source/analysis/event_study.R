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
library(yaml)
library(fs)


#######################################
# 2. Normalization helpers
#######################################
norm_options <- c(TRUE)

NormalizeOutcome <- function(df, outcome) {
  outcome_norm <- paste(outcome, "norm", sep = "_")
  sd_outcome_var <- outcome
  
  df_norm <- df %>%
    group_by(repo_name) %>%
    mutate(
      mean_outcome = if_else(
        quasi_treatment_group == 0,
        mean(get(outcome), na.rm = TRUE),
        mean(get(outcome)[time_index < quasi_treatment_group & time_index >= (quasi_treatment_group - 5)], na.rm = TRUE)
      ),
      sd_outcome = if_else(
        quasi_treatment_group == 0,
        sd(get(sd_outcome_var), na.rm = TRUE),
        sd(get(sd_outcome_var)[time_index < quasi_treatment_group & time_index >= (quasi_treatment_group - 5)], na.rm = TRUE)
      )
    ) %>%
    ungroup()
  
  df_norm[[outcome_norm]] <- (df_norm[[outcome]] - df_norm$mean_outcome) / df_norm$sd_outcome
  df_norm[is.finite(df_norm[[outcome_norm]]), ]
}

BuildCommonSample <- function(df, outcomes) {
  valid_repos <- lapply(outcomes, function(outcome) {
    df_norm <- NormalizeOutcome(df, outcome)
    unique(df_norm$repo_name)
  })
  keep_repos <- Reduce(intersect, valid_repos)
  df %>% filter(repo_name %in% keep_repos)
}


#######################################
# 3. Estimation functions
#######################################
EventStudy <- function(df, outcome, control_group, method = c("cs", "sa", "es"), normalize = FALSE, title = NULL) {
  method <- match.arg(method)
  df_est <- if (normalize) NormalizeOutcome(df, outcome) else df %>% mutate("{outcome}_norm" := .data[[outcome]])
  yvar <- if (normalize) paste0(outcome, "_norm") else outcome
  
  res_mat <- switch(method,
                    cs = {
                      set.seed(420)
                      att <- att_gt(
                        yname = yvar, tname = "time_index", idname = "repo_id",
                        gname = "treatment_group", xformla = ~1, data = df_est,
                        control_group = control_group, allow_unbalanced_panel = TRUE,
                        clustervars = "repo_name", est_method = "dr"
                      )
                      est <- aggte(att, type = "dynamic", na.rm = TRUE) %>%
                        tidy() %>%
                        rename(sd = std.error, ci_low = conf.low, ci_high = conf.high) %>%
                        select(event.time, estimate, sd, ci_low, ci_high) %>%
                        mutate(event.time = as.numeric(event.time)) %>%
                        column_to_rownames("event.time") %>%
                        as.matrix()
                      est
                    },
                    sa = {
                      est_formula <- as.formula(sprintf("%s ~ sunab(treatment_group, time_index) | repo_name + time_index", yvar))
                      est_obj <- feols(est_formula, df_est, vcov = ~repo_name)
                      CleanFixEst(est_obj, "time_index::")
                    },
                    es = {
                      est <- eventstudyr::EventStudy(
                        estimator = "OLS", data = df_est, outcomevar = yvar,
                        policyvar = "treatment", idvar = "repo_name",
                        timevar = "time_index", post = 5, pre = 0
                      )$output %>%
                        tidy() %>%
                        rename(sd = std.error, ci_low = conf.low, ci_high = conf.high) %>%
                        select(term, estimate, sd, ci_low, ci_high) %>%
                        mutate(term = case_when(
                          str_detect(term, "fd_lead") ~ -as.numeric(str_extract(term, "\\d+$")),
                          str_detect(term, "lead") ~ -(as.numeric(str_extract(term, "\\d+$")) + 1),
                          str_detect(term, "lag") ~ as.numeric(str_extract(term, "\\d+$")),
                          TRUE ~ 0
                        )) %>%
                        column_to_rownames("term") %>%
                        as.matrix()
                      est
                    }
  )
  
  res_mat <- EnsureMinusOneRow(res_mat)
  res_mat[is.na(res_mat)] <- 0
  res_mat <- res_mat[order(as.numeric(rownames(res_mat))), , drop = FALSE]
  
  plot_obj <- fixest::coefplot(
    res_mat,
    main = title,
    xlab = "Event time (k)",
    keep = "^-[1-5]|[0-5]",
    drop = "[[:digit:]]{2}",
    ref.line = 0
  )
  
  list(plot = plot_obj, results = res_mat)
}


#######################################
# 4. Testing / diagnostics
#######################################
CleanFixEst <- function(est_obj, term_prefix) {
  est_obj %>%
    tidy() %>%
    mutate(term = as.numeric(str_remove(term, paste0("^", term_prefix))),
           sd = std.error) %>%
    select(-std.error, -statistic, -p.value) %>%
    mutate(ci_low = estimate - 1.96 * sd,
           ci_high = estimate + 1.96 * sd) %>%
    column_to_rownames("term") %>%
    as.matrix()
}

EnsureMinusOneRow <- function(est_mat) {
  if (!("-1" %in% rownames(est_mat))) {
    blank_row <- matrix(c(0, 0, 0, 0), nrow = 1)
    colnames(blank_row) <- colnames(est_mat)
    rownames(blank_row) <- "-1"
    est_mat <- rbind(est_mat, blank_row)
  }
  est_mat
}

TestPretrends <- function(res_mat, terms = as.character(-5:-1)) {
  available <- intersect(rownames(res_mat), terms)
  if (length(available) == 0) return(NA_real_)
  
  sorted_terms <- available[order(as.numeric(available))]
  estimates <- res_mat[sorted_terms, "estimate"]
  sds <- res_mat[sorted_terms, "sd"]
  
  keep_idx <- sds != 0 & !is.na(sds)
  if (all(!keep_idx)) return(NA_real_)
  
  estimates <- estimates[keep_idx]
  variances <- sds[keep_idx]^2
  sigma <- diag(variances, nrow = length(variances))
  
  tryCatch({
    wald.test(b = estimates, Sigma = sigma, Terms = seq_along(estimates))$result$chi2[["P"]]
  }, error = function(e) NA_real_)
}


#######################################
# 5. Comparison / plotting
#######################################
CompareES <- function(es_list,
                      legend_title = NULL,
                      legend_labels = NULL,
                      title = "",
                      add_p = TRUE,
                      ylim = NULL) {
  old_par <- par(no.readonly = TRUE)
  on.exit(par(old_par), add = TRUE)
  par(bg = "white")
  
  results <- lapply(es_list, `[[`, "results")
  results <- Filter(function(x) !is.null(x) && nrow(x) > 0, results)
  if (length(results) == 0) {
    warning("CompareES: no results to plot")
    return(invisible(NULL))
  }
  
  par(mar = c(8, 4, 2, 2) + 0.1)
  plot_args <- list(
    object = results,
    multi = TRUE,
    xlab = "Event time (k)",
    ylab = "",
    main = title,
    keep = "^-[1-5]|[0-5]",
    drop = "[[:digit:]]{2}",
    order = c("-5", "-4", "-3", "-2", "-1", "0", "1", "2", "3", "4", "5"),
    xaxt = "n",
    yaxt = "n",
    grid = FALSE
  )
  if (!is.null(ylim)) plot_args$ylim <- ylim
  do.call(fixest::coefplot, plot_args)
  
  if (!is.null(legend_labels)) {
    par(xpd = NA)
    legend("top", legend = legend_labels, title = legend_title,
           horiz = TRUE, bty = "o", box.lwd = 0.8, box.col = "grey40",
           bg = "white", xjust = 0.5,
           col = seq_along(results), pch = 20,
           lwd = 1, lty = seq_along(results))
    par(xpd = FALSE)
  }
  
  if (add_p) {
    if (length(results) == 2) {
      wp <- CompareEventCoefsWald(results, terms = 0:5)
      wald_lbl <- paste0("Wald test p-value: ", sprintf("%.3f", wp))
    } else if (length(results) > 2) {
      combos <- combn(length(results), 2)
      wald_lbl <- paste(
        apply(combos, 2, function(idx) {
          p_val <- CompareEventCoefsWald(results[idx], terms = 0:5)
          paste0(legend_labels[idx[1]], " vs ", legend_labels[idx[2]], ": p=", sprintf("%.3f", p_val))
        }),
        collapse = " | "
      )
    }
    
    pre_p <- vapply(results, TestPretrends, numeric(1))
    pre_strs <- ifelse(pre_p < 0.001,
                       "p-value < 0.001",
                       sprintf("p-value: %.3f", pre_p))
    pre_lbl <- paste(paste0("Pretrend (", legend_labels, ") ", pre_strs), collapse = "\n")
    if (length(results) == 1) {
      pre_lbl <- paste0("Pretrend p-value: ", pre_strs)
      wald_lbl <- ""
    }
    
    usr <- par("usr")
    x_left <- usr[1] + 0.02 * (usr[2] - usr[1])
    mtext(wald_lbl, side = 1, line = -1, at = x_left, adj = 0, cex = 0.8)
    mtext(pre_lbl, side = 1, line = -1.8, at = x_left, adj = 0, cex = 0.8)
  }
}

CompareEventCoefsWald <- function(tidy_list, terms = 0:5) {
  sel <- as.character(terms)
  m1 <- as.matrix(tidy_list[[1]])
  m2 <- as.matrix(tidy_list[[2]])
  available <- intersect(intersect(rownames(m1), rownames(m2)), sel)
  if (length(available) == 0) return(NA_real_)
  
  avail_sorted <- available[order(as.numeric(available))]
  if (length(avail_sorted) < 4) return(NA)
  
  t1 <- tidy_list[[1]][avail_sorted, ]
  t2 <- tidy_list[[2]][avail_sorted, ]
  delta <- t1[, "estimate"] - t2[, "estimate"]
  var_sum <- t1[, "sd"]^2 + t2[, "sd"]^2
  Sigma <- diag(var_sum, nrow = length(var_sum))
  
  tryCatch({
    wald.test(b = delta, Sigma = Sigma, Terms = seq_along(delta))$result$chi2[["P"]]
  }, error = function(e) NA)
}


#######################################
# 6. Sample restriction helpers
#######################################
CheckPreTreatmentThreshold <- function(df_panel_nyt, periods_count, outcome, count_thresh) {
  df_panel_nyt %>%
    filter(time_index >= (treatment_group - periods_count),
           time_index < treatment_group) %>%
    group_by(repo_name) %>%
    summarise(meets_threshold = all(.data[[outcome]] >= count_thresh), .groups = "drop") %>%
    filter(meets_threshold) %>% pull(repo_name)
}

HasMinPreTreatmentPeriods <- function(df_panel_nyt, periods_count) {
  projects <- df_panel_nyt %>%
    group_by(repo_name) %>%
    summarise(n_pre = sum(time_index < treatment_group), .groups = "drop") %>%
    filter(n_pre >= periods_count) %>% pull(repo_name)
  df_panel_nyt %>% filter(repo_name %in% projects)
}


#######################################
# 7. YAML readers and mode builders
#######################################
BuildOutcomeModes <- function(outcome_cfg, control_group, outdir_dataset) {
  expand <- function(cat, out, norm, control_group) list(
    outcome   = out,
    category  = cat,
    normalize = norm,
    control_group = control_group,
    data      = paste0("df_panel_", control_group),
    file      = file.path(outdir_dataset, control_group, cat,
                          paste0(out, if (norm) "_norm", ".png"))
  )
  
  modes <- lapply(names(outcome_cfg), function(cat) {
    outcomes <- outcome_cfg[[cat]]$main
    control_groups <- c(control_group)
    do.call(c, lapply(control_groups, function(control_group) {
      dir_create(file.path(outdir_dataset, control_group, cat))
      do.call(c, lapply(outcomes, function(out) {
        lapply(norm_options, function(norm) expand(cat, out, norm, control_group))
      }))
    }))
  })
  
  do.call(c, modes)
}

BuildOrgPracticeModes <- function(org_practice_cfg, control_group, outdir_dataset) {
  modes <- list()
  for (org_practice in names(org_practice_cfg)) {
    for (main_cat in names(org_practice_cfg[[org_practice]])) {
      for (sub_cat in names(org_practice_cfg[[org_practice]][[main_cat]])) {
        mains <- org_practice_cfg[[org_practice]][[main_cat]][[sub_cat]]$main
        for (outcome in mains) {
          folder <- file.path(outdir_dataset, control_group, org_practice, main_cat, sub_cat, outcome)
          dir_create(folder)
          modes <- append(modes, list(
            list(
              continuous_covariate   = outcome,
              filters                = list(list(col = paste0(outcome, "_2bin"), vals = c(1, 0))),
              legend_labels          = c("High", "Low"),
              legend_title           = paste(paste(org_practice, main_cat, sep = " -> "),
                                             paste(sub_cat, outcome, sep = " -> "), sep = "\n"),
              control_group          = control_group,
              data                   = paste0("df_panel_", control_group),
              folder                 = folder
            )
          ))
        }
      }
    }
  }
  modes
}


#######################################
# 8. Org practice bin helpers
#######################################
CreateOrgPracticeBin <- function(df, covar, past_periods = 1) {
  col_bin <- paste0(covar, "_2bin")
  
  df %>%
    group_by(repo_name) %>%
    mutate(
      pre_vals   = if_else(
        time_index < quasi_treatment_group & time_index >= (quasi_treatment_group - past_periods),
        .data[[covar]], NA_real_
      ),
      mean_pre   = mean(pre_vals, na.rm = TRUE),
      mean_other = mean(.data[[covar]], na.rm = TRUE),
      !!col_bin  := if_else(mean_pre > mean_other, 1, 0)
    ) %>%
    ungroup() %>%
    select(-pre_vals, -mean_pre, -mean_other)
}


#######################################
# 9. Main execution
#######################################
main <- function() {
  SEED <- 420
  set.seed(SEED)
  
  INDIR  <- "drive/output/derived/org_characteristics/org_panel"
  INDIR_YAML <- "source/derived/org_characteristics"
  OUTDIR <- "output/analysis/event_study"
  dir_create(OUTDIR)
  
  DATASETS <- c("important_thresh", "important_topk")
  exclude_outcomes <- c("num_downloads")
  
  outcome_cfg      <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))
  org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariate_organization.yaml"))
  
  metrics    <- c("cs", "sa")
  metrics_fn <- c("Callaway and Sant'anna 2020", "Sun and Abraham 2020")
  
  plan(multisession, workers = availableCores() - 1)
  
  for (dataset in DATASETS) {
    message("Processing dataset: ", dataset)
    outdir_dataset <- file.path(OUTDIR, dataset)
    dir_create(outdir_dataset)
    
    df_panel <- read_parquet(file.path(INDIR, dataset, "panel.parquet"))
    
    all_outcomes <- unlist(lapply(outcome_cfg, function(x) x$main))
    df_panel_common <- BuildCommonSample(df_panel, all_outcomes)
    
    df_panel_notyettreated <- df_panel_common %>% filter(num_departures == 1)
    df_panel_nevertreated  <- df_panel_common %>% filter(num_departures <= 1)
    
    assign("df_panel_notyettreated", df_panel_notyettreated, envir = .GlobalEnv)
    assign("df_panel_nevertreated",  df_panel_nevertreated,  envir = .GlobalEnv)
    
    outcome_modes      <- BuildOutcomeModes(outcome_cfg, "nevertreated", outdir_dataset)
    org_practice_modes <- BuildOrgPracticeModes(org_practice_cfg, "nevertreated", outdir_dataset)
    
    coeffs_all <- list()
    
    #######################################
    # PDF to collect all plots
    #######################################
    pdf(file.path(outdir_dataset, "all_results.pdf"))
    
    #######################################
    # Outcomes
    #######################################
    for (outcome_mode in outcome_modes) {
      es_list <- lapply(metrics, function(m) {
        panel <- get(outcome_mode$data)
        EventStudy(panel, outcome_mode$outcome, outcome_mode$control_group,
                   m, title = "", normalize = outcome_mode$normalize)
      })
      
      # PNG
      png(outcome_mode$file)
      CompareES(es_list, title = outcome_mode$outcome, legend_labels = metrics_fn)
      dev.off()
      
      # PDF
      CompareES(es_list, title = outcome_mode$outcome, legend_labels = metrics_fn)
      
      # Collect coefficients
      for (i in seq_along(es_list)) {
        res <- es_list[[i]]$results
        coeffs_all[[length(coeffs_all) + 1]] <- as_tibble(res, rownames = "event_time") %>%
          mutate(
            dataset = dataset,
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
    # Org practice splits
    #######################################
    df_bin_change <- data.frame()
    
    for (practice_mode in org_practice_modes) {
      covar <- practice_mode$continuous_covariate
      base_df <- get(practice_mode$data)
      
      # create the _2bin column for this covariate
      base_df <- CreateOrgPracticeBin(base_df, covar, past_periods = 5)
      df_bin_change <- rbind(df_bin_change,
                             CompareBinsFast(base_df, covar, 5, 1) %>%
                               mutate(covar = covar, dataset = dataset))
      
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
            do.call(CompareES, list(es_list,
                                    legend_labels = labels,
                                    legend_title  = practice_mode$legend_title,
                                    title = outcome_mode$outcome))
            dev.off()
            
            # PDF
            do.call(CompareES, list(es_list,
                                    legend_labels = labels,
                                    legend_title  = practice_mode$legend_title,
                                    title = outcome_mode$outcome))
            
            # Collect coefficients
            for (j in seq_along(es_list)) {
              res <- es_list[[j]]$results
              split_val <- practice_mode$filters[[1]]$vals[j]
              coeffs_all[[length(coeffs_all) + 1]] <- as_tibble(res, rownames = "event_time") %>%
                mutate(
                  dataset = dataset,
                  category = outcome_mode$category,
                  outcome = outcome_mode$outcome,
                  normalize = norm,
                  method = metrics[1],   # split uses cs
                  covar = covar,
                  split_value = split_val
                )
            }
          }
        }
      }
    }
    
    dev.off()  # close dataset PDF
    
    #######################################
    # Export results
    #######################################
    coeffs_df <- bind_rows(coeffs_all) %>%
      mutate(event_time = as.numeric(event_time))
    write_csv(coeffs_df, file.path(outdir_dataset, "all_coefficients.csv"))
    
    write_csv(df_bin_change, file.path(outdir_dataset, "bin_change.csv"))
  }
}

#######################################
# Run
#######################################
if (sys.nframe() == 0) {
  main()
}
