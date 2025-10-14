#######################################
# 1. Libraries & Sources
#######################################
library(policytree)
library(gridExtra)
library(tidyverse)
library(grf)
library(arrow)
library(yaml)
library(fs)
library(ggrepel)
library(binsreg)
library(RColorBrewer)

source("source/lib/helpers.R")
source("source/analysis/causal_forest_helpers.R")

#######################################
# 2. Global Settings
#######################################
SEED <- 420
set.seed(SEED)

INDIR       <- "drive/output/derived/org_characteristics/org_panel"
OUTDIR      <- "output/analysis/causal_forest_event_study"
INDIR_YAML  <- "source/derived/org_characteristics"

dir_create(OUTDIR)

#######################################
# 3. Helper Functions
#######################################
CalculatePropensityScores <- function(model, df_data, method, X_keep, Y) {
  if (method == "lm_forest") {
    W <- model.matrix(~ treatment_arm - 1, data = df_data)
    W <- W[, setdiff(colnames(W), colnames(W)[grepl("never-treated", colnames(W))])]
  } else if (method == "multi_arm") {
    W <- df_data$treatment_arm
  } else if (method == "lm_forest_nonlinear") {
    W_treat <- model.matrix(~ treatment_arm - 1, data = df_data)
    W_treat <- W_treat[, setdiff(colnames(W_treat), colnames(W_treat)[grepl("never-treated", colnames(W_treat))])]
    W_time <- model.matrix(~ factor(time_index) - 1, data = df_data)
    W <- cbind(W_treat, W_time)
  } else {
    stop("Unknown method: ", method)
  }
  
  mc_pred <- predict(model, drop = TRUE)
  tau_hat <- mc_pred$predictions
  if (method == "multi_arm") {
    colnames(tau_hat) <- gsub(" - never-treated", "", colnames(tau_hat))
  }
  
  n <- nrow(tau_hat)
  k <- ncol(tau_hat)
  
  treatment_residual <- W - model$W.hat
  outcome_residual   <- matrix(Y - model$Y.hat, nrow = n, ncol = k, byrow = FALSE)
  scaling_factor     <- model$W.hat * (1 - model$W.hat)
  
  prop_score <- tau_hat + (treatment_residual / scaling_factor) * outcome_residual
  prop_score_low_bound <- quantile(prop_score, .01, na.rm = T)
  prop_score_high_bound <- quantile(prop_score, .99, na.rm = T)
  prop_score[prop_score>prop_score_high_bound | prop_score < prop_score_low_bound] <- NA
  prop_score
}

# ---- Plotting ----
PlotBinsregEventStudy <- function(df, x_var, y_var, event_var,
                                  outcome = NULL, category = NULL,
                                  var_rank = NULL, var_imp = NULL,
                                  fill_alpha = .3, point_shape = 16,
                                  out_file = NULL) {
  df <- df %>%
    mutate(z_x = scale(.data[[x_var]])[, 1],
           "{event_var}" := factor(.data[[event_var]], levels = -5:5)) %>%
    filter(z_x >= -5 & z_x <= 5) %>%
    filter(event_time != -1)
  
  event_colors <- c("-5" = "lavenderblush3", "-4" = "darkorange", "-3" = "forestgreen",
                    "-2" = "maroon", "0"  = "khaki", "1"  = "sienna", "2"  = "steelblue",
                    "3"  = "brown", "4"  = "gold", "5"  = "navy")
  
  b <- binsreg(
    y = df[[y_var]], x = df$z_x,
    cb = TRUE, ci = TRUE,
    nsims = 4000, simsgrid = 100, randcut = 1,
    by = df[[event_var]], vce = "HC1",
    bycolors = unname(event_colors),
    bysymbols = point_shape, legendTitle = "event time",
  )
  
  # Check if *all* groups are missing CI
  all_missing_ci <- all(
    vapply(b$data.plot, function(g) is.null(g$data.ci), logical(1))
  )
  
  if (all_missing_ci) {
    message("All groups missing CI, refitting with nbins=1.")
    b <- binsreg(
      y = df[[y_var]], x = df$z_x,
      cb = TRUE, ci = TRUE,
      nbins = 1,
      nsims = 4000, simsgrid = 100, randcut = 1,
      by = df[[event_var]], vce = "HC1",
      bycolors = unname(event_colors),
      bysymbols = point_shape, legendTitle = "event time"
    )
  }
  
  
  # Summaries (event-level regression)
  reg_summaries <- df %>%
    group_by(.data[[event_var]]) %>%
    group_modify(~ {
      fit <- tryCatch(
        lm(reformulate("z_x", response = y_var), data = .x),
        error = function(e) NULL
      )
      if (!is.null(fit)) {
        coef_summary <- summary(fit)$coefficients
        if ("z_x" %in% rownames(coef_summary)) {
          tibble(coef = coef_summary["z_x", 1],
                 se   = coef_summary["z_x", 2])
        } else tibble(coef = NA_real_, se = NA_real_)
      } else tibble(coef = NA_real_, se = NA_real_)
    }) %>%
    ungroup() %>%
    mutate(
      level = as.character(.data[[event_var]]),
      legend_label = ifelse(
        !is.na(coef),
        sprintf("%s\nβ=%.3f, se=%.3f", level, coef, se),
        level
      )
    )
  
  legend_labels <- setNames(reg_summaries$legend_label, reg_summaries$level)
  
  # Build pathway-style title
  title_text <- paste0(
    "Binscatter of Event-study Coefficients\n",
    "Outcome: ", outcome, "\n",
    category,    "\n",
    if (!is.null(var_rank) | !is.null(var_imp)) {
      paste0(
        if (!is.null(var_rank)) paste0("Rank: ", var_rank) else "",
        if (!is.null(var_rank) & !is.null(var_imp)) ", " else "",
        if (!is.null(var_imp)) paste0("Importance: ", signif(var_imp, 3)) else ""
      )
    } else ""
  )
  
  p <- b$bins_plot +
    scale_color_manual(values = event_colors,
                       breaks = names(legend_labels),
                       labels = legend_labels,
                       name = "event time") +
    scale_fill_manual(values = scales::alpha(event_colors, fill_alpha),
                      breaks = names(legend_labels),
                      labels = legend_labels,
                      name = "event time") +
    labs(title = title_text,
         x = paste0(x_var, " (standardized, |z| ≤ 5)"),
         y = y_var) +
    theme_minimal(base_size = 12) +
    theme(
      legend.position = "right",
      legend.text = element_text(size = 8),     # smaller font
      legend.title = element_text(size = 9)     # slightly smaller title too
    )
  
  if (!is.null(out_file)) ggsave(filename = out_file, plot = p, width = 7, height = 5)
  p
}

#######################################
# 4. Core Analysis
#######################################
RunForestEventStudy <- function(outcome, df_panel_common, org_practice_modes,
                                outdir, outdir_ds, method,
                                rolling_period,
                                top = "all",
                                use_existing = FALSE) {
  mode_suffix <- if (top == "all") "all" else paste0("top", top)
  message("Running forest event study: ", outcome, 
          " (", method, ", rolling", rolling_period, ", ", mode_suffix, ")")
  
  if (method == "lm_forest") {
    df_panel_outcome <- ResidualizeOutcome(df_panel_common, outcome)
  } else if (method == "lm_forest_nonlinear") {
    df_panel_outcome <- NormalizeOutcome(df_panel_common, outcome)
    norm_outcome_col <- paste(outcome, "norm", sep = "_")
    df_panel_outcome <- FirstDifferenceOutcome(df_panel_outcome, norm_outcome_col)
    df_panel_outcome$resid_outcome <- df_panel_outcome$fd_outcome
  } 
  
  covars     <- unlist(lapply(org_practice_modes, \(x) x$continuous_covariate))
  covars_imp <- unlist(lapply(org_practice_modes, function(x) {
    if (!str_detect(x$legend_title, "organizational_routines")) paste0(x$continuous_covariate, "_imp")
  }))
  
  df_data <- ComputeCovariateMeans(df_panel_outcome, c(covars, covars_imp), rolling_period)  %>%
    filter(quasi_event_time != -1) %>%
    mutate(treatment_arm = factor(ifelse(treatment_group == 0, "never-treated",
                                         paste0("cohort", treatment_group, "event_time", quasi_event_time))),
           treatment_arm = relevel(treatment_arm, ref = "never-treated"))
  
  Y <- df_data$resid_outcome
  X <- as.matrix(df_data %>% select(all_of(paste0(covars, "_mean"))))
  
  set.seed(SEED)
  Y_reg_forest <- df_data %>% filter(quasi_event_time >= 0) %>% pull(resid_outcome)
  X_reg_forest <- as.matrix(df_data %>% filter(quasi_event_time >= 0) %>% 
                              select(all_of(paste0(covars, "_mean"))))
  
  if (top == "all") {
    X_keep <- X[, paste0(covars, "_mean"), drop = FALSE]
  } else {
    varimp.Y <- variable_importance(regression_forest(X_reg_forest, Y_reg_forest))
    ranked_vars <- colnames(X)[order(varimp.Y, decreasing = TRUE)]
    keep <- ranked_vars[!duplicated(sub("_imp_mean$", "_mean", ranked_vars))][1:top]
    X_keep <- X[, keep, drop = FALSE]
  }
  
  outfile <- file.path(outdir_ds, paste0(method, "_", outcome, "_rolling", rolling_period, "_", mode_suffix, ".rds"))
  model <- FitForestModel(df_data, X_keep, Y, method, outfile, use_existing)
  
  tau_hat <- predict(model, drop = TRUE)$predictions
  if (method == "multi_arm") {
    colnames(tau_hat) <- gsub(" - never-treated", "", colnames(tau_hat))
  }
  
  prop_scores <- CalculatePropensityScores(model, df_data, method, X_keep, Y)
  df_es_coef  <- AggregateEventStudy(tau_hat, df_data, max(df_data$time_index, na.rm = TRUE),
                                     "event_time")
  df_es_coefs <- cbind(df_data %>% select(repo_name, time_period, treatment_group,
                                          quasi_treatment_group, time_index),
                       df_es_coef)
  
  keep_idx <- df_es_coefs$treatment_group > 0
  df_long_treated <- df_es_coefs[keep_idx, ] %>%
    cbind(X_keep[keep_idx, , drop = FALSE]) %>%
    pivot_longer(
      cols = starts_with("event_time"),
      names_to = "event_time",
      names_prefix = "event_time",
      values_to = "coef"
    ) %>%
    mutate(event_time = as.integer(event_time)) %>%
    distinct(repo_name, treatment_group, event_time, .keep_all = TRUE)
  
  df_es_prop_att  <- AggregateEventStudy(
    prop_scores, df_data, max(df_data$time_index, na.rm = TRUE), "att") %>%
    rename(prop_scores_att = att)
  df_es_att <- cbind(df_data %>% select(repo_name, time_period, treatment_group,
                                        quasi_treatment_group, time_index),
                     df_es_prop_att)

  keep_idx <- df_es_att$treatment_group > 0
  df_long_att <- df_es_att[keep_idx, ] %>%
    cbind(X_keep[keep_idx, , drop = FALSE])
  dr.rewards <- cbind(
    control = df_long_att %>% pull(prop_scores_att) * -1,
    treat   = df_long_att %>% pull(prop_scores_att) - mean(df_long_att$prop_scores_att)
  )
  policy_tree_covars <- df_long_att %>% select(colnames(X_keep))
  subset <- complete.cases(policy_tree_covars)
  tree <- policy_tree(policy_tree_covars[subset,], dr.rewards[subset,], min.node.size = 200)

  # Save policy tree object
  outfile_tree <- file.path(outdir_ds, paste0(method, "_", outcome, "_policytree_rolling", rolling, ".rds"))
  saveRDS(tree, outfile_tree)

  # Save policy tree plot
  dir_create(file.path(outdir, method))
  outfile_tree_plot <- file.path(outdir, method, paste0("policytree_", outcome, "_", method, "_", mode_suffix, ".png"))
  png(outfile_tree_plot, width = 800, height = 600)
  plot(tree, leaf.labels = c("dont treat", "treat"))
  dev.off()

  pi.hat.eval <- predict(tree, policy_tree_covars[subset,]) - 1
  mean((df_long_att %>% pull(prop_scores_att))[subset][pi.hat.eval==1])
  mean((df_long_att %>% pull(prop_scores_att))[subset][pi.hat.eval==0])

  # Variable importance
  varimp <- variable_importance(model)
  ranked <- order(varimp, decreasing = TRUE)
  
  varimp_df <- tibble(
    variable = colnames(X_keep)[ranked],
    importance = varimp[ranked],
    rank = seq_along(ranked),
    outcome = outcome
  )
  
  plot_list <- list()
  
  for (i in seq_along(ranked)) {
    v <- colnames(X_keep)[ranked[i]]
    imp <- varimp[ranked[i]]
    
    matched_cat <- NULL
    for (m in org_practice_modes) {
      if (v %in% paste0(m$continuous_covariate, "_mean")) {
        matched_cat <- m$legend_title
        break
      }
    }
    
    dir_create(file.path(outdir, method))
    outfile_bins_png <- file.path(outdir, method, paste0("binscatter_", v, "_", method, "_", outcome, ".png"))
    
    p <- tryCatch(
      {
        PlotBinsregEventStudy(df_long_treated,
                              x_var     = v,
                              y_var     = "coef",
                              event_var = "event_time",
                              outcome   = paste0(outcome, " (rolling", rolling_period, ", ", mode_suffix, ")"),
                              category  = matched_cat,
                              var_rank  = i,
                              var_imp   = imp,
                              out_file  = outfile_bins_png)
      },
      error = function(e) {
        message("Plotting failed for ", v, " (", outcome, "): ", e$message)
        NULL
      }
    )
    
    plot_list[[paste0(v, "_", outcome)]] <- p
  }
  
  return(list(varimp_df = varimp_df, plots = plot_list))
}

#######################################
# 5. Main Entry Point
#######################################
outcome_cfg <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))
org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariate_organization.yaml"))

# Define which outcomes to run
OUTCOME_LIST <- c("pull_request_opened", "pull_request_merged",
                  "major_minor_release_count", "overall_new_release_count")

# Loop over both important_topk and important_thresh
for (variant in c("important_thresh")) {
  outdir_outcome_ds <- file.path(OUTDIR_DS, variant)
  
  df_panel <- read_parquet(file.path(INDIR, variant, "panel.parquet"))
  df_panel_common <- BuildCommonSample(df_panel, OUTCOME_LIST) %>%
    filter(num_departures <= 1) %>%
    mutate(quasi_event_time = time_index - quasi_treatment_group)
  
  org_practice_modes <- BuildOrgPracticeModes(
    org_practice_cfg,
    "nevertreated",
    file.path(OUTDIR, variant)
  )
  
  for (method in c("lm_forest", "multi_arm")) {
    for (outcome in OUTCOME_LIST) {
      outdir_outcome <- file.path(OUTDIR, variant, outcome)
      RunForestEventStudy(outcome, df_panel_common, org_practice_modes, outdir_outcome, 
                          outdir_outcome_ds, method = method, use_existing = TRUE)
    }
  }
}
