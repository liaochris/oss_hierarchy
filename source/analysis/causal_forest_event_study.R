#######################################
# 1. Libraries
#######################################
library(tidyverse)
library(grf)
library(arrow)
library(yaml)
library(fs)
library(ggrepel)

source("source/lib/helpers.R")

#######################################
# 2. Global settings
#######################################
SEED <- 420
set.seed(SEED)

INDIR  <- "drive/output/derived/org_characteristics/org_panel"

OUTDIR <- "output/derived/analysis/causal_forest_event_study"
dir_create(file.path(OUTDIR))

#######################################
# 3. Helper functions
#######################################

ResidualizeOutcome <- function(df, outcome) {
  df_norm <- NormalizeOutcome(df, outcome)
  norm_outcome_col <- paste(outcome, "norm", sep = "_")
  
  df_norm <- df_norm %>%
    group_by(repo_name) %>%
    mutate(baseline = ifelse(quasi_event_time == -1, !!sym(norm_outcome_col), NA)) %>%
    fill(baseline, .direction = "downup") %>%
    mutate(fd_outcome = !!sym(norm_outcome_col) - baseline) %>%
    ungroup() %>%
    filter(quasi_event_time != -1 & !is.na(fd_outcome))
  
  avg_adj <- df_norm %>%
    filter(treatment_group == 0) %>%
    group_by(quasi_treatment_group, quasi_event_time) %>%
    summarise(avg_fd = mean(fd_outcome, na.rm = TRUE), .groups = "drop")
  
  df_norm <- df_norm %>%
    left_join(avg_adj, by = c("quasi_treatment_group", "quasi_event_time")) %>%
    mutate(resid_outcome = fd_outcome - avg_fd) %>%
    select(-baseline, -avg_fd, -fd_outcome)
  
  df_norm
}

ComputeCovariateMeans <- function(df, covars) {
  means <- df %>%
    filter(quasi_event_time >= -5 & quasi_event_time < -1) %>%
    group_by(repo_name) %>%
    summarise(across(all_of(covars), ~ mean(.x, na.rm = TRUE),
                     .names = "{.col}_mean"),
              .groups = "drop")
  
  df %>%
    filter(quasi_event_time >= -5 & quasi_event_time <= 5) %>%
    left_join(means, by = "repo_name")
}

AggregateEventStudyObs <- function(tau_hat, df, max_time) {
  arm_info <- data.frame(
    arm    = colnames(tau_hat),
    cohort = as.integer(sub("cohort(\\d+).*", "\\1", colnames(tau_hat))),
    e      = ifelse(grepl("event_time[-\\.]", colnames(tau_hat)),
                    -as.integer(sub(".*event_time[-\\.](\\d+)", "\\1", colnames(tau_hat))),
                    as.integer(sub(".*event_time(\\d+)", "\\1", colnames(tau_hat))))
  )
  
  cohort_probs <- df %>%
    filter(quasi_treatment_group > 0) %>%
    count(quasi_treatment_group, name = "n") %>%
    mutate(p_g = n / sum(n))
  
  est_list <- lapply(sort(unique(arm_info$e)), function(ev) {
    valid <- subset(arm_info, e == ev & cohort + ev <= max_time)
    if (!nrow(valid)) return(NULL)
    probs <- filter(cohort_probs, quasi_treatment_group %in% valid$cohort) %>%
      mutate(p_cond = p_g / sum(p_g))
    tau_mat <- vapply(valid$cohort, function(g) {
      w_g <- probs$p_cond[probs$quasi_treatment_group == g]
      tau_hat[, valid$arm[valid$cohort == g]] * w_g
    }, FUN.VALUE = numeric(nrow(tau_hat)))
    if (is.null(dim(tau_mat))) tau_mat <- matrix(tau_mat, ncol = 1)
    rowSums(tau_mat)
  })
  
  out <- as.data.frame(do.call(cbind, est_list))
  colnames(out) <- paste0("event_time", sort(unique(arm_info$e)))
  cbind(obs_id = seq_len(nrow(out)), out)
}

#######################################
# 4. Main function for one outcome
#######################################
RunForestEventStudy <- function(outcome, df_panel_common, org_practice_modes, outdir,
                                outdir_ds, method = c("lm_forest", "multi_arm"),
                                use_existing = FALSE) {
  method <- match.arg(method)
  message("Running forest event study for outcome: ", outcome, " using method: ", method)
  
  # Residualize outcome
  df_panel_outcome <- ResidualizeOutcome(df_panel_common, outcome)
  
  # Build covariate sets
  covars <- paste0(unlist(lapply(org_practice_modes, \(x) x$continuous_covariate)))
  covars_imp <- paste0(
    unlist(lapply(org_practice_modes, function(x) {
      if (!str_detect(x$legend_title, "organizational_routines")) x$continuous_covariate else NULL
    })), "_imp"
  )
  
  df_data <- ComputeCovariateMeans(df_panel_outcome, c(covars, covars_imp)) %>%
    mutate(treatment_arm = factor(ifelse(treatment_group == 0, 
                                         "never-treated", 
                                         paste0("cohort", treatment_group, "event_time", quasi_event_time)))) %>%
    mutate(treatment_arm = relevel(treatment_arm, ref = "never-treated"))
  
  covars_mean <- paste0(covars, "_mean")
  covars_mean_imp <- paste0(covars_imp, "_mean")
  
  # Build Y, X
  Y <- df_data %>% pull(resid_outcome)
  X <- df_data %>% select(all_of(c(covars_mean, covars_mean_imp))) %>% as.matrix()
  
  # Variable selection via regression forest
  Y.forest <- regression_forest(X, Y)
  varimp.Y <- variable_importance(Y.forest)
  ranked_vars <- colnames(X)[order(varimp.Y, decreasing = TRUE)]
  strip_imp <- \(v) sub("_imp_mean$", "_mean", v)
  keep <- ranked_vars[!duplicated(strip_imp(ranked_vars))][1:15]
  X_keep <- X[, keep]
  
  # 🔹 Define output file
  outfile <- file.path(outdir_ds, paste0(method, "_", outcome, ".rds"))
  
  # 🔹 Load or fit model
  if (file.exists(outfile) && use_existing) {
    message("Loading existing model: ", outfile)
    model <- readRDS(outfile)
  } else {
    message("Fitting new model: ", outfile)
    system.time({
      if (method == "lm_forest") {
        W <- model.matrix(~ treatment_arm - 1, data = df_data)
        colnames(W) <- sub("^treatment_arm", "", colnames(W))
        W <- W[, setdiff(colnames(W), "never-treated")]
        
        model <- lm_forest(
          X = X_keep, Y = Y, W = W,
          clusters = df_data %>% pull(repo_id)
        )
        
      } else if (method == "multi_arm") {
        W <- df_data$treatment_arm
        
        model <- multi_arm_causal_forest(
          X = X_keep, Y = Y, W = W,
          clusters = df_data %>% pull(repo_id)
        )
      }
    })
    
    dir_create(outdir_ds)
    saveRDS(model, file = outfile)
  }
  # Predict treatment effects
  mc.pred <- predict(model, drop = TRUE)
  tau_hat <- mc.pred$predictions
  if (method == "multi_arm") {
    colnames(tau_hat) <- gsub(" - never-treated", "", colnames(tau_hat))
  }
  
  # Aggregate event study observations
  df_es_coefs <- cbind(
    df_data %>%
      select(repo_name, time_period, treatment_group, quasi_treatment_group, time_index),
    AggregateEventStudyObs(tau_hat, df_data, max_time = max(df_data$time_index, na.rm = TRUE))
  )
  
  keep_idx <- df_es_coefs$treatment_group > 0
  df_long <- cbind(df_es_coefs[keep_idx, ], X_keep[keep_idx, , drop = FALSE]) %>%
    select(all_of(c(colnames(X_keep), "repo_name", "treatment_group")), starts_with("event_time")) %>%
    pivot_longer(cols = starts_with("event_time"),
                 names_to = "event_time", values_to = "coef") %>%
    mutate(event_time = as.integer(sub("event_time", "", event_time)))
  
  df_long_treated <- df_long %>% filter(treatment_group != 0)
  
  # Variable importance (forest level)
  varimp <- variable_importance(model)
  ranked.vars <- order(varimp, decreasing = TRUE)
  vars <- colnames(X_keep)[ranked.vars][1:10]
  
  # Save plots
  for (v in vars) {
    line_ends <- df_long_treated %>%
      filter(!is.na(.data[[v]])) %>%
      group_by(event_time) %>%
      group_modify(~{
        df <- .x %>% transmute(
          x = scale(.data[[v]], center = TRUE, scale = TRUE)[, 1],
          y = coef
        )
        m <- lm(y ~ x, data = df)
        max_x <- max(df$x, na.rm = TRUE)
        tibble(
          event_time = unique(.x$event_time),
          max_x      = max_x,
          fit_y      = predict(m, newdata = data.frame(x = max_x)),
          coef       = coef(summary(m))[2, 1],
          se         = coef(summary(m))[2, 2]
        )
      }) %>% ungroup()
    
    lbls <- line_ends %>%
      mutate(lbl = paste0(event_time,
                          " (β=", sprintf("%.2f", coef),
                          ", se=", sprintf("%.2f", se), ")")) %>%
      select(event_time, lbl) %>%
      deframe()
    
    p <- ggplot(df_long_treated, aes(x = scale(.data[[v]])[, 1], y = coef, color = factor(event_time))) +
      geom_point(alpha = 0.2) +
      geom_smooth(method = "lm", formula = y ~ x, se = TRUE) +
      geom_text_repel(data = line_ends,
                      aes(x = max_x, y = fit_y, label = event_time, color = factor(event_time)),
                      show.legend = FALSE, nudge_x = 0.2, size = 3) +
      scale_color_discrete(name = "event_time", labels = lbls) +
      labs(title = paste("Event-study coefficients vs", v, "\noutcome:", outcome),
           x = paste0(v, " (standardized)"),
           y = "Coefficient (raw units)") +
      theme_minimal()
    
    dir_create(outdir)
    ggsave(filename = file.path(outdir, paste0("plot_", v,"_",method, ".png")), 
           plot = p, width = 7, height = 5)
  }
}


#######################################
# 5. Example: Run for 4 outcomes
#######################################

INDIR_YAML <- "source/derived/org_characteristics"
OUTDIR <- "output/analysis/causal_forest_event_study"
OUTDIR_DS <- "drive/output/analysis/causal_forest_event_study"
outcome_cfg <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))
org_practice_cfg <- yaml.load_file(file.path(INDIR_YAML, "covariate_organization.yaml"))

# Define which outcomes to run
OUTCOME_LIST <- c("pull_request_opened", "pull_request_merged",
                  "major_minor_release_count", "overall_new_release_count")

# Loop over both important_topk and important_thresh
for (variant in c("important_topk")) {
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
