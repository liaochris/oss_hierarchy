library(dplyr)        
library(tidyr)        
library(broom)        
library(fixest)       
library(did)          
library(eventstudyr)  
library(aod)     
library(stringr) 
library(tibble)  
library(purrr)  

EventStudy <- function(df, outcome, control_group, method = c("cs", "sa", "es"), normalize = FALSE, title = NULL) {
  method <- match.arg(method)
  df_est <- if (normalize) NormalizeOutcome(df, outcome) else df %>% mutate("{outcome}_norm" := .data[[outcome]])
  yvar <- if (normalize) paste0(outcome, "_norm") else outcome
  
  # ensure title is a character scalar for coefplot
  title <- if (is.null(title)) "" else as.character(title)
  
  ref_p <- -1
  
  res <- switch(method,
                cs = {
                  set.seed(420)
                  att <- att_gt(
                    yname = yvar, tname = "time_index", idname = "repo_id",
                    gname = "treatment_group", xformla = ~1, data = df_est,
                    control_group = control_group, allow_unbalanced_panel = TRUE,
                    clustervars = "repo_name", est_method = "dr"
                  )
                  dyn <- aggte(att, type = "dynamic", na.rm = TRUE) %>%
                    broom::tidy() %>%
                    dplyr::rename(sd = std.error, ci_low = conf.low, ci_high = conf.high) %>%
                    dplyr::select(event.time, estimate, sd, ci_low, ci_high) %>%
                    dplyr::mutate(event.time = as.numeric(event.time)) %>%
                    tibble::column_to_rownames("event.time") %>%
                    as.matrix()
                  list(results = dyn, att_overall = NULL)
                },
                sa = {
                  est_formula <- as.formula(sprintf("%s ~ sunab(treatment_group, time_index, ref.p=%d) | repo_name + time_index", yvar, ref_p))
                  est_obj <- feols(est_formula, df_est, vcov = ~repo_name)
                  
                  dyn_mat <- CleanFixEst(est_obj, "time_index::")
                  
                  att_overall_tbl <- tryCatch({
                    agg_obj <- suppressWarnings(aggregate(est_obj, agg = "ATT"))
                    # try broom::tidy first, but avoid calling tidy on numeric
                    tidy_try <- tryCatch(broom::tidy(agg_obj), error = function(e) NULL)
                    if (!is.null(tidy_try) && all(c("estimate", "std.error", "conf.low", "conf.high") %in% colnames(tidy_try))) {
                      tib <- tidy_try
                      tib <- tib[1, , drop = FALSE]
                      tib %>% dplyr::transmute(estimate = estimate, sd = std.error, ci_low = conf.low, ci_high = conf.high)
                    } else {
                      # fallback: try to coerce agg_obj to data.frame / list and extract named elements
                      df_try <- tryCatch(as.data.frame(agg_obj), error = function(e) NULL)
                      if (!is.null(df_try) && all(c("estimate", "std.error", "conf.low", "conf.high") %in% colnames(df_try))) {
                        tib <- df_try
                        tib <- tib[1, , drop = FALSE]
                        tib %>% dplyr::transmute(estimate = estimate, sd = std.error, ci_low = conf.low, ci_high = conf.high)
                      } else if (is.list(agg_obj) && all(c("estimate", "std.error", "conf.low", "conf.high") %in% names(agg_obj))) {
                        tib <- tibble::tibble(estimate = agg_obj$estimate, sd = agg_obj$std.error, ci_low = agg_obj$conf.low, ci_high = agg_obj$conf.high)
                        tib[1, , drop = FALSE]
                      } else {
                        NULL
                      }
                    }
                  }, error = function(e) NULL)
                  list(results = dyn_mat, att_overall = att_overall_tbl)
                },
                es = {
                  est <- eventstudyr::EventStudy(
                    estimator = "OLS", data = df_est, outcomevar = yvar,
                    policyvar = "treatment", idvar = "repo_name",
                    timevar = "time_index", post = 5, pre = 0
                  )$output %>%
                    broom::tidy() %>%
                    dplyr::rename(sd = std.error, ci_low = conf.low, ci_high = conf.high) %>%
                    dplyr::select(term, estimate, sd, ci_low, ci_high) %>%
                    dplyr::mutate(term = dplyr::case_when(
                      stringr::str_detect(term, "fd_lead") ~ -as.numeric(stringr::str_extract(term, "\\d+$")),
                      stringr::str_detect(term, "lead") ~ -(as.numeric(stringr::str_extract(term, "\\d+$")) + 1),
                      stringr::str_detect(term, "lag") ~ as.numeric(stringr::str_extract(term, "\\d+$")),
                      TRUE ~ 0
                    )) %>%
                    tibble::column_to_rownames("term") %>%
                    as.matrix()
                  list(results = est, att_overall = NULL)
                }
  )
  
  dyn_mat <- res$results
  dyn_mat <- EnsureMinusOneRow(dyn_mat, ref_p)
  dyn_mat[is.na(dyn_mat)] <- 0
  dyn_mat <- dyn_mat[order(as.numeric(rownames(dyn_mat))), , drop = FALSE]
  
  plot_obj <- fixest::coefplot(
    dyn_mat,
    main = title,
    xlab = "Event time (k)",
    keep = "^-[1-5]|[0-5]",
    drop = "[[:digit:]]{2}",
    ref.line = 0
  )
  
  list(plot = plot_obj, results = dyn_mat, att_overall = res$att_overall)
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

EnsureMinusOneRow <- function(est_mat, ref_p) {
  if (!(as.character(ref_p) %in% rownames(est_mat))) {
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
                      add_comparison = TRUE,
                      add_pretrends = TRUE,
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
  
  par(oma = c(0, 0, 0, 0))
  par(mar = c(3.2, 4, 1.2, 1.2))
  par(xaxs = "r", yaxs = "r")
  par(mgp  = c(3, 1.5, 0))
  
  par(cex.axis = 1.5) 
  par(cex.lab  = 1.5)
  par(las = 1)
  
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
    n_items <- length(legend_labels)
    ncol_use <- ceiling(n_items / 2) +1  # columns per row -> produces `desired_rows` rows
    
    par(xpd = NA)   # allow drawing in the figure margins (legend may sit above plot)
    legend("top",
           legend = legend_labels,
           title = legend_title,
           horiz = TRUE,
           ncol = ncol_use,            # <- use computed value
           bty = "o",
           box.lwd = 0.8,
           box.col = "grey40",
           bg = "white",
           xjust = 0.5,
           col = seq_along(results),
           pch = 20,
           lwd = 1,
           lty = seq_along(results),
           cex = 1.0)
    par(xpd = FALSE)
  }
  usr <- par("usr")
  x_left <- usr[1] + 0.02 * (usr[2] - usr[1])
  
  if (add_comparison) {
    wald_lbl <- ""
    if (length(results) == 2) {
      wp <- CompareEventCoefsWald(results, terms = 1:5)
      wald_lbl <- paste0(legend_labels[1], " vs. ", legend_labels[2], " wald test p-value: ", sprintf("%.3f", wp))
    } else if (length(results) > 2) {
      combos <- combn(length(results), 2)
      wald_lbl <- paste(
        apply(combos, 2, function(idx) {
          p_val <- CompareEventCoefsWald(results[idx], terms = 1:5)
          paste0(legend_labels[idx[1]], " vs ", legend_labels[idx[2]], ": p=", sprintf("%.3f", p_val))
        }),
        collapse = " | "
      )
    }
    mtext(wald_lbl, side = 1, line = -2.1, at = x_left, adj = 0, cex = 1.4)
  }
  if (add_pretrends) {
    pre_lbl <- ""
    pre_p <- vapply(results, TestPretrends, numeric(1))
    pre_strs <- ifelse(pre_p < 0.001,
                       "p-value < 0.001",
                       sprintf("p-value: %.3f", pre_p))
    pre_lbl <- paste(paste0("Pretrend (", legend_labels, ") ", pre_strs), collapse = "\n")
    if (length(results) == 1) {
      pre_lbl <- paste0("Pretrend ", pre_strs)
    }
    mtext(pre_lbl, side = 1, line = -3.2, at = x_left, adj = 0, cex = 1.4)
  }
}



CompareEventCoefsWald <- function(tidy_list, terms = 1:5) {
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

