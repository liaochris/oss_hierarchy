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

FixEstCoefMatrix <- function(est_obj, term_prefix) {
  est_obj %>%
    tidy() %>%
    mutate(term = as.numeric(str_remove(term, paste0("^", term_prefix))), sd = std.error) %>%
    select(-std.error, -statistic, -p.value) %>%
    mutate(ci_low = estimate - 1.96 * sd, ci_high = estimate + 1.96 * sd) %>%
    column_to_rownames("term") %>%
    as.matrix()
}

AddRefPeriodRow <- function(est_mat, ref_period) {
  if (!(as.character(ref_period) %in% rownames(est_mat))) {
    blank_row <- matrix(0, nrow = 1, ncol = ncol(est_mat),
                        dimnames = list(as.character(ref_period), colnames(est_mat)))
    est_mat <- rbind(est_mat, blank_row)
  }
  est_mat
}

TestPretrends <- function(coef_mat, terms = as.character(MIN_EVENT_TIME:-1)) {
  available <- intersect(rownames(coef_mat), terms)
  if (length(available) == 0) return(NA_real_)

  sorted_terms <- available[order(as.numeric(available))]
  estimates <- coef_mat[sorted_terms, "estimate"]
  sds <- coef_mat[sorted_terms, "sd"]

  keep_index <- sds != 0 & !is.na(sds)
  if (all(!keep_index)) return(NA_real_)

  estimates <- estimates[keep_index]
  variances <- sds[keep_index]^2
  sigma <- diag(variances, nrow = length(variances))

  tryCatch({
    wald.test(b = estimates, Sigma = sigma, Terms = seq_along(estimates))$result$chi2[["P"]]
  }, error = function(e) NA_real_)
}

WaldTestEventCoefs <- function(coef_mats, terms = 1:MAX_EVENT_TIME) {
  sel_terms <- as.character(terms)
  m1 <- as.matrix(coef_mats[[1]])
  m2 <- as.matrix(coef_mats[[2]])
  available <- intersect(intersect(rownames(m1), rownames(m2)), sel_terms)
  if (length(available) == 0) return(NA_real_)

  avail_sorted <- available[order(as.numeric(available))]
  if (length(avail_sorted) < 4) return(NA_real_)

  t1 <- coef_mats[[1]][avail_sorted, ]
  t2 <- coef_mats[[2]][avail_sorted, ]
  delta <- t1[, "estimate"] - t2[, "estimate"]
  Sigma <- diag(t1[, "sd"]^2 + t2[, "sd"]^2, nrow = length(delta))

  tryCatch({
    wald.test(b = delta, Sigma = Sigma, Terms = seq_along(delta))$result$chi2[["P"]]
  }, error = function(e) NA_real_)
}

FitEventStudy <- function(df, outcome, control_group, method = c("cs", "sa", "es"), normalize = FALSE, title = NULL, make_plot = TRUE) {
  method <- match.arg(method)
  df_est     <- if (normalize) NormalizeOutcome(df, outcome) else df
  yvar       <- if (normalize) paste0(outcome, "_norm") else outcome
  title      <- if (is.null(title)) "" else as.character(title)
  ref_period <- -1

  keep_pattern <- paste0("^-[1-", abs(MIN_EVENT_TIME), "]|[0-", MAX_EVENT_TIME, "]")
  drop_pattern <- "[[:digit:]]{2}"
  event_order  <- as.character(c(MIN_EVENT_TIME:-1, 0:MAX_EVENT_TIME))

  method_result <- switch(method,
    cs = {
      set.seed(SEED)
      att <- att_gt(yname = yvar, tname = "time_index", idname = "repo_id",
        gname = "treatment_group", xformla = ~1, data = df_est,
        control_group = control_group, allow_unbalanced_panel = TRUE,
        clustervars = "repo_name", est_method = "dr")
      dynamic_att <- aggte(att, type = "dynamic", na.rm = TRUE) %>%
        broom::tidy() %>%
        dplyr::rename(sd = std.error, ci_low = conf.low, ci_high = conf.high) %>%
        dplyr::select(event.time, estimate, sd, ci_low, ci_high) %>%
        dplyr::mutate(event.time = as.numeric(event.time)) %>%
        tibble::column_to_rownames("event.time") %>%
        as.matrix()
      list(results = dynamic_att, att_overall = NULL)
    },
    sa = {
      est_formula <- as.formula(sprintf(
        "%s ~ sunab(treatment_group, time_index, ref.p=%d) | repo_name + time_index", yvar, ref_period))
      est_obj <- feols(est_formula, df_est, vcov = ~repo_name)
      dyn_mat <- FixEstCoefMatrix(est_obj, "time_index::")
      att_overall_tbl <- tryCatch({
        broom::tidy(suppressWarnings(aggregate(est_obj, agg = "ATT")))[1, , drop = FALSE] %>%
          dplyr::transmute(estimate, sd = std.error, ci_low = conf.low, ci_high = conf.high)
      }, error = function(e) NULL)
      list(results = dyn_mat, att_overall = att_overall_tbl)
    },
    es = {
      est_mat <- eventstudyr::EventStudy(estimator = "OLS", data = df_est, outcomevar = yvar,
        policyvar = "treatment", idvar = "repo_name", timevar = "time_index",
        post = 4, pre = 0, overidpost = 0)$output %>%
        broom::tidy() %>%
        dplyr::rename(sd = std.error, ci_low = conf.low, ci_high = conf.high) %>%
        dplyr::select(term, estimate, sd, ci_low, ci_high) %>%
        dplyr::mutate(term = dplyr::case_when(
          stringr::str_detect(term, "fd_lead") ~ -as.numeric(stringr::str_extract(term, "\\d+$")),
          stringr::str_detect(term, "lead")    ~ -(as.numeric(stringr::str_extract(term, "\\d+$")) + 1),
          stringr::str_detect(term, "lag")     ~ as.numeric(stringr::str_extract(term, "\\d+$")),
          TRUE ~ 0
        )) %>%
        tibble::column_to_rownames("term") %>%
        as.matrix()
      list(results = est_mat, att_overall = NULL)
    }
  )

  dyn_mat <- method_result$results
  dyn_mat <- AddRefPeriodRow(dyn_mat, ref_period)
  dyn_mat <- dyn_mat[order(as.numeric(rownames(dyn_mat))), , drop = FALSE]

  plot_obj <- if (make_plot) {
    fixest::coefplot(dyn_mat, main = title, xlab = "Event time (k)",
      keep = keep_pattern, drop = drop_pattern, ref.line = 0)
  } else NULL

  list(plot = plot_obj, results = dyn_mat, att_overall = method_result$att_overall)
}

WeightedAggregateCoefMatrix <- function(coef_mat_list, n_treated) {
  shared_event_times <- Reduce(intersect, lapply(coef_mat_list, rownames))
  ref_period         <- "-1"
  est_times          <- setdiff(shared_event_times, ref_period)

  estimates <- vapply(coef_mat_list, function(m) m[est_times, "estimate"], numeric(length(est_times)))
  sds       <- vapply(coef_mat_list, function(m) m[est_times, "sd"],       numeric(length(est_times)))

  sample_weights <- n_treated / sum(n_treated)
  w_mat          <- matrix(sample_weights, nrow = length(est_times), ncol = length(n_treated), byrow = TRUE)

  agg_estimate <- rowSums(estimates * w_mat)
  agg_sd       <- sqrt(rowSums(sds^2 * w_mat^2))

  result <- cbind(estimate = agg_estimate, sd = agg_sd,
                  ci_low   = agg_estimate - 1.96 * agg_sd,
                  ci_high  = agg_estimate + 1.96 * agg_sd)
  rownames(result) <- est_times

  if (ref_period %in% shared_event_times) {
    ref_row <- matrix(0, nrow = 1, ncol = ncol(result),
                      dimnames = list(ref_period, colnames(result)))
    result  <- rbind(result, ref_row)
  }
  result[order(as.numeric(rownames(result))), , drop = FALSE]
}

ComputeSharedYLim <- function(coef_mats, pad_frac = 0.05) {
  plotted_coef_mats <- lapply(coef_mats, function(mat) {
    event_time <- suppressWarnings(as.numeric(rownames(mat)))
    keep <- !is.na(event_time) & event_time >= MIN_EVENT_TIME & event_time <= MAX_EVENT_TIME
    mat[keep, , drop = FALSE]
  })

  ci_low  <- unlist(lapply(plotted_coef_mats, function(mat) mat[, "ci_low"]), use.names = FALSE)
  ci_high <- unlist(lapply(plotted_coef_mats, function(mat) mat[, "ci_high"]), use.names = FALSE)
  ci_low  <- ci_low[is.finite(ci_low)]
  ci_high <- ci_high[is.finite(ci_high)]

  lower <- min(ci_low)
  upper <- max(ci_high)
  span  <- upper - lower
  pad   <- max(span, max(abs(lower), abs(upper), 1) * (span == 0)) * pad_frac

  c(lower - pad, upper + pad)
}

PlotEventStudyBatch <- function(plot_specs) {
  if (length(plot_specs) == 0) return(invisible(NULL))

  coef_mats <- purrr::flatten(lapply(plot_specs, function(plot_spec)
    lapply(plot_spec$es_list, `[[`, "results")))
  shared_ylim <- ComputeSharedYLim(coef_mats)

  for (plot_spec in plot_specs) {
    png_args <- plot_spec$png_args
    if (is.null(png_args)) png_args <- list()
    do.call(png, c(list(filename = plot_spec$out_path), png_args))
    plot_args <- list(
      es_list = plot_spec$es_list,
      legend_labels = plot_spec$legend_labels,
      legend_title = plot_spec$legend_title,
      title = "",
      ylim = shared_ylim
    )
    if (!is.null(plot_spec$add_comparison)) {
      plot_args$add_comparison <- plot_spec$add_comparison
    }
    do.call(PlotEventStudyComparison, plot_args)
    dev.off()
  }
}

PlotEventStudyComparison <- function(es_list, legend_title = NULL, legend_labels = NULL,
                                     title = "", add_comparison = TRUE, add_pretrends = TRUE,
                                     ylim = NULL) {
  old_par <- par(no.readonly = TRUE)
  on.exit(par(old_par), add = TRUE)

  coef_mats <- lapply(es_list, `[[`, "results")
  coef_mats <- Filter(function(x) !is.null(x) && nrow(x) > 0, coef_mats)
  if (length(coef_mats) == 0) {
    warning("PlotEventStudyComparison: no results to plot")
    return(invisible(NULL))
  }

  keep_pattern <- paste0("^-[1-", abs(MIN_EVENT_TIME), "]|[0-", MAX_EVENT_TIME, "]")
  event_order  <- as.character(c(MIN_EVENT_TIME:-1, 0:MAX_EVENT_TIME))

  par(bg = "white", oma = c(0,0,0,0), mar = c(3.2,4,1.2,1.2), xaxs = "r", yaxs = "r",
      mgp = c(3,1.5,0), cex.axis = 1.5, cex.lab = 1.5, las = 1)

  plot_args <- list(object = coef_mats, multi = TRUE, xlab = "Event time (k)", ylab = "",
    main = title, keep = keep_pattern, drop = "[[:digit:]]{2}", order = event_order,
    xaxt = "n", yaxt = "n", grid = FALSE)
  if (!is.null(ylim)) plot_args$ylim <- ylim
  do.call(fixest::coefplot, plot_args)

  if (!is.null(legend_labels)) {
    par(xpd = NA)
    legend("top", legend = legend_labels, title = legend_title, horiz = TRUE,
           ncol = ceiling(length(legend_labels) / 2) + 1, bty = "o", box.lwd = 0.8,
           box.col = "grey40", bg = "white", xjust = 0.5, col = seq_along(coef_mats),
           pch = 20, lwd = 1, lty = seq_along(coef_mats), cex = 1.0)
    par(xpd = FALSE)
  }

  usr <- par("usr")
  x_left <- usr[1] + 0.02 * (usr[2] - usr[1])

  if (add_comparison) {
    wald_label <- ""
    if (length(coef_mats) == 2) {
      wald_p     <- WaldTestEventCoefs(coef_mats, terms = 1:MAX_EVENT_TIME)
      wald_label <- paste0(legend_labels[1], " vs. ", legend_labels[2],
                           " wald test p-value: ", sprintf("%.3f", wald_p))
    } else if (length(coef_mats) > 2) {
      combos     <- combn(length(coef_mats), 2)
      wald_label <- paste(apply(combos, 2, function(pair) {
        wald_p <- WaldTestEventCoefs(coef_mats[pair], terms = 1:MAX_EVENT_TIME)
        paste0(legend_labels[pair[1]], " vs ", legend_labels[pair[2]], ": p=", sprintf("%.3f", wald_p))
      }), collapse = " | ")
    }
    mtext(wald_label, side = 1, line = -2.1, at = x_left, adj = 0, cex = 1.4)
  }

  if (add_pretrends) {
    pretrend_p    <- vapply(coef_mats, TestPretrends, numeric(1))
    pretrend_strings <- ifelse(pretrend_p < 0.001, "p-value < 0.001", sprintf("p-value: %.3f", pretrend_p))
    pretrend_label  <- if (length(coef_mats) == 1) {
      paste0("Pretrend ", pretrend_strings)
    } else {
      paste(paste0("Pretrend (", legend_labels, ") ", pretrend_strings), collapse = "\n")
    }
    mtext(pretrend_label, side = 1, line = -3.2, at = x_left, adj = 0, cex = 1.4)
  }
}
