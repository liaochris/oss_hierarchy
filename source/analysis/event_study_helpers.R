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
           lwd = 1, lty = seq_along(results),
           cex = 1.2)
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
    mtext(wald_lbl, side = 1, line = -2, at = x_left, adj = 0, cex = 1.2)
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
    mtext(pre_lbl, side = 1, line = -3, at = x_left, adj = 0, cex = 0.8)
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


GetEventAtt <- function(df, outcome, control_group, method = "sa", normalize = FALSE, periods = 1:5) {
  es <- EventStudy(df, outcome, control_group, method = method, normalize = normalize, title = NULL)
  res_mat <- as.matrix(es$results)
  out <- purrr::map_dfr(periods, function(p) {
    row <- res_mat[as.character(p), , drop = TRUE]
    tibble::tibble(period = p,
                   estimate = as.numeric(row["estimate"]),
                   sd = as.numeric(row["sd"]),
                   ci_low = as.numeric(row["ci_low"]),
                   ci_high = as.numeric(row["ci_high"]))
  })
  out
}

ComputeDiffHighLow <- function(att_high, att_low) {
  high <- att_high %>% dplyr::rename_with(~paste0("high_", .), -period)
  low  <- att_low  %>% dplyr::rename_with(~paste0("low_",  .), -period)
  df <- dplyr::left_join(high, low, by = "period")
  df %>%
    dplyr::mutate(diff_estimate = high_estimate - low_estimate,
                  diff_se = sqrt((high_sd^2) + (low_sd^2)),
                  diff_ci_low = diff_estimate - 1.96 * diff_se,
                  diff_ci_high = diff_estimate + 1.96 * diff_se) %>%
    dplyr::select(period,
                  high_estimate, high_sd, high_ci_low, high_ci_high,
                  low_estimate, low_sd, low_ci_low, low_ci_high,
                  diff_estimate, diff_se, diff_ci_low, diff_ci_high)
}

ComputeWeightedAvgAcrossPeriods <- function(att_diffs_df, periods = 1:5) {
  att_diffs_df %>%
    dplyr::filter(period %in% periods) %>%
    dplyr::mutate(var = diff_se^2) %>%
    dplyr::group_by(covar, outcome) %>%
    dplyr::summarise(
      w_sum = sum(ifelse(!is.na(var) & var > 0, 1/var, 0)),
      weighted_mean = ifelse(w_sum > 0, sum((ifelse(!is.na(var) & var > 0, 1/var, 0)) * diff_estimate, na.rm = TRUE) / w_sum, NA_real_),
      weighted_var = ifelse(w_sum > 0, 1 / w_sum, NA_real_),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      weighted_se = sqrt(weighted_var),
      ci_low = weighted_mean - 1.96 * weighted_se,
      ci_high = weighted_mean + 1.96 * weighted_se
    ) %>%
    dplyr::select(covar, outcome, estimate = weighted_mean, se = weighted_se, ci_low, ci_high)
}

ReadPracticeMap <- function(map_file) {
  readr::read_csv(map_file, col_types = readr::cols(
    code_name = readr::col_character(),
    display_name = readr::col_character(),
    group = readr::col_character(),
    order = readr::col_double()
  )) %>%
    dplyr::distinct(code_name, .keep_all = TRUE) %>%
    dplyr::mutate(
      group = dplyr::case_when(
        stringr::str_detect(tolower(.data$group), "know") ~ "Knowledge",
        stringr::str_detect(tolower(.data$group), "talent") ~ "Talent",
        stringr::str_detect(tolower(.data$group), "routine|rout") ~ "Routines",
        TRUE ~ .data$group
      )
    )
}

ApplyPracticeMap <- function(att_diffs, map_df, include_unmapped = FALSE) {
  joined <- att_diffs %>%
    dplyr::left_join(map_df %>% dplyr::select(.data$code_name, .data$display_name, .data$group, .data$order),
                     by = c("covar" = "code_name"))
  if (!include_unmapped) {
    joined <- joined %>% dplyr::filter(!is.na(.data$display_name))
  }
  joined %>%
    dplyr::mutate(
      display_name = .data$display_name,
      group = dplyr::coalesce(.data$group, "Other"),
      order = dplyr::coalesce(.data$order, 9999),
      group = factor(.data$group, levels = c("Knowledge", "Talent", "Routines", "Other"))
    )
}

ApplyOutcomeMap <- function(att_diffs, outcome_map) {
  att_diffs %>% dplyr::mutate(outcome_label = dplyr::recode(.data$outcome, !!!outcome_map, .default = .data$outcome))
}

PlotPracticeOutcomeHorizontal <- function(att_diffs,
                                          out_file,
                                          practice_map = NULL,
                                          outcome_map = NULL,
                                          periods = 1:5,
                                          label_offset = 0.18,
                                          bracket_offset = 0.40,
                                          right_pad = 0.18,
                                          left_margin_pt = 160,
                                          cap_height = 0.12,
                                          cap_thickness = 1.0,
                                          point_size = 3,
                                          point_alpha = 0.6,
                                          width = 10,
                                          height = 8) {
  df <- att_diffs
  map_df <- if (is.character(practice_map)) ReadPracticeMap(practice_map) else practice_map
  if (!is.null(map_df)) df <- ApplyPracticeMap(df, map_df, include_unmapped = FALSE)
  if (!is.null(outcome_map)) df <- ApplyOutcomeMap(df, outcome_map) else df$outcome_label <- df$outcome
  
  stopifnot(c("diff_estimate", "diff_se", "period", "covar") %in% names(df))
  
  summary_df <- df %>%
    dplyr::filter(period %in% periods) %>%
    dplyr::mutate(var = diff_se^2) %>%
    dplyr::group_by(display_name, group, outcome_label) %>%
    dplyr::summarise(
      w = sum(ifelse(!is.na(var) & var > 0, 1/var, 0)),
      estimate = ifelse(w > 0, sum((ifelse(!is.na(var) & var > 0, 1/var, 0)) * diff_estimate, na.rm = TRUE)/w, NA_real_),
      var_w = ifelse(w > 0, 1/w, NA_real_),
      .groups = "drop"
    ) %>%
    dplyr::mutate(se = sqrt(var_w), ci_lo = estimate - 1.96 * se, ci_hi = estimate + 1.96 * se)
  
  if (!is.null(map_df)) {
    order_pracs <- map_df %>% dplyr::arrange(factor(group, levels = c("Knowledge", "Talent", "Routines")), order) %>% dplyr::pull(display_name) %>% unique()
    summary_df <- summary_df %>% dplyr::filter(display_name %in% order_pracs) %>% dplyr::mutate(display_name = factor(display_name, levels = order_pracs))
  } else {
    summary_df <- summary_df %>% dplyr::mutate(display_name = factor(display_name, levels = unique(display_name)))
  }
  
  pos <- tibble::tibble(display_name = levels(summary_df$display_name), y = seq_along(levels(summary_df$display_name)))
  summary_df <- dplyr::left_join(summary_df, pos, by = "display_name") %>% dplyr::mutate(y_plot = y)
  
  x_min <- min(c(summary_df$ci_lo, summary_df$estimate), na.rm = TRUE)
  x_max <- max(c(summary_df$ci_hi, summary_df$estimate), na.rm = TRUE)
  if (!is.finite(x_min) || !is.finite(x_max)) { x_min <- -1; x_max <- 1 }
  x_range <- ifelse((x_max - x_min) == 0, 1, x_max - x_min)
  
  bracket_x <- x_min - bracket_offset * x_range
  label_x   <- x_min - label_offset * x_range
  x_right   <- x_max + right_pad * x_range
  
  # ensure plotted data does not extend into the label text area:
  min_data_x <- label_x + 0.02 * x_range
  
  # create plotting coordinates (clipped) so CI lines and points stop before labels
  summary_df <- summary_df %>%
    dplyr::mutate(
      ci_lo_plot = dplyr::if_else(is.na(ci_lo), NA_real_, pmax(ci_lo, min_data_x)),
      ci_hi_plot = dplyr::if_else(is.na(ci_hi), NA_real_, pmin(ci_hi, x_right)),
      estimate_plot = dplyr::if_else(is.na(estimate), NA_real_, pmax(pmin(estimate, x_right), min_data_x))
    )
  
  group_span <- summary_df %>% dplyr::group_by(group) %>% dplyr::summarise(ymin = min(y_plot), ymax = max(y_plot), .groups = "drop") %>% dplyr::arrange(factor(group, levels = c("Knowledge", "Talent", "Routines", "Other")))
  
  summary_df$Outcome <- summary_df$outcome_label
  summary_df <- summary_df[nrow(summary_df):1, ]
  p <- ggplot2::ggplot(summary_df, ggplot2::aes(x = estimate, y = y_plot, color = Outcome, fill = Outcome)) +
    ggplot2::geom_vline(xintercept = 0, linetype = "dotted", color = "gray50") +
    # use clipped plotting coordinates for segments so they don't run into labels
    ggplot2::geom_segment(ggplot2::aes(x = ci_lo_plot, xend = ci_hi_plot, y = y_plot, yend = y_plot), size = 0.8, na.rm = TRUE) +
    ggplot2::geom_segment(ggplot2::aes(x = ci_lo_plot, xend = ci_lo_plot, y = y_plot - cap_height, yend = y_plot + cap_height), size = cap_thickness, na.rm = TRUE) +
    ggplot2::geom_segment(ggplot2::aes(x = ci_hi_plot, xend = ci_hi_plot, y = y_plot - cap_height, yend = y_plot + cap_height), size = cap_thickness, na.rm = TRUE) +
    # plot points at clipped estimate_plot (so a point won't sit inside the label zone)
    ggplot2::geom_point(ggplot2::aes(x = estimate_plot), shape = 21, size = point_size, stroke = 0.5, color = "black", alpha = point_alpha, na.rm = TRUE)
  
  if (nrow(group_span) > 0) {
    p <- p +
      ggplot2::geom_segment(data = group_span, ggplot2::aes(x = bracket_x, xend = bracket_x, y = ymin - 0.5, yend = ymax + 0.5), inherit.aes = FALSE, color = "black", size = 0.95) +
      ggplot2::geom_segment(data = group_span, ggplot2::aes(x = bracket_x, xend = bracket_x + 0.02 * x_range, y = ymin - 0.5, yend = ymin - 0.5), inherit.aes = FALSE, color = "black", size = 0.95) +
      ggplot2::geom_segment(data = group_span, ggplot2::aes(x = bracket_x, xend = bracket_x + 0.02 * x_range, y = ymax + 0.5, yend = ymax + 0.5), inherit.aes = FALSE, color = "black", size = 0.95) +
      ggplot2::geom_text(data = group_span, ggplot2::aes(x = bracket_x - 0.03 * x_range, y = (ymin + ymax)/2, label = group), inherit.aes = FALSE, angle = 90, hjust = 0.5, vjust = 0.5, size = 4)
  }
  
  p <- p +
    ggplot2::geom_text(data = pos, ggplot2::aes(x = label_x, y = y, label = display_name), inherit.aes = FALSE, hjust = 0, vjust = 0.5, size = 3.6, color = "grey20") +
    ggplot2::scale_y_continuous(breaks = NULL, expand = c(0.02, 0.02)) +
    ggplot2::labs(x = "ATT difference between high and low", y = "") +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      panel.grid.major.y = ggplot2::element_blank(),
      legend.position = "top",
      plot.margin = grid::unit(c(5, 5, 5, left_margin_pt), "pt")
    ) +
    ggplot2::coord_cartesian(xlim = c(bracket_x - 0.02 * x_range, x_right))
  
  ggplot2::ggsave(filename = out_file, plot = p, width = width, height = height, dpi = 300)
  invisible(p)
}
