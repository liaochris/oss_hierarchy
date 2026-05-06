library(tidyverse)
library(arrow)
library(fs)
library(jsonlite)
library(knitr)

source("source/lib/helpers.R")
source("source/lib/forest_helpers.R")
source("source/lib/constants.R")

INDIR_PREP <- "output/analysis/data_prep"

PC_LABELS <- setNames(
  vapply(names(PC_GROUPS), function(g) sub(" score$", "", PCGroupFriendlyLabel(g)), character(1)),
  paste0(names(PC_GROUPS), "_principal_component1")
)

LoadForestData <- function(indir_cf, importance_type, rolling_panel,
                           qualified_sample, control_group, covar_type_dir = "pc1") {
  split_var <- FOREST_TRAINING_OUTCOME

  if (qualified_sample %in% names(AGGREGATED_SAMPLES)) {
    sub_sample_names <- AGGREGATED_SAMPLES[[qualified_sample]]
    sub_dfs <- setNames(
      lapply(sub_sample_names, function(s) {
        path <- file.path(indir_cf, importance_type, rolling_panel, s, control_group,
                          covar_type_dir, paste0(split_var, "_repo_att_event_study_forest.parquet"))
        tryCatch(read_parquet(path), error = function(e) NULL)
      }),
      sub_sample_names
    )
    sub_dfs <- Filter(Negate(is.null), sub_dfs)
    if (length(sub_dfs) == 0) return(NULL)
    list(df = bind_rows(sub_dfs), sub_dfs = sub_dfs)
  } else {
    path <- file.path(indir_cf, importance_type, rolling_panel, qualified_sample,
                      control_group, covar_type_dir,
                      paste0(split_var, "_repo_att_event_study_forest.parquet"))
    df <- tryCatch(read_parquet(path), error = function(e) NULL)
    if (is.null(df)) return(NULL)
    list(df = df, sub_dfs = NULL)
  }
}

LoadPanelData <- function(importance_type, rolling_panel, qualified_sample, control_group) {
  if (qualified_sample %in% names(AGGREGATED_SAMPLES)) {
    sub_panels <- lapply(AGGREGATED_SAMPLES[[qualified_sample]], function(s)
      LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel, s, control_group))
    sub_panels <- Filter(function(p) nrow(p) > 0, sub_panels)
    if (length(sub_panels) == 0) return(tibble())
    bind_rows(sub_panels)
  } else {
    LoadPreparedSample(INDIR_PREP, importance_type, rolling_panel, qualified_sample, control_group)
  }
}

BinarizePCScores <- function(df, pc_cols) {
  medians <- sapply(pc_cols, function(col) median(df[[col]], na.rm = TRUE))
  df_bin  <- df %>% mutate(across(all_of(pc_cols),
                                   ~ ifelse(.x > medians[cur_column()], "high", "low"),
                                   .names = "{.col}"))
  list(df = df_bin, medians = medians)
}

ComputeOutcomeStats <- function(panel_df, outcome_cols = c("pull_request_opened", "pull_request_merged", "overall_new_release_count")) {
  map_df(outcome_cols, function(col) {
    vec <- panel_df[[col]]
    tibble(outcome = col, median = median(vec, na.rm = TRUE), mean = mean(vec, na.rm = TRUE), sd = sd(vec, na.rm = TRUE))
  })
}

BuildLatexAndSave <- function(outcome_stats, pct_treated, num_projects, num_periods, outdir_ds) {
  outcome_display <- outcome_stats %>%
    transmute(Outcome = outcome,
              Mean    = formatC(mean,   format = "f", digits = 3),
              Median  = ifelse(is.na(median), "", formatC(median, format = "f", digits = 3)),
              SD      = formatC(sd,     format = "f", digits = 3))

  avg_periods <- if (num_projects > 0) as.numeric(num_periods) / as.numeric(num_projects) else NA_real_
  avg_periods_str <- if (!is.na(avg_periods)) {
    sprintf("%s/%s = %s", as.character(num_periods), as.character(num_projects),
            formatC(avg_periods, format = "f", digits = 2))
  } else {
    sprintf("%s/%s", as.character(num_periods), as.character(num_projects))
  }

  summary_df <- tibble(
    Outcome = c("PctTreated", "NumProjects", "AvgPeriods"),
    Mean    = c(formatC(pct_treated, format = "f", digits = 3), as.character(num_projects), avg_periods_str),
    Median  = c("", "", ""),
    SD      = c("", "", "")
  )

  outcome_latex <- as.character(knitr::kable(outcome_display, format = "latex", booktabs = TRUE, linesep = ""))
  out_lines <- strsplit(outcome_latex, "\n")[[1]]
  bot_idx   <- max(which(grepl("^\\\\bottomrule", out_lines)))
  if (is.na(bot_idx) || length(bot_idx) == 0) bot_idx <- length(out_lines)

  esc_latex <- function(s) gsub("([&%#$_{}~^\\\\])", "\\\\\\1", s)
  make_row  <- function(r) paste0(esc_latex(r["Outcome"]), " & ", esc_latex(r["Mean"]), " & ", esc_latex(r["Median"]), " & ", esc_latex(r["SD"]), " \\\\")

  full_lines <- c(
    out_lines[1:(bot_idx - 1)],
    "\\midrule",
    apply(summary_df, 1, make_row),
    "\\bottomrule",
    "\\end{tabular}"
  )
  writeLines(paste0(full_lines, collapse = "\n"), file.path(outdir_ds, "outcome_summary_table.tex"))
}

CreatePracticeComboTables <- function(df_bins, practice_vars, outdir_ds) {
  for (k in c(2, 3)) {
    combos <- combn(practice_vars, k, simplify = FALSE)

    rows_all <- lapply(combos, function(vars) {
      df_bins %>%
        group_by(across(all_of(vars))) %>%
        summarize(att_dr_mean = mean(att_dr, na.rm = TRUE), count = n(), .groups = "drop") %>%
        arrange(-att_dr_mean) %>%
        mutate(rank           = row_number(),
               pattern        = apply(as.matrix(pick(all_of(vars))), 1, paste, collapse = "-"),
               practice_subset = paste(vars, collapse = " x ")) %>%
        select(practice_subset, pattern, rank, att_dr_mean, count)
    })
    write_csv(bind_rows(rows_all) %>% arrange(-att_dr_mean),
              file.path(outdir_ds, paste0("practice_combo_k", k, "_table.csv")))

    rows_high <- lapply(combos, function(vars) {
      grp      <- df_bins %>%
        group_by(across(all_of(vars))) %>%
        summarize(att_dr_mean = mean(att_dr, na.rm = TRUE), count = n(), .groups = "drop")
      all_high <- grp %>% filter(if_all(all_of(vars), ~ .x == "high"))
      others   <- grp %>% filter(!if_all(all_of(vars), ~ .x == "high"))
      if (nrow(all_high) == 0) return(NULL)
      att_others <- if (nrow(others) > 0 && sum(others$count) > 0)
        sum(others$att_dr_mean * others$count) / sum(others$count) else NA_real_
      tibble(practice_subset = paste(vars, collapse = " x "),
             att_high        = all_high$att_dr_mean,
             att_others_wavg = att_others,
             difference      = all_high$att_dr_mean - att_others,
             count_high      = all_high$count)
    })
    write_csv(bind_rows(rows_high) %>% arrange(-difference),
              file.path(outdir_ds, paste0("practice_combo_k", k, "_high_table.csv")))
  }
}

PlotGradientGrid <- function(df_summary, practice_vars, fixed_rank_order = NULL) {
  mat        <- as.matrix(df_summary[practice_vars])
  df_summary <- df_summary %>%
    mutate(combo_key   = apply(mat, 1, function(r) paste(toupper(substr(r, 1, 1)), collapse = "-")),
           forest_rank = row_number(desc(att_dr_mean)))

  df_plot <- if (!is.null(fixed_rank_order)) {
    df_summary %>% mutate(row_pos = match(combo_key, fixed_rank_order)) %>% filter(!is.na(row_pos))
  } else {
    df_summary %>% mutate(row_pos = forest_rank)
  }

  ggplot(df_plot, aes(x = 1, y = row_pos, fill = forest_rank)) +
    geom_tile(color = "grey80", linewidth = 0.3, width = 1, height = 1) +
    scale_fill_gradient(low = "white", high = "black", limits = c(1, 32), guide = "none") +
    scale_y_reverse(breaks = c(1, 8, 16, 24, 32), expand = c(0, 0)) +
    scale_x_continuous(breaks = NULL, expand = c(0, 0)) +
    coord_fixed(xlim = c(0.5, 1.5), ylim = c(32.5, 0.5)) +
    labs(x = NULL, y = "Rank") +
    theme_minimal(base_size = 7) +
    theme(axis.text.y  = element_text(size = 6),
          axis.title.y = element_text(size = 7, angle = 90),
          panel.grid   = element_blank(),
          axis.ticks   = element_blank(),
          plot.margin  = margin(2, 2, 2, 2, "pt"))
}

LoadFoldSummary <- function(fold_rds_path, df_continuous, df_bins, practice_vars) {
  forest     <- readRDS(fold_rds_path)
  x_all      <- as.matrix(df_continuous %>% select(all_of(colnames(forest$X.orig))))
  tau_hat    <- predict(forest, newdata = x_all, drop = TRUE)$predictions
  tau_scalar <- if (is.matrix(tau_hat)) rowMeans(tau_hat, na.rm = TRUE) else as.numeric(tau_hat)

  df_bins %>%
    mutate(fold_att = tau_scalar) %>%
    group_by(across(all_of(practice_vars))) %>%
    summarize(att_dr_mean = mean(fold_att, na.rm = TRUE), count = n(), .groups = "drop") %>%
    arrange(-att_dr_mean) %>%
    mutate(rank = row_number())
}

PlotCrossForestGrid <- function(df_summary, fold_summaries, practice_vars, outdir_ds) {
  ComboKeys <- function(df) {
    mat <- as.matrix(df[practice_vars])
    apply(mat, 1, function(r) paste(toupper(substr(r, 1, 1)), collapse = "-"))
  }
  full_keys <- ComboKeys(fold_summaries[[1]] %>% arrange(-att_dr_mean))

  df_all <- bind_rows(lapply(seq_along(fold_summaries), function(i) {
    fs <- fold_summaries[[i]]
    fs %>%
      mutate(combo_key   = ComboKeys(.),
             forest_rank = rank(-att_dr_mean, ties.method = "first"),
             row_pos     = match(combo_key, full_keys),
             x_pos       = i) %>%
      filter(!is.na(row_pos)) %>%
      select(x_pos, row_pos, forest_rank)
  }))

  n_folds <- length(fold_summaries)
  p <- ggplot(df_all, aes(x = x_pos, y = row_pos, fill = forest_rank)) +
    geom_tile(color = "grey80", linewidth = 0.2, width = 1, height = 1) +
    scale_fill_gradient(low = "white", high = "black", limits = c(1, 32), guide = "none") +
    scale_x_continuous(breaks = seq_len(n_folds), labels = paste0("F", seq_len(n_folds)), expand = c(0, 0)) +
    scale_y_reverse(breaks = c(1, 8, 16, 24, 32), expand = c(0, 0)) +
    coord_fixed(xlim = c(0.5, n_folds + 0.5), ylim = c(32.5, 0.5)) +
    labs(x = NULL, y = "Rank") +
    theme_minimal(base_size = 7) +
    theme(axis.text.x  = element_text(size = 6),
          axis.text.y  = element_text(size = 6),
          axis.title.y = element_text(size = 7, angle = 90),
          panel.grid   = element_blank(),
          axis.ticks   = element_blank(),
          plot.margin  = margin(2, 2, 2, 2, "pt"))

  tile_in <- 0.18
  ggsave(file.path(outdir_ds, "cross_forest_grid.png"), p,
         width  = n_folds * tile_in + 0.6,
         height = 32 * tile_in + 0.4,
         dpi = 300)
}

ComputeFoldCorrelations <- function(df_summary, fold_summaries, practice_vars, outdir_ds) {
  ComboKeys <- function(df) {
    mat <- as.matrix(df[practice_vars])
    apply(mat, 1, function(r) paste(toupper(substr(r, 1, 1)), collapse = "-"))
  }
  full_keys <- ComboKeys(df_summary %>% arrange(-att_dr_mean))

  att_mat <- sapply(fold_summaries, function(fs) {
    fs$combo_key <- ComboKeys(fs)
    fs[match(full_keys, fs$combo_key), ]$att_dr_mean
  })
  rank_mat <- apply(att_mat, 2, rank, na.last = "keep")

  corr_att  <- cor(att_mat,  method = "pearson",  use = "pairwise.complete.obs")
  corr_rank <- cor(rank_mat, method = "spearman", use = "pairwise.complete.obs")

  PlotCorrHeatmap <- function(corr_mat, title) {
    n <- nrow(corr_mat)
    rownames(corr_mat) <- colnames(corr_mat) <- paste0("F", seq_len(n))
    corr_mat %>%
      as.data.frame() %>%
      tibble::rownames_to_column("fold1") %>%
      pivot_longer(-fold1, names_to = "fold2", values_to = "corr") %>%
      mutate(label = sprintf("%.2f", corr)) %>%
      ggplot(aes(x = fold1, y = fold2, fill = corr)) +
      geom_tile(color = "white") +
      geom_text(aes(label = label), size = 3) +
      scale_fill_gradient2(limits = c(-1, 1), midpoint = 0,
                           low = "blue", mid = "white", high = "red", name = "r") +
      labs(title = title, x = NULL, y = NULL) +
      theme_minimal(base_size = 10) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
      coord_fixed()
  }

  ggsave(file.path(outdir_ds, "fold_corr_att.png"),  PlotCorrHeatmap(corr_att,  "Fold Correlation (ATT)"),  width = 8, height = 7, dpi = 300)
  ggsave(file.path(outdir_ds, "fold_corr_rank.png"), PlotCorrHeatmap(corr_rank, "Fold Correlation (Rank)"), width = 8, height = 7, dpi = 300)
}

ExportVariableImportanceTex <- function(fold_rds_paths, outdir_ds) {
  existing <- fold_rds_paths[file.exists(fold_rds_paths)]
  if (length(existing) == 0) return(invisible(NULL))

  varimp_df <- bind_rows(lapply(existing, function(path) {
    forest <- readRDS(path)
    vi     <- variable_importance(forest)
    tibble(variable = colnames(forest$X.orig), importance = as.numeric(vi))
  })) %>%
    group_by(variable) %>%
    summarize(importance = mean(importance, na.rm = TRUE), .groups = "drop") %>%
    arrange(-importance) %>%
    mutate(rank  = row_number(),
           label = ifelse(variable %in% names(PC_LABELS),
                          paste0(PC_LABELS[variable], " (PC1)"), variable))

  out_tbl    <- varimp_df %>% transmute(Rank = rank, Variable = label, `Mean Split Share` = sprintf("%.4f", importance))
  latex_body <- as.character(knitr::kable(out_tbl, format = "latex", booktabs = TRUE, linesep = ""))
  writeLines(latex_body, file.path(outdir_ds, "variable_importance.tex"))
  invisible(varimp_df)
}
