library(tidyverse)
library(arrow)
library(fs)

source("source/analysis/analyze_forest/helpers.R")
source("source/lib/R/event_study_helpers.R")

IMPORTANCE_TYPE  <- "important_degree_top3"
ROLLING_PANEL    <- "rolling5"
QUALIFIED_SAMPLE <- "exact2"
CONTROL_GROUP    <- "nevertreated"
NORM_LABEL       <- "norm"
NORMALIZE        <- TRUE
INDIR_PANEL      <- "output/derived/analysis_panel"
INDIR_FOREST     <- "output/analysis/event_study_forest"
ANALYZE_DIR      <- file.path("output/analysis/analyze_forest", IMPORTANCE_TYPE, ROLLING_PANEL,
                              QUALIFIED_SAMPLE, CONTROL_GROUP, NORM_LABEL)
OUTDIR           <- "issue"
OUTCOMES         <- c(outcome_variables$github_outcomes$run, outcome_variables$downstream_outcomes$run)

ComboGroups <- function(df_binarized, vars) {
  classified <- df_binarized %>% drop_na(all_of(vars))
  all_high   <- Reduce(`&`, lapply(vars, function(v) classified[[v]] == "high"))
  tibble(repo_name = classified$repo_name,
         group     = factor(ifelse(all_high, "All-high", "Everyone else"),
                            levels = c("All-high", "Everyone else")))
}

Main <- function() {
  panel <- LoadPreparedSample(INDIR_PANEL, IMPORTANCE_TYPE, ROLLING_PANEL, QUALIFIED_SAMPLE, CONTROL_GROUP)
  forest_results <- LoadForestResults(INDIR_FOREST, IMPORTANCE_TYPE, ROLLING_PANEL,
                                      QUALIFIED_SAMPLE, CONTROL_GROUP, NORM_LABEL)
  pc_score_cols  <- grep("_pc_score$", colnames(forest_results$df), value = TRUE)
  df_binarized   <- BinarizePCScores(forest_results$sub_dfs, pc_score_cols)$df

  top_combos <- read_csv(file.path(ANALYZE_DIR, "pc_score_combo_k2_high_table.csv"), show_col_types = FALSE) %>%
    arrange(desc(difference)) %>%
    head(3) %>%
    pull(pc_score_subset)

  for (combo_subset in top_combos) {
    vars        <- strsplit(combo_subset, " x ")[[1]]
    combo_slug  <- paste(sub("_pc_score$", "", vars), collapse = "_x_")
    combo_label <- paste(PC_LABELS[vars], collapse = " & ")
    base_df     <- panel %>% inner_join(ComboGroups(df_binarized, vars), by = "repo_name")
    print(combo_subset)
    for (outcome in OUTCOMES) {
      es_list <- lapply(levels(base_df$group), function(grp) {
        tryCatch(
          FitEventStudy(base_df %>% filter(group == grp), outcome, CONTROL_GROUP,
                        method = "sa", normalize = NORMALIZE, make_plot = FALSE),
          error = function(e) NULL
        )
      })
      valid <- !vapply(es_list, is.null, logical(1))
      if (!any(valid)) next

      out_path <- file.path(OUTDIR, paste0("pc_combo_event_study_", combo_slug, "_", outcome, ".png"))
      PlotEventStudyBatch(list(list(
        es_list       = es_list[valid],
        out_path      = out_path,
        legend_labels = levels(base_df$group)[valid],
        legend_title  = combo_label,
        png_args      = list(width = 800, height = 500)
      )))
    }
  }
  invisible(NULL)
}

Main()
