NormalizeOutcome <- function(df, outcome) {
  outcome_sym <- rlang::sym(outcome)
  outcome_norm <- paste0(outcome, "_norm")

  df_norm <- df %>%
    group_by(repo_name) %>%
    mutate(
      mean_outcome = mean((!!outcome_sym)[time_index < quasi_treatment_group &
                                            time_index >= (quasi_treatment_group - 5)], na.rm = TRUE),
      sd_outcome   = sd((!!outcome_sym)[time_index < quasi_treatment_group &
                                          time_index >= (quasi_treatment_group - 5)], na.rm = TRUE)
    ) %>%
    ungroup()

  df_norm[[outcome_norm]] <- (df_norm[[outcome]] - df_norm$mean_outcome)/df_norm$sd_outcome
  df_norm[is.finite(df_norm[[outcome_norm]]), ]
}

BuildOrgPracticeModes <- function(org_practice_cfg, control_group, outdir_dataset, build_dir) {
  modes <- list()
  for (category in names(org_practice_cfg)) {
    for (sub_cat in names(org_practice_cfg[[category]])) {
      mains <- org_practice_cfg[[category]][[sub_cat]]$run
      for (outcome in mains) {
        folder <- file.path(outdir_dataset, control_group, category, sub_cat, outcome)
        if (build_dir) {
          dir_create(folder)
        }
        modes <- append(modes, list(
          list(
            continuous_covariate   = outcome,
            filters                = list(list(col = paste0(outcome, "_2bin"), vals = c(1, 0))),
            legend_labels          = c("High", "Low"),
            legend_title           = outcome,
            control_group          = control_group,
            data                   = control_group,
            folder                 = folder
          )
        ))
      }
    }
  }
  modes
}

QualifiedSampleLabel <- function(qualified_sample) {
  if (grepl("^exact[0-9]+$", qualified_sample)) {
    count <- sub("^exact", "", qualified_sample)
    return(paste0("num-qualified=", count))
  }
  qualified_sample
}

PreparedSampleDir <- function(base_dir, importance_type, rolling_panel, qualified_sample, control_group) {
  file.path(base_dir, importance_type, rolling_panel, qualified_sample, control_group)
}

LoadPreparedSample <- function(base_dir, importance_type, rolling_panel, qualified_sample, control_group,
                               with_pc_scores = FALSE) {
  filename <- if (with_pc_scores) "panel_with_pc_scores.parquet" else "panel.parquet"
  path <- file.path(PreparedSampleDir(base_dir, importance_type, rolling_panel, qualified_sample, control_group),
                    filename)
  arrow::read_parquet(path)
}

BuildOutcomeModes <- function(outcome_cfg, control_group, outdir_dataset, norm_options, build_dir = TRUE) {
  expand <- function(cat, out, norm, control_group) list(
    outcome   = out,
    category  = cat,
    normalize = norm,
    control_group = control_group,
    data      = control_group,
    file      = file.path(outdir_dataset, control_group, cat,
                          paste0(out, if (norm) "_norm", ".png"))
  )

  modes <- lapply(names(Filter(is.list, outcome_cfg)), function(cat) {
    outcomes <- outcome_cfg[[cat]]$run
    control_groups <- c(control_group)
    do.call(c, lapply(control_groups, function(control_group) {
      if (build_dir) {
        dir_create(file.path(outdir_dataset, control_group, cat))
      }
      do.call(c, lapply(outcomes, function(out) {
        lapply(norm_options, function(norm) expand(cat, out, norm, control_group))
      }))
    }))
  })

  do.call(c, modes)
}
