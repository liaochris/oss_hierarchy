
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
  
  df_norm[[outcome_norm]] <- (df_norm[[outcome]] - df_norm$mean_outcome) / df_norm$sd_outcome
  df_norm[is.finite(df_norm[[outcome_norm]]), ]
}

BuildOrgPracticeModes <- function(org_practice_cfg, control_group, outdir_dataset, build_dir) {
  modes <- list()
  for (org_practice in names(org_practice_cfg)) {
    for (main_cat in names(org_practice_cfg[[org_practice]])) {
      for (sub_cat in names(org_practice_cfg[[org_practice]][[main_cat]])) {
        mains <- org_practice_cfg[[org_practice]][[main_cat]][[sub_cat]]$main
        for (outcome in mains) {
          folder <- file.path(outdir_dataset, control_group, org_practice, main_cat, sub_cat, outcome)
          if (build_dir) {
            dir_create(folder)
          }
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

BuildCommonSample <- function(df, outcomes) {
  valid_repos <- lapply(outcomes, function(outcome) {
    df_norm <- NormalizeOutcome(df, outcome)
    unique(df_norm$repo_name)
  })
  keep_repos <- Reduce(intersect, valid_repos)
  df %>% filter(repo_name %in% keep_repos)
}

KeepSustainedImportant <- function(df_panel_common, lb = 1, ub = Inf) {
  repos_with_important <- df_panel_common %>% 
    filter(time_index - quasi_treatment_group == 0 & num_important_qualified >= lb & num_important_qualified <= ub) %>%
    pull(repo_name)
  df_panel_common %>% filter(repo_name %in% repos_with_important)
}

BuildOutcomeModes <- function(outcome_cfg, control_group, outdir_dataset, norm_options, build_dir = TRUE) {
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

