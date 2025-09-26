
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

