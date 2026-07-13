# Export the ESTIMATOR's exact actual opened series (from LoadObservedOpenedCohort / member panel)
# plus the treated flag, using the CANONICAL draws root (default INDIR_DRAWS). EVERY diagnostic and the
# cyborg builder use THIS, not the analysis panel (which counts outside-contributor opens the model
# never predicts -- 16% of cells / 19% of total volume differ).
suppressMessages({library(tidyverse); library(arrow)})
src <- readLines("source/analysis/model_event_study/estimate_model_event_study.R")
src <- src[!grepl("Main\\(\\)", src)]
eval(parse(text = paste(src, collapse = "\n")), envir = globalenv())
SUB_SAMPLES <<- c("exact1")
sd <- LoadSampleData("exact1")

out <- sd$actual %>%
  mutate(quasi_event_time = time_index - quasi_treatment_group,
         is_treated = treatment_group != 0) %>%
  select(repo_name, quasi_event_time, is_treated, pull_request_opened)
write_csv(out, "issue/improve_latent/results/actual_opened_exact1.csv")
cat("wrote actual_opened_exact1.csv:", nrow(out), "cells,",
    n_distinct(out$repo_name), "repos, total opened =", sum(out$pull_request_opened), "\n")
cat("event-time range:", range(out$quasi_event_time), "\n")
