# Export the ESTIMATOR's exact trimmed repo set (exact1) so every diagnostic uses the same sample,
# using the CANONICAL draws root (default INDIR_DRAWS). TrimOutcomeOutlierRepos: [1,99] pct of
# pre-period [-5,-1] mean pull_request_opened, bounds over ALL common repos (treated + control).
suppressMessages({library(tidyverse); library(arrow)})
src <- readLines("source/analysis/model_event_study/estimate_model_event_study.R")
src <- src[!grepl("Main\\(\\)", src)]
eval(parse(text = paste(src, collapse = "\n")), envir = globalenv())
SUB_SAMPLES <<- c("exact1")
sd <- LoadSampleData("exact1")
out <- sd$skeleton %>%
  distinct(repo_name, treatment_group) %>%
  mutate(is_treated = treatment_group != 0) %>%
  select(repo_name, is_treated)
write_csv(out, "issue/improve_latent/results/trimmed_repos_exact1.csv")
cat("wrote trimmed_repos_exact1.csv:", nrow(out), "repos;",
    sum(out$is_treated), "treated,", sum(!out$is_treated), "control\n")
