# exact1 cyborg event-study runner with options: restrict (full|win|support) to align the actual's
# estimation window with the draws' [-5,5], and dropK to drop the K largest projects (by pre-period
# [-5,-1] mean opened) on top of the [1,99] trim. Reuses the REAL FitEventStudy/DrawPointEstimates/
# SummariseBand. Usage: Rscript run_es_opt.R <candidate_root> <label> [restrict=full] [dropK=0]
library(tidyverse)
library(arrow)

args <- commandArgs(trailingOnly = TRUE)
candidate_root <- args[[1]]
label          <- args[[2]]
restrict       <- if (length(args) >= 3) args[[3]] else "full"
dropK          <- if (length(args) >= 4) as.integer(args[[4]]) else 0

code <- readLines("source/analysis/model_event_study/estimate_model_event_study.R")
code <- code[!grepl("^\\s*Main\\(\\)\\s*$", code)]
eval(parse(text = paste(code, collapse = "\n")), envir = globalenv())

INDIR_DRAWS    <<- candidate_root
SUB_SAMPLES    <<- c("exact1")
FIGURE_SAMPLES <<- c("exact1")

ComputeSampleBands <- function(samples_data_by_subsample, outcome, normalize) {
  sd <- samples_data_by_subsample[["exact1"]]
  if (dropK > 0) {
    pre <- sd$actual %>%
      mutate(qet = time_index - quasi_treatment_group) %>%
      filter(qet >= -5, qet < 0) %>%
      group_by(repo_name) %>% summarise(m = mean(pull_request_opened), .groups = "drop") %>%
      arrange(desc(m))
    drop_repos <- head(pre$repo_name, dropK)
    keep <- setdiff(unique(sd$skeleton$repo_name), drop_repos)
    sd <- SubsetSampleData(sd, keep)
    message("dropped top-", dropK, ": ", paste(drop_repos, collapse = ", "))
  }
  actual_df <- sd$actual
  if (restrict == "win") {
    actual_df <- actual_df %>% filter(time_index - quasi_treatment_group >= -5, time_index - quasi_treatment_group <= 5)
  } else if (restrict == "support") {
    support <- sd$draw_list[[1]] %>% distinct(repo_name, quasi_event_time)
    actual_df <- actual_df %>% mutate(quasi_event_time = time_index - quasi_treatment_group) %>%
      semi_join(support, by = c("repo_name", "quasi_event_time"))
  }
  actual_results <- FitEventStudy(actual_df, outcome, CONTROL_GROUP, "sa", normalize = normalize, make_plot = FALSE)$results
  draws <- DrawPointEstimates(sd, outcome, normalize)
  list(band_by_sample = list(exact1 = SummariseBand(draws, actual_results, KSTAR)))
}

samples_data <- setNames(lapply(SUB_SAMPLES, LoadSampleData), SUB_SAMPLES)
rows <- list()
for (normalize in c(TRUE, FALSE)) {
  band <- ComputeSampleBands(samples_data, "pull_request_opened", normalize)$band_by_sample[["exact1"]]
  rows[[length(rows) + 1]] <- band %>%
    transmute(label = label, sample = "exact1", outcome = "pull_request_opened",
              normalize = normalize, event_time, p2.5, p50, p97.5, actual)
  s <- band %>% filter(event_time >= 1, event_time <= 5)
  gap <- mean(s$actual - s$p50); cov <- mean(s$actual >= s$p2.5 & s$actual <= s$p97.5)
  cat(sprintf("  opened %s: gap=%.3f cov=%.2f\n", ifelse(normalize, "norm", "raw"), gap, cov))
}
write_csv(bind_rows(rows), file.path("issue/improve_latent/results", paste0("es_bands_", label, ".csv")))
