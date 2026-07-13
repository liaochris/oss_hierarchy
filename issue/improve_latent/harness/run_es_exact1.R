# exact1-only event-study runner for issue/improve_latent. Sources the REAL estimator (dropping
# Main()), restricts to the exact1 subsample, re-points INDIR_DRAWS at a candidate draws root, and
# writes the exact1 raw+norm bands. Numbers are directly comparable to the canonical band_estimates.csv
# (exact1 rows). Reuses the real FitEventStudy / DrawPointEstimates / SummariseBand at 100 draws.
#
# Usage: Rscript run_es_exact1.R <candidate_root> <label> [restrict]
library(tidyverse)
library(arrow)

args <- commandArgs(trailingOnly = TRUE)
candidate_root <- args[[1]]
label          <- args[[2]]
restrict       <- if (length(args) >= 3) args[[3]] else "full"

src_file <- "source/analysis/model_event_study/estimate_model_event_study.R"
code <- readLines(src_file)
code <- code[!grepl("^\\s*Main\\(\\)\\s*$", code)]
eval(parse(text = paste(code, collapse = "\n")), envir = globalenv())

INDIR_DRAWS    <<- candidate_root
SUB_SAMPLES    <<- c("exact1")
FIGURE_SAMPLES <<- c("exact1")

ComputeSampleBands <- function(samples_data_by_subsample, outcome, normalize) {
  sd <- samples_data_by_subsample[["exact1"]]
  actual_df <- sd$actual
  if (restrict == "win") {
    actual_df <- actual_df %>%
      filter(time_index - quasi_treatment_group >= -5, time_index - quasi_treatment_group <= 5)
  } else if (restrict == "support") {
    support <- sd$draw_list[[1]] %>% distinct(repo_name, quasi_event_time)
    actual_df <- actual_df %>%
      mutate(quasi_event_time = time_index - quasi_treatment_group) %>%
      semi_join(support, by = c("repo_name", "quasi_event_time"))
  }
  actual_results <- FitEventStudy(actual_df, outcome, CONTROL_GROUP, "sa",
                                  normalize = normalize, make_plot = FALSE)$results
  draws <- DrawPointEstimates(sd, outcome, normalize)
  band <- SummariseBand(draws, actual_results, KSTAR)
  list(band_by_sample = list(exact1 = band))
}

samples_data <- setNames(lapply(SUB_SAMPLES, LoadSampleData), SUB_SAMPLES)

rows <- list()
for (normalize in c(TRUE, FALSE)) {
  for (outcome in ES_OUTCOMES) {
    bands <- ComputeSampleBands(samples_data, outcome, normalize)
    band <- bands$band_by_sample[["exact1"]]
    rows[[length(rows) + 1]] <- band %>%
      transmute(label = label, sample = "exact1", outcome = outcome,
                normalize = normalize, event_time, p2.5, p50, p97.5, actual)
  }
}
out <- bind_rows(rows)
out_path <- file.path("issue/improve_latent/results", paste0("es_bands_", label, ".csv"))
write_csv(out, out_path)
cat("wrote", out_path, "\n")

opened <- out %>% filter(outcome == "pull_request_opened", event_time >= 1, event_time <= 5)
for (nz in c(TRUE, FALSE)) {
  s <- opened %>% filter(normalize == nz)
  gap <- mean(s$actual - s$p50)
  cov <- mean(s$actual >= s$p2.5 & s$actual <= s$p97.5)
  cat(sprintf("  opened %s: gap(actual-p50)=%.3f  coverage=%.2f\n", ifelse(nz, "norm", "raw"), gap, cov))
}
