library(tidyverse)
library(arrow)

source("source/lib/R/config_loaders.R")
source("source/lib/R/analysis_utils.R")
source("source/lib/R/constants.R")

INDIR_PREP <- "output/derived/analysis_panel"

LoadAnalysisPanel <- function(importance_type, rolling_panel, qualified_sample, control_group) {
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
