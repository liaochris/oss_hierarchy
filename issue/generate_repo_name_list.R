library(arrow)
library(yaml)
library(tidyverse)
source("source/lib/helpers.R")

SEED <- 420
set.seed(SEED)

INDIR  <- "drive/output/derived/org_characteristics/org_panel"
OUTDIR <- "issue"

dataset <- "important_topk_exact1"
exclude_outcomes <- c("num_downloads")
rolling_panel <- "rolling5"

outcome_cfg      <- yaml.load_file(file.path(INDIR_YAML, "outcome_organization.yaml"))

outdir_dataset <- file.path(OUTDIR, dataset, rolling_panel)

panel_dataset <- gsub("_exact1", "", dataset)
panel_dataset <- gsub("_oneQual", "", panel_dataset)
num_qualified_label <- ifelse(grepl("_exact1", dataset), "num-qualified=1",
                              ifelse(grepl("_oneQual", dataset), "num-qualified>=1", "all obs"))
df_panel <- read_parquet(file.path(INDIR, panel_dataset, paste0("panel_", rolling_panel, ".parquet")))

all_outcomes <- unlist(lapply(outcome_cfg, function(x) x$main))
df_panel_common <- BuildCommonSample(df_panel, all_outcomes)

if (endsWith(dataset, "exact1")) {
  df_panel_common <- KeepSustainedImportant(df_panel_common, lb = 1, ub = 1)
} else {
  df_panel_common <- KeepSustainedImportant(df_panel_common)
}

df_panel_nevertreated  <- df_panel_common %>% filter(num_departures <= 1)
df_panel_nevertreated %>% select(repo_name) %>% unique() %>% write.csv(file.path(OUTDIR, "repo_name_list.csv"))
