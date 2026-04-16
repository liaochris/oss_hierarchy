library(jsonlite)

SEED          <- 420
set.seed(SEED)
NA_THRESHOLD  <- 200
NORM_OPTIONS  <- c(TRUE)
N_FOLDS       <- 10
N_TREES       <- 2000
CONTROL_GROUP <- "nevertreated"
YLIM_DEFAULT  <- c(-1.5, 0.75)
YLIM_WIDE     <- c(-3, 1.5)
YLIM_FOREST   <- c(-4, 2)
PNG_NCOL      <- 2

_cfg           <- fromJSON("source/lib/pipeline_config.json")
DATASETS       <- _cfg$importance_types$run
ROLLING_LABELS <- paste0("rolling", _cfg$rolling_periods$run)
ROLLING_PERIOD <- _cfg$rolling_periods$run[[1]]
INDIR_PC       <- "output/analysis/data_prep"
