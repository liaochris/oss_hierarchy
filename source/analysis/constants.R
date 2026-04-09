SEED          <- 420
set.seed(SEED)
NA_THRESHOLD  <- 200
NORM_OPTIONS  <- c(TRUE)
N_FOLDS       <- 10
N_TREES       <- 2000
ROLLING_PERIOD <- 5
CONTROL_GROUP <- "nevertreated"
YLIM_DEFAULT  <- c(-1.5, 0.75)
YLIM_WIDE     <- c(-3, 1.5)
YLIM_FOREST   <- c(-4, 2)
PNG_NCOL      <- 2

DATASETS       <- c("important_degree_top3")
ROLLING_LABELS <- c("rolling5")
INDIR_PC       <- "output/analysis/data_prep"
