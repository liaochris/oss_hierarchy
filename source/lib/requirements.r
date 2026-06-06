cran_packages <- c(
  "aod",
  "arrow",
  "broom",
  "did",
  "dplyr",
  "eventstudyr",
  "fixest",
  "fs",
  "grf",
  "jsonlite",
  "knitr",
  "purrr",
  "rlang",
  "stringr",
  "tibble",
  "tidyr",
  "tidyverse",
  "zeallot"
)

install_missing_cran <- function(packages) {
  missing <- packages[!vapply(packages, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing) == 0) {
    return(invisible(NULL))
  }

  install.packages(missing, repos = "https://cloud.r-project.org")
  invisible(NULL)
}

install_missing_cran(c(cran_packages, "remotes"))

if (!requireNamespace("SaveData", quietly = TRUE)) {
  remotes::install_github("gslab-econ/gslab_r", subdir = "SaveData", upgrade = "never")
}
