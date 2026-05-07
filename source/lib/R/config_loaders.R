`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

LoadProjectConfig <- function(config_path) {
  if (!file.exists(config_path)) {
    stop("Config file not found at: ", config_path)
  }
  jsonlite::fromJSON(config_path, simplifyVector = TRUE)
}

ExtractConfigValues <- function(entry, include_extended = FALSE) {
  values <- entry$run %||% character(0)
  if (include_extended) {
    values <- c(values, entry$extended %||% character(0))
  }
  unname(unlist(values, use.names = FALSE))
}

FlattenConfigValues <- function(config_block, include_extended = FALSE) {
  unname(unlist(lapply(config_block, ExtractConfigValues,
                       include_extended = include_extended), use.names = FALSE))
}
