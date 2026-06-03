NormalizePCGroupName <- function(category_name) {
  if (identical(category_name, "organizational_routines")) {
    return("problem_solving_routines")
  }
  category_name
}

PCGroupFriendlyLabel <- function(group_name) {
  label_map <- c(
    collaboration = "Collaboration score",
    knowledge_level = "Knowledge level score",
    discussion_quality = "Discussion quality score",
    investment_in_new_talent = "Investment in new talent score",
    problem_solving_routines = "Problem-solving routines score"
  )
  label_map[[group_name]] %||% paste(tools::toTitleCase(gsub("_", " ", group_name)), "score")
}

PCGroupSignFlip <- function(group_name) {
  identical(group_name, "knowledge_level")
}

PCGroupsConfig <- function(feature_cfg) {
  groups <- lapply(names(feature_cfg), function(category_name) {
    compat_name <- NormalizePCGroupName(category_name)
    vars <- FlattenConfigValues(feature_cfg[[category_name]], include_extended = FALSE)
    list(
      vars = vars,
      sign_flip = PCGroupSignFlip(compat_name),
      friendly_label = PCGroupFriendlyLabel(compat_name)
    )
  })
  names(groups) <- vapply(names(feature_cfg), NormalizePCGroupName, character(1))
  groups
}

ForestTrainingOutcome <- function(pipeline_inputs) {
  pipeline_inputs$forest_training_outcome
}
