library(jsonlite)

if (!exists("LoadProjectConfig", mode = "function")) {
  source("source/lib/R/config_loaders.R")
}
if (!exists("PCGroupsConfig", mode = "function")) {
  source("source/lib/R/pc_groups.R")
}

pipeline_inputs     <- LoadProjectConfig("source/lib/config/pipeline_inputs.json")
feature_variables   <- LoadProjectConfig("source/lib/config/feature_variables.json")
outcome_variables   <- LoadProjectConfig("source/lib/config/outcome_variables.json")
analysis_parameters <- LoadProjectConfig("source/lib/config/analysis_parameters.json")
plot_settings       <- LoadProjectConfig("source/lib/config/plotting.json")

SEED           <- analysis_parameters$seed
set.seed(SEED)
PC_INCLUSION_NA_THRESHOLD   <- analysis_parameters$pc_inclusion_na_threshold
FOREST_FEATURE_NA_THRESHOLD <- analysis_parameters$forest_feature_na_threshold
NA_THRESHOLD   <- FOREST_FEATURE_NA_THRESHOLD
NORM_OPTIONS   <- unlist(analysis_parameters$norm_options, use.names = FALSE)
N_FOLDS        <- analysis_parameters$n_folds
N_TREES        <- analysis_parameters$trees_in_forest_event_study
MAX_EVENT_TIME <- analysis_parameters$max_event_time
MIN_EVENT_TIME <- -MAX_EVENT_TIME
PNG_NCOL       <- plot_settings$png_ncol

IMPORTANCE_TYPES       <- ExtractConfigValues(pipeline_inputs$importance_types)
ROLLING_LABELS         <- paste0("rolling", ExtractConfigValues(pipeline_inputs$rolling_periods))
ROLLING_PERIOD         <- ExtractConfigValues(pipeline_inputs$rolling_periods)[[1]]
QUALIFIED_SAMPLES      <- ExtractConfigValues(pipeline_inputs$qualified_samples)
CONTROL_GROUPS         <- ExtractConfigValues(pipeline_inputs$control_groups)
CONTROL_GROUP          <- CONTROL_GROUPS[[1]]
COVAR_TYPES            <- ExtractConfigValues(pipeline_inputs$feature_sets)
EVENT_STUDY_SPLITS     <- ExtractConfigValues(pipeline_inputs$event_study_splits)
FOREST_TRAINING_OUTCOME <- ForestTrainingOutcome(pipeline_inputs)

pc_groups_cfg <- PCGroupsConfig(feature_variables)
PC_GROUPS <- lapply(pc_groups_cfg, function(group_cfg) group_cfg$vars)

OUTCOMES <- FlattenConfigValues(outcome_variables, include_extended = FALSE)

AGGREGATED_SAMPLES <- list(
  exact_1_2 = c("exact1", "exact2")
)
