library(jsonlite)

if (!exists("LoadProjectConfig", mode = "function")) {
  source("source/lib/helpers.R")
}

PROJECT_CONFIG_PATH <- "source/lib/project_config.json"

project_cfg         <- LoadProjectConfig(PROJECT_CONFIG_PATH)
pipeline_inputs     <- project_cfg$pipeline_inputs
feature_variables   <- project_cfg$feature_variables
outcome_variables   <- project_cfg$outcome_variables
analysis_parameters <- project_cfg$analysis_parameters
plot_settings       <- project_cfg$plotting

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
YLIM_DEFAULT   <- unlist(plot_settings$ylim_default, use.names = FALSE)
YLIM_WIDE      <- unlist(plot_settings$ylim_wide, use.names = FALSE)
YLIM_FOREST    <- unlist(plot_settings$ylim_forest, use.names = FALSE)
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

