from pathlib import Path
import json

import pandas as pd


CONFIG_DIR = Path("source/lib/config")
MEAN_REVERSION_ESTIMATES = Path("output/derived/model_prediction/mean_reversion/mean_reversion_estimates.csv")


def LoadGlobals(json_path):
    path = Path(json_path)
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def LoadGlobalSettings(json_path=None):
    return LoadGlobals(json_path or CONFIG_DIR / "global_settings.json")


def LoadPipelineInputs(json_path=None):
    return LoadGlobals(json_path or CONFIG_DIR / "pipeline_inputs.json")


def LoadImportanceSpecifications(json_path=None):
    return LoadGlobals(json_path or CONFIG_DIR / "importance_specifications.json")


def LoadAnalysisParameters(json_path=None):
    return LoadGlobals(json_path or CONFIG_DIR / "analysis_parameters.json")


def LoadPlotSettings(json_path=None):
    return LoadGlobals(json_path or CONFIG_DIR / "plotting.json")


def LoadFeatureVariables(json_path=None):
    return LoadGlobals(json_path or CONFIG_DIR / "feature_variables.json")


def LoadOutcomeVariables(json_path=None):
    return LoadGlobals(json_path or CONFIG_DIR / "outcome_variables.json")


def LoadPaperSettings(json_path=None):
    return LoadGlobals(json_path or CONFIG_DIR / "paper_settings.json")


def LoadMeanReversionAdj(importance_type, control_group, estimates_path=None):
    # Mean-reversion adjustments {num_important_qualified: delta} for one spec, from the pooled FE-Poisson
    # estimates (delta = exp(-beta) in (0, 1], num_important_qualified==0 reference). Missing file => {}.
    path = Path(estimates_path) if estimates_path else MEAN_REVERSION_ESTIMATES
    if not path.exists():
        return {}
    estimates = pd.read_csv(path)
    matched = estimates[(estimates["importance_type"] == importance_type)
                        & (estimates["control_group"] == control_group)]
    return {int(row["num_important_qualified"]): float(row["mean_reversion_adj"]) for _, row in matched.iterrows()}


def LoadModelPredictionConfig():
    return LoadGlobals(CONFIG_DIR / "model_prediction.json")


def FlattenConfigValues(config_block, phases=("run",)):
    values = []
    for item in config_block.values():
        if not isinstance(item, dict):
            continue
        if any(phase in item for phase in phases):
            for phase in phases:
                values.extend(item.get(phase, []))
            continue
        for nested_item in item.values():
            if not isinstance(nested_item, dict):
                continue
            for phase in phases:
                values.extend(nested_item.get(phase, []))
    return values
