from pathlib import Path
import json


CONFIG_DIR = Path("source/lib/config")


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
