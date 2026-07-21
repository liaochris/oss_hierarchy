import sys
from pathlib import Path

import pandas as pd

from source.derived.analysis_panel.panel_filters import FilterControlGroup, FilterQualifiedSample
from source.lib.python.config_loaders import LoadPipelineInputs
from source.lib.JMSLab.SaveData import SaveData

INDIR  = Path("drive/output/derived/org_outcomes_practices/org_panel")
OUTDIR = Path("output/derived/analysis_panel")
CANDIDATE_SUBDIR = "candidate"

PIPELINE_INPUTS   = LoadPipelineInputs()
QUALIFIED_SAMPLES = PIPELINE_INPUTS["qualified_samples"]["run"]
CONTROL_GROUPS    = PIPELINE_INPUTS["control_groups"]["run"]

_args           = dict(a.split('=', 1) for a in sys.argv[1:])
IMPORTANCE_TYPE = _args['CL_IMPORTANCE_TYPE']
ROLLING_PERIOD  = _args['CL_ROLLING_PERIOD']


def Main():
    ProcessCombo(IMPORTANCE_TYPE, ROLLING_PERIOD)


def ProcessCombo(importance_type, rolling_period):
    panel = pd.read_parquet(INDIR / importance_type / rolling_period / "panel.parquet")
    for qualified_sample in QUALIFIED_SAMPLES:
        for control_group in CONTROL_GROUPS:
            candidate = BuildCandidateSample(panel, qualified_sample, control_group)
            outdir = OUTDIR / CANDIDATE_SUBDIR / importance_type / rolling_period / qualified_sample / control_group
            outdir.mkdir(parents=True, exist_ok=True)
            SaveData(candidate, ["repo_name", "time_index"], outdir / "panel.parquet", outdir / "panel.log")


def BuildCandidateSample(panel, qualified_sample, control_group):
    candidate = panel.copy()
    candidate = FilterQualifiedSample(candidate, qualified_sample)
    candidate = FilterControlGroup(candidate, control_group)
    candidate["quasi_event_time"] = candidate["time_index"] - candidate["quasi_treatment_group"]
    return candidate


if __name__ == "__main__":
    Main()
