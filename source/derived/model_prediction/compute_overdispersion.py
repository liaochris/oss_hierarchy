# Share of organizations in the model's fitted (trimmed) control panel whose pre-period opened counts are
# over-dispersed --- i.e. modeled as negative binomial rather than Poisson. Read from the model's own family
# assignment (distribution_params) so the figure matches the opened-cohort counts the model actually consumes.

from pathlib import Path

import pandas as pd

from source.lib.JMSLab.autofill import GenerateAutofillMacros
from source.lib.python.config_loaders import LoadModelPredictionConfig, LoadPaperSettings

MODEL_PREDICTION_CONFIG = LoadModelPredictionConfig()
PAPER_SETTINGS          = LoadPaperSettings()

OUTCOME_SAMPLE         = MODEL_PREDICTION_CONFIG["outcome_samples"]["run"][0]
DISTRIBUTION    = MODEL_PREDICTION_CONFIG["distribution_types"]["run"][0]
ESTIMATION      = MODEL_PREDICTION_CONFIG["member_probability_estimation"]["run"][0]
INDIR_PARAMS    = Path("output/analysis/model_prediction")
AUTOFILL_OUTDIR = Path("output/autofill")


def Main():
    params_path = (
        INDIR_PARAMS / OUTCOME_SAMPLE / DISTRIBUTION / "parameters" / ESTIMATION
        / PAPER_SETTINGS["primary_importance_type"] / PAPER_SETTINGS["primary_qualified_sample"]
        / PAPER_SETTINGS["primary_control_group"] / "distribution_params.parquet"
    )
    distribution_params = pd.read_parquet(params_path)
    fitted_panel = distribution_params[distribution_params["fold_id"] >= 0]

    PctOrgsOverdispersed = float((fitted_panel["distribution_type"] == "negative_binomial").mean() * 100)
    AUTOFILL_OUTDIR.mkdir(parents=True, exist_ok=True)
    GenerateAutofillMacros(
        ["PctOrgsOverdispersed"],
        "{:.0f}",
        str(AUTOFILL_OUTDIR / "overdispersion_autofill.tex"),
    )


if __name__ == "__main__":
    Main()
