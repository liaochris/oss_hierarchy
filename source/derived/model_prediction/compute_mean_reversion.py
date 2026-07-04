# Mean-reversion adjustment: a within-org fixed-effects Poisson on pre-period opened counts, with
# num_important_qualified==0 as the reference. mean_reversion_adj = exp(-beta) in (0,1] is the factor
# an org's opening rate reverts by when it drops from that many qualified important members down to
# zero (equivalently, having the members present raises the rate by 1/mean_reversion_adj).

from pathlib import Path

import numpy as np
import pandas as pd
import pyfixest as pf

from source.derived.analysis_panel.panel_filters import CreateCompletePanel, FilterControlGroup
from source.lib.JMSLab.SaveData import SaveData
from source.lib.JMSLab.autofill import GenerateAutofillMacros
from source.lib.python.config_loaders import LoadAnalysisParameters, LoadPaperSettings, LoadPipelineInputs

CONFIG          = LoadPipelineInputs()
PAPER_SETTINGS  = LoadPaperSettings()
MAX_EVENT_TIME  = LoadAnalysisParameters()["max_event_time"]
MIN_EVENT_TIME  = -MAX_EVENT_TIME

INDIR_ORG_PANEL = Path("drive/output/derived/org_outcomes_practices/org_panel")
OUTDIR          = Path("output/derived/model_prediction/mean_reversion")
AUTOFILL_OUTDIR = Path("output/autofill")
ROLLING_LABEL   = f"rolling{CONFIG['rolling_periods']['run'][0]}"

META_COLUMNS  = ["repo_name", "time_index", "quasi_treatment_group", "num_important_qualified", "num_departures"]
MAX_QUALIFIED = 3    # num_important_qualified is capped here; the top level means "3 or more"


def Main():
    adjustment_rows = []
    for importance_type in CONFIG["importance_types"]["run"]:
        for control_group in CONFIG["control_groups"]["run"]:
            pre_period = BuildPrePeriod(importance_type, control_group)
            adjustment_rows.extend(EstimateReversionAdj(pre_period, importance_type, control_group))

    OUTDIR.mkdir(parents=True, exist_ok=True)
    SaveData(
        pd.DataFrame(adjustment_rows),
        ["importance_type", "control_group", "num_important_qualified"],
        OUTDIR / "mean_reversion_estimates.csv",
        OUTDIR / "mean_reversion_estimates.log",
    )
    WritePaperOutputs(adjustment_rows)


def BuildPrePeriod(importance_type, control_group):
    panel_path = INDIR_ORG_PANEL / importance_type / ROLLING_LABEL / "panel.parquet"
    org_panel = pd.read_parquet(panel_path, columns=META_COLUMNS + ["pull_request_opened"])
    org_panel = FilterControlGroup(org_panel, control_group)
    org_panel = CreateCompletePanel(org_panel, ["pull_request_opened"], MIN_EVENT_TIME, MAX_EVENT_TIME)

    org_panel["quasi_event_time"] = org_panel["time_index"] - org_panel["quasi_treatment_group"]
    # Use every pre-period the org is present for; the selection Poisson benefits from the full history.
    pre_period = org_panel[org_panel["quasi_event_time"] < 0].copy()
    pre_period["num_important_qualified"] = np.minimum(pre_period["num_important_qualified"].astype(int), MAX_QUALIFIED)
    return pre_period


def EstimateReversionAdj(pre_period, importance_type, control_group):
    reversion_fit = pf.fepois("pull_request_opened ~ i(num_important_qualified, ref=0) | repo_name", data=pre_period)
    confidence_intervals = reversion_fit.confint()
    adjustment_rows = []
    for coefficient_name, log_rate_ratio in reversion_fit.coef().items():
        num_important_qualified = int(float(coefficient_name.split("::")[1]))
        adjustment_rows.append({"importance_type": importance_type, "control_group": control_group,
                                "num_important_qualified": num_important_qualified,
                                "beta": float(log_rate_ratio),
                                "beta_ci_lower": float(confidence_intervals.loc[coefficient_name].iloc[0]),
                                "beta_ci_upper": float(confidence_intervals.loc[coefficient_name].iloc[1]),
                                "mean_reversion_adj": float(np.exp(-log_rate_ratio))})
    return adjustment_rows


def WritePaperOutputs(adjustment_rows):
    # Tablefill data + autofill macros for the paper's primary specification only.
    primary = sorted(
        (row for row in adjustment_rows
         if row["importance_type"] == PAPER_SETTINGS["primary_importance_type"]
         and row["control_group"] == PAPER_SETTINGS["primary_control_group"]),
        key=lambda row: row["num_important_qualified"],
    )

    # Each L contributes three fill values in the order the table template consumes them:
    # beta and delta on the top row, then the confidence interval on the parenthesized second row.
    table_lines = ["<tab:mean_reversion>"]
    for row in primary:
        table_lines.append(f"{row['beta']:.3f}")
        table_lines.append(f"{row['mean_reversion_adj']:.3f}")
        table_lines.append(f"{row['beta_ci_lower']:.3f}, {row['beta_ci_upper']:.3f}")
    (OUTDIR / "mean_reversion_table.txt").write_text("\n".join(table_lines) + "\n", encoding="utf-8")

    # Only the macro the paper actually references: the q=1-vs-q=0 relative opening rate
    # (delta_1 as a percentage). The estimation window is now each org's full pre-period history.
    delta_by_level = {row["num_important_qualified"]: row["mean_reversion_adj"] for row in primary}
    MeanReversionRelativeRateOne = delta_by_level.get(1, float("nan")) * 100
    AUTOFILL_OUTDIR.mkdir(parents=True, exist_ok=True)
    GenerateAutofillMacros(
        [["MeanReversionRelativeRateOne"]],
        ["{:.1f}"],
        str(AUTOFILL_OUTDIR / "mean_reversion_autofill.tex"),
    )


if __name__ == "__main__":
    Main()
