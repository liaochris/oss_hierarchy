import json
from pathlib import Path

import numpy as np
import pandas as pd

from source.analysis.model_prediction.helpers import (
    BaselineNormalizedError,
    CrossOrgMean,
    CrossOrgMedian,
    CrossOrgRootMeanSquare,
    ModelPredictionCombos,
    RawTrajectoryError,
    RelativeTrajectoryError,
)
from source.lib.JMSLab.SaveData import SaveData
from source.lib.python.config_loaders import LoadAnalysisParameters, LoadPipelineInputs
from source.lib.python.repo_utils import MakeRepoNameSafe

_cfg = LoadPipelineInputs()
_ap  = LoadAnalysisParameters()

K                = _ap["max_event_time"]
POST_EVENT_TIMES = list(range(0, K + 1))
PRE_EVENT_TIMES  = list(range(-K, 0))
CANDIDATES       = ["avg_L", "avg_L_rounded", "last"]

FIRST_ROLLING_PERIOD = f"rolling{_cfg['rolling_periods']['run'][0]}"

INDIR_PANEL  = Path("output/analysis/data_prep")
INDIR_MEMBER = Path("drive/output/derived/model_prediction/event_time_member_panel")
OUTDIR_DATA  = Path("drive/output/analysis/model_prediction")
OUTDIR_TABS  = Path("output/analysis/model_prediction")


def Main():
    for combo in ModelPredictionCombos(_cfg):
        ProcessCombo(combo)


def ProcessCombo(combo):
    importance_type  = combo["importance_type"]
    qualified_sample = combo["qualified_sample"]
    control_group    = combo["control_group"]

    panel_path = INDIR_PANEL / importance_type / FIRST_ROLLING_PERIOD / qualified_sample / control_group / "panel.parquet"
    if not panel_path.exists():
        return

    treatment_panel = pd.read_parquet(panel_path, columns=["repo_name", "treatment"])
    control_repos   = treatment_panel[treatment_panel["treatment"] == 0]["repo_name"].unique()

    member_dir = INDIR_MEMBER / importance_type / qualified_sample / control_group
    records    = []
    for repo_name in control_repos:
        repo_records = LoadRepoRecords(repo_name, member_dir)
        if repo_records is None:
            continue
        records.extend(repo_records)

    if not records:
        return

    results_df = pd.DataFrame(records)
    summary    = BuildSummary(results_df, combo)

    outdir_data = OUTDIR_DATA / importance_type / qualified_sample / control_group
    outdir_tabs = OUTDIR_TABS / importance_type / qualified_sample / control_group / "tables"
    outdir_data.mkdir(parents=True, exist_ok=True)
    outdir_tabs.mkdir(parents=True, exist_ok=True)

    SaveData(results_df, ["repo_name", "candidate", "quasi_event_time"], outdir_data / "problem_flow_selection.parquet", outdir_data / "problem_flow_selection.log")
    SaveTable(summary, outdir_tabs / "problem_flow_selection.tex")
    SaveRecommendedConfig(summary, outdir_tabs.parent / "recommended_config.json")


def ComputePredictedTrajectory(pre_period_opens, candidate):
    if candidate == "avg_L":
        flow = float(pre_period_opens.mean())
        return {t: flow for t in POST_EVENT_TIMES}
    if candidate == "avg_L_rounded":
        flow = float(round(pre_period_opens.mean()))
        return {t: flow for t in POST_EVENT_TIMES}
    if candidate == "last":
        flow = float(pre_period_opens.loc[-1])
        return {t: flow for t in POST_EVENT_TIMES}
    raise ValueError(f"Unknown candidate: {candidate}")


def LoadRepoRecords(repo_name, member_dir):
    safe_name = MakeRepoNameSafe(repo_name)
    repo_path = member_dir / f"{safe_name}.parquet"
    if not repo_path.exists():
        return None

    member_panel  = pd.read_parquet(repo_path, columns=["quasi_event_time", "repo_pull_request_opened"])
    opens_series  = member_panel.drop_duplicates("quasi_event_time").set_index("quasi_event_time")["repo_pull_request_opened"]

    pre_period_opens  = opens_series[opens_series.index < 0]
    post_period_opens = opens_series[opens_series.index.isin(POST_EVENT_TIMES)]
    if pre_period_opens.empty or post_period_opens.empty:
        return None

    pre_period_mean = float(pre_period_opens.mean())
    if pre_period_mean == 0:
        return None

    records = []
    for candidate in CANDIDATES:
        try:
            predicted_trajectory = ComputePredictedTrajectory(pre_period_opens, candidate)
        except (KeyError, ValueError):
            continue

        observed_trajectory = post_period_opens.to_dict()

        relative_error_all   = RelativeTrajectoryError(predicted_trajectory, observed_trajectory, POST_EVENT_TIMES)
        raw_error_all        = RawTrajectoryError(predicted_trajectory, observed_trajectory, POST_EVENT_TIMES)
        baseline_error_all   = BaselineNormalizedError(predicted_trajectory, observed_trajectory, POST_EVENT_TIMES, pre_period_mean)

        for event_time in POST_EVENT_TIMES:
            records.append({
                "repo_name":                repo_name,
                "candidate":                candidate,
                "quasi_event_time":         event_time,
                "problem_flow_estimate":    predicted_trajectory.get(event_time, np.nan),
                "repo_pull_request_opened": observed_trajectory.get(event_time, 0),
                "pre_period_mean":          pre_period_mean,
                "relative_error_all":       relative_error_all,
                "raw_error_all":            raw_error_all,
                "baseline_error_all":       baseline_error_all,
                "relative_error_k":         RelativeTrajectoryError(predicted_trajectory, observed_trajectory, [event_time]),
                "raw_error_k":              RawTrajectoryError(predicted_trajectory, observed_trajectory, [event_time]),
                "baseline_error_k":         BaselineNormalizedError(predicted_trajectory, observed_trajectory, [event_time], pre_period_mean),
            })
    return records if records else None


def BuildSummary(results_df, combo):
    repo_level = results_df.drop_duplicates(["repo_name", "candidate"])[["repo_name", "candidate", "relative_error_all", "raw_error_all", "baseline_error_all"]]

    rows = []
    for candidate, candidate_group in repo_level.groupby("candidate"):
        rows.append({
            "candidate":                    candidate,
            "event_time_sample":            "all",
            "rms_relative_count_error":     CrossOrgRootMeanSquare(candidate_group["relative_error_all"]),
            "mean_relative_count_error":    CrossOrgMean(candidate_group["relative_error_all"]),
            "median_relative_count_error":  CrossOrgMedian(candidate_group["relative_error_all"]),
            "mean_baseline_error":          CrossOrgMean(candidate_group["baseline_error_all"]),
            "median_baseline_error":        CrossOrgMedian(candidate_group["baseline_error_all"]),
        })

    for (candidate, event_time), event_time_group in results_df.groupby(["candidate", "quasi_event_time"]):
        rows.append({
            "candidate":                    candidate,
            "event_time_sample":            f"k={int(event_time)}",
            "rms_relative_count_error":     CrossOrgRootMeanSquare(event_time_group["relative_error_k"]),
            "mean_relative_count_error":    CrossOrgMean(event_time_group["relative_error_k"]),
            "median_relative_count_error":  CrossOrgMedian(event_time_group["relative_error_k"]),
            "mean_baseline_error":          CrossOrgMean(event_time_group["baseline_error_k"]),
            "median_baseline_error":        CrossOrgMedian(event_time_group["baseline_error_k"]),
        })

    summary  = pd.DataFrame(rows)
    all_rows = summary[summary["event_time_sample"] == "all"]
    winner   = all_rows.loc[all_rows["mean_baseline_error"].abs().idxmin(), "candidate"]
    summary["selected_on_mean_baseline_all"] = (
        (summary["candidate"] == winner) & (summary["event_time_sample"] == "all")
    )
    summary["combo"] = f"{combo['importance_type']}/{combo['qualified_sample']}/{combo['control_group']}"
    return summary


def SaveTable(summary, path):
    cols = ["candidate", "event_time_sample",
            "mean_relative_count_error", "median_relative_count_error",
            "mean_baseline_error", "median_baseline_error",
            "selected_on_mean_baseline_all"]
    display = summary[cols].sort_values(["candidate", "event_time_sample"])
    path.write_text(display.to_latex(index=False, float_format="%.2f"))


def SaveRecommendedConfig(summary, path):
    all_rows = summary[summary["event_time_sample"] == "all"]
    winner   = all_rows.loc[all_rows["mean_baseline_error"].abs().idxmin()]
    config = {
        "problem_flow_rule": winner["candidate"],
        "individual_share_estimator": None,
        "contributor_partition": None,
        "selection_objective": "min_abs_mean_baseline_error_all",
        "winner_errors": {
            "mean_baseline_error_all":   round(float(winner["mean_baseline_error"]), 4),
            "median_baseline_error_all": round(float(winner["median_baseline_error"]), 4),
        },
    }
    path.write_text(json.dumps(config, indent=2))


if __name__ == "__main__":
    Main()
