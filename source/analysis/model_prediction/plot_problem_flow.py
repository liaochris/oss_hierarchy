import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import gaussian_kde

from source.analysis.model_prediction.helpers import ModelPredictionCombos
from source.lib.python.config_loaders import LoadPipelineInputs

_cfg = LoadPipelineInputs()

CANDIDATES       = ["avg_L", "avg_L_rounded", "last"]
CANDIDATE_LABELS = {
    "avg_L":         r"$\hat{N}^{avg}$",
    "avg_L_rounded": r"$\hat{N}^{avg,round}$",
    "last":          r"$\hat{N}^{last}$",
}

INDIR  = Path("drive/output/analysis/model_prediction")
OUTDIR = Path("output/analysis/model_prediction")


def Main():
    for combo in ModelPredictionCombos(_cfg):
        ProcessCombo(combo)


def ProcessCombo(combo):
    importance_type  = combo["importance_type"]
    qualified_sample = combo["qualified_sample"]
    control_group    = combo["control_group"]

    data_path = INDIR / importance_type / qualified_sample / control_group / "problem_flow_selection.parquet"
    if not data_path.exists():
        return

    results_df = pd.read_parquet(data_path)
    plot_dir   = OUTDIR / importance_type / qualified_sample / control_group / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    config_path = OUTDIR / importance_type / qualified_sample / control_group / "recommended_config.json"
    if not config_path.exists():
        return
    winner = json.loads(config_path.read_text())["problem_flow_rule"]

    PlotErrorByEventTime(results_df, plot_dir / "problem_flow_error_by_k.png")
    PlotWinnerDensity(results_df, winner, plot_dir / "problem_flow_winner_density.png", xscale="linear")
    PlotWinnerDensity(results_df, winner, plot_dir / "problem_flow_winner_density_symlog.png", xscale="symlog")


def PlotErrorByEventTime(results_df, path):
    panel_configs = [
        (0, 0, "relative_error_k", "median", "Median relative error"),
        (0, 1, "relative_error_k", "mean",   "Mean relative error"),
        (1, 0, "baseline_error_k", "median", "Median baseline-normalized error (%)"),
        (1, 1, "baseline_error_k", "mean",   "Mean baseline-normalized error (%)"),
    ]
    fig, axes = plt.subplots(2, 2, figsize=(10, 7), sharex=True)

    for row, col, metric, agg, title in panel_configs:
        ax = axes[row, col]
        for candidate in CANDIDATES:
            candidate_df  = results_df[results_df["candidate"] == candidate]
            by_event_time = candidate_df.groupby("quasi_event_time")[metric].agg(agg).reset_index()
            ax.plot(by_event_time["quasi_event_time"], by_event_time[metric],
                    marker="o", label=CANDIDATE_LABELS[candidate])
        if row == 1:
            ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
            ax.set_xlabel("Event time k")
        ax.set_title(title)
        ax.legend(fontsize=8)

    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def PlotWinnerDensity(results_df, winner, path, xscale="linear"):
    winner_df = results_df[results_df["candidate"] == winner]

    all_series = winner_df.drop_duplicates("repo_name")["baseline_error_all"].dropna().values
    k_series   = {
        int(event_time): winner_df[winner_df["quasi_event_time"] == event_time]["baseline_error_k"].dropna().values
        for event_time in sorted(winner_df["quasi_event_time"].unique())
    }

    x_min = min(all_series.min(), min(v.min() for v in k_series.values() if len(v) >= 2))
    x_max = max(all_series.max(), max(v.max() for v in k_series.values() if len(v) >= 2))

    if xscale == "symlog":
        linthresh = max(1.0, abs(x_min) / 100, abs(x_max) / 100)
        x_grid = np.concatenate([
            np.linspace(x_min, -linthresh, 150),
            np.linspace(-linthresh, linthresh, 50),
            np.linspace(linthresh, x_max, 150),
        ])
    else:
        x_grid = np.linspace(x_min, x_max, 300)

    fig, ax = plt.subplots(figsize=(8, 5))

    if len(all_series) >= 2:
        ax.plot(x_grid, gaussian_kde(all_series)(x_grid), label="all", linewidth=2)

    for event_time, k_values in k_series.items():
        if len(k_values) >= 2:
            ax.plot(x_grid, gaussian_kde(k_values)(x_grid), label=f"k={event_time}", linestyle="--")

    if xscale == "symlog":
        ax.set_xscale("symlog", linthresh=linthresh)

    ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
    ax.set_xlabel("Baseline-normalized error (%)")
    ax.set_ylabel("Density")
    ax.set_title(f"Error distribution by event time — {CANDIDATE_LABELS[winner]}")
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    Main()
