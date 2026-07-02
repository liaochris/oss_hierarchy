# Per-cell binscatter of each control org's post/pre opening-rate ratio against its post-period
# opened volume, to check whether the mean-reversion ratio depends on org volume.

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from binsreg import binsreg

INDIR  = Path("output/derived/model_prediction/mean_reversion")
OUTDIR = Path("output/analysis/model_prediction/mean_reversion/diagnostics")

CELL_KEYS = ["variant", "importance_type", "qualified_sample", "control_group"]


def Main():
    org_reversion_stats = pd.read_parquet(INDIR / "org_reversion_stats.parquet")
    for cell_values, cell_stats in org_reversion_stats.groupby(CELL_KEYS):
        PlotRatioVsVolume(cell_stats, cell_values)


def PlotRatioVsVolume(cell_stats, cell_values):
    variant, importance_type, qualified_sample, control_group = cell_values
    cell_label = " / ".join(cell_values)
    volume = cell_stats["opened_post_volume"].to_numpy(dtype=float)
    ratio  = cell_stats["opened_ratio"].to_numpy(dtype=float)
    finite_mask = np.isfinite(volume) & np.isfinite(ratio)
    volume = volume[finite_mask]
    ratio  = ratio[finite_mask]
    median_ratio = float(np.median(ratio))

    fig, ax = plt.subplots(figsize=(7, 5.5))
    ax.scatter(volume, ratio, s=12, alpha=0.4, edgecolor="none", color="#3A5F8A")
    ax.axhline(1.0, color="0.6", linestyle="--", linewidth=1)
    ax.axhline(median_ratio, color="#C0392B", linewidth=1.5, label=f"median ratio = {median_ratio:.2f}")
    DrawBinscatter(ax, volume, ratio, cell_label)
    ax.set_xscale("symlog"); ax.set_yscale("symlog")
    ax.set_xlabel("Post-period pull requests opened (total count)")
    ax.set_ylabel("post/pre per-period rate ratio")
    ax.set_title(f"Pull requests opened  (n={len(ratio)})")
    ax.legend(fontsize=9, loc="upper left")
    fig.suptitle("Reversion ratio vs post-period volume", fontsize=12)
    fig.tight_layout()
    variant_dir = OUTDIR / variant
    variant_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(variant_dir / f"ratio_vs_volume_{importance_type}_{qualified_sample}_{control_group}.png", dpi=130)
    plt.close(fig)


def DrawBinscatter(ax, volume, ratio, cell_label):
    try:
        binscatter_result = binsreg(y=ratio, x=volume, ci=(3, 3), line=(3, 3))
        binscatter_plot = binscatter_result.data_plot[0]
        binned_points = binscatter_plot.dots
        confidence_intervals = binscatter_plot.ci
        trend_line = binscatter_plot.line
        ax.scatter(binned_points["x"], binned_points["fit"], color="#1F3A5F", s=25, zorder=4)
        ax.vlines(confidence_intervals["x"], confidence_intervals["ci_l"], confidence_intervals["ci_r"],
                  color="#1F3A5F", linewidth=1, zorder=3)
        ax.plot(trend_line["x"], trend_line["fit"], color="#E08214", linewidth=1.5,
                zorder=2, label="binscatter trend")
    except Exception as binsreg_error:
        print(f"binsreg failed for {cell_label}: {binsreg_error}")


if __name__ == "__main__":
    Main()
