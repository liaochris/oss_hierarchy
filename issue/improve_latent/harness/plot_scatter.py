"""Opened-panel prediction scatter that KEEPS zero cells, on a symlog scale (linear in [0,1], log
beyond) so predicted=0 / actual=0 points are visible at the axes. Reuses the real BuildOrgPoints /
TrimOutlierRepos / FitThroughOrigin (source untouched); the only change vs the pipeline figure is the
axis scale and that no positivity filter is applied -- beta/R2 are the through-origin fit on ALL cells.
Writes figures/<tag>/scatter/org_prediction_scatter.png (replacing the log-log positive-only version).

Usage: python plot_scatter.py <tag> [<tag> ...]
"""
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

import source.analysis.model_prediction.plot_prediction_scatter as pps

DRAWS = Path("issue/improve_latent/draws")
FIG = Path("issue/improve_latent/figures")
COLORS = {"control": "#2a78d6", "treated": "#e34948"}


def PlotTag(tag, member_panel):
    pps.DRAWS_PATH = DRAWS / tag / "exact1" / "raw_draws.parquet"
    org = pps.BuildOrgPoints(member_panel)
    fig, ax = plt.subplots(figsize=(6.2, 6.2))
    for label, mask in [("control", ~org["is_treated"]), ("treated", org["is_treated"])]:
        sub = org[mask]
        ax.scatter(sub["opened_predicted"], sub["repo_pull_request_opened"],
                   s=9, alpha=0.18, color=COLORS[label], linewidths=0, label=label)
    hi = float(max(org["opened_predicted"].max(), org["repo_pull_request_opened"].max()))
    ax.plot([0, hi], [0, hi], color="#4a4a4a", ls="--", lw=1, label="45$^\\circ$")
    ax.set_xscale("symlog", linthresh=1); ax.set_yscale("symlog", linthresh=1)
    ax.set_xlim(-0.3, hi * 1.3); ax.set_ylim(-0.3, hi * 1.3)

    lines = []
    for label in ["control", "treated", "pooled"]:
        sub = org if label == "pooled" else org[org["is_treated"] == (label == "treated")]
        s = pps.FitThroughOrigin(sub["opened_predicted"], sub["repo_pull_request_opened"])
        n0 = int(((sub["opened_predicted"] == 0) | (sub["repo_pull_request_opened"] == 0)).sum())
        lines.append(f"{label}: β={s['beta']:.2f} R²={s['r_squared']:.2f} n={s['n']} (zeros={n0})")
    ax.text(0.03, 0.97, "\n".join(lines), transform=ax.transAxes, va="top", fontsize=8,
            bbox=dict(boxstyle="round", fc="white", alpha=0.85))
    ax.set_xlabel("predicted opened (symlog)"); ax.set_ylabel("actual opened (symlog)")
    ax.set_title(f"Opened: predicted vs actual, incl. zeros ({tag})", fontsize=10)
    ax.legend(fontsize=8, loc="lower right")
    out = FIG / tag / "scatter"; out.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(); fig.savefig(out / "org_prediction_scatter.png", dpi=140); plt.close(fig)
    print(f"  wrote {out/'org_prediction_scatter.png'}  ("
          f"control β={pps.FitThroughOrigin(org[~org['is_treated']]['opened_predicted'], org[~org['is_treated']]['repo_pull_request_opened'])['beta']:.3f})")


def main():
    member_panel = pps.TrimOutlierRepos(pps.LoadMemberData())   # loaded once, reused for all tags
    for tag in sys.argv[1:]:
        PlotTag(tag, member_panel)


if __name__ == "__main__":
    main()
