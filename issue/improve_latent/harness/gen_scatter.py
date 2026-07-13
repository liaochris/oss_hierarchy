"""Opened-panel prediction scatter for a candidate, via the REAL scatter code
(source/analysis/model_prediction/plot_prediction_scatter.py). Overrides only DRAWS_PATH and OUTDIR,
reuses the real trim (pooled [1,99] pre-mean, matching the event study) and through-origin OLS
(actual ~ predicted). Reports beta/se/R2 for control (primary), treated, pooled, and a drop-largest-N
robustness sweep on the opened panel. Writes the org figure PNG + a per-candidate scatter stats CSV.

Usage: python gen_scatter.py <tag>   (draws under issue/improve_latent/draws/<tag>/exact1)
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

import source.analysis.model_prediction.plot_prediction_scatter as pps

DRAWS = Path("issue/improve_latent/draws")
FIG   = Path("issue/improve_latent/figures")
RES   = Path("issue/improve_latent/results")
DROP_N = [0, 1, 3, 5, 10]


def OpenedStats(org, drop_n=0):
    rows = []
    for label, subset in [("control", org[~org["is_treated"]]),
                          ("treated", org[org["is_treated"]]),
                          ("pooled", org)]:
        s = subset.sort_values("repo_pull_request_opened", ascending=False)
        if drop_n > 0:
            s = s.iloc[drop_n:]
        fit = pps.FitThroughOrigin(s["opened_predicted"], s["repo_pull_request_opened"])
        rows.append({"group": label, "drop_n": drop_n, **fit})
    return rows


def main():
    tag = sys.argv[1]
    pps.DRAWS_PATH = DRAWS / tag / "exact1" / "raw_draws.parquet"
    pps.OUTDIR = FIG / tag / "scatter"
    pps.OUTDIR.mkdir(parents=True, exist_ok=True)

    member_panel = pps.TrimOutlierRepos(pps.LoadMemberData())
    org = pps.BuildOrgPoints(member_panel)

    stats_records = []
    pps.PlotOrgFigure(org, pps.OUTDIR / "org_prediction_scatter.png", stats_records)

    all_rows = []
    for drop_n in DROP_N:
        all_rows.extend(OpenedStats(org, drop_n))
    stats = pd.DataFrame(all_rows)
    stats.insert(0, "candidate", tag)
    stats.to_csv(RES / f"scatter_opened_{tag}.csv", index=False)

    print(f"opened scatter beta (tag={tag}), actual ~ predicted through origin:")
    base = stats[stats["drop_n"] == 0].set_index("group")
    for g in ["control", "treated", "pooled"]:
        r = base.loc[g]
        print(f"  {g:8s} beta={r['beta']:.3f} se={r['se']:.3f} R2={r['r_squared']:.3f} n={int(r['n'])}")
    ctrl = stats[stats["group"] == "control"].set_index("drop_n")["beta"]
    print("  control beta by drop_n:", {int(k): round(float(v), 3) for k, v in ctrl.items()})


if __name__ == "__main__":
    main()
