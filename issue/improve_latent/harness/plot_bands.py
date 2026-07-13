"""Plot the cyborg event-study band (actual vs modeled-control p50 with [p2.5,p97.5]) for
pull_request_opened, as a 2x3 grid: rows = raw / norm, cols = three configs --
  (1) full window  (actual SA-fit on the whole panel; model on [-5,5])   -- es_bands_<tag>_ctrliso.csv
  (2) window-aligned [-5,5]  (Q1; both arms on [-5,5])                     -- es_bands_<tag>_win_d0.csv
  (3) [-5,5] + drop top-3 largest projects (Q2)                           -- es_bands_<tag>_win_d3.csv
-> issue/improve_latent/figures/<tag>/es_band_opened.png. Falls back to whatever CSVs exist.

Usage: python plot_bands.py <tag> [<tag> ...]
"""
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

RES = Path("issue/improve_latent/results")
FIG = Path("issue/improve_latent/figures")
CONFIGS = [("full window", "es_bands_{tag}_ctrliso.csv"),
           ("[-5,5] aligned", "es_bands_{tag}_win_d0.csv"),
           ("[-5,5] + drop top-3", "es_bands_{tag}_win_d3.csv")]


def PostGap(df, nz):
    s = df[(df["outcome"] == "pull_request_opened") & (df["normalize"] == nz) & (df["event_time"].between(1, 5))]
    return float((s["actual"] - s["p50"]).mean())


def PlotTag(tag):
    fig, axes = plt.subplots(2, 3, figsize=(15, 8), squeeze=False)
    for col, (cfg_name, pattern) in enumerate(CONFIGS):
        path = RES / pattern.format(tag=tag)
        if not path.exists():
            for row in range(2):
                axes[row][col].text(0.5, 0.5, f"{cfg_name}\n(not run)", ha="center", va="center")
            continue
        d = pd.read_csv(path)
        o = d[d["outcome"] == "pull_request_opened"]
        for row, (nz, lab) in enumerate([(False, "raw (levels)"), (True, "norm (z-score)")]):
            ax = axes[row][col]
            s = o[o["normalize"] == nz].sort_values("event_time")
            ax.fill_between(s["event_time"], s["p2.5"], s["p97.5"], color="#e34948", alpha=0.20, label="model 95% band")
            ax.plot(s["event_time"], s["p50"], color="#e34948", lw=2, marker="s", ms=4, label="model p50 (control)")
            ax.plot(s["event_time"], s["actual"], color="black", lw=2, marker="o", ms=4, label="actual")
            ax.axvline(0, color="grey", ls=":", lw=1); ax.axhline(0, color="grey", ls="--", lw=0.7)
            ax.set_title(f"{cfg_name}  |  opened {lab}\npost gap={PostGap(d, nz):+.2f}", fontsize=9)
            if row == 1:
                ax.set_xlabel("event time")
            if col == 0 and row == 0:
                ax.legend(fontsize=7)
    fig.suptitle(f"Cyborg event study (actual treated + modeled control): {tag}", fontsize=12)
    fig.tight_layout()
    outdir = FIG / tag; outdir.mkdir(parents=True, exist_ok=True)
    fig.savefig(outdir / "es_band_opened.png", dpi=120); plt.close(fig)
    print(f"  wrote {outdir/'es_band_opened.png'}")


def main():
    for tag in sys.argv[1:]:
        PlotTag(tag)


if __name__ == "__main__":
    main()
