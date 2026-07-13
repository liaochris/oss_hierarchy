"""Compile the full candidate scoreboard for issue/improve_latent: for each candidate, the CYBORG
event study opened raw/norm gap (actual-p50) + coverage over k=1..5, and the opened-panel scatter
through-origin beta/R2 (control, pooled) with the drop-top-10 robustness. Reads es_bands_<tag>_ctrliso.csv
and scatter_opened_<tag>.csv. Writes issue/improve_latent/results/scoreboard.csv, ranked so the primary
event-study metric (|norm gap|, then |raw gap|) leads.
"""
from pathlib import Path

import numpy as np
import pandas as pd

RES = Path("issue/improve_latent/results")
CANDIDATES = ["baseline", "flat", "vasicek_shrink", "perm_trans", "damped_trend", "cre_proj",
              "pooled_decay_trim", "cre_last", "bin_decay", "pooled_decay",
              "ar1_partial", "pooled_decay_med", "decay_last", "glm_pois", "glm_pois_trend",
              "glm_nb", "glm_twopart", "glm_offset", "glm_eqw", "nbrf", "glm_flatpost",
              "glm_eqw_lags", "glm_eqw_mean3", "glm_eqw_mean5", "glm_normmean", "glm_normz",
              "ml_gbm", "ml_rf"]
FAMILY = {"baseline": "delta (current)", "flat": "no-reversion", "vasicek_shrink": "F1 shrinkage",
          "perm_trans": "F2 perm-transitory", "damped_trend": "F5 damped-trend", "cre_proj": "F4b CRE-slope",
          "pooled_decay_trim": "F3 pooled-decay(trim)", "cre_last": "F4b CRE-last(ML)", "bin_decay": "F4a bins",
          "pooled_decay": "F3 pooled-decay", "ar1_partial": "F3 AR1-partial", "pooled_decay_med": "F3 pooled-decay(med)",
          "decay_last": "F4b decay-on-last(ML)", "glm_pois": "Poisson-GLM (RF-distilled)",
          "glm_pois_trend": "Poisson-GLM + pre-slope", "glm_nb": "NB2-GLM (mean+dispersion)",
          "glm_twopart": "NB/Poisson two-part (Q3)", "glm_offset": "NB-GLM exposure-offset (Q2)",
          "glm_eqw": "NB-GLM org-equal weights (Q4)", "nbrf": "NB with RF mean (Q1)",
          "glm_flatpost": "org-equal, SINGLE flat post level (PRIMARY)",
          "glm_eqw_lags": "anchor=5 lags", "glm_eqw_mean3": "anchor=mean(-1..-3)",
          "glm_eqw_mean5": "anchor=mean(-1..-5)", "glm_normmean": "normalized input (ratio)+offset",
          "glm_normz": "normalized input (z-score)+offset",
          "ml_gbm": "ML ceiling (GBM)", "ml_rf": "ML ceiling (RF)"}


def CyborgOpened(tag):
    path = RES / f"es_bands_{tag}_ctrliso.csv"
    if not path.exists():
        return {}
    d = pd.read_csv(path)
    o = d[d["outcome"] == "pull_request_opened"]
    out = {}
    for nz, lab in [(True, "norm"), (False, "raw")]:
        s = o[(o["normalize"] == nz) & (o["event_time"].between(1, 5))]
        out[f"{lab}_gap"] = round(float((s["actual"] - s["p50"]).mean()), 3)
        out[f"{lab}_cov"] = round(float(((s["actual"] >= s["p2.5"]) & (s["actual"] <= s["p97.5"])).mean()), 2)
    return out


def Scatter(tag):
    path = RES / f"scatter_opened_{tag}.csv"
    if not path.exists():
        return {}
    d = pd.read_csv(path)
    base = d[(d["drop_n"] == 0) & (d["group"] == "control")].iloc[0]
    drop10 = d[(d["drop_n"] == 10) & (d["group"] == "control")].iloc[0]
    return {"ctrl_beta": round(float(base["beta"]), 3), "ctrl_R2": round(float(base["r_squared"]), 3),
            "ctrl_beta_drop10": round(float(drop10["beta"]), 3)}


def main():
    rows = []
    for tag in CANDIDATES:
        cyborg, scatter = CyborgOpened(tag), Scatter(tag)
        if not cyborg and not scatter:
            continue
        rows.append({"candidate": tag, "family": FAMILY.get(tag, ""), **cyborg, **scatter})
    df = pd.DataFrame(rows)
    df["rank_key"] = df["norm_gap"].abs() + 0.03 * df["raw_gap"].abs()
    df = df.sort_values("rank_key").drop(columns="rank_key").reset_index(drop=True)
    df.to_csv(RES / "scoreboard.csv", index=False)
    cols = ["candidate", "family", "raw_gap", "raw_cov", "norm_gap", "norm_cov", "ctrl_beta", "ctrl_R2", "ctrl_beta_drop10"]
    print(df[cols].to_string(index=False))


if __name__ == "__main__":
    main()
