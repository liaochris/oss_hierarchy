"""Emit the cross-fitted ML forecast as a per-repo post-period table (repo_name, quasi_event_time,
forecast) so the ML CEILING model can be scored through the SAME cyborg event study + scatter as every
other candidate. Features = pre-period OUTCOMES only (pre-series stats + horizon). Control repos get
out-of-sample GroupKFold predictions; treated repos use the full-control fit. Writes results/<model>.parquet.

Usage: python fit_ml_forecast.py [rf|gbm]
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.model_selection import GroupKFold

RES = Path("issue/improve_latent/results")
PRE = list(range(-5, 0))
POST = list(range(1, 6))
FEAT_COLS = ["pre_mean", "pre_median", "pre_last", "pre_first", "pre_max", "pre_min",
             "pre_std", "pre_slope", "pre_norm_slope", "pre_last_over_mean", "horizon"]


def LoadWide():
    actual = pd.read_csv(RES / "actual_opened_exact1.csv")
    trim = pd.read_csv(RES / "trimmed_repos_exact1.csv")
    keep = set(trim["repo_name"])
    a = actual[actual["repo_name"].isin(keep) & actual["quasi_event_time"].between(-5, 5)]
    wide = a.pivot_table(index="repo_name", columns="quasi_event_time",
                         values="pull_request_opened", aggfunc="first")
    is_treated = trim.set_index("repo_name")["is_treated"]
    return wide, is_treated


def Features(wide, need_target):
    rows = []
    for repo, r in wide.iterrows():
        pre = np.array([r.get(k, np.nan) for k in PRE], float)
        if np.isfinite(pre).sum() < 2:
            continue
        v = pre[np.isfinite(pre)]
        t = np.arange(len(v), dtype=float)
        slope = np.polyfit(t, v, 1)[0]
        mean = v.mean()
        feat = {"repo_name": repo, "pre_mean": mean, "pre_median": np.median(v), "pre_last": v[-1],
                "pre_first": v[0], "pre_max": v.max(), "pre_min": v.min(),
                "pre_std": v.std(ddof=1) if len(v) > 1 else 0.0, "pre_slope": slope,
                "pre_norm_slope": slope / mean if mean > 0 else 0.0,
                "pre_last_over_mean": v[-1] / mean if mean > 0 else 1.0}
        for k in POST:
            has_target = k in wide.columns and np.isfinite(r.get(k, np.nan))
            if need_target and not has_target:
                continue
            rows.append({**feat, "horizon": k, "post": float(r[k]) if has_target else np.nan})
    return pd.DataFrame(rows)


def MakeModel(kind):
    if kind == "rf":
        return RandomForestRegressor(n_estimators=300, min_samples_leaf=5, n_jobs=8, random_state=0)
    return GradientBoostingRegressor(n_estimators=300, max_depth=3, learning_rate=0.05, subsample=0.8, random_state=0)


def main():
    kind = sys.argv[1] if len(sys.argv) > 1 else "rf"
    wide, is_treated = LoadWide()
    control_repos = set(is_treated[~is_treated].index)

    all_feat = Features(wide, need_target=False)
    control_train = Features(wide.loc[wide.index.isin(control_repos)], need_target=True)

    forecasts = []
    # control: out-of-sample GroupKFold
    gkf = GroupKFold(n_splits=5)
    Xc = control_train[FEAT_COLS].to_numpy(float); yc = control_train["post"].to_numpy(float)
    groups = control_train["repo_name"].to_numpy()
    pred = np.zeros(len(yc))
    for tr, te in gkf.split(Xc, yc, groups):
        m = MakeModel(kind); m.fit(Xc[tr], yc[tr]); pred[te] = m.predict(Xc[te])
    control_out = control_train[["repo_name", "horizon"]].copy(); control_out["forecast"] = np.clip(pred, 0, None)
    forecasts.append(control_out)
    # treated: full-control fit
    m = MakeModel(kind); m.fit(Xc, yc)
    treated_feat = all_feat[~all_feat["repo_name"].isin(control_repos)]
    tp = np.clip(m.predict(treated_feat[FEAT_COLS].to_numpy(float)), 0, None)
    treated_out = treated_feat[["repo_name", "horizon"]].copy(); treated_out["forecast"] = tp
    forecasts.append(treated_out)

    out = pd.concat(forecasts, ignore_index=True).rename(columns={"horizon": "quasi_event_time"})
    out.to_parquet(RES / f"ml_{kind}.parquet")
    print(f"wrote ml_{kind}.parquet  repos={out['repo_name'].nunique()}  rows={len(out)}")


if __name__ == "__main__":
    main()
