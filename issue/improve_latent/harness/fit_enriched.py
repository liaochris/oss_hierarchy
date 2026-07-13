"""Iteration 2: (1) fit RF/GBM on the enriched pre-period-outcome features and report OOS R2 +
feature IMPORTANCES (the scores to trim by -- what matters for pull opening); (2) fit an enriched NB2
GLM spec ladder (log link), especially the multi-indicator PERMANENT-extraction + de-peaking spec that
targets the norm gap; write NB forecast tables (mean mu_{i,k} + dispersion alpha) for scoring via
build_draws.py nbglm:. Cross-fitted on control (GroupKFold); treated use the full-control fit.

Usage: python fit_enriched.py
"""
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.model_selection import GroupKFold

RES = Path("issue/improve_latent/results")
POST = list(range(1, 6))
ML_FEATURES = ["log_last", "log_mean", "norm_slope", "log_sd", "log_rev_mean", "log_merg_mean",
               "conv_merg", "conv_rev", "backlog_ratio", "gap_growth", "log_n_openers", "hhi_open",
               "top_open_share", "log_permanent", "depeak"]


def LoadLong(need_target):
    feat = pd.read_parquet(RES / "enriched_features.parquet")
    post = pd.read_parquet(RES / "enriched_post.parquet")
    long = post.merge(feat, on="repo_name", how="left") if need_target else \
        feat.assign(key=1).merge(pd.DataFrame({"horizon": POST, "key": 1}), on="key").drop(columns="key")
    return feat, long


def ThruOriginR2(a, p):
    a, p = np.asarray(a, float), np.asarray(p, float)
    return float(1 - np.sum((a - p) ** 2) / np.sum(a ** 2))


def Design(df, spec):
    cols = {"intercept": np.ones(len(df))}
    for f in spec:
        cols[f] = df[f].to_numpy(float)
    for k in POST[1:]:
        cols[f"h{k}"] = (df["horizon"] == k).astype(float).to_numpy()
    return np.column_stack(list(cols.values())), list(cols.keys())


def FitPoisson(X, y):
    return sm.GLM(y, X, family=sm.families.Poisson()).fit(maxiter=200)


def CVPoissonR2(long_ctrl, spec):
    X, _ = Design(long_ctrl, spec)
    y = long_ctrl["post"].to_numpy(float)
    groups = long_ctrl["repo_name"].to_numpy()
    pred = np.zeros(len(y))
    for tr, te in GroupKFold(5).split(X, y, groups):
        pred[te] = FitPoisson(X[tr], y[tr]).predict(X[te])
    return ThruOriginR2(y, pred)


def MLImportances(long_ctrl):
    X = long_ctrl[ML_FEATURES + ["horizon"]].to_numpy(float)
    y = long_ctrl["post"].to_numpy(float)
    groups = long_ctrl["repo_name"].to_numpy()
    out = {}
    for name, mk in [("RandomForest", lambda: RandomForestRegressor(n_estimators=300, min_samples_leaf=5, n_jobs=8, random_state=0)),
                     ("GradientBoosting", lambda: GradientBoostingRegressor(n_estimators=300, max_depth=3, learning_rate=0.05, subsample=0.8, random_state=0))]:
        pred = np.zeros(len(y))
        for tr, te in GroupKFold(5).split(X, y, groups):
            m = mk(); m.fit(X[tr], y[tr]); pred[te] = m.predict(X[te])
        out[name] = ThruOriginR2(y, pred)
    gbm = GradientBoostingRegressor(n_estimators=300, max_depth=3, learning_rate=0.05, subsample=0.8, random_state=0)
    gbm.fit(X, y)
    imp = sorted(zip(ML_FEATURES + ["horizon"], gbm.feature_importances_), key=lambda kv: -kv[1])
    return out, imp


def WriteNBTable(feat, long_ctrl, long_all_treated, spec, name):
    X, names = Design(long_ctrl, spec)
    y = long_ctrl["post"].to_numpy(float)
    groups = long_ctrl["repo_name"].to_numpy()
    pred = np.zeros(len(y))
    for tr, te in GroupKFold(5).split(X, y, groups):
        pred[te] = FitPoisson(X[tr], y[tr]).predict(X[te])
    ctrl_out = long_ctrl[["repo_name", "horizon"]].copy(); ctrl_out["forecast"] = np.clip(pred, 0, None)
    full = FitPoisson(X, y)
    Xt, _ = Design(long_all_treated, spec)
    trt_out = long_all_treated[["repo_name", "horizon"]].copy(); trt_out["forecast"] = np.clip(full.predict(Xt), 0, None)
    alpha = float(sm.NegativeBinomial(y, X).fit(disp=0, maxiter=300).params[-1])
    out = pd.concat([ctrl_out, trt_out], ignore_index=True).rename(columns={"horizon": "quasi_event_time"})
    out["nb_alpha"] = alpha
    out.to_parquet(RES / f"{name}.parquet")
    return dict(zip(names, [round(float(b), 3) for b in full.params])), alpha, ThruOriginR2(y, pred)


def main():
    feat, long = LoadLong(need_target=True)
    long_ctrl = long[~long["is_treated"]]
    treated_all = long[long["is_treated"]]

    ml_r2, imp = MLImportances(long_ctrl)
    print("ML OOS through-origin R2 (enriched features):", {k: round(v, 3) for k, v in ml_r2.items()})
    print("GBM feature importance (post-period opened), enriched features:")
    for f, v in imp:
        print(f"  {f:16s} {v:.3f}")

    specs = {
        "A last (baseline)":         ["log_last"],
        "E permanent + depeak":      ["log_permanent", "depeak"],
        "E + C backlog":             ["log_permanent", "depeak", "backlog_ratio", "gap_growth"],
        "E + D composition":         ["log_permanent", "depeak", "hhi_open", "log_n_openers"],
        "E + B conversion":          ["log_permanent", "depeak", "conv_merg"],
        "all families":              ["log_last", "log_permanent", "depeak", "conv_merg", "backlog_ratio",
                                      "gap_growth", "hhi_open", "log_n_openers"],
    }
    print("\nPoisson GLM (log link) OOS through-origin R2 by spec (baseline last-only = 0.443):")
    for name, spec in specs.items():
        print(f"  {name:24s} R2={CVPoissonR2(long_ctrl, spec):.3f}")

    # Norm-preserving reversion: keep the OPENED-MEAN anchor (what the norm ES normalizes by) but let the
    # reversion depth depend on n_openers (breadth -> sustainability; GBM importance #2). forecast_k =
    # open_mean * (a_k + b_k*log_n_openers), cross-fitted OLS of the reversion ratio on control.
    ctrl_feat = feat[~feat["is_treated"]]
    folds = pd.Series([int(abs(hash(r))) % 5 for r in ctrl_feat["repo_name"]], index=ctrl_feat.index)
    post_by = long_ctrl.set_index(["repo_name", "horizon"])["post"]
    ratio_rows = []
    for fold in range(5):
        train = ctrl_feat[folds != fold]; test = ctrl_feat[folds == fold]
        coef = {}
        for k in POST:
            tr = train[train["repo_name"].isin(post_by.xs(k, level="horizon").index)]
            y = np.array([post_by.get((r, k)) / m for r, m in zip(tr["repo_name"], tr["open_mean"]) if (r, k) in post_by.index])
            x = tr["log_n_openers"].to_numpy()[:len(y)]
            X = np.column_stack([np.ones_like(x), x])
            coef[k] = np.linalg.lstsq(X, y, rcond=None)[0]
        for _, r in test.iterrows():
            for k in POST:
                a_k, b_k = coef[k]
                ratio_rows.append({"repo_name": r["repo_name"], "quasi_event_time": k,
                                   "forecast": max((a_k + b_k * r["log_n_openers"]) * r["open_mean"], 0.0)})
    coef_full = {}
    for k in POST:
        tr = ctrl_feat[ctrl_feat["repo_name"].isin(post_by.xs(k, level="horizon").index)]
        y = np.array([post_by.get((r, k)) / m for r, m in zip(tr["repo_name"], tr["open_mean"]) if (r, k) in post_by.index])
        x = tr["log_n_openers"].to_numpy()[:len(y)]
        coef_full[k] = np.linalg.lstsq(np.column_stack([np.ones_like(x), x]), y, rcond=None)[0]
    for _, r in feat[feat["is_treated"]].iterrows():
        for k in POST:
            a_k, b_k = coef_full[k]
            ratio_rows.append({"repo_name": r["repo_name"], "quasi_event_time": k,
                               "forecast": max((a_k + b_k * r["log_n_openers"]) * r["open_mean"], 0.0)})
    pd.DataFrame(ratio_rows).to_parquet(RES / "decay_openers.parquet")
    print("\nwrote decay_openers.parquet (opened-mean anchor x n_openers-modulated reversion). c_k coefs [a,b]:",
          {k: [round(float(x), 3) for x in v] for k, v in coef_full.items()})

    print("\nWriting NB tables for the permanent-extraction specs:")
    for spec, name in [(["log_permanent"], "glm_perm_only"),
                       (["log_permanent", "depeak"], "glm_perm"),
                       (["log_permanent", "log_n_openers"], "glm_perm_openers"),
                       (["log_permanent", "depeak", "hhi_open", "log_n_openers"], "glm_perm_comp")]:
        coefs, alpha, r2 = WriteNBTable(feat, long_ctrl, treated_all, spec, name)
        print(f"  {name:18s} alpha={alpha:.3f}  OOS_R2={r2:.3f}  coefs={coefs}")


if __name__ == "__main__":
    main()
