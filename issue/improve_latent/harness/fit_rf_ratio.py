"""RF on the NORMALIZED target (Q2 idea, RF form): predict the reversion ratio post_k/open_mean from
pre-period opened features, then set the count mean mu = open_mean * ratio_pred. Anchoring on the
opened pre-mean (what the norm ES normalizes by) should pull the norm gap toward 0 like pooled_decay,
while the RF keeps level flexibility. NB2 dispersion alpha by MoM. Cross-fitted on control. Writes
results/nbrf_ratio.parquet for scoring via build_draws.py nbglm:.
"""
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GroupKFold

RES = Path("issue/improve_latent/results")
POST = list(range(1, 6))
FEATS = ["log_last", "log_mean", "norm_slope", "log_sd", "log_n_openers"]


def main():
    feat = pd.read_parquet(RES / "enriched_features.parquet")
    post = pd.read_parquet(RES / "enriched_post.parquet")
    long = post.merge(feat, on="repo_name")
    long["ratio"] = long["post"] / long["open_mean"].clip(lower=1e-9)
    long["horizon_f"] = long["horizon"].astype(float)
    cols = FEATS + ["horizon_f"]

    ctrl = long[~long["is_treated"]]; trt = long[long["is_treated"]]
    Xc = ctrl[cols].to_numpy(float); yc = ctrl["ratio"].to_numpy(float); groups = ctrl["repo_name"].to_numpy()
    mk = lambda: RandomForestRegressor(n_estimators=400, min_samples_leaf=5, n_jobs=8, random_state=0)
    predc = np.zeros(len(yc))
    for tr, te in GroupKFold(5).split(Xc, yc, groups):
        m = mk(); m.fit(Xc[tr], yc[tr]); predc[te] = m.predict(Xc[te])
    mu_c = np.clip(predc * ctrl["open_mean"].to_numpy(float), 0, None)
    ctrl_out = ctrl[["repo_name", "horizon"]].copy(); ctrl_out["forecast"] = mu_c

    full = mk(); full.fit(Xc, yc)
    mu_t = np.clip(full.predict(trt[cols].to_numpy(float)) * trt["open_mean"].to_numpy(float), 0, None)
    trt_out = trt[["repo_name", "horizon"]].copy(); trt_out["forecast"] = mu_t

    y_actual = ctrl["post"].to_numpy(float)
    alpha = max(float(np.sum((y_actual - mu_c) ** 2 - y_actual) / np.sum(mu_c ** 2)), 0.0)
    out = pd.concat([ctrl_out, trt_out], ignore_index=True).rename(columns={"horizon": "quasi_event_time"})
    out["nb_alpha"] = alpha
    out.to_parquet(RES / "nbrf_ratio.parquet")
    r2 = 1 - np.sum((y_actual - mu_c) ** 2) / np.sum(y_actual ** 2)
    print(f"nbrf_ratio: RF on post/open_mean, mu=open_mean*ratio; alpha={alpha:.3f}, control thru-orig R2={r2:.3f}")


if __name__ == "__main__":
    main()
