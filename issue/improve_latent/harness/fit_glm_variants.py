"""Iteration 2b: four variants that change HOW the NB/Poisson mean is estimated (not the features),
each written as an nbglm table (mean mu_{i,k} + per-repo dispersion alpha) for scoring via
build_draws.py nbglm:. glm_nb (alpha=1.19 MLE, log_last) stays the standard to beat.

  nbrf        (Q1): parameterize the NB with the RandomForest's per-cell prediction as the mean;
                    alpha by NB2 method-of-moments on the RF residuals.
  glm_offset  (Q2): normalize by exposure -- log(open_mean) as a GLM OFFSET, so the model predicts
                    opened RELATIVE to the pre-mean (the norm-friendly anchor), with a last-vs-mean tilt.
  glm_eqw     (Q4): same mean spec as glm_nb (log_last) but ORG-EQUAL weights (var_weights=1/open_mean)
                    instead of the count-weighting the Poisson MLE implies -- aligns the fit with the
                    norm ES's per-org weighting.
  glm_twopart (Q3): glm_nb mean, but per-repo FAMILY by pre-period dispersion -- Poisson (alpha=0) for
                    equidispersed orgs, NB (group alpha) for overdispersed orgs (the adaptive split).

Usage: python fit_glm_variants.py
"""
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.model_selection import GroupKFold

RES = Path("issue/improve_latent/results")
POST = list(range(1, 6))


def LoadLong():
    feat = pd.read_parquet(RES / "enriched_features.parquet")
    feat["depeak_lastmean"] = feat["log_last"] - feat["log_mean"]
    feat["disp"] = np.expm1(feat["log_sd"]) ** 2 / feat["open_mean"].clip(lower=1e-9)   # index of dispersion
    post = pd.read_parquet(RES / "enriched_post.parquet")
    long = post.merge(feat, on="repo_name")
    return feat, long


def Design(df, spec, use_horizon=True):
    cols = [np.ones(len(df))] + [df[c].to_numpy(float) for c in spec]
    if use_horizon:
        for k in POST[1:]:
            cols.append((df["horizon"] == k).astype(float).to_numpy())
    return np.column_stack(cols)


def AlphaMoM(y, mu, w=None):
    w = np.ones_like(y, float) if w is None else w
    den = np.sum(w * mu ** 2)
    return max(float(np.sum(w * ((y - mu) ** 2 - y)) / den), 0.0) if den > 0 else 0.0


def ThruR2(a, p):
    a, p = np.asarray(a, float), np.asarray(p, float)
    return float(1 - np.sum((a - p) ** 2) / np.sum(a ** 2))


def MonoHorizon(params, n_spec):
    # Enforce a NON-INCREASING post-period horizon effect (reversion doesn't re-accelerate): the horizon
    # dummies are the last len(POST)-1 params [h2..h5]; clamp the cumulative sequence [0,h2,..,h5] to its
    # running minimum so the implied opened factor exp(.) is monotone non-increasing across k.
    p = params.copy()
    n_h = len(POST) - 1
    seq = np.concatenate([[0.0], p[-n_h:]])
    p[-n_h:] = np.minimum.accumulate(seq)[1:]
    return p


def Linpred(X, params, offset):
    lp = X @ params
    return np.exp(lp + (offset if offset is not None else 0.0))


def GLMPredict(long, spec, offset_col=None, weight_col=None, n_folds=5, mono=False, use_horizon=True):
    control = long[~long["is_treated"]]; treated = long[long["is_treated"]]

    def off(df):
        return np.log(df[offset_col].to_numpy(float).clip(min=1e-9)) if offset_col else None

    def wts(df):
        return (1.0 / df[weight_col].to_numpy(float).clip(min=1e-9)) if weight_col else None

    yC = control["post"].to_numpy(float); XC = Design(control, spec, use_horizon); groups = control["repo_name"].to_numpy()
    n_spec = len(spec)
    predC = np.zeros(len(yC))
    for tr, te in GroupKFold(n_folds).split(XC, yC, groups):
        m = sm.GLM(yC[tr], XC[tr], family=sm.families.Poisson(),
                   offset=(off(control)[tr] if offset_col else None),
                   var_weights=(wts(control)[tr] if weight_col else None)).fit(maxiter=300)
        pr = MonoHorizon(m.params, n_spec) if mono else m.params
        predC[te] = Linpred(XC[te], pr, off(control)[te] if offset_col else None)
    full = sm.GLM(yC, XC, family=sm.families.Poisson(), offset=off(control),
                  var_weights=wts(control)).fit(maxiter=300)
    pf = MonoHorizon(full.params, n_spec) if mono else full.params
    predT = Linpred(Design(treated, spec, use_horizon), pf, off(treated)) if len(treated) else np.array([])
    ctrl_out = control[["repo_name", "horizon"]].copy(); ctrl_out["forecast"] = np.clip(predC, 0, None)
    trt_out = treated[["repo_name", "horizon"]].copy(); trt_out["forecast"] = np.clip(predT, 0, None)
    return ctrl_out, trt_out, yC, predC, full


def StratifiedGLM(long, spec, n_strata, name, weight_cols=None, strat_col="open_mean"):
    # Separate GLM per stratum of an observable pre-period characteristic (strat_col: open_mean for SIZE,
    # theil_norm_slope for robust TREND). Each stratum gets its OWN level, horizon, NB alpha, AND weighting
    # (weight_cols[s]="open_mean" for org-equal, None for count). Segmentation on an observable -- NOT a
    # different model per horizon.
    if weight_cols is None:
        weight_cols = ["open_mean"] * n_strata
    repo_key = long.groupby("repo_name")[strat_col].first()
    edges = np.quantile(repo_key, np.linspace(0, 1, n_strata + 1))
    edges[0], edges[-1] = -np.inf, np.inf
    parts = []
    for s in range(n_strata):
        sub = long[(long[strat_col] > edges[s]) & (long[strat_col] <= edges[s + 1])]
        c, t, y, p, full = GLMPredict(sub, spec, weight_col=weight_cols[s])
        alpha = AlphaMoM(y, p)
        for df_ in (c, t):
            df_["nb_alpha"] = alpha
        parts.append(pd.concat([c, t], ignore_index=True))
        hz = np.concatenate([[0.0], np.asarray(full.params[-(len(POST) - 1):], float)])
        print(f"    stratum {s} ({strat_col} in ({edges[s]:.2f},{edges[s+1]:.2f}], {sub['repo_name'].nunique()} repos): "
              f"alpha={alpha:.2f} horizon_factor@k5={np.exp(hz[-1]):.2f}")
    out = pd.concat(parts, ignore_index=True).rename(columns={"horizon": "quasi_event_time"})
    out.to_parquet(RES / f"{name}.parquet")
    return out


def WriteConstAlpha(name, ctrl_out, trt_out, yC, predC, weight_for_alpha=None):
    alpha = AlphaMoM(yC, predC, weight_for_alpha)
    out = pd.concat([ctrl_out, trt_out], ignore_index=True).rename(columns={"horizon": "quasi_event_time"})
    out["nb_alpha"] = alpha
    out.to_parquet(RES / f"{name}.parquet")
    return alpha


def main():
    feat, long = LoadLong()

    # Q1 nbrf: RF mean + NB2 dispersion (MoM on RF residuals over control)
    rf = pd.read_parquet(RES / "ml_rf.parquet").rename(columns={"quasi_event_time": "horizon"})
    ctrl_repos = set(feat[~feat["is_treated"]]["repo_name"])
    actual = pd.read_parquet(RES / "enriched_post.parquet")
    rf_ctrl = rf[rf["repo_name"].isin(ctrl_repos)].merge(actual, on=["repo_name", "horizon"])
    alpha_rf = AlphaMoM(rf_ctrl["post"].to_numpy(float), rf_ctrl["forecast"].to_numpy(float))
    nbrf = rf.rename(columns={"horizon": "quasi_event_time"}).copy(); nbrf["nb_alpha"] = alpha_rf
    nbrf.to_parquet(RES / "nbrf.parquet")
    print(f"nbrf   : RF mean + NB2 alpha={alpha_rf:.3f}  (RF OOS R2 on control={ThruR2(rf_ctrl['post'], rf_ctrl['forecast']):.3f})")

    # Q2 glm_offset: exposure offset = log(open_mean); tilt by (log_last - log_mean)
    c, t, y, p, full = GLMPredict(long, ["depeak_lastmean"], offset_col="open_mean")
    a = WriteConstAlpha("glm_offset", c, t, y, p)
    print(f"glm_offset: offset=log(open_mean), tilt(log_last-log_mean) coef={full.params[1]:.3f}, alpha={a:.3f}, OOS_R2={ThruR2(y,p):.3f}")

    # Q4 glm_eqw: log_last mean, ORG-EQUAL weights (var_weights=1/open_mean)
    c, t, y, p, full = GLMPredict(long, ["log_last"], weight_col="open_mean")
    a = WriteConstAlpha("glm_eqw", c, t, y, p)
    print(f"glm_eqw   : log_last, org-equal weights, log_last coef={full.params[1]:.3f}, alpha={a:.3f}, OOS_R2={ThruR2(y,p):.3f}")

    # Q2/Q3 anchor variants: identical to glm_eqw (org-equal + NB + horizon) but a different pre-period
    # anchor -- 5 separate lags y_{-1..-5}; mean of the 3 most recent; mean of all 5 pre-periods.
    for spec, name in [(["log_open_m1", "log_open_m2", "log_open_m3", "log_open_m4", "log_open_m5"], "glm_eqw_lags"),
                       (["log_mean3"], "glm_eqw_mean3"),
                       (["log_mean"], "glm_eqw_mean5")]:
        c, t, y, p, full = GLMPredict(long, spec, weight_col="open_mean")
        a = WriteConstAlpha(name, c, t, y, p)
        print(f"{name:14s}: anchor={spec}, alpha={a:.3f}, OOS_R2={ThruR2(y,p):.3f}, "
              f"coefs={[round(float(b),3) for b in full.params[1:1+len(spec)]]}")

    # PRIMARY: glm_flatpost -- org-equal, single flat post level (no horizon dummies); one post indicator
    # instead of fitting each period's control outcome.
    c, t, y, p, full = GLMPredict(long, ["log_last"], weight_col="open_mean", use_horizon=False)
    a = WriteConstAlpha("glm_flatpost", c, t, y, p)
    print(f"glm_flatpost (PRIMARY): single flat post level, log_last coef={full.params[1]:.3f}, alpha={a:.3f}, OOS_R2={ThruR2(y,p):.3f}")

    # Normalized-input experiments (negative result): normalized last value + exposure offset
    for spec, name in [(["last_over_mean"], "glm_normmean"), (["last_z"], "glm_normz")]:
        c, t, y, p, full = GLMPredict(long, spec, offset_col="open_mean", weight_col="open_mean")
        a = WriteConstAlpha(name, c, t, y, p)
        print(f"{name:12s}: normalized input {spec} + offset, alpha={a:.3f}, OOS_R2={ThruR2(y,p):.3f}")

    # glm_eqw10: same as glm_eqw but 10 folds (test whether more CV helps k4-5)
    c, t, y, p, full = GLMPredict(long, ["log_last"], weight_col="open_mean", n_folds=10)
    a = WriteConstAlpha("glm_eqw10", c, t, y, p)
    print(f"glm_eqw10 : log_last, org-equal weights, 10 folds, alpha={a:.3f}, OOS_R2={ThruR2(y,p):.3f}")

    # glm_eqw_mono: org-equal weights + MONOTONE non-increasing horizon (reversion doesn't re-accelerate)
    c, t, y, p, full = GLMPredict(long, ["log_last"], weight_col="open_mean", mono=True)
    a = WriteConstAlpha("glm_eqw_mono", c, t, y, p)
    hcoef = MonoHorizon(full.params, 1)[-4:]
    print(f"glm_eqw_mono: org-equal + monotone horizon, clamped h2..h5={[round(float(x),3) for x in hcoef]}, alpha={a:.3f}, OOS_R2={ThruR2(y,p):.3f}")

    # NOTE: glm_eqw_medh / glm_eqw_flath (org-equal level + a median / count-weighted horizon) were removed
    # -- taking the horizon from a different estimation than the level is "a different model per horizon,"
    # which is not allowed. One model, one estimation.

    # Size-stratified org-equal GLM: a separate model per pre-period size group (large repos get their own,
    # stable horizon instead of inheriting the small-repo tail's rising horizon).
    print("glm_size2 (small/large, both org-equal):")
    StratifiedGLM(long, ["log_last"], 2, "glm_size2")
    print("glm_size3 (small/mid/large, both org-equal):")
    StratifiedGLM(long, ["log_last"], 3, "glm_size3")
    print("glm_mixed2 (small=org-equal glm_eqw, large=count-weighted glm_nb):")
    StratifiedGLM(long, ["log_last"], 2, "glm_mixed2", weight_cols=["open_mean", None])
    print("glm_mixed3 (small=eqw, mid=eqw, large=count):")
    StratifiedGLM(long, ["log_last"], 3, "glm_mixed3", weight_cols=["open_mean", "open_mean", None])

    # Q2+Q4 glm_offset_eqw: exposure offset=log(open_mean) AND org-equal weights (both principled; one model)
    c, t, y, p, full = GLMPredict(long, ["depeak_lastmean"], offset_col="open_mean", weight_col="open_mean")
    a = WriteConstAlpha("glm_offset_eqw", c, t, y, p)
    print(f"glm_offset_eqw: offset+org-equal weights, tilt coef={full.params[1]:.3f}, alpha={a:.3f}, OOS_R2={ThruR2(y,p):.3f}")

    # Q3 glm_twopart: log_last mean (unweighted), per-repo family Poisson (disp<=1) / NB (disp>1)
    c, t, y, p, full = GLMPredict(long, ["log_last"])
    over = feat[feat["disp"] > 1.0]["repo_name"]
    yC = long[~long["is_treated"]]; overC = yC[yC["repo_name"].isin(set(over))]
    XoverC = Design(overC, ["log_last"])
    # alpha estimated on the overdispersed control cells only
    predOver = full.predict(XoverC)
    alpha_over = AlphaMoM(overC["post"].to_numpy(float), predOver)
    disp_by_repo = feat.set_index("repo_name")["disp"].to_dict()
    out = pd.concat([c, t], ignore_index=True).rename(columns={"horizon": "quasi_event_time"})
    out["nb_alpha"] = out["repo_name"].map(lambda r: alpha_over if disp_by_repo.get(r, 0) > 1.0 else 0.0)
    out.to_parquet(RES / "glm_twopart.parquet")
    n_over = int((feat["disp"] > 1.0).sum())
    print(f"glm_twopart: {n_over}/{len(feat)} overdispersed (NB alpha={alpha_over:.3f}), rest Poisson; OOS_R2={ThruR2(y,p):.3f}")


if __name__ == "__main__":
    main()
