"""Cross-fitted pre-period-only forecasters whose pooled reversion law is learned from the CONTROL
group (5-fold by repo, so a control org is never in its own training fold; treated orgs use the
full-control fit). Each org contributes only its pre-period opened level/shape; the reversion
magnitude is a pooled object (like the delta the pipeline pools today, but learned from opened
OUTCOMES, not headcount). Writes issue/improve_latent/results/<method>.parquet with columns
repo_name, quasi_event_time, forecast -- injected into the model via build_draws.py `table:<method>`.

Methods:
  pooled_decay     : forecast_k = c_k * pre_mean, c_k = ratio-of-sums Sum(post_k)/Sum(pre_mean) over
                     train control (aggregate/level-weighted reversion curve).
  pooled_decay_med : c_k = median over train control of (post_k / pre_mean) (typical-org reversion).
  bin_decay        : c_k^{bin} by pre-period trajectory bin (declining/stagnant/increasing).
  cre_proj         : Chamberlain-Mundlak: post_k/pre_mean projected on the normalized pre-slope
                     (continuous trajectory), forecast_k = pre_mean * (a_k + b_k * norm_slope).
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize

RES = Path("issue/improve_latent/results")
PRE = list(range(-5, 0))
POST = list(range(1, 6))
N_FOLDS = 5
SLOPE_THRESHOLD = 0.10


def LoadPanel():
    actual = pd.read_csv(RES / "actual_opened_exact1.csv")
    trim = pd.read_csv(RES / "trimmed_repos_exact1.csv")
    keep = set(trim["repo_name"])
    a = actual[actual["repo_name"].isin(keep) & actual["quasi_event_time"].between(-5, 5)]
    wide = a.pivot_table(index="repo_name", columns="quasi_event_time",
                         values="pull_request_opened", aggfunc="first")
    is_treated = trim.set_index("repo_name")["is_treated"]
    feat = pd.DataFrame(index=wide.index)
    pre = wide[[k for k in PRE if k in wide.columns]]
    feat["pre_mean"] = pre.mean(axis=1)
    times = np.array([k for k in PRE if k in wide.columns], float)
    feat["slope"] = pre.apply(lambda row: np.polyfit(times[np.isfinite(row.values)],
                              row.values[np.isfinite(row.values)], 1)[0]
                              if np.isfinite(row.values).sum() >= 2 else np.nan, axis=1)
    feat["norm_slope"] = feat["slope"] / feat["pre_mean"].replace(0, np.nan)
    feat["last"] = wide[-1] if -1 in wide.columns else pre.iloc[:, -1]
    feat["is_treated"] = is_treated.reindex(feat.index).fillna(False)
    feat = feat[feat["pre_mean"] > 0].dropna(subset=["norm_slope"])
    post = wide[[k for k in POST if k in wide.columns]]
    return feat, post


def Bin(norm_slope):
    return np.where(norm_slope > SLOPE_THRESHOLD, "increasing",
                    np.where(norm_slope < -SLOPE_THRESHOLD, "declining", "stagnant"))


def FoldAssignments(index, n_folds):
    # deterministic repo-hash folds (no Date/random; stable across runs)
    return pd.Series([int(abs(hash(r))) % n_folds for r in index], index=index)


def FitPooledDecay(train_feat, train_post, ratio_of_sums=True, trim_top=0.0, anchor="pre_mean"):
    c = {}
    keep = train_feat.index
    if trim_top > 0:   # robust aggregate: drop the largest-pre_mean repos before the level-weighted ratio
        cutoff = train_feat["pre_mean"].quantile(1 - trim_top)
        keep = train_feat[train_feat["pre_mean"] <= cutoff].index
    for k in POST:
        if k not in train_post.columns:
            continue
        common = keep.intersection(train_post[k].dropna().index)
        anch = train_feat.loc[common, anchor]; pk = train_post.loc[common, k]
        ok = anch > 0
        c[k] = float(pk[ok].sum() / anch[ok].sum()) if ratio_of_sums else float((pk[ok] / anch[ok]).median())
    return c


def FitBinDecay(train_feat, train_post):
    train_feat = train_feat.assign(bin=Bin(train_feat["norm_slope"].to_numpy()))
    curves = {}
    for b in ["declining", "stagnant", "increasing"]:
        sub = train_feat[train_feat["bin"] == b]
        curves[b] = FitPooledDecay(sub, train_post.loc[train_post.index.intersection(sub.index)])
    return curves


def FitCREProjection(train_feat, train_post):
    coefs = {}
    for k in POST:
        if k not in train_post.columns:
            continue
        common = train_feat.index.intersection(train_post[k].dropna().index)
        y = (train_post.loc[common, k] / train_feat.loc[common, "pre_mean"]).to_numpy()
        x = train_feat.loc[common, "norm_slope"].to_numpy()
        X = np.column_stack([np.ones_like(x), x])
        coefs[k] = np.linalg.lstsq(X, y, rcond=None)[0]   # [a_k, b_k]
    return coefs


def FitCRELast(train_feat, train_post):
    # ML-informed Chamberlain-Mundlak projection: post_k ~ intercept + pre_last + pre_mean (the two levels
    # the GBM leans on), fit cross-sectionally on control. Uses the LEVELS the ML found predictive, NOT the
    # slope (which extrapolates risers up). Coefficients estimated by OLS, cross-fitted.
    coefs = {}
    for k in POST:
        if k not in train_post.columns:
            continue
        common = train_feat.index.intersection(train_post[k].dropna().index)
        y = train_post.loc[common, k].to_numpy()
        X = np.column_stack([np.ones(len(common)),
                             train_feat.loc[common, "last"].to_numpy(),
                             train_feat.loc[common, "pre_mean"].to_numpy()])
        coefs[k] = np.linalg.lstsq(X, y, rcond=None)[0]   # [intercept, b_last, b_mean]
    return coefs


def FitAR1Partial(train_feat, train_post):
    # Fama-French / OU partial adjustment: forecast_k/pre_mean = cinf + (last/pre_mean - cinf) * rho^k.
    # Fit the long-run ratio cinf and reversion rho to the MEDIAN decay curve (robust to the heavy right
    # tail of risers, which an unweighted ratio-SSE would chase into a degenerate near-unit-root fit).
    common = train_feat.index
    median_last = float((train_feat.loc[common, "last"] / train_feat.loc[common, "pre_mean"]).median())
    targets = []
    for k in POST:
        if k not in train_post.columns:
            continue
        idx = common.intersection(train_post[k].dropna().index)
        targets.append((k, float((train_post.loc[idx, k] / train_feat.loc[idx, "pre_mean"]).median())))

    def sse(theta):
        cinf, rho = theta
        return sum((mk - (cinf + (median_last - cinf) * rho ** k)) ** 2 for k, mk in targets)

    best = minimize(sse, (0.4, 0.5), method="Nelder-Mead",
                    bounds=[(0.0, 1.5), (0.01, 0.99)], options={"maxiter": 600})
    return {"cinf": float(np.clip(best.x[0], 0.0, 1.5)), "rho": float(np.clip(best.x[1], 0.01, 0.99)),
            "median_last": median_last}


def Predict(method, feat, fit):
    rows = []
    for repo, r in feat.iterrows():
        pm = r["pre_mean"]
        for k in POST:
            if method in ("pooled_decay", "pooled_decay_med", "pooled_decay_trim"):
                f = fit.get(k, 1.0) * pm
            elif method == "decay_last":
                f = fit.get(k, 1.0) * r["last"]
            elif method == "bin_decay":
                b = Bin(np.array([r["norm_slope"]]))[0]
                f = fit[b].get(k, 1.0) * pm
            elif method == "cre_proj":
                a_k, b_k = fit[k]
                f = max(a_k + b_k * r["norm_slope"], 0.0) * pm
            elif method == "cre_last":
                intercept, b_last, b_mean = fit[k]
                f = intercept + b_last * r["last"] + b_mean * pm
            elif method == "ar1_partial":
                last_ratio = r["last"] / pm if pm > 0 else 1.0
                f = (fit["cinf"] + (last_ratio - fit["cinf"]) * fit["rho"] ** k) * pm
            rows.append({"repo_name": repo, "quasi_event_time": k, "forecast": max(f, 0.0)})
    return pd.DataFrame(rows)


def Fit(method, feat, post):
    if method == "pooled_decay":     return FitPooledDecay(feat, post, ratio_of_sums=True)
    if method == "pooled_decay_med": return FitPooledDecay(feat, post, ratio_of_sums=False)
    if method == "pooled_decay_trim":return FitPooledDecay(feat, post, ratio_of_sums=True, trim_top=0.02)
    if method == "decay_last":       return FitPooledDecay(feat, post, ratio_of_sums=False, anchor="last")
    if method == "bin_decay":        return FitBinDecay(feat, post)
    if method == "cre_proj":         return FitCREProjection(feat, post)
    if method == "cre_last":         return FitCRELast(feat, post)
    if method == "ar1_partial":      return FitAR1Partial(feat, post)
    raise ValueError(method)


def main():
    method = sys.argv[1]
    feat, post = LoadPanel()
    control = feat[~feat["is_treated"]]
    folds = FoldAssignments(control.index, N_FOLDS)
    # control forecasts: cross-fitted (held-out fold)
    control_forecasts = []
    for fold in range(N_FOLDS):
        train = control[folds != fold]
        test = control[folds == fold]
        fit = Fit(method, train, post.loc[post.index.intersection(train.index)])
        control_forecasts.append(Predict(method, test, fit))
    # treated forecasts: full-control fit
    full_fit = Fit(method, control, post.loc[post.index.intersection(control.index)])
    treated_forecasts = Predict(method, feat[feat["is_treated"]], full_fit)
    out = pd.concat(control_forecasts + [treated_forecasts], ignore_index=True)
    out.to_parquet(RES / f"{method}.parquet")
    # report the learned reversion curve (full-control fit) for transparency
    print(f"method={method}  repos={out['repo_name'].nunique()}  rows={len(out)}")
    if method in ("pooled_decay", "pooled_decay_med", "pooled_decay_trim", "decay_last"):
        print("  learned c_k (full control):", {k: round(v, 3) for k, v in full_fit.items()})
    elif method == "bin_decay":
        for b, c in full_fit.items():
            print(f"  {b:11s} c_k:", {k: round(v, 3) for k, v in c.items()})
    elif method == "cre_proj":
        print("  learned [a_k, b_k] (full control):", {k: [round(x, 3) for x in v] for k, v in full_fit.items()})
    elif method == "ar1_partial":
        print(f"  learned cinf={full_fit['cinf']:.3f}  rho={full_fit['rho']:.3f}  (half-life={np.log(2)/-np.log(full_fit['rho']):.2f} periods)")


if __name__ == "__main__":
    main()
