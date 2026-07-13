"""Distill the RandomForest into a simple, interpretable Poisson GLM (log link) that parameterizes the
latent count MEAN directly, using the RF's key features (pre_last dominant, then pre_mean and the
last/mean ratio, with a horizon decay). Multiplicative log-link => no additive intercept to distort the
normalized event study. Poisson QMLE gives a consistent mean even under overdispersion; the NB dispersion
is handled downstream by build_draws' adaptive refit. Cross-fitted on control (GroupKFold); treated use
the full-control fit. Reports OOS R2 for nested feature specs (how much of the RF's 0.53 a few coefficients
recover) and writes results/glm_pois.parquet.

Usage: python fit_glm_forecast.py            # compare specs + write the chosen table
"""
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.model_selection import GroupKFold

RES = Path("issue/improve_latent/results")
PRE = list(range(-5, 0))
POST = list(range(1, 6))


def LoadWide():
    actual = pd.read_csv(RES / "actual_opened_exact1.csv")
    trim = pd.read_csv(RES / "trimmed_repos_exact1.csv")
    a = actual[actual["repo_name"].isin(set(trim["repo_name"])) & actual["quasi_event_time"].between(-5, 5)]
    wide = a.pivot_table(index="repo_name", columns="quasi_event_time", values="pull_request_opened", aggfunc="first")
    return wide, trim.set_index("repo_name")["is_treated"]


def LongFrame(wide, repos, need_target):
    rows = []
    for repo in repos:
        r = wide.loc[repo]
        pre = np.array([r.get(k, np.nan) for k in PRE], float)
        if np.isfinite(pre).sum() < 2:
            continue
        v = pre[np.isfinite(pre)]
        mean, last = v.mean(), v[-1]
        slope = np.polyfit(np.arange(len(v), dtype=float), v, 1)[0]
        feat = {"repo_name": repo, "log_last": np.log1p(last), "log_mean": np.log1p(mean),
                "last_over_mean": last / mean if mean > 0 else 1.0,
                "log_std": np.log1p(v.std(ddof=1) if len(v) > 1 else 0.0),
                "norm_slope": slope / mean if mean > 0 else 0.0}
        for k in POST:
            has = k in wide.columns and np.isfinite(r.get(k, np.nan))
            if need_target and not has:
                continue
            rows.append({**feat, "horizon": k, "post": float(r[k]) if has else np.nan})
    return pd.DataFrame(rows)


def DesignMatrix(df, spec):
    cols = {"intercept": np.ones(len(df))}
    for f in spec:
        cols[f] = df[f].to_numpy(float)
    for k in POST[1:]:                       # horizon dummies (k=1 reference)
        cols[f"h{k}"] = (df["horizon"] == k).astype(float).to_numpy()
    return pd.DataFrame(cols, index=df.index)


def ThruOriginR2(a, p):
    a, p = np.asarray(a, float), np.asarray(p, float)
    return float(1 - np.sum((a - p) ** 2) / np.sum(a ** 2))


def FitPoisson(X, y):
    return sm.GLM(y, X, family=sm.families.Poisson()).fit(maxiter=200)


def CVR2(df, spec):
    X = DesignMatrix(df, spec).to_numpy(float)
    y = df["post"].to_numpy(float)
    groups = df["repo_name"].to_numpy()
    pred = np.zeros(len(y))
    for tr, te in GroupKFold(n_splits=5).split(X, y, groups):
        pred[te] = FitPoisson(X[tr], y[tr]).predict(X[te])
    return ThruOriginR2(y, pred), pred


def main():
    wide, is_treated = LoadWide()
    control = [r for r in wide.index if not is_treated.get(r, False)]
    treated = [r for r in wide.index if is_treated.get(r, False)]
    control_long = LongFrame(wide, control, need_target=True)

    specs = {
        "last only":            ["log_last"],
        "last + mean":          ["log_last", "log_mean"],
        "last + mean + ratio":  ["log_last", "log_mean", "last_over_mean"],
        "last + mean + ratio + std": ["log_last", "log_mean", "last_over_mean", "log_std"],
        "last + norm_slope":    ["log_last", "norm_slope"],
    }
    print("Poisson GLM (log link), OOS through-origin R2 vs RF ceiling 0.53:")
    for name, spec in specs.items():
        r2, _ = CVR2(control_long, spec)
        print(f"  {name:30s} R2={r2:.3f}  (n_coef={1 + len(spec) + 4})")

    treated_long = LongFrame(wide, treated, need_target=False)
    yc = control_long["post"].to_numpy(float)
    groups = control_long["repo_name"].to_numpy()

    def WriteTable(spec, name):
        Xc = DesignMatrix(control_long, spec).to_numpy(float)
        pred = np.zeros(len(yc))
        for tr, te in GroupKFold(n_splits=5).split(Xc, yc, groups):
            pred[te] = FitPoisson(Xc[tr], yc[tr]).predict(Xc[te])
        control_out = control_long[["repo_name", "horizon"]].copy(); control_out["forecast"] = np.clip(pred, 0, None)
        full = FitPoisson(Xc, yc)
        Xt = DesignMatrix(treated_long, spec).to_numpy(float)
        treated_out = treated_long[["repo_name", "horizon"]].copy(); treated_out["forecast"] = np.clip(full.predict(Xt), 0, None)
        out = pd.concat([control_out, treated_out], ignore_index=True).rename(columns={"horizon": "quasi_event_time"})
        return out, full, control_out

    # glm_pois: last only (simplest AND best OOS)
    out_pois, full_pois, _ = WriteTable(["log_last"], "glm_pois")
    out_pois.to_parquet(RES / "glm_pois.parquet")
    print("\nglm_pois 'last only' coefficients (log-mean scale):",
          {n: round(float(b), 3) for n, b in zip(["intercept", "log_last"] + [f"h{k}" for k in POST[1:]], full_pois.params)})

    # glm_pois_trend: last + norm_slope (does the pre-period trend add signal?)
    out_trend, full_trend, _ = WriteTable(["log_last", "norm_slope"], "glm_pois_trend")
    out_trend.to_parquet(RES / "glm_pois_trend.parquet")
    print("glm_pois_trend coefficients:",
          {n: round(float(b), 3) for n, b in zip(["intercept", "log_last", "norm_slope"] + [f"h{k}" for k in POST[1:]], full_trend.params)})

    # glm_nb: same mean as glm_pois, but estimate the NB2 dispersion alpha (Var = mu + alpha*mu^2) by MLE.
    Xc = DesignMatrix(control_long, ["log_last"]).to_numpy(float)
    nb_res = sm.NegativeBinomial(yc, Xc).fit(disp=0, maxiter=200)
    alpha = float(nb_res.params[-1])   # last param is alpha
    out_nb = out_pois.copy(); out_nb["nb_alpha"] = alpha
    out_nb.to_parquet(RES / "glm_nb.parquet")
    print(f"glm_nb: NB2 dispersion alpha={alpha:.4f}  (implied Var/mean at mu=5 -> {1 + alpha*5:.2f}x Poisson)")
    print(f"wrote glm_pois / glm_pois_trend / glm_nb tables  ({out_pois['repo_name'].nunique()} repos)")


if __name__ == "__main__":
    main()
