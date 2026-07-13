"""Phase C (item 1.1.1.3): establish the ceiling of predictable post-period opened signal given ONLY
pre-period opened outcomes, out-of-sample (repo-level GroupKFold). Compares OLS(pre-mean), OLS(all
pre-features), RandomForest, GradientBoosting. Reports OOS R2 (centered, on levels and on log1p) and
the through-origin R2 that the scatter uses, plus GBM feature importances -> which pre-period transforms
carry the signal. If even the ML ceiling is ~0.4, the simple reversion rules are near-optimal and the
0.7-0.8 target is not attainable from pre-period outcomes alone.
"""
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import GroupKFold

RES = Path("issue/improve_latent/results")
PRE = list(range(-5, 0))
POST = list(range(1, 6))


def LoadControl():
    actual = pd.read_csv(RES / "actual_opened_exact1.csv")
    trim = pd.read_csv(RES / "trimmed_repos_exact1.csv")
    control = set(trim[~trim["is_treated"]]["repo_name"])
    a = actual[actual["repo_name"].isin(control) & actual["quasi_event_time"].between(-5, 5)]
    return a.pivot_table(index="repo_name", columns="quasi_event_time",
                         values="pull_request_opened", aggfunc="first")


def Features(wide):
    rows = []
    for repo, r in wide.iterrows():
        pre = np.array([r.get(k, np.nan) for k in PRE], float)
        if np.isfinite(pre).sum() < 2:
            continue
        pre_valid = pre[np.isfinite(pre)]
        t = np.arange(len(pre_valid), dtype=float)
        slope = np.polyfit(t, pre_valid, 1)[0] if len(pre_valid) >= 2 else 0.0
        mean = pre_valid.mean()
        feat = {"repo_name": repo,
                "pre_mean": mean, "pre_median": np.median(pre_valid), "pre_last": pre_valid[-1],
                "pre_first": pre_valid[0], "pre_max": pre_valid.max(), "pre_min": pre_valid.min(),
                "pre_std": pre_valid.std(ddof=1) if len(pre_valid) > 1 else 0.0,
                "pre_slope": slope, "pre_norm_slope": slope / mean if mean > 0 else 0.0,
                "pre_last_over_mean": pre_valid[-1] / mean if mean > 0 else 1.0}
        for k in POST:
            if k in wide.columns and np.isfinite(r.get(k, np.nan)):
                rows.append({**feat, "horizon": k, "post": float(r[k])})
    return pd.DataFrame(rows)


def ThroughOriginR2(actual, predicted):
    a, p = np.asarray(actual, float), np.asarray(predicted, float)
    return float(1 - np.sum((a - p) ** 2) / np.sum(a ** 2))


def CenteredR2(actual, predicted):
    a, p = np.asarray(actual, float), np.asarray(predicted, float)
    return float(1 - np.sum((a - p) ** 2) / np.sum((a - a.mean()) ** 2))


def CVPredict(model_factory, X, y, groups, n_folds=5):
    pred = np.zeros(len(y))
    gkf = GroupKFold(n_splits=n_folds)
    for train_idx, test_idx in gkf.split(X, y, groups):
        model = model_factory()
        model.fit(X[train_idx], y[train_idx])
        pred[test_idx] = model.predict(X[test_idx])
    return pred


def main():
    wide = LoadControl()
    df = Features(wide)
    feat_cols = ["pre_mean", "pre_median", "pre_last", "pre_first", "pre_max", "pre_min",
                 "pre_std", "pre_slope", "pre_norm_slope", "pre_last_over_mean", "horizon"]
    X = df[feat_cols].to_numpy(float)
    y = df["post"].to_numpy(float)
    ylog = np.log1p(y)
    groups = df["repo_name"].to_numpy()

    models = {
        "OLS(pre_mean only)": (lambda: LinearRegression(fit_intercept=False),
                               df[["pre_mean"]].to_numpy(float)),
        "OLS(all pre-feat)":  (lambda: LinearRegression(), X),
        "RandomForest":       (lambda: RandomForestRegressor(n_estimators=300, min_samples_leaf=5, n_jobs=8, random_state=0), X),
        "GradientBoosting":   (lambda: GradientBoostingRegressor(n_estimators=300, max_depth=3, learning_rate=0.05, subsample=0.8, random_state=0), X),
    }
    print(f"control repo x horizon rows: {len(df)}  (repos={df['repo_name'].nunique()})")
    print(f"{'model':22s} {'R2_level':>9s} {'R2_thruorig':>12s} {'R2_log1p':>9s}")
    for name, (factory, Xm) in models.items():
        pred = CVPredict(factory, Xm, y, groups)
        predlog = CVPredict(factory, Xm, ylog, groups)
        print(f"{name:22s} {CenteredR2(y, pred):9.3f} {ThroughOriginR2(y, pred):12.3f} {CenteredR2(ylog, predlog):9.3f}")

    gbm = GradientBoostingRegressor(n_estimators=300, max_depth=3, learning_rate=0.05, subsample=0.8, random_state=0)
    gbm.fit(X, y)
    importance = sorted(zip(feat_cols, gbm.feature_importances_), key=lambda kv: -kv[1])
    print("\nGBM feature importance (post-period opened):")
    for f, imp in importance:
        print(f"  {f:20s} {imp:.3f}")


if __name__ == "__main__":
    main()
