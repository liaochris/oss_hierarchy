"""Phase A diagnostic (issue/improve_latent, item 1.1.1.1 + the overdispersion question). On the
ESTIMATOR's trimmed CONTROL repos (opened-cohort actual), using pre-period [-5,-1] and post [1,5]:
  1. Trajectory bins: classify each org declining/stagnant/increasing from its pre-period slope; % each.
  2. Normalized post-period paths: opened / pre-mean (and z-score), aggregate and by bin -> is the
     reversion uniform or bin-specific? Does flat-forward over-predict?
  3. Overdispersion: is actual control post-period opened over-dispersed relative to the baseline
     (delta) model? Variance-to-mean ratio + PIT calibration histogram of actual within model draws.
Writes figures to issue/improve_latent/figures/phase_a/ and prints a summary.
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

RES = Path("issue/improve_latent/results")
FIG = Path("issue/improve_latent/figures/phase_a")
BASELINE_DRAWS = Path("issue/improve_latent/draws/baseline/exact1/raw_draws.parquet")
PRE = list(range(-5, 0))
POST = list(range(1, 6))
SLOPE_THRESHOLD = 0.10   # |normalized pre-period slope| per period; below => stagnant


def LoadControl():
    actual = pd.read_csv(RES / "actual_opened_exact1.csv")
    trim = pd.read_csv(RES / "trimmed_repos_exact1.csv")
    control = set(trim[~trim["is_treated"]]["repo_name"])
    a = actual[actual["repo_name"].isin(control) & actual["quasi_event_time"].between(-5, 5)]
    return a.pivot_table(index="repo_name", columns="quasi_event_time",
                         values="pull_request_opened", aggfunc="first")


def PreStats(wide):
    pre = wide[[k for k in PRE if k in wide.columns]]
    pre_mean = pre.mean(axis=1)
    pre_sd = pre.std(axis=1, ddof=1)
    times = np.array([k for k in PRE if k in wide.columns], float)
    slope = pre.apply(lambda row: np.polyfit(times[np.isfinite(row.values)],
                                             row.values[np.isfinite(row.values)], 1)[0]
                      if np.isfinite(row.values).sum() >= 2 else np.nan, axis=1)
    norm_slope = slope / pre_mean.replace(0, np.nan)
    return pre_mean, pre_sd, slope, norm_slope


def Classify(norm_slope):
    label = pd.Series("stagnant", index=norm_slope.index)
    label[norm_slope > SLOPE_THRESHOLD] = "increasing"
    label[norm_slope < -SLOPE_THRESHOLD] = "declining"
    return label


def PlotBins(label):
    counts = label.value_counts()
    pct = 100 * counts / counts.sum()
    fig, ax = plt.subplots(figsize=(5, 4))
    order = ["declining", "stagnant", "increasing"]
    ax.bar(order, [pct.get(k, 0) for k in order], color=["#e34948", "#9aa0a6", "#2a78d6"])
    for i, k in enumerate(order):
        ax.text(i, pct.get(k, 0) + 1, f"{pct.get(k,0):.0f}%\n(n={int(counts.get(k,0))})", ha="center")
    ax.set_ylabel("% of control organizations"); ax.set_title("Pre-period trajectory bins (control)")
    ax.set_ylim(0, max(pct) + 10)
    fig.tight_layout(); fig.savefig(FIG / "trajectory_bins.png", dpi=130); plt.close(fig)
    return pct


def PlotNormalizedPaths(wide, pre_mean, pre_sd, label):
    times = sorted(k for k in range(-5, 6) if k in wide.columns)
    norm_by_mean = wide[times].div(pre_mean, axis=0)
    zscore = wide[times].sub(pre_mean, axis=0).div(pre_sd.replace(0, np.nan), axis=0)
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    for ax, data, ylab, ref in [(axes[0], norm_by_mean, "opened / pre-period mean", 1.0),
                                (axes[1], zscore, "z-score (pre-period mean, sd)", 0.0)]:
        ax.plot(times, data.mean(axis=0), color="black", lw=2.5, label="all control")
        for grp, col in [("declining", "#e34948"), ("stagnant", "#9aa0a6"), ("increasing", "#2a78d6")]:
            repos = label[label == grp].index
            ax.plot(times, data.loc[repos].mean(axis=0), color=col, lw=1.8, label=grp)
        ax.axvline(0, color="grey", ls=":", lw=1); ax.axhline(ref, color="grey", ls="--", lw=0.8)
        ax.set_xlabel("event time"); ax.set_ylabel(ylab); ax.legend(fontsize=8)
    axes[0].set_title("Normalized opened path (mean over control orgs)")
    axes[1].set_title("Standardized opened path")
    fig.tight_layout(); fig.savefig(FIG / "normalized_paths.png", dpi=130); plt.close(fig)
    # flat-forward vs actual: post mean of (opened/pre_mean) -- 1.0 would be flat; <1 means reversion down
    post_ratio = norm_by_mean[[k for k in POST if k in norm_by_mean.columns]].mean(axis=0)
    return post_ratio


def Overdispersion(wide, pre_mean):
    draws = pd.read_parquet(BASELINE_DRAWS)
    draws = draws[(~draws["is_treated"]) & draws["quasi_event_time"].isin(POST)]
    model = draws.groupby(["repo_name", "quasi_event_time"])["pull_request_opened"].agg(["mean", "var"]).reset_index()
    # PIT of actual within model draws (calibration): U-shaped => actual over-dispersed vs model
    pit = []
    var_ratio_rows = []
    for k in POST:
        dk = draws[draws["quasi_event_time"] == k]
        actual_k = wide[k] if k in wide.columns else None
        if actual_k is None:
            continue
        for repo, arr in dk.groupby("repo_name")["pull_request_opened"]:
            if repo in actual_k.index and np.isfinite(actual_k[repo]):
                a = actual_k[repo]; d = arr.to_numpy()
                pit.append((np.sum(d < a) + 0.5 * np.sum(d == a)) / len(d))
        # pooled cross-repo index of dispersion (variance/mean) at k: actual vs model draw-means
        act_vals = actual_k.reindex(dk["repo_name"].unique()).dropna()
        mod_means = model[model["quasi_event_time"] == k].set_index("repo_name")["mean"].reindex(act_vals.index)
        var_ratio_rows.append({
            "k": k,
            "actual_mean": act_vals.mean(), "actual_var": act_vals.var(ddof=1),
            "actual_VMR": act_vals.var(ddof=1) / act_vals.mean() if act_vals.mean() > 0 else np.nan,
            "model_mean": mod_means.mean(),
            "model_predictive_var": (model[model.quasi_event_time == k].set_index("repo_name")["var"].reindex(act_vals.index).mean()
                                     + mod_means.var(ddof=1)),
        })
    pit = np.array(pit)
    vr = pd.DataFrame(var_ratio_rows)
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    axes[0].hist(pit, bins=20, color="#2a78d6", edgecolor="white")
    axes[0].axhline(len(pit) / 20, color="red", ls="--", label="uniform (calibrated)")
    axes[0].set_title("PIT of actual within baseline model draws\n(U-shape => actual over-dispersed)")
    axes[0].set_xlabel("PIT"); axes[0].legend(fontsize=8)
    axes[1].plot(vr["k"], vr["actual_var"], "o-", color="black", label="actual cross-repo var")
    axes[1].plot(vr["k"], vr["model_predictive_var"], "s-", color="#e34948", label="model predictive var")
    axes[1].set_xlabel("post event time"); axes[1].set_ylabel("variance"); axes[1].legend(fontsize=8)
    axes[1].set_title("Cross-repo dispersion: actual vs model")
    fig.tight_layout(); fig.savefig(FIG / "overdispersion.png", dpi=130); plt.close(fig)
    return pit, vr


def main():
    FIG.mkdir(parents=True, exist_ok=True)
    wide = LoadControl()
    pre_mean, pre_sd, slope, norm_slope = PreStats(wide)
    valid = norm_slope.dropna().index
    wide, pre_mean, pre_sd, norm_slope = wide.loc[valid], pre_mean.loc[valid], pre_sd.loc[valid], norm_slope.loc[valid]
    label = Classify(norm_slope)

    print(f"control repos (trimmed, >=2 pre points): {len(valid)}")
    pct = PlotBins(label)
    print("\nTrajectory bins (pre-period normalized slope, threshold +/-%.2f/period):" % SLOPE_THRESHOLD)
    for k in ["declining", "stagnant", "increasing"]:
        print(f"  {k:11s}: {pct.get(k,0):5.1f}%")

    post_ratio = PlotNormalizedPaths(wide, pre_mean, pre_sd, label)
    print("\nPost-period opened / pre-period mean (1.0 = flat-forward; <1 = reverts DOWN):")
    print("  all control:", {int(k): round(float(v), 3) for k, v in post_ratio.items()})
    for grp in ["declining", "stagnant", "increasing"]:
        repos = label[label == grp].index
        pr = wide.loc[repos][[k for k in POST if k in wide.columns]].div(pre_mean.loc[repos], axis=0).mean(axis=0)
        print(f"  {grp:11s}:", {int(k): round(float(v), 3) for k, v in pr.items()})

    pit, vr = Overdispersion(wide, pre_mean)
    print("\nOverdispersion (actual control post-period vs baseline delta model):")
    print(f"  PIT mass in tails [<0.05 or >0.95]: {100*np.mean((pit<0.05)|(pit>0.95)):.1f}%  (uniform => 10%)")
    print(f"  PIT mean={pit.mean():.3f} (0.5 calibrated), std={pit.std():.3f} (0.289 uniform)")
    print("  cross-repo variance actual vs model predictive by k:")
    for _, r in vr.iterrows():
        print(f"    k={int(r['k'])}: actual_var={r['actual_var']:.2f}  model_pred_var={r['model_predictive_var']:.2f}"
              f"  ratio={r['actual_var']/r['model_predictive_var']:.2f}  actual_VMR={r['actual_VMR']:.2f}")


if __name__ == "__main__":
    main()
