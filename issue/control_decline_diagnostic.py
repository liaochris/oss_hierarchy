"""Diagnostic: post-event decline in opened and merged PRs for control vs treated orgs.

Builds the event-time trajectory of opened (and total-merged) PRs, with each org
normalized to its own pre-period mean, separately for control and treated orgs, and
overlays 95% bootstrap confidence intervals on the cross-org mean and median at each
event time. The model-predicted trajectory (mean over the 500 draws per org, then the
cross-org median and mean) is overlaid on each panel.

The bootstrap resamples ORGS (with replacement), not periods within an org: at each
event-time t we observe one normalized value per org, and the CI quantifies how the
cross-org mean/median would move under a different sample of orgs.

Run from the repo root with the project env:
    source activate org_resilience && export PYTHONPATH=. && python issue/control_decline_diagnostic.py
"""
import numpy as np
import pandas as pd
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from source.lib.python.repo_utils import MakeRepoNameSafe

MATCHING_SPECS = ["exact1", "exact_1_2", "exact2"]
OUTDIR = Path("issue")
N_BOOTSTRAP = 2000
BOOTSTRAP_RNG = np.random.default_rng(0)

DRAWS_BASE = Path("drive/output/analysis/model_prediction/opened_cohort/adaptive/draws/pooled/important_degree_top3")
NON_AGGREGATED = {"exact1": ["exact1"], "exact2": ["exact2"], "exact_1_2": ["exact1", "exact2"]}

# normalization baseline = the 5 most recent pre-periods (matches the event study's NormalizeOutcome window)
PRE_LO, PRE_HI = -5, -1

OUTCOMES = {
    "opened": {"observed": ["repo_pull_request_opened"],                                       "draw": "pull_request_opened"},
    "merged": {"observed": ["repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"], "draw": "pull_request_merged"},
}

XLIM = (-10, 5)

NORMALIZATIONS = {
    "zscore": {"suffix": "", "baseline": 0.0,
               "ylabel": "z-score vs org pre-period", "title": "each org z-scored to its own pre-period"},
    "ratio":  {"suffix": "_ratio", "baseline": 1.0,
               "ylabel": "level vs org pre-period mean (=1 pre)", "title": "each org normalized by its own pre-period mean"},
}


def AnalysisPanelPath(spec):
    return Path(
        f"output/derived/analysis_panel/important_degree_top3/rolling5/{spec}/nevertreated/panel.parquet"
    )


def MemberPanelDir(spec):
    return Path(
        "drive/output/derived/model_prediction/event_time_member_panel/"
        f"opened_cohort/important_degree_top3/{spec}/nevertreated"
    )


def Main():
    for spec in MATCHING_SPECS:
        treated_repos, control_repos = ClassifyRepos(AnalysisPanelPath(spec))
        member_panel_dir = MemberPanelDir(spec)
        for outcome, cfg in OUTCOMES.items():
            model_draws = LoadModelDraws(spec, cfg["draw"])
            for normalize, norm_cfg in NORMALIZATIONS.items():
                control_summary = SummarizeProfile(CollectObservedProfiles(control_repos, member_panel_dir, cfg["observed"], normalize))
                treated_summary = SummarizeProfile(CollectObservedProfiles(treated_repos, member_panel_dir, cfg["observed"], normalize))

                control_model = SummarizeProfile(CollectModelProfiles(control_repos, model_draws, normalize))
                treated_model = SummarizeProfile(CollectModelProfiles(treated_repos, model_draws, normalize))

                for summary, group, source in [(control_summary, "control", "actual"), (treated_summary, "treated", "actual"),
                                               (control_model, "control", "model"), (treated_model, "treated", "model")]:
                    summary.insert(0, "source", source)
                    summary.insert(0, "group", group)
                combined = pd.concat([control_summary, treated_summary, control_model, treated_model], ignore_index=True)
                combined.to_csv(OUTDIR / f"control_decline_profile_{spec}_{outcome}{norm_cfg['suffix']}.csv", index=False)

                PlotProfiles(control_summary, treated_summary, control_model, treated_model,
                             len(control_repos), len(treated_repos), spec, outcome, norm_cfg)
                print(f"=== {spec} | {outcome} | {normalize} ===")
                print(combined.round(3).to_string(index=False))


def ClassifyRepos(analysis_panel_path):
    df_panel = pd.read_parquet(analysis_panel_path)
    df_base = df_panel[df_panel["quasi_event_time"] == 0][["repo_name", "num_dropouts"]].copy()
    df_base["is_treated"] = df_base["num_dropouts"] > 0
    treated_repos = set(df_base[df_base["is_treated"]]["repo_name"])
    control_repos = set(df_base[~df_base["is_treated"]]["repo_name"])
    return treated_repos, control_repos


def CollectObservedProfiles(repos, member_panel_dir, observed_cols, normalize):
    """For each org, observed PRs (sum of observed_cols) normalized by that org's pre-period."""
    values_by_event_time = {}
    for repo_name in repos:
        member_path = member_panel_dir / f"{MakeRepoNameSafe(repo_name)}.parquet"
        if not member_path.exists():
            continue
        df_member = pd.read_parquet(member_path, columns=["quasi_event_time"] + observed_cols)
        per_period = df_member.groupby("quasi_event_time")[observed_cols].first().sum(axis=1)
        AppendNormalized(per_period, values_by_event_time, normalize)
    return {event_time: np.array(values) for event_time, values in values_by_event_time.items()}


def LoadModelDraws(spec, draw_col):
    frames = []
    for sub in NON_AGGREGATED[spec]:
        draws = pd.read_parquet(DRAWS_BASE / sub / "nevertreated" / "raw_draws.parquet",
                                columns=["repo_name", "quasi_event_time", "draw_id", draw_col])
        frames.append(draws.rename(columns={draw_col: "value"}))
    return pd.concat(frames, ignore_index=True)


def CollectModelProfiles(repos, draws, normalize):
    """Normalize each DRAW by its own pre-period, then average over draws per (org, event_time).

    (normalizing the draw-mean instead would divide by a near-zero pre-period moment and explode.)"""
    d = draws[draws["repo_name"].isin(repos)]
    pre = d[d["quasi_event_time"].between(PRE_LO, PRE_HI)].groupby(["repo_name", "draw_id"])["value"]
    if normalize == "ratio":
        moments = pre.agg(pre_mean="mean").reset_index()
        d = d.merge(moments, on=["repo_name", "draw_id"])
        d = d[(d["pre_mean"] > 0) & d["pre_mean"].notna()].copy()
        d["norm"] = d["value"] / d["pre_mean"]
    else:
        moments = pre.agg(pre_mean="mean", pre_sd=lambda x: x.std(ddof=1)).reset_index()
        d = d.merge(moments, on=["repo_name", "draw_id"])
        d = d[(d["pre_sd"] > 0) & d["pre_sd"].notna()].copy()
        d["norm"] = (d["value"] - d["pre_mean"]) / d["pre_sd"]
    org_norm = d.groupby(["repo_name", "quasi_event_time"])["norm"].mean().reset_index()
    return {int(et): g["norm"].to_numpy() for et, g in org_norm.groupby("quasi_event_time")}


def AppendNormalized(per_period, values_by_event_time, normalize):
    pre_period = per_period[(per_period.index >= PRE_LO) & (per_period.index <= PRE_HI)]
    pre_mean = pre_period.mean()
    if normalize == "ratio":
        if not np.isfinite(pre_mean) or pre_mean <= 0:
            return
        normalized = per_period / pre_mean
    else:
        pre_sd = pre_period.std(ddof=1) if len(pre_period) >= 2 else np.nan
        if not np.isfinite(pre_sd) or pre_sd == 0:
            return
        normalized = (per_period - pre_mean) / pre_sd
    for event_time, value in normalized.items():
        values_by_event_time.setdefault(int(event_time), []).append(value)


def BootstrapInterval(values, statistic):
    resample_index = BOOTSTRAP_RNG.integers(0, len(values), (N_BOOTSTRAP, len(values)))
    bootstrap_statistics = statistic(values[resample_index], axis=1)
    return np.percentile(bootstrap_statistics, 2.5), np.percentile(bootstrap_statistics, 97.5)


def SummarizeProfile(values_by_event_time):
    rows = []
    for event_time in sorted(values_by_event_time):
        values = values_by_event_time[event_time]
        mean_lo, mean_hi = BootstrapInterval(values, np.mean)
        median_lo, median_hi = BootstrapInterval(values, np.median)
        rows.append({
            "quasi_event_time": event_time, "n_orgs": len(values),
            "mean": values.mean(), "mean_ci_lo": mean_lo, "mean_ci_hi": mean_hi,
            "median": np.median(values), "median_ci_lo": median_lo, "median_ci_hi": median_hi,
        })
    return pd.DataFrame(rows)


def VisibleYRange(actual_summaries, model_summaries):
    """Tightest y-range covering all plotted series within XLIM, with 5% padding."""
    lows, highs = [], []
    for summary in actual_summaries:
        window = summary[summary["quasi_event_time"].between(*XLIM)]
        lows += [window["median_ci_lo"].min(), window["mean_ci_lo"].min()]
        highs += [window["median_ci_hi"].max(), window["mean_ci_hi"].max()]
    for summary in model_summaries:
        window = summary[summary["quasi_event_time"].between(*XLIM)]
        lows += [window["median"].min(), window["mean"].min()]
        highs += [window["median"].max(), window["mean"].max()]
    lo, hi = min(lows), max(highs)
    pad = 0.05 * (hi - lo)
    return lo - pad, hi + pad


def PlotProfiles(control_summary, treated_summary, control_model, treated_model, n_control, n_treated, spec, outcome, norm_cfg):
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharex=True, sharey=True)
    panels = [
        (axes[0], control_summary, control_model, f"Control (never-treated), n={n_control}"),
        (axes[1], treated_summary, treated_model, f"Treated, n={n_treated}"),
    ]
    for ax, summary, model_summary, title in panels:
        ax.plot(summary["quasi_event_time"], summary["median"], "o-", color="C0", label="actual median org")
        ax.fill_between(summary["quasi_event_time"], summary["median_ci_lo"], summary["median_ci_hi"],
                        color="C0", alpha=0.20)
        ax.plot(summary["quasi_event_time"], summary["mean"], "s--", color="C3",
                label="actual mean org (right-skewed)")
        ax.fill_between(summary["quasi_event_time"], summary["mean_ci_lo"], summary["mean_ci_hi"],
                        color="C3", alpha=0.15)
        ax.plot(model_summary["quasi_event_time"], model_summary["median"], "^-", color="C2",
                label="model median org")
        ax.plot(model_summary["quasi_event_time"], model_summary["mean"], "v--", color="C1",
                label="model mean org")
        ax.axvline(0, color="grey", ls=":", lw=1)
        ax.axhline(norm_cfg["baseline"], color="grey", lw=0.6, alpha=0.5)
        ax.axvspan(0.5, 5.5, color="orange", alpha=0.08)
        ax.set_title(title)
        ax.set_xlabel("quasi_event_time")
        ax.legend(fontsize=8)
    axes[0].set_ylabel(f"{outcome} PRs, {norm_cfg['ylabel']}")
    axes[0].set_xlim(*XLIM)
    axes[0].set_ylim(*VisibleYRange([control_summary, treated_summary], [control_model, treated_model]))
    fig.suptitle(f"{outcome.capitalize()}-PR trajectory with 95% bootstrap CIs "
                 f"({spec}; {norm_cfg['title']})", fontsize=12)
    fig.tight_layout()
    fig.savefig(OUTDIR / f"control_decline_profile_ci_{spec}_{outcome}{norm_cfg['suffix']}.png", dpi=110)
    plt.close(fig)


if __name__ == "__main__":
    Main()
