"""Diagnostic: post-event decline in opened PRs for control (never-treated) orgs.

Builds the event-time trajectory of opened PRs, with each org normalized to its
own pre-period mean, separately for control and treated orgs, and overlays 95%
bootstrap confidence intervals on the cross-org mean and median at each event time.

The bootstrap resamples ORGS (with replacement), not periods within an org: at each
event-time t we observe one normalized value per org, and the CI quantifies how the
cross-org mean/median would move under a different sample of orgs (cross-sectional
sampling uncertainty over the ~700 control / ~460 treated orgs).

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

ANALYSIS_PANEL = Path(
    "output/derived/analysis_panel/important_degree_top3/rolling5/exact_1_2/nevertreated/panel.parquet"
)
MEMBER_PANEL_DIR = Path(
    "drive/output/derived/model_prediction/event_time_member_panel/"
    "opened_cohort/important_degree_top3/exact_1_2/nevertreated"
)
OUTDIR = Path("issue")
N_BOOTSTRAP = 2000
BOOTSTRAP_RNG = np.random.default_rng(0)


def Main():
    treated_repos, control_repos = ClassifyRepos()
    control_summary = SummarizeProfile(CollectNormalizedOpens(control_repos))
    treated_summary = SummarizeProfile(CollectNormalizedOpens(treated_repos))

    control_summary.insert(0, "group", "control")
    treated_summary.insert(0, "group", "treated")
    combined = pd.concat([control_summary, treated_summary], ignore_index=True)
    combined.to_csv(OUTDIR / "control_decline_profile.csv", index=False)

    PlotProfiles(control_summary, treated_summary, len(control_repos), len(treated_repos))
    print(combined.round(3).to_string(index=False))


def ClassifyRepos():
    df_panel = pd.read_parquet(ANALYSIS_PANEL)
    df_base = df_panel[df_panel["quasi_event_time"] == 0][["repo_name", "num_dropouts"]].copy()
    df_base["is_treated"] = df_base["num_dropouts"] > 0
    treated_repos = set(df_base[df_base["is_treated"]]["repo_name"])
    control_repos = set(df_base[~df_base["is_treated"]]["repo_name"])
    return treated_repos, control_repos


def CollectNormalizedOpens(repos):
    """For each org, opened PRs at every event-time divided by that org's pre-period mean."""
    opens_by_event_time = {}
    for repo_name in repos:
        member_path = MEMBER_PANEL_DIR / f"{MakeRepoNameSafe(repo_name)}.parquet"
        if not member_path.exists():
            continue
        df_member = pd.read_parquet(member_path, columns=["quasi_event_time", "repo_pull_request_opened"])
        opens_per_period = df_member.groupby("quasi_event_time")["repo_pull_request_opened"].first()
        pre_period_opens = opens_per_period[opens_per_period.index < 0]
        if len(pre_period_opens) == 0 or pre_period_opens.mean() <= 0:
            continue
        normalized = opens_per_period / pre_period_opens.mean()
        for event_time, value in normalized.items():
            opens_by_event_time.setdefault(int(event_time), []).append(value)
    return {event_time: np.array(values) for event_time, values in opens_by_event_time.items()}


def BootstrapInterval(values, statistic):
    resample_index = BOOTSTRAP_RNG.integers(0, len(values), (N_BOOTSTRAP, len(values)))
    bootstrap_statistics = statistic(values[resample_index], axis=1)
    return np.percentile(bootstrap_statistics, 2.5), np.percentile(bootstrap_statistics, 97.5)


def SummarizeProfile(opens_by_event_time):
    rows = []
    for event_time in sorted(opens_by_event_time):
        values = opens_by_event_time[event_time]
        mean_lo, mean_hi = BootstrapInterval(values, np.mean)
        median_lo, median_hi = BootstrapInterval(values, np.median)
        rows.append({
            "quasi_event_time": event_time, "n_orgs": len(values),
            "mean": values.mean(), "mean_ci_lo": mean_lo, "mean_ci_hi": mean_hi,
            "median": np.median(values), "median_ci_lo": median_lo, "median_ci_hi": median_hi,
        })
    return pd.DataFrame(rows)


def PlotProfiles(control_summary, treated_summary, n_control, n_treated):
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharex=True, sharey=True)
    panels = [
        (axes[0], control_summary, f"Control (never-treated), n={n_control}"),
        (axes[1], treated_summary, f"Treated, n={n_treated}"),
    ]
    for ax, summary, title in panels:
        ax.plot(summary["quasi_event_time"], summary["median"], "o-", color="C0", label="median org")
        ax.fill_between(summary["quasi_event_time"], summary["median_ci_lo"], summary["median_ci_hi"],
                        color="C0", alpha=0.20)
        ax.plot(summary["quasi_event_time"], summary["mean"], "s--", color="C3",
                label="mean org (right-skewed)")
        ax.fill_between(summary["quasi_event_time"], summary["mean_ci_lo"], summary["mean_ci_hi"],
                        color="C3", alpha=0.15)
        ax.axvline(0, color="grey", ls=":", lw=1)
        ax.axhline(1.0, color="grey", lw=0.6, alpha=0.5)
        ax.axvspan(0.5, 5.5, color="orange", alpha=0.08)
        ax.set_title(title)
        ax.set_xlabel("quasi_event_time")
        ax.legend(fontsize=9)
    axes[0].set_ylabel("opened PRs / org pre-period mean")
    axes[0].set_ylim(0, 1.8)
    fig.suptitle("Opened-PR trajectory with 95% bootstrap CIs (each org normalized to its own pre-mean=1.0)",
                 fontsize=12)
    fig.tight_layout()
    fig.savefig(OUTDIR / "control_decline_profile_ci.png", dpi=110)


if __name__ == "__main__":
    Main()
