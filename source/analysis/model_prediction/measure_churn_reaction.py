"""Descriptive measurement of the three forces that break the no-reaction counterfactual
(paper Assumptions 9-10): (a) natural dropout, (b) natural entry, (c) survivor reaction.

Everything is measured on the review / direct-merge / after-review stages CONDITIONAL ON OPENS
(each stage as a per-opener rate over its own denominator), so it is insulated from open-count
prediction error, and always split treated vs control: controls (no departure) reveal natural
churn (a+b); the treated-minus-control contrast reveals the departure response (c).

These are the "facts to discipline the model" behind the churn/reaction framework memo; this
script does not change the counterfactual. Sample definition (outlier trim + incumbent/new/dropout
classification) is reused from plot_prediction_scatter so the figures share the event-study org set.
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# The prediction scatter already owns the canonical sample loaders + membership classification;
# import them rather than duplicate. (A future refactor could lift these into a shared
# prediction_data helper; kept here to avoid touching the in-flux scatter script.)
from source.analysis.model_prediction.plot_prediction_scatter import (
    CONTROL_GROUP, CROSSMERGE_COUNT_COLUMNS, CROSSMERGE_DIR, DISTRIBUTION_TYPE,
    ESTIMATION_APPROACH, IMPORTANCE_TYPE, MEMBER_PANEL_COLUMNS, ORG_COLORS, POST_PERIODS,
    PRE_PERIODS, QUALIFIED_SAMPLE, OUTCOME_SAMPLE,
    ClassifyMembership, LoadMemberData, LoadTreatmentInfo,
)

OUTDIR = (Path("output/analysis/model_prediction") / OUTCOME_SAMPLE / DISTRIBUTION_TYPE / "evaluation"
          / ESTIMATION_APPROACH / IMPORTANCE_TYPE / QUALIFIED_SAMPLE / CONTROL_GROUP / "churn_reaction")

EVENT_TIMES = PRE_PERIODS + [0] + POST_PERIODS
GROUPS      = [("treated", True), ("control", False)]

# Conditional-on-opens stage rates: (label, numerator, denominator). Review & the two direct-merge
# splits are per opened pull; the two after-review splits are per reviewed-opened pull.
CONDITIONAL_RATES = [
    ("P(reviewed|open)",       "member_pull_request_reviewed_opened",                  "member_pull_request_opened"),
    ("self direct-merge|open", "member_pull_request_merged_self",                      "member_pull_request_opened"),
    ("other direct-merge|open", "member_pull_request_merged_direct_opened_other",       "member_pull_request_opened"),
    ("self after-review",      "member_pull_request_merged_after_review_opened_self",  "member_pull_request_reviewed_opened"),
    ("other after-review",     "member_pull_request_merged_after_review_opened_other", "member_pull_request_reviewed_opened"),
]
# Channels of j*'s freed load that a survivor can pick up (the (opener, other) crossmerge columns).
PICKUP_CHANNELS = [
    ("solo review",    "n_solo_reviewed"),
    ("other-direct",   "n_merged_direct_other"),
    ("other-after",    "n_merged_after_review_other"),
]


def Main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    treatment_by_repo, dropouts_by_repo = LoadTreatmentInfo()

    member_panel = LoadMemberData()
    member_panel = member_panel[member_panel["repo_name"].isin(treatment_by_repo)].copy()

    membership = ClassifyMembership(member_panel, dropouts_by_repo)
    member_panel = member_panel.merge(membership, on=["repo_name", "actor_id"], how="left")
    member_panel["is_treated"] = member_panel["repo_name"].map(treatment_by_repo).fillna(False).astype(bool)

    incumbent_activity, entrant_share = MeasureChurn(member_panel)
    entrant_profile         = MeasureEntrantProfile(member_panel)
    reaction_by_time, reaction_summary = MeasureReaction(member_panel)
    sample_sizes            = MeasureSampleSizes(member_panel)
    pickup                  = MeasurePickup(member_panel, dropouts_by_repo)

    PlotChurn(incumbent_activity, entrant_share, OUTDIR / "churn_decomposition.png")
    PlotReaction(reaction_by_time, OUTDIR / "reaction_by_event_time.png")
    WriteTables(entrant_profile, reaction_summary, sample_sizes, pickup, OUTDIR / "churn_reaction_tables.txt")
    print(f"Wrote 2 figures and churn_reaction_tables.txt to {OUTDIR}")


def PooledRate(frame, numerator, denominator):
    total = frame[denominator].sum()
    return frame[numerator].sum() / total if total > 0 else np.nan


# ---------------------------------------------------------------------------
# (a) natural dropout + (b) natural entry: population dynamics by event time.
# ---------------------------------------------------------------------------

def MeasureChurn(member_panel):
    # Active = any opener-side or reviewer/merger activity that period (matches the scatter's
    # incumbent activity definition), so pure reviewers count.
    activity_columns = MEMBER_PANEL_COLUMNS + CROSSMERGE_COUNT_COLUMNS
    member_panel = member_panel[member_panel["quasi_event_time"].isin(EVENT_TIMES)].copy()
    member_panel["active"] = member_panel[activity_columns].sum(axis=1) > 0

    # (a) natural dropout: fraction of pre-active incumbents (excludes j*) active in each period.
    incumbents = member_panel[member_panel["membership"] == "incumbent"]
    incumbent_activity = (incumbents.groupby(["is_treated", "quasi_event_time"])["active"].mean()
                          .rename("active_fraction").reset_index())

    # (b) entrant share of opens: entrants' opens / all opens, by event time.
    opens = member_panel.groupby(["is_treated", "quasi_event_time", "membership"])["member_pull_request_opened"].sum()
    opens = opens.unstack("membership").fillna(0.0)
    entrant_share = (opens.get("new", 0.0) / opens.sum(axis=1).replace(0.0, np.nan)).rename("entrant_open_share").reset_index()
    return incumbent_activity, entrant_share


# ---------------------------------------------------------------------------
# (b) entrant conditional-behavior profile vs incumbents (pooled over post).
# ---------------------------------------------------------------------------

def MeasureEntrantProfile(member_panel):
    post = member_panel[member_panel["quasi_event_time"].isin(POST_PERIODS)]
    records = []
    for group_label, is_treated in GROUPS:
        for membership in ["incumbent", "new"]:
            subset = post[(post["is_treated"] == is_treated) & (post["membership"] == membership)]
            row = {"group": group_label, "member_type": membership,
                   "opens": int(subset["member_pull_request_opened"].sum())}
            for rate_label, numerator, denominator in CONDITIONAL_RATES:
                row[rate_label] = PooledRate(subset, numerator, denominator)
            records.append(row)
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# (c) survivor reaction: conditional rates for surviving incumbents, treated vs control.
# ---------------------------------------------------------------------------

def MeasureReaction(member_panel):
    incumbents = member_panel[member_panel["membership"] == "incumbent"]

    time_records = []
    for rate_label, numerator, denominator in CONDITIONAL_RATES:
        for group_label, is_treated in GROUPS:
            group = incumbents[incumbents["is_treated"] == is_treated]
            for event_time in EVENT_TIMES:
                subset = group[group["quasi_event_time"] == event_time]
                time_records.append({"rate": rate_label, "group": group_label,
                                     "quasi_event_time": event_time, "value": PooledRate(subset, numerator, denominator)})
    reaction_by_time = pd.DataFrame(time_records)

    # pre->post change per group, and the difference-in-differences = the departure response.
    summary_records = []
    for rate_label, numerator, denominator in CONDITIONAL_RATES:
        deltas = {}
        row = {"rate": rate_label}
        for group_label, is_treated in GROUPS:
            group = incumbents[incumbents["is_treated"] == is_treated]
            pre_rate  = PooledRate(group[group["quasi_event_time"].isin(PRE_PERIODS)],  numerator, denominator)
            post_rate = PooledRate(group[group["quasi_event_time"].isin(POST_PERIODS)], numerator, denominator)
            deltas[group_label] = post_rate - pre_rate
            row[f"{group_label}_pre"]  = pre_rate
            row[f"{group_label}_post"] = post_rate
        row["reaction_DiD"] = deltas["treated"] - deltas["control"]
        summary_records.append(row)
    return reaction_by_time, pd.DataFrame(summary_records)


def MeasureSampleSizes(member_panel):
    # The conditional-rate denominators for surviving incumbents; treated post is thin, so rates there are noisy.
    incumbents = member_panel[member_panel["membership"] == "incumbent"]
    records = []
    for group_label, is_treated in GROUPS:
        group = incumbents[incumbents["is_treated"] == is_treated]
        for window_label, periods in [("pre", PRE_PERIODS), ("post", POST_PERIODS)]:
            window = group[group["quasi_event_time"].isin(periods)]
            records.append({"group": group_label, "window": window_label,
                           "incumbents": window[["repo_name", "actor_id"]].drop_duplicates().shape[0],
                           "opens": int(window["member_pull_request_opened"].sum()),
                           "reviewed_opens": int(window["member_pull_request_reviewed_opened"].sum())})
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# (c) pickup: who absorbs j*'s freed review/merge load (treated repos, per repo-open).
# ---------------------------------------------------------------------------

def MeasurePickup(member_panel, dropouts_by_repo):
    crossmerge = pd.read_parquet(CROSSMERGE_DIR, columns=["repo_name", "quasi_event_time",
                                 "opener_id", "other_id"] + CROSSMERGE_COUNT_COLUMNS)
    # member_panel is already outlier-trimmed, so its treated repos carry the trim into the crossmerge.
    treated_repos = set(member_panel.loc[member_panel["is_treated"], "repo_name"].unique())
    crossmerge = crossmerge[crossmerge["repo_name"].isin(treated_repos)].copy()

    # Classify the handler (other_id): the departing key member, a surviving incumbent, or an entrant.
    incumbent_pairs = set(member_panel.loc[member_panel["membership"] == "incumbent", ["repo_name", "actor_id"]]
                          .itertuples(index=False, name=None))

    def handler_class(repo_name, other_id):
        if other_id in dropouts_by_repo.get(repo_name, set()):
            return "departer (j*)"
        return "incumbent" if (repo_name, other_id) in incumbent_pairs else "entrant"

    crossmerge["handler"] = [handler_class(repo_name, other_id)
                             for repo_name, other_id in zip(crossmerge["repo_name"], crossmerge["other_id"])]
    # Event time 0 (departer still present) and anything outside the [-5, 5] analysis window are dropped.
    crossmerge["window"] = np.where(crossmerge["quasi_event_time"].isin(PRE_PERIODS), "pre", "post")
    crossmerge = crossmerge[crossmerge["quasi_event_time"].isin(PRE_PERIODS + POST_PERIODS)]

    # Normalize each channel by treated repo-opens in the window (rate per opened pull, like diag_direct_source).
    repo_opens = (member_panel[member_panel["is_treated"]]
                  .groupby(["repo_name", "quasi_event_time"])["repo_pull_request_opened"].first().reset_index())
    opens_by_window = {"pre":  repo_opens.loc[repo_opens["quasi_event_time"].isin(PRE_PERIODS),  "repo_pull_request_opened"].sum(),
                       "post": repo_opens.loc[repo_opens["quasi_event_time"].isin(POST_PERIODS), "repo_pull_request_opened"].sum()}

    records = []
    for channel_label, count_column in PICKUP_CHANNELS:
        for window in ["pre", "post"]:
            window_rows = crossmerge[crossmerge["window"] == window]
            for handler in ["departer (j*)", "incumbent", "entrant"]:
                load = window_rows.loc[window_rows["handler"] == handler, count_column].sum()
                records.append({"channel": channel_label, "handler": handler, "window": window,
                               "rate_per_open": load / opens_by_window[window] if opens_by_window[window] > 0 else np.nan})
    pickup = pd.DataFrame(records).pivot_table(index=["channel", "handler"], columns="window",
                                               values="rate_per_open").reset_index()
    return pickup[["channel", "handler", "pre", "post"]]


# ---------------------------------------------------------------------------
# Figures and tables.
# ---------------------------------------------------------------------------

def PlotChurn(incumbent_activity, entrant_share, outpath):
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.4))
    for group_label, is_treated in GROUPS:
        curve = incumbent_activity[incumbent_activity["is_treated"] == is_treated].sort_values("quasi_event_time")
        axes[0].plot(curve["quasi_event_time"], curve["active_fraction"], marker="o", markersize=4,
                     color=ORG_COLORS[group_label], label=group_label)
        share = entrant_share[entrant_share["is_treated"] == is_treated].sort_values("quasi_event_time")
        axes[1].plot(share["quasi_event_time"], share["entrant_open_share"], marker="o", markersize=4,
                     color=ORG_COLORS[group_label], label=group_label)
    StyleTimeAxis(axes[0], "(a) natural dropout: incumbent active fraction", "fraction of pre-active incumbents active")
    StyleTimeAxis(axes[1], "(b) natural entry: entrant share of opens", "entrant opens / all opens")
    axes[0].legend(frameon=False, fontsize=8)
    fig.suptitle("Natural churn (control) vs departure-induced churn (treated), by event time", y=1.02)
    fig.tight_layout()
    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)


def PlotReaction(reaction_by_time, outpath):
    rate_labels = [rate_label for rate_label, _, _ in CONDITIONAL_RATES]
    fig, axes = plt.subplots(1, len(rate_labels), figsize=(3.4 * len(rate_labels), 4.0), squeeze=False)
    for ax, rate_label in zip(axes[0], rate_labels):
        panel = reaction_by_time[reaction_by_time["rate"] == rate_label]
        for group_label, _ in GROUPS:
            curve = panel[panel["group"] == group_label].sort_values("quasi_event_time")
            ax.plot(curve["quasi_event_time"], curve["value"], marker="o", markersize=3,
                    color=ORG_COLORS[group_label], label=group_label)
        StyleTimeAxis(ax, rate_label, "rate (incumbents)")
    axes[0][0].legend(frameon=False, fontsize=8)
    fig.suptitle("Survivor reaction: conditional stage rates for surviving incumbents, treated vs control", y=1.03)
    fig.tight_layout()
    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)


def StyleTimeAxis(ax, title, ylabel):
    ax.axvline(0, color="#999999", linestyle=":", linewidth=1)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_title(title, fontsize=9)
    ax.set_xlabel("quasi event time", fontsize=8)
    ax.set_ylabel(ylabel, fontsize=8)
    ax.tick_params(labelsize=7)


def WriteTables(entrant_profile, reaction_summary, sample_sizes, pickup, outpath):
    with open(outpath, "w") as handle:
        handle.write("CHURN & REACTION MEASUREMENT (opened_cohort / important_degree_top3 / exact1 / nevertreated)\n")
        handle.write("Conditional-on-opens stage rates; surviving incumbents exclude the departed key member j*.\n\n")
        handle.write("(c) SURVIVOR REACTION -- conditional rates, pre vs post, treated vs control; "
                     "reaction_DiD = (treated post-pre) - (control post-pre)\n")
        handle.write(reaction_summary.round(3).to_string(index=False) + "\n\n")
        handle.write("    sample sizes (surviving incumbents; treated post is thin -> those rates are noisy)\n")
        handle.write(sample_sizes.to_string(index=False) + "\n\n")
        handle.write("(b) ENTRANT PROFILE -- conditional rates pooled over post-periods, entrants vs incumbents\n")
        handle.write(entrant_profile.round(3).to_string(index=False) + "\n\n")
        handle.write("(c) PICKUP -- j*'s freed load and who absorbs it, treated repos, rate per repo-open, pre vs post\n")
        handle.write(pickup.round(4).to_string(index=False) + "\n")


if __name__ == "__main__":
    Main()
