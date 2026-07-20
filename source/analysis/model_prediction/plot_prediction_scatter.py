import json
import warnings
from pathlib import Path

import binsreg
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd
import statsmodels.api as sm

from source.lib.python.config_loaders import LoadPipelineInputs, LoadModelPredictionConfig
from source.lib.python.repo_utils import MakeRepoNameSafe
from source.lib.JMSLab.autofill import GenerateAutofillMacros

CONFIG                  = LoadPipelineInputs()
MODEL_PREDICTION_CONFIG = LoadModelPredictionConfig()

OUTCOME_SAMPLE             = MODEL_PREDICTION_CONFIG["outcome_samples"]["run"][0]
DISTRIBUTION_TYPE   = MODEL_PREDICTION_CONFIG["distribution_types"]["run"][0]
ESTIMATION_APPROACH = MODEL_PREDICTION_CONFIG["member_probability_estimation"]["run"][0]
IMPORTANCE_TYPE     = CONFIG["importance_types"]["run"][0]
QUALIFIED_SAMPLE    = CONFIG["qualified_samples"]["run"][0]
CONTROL_GROUP       = CONFIG["control_groups"]["run"][0]
ROLLING_LABEL       = f"rolling{CONFIG['rolling_periods']['run'][0]}"
PRE_PERIOD_COUNT    = CONFIG["rolling_periods"]["run"][0]
POST_PERIODS        = list(range(1, PRE_PERIOD_COUNT + 1))
PRE_PERIODS         = list(range(-PRE_PERIOD_COUNT, 0))

DRAWS_PATH = (Path("drive/output/analysis/model_prediction") / OUTCOME_SAMPLE / DISTRIBUTION_TYPE / "draws"
              / ESTIMATION_APPROACH / IMPORTANCE_TYPE / QUALIFIED_SAMPLE / CONTROL_GROUP / "raw_draws.parquet")
MEMBER_PROBS_PATH = (Path("output/analysis/model_prediction") / OUTCOME_SAMPLE / DISTRIBUTION_TYPE / "parameters"
                     / ESTIMATION_APPROACH / IMPORTANCE_TYPE / QUALIFIED_SAMPLE / CONTROL_GROUP / "member_probabilities.parquet")
MEMBER_PANEL_DIR = (Path("drive/output/derived/model_prediction/event_time_member_panel")
                    / OUTCOME_SAMPLE / IMPORTANCE_TYPE / QUALIFIED_SAMPLE / CONTROL_GROUP)
CROSSMERGE_DIR = (Path("drive/output/derived/model_prediction/event_time_member_crossmerge")
                  / OUTCOME_SAMPLE / IMPORTANCE_TYPE / QUALIFIED_SAMPLE / CONTROL_GROUP)
ANALYSIS_PANEL_PATH = (Path("output/derived/analysis_panel") / OUTCOME_SAMPLE / IMPORTANCE_TYPE / ROLLING_LABEL
                       / QUALIFIED_SAMPLE / CONTROL_GROUP / "panel.parquet")
OUTDIR = (Path("output/analysis/model_prediction") / OUTCOME_SAMPLE / DISTRIBUTION_TYPE / "evaluation"
          / ESTIMATION_APPROACH / IMPORTANCE_TYPE / QUALIFIED_SAMPLE / CONTROL_GROUP / "scatter")
AUTOFILL_PATH = Path("output/autofill/model_prediction_autofill.tex")
SCATTER_SLOPE_TOLERANCE = 0.05   # the control through-origin slope must lie within this of 1

ORG_COLORS        = {"control": "#2a78d6", "treated": "#e34948"}
# Raw member points are the de-emphasized backdrop (incumbents grey so the blue/red binscatter reads on top);
# new members stay orange so their predicted=0 mass is visible. The binscatter is colored by treated/control
# (ORG_COLORS) exactly like the org figure, so the causal contrast pops the same way.
MEMBERSHIP_COLORS = {"incumbent": "#9aa0a6", "new": "#eb6834", "dropout": "#7d3ac1"}
TREATED_MARKER    = {False: "o", True: "^"}
IDENTITY_COLOR         = "#4a4a4a"
POINT_ALPHA            = 0.14
POINT_SIZE             = 7
MIN_BINSCATTER_POINTS  = 30
MIN_BINSCATTER_UNIQUE  = 5
BINSCATTER_BINS        = 10

REPO_COUNT_COLUMNS = ["repo_pull_request_opened", "repo_pull_request_reviewed",
                      "repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"]
MEMBER_PANEL_COLUMNS = ["member_pull_request_opened", "member_pull_request_reviewed_opened",
                        "member_pull_request_merged_self", "member_pull_request_merged_direct_opened_other",
                        "member_pull_request_merged_after_review_opened_self",
                        "member_pull_request_merged_after_review_opened_other"]
CROSSMERGE_COUNT_COLUMNS = ["n_merged_direct_other", "n_merged_after_review_other", "n_solo_reviewed"]


def Main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    treatment_by_repo, dropouts_by_repo = LoadTreatmentInfo()

    stats_records = []

    member_panel = LoadMemberData()
    member_panel = member_panel[member_panel["repo_name"].isin(treatment_by_repo)].copy()

    org_points = BuildOrgPoints(member_panel)
    PlotOrgFigure(org_points, OUTDIR / "org_prediction_scatter.png", stats_records)
    PlotOpenedScatter(org_points, OUTDIR / "opened_prediction_scatter.png", AUTOFILL_PATH)

    membership = ClassifyMembership(member_panel, dropouts_by_repo)
    member_actor  = BuildMemberActorPoints(member_panel, membership, treatment_by_repo)
    member_cond   = BuildMemberCondOpenPoints(member_panel, membership, treatment_by_repo)
    PlotMemberFigure(member_actor, MEMBER_ACTOR_PANELS, OUTDIR / "member_prediction_scatter.png", stats_records)
    PlotMemberFigure(member_cond, MEMBER_COND_PANELS, OUTDIR / "member_cond_open_prediction_scatter.png", stats_records)

    WriteStatsFile(stats_records, OUTDIR / "prediction_scatter_stats.txt")
    print(f"Wrote 3 figures and prediction_scatter_stats.txt to {OUTDIR}")


def LoadTreatmentInfo():
    df_panel = pd.read_parquet(ANALYSIS_PANEL_PATH, columns=["repo_name", "quasi_event_time", "num_dropouts", "dropouts_actors"])
    df_event_zero = df_panel[df_panel["quasi_event_time"] == 0].drop_duplicates("repo_name")
    treatment_by_repo = dict(zip(df_event_zero["repo_name"], df_event_zero["num_dropouts"] > 0))
    dropouts_by_repo = {
        row["repo_name"]: (set(json.loads(row["dropouts_actors"])) if row["num_dropouts"] > 0 else set())
        for _, row in df_event_zero.iterrows()
    }
    return treatment_by_repo, dropouts_by_repo


def LoadMemberData():
    RequireExists(MEMBER_PANEL_DIR, "member panel directory")
    RequireExists(CROSSMERGE_DIR, "crossmerge directory")
    # Read each leaf directory as one pyarrow multi-file dataset rather than looping over ~571 files each;
    # the per-file Python loop is the only slow part on this disk.
    member_panel = pd.read_parquet(MEMBER_PANEL_DIR,
                                   columns=["repo_name", "quasi_event_time", "actor_id"] + REPO_COUNT_COLUMNS + MEMBER_PANEL_COLUMNS)
    RequireColumns(member_panel, ["repo_name", "quasi_event_time", "actor_id"] + REPO_COUNT_COLUMNS + MEMBER_PANEL_COLUMNS, "member panel")

    crossmerge_raw = pd.read_parquet(CROSSMERGE_DIR,
                                     columns=["repo_name", "quasi_event_time", "other_id"] + CROSSMERGE_COUNT_COLUMNS)
    RequireColumns(crossmerge_raw, ["repo_name", "quasi_event_time", "other_id"] + CROSSMERGE_COUNT_COLUMNS, "crossmerge")
    # The crossmerge indexes each other-role action by the acting member (other_id); collapse the opener axis
    # so it merges onto the member panel as that member's own reviewer/merger counts per period.
    crossmerge = (crossmerge_raw.groupby(["repo_name", "quasi_event_time", "other_id"])[CROSSMERGE_COUNT_COLUMNS]
                  .sum().reset_index().rename(columns={"other_id": "actor_id"}))
    member_panel = member_panel.merge(crossmerge, on=["repo_name", "quasi_event_time", "actor_id"], how="left")
    member_panel[CROSSMERGE_COUNT_COLUMNS] = member_panel[CROSSMERGE_COUNT_COLUMNS].fillna(0)
    return member_panel


def BuildOrgPoints(member_panel):
    RequireExists(DRAWS_PATH, "raw_draws.parquet")
    draws = pd.read_parquet(DRAWS_PATH)
    RequireColumns(draws, ["repo_name", "quasi_event_time", "is_treated", "pull_request_opened",
                           "pull_request_reviewed", "pull_request_merged_direct",
                           "pull_request_merged_after_review"], "raw_draws")
    draws = draws[draws["quasi_event_time"].isin(POST_PERIODS)]
    predicted = draws.groupby(["repo_name", "quasi_event_time"]).agg(
        is_treated=("is_treated", "first"),
        opened_predicted=("pull_request_opened", "mean"),
        opened_draw_sum=("pull_request_opened", "sum"),
        reviewed_draw_sum=("pull_request_reviewed", "sum"),
        merged_direct_draw_sum=("pull_request_merged_direct", "sum"),
        merged_after_review_draw_sum=("pull_request_merged_after_review", "sum"),
    ).reset_index()

    post_panel = member_panel[member_panel["quasi_event_time"].isin(POST_PERIODS)]
    actual = post_panel.groupby(["repo_name", "quasi_event_time"])[REPO_COUNT_COLUMNS].first().reset_index()
    org = predicted.merge(actual, on=["repo_name", "quasi_event_time"], how="inner")

    org["review_rate_predicted"]        = SafeRatio(org["reviewed_draw_sum"],            org["opened_draw_sum"])
    org["direct_merge_rate_predicted"]  = SafeRatio(org["merged_direct_draw_sum"],       org["opened_draw_sum"])
    org["after_review_rate_predicted"]  = SafeRatio(org["merged_after_review_draw_sum"], org["reviewed_draw_sum"])
    org["review_rate_actual"]           = SafeRatio(org["repo_pull_request_reviewed"],            org["repo_pull_request_opened"])
    org["direct_merge_rate_actual"]     = SafeRatio(org["repo_pull_request_merged_direct"],       org["repo_pull_request_opened"])
    org["after_review_rate_actual"]     = SafeRatio(org["repo_pull_request_merged_after_review"], org["repo_pull_request_reviewed"])
    return org


def ClassifyMembership(member_panel, dropouts_by_repo):
    # Incumbent = any pre-period activity (the member panel is a full member x period grid, so a member's mere
    # presence is not activity); new = active only post-departure (no pre-period activity, so the model gives
    # it a 0 prediction); dropout = the departed key member. Activity spans opener-indexed panel counts and the
    # reviewer/merger (crossmerge) counts, so pure reviewers who never open still count as incumbents.
    activity_columns = MEMBER_PANEL_COLUMNS + CROSSMERGE_COUNT_COLUMNS
    pre_period = member_panel[member_panel["quasi_event_time"].isin(PRE_PERIODS)]
    pre_period_activity = pre_period.groupby(["repo_name", "actor_id"])[activity_columns].sum().sum(axis=1)
    incumbent_pairs = set(pre_period_activity[pre_period_activity > 0].index)

    pairs = member_panel[["repo_name", "actor_id"]].drop_duplicates().copy()

    def classify(repo_name, actor_id):
        if actor_id in dropouts_by_repo.get(repo_name, set()):
            return "dropout"
        if (repo_name, actor_id) in incumbent_pairs:
            return "incumbent"
        return "new"

    pairs["membership"] = [classify(repo_name, actor_id)
                           for repo_name, actor_id in zip(pairs["repo_name"], pairs["actor_id"])]
    return pairs


MEMBER_ACTOR_PANELS = [
    ("Opens",                          "open"),
    ("Sole reviews (as reviewer)",     "sole_review"),
    ("Direct merges (as merger)",      "direct_merge"),
    ("After-review merges (as merger)", "after_review_merge"),
]


def MemberActorNumerators(frame):
    return pd.DataFrame({
        "open":               frame["member_pull_request_opened"],
        "sole_review":        frame["n_solo_reviewed"],
        "direct_merge":       frame["member_pull_request_merged_self"] + frame["n_merged_direct_other"],
        "after_review_merge": frame["member_pull_request_merged_after_review_opened_self"] + frame["n_merged_after_review_other"],
    })


def BuildMemberActorPoints(member_panel, membership, treatment_by_repo):
    repo_opened_by_repo_period = (member_panel.groupby(["repo_name", "quasi_event_time"])["repo_pull_request_opened"]
                                  .first().reset_index())

    pre = member_panel[member_panel["quasi_event_time"].isin(PRE_PERIODS)]
    pre_member = pd.concat([pre[["repo_name", "actor_id"]].reset_index(drop=True),
                            MemberActorNumerators(pre).reset_index(drop=True)], axis=1)
    pre_member_sum = pre_member.groupby(["repo_name", "actor_id"]).sum()
    pre_repo_opened = (repo_opened_by_repo_period[repo_opened_by_repo_period["quasi_event_time"].isin(PRE_PERIODS)]
                       .groupby("repo_name")["repo_pull_request_opened"].sum())
    predicted_rates = pre_member_sum.div(pre_member_sum.index.get_level_values("repo_name").map(pre_repo_opened), axis=0)
    predicted_rates = predicted_rates.reset_index().rename(columns={key: f"pred_{key}" for _, key in MEMBER_ACTOR_PANELS})

    post = member_panel[member_panel["quasi_event_time"].isin(POST_PERIODS)].copy()
    actual_numerators = MemberActorNumerators(post)
    for _, key in MEMBER_ACTOR_PANELS:
        post[f"act_{key}"] = SafeRatio(actual_numerators[key], post["repo_pull_request_opened"])

    points = post[["repo_name", "actor_id", "quasi_event_time"] + [f"act_{key}" for _, key in MEMBER_ACTOR_PANELS]]
    points = points.merge(predicted_rates[["repo_name", "actor_id"] + [f"pred_{key}" for _, key in MEMBER_ACTOR_PANELS]],
                          on=["repo_name", "actor_id"], how="left")
    points = points.merge(membership, on=["repo_name", "actor_id"], how="left")
    return FinalizeMemberPoints(points, [key for _, key in MEMBER_ACTOR_PANELS], treatment_by_repo)


MEMBER_COND_PANELS = [
    ("P(reviewed | opened)",      "prob_review_opened",             "member_pull_request_reviewed_opened",                 "member_pull_request_opened"),
    ("Self direct-merge | open",  "prob_merge_self",                "member_pull_request_merged_self",                     "member_pull_request_opened"),
    ("Other direct-merge | open", "prob_merge_direct_opened_other", "member_pull_request_merged_direct_opened_other",      "member_pull_request_opened"),
    ("Self after-review merge",   "prob_merge_after_review_self",   "member_pull_request_merged_after_review_opened_self", "member_pull_request_reviewed_opened"),
    ("Other after-review merge",  "prob_merge_after_review_other",  "member_pull_request_merged_after_review_opened_other", "member_pull_request_reviewed_opened"),
]


def BuildMemberCondOpenPoints(member_panel, membership, treatment_by_repo):
    fitted_columns = ["repo_name", "actor_id"] + [prob_col for _, prob_col, _, _ in MEMBER_COND_PANELS]
    fitted = pd.read_parquet(MEMBER_PROBS_PATH, columns=fitted_columns)

    post = member_panel[member_panel["quasi_event_time"].isin(POST_PERIODS)].copy()
    for _, prob_col, numerator_col, denominator_col in MEMBER_COND_PANELS:
        post[f"act_{prob_col}"] = SafeRatio(post[numerator_col], post[denominator_col])

    points = post[["repo_name", "actor_id", "quasi_event_time"] + [f"act_{prob_col}" for _, prob_col, _, _ in MEMBER_COND_PANELS]]
    points = points.merge(fitted.rename(columns={prob_col: f"pred_{prob_col}" for _, prob_col, _, _ in MEMBER_COND_PANELS}),
                          on=["repo_name", "actor_id"], how="left")
    points = points.merge(membership, on=["repo_name", "actor_id"], how="left")
    return FinalizeMemberPoints(points, [prob_col for _, prob_col, _, _ in MEMBER_COND_PANELS], treatment_by_repo)


def FinalizeMemberPoints(points, keys, treatment_by_repo):
    points["is_treated"] = points["repo_name"].map(treatment_by_repo).fillna(False).astype(bool)
    predicted_zero = points["membership"].isin(["new", "dropout"])
    for key in keys:
        points.loc[predicted_zero, f"pred_{key}"] = 0.0
        points[f"pred_{key}"] = points[f"pred_{key}"].fillna(0.0)
    return points


def FitThroughOrigin(predicted, actual):
    predicted = np.asarray(predicted, dtype=float)
    actual = np.asarray(actual, dtype=float)
    valid = np.isfinite(predicted) & np.isfinite(actual)
    predicted, actual = predicted[valid], actual[valid]
    if len(predicted) < 2 or np.allclose(predicted, 0.0):
        return {"n": int(len(predicted)), "beta": np.nan, "se": np.nan, "r_squared": np.nan}
    fit = sm.OLS(actual, predicted).fit()
    return {"n": int(len(predicted)), "beta": float(fit.params[0]),
            "se": float(fit.bse[0]), "r_squared": float(fit.rsquared)}


def PlotOpenedScatter(org, outpath, autofill_path):
    fits = {"control": FitThroughOrigin(org.loc[~org["is_treated"], "opened_predicted"], org.loc[~org["is_treated"], "repo_pull_request_opened"]),
            "treated": FitThroughOrigin(org.loc[org["is_treated"], "opened_predicted"], org.loc[org["is_treated"], "repo_pull_request_opened"]),
            "pooled":  FitThroughOrigin(org["opened_predicted"], org["repo_pull_request_opened"])}

    fig, ax = plt.subplots(figsize=(6.0, 6.0))
    axis_max = float(max(org["opened_predicted"].max(), org["repo_pull_request_opened"].max()))
    for label, mask in [("control", ~org["is_treated"]), ("treated", org["is_treated"])]:
        subset = org[mask]
        ax.scatter(subset["opened_predicted"], subset["repo_pull_request_opened"], s=POINT_SIZE,
                   alpha=POINT_ALPHA, color=ORG_COLORS[label], marker="o", linewidths=0, label=label)
        ax.plot([0, axis_max], [0, fits[label]["beta"] * axis_max], color=ORG_COLORS[label], linewidth=1.2)
    ax.plot([0, axis_max], [0, axis_max], color=IDENTITY_COLOR, linestyle="--", linewidth=1, label="45$^\\circ$")
    ax.set_xscale("symlog", linthresh=1)
    ax.set_yscale("symlog", linthresh=1)
    ax.set_xlim(-0.3, axis_max * 1.3)
    ax.set_ylim(-0.3, axis_max * 1.3)
    ax.set_xlabel("Predicted pull requests opened")
    ax.set_ylabel("Actual pull requests opened")
    ax.spines[["top", "right"]].set_visible(False)

    box_lines = []
    for label in ("control", "treated"):
        box_lines.append(label.capitalize())
        box_lines.append(f"  Slope = {fits[label]['beta']:.2f} ({fits[label]['se']:.2f})")
        box_lines.append(f"  $R^2$ = {fits[label]['r_squared']:.2f}")
    ax.text(0.04, 0.96, "\n".join(box_lines), transform=ax.transAxes, va="top", ha="left", fontsize=9,
            bbox=dict(boxstyle="round,pad=0.4", facecolor="white", edgecolor="0.6", alpha=0.9))
    ax.legend(loc="lower right", fontsize=8, frameon=False)
    fig.tight_layout()
    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)

    control_slope = fits["control"]["beta"]
    assert abs(control_slope - 1.0) <= SCATTER_SLOPE_TOLERANCE, \
        f"Control through-origin slope {control_slope:.3f} is not within {SCATTER_SLOPE_TOLERANCE:.0%} of 1"
    WriteScatterAutofill(fits, autofill_path)


def WriteScatterAutofill(fits, autofill_path):
    ScatterControlSlope       = fits["control"]["beta"]
    ScatterTreatedSlope       = fits["treated"]["beta"]
    ScatterControlRSquaredPct = fits["control"]["r_squared"] * 100
    ScatterTreatedRSquaredPct = fits["treated"]["r_squared"] * 100
    autofill_path.parent.mkdir(parents=True, exist_ok=True)
    GenerateAutofillMacros(
        [["ScatterControlSlope", "ScatterTreatedSlope"], ["ScatterControlRSquaredPct", "ScatterTreatedRSquaredPct"]],
        ["{:.2f}", "{:.0f}"],
        str(autofill_path))


def DrawBinScatter(ax, predicted, actual, color, marker):
    # Cattaneo et al. binscatter (binsreg): up to BINSCATTER_BINS quantile bins, so calibration against the unit
    # line is legible through the overplotting. Skip groups binsreg cannot bin (tiny, degenerate mass points).
    predicted = np.asarray(predicted, dtype=float)
    actual = np.asarray(actual, dtype=float)
    valid = np.isfinite(predicted) & np.isfinite(actual)
    predicted, actual = predicted[valid], actual[valid]
    if len(predicted) < MIN_BINSCATTER_POINTS or np.unique(predicted).size < MIN_BINSCATTER_UNIQUE:
        return
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            estimate = binsreg.binsreg(actual, predicted, noplot=True, nbins=BINSCATTER_BINS)
    except (ValueError, np.linalg.LinAlgError):
        return
    dots = estimate.data_plot[0].dots
    if dots is None:
        return
    dots = dots.dropna(subset=["x", "fit"])
    ax.plot(dots["x"], dots["fit"], marker=marker, linestyle="none", color=color, markersize=9,
            markeredgecolor="black", markeredgewidth=0.6, zorder=5)


def DrawIdentityLine(ax, group_frames, log_scale=False):
    finite = np.concatenate([frame[["predicted", "actual"]].to_numpy().ravel() for frame in group_frames.values()])
    finite = finite[np.isfinite(finite)]
    if len(finite) == 0:
        return
    axis_max = float(np.nanmax(finite))
    axis_max = axis_max if axis_max > 0 else 1.0
    if log_scale:
        positive = finite[finite > 0]
        axis_min = float(np.nanmin(positive)) if len(positive) else axis_max / 100.0
    else:
        axis_min = 0.0
    ax.plot([axis_min, axis_max], [axis_min, axis_max], color=IDENTITY_COLOR, linestyle="--", linewidth=1, zorder=1)


def PlotOrgFigure(org, outpath, stats_records):
    panels = [
        ("Pulls opened (count)",        "opened_predicted",           "repo_pull_request_opened",    True),
        ("Review rate  π^r",           "review_rate_predicted",       "review_rate_actual",          False),
        ("Direct-merge rate  π^{m|o}", "direct_merge_rate_predicted", "direct_merge_rate_actual",    False),
        ("After-review merge rate  π^{m|r}", "after_review_rate_predicted", "after_review_rate_actual", False),
    ]
    fig, axes = plt.subplots(1, len(panels), figsize=(4.4 * len(panels), 4.6), squeeze=False)
    for ax, (title, predicted_col, actual_col, log_scale) in zip(axes[0], panels):
        group_frames = {}
        for label, mask in {"control": ~org["is_treated"], "treated": org["is_treated"]}.items():
            frame = pd.DataFrame({"predicted": org.loc[mask, predicted_col], "actual": org.loc[mask, actual_col]}).dropna()
            if log_scale:
                frame = frame[(frame["predicted"] > 0) & (frame["actual"] > 0)]
            group_frames[label] = frame
            ax.scatter(frame["predicted"], frame["actual"], s=POINT_SIZE, alpha=POINT_ALPHA,
                       color=ORG_COLORS[label], marker="o", linewidths=0, label=label)
        if log_scale:
            ax.set_xscale("log")
            ax.set_yscale("log")
        DrawIdentityLine(ax, group_frames, log_scale)
        for label in ["control", "treated"]:
            DrawBinScatter(ax, group_frames[label]["predicted"], group_frames[label]["actual"], ORG_COLORS[label], "o")

        stat_lines = []
        for label in ["control", "treated", "pooled"]:
            frame = (pd.concat(group_frames.values(), ignore_index=True) if label == "pooled" else group_frames[label])
            stat = FitThroughOrigin(frame["predicted"], frame["actual"])
            stats_records.append({"figure": outpath.stem, "panel": title, "group": label, **stat})
            stat_lines.append(FormatStatLine(label, stat))
        WriteStatsBox(ax, stat_lines)
        StylePanel(ax, title)
    fig.legend(*DedupLegend(axes[0][0]), loc="lower center", ncol=2, frameon=False, bbox_to_anchor=(0.5, -0.02))
    fig.suptitle("Organization-level model fit (predicted vs actual, post-periods 1–5)", y=1.02)
    fig.tight_layout()
    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)


def PlotMemberFigure(points, panel_specs, outpath, stats_records):
    keys = [spec[1] for spec in panel_specs]
    fig, axes = plt.subplots(1, len(panel_specs), figsize=(4.4 * len(panel_specs), 4.8), squeeze=False)
    for ax, spec in zip(axes[0], panel_specs):
        title, key = spec[0], spec[1]
        frame = pd.DataFrame({
            "predicted": points[f"pred_{key}"], "actual": points[f"act_{key}"],
            "membership": points["membership"], "is_treated": points["is_treated"],
        }).dropna(subset=["predicted", "actual"])
        # Drop members with no signal on THIS panel's dimension (predicted 0 AND actual 0): the full
        # member x period grid otherwise piles thousands of trivial points at the origin. Applied per panel,
        # so membership (esp. "new", which is predicted 0) is figure-specific -- a member appears in the
        # opens panel only if they opened, in the review panel only if they reviewed, etc. This does not
        # change any through-origin slope or R^2 (predicted-0 points contribute 0 to all sums); it only
        # removes the origin clutter and makes each group's n reflect activity on that dimension.
        frame = frame[~((frame["predicted"] == 0) & (frame["actual"] == 0))]

        group_frames = {}
        for membership_label in ["incumbent", "new", "dropout"]:
            for is_treated in [False, True]:
                subset = frame[(frame["membership"] == membership_label) & (frame["is_treated"] == is_treated)]
                if subset.empty:
                    continue
                group_frames[(membership_label, is_treated)] = subset[["predicted", "actual"]]
                ax.scatter(subset["predicted"], subset["actual"], s=POINT_SIZE, alpha=POINT_ALPHA,
                           color=MEMBERSHIP_COLORS[membership_label], marker=TREATED_MARKER[is_treated],
                           linewidths=0)
        DrawIdentityLine(ax, group_frames)
        for (membership_label, is_treated), subset in group_frames.items():
            if membership_label == "incumbent":
                DrawBinScatter(ax, subset["predicted"], subset["actual"],
                               ORG_COLORS["treated" if is_treated else "control"], TREATED_MARKER[is_treated])

        stat_lines = []
        for membership_label, is_treated in [("incumbent", True), ("incumbent", False), ("new", None), ("dropout", None)]:
            if is_treated is None:
                subset = frame[frame["membership"] == membership_label]
                group_label = membership_label
            else:
                subset = frame[(frame["membership"] == membership_label) & (frame["is_treated"] == is_treated)]
                group_label = f"{membership_label}-{'treated' if is_treated else 'control'}"
            if subset.empty:
                continue
            stat = FitThroughOrigin(subset["predicted"], subset["actual"])
            stats_records.append({"figure": outpath.stem, "panel": title, "group": group_label, **stat})
            report_fit = membership_label == "incumbent"
            stat_lines.append(FormatStatLine(group_label, stat, report_fit=report_fit))
        WriteStatsBox(ax, stat_lines)
        StylePanel(ax, title)
    membership_handles = [Line2D([0], [0], marker="o", linestyle="", markerfacecolor=color, markeredgecolor="none",
                                 markersize=7, label=membership_label)
                          for membership_label, color in MEMBERSHIP_COLORS.items()]
    binscatter_handles = [Line2D([0], [0], marker=TREATED_MARKER[is_treated], linestyle="",
                                 markerfacecolor=ORG_COLORS["treated" if is_treated else "control"],
                                 markeredgecolor="black", markeredgewidth=0.6, markersize=9,
                                 label="treated" if is_treated else "control")
                          for is_treated in [False, True]]
    fig.legend(handles=membership_handles, title="raw points (members)", loc="lower center", ncol=3,
               frameon=False, bbox_to_anchor=(0.38, -0.06))
    fig.legend(handles=binscatter_handles, title="binscatter (incumbents)", loc="lower center", ncol=2,
               frameon=False, bbox_to_anchor=(0.74, -0.06))
    fig.suptitle(f"{outpath.stem} (predicted vs actual, post-periods 1–5)", y=1.02)
    fig.tight_layout()
    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)


def StylePanel(ax, title):
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_title(title, fontsize=9)
    ax.set_xlabel("predicted", fontsize=8)
    ax.set_ylabel("actual", fontsize=8)
    ax.tick_params(labelsize=7)


def FormatStatLine(label, stat, report_fit=True):
    if not report_fit or not np.isfinite(stat["beta"]):
        return f"{label}: n={stat['n']}"
    return f"{label}: β={stat['beta']:.2f} se={stat['se']:.2f} R²={stat['r_squared']:.2f} n={stat['n']}"


def WriteStatsBox(ax, stat_lines):
    ax.text(0.03, 0.97, "\n".join(stat_lines), transform=ax.transAxes, fontsize=6.5, va="top", ha="left",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.7))


def DedupLegend(ax):
    handles, labels = ax.get_legend_handles_labels()
    unique = dict(zip(labels, handles))
    return list(unique.values()), list(unique.keys())


def WriteStatsFile(stats_records, outpath):
    stats = pd.DataFrame(stats_records)[["figure", "panel", "group", "n", "beta", "se", "r_squared"]]
    with open(outpath, "w") as handle:
        handle.write(stats.to_string(index=False))
        handle.write("\n")


def SafeRatio(numerator, denominator):
    numerator = np.asarray(numerator, dtype=float)
    denominator = np.asarray(denominator, dtype=float)
    return np.divide(numerator, denominator, out=np.full(len(numerator), np.nan), where=denominator > 0)


def RequireExists(path, description):
    if not Path(path).exists():
        raise FileNotFoundError(f"Missing {description}: {path}. Regenerate the upstream model outputs first.")


def RequireColumns(frame, columns, description):
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise KeyError(f"{description} is missing expected columns {missing} (stale schema?). Present: {list(frame.columns)}")


if __name__ == "__main__":
    Main()
