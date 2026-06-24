import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D
from matplotlib.offsetbox import TextArea, DrawingArea, HPacker, VPacker, AnnotationBbox
import numpy as np
import pandas as pd
from pathlib import Path
from itertools import product
from joblib import Parallel, delayed
from scipy.stats import ks_2samp
from statsmodels.nonparametric.kde import KDEUnivariate

from source.lib.python.config_loaders import LoadGlobalSettings, LoadPipelineInputs, LoadModelPredictionConfig

GLOBAL_SETTINGS         = LoadGlobalSettings()
MODEL_PREDICTION_CONFIG = LoadModelPredictionConfig()
CONFIG                  = LoadPipelineInputs()

INDIR  = Path("output/analysis/model_prediction")
OUTDIR = Path("output/analysis/model_prediction")
VARIANTS              = MODEL_PREDICTION_CONFIG["variants"]["run"]
DISTRIBUTION_TYPES    = MODEL_PREDICTION_CONFIG["distribution_types"]["run"]
ESTIMATION_APPROACHES = MODEL_PREDICTION_CONFIG["member_probability_estimation"]["run"]
EVALUATION_FIGURES    = MODEL_PREDICTION_CONFIG["evaluation_figures"]["run"]
N_JOBS                = GLOBAL_SETTINGS["n_jobs"]

OUTCOMES = ["opened", "reviewed", "merged_direct", "merged_after_review", "merged_total"]
STAGES   = ["opened", "reviewed", "merged_direct", "merged_after_review"]

OUTCOME_LABELS = {
    "opened":              "Opened",
    "reviewed":            "Reviewed",
    "merged_direct":       "Direct merged",
    "merged_after_review": "Reviewed merged",
    "merged_total":        "Total merged",
}
STAGE_LABELS = {
    "opened":              "Open",
    "reviewed":            "Review",
    "merged_direct":       "Direct merge",
    "merged_after_review": "Reviewed merge",
}
GROUP_LABELS = {"treated": "Treated", "control": "Control"}
SIGNED_AXIS_LABEL = r"Signed std residual $(x - \mu) / \sigma$"
SQUARED_RESIDUAL_CLIP = 15
SIGNED_RESIDUAL_CLIP  = 10
DECOMP_PERCENT_CLIP   = 1200
DECOMP_PERCENT_QUANTILE = 0.90

MEDIAN_COLOR    = "#C0392B"
MEAN_COLOR      = "#27AE60"
REFERENCE_COLOR = "#1A1A1A"
KS_LINE_COLOR   = "#8E44AD"


def Main():
    importance_types  = CONFIG["importance_types"]["run"]
    qualified_samples = CONFIG["qualified_samples"]["run"]
    control_groups    = CONFIG["control_groups"]["run"]

    Parallel(n_jobs=N_JOBS)(
        delayed(RunCombination)(
            variant, distribution_type, estimation_approach,
            importance_type, qualified_sample, control_group
        )
        for variant, distribution_type, estimation_approach, importance_type, qualified_sample, control_group in product(
            VARIANTS, DISTRIBUTION_TYPES, ESTIMATION_APPROACHES,
            importance_types, qualified_samples, control_groups
        )
    )


def RunCombination(variant, distribution_type, estimation_approach,
                   importance_type, qualified_sample, control_group):
    pred_dir = (
        INDIR / variant / distribution_type / "residuals" / estimation_approach
        / importance_type / qualified_sample / control_group
    )
    base_out = (
        OUTDIR / variant / distribution_type / "evaluation" / estimation_approach
        / importance_type / qualified_sample / control_group
    )

    df_insample    = pd.read_parquet(pred_dir / "insample_period.parquet")
    df_leaveoneout = pd.read_parquet(pred_dir / "leaveoneout_period.parquet")
    df_post        = pd.read_parquet(pred_dir / "post_period.parquet")
    df_insample_reference    = pd.read_parquet(pred_dir / "insample_reference.parquet")
    df_leaveoneout_reference = pd.read_parquet(pred_dir / "leaveoneout_reference.parquet")
    df_post_reference        = pd.read_parquet(pred_dir / "post_reference.parquet")

    panels_squared = base_out / "panels" / "squared"
    panels_signed  = base_out / "panels" / "signed"
    panels_squared.mkdir(parents=True, exist_ok=True)
    panels_signed.mkdir(parents=True, exist_ok=True)

    fit_panels = [
        ("pre_period_fit_insample",    df_insample,    df_insample_reference,    "Pre-Period Fit (InSample)"),
        ("pre_period_fit_leaveoneout", df_leaveoneout, df_leaveoneout_reference, "Pre-Period Fit (LeaveOneOut)"),
        ("post_period_fit",            df_post,        df_post_reference,        "Post-Period Fit"),
    ]
    for name, df_residuals, df_reference, title in fit_panels:
        SignedFitPanel(panels_signed / f"{name}.png", df_residuals, df_reference, title)
        if name == "post_period_fit":
            PostSquaredFitPanel(panels_squared / f"{name}.png", df_residuals, df_reference, df_leaveoneout, title)
        else:
            SquaredFitPanel(panels_squared / f"{name}.png", df_residuals, df_reference, title)

    decomp_panels = [
        ("pre_period_decomp_insample",    df_insample,    "Pre-Period Decomp (InSample)"),
        ("pre_period_decomp_leaveoneout", df_leaveoneout, "Pre-Period Decomp (LeaveOneOut)"),
        ("post_period_decomp",            df_post,        "Post-Period Decomp"),
    ]
    for name, df_residuals, title in decomp_panels:
        SquaredDecompPanel(panels_squared / f"{name}.png", df_residuals, title)

    ComparisonFigure(base_out / "panels" / "comparisons.png", df_leaveoneout, df_post)

    if "individual" in EVALUATION_FIGURES:
        PlotIndividual(base_out, {"InSample": df_insample, "LeaveOneOut": df_leaveoneout, "Post-Period": df_post})


def SplitGroups(df_residuals):
    return {"treated": df_residuals[df_residuals["is_treated"]], "control": df_residuals[~df_residuals["is_treated"]]}


def SignedFitPanel(outpath, residuals, reference, title):
    residuals_by_group, reference_by_group = SplitGroups(residuals), SplitGroups(reference)
    MakePanel(
        outpath, rows=OUTCOMES,
        cols={group: residuals_by_group[group] for group in GROUP_LABELS},
        col_getter=lambda group_df, outcome: group_df[f"signed_std_residual_{outcome}"],
        row_labels=OUTCOME_LABELS, col_labels=GROUP_LABELS,
        suptitle=f"{title} Signed Std Residual", xlabel=SIGNED_AXIS_LABEL,
        clip=SIGNED_RESIDUAL_CLIP,
        ref_cols={group: reference_by_group[group] for group in GROUP_LABELS},
        ref_getter=lambda group_df, outcome: group_df[f"signed_std_residual_{outcome}"],
    )


def SquaredFitPanel(outpath, residuals, reference, title):
    residuals_by_group, reference_by_group = SplitGroups(residuals), SplitGroups(reference)
    MakePanel(
        outpath, rows=OUTCOMES,
        cols={group: residuals_by_group[group] for group in GROUP_LABELS},
        col_getter=lambda group_df, outcome: group_df[f"squared_std_residual_{outcome}"],
        row_labels=OUTCOME_LABELS, col_labels=GROUP_LABELS,
        suptitle=f"{title} Squared Std Residual",
        clip=SQUARED_RESIDUAL_CLIP,
        ref_cols={group: reference_by_group[group] for group in GROUP_LABELS},
        ref_getter=lambda group_df, outcome: group_df[f"signed_std_residual_{outcome}"] ** 2 - 1.0,
    )


def PostSquaredFitPanel(outpath, df_post, df_post_reference, df_leaveoneout, title):
    """Post-period squared-residual fit, with an added treated row net of each org's LOO baseline."""
    post_by_group = SplitGroups(df_post)
    reference_by_group = SplitGroups(df_post_reference)
    loo_treated_residuals = SplitGroups(df_leaveoneout)["treated"]
    loo_baseline = (loo_treated_residuals.groupby("repo_name")[[f"squared_std_residual_{outcome}" for outcome in OUTCOMES]]
                    .mean().add_prefix("baseline_").reset_index())

    def subtract_baseline(group_residuals, residual_column_name):
        residuals_minus_baseline = group_residuals.merge(loo_baseline, on="repo_name", how="left")
        for outcome in OUTCOMES:
            residuals_minus_baseline[residual_column_name(outcome)] = (
                residuals_minus_baseline[residual_column_name(outcome)]
                - residuals_minus_baseline[f"baseline_squared_std_residual_{outcome}"])
        return residuals_minus_baseline

    def with_reference_squared(reference):
        reference = reference.copy()
        for outcome in OUTCOMES:
            reference[f"reference_squared_{outcome}"] = reference[f"signed_std_residual_{outcome}"] ** 2 - 1.0
        return reference

    cols = {
        "treated":          post_by_group["treated"],
        "control":          post_by_group["control"],
        "treated_adjusted": subtract_baseline(post_by_group["treated"], lambda outcome: f"squared_std_residual_{outcome}"),
    }
    ref_cols = {
        "treated":          with_reference_squared(reference_by_group["treated"]),
        "control":          with_reference_squared(reference_by_group["control"]),
        "treated_adjusted": subtract_baseline(with_reference_squared(reference_by_group["treated"]),
                                              lambda outcome: f"reference_squared_{outcome}"),
    }
    col_labels = {"treated": "Treated", "control": "Control",
                  "treated_adjusted": r"Treated $-$ LOO baseline"}
    MakePanel(
        outpath, rows=OUTCOMES, cols=cols,
        col_getter=lambda group_df, outcome: group_df[f"squared_std_residual_{outcome}"],
        row_labels=OUTCOME_LABELS, col_labels=col_labels,
        suptitle=f"{title} Squared Std Residual",
        clip=SQUARED_RESIDUAL_CLIP,
        ref_cols=ref_cols, ref_getter=lambda group_df, outcome: group_df[f"reference_squared_{outcome}"],
    )


def DrawOverlay(ax, series_1, series_2, clip):
    series_1, series_2 = series_1.dropna(), series_2.dropna()
    if len(series_1) == 0 or len(series_2) == 0:
        return
    hist_range = (-clip, clip) if clip is not None else None
    for series, fill, median_color, dash in [(series_1, "#3A5F8A", "#1F3A5F", "-"),
                                             (series_2, "#E67E22", "#A0522D", "--")]:
        ax.hist(series.clip(-clip, clip) if clip is not None else series, bins=25, range=hist_range,
                density=True, alpha=0.5, color=fill, edgecolor="white", linewidth=0.3)
        ax.axvline(float(series.median()), color=median_color, linewidth=1.2, linestyle=dash, zorder=5)
    if clip is not None:
        ax.set_xlim(-clip, clip)
    ks_result = ks_2samp(series_1.values, series_2.values)
    ks_x = KSMaxDistanceLocation(series_1.values, series_2.values)
    if ks_x is not None and (clip is None or -clip <= ks_x <= clip):
        ax.axvline(ks_x, color=KS_LINE_COLOR, linestyle=":", linewidth=1.2, zorder=6)
    ax.text(0.97, 0.97, f"n={len(series_1)} / {len(series_2)}\nKS D={ks_result.statistic:.3f}\np={ks_result.pvalue:.3f}",
            transform=ax.transAxes, fontsize=7, va="top", ha="right",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.7))
    ax.yaxis.set_major_locator(mticker.MaxNLocator(3))
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=7)


def PerOrgMeanResiduals(df_residuals):
    return df_residuals.groupby("repo_name")[[f"signed_std_residual_{outcome}" for outcome in OUTCOMES]].mean()


def ComparisonFigure(outpath, df_leaveoneout, df_post):
    loo_by_group, post_by_group = SplitGroups(df_leaveoneout), SplitGroups(df_post)
    comparisons = [
        ("Treated pre-LOO vs control post", PerOrgMeanResiduals(loo_by_group["treated"]),  PerOrgMeanResiduals(post_by_group["control"])),
        ("Pre-LOO: treated vs control",     PerOrgMeanResiduals(loo_by_group["treated"]),  PerOrgMeanResiduals(loo_by_group["control"])),
        ("Post: treated vs control",        PerOrgMeanResiduals(post_by_group["treated"]), PerOrgMeanResiduals(post_by_group["control"])),
    ]
    comparison_series = [group_means[f"signed_std_residual_{outcome}"].dropna()
                         for _, group_1, group_2 in comparisons
                         for group_means in (group_1, group_2) for outcome in OUTCOMES]
    clip = SharedDisplayBound(comparison_series, SIGNED_RESIDUAL_CLIP)

    fig, axes = plt.subplots(len(comparisons), len(OUTCOMES),
                             figsize=(4 * len(OUTCOMES), 3.3 * len(comparisons)), squeeze=False)
    for row_index, (row_title, group_1, group_2) in enumerate(comparisons):
        for col_index, outcome in enumerate(OUTCOMES):
            ax = axes[row_index][col_index]
            DrawOverlay(ax, group_1[f"signed_std_residual_{outcome}"], group_2[f"signed_std_residual_{outcome}"], clip)
            if row_index == 0:
                ax.set_title(OUTCOME_LABELS[outcome], fontsize=10)
            if row_index == len(comparisons) - 1:
                ax.set_xlabel(SIGNED_AXIS_LABEL, fontsize=8)
            if col_index == 0:
                ax.set_ylabel(row_title, fontsize=9)

    comparison_handles = [
        Line2D([0], [0], color="#1F3A5F",     linestyle="-",  linewidth=1.4, label="First-group median"),
        Line2D([0], [0], color="#A0522D",     linestyle="--", linewidth=1.4, label="Second-group median"),
        Line2D([0], [0], color=KS_LINE_COLOR, linestyle=":",  linewidth=1.4, label="Max KS distance"),
    ]
    fig.legend(handles=comparison_handles, loc="lower center", ncol=3, fontsize=9, frameon=False)
    fig.suptitle("Residual-distribution comparisons (per-org mean signed residual, density-normalized; "
                 "first group = blue/solid median, second = orange/dashed median)", fontsize=11)
    fig.tight_layout(rect=(0, 0.03, 1, 0.96))
    fig.savefig(outpath, dpi=150)
    plt.close(fig)


def SquaredDecompPanel(outpath, residuals, title):
    residuals_by_group = SplitGroups(residuals)
    MakePanel(
        outpath, rows=STAGES,
        cols={group: residuals_by_group[group] for group in GROUP_LABELS},
        col_getter=lambda group_df, stage: DecompPct(group_df, stage),
        row_labels=STAGE_LABELS, col_labels=GROUP_LABELS,
        suptitle=f"{title} — % of total-merge residual",
        xlabel="% of total-merge residual", clip=DECOMP_PERCENT_CLIP,
        bound_quantile=DECOMP_PERCENT_QUANTILE,
    )


def PlotIndividual(base_out, residuals_by_eval):
    for eval_name, residuals in residuals_by_eval.items():
        slug = eval_name.lower().replace("-", "_").replace(" ", "_")
        for group_name, group_df in SplitGroups(residuals).items():
            indiv_dir = base_out / "individual" / group_name
            indiv_dir.mkdir(parents=True, exist_ok=True)
            for outcome in OUTCOMES:
                PlotErrorDistribution(
                    group_df[f"squared_std_residual_{outcome}"],
                    f"{eval_name} {OUTCOME_LABELS[outcome]} — {group_name}",
                    indiv_dir / f"{slug}_squared_{outcome}.png",
                    clip=SQUARED_RESIDUAL_CLIP,
                )


def FormatStat(value, decimals=2):
    if value < 0:
        return rf"$\mathbf{{-}}${abs(value):.{decimals}f}"
    return f"{value:.{decimals}f}"


def SharedDisplayBound(series_list, hard_cap, quantile=0.99):
    nonempty_series = [series for series in series_list if series is not None and len(series) > 0]
    pooled_series = pd.concat(nonempty_series).dropna() if nonempty_series else pd.Series(dtype=float)
    if pooled_series.empty:
        return hard_cap
    data_extent        = float(pooled_series.abs().quantile(quantile))
    whole_number_bound = int(np.ceil(data_extent))
    return min(hard_cap, max(1, whole_number_bound))


def DecompPct(df_residuals, stage):
    total_merge_residual = df_residuals["squared_std_residual_merged_total"].replace(0, np.nan)
    return df_residuals[f"delta_squared_std_residual_{stage}"] / total_merge_residual * 100


def KSSummary(panel_values, reference):
    if reference is None:
        return None
    reference = np.asarray(reference, dtype=float)
    reference = reference[~np.isnan(reference)]
    if len(panel_values) < 3 or len(reference) < 3:
        return None
    statistic = float(ks_2samp(panel_values.values, reference).statistic)
    location  = KSMaxDistanceLocation(panel_values.values, reference)
    return statistic, location


def KSMaxDistanceLocation(sample_1, sample_2):
    sample_1 = np.sort(np.asarray(sample_1, dtype=float))
    sample_2 = np.sort(np.asarray(sample_2, dtype=float))
    sample_1 = sample_1[~np.isnan(sample_1)]
    sample_2 = sample_2[~np.isnan(sample_2)]
    if len(sample_1) < 3 or len(sample_2) < 3:
        return None
    evaluation_points = np.concatenate([sample_1, sample_2])
    cdf_1 = np.searchsorted(sample_1, evaluation_points, side="right") / len(sample_1)
    cdf_2 = np.searchsorted(sample_2, evaluation_points, side="right") / len(sample_2)
    return float(evaluation_points[np.argmax(np.abs(cdf_1 - cdf_2))])


def DistributionLegendHandles(include_reference):
    # Median/Mean/Max-KS are keyed by swatches inside each subplot's stats box; the bottom legend
    # only needs the black KDE reference curve, which has no box entry.
    if not include_reference:
        return []
    return [Line2D([0], [0], color=REFERENCE_COLOR, linestyle="-", linewidth=1.3, label="Model null (KDE)")]


def DrawDistribution(ax, panel_values, clip, edge_lw=0.4):
    if clip is not None:
        n_lo = int((panel_values < -clip).sum())
        n_hi = int((panel_values > clip).sum())
        bin_width = clip * 2 / 25
        ax.hist(panel_values.clip(-clip, clip), bins=25, range=(-clip, clip),
                color="#3A5F8A", edgecolor="white", linewidth=edge_lw, alpha=0.85)
        if n_lo > 0:
            ax.bar(-clip, n_lo, width=-bin_width, align="edge", color="#E74C3C",
                   alpha=0.9, zorder=3, edgecolor="white", linewidth=edge_lw,
                   label=f"Truncated at −{clip}: n={n_lo}")
        if n_hi > 0:
            ax.bar(clip, n_hi, width=bin_width, align="edge", color="#E74C3C",
                   alpha=0.9, zorder=3, edgecolor="white", linewidth=edge_lw,
                   label=f"Truncated at +{clip}: n={n_hi}")
        ax.set_xlim(-clip - bin_width, clip + bin_width)
    else:
        n_lo = n_hi = 0
        ax.hist(panel_values, bins=25, color="#3A5F8A", edgecolor="white", linewidth=edge_lw, alpha=0.85)

    p25, p75 = float(panel_values.quantile(0.25)), float(panel_values.quantile(0.75))
    panel_median, panel_mean = float(panel_values.median()), float(panel_values.mean())
    ax.axvspan(p25, p75, alpha=0.12, color="#888888", label=f"IQR  [{FormatStat(p25, 1)}, {FormatStat(p75, 1)}]")
    if clip is None or -clip <= panel_median <= clip:
        ax.axvline(panel_median, color=MEDIAN_COLOR, linewidth=1.6, linestyle="-", zorder=5,
                   label=f"Median = {FormatStat(panel_median, 1)}")
    if clip is None or -clip <= panel_mean <= clip:
        ax.axvline(panel_mean, color=MEAN_COLOR, linewidth=1.6, linestyle="--", zorder=5,
                   label=f"Mean = {FormatStat(panel_mean, 1)}")
    return n_lo + n_hi


def StatsSegments(panel_values, clip, n_trunc, reference=None):
    # each segment is (text, line) where line is (color, linestyle) for a swatch, or None for plain text
    segments = [
        (f"n={len(panel_values)}", None),
        (f"Mean={FormatStat(float(panel_values.mean()))}", (MEAN_COLOR, "--")),
        (f"Med={FormatStat(float(panel_values.median()))}", (MEDIAN_COLOR, "-")),
        (f"95%=[{FormatStat(float(panel_values.quantile(0.025)))},{FormatStat(float(panel_values.quantile(0.975)))}]", None),
    ]
    if clip is not None:
        segments.append((f"|x|>{clip}: {n_trunc}", None))
    ks_summary = KSSummary(panel_values, reference)
    if ks_summary is not None:
        statistic, location = ks_summary
        segments.append((f"KS D={statistic:.3f} @ x={location:.2f}", (KS_LINE_COLOR, ":")))
    return segments


def DrawStatsBox(ax, segments):
    segment_rows = []
    for segment_text, swatch_style in segments:
        swatch = DrawingArea(20, 8, 0, 0)
        if swatch_style is not None:
            color, linestyle = swatch_style
            swatch.add_artist(Line2D([1, 19], [4, 4], color=color, linestyle=linestyle, linewidth=1.6))
        segment_rows.append(HPacker(children=[swatch, TextArea(segment_text, textprops=dict(color="black", size=7))],
                                    align="center", pad=0, sep=3))
    packed = VPacker(children=segment_rows, align="left", pad=0, sep=1)
    box = AnnotationBbox(packed, (0.985, 0.985), xycoords="axes fraction", box_alignment=(1, 1),
                         frameon=True, pad=0.3,
                         bboxprops=dict(facecolor="white", alpha=0.7, edgecolor="0.7", linewidth=0.5))
    ax.add_artist(box)


def OverlayReferenceKde(ax, reference, n_in_range, clip):
    reference = np.asarray(reference, dtype=float)
    reference = reference[~np.isnan(reference)]
    if clip is not None:
        reference = reference[(reference >= -clip) & (reference <= clip)]
    if len(reference) < 5 or np.std(reference) == 0 or n_in_range <= 0:
        return
    kde = KDEUnivariate(reference)
    kde.fit(kernel="gau", bw="scott", fft=True, gridsize=512)
    hist_width = clip * 2 / 25 if clip is not None else (reference.max() - reference.min()) / 25
    ax.plot(kde.support, kde.density * n_in_range * hist_width, color=REFERENCE_COLOR, linewidth=1.3, zorder=6)


def PlotPanelSubplot(ax, panel_values, title="", xlabel="", clip=None, reference=None):
    if len(panel_values) == 0:
        ax.set_title(title, fontsize=9)
        return
    n_trunc = DrawDistribution(ax, panel_values, clip)
    if reference is not None:
        OverlayReferenceKde(ax, reference, len(panel_values) - n_trunc, clip)
        ks_x = KSMaxDistanceLocation(panel_values.values, reference)
        if ks_x is not None and (clip is None or -clip <= ks_x <= clip):
            ax.axvline(ks_x, color=KS_LINE_COLOR, linestyle=":", linewidth=1.4, zorder=7)
    DrawStatsBox(ax, StatsSegments(panel_values, clip, n_trunc, reference))
    ax.set_title(title, fontsize=9)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=8)
    ax.set_ylabel("Count", fontsize=8)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=7)


def PlotErrorDistribution(series, xlabel, outpath, clip=None):
    n_total = len(series)
    panel_values = series.dropna()
    nan_percentage = 100.0 * (n_total - len(panel_values)) / n_total

    fig, ax = plt.subplots(figsize=(7, 4.5))
    n_trunc = DrawDistribution(ax, panel_values, clip, edge_lw=0.5)
    if nan_percentage > 0:
        ax.plot([], [], " ", label=f"NA: {nan_percentage:.1f}%")
    if clip is not None:
        ax.plot([], [], " ", label=f"|x|>{clip}: {n_trunc}")

    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel("Org-period count", fontsize=11)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.legend(frameon=False, fontsize=9, loc="upper right")
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=9)
    fig.tight_layout()
    fig.savefig(outpath, dpi=150)
    plt.close(fig)


def RenderPanelGrid(outpath, cell_grid, hard_cap,
                    suptitle="", xlabel="Squared std residual", suptitle_fontsize=12, bound_quantile=0.99):
    all_cell_series = [cell_series for cell_row in cell_grid for (_, cell_series, _) in cell_row if cell_series is not None]
    shared_clip = SharedDisplayBound(all_cell_series, hard_cap, bound_quantile) if hard_cap is not None else None
    n_rows = len(cell_grid)
    n_cols = max(len(cell_row) for cell_row in cell_grid)
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 3.5 * n_rows), squeeze=False)
    for row_index, cell_row in enumerate(cell_grid):
        for col_index in range(n_cols):
            ax = axes[row_index][col_index]
            cell_title, cell_series, cell_reference = cell_row[col_index] if col_index < len(cell_row) else ("", None, None)
            if cell_series is None or len(cell_series) == 0:
                ax.set_title(cell_title, fontsize=9)
                continue
            PlotPanelSubplot(ax, cell_series, cell_title, xlabel=xlabel, clip=shared_clip, reference=cell_reference)
    include_reference = any(cell[2] is not None for cell_row in cell_grid for cell in cell_row)
    handles = DistributionLegendHandles(include_reference)
    bottom_margin = 0.0
    if handles:
        fig.legend(handles=handles, loc="lower center", ncol=len(handles), fontsize=9, frameon=False)
        bottom_margin = 0.03
    fig.suptitle(suptitle, fontsize=suptitle_fontsize)
    fig.tight_layout(rect=(0, bottom_margin, 1, 0.97))
    fig.savefig(outpath, dpi=150)
    plt.close(fig)


def MakePanel(outpath, rows, cols, col_getter, row_labels, col_labels,
              suptitle="", xlabel="Squared std residual", clip=None,
              ref_cols=None, ref_getter=None, bound_quantile=0.99):
    def reference_for(col_key, row_key):
        if ref_cols is None or ref_cols.get(col_key) is None:
            return None
        return ref_getter(ref_cols[col_key], row_key).dropna().values

    cell_grid = [
        [(f"{row_labels[row_key]}\n{col_labels[col_key]}",
          col_getter(group_df, row_key).dropna(),
          reference_for(col_key, row_key))
         for row_key in rows]
        for col_key, group_df in cols.items()
    ]
    RenderPanelGrid(outpath, cell_grid, clip, suptitle=suptitle, xlabel=xlabel, bound_quantile=bound_quantile)


if __name__ == "__main__":
    Main()
