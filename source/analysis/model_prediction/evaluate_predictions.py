import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from pathlib import Path
from itertools import product
from scipy.stats import ks_2samp, gaussian_kde

from source.lib.python.config_loaders import LoadPipelineInputs, LoadModelPredictionConfig

MODEL_PREDICTION_CONFIG = LoadModelPredictionConfig()
CONFIG                  = LoadPipelineInputs()

INDIR  = Path("output/analysis/model_prediction")
OUTDIR = Path("output/analysis/model_prediction")
VARIANTS              = MODEL_PREDICTION_CONFIG["variants"]["run"]
DISTRIBUTION_TYPES    = MODEL_PREDICTION_CONFIG["distribution_types"]["run"]
ESTIMATION_APPROACHES = MODEL_PREDICTION_CONFIG["member_probability_estimation"]["run"]
EVALUATION_FIGURES    = MODEL_PREDICTION_CONFIG["evaluation_figures"]["run"]

OUTCOMES = ["open", "review", "direct_merge", "reviewed_merge", "total_merge"]
STAGES   = ["open", "review", "direct_merge", "reviewed_merge"]

OUTCOME_LABELS = {
    "open":           r"$N^o$",
    "review":         r"$N^r$",
    "direct_merge":   r"$N^{m|o}$",
    "reviewed_merge": r"$N^{m|r}$",
    "total_merge":    r"$N^m$",
}
STAGE_LABELS = {
    "open":           r"$\Delta Q_\mathrm{open}$",
    "review":         r"$\Delta Q_\mathrm{review}$",
    "direct_merge":   r"$\Delta Q_\mathrm{direct\ merge}$",
    "reviewed_merge": r"$\Delta Q_\mathrm{reviewed\ merge}$",
}
GROUP_LABELS = {"treated": "Treated", "control": "Control"}
Z_AXIS_LABEL = r"$Z = (x - \mu) / \sigma$"
SQUARED_RESIDUAL_CLIP = 15
SIGNED_RESIDUAL_CLIP  = 10


def Main():
    importance_types  = CONFIG["importance_types"]["run"]
    qualified_samples = CONFIG["qualified_samples"]["run"]
    control_groups    = CONFIG["control_groups"]["run"]

    for variant, distribution_type, estimation_approach, importance_type, qualified_sample, control_group in product(
        VARIANTS, DISTRIBUTION_TYPES, ESTIMATION_APPROACHES,
        importance_types, qualified_samples, control_groups
    ):
        RunCombination(
            variant, distribution_type, estimation_approach,
            importance_type, qualified_sample, control_group
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

    panels_squared = base_out / "panels" / "q"
    panels_signed  = base_out / "panels" / "z"
    panels_squared.mkdir(parents=True, exist_ok=True)
    panels_signed.mkdir(parents=True, exist_ok=True)

    fit_panels = [
        ("pre_period_fit_insample",    df_insample,    df_insample_reference,    "Pre-Period Fit (InSample)"),
        ("pre_period_fit_leaveoneout", df_leaveoneout, df_leaveoneout_reference, "Pre-Period Fit (LeaveOneOut)"),
        ("post_period_fit",            df_post,        df_post_reference,        "Post-Period Fit"),
    ]
    for name, df_residuals, df_reference, title in fit_panels:
        SignedFitPanel(panels_signed / f"{name}.png", df_residuals, df_reference, title)
        SquaredFitPanel(panels_squared / f"{name}.png", df_residuals, df_reference, title)

    decomp_panels = [
        ("pre_period_decomp_insample",    df_insample,    "Pre-Period Decomp (InSample)"),
        ("pre_period_decomp_leaveoneout", df_leaveoneout, "Pre-Period Decomp (LeaveOneOut)"),
        ("post_period_decomp",            df_post,        "Post-Period Decomp"),
    ]
    for name, df_residuals, title in decomp_panels:
        SquaredDecompPanel(panels_squared / f"{name}.png", df_residuals, title)
        SignedDecompPanel(panels_signed / f"{name}.png", df_residuals, title)

    if "individual" in EVALUATION_FIGURES:
        PlotIndividual(base_out, {"InSample": df_insample, "LeaveOneOut": df_leaveoneout, "Post-Period": df_post})


def SplitGroups(df):
    return {"treated": df[df["is_treated"]], "control": df[~df["is_treated"]]}


def SignedFitPanel(outpath, residuals, reference, title):
    groups, refs = SplitGroups(residuals), SplitGroups(reference)
    MakePanel(
        outpath, rows=OUTCOMES,
        cols={group: groups[group] for group in GROUP_LABELS},
        col_getter=lambda df, o: df[f"signed_std_residual_{o}"],
        row_labels=OUTCOME_LABELS, col_labels=GROUP_LABELS,
        suptitle=f"{title} Signed Std Residual", xlabel=Z_AXIS_LABEL,
        clip=SIGNED_RESIDUAL_CLIP,
        ref_cols={group: refs[group] for group in GROUP_LABELS},
        ref_getter=lambda df, o: df[f"signed_std_residual_{o}"],
    )


def SquaredFitPanel(outpath, residuals, reference, title):
    groups, refs = SplitGroups(residuals), SplitGroups(reference)
    MakePanel(
        outpath, rows=OUTCOMES,
        cols={group: groups[group] for group in GROUP_LABELS},
        col_getter=lambda df, o: df[f"squared_std_residual_{o}"],
        row_labels=OUTCOME_LABELS, col_labels=GROUP_LABELS,
        suptitle=f"{title} Squared Std Residual",
        clip=SQUARED_RESIDUAL_CLIP,
        ref_cols={group: refs[group] for group in GROUP_LABELS},
        ref_getter=lambda df, o: df[f"signed_std_residual_{o}"] ** 2 - 1.0,
    )


def SquaredDecompPanel(outpath, residuals, title):
    groups = SplitGroups(residuals)
    MakePanel(
        outpath, rows=STAGES,
        cols={group: groups[group] for group in GROUP_LABELS},
        col_getter=lambda df, s: DecompPct(df, s),
        row_labels=STAGE_LABELS, col_labels=GROUP_LABELS,
        suptitle=f"{title} — % of total-merge residual",
        xlabel="% of total-merge residual", clip=SQUARED_RESIDUAL_CLIP,
    )


def SignedDecompPanel(outpath, residuals, title):
    groups = SplitGroups(residuals)
    MakeDecompSignedPanel(
        outpath,
        {"Treated": groups["treated"], "Control": groups["control"]},
        suptitle=f"{title} Signed",
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
                    indiv_dir / f"{slug}_Q_{outcome}.png",
                    clip=SQUARED_RESIDUAL_CLIP,
                )


def FormatStat(value, decimals=2):
    if value < 0:
        return rf"$\mathbf{{-}}${abs(value):.{decimals}f}"
    return f"{value:.{decimals}f}"


def SharedDisplayBound(series_list, hard_cap, quantile=0.99):
    nonempty = [s for s in series_list if s is not None and len(s) > 0]
    pooled = pd.concat(nonempty).dropna() if nonempty else pd.Series(dtype=float)
    if pooled.empty:
        return hard_cap
    data_extent        = float(pooled.abs().quantile(quantile))
    whole_number_bound = int(np.ceil(data_extent))
    return min(hard_cap, max(1, whole_number_bound))


def DecompPct(df, stage):
    total = df["squared_std_residual_total_merge"].replace(0, np.nan)
    return df[f"delta_squared_std_residual_{stage}"] / total * 100


def KSNote(vals, reference):
    if reference is None:
        return ""
    reference = np.asarray(reference, dtype=float)
    reference = reference[~np.isnan(reference)]
    if len(vals) < 3 or len(reference) < 3:
        return ""
    ks = ks_2samp(vals.values, reference)
    return f"\nKS D={ks.statistic:.3f} p={ks.pvalue:.3f}"


def DrawDistribution(ax, vals, clip, edge_lw=0.4):
    if clip is not None:
        n_lo = int((vals < -clip).sum())
        n_hi = int((vals > clip).sum())
        bin_width = clip * 2 / 25
        ax.hist(vals.clip(-clip, clip), bins=25, range=(-clip, clip),
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
        ax.hist(vals, bins=25, color="#3A5F8A", edgecolor="white", linewidth=edge_lw, alpha=0.85)

    p25, p75 = float(vals.quantile(0.25)), float(vals.quantile(0.75))
    median_val, mean_val = float(vals.median()), float(vals.mean())
    ax.axvspan(p25, p75, alpha=0.12, color="#888888", label=f"IQR  [{FormatStat(p25, 1)}, {FormatStat(p75, 1)}]")
    if clip is None or -clip <= median_val <= clip:
        ax.axvline(median_val, color="#C0392B", linewidth=1.6, linestyle="-", zorder=5,
                   label=f"Median = {FormatStat(median_val, 1)}")
    if clip is None or -clip <= mean_val <= clip:
        ax.axvline(mean_val, color="#27AE60", linewidth=1.6, linestyle="--", zorder=5,
                   label=f"Mean = {FormatStat(mean_val, 1)}")
    return n_lo + n_hi


def StatsText(vals, clip, n_trunc, reference=None):
    lines = [
        f"n={len(vals)}",
        f"Mean={FormatStat(float(vals.mean()))}",
        f"Med={FormatStat(float(vals.median()))}",
        f"95%=[{FormatStat(float(vals.quantile(0.025)))},{FormatStat(float(vals.quantile(0.975)))}]",
    ]
    if clip is not None:
        lines.append(f"|x|>{clip}: {n_trunc}")
    return "\n".join(lines) + KSNote(vals, reference)


def OverlayReferenceKde(ax, reference, n_in_range, clip):
    reference = np.asarray(reference, dtype=float)
    reference = reference[~np.isnan(reference)]
    if clip is not None:
        reference = reference[(reference >= -clip) & (reference <= clip)]
    if len(reference) < 5 or np.std(reference) == 0 or n_in_range <= 0:
        return
    grid = np.linspace(reference.min(), reference.max(), 200)
    bin_width = clip * 2 / 25 if clip is not None else (grid[-1] - grid[0]) / 25
    ax.plot(grid, gaussian_kde(reference)(grid) * n_in_range * bin_width,
            color="#1A1A1A", linewidth=1.3, zorder=6)


def PlotPanelSubplot(ax, vals, title="", xlabel="", clip=None, reference=None):
    if len(vals) == 0:
        ax.set_title(title, fontsize=9)
        return
    n_trunc = DrawDistribution(ax, vals, clip)
    if reference is not None:
        OverlayReferenceKde(ax, reference, len(vals) - n_trunc, clip)
    ax.text(0.97, 0.97, StatsText(vals, clip, n_trunc, reference), transform=ax.transAxes,
            fontsize=7, va="top", ha="right",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.7))
    ax.set_title(title, fontsize=9)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=8)
    ax.set_ylabel("Count", fontsize=8)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=7)


def PlotErrorDistribution(series, xlabel, outpath, clip=None):
    n_total = len(series)
    vals    = series.dropna()
    na_pct  = 100.0 * (n_total - len(vals)) / n_total

    fig, ax = plt.subplots(figsize=(7, 4.5))
    n_trunc = DrawDistribution(ax, vals, clip, edge_lw=0.5)
    if na_pct > 0:
        ax.plot([], [], " ", label=f"NA: {na_pct:.1f}%")
    if clip is not None:
        ax.plot([], [], " ", label=f"|x|>{clip}: {n_trunc}")

    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel("Org-period count", fontsize=11)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.legend(frameon=False, fontsize=9, loc="upper right")
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=9)
    fig.tight_layout()
    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)


def RenderPanelGrid(outpath, cell_grid, hard_cap,
                    suptitle="", xlabel="Squared std residual", suptitle_fontsize=12):
    all_series  = [series for row in cell_grid for (_, series, _) in row if series is not None]
    shared_clip = SharedDisplayBound(all_series, hard_cap) if hard_cap is not None else None
    n_rows = len(cell_grid)
    n_cols = max(len(row) for row in cell_grid)
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 3.5 * n_rows), squeeze=False)
    for i, row in enumerate(cell_grid):
        for j in range(n_cols):
            ax = axes[i][j]
            title, series, reference = row[j] if j < len(row) else ("", None, None)
            if series is None or len(series) == 0:
                ax.set_title(title, fontsize=9)
                continue
            PlotPanelSubplot(ax, series, title, xlabel=xlabel, clip=shared_clip, reference=reference)
    fig.suptitle(suptitle, fontsize=suptitle_fontsize, y=1.01)
    fig.tight_layout()
    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)


def MakePanel(outpath, rows, cols, col_getter, row_labels, col_labels,
              suptitle="", xlabel="Squared std residual", clip=None,
              ref_cols=None, ref_getter=None):
    def reference_for(col_key, row_key):
        if ref_cols is None or ref_cols.get(col_key) is None:
            return None
        return ref_getter(ref_cols[col_key], row_key).dropna().values

    cell_grid = [
        [(f"{row_labels[row_key]}\n{col_labels[col_key]}",
          col_getter(df_col, row_key).dropna(),
          reference_for(col_key, row_key))
         for row_key in rows]
        for col_key, df_col in cols.items()
    ]
    RenderPanelGrid(outpath, cell_grid, clip, suptitle=suptitle, xlabel=xlabel)


def MakeDecompSignedPanel(outpath, group_data, suptitle=""):
    def StageSeries(df, stage):
        total = df["squared_std_residual_total_merge"].replace(0, np.nan)
        return (df[f"delta_squared_std_residual_{stage}"] / total * df["signed_std_residual_total_merge"]).dropna()

    cell_grid = [
        [(f"{STAGE_LABELS[stage]}\n{label}", StageSeries(df, stage), None) for stage in STAGES]
        for label, df in group_data.items()
    ]
    RenderPanelGrid(outpath, cell_grid, SIGNED_RESIDUAL_CLIP,
                    suptitle=suptitle, xlabel=r"$(\Delta Q / Q^m) \times Z^m$", suptitle_fontsize=11)


if __name__ == "__main__":
    Main()
