import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from pathlib import Path
from itertools import product
from scipy.stats import kstest, chi2

from source.lib.python.config_loaders import LoadPipelineInputs, LoadModelPredictionConfig

MODEL_PREDICTION_CONFIG = LoadModelPredictionConfig()
CONFIG                  = LoadPipelineInputs()

INDIR  = Path("output/analysis/model_prediction")
OUTDIR = Path("output/analysis/model_prediction")
VARIANTS              = MODEL_PREDICTION_CONFIG["variants"]["run"]
DISTRIBUTION_TYPES    = MODEL_PREDICTION_CONFIG["distribution_types"]["run"]
ESTIMATION_APPROACHES = MODEL_PREDICTION_CONFIG["member_probability_estimation"]["run"]

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
        INDIR / variant / distribution_type / "predictions" / estimation_approach
        / importance_type / qualified_sample / control_group
    )
    base_out = (
        OUTDIR / variant / distribution_type / "evaluation" / estimation_approach
        / importance_type / qualified_sample / control_group
    )

    df_insample = pd.read_parquet(pred_dir / "insample_evaluation.parquet")
    df_loo      = pd.read_parquet(pred_dir / "leaveoneout_evaluation.parquet")
    df_post     = pd.read_parquet(pred_dir / "post_evaluation.parquet")
    df_insample_signed = pd.read_parquet(pred_dir / "insample_period_z.parquet")
    df_loo_signed      = pd.read_parquet(pred_dir / "leaveoneout_period_z.parquet")
    df_post_signed     = pd.read_parquet(pred_dir / "post_period_z.parquet")

    df_post_avg        = df_post.groupby(["repo_name", "is_treated"]).mean(numeric_only=True).reset_index()
    df_post_signed_avg = df_post_signed.groupby(["repo_name", "is_treated"]).mean(numeric_only=True).reset_index()

    groups = {
        "treated": (
            df_insample[df_insample["is_treated"]], df_loo[df_loo["is_treated"]],
            df_post_avg[df_post_avg["is_treated"]],
            df_insample_signed[df_insample_signed["is_treated"]], df_loo_signed[df_loo_signed["is_treated"]],
            df_post_signed_avg[df_post_signed_avg["is_treated"]],
        ),
        "control": (
            df_insample[~df_insample["is_treated"]], df_loo[~df_loo["is_treated"]],
            df_post_avg[~df_post_avg["is_treated"]],
            df_insample_signed[~df_insample_signed["is_treated"]], df_loo_signed[~df_loo_signed["is_treated"]],
            df_post_signed_avg[~df_post_signed_avg["is_treated"]],
        ),
    }

    panels_squared = base_out / "panels" / "q"
    panels_signed  = base_out / "panels" / "z"
    panels_squared.mkdir(parents=True, exist_ok=True)
    panels_signed.mkdir(parents=True, exist_ok=True)

    treated_data = groups["treated"]
    control_data = groups["control"]

    MakePanel(
        panels_squared / "pre_period_fit_insample.png",
        rows=OUTCOMES,
        cols={"treated": treated_data[0], "control": control_data[0]},
        col_getter=lambda df, o: df[f"mean_squared_std_residual_insample_{o}"],
        row_labels=OUTCOME_LABELS,
        col_labels={"treated": "Treated", "control": "Control"},
        suptitle="Pre-Period Fit (InSample) Squared Std Residual",
        clip=SQUARED_RESIDUAL_CLIP,
        ks_dist="chi2m1",
    )
    MakePanel(
        panels_squared / "pre_period_fit_leaveoneout.png",
        rows=OUTCOMES,
        cols={"treated": treated_data[1], "control": control_data[1]},
        col_getter=lambda df, o: df[f"mean_squared_std_residual_leaveoneout_{o}"],
        row_labels=OUTCOME_LABELS,
        col_labels={"treated": "Treated", "control": "Control"},
        suptitle="Pre-Period Fit (LeaveOneOut) Squared Std Residual",
        clip=SQUARED_RESIDUAL_CLIP,
        ks_dist="chi2m1",
    )

    MakePanel(
        panels_squared / "pre_period_decomp_insample.png",
        rows=STAGES,
        cols={"treated": treated_data[0], "control": control_data[0]},
        col_getter=lambda df, s: DecompPct(df, s, "mean_squared_std_residual_insample"),
        row_labels=STAGE_LABELS,
        col_labels={"treated": "Treated", "control": "Control"},
        suptitle="Pre-Period Decomp (InSample) — % of total-merge residual",
        xlabel="% of total-merge residual",
        clip=SQUARED_RESIDUAL_CLIP,
        ks_dist="chi2m1",
    )
    MakePanel(
        panels_squared / "pre_period_decomp_leaveoneout.png",
        rows=STAGES,
        cols={"treated": treated_data[1], "control": control_data[1]},
        col_getter=lambda df, s: DecompPct(df, s, "mean_squared_std_residual_leaveoneout"),
        row_labels=STAGE_LABELS,
        col_labels={"treated": "Treated", "control": "Control"},
        suptitle="Pre-Period Decomp (LeaveOneOut) — % of total-merge residual",
        xlabel="% of total-merge residual",
        clip=SQUARED_RESIDUAL_CLIP,
        ks_dist="chi2m1",
    )

    MakePanelSingleCol(
        panels_squared / "post_period_fit_control.png",
        rows=OUTCOMES, df=control_data[2],
        col_getter=lambda df, o: df[f"squared_std_residual_{o}"],
        row_labels=OUTCOME_LABELS,
        suptitle="Post-Period Fit Squared Std Residual — Control",
        clip=SQUARED_RESIDUAL_CLIP,
        ks_dist="chi2m1",
    )
    MakePanelSingleCol(
        panels_squared / "post_period_fit_treated.png",
        rows=OUTCOMES, df=treated_data[2],
        col_getter=lambda df, o: df[f"squared_std_residual_{o}"],
        row_labels=OUTCOME_LABELS,
        suptitle="Post-Period Fit Squared Std Residual — Treated",
        clip=SQUARED_RESIDUAL_CLIP,
        ks_dist="chi2m1",
    )

    MakePanelSingleCol(
        panels_squared / "post_period_decomp_control.png",
        rows=STAGES, df=control_data[2],
        col_getter=lambda df, s: PostDecompPct(df, s),
        row_labels=STAGE_LABELS,
        suptitle="Post-Period Decomp — % of total-merge residual — Control",
        xlabel="% of total-merge residual",
        clip=SQUARED_RESIDUAL_CLIP,
        ks_dist="chi2m1",
    )
    MakePanelSingleCol(
        panels_squared / "post_period_decomp_treated.png",
        rows=STAGES, df=treated_data[2],
        col_getter=lambda df, s: PostDecompPct(df, s),
        row_labels=STAGE_LABELS,
        suptitle="Post-Period Decomp — % of total-merge residual — Treated",
        xlabel="% of total-merge residual",
        clip=SQUARED_RESIDUAL_CLIP,
        ks_dist="chi2m1",
    )

    MakePanel(
        panels_signed / "pre_period_fit_insample.png",
        rows=OUTCOMES,
        cols={"treated": treated_data[3], "control": control_data[3]},
        col_getter=lambda df, o: df[f"mean_signed_std_residual_{o}"],
        row_labels=OUTCOME_LABELS,
        col_labels={"treated": "Treated", "control": "Control"},
        suptitle="Pre-Period Fit (InSample) Signed Std Residual",
        xlabel=r"$Z = (x - \mu) / \sigma$",
        clip=SIGNED_RESIDUAL_CLIP,
        ks_dist="norm",
    )
    MakePanel(
        panels_signed / "pre_period_fit_leaveoneout.png",
        rows=OUTCOMES,
        cols={"treated": treated_data[4], "control": control_data[4]},
        col_getter=lambda df, o: df[f"signed_std_residual_{o}"],
        row_labels=OUTCOME_LABELS,
        col_labels={"treated": "Treated", "control": "Control"},
        suptitle="Pre-Period Fit (LeaveOneOut) Signed Std Residual",
        xlabel=r"$Z = (x - \mu) / \sigma$",
        clip=SIGNED_RESIDUAL_CLIP,
        ks_dist="norm",
    )

    for group_name, group_squared, group_signed, group_loo_squared, group_loo_signed in [
        ("treated", treated_data[0], treated_data[3], treated_data[1], treated_data[4]),
        ("control", control_data[0], control_data[3], control_data[1], control_data[4]),
    ]:
        MakeDecompSignedPanel(
            panels_signed / f"pre_period_decomp_insample_{group_name}.png",
            df_squared=group_squared, df_signed=group_signed,
            squared_prefix="mean_squared_std_residual_insample", signed_prefix="mean_signed_std_residual",
            suptitle=f"Pre-Period Decomp (InSample) Signed — {group_name.title()}",
        )
        MakeDecompSignedPanel(
            panels_signed / f"pre_period_decomp_leaveoneout_{group_name}.png",
            df_squared=group_loo_squared, df_signed=group_loo_signed,
            squared_prefix="mean_squared_std_residual_leaveoneout", signed_prefix="signed_std_residual",
            suptitle=f"Pre-Period Decomp (LeaveOneOut) Signed — {group_name.title()}",
        )

    MakePanelSingleCol(
        panels_signed / "post_period_fit_control.png",
        rows=OUTCOMES, df=control_data[5],
        col_getter=lambda df, o: df[f"signed_std_residual_{o}"],
        row_labels=OUTCOME_LABELS,
        suptitle="Post-Period Fit Signed Std Residual — Control",
        xlabel=r"$Z = (x - \mu) / \sigma$",
        clip=SIGNED_RESIDUAL_CLIP,
        ks_dist="norm",
    )
    MakePanelSingleCol(
        panels_signed / "post_period_fit_treated.png",
        rows=OUTCOMES, df=treated_data[5],
        col_getter=lambda df, o: df[f"signed_std_residual_{o}"],
        row_labels=OUTCOME_LABELS,
        suptitle="Post-Period Fit Signed Std Residual — Treated",
        xlabel=r"$Z = (x - \mu) / \sigma$",
        clip=SIGNED_RESIDUAL_CLIP,
        ks_dist="norm",
    )

    for group_name, group_post_squared, group_post_signed in [
        ("control", control_data[2], control_data[5]),
        ("treated", treated_data[2], treated_data[5]),
    ]:
        MakeDecompSignedPanel(
            panels_signed / f"post_period_decomp_{group_name}.png",
            df_squared=group_post_squared, df_signed=group_post_signed,
            squared_prefix="", signed_prefix="signed_std_residual",
            total_col="squared_std_residual_total_merge",
            stage_fmt="delta_squared_std_residual_{s}",
            suptitle=f"Post-Period Decomp Signed — {group_name.title()}",
        )

    for group_name, group_insample, group_loo, group_post in [
        ("treated", treated_data[0], treated_data[1], treated_data[2]),
        ("control", control_data[0], control_data[1], control_data[2]),
    ]:
        indiv_dir = base_out / "individual" / group_name
        indiv_dir.mkdir(parents=True, exist_ok=True)
        for o in OUTCOMES:
            PlotErrorDistribution(
                group_insample[f"mean_squared_std_residual_insample_{o}"],
                f"InSample {OUTCOME_LABELS[o]} — {group_name}",
                indiv_dir / f"pre_period_insample_Q_{o}.png",
                clip=SQUARED_RESIDUAL_CLIP,
                ks_dist="chi2m1",
            )
            PlotErrorDistribution(
                group_loo[f"mean_squared_std_residual_leaveoneout_{o}"],
                f"LeaveOneOut {OUTCOME_LABELS[o]} — {group_name}",
                indiv_dir / f"pre_period_leaveoneout_Q_{o}.png",
                clip=SQUARED_RESIDUAL_CLIP,
                ks_dist="chi2m1",
            )
            PlotErrorDistribution(
                group_post[f"squared_std_residual_{o}"],
                f"Post-Period {OUTCOME_LABELS[o]} — {group_name}",
                indiv_dir / f"post_period_Q_{o}.png",
                clip=SQUARED_RESIDUAL_CLIP,
                ks_dist="chi2m1",
            )
        for s in STAGES:
            PlotErrorDistribution(
                group_insample[f"mean_squared_std_residual_insample_delta_{s}"],
                f"InSample {STAGE_LABELS[s]} — {group_name}",
                indiv_dir / f"pre_period_insample_delta_Q_{s}.png",
                clip=SQUARED_RESIDUAL_CLIP,
                ks_dist="chi2m1",
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


def DecompPct(df, stage, prefix):
    total = df[f"{prefix}_total_merge"].replace(0, np.nan)
    return df[f"{prefix}_delta_{stage}"] / total * 100


def PostDecompPct(df, stage):
    total = df["squared_std_residual_total_merge"].replace(0, np.nan)
    return df[f"delta_squared_std_residual_{stage}"] / total * 100


def KSNote(vals, ks_dist):
    if ks_dist is None or len(vals) < 3:
        return ""
    if ks_dist == "norm":
        ks = kstest(vals.values, "norm")
    elif ks_dist == "chi2m1":
        ks = kstest(vals.values, lambda x: chi2.cdf(x + 1, df=1))
    else:
        return ""
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


def StatsText(vals, clip, n_trunc, ks_dist):
    lines = [
        f"n={len(vals)}",
        f"Mean={FormatStat(float(vals.mean()))}",
        f"Med={FormatStat(float(vals.median()))}",
        f"95%=[{FormatStat(float(vals.quantile(0.025)))},{FormatStat(float(vals.quantile(0.975)))}]",
    ]
    if clip is not None:
        lines.append(f"|x|>{clip}: {n_trunc}")
    return "\n".join(lines) + KSNote(vals, ks_dist)


def PlotPanelSubplot(ax, vals, title="", xlabel="", clip=None, ks_dist=None):
    if len(vals) == 0:
        ax.set_title(title, fontsize=9)
        return
    n_trunc = DrawDistribution(ax, vals, clip)
    ax.text(0.97, 0.97, StatsText(vals, clip, n_trunc, ks_dist), transform=ax.transAxes,
            fontsize=7, va="top", ha="right",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.7))
    ax.set_title(title, fontsize=9)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=8)
    ax.set_ylabel("Count", fontsize=8)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=7)


def PlotErrorDistribution(series, xlabel, outpath, clip=None, ks_dist=None):
    n_total = len(series)
    vals    = series.dropna()
    na_pct  = 100.0 * (n_total - len(vals)) / n_total

    fig, ax = plt.subplots(figsize=(7, 4.5))
    n_trunc = DrawDistribution(ax, vals, clip, edge_lw=0.5)
    if na_pct > 0:
        ax.plot([], [], " ", label=f"NA: {na_pct:.1f}%")
    if clip is not None:
        ax.plot([], [], " ", label=f"|x|>{clip}: {n_trunc}")
    ks_note = KSNote(vals, ks_dist)
    if ks_note:
        ax.plot([], [], " ", label=ks_note.strip())

    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel("Organization count", fontsize=11)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.legend(frameon=False, fontsize=9, loc="upper right")
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=9)
    fig.tight_layout()
    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)


def RenderPanelGrid(outpath, cell_grid, hard_cap, ks_dist,
                    suptitle="", xlabel="Squared std residual", suptitle_fontsize=12):
    all_series  = [series for row in cell_grid for (_, series) in row if series is not None]
    shared_clip = SharedDisplayBound(all_series, hard_cap) if hard_cap is not None else None
    n_rows = len(cell_grid)
    n_cols = max(len(row) for row in cell_grid)
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 3.5 * n_rows), squeeze=False)
    for i, row in enumerate(cell_grid):
        for j in range(n_cols):
            ax = axes[i][j]
            title, series = row[j] if j < len(row) else ("", None)
            if series is None or len(series) == 0:
                ax.set_title(title, fontsize=9)
                continue
            PlotPanelSubplot(ax, series, title, xlabel=xlabel, clip=shared_clip, ks_dist=ks_dist)
    fig.suptitle(suptitle, fontsize=suptitle_fontsize, y=1.01)
    fig.tight_layout()
    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)


def MakePanel(outpath, rows, cols, col_getter, row_labels, col_labels,
              suptitle="", xlabel="Squared std residual", clip=None, ks_dist=None):
    cell_grid = [
        [(f"{row_labels[row_key]}\n{col_labels[col_key]}", col_getter(df_col, row_key).dropna())
         for row_key in rows]
        for col_key, df_col in cols.items()
    ]
    RenderPanelGrid(outpath, cell_grid, clip, ks_dist, suptitle=suptitle, xlabel=xlabel)


def MakePanelSingleCol(outpath, rows, df, col_getter, row_labels,
                       suptitle="", xlabel="Squared std residual", clip=None, ks_dist=None):
    cell_grid = [[(row_labels[row_key], col_getter(df, row_key).dropna()) for row_key in rows]]
    RenderPanelGrid(outpath, cell_grid, clip, ks_dist, suptitle=suptitle, xlabel=xlabel)


def MakeDecompSignedPanel(outpath, df_squared, df_signed, squared_prefix, signed_prefix,
                          total_col=None, stage_fmt=None, suptitle=""):
    total_col = total_col or f"{squared_prefix}_total_merge"
    stage_fmt = stage_fmt or f"{squared_prefix}_delta_{{s}}"
    signed_total_col = f"{signed_prefix}_total_merge"
    signed_cols = [c for c in df_signed.columns if c.startswith(signed_prefix)]
    merged = df_squared.merge(df_signed[["repo_name"] + signed_cols].drop_duplicates("repo_name"),
                              on="repo_name", how="inner")
    has_totals = total_col in merged.columns and signed_total_col in merged.columns

    def StageSeries(stage):
        stage_col = stage_fmt.format(s=stage)
        if not has_totals or stage_col not in merged.columns:
            return None
        return (merged[stage_col] / merged[total_col].replace(0, np.nan) * merged[signed_total_col]).dropna()

    cell_grid = [[(STAGE_LABELS[stage], StageSeries(stage)) for stage in STAGES]]
    RenderPanelGrid(outpath, cell_grid, SIGNED_RESIDUAL_CLIP, "norm",
                    suptitle=suptitle, xlabel=r"$(\Delta Q / Q^m) \times Z^m$", suptitle_fontsize=11)


if __name__ == "__main__":
    Main()
