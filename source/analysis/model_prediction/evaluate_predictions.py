import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from pathlib import Path
from itertools import product

from source.lib.python.config_loaders import LoadPipelineInputs, LoadAnalysisParameters, LoadModelPredictionConfig

_ap = LoadAnalysisParameters()
_model_prediction_config = LoadModelPredictionConfig()

INDIR_PREDICTIONS     = Path("output/analysis/model_prediction")
OUTDIR                = Path("output/analysis/model_prediction")
VARIANTS              = _model_prediction_config["variants"]["run"]
ESTIMATION_APPROACHES = _model_prediction_config["member_probability_estimation"]["run"]
QUALIFIED_SAMPLES     = {"exact1", "exact2", "exact_1_2"}
POST_TIMES        = list(range(1, _ap["max_event_time"] + 1))
EVAL_KEYS         = [*[str(t) for t in POST_TIMES], "1_5"]

OUTCOMES = ["open", "review_rate", "merge_rate"]
METRIC_LABELS = {
    "signed_std_residual":    "Signed std. residual (model-implied)",
    "squared_std_residual":   "Squared std. residual, centered (model-implied)",
    "percentage_point_error": "Prediction error: percentage points",
    "percent_error":          "Prediction error: % deviation",
}

ERROR_METRICS = [
    (f"{metric}_{outcome}", f"{METRIC_LABELS[metric]}: {outcome.replace('_', ' ')}")
    for metric in _model_prediction_config["error_metrics"]["run"]
    for outcome in OUTCOMES
]


def Main():
    cfg = LoadPipelineInputs()
    importance_types  = cfg["importance_types"]["run"]
    qualified_samples = [s for s in cfg["qualified_samples"]["run"] if s in QUALIFIED_SAMPLES]
    control_groups    = cfg["control_groups"]["run"]

    for variant, importance_type, qualified_sample, control_group, estimation_approach in product(
        VARIANTS, importance_types, qualified_samples, control_groups, ESTIMATION_APPROACHES
    ):
        RunCombination(variant, importance_type, qualified_sample, control_group, estimation_approach)
        RunPrePeriodEvaluation(variant, importance_type, qualified_sample, control_group, estimation_approach)


def RunCombination(variant, importance_type, qualified_sample, control_group, estimation_approach):
    pred_dir = INDIR_PREDICTIONS / variant / "predictions" / estimation_approach / importance_type / qualified_sample / control_group
    base     = OUTDIR / variant / "evaluation" / estimation_approach / importance_type / qualified_sample / control_group

    all_summary_rows = []

    for key in EVAL_KEYS:
        if key == "1_5":
            frames = []
            for t in POST_TIMES:
                df_t = pd.read_parquet(pred_dir / f"event_time_{t}" / "predictions.parquet")
                df_t["quasi_event_time"] = t
                frames.append(df_t)
            df_pred = pd.concat(frames, ignore_index=True)
        else:
            df_pred = pd.read_parquet(pred_dir / f"event_time_{key}" / "predictions.parquet")

        subgroups = {"treated": df_pred[df_pred["is_treated"]], "control": df_pred[~df_pred["is_treated"]]}

        for group_name, df_group in subgroups.items():
            outdir = base / f"event_time_{key}" / group_name
            outdir.mkdir(parents=True, exist_ok=True)

            summary_rows = []
            for col, label in ERROR_METRICS:
                PlotErrorDistribution(df_group[col], label, outdir / f"{col}.png")
                vals = df_group[col].dropna()
                row = {
                    "metric": col,
                    "n":      len(vals),
                    "n_na":   df_group[col].isna().sum(),
                    "mean":   vals.mean(),
                    "median": vals.median(),
                    "sd":     vals.std(),
                    "p10":    vals.quantile(0.10),
                    "p25":    vals.quantile(0.25),
                    "p75":    vals.quantile(0.75),
                    "p90":    vals.quantile(0.90),
                }
                summary_rows.append(row)
                all_summary_rows.append({"event_time": key, "group": group_name, **row})

            pd.DataFrame(summary_rows).to_csv(outdir / "prediction_error_summary.csv", index=False)

    pd.DataFrame(all_summary_rows).to_csv(base / "prediction_error_summary_all.csv", index=False)


PRE_PERIOD_METRICS = [
    (f"{metric}_{outcome}", f"{METRIC_LABELS[metric]}: {outcome.replace('_', ' ')}")
    for metric in ["signed_std_residual", "squared_std_residual"]
    for outcome in OUTCOMES
]


def RunPrePeriodEvaluation(variant, importance_type, qualified_sample, control_group, estimation_approach):
    pre_period_path = (
        OUTDIR / variant / "pre_period" / estimation_approach
        / importance_type / qualified_sample / control_group / "pre_period_evaluation.parquet"
    )
    df = pd.read_parquet(pre_period_path)

    subgroups = {"treated": df[df["is_treated"]], "control": df[~df["is_treated"]]}

    for group_name, df_group in subgroups.items():
        outdir = (
            OUTDIR / variant / "pre_period" / estimation_approach
            / importance_type / qualified_sample / control_group / group_name
        )
        outdir.mkdir(parents=True, exist_ok=True)

        for col, label in PRE_PERIOD_METRICS:
            PlotErrorDistribution(df_group[col], label, outdir / f"{col}.png")


CLIP_LIMIT = 500


def PlotErrorDistribution(series, xlabel, outpath):
    n_total    = len(series)
    vals       = series.dropna()
    na_pct     = 100.0 * (n_total - len(vals)) / n_total

    n_before   = len(vals)
    n_trim     = -(-n_before // 100)  # ceil(1% of n)
    vals       = vals.sort_values().iloc[:-n_trim] if n_trim < n_before else vals
    trim_pct   = 100.0 * n_trim / n_before if n_before > 0 else 0.0

    n_lo       = int((vals < -CLIP_LIMIT).sum())
    n_hi       = int((vals >  CLIP_LIMIT).sum())
    median_val = vals.median()
    mean_val   = vals.mean()
    p25        = vals.quantile(0.25)
    p75        = vals.quantile(0.75)
    vals       = vals.clip(-CLIP_LIMIT, CLIP_LIMIT)

    fig, ax = plt.subplots(figsize=(7, 4.5))

    ax.hist(vals, bins=25, color="#3A5F8A", edgecolor="white", linewidth=0.5, alpha=0.85,
            range=(-CLIP_LIMIT, CLIP_LIMIT))
    ax.axvspan(p25, p75, alpha=0.12, color="#888888",
               label=f"IQR  [{p25:.1f}, {p75:.1f}]")
    ax.axvline(median_val, color="#C0392B", linewidth=1.8, linestyle="-",  zorder=5,
               label=f"Median = {median_val:.1f}")
    ax.axvline(mean_val,   color="#27AE60", linewidth=1.8, linestyle="--", zorder=5,
               label=f"Mean = {mean_val:.1f}")
    if n_lo > 0:
        ax.plot([], [], " ", label=f"Bunched at −{CLIP_LIMIT}: n={n_lo}")
    if n_hi > 0:
        ax.plot([], [], " ", label=f"Bunched at +{CLIP_LIMIT}: n={n_hi}")
    if na_pct > 0:
        ax.plot([], [], " ", label=f"NA: {na_pct:.1f}% (insufficient activity)")
    ax.plot([], [], " ", label=f"Top 1% trimmed: n={n_trim} ({trim_pct:.1f}%)")

    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel("Organization count", fontsize=11)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.legend(frameon=False, fontsize=9, loc="upper right")
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=9)
    fig.tight_layout()
    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    Main()
