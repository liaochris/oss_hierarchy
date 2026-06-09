import hashlib
import json

import numpy as np
import pandas as pd
from pathlib import Path
from itertools import product

from joblib import Parallel, delayed

from source.lib.python.config_loaders import (
    LoadGlobalSettings, LoadPipelineInputs, LoadAnalysisParameters, LoadModelPredictionConfig
)
from source.lib.python.repo_utils import MakeRepoNameSafe
from source.lib.JMSLab.SaveData import SaveData
from source.lib.model.staged_count_model import (
    FitLatentDistribution, FitMemberProbabilities, ComputeStageProbabilities, DrawCounts
)

GLOBAL_SETTINGS         = LoadGlobalSettings()
PARAMETERS              = LoadAnalysisParameters()
MODEL_PREDICTION_CONFIG = LoadModelPredictionConfig()
CONFIG                  = LoadPipelineInputs()

INDIR_MEMBER_PANEL   = Path("drive/output/derived/model_prediction/event_time_member_panel")
INDIR_ANALYSIS_PANEL = Path("output/derived/analysis_panel")
INDIR_FITTED         = Path("output/analysis/model_prediction")
OUTDIR               = Path("output/analysis/model_prediction")
ROLLING_LABEL         = f"rolling{CONFIG['rolling_periods']['run'][0]}"
VARIANTS              = MODEL_PREDICTION_CONFIG["variants"]["run"]
DISTRIBUTION_TYPES    = MODEL_PREDICTION_CONFIG["distribution_types"]["run"]
ESTIMATION_APPROACHES = MODEL_PREDICTION_CONFIG["member_probability_estimation"]["run"]

N_MODEL_DRAWS = PARAMETERS["n_model_draws"]
N_JOBS        = GLOBAL_SETTINGS["n_jobs"]

OUTCOMES = ["open", "review", "direct_merge", "reviewed_merge", "total_merge"]
STAGES   = ["open", "review", "direct_merge", "reviewed_merge"]


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
    residuals_outdir = (
        OUTDIR / variant / distribution_type / "residuals" / estimation_approach
        / importance_type / qualified_sample / control_group
    )
    draws_outdir = (
        OUTDIR / variant / distribution_type / "draws" / estimation_approach
        / importance_type / qualified_sample / control_group
    )
    residuals_outdir.mkdir(parents=True, exist_ok=True)
    draws_outdir.mkdir(parents=True, exist_ok=True)

    fitted_dir = (
        INDIR_FITTED / variant / distribution_type / "parameters" / estimation_approach
        / importance_type / qualified_sample / control_group
    )
    df_dist  = pd.read_parquet(fitted_dir / "distribution_params.parquet")
    df_probs = pd.read_parquet(fitted_dir / "member_probabilities.parquet")

    panel_path = (
        INDIR_ANALYSIS_PANEL / importance_type / ROLLING_LABEL
        / qualified_sample / control_group / "panel.parquet"
    )
    df_panel = pd.read_parquet(panel_path)
    df_all_repos = df_panel[df_panel["quasi_event_time"] == 0][
        ["repo_name", "dropouts_actors", "num_dropouts"]
    ]

    results = Parallel(n_jobs=N_JOBS)(
        delayed(ProcessRepo)(
            row["repo_name"],
            row["num_dropouts"] > 0,
            set(json.loads(row["dropouts_actors"])) if row["num_dropouts"] > 0 else set(),
            df_dist[df_dist["repo_name"] == row["repo_name"]],
            df_probs[df_probs["repo_name"] == row["repo_name"]],
            variant, importance_type, qualified_sample, control_group,
            distribution_type, estimation_approach,
        )
        for _, row in df_all_repos.iterrows()
    )

    period_rows      = {"insample_period": [], "leaveoneout_period": [], "post_period": []}
    reference_frames = {"insample_reference": [], "leaveoneout_reference": [], "post_reference": []}
    raw_draw_frames  = []

    for result in results:
        if result is None:
            continue
        for key in period_rows:
            period_rows[key].extend(result[key])
        for key in reference_frames:
            reference_frames[key].append(result[key])
        raw_draw_frames.append(result["raw_draws"])

    for key, rows in period_rows.items():
        SaveData(pd.DataFrame(rows), ["repo_name", "quasi_event_time"],
                 residuals_outdir / f"{key}.parquet", residuals_outdir / f"{key}.log")
    for key, frames in reference_frames.items():
        SaveData(pd.concat(frames, ignore_index=True), ["repo_name", "quasi_event_time", "draw"],
                 residuals_outdir / f"{key}.parquet", residuals_outdir / f"{key}.log")

    SaveData(pd.concat(raw_draw_frames, ignore_index=True),
             ["repo_name", "quasi_event_time", "draw_id"],
             draws_outdir / "raw_draws.parquet", draws_outdir / "raw_draws.log")


def ProcessRepo(repo_name, is_treated, dropout_set,
                df_dist_repo, df_member_probs,
                variant, importance_type, qualified_sample, control_group,
                distribution_type, estimation_approach):
    if df_dist_repo.empty:
        return None

    member_path = (
        INDIR_MEMBER_PANEL / variant / importance_type / qualified_sample
        / control_group / f"{MakeRepoNameSafe(repo_name)}.parquet"
    )
    if not member_path.exists():
        return None

    df_member = pd.read_parquet(member_path)
    df_repo_counts = (
        df_member.groupby("quasi_event_time")[
            ["repo_pull_request_opened", "repo_pull_request_reviewed",
             "repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"]
        ].first().reset_index()
    )
    all_periods  = sorted(int(k) for k in df_repo_counts["quasi_event_time"])
    pre_periods  = [k for k in all_periods if k < 0]
    if len(pre_periods) == 0:
        return None
    full_set_periods = [k for k in all_periods if k <= 0]   # member still present (incl. k=0)
    post_periods     = [k for k in all_periods if k >= 1]
    observed_by_period = {int(row["quasi_event_time"]): ExtractObserved(row) for _, row in df_repo_counts.iterrows()}

    dist_row = df_dist_repo.iloc[0]
    repo_distribution = dist_row["distribution_type"]
    dist_params = {
        "poisson_rate":           dist_row["poisson_rate"],
        "negative_binomial_size": dist_row["negative_binomial_size"],
        "negative_binomial_prob": dist_row["negative_binomial_prob"],
    }

    full_stage_probs = ComputeStageProbabilities(df_member_probs)
    if is_treated:
        post_stage_probs = ComputeStageProbabilities(df_member_probs[~df_member_probs["actor_id"].isin(dropout_set)])
    else:
        post_stage_probs = full_stage_probs

    rng = np.random.default_rng(int(hashlib.md5(repo_name.encode()).hexdigest()[:8], 16))

    # Two batched draws per repo: full-set block (k<=0) and post block (k>=1), reshaped to per-period blocks.
    period_draws = {
        **DrawPeriodBlock(repo_distribution, dist_params, full_stage_probs, full_set_periods, N_MODEL_DRAWS, rng),
        **DrawPeriodBlock(repo_distribution, dist_params, post_stage_probs, post_periods, N_MODEL_DRAWS, rng),
    }
    period_stage_probs = {k: full_stage_probs for k in full_set_periods}
    period_stage_probs.update({k: post_stage_probs for k in post_periods})

    raw_draws = BuildRawDrawRows(repo_name, is_treated, all_periods, period_draws)

    insample_period_rows, insample_ref_blocks = ScorePeriods(repo_name, is_treated, pre_periods, observed_by_period, period_draws, period_stage_probs, rng)
    post_period_rows,     post_ref_blocks     = ScorePeriods(repo_name, is_treated, post_periods, observed_by_period, period_draws, period_stage_probs, rng)

    leaveoneout_period_rows, leaveoneout_ref_blocks = LeaveOneOutResiduals(
        repo_name, is_treated, pre_periods, df_member, observed_by_period,
        distribution_type, estimation_approach, rng)

    return {
        "raw_draws":             raw_draws,
        "insample_period":       insample_period_rows,
        "leaveoneout_period":    leaveoneout_period_rows,
        "post_period":           post_period_rows,
        "insample_reference":    ReferenceFrame(repo_name, is_treated, insample_ref_blocks),
        "leaveoneout_reference": ReferenceFrame(repo_name, is_treated, leaveoneout_ref_blocks),
        "post_reference":        ReferenceFrame(repo_name, is_treated, post_ref_blocks),
    }


def DrawPeriodBlock(repo_distribution, dist_params, stage_probs, periods, n_draws, rng):
    if not periods:
        return {}
    total = len(periods) * n_draws
    outcome_arrays = DrawCounts(repo_distribution, dist_params, *stage_probs, total, rng)
    reshaped = [array.reshape(len(periods), n_draws) for array in outcome_arrays]
    return {period: tuple(array[i] for array in reshaped) for i, period in enumerate(periods)}


def BuildRawDrawRows(repo_name, is_treated, periods, period_draws):
    frames = []
    for period in periods:
        opened, reviewed, direct_merge, reviewed_merge, total_merge = period_draws[period]
        frames.append(pd.DataFrame({
            "repo_name":                        repo_name,
            "is_treated":                       is_treated,
            "quasi_event_time":                 period,
            "draw_id":                          np.arange(len(opened)),
            "pull_request_opened":              opened,
            "pull_request_reviewed":            reviewed,
            "pull_request_merged_direct":       direct_merge,
            "pull_request_merged_after_review": reviewed_merge,
            "pull_request_merged":              total_merge,
        }))
    return pd.concat(frames, ignore_index=True)


def ScorePeriods(repo_name, is_treated, periods, observed_by_period, period_draws, period_stage_probs, rng):
    period_rows = []
    reference_blocks = []
    for period in periods:
        observed = observed_by_period[period]
        draws    = period_draws[period]
        _, prob_review, prob_merge_direct, prob_merge_after_review = period_stage_probs[period]
        squared_row = AllSquaredResiduals(observed, *draws, prob_review, prob_merge_direct, prob_merge_after_review, rng)
        signed_row  = AllSignedResiduals(observed, *draws)
        period_rows.append(PeriodRow(repo_name, is_treated, period, signed_row, squared_row))
        reference_blocks.append((period, {outcome: StandardizeDraws(draw) for outcome, draw in zip(OUTCOMES, draws)}))
    return period_rows, reference_blocks


def LeaveOneOutResiduals(repo_name, is_treated, pre_periods, df_member, observed_by_period,
                         distribution_type, estimation_approach, rng):
    period_rows = []
    reference_blocks = []
    if len(pre_periods) < 2:
        return period_rows, reference_blocks

    df_pre = df_member[df_member["quasi_event_time"].isin(pre_periods)]
    for held_out_time in pre_periods:
        df_train = df_pre[df_pre["quasi_event_time"] != held_out_time]
        df_repo_train = (
            df_train.groupby("quasi_event_time")[
                ["repo_pull_request_opened", "repo_pull_request_reviewed",
                 "repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"]
            ].first().reset_index()
        )
        counts_train = df_repo_train["repo_pull_request_opened"].values.astype(float)
        dist_loo  = FitLatentDistribution(repo_name, counts_train, distribution_type)
        probs_loo = ComputeStageProbabilities(pd.DataFrame(FitMemberProbabilities(repo_name, df_train, df_repo_train, estimation_approach)))
        dist_params_loo = {
            "poisson_rate":           dist_loo["poisson_rate"],
            "negative_binomial_size": dist_loo["negative_binomial_size"],
            "negative_binomial_prob": dist_loo["negative_binomial_prob"],
        }
        draws = DrawCounts(dist_loo["distribution_type"], dist_params_loo, *probs_loo, N_MODEL_DRAWS, rng)
        observed = observed_by_period[held_out_time]
        squared_row = AllSquaredResiduals(observed, *draws, probs_loo[1], probs_loo[2], probs_loo[3], rng)
        signed_row  = AllSignedResiduals(observed, *draws)
        period_rows.append(PeriodRow(repo_name, is_treated, held_out_time, signed_row, squared_row))
        reference_blocks.append((held_out_time, {outcome: StandardizeDraws(draw) for outcome, draw in zip(OUTCOMES, draws)}))
    return period_rows, reference_blocks


# ---------------------------------------------------------------------------
# Stage probabilities and observed-count extraction
# ---------------------------------------------------------------------------

def ExtractObserved(row):
    return {
        "pull_request_opened_observed":          float(row["repo_pull_request_opened"]),
        "pull_request_reviewed_observed":        float(row["repo_pull_request_reviewed"]),
        "pull_request_merged_directly_observed": float(row["repo_pull_request_merged_direct"]),
        "pull_request_merged_reviewed_observed": float(row["repo_pull_request_merged_after_review"]),
        "pull_request_merged_total_observed":    float(row["repo_pull_request_merged_direct"] + row["repo_pull_request_merged_after_review"]),
    }


# ---------------------------------------------------------------------------
# Standardized-residual metrics
# ---------------------------------------------------------------------------

def ComputeSquaredStdResidual(observed, draws):
    """((observed - mean) / std)^2 - 1."""
    mean = float(np.mean(draws))
    std  = float(np.std(draws))
    if std == 0.0 or np.isnan(std):
        return np.nan
    return ((observed - mean) / std) ** 2 - 1.0


def ComputeSignedStdResidual(observed, draws):
    """(observed - mean) / std  (signed standardized residual)."""
    mean = float(np.mean(draws))
    std  = float(np.std(draws))
    if std == 0.0 or np.isnan(std):
        return np.nan
    return (observed - mean) / std


def StageDecomposition(observed_opened, observed_reviewed, observed_merged_directly, observed_merged_total,
                       direct_merge_draw, reviewed_merge_draw,
                       prob_review, prob_merge_direct, prob_merge_after_review, n_draws, rng):
    """Delta decomposition of the squared standardized residual of total merges across stages."""
    total_merge_draw = direct_merge_draw + reviewed_merge_draw

    # Step 1: full model squared residual
    squared_residual_total = ComputeSquaredStdResidual(observed_merged_total, total_merge_draw)

    # Step 2: condition on observed opens
    prob_review             = np.clip(prob_review, 0.0, 1.0)
    prob_merge_direct       = np.clip(prob_merge_direct, 0.0, 1.0)
    prob_merge_after_review = np.clip(prob_merge_after_review, 0.0, 1.0)
    n_opened = int(observed_opened)
    conditional_reviewed_draw     = rng.binomial(n_opened, prob_review, n_draws)
    remaining                     = n_opened - conditional_reviewed_draw
    conditional_direct_merge_prob = np.clip(prob_merge_direct / (1.0 - prob_review), 0.0, 1.0) if prob_review < 1.0 else 0.0
    conditional_direct_merge_draw = rng.binomial(remaining, conditional_direct_merge_prob, n_draws)
    conditional_reviewed_merge_draw = rng.binomial(conditional_reviewed_draw, prob_merge_after_review, n_draws)
    conditional_total_merge_draw  = conditional_direct_merge_draw + conditional_reviewed_merge_draw
    squared_residual_fix_opened   = ComputeSquaredStdResidual(observed_merged_total, conditional_total_merge_draw)

    # Step 3: condition on observed opens and reviews
    n_reviewed = int(observed_reviewed)
    check_direct_merge_draw       = rng.binomial(int(n_opened - n_reviewed), conditional_direct_merge_prob, n_draws)
    check_reviewed_merge_draw     = rng.binomial(n_reviewed, prob_merge_after_review, n_draws)
    check_total_merge_draw        = check_direct_merge_draw + check_reviewed_merge_draw
    squared_residual_fix_opened_reviewed = ComputeSquaredStdResidual(observed_merged_total, check_total_merge_draw)

    # Step 4: condition on observed opens, reviews, and direct merges
    fixed_reviewed_merge_draw     = rng.binomial(n_reviewed, prob_merge_after_review, n_draws)
    fixed_total_merge_draw        = int(observed_merged_directly) + fixed_reviewed_merge_draw
    squared_residual_fix_opened_reviewed_direct = ComputeSquaredStdResidual(observed_merged_total, fixed_total_merge_draw)

    # If any step is NaN, propagate NaN so the averages use a consistent period set
    if any(np.isnan(v) for v in [squared_residual_total, squared_residual_fix_opened,
                                 squared_residual_fix_opened_reviewed, squared_residual_fix_opened_reviewed_direct]):
        return np.nan, np.nan, np.nan, np.nan, np.nan

    delta_open           = squared_residual_total - squared_residual_fix_opened
    delta_review         = squared_residual_fix_opened - squared_residual_fix_opened_reviewed
    delta_direct_merge   = squared_residual_fix_opened_reviewed - squared_residual_fix_opened_reviewed_direct
    delta_reviewed_merge = squared_residual_fix_opened_reviewed_direct

    return squared_residual_total, delta_open, delta_review, delta_direct_merge, delta_reviewed_merge


# ---------------------------------------------------------------------------
# Per-period residual rows and per-repo aggregation
# ---------------------------------------------------------------------------

def AllSquaredResiduals(observed, opened_draw, reviewed_draw, direct_merge_draw, reviewed_merge_draw, total_merge_draw,
                        prob_review, prob_merge_direct, prob_merge_after_review, rng):
    (total, delta_open, delta_review, delta_direct_merge, delta_reviewed_merge) = StageDecomposition(
        observed["pull_request_opened_observed"], observed["pull_request_reviewed_observed"],
        observed["pull_request_merged_directly_observed"], observed["pull_request_merged_total_observed"],
        direct_merge_draw, reviewed_merge_draw,
        prob_review, prob_merge_direct, prob_merge_after_review, N_MODEL_DRAWS, rng
    )
    return {
        "squared_std_residual_open":           ComputeSquaredStdResidual(observed["pull_request_opened_observed"], opened_draw),
        "squared_std_residual_review":         ComputeSquaredStdResidual(observed["pull_request_reviewed_observed"], reviewed_draw),
        "squared_std_residual_direct_merge":   ComputeSquaredStdResidual(observed["pull_request_merged_directly_observed"], direct_merge_draw),
        "squared_std_residual_reviewed_merge": ComputeSquaredStdResidual(observed["pull_request_merged_reviewed_observed"], reviewed_merge_draw),
        "squared_std_residual_total_merge":    total,
        "delta_squared_std_residual_open":           delta_open,
        "delta_squared_std_residual_review":         delta_review,
        "delta_squared_std_residual_direct_merge":   delta_direct_merge,
        "delta_squared_std_residual_reviewed_merge": delta_reviewed_merge,
    }


def AllSignedResiduals(observed, opened_draw, reviewed_draw, direct_merge_draw, reviewed_merge_draw, total_merge_draw):
    return {
        "signed_std_residual_open":           ComputeSignedStdResidual(observed["pull_request_opened_observed"], opened_draw),
        "signed_std_residual_review":         ComputeSignedStdResidual(observed["pull_request_reviewed_observed"], reviewed_draw),
        "signed_std_residual_direct_merge":   ComputeSignedStdResidual(observed["pull_request_merged_directly_observed"], direct_merge_draw),
        "signed_std_residual_reviewed_merge": ComputeSignedStdResidual(observed["pull_request_merged_reviewed_observed"], reviewed_merge_draw),
        "signed_std_residual_total_merge":    ComputeSignedStdResidual(observed["pull_request_merged_total_observed"], total_merge_draw),
    }


# ---------------------------------------------------------------------------
# Simulated-null reference (draws from the fitted model, standardized as the
# real residuals are, so a cross-org KS test compares against the model's own
# finite-sample null rather than an analytic N(0,1)/chi^2 reference)
# ---------------------------------------------------------------------------

def StandardizeDraws(draws):
    std = float(np.std(draws))
    if std == 0.0 or np.isnan(std):
        return None
    return (np.asarray(draws, dtype=float) - float(np.mean(draws))) / std


def ReferenceFrame(repo_name, is_treated, blocks):
    # blocks: list of (quasi_event_time, {outcome -> standardized draws or None}); one block per period.
    frames = []
    for quasi_event_time, standardized in blocks:
        length = max((len(v) for v in standardized.values() if v is not None), default=0)
        if length == 0:
            continue
        block = pd.DataFrame({
            f"signed_std_residual_{outcome}": (np.full(length, np.nan) if draws is None else draws)
            for outcome, draws in standardized.items()
        })
        block.insert(0, "quasi_event_time", int(quasi_event_time))
        block.insert(1, "draw", np.arange(length))
        frames.append(block)
    frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        {"quasi_event_time": [], "draw": [], **{f"signed_std_residual_{outcome}": [] for outcome in OUTCOMES}})
    frame.insert(0, "repo_name", repo_name)
    frame.insert(1, "is_treated", is_treated)
    return frame


def PeriodRow(repo_name, is_treated, quasi_event_time, signed_row, squared_row):
    return {
        "repo_name": repo_name, "is_treated": is_treated, "quasi_event_time": int(quasi_event_time),
        **{k: v for k, v in signed_row.items() if k.startswith("signed_std_residual_")},
        **{k: v for k, v in squared_row.items()
           if k.startswith("squared_std_residual_") or k.startswith("delta_squared_std_residual_")},
    }


if __name__ == "__main__":
    Main()
