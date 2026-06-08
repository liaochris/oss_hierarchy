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
from source.analysis.model_prediction.model_fitting_utils import (
    FitLatentDistribution, FitMemberProbabilities
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
POST_TIMES            = list(range(1, PARAMETERS["max_event_time"] + 1))

N_MODEL_DRAWS = 1000
N_JOBS        = GLOBAL_SETTINGS["n_jobs"]
PROB_SUM_TOLERANCE = 1e-6

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
    outdir = (
        OUTDIR / variant / distribution_type / "residuals" / estimation_approach
        / importance_type / qualified_sample / control_group
    )
    outdir.mkdir(parents=True, exist_ok=True)

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

    insample_rows, leaveoneout_rows, post_rows = [], [], []
    insample_signed_rows, leaveoneout_signed_rows, post_signed_rows = [], [], []

    for result in results:
        if result is None:
            continue
        (insample_row, leaveoneout_row, repo_post_rows,
         insample_signed_row, leaveoneout_signed_rows_repo, post_signed_rows_repo) = result
        insample_rows.append(insample_row)
        leaveoneout_rows.append(leaveoneout_row)
        post_rows.extend(repo_post_rows)
        insample_signed_rows.append(insample_signed_row)
        leaveoneout_signed_rows.extend(leaveoneout_signed_rows_repo)
        post_signed_rows.extend(post_signed_rows_repo)

    SaveData(pd.DataFrame(insample_rows), ["repo_name"],
             outdir / "insample_evaluation.parquet", outdir / "insample_evaluation.log")
    SaveData(pd.DataFrame(leaveoneout_rows), ["repo_name"],
             outdir / "leaveoneout_evaluation.parquet", outdir / "leaveoneout_evaluation.log")
    SaveData(pd.DataFrame(post_rows), ["repo_name", "quasi_event_time"],
             outdir / "post_evaluation.parquet", outdir / "post_evaluation.log")
    SaveData(pd.DataFrame(insample_signed_rows), ["repo_name"],
             outdir / "insample_period_z.parquet", outdir / "insample_period_z.log")
    SaveData(pd.DataFrame(leaveoneout_signed_rows), ["repo_name", "quasi_event_time"],
             outdir / "leaveoneout_period_z.parquet", outdir / "leaveoneout_period_z.log")
    SaveData(pd.DataFrame(post_signed_rows), ["repo_name", "quasi_event_time"],
             outdir / "post_period_z.parquet", outdir / "post_period_z.log")


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
    df_pre    = df_member[df_member["quasi_event_time"] < 0].copy()
    pre_times = sorted(df_pre["quasi_event_time"].unique())
    if len(pre_times) == 0:
        return None

    dist_row = df_dist_repo.iloc[0]
    repo_distribution = dist_row["distribution_type"]
    dist_params = {
        "poisson_rate":           dist_row["poisson_rate"],
        "negative_binomial_size": dist_row["negative_binomial_size"],
        "negative_binomial_prob": dist_row["negative_binomial_prob"],
    }

    # Stage-completion probabilities from full member set
    prob_open, prob_review, prob_merge_direct, prob_merge_after_review = ComputeStageProbabilities(df_member_probs)

    rng = np.random.default_rng(int(hashlib.md5(repo_name.encode()).hexdigest()[:8], 16))

    # --- InSample draws (reused for control post-period) ---
    (opened_insample, reviewed_insample, direct_merge_insample,
     reviewed_merge_insample, total_merge_insample) = DrawCounts(
        repo_distribution, dist_params,
        prob_open, prob_review, prob_merge_direct, prob_merge_after_review, N_MODEL_DRAWS, rng
    )

    # --- InSample evaluation ---
    df_repo_pre = (
        df_pre.groupby("quasi_event_time")[
            ["repo_pull_request_opened", "repo_pull_request_reviewed",
             "repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"]
        ].first().reset_index()
    )
    insample_squared_per_period = []
    insample_signed_list        = []
    for _, repo_row in df_repo_pre.iterrows():
        observed = ExtractObserved(repo_row)
        squared_row = AllSquaredResiduals(observed, opened_insample, reviewed_insample,
                                          direct_merge_insample, reviewed_merge_insample, total_merge_insample,
                                          prob_review, prob_merge_direct, prob_merge_after_review, rng)
        signed_row  = AllSignedResiduals(observed, opened_insample, reviewed_insample,
                                         direct_merge_insample, reviewed_merge_insample, total_merge_insample)
        insample_squared_per_period.append(squared_row)
        insample_signed_list.append({"quasi_event_time": int(repo_row["quasi_event_time"]), **signed_row})

    insample_row        = MeanSquaredResidualRow(insample_squared_per_period, repo_name, is_treated, "insample")
    insample_signed_row = MeanSignedResidualRow(insample_signed_list, repo_name, is_treated)

    # --- LeaveOneOut evaluation ---
    leaveoneout_squared_per_period = []
    leaveoneout_signed_rows_repo   = []
    if len(pre_times) >= 2:
        for held_out_time in pre_times:
            train_times = [t for t in pre_times if t != held_out_time]
            df_train = df_pre[df_pre["quasi_event_time"].isin(train_times)]
            df_repo_train = (
                df_train.groupby("quasi_event_time")[
                    ["repo_pull_request_opened", "repo_pull_request_reviewed",
                     "repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"]
                ].first().reset_index()
            )
            counts_train = df_repo_train["repo_pull_request_opened"].values.astype(float)
            dist_loo  = FitLatentDistribution(repo_name, counts_train, distribution_type)
            probs_loo = FitMemberProbabilities(repo_name, df_train, df_repo_train, estimation_approach)
            df_probs_loo = pd.DataFrame(probs_loo)
            (prob_open_loo, prob_review_loo,
             prob_merge_direct_loo, prob_merge_after_review_loo) = ComputeStageProbabilities(df_probs_loo)
            dist_params_loo = {
                "poisson_rate":           dist_loo["poisson_rate"],
                "negative_binomial_size": dist_loo["negative_binomial_size"],
                "negative_binomial_prob": dist_loo["negative_binomial_prob"],
            }

            (opened_loo, reviewed_loo, direct_merge_loo,
             reviewed_merge_loo, total_merge_loo) = DrawCounts(
                dist_loo["distribution_type"], dist_params_loo,
                prob_open_loo, prob_review_loo, prob_merge_direct_loo, prob_merge_after_review_loo, N_MODEL_DRAWS, rng
            )
            held_row = df_repo_pre[df_repo_pre["quasi_event_time"] == held_out_time].iloc[0]
            observed = ExtractObserved(held_row)
            squared_row = AllSquaredResiduals(observed, opened_loo, reviewed_loo,
                                              direct_merge_loo, reviewed_merge_loo, total_merge_loo,
                                              prob_review_loo, prob_merge_direct_loo, prob_merge_after_review_loo, rng)
            signed_row = AllSignedResiduals(observed, opened_loo, reviewed_loo,
                                            direct_merge_loo, reviewed_merge_loo, total_merge_loo)
            leaveoneout_squared_per_period.append(squared_row)
            leaveoneout_signed_rows_repo.append({
                "repo_name": repo_name, "quasi_event_time": int(held_out_time),
                "is_treated": is_treated, **signed_row
            })

    leaveoneout_row = MeanSquaredResidualRow(leaveoneout_squared_per_period, repo_name, is_treated, "leaveoneout")

    # --- Post-period draws and evaluation ---
    df_remaining = df_member_probs[~df_member_probs["actor_id"].isin(dropout_set)]
    if is_treated:
        (prob_open_cf, prob_review_cf,
         prob_merge_direct_cf, prob_merge_after_review_cf) = ComputeStageProbabilities(df_remaining)
        (opened_post, reviewed_post, direct_merge_post,
         reviewed_merge_post, total_merge_post) = DrawCounts(
            repo_distribution, dist_params,
            prob_open_cf, prob_review_cf, prob_merge_direct_cf, prob_merge_after_review_cf, N_MODEL_DRAWS, rng
        )
    else:
        # Reuse InSample draws for control (same parameters)
        opened_post, reviewed_post, direct_merge_post, reviewed_merge_post, total_merge_post = (
            opened_insample, reviewed_insample, direct_merge_insample, reviewed_merge_insample, total_merge_insample
        )
        prob_review_cf, prob_merge_direct_cf, prob_merge_after_review_cf = (
            prob_review, prob_merge_direct, prob_merge_after_review
        )

    post_rows = []
    post_signed_rows_repo = []
    for t in POST_TIMES:
        df_post = df_member[df_member["quasi_event_time"] == t]
        if df_post.empty:
            continue
        obs_row = df_post.groupby("quasi_event_time")[
            ["repo_pull_request_opened", "repo_pull_request_reviewed",
             "repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"]
        ].first().iloc[0]
        observed = ExtractObserved(obs_row)
        squared_row = AllSquaredResiduals(observed, opened_post, reviewed_post,
                                          direct_merge_post, reviewed_merge_post, total_merge_post,
                                          prob_review_cf, prob_merge_direct_cf, prob_merge_after_review_cf, rng)
        signed_row = AllSignedResiduals(observed, opened_post, reviewed_post,
                                        direct_merge_post, reviewed_merge_post, total_merge_post)
        post_rows.append({"repo_name": repo_name, "quasi_event_time": t, "is_treated": is_treated, **squared_row})
        post_signed_rows_repo.append({"repo_name": repo_name, "quasi_event_time": t, "is_treated": is_treated, **signed_row})

    return (
        insample_row, leaveoneout_row, post_rows,
        insample_signed_row, leaveoneout_signed_rows_repo, post_signed_rows_repo
    )


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


def ComputeStageProbabilities(df_member_probs):
    prob_open = float(df_member_probs["prob_open"].sum())
    assert -PROB_SUM_TOLERANCE <= prob_open <= 1.0 + PROB_SUM_TOLERANCE, f"prob_open sum out of [0, 1]: {prob_open}"
    prob_review             = 1.0 - float(np.prod(1.0 - df_member_probs["prob_review"].values))
    prob_merge_direct       = float(df_member_probs["prob_merge_direct"].sum())
    prob_merge_after_review = float(df_member_probs["prob_merge_after_review"].sum())
    return prob_open, prob_review, prob_merge_direct, prob_merge_after_review


# ---------------------------------------------------------------------------
# Simulation draws (N_MODEL_DRAWS per repo)
# ---------------------------------------------------------------------------

def DrawCounts(distribution_type, dist_params,
               prob_open, prob_review, prob_merge_direct, prob_merge_after_review, n_draws, rng):
    """Draw n_draws outcome vectors from the model DGP via sequential multinomial decomposition."""
    # Latent problem count
    if distribution_type == "poisson":
        latent_problem_count_draw = rng.poisson(dist_params["poisson_rate"], n_draws)
    elif distribution_type == "negative_binomial":
        latent_problem_count_draw = rng.negative_binomial(
            dist_params["negative_binomial_size"], dist_params["negative_binomial_prob"], n_draws
        )
    else:
        raise ValueError(f"Unknown distribution_type for DrawCounts: {distribution_type}")

    # Stage 1: opened
    prob_open = np.clip(prob_open, 0.0, 1.0)
    pull_request_opened_draw = rng.binomial(latent_problem_count_draw, prob_open)

    # Stage 2: review vs direct-merge vs exit (sequential multinomial decomposition)
    prob_review = np.clip(prob_review, 0.0, 1.0)
    pull_request_reviewed_draw = rng.binomial(pull_request_opened_draw, prob_review)
    remaining = pull_request_opened_draw - pull_request_reviewed_draw
    if prob_review < 1.0:
        conditional_direct_merge_prob = np.clip(prob_merge_direct / (1.0 - prob_review), 0.0, 1.0)
    else:
        conditional_direct_merge_prob = 0.0
    pull_request_merged_directly_draw = rng.binomial(remaining, conditional_direct_merge_prob)

    # Stage 3: merge after review
    prob_merge_after_review = np.clip(prob_merge_after_review, 0.0, 1.0)
    pull_request_merged_reviewed_draw = rng.binomial(pull_request_reviewed_draw, prob_merge_after_review)

    pull_request_merged_total_draw = pull_request_merged_directly_draw + pull_request_merged_reviewed_draw
    return (pull_request_opened_draw, pull_request_reviewed_draw,
            pull_request_merged_directly_draw, pull_request_merged_reviewed_draw,
            pull_request_merged_total_draw)


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


def MeanSquaredResidualRow(rows, repo_name, is_treated, prefix):
    columns = ["open", "review", "direct_merge", "reviewed_merge", "total_merge",
               "delta_open", "delta_review", "delta_direct_merge", "delta_reviewed_merge"]
    if not rows:
        row = {f"mean_squared_std_residual_{prefix}_{col}": np.nan for col in columns}
    else:
        def mean(key):
            vals = [r[key] for r in rows if not np.isnan(r[key])]
            return float(np.mean(vals)) if vals else np.nan

        row = {
            f"mean_squared_std_residual_{prefix}_open":           mean("squared_std_residual_open"),
            f"mean_squared_std_residual_{prefix}_review":         mean("squared_std_residual_review"),
            f"mean_squared_std_residual_{prefix}_direct_merge":   mean("squared_std_residual_direct_merge"),
            f"mean_squared_std_residual_{prefix}_reviewed_merge": mean("squared_std_residual_reviewed_merge"),
            f"mean_squared_std_residual_{prefix}_total_merge":    mean("squared_std_residual_total_merge"),
            f"mean_squared_std_residual_{prefix}_delta_open":           mean("delta_squared_std_residual_open"),
            f"mean_squared_std_residual_{prefix}_delta_review":         mean("delta_squared_std_residual_review"),
            f"mean_squared_std_residual_{prefix}_delta_direct_merge":   mean("delta_squared_std_residual_direct_merge"),
            f"mean_squared_std_residual_{prefix}_delta_reviewed_merge": mean("delta_squared_std_residual_reviewed_merge"),
        }
    return {"repo_name": repo_name, "is_treated": is_treated, **row}


def MeanSignedResidualRow(rows, repo_name, is_treated):
    if not rows:
        row = {f"mean_signed_std_residual_{col}": np.nan for col in OUTCOMES}
    else:
        def mean(key):
            vals = [r[key] for r in rows if not np.isnan(r.get(key, np.nan))]
            return float(np.mean(vals)) if vals else np.nan
        row = {
            "mean_signed_std_residual_open":           mean("signed_std_residual_open"),
            "mean_signed_std_residual_review":         mean("signed_std_residual_review"),
            "mean_signed_std_residual_direct_merge":   mean("signed_std_residual_direct_merge"),
            "mean_signed_std_residual_reviewed_merge": mean("signed_std_residual_reviewed_merge"),
            "mean_signed_std_residual_total_merge":    mean("signed_std_residual_total_merge"),
        }
    return {"repo_name": repo_name, "is_treated": is_treated, **row}


if __name__ == "__main__":
    Main()
