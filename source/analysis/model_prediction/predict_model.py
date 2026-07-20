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
    FitLatentDistribution, FitMemberProbabilities, ComputeStageProbabilities, DrawCounts,
    MemberProbabilityRows, MEMBER_COUNT_COLUMNS
)

GLOBAL_SETTINGS         = LoadGlobalSettings()
PARAMETERS              = LoadAnalysisParameters()
MODEL_PREDICTION_CONFIG = LoadModelPredictionConfig()
CONFIG                  = LoadPipelineInputs()

INDIR_MEMBER_PANEL   = Path("drive/output/derived/model_prediction/event_time_member_panel")
INDIR_MEMBER_CROSS   = Path("drive/output/derived/model_prediction/event_time_member_crossmerge")
INDIR_ANALYSIS_PANEL = Path("output/derived/analysis_panel")
INDIR_FITTED         = Path("output/analysis/model_prediction")
OUTDIR               = Path("output/analysis/model_prediction")
DATASTORE_OUTDIR     = Path("drive/output/analysis/model_prediction")
ROLLING_LABEL         = f"rolling{CONFIG['rolling_periods']['run'][0]}"
PRE_PERIOD_COUNT      = CONFIG['rolling_periods']['run'][0]
OUTCOME_SAMPLES              = MODEL_PREDICTION_CONFIG["outcome_samples"]["run"]
DISTRIBUTION_TYPES    = MODEL_PREDICTION_CONFIG["distribution_types"]["run"]
ESTIMATION_APPROACHES = MODEL_PREDICTION_CONFIG["member_probability_estimation"]["run"]

N_MODEL_DRAWS = PARAMETERS["n_model_draws"]
N_JOBS        = GLOBAL_SETTINGS["n_jobs"]

OUTCOMES = ["opened", "reviewed", "merged_direct", "merged_after_review", "merged"]
STAGES   = ["opened", "reviewed", "merged_direct", "merged_after_review"]


def Main():
    importance_types  = CONFIG["importance_types"]["run"]
    qualified_samples = CONFIG["qualified_samples"]["run"]
    control_groups    = CONFIG["control_groups"]["run"]

    for outcome_sample, distribution_type, estimation_approach, importance_type, qualified_sample, control_group in product(
        OUTCOME_SAMPLES, DISTRIBUTION_TYPES, ESTIMATION_APPROACHES,
        importance_types, qualified_samples, control_groups
    ):
        RunCombination(
            outcome_sample, distribution_type, estimation_approach,
            importance_type, qualified_sample, control_group
        )


def RunCombination(outcome_sample, distribution_type, estimation_approach,
                   importance_type, qualified_sample, control_group):
    def stage_dir(root, stage):
        return (root / outcome_sample / distribution_type / stage / estimation_approach
                / importance_type / qualified_sample / control_group)

    residuals_outdir      = stage_dir(OUTDIR, "residuals")
    reference_data_outdir = stage_dir(DATASTORE_OUTDIR, "residuals")
    draws_log_outdir      = stage_dir(OUTDIR, "draws")
    draws_data_outdir     = stage_dir(DATASTORE_OUTDIR, "draws")
    for directory in (residuals_outdir, reference_data_outdir, draws_log_outdir, draws_data_outdir):
        directory.mkdir(parents=True, exist_ok=True)

    fitted_dir = (
        INDIR_FITTED / outcome_sample / distribution_type / "parameters" / estimation_approach
        / importance_type / qualified_sample / control_group
    )
    df_dist  = pd.read_parquet(fitted_dir / "distribution_params.parquet")
    df_probs = pd.read_parquet(fitted_dir / "member_probabilities.parquet")

    panel_path = (
        INDIR_ANALYSIS_PANEL / outcome_sample / importance_type / ROLLING_LABEL
        / qualified_sample / control_group / "panel.parquet"
    )
    df_panel = pd.read_parquet(panel_path)
    df_all_repos = df_panel[df_panel["quasi_event_time"] == 0][
        ["repo_name", "dropouts_actors", "num_dropouts"]
    ]

    repo_results = Parallel(n_jobs=N_JOBS)(
        delayed(ProcessRepo)(
            row["repo_name"],
            row["num_dropouts"] > 0,
            set(json.loads(row["dropouts_actors"])) if row["num_dropouts"] > 0 else set(),
            df_dist[df_dist["repo_name"] == row["repo_name"]],
            df_probs[df_probs["repo_name"] == row["repo_name"]],
            outcome_sample, importance_type, qualified_sample, control_group,
            distribution_type, estimation_approach,
        )
        for _, row in df_all_repos.iterrows()
    )

    period_rows      = {"insample_period": [], "leaveoneout_period": [], "post_period": []}
    reference_frames = {"insample_reference": [], "leaveoneout_reference": [], "post_reference": []}
    raw_draw_frames  = []

    for repo_result in repo_results:
        if repo_result is None:
            continue
        for residual_kind in period_rows:
            period_rows[residual_kind].extend(repo_result[residual_kind])
        for residual_kind in reference_frames:
            reference_frames[residual_kind].append(repo_result[residual_kind])
        raw_draw_frames.append(repo_result["raw_draws"])

    def write_period(residual_kind, residual_rows):
        SaveData(pd.DataFrame(residual_rows), ["repo_name", "quasi_event_time"],
                 residuals_outdir / f"{residual_kind}.parquet", residuals_outdir / f"{residual_kind}.log")

    def write_reference(residual_kind, reference_block_frames):
        SaveData(pd.concat(reference_block_frames, ignore_index=True), ["repo_name", "quasi_event_time", "draw"],
                 reference_data_outdir / f"{residual_kind}.parquet", residuals_outdir / f"{residual_kind}.log")

    def write_draws(raw_draw_frames):
        SaveData(pd.concat(raw_draw_frames, ignore_index=True), ["repo_name", "quasi_event_time", "draw_id"],
                 draws_data_outdir / "raw_draws.parquet", draws_log_outdir / "raw_draws.log")

    write_jobs = (
        [delayed(write_period)(kind, rows) for kind, rows in period_rows.items()]
        + [delayed(write_reference)(kind, frames) for kind, frames in reference_frames.items()]
        + [delayed(write_draws)(raw_draw_frames)]
    )
    Parallel(n_jobs=N_JOBS, prefer="threads")(write_jobs)




def CounterfactualStageProbs(surviving_member_probs, df_member, df_crossmerge, dropout_set, pre_periods):
    # No-reaction: each surviving opener loses exactly what the departing members did to its pulls -- the pulls
    # they merged (direct and after-review other-merges) and the pulls they solely reviewed. Subtract each
    # departed count over j's own denominator (opened pulls, or reviewed-opened pulls) from the corresponding
    # rate. Self-merges and survivors' own rates are untouched; group-reviewed pulls survive (only solo drops).
    df_pre            = df_member[df_member["quasi_event_time"].isin(pre_periods)]
    opens_by_actor    = df_pre.groupby("actor_id")["member_pull_request_opened"].sum()
    reviewed_by_actor = df_pre.groupby("actor_id")["member_pull_request_reviewed_opened"].sum()

    if df_crossmerge is not None and not df_crossmerge.empty:
        departed = df_crossmerge[df_crossmerge["quasi_event_time"].isin(pre_periods)
                                 & df_crossmerge["other_id"].isin(dropout_set)]
        c_direct = departed.groupby("opener_id")["n_merged_direct_other"].sum()
        c_after  = departed.groupby("opener_id")["n_merged_after_review_other"].sum()
        c_review = departed.groupby("opener_id")["n_solo_reviewed"].sum()
    else:
        c_direct = c_after = c_review = pd.Series(dtype=float)

    def reduced_rate(rate, actor, departed_counts, denom_by_actor):
        denom = float(denom_by_actor.get(actor, 0.0))
        return max(0.0, rate - float(departed_counts.get(actor, 0.0)) / denom) if denom > 0 else rate

    adjusted = surviving_member_probs.copy()
    adjusted["prob_review_opened"] = [
        reduced_rate(rate, actor, c_review, opens_by_actor)
        for actor, rate in zip(adjusted["actor_id"], adjusted["prob_review_opened"])]
    adjusted["prob_merge_direct_opened_other"] = [
        reduced_rate(rate, actor, c_direct, opens_by_actor)
        for actor, rate in zip(adjusted["actor_id"], adjusted["prob_merge_direct_opened_other"])]
    adjusted["prob_merge_after_review_other"] = [
        reduced_rate(rate, actor, c_after, reviewed_by_actor)
        for actor, rate in zip(adjusted["actor_id"], adjusted["prob_merge_after_review_other"])]
    return adjusted


def ProcessRepo(repo_name, is_treated, dropout_set,
                df_dist_repo, df_member_probs,
                outcome_sample, importance_type, qualified_sample, control_group,
                distribution_type, estimation_approach):
    if df_dist_repo.empty:
        return None

    member_path = (
        INDIR_MEMBER_PANEL / outcome_sample / importance_type / qualified_sample
        / control_group / f"{MakeRepoNameSafe(repo_name)}.parquet"
    )
    if not member_path.exists():
        return None

    df_member = pd.read_parquet(member_path)
    crossmerge_path = (
        INDIR_MEMBER_CROSS / outcome_sample / importance_type / qualified_sample
        / control_group / f"{MakeRepoNameSafe(repo_name)}.parquet"
    )
    df_crossmerge   = pd.read_parquet(crossmerge_path) if crossmerge_path.exists() else None
    df_repo_counts = (
        df_member.groupby("quasi_event_time")[
            ["repo_pull_request_opened", "repo_pull_request_reviewed",
             "repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"]
        ].first().reset_index()
    )
    all_periods  = sorted(int(k) for k in df_repo_counts["quasi_event_time"])
    # Everything downstream is bounded to the +/- PRE_PERIOD_COUNT event-study window.
    pre_periods       = [k for k in all_periods if -PRE_PERIOD_COUNT <= k < 0]
    if len(pre_periods) == 0:
        return None
    treatment_periods = [k for k in all_periods if k == 0]   # member still present at event time 0
    post_periods      = [k for k in all_periods if 1 <= k <= PRE_PERIOD_COUNT]
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
        # No-reaction counterfactual: recompute the stage inputs over the surviving members only, so opens lose
        # j*'s share (unredistributed), and each surviving opener drops exactly what j* did to its pulls -- the
        # pulls j* merged and the pulls j* solely reviewed (group-reviewed pulls survive). No survivor compensates.
        surviving_member_probs = df_member_probs[~df_member_probs["actor_id"].isin(dropout_set)]
        counterfactual_probs   = CounterfactualStageProbs(
            surviving_member_probs, df_member, df_crossmerge, dropout_set, pre_periods)
        post_stage_probs = ComputeStageProbabilities(counterfactual_probs)
    else:
        post_stage_probs = full_stage_probs

    rng = np.random.default_rng(int(hashlib.md5(repo_name.encode()).hexdigest()[:8], 16))

    repo_variance_to_mean_ratio = (1.0 / dist_params["negative_binomial_prob"]
                             if repo_distribution == "negative_binomial" else 1.0)
    post_distribution, post_dist_params = PostLatentDistribution(dist_row["post_latent_mean"], repo_variance_to_mean_ratio)
    pre_block       = DrawPeriodBlock(repo_distribution, dist_params, full_stage_probs, pre_periods, N_MODEL_DRAWS, rng)
    treatment_block = DrawPeriodBlock(repo_distribution, dist_params, full_stage_probs, treatment_periods, N_MODEL_DRAWS, rng)
    post_block      = DrawPeriodBlock(post_distribution, post_dist_params, post_stage_probs, post_periods, N_MODEL_DRAWS, rng)
    period_draws = {**pre_block, **treatment_block, **post_block}
    period_stage_probs = {k: full_stage_probs for k in pre_periods + treatment_periods}
    period_stage_probs.update({k: post_stage_probs for k in post_periods})

    drawn_periods = pre_periods + treatment_periods + post_periods
    raw_draws = BuildRawDrawRows(repo_name, is_treated, drawn_periods, period_draws)

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


def PostLatentDistribution(post_latent_mean, repo_variance_to_mean_ratio):
    latent_mean = max(float(post_latent_mean), 1e-9)
    if not np.isfinite(repo_variance_to_mean_ratio) or repo_variance_to_mean_ratio <= 1.0:
        return "poisson", {"poisson_rate": latent_mean,
                           "negative_binomial_size": np.nan, "negative_binomial_prob": np.nan}
    return "negative_binomial", {"poisson_rate": np.nan,
                                 "negative_binomial_size": latent_mean / (repo_variance_to_mean_ratio - 1.0),
                                 "negative_binomial_prob": 1.0 / repo_variance_to_mean_ratio}


def DrawPeriodBlock(repo_distribution, dist_params, stage_probs, periods, n_draws, rng):
    if not periods:
        return {}
    n_draws_total = len(periods) * n_draws
    flat_outcome_draws = DrawCounts(repo_distribution, dist_params, *stage_probs, n_draws_total, rng)
    outcome_draw_grids = [outcome_draws.reshape(len(periods), n_draws) for outcome_draws in flat_outcome_draws]
    return {
        period: tuple(outcome_draw_grid[period_index] for outcome_draw_grid in outcome_draw_grids)
        for period_index, period in enumerate(periods)
    }


def BuildRawDrawRows(repo_name, is_treated, periods, period_draws):
    period_frames = []
    for period in periods:
        opened, reviewed, merged_direct, merged_after_review, merged = period_draws[period]
        period_frames.append(pd.DataFrame({
            "repo_name":                        repo_name,
            "is_treated":                       is_treated,
            "quasi_event_time":                 period,
            "draw_id":                          np.arange(len(opened)),
            "pull_request_opened":              opened,
            "pull_request_reviewed":            reviewed,
            "pull_request_merged_direct":       merged_direct,
            "pull_request_merged_after_review": merged_after_review,
            "pull_request_merged":              merged,
        }))
    return pd.concat(period_frames, ignore_index=True)


def ScorePeriods(repo_name, is_treated, periods, observed_by_period, period_draws, period_stage_probs, rng):
    period_rows = []
    reference_blocks = []
    for period in periods:
        observed = observed_by_period[period]
        draws    = period_draws[period]
        squared_residuals = AllSquaredResiduals(observed, *draws)
        signed_residuals  = AllSignedResiduals(observed, *draws)
        period_rows.append(PeriodRow(repo_name, is_treated, period, signed_residuals, squared_residuals))
        reference_blocks.append((period, {outcome: StandardizeDraws(draw) for outcome, draw in zip(OUTCOMES, draws)}))
    return period_rows, reference_blocks


def LeaveOneOutResiduals(repo_name, is_treated, pre_periods, df_member, observed_by_period,
                         distribution_type, estimation_approach, rng):
    period_rows = []
    reference_blocks = []
    if len(pre_periods) < 2:
        return period_rows, reference_blocks

    df_pre = df_member[df_member["quasi_event_time"].isin(pre_periods)]
    repo_counts_by_period = df_pre.groupby("quasi_event_time")[
        ["repo_pull_request_opened", "repo_pull_request_reviewed",
         "repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"]
    ].first()
    total_opened_full   = float(repo_counts_by_period["repo_pull_request_opened"].sum())
    total_reviewed_full = float(repo_counts_by_period["repo_pull_request_reviewed"].sum())
    full_member_sums = df_pre.groupby("actor_id")[MEMBER_COUNT_COLUMNS].sum() if estimation_approach == "pooled" else None

    for held_out_time in pre_periods:
        counts_train = repo_counts_by_period["repo_pull_request_opened"].drop(held_out_time).values.astype(float)
        dist_loo  = FitLatentDistribution(repo_name, counts_train, distribution_type)
        if estimation_approach == "pooled":
            held_member_sums   = df_pre.loc[df_pre["quasi_event_time"] == held_out_time].set_index("actor_id")[MEMBER_COUNT_COLUMNS]
            member_sums_loo    = full_member_sums.subtract(held_member_sums, fill_value=0)
            total_opened_loo   = total_opened_full   - float(repo_counts_by_period.loc[held_out_time, "repo_pull_request_opened"])
            total_reviewed_loo = total_reviewed_full - float(repo_counts_by_period.loc[held_out_time, "repo_pull_request_reviewed"])
            member_probs_loo   = MemberProbabilityRows(repo_name, member_sums_loo, total_opened_loo, total_reviewed_loo)
        else:
            df_train      = df_pre[df_pre["quasi_event_time"] != held_out_time]
            df_repo_train = repo_counts_by_period.drop(held_out_time).reset_index()
            member_probs_loo = FitMemberProbabilities(repo_name, df_train, df_repo_train, estimation_approach)
        probs_loo = ComputeStageProbabilities(pd.DataFrame(member_probs_loo))
        dist_params_loo = {
            "poisson_rate":           dist_loo["poisson_rate"],
            "negative_binomial_size": dist_loo["negative_binomial_size"],
            "negative_binomial_prob": dist_loo["negative_binomial_prob"],
        }
        draws = DrawCounts(dist_loo["distribution_type"], dist_params_loo, *probs_loo, N_MODEL_DRAWS, rng)
        observed = observed_by_period[held_out_time]
        squared_residuals = AllSquaredResiduals(observed, *draws)
        signed_residuals  = AllSignedResiduals(observed, *draws)
        period_rows.append(PeriodRow(repo_name, is_treated, held_out_time, signed_residuals, squared_residuals))
        reference_blocks.append((held_out_time, {outcome: StandardizeDraws(draw) for outcome, draw in zip(OUTCOMES, draws)}))
    return period_rows, reference_blocks


def ExtractObserved(row):
    return {
        "pull_request_opened_observed":          float(row["repo_pull_request_opened"]),
        "pull_request_reviewed_observed":        float(row["repo_pull_request_reviewed"]),
        "pull_request_merged_direct_observed":        float(row["repo_pull_request_merged_direct"]),
        "pull_request_merged_after_review_observed":  float(row["repo_pull_request_merged_after_review"]),
        "pull_request_merged_observed":         float(row["repo_pull_request_merged_direct"] + row["repo_pull_request_merged_after_review"]),
    }


def ComputeSquaredStdResidual(observed, draws):
    draw_mean = float(np.mean(draws))
    draw_std  = float(np.std(draws))
    if draw_std == 0.0 or np.isnan(draw_std):
        return np.nan
    return ((observed - draw_mean) / draw_std) ** 2 - 1.0


def ComputeSignedStdResidual(observed, draws):
    draw_mean = float(np.mean(draws))
    draw_std  = float(np.std(draws))
    if draw_std == 0.0 or np.isnan(draw_std):
        return np.nan
    return (observed - draw_mean) / draw_std


def AllSquaredResiduals(observed, opened_draw, reviewed_draw, merged_direct_draw, merged_after_review_draw, merged_draw):
    return {
        "squared_std_residual_opened":             ComputeSquaredStdResidual(observed["pull_request_opened_observed"], opened_draw),
        "squared_std_residual_reviewed":           ComputeSquaredStdResidual(observed["pull_request_reviewed_observed"], reviewed_draw),
        "squared_std_residual_merged_direct":      ComputeSquaredStdResidual(observed["pull_request_merged_direct_observed"], merged_direct_draw),
        "squared_std_residual_merged_after_review": ComputeSquaredStdResidual(observed["pull_request_merged_after_review_observed"], merged_after_review_draw),
        "squared_std_residual_merged":       ComputeSquaredStdResidual(observed["pull_request_merged_observed"], merged_draw),
    }


def AllSignedResiduals(observed, opened_draw, reviewed_draw, merged_direct_draw, merged_after_review_draw, merged_draw):
    return {
        "signed_std_residual_opened":             ComputeSignedStdResidual(observed["pull_request_opened_observed"], opened_draw),
        "signed_std_residual_reviewed":           ComputeSignedStdResidual(observed["pull_request_reviewed_observed"], reviewed_draw),
        "signed_std_residual_merged_direct":      ComputeSignedStdResidual(observed["pull_request_merged_direct_observed"], merged_direct_draw),
        "signed_std_residual_merged_after_review": ComputeSignedStdResidual(observed["pull_request_merged_after_review_observed"], merged_after_review_draw),
        "signed_std_residual_merged":       ComputeSignedStdResidual(observed["pull_request_merged_observed"], merged_draw),
    }


# Simulated-null reference: draws from the fitted model standardized exactly as the real
# residuals are, so a cross-org KS test compares against the model's own finite-sample null
# rather than an analytic N(0,1)/chi^2 reference.
def StandardizeDraws(draws):
    draw_std = float(np.std(draws))
    if draw_std == 0.0 or np.isnan(draw_std):
        return None
    return (np.asarray(draws, dtype=float) - float(np.mean(draws))) / draw_std


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


def PeriodRow(repo_name, is_treated, quasi_event_time, signed_residuals, squared_residuals):
    return {
        "repo_name": repo_name, "is_treated": is_treated, "quasi_event_time": int(quasi_event_time),
        **{residual_name: residual_value for residual_name, residual_value in signed_residuals.items()
           if residual_name.startswith("signed_std_residual_")},
        **{residual_name: residual_value for residual_name, residual_value in squared_residuals.items()
           if residual_name.startswith("squared_std_residual_")},
    }


if __name__ == "__main__":
    Main()
