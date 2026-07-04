# SCRIPT SINGLE-USE EXCEPTION: standalone read-only diagnostic under issue/; not part of the SCons pipeline.
#
# Compares two ways of setting the org baseline latent opening rate lambda_i^0 in the staged-count model,
# reporting how the post-period signed-residual fit changes across 10 cells (5 outcomes x {treated, control}).
#
# CURRENT  : period_multiplier[k] = member_effect[k] / mean_over_pre_periods(member_effect)   (as in predict_model.py)
# SWITCHED : period_multiplier[k] = exp(alpha_i) * member_effect[k] / m                        (lambda_i^0 = e^{alpha_i})
#            where m = base mean of F_i (expected_latent_count) and alpha_i = org FE from the FE-Poisson.
#
# Everything else (stage probs, per-repo RNG seed, Z definition, reference standardization) is identical and
# uses the SAME seed per repo, so draws are paired between arms.

import os
os.chdir("/Volumes/Extreme SSD/oss_hierarchy")

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyfixest as pf
from scipy.stats import ks_2samp

from source.lib.python.config_loaders import LoadPipelineInputs, LoadAnalysisParameters
from source.lib.python.repo_utils import MakeRepoNameSafe
from source.lib.model.staged_count_model import ComputeStageProbabilities, DrawCounts, FitLatentDistribution
from source.derived.model_prediction.compute_mean_reversion import BuildPrePeriod

CONFIG        = LoadPipelineInputs()
PARAMETERS    = LoadAnalysisParameters()
N_MODEL_DRAWS = PARAMETERS["n_model_draws"]
ROLLING_LABEL = f"rolling{CONFIG['rolling_periods']['run'][0]}"

VARIANT             = "opened_cohort"
DISTRIBUTION_TYPE   = "adaptive"
ESTIMATION_APPROACH = "pooled"
IMPORTANCE_TYPE     = "important_degree_top3"
QUALIFIED_SAMPLE    = "exact1"
CONTROL_GROUP       = "nevertreated"

OUTCOMES = ["opened", "reviewed", "merged_direct", "merged_after_review", "merged"]

INDIR_MEMBER_PANEL   = Path("drive/output/derived/model_prediction/event_time_member_panel")
INDIR_ANALYSIS_PANEL = Path("output/derived/analysis_panel")
INDIR_MEAN_REVERSION = Path("output/derived/model_prediction/mean_reversion")
INDIR_FITTED         = Path("output/analysis/model_prediction")

FITTED_DIR = (
    INDIR_FITTED / VARIANT / DISTRIBUTION_TYPE / "parameters" / ESTIMATION_APPROACH
    / IMPORTANCE_TYPE / QUALIFIED_SAMPLE / CONTROL_GROUP
)
RESIDUALS_DIR = (
    INDIR_FITTED / VARIANT / DISTRIBUTION_TYPE / "residuals" / ESTIMATION_APPROACH
    / IMPORTANCE_TYPE / QUALIFIED_SAMPLE / CONTROL_GROUP
)
REFERENCE_DIR = (
    Path("drive/output/analysis/model_prediction") / VARIANT / DISTRIBUTION_TYPE / "residuals"
    / ESTIMATION_APPROACH / IMPORTANCE_TYPE / QUALIFIED_SAMPLE / CONTROL_GROUP
)


# ---- replicated logic from predict_model.py (kept byte-for-byte where it matters) ----

def LoadMeanReversionAdj():
    df_estimates = pd.read_csv(INDIR_MEAN_REVERSION / "mean_reversion_estimates.csv")
    matched = df_estimates[
        (df_estimates["importance_type"] == IMPORTANCE_TYPE)
        & (df_estimates["control_group"] == CONTROL_GROUP)
    ]
    return {int(row["num_important_qualified"]): float(row["mean_reversion_adj"]) for _, row in matched.iterrows()}


def MeanReversionAdj(mean_reversion_adj_table, num_important_qualified):
    if num_important_qualified <= 0 or not mean_reversion_adj_table:
        return 1.0
    return mean_reversion_adj_table.get(min(int(num_important_qualified), max(mean_reversion_adj_table)), 1.0)


def ExtractObserved(row):
    return {
        "pull_request_opened_observed":              float(row["repo_pull_request_opened"]),
        "pull_request_reviewed_observed":            float(row["repo_pull_request_reviewed"]),
        "pull_request_merged_direct_observed":       float(row["repo_pull_request_merged_direct"]),
        "pull_request_merged_after_review_observed": float(row["repo_pull_request_merged_after_review"]),
        "pull_request_merged_observed":              float(row["repo_pull_request_merged_direct"] + row["repo_pull_request_merged_after_review"]),
    }


def ComputeSignedStdResidual(observed, draws):
    draw_mean = float(np.mean(draws))
    draw_std  = float(np.std(draws))
    if draw_std == 0.0 or np.isnan(draw_std):
        return np.nan
    return (observed - draw_mean) / draw_std


def StandardizeDraws(draws):
    draw_std = float(np.std(draws))
    if draw_std == 0.0 or np.isnan(draw_std):
        return None
    return (np.asarray(draws, dtype=float) - float(np.mean(draws))) / draw_std


def AllSignedResiduals(observed, opened_draw, reviewed_draw, merged_direct_draw, merged_after_review_draw, merged_draw):
    return {
        "signed_std_residual_opened":              ComputeSignedStdResidual(observed["pull_request_opened_observed"], opened_draw),
        "signed_std_residual_reviewed":            ComputeSignedStdResidual(observed["pull_request_reviewed_observed"], reviewed_draw),
        "signed_std_residual_merged_direct":       ComputeSignedStdResidual(observed["pull_request_merged_direct_observed"], merged_direct_draw),
        "signed_std_residual_merged_after_review": ComputeSignedStdResidual(observed["pull_request_merged_after_review_observed"], merged_after_review_draw),
        "signed_std_residual_merged":              ComputeSignedStdResidual(observed["pull_request_merged_observed"], merged_draw),
    }


def DrawPeriodBlock(repo_distribution, dist_params, stage_probs, periods, n_draws, rng, latent_rate_multiplier):
    if not periods:
        return {}
    n_draws_total = len(periods) * n_draws
    flat_outcome_draws = DrawCounts(repo_distribution, dist_params, *stage_probs, n_draws_total, rng, latent_rate_multiplier)
    outcome_draw_grids = [outcome_draws.reshape(len(periods), n_draws) for outcome_draws in flat_outcome_draws]
    return {
        period: tuple(outcome_draw_grid[period_index] for outcome_draw_grid in outcome_draw_grids)
        for period_index, period in enumerate(periods)
    }


def DrawByMultiplier(repo_distribution, dist_params, stage_probs, periods, period_multiplier, n_draws, rng):
    block = {}
    for multiplier in sorted({period_multiplier[k] for k in periods}):
        matched_periods = [k for k in periods if period_multiplier[k] == multiplier]
        block.update(DrawPeriodBlock(repo_distribution, dist_params, stage_probs, matched_periods, n_draws, rng, multiplier))
    return block


def ProcessRepo(repo_name, is_treated, dropout_set, df_dist_repo, df_member_probs,
                mean_reversion_adj_table, num_important_qualified_by_period, alpha_i, arm_name):
    """Returns (post_signed_rows, post_reference_frame) for one repo, one arm.

    arm_name in {current, switched, baseline_fit}; alpha_i is the org FE (log baseline) for switched.
    """
    if df_dist_repo.empty:
        return None

    member_path = (
        INDIR_MEMBER_PANEL / VARIANT / IMPORTANCE_TYPE / QUALIFIED_SAMPLE
        / CONTROL_GROUP / f"{MakeRepoNameSafe(repo_name)}.parquet"
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
    all_periods = sorted(int(k) for k in df_repo_counts["quasi_event_time"])
    pre_periods = [k for k in all_periods if k < 0]
    if len(pre_periods) == 0:
        return None
    post_periods = [k for k in all_periods if k >= 1]
    observed_by_period = {int(row["quasi_event_time"]): ExtractObserved(row) for _, row in df_repo_counts.iterrows()}

    dist_row = df_dist_repo.iloc[0]
    repo_distribution = dist_row["distribution_type"]
    dist_params = {
        "poisson_rate":           dist_row["poisson_rate"],
        "negative_binomial_size": dist_row["negative_binomial_size"],
        "negative_binomial_prob": dist_row["negative_binomial_prob"],
    }
    base_mean = float(dist_row["expected_latent_count"])

    full_stage_probs = ComputeStageProbabilities(df_member_probs)
    if is_treated:
        post_stage_probs = ComputeStageProbabilities(df_member_probs[~df_member_probs["actor_id"].isin(dropout_set)])
    else:
        post_stage_probs = full_stage_probs

    rng = np.random.default_rng(int(hashlib.md5(repo_name.encode()).hexdigest()[:8], 16))

    member_effect = {k: 1.0 / MeanReversionAdj(mean_reversion_adj_table, num_important_qualified_by_period.get(k, 0)) for k in all_periods}
    if arm_name == "current":
        # CURRENT: divide F_i (fit over q-mixed pre-period) by the average pre-period member effect.
        pre_period_member_effect = float(np.mean([member_effect[k] for k in pre_periods]))
        period_multiplier = {k: member_effect[k] / pre_period_member_effect for k in all_periods}
    elif arm_name == "switched":
        # SWITCHED: lambda_i^0 = exp(alpha_i); target latent mean each period = exp(alpha_i) * member_effect[k].
        # F_i draws at base mean = base_mean, so the multiplier that maps base_mean -> target is (target / base_mean).
        lambda_baseline = float(np.exp(alpha_i))
        period_multiplier = {k: lambda_baseline * member_effect[k] / base_mean for k in all_periods}
    elif arm_name == "baseline_fit":
        # BASELINE_FIT: baseline lambda_i^0 = mean of delta-scaled pre counts; family via the Pearson dispersion
        # of raw counts against their period means mu_t = lambda0/delta_t (so q-variation is not read as
        # over-dispersion). Draw each period at base mean lambda0 * member_effect[k] (= mu_t).
        pre_counts = np.array([observed_by_period[k]["pull_request_opened_observed"] for k in pre_periods], dtype=float)
        pre_delta  = np.array([MeanReversionAdj(mean_reversion_adj_table, num_important_qualified_by_period.get(k, 0))
                               for k in pre_periods], dtype=float)
        lambda0 = float(np.mean(pre_delta * pre_counts))
        mu_pre = lambda0 / pre_delta
        if lambda0 > 0 and len(pre_periods) > 1 and np.all(mu_pre > 0):
            phi = float(np.mean((pre_counts - mu_pre) ** 2 / mu_pre))
        else:
            phi = 1.0
        if phi <= 1.0 or lambda0 <= 0:
            repo_distribution = "poisson"
            dist_params = {"poisson_rate": lambda0, "negative_binomial_size": np.nan, "negative_binomial_prob": np.nan}
        else:
            prob = 1.0 / phi   # NB Fano factor 1/p = phi (constant across periods when size scales, p fixed)
            repo_distribution = "negative_binomial"
            dist_params = {"poisson_rate": np.nan, "negative_binomial_size": lambda0 / (phi - 1.0), "negative_binomial_prob": prob}
        period_multiplier = {k: member_effect[k] for k in all_periods}
    else:
        raise ValueError(f"Unknown arm_name: {arm_name}")

    # Only pre + post are needed for the post-period fit, but the RNG draw order must match predict_model.py
    # (pre block, then treatment block at k==0, then post block) so the SAME seed yields identical post draws
    # across arms and reproduces the on-disk CURRENT numbers.
    treatment_periods = [k for k in all_periods if k == 0]
    _pre_block = DrawByMultiplier(repo_distribution, dist_params, full_stage_probs, pre_periods, period_multiplier, N_MODEL_DRAWS, rng)
    _treat_block = DrawByMultiplier(repo_distribution, dist_params, full_stage_probs, treatment_periods, period_multiplier, N_MODEL_DRAWS, rng)
    post_block = DrawByMultiplier(repo_distribution, dist_params, post_stage_probs, post_periods, period_multiplier, N_MODEL_DRAWS, rng)

    signed_rows = []
    reference_blocks = []
    for period in post_periods:
        observed = observed_by_period[period]
        draws = post_block[period]
        signed = AllSignedResiduals(observed, *draws)
        signed_rows.append({"repo_name": repo_name, "is_treated": is_treated, "quasi_event_time": period, **signed})
        reference_blocks.append((period, {outcome: StandardizeDraws(draw) for outcome, draw in zip(OUTCOMES, draws)}))

    reference_frame = BuildReferenceFrame(repo_name, is_treated, reference_blocks)
    return signed_rows, reference_frame


def BuildReferenceFrame(repo_name, is_treated, blocks):
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
        frames.append(block)
    if not frames:
        frame = pd.DataFrame({"quasi_event_time": [], **{f"signed_std_residual_{o}": [] for o in OUTCOMES}})
    else:
        frame = pd.concat(frames, ignore_index=True)
    frame.insert(0, "repo_name", repo_name)
    frame.insert(1, "is_treated", is_treated)
    return frame


# ---- cell statistics ----

def SplitGroups(df_residuals):
    return {"treated": df_residuals[df_residuals["is_treated"]], "control": df_residuals[~df_residuals["is_treated"]]}


def CellStats(df_residuals, df_reference):
    """Return {(outcome, group): (mean, median, ks)} pooling over repos x post-periods."""
    residuals_by_group = SplitGroups(df_residuals)
    reference_by_group = SplitGroups(df_reference)
    stats = {}
    for group in ("treated", "control"):
        for outcome in OUTCOMES:
            observed = residuals_by_group[group][f"signed_std_residual_{outcome}"].dropna()
            reference = reference_by_group[group][f"signed_std_residual_{outcome}"].dropna()
            mean_val = float(observed.mean()) if len(observed) else np.nan
            median_val = float(observed.median()) if len(observed) else np.nan
            if len(observed) >= 3 and len(reference) >= 3:
                ks_val = float(ks_2samp(observed.values, reference.values).statistic)
            else:
                ks_val = np.nan
            stats[(outcome, group)] = (mean_val, median_val, ks_val)
    return stats


def BuildArm(df_all_repos, df_dist, df_probs, mean_reversion_adj_table,
             num_important_qualified_by_repo, alpha_by_repo, arm_name):
    signed_rows_all = []
    reference_frames = []
    for _, row in df_all_repos.iterrows():
        repo_name = row["repo_name"]
        is_treated = row["num_dropouts"] > 0
        dropout_set = set(json.loads(row["dropouts_actors"])) if is_treated else set()
        alpha_i = alpha_by_repo[repo_name] if arm_name == "switched" else None
        result = ProcessRepo(
            repo_name, is_treated, dropout_set,
            df_dist[df_dist["repo_name"] == repo_name],
            df_probs[df_probs["repo_name"] == repo_name],
            mean_reversion_adj_table,
            num_important_qualified_by_repo.get(repo_name, {}),
            alpha_i, arm_name,
        )
        if result is None:
            continue
        signed_rows, reference_frame = result
        signed_rows_all.extend(signed_rows)
        reference_frames.append(reference_frame)
    df_signed = pd.DataFrame(signed_rows_all)
    df_reference = pd.concat(reference_frames, ignore_index=True)
    return df_signed, df_reference


def FormatTable(current_stats, switched_stats):
    header = (f"{'outcome':<20}{'grp':<8}"
              f"{'mean_cur':>10}{'mean_sw':>10}{'d_mean':>9}"
              f"{'med_cur':>10}{'med_sw':>10}{'d_med':>9}"
              f"{'ks_cur':>9}{'ks_sw':>9}{'d_ks':>9}")
    lines = [header, "-" * len(header)]
    for group in ("treated", "control"):
        for outcome in OUTCOMES:
            mc, mdc, kc = current_stats[(outcome, group)]
            ms, mds, ks = switched_stats[(outcome, group)]
            lines.append(
                f"{outcome:<20}{group:<8}"
                f"{mc:>10.3f}{ms:>10.3f}{ms - mc:>9.3f}"
                f"{mdc:>10.3f}{mds:>10.3f}{mds - mdc:>9.3f}"
                f"{kc:>9.3f}{ks:>9.3f}{ks - kc:>9.3f}"
            )
    return "\n".join(lines)


def Main():
    df_dist  = pd.read_parquet(FITTED_DIR / "distribution_params.parquet")
    df_probs = pd.read_parquet(FITTED_DIR / "member_probabilities.parquet")

    mean_reversion_adj_table = LoadMeanReversionAdj()

    panel_path = INDIR_ANALYSIS_PANEL / IMPORTANCE_TYPE / ROLLING_LABEL / QUALIFIED_SAMPLE / CONTROL_GROUP / "panel.parquet"
    df_panel = pd.read_parquet(panel_path)
    df_all_repos = df_panel[df_panel["quasi_event_time"] == 0][["repo_name", "dropouts_actors", "num_dropouts"]]
    num_important_qualified_by_repo = {
        repo_name: dict(zip(group["quasi_event_time"].astype(int), group["num_important_qualified"].astype(int)))
        for repo_name, group in df_panel.groupby("repo_name")
    }

    # ---- org fixed effects alpha_i from the FE-Poisson (same fit as compute_mean_reversion.py) ----
    pre_period = BuildPrePeriod(IMPORTANCE_TYPE, CONTROL_GROUP)
    # fixef() rejects i() transformations, so use explicit q_L dummies (q=0 the omitted reference)
    # -- an identical fit to compute_mean_reversion.py's i(num_important_qualified, ref=0).
    qualified_levels = [L for L in sorted(pre_period["num_important_qualified"].unique()) if L >= 1]
    dummy_terms = []
    for L in qualified_levels:
        pre_period[f"q_{L}"] = (pre_period["num_important_qualified"] == L).astype(int)
        dummy_terms.append(f"q_{L}")
    reversion_fit = pf.fepois(f"pull_request_opened ~ {' + '.join(dummy_terms)} | repo_name", data=pre_period)
    fixef = reversion_fit.fixef()
    fe_key = next(iter(fixef))  # e.g. "repo_name"
    alpha_by_repo = {str(level): float(value) for level, value in fixef[fe_key].items()}

    # Pair over identical repos: drop any repo absent from the FE fit from BOTH arms.
    all_repo_names = list(df_all_repos["repo_name"])
    missing_alpha = [r for r in all_repo_names if r not in alpha_by_repo]
    df_all_repos = df_all_repos[df_all_repos["repo_name"].isin(alpha_by_repo)].copy()

    df_current_signed, df_current_reference = BuildArm(
        df_all_repos, df_dist, df_probs, mean_reversion_adj_table,
        num_important_qualified_by_repo, alpha_by_repo, "current")
    df_switched_signed, df_switched_reference = BuildArm(
        df_all_repos, df_dist, df_probs, mean_reversion_adj_table,
        num_important_qualified_by_repo, alpha_by_repo, "baseline_fit")

    current_stats  = CellStats(df_current_signed, df_current_reference)
    switched_stats = CellStats(df_switched_signed, df_switched_reference)

    # ---- VALIDATION: reproduce the on-disk CURRENT numbers with the same machinery ----
    df_disk_post      = pd.read_parquet(RESIDUALS_DIR / "post_period.parquet")
    df_disk_reference = pd.read_parquet(REFERENCE_DIR / "post_reference.parquet")
    disk_stats = CellStats(df_disk_post, df_disk_reference)

    n_treated = df_current_signed[df_current_signed["is_treated"]]["repo_name"].nunique()
    n_control = df_current_signed[~df_current_signed["is_treated"]]["repo_name"].nunique()

    print("=" * 110)
    print("VALIDATION: re-drawn CURRENT arm vs on-disk pipeline outputs (should match within Monte-Carlo noise)")
    print("=" * 110)
    print(f"{'outcome':<20}{'grp':<8}{'mean_disk':>12}{'mean_redraw':>12}{'d':>9}"
          f"{'ks_disk':>10}{'ks_redraw':>11}{'d':>9}")
    max_mean_gap = 0.0
    max_ks_gap = 0.0
    for group in ("treated", "control"):
        for outcome in OUTCOMES:
            md, _, kd = disk_stats[(outcome, group)]
            mr, _, kr = current_stats[(outcome, group)]
            max_mean_gap = max(max_mean_gap, abs(md - mr))
            max_ks_gap = max(max_ks_gap, abs(kd - kr))
            print(f"{outcome:<20}{group:<8}{md:>12.3f}{mr:>12.3f}{mr - md:>9.3f}"
                  f"{kd:>10.3f}{kr:>11.3f}{kr - kd:>9.3f}")
    print(f"\nmax |mean gap| = {max_mean_gap:.4f}   max |KS gap| = {max_ks_gap:.4f}")
    print()

    print("=" * 110)
    print("MAIN COMPARISON: CURRENT (raw fit / member-effect-normalized) vs BASELINE_FIT (fit at q=0 baseline, Pearson-phi family)")
    print("Columns labeled '_sw' are the BASELINE_FIT arm.")
    print("Post-periods = quasi_event_time >= 1. Good signed fit = mean/median near 0 and low KS-vs-null.")
    print("=" * 110)
    print(FormatTable(current_stats, switched_stats))
    print()
    print(f"repos per arm (paired, present in FE fit): {len(df_all_repos)}"
          f"  (treated={n_treated}, control={n_control})")
    print(f"repos dropped for missing alpha_i: {len(missing_alpha)}")
    if missing_alpha:
        print(f"  dropped repos: {missing_alpha}")
    print(f"N_MODEL_DRAWS = {N_MODEL_DRAWS}")


if __name__ == "__main__":
    Main()
