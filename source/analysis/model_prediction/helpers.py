from collections import defaultdict
from itertools import product

import numpy as np
import pandas as pd


def RelativeTrajectoryError(pred_traj, obs_traj, k_set):
    squared_relative_errors = []
    for event_time in k_set:
        predicted = pred_traj.get(event_time, np.nan)
        observed  = obs_traj.get(event_time, 0.0)
        if np.isnan(predicted) or predicted == 0:
            return np.nan
        squared_relative_errors.append(((predicted - observed) / predicted) ** 2)
    if not squared_relative_errors:
        return np.nan
    return np.sqrt(np.mean(squared_relative_errors))


def RawTrajectoryError(pred_traj, obs_traj, k_set):
    squared_errors = [
        (pred_traj.get(event_time, 0.0) - obs_traj.get(event_time, 0.0)) ** 2
        for event_time in k_set
    ]
    if not squared_errors:
        return np.nan
    return np.sqrt(np.mean(squared_errors))


def SharePartitionError(p_hat, p_obs_traj, singleton_members, other_members, known_members, k_set):
    n_other         = len(other_members)
    hat_P_O         = sum(p_hat.get(j, 0.0) for j in other_members)
    n_known_other   = sum(1 for j in other_members if j in known_members)
    n_unknown_other = n_other - n_known_other

    acc = defaultdict(float)

    for k in k_set:
        obs_k = p_obs_traj.get(k, {})
        P_O_k = sum(obs_k.get(j, 0.0) for j in other_members)

        for j in singleton_members:
            err_sq = (p_hat.get(j, 0.0) - obs_k.get(j, 0.0)) ** 2
            key = "known_singleton_cell_fit" if j in known_members else "unknown_singleton_cell_fit"
            acc[key] += err_sq

        if n_other > 0:
            cell_err_sq = (hat_P_O - P_O_k) ** 2
            acc["known_other_cell_fit"]   += cell_err_sq * n_known_other   / n_other ** 2
            acc["unknown_other_cell_fit"] += cell_err_sq * n_unknown_other / n_other ** 2

            avg_obs_other = P_O_k / n_other
            for j in other_members:
                err_sq = (avg_obs_other - obs_k.get(j, 0.0)) ** 2
                key = "known_other_coarsening" if j in known_members else "unknown_other_coarsening"
                acc[key] += err_sq

    n_k = len(k_set)
    sq = {key: val / n_k for key, val in acc.items()}

    sq["singleton_cell_fit"] = sq.get("known_singleton_cell_fit", 0.0) + sq.get("unknown_singleton_cell_fit", 0.0)
    sq["other_cell_fit"]     = sq.get("known_other_cell_fit", 0.0)     + sq.get("unknown_other_cell_fit", 0.0)
    sq["other_coarsening"]   = sq.get("known_other_coarsening", 0.0)   + sq.get("unknown_other_coarsening", 0.0)
    sq["total"]              = sq["singleton_cell_fit"] + sq["other_cell_fit"] + sq["other_coarsening"]

    result = {key: np.sqrt(val) for key, val in sq.items()}

    total_sq = sq["total"]
    share_pairs = [
        ("share_singleton_cell_fit",         "singleton_cell_fit"),
        ("share_other_cell_fit",             "other_cell_fit"),
        ("share_other_coarsening",           "other_coarsening"),
        ("share_known_singleton_cell_fit",   "known_singleton_cell_fit"),
        ("share_unknown_singleton_cell_fit", "unknown_singleton_cell_fit"),
        ("share_known_other_cell_fit",       "known_other_cell_fit"),
        ("share_unknown_other_cell_fit",     "unknown_other_cell_fit"),
        ("share_known_other_coarsening",     "known_other_coarsening"),
        ("share_unknown_other_coarsening",   "unknown_other_coarsening"),
    ]
    for share_key, sq_key in share_pairs:
        result[share_key] = sq.get(sq_key, 0.0) / total_sq if total_sq > 0 else np.nan

    return result


def WeightedStageAggregate(per_stage_errors, weights):
    total_sq = sum(weights.get(s, 0.0) * v ** 2 for s, v in per_stage_errors.items() if not np.isnan(v))
    return np.sqrt(total_sq)


def CrossOrgRootMeanSquare(per_org_values):
    valid_values = [v for v in per_org_values if not np.isnan(v)]
    if not valid_values:
        return np.nan
    mean_squared = np.mean([v ** 2 for v in valid_values])
    return np.sqrt(mean_squared)


def CrossOrgMedian(per_org_values):
    vals = [v for v in per_org_values if not np.isnan(v)]
    return float(np.median(vals)) if vals else np.nan


def ComputeIndividualShares(panel, ell):
    pre = panel[panel["quasi_event_time"].between(-ell, -1)]
    result = {}
    for stage, col in [
        ("o", "member_pull_request_opened"),
        ("r", "member_pull_request_reviewed"),
        ("m", "member_pull_request_merged"),
    ]:
        totals = pre.groupby("actor_id")[col].sum()
        grand  = totals.sum()
        result[stage] = (totals / grand).to_dict() if grand > 0 else {aid: 0.0 for aid in totals.index}
    return result


def BuildSingletonOtherPartition(pre_period_panel, rule, stage, member_universe, top_k=5, coverage_threshold=0.80):
    member_universe = set(member_universe)
    all_cols  = ["member_pull_request_opened", "member_pull_request_reviewed", "member_pull_request_merged"]
    stage_col = {"o": "member_pull_request_opened", "r": "member_pull_request_reviewed", "m": "member_pull_request_merged"}[stage]

    if rule == "all":
        return member_universe, set()

    cols   = all_cols if rule in ("top5", "cover80p") else [stage_col]
    totals = (
        pre_period_panel.groupby("actor_id")[cols].sum().sum(axis=1)
        .reindex(list(member_universe), fill_value=0)
    )

    if rule in ("top5", "top5_per_stage"):
        top = set(totals.nlargest(top_k).index)
        return top, member_universe - top

    total_activity = totals.sum()
    if total_activity == 0:
        return set(), member_universe
    sorted_actors = totals.sort_values(ascending=False)
    cumsum        = sorted_actors.cumsum()
    top = set(sorted_actors[cumsum.shift(1, fill_value=0) < coverage_threshold * total_activity].index)
    remaining = sorted_actors[~sorted_actors.index.isin(top)]
    if not remaining.empty:
        top.add(remaining.index[0])
    return top, member_universe - top


def IndependentBernoulliAggregation(individual_shares, exclude=None):
    prob_none = 1.0
    for j, p in individual_shares.items():
        if j != exclude:
            prob_none *= 1.0 - min(max(p, 0.0), 1.0)
    return 1.0 - prob_none


def ScaledRealizedRemainingShares(panel, p_hat_dep, departed_id, stage):
    col   = {"o": "member_pull_request_opened", "r": "member_pull_request_reviewed", "m": "member_pull_request_merged"}[stage]
    scale = 1.0 - p_hat_dep
    result = {}
    for k, grp in panel[panel["quasi_event_time"] >= 0].groupby("quasi_event_time"):
        remaining = grp[grp["actor_id"] != departed_id]
        total     = remaining[col].sum()
        if total == 0:
            result[k] = None
        else:
            result[k] = {row["actor_id"]: scale * row[col] / total for _, row in remaining.iterrows()}
    return result


def ComponentCorrectedShares(p_hat, p_obs_k, singleton_members, other_members, level):
    n_other = len(other_members)
    hat_P_O = sum(p_hat.get(j, 0.0) for j in other_members)
    P_O_obs = sum(p_obs_k.get(j, 0.0) for j in other_members)

    result = {}
    if level == "model_exploded":
        for j in singleton_members:
            result[j] = p_hat.get(j, 0.0)
        for j in other_members:
            result[j] = hat_P_O / n_other if n_other > 0 else 0.0
    elif level == "singleton_cell_fit_corrected":
        for j in singleton_members:
            result[j] = p_obs_k.get(j, 0.0)
        for j in other_members:
            result[j] = hat_P_O / n_other if n_other > 0 else 0.0
    elif level == "other_cell_fit_corrected":
        for j in singleton_members:
            result[j] = p_obs_k.get(j, 0.0)
        for j in other_members:
            result[j] = P_O_obs / n_other if n_other > 0 else 0.0
    elif level == "full_oracle":
        for j in list(singleton_members) + list(other_members):
            result[j] = p_obs_k.get(j, 0.0)
    else:
        raise ValueError(f"Unknown level: {level}")
    return result


def ModelPredictionCombos(cfg):
    for importance_type, qualified_sample, control_group in product(
        cfg["importance_types"]["run"],
        cfg["qualified_samples"]["run"],
        cfg["control_groups"]["run"],
    ):
        for rolling_period in [f"rolling{p}" for p in cfg["rolling_periods"]["run"]]:
            yield {
                "importance_type":  importance_type,
                "rolling_period":   rolling_period,
                "qualified_sample": qualified_sample,
                "control_group":    control_group,
            }
