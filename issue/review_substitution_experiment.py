# TEMPORARY EXPERIMENT (issue/): how much of the post-period error in the pull_request_merged
# event-study band is driven by the simulated review count rather than the merge-after-review rate?
#
# Rebuilds the merge-after-review path of the model draws using the ACTUAL post-period review count
# as the binomial n, instead of the simulated reviewed_draw, holding merged_direct and the fitted
# per-repo prob_merge_after_review fixed. Writes counterfactual raw_draws parquet(s) that the
# companion R script feeds through the real event-study code path.
#
# SCRIPT SINGLE-USE EXCEPTION: throwaway diagnostic, not part of the SCons pipeline.

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from source.lib.model.staged_count_model import ComputeStageProbabilities

SAMPLE          = "exact1"
IMPORTANCE_TYPE = "important_degree_top3"
CONTROL_GROUP   = "nevertreated"
VARIANT, DISTRIBUTION, ESTIMATION = "opened_cohort", "adaptive", "pooled"

DRAWS_DIR   = Path(f"drive/output/analysis/model_prediction/{VARIANT}/{DISTRIBUTION}/draws/"
                   f"{ESTIMATION}/{IMPORTANCE_TYPE}/{SAMPLE}/{CONTROL_GROUP}")
PARAMS_DIR  = Path(f"output/analysis/model_prediction/{VARIANT}/{DISTRIBUTION}/parameters/"
                   f"{ESTIMATION}/{IMPORTANCE_TYPE}/{SAMPLE}/{CONTROL_GROUP}")
MEMBER_DIR  = Path(f"drive/output/derived/model_prediction/event_time_member_panel/"
                   f"{VARIANT}/{IMPORTANCE_TYPE}/{SAMPLE}/{CONTROL_GROUP}")
OUTDIR      = Path(f"issue/review_substitution/draws/{SAMPLE}")


def Main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    draws = pd.read_parquet(DRAWS_DIR / "raw_draws.parquet")

    observed_reviewed = LoadObservedReviewed()
    prob_merge_after_review = LoadMergeAfterReviewProb()

    counterfactual = SubstituteObservedReview(draws, observed_reviewed, prob_merge_after_review)
    counterfactual.to_parquet(OUTDIR / "raw_draws.parquet", index=False)

    ReportShift(draws, counterfactual, observed_reviewed)


def LoadObservedReviewed():
    member = pd.read_parquet(MEMBER_DIR)
    return (member[["repo_name", "quasi_event_time", "repo_pull_request_reviewed"]]
            .drop_duplicates()
            .set_index(["repo_name", "quasi_event_time"])["repo_pull_request_reviewed"])


def LoadMergeAfterReviewProb():
    member_probs = pd.read_parquet(PARAMS_DIR / "member_probabilities.parquet")
    prob_by_repo = {}
    for repo_name, group in member_probs.groupby("repo_name"):
        _, _, _, prob_merge_after_review = ComputeStageProbabilities(group)
        prob_by_repo[repo_name] = float(np.clip(prob_merge_after_review, 0.0, 1.0))
    return prob_by_repo


def SubstituteObservedReview(draws, observed_reviewed, prob_merge_after_review):
    counterfactual = draws.copy()
    post_mask = counterfactual["quasi_event_time"] >= 1
    for (repo_name, quasi_event_time), block in counterfactual[post_mask].groupby(["repo_name", "quasi_event_time"]):
        key = (repo_name, quasi_event_time)
        if key not in observed_reviewed.index or repo_name not in prob_merge_after_review:
            continue
        n_reviewed = int(observed_reviewed.loc[key])
        prob       = prob_merge_after_review[repo_name]
        # Deterministic per-(repo, period) seed so the experiment is reproducible.
        seed = int(hashlib.md5(f"{repo_name}|{quasi_event_time}".encode()).hexdigest()[:8], 16)
        rng  = np.random.default_rng(seed)
        merged_after_review = rng.binomial(n_reviewed, prob, size=len(block))
        idx = block.index
        counterfactual.loc[idx, "pull_request_merged_after_review"] = merged_after_review
        counterfactual.loc[idx, "pull_request_merged"] = (
            counterfactual.loc[idx, "pull_request_merged_direct"] + merged_after_review)
    return counterfactual


def ReportShift(draws, counterfactual, observed_reviewed):
    post = draws["quasi_event_time"] >= 1
    sim_reviewed_mean = draws[post].groupby("quasi_event_time")["pull_request_reviewed"].mean()
    obs_reviewed_mean = (observed_reviewed.reset_index()
                         .query("quasi_event_time >= 1")
                         .groupby("quasi_event_time")["repo_pull_request_reviewed"].mean())
    orig_merged = draws[post].groupby("quasi_event_time")["pull_request_merged"].mean()
    cf_merged   = counterfactual[counterfactual["quasi_event_time"] >= 1].groupby("quasi_event_time")["pull_request_merged"].mean()
    summary = pd.DataFrame({
        "sim_reviewed_mean":   sim_reviewed_mean,
        "obs_reviewed_mean":   obs_reviewed_mean,
        "orig_merged_mean":    orig_merged,
        "cf_merged_mean":      cf_merged,
    })
    print(summary.round(3))
    summary.to_csv(Path("issue/review_substitution") / "draw_mean_shift.csv")


if __name__ == "__main__":
    Main()
