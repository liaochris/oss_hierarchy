"""Clean latent-arrival experiment harness for issue/improve_latent. Imports the REAL model library
read-only and mirrors source/analysis/model_prediction/predict_model.py::ProcessRepo, swapping ONLY
the latent problem-arrival component:

  method = "baseline" : the committed delta model (reproduces the pipeline byte-for-byte at matched
                        seed/order -- the reproduction gate).
  method = <name>     : a pre-period-OUTCOMES-only forecaster from forecasters.py that DROPS delta
                        (base member_effect = 1 everywhere, latent refit at exposure=1 = simple pre
                        mean) and sets each post-period latent mean to the forecast.
  method = "table:<f>": inject a precomputed per-repo post-period forecast table (results/<f>.parquet
                        with columns repo_name, quasi_event_time, forecast) -- for cross-fitted
                        methods (trajectory bins, Chamberlain-Mundlak) whose fit uses actual post data.

pi^o = 1 exactly for every repo, so E[opened_k] = latent mean = forecast_k (no calibration). The
treated arm always uses the REAL no-reaction counterfactual (predict_model.CounterfactualStageProbs);
the latent change is orthogonal to it. Writes raw_draws.parquet (pipeline schema) under
issue/improve_latent/draws/<tag>/<sample>/.

Usage: python build_draws.py <method> [n_draws=100] [sample=exact1] [tag=<method>]
"""
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import Parallel, delayed

sys.path.insert(0, str(Path(__file__).resolve().parent))
import forecasters
from source.lib.python.config_loaders import LoadMeanReversionAdj
from source.lib.python.repo_utils import MakeRepoNameSafe
from source.lib.model.staged_count_model import ComputeStageProbabilities, MeanReversionAdj, FitLatentDistribution
from source.analysis.model_prediction.predict_model import DrawByMultiplier, BuildRawDrawRows, CounterfactualStageProbs

BASE_MEMBER = Path("drive/output/derived/model_prediction/event_time_member_panel")
BASE_CROSS  = Path("drive/output/derived/model_prediction/event_time_member_crossmerge")
FITTED      = Path("output/analysis/model_prediction")
ANALYSIS    = Path("output/derived/analysis_panel")
RESULTS     = Path("issue/improve_latent/results")
OUTROOT     = Path("issue/improve_latent/draws")

VARIANT, DIST, ESTIM = "opened_cohort", "adaptive", "pooled"
IMPORTANCE, CONTROL, ROLLING = "important_degree_top3", "nevertreated", "rolling5"
PRE_COUNT = 5
N_JOBS = 8


def DistMean(repo_distribution, dist_params):
    if repo_distribution == "poisson":
        return float(dist_params["poisson_rate"])
    return float(dist_params["negative_binomial_size"]
                 * (1.0 - dist_params["negative_binomial_prob"]) / dist_params["negative_binomial_prob"])


def ProcessRepo(repo_name, is_treated, dropout_set, df_dist_repo, df_member_probs,
                qualified_sample, mean_reversion_adj_table, num_important_qualified_by_period,
                method, n_draws, pooled_params, forecast_table, nb_alpha=None):
    if df_dist_repo.empty:
        return None
    safe = MakeRepoNameSafe(repo_name)
    member_path = BASE_MEMBER / VARIANT / IMPORTANCE / qualified_sample / CONTROL / f"{safe}.parquet"
    if not member_path.exists():
        return None
    df_member = pd.read_parquet(member_path)
    cross_path = BASE_CROSS / VARIANT / IMPORTANCE / qualified_sample / CONTROL / f"{safe}.parquet"
    df_crossmerge = pd.read_parquet(cross_path) if cross_path.exists() else None

    df_repo_counts = df_member.groupby("quasi_event_time")[
        ["repo_pull_request_opened", "repo_pull_request_reviewed",
         "repo_pull_request_merged_direct", "repo_pull_request_merged_after_review"]].first().reset_index()
    all_periods = sorted(int(k) for k in df_repo_counts["quasi_event_time"])
    pre_periods = [k for k in all_periods if -PRE_COUNT <= k < 0]
    if not pre_periods:
        return None
    treatment_periods = [k for k in all_periods if k == 0]
    post_periods      = [k for k in all_periods if 1 <= k <= PRE_COUNT]
    pre_counts_df = df_repo_counts[df_repo_counts["quasi_event_time"].isin(pre_periods)].sort_values("quasi_event_time")

    if method == "baseline":
        member_effect = {k: 1.0 / MeanReversionAdj(mean_reversion_adj_table, num_important_qualified_by_period.get(k, 0))
                         for k in all_periods}
        dist_row = df_dist_repo.iloc[0]
        repo_distribution = dist_row["distribution_type"]
        dist_params = {"poisson_rate": dist_row["poisson_rate"],
                       "negative_binomial_size": dist_row["negative_binomial_size"],
                       "negative_binomial_prob": dist_row["negative_binomial_prob"]}
    else:
        member_effect = {k: 1.0 for k in all_periods}   # drop delta
        counts_pre = pre_counts_df["repo_pull_request_opened"].to_numpy(float)
        dist = FitLatentDistribution(repo_name, counts_pre, np.ones(len(counts_pre)), "adaptive")
        repo_distribution = dist["distribution_type"]
        dist_params = {"poisson_rate": dist["poisson_rate"],
                       "negative_binomial_size": dist["negative_binomial_size"],
                       "negative_binomial_prob": dist["negative_binomial_prob"]}
        if post_periods and not method.startswith("nbglm:"):
            dist_mean = DistMean(repo_distribution, dist_params)
            if dist_mean > 0:
                if method.startswith("table:"):
                    repo_forecast = forecast_table.get(repo_name, {})
                    for k in sorted(post_periods):
                        if k in repo_forecast:
                            member_effect[k] = float(repo_forecast[k]) / dist_mean
                else:
                    y_pre = pre_counts_df["repo_pull_request_opened"].to_numpy(float)
                    if len(y_pre) >= 2:
                        forecast = forecasters.METHODS[method](y_pre, len(post_periods), pooled_params)
                        for i, k in enumerate(sorted(post_periods)):
                            member_effect[k] = float(forecast[i]) / dist_mean

    full_stage_probs = ComputeStageProbabilities(df_member_probs)
    if is_treated:
        surviving_member_probs = df_member_probs[~df_member_probs["actor_id"].isin(dropout_set)]
        counterfactual_probs = CounterfactualStageProbs(
            surviving_member_probs, df_member, df_crossmerge, dropout_set, pre_periods)
        post_stage_probs = ComputeStageProbabilities(counterfactual_probs)
    else:
        post_stage_probs = full_stage_probs

    rng = np.random.default_rng(int(hashlib.md5(repo_name.encode()).hexdigest()[:8], 16))
    drawn = pre_periods + treatment_periods + post_periods
    pre_block       = DrawByMultiplier(repo_distribution, dist_params, full_stage_probs, pre_periods, member_effect, n_draws, rng)
    treatment_block = DrawByMultiplier(repo_distribution, dist_params, full_stage_probs, treatment_periods, member_effect, n_draws, rng)
    if method.startswith("nbglm:"):
        # Full NB2 post-period draw: mean mu_k from the GLM table, per-repo dispersion alpha (Var = mu +
        # alpha*mu^2) mapped to negative_binomial(size=1/alpha, prob=1/(1+alpha*mu_k)); alpha<=0 => Poisson
        # (the two-part model draws Poisson for equidispersed orgs, NB for overdispersed).
        repo_forecast = forecast_table.get(repo_name, {})
        post_block = {}
        for k in post_periods:
            mu_k = float(repo_forecast.get(k, DistMean(repo_distribution, dist_params)))
            if nb_alpha is None or nb_alpha <= 1e-6:
                pois_params = {"poisson_rate": max(mu_k, 1e-9), "negative_binomial_size": np.nan, "negative_binomial_prob": np.nan}
                post_block.update(DrawByMultiplier("poisson", pois_params, post_stage_probs, [k], {k: 1.0}, n_draws, rng))
            else:
                nb_params = {"poisson_rate": np.nan, "negative_binomial_size": 1.0 / nb_alpha,
                             "negative_binomial_prob": 1.0 / (1.0 + nb_alpha * max(mu_k, 1e-9))}
                post_block.update(DrawByMultiplier("negative_binomial", nb_params, post_stage_probs, [k], {k: 1.0}, n_draws, rng))
    else:
        post_block = DrawByMultiplier(repo_distribution, dist_params, post_stage_probs, post_periods, member_effect, n_draws, rng)
    period_draws = {**pre_block, **treatment_block, **post_block}
    return BuildRawDrawRows(repo_name, is_treated, drawn, period_draws)


def LoadPreSeriesByRepo(df_all_repos, qualified_sample):
    pre_series = {}
    for repo_name in df_all_repos["repo_name"]:
        member_path = BASE_MEMBER / VARIANT / IMPORTANCE / qualified_sample / CONTROL / f"{MakeRepoNameSafe(repo_name)}.parquet"
        if not member_path.exists():
            continue
        df_member = pd.read_parquet(member_path, columns=["quasi_event_time", "repo_pull_request_opened"])
        counts = df_member.groupby("quasi_event_time")["repo_pull_request_opened"].first()
        pre = counts[(counts.index >= -PRE_COUNT) & (counts.index < 0)].sort_index()
        if len(pre) >= 2:
            pre_series[repo_name] = pre.to_numpy(float)
    return pre_series


def BuildForSample(qualified_sample, method, n_draws, tag):
    fitted_dir = FITTED / VARIANT / DIST / "parameters" / ESTIM / IMPORTANCE / qualified_sample / CONTROL
    df_dist  = pd.read_parquet(fitted_dir / "distribution_params.parquet")
    df_probs = pd.read_parquet(fitted_dir / "member_probabilities.parquet")
    mean_reversion_adj_table = LoadMeanReversionAdj(IMPORTANCE, CONTROL)

    panel_path = ANALYSIS / IMPORTANCE / ROLLING / qualified_sample / CONTROL / "panel.parquet"
    df_panel = pd.read_parquet(panel_path)
    df_all_repos = df_panel[df_panel["quasi_event_time"] == 0][["repo_name", "dropouts_actors", "num_dropouts"]]
    num_important_qualified_by_repo = {
        repo_name: dict(zip(group["quasi_event_time"].astype(int), group["num_important_qualified"].astype(int)))
        for repo_name, group in df_panel.groupby("repo_name")
    }

    pooled_params, forecast_table, nb_alpha_by_repo = {}, {}, {}
    if method not in ("baseline",) and not method.startswith(("table:", "nbglm:")):
        pre_series = LoadPreSeriesByRepo(df_all_repos, qualified_sample)
        pooled_params = forecasters.FitPooledParameters(pre_series)
    if method.startswith(("table:", "nbglm:")):
        table_path = RESULTS / f"{method.split(':', 1)[1]}.parquet"
        forecast_df = pd.read_parquet(table_path)
        forecast_table = {
            repo_name: dict(zip(group["quasi_event_time"].astype(int), group["forecast"].astype(float)))
            for repo_name, group in forecast_df.groupby("repo_name")
        }
        if method.startswith("nbglm:"):
            nb_alpha_by_repo = forecast_df.groupby("repo_name")["nb_alpha"].first().to_dict()

    repo_results = Parallel(n_jobs=N_JOBS)(
        delayed(ProcessRepo)(
            row["repo_name"], row["num_dropouts"] > 0,
            set(json.loads(row["dropouts_actors"])) if row["num_dropouts"] > 0 else set(),
            df_dist[df_dist["repo_name"] == row["repo_name"]],
            df_probs[df_probs["repo_name"] == row["repo_name"]],
            qualified_sample, mean_reversion_adj_table,
            num_important_qualified_by_repo.get(row["repo_name"], {}),
            method, n_draws, pooled_params, forecast_table, nb_alpha_by_repo.get(row["repo_name"]),
        )
        for _, row in df_all_repos.iterrows()
    )
    raw_draws = pd.concat([r for r in repo_results if r is not None], ignore_index=True)
    outdir = OUTROOT / tag / qualified_sample
    outdir.mkdir(parents=True, exist_ok=True)
    raw_draws.to_parquet(outdir / "raw_draws.parquet")
    print(f"wrote {outdir/'raw_draws.parquet'}  repos={raw_draws['repo_name'].nunique()}  "
          f"rows={len(raw_draws)}  draws={n_draws}  method={method}")


def main():
    method   = sys.argv[1] if len(sys.argv) > 1 else "baseline"
    n_draws  = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    sample   = sys.argv[3] if len(sys.argv) > 3 else "exact1"
    tag      = next((a.split("=", 1)[1] for a in sys.argv[4:] if a.startswith("tag=")), method.replace(":", "_"))
    BuildForSample(sample, method, n_draws, tag)


if __name__ == "__main__":
    main()
