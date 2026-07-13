"""Build the enriched pre-period-OUTCOME feature set for issue/improve_latent iteration 2, from the
opened-cohort member panel (the org's own staged-count outcomes + member composition). All features are
pre-period [-5,-1] only. Caches to results/enriched_features.parquet (per repo) and the post-period
opened targets to results/enriched_post.parquet (repo x horizon). Reused by fit_enriched.py.

Feature families (each with an economic microfoundation):
  A opened series        : log_last, log_mean, norm_slope, log_sd
  B downstream levels    : log_rev_mean, log_merg_mean, conv_merg (merged/opened), conv_rev (reviewed/opened)
  C backlog/congestion   : backlog_ratio = (opened-merged)/opened ; gap_growth = (open_slope-merg_slope)/mean
  D member composition   : log_n_openers, hhi_open (concentration), top_open_share
  E permanent extraction : log_permanent = mean(log1p of opened/reviewed/merged pre-means) ; depeak = log_last - log_permanent
"""
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import theilslopes

from source.lib.python.repo_utils import MakeRepoNameSafe

RES = Path("issue/improve_latent/results")
MP = Path("drive/output/derived/model_prediction/event_time_member_panel/opened_cohort/important_degree_top3/exact1/nevertreated")
PRE = list(range(-5, 0))
POST = list(range(1, 6))


def RepoFeatures(df):
    g = df.groupby("quasi_event_time").first(numeric_only=True)
    g["merged"] = g["repo_pull_request_merged_direct"] + g["repo_pull_request_merged_after_review"]
    pre = g[(g.index >= -5) & (g.index < 0)].sort_index()
    if len(pre) < 2 or pre["repo_pull_request_opened"].mean() <= 0:
        return None
    open_pre = pre["repo_pull_request_opened"].to_numpy(float)
    rev_pre = pre["repo_pull_request_reviewed"].to_numpy(float)
    merg_pre = pre["merged"].to_numpy(float)
    t = np.arange(len(open_pre), dtype=float)
    open_mean, open_last = open_pre.mean(), open_pre[-1]
    open_slope = np.polyfit(t, open_pre, 1)[0]
    merg_slope = np.polyfit(t, merg_pre, 1)[0]
    # member composition of OPENING over the pre-window (most relevant to pull opening)
    pre_members = df[df["quasi_event_time"].isin(PRE)]
    opens_by_actor = pre_members.groupby("actor_id")["member_pull_request_opened"].sum()
    opens_by_actor = opens_by_actor[opens_by_actor > 0]
    total_open = float(opens_by_actor.sum())
    shares = (opens_by_actor / total_open).to_numpy() if total_open > 0 else np.array([1.0])
    log_perm = np.mean([np.log1p(open_mean), np.log1p(rev_pre.mean()), np.log1p(merg_pre.mean())])
    theil_slope = float(theilslopes(open_pre, np.arange(len(open_pre)))[0]) if len(open_pre) >= 2 else 0.0
    opened_by_k = pre["repo_pull_request_opened"]   # indexed by event time in [-5,-1]
    lags = {}
    for lag in range(1, 6):   # y_{-1}..y_{-5}
        v = opened_by_k.get(-lag, np.nan)
        lags[f"log_open_m{lag}"] = float(np.log1p(v)) if np.isfinite(v) else float(np.log1p(open_mean))
    recent3 = [opened_by_k.get(-l, np.nan) for l in (1, 2, 3)]
    recent3 = [v for v in recent3 if np.isfinite(v)]
    mean3 = float(np.mean(recent3)) if recent3 else open_mean
    open_sd = float(open_pre.std(ddof=1)) if len(open_pre) > 1 else 0.0
    return {
        # A
        "log_last": np.log1p(open_last), "log_mean": np.log1p(open_mean),
        "norm_slope": open_slope / open_mean, "log_sd": np.log1p(open_pre.std(ddof=1) if len(open_pre) > 1 else 0.0),
        # B
        "log_rev_mean": np.log1p(rev_pre.mean()), "log_merg_mean": np.log1p(merg_pre.mean()),
        "conv_merg": merg_pre.mean() / open_mean, "conv_rev": rev_pre.mean() / open_mean,
        # C
        "backlog_ratio": np.clip((open_mean - merg_pre.mean()) / open_mean, -1, 1),
        "gap_growth": (open_slope - merg_slope) / open_mean,
        # D
        "log_n_openers": np.log1p(len(opens_by_actor)), "hhi_open": float((shares ** 2).sum()),
        "top_open_share": float(shares.max()),
        # E
        "log_permanent": float(log_perm), "depeak": float(np.log1p(open_last) - log_perm),
        # robust trend (Theil-Sen slope, normalized by pre-mean) for trend-stratification
        "theil_norm_slope": theil_slope / open_mean,
        # lags and recent-3 mean anchors (Q2/Q3)
        **lags, "log_mean3": float(np.log1p(mean3)),
        # normalized last value: ratio to pre-mean, and z-score by pre-sd
        "last_over_mean": open_last / open_mean,
        "last_z": (open_last - open_mean) / open_sd if open_sd > 1e-9 else 0.0,
        # raw levels needed downstream
        "open_mean": open_mean, "open_last": open_last,
    }


def Build():
    trim = pd.read_csv(RES / "trimmed_repos_exact1.csv")
    feat_rows, post_rows = [], []
    for _, r in trim.iterrows():
        repo = r["repo_name"]
        f = MP / f"{MakeRepoNameSafe(repo)}.parquet"
        if not f.exists():
            continue
        df = pd.read_parquet(f)
        feats = RepoFeatures(df)
        if feats is None:
            continue
        feats.update({"repo_name": repo, "is_treated": bool(r["is_treated"])})
        feat_rows.append(feats)
        g = df.groupby("quasi_event_time")["repo_pull_request_opened"].first()
        for k in POST:
            if k in g.index and np.isfinite(g[k]):
                post_rows.append({"repo_name": repo, "horizon": k, "post": float(g[k])})
    feat = pd.DataFrame(feat_rows)
    post = pd.DataFrame(post_rows)
    feat.to_parquet(RES / "enriched_features.parquet")
    post.to_parquet(RES / "enriched_post.parquet")
    print(f"features: {feat.shape} ({feat['is_treated'].sum()} treated); post rows: {len(post)}")
    return feat, post


if __name__ == "__main__":
    Build()
