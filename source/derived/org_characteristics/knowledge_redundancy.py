from pathlib import Path
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from source.lib.helpers import ImputeTimePeriod
import time
import random
import torch
from transformers import AutoTokenizer, AutoModel

device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")

TIME_PERIOD = 6

INDIR = Path("drive/output/derived/repo_level_data/repo_actions")
INDIR_GRAPH = Path("drive/output/derived/graph_structure/interactions")
INDIR_TEXT = Path("drive/output/derived/org_characteristics/cleaned_text")
OUTDIR_AGG_MINILM = Path("drive/output/derived/org_characteristics/repo_knowledge_redundancy/minilm")
OUTDIR_AGG_MPNET = Path("drive/output/derived/org_characteristics/repo_knowledge_redundancy/mpnet")
OUTDIR_TEXT_MINILM  = Path("drive/output/derived/org_characteristics/text_variance/minilm")
OUTDIR_TEXT_MPNET = Path("drive/output/derived/org_characteristics/text_variance/mpnet")

def ProcessRepo(
    repo_name,
    INDIR=INDIR,
    INDIR_GRAPH=INDIR_GRAPH,
    INDIR_TEXT=INDIR_TEXT,
    OUTDIR_AGG_MINILM=OUTDIR_AGG_MINILM,
    OUTDIR_AGG_MPNET=OUTDIR_AGG_MPNET,
    OUTDIR_TEXT_MINILM=OUTDIR_TEXT_MINILM,
    OUTDIR_TEXT_MPNET=OUTDIR_TEXT_MPNET
):
    infile = INDIR / f"{repo_name}.parquet"
    infile_graph = INDIR_GRAPH / f"{repo_name}.parquet"
    infile_cleaned_text = INDIR_TEXT / f"{repo_name}.parquet"

    MODEL_ID = "sentence-transformers/all-MiniLM-L12-v2"
    #MODEL_ID = "sentence-transformers/all-mpnet-base-v2"
    OUTDIR_TEXT = OUTDIR_TEXT_MINILM if MODEL_ID == "sentence-transformers/all-MiniLM-L12-v2" else OUTDIR_TEXT_MPNET
    OUTDIR_AGG =  OUTDIR_AGG_MINILM if MODEL_ID == "sentence-transformers/all-MiniLM-L12-v2" else OUTDIR_AGG_MPNET

    OUTDIR_AGG.mkdir(parents=True, exist_ok=True)
    OUTDIR_TEXT.mkdir(parents=True, exist_ok=True)

    outfile_text = OUTDIR_TEXT / f"{repo_name}.parquet"
    outfile_agg = OUTDIR_AGG / f"{repo_name}.parquet"

    if outfile_agg.exists() and outfile_text.exists():
        print(f"Skipping {repo_name}, outputs already exist.")
        return

    if not infile.exists():
        print(f"⚠️ Skipping {repo_name}, missing input file: {infile}")
        return
    if not infile_graph.exists():
        print(f"⚠️ Skipping {repo_name}, missing graph file: {infile_graph}")
        return
    if not infile_cleaned_text.exists():
        print(f"⚠️ Skipping {repo_name}, missing cleaned text file: {infile_cleaned_text}")
        return

    print(f"Processing {repo_name}...")

    df_all = pd.read_parquet(infile)
    df_text = pd.read_parquet(infile_cleaned_text)

    df_all = ImputeTimePeriod(df_all, TIME_PERIOD)
    df_all["type_broad"] = df_all["type"].apply(
        lambda x: "pull request review" 
        if x.startswith("pull request review") and x != "pull request review comment" 
        else x
    )
    df_all["type_issue_pr"] = df_all["type"].apply(
        lambda x: "issue" if x.startswith("issue") else "pull request"
    )

    df_problem_member_stats = CalculateMemberStatsPerProblem(df_all)
    df_project_hhi = CalculateProjectHHI(df_all)
    df_project_problem_hhi = CalculateProjectProblemHHI(df_all)
    df_issue_pr_split = ActorTypeMix(df_all)
    df_avg_type = AverageTypeCount(df_all)
    df_pr_merge_stats = PercentPullsMergedReviewed(df_all)
    avg_pr_counts = CalculateAvgPRDiscCounts(df_all)

    # Text redundancy
    df_docs = CreateDocs(df_text)

    tokenizer, model = LoadEmbeddingModel(MODEL_ID)

    df_text_dispersion_results = ComputeAuthorSimilarityByTimePeriod(
        df_docs,
        tokenizer,
        model,
        n_samples=1000
    )
    if 'same_author_pairs' in df_text_dispersion_results.columns and 'diff_author_pairs' in df_text_dispersion_results.columns:
        df_text_dispersion_results.to_parquet(OUTDIR_TEXT / f"{repo_name}.parquet")
        df_text_dispersion_results = df_text_dispersion_results.drop(columns=['same_author_pairs', 'diff_author_pairs'])
    else:
        df_text_dispersion_results = pd.DataFrame()
    df_combined_knowledge_redundancy = ConcatStatsByTimePeriod(
        df_problem_member_stats,
        df_project_hhi,
        df_project_problem_hhi,
        df_issue_pr_split,
        df_avg_type,
        df_pr_merge_stats,
        avg_pr_counts,
        df_text_dispersion_results
    )
    
    df_combined_knowledge_redundancy.to_parquet(outfile_agg)

def ProcessAllRepos(n_jobs=4):
    repo_files = [f.stem for f in INDIR.glob("*.parquet")]
    random.shuffle(repo_files)
    Parallel(n_jobs=n_jobs)(
        delayed(ProcessRepo)(repo_name) for repo_name in repo_files
    )


### ---- Analysis functions ----

def IndividualActivityShares(df, actor_col='actor_id', time_col='time_period', type_col='type_broad'):
    actor_counts = df.groupby([time_col, actor_col, type_col]).size().rename('count')
    type_totals = df.groupby([time_col, type_col]).size().rename('total_count')
    return (
        actor_counts
        .reset_index()
        .merge(type_totals.reset_index(), on=[time_col, type_col])
        .assign(share=lambda d: d['count'] / d['total_count'])
    )

def CalculateProjectHHI(df_all):
    df_all_not_reopened = df_all[~df_all['type_broad'].str.endswith('reopened')]
    df_indiv_activity_share = IndividualActivityShares(df_all_not_reopened)

    df_all_comments = df_all[df_all['type_broad'].str.endswith('comment')].assign()
    df_all_comments['type_comments'] = 'discussion comment'
    df_indiv_activity_share = pd.concat([
        df_indiv_activity_share,
        IndividualActivityShares(df_all_comments, type_col = 'type_comments').rename(
            columns={'type_comments':'type_broad'}
        )
    ])
    df_indiv_activity_share['share_sq'] = df_indiv_activity_share['share']**2
    df_project_hhi = df_indiv_activity_share.groupby(['time_period','type_broad'])['share_sq'].sum()

    return (
        df_project_hhi.reset_index(name = 'hhi_project')
        .pivot(index="time_period", columns="type_broad", values="hhi_project")
        .rename(columns=lambda c: f"proj_hhi_{c.replace(' ','_')}")
        .reset_index()
    )

def IndividualProblemActivityShares(df, actor_col='actor_id', problem_col='thread_number', project_col='repo_name', type_col='type_broad', time_col = 'time_period'):
    actor_counts = df.groupby([project_col, problem_col, time_col, actor_col, type_col]).size().rename('count')
    type_totals = df.groupby([project_col, problem_col, time_col, type_col]).size().rename('total_count')
    return (
        actor_counts
        .reset_index()
        .merge(type_totals.reset_index(), on=[project_col, problem_col, type_col, time_col])
        .assign(share=lambda d: d['count'] / d['total_count'])
    )

def CalculateProjectProblemHHI(df_all):
    df_all_disc = df_all[(df_all['type_broad'].str.endswith('comment')) |
        (df_all['type_broad'].str.endswith('review'))]
    df_indiv_prob_activity_share = IndividualProblemActivityShares(df_all_disc)
    df_all_comments = df_all[df_all['type_broad'].str.endswith('comment')].assign()
    df_all_comments['type_comments'] = 'discussion comment'

    df_indiv_prob_activity_share = pd.concat([
        df_indiv_prob_activity_share,
        IndividualProblemActivityShares(df_all_comments, type_col='type_comments')
    ])
    df_project_problem_hhi = (
        df_indiv_prob_activity_share
        .assign(share_sq=lambda d: d['share']**2)
        .groupby(['repo_name','time_period','thread_number','type_broad'])
        .agg(problem_hhi=('share_sq','sum'), total_count=('total_count','first'))
        .groupby(['repo_name','time_period','type_broad'])
        .apply(lambda g: pd.Series({
            "hhi_project_problem": np.average(g["problem_hhi"], weights=g["total_count"])
        }))
        .reset_index()
    )
    if df_project_problem_hhi.empty:
        return pd.DataFrame()
    return (
        df_project_problem_hhi
        .pivot(index="time_period", columns="type_broad", values="hhi_project_problem")
        .rename(columns=lambda c: f"proj_prob_hhi_{c.replace(' ','_')}")
        .reset_index()
    )

def ActorTypeMix(df):
    actor_types = (
        df.groupby(['repo_name', 'time_period', 'actor_id'])['type_issue_pr']
        .unique()
        .apply(lambda x: 'issue_only' if set(x) == {'issue'}
               else 'pr_only' if set(x) == {'pull request'}
               else 'both')
    )
    shares = (
        actor_types.reset_index(name='category')
        .groupby(['repo_name', 'time_period', 'category'])
        .size()
        .groupby(level=[0,1])
        .apply(lambda g: g / g.sum())
    )
    return (
        shares.droplevel([0,1])
        .reset_index(name='share')
        .pivot(index="time_period", columns="category", values="share")
        .rename(columns=lambda c: f"share_{c}")
        .reset_index()
    )

def AverageTypeCount(df_all):
    return (
        df_all.groupby(['repo_name', 'time_period', 'actor_id'])['type_broad']
        .nunique()  
        .groupby(['repo_name', 'time_period'])
        .mean()   
        .reset_index(name='avg_unique_types')
    )

def PercentPullsMergedReviewed(df_all):
    merged = df_all[df_all['type'] == "pull request merged"][
        ['repo_name','thread_number','time_period']].drop_duplicates()
    indicators = (
        df_all[df_all['type'].eq("pull request review approved") | df_all['type_broad'].eq("pull request review")]
        .assign(has_approved=lambda d: d['type'].eq("pull request review approved"),
                has_review=lambda d: d['type_broad'].eq("pull request review"))
        .groupby(['repo_name','thread_number','time_period'])[['has_approved','has_review']]
        .max()
        .reset_index()
    )
    percents = (
        merged.merge(indicators, on=['repo_name','thread_number','time_period'], how='left')
        .fillna(False)
        .groupby('time_period')
        .agg(n_merged=('thread_number','nunique'),
             n_with_approved=('has_approved','sum'),
             n_with_review=('has_review','sum'))
        .assign(pct_with_approved=lambda d: d['n_with_approved'] / d['n_merged'],
                pct_with_review=lambda d: d['n_with_review'] / d['n_merged'])
        .reset_index()
        .drop(columns=['n_merged','n_with_approved','n_with_review'])
    )
    return percents

def CalculateAvgPRDiscCounts(df_all):
    avg_pr_counts = (
        df_all[df_all['type_broad'].isin(['pull request comment','pull request review comment','pull request review'])]
        .groupby(['thread_number','time_period','type_broad'])
        .size()
        .unstack(fill_value=0)
        .assign(all_disc=lambda d: d.sum(axis=1))
        .groupby('time_period')
        .mean()
        .reset_index()
        .rename(columns={
            'pull request comment': 'avg_pull_request_comment_count',
            'pull request review comment': 'avg_pull_request_review_comment_count',
            'pull request review': 'avg_pull_request_review_count',
            'all_disc': 'avg_all_disc_count'
        })
    )
    return avg_pr_counts

def CalculateMemberStatsPerProblem(df_all):
    df_problem = df_all.groupby(['thread_number', 'time_period'], as_index=False).agg(
        avg_members_per_problem=('actor_id', 'nunique')
    )
    df_problem['pct_members_multiple'] = (df_problem['avg_members_per_problem'] > 1).astype(int)
    return df_problem.groupby(['time_period'], as_index = False).agg({
        'avg_members_per_problem': 'mean',
        'pct_members_multiple': 'mean'
    })

def CreateDocs(df_text):
    df_docs = (
        df_text
        .drop_duplicates(subset=["thread_number", "time_period", "actor_id", "cleaned_text"])
        .dropna(subset=["thread_number", "time_period", "actor_id", "cleaned_text"])
        .groupby(["thread_number", "actor_id", "time_period"])
        .agg(doc_text=("cleaned_text", list), comment_count=("cleaned_text", "count"))
        .reset_index()
    )
    df_docs['doc_text'] = df_docs['doc_text'].apply(lambda x: " ".join(x))
    return df_docs

def LoadEmbeddingModel(model_id="sentence-transformers/all-MiniLM-L6-v2"):
    tokenizer = AutoTokenizer.from_pretrained(model_id)
    model = AutoModel.from_pretrained(model_id).to(device)
    model.eval()
    return tokenizer, model

def GeneratePairsAndWeights(
    subset,
    weights,
    allow_self_pairs=False,
    max_same_pairs=1_000_000,
    max_diff_pairs=1_000_000,
    rng=None
):
    """
    Generate actor pairs with weights, with separate thresholds for same-actor
    and different-actor pairs.
    
    - If total same-actor pairs <= max_same_pairs → build all (exact).
    - Else → sample uniformly at random.
    - Same for different-actor pairs with max_diff_pairs.
    """
    n = len(subset)
    actor_ids = subset["actor_id"].values

    if rng is None:
        rng = np.random.default_rng()

    if n < 2:
        return np.empty((0, 2), int), np.array([]), np.empty((0, 2), int), np.array([])

    # --- Step 1: Build or sample candidate pairs ---
    I, J = np.triu_indices(n, k=0 if allow_self_pairs else 1)

    # Compute pair-level info
    pair_weights = 0.5 * (weights.values[I] + weights.values[J])
    same_mask = actor_ids[I] == actor_ids[J]

    same_pairs = np.column_stack([I[same_mask], J[same_mask]])
    same_weights = pair_weights[same_mask]

    diff_pairs = np.column_stack([I[~same_mask], J[~same_mask]])
    diff_weights = pair_weights[~same_mask]

    # --- Step 2: Apply thresholds separately ---
    if len(same_pairs) > max_same_pairs:
        idx = rng.choice(len(same_pairs), size=max_same_pairs, replace=False)
        same_pairs, same_weights = same_pairs[idx], same_weights[idx]

    if len(diff_pairs) > max_diff_pairs:
        idx = rng.choice(len(diff_pairs), size=max_diff_pairs, replace=False)
        diff_pairs, diff_weights = diff_pairs[idx], diff_weights[idx]

    return same_pairs, same_weights, diff_pairs, diff_weights

def AvgSimilarity(
    pairs, pair_weights, rng, n_samples, with_replacement,
    subset, embed_cache, tokenizer, model,
    label, time_period, batch_size=32
):
    if len(pairs) == 0:
        return np.nan, []

    probs = pair_weights / pair_weights.sum()
    chosen_idx = rng.choice(
        len(pairs),
        size=min(n_samples, len(pairs)),
        replace=with_replacement,
        p=probs
    )
    chosen_pairs = pairs[chosen_idx]

    unique_texts = {subset.loc[i, "doc_text"] for i, j in chosen_pairs} | {
        subset.loc[j, "doc_text"] for i, j in chosen_pairs
    }
    new_texts = [t for t in unique_texts if t not in embed_cache]

    for start in range(0, len(new_texts), batch_size):
        batch = new_texts[start:start + batch_size]
        enc = tokenizer(batch, padding=True, truncation=True, return_tensors="pt").to(device)

        with torch.no_grad():
            outputs = model(**enc)
            vecs = outputs.last_hidden_state.mean(dim=1)  # mean pooling
            vecs = vecs.cpu().numpy()

        for t, v in zip(batch, vecs):
            embed_cache[t] = v

    sims, records = [], []
    for i, j in chosen_pairs:
        v1 = embed_cache[subset.loc[i, "doc_text"]]
        v2 = embed_cache[subset.loc[j, "doc_text"]]
        sim = np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))
        sim = (sim + 1) / 2
        sims.append(sim)
        records.append({
            "time_period": time_period,
            "label": label,
            "i": int(i),
            "j": int(j),
            "actor_i": subset.loc[i, "actor_id"],
            "actor_j": subset.loc[j, "actor_id"],
            "similarity": float(sim)
        })

    return np.mean(sims), records


def ComputeAuthorSimilarityByTimePeriod(
    df_docs,
    tokenizer,
    model,
    n_samples=100,
    random_state=42,
    allow_self_pairs=False,
    with_replacement=True,
    batch_size=32
):
    rng = np.random.default_rng(random_state)
    results = []

    for time_period, subset in df_docs.groupby("time_period"):
        subset = subset.reset_index(drop=True)

        if len(subset) < 2:
            results.append({
                "time_period": time_period,
                "AvgSame": np.nan,
                "AvgDiff": np.nan,
                "R": np.nan,
                "pairs_used": []
            })
            continue

        embed_cache = {}
        total_comments = subset["comment_count"].sum()
        weights = subset["comment_count"] / total_comments

        same_pairs, same_weights, diff_pairs, diff_weights = GeneratePairsAndWeights(
            subset, weights, allow_self_pairs
        )

        avg_same, same_records = AvgSimilarity(
            same_pairs, same_weights, rng, n_samples, with_replacement,
            subset, embed_cache, tokenizer, model,
            label="same", time_period=time_period, batch_size=batch_size
        )
        avg_diff, diff_records = AvgSimilarity(
            diff_pairs, diff_weights, rng, n_samples, with_replacement,
            subset, embed_cache, tokenizer, model,
            label="diff", time_period=time_period, batch_size=batch_size
        )

        ratio = avg_same / avg_diff if (avg_diff not in (0, np.nan)) else np.nan


        results.append({
            "time_period": time_period,
            "cos_sim_same_actor": avg_same,
            "cos_sim_diff_actor": avg_diff,
            "text_sim_ratio": ratio,
            "same_author_pairs": same_records,
            "diff_author_pairs": diff_records
        })

    return pd.DataFrame(results).sort_values("time_period").reset_index(drop=True)


def ConcatStatsByTimePeriod(*dfs):
    dfs_with_index = [df.set_index("time_period") for df in dfs if not df.empty]
    return pd.concat(dfs_with_index, axis=1)


def Main():
    ProcessAllRepos(n_jobs=2)


if __name__ == "__main__":
    Main()
