from pathlib import Path
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
import torch
from transformers import AutoTokenizer, AutoModel

device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")

INDIR_TEXT = Path("drive/output/derived/problem_level_data/cleaned_text")
OUTDIR_TEXT_MINILM = Path("drive/output/derived/problem_level_data/text_variance/minilm")
OUTDIR_TEXT_MPNET = Path("drive/output/derived/problem_level_data/text_variance/mpnet")


def Main():
    ProcessAllReposTextDispersion(n_jobs=2)

def ProcessAllReposTextDispersion(n_jobs):
    repo_files = [f.stem for f in INDIR_TEXT.glob("*.parquet")]
    Parallel(n_jobs=n_jobs)(delayed(ProcessRepoTextDispersion)(repo_name) for repo_name in repo_files)


def ProcessRepoTextDispersion(repo_name):
    infile_cleaned_text = INDIR_TEXT / f"{repo_name}.parquet"

    #MODEL_ID = "sentence-transformers/all-MiniLM-L12-v2"
    MODEL_ID = "sentence-transformers/all-mpnet-base-v2"
    OUTDIR_TEXT = OUTDIR_TEXT_MINILM if MODEL_ID == "sentence-transformers/all-MiniLM-L12-v2" else OUTDIR_TEXT_MPNET
    OUTDIR_TEXT.mkdir(parents=True, exist_ok=True)

    outfile_text = OUTDIR_TEXT / f"{repo_name}.parquet"
    if outfile_text.exists():
        print(f"Skipping {repo_name}, output already exists.")
        return

    if not infile_cleaned_text.exists():
        print(f"⚠️ Skipping {repo_name}, missing cleaned text file.")
        return

    print(f"Processing text dispersion for {repo_name}...")

    df_text = pd.read_parquet(infile_cleaned_text)
    df_docs = CreateDocs(df_text)

    tokenizer, model = LoadEmbeddingModel(MODEL_ID)
    df_text_dispersion_results = ComputeAuthorSimilarityByTimePeriod(df_docs, tokenizer, model, n_samples=100)

    if {'same_author_pairs', 'diff_author_pairs'}.issubset(df_text_dispersion_results.columns):
        df_text_dispersion_results.to_parquet(outfile_text)
    else:
        print(f"⚠️ Empty text dispersion calculations for {repo_name}...")

def CreateDocs(df_text):
    df_docs = (
        df_text.drop_duplicates(subset=["thread_number", "time_period", "actor_id", "cleaned_text"])
        .dropna(subset=["thread_number", "time_period", "actor_id", "cleaned_text"])
        .groupby(["thread_number", "actor_id", "time_period"])
        .agg(doc_text=("cleaned_text", list), comment_count=("cleaned_text", "count"))
        .reset_index()
    )
    df_docs['doc_text'] = df_docs['doc_text'].apply(lambda x: " ".join(x))
    return df_docs


def LoadEmbeddingModel(model_id):
    tokenizer = AutoTokenizer.from_pretrained(model_id)
    model = AutoModel.from_pretrained(model_id).to(device)
    model.eval()
    return tokenizer, model


def GeneratePairsAndWeights(subset, weights, allow_self_pairs=False, max_same_pairs=1_000_000_000_000, max_diff_pairs=1_000_000_000_000, rng=None):
    n = len(subset)
    if rng is None:
        rng = np.random.default_rng()
    if n < 2:
        return np.empty((0, 2), int), np.array([]), np.empty((0, 2), int), np.array([])

    actor_ids, (I, J) = subset["actor_id"].values, np.triu_indices(n, k=0 if allow_self_pairs else 1)
    pair_weights, same_mask = 0.5 * (weights.values[I] + weights.values[J]), actor_ids[I] == actor_ids[J]

    same_pairs, same_weights = np.column_stack([I[same_mask], J[same_mask]]), pair_weights[same_mask]
    diff_pairs, diff_weights = np.column_stack([I[~same_mask], J[~same_mask]]), pair_weights[~same_mask]

    if len(same_pairs) > max_same_pairs:
        idx = np.random.default_rng().choice(len(same_pairs), size=max_same_pairs, replace=False)
        same_pairs, same_weights = same_pairs[idx], same_weights[idx]
    if len(diff_pairs) > max_diff_pairs:
        idx = np.random.default_rng().choice(len(diff_pairs), size=max_diff_pairs, replace=False)
        diff_pairs, diff_weights = diff_pairs[idx], diff_weights[idx]

    return same_pairs, same_weights, diff_pairs, diff_weights


def AvgSimilarity(pairs, pair_weights, rng, n_samples, with_replacement, subset, embed_cache, tokenizer, model, label, time_period, batch_size=32):
    if len(pairs) == 0:
        return np.nan, []

    probs = pair_weights / pair_weights.sum()
    chosen_pairs = pairs[rng.choice(len(pairs), size=min(n_samples, len(pairs)), replace=with_replacement, p=probs)]

    unique_texts = {subset.loc[i, "doc_text"] for i, j in chosen_pairs} | {subset.loc[j, "doc_text"] for i, j in chosen_pairs}
    new_texts = [t for t in unique_texts if t not in embed_cache]

    for start in range(0, len(new_texts), batch_size):
        batch = new_texts[start:start + batch_size]
        enc = tokenizer(batch, padding=True, truncation=True, return_tensors="pt").to(device)
        with torch.no_grad():
            vecs = model(**enc).last_hidden_state.mean(dim=1).cpu().numpy()
        for t, v in zip(batch, vecs):
            embed_cache[t] = v

    sims, records = [], []
    for i, j in chosen_pairs:
        v1, v2 = embed_cache[subset.loc[i, "doc_text"]], embed_cache[subset.loc[j, "doc_text"]]
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


def ComputeAuthorSimilarityByTimePeriod(df_docs, tokenizer, model, n_samples=100, random_state=42, allow_self_pairs=False, with_replacement=True, batch_size=32):
    rng, results = np.random.default_rng(random_state), []
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

        embed_cache, total_comments = {}, subset["comment_count"].sum()
        weights = subset["comment_count"] / total_comments
        same_pairs, same_weights, diff_pairs, diff_weights = GeneratePairsAndWeights(subset, weights, allow_self_pairs)

        avg_same, same_records = AvgSimilarity(same_pairs, same_weights, rng, n_samples, with_replacement, subset, embed_cache, tokenizer, model, "same", time_period, batch_size)
        avg_diff, diff_records = AvgSimilarity(diff_pairs, diff_weights, rng, n_samples, with_replacement, subset, embed_cache, tokenizer, model, "diff", time_period, batch_size)
        ratio = avg_same / avg_diff if (avg_diff not in (0, np.nan)) else np.nan

        results.append({
            "time_period": time_period,
            "cos_sim_same_actor": avg_same,
            "cos_sim_diff_actor": avg_diff,
            "text_sim_ratio": ratio,
            "same_author_pairs": same_records,
            "diff_author_pairs": diff_records
        })
    df_final = pd.DataFrame(results)
    if 'time_period' in df_final.columns:
        return df_final.sort_values("time_period").reset_index(drop=True)
    return pd.DataFrame()


if __name__ == "__main__":
    Main()
