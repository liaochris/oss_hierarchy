import re
import time
import numpy as np
import pandas as pd
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from umap import UMAP
from pathlib import Path
from sklearn.feature_extraction.text import CountVectorizer

# ----------------------------
# FUNCTIONS
# ----------------------------
RE_PATTERNS = [
    (re.compile(r"```.*?```", re.DOTALL), " "),
    (re.compile(r"`[^`]+`"), " "),
    (re.compile(r"^(?:\s{4,}|\t+).*$", re.MULTILINE), " "),
    (re.compile(r"^\s*[\w\s<>,\[\]\(\)\"']+\s*[{;}]\s*$", re.MULTILINE), " "),
    (re.compile(r"\[.*?\]\(.*?\)", re.M), " "),
    (re.compile(r"http\S+"), " "),
    (re.compile(r"\S+@\S+"), " "),
    (re.compile(r"@[A-Za-z0-9_.-]+"), " "),
    (re.compile(r"#\d+"), " "),
    (re.compile(r"(Reply to this email directly|view it on GitHub)", re.I), " "),
    (re.compile(r"(Sent from my .*phone)", re.I), " "),
    (re.compile(r"<[^>]+>"), " "),
    (re.compile(r"&[a-z]+;"), " "),
    (re.compile(r"[*_~]+"), " "),
    (re.compile(r":[a-z0-9_+\- ]+:", re.I), " "),
    (re.compile(r"[A-Za-z]:\\[^\s]+"), " "),
    (re.compile(r"v?\d+\.\d+(\.\d+)*"), " "),
    (re.compile(r"^(From|Sent|To|Cc|Subject):.*$", re.I | re.MULTILINE), " "),
    (re.compile(r"\s+[)\]}]+"), " "),
    (re.compile(r"[(\[{]+\s+"), " "),
    (re.compile(r"^\W+|\W+$"), " ")
]


def CleanTextColumn(df, text_column):
    df = df.copy()
    df[text_column] = df[text_column].fillna("").astype(str)

    def clean_text(text: str) -> str:
        for pattern, repl in RE_PATTERNS:
            text = pattern.sub(repl, text)
        text = "\n".join([line for line in text.splitlines() if not line.strip().startswith(">")])
        text = text.replace("\n", " ").replace("\r", " ")
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    df["cleaned_text"] = df[text_column].map(clean_text)
    df = df[df["cleaned_text"].str.strip().astype(bool)].reset_index(drop=True)
    return df


def ChunkAndEmbed(text, model):
    tokens = model.tokenizer.encode(text, add_special_tokens=False)
    if not tokens:
        return None, 0

    chunks = [tokens[i:i + CHUNK_SIZE] for i in range(0, len(tokens), CHUNK_SIZE)]
    chunk_texts = [model.tokenizer.decode(chunk) for chunk in chunks]

    embeddings = model.encode(chunk_texts, batch_size=BATCH_SIZE, show_progress_bar=False)
    lengths = np.array([len(chunk) for chunk in chunks])
    weights = lengths / lengths.sum()
    pooled_embedding = np.average(embeddings, axis=0, weights=weights)

    return pooled_embedding, len(tokens)


def PoolByGroup(df, model, group_col):
    group_embeddings = {}
    for group_id, group in tqdm(df.groupby(group_col), desc=f"Processing {group_col}s"):
        doc_embeddings, doc_weights = [], []
        total_tokens = 0
        for text in group["cleaned_text"]:
            emb, length = ChunkAndEmbed(text, model)
            if emb is not None:
                doc_embeddings.append(emb)
                doc_weights.append(length)
                total_tokens += length
        if doc_embeddings and total_tokens >= MIN_TOKENS_PER_GROUP:
            doc_embeddings = np.vstack(doc_embeddings)
            doc_weights = np.array(doc_weights) / np.sum(doc_weights)
            pooled_embedding = np.average(doc_embeddings, axis=0, weights=doc_weights)
            group_embeddings[group_id] = pooled_embedding
    return group_embeddings


def RunBERTopic(texts, embeddings, embedding_model):
    vectorizer_model = CountVectorizer(stop_words=STOP_WORDS, ngram_range=NGRAM_RANGE)
    topic_model = BERTopic(
        embedding_model=embedding_model,
        vectorizer_model=vectorizer_model,
        umap_model=None,
        verbose=False
    )
    start = time.time()
    topic_model.fit_transform(texts, embeddings=embeddings)
    elapsed = time.time() - start
    return topic_model, elapsed


def BenchmarkUnsupervised(df_text_cleaned, embedding_model, group_col):
    group_embeddings = PoolByGroup(df_text_cleaned, embedding_model, group_col=group_col)
    embeddings = np.vstack(list(group_embeddings.values()))
    texts = df_text_cleaned.groupby(group_col)["cleaned_text"].apply(lambda x: " ".join(x)).loc[
        group_embeddings.keys()].tolist()

    bertopic_model, elapsed = RunBERTopic(texts, embeddings=embeddings, embedding_model=embedding_model)
    df_topics = bertopic_model.get_document_info(texts)

    bertopic_model_reduced = bertopic_model.reduce_topics(texts, nr_topics=NR_TOPICS_REDUCED)
    df_reduced_topics = bertopic_model_reduced.get_document_info(texts)

    df_topics[group_col] = list(group_embeddings.keys())
    df_reduced_topics[group_col] = list(group_embeddings.keys())

    return {
        "model_full": bertopic_model,
        "model_reduced": bertopic_model_reduced,
        "df_topics_full": df_topics,
        "df_topics_reduced": df_reduced_topics,
        "runtime_sec": elapsed
    }

# ----------------------------
# GLOBAL PARAMETERS + MAIN CONFIG
# ----------------------------
INPUT_INDIR = Path("drive/output/derived/problem_level_data/repo_actions")
ANALYSIS_INDIR  = Path("output/derived/graph_structure")

EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"

GROUP_COLS = ["thread_number", "actor_id"]

# Embedding & chunking
CHUNK_SIZE = 256
BATCH_SIZE = 64
MIN_TOKENS_PER_GROUP = 10

# BERTopic
TOP_N_WORDS = 20
STOP_WORDS = "english"
NGRAM_RANGE = (1, 3)
NR_TOPICS_REDUCED = 20

# UMAP (if used)
N_COMPONENTS = 25
RANDOM_STATE = 42

# Output paths
OUTPUT_DIR = Path("drive/output/derived/problem_level_data/topic_classification")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def Main():
    projects_with_important = (
        pd.read_csv(ANALYSIS_INDIR / "analysis_summary.csv")
        .query("num_dropouts==1")["file"].unique()
    )
    projects = [project.replace(".parquet", "") for project in projects_with_important]
    embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)

    for project in projects:
        print(f"\n=== Processing project: {project} ===")
        parquet_file = INPUT_INDIR / f"{project}.parquet"

        if not parquet_file.exists():
            print(f"⚠️ Skipping {project}, file not found: {parquet_file}")
            continue

        df_all = pd.read_parquet(parquet_file)

        df_text = (
            df_all.sort_values(["thread_number", "created_at"])
            [["thread_number", "actor_id", "text"]]
            .dropna()
        )
        df_text_cleaned = CleanTextColumn(df_text, "text")

        for group_col in GROUP_COLS:
            results = BenchmarkUnsupervised(df_text_cleaned, embedding_model, group_col=group_col)

            results["df_topics_full"].to_parquet(
                OUTPUT_DIR / f"{project}_bertopic_{group_col}_full.parquet"
            )
            results["df_topics_reduced"].to_parquet(
                OUTPUT_DIR / f"{project}_bertopic_{group_col}_reduced.parquet"
            )

            results["model_full"].save(
                OUTPUT_DIR / f"{project}_bertopic_model_{group_col}_full"
            )
            results["model_reduced"].save(
                OUTPUT_DIR / f"{project}_bertopic_model_{group_col}_reduced


if __name__ == "__main__":
    Main()
