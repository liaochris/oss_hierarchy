
import re
import time
import numpy as np
import pandas as pd
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from umap import UMAP
from pathlib import Path
import re
import pandas as pd

# --- Precompiled regex patterns for speed ---
RE_PATTERNS = [
    # Code blocks & inline code
    (re.compile(r"```.*?```", re.DOTALL), " "),           
    (re.compile(r"`[^`]+`"), " "),                        
    (re.compile(r"^(?:\s{4,}|\t+).*$", re.MULTILINE), " "),  
    (re.compile(r"^\s*[\w\s<>,\[\]\(\)\"']+\s*[{;}]\s*$", re.MULTILINE), " "),  
    
    # Markdown links [foo](url)
    (re.compile(r"\[.*?\]\(.*?\)", re.M), " "),           

    # URLs, emails, mentions, issue refs
    (re.compile(r"http\S+"), " "),                        
    (re.compile(r"\S+@\S+"), " "),                        
    (re.compile(r"@[A-Za-z0-9_.-]+"), " "),               
    (re.compile(r"#\d+"), " "),                           

    # Boilerplate / artifacts
    (re.compile(r"(Reply to this email directly|view it on GitHub)", re.I), " "),
    (re.compile(r"(Sent from my .*phone)", re.I), " "),
    
    # HTML / markdown artifacts
    (re.compile(r"<[^>]+>"), " "),                        
    (re.compile(r"&[a-z]+;"), " "),                        
    (re.compile(r"[*_~]+"), " "),                         

    # Emoji shortcodes :smile: :ok hand:
    (re.compile(r":[a-z0-9_+\- ]+:", re.I), " "),         

    # Versions / paths
    (re.compile(r"[A-Za-z]:\\[^\s]+"), " "),              
    (re.compile(r"v?\d+\.\d+(\.\d+)*"), " "),             

    # Email headers
    (re.compile(r"^(From|Sent|To|Cc|Subject):.*$", re.I | re.MULTILINE), " "),

    # Orphan brackets and punctuation
    (re.compile(r"\s+[)\]}]+"), " "),   
    (re.compile(r"[(\[{]+\s+"), " "),   
    (re.compile(r"^\W+|\W+$"), " ")
]


def CleanTextColumn(df, text_column="text"):
    df = df.copy()
    df[text_column] = df[text_column].fillna("").astype(str)

    def clean_github_issue(text: str) -> str:
        # Run regex substitutions
        for pattern, repl in RE_PATTERNS:
            text = pattern.sub(repl, text)

        # Drop quoted lines ("> …")
        text = "\n".join(
            [line for line in text.splitlines() if not line.strip().startswith(">")]
        )

        # Normalize whitespace
        text = text.replace("\n", " ").replace("\r", " ")
        text = re.sub(r"\s+", " ", text)

        return text.strip()

    # Clean text
    df["cleaned_text"] = df[text_column].map(clean_github_issue)

    # Drop rows where cleaned_text is empty/whitespace
    df = df[df["cleaned_text"].str.strip().astype(bool)].reset_index(drop=True)

    return df

def ChunkAndEmbed(text, model, chunk_size=None, batch_size=32):
    if chunk_size is None:
        chunk_size = model.get_max_seq_length()

    tokens = model.tokenizer.encode(text, add_special_tokens=False)
    if not tokens:
        return None, 0

    # Split into chunks
    chunks = [tokens[i:i+chunk_size] for i in range(0, len(tokens), chunk_size)]
    chunk_texts = [model.tokenizer.decode(chunk) for chunk in chunks]

    # Embed chunks
    embeddings = model.encode(
        chunk_texts,
        batch_size=batch_size,
        show_progress_bar=False
    )

    # Weighted average by token length
    lengths = np.array([len(chunk) for chunk in chunks])
    weights = lengths / lengths.sum()
    pooled_embedding = np.average(embeddings, axis=0, weights=weights)

    return pooled_embedding, len(tokens)


def PoolByGroup(df, model, group_col="thread_number", chunk_size=None, batch_size=32):
    if chunk_size is None:
        chunk_size = model.get_max_seq_length()

    group_embeddings = {}
    for group_id, group in tqdm(df.groupby(group_col), desc=f"Processing {group_col}s"):
        doc_embeddings, doc_weights = [], []
        for text in group["cleaned_text"]:
            emb, length = ChunkAndEmbed(text, model, chunk_size, batch_size)
            if emb is not None:
                doc_embeddings.append(emb)
                doc_weights.append(length)
        if doc_embeddings:
            doc_embeddings = np.vstack(doc_embeddings)
            doc_weights = np.array(doc_weights) / np.sum(doc_weights)
            pooled_embedding = np.average(doc_embeddings, axis=0, weights=doc_weights)
            group_embeddings[group_id] = pooled_embedding
    return group_embeddings

from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer
import time

def RunBERTopic(texts, embeddings=None, embedding_model=None,
                top_n_words=10, stop_words="english",
                ngram_range=(1, 3)):
    start = time.time()

    vectorizer_model = CountVectorizer(stop_words=stop_words, ngram_range=ngram_range)
    topic_model = BERTopic(
        embedding_model=embedding_model,
        vectorizer_model=vectorizer_model,
        umap_model=None,   # disable UMAP since you’re passing embeddings
        verbose=False
    )

    topics, probs = topic_model.fit_transform(texts, embeddings=embeddings)
    elapsed = time.time() - start

    topics_out = {}
    for topic_id in set(topics):
        if topic_id == -1:
            continue
        topic_words = [
            word for word, _ in topic_model.get_topic(topic_id)[:top_n_words]
            if len(word) >= 5
        ]
        topics_out[topic_id] = topic_words

    return topic_model, elapsed, topics_out

# ----------------------------
# Benchmarking HDP vs HDBSCAN vs BERTopic
# ----------------------------
def ReduceEmbeddings(embeddings, random_state=42):
    reducer = UMAP(metric="cosine", random_state=random_state, n_components = 10)
    return reducer.fit_transform(embeddings)

def BenchmarkUnsupervised(df_text_cleaned, embedding_model, group_col="thread_number", top_n_words=10):
    group_embeddings = PoolByGroup(
        df_text_cleaned,
        embedding_model,
        group_col=group_col,
        chunk_size=256,
        batch_size=64
    )

    embeddings = np.vstack(list(group_embeddings.values()))
    texts = df_text_cleaned.groupby(group_col)["cleaned_text"].apply(lambda x: " ".join(x)).loc[
        group_embeddings.keys()].tolist()

    # BERTopic (already does its own reduction internally)
    bertopic_model, bertopic_time, _ = RunBERTopic(
        texts, embeddings = embeddings, top_n_words=top_n_words,
    )
    df_topics = bertopic_model.get_document_info(texts)
    print(bertopic_time)
    bertopic_model_reduced = bertopic_model.reduce_topics(texts, nr_topics=5)
    df_reduced_topics = bertopic_model_reduced.get_document_info(texts)
    
    return bertopic_model, bertopic_model_reduced, df_topics, df_reduced_topics

# ----------------------------
# Example run
# ----------------------------
INDIR = Path('drive/output/derived/repo_level_data/repo_actions')

df_all = pd.read_parquet(INDIR / 'dotnet_roslyn.parquet')

embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

df_text = (
    df_all.sort_values(['thread_number', 'created_at'])
    [['thread_number', 'actor_id', 'text']]
    .dropna()
)

df_text_cleaned = CleanTextColumn(df_text, text_column="text")
df_text_cleaned = df_text_cleaned[df_text_cleaned["cleaned_text"].str.strip().astype(bool)]
df_text_cleaned = df_text_cleaned[df_text_cleaned['cleaned_text'].apply(lambda x: len(x.split(" "))>10)]

bertopic_model, bertopic_model_reduced, df_topics, df_reduced_topics = BenchmarkUnsupervised(
    df_text_cleaned,
    embedding_model,
    group_col="thread_number",
    top_n_words=10
)

