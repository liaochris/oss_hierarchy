import os
import re
import pandas as pd
from pandarallel import pandarallel
from pathlib import Path
from source.lib.helpers import ImputeTimePeriod
import random
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
pandarallel.initialize(progress_bar = True)

RE_PATTERNS = [
    # Code blocks / inline code
    (re.compile(r"```.*?```", re.DOTALL), " "),
    (re.compile(r"`[^`]+`"), " "),

    # Indented code/log lines
    (re.compile(r"^(?:\s{4,}|\t+).*$", re.MULTILINE), " "),
    (re.compile(r"\b(?:INFO|ERROR|WARN|DEBUG|TRACE)\b.*", re.IGNORECASE), " "),  # logs

    # Markdown links / raw URLs
    (re.compile(r"\[.*?\]\(.*?\)", re.M), "[URL]"),
    (re.compile(r"http\S+"), "[URL]"),

    # Emails / mentions / issues
    (re.compile(r"\S+@\S+"), "[EMAIL]"),
    (re.compile(r"@[A-Za-z0-9_.-]+"), "[USER]"),
    (re.compile(r"#\d+"), "[ISSUE]"),

    # HTML / entities
    (re.compile(r"<[^>]+>"), " "),
    (re.compile(r"&[a-z]+;"), " "),

    # Markdown decorations
    (re.compile(r"[*_~]+"), " "),
    (re.compile(r":([a-z0-9_+\- ]+):", re.I), r"[EMOJI:\1]"),

    # Windows file paths
    (re.compile(r"[A-Za-z]:\\[^\s]+"), "[PATH]"),

    # Version numbers / commit hashes
    (re.compile(r"v?\d+\.\d+(\.\d+)*"), "[VERSION]"),
    (re.compile(r"\b[0-9a-f]{7,40}\b"), "[HASH]"),

    # Email headers
    (re.compile(r"^(From|Sent|To|Cc|Subject):.*$", re.I | re.MULTILINE), " "),

    # Extraneous brackets
    (re.compile(r"\s+[)\]}]+"), " "),
    (re.compile(r"[(\[{]+\s+"), " "),

    # Strip leading/trailing non-word chars
    (re.compile(r"^\W+|\W+$"), " ")
]


def NormalizeTokens(text: str) -> str:
    # Fix dangling half-tokens
    token_map = {
        "URL": "[URL]",
        "VERSION": "[VERSION]",
        "USER": "[USER]",
        "EMAIL": "[EMAIL]",
        "ISSUE": "[ISSUE]",
        "HASH": "[HASH]",
        "PATH": "[PATH]",
    }
    for token, full in token_map.items():
        text = re.sub(rf"\b{token}\]", full, text)

    # Collapse consecutive tokens ([URL] [URL] -> [URL])
    text = re.sub(r"(\[[A-Z]+\])(?:\s+\1)+", r"\1", text)

    return text


def CleanTextColumn(df, text_column, parallel_threshold=100_000):
    df = df.copy()
    df[text_column] = df[text_column].fillna("").astype(str)

    def clean_text(text: str) -> str:
        for pattern, repl in RE_PATTERNS:
            text = pattern.sub(repl, text)
        text = "\n".join(
            [line for line in text.splitlines() if not line.strip().startswith(">")]
        )
        text = text.replace("\n", " ").replace("\r", " ")
        text = re.sub(r"\s+", " ", text)
        text = NormalizeTokens(text)
        return text.strip()

    if len(df) >= parallel_threshold:
        pandarallel.initialize(progress_bar=True, verbose=0)
        df["cleaned_text"] = df[text_column].parallel_map(clean_text)
    else:
        df["cleaned_text"] = df[text_column].map(clean_text)

    return df

def AddVaderSentiment(df_text, text_col='cleaned_text', parallel_threshold=10000):
    sia = SentimentIntensityAnalyzer()

    def analyze_text(text):
        if not isinstance(text, str) or not text.strip():
            return {"pos": 0.0, "neu": 0.0, "neg": 0.0, "compound": 0.0}

        sentences = [s.strip() for s in re.split(r"[.!?]+", text) if s.strip()]
        scores = [sia.polarity_scores(s) for s in sentences]

        n = len(scores)
        if n == 0:
            return {"pos": 0.0, "neu": 0.0, "neg": 0.0, "compound": 0.0}

        return {
            "pos": sum(s['pos'] for s in scores) / n,
            "neu": sum(s['neu'] for s in scores) / n,
            "neg": sum(s['neg'] for s in scores) / n,
            "compound": sum(s['compound'] for s in scores) / n,
        }

    df_text = df_text.copy()

    if len(df_text) > parallel_threshold and hasattr(df_text[text_col], "parallel_apply"):
        results = df_text[text_col].parallel_apply(analyze_text)
    else:
        results = df_text[text_col].apply(analyze_text)

    sentiment_df = pd.DataFrame(results.tolist(), index=df_text.index)
    return pd.concat([df_text, sentiment_df], axis=1)


def Main():

    INPUT_INDIR = Path("drive/output/derived/problem_level_data/repo_actions")
    OUTPUT_DIR = Path("drive/output/derived/problem_level_data/cleaned_text")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    projects = [f for f in os.listdir(INPUT_INDIR) if f.endswith(".parquet")]
    random.shuffle(projects)
    for project_file in projects:
        project = Path(project_file).stem
        print(f"\n=== Processing project: {project} ===")
        parquet_file = INPUT_INDIR / project_file

        if not parquet_file.exists():
            print(f"⚠️ Skipping {project}, file not found: {parquet_file}")
            continue

        if (OUTPUT_DIR / f"{project}.parquet").exists():
            print(f"⚠️ Skipping {project}, already exists")
            continue
            
        df_all = pd.read_parquet(parquet_file)
        df_all = ImputeTimePeriod(df_all, 6)

        df_text = (
            df_all.sort_values(["thread_number", "created_at"])
            [["thread_number", "time_period", "actor_id", "text","action_id","labels","assignees","type"]]
            .query('~text.isna()')
        )
        print(f"🧹 Cleaning text for {project}...")
        df_text_cleaned = CleanTextColumn(df_text, "text")
        df_text_cleaned = AddVaderSentiment(df_text_cleaned)

        df_text_cleaned.to_parquet(OUTPUT_DIR / f"{project}.parquet")


if __name__ == "__main__":
    Main()
