import os
import re
import pandas as pd
from pandarallel import pandarallel
from pathlib import Path
from source.lib.helpers import ImputeTimePeriod
import random

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
    (re.compile(r":[a-z0-9_+\- ]+:", re.I), " "),  # emojis

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


def normalize_tokens(text: str) -> str:
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
        text = normalize_tokens(text)
        return text.strip()

    if len(df) >= parallel_threshold:
        pandarallel.initialize(progress_bar=True, verbose=0)
        df["cleaned_text"] = df[text_column].parallel_map(clean_text)
    else:
        df["cleaned_text"] = df[text_column].map(clean_text)

    return df


INPUT_INDIR = Path("drive/output/derived/repo_level_data/repo_actions")
OUTPUT_DIR = Path("drive/output/derived/repo_level_data/cleaned_text")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def Main():
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
            [["thread_number", "time_period", "actor_id", "text","action_id","labels","assignees"]]
            .query('~text.isna()')
        )
        print(f"🧹 Cleaning text for {project}...")
        df_text_cleaned = CleanTextColumn(df_text, "text")
        df_text_cleaned.to_parquet(OUTPUT_DIR / f"{project}.parquet")


if __name__ == "__main__":
    Main()
