"""
Text-similarity knowledge redundancy metrics, computed from mpnet embeddings.
Companion to text_variance.py — consumes its output and produces per-repo rolling stats.
"""
import random
from pathlib import Path
import pandas as pd
from joblib import Parallel, delayed
from source.lib.helpers import LoadGlobals

_globals        = LoadGlobals("source/lib/globals.json")
ROLLING_PERIODS = _globals["rolling_periods"]
N_JOBS          = _globals["n_jobs"]

INDIR_TEXT = Path("drive/output/derived/problem_level_data/text_variance/mpnet")
OUTDIR     = Path("drive/output/derived/org_characteristics/repo_knowledge_redundancy/mpnet/text_similarity")


def Main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    repo_files = [f.stem for f in INDIR_TEXT.glob("*.parquet")]
    random.shuffle(repo_files)
    Parallel(n_jobs=N_JOBS)(delayed(ProcessRepo)(repo_name) for repo_name in repo_files)


def ProcessRepo(repo_name):
    infile  = INDIR_TEXT / f"{repo_name}.parquet"
    outfile = OUTDIR / f"{repo_name}.parquet"

    if not infile.exists():
        return

    df = pd.read_parquet(infile).drop(columns=["same_author_pairs", "diff_author_pairs"], errors="ignore")
    df_rolling = (
        df.set_index("time_period")[["cos_sim_same_actor", "cos_sim_diff_actor", "text_sim_ratio"]]
        .rolling(ROLLING_PERIODS, min_periods=1).mean()
        .reset_index()
    )
    df_rolling.to_parquet(outfile)


if __name__ == "__main__":
    Main()
