# TEMPORARY SCRIPT — delete after running once to backfill SaveData logs.
# Reads all existing parquets in link_issue_pull_request, adds event_order
# to timeline files (missing pre-migration), and resaves with SaveData logs.
# Does NOT re-run any GitHub scraping.
import glob
from pathlib import Path

import numpy as np
import pandas as pd

from source.lib.python.data_utils import JsonSerialize
from source.lib.JMSLab.SaveData import SaveData

DRIVE_DIR = Path("drive/output/scrape/link_issue_pull_request")
LOG_DIR   = Path("output/scrape/link_issue_pull_request")

SUBDIRS = {
    "linked_issue_to_pull_request": ["repo_name", "issue_number"],
    "linked_pull_request_to_issue": ["repo_name", "pr_number"],
    "timeline":                     ["repo_name", "issue_number", "event_order"],
}

for subdir, keys in SUBDIRS.items():
    data_dir = DRIVE_DIR / subdir
    log_dir  = LOG_DIR / subdir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = sorted(glob.glob(str(data_dir / "*.parquet")))
    print(f"\n{subdir}: {len(parquet_files)} files")

    for f in parquet_files:
        df = pd.read_parquet(f)
        if df.empty:
            print(f"  SKIP (empty): {Path(f).name}")
            continue

        if subdir == "timeline":
            df["event_order"] = (
                df.groupby(["repo_name", "issue_number"]).cumcount() + 1
            )

        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (dict, list, np.ndarray))).any():
                df[col] = df[col].apply(JsonSerialize)

        n_before = len(df)
        df = df.drop_duplicates()
        if len(df) < n_before:
            print(f"    dropped {n_before - len(df)} duplicate rows")

        stem = Path(f).stem
        safe_repo = stem.removesuffix("_issue_timeline") if subdir == "timeline" else stem
        repo_log = log_dir / f"{safe_repo}.log"

        print(f"  {Path(f).name}")
        SaveData(df, keys, f, repo_log, append=False)
