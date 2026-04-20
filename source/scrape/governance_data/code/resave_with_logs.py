import glob
from pathlib import Path

import pandas as pd

from source.lib.JMSLab.SaveData import SaveData

DRIVE_DIR = Path("drive/output/scrape/governance_data")
LOG_DIR = Path("output/scrape/governance_data/logs")
KEYS = ["repo", "source", "date", "commit", "file_path"]


def Main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    parquet_files = sorted(glob.glob(str(DRIVE_DIR / "*.parquet")))
    print(f"governance_data: {len(parquet_files)} files")

    for f in parquet_files:
        df = pd.read_parquet(f)
        if df.empty:
            print(f"  SKIP (empty): {Path(f).name}")
            continue

        print(f"  {Path(f).name}")
        SaveData(df, KEYS, f, LOG_DIR / f"{Path(f).stem}.log", append=False)


if __name__ == "__main__":
    Main()
