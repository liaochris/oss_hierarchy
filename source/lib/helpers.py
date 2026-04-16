from pathlib import Path
import numpy as np
import pandas as pd
import json


def CleanDirs(dirs, patterns=("*.parquet", "*.log")):
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        for pattern in patterns:
            for f in d.glob(pattern):
                if f.name != "sconscript.log":
                    f.unlink(missing_ok=True)


def ImputeTimePeriod(df, time_period_months):
    df = df.copy()
    df["created_at"] = pd.to_datetime(df["created_at"], format='ISO8601')

    m = df["created_at"].dt.month
    y = df["created_at"].dt.year
    period_idx = np.ceil(m / time_period_months).astype(int)
    start_month = (period_idx - 1) * time_period_months + 1

    df["time_period"] = pd.to_datetime(
        dict(year=y, month=start_month, day=1)
    )

    return df


def LoadGlobals(json_path):
    path = Path(json_path)
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)

def MakeRepoNameSafe(repo_name):
    return repo_name.replace("/", "___")


def MakeRepoNameOriginal(safe_name):
    return safe_name.replace("___", "/")


def JsonSerialize(x):
    if isinstance(x, (dict, list, np.ndarray)):
        return json.dumps(x, default=lambda o: o.tolist() if isinstance(o, np.ndarray) else str(o))
    return x


def JsonDeserialize(x, default=None):
    if isinstance(x, str):
        try:
            return json.loads(x)
        except (json.JSONDecodeError, TypeError):
            return default
    return x
