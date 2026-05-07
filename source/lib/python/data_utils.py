import numpy as np
import pandas as pd
import json


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
