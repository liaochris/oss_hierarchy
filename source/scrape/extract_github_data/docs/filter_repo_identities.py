import pandas as pd
import numpy as np
from pathlib import Path
from source.lib.JMSLab.SaveData import SaveData


def ToUnixSeconds(series):
    dt_series = pd.to_datetime(series)
    if dt_series.dtype.tz is not None:
        dt_series = dt_series.dt.tz_convert("UTC").dt.tz_localize(None)
    return dt_series.astype("datetime64[s]").to_numpy().astype(np.int64)


def FindBestChainWeighted(df_group):
    df_temp = df_group.copy()
    df_temp["orig_index"] = df_temp.index
    durations = (
        (df_temp["last_seen"] - df_temp["first_seen"]).dt.total_seconds().to_numpy()
    )

    # Sort rows by last_seen
    sort_order = np.argsort(df_temp["last_seen"].to_numpy())
    df_sorted = df_temp.iloc[sort_order].reset_index(drop=True)
    durations = durations[sort_order]
    first_sec = ToUnixSeconds(df_sorted["first_seen"])
    last_sec = ToUnixSeconds(df_sorted["last_seen"])

    n = len(df_sorted)
    if n == 0:
        return df_group.iloc[[]]

    # Vectorized candidate predecessor indices; ensure that each candidate index is less than i
    candidate_p = np.searchsorted(last_sec, first_sec, side="right") - 1
    indices = np.arange(n)
    p = np.where(candidate_p < indices, candidate_p, -1)

    dp = np.empty(n)
    dp[0] = durations[0]
    for i in range(1, n):
        dp[i] = max(dp[i - 1], durations[i] + (dp[p[i]] if p[i] != -1 else 0))

    chain_indices = []
    i = n - 1
    while i >= 0:
        if i == 0 or dp[i] > dp[i - 1]:
            chain_indices.append(i)
            i = p[i] if p[i] != -1 else -1
        else:
            i -= 1
    chain_indices = sorted(chain_indices)
    best_chain = df_sorted.loc[chain_indices].sort_values(by="first_seen")
    orig_indices = best_chain["orig_index"].to_numpy()
    return df_group.loc[orig_indices].sort_values(by="first_seen")


def FilterByBestChain(df, group_col):
    groups = [FindBestChainWeighted(group_df) for _, group_df in df.groupby(group_col)]
    return pd.concat(groups).sort_index()


def Main():
    indir = Path("output/scrape/extract_github_data")
    df_full = pd.read_csv(indir / "repo_id_history.csv")
    df_full["first_seen"] = pd.to_datetime(df_full["first_seen"])
    df_full["last_seen"] = pd.to_datetime(df_full["last_seen"])

    df_filtered = FilterByBestChain(df_full, "repo_id")

    SaveData(
        df_filtered,
        ["repo_group", "first_seen", "last_seen", "repo_id", "repo_name"],
        indir / "repo_id_history_filtered.csv",
        indir / "repo_id_history_filtered.log",
    )


if __name__ == "__main__":
    Main()
