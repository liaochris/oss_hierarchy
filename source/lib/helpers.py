from collections.abc import Iterable
import polars as pl
import numpy as np
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor

def AddToTableList(table_list, add_list, length):
    table_list = table_list.copy()
    if len(add_list) != length:
        add_list.extend(["" for i in range(length - len(add_list))])
    table_list.append(add_list)

    return table_list


def ExportTable(filename, data, tablename, fmt):
    with open(filename, "w") as f:
        f.write(f'<tab:{tablename}>\n')
        np.savetxt(f, data, fmt=fmt, delimiter = '\t')
        
    print(f"{filename} has been saved!")


def ReadFile(file):
    try:
        if file.endswith('.csv'):
            return pd.read_csv(file)
        if file.endswith('.parquet'):
            return pd.read_parquet(file)
    except:
        print(file)

def ReadFileList(file_list):
    with ThreadPoolExecutor(8) as pool:
        df = pd.concat(pool.map(ReadFile, file_list))
    return df


def ImputeTimePeriod(df: pd.DataFrame, time_period_months: int) -> pd.DataFrame:
    df = df.copy()
    df["created_at"] = pd.to_datetime(df["created_at"])

    # bucket months into 1..(12/t)
    m = df["created_at"].dt.month
    y = df["created_at"].dt.year
    # compute period index (1-based)
    period_idx = np.ceil(m / time_period_months).astype(int)
    # compute starting month of the bucket
    start_month = (period_idx - 1) * time_period_months + 1

    # build the first day of the period
    df["time_period"] = pd.to_datetime(
        dict(year=y, month=start_month, day=1)
    )

    return df

def ImputeTimePeriodPL(lf: pl.LazyFrame, ts_col: str, months: int) -> pl.LazyFrame:
    col = pl.col(ts_col)
    year = col.dt.year()
    month = col.dt.month()
    # which bucket this month belongs to (1-based)
    bucket = ((month - 1) // months) + 1
    start_month = (bucket - 1) * months + 1
    return lf.with_columns(
        pl.datetime(year, start_month, 1).alias("time_period")
    )

def GetLatestRepoName(repo_name, repo_df):
    df_repo_group = repo_df[repo_df['repo_name'] == repo_name]
    if df_repo_group.shape[0] == 0:
        return repo_name
    else:
        repo_group = df_repo_group['repo_group'].values[0]
    repo_data = repo_df[repo_df['repo_group'] == repo_group]
    
    latest_repo = repo_data.loc[repo_data['last_seen'].idxmax()]
    return latest_repo['repo_name'] 

def GetAllRepoNames(repo_name, repo_df):
    df_repo_group = repo_df[repo_df['repo_name'] == repo_name]
    if df_repo_group.shape[0] == 0:
        return repo_name
    else:
        repo_group = df_repo_group['repo_group'].values[0]
    repo_data = repo_df[repo_df['repo_group'] == repo_group]
    
    return repo_data['repo_name'].unique().tolist()

def RemoveDuplicatesFlattened(values):
    flat = (x for v in values for x in (v if isinstance(v, list) else [v]) if pd.notnull(x))
    seen, out = set(), []
    for x in flat:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def WeightedMean(values, weights, zero_weight_return = np.nan):
    if weights.sum() == 0:
        return zero_weight_return
    return (values * weights).sum() / weights.sum()


def ConcatStatsByTimePeriod(*dfs):
    dfs_with_index = [df.set_index("time_period") for df in dfs if not df.empty]
    if len(dfs_with_index)==0:
        return pd.DataFrame
    return pd.concat(dfs_with_index, axis=1)

def LoadFilteredImportantMembers(repo_name, INDIR_IMPORTANT, INDIR_LIB, importance_type):
    importance_parameters_all = json.load(open(INDIR_LIB / "importance.json"))
    if importance_type not in importance_parameters_all:
        raise ValueError(f"importance_type '{importance_type}' not found in importance.json")

    importance_parameters = importance_parameters_all[importance_type]
    df_important_members = pd.read_parquet(INDIR_IMPORTANT / f"{repo_name}.parquet")

    df_filtered_important = df_important_members.copy()
    for col, value in importance_parameters.items():
        if col in df_filtered_important.columns:
            df_filtered_important = df_filtered_important[df_filtered_important[col] == value]

    # Normalize time_period and cast important_actors to ints
    df_filtered_important["time_period"] = (
        pd.to_datetime(df_filtered_important["time_period"], format="%Y-%m")
        .dt.to_period("M")
        .dt.to_timestamp()
    )
    df_filtered_important["important_actors"] = df_filtered_important["important_actors"].apply(lambda x: [int(ele) for ele in x])

    return df_filtered_important

def FilterOnImportant(df, df_filtered_important):
    df = df.copy()
    df = pd.merge(df, df_filtered_important, how = 'left')
    df = df[
        df.apply(
            lambda x: isinstance(x["important_actors"], Iterable) and x["actor_id"] in x["important_actors"],
            axis=1,
        )
    ]
    return df

TIME_PERIOD = 6
def ApplyRolling(df_all, rolling_periods, stat_func, **kwargs):
    results = []
    unique_periods = sorted(df_all['time_period'].unique())
    for t in unique_periods:
        window_start = t - pd.DateOffset(months=(rolling_periods - 1) * TIME_PERIOD)
        df_window = df_all[(df_all['time_period'] >= window_start) & (df_all['time_period'] <= t)]
        if df_window.empty:
            continue
        df_result = stat_func(df_window, **kwargs)
        if not df_result.empty:
            df_result = df_result.assign(time_period=t)
            results.append(df_result)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()
