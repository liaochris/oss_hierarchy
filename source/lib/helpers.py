import polars as pl
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import datetime
from pathlib import Path

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
