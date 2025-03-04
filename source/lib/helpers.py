import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import datetime

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


def ImputeTimePeriod(df, time_period_months):
    t = time_period_months
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['year'] = df['created_at'].dt.year
    df['time_period'] = df['created_at'].apply(lambda x: datetime.date(x.year, int(t*(x.month/t if x.month%t == 0 else np.ceil(x.month/t))-(t-1)), 1))
    # Let t be your time period grouping parameter (e.g., 3 for quarterly).
    # Extract the month as a vector.
    m = df['created_at'].dt.month
    # Compute the starting month of the period:
    # If m is exactly divisible by t, then start = m - (t - 1)
    # Otherwise, start = t * ceil(m / t) - (t - 1)
    month_val = np.where(
        m % t == 0,
        m - (t - 1),
        t * np.ceil(m / t) - (t - 1)
    ).astype(int)
    # Construct the time_period as a date with day=1.
    # We use pd.to_datetime with a dictionary for vectorized construction.
    df['time_period'] = pd.to_datetime(
        dict(year=df['year'], month=month_val, day=1)
    ).dt.date

    df['time_period'] = pd.to_datetime(df['time_period'])
    
    return df 