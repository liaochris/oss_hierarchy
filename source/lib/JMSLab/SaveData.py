# -*- coding: utf-8 -*-

import pandas as pd
import hashlib
import re
import pathlib


def SaveData(df, keys, out_file, log_file = '', append = False, sortbykey = True):
    extension = CheckExtension(out_file)
    CheckKeys(df, keys)
    # reorder df so keys are on the left
    cols_reordered = keys + [col for col in df.columns if col not in keys]
    df = df[cols_reordered]
    summary_stats = GetSummaryStats(df) if log_file else None
    df = SaveDf(df, keys, out_file, sortbykey, extension)
    if log_file:
        df_hash = hashlib.md5(pd.util.hash_pandas_object(df, index=False).values).hexdigest()
        SaveLog(df_hash, keys, summary_stats, out_file, append, log_file)


def CheckExtension(out_file):
    if type(out_file) == str:
        extension = re.findall(r'\.[a-z]+$', out_file)
    elif type(out_file) == pathlib.PosixPath:
        extension = [out_file.suffix]
    else:
        raise ValueError('Output file format must either be string or pathlib.PosixPath')
    if not extension[0] in ['.csv', '.dta', '.parquet']:
        raise ValueError("File extension should be one of .csv, .dta or .parquet.")
    return extension[0]


def CheckKeys(df, keys):
    if not isinstance(keys, list):
        raise TypeError("Keys must be specified as a list.")
    missing_columns = [key for key in keys if key not in df.columns]
    if missing_columns:
        raise ValueError(f'These keys are not among the columns: {", ".join(missing_columns)}.')
    keys_missing_values = [key for key in keys if df[key].isnull().any()]
    if keys_missing_values:
        raise ValueError(f'The following keys are missing in some rows: {", ".join(keys_missing_values)}.')
    list_valued_keys = [key for key in keys
                        if df[key].dtype == object
                        and pd.api.types.infer_dtype(df[key], skipna=True) != "string"
                        and df[key].map(lambda value: type(value) == list).any()]
    if list_valued_keys:
        raise TypeError("No key can contain keys of type list")
    if df.duplicated(subset=keys).any():
        raise ValueError("Keys do not uniquely identify the observations.")


def GetSummaryStats(df):
    var_types = df.dtypes
    with pd.option_context("future.no_silent_downcasting", True):
        var_stats = df.describe(include='all').transpose().fillna('').infer_objects(copy=False)

    var_stats['count'] = df.notnull().sum()
    var_stats = var_stats.drop(columns=['top', 'freq'], errors='ignore')

    summary_stats = pd.DataFrame({'type': var_types}).\
        merge(var_stats, how = 'left', left_index = True, right_index = True)
    summary_stats = summary_stats.round(4)

    return summary_stats


def SaveDf(df, keys, out_file, sortbykey, extension):
    if sortbykey:
        df.sort_values(keys, inplace = True)

    if extension == '.csv':
        df.to_csv(out_file, index = False)
    if extension == '.dta':
        df.to_stata(out_file, write_index = False)
    if extension == '.parquet':
        df.to_parquet(out_file, index = False)

    print(f"File '{out_file}' saved successfully.")
    return df


def SaveLog(df_hash, keys, summary_stats, out_file, append, log_file):
    if not log_file:
        return
    with open(log_file, 'a' if append else 'w') as f:
        if append:
            f.write('\n\n')
        f.write('File: %s\n\n' % (out_file))
        f.write('MD5 hash: %s\n\n' % (df_hash))
        f.write('Keys: ')
        for item in keys:
            f.write('%s ' % (item))
        f.write('\n\n')
        f.write(summary_stats.to_string(header = True, index = True))
        f.write("\n\n")
