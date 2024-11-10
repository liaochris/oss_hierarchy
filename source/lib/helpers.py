import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


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