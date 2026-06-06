#!/usr/bin/env python3
import pandas as pd
from datetime import datetime
from source.lib.JMSLab.SaveData import SaveData
from source.lib.python.config_loaders import LoadGlobalSettings


def Main():
    globals_data = LoadGlobalSettings()

    min_downloads = int(globals_data["pip_min_monthly_donwloads"])
    lower_bound = datetime.fromisoformat(globals_data["pip_downloads_start_date"])
    upper_bound = datetime.fromisoformat(globals_data["pip_downloads_end_date"])

    results_monthly_downloads = pd.read_parquet("drive/output/scrape/pypi_downloads/pypi_monthly_downloads.parquet", dtype_backend = 'pyarrow')
    
    results_monthly_downloads["month"] = pd.to_datetime(
        results_monthly_downloads["month"]
    )

    results_monthly_downloads_filt = results_monthly_downloads[
        (results_monthly_downloads["num_downloads"] >= min_downloads)
        & (results_monthly_downloads["month"] > lower_bound)
        & (results_monthly_downloads["month"] < upper_bound)
    ]
    results_monthly_downloads_filt["count"] = (
        results_monthly_downloads_filt.groupby("project")["month"].transform(
            "count"
        )
    )

    downloaded_packages = results_monthly_downloads_filt[
        results_monthly_downloads_filt["count"] >= 1
    ][["project"]].drop_duplicates()

    SaveData(
        downloaded_packages,
        ["project"],
        "output/scrape/pypi_downloads/popular_python_packages.csv",
        "output/scrape/pypi_downloads/popular_python_packages.log",
    )


if __name__ == "__main__":
    Main()
