#!/usr/bin/env python3
import json
import os
from pathlib import Path
from google.cloud import bigquery
from source.lib.JMSLab.SaveData import SaveData
from source.lib.helpers import LoadGlobals

def ExecuteQuery(query):
    client = bigquery.Client()
    query_job = client.query(query)
    results_df = query_job.to_dataframe()
    return results_df


def ExportToCsv(dataframe):
    dataframe = dataframe[~dataframe["project"].isna()]

    output_folder = Path("drive/output/scrape/pypi_downloads")
    log_folder = Path("output/scrape/pypi_downloads")

    SaveData(
        dataframe,
        ["project", "month"],
        output_folder / "pypi_monthly_downloads.parquet",
        log_folder / "pypi_monthly_downloads.log",
    )

def Main():
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("Need to set up GOOGLE_APPLICATION_CREDENTIALS environment variable")
        return

    globals_data = LoadGlobals("source/lib/globals.json")
    start_date = globals_data["pip_downloads_start_date"]
    end_date = globals_data["pip_downloads_end_date"]

    query_monthly_downloads = f"""
    SELECT
      file.project AS project,
      COUNT(*) AS num_downloads,
      DATE_TRUNC(DATE(timestamp), MONTH) AS month
    FROM `bigquery-public-data.pypi.file_downloads` AS file
    WHERE DATE(timestamp)
      BETWEEN DATE('{start_date}') AND DATE('{end_date}')
    GROUP BY month, project
    ORDER BY month DESC
    """
    results_monthly_downloads = ExecuteQuery(query_monthly_downloads)

    ExportToCsv(results_monthly_downloads)


if __name__ == "__main__":
    Main()
