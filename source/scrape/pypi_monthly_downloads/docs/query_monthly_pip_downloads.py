#!/usr/bin/env python3
import os
from google.cloud import bigquery
from source.lib.JMSLab.SaveData import SaveData

def ExecuteQuery(query):
    client = bigquery.Client()
    query_job = client.query(query)
    results_df = query_job.to_dataframe()
    return results_df

def ExportToCsv(dataframe):
    dataframe = dataframe[~dataframe['project'].isna()]
    SaveData(dataframe, ['project','month'],
             "drive/output/scrape/pypi_monthly_downloads/pypi_monthly_downloads.csv",
             "output/scrape/pypi_monthly_downloads/pypi_monthly_downloads.log")

def Main():
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ.keys():
        print("Need to set up GOOGLE_APPLICATION_CREDENTIALS environment variable")
        return
    
    query_monthly_downloads = """
    SELECT
      file.project as `project`,
      COUNT(*) AS num_downloads,
      DATE_TRUNC(DATE(timestamp), MONTH) AS `month`
    FROM `bigquery-public-data.pypi.file_downloads`
    WHERE DATE(timestamp)
      BETWEEN DATE_TRUNC(DATE_SUB(DATE(2024, 12, 31), INTERVAL 180 MONTH), MONTH)
        AND DATE(2024, 12, 31)
    GROUP BY `month`, `project`
    ORDER BY `month` DESC
    """
    results_monthly_downloads = ExecuteQuery(query_monthly_downloads)
    
    ExportToCsv(results_monthly_downloads)

if __name__ == '__main__':
    Main()