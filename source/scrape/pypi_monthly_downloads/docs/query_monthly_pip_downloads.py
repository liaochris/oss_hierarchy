#!/usr/bin/env python3
import os
from google.cloud import bigquery

def execute_query(query):
    client = bigquery.Client()
    query_job = client.query(query)
    results_df = query_job.to_dataframe()
    return results_df

def export_to_csv(dataframe, path):
    dataframe.to_csv(path, index=False)

def main():
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
      BETWEEN DATE_TRUNC(DATE_SUB(DATE(2023, 8, 30), INTERVAL 180 MONTH), MONTH)
        AND DATE(2023, 8, 30)
    GROUP BY `month`, `project`
    ORDER BY `month` DESC
    """
    results_monthly_downloads = execute_query(query_monthly_downloads)
    
    export_to_csv(results_monthly_downloads, "drive/source/scrape/pypi_monthly_downloads/orig/pypi_monthly_downloads.csv")

if __name__ == '__main__':
    main()