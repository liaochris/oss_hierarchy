import os
import random
import warnings
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from datetime import date, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

def GetProjectList(indir_github_projects):
    df_python_projects = pd.read_csv(indir_github_projects / "linked_pypi_github.csv")
    python_projects = list(
        df_python_projects[df_python_projects["github repository"] != "Unavailable"]["package"].unique()
    )
    random.shuffle(python_projects)
    return python_projects


def MakeDateWindows(start_date, end_date, months_per_window=1):
    windows = []
    y, m = start_date.year, start_date.month
    while date(y, m, 1) <= end_date:
        win_start = date(y, m, 1)
        next_m = m + months_per_window
        next_y = y + (next_m - 1) // 12
        next_m = ((next_m - 1) % 12) + 1
        win_end = date(next_y, next_m, 1) - timedelta(days=1)
        if win_end > end_date:
            win_end = end_date
        windows.append((win_start, win_end))
        y, m = next_y, next_m
    return windows

def EstimateBytes(client, sql, python_projects, start_date, end_date):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("projects", "STRING", python_projects),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date.isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date.isoformat()),
        ],
        dry_run=True,
        use_query_cache=False,
    )
    job = client.query(sql, job_config=job_config)
    return job.total_bytes_processed

def RunQueryToParquet(client, sql, python_projects, start_date, end_date, outfile):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("projects", "STRING", python_projects),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date.isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date.isoformat()),
        ]
    )
    result = client.query(sql, job_config=job_config).result()
    arrow = result.to_arrow(create_bqstorage_client=True)
    if arrow.num_rows == 0:
        return False
    pq.write_table(arrow, outfile)
    return True

def ExportAllProjectsChunked(client, python_projects, outdir, start_date=date(2017,1,1), end_date=date(2024,12,31), months_per_window=1, max_estimated_bytes_per_window=None):
    pypi_downloads_sql = """
    SELECT 
        project, 
        DATE(timestamp) AS date, 
        file.version AS library_version, 
        CASE ARRAY_LENGTH(SPLIT(details.python, ".")) 
            WHEN 1 THEN SPLIT(details.python, ".")[0] 
            ELSE CONCAT(SPLIT(details.python, ".")[0], ".", SPLIT(details.python, ".")[1]) 
        END AS python_version, 
        COUNT(*) AS num_downloads
    FROM `bigquery-public-data.pypi.file_downloads`
    WHERE project IN UNNEST(@projects) 
      AND DATE(timestamp) BETWEEN @start_date AND @end_date
    GROUP BY project, date, library_version, python_version
    ORDER BY project, date, library_version
    """
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    windows = MakeDateWindows(start_date, end_date, months_per_window=months_per_window)
    bytes_skipped = 0
    files_written = 0

    for win_start, win_end in windows:
        est_bytes = EstimateBytes(client, pypi_downloads_sql, python_projects, win_start, win_end)
        if max_estimated_bytes_per_window is not None and est_bytes > max_estimated_bytes_per_window:
            bytes_skipped += est_bytes
            continue
        outfile = outdir / f"pypi_downloads_{win_start.isoformat()}_{win_end.isoformat()}.parquet"
        did_write = RunQueryToParquet(client, pypi_downloads_sql, python_projects, win_start, win_end, outfile)
        if did_write:
            files_written += 1
            print(f"Wrote {outfile}")
        else:
            print(f"No rows for {win_start}–{win_end}, skipped write")

    print(f"Done. Files written: {files_written}. Skipped bytes (estimated): {bytes_skipped}.")

def Main():
    warnings.filterwarnings("ignore")
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("Need to set up GOOGLE_APPLICATION_CREDENTIALS environment variable")
        return

    project_id =  "serene-bazaar-470016-h8"  # changes whenever I use a new BQ email
    client = bigquery.Client(project=project_id)
    outdir = Path("drive/output/scrape/pypi_package_downloads")
    indir_github_projects = Path("output/derived/collect_github_repos")

    python_projects = GetProjectList(indir_github_projects)
    ExportAllProjectsChunked(
        client=client,
        python_projects=python_projects,
        outdir=outdir,
        months_per_window=1,
        max_estimated_bytes_per_window=None  
    )