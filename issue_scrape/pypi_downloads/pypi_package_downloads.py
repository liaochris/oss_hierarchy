import os
import warnings
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from source.lib.helpers import LoadGlobals
import pyarrow.parquet as pq
from dateutil.relativedelta import relativedelta


def GetProjectList(INDIR):
    df_python_projects = pd.read_csv(INDIR / "linked_pypi_github.csv")
    python_projects = list(
        df_python_projects[df_python_projects["github repository"] != "Unavailable"]["package"].unique()
    )
    return python_projects



def MakeDateWindows(start_date, end_date):
    curr = start_date.replace(day=1)
    while curr <= end_date:
        last_day = curr + relativedelta(day=31) # Automatically handles month ends
        yield curr, min(last_day, end_date)
        curr += relativedelta(months=1)

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


def ExportAllProjectsChunked(client, python_projects, OUTDIR, start_date, end_date):
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

    windows = MakeDateWindows(start_date, end_date)

    for win_start, win_end in windows:
        outfile = OUTDIR / f"pypi_downloads_{win_start.isoformat()}_{win_end.isoformat()}.parquet"
        did_write = RunQueryToParquet(client, pypi_downloads_sql, python_projects, win_start, win_end, outfile)
        if did_write:
            print(f"Wrote {outfile}")
        else:
            print(f"No rows for {win_start}–{win_end}, skipped write")


def Main():
    warnings.filterwarnings("ignore")
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("Need to set up GOOGLE_APPLICATION_CREDENTIALS environment variable")
        return

    globals_data = LoadGlobals("source/lib/globals.json")


    project_id = globals_data.get("project_id")
    client = bigquery.Client(project=project_id)
    INDIR = Path("output/scrape/pypi_site_info")
    OUTDIR = Path("drive/output/scrape/pypi_downloads/pypi_package_downloads")
    OUTDIR.mkdir(parents=True, exist_ok=True)

    python_projects = GetProjectList(INDIR)

    python_downloads_start_date = pd.to_datetime(globals_data['pip_downloads_start_date']).date()
    python_downloads_end_date = pd.to_datetime(globals_data['pip_downloads_end_date']).date()

    ExportAllProjectsChunked(
        client, python_projects, OUTDIR,
        start_date=python_downloads_start_date, end_date=python_downloads_end_date
    )