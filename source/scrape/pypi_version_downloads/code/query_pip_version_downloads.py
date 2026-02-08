import os
import warnings
import uuid
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from source.lib.helpers import LoadGlobals
from source.lib.JMSLab.SaveData import SaveData
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


def UploadProjectsToTempTable(client, project_id, python_projects):
    dataset_suffix = uuid.uuid4().hex[:8]
    dataset_id = f"temp_pypi_projects_{dataset_suffix}"
    table_id = f"projects_{uuid.uuid4().hex[:8]}"
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = client.location or "US"
    client.create_dataset(dataset, exists_ok=True)
    table_ref = dataset_ref.table(table_id)
    df = pd.DataFrame({"project": python_projects})
    load_job = client.load_table_from_dataframe(df, table_ref)
    load_job.result()
    full_table = f"{project_id}.{dataset_id}.{table_id}"
    print(f"Uploaded projects to temp table {full_table}")
    return full_table, dataset_id


def DropTempDataset(client, project_id, dataset_id):
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
    print(f"Dropped temp dataset {project_id}.{dataset_id}")


def RunQueryToParquet(client, sql, python_projects, start_date, end_date, outfile, logfile):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("projects", "STRING", python_projects),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date.isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date.isoformat()),
        ]
    )
    query_job = client.query(sql, job_config=job_config)
    results_df = query_job.to_dataframe()
    if results_df.empty:
        return False
    SaveData(
        results_df,
        ["project", "year", "month", "library_version"],
        outfile,
        logfile
    )
    return True


def ExportAllProjectsChunked(client, python_projects, OUTDIR, LOG_DIR, start_date, end_date):
    pypi_downloads_sql = """
    SELECT
        file.project AS project,
        EXTRACT(YEAR FROM DATE(timestamp)) AS year,
        EXTRACT(MONTH FROM DATE(timestamp)) AS month,
        file.version AS library_version,
        COUNT(*) AS num_downloads
    FROM `bigquery-public-data.pypi.file_downloads`
    WHERE project IN UNNEST(@projects)
    AND DATE(timestamp) BETWEEN @start_date AND @end_date
    GROUP BY project, year, month, library_version
    """
    pypi_downloads_sql_join = """
    SELECT
        file.project AS project,
        EXTRACT(YEAR FROM DATE(timestamp)) AS year,
        EXTRACT(MONTH FROM DATE(timestamp)) AS month,
        file.version AS library_version,
        COUNT(*) AS num_downloads
    FROM `bigquery-public-data.pypi.file_downloads`
    JOIN `{projects_table}` AS p
      ON p.project = file.project
    WHERE DATE(timestamp) BETWEEN @start_date AND @end_date
    GROUP BY project, year, month, library_version
    """

    project_id = client.project
    temp_table, temp_dataset_id = UploadProjectsToTempTable(client, project_id, python_projects)

    windows = MakeDateWindows(start_date, end_date)

    for win_start, win_end in windows:
        outfile = OUTDIR / f"pypi_downloads_{win_start.isoformat()}_{win_end.isoformat()}.parquet"
        logfile = LOG_DIR / f"pypi_downloads_{win_start.isoformat()}_{win_end.isoformat()}.log"
        if temp_table:
            sql = pypi_downloads_sql_join.format(projects_table=temp_table)
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("start_date", "DATE", win_start.isoformat()),
                    bigquery.ScalarQueryParameter("end_date", "DATE", win_end.isoformat()),
                ]
            )
            query_job = client.query(sql, job_config=job_config)
            results_df = query_job.to_dataframe()
            if not results_df.empty:
                SaveData(
                    results_df,
                    ["project", "year", "month", "library_version"],
                    outfile,
                    logfile
                )
            else:
                print(f"No rows for {win_start}–{win_end}, skipped write")
        else:
            did_write = RunQueryToParquet(client, pypi_downloads_sql, python_projects, win_start, win_end, outfile, logfile)
            if not did_write:
                print(f"No rows for {win_start}–{win_end}, skipped write")

    if temp_dataset_id:
        DropTempDataset(client, project_id, temp_dataset_id)


def Main():
    warnings.filterwarnings("ignore")
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("Need to set up GOOGLE_APPLICATION_CREDENTIALS environment variable")
        return

    globals_data = LoadGlobals("source/lib/globals.json")

    project_id = globals_data.get("project_id")
    client = bigquery.Client(project=project_id)
    INDIR = Path("output/scrape/pypi_site_info")
    OUTDIR = Path("drive/output/scrape/pypi_version_downloads")
    LOG_DIR = Path("output/scrape/pypi_version_downloads")
    OUTDIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    python_projects = GetProjectList(INDIR)

    python_downloads_start_date = pd.to_datetime("2024-03-01").date()
    python_downloads_end_date = pd.to_datetime(globals_data['pip_downloads_end_date']).date()

    ExportAllProjectsChunked(
        client, python_projects, OUTDIR, LOG_DIR,
        start_date=python_downloads_start_date, end_date=python_downloads_end_date
    )


if __name__ == "__main__":
    Main()
