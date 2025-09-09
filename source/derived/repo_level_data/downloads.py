from source.lib.helpers import ImputeTimePeriod
import glob
import os
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def ReadParquetHandlingDbdate(file_path, pypi_package_names=None):
    table = pq.read_table(file_path)

    # Cast dbdate columns to string
    new_columns = []
    for i, field in enumerate(table.schema):
        if "dbdate" in str(field.type).lower():
            new_columns.append(table.column(i).cast(pa.string()))
        else:
            new_columns.append(table.column(i))
    table = pa.Table.from_arrays(new_columns, table.schema.names)

    # Filter rows at the Arrow level (not pandas)
    if pypi_package_names:
        mask = pa.compute.is_in(table["project"], value_set=pa.array(pypi_package_names))
        table = table.filter(mask)

    return table.to_pandas()


def ProcessSoftwareDownloads(file_path, time_period):
    df = ReadParquetHandlingDbdate(file_path)
    project = df["project"].iloc[0]
    if "date" in df.columns:
        df = df.rename(columns={"date": "created_at"})
    df["created_at"] = pd.to_datetime(df["created_at"])

    def ClassifyReleaseType(release_version):
        parts = release_version.split(".")
        if not all(p.isdigit() for p in parts):
            return "other"
        if len(parts) == 2:
            return "major" if parts[1] == "0" else "minor"
        elif len(parts) >= 3:
            if parts[1] == "0" and parts[2] == "0":
                return "major"
            elif parts[2] == "0":
                return "minor"
            else:
                return "patch"
        return "other"

    df["release_type"] = df["library_version"].apply(ClassifyReleaseType)
    df_first = df.sort_values("created_at").groupby("library_version", as_index=False).first()
    df_first = ImputeTimePeriod(df_first, time_period)

    summary_list = []
    for tp in sorted(df_first["time_period"].unique())[1:]:
        df_tp = df_first[df_first["time_period"] == tp]
        overall_count = len(df_tp)
        major_count = (df_tp["release_type"] == "major").sum()
        minor_count = (df_tp["release_type"] == "minor").sum()
        patch_count = (df_tp["release_type"] == "patch").sum()
        other_count = (df_tp["release_type"] == "other").sum()
        major_minor_count = df_tp["release_type"].isin(["major", "minor"]).sum()
        major_minor_patch_count = df_tp["release_type"].isin(["major", "minor", "patch"]).sum()

        df_sorted = df_tp.sort_values("created_at", ascending=False)
        df_valid = df_sorted[df_sorted["release_type"].isin(["major", "minor", "patch"])]

        latest_overall_downloads = df_valid.iloc[0]["num_downloads"] if not df_valid.empty else 0
        latest_dict = (
            df_sorted.drop_duplicates(subset=["release_type"])
            .set_index("release_type")["num_downloads"]
            .to_dict()
        )
        latest_major_downloads = latest_dict.get("major", 0)
        latest_minor_downloads = latest_dict.get("minor", 0)

        summary_list.append(
            {
                "time_period": tp,
                "overall_new_release_count": overall_count,
                "major_new_release_count": major_count,
                "minor_new_release_count": minor_count,
                "patch_new_release_count": patch_count,
                "other_new_release_count": other_count,
                "major_minor_release_count": major_minor_count,
                "major_minor_patch_release_count": major_minor_patch_count,
                "latest_major_downloads": latest_major_downloads,
                "latest_minor_downloads": latest_minor_downloads,
                "latest_mmp_downloads": latest_overall_downloads,
            }
        )

    summary_df = pd.DataFrame(summary_list)
    summary_df["project"] = project
    return summary_df


def GetOutcomeEventCounts(df_all, time_period):
    df_all = ImputeTimePeriod(df_all, time_period)
    df_all["type_broad"] = df_all["type"].apply(
        lambda x: "pull request review"
        if x.startswith("pull request review") and x != "pull request review comment"
        else x
    )

    outcome_event_types = [
        "issue opened",
        "pull request opened",
        "pull request merged",
        "pull request closed",
        "issue closed",
    ]
    return (
        df_all[df_all["type_broad"].isin(outcome_event_types)]
        .groupby(["time_period", "type_broad"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )


def GetDownloadsAgg(df_downloads, pypi_package_names, time_period):
    df_downloads_pypi = df_downloads[df_downloads["project"].isin(pypi_package_names)]
    df_downloads_pypi = ImputeTimePeriod(df_downloads_pypi.rename(columns={"month": "created_at"}), time_period)
    df_downloads_pypi = df_downloads_pypi[
        (df_downloads_pypi["time_period"].dt.year > 2018)
        | (
            (df_downloads_pypi["time_period"].dt.year == 2018)
            & (df_downloads_pypi["time_period"].dt.month >= 7)
        )
    ]
    return df_downloads_pypi.groupby("time_period", as_index=False)["num_downloads"].sum()


def GetProjectDownloadsSummary(indir_project_downloads, time_period, repo):
    file_list = glob.glob(os.path.join(indir_project_downloads, "*.parquet"))
    summary_dfs = [ProcessSoftwareDownloads(fp, time_period) for fp in file_list]

    release_download_summary = pd.concat(summary_dfs, ignore_index=True)
    release_download_summary.drop(["project", "license"], axis=1, inplace=True, errors="ignore")
    release_download_summary = release_download_summary.rename(columns={"github repository": "repo_name"})
    release_download_summary["repo_name"] = repo
    return release_download_summary


def Main():
    repo = "pandas-dev/pandas"  # change to whichever repo you want
    time_period = 6

    indir = Path("drive/output/derived/repo_level_data/repo_actions")
    indir_monthly_downloads = Path("drive/output/scrape/pypi_monthly_downloads")
    indir_pypi_github = Path("output/derived/collect_github_repos")
    indir_github_map = Path("output/scrape/extract_github_data/")
    indir_project_downloads = "drive/output/scrape/pypi_package_downloads"

    df_downloads = pd.read_parquet(indir_monthly_downloads / "pypi_monthly_downloads.parquet")
    df_pypi_github_map = pd.read_csv(indir_pypi_github / "linked_pypi_github.csv")
    df_github_map = pd.read_csv(indir_github_map / "repo_id_history_latest.csv")
    df_all = pd.read_parquet(indir / f"{repo.replace('/', '_')}.parquet")

    df_outcome_event_counts = GetOutcomeEventCounts(df_all, time_period)
    repo_names = df_github_map.query("latest_repo_name == @repo")["repo_name"].unique().tolist()
    pypi_package_names = df_pypi_github_map[df_pypi_github_map["github repository"].isin(repo_names)]["package"].tolist()

    if pypi_package_names:
        df_downloads_agg = GetDownloadsAgg(df_downloads, pypi_package_names, time_period)
        release_download_summary = GetProjectDownloadsSummary(indir_project_downloads, time_period, repo)



if __name__ == "__main__":
    Main()