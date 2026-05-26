import random
from pathlib import Path
import pandas as pd
import duckdb
from joblib import Parallel, delayed
from source.lib.python.filesystem_utils import CleanDirs, WriteDirectoryHash
from source.lib.python.data_utils import ImputeTimePeriod
from source.lib.python.config_loaders import LoadGlobalSettings
from source.lib.python.repo_utils import MakeRepoNameOriginal
from source.derived.org_outcomes_practices.helpers import AddTypeBroad, ConcatStatsByTimePeriod
from source.lib.JMSLab.SaveData import SaveData

_globals = LoadGlobalSettings()
N_JOBS   = _globals["n_jobs"]

INDIR                   = Path("drive/output/derived/action_data/repo_actions")
INDIR_MONTHLY_DOWNLOADS = Path("drive/output/scrape/pypi_downloads")
INDIR_PYPI_GITHUB       = Path("output/scrape/pypi_site_info")
INDIR_GITHUB_MAP        = Path("output/scrape/extract_github_data/")
INDIR_PROJECT_DOWNLOADS = Path("drive/output/scrape/pypi_version_downloads")
OUTDIR                  = Path("drive/output/derived/org_outcomes_practices/org_outcomes")
HASH_FILE               = Path("output/derived/hashes/org_outcomes.txt")
LOG_OUTDIR              = Path("output/derived/org_outcomes_practices/org_outcomes")
TIME_PERIOD             = _globals["time_period_months"]


def Main():
    CleanOutputs()
    repo_files = [f.stem for f in INDIR.glob("*.parquet") if not f.stem.startswith("._")]
    random.shuffle(repo_files)
    repo_package_map = LoadRepoPackageMap()
    monthly_by_repo, project_by_repo = PreloadAllDownloads(repo_package_map)

    Parallel(n_jobs=N_JOBS)(
        delayed(ProcessRepo)(
            repo_file,
            monthly_by_repo.get(MakeRepoNameOriginal(repo_file), pd.DataFrame()),
            project_by_repo.get(MakeRepoNameOriginal(repo_file), pd.DataFrame()),
        )
        for repo_file in repo_files
    )

    WriteDirectoryHash(OUTDIR, HASH_FILE)


def CleanOutputs():
    CleanDirs([OUTDIR, LOG_OUTDIR])


def ProcessRepo(repo_file, df_downloads, df_project_downloads):
    outfile_agg = OUTDIR / f"{repo_file}.parquet"
    df_all      = pd.read_parquet(INDIR / f"{repo_file}.parquet")

    stats = [GetOutcomeEventCounts(df_all, TIME_PERIOD)]
    if not df_downloads.empty:
        stats.append(GetDownloadsAgg(df_downloads, TIME_PERIOD))
        if not df_project_downloads.empty:
            release_summary = ProcessSoftwareDownloads(df_project_downloads, TIME_PERIOD)
            if not release_summary.empty:
                stats.append(release_summary)

    df_combined = ConcatStatsByTimePeriod(*stats)
    if not df_combined.empty:
        SaveData(df_combined, ["time_period"], outfile_agg, LOG_OUTDIR / f"{repo_file}.log")


def LoadRepoPackageMap():
    df_github_map = pd.read_csv(INDIR_GITHUB_MAP / "repo_id_history_final.csv")
    df_pypi_github_map = pd.read_csv(INDIR_PYPI_GITHUB / "linked_pypi_github.csv")
    df_repo_packages = (
        df_github_map[["latest_repo_name", "repo_name"]]
        .dropna()
        .merge(
            df_pypi_github_map[["github repository", "package"]].dropna(),
            left_on="repo_name",
            right_on="github repository",
            how="inner",
        )
    )

    if df_repo_packages.empty:
        return {}

    return (
        df_repo_packages.groupby("latest_repo_name")["package"]
        .agg(lambda x: sorted(pd.unique(x).tolist()))
        .to_dict()
    )


def PreloadAllDownloads(repo_package_map):
    all_pypi_names = list({pkg for pkgs in repo_package_map.values() for pkg in pkgs})
    if not all_pypi_names:
        return {}, {}

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={N_JOBS}")
    con.execute("SET memory_limit='4GB'")
    try:
        monthly_path = str(INDIR_MONTHLY_DOWNLOADS / "pypi_monthly_downloads.parquet")
        df_monthly = con.execute(
            "SELECT project, month, num_downloads FROM read_parquet(?) WHERE project = ANY(?)",
            [monthly_path, all_pypi_names],
        ).df()

        version_files = [str(f) for f in INDIR_PROJECT_DOWNLOADS.glob("*.parquet") if not f.name.startswith("._")]
        df_project = con.execute(
            "SELECT year, month, library_version, project FROM read_parquet(?) WHERE project = ANY(?)",
            [version_files, all_pypi_names],
        ).df() if version_files else pd.DataFrame()
    finally:
        con.close()

    monthly_by_pkg = {pkg: sub for pkg, sub in df_monthly.groupby("project", sort=False)}
    del df_monthly
    project_by_pkg = {pkg: sub for pkg, sub in df_project.groupby("project", sort=False)} if not df_project.empty else {}
    del df_project

    monthly_by_repo = {}
    project_by_repo = {}
    for repo, packages in repo_package_map.items():
        monthly_parts = [monthly_by_pkg[p] for p in packages if p in monthly_by_pkg]
        if monthly_parts:
            monthly_by_repo[repo] = pd.concat(monthly_parts, ignore_index=True)
        project_parts = [project_by_pkg[p] for p in packages if p in project_by_pkg]
        if project_parts:
            project_by_repo[repo] = pd.concat(project_parts, ignore_index=True)

    del monthly_by_pkg, project_by_pkg
    return monthly_by_repo, project_by_repo


def GetOutcomeEventCounts(df_all, time_period):
    df_all = ImputeTimePeriod(df_all, time_period)
    df_all = AddTypeBroad(df_all)
    event_types = ["issue opened", "pull request opened", "pull request merged", "pull request closed", "issue closed"]
    df_out = (
        df_all[df_all["type_broad"].isin(event_types)]
        .groupby(["time_period", "type_broad"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    df_out.columns = [c.replace(" ", "_") for c in df_out.columns]
    return df_out


def GetDownloadsAgg(df_downloads, time_period):
    df_downloads = ImputeTimePeriod(df_downloads.rename(columns={"month": "created_at"}), time_period)
    df_downloads = df_downloads[
        (df_downloads["time_period"].dt.year > 2018)
        | ((df_downloads["time_period"].dt.year == 2018) & (df_downloads["time_period"].dt.month >= 7))
    ]
    return df_downloads.groupby("time_period", as_index=False)["num_downloads"].sum()


def ProcessSoftwareDownloads(df, time_period):
    df = df.rename(columns={"date": "created_at"}) if "date" in df.columns else df
    if "created_at" not in df.columns and {"year", "month"}.issubset(df.columns):
        df["created_at"] = pd.to_datetime(dict(year=df["year"], month=df["month"], day=1))
    if "created_at" not in df.columns:
        return pd.DataFrame()
    df["created_at"] = pd.to_datetime(df["created_at"])

    def ClassifyReleaseType(version):
        parts = version.split(".")
        if not all(p.isdigit() for p in parts):
            return "other"
        if len(parts) == 2:
            return "major" if parts[1] == "0" else "minor"
        if len(parts) >= 3:
            if parts[1] == "0" and parts[2] == "0":
                return "major"
            if parts[2] == "0":
                return "minor"
            return "patch"
        return "other"

    df["release_type"] = df["library_version"].apply(ClassifyReleaseType)
    df_first = df.sort_values("created_at").groupby("library_version", as_index=False).first()
    df_first = ImputeTimePeriod(df_first, time_period)

    summary = []
    for tp in sorted(df_first["time_period"].unique())[1:]:
        df_tp = df_first[df_first["time_period"] == tp]
        counts = df_tp["release_type"].value_counts()
        summary.append({
            "time_period": tp,
            "overall_new_release_count": len(df_tp),
            "major_new_release_count": counts.get("major", 0),
            "minor_new_release_count": counts.get("minor", 0),
            "patch_new_release_count": counts.get("patch", 0),
            "other_new_release_count": counts.get("other", 0),
            "major_minor_release_count": df_tp["release_type"].isin(["major", "minor"]).sum(),
            "major_minor_patch_release_count": df_tp["release_type"].isin(["major", "minor", "patch"]).sum(),
        })
    return pd.DataFrame(summary)


if __name__ == "__main__":
    Main()
