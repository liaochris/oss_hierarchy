import random
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds
from source.lib.helpers import ImputeTimePeriod, ConcatStatsByTimePeriod, LoadGlobals, MakeRepoNameOriginal
from source.lib.JMSLab.SaveData import SaveData

_globals = LoadGlobals("source/lib/globals.json")
N_JOBS   = _globals["n_jobs"]

INDIR                  = Path("drive/output/derived/problem_level_data/repo_actions")
INDIR_MONTHLY_DOWNLOADS = Path("drive/output/scrape/pypi_downloads")
INDIR_PYPI_GITHUB      = Path("output/scrape/pypi_site_info")
INDIR_GITHUB_MAP       = Path("output/scrape/extract_github_data/")
INDIR_PROJECT_DOWNLOADS = Path("drive/output/scrape/pypi_downloads/pypi_package_downloads_package_level")
OUTDIR                 = Path("drive/output/derived/org_outcomes_practices/org_outcomes")
LOG_OUTDIR             = Path("output/derived/org_outcomes_practices/org_outcomes")
TIME_PERIOD            = _globals["time_period_months"]


def Main():
    CleanOutputs()
    repo_files = [f.stem for f in INDIR.glob("*.parquet") if not f.stem.startswith("._")]
    random.shuffle(repo_files)

    with ProcessPoolExecutor(max_workers=N_JOBS) as executor:
        futures = [executor.submit(ProcessRepo, repo_file) for repo_file in repo_files]
        for f in futures:
            f.result()


def CleanOutputs():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    LOG_OUTDIR.mkdir(parents=True, exist_ok=True)
    for f in list(OUTDIR.glob("*.parquet")) + list(LOG_OUTDIR.glob("*.log")):
        f.unlink(missing_ok=True)


def ProcessRepo(repo_file):
    repo         = MakeRepoNameOriginal(repo_file)
    outfile_agg  = OUTDIR / f"{repo_file}.parquet"
    pypi_names   = LoadRepoMappings(repo)
    df_downloads = LoadFilteredDownloads(INDIR_MONTHLY_DOWNLOADS / "pypi_monthly_downloads.parquet", pypi_names)
    df_all       = pd.read_parquet(INDIR / f"{repo_file}.parquet")

    stats = [GetOutcomeEventCounts(df_all, TIME_PERIOD)]
    if not df_downloads.empty:
        stats.append(GetDownloadsAgg(df_downloads, TIME_PERIOD))
        release_summary = GetReleaseSummary(INDIR_PROJECT_DOWNLOADS, TIME_PERIOD, pypi_names)
        if not release_summary.empty:
            stats.append(release_summary)

    df_combined = ConcatStatsByTimePeriod(*stats)
    if not df_combined.empty:
        SaveData(df_combined, ["time_period"], outfile_agg, LOG_OUTDIR / f"{repo_file}.log")


def LoadRepoMappings(repo):
    df_github_map = pd.read_csv(INDIR_GITHUB_MAP / "repo_id_history_final.csv")
    repo_names = df_github_map.query("latest_repo_name == @repo")["repo_name"].unique().tolist()
    df_pypi_github_map = pd.read_csv(INDIR_PYPI_GITHUB / "linked_pypi_github.csv")
    if repo_names:
        return df_pypi_github_map[df_pypi_github_map["github repository"].isin(repo_names)]["package"].unique().tolist()
    return []


def LoadFilteredDownloads(parquet_path, pypi_names):
    if not pypi_names:
        return pd.DataFrame()
    dataset = ds.dataset(parquet_path, format="parquet")
    table = dataset.to_table(filter=ds.field("project").isin(pypi_names))
    # Strip embedded pandas metadata: BigQuery-written files annotate date columns
    # as 'dbdate' which pandas 2+ doesn't recognize. Arrow schema is correct (date32).
    return table.replace_schema_metadata({}).to_pandas()


def GetOutcomeEventCounts(df_all, time_period):
    df_all = ImputeTimePeriod(df_all, time_period)
    df_all["type_broad"] = df_all["type"].apply(
        lambda x: "pull request review" if x.startswith("pull request review") and x != "pull request review comment" else x
    )
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


def GetReleaseSummary(indir_project_downloads, time_period, pypi_names):
    files = [f for f in indir_project_downloads.glob("*.parquet") if f.stem in pypi_names]
    if not files:
        return pd.DataFrame()
    df_repo = pd.concat([pd.read_parquet(f) for f in files])
    return ProcessSoftwareDownloads(df_repo, time_period)


def ProcessSoftwareDownloads(df, time_period):
    df = df.rename(columns={"date": "created_at"}) if "date" in df.columns else df
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
