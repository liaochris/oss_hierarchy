import pandas as pd
from pathlib import Path
import glob

from source.lib.helpers import MakeRepoNameSafe
from source.lib.JMSLab.SaveData import SaveData


def Main():
    INDIR_PYPI = Path("output/scrape/pypi_downloads")
    INDIR_LINK = Path("output/scrape/pypi_site_info")
    INDIR_REPO = Path("output/scrape/extract_github_data")
    INDIR_ISSUE = Path("drive/output/scrape/extract_github_data/repo_level_data/issue")
    INDIR_PR = Path("drive/output/scrape/extract_github_data/repo_level_data/pr")
    INDIR_LINKED = Path("drive/output/scrape/link_issue_pull_request/linked_issue_to_pull_request")
    OUTDIR = Path("output/analysis/summ_stats")

    OUTDIR.mkdir(parents=True, exist_ok=True)

    df_popular = pd.read_csv(INDIR_PYPI / "popular_python_packages.csv")
    df_linked = pd.read_csv(INDIR_LINK / "linked_pypi_github.csv")
    df_repo_history = pd.read_csv(INDIR_REPO / "repo_id_history_final.csv")

    issue_files = set(Path(f).stem.lower() for f in glob.glob(str(INDIR_ISSUE / "*.parquet")))
    pr_files = set(Path(f).stem.lower() for f in glob.glob(str(INDIR_PR / "*.parquet")))
    linked_files = set(Path(f).stem.lower() for f in glob.glob(str(INDIR_LINKED / "*.parquet")))

    df_summary = BuildSummary(df_popular, df_linked, df_repo_history, issue_files, pr_files,
                              linked_files)

    SaveData(
        df_summary,
        ["package"],
        OUTDIR / "project_summary.parquet",
        OUTDIR / "project_summary.log"
    )

    ExportSummaryTable(df_summary, OUTDIR / "summary_table.txt")


def BuildSummary(df_popular, df_linked, df_repo_history, issue_files, pr_files,
                 linked_files):
    df = df_popular.rename(columns={"project": "package"}).copy()
    df = df.dropna(subset=["package"])

    df_linked_dedup = df_linked[["package", "github repository"]].drop_duplicates(subset=["package"])
    df = df.merge(
        df_linked_dedup.rename(columns={"github repository": "pypi_github_link"}),
        on="package",
        how="left"
    )

    df["pypi_has_github_link"] = (df["pypi_github_link"].notna()) & (df["pypi_github_link"] != "Unavailable")

    repo_lookup = df_repo_history[["repo_name", "latest_repo_name"]].drop_duplicates()
    repo_lookup["repo_name_lower"] = repo_lookup["repo_name"].str.lower()
    repo_lookup = repo_lookup.set_index("repo_name_lower")["latest_repo_name"].to_dict()

    df["unique_repo"] = df["pypi_github_link"].str.lower().map(repo_lookup)
    df["in_archive"] = df["unique_repo"].notna()

    safe_repo = df["unique_repo"].apply(
        lambda x: MakeRepoNameSafe(x).lower() if pd.notna(x) else None
    )

    df["has_issues"] = safe_repo.isin(issue_files)
    df["has_prs"] = safe_repo.isin(pr_files)
    df["has_both"] = df["has_issues"] & df["has_prs"]
    df["has_linked"] = safe_repo.isin(linked_files)

    return df[["package", "pypi_github_link", "pypi_has_github_link", "unique_repo", "in_archive",
               "has_issues", "has_prs", "has_both", "has_linked"]]


def ExportSummaryTable(df, outfile):
    df_repos = df[df["unique_repo"].notna()].drop_duplicates(subset=["unique_repo"])
    summary_rows = [
        ("PyPI packages", len(df)),
        ("  with GitHub link", df["pypi_has_github_link"].sum()),
        ("  in GitHub Archive", df["in_archive"].sum()),
        ("Unique repos", len(df_repos)),
        ("  with issues", df_repos["has_issues"].sum()),
        ("  with PRs", df_repos["has_prs"].sum()),
        ("  with both", df_repos["has_both"].sum()),
        ("  with linked", df_repos["has_linked"].sum()),
    ]
    lines = []
    for label, count in summary_rows:
        lines.append(f"{label}\t{count:>8,}")

    output = "\n".join(lines)
    outfile.write_text(output)


if __name__ == "__main__":
    Main()
