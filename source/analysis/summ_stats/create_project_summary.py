import json

import pandas as pd
from pathlib import Path
import glob

from source.lib.python.config_loaders import LoadPipelineInputs
from source.lib.python.repo_utils import MakeRepoNameSafe
from source.lib.JMSLab.SaveData import SaveData
from source.lib.JMSLab.autofill import GenerateAutofillMacros

AUTOFILL_OUTDIR    = Path("output/autofill")
GLOBAL_SETTINGS    = Path("source/lib/config/global_settings.json")
INDIR_PYPI         = Path("output/scrape/pypi_downloads")
INDIR_LINK         = Path("output/scrape/pypi_site_info")
INDIR_REPO         = Path("output/scrape/extract_github_data")
INDIR_ISSUE        = Path("drive/output/scrape/extract_github_data/repo_level_data/issue")
INDIR_PR           = Path("drive/output/scrape/extract_github_data/repo_level_data/pr")
INDIR_ISSUE_LINKED = Path("drive/output/scrape/link_issue_pull_request/linked_issue_to_pull_request")
INDIR_PR_LINKED    = Path("drive/output/scrape/link_issue_pull_request/linked_pull_request_to_issue")
INDIR_GRAPH_LOG    = Path("output/derived/graph_structure/exported_graphs_log.csv")
OUTDIR             = Path("output/analysis/summ_stats")


def Main():
    pipeline_cfg = LoadPipelineInputs()
    analysis_spec = BuildAnalysisSpec(pipeline_cfg)

    INDIR_ORG_PANEL = Path("drive/output/derived/org_outcomes_practices/org_panel") / analysis_spec["importance_type"] / analysis_spec["rolling_label"] / "panel.parquet"

    OUTDIR.mkdir(parents=True, exist_ok=True)

    df_popular = pd.read_csv(INDIR_PYPI / "popular_python_packages.csv")
    df_linked = pd.read_csv(INDIR_LINK / "linked_pypi_github.csv")
    df_repo_history = pd.read_csv(INDIR_REPO / "repo_id_history_final.csv")

    issue_files = set(Path(f).stem.lower() for f in glob.glob(str(INDIR_ISSUE / "*.parquet")))
    pr_files = set(Path(f).stem.lower() for f in glob.glob(str(INDIR_PR / "*.parquet")))
    issue_linked_files = set(Path(f).stem.lower() for f in glob.glob(str(INDIR_ISSUE_LINKED / "*.parquet")))
    pr_linked_files = set(Path(f).stem.lower() for f in glob.glob(str(INDIR_PR_LINKED / "*.parquet")))

    df_graph_log = pd.read_csv(INDIR_GRAPH_LOG)
    df_org_panel = pd.read_parquet(INDIR_ORG_PANEL)
    exact_sample_panels = {
        sample: pd.read_parquet(analysis_spec["prepared_panel_paths"][sample])
        for sample in analysis_spec["exact_samples"]
    }

    df_summary = BuildSummary(
        df_popular, df_linked, df_repo_history,
        issue_files, pr_files, issue_linked_files, pr_linked_files,
        df_graph_log, df_org_panel, analysis_spec["exact_samples"], exact_sample_panels
    )

    SaveData(
        df_summary,
        ["package"],
        OUTDIR / "project_summary.parquet",
        OUTDIR / "project_summary.log"
    )

    ExportSummaryTable(df_summary, analysis_spec["exact_samples"], OUTDIR / "summary_table.txt")
    GenerateConfigAutofill()


def BuildSummary(df_popular, df_linked, df_repo_history,
                 issue_files, pr_files, issue_linked_files, pr_linked_files,
                 df_graph_log, df_org_panel, exact_samples, exact_sample_panels):
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
    df["has_issue_linked"] = safe_repo.isin(issue_linked_files)
    df["has_pr_linked"] = safe_repo.isin(pr_linked_files)

    df_graph = BuildGraphPeriodCounts(df_graph_log)
    df_org_flags = BuildOrgPanelFlags(df_org_panel, exact_samples)
    df_balanced = BuildBalancedPanelFlags(exact_samples, exact_sample_panels)

    df = df.merge(df_graph, left_on="unique_repo", right_on="repo", how="left").drop(columns=["repo"])
    df = df.merge(df_org_flags, left_on="unique_repo", right_on="repo_name", how="left").drop(columns=["repo_name"])
    df = df.merge(df_balanced, left_on="unique_repo", right_on="repo_name", how="left").drop(columns=["repo_name"])

    sample_flag_cols = [f"in_{sample}_degree_top3_sample" for sample in exact_samples]
    complete_panel_cols = [f"in_complete_panel_{sample}_degree_top3" for sample in exact_samples]

    return df[[
        "package", "pypi_github_link", "pypi_has_github_link", "unique_repo",
        "in_archive", "has_issues", "has_prs", "has_both", "has_issue_linked", "has_pr_linked",
        "num_graph_periods",
        "num_departures_degree_top3", "is_treated_degree_top3",
        "num_important_qualified_degree_top3_at_treatment",
        *sample_flag_cols,
        *complete_panel_cols,
    ]]


def BuildAnalysisSpec(pipeline_cfg):
    importance_type = pipeline_cfg["importance_types"]["run"][0]
    rolling_label = f"rolling{pipeline_cfg['rolling_periods']['run'][0]}"
    control_group = pipeline_cfg["control_groups"]["run"][0]
    exact_samples = sorted(
        [sample for sample in pipeline_cfg["qualified_samples"]["run"] if "_" not in sample],
        key=lambda sample: int(sample.removeprefix("exact"))
    )
    prepared_panel_paths = {
        sample: Path("output/derived/analysis_panel") / importance_type / rolling_label / sample / control_group / "panel.parquet"
        for sample in exact_samples
    }
    return {
        "importance_type": importance_type,
        "rolling_label": rolling_label,
        "control_group": control_group,
        "exact_samples": exact_samples,
        "prepared_panel_paths": prepared_panel_paths,
    }


def BuildGraphPeriodCounts(df_graph_log):
    df = df_graph_log[["repo", "per_period_exported"]].copy()
    df["num_graph_periods"] = df["per_period_exported"].apply(
        lambda x: len(json.loads(x)) if pd.notna(x) and x != "{}" else 0
    )
    return df[["repo", "num_graph_periods"]]


def BuildOrgPanelFlags(df_org_panel, exact_samples):
    panel = df_org_panel[["repo_name", "time_index", "quasi_treatment_group", "num_important_qualified", "num_departures"]].copy()

    # num_departures is cumulative — take max per repo for total departures
    departures = panel.groupby("repo_name")["num_departures"].max().reset_index()
    departures = departures.rename(columns={"num_departures": "num_departures_degree_top3"})
    departures["is_treated_degree_top3"] = departures["num_departures_degree_top3"] == 1

    at_treatment = panel[panel["time_index"] == panel["quasi_treatment_group"]][["repo_name", "num_important_qualified"]].drop_duplicates("repo_name")
    at_treatment = at_treatment.rename(columns={"num_important_qualified": "num_important_qualified_degree_top3_at_treatment"})

    result = departures.merge(at_treatment, on="repo_name", how="left")

    n = result["num_important_qualified_degree_top3_at_treatment"]
    for sample in exact_samples:
        qualified_count = int(sample.removeprefix("exact"))
        result[f"in_{sample}_degree_top3_sample"] = n == qualified_count

    return result


def BuildBalancedPanelFlags(exact_samples, exact_sample_panels):
    repos_by_sample = {
        sample: set(panel_df["repo_name"].unique())
        for sample, panel_df in exact_sample_panels.items()
    }
    all_repos = set().union(*repos_by_sample.values()) if repos_by_sample else set()
    df = pd.DataFrame({"repo_name": sorted(all_repos)})
    for sample in exact_samples:
        df[f"in_complete_panel_{sample}_degree_top3"] = df["repo_name"].isin(repos_by_sample[sample])
    return df


def ExportSummaryTable(df, exact_samples, outfile):
    df_repos = df[df["unique_repo"].notna()].drop_duplicates(subset=["unique_repo"])
    summary_rows = [
        ("PyPI packages", len(df)),
        ("  with GitHub link", df["pypi_has_github_link"].sum()),
        ("  in GitHub Archive", df["in_archive"].sum()),
        ("Unique repos", len(df_repos)),
        ("  with issues", df_repos["has_issues"].sum()),
        ("  with PRs", df_repos["has_prs"].sum()),
        ("  with both", df_repos["has_both"].sum()),
        ("  with issue linked", df_repos["has_issue_linked"].sum()),
        ("  with PR linked", df_repos["has_pr_linked"].sum()),
        ("  with graph data (any period)", (df_repos["num_graph_periods"] > 0).sum()),
    ]
    exact_sample_cols = [f"in_{s}_degree_top3_sample" for s in exact_samples]
    complete_panel_cols = [f"in_complete_panel_{s}_degree_top3" for s in exact_samples]
    for col in exact_sample_cols:
        sample = col.replace("in_", "").replace("_degree_top3_sample", "")
        summary_rows.append((f"  degree_top3: in {sample} sample", df_repos[col].sum()))
    for col in complete_panel_cols:
        sample = col.replace("in_complete_panel_", "").replace("_degree_top3", "")
        summary_rows.append((f"  degree_top3: complete panel {sample}", df_repos[col].sum()))
    lines = []
    for label, count in summary_rows:
        lines.append(f"{label}\t{count:>8,}")

    output = "\n".join(lines)
    outfile.write_text(output)


def GenerateConfigAutofill():
    with open(GLOBAL_SETTINGS, encoding="utf-8") as fh:
        settings = json.load(fh)
    AUTOFILL_OUTDIR.mkdir(parents=True, exist_ok=True)
    PipMinMonthlyDownloads = settings["pip_min_monthly_donwloads"]
    GenerateAutofillMacros(
        ["PipMinMonthlyDownloads"],
        "{:,}",
        str(AUTOFILL_OUTDIR / "config_autofill.tex"),
    )


if __name__ == "__main__":
    Main()
