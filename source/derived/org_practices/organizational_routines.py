import random, shutil
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import numpy as np
import pandas as pd
from source.lib.helpers import ApplyRolling, ConcatStatsByTimePeriod, ImputeTimePeriod, LoadGlobals

_globals        = LoadGlobals("source/lib/globals.json")
TIME_PERIOD     = _globals["time_period_months"]
ROLLING_PERIODS = _globals["rolling_periods"]
N_JOBS          = _globals["n_jobs"]

INDIR       = Path("drive/output/derived/problem_level_data/repo_actions")
INDIR_FILE  = Path("drive/output/scrape/governance_data")
OUTDIR      = Path("drive/output/derived/org_practices/repo_organizational_routines")

# Dates GitHub introduced these features (used for zero-filling periods before each existed)
_CODEOWNERS_LAUNCH = pd.Timestamp("2017-07-01")  # https://github.blog/news-insights/product-news/introducing-code-owners/
_TEMPLATES_LAUNCH  = pd.Timestamp("2016-01-01")  # https://github.blog/developer-skills/github/issue-and-pull-request-templates/
_DATA_CUTOFF       = pd.Timestamp("2024-12-31")


def Main():
    CleanOutputs()
    repos = [f.stem for f in INDIR.glob("*.parquet")]
    random.shuffle(repos)
    with ProcessPoolExecutor(max_workers=N_JOBS) as executor:
        futures = [executor.submit(ProcessRepo, repo) for repo in repos]
        for f in futures:
            f.result()


def CleanOutputs():
    target = OUTDIR / f"rolling{ROLLING_PERIODS}"
    if target.exists():
        shutil.rmtree(target)


def ProcessRepo(repo):
    outdir  = OUTDIR / f"rolling{ROLLING_PERIODS}"
    outdir.mkdir(parents=True, exist_ok=True)
    outfile = outdir / f"{repo}.parquet"

    df_actions = ImputeTimePeriod(pd.read_parquet(INDIR / f"{repo}.parquet"), TIME_PERIOD)

    governance_name = repo.replace("___", "_", 1)
    file_path = INDIR_FILE / f"{governance_name}.parquet"
    if file_path.exists():
        df_files = pd.read_parquet(file_path)
        df_files["date"] = pd.to_datetime(df_files["date"], utc=True)
        df_files = ImputeTimePeriod(df_files.rename(columns={"date": "created_at"}), TIME_PERIOD)
        df_assignment_files = OrganizingFiles(df_files)
    else:
        df_assignment_files = pd.DataFrame()

    stats = PrimaryCalculations(df_actions, df_assignment_files)
    df_combined = ForwardFillOrganizingCols(ConcatStatsByTimePeriod(*stats))
    if not df_combined.empty:
        df_combined.to_parquet(outfile)


def PrimaryCalculations(df_actions, df_assignment_files):
    return [OrganizedProblemSolving(df_actions), df_assignment_files]


def ComputeMeanStatus(df, status_col):
    mean_val = (
        df.sort_values(["thread_number", status_col], ascending=[True, False])
        .drop_duplicates("thread_number", keep="first")[status_col].mean()
    )
    return pd.DataFrame({f"mean_{status_col}": [mean_val]})


def _OrganizedProblemSolvingCore(df_actions):
    df_actions = df_actions.drop("time_period", axis=1)
    df_pr = df_actions[df_actions["type"].str.startswith("pull request")].copy()
    df_pr["has_reviewer"] = df_pr["requested_reviewers"].apply(lambda x: len(x) > 0)
    df_all2 = df_actions.copy()
    df_all2["has_tag"]      = df_all2["labels"].apply(lambda x: len(x) > 0)
    df_all2["has_assignee"] = df_all2["assignees"].apply(lambda x: len(x) > 0)
    return (ComputeMeanStatus(df_pr, "has_reviewer")
            .join([ComputeMeanStatus(df_all2, "has_tag"), ComputeMeanStatus(df_all2, "has_assignee")], how="outer"))

def OrganizedProblemSolving(df_actions):
    return ApplyRolling(df_actions, ROLLING_PERIODS, _OrganizedProblemSolvingCore, time_period=TIME_PERIOD)


def FirstFilePresence(df_files, file_type, col_name):
    first_period = df_files.loc[df_files["file_type"] == file_type, "time_period"].min()
    return (df_files.query("time_period == @first_period and file_type == @file_type")
            .assign(**{col_name: 1})[["time_period", col_name]]
            .set_index("time_period").drop_duplicates())


def UniqueIssueTemplateCount(df_files):
    df_uq = (df_files.query('file_type == "issue_template"')
             .groupby("time_period", as_index=False)["file_path"].unique())
    if df_uq.empty:
        return pd.DataFrame(columns=["time_period", "issue_template_count"])
    df_uq["issue_template_count"] = df_uq["file_path"].apply(
        lambda x: len({f.split("/")[-1].split(".")[0] for f in x})
    )
    df_uq = df_uq[["time_period", "issue_template_count"]]
    if ROLLING_PERIODS == 1:
        return df_uq
    return df_uq.set_index("time_period").rolling(ROLLING_PERIODS, min_periods=1).mean().reset_index()


def FirstCodeowners(df_files):
    return (df_files.query('file_type == "codeowners"')
            .sort_values("created_at").drop_duplicates("file_type", keep="first")
            .loc[lambda df: df["time_period"] <= _DATA_CUTOFF]
            .assign(has_codeowners=1)[["time_period", "has_codeowners"]]
            .set_index("time_period"))


def OrganizingFiles(df_files):
    df = (FirstCodeowners(df_files)
          .join([FirstFilePresence(df_files, "issue_template", "has_issue_template"),
                 FirstFilePresence(df_files, "pr_template",    "has_pr_template"),
                 UniqueIssueTemplateCount(df_files).set_index("time_period")], how="outer"))
    return df[df.index <= _DATA_CUTOFF].reset_index()


def ForwardFillOrganizingCols(df):
    """Forward-fill file-presence indicators; zero-fill for periods after each feature launched on GitHub."""
    file_cols = ["has_codeowners", "has_issue_template", "has_pr_template", "issue_template_count"]
    for col in file_cols:
        if col not in df.columns:
            df[col] = np.nan
    df[file_cols] = df[file_cols].ffill()

    df.loc[df["time_period"] >= _CODEOWNERS_LAUNCH, "has_codeowners"] = (
        df.loc[df["time_period"] >= _CODEOWNERS_LAUNCH, "has_codeowners"].fillna(0)
    )
    mask_templates = df["time_period"] >= _TEMPLATES_LAUNCH
    df.loc[mask_templates, ["has_issue_template", "has_pr_template", "issue_template_count"]] = (
        df.loc[mask_templates, ["has_issue_template", "has_pr_template", "issue_template_count"]].fillna(0)
    )
    return df


if __name__ == "__main__":
    Main()
