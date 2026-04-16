import json, random
from pathlib import Path
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from source.lib.helpers import JsonDeserialize, JsonSerialize, LoadGlobals, MakeRepoNameOriginal
from source.lib.JMSLab.SaveData import SaveData

_globals = LoadGlobals("source/lib/globals.json")
TIME_PERIOD     = _globals["time_period_months"]
N_JOBS          = _globals["n_jobs"]

_pipeline = LoadGlobals("source/lib/pipeline_config.json")
IMPORTANCE_TYPES = _pipeline["importance_types"]["run"] + _pipeline["importance_types"]["extended"]
ROLLING_PERIODS  = [f"rolling{p}" for p in _pipeline["rolling_periods"]["run"] + _pipeline["rolling_periods"]["extended"]]

INDIR           = Path("drive/output/derived/action_data/repo_actions")
INDIR_IMPORTANT = Path("output/derived/graph_structure/important_members")
INDIR_LIB       = Path("source/lib")
INDIR_CHARS     = Path("drive/output/derived/org_outcomes_practices")
OUTDIR          = INDIR_CHARS / "org_panel"
LOG_OUTDIR      = Path("output/derived/org_outcomes_practices")

LAST_PERIOD     = "2024-07-01"
SEED            = 420

with open(INDIR_LIB / "importance.json") as f:
    _importance_params = json.load(f)

DATASETS = {
    "outcomes": {
        "all": INDIR_CHARS / "org_outcomes",
    },
    "collaboration": {
        "all": INDIR_CHARS / "repo_collaboration/all",
        "imp": INDIR_CHARS / "repo_collaboration",
    },
    "shared_knowledge": {
        "all": INDIR_CHARS / "repo_shared_knowledge/all",
        "imp": INDIR_CHARS / "repo_shared_knowledge",
    },
    "discussion_quality": {
        "all": INDIR_CHARS / "repo_discussion_quality/all",
        "imp": INDIR_CHARS / "repo_discussion_quality",
    },
    "investment_in_new_talent": {
        "all": INDIR_CHARS / "repo_investment_in_new_talent/all",
        "imp": INDIR_CHARS / "repo_investment_in_new_talent",
    },
    "organizational_routines": {
        "all": INDIR_CHARS / "repo_organizational_routines",
    },
}


def Main():
    repo_files = [f.stem for f in INDIR.glob("*.parquet") if not f.stem.startswith("._")]
    random.shuffle(repo_files)

    for importance_type in IMPORTANCE_TYPES:
        for rolling in ROLLING_PERIODS:
            (OUTDIR / importance_type / rolling).mkdir(parents=True, exist_ok=True)
            results = Parallel(n_jobs=N_JOBS)(
                delayed(ProcessRepo)(repo, importance_type, rolling) for repo in repo_files
            )

            valid = [(d, c) for d, c in results if d is not None]
            if not valid:
                print(f"No data for importance_type={importance_type}, rolling={rolling}")
                continue

            df_list, col_dicts = zip(*valid)
            columns_dict = _BuildColumnsDict(col_dicts)

            df_all = pd.concat(
                [df.loc[:, ~df.columns.duplicated()] for df in df_list], axis=0
            )
            df_all = df_all[df_all["repo_name"].notna()]
            df_all = _ProcessRepoFrame(df_all, columns_dict)
            df_all = _AssignQuasiTreatment(df_all, SEED)
            LOG_OUTDIR.mkdir(parents=True, exist_ok=True)
            for col in ["important_actors", "important_qualified_actors", "dropouts_actors"]:
                if col in df_all.columns:
                    df_all[col] = df_all[col].apply(JsonSerialize)
            SaveData(df_all, ["repo_name", "time_period"], OUTDIR / importance_type / rolling / "panel.parquet", LOG_OUTDIR / f"org_panel_{importance_type}_{rolling}.log")


def ProcessRepo(repo_name, importance_type, rolling):
    dfs, cols = {}, {}
    csv_path = INDIR_IMPORTANT / f"{repo_name}.csv"

    if csv_path.exists():
        df_imp = pd.read_csv(csv_path).copy()
        df_imp["time_period"] = pd.to_datetime(df_imp["time_period"])
        df_imp = df_imp.set_index("time_period")
        for col, val in _importance_params[importance_type].items():
            if col in df_imp.columns:
                df_imp = df_imp[df_imp[col] == val]
        dfs["df_filtered_important"] = df_imp
        cols["df_filtered_important"] = df_imp.columns.tolist()

    for category, variants in DATASETS.items():
        for variant, base in variants.items():
            key = f"df_{category}" if variant == "all" else f"df_{category}_imp"
            if category == "outcomes":
                path = base / f"{repo_name}.parquet"
            elif variant == "imp":
                path = base / importance_type / rolling / f"{repo_name}.parquet"
            else:
                path = base / rolling / f"{repo_name}.parquet"

            df = _LoadParquetSafe(path)
            if df is not None:
                if variant == "imp":
                    df.columns = [
                        f"{col}_imp" if col not in ("repo_name", "time_period") else col
                        for col in df.columns
                    ]
                dfs[key] = df.set_index("time_period")
                cols[key] = [c for c in df.columns if c != "time_period"]

    if not dfs:
        return None, None

    df_all = pd.concat(dfs.values(), axis=1, join="outer").reset_index()
    df_all["repo_name"] = MakeRepoNameOriginal(repo_name)
    return df_all, cols


def _LoadParquetSafe(path):
    try:
        return pd.read_parquet(path) if path.exists() else None
    except Exception:
        return None


def _BuildColumnsDict(col_dicts):
    keys = [
        "df_outcomes",
        "df_collaboration", "df_collaboration_imp",
        "df_shared_knowledge", "df_shared_knowledge_imp",
        "df_discussion_quality", "df_discussion_quality_imp",
        "df_investment_in_new_talent", "df_investment_in_new_talent_imp",
        "df_organizational_routines",
        "df_filtered_important",
    ]
    columns_dict = {k: [] for k in keys}
    for d in col_dicts:
        for k, v in d.items():
            columns_dict[k].append(v)
    return {k: sorted(set(col for sub in v for col in sub)) for k, v in columns_dict.items()}


def _ProcessRepoFrame(df_all, columns_dict):
    df_all = _RegularizeTimeIndex(df_all)
    df_all = _ExtendToLastPeriod(df_all)

    fill_zero_cols = (
        ["num_important", "num_important_qualified", "num_dropouts"]
        + columns_dict["df_outcomes"]
    )
    df_all = _FillWithZero(df_all, fill_zero_cols)
    df_all = _ForwardFillRoutines(df_all, [
        "has_codeowners", "has_issue_template", "has_pr_template",
        "has_good_first_issue", "has_contributing_guide", "has_code_of_conduct",
    ])
    df_all = _FillListCols(df_all, ["important_actors", "important_qualified_actors", "dropouts_actors"])

    mask = df_all["time_period"] >= pd.Timestamp("2016-01-01")
    df_all.loc[mask, ["issue_template_count"]] = df_all.loc[mask, ["issue_template_count"]].fillna(0)

    df_all["num_departures"] = df_all.groupby("repo_name")["num_dropouts"].transform("sum")
    df_all = _AssignTreatmentDate(df_all)
    df_all = _CreateTimeIndex(df_all)
    df_all["repo_id"] = pd.factorize(df_all["repo_name"])[0] + 1
    df_all["treatment"] = (df_all["treatment_group"] >= df_all["time_index"]).astype(int)
    return df_all


def _RegularizeTimeIndex(df):
    df = df.copy()
    df["time_period"] = pd.to_datetime(df["time_period"])
    freq = f"{TIME_PERIOD}MS"
    out = []
    for repo, g in df.groupby("repo_name"):
        idx = pd.date_range(g["time_period"].min(), g["time_period"].max(), freq=freq)
        g = g.set_index("time_period").reindex(idx).assign(repo_name=repo)
        g.index.name = "time_period"
        out.append(g.reset_index())
    return pd.concat(out, ignore_index=True)


def _ExtendToLastPeriod(df):
    df = df.copy()
    df["time_period"] = pd.to_datetime(df["time_period"])
    last_period = pd.to_datetime(LAST_PERIOD)
    freq = f"{TIME_PERIOD}MS"
    out = []
    for repo, g in df.groupby("repo_name", sort=False):
        idx   = pd.DatetimeIndex(g["time_period"])
        extra = (
            pd.date_range(idx.max() + pd.offsets.MonthBegin(TIME_PERIOD), last_period, freq=freq)
            if idx.max() < last_period else pd.DatetimeIndex([])
        )
        g = g.set_index("time_period").reindex(idx.union(extra))
        g["later_periods"] = 0
        if len(extra):
            g.loc[extra, "later_periods"] = 1
        g["repo_name"] = repo
        g.index.name = "time_period"
        out.append(g.reset_index())
    return pd.concat(out, ignore_index=True)


def _FillWithZero(df, cols):
    existing = [c for c in cols if c in df.columns]
    if existing:
        df[existing] = df[existing].fillna(0)
    return df


def _ForwardFillRoutines(df, cols):
    existing = [c for c in cols if c in df.columns]
    if existing:
        df[existing] = df.groupby("repo_name", sort=False)[existing].transform("ffill")
    return df


def _FillListCols(df, cols):
    for col in [c for c in cols if c in df.columns]:
        df[col] = df[col].apply(lambda x: JsonDeserialize(x, default=[]) if pd.notna(x) else [])
    return df


def _AssignTreatmentDate(df):
    # Treatment = exactly one departure across the full repo history
    mask = (df["num_dropouts"] == 1) & (df["num_departures"] == 1)
    first_dates = df.loc[mask].groupby("repo_name")["time_period"].first()
    df["treatment_date"] = df["repo_name"].map(first_dates)
    return df


def _CreateTimeIndex(df):
    mapping = {d: i + 1 for i, d in enumerate(sorted(df["time_period"].unique()))}
    df["time_index"]       = df["time_period"].map(mapping)
    df["treatment_group"]  = df["treatment_date"].map(mapping).fillna(0)
    return df


def _AssignQuasiTreatment(df, seed):
    """
    For never-treated repos, draw a pseudo-treatment time from the empirical distribution
    of actual treatment times (conditioned on when the repo first appears in the data),
    preserving comparability with treated repos in event-study designs.
    """
    np.random.seed(seed)
    first_time_index = df.groupby("repo_name")["time_index"].min()

    probs_by_time = (
        df.loc[df["treatment_group"] != 0]
        .drop_duplicates(["repo_name", "treatment_group"])
        .assign(first_time_index=lambda d: d["repo_name"].map(first_time_index))
        .groupby("first_time_index")["treatment_group"]
        .value_counts(normalize=True)
        .unstack(fill_value=0)
    )

    repo_assign = (
        df[["repo_name", "treatment_group"]]
        .drop_duplicates()
        .assign(first_time_index=lambda d: d["repo_name"].map(first_time_index))
    )

    def _draw_group(row):
        if row.treatment_group != 0:
            return row.treatment_group
        if row.first_time_index not in probs_by_time.index:
            return np.nan
        dist = probs_by_time.loc[row.first_time_index]
        return np.random.choice(dist.index, p=dist.values)

    repo_assign["quasi_treatment_group"] = repo_assign.apply(_draw_group, axis=1)
    return df.merge(
        repo_assign.drop(columns="first_time_index"),
        on=["repo_name", "treatment_group"],
        how="left",
    )


if __name__ == "__main__":
    Main()
