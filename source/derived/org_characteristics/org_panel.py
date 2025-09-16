import pandas as pd
import json
import random
from pathlib import Path
from joblib import Parallel, delayed

INDIR = Path("drive/output/derived/problem_level_data/repo_actions")
INDIR_IMPORTANT = Path("drive/output/derived/graph_structure/important_members")
INDIR_LIB = Path("source/lib")
INDIR_CHARACTERISTICS = Path("drive/output/derived/org_characteristics")
OUTDIR = INDIR_CHARACTERISTICS / 'org_panel'

time_period = 6
last_period = '2024-07-01'
importance_parameters = json.load(open(INDIR_LIB / "importance.json"))

DATASETS = {
    "df_outcomes": INDIR_CHARACTERISTICS / "org_outcomes",
    "df_knowledge_redundancy": INDIR_CHARACTERISTICS / "repo_knowledge_redundancy/minilm/all",
    "df_routines": INDIR_CHARACTERISTICS / "repo_routines",
    "df_talent_investment": INDIR_CHARACTERISTICS / "repo_talent_investment/all",
}


def Main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    
    repo_files = [f.stem for f in INDIR.glob("*.parquet")]
    random.shuffle(repo_files)
    results = Parallel(n_jobs=12)(delayed(ProcessRepo)(r) for r in repo_files)
    df_list, col_dicts = zip(*[(d, c) for d, c in results if d is not None])

    columns_dict = CreateColumnsDict(col_dicts)

    df_all = pd.concat(df_list, axis=0) if df_list else None
    df_all = df_all[df_all['repo_name'].notna()]
    df_all = ProcessRepoFrame(df_all, time_period, last_period, columns_dict)
    df_all.to_parquet(OUTDIR / 'panel.parquet')


def ProcessRepo(repo_name):
    dfs, cols = {}, {}
    df_imp = LoadParquetSafe(INDIR_IMPORTANT / f"{repo_name}.parquet")
    if df_imp is not None:
        df_imp = df_imp.copy()
        df_imp["time_period"] = pd.to_datetime(df_imp["time_period"])
        df_imp = df_imp.set_index("time_period")
        for col, val in importance_parameters.items():
            if col in df_imp.columns:
                df_imp = df_imp[df_imp[col] == val]
        dfs["df_filtered_important"] = df_imp
        cols["df_filtered_important"] = df_imp.columns.tolist()
    for name, base in DATASETS.items():
        df = LoadParquetSafe(base / f"{repo_name}.parquet")
        if df is not None:
            dfs[name] = df
            cols[name] = df.columns.tolist()
    if dfs:
        df_all = pd.concat(dfs.values(), axis=1, join="outer").reset_index()
        df_all['repo_name'] = repo_name.replace("_","/",1)
        return df_all, cols
    return None, None


def CreateColumnsDict(col_dicts):
    columns_dict = {k: sorted(set(col for sub in v for col in sub)) 
                for k, v in {k: [] for k in ["df_outcomes","df_knowledge_redundancy","df_routines","df_talent_investment","df_filtered_important"]}.items()}
    for d in col_dicts:
        for k, v in d.items():
            columns_dict[k].append(v)
    for k, v in columns_dict.items():
        columns_dict[k] = sorted(set(col for sub in v for col in sub))
    return columns_dict


def ProcessRepoFrame(df_all, time_period, last_period, columns_dict):
    df_all = RegularizeTimeIndex(df_all, time_period)
    df_all = ExtendToLastPeriod(df_all, time_period, last_period)
    fill_with_zero_cols = ["num_important", "num_important_qualified", "num_dropouts"] + columns_dict['df_outcomes']
    df_all = FillWithZero(df_all, fill_with_zero_cols)
    df_all = ForwardFillRoutines(df_all, ["has_codeowners","has_issue_template","has_pr_template","has_good_first_issue","has_contributing_guide","has_code_of_conduct"])
    df_all = FillListCols(df_all, ["important_actors","important_qualified_actors","dropouts_actors"])
    mask = df_all['time_period'] >= "2016-01-01"
    df_all.loc[mask, ["issue_template_count"]] = df_all.loc[mask, ["issue_template_count"]].fillna(0)
    df_all['num_departures'] = df_all.groupby('repo_name')['num_dropouts'].transform('sum')
    df_all = AssignTreatmentDate(df_all)
    df_all = CreateTimeIndex(df_all)
    df_all["repo_id"] = pd.factorize(df_all["repo_name"])[0] + 1
    return df_all


def LoadParquetSafe(path):
    try:
        return pd.read_parquet(path) if path.exists() else None
    except Exception:
        return None


def RegularizeTimeIndex(df, time_period):
    df = df.copy()
    df["time_period"] = pd.to_datetime(df["time_period"])
    out, freq = [], f"{time_period}MS"
    for repo, g in df.groupby("repo_name"):
        idx = pd.date_range(g["time_period"].min(), g["time_period"].max(), freq=freq)
        g = g.set_index("time_period").reindex(idx).assign(repo_name=repo)
        g.index.name = "time_period"
        out.append(g.reset_index())
    return pd.concat(out, ignore_index=True)


def ExtendToLastPeriod(df, time_period, last_period):
    df = df.copy()
    df["time_period"] = pd.to_datetime(df["time_period"])
    last_period = pd.to_datetime(last_period)
    out, freq = [], f"{time_period}MS"
    for repo, g in df.groupby("repo_name", sort=False):
        idx = pd.DatetimeIndex(g["time_period"])
        extra = pd.date_range(idx.max() + pd.offsets.MonthBegin(time_period), last_period, freq=freq) if idx.max() < last_period else pd.DatetimeIndex([])
        g = g.set_index("time_period").reindex(idx.union(extra))
        g["later_periods"] = 0
        if len(extra): g.loc[extra, "later_periods"] = 1
        g["repo_name"] = repo
        g.index.name = "time_period"
        out.append(g.reset_index())
    return pd.concat(out, ignore_index=True)


def FillWithZero(df, cols):
    existing = [c for c in cols if c in df.columns]
    if existing: df[existing] = df[existing].fillna(0)
    return df


def ForwardFillRoutines(df, cols):
    existing = [c for c in cols if c in df.columns]
    if existing: df[existing] = df.groupby("repo_name", sort=False)[existing].transform("ffill")
    return df


def FillListCols(df, cols):
    for col in [c for c in cols if c in df.columns]:
        df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [])
    return df


def AssignTreatmentDate(df):
    mask = (df["num_dropouts"] == 1) & (df["num_departures"] == 1)
    first_dates = df.loc[mask].groupby("repo_name")["time_period"].first()
    df["treatment_date"] = df["repo_name"].map(first_dates)
    return df


def CreateTimeIndex(df):
    mapping = {d: i+1 for i, d in enumerate(sorted(df["time_period"].unique()))}
    df["time_index"] = df["time_period"].map(mapping)
    return df


if __name__ == "__main__":
    Main()