from collections.abc import Iterable
import pandas as pd


def AddTypeBroad(df_actions):
    mask = (
        df_actions["type"].str.startswith("pull request review")
        & (df_actions["type"] != "pull request review comment")
    )
    df_actions["type_broad"] = df_actions["type"].where(~mask, "pull request review")
    return df_actions


def LoadBotList(indir_bot):
    return pd.to_numeric(pd.read_csv(indir_bot / "bot_list.csv")["actor_id"]).unique().tolist()


def ConcatStatsByTimePeriod(*dfs):
    dfs_with_index = [df.set_index("time_period") for df in dfs if not df.empty]
    if len(dfs_with_index) == 0:
        return pd.DataFrame()
    return pd.concat(dfs_with_index, axis=1).reset_index()


def LoadFilteredImportantMembers(repo_name, INDIR_IMPORTANT, INDIR_LIB, importance_type):
    from source.lib.python.data_utils import JsonDeserialize
    from source.lib.python.config_loaders import LoadImportanceSpecifications
    importance_parameters_all = LoadImportanceSpecifications(INDIR_LIB / "config" / "importance_specifications.json")
    if importance_type not in importance_parameters_all:
        raise ValueError(f"importance_type '{importance_type}' not found in project_config.json")

    importance_parameters = importance_parameters_all[importance_type]
    df_important_members = pd.read_csv(INDIR_IMPORTANT / f"{repo_name}.csv")

    df_filtered_important = df_important_members.copy()
    for col, value in importance_parameters.items():
        if col in df_filtered_important.columns:
            df_filtered_important = df_filtered_important[df_filtered_important[col] == value]

    df_filtered_important["time_period"] = (
        pd.to_datetime(df_filtered_important["time_period"], format="%Y-%m")
        .dt.to_period("M")
        .dt.to_timestamp()
    )
    df_filtered_important["important_actors"] = df_filtered_important["important_actors"].apply(
        lambda x: [int(ele) for ele in JsonDeserialize(x, default=[])]
    )

    return df_filtered_important


def FilterOnImportant(df, df_filtered_important):
    df = df.copy()
    df = pd.merge(df, df_filtered_important, how="left")
    df = df[
        df.apply(
            lambda x: isinstance(x["important_actors"], Iterable) and x["actor_id"] in x["important_actors"],
            axis=1,
        )
    ]
    return df


def ApplyRolling(df_all, rolling_periods, stat_func, time_period=6, **kwargs):
    results = []
    unique_periods = sorted(df_all["time_period"].unique())
    for t in unique_periods:
        window_start = t - pd.DateOffset(months=(rolling_periods - 1) * time_period)
        df_window = df_all[(df_all["time_period"] >= window_start) & (df_all["time_period"] <= t)]
        if df_window.empty:
            continue
        df_result = stat_func(df_window, **kwargs)
        if not df_result.empty:
            df_result = df_result.assign(time_period=t)
            results.append(df_result)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()
