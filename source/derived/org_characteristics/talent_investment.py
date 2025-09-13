from pathlib import Path
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from source.lib.helpers import ImputeTimePeriod
import networkx as nx

TIME_PERIOD = 6
INDIR = Path("drive/output/derived/problem_level_data/repo_actions")
INDIR_GRAPH = Path("drive/output/derived/graph_structure/graphs")
INDIR_GRAPH_INTERACTIONS = Path("drive/output/derived/graph_structure/interactions")
INDIR_TEXT = Path("drive/output/derived/org_characteristics/cleaned_text")
OUTDIR_INVESTMENT = Path("drive/output/derived/org_characteristics/repo_talent_investment")

def Main():
    repo_files = [f.stem for f in INDIR.glob("*.parquet")]
    Parallel(n_jobs=8)(delayed(ProcessRepo)(repo_name) for repo_name in repo_files)


def ProcessRepo(repo_name):
    infile, infile_graph, infile_cleaned_text, outfile_investment = GetRepoPaths(repo_name)
    OUTDIR_INVESTMENT.mkdir(parents=True, exist_ok=True)

    if outfile_investment.exists():
        print(f"Skipping {repo_name}, outputs already exist.")
        return
    if not (infile.exists() and infile_graph.exists() and infile_cleaned_text.exists()):
        print(f"⚠️ Skipping {repo_name}, missing input(s).")
        return

    print(f"Processing {repo_name}...")
    df_all, df_interactions, df_text = LoadAndImpute(infile, infile_graph, infile_cleaned_text)

    df_interactions = pd.merge(
        df_interactions, df_text[['action_id', 'cleaned_text']], on='action_id', how='left'
    )

    df_text_response = BuildResponseDict(df_interactions, df_text)
    df_text_response = pd.merge(
        df_text_response, df_all[['action_id', 'type']], on='action_id', how='left'
    )

    df_text_can_receive, df_response_rate = CalculateResponseRate(df_text_response)
    df_days_to_respond = CalculateResponseSummary(df_text_can_receive)
    df_text_sentiment = CalculateTextSentiment(df_text)

    bot_list = LoadBotList()
    df_growth_opportunities = CalculateNewcomerAdoption(df_all, bot_list)
    df_closeness_centrality = CalculateClosenessCentrality(repo_name, INDIR_GRAPH)


def GetRepoPaths(repo_name):
    infile = INDIR / f"{repo_name}.parquet"
    infile_graph = INDIR_GRAPH_INTERACTIONS / f"{repo_name}.parquet"
    infile_cleaned_text = INDIR_TEXT / f"{repo_name}.parquet"
    outfile_investment = OUTDIR_INVESTMENT / f"{repo_name}.parquet"
    return infile, infile_graph, infile_cleaned_text, outfile_investment


def LoadAndImpute(infile, infile_graph, infile_cleaned_text):
    df_all = pd.read_parquet(infile)
    df_interactions = pd.read_parquet(infile_graph)
    df_text = pd.read_parquet(infile_cleaned_text)
    return (
        ImputeTimePeriod(df_all, TIME_PERIOD),
        ImputeTimePeriod(df_interactions, TIME_PERIOD),
        ImputeTimePeriod(df_text, TIME_PERIOD),
    )


def LoadBotList():
    return (
        pd.to_numeric(pd.read_parquet(bot_list_path)['actor_id'])
        .dropna()
        .astype(int)
        .tolist()
    )


def BuildResponseDict(df_interactions, df_text):
    df_interactions['message'] = [
        {"cleaned_text": c, "created_at": t, "text": x}
        for c, t, x in zip(df_interactions['cleaned_text'], df_interactions['created_at'], df_interactions['text'])
    ]
    df_interactions['sender_block'] = (
        (df_interactions['sender'] != df_interactions.groupby('discussion_id')['sender'].shift())
        .astype(int).groupby(df_interactions['discussion_id']).cumsum()
    )
    grouped = (
        df_interactions.groupby(['discussion_id', 'sender_block', 'sender'], as_index=False)
        .agg({'message': list, 'time_period': 'first', 'created_at': 'first', 'action_id': 'first'})
        .rename(columns={'sender': 'actor_id', 'message': 'self_data'})
    )
    grouped['recipient_block'] = grouped['sender_block'] - 1
    grouped['response_block'] = grouped['sender_block'] + 1

    return (
        grouped.merge(
            grouped[['discussion_id', 'sender_block', 'self_data']],
            left_on=['discussion_id', 'recipient_block'], right_on=['discussion_id', 'sender_block'],
            how='left', suffixes=('', '_recipient')
        ).rename(columns={'self_data_recipient': 'recipient_data'})
        .merge(
            grouped[['discussion_id', 'sender_block', 'self_data']],
            left_on=['discussion_id', 'response_block'], right_on=['discussion_id', 'sender_block'],
            how='left', suffixes=('', '_response')
        ).rename(columns={'self_data_response': 'response_data'})
        .drop(columns=['sender_block_response', 'sender_block_recipient'])
        .assign(
            recipient_block=lambda d: d['recipient_block'].where(d['recipient_data'].notna()),
            response_block=lambda d: d['response_block'].where(d['response_data'].notna()),
            actor_id=lambda d: pd.to_numeric(d['actor_id'], errors='coerce')
        )
    )

def CalculateResponseRate(df_text_response):
    def IsEligibleForResponse(row_type):
        return (
            "pull request review" not in row_type
            or row_type == "pull request review comment"
        )

    df_text_can_receive = df_text_response[
        df_text_response['type'].apply(IsEligibleForResponse)
    ]

    rate_by_period = (
        df_text_can_receive
        .groupby('time_period', as_index=False)['response_data']
        .apply(lambda responses: responses.notna().mean())
        .rename(columns={'response_data': 'response_rate'})
    )

    rate_by_period_type = (
        df_text_can_receive
        .groupby(['time_period', 'type'])['response_data']
        .apply(lambda responses: responses.notna().mean())
        .unstack('type')
        .add_prefix('response_rate_')
        .reset_index()
    )
    rate_by_period_type.columns = [c.replace(" ", "_") for c in rate_by_period_type.columns]

    df_response_rate = (
        rate_by_period
        .merge(rate_by_period_type, on='time_period', how='left')
    )

    return df_text_can_receive, df_response_rate


def CalculateResponseSummary(df_text_can_receive):
    df_had_response = df_text_can_receive[df_text_can_receive['response_data'].notna()].copy()

    df_had_response['respond_time'] = df_had_response['response_data'].apply(
        lambda msgs: min((m['created_at'] for m in msgs if m.get('created_at')), default=pd.NaT)
    )
    df_had_response['last_msg_time'] = df_had_response['self_data'].apply(
        lambda msgs: max((m['created_at'] for m in msgs if m.get('created_at')), default=pd.NaT)
    )
    df_had_response['time_to_response_days'] = (
        (df_had_response['respond_time'] - df_had_response['last_msg_time']).dt.total_seconds() / 86400
    )

    mean_by_period = (
        df_had_response.groupby('time_period', as_index=False)['time_to_response_days']
        .mean().rename(columns={'time_to_response_days': 'mean_days_to_respond'})
    )

    mean_by_period_type = (
        df_had_response.groupby(['time_period', 'type'])['time_to_response_days']
        .mean().unstack('type').add_prefix('mean_days_to_respond_').reset_index()
    )
    mean_by_period_type.columns = [c.replace(" ", "_") for c in mean_by_period_type.columns]

    mean_last_comment = (
        df_had_response.groupby('discussion_id').tail(1)
        .groupby('time_period')['time_to_response_days']
        .mean().rename('mean_days_to_respond_to_last_comment').reset_index()
    )

    return (
        mean_by_period
        .merge(mean_by_period_type, on='time_period', how='left')
        .merge(mean_last_comment, on='time_period', how='left')
    )


def CalculateTextSentiment(df_text):
    overall_sentiment = (
        df_text
        .groupby(['time_period'], as_index=False)[['pos', 'neg', 'compound']]
        .mean()
        .rename(columns={
            'pos': 'pos_sentiment_avg',
            'neg': 'neg_sentiment_avg',
            'compound': 'ov_sentiment_avg',
        })
    )

    type_sentiment = (
        df_text
        .groupby(['time_period', 'type'], as_index=False)[['pos', 'neg', 'compound']]
        .mean()
        .rename(columns={
            'pos': 'pos_sentiment_avg',
            'neg': 'neg_sentiment_avg',
            'compound': 'ov_sentiment_avg',
        })
        .pivot(index='time_period', columns='type')
    )

    type_sentiment.columns = [
        f"{str(col_type).replace(' ', '_')}_{col_metric}"
        for col_metric, col_type in type_sentiment.columns
    ]
    type_sentiment = type_sentiment.reset_index()

    combined_sentiment = overall_sentiment.merge(type_sentiment, on='time_period', how='left')
    return combined_sentiment


def CalculateNewcomerAdoption(df_all, bot_list, past_periods=3):
    df_all = df_all[~df_all['actor_id'].isin(bot_list)].copy()

    review_types = {
        "pull request review commented",
        "pull request review approved",
        "pull request review changes requested",
        "pull request review dismissed"
    }
    act_map = {
        "pull request merged": "pull_request_merged",
        **{t: "pull_request_review" for t in review_types}
    }

    df_all = df_all[df_all['type'].isin(act_map.keys())].copy()
    df_all['activity'] = df_all['type'].map(act_map)

    first_seen = df_all.groupby("actor_id")['time_period'].min()
    df_all = df_all.merge(first_seen.rename("first_seen"), on="actor_id")

    results = []
    periods = sorted(df_all['time_period'].unique())

    for i, period in enumerate(periods):
        row = {"time_period": period}

        if i == 0:
            for act in ["pull_request_merged", "pull_request_review", "any_activity"]:
                row[f"{act}_eligible"] = row[f"{act}_adopters"] = row[f"{act}_rate"] = np.nan
            results.append(row)
            continue

        newcomers = first_seen[(first_seen >= period - pd.DateOffset(months=past_periods*TIME_PERIOD)) & (first_seen <= period)].index
        if newcomers.empty:
            results.append(row)
            continue

        df_new = df_all[df_all['actor_id'].isin(newcomers)]

        for act in ["pull_request_merged", "pull_request_review"]:
            did_before = df_new[(df_new['time_period'] < period) & (df_new['activity'] == act)]['actor_id'].unique()
            eligible = set(newcomers) - set(did_before)
            did_now = df_new[(df_new['time_period'] == period) & (df_new['activity'] == act)]['actor_id'].unique()
            adopters = set(did_now) & eligible

            row[f"{act}_eligible"] = len(eligible)
            row[f"{act}_adopters"] = len(adopters)
            row[f"{act}_rate"] = len(adopters) / len(eligible) if eligible else 0

        did_before_any = df_new[(df_new['time_period'] < period) & df_new['activity'].notna()]['actor_id'].unique()
        eligible_any = set(newcomers) - set(did_before_any)
        did_now_any = df_new[(df_new['time_period'] == period) & df_new['activity'].notna()]['actor_id'].unique()
        adopters_any = set(did_now_any) & eligible_any

        row["any_activity_eligible"] = len(eligible_any)
        row["any_activity_adopters"] = len(adopters_any)
        row["any_activity_rate"] = len(adopters_any) / len(eligible_any) if eligible_any else 0

        results.append(row)

    return pd.DataFrame(results)


def CalculateClosenessCentrality(repo_name, INDIR_GRAPH):
    df_centrality_all = pd.DataFrame()
    for folder in INDIR_GRAPH.iterdir():
        if not folder.is_dir():
            continue

        gexf_path = folder / f"{repo_name}.gexf"
        if not gexf_path.exists():
            continue

        try:
            print(f"Processing {repo_name}...{folder.name}")
            graph = nx.read_gexf(gexf_path)
            centrality = nx.average_clustering(graph, weight="weight")
            date = pd.to_datetime(folder.name, format="%Y%m")
            df_centrality = pd.DataFrame({
                "time_period": [date],
                "closeness_centrality": [centrality]
            })
            
            df_centrality_all = pd.concat([df_centrality_all, df_centrality])
        except Exception as e:
            print(f"Skipping {gexf_path}: {e}")
            continue
    return df_centrality_all


if __name__ == "__main__":
    Main()
