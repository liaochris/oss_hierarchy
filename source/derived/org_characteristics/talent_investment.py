from pathlib import Path
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from source.lib.helpers import ImputeTimePeriod, ConcatStatsByTimePeriod, LoadFilteredImportantMembers, FilterOnImportant, ApplyRolling
import networkx as nx
import random

TIME_PERIOD = 6
INDIR = Path("drive/output/derived/problem_level_data/repo_actions")
INDIR_BOT = Path("output/derived/create_bot_list")
INDIR_GRAPH = Path("drive/output/derived/graph_structure/graphs")
INDIR_GRAPH_INTERACTIONS = Path("drive/output/derived/graph_structure/interactions")
INDIR_TEXT = Path("drive/output/derived/problem_level_data/cleaned_text")
INDIR_LIB = Path("source/lib")
INDIR_IMPORTANT = Path('drive/output/derived/graph_structure/important_members')
OUTDIR_INVESTMENT = Path("drive/output/derived/org_characteristics/repo_talent_investment")

# -----------------------------
# Rolling Helper
# -----------------------------
def ApplyRolling(df_all, rolling_periods, stat_func, **kwargs):
    results = []
    unique_periods = sorted(df_all['time_period'].unique())
    for t in unique_periods:
        window_start = t - pd.DateOffset(months=(rolling_periods - 1) * TIME_PERIOD)
        df_window = df_all[(df_all['time_period'] >= window_start) & (df_all['time_period'] <= t)]
        if df_window.empty:
            continue
        df_result = stat_func(df_window, **kwargs)
        if not df_result.empty:
            df_result = df_result.assign(time_period=t)
            results.append(df_result)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()


# -----------------------------
# Main Pipeline
# -----------------------------
def Main(rolling_periods=1):
    repo_files = [f.stem for f in INDIR.glob("*.parquet")]
    random.shuffle(repo_files)
    Parallel(n_jobs=8)(delayed(ProcessRepo)(repo_name, rolling_periods) for repo_name in repo_files)


def ProcessRepo(repo_name, rolling_periods=1):
    infile, infile_graph, infile_cleaned_text = GetRepoPaths(repo_name)
    if not (infile.exists() and infile_graph.exists() and infile_cleaned_text.exists()):
        print(f"⚠️ Skipping {repo_name}, missing input(s).")
        return

    df_all, df_interactions, df_text = LoadAndImpute(infile, infile_graph, infile_cleaned_text)
    bot_list = pd.to_numeric(pd.read_parquet(INDIR_BOT / "bot_list.parquet")["actor_id"]).unique().tolist()

    df_text_response = BuildResponseDict(df_interactions, df_text)
    df_text_response = pd.merge(df_text_response, df_all[['action_id', 'type']], on='action_id', how='left')

    for contributor_subset in ["all", "important_thresh", "important_topk"]:
        outfile_investment = OUTDIR_INVESTMENT / contributor_subset / f"rolling{rolling_periods}" / f"{repo_name}.parquet"  
        (OUTDIR_INVESTMENT / contributor_subset /  f"rolling{rolling_periods}" ).mkdir(parents=True, exist_ok=True)

        if outfile_investment.exists():
            print(f"Skipping {repo_name}, outputs already exist.")
            return
        print(f"Processing {repo_name}...")

        if contributor_subset == "important":
            if not (INDIR_IMPORTANT / f"{repo_name}.parquet").exists():
                continue
            df_filtered_important = LoadFilteredImportantMembers(repo_name, INDIR_IMPORTANT, INDIR_LIB, contributor_subset)
            df_filtered_important_members = df_filtered_important[["time_period", "important_actors"]]
            df_all = FilterOnImportant(df_all, df_filtered_important_members)
            df_text_response = FilterOnImportant(df_text_response, df_filtered_important_members)
            if df_all.empty or df_text_response.empty:
                continue
            df_closeness_centrality = CalculateClusteringCoefficient(repo_name, bot_list, df_filtered_important_members, rolling_periods)
        else:
            df_closeness_centrality = CalculateClusteringCoefficient(repo_name, bot_list, pd.DataFrame(), rolling_periods)

        df_text_can_receive, df_response_rate = CalculateResponseRate(df_text_response, bot_list, rolling_periods)
        df_days_to_respond = CalculateResponseSummary(df_text_can_receive, bot_list, rolling_periods)
        df_text_sentiment = CalculateTextSentiment(df_text, bot_list, rolling_periods)
        df_growth_opportunities = CalculateNewcomerAdoption(df_all, bot_list, past_periods=3, rolling_periods=rolling_periods)

        stats = [df_response_rate, df_days_to_respond, df_text_sentiment, df_growth_opportunities, df_closeness_centrality]
        df_combined = ConcatStatsByTimePeriod(*stats)
        if not df_combined.empty:
            df_combined.to_parquet(outfile_investment)


def GetRepoPaths(repo_name):
    infile = INDIR / f"{repo_name}.parquet"
    infile_graph = INDIR_GRAPH_INTERACTIONS / f"{repo_name}.parquet"
    infile_cleaned_text = INDIR_TEXT / f"{repo_name}.parquet"
    return infile, infile_graph, infile_cleaned_text


def LoadAndImpute(infile, infile_graph, infile_cleaned_text):
    df_all = pd.read_parquet(infile)
    df_interactions = pd.read_parquet(infile_graph)
    df_text = pd.read_parquet(infile_cleaned_text)

    df_text["actor_id"] = pd.to_numeric(df_text["actor_id"])
    df_all["actor_id"] = pd.to_numeric(df_all["actor_id"])

    return (
        ImputeTimePeriod(df_all, TIME_PERIOD),
        ImputeTimePeriod(df_interactions, TIME_PERIOD),
        df_text
    )


def BuildResponseDict(df_interactions, df_text):
    df_interactions = pd.merge(
        df_interactions, df_text[['action_id', 'cleaned_text']], on='action_id', how='left'
    )

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

def _ResponseRateCore(df_text_response, bot_list):
    def IsEligible(row_type):
        return ("pull request review" not in row_type) or (row_type == "pull request review comment")

    df = df_text_response[df_text_response['type'].apply(IsEligible) & ~df_text_response['actor_id'].isin(bot_list)]
    if df.empty:
        return pd.DataFrame({'response_rate': [np.nan]})

    overall = df['response_data'].notna().mean()
    by_type = df.groupby('type')['response_data'].apply(lambda r: r.notna().mean()).rename(lambda c: f"response_rate_{c.replace(' ', '_')}")

    return pd.DataFrame([{**{'response_rate': overall}, **by_type.to_dict()}])

def CalculateResponseRate(df_text_response, bot_list, rolling_periods=1):
    df_text_can_receive = df_text_response[df_text_response['actor_id'].notna()]
    return df_text_can_receive, ApplyRolling(df_text_response, rolling_periods, _ResponseRateCore, bot_list=bot_list)


def _ResponseSummaryCore(df_text_can_receive, bot_list):
    df = df_text_can_receive[df_text_can_receive['response_data'].notna() & ~df_text_can_receive['actor_id'].isin(bot_list)].copy()
    if df.empty:
        return pd.DataFrame({'mean_days_to_respond': [np.nan]})

    df['respond_time'] = df['response_data'].apply(lambda msgs: min((m['created_at'] for m in msgs if m.get('created_at')), default=pd.NaT))
    df['last_msg_time'] = df['self_data'].apply(lambda msgs: max((m['created_at'] for m in msgs if m.get('created_at')), default=pd.NaT))
    df = df[df['respond_time'].notna() & df['last_msg_time'].notna()]
    if df.empty:
        return pd.DataFrame({'mean_days_to_respond': [np.nan]})

    df['days_to_respond'] = (pd.to_datetime(df['respond_time']) - pd.to_datetime(df['last_msg_time'])).dt.total_seconds() / 86400
    overall = df['days_to_respond'].mean()
    by_type = df.groupby('type')['days_to_respond'].mean().rename(lambda c: f"mean_days_to_respond_{c.replace(' ', '_')}")
    last_comment = df.groupby('discussion_id').tail(1)['days_to_respond'].mean()

    return pd.DataFrame([{**{'mean_days_to_respond': overall, 'mean_days_to_respond_to_last_comment': last_comment}, **by_type.to_dict()}])

def CalculateResponseSummary(df_text_can_receive, bot_list, rolling_periods=1):
    return ApplyRolling(df_text_can_receive, rolling_periods, _ResponseSummaryCore, bot_list=bot_list)


def _TextSentimentCore(df_text, bot_list):
    df = df_text[~df_text['actor_id'].isin(bot_list)].copy()
    if df.empty:
        return pd.DataFrame({'pos_sentiment_avg': [np.nan], 'neg_sentiment_avg': [np.nan], 'ov_sentiment_avg': [np.nan]})

    overall = df[['pos', 'neg', 'compound']].mean().rename({'pos': 'pos_sentiment_avg', 'neg': 'neg_sentiment_avg', 'compound': 'ov_sentiment_avg'})

    by_type = (
        df.groupby('type')[['pos', 'neg', 'compound']].mean()
          .rename(columns={'pos': 'pos_sentiment_avg', 'neg': 'neg_sentiment_avg', 'compound': 'ov_sentiment_avg'})
    )

    by_type_flat = {f"{t}_{metric}".replace(" ", "_"): val for t, row in by_type.iterrows() for metric, val in row.items()}

    return pd.DataFrame([{**overall.to_dict(), **by_type_flat}])

def CalculateTextSentiment(df_text, bot_list, rolling_periods=1):
    return ApplyRolling(df_text, rolling_periods, _TextSentimentCore, bot_list=bot_list)


def CalculateNewcomerAdoption(df_all, bot_list, past_periods, rolling_periods=1):
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

    first_seen = df_all.groupby("actor_id")['time_period'].min()

    df_all = df_all[df_all['type'].isin(act_map.keys())].copy()
    df_all['activity'] = df_all['type'].map(act_map)
    df_all = df_all.merge(first_seen.rename("first_seen"), on="actor_id")

    results = []
    periods = sorted(df_all['time_period'].unique())

    for i, period in enumerate(periods):
        row = {"time_period": period}

        if i == 0:
            for act in ["pull_request_merged", "pull_request_review", "any_activity"]:
                row[f"{act}_eligible"] = np.nan
                row[f"{act}_adopters"] = np.nan
                row[f"{act}_rate"] = np.nan
            results.append(row)
            continue

        newcomers = first_seen[
            (first_seen >= period - pd.DateOffset(months=past_periods * TIME_PERIOD)) &
            (first_seen < period)
        ].index

        if newcomers.empty:
            results.append(row)
            continue

        df_new = df_all[df_all['actor_id'].isin(newcomers)]

        act_list = ["pull_request_merged", "pull_request_review"] if period >= pd.to_datetime('2020-07-01') else ["pull_request_merged"]
        for act in act_list:
            did_before = df_new[(df_new['time_period'] < period) & (df_new['activity'] == act)]['actor_id'].unique()
            eligible = set(newcomers) - set(did_before)
            did_now = df_new[(df_new['time_period'] == period) & (df_new['activity'] == act)]['actor_id'].unique()
            adopters = set(did_now) & eligible

            row[f"{act}_eligible"] = len(eligible)
            row[f"{act}_adopters"] = len(adopters)
            row[f"{act}_rate"] = len(adopters) / len(eligible) if eligible else np.nan

        did_before_any = df_new[(df_new['time_period'] < period) & df_new['activity'].notna()]['actor_id'].unique()
        eligible_any = set(newcomers) - set(did_before_any)
        did_now_any = df_new[(df_new['time_period'] == period) & df_new['activity'].notna()]['actor_id'].unique()
        adopters_any = set(did_now_any) & eligible_any

        row["any_activity_eligible"] = len(eligible_any)
        row["any_activity_adopters"] = len(adopters_any)
        row["any_activity_rate"] = len(adopters_any) / len(eligible_any) if eligible_any else np.nan

        results.append(row)

    df_elg = pd.DataFrame(results)
    if df_elg.empty:
        return pd.DataFrame(columns=["time_period"])  # <--- ensure time_period always exists

    rate_cols = [c for c in df_elg if c.endswith("_rate")]
    weighted_df = pd.DataFrame({
        f"{c}_weighted": (df_elg[c] * df_elg[c.replace("_rate", "_eligible")]).rolling(rolling_periods, min_periods=1).sum()
                          / df_elg[c.replace("_rate", "_eligible")].rolling(rolling_periods, min_periods=1).sum()
        for c in rate_cols
    })

    return pd.concat([df_elg[['time_period']], weighted_df], axis=1)

def CalculateClusteringCoefficient(repo_name, bot_list, df_filtered_important_members, rolling_periods=1):
    results = []
    folder_dates = [pd.to_datetime(folder.name, format="%Y%m") for folder in INDIR_GRAPH.iterdir() if folder.is_dir()]
    for t in sorted(folder_dates):
        window_start = t - pd.DateOffset(months=(rolling_periods - 1) * TIME_PERIOD)
        window_folders = [f for f in INDIR_GRAPH.iterdir() if f.is_dir() and window_start <= pd.to_datetime(f.name, format="%Y%m") <= t]
        G = nx.Graph()
        for folder in window_folders:
            gexf_path = folder / f"{repo_name}.gexf"
            if not gexf_path.exists():
                continue
            try:
                g = nx.read_gexf(gexf_path)
                g = nx.relabel_nodes(g, int)
                g = g.subgraph(set(g.nodes) - set(bot_list)).copy()
                if not df_filtered_important_members.empty:
                    important_series = df_filtered_important_members.loc[df_filtered_important_members['time_period'] == pd.to_datetime(folder.name, format="%Y%m"), 'important_actors']
                    important_list = [actor for sublist in important_series for actor in sublist]
                    g = g.subgraph(set(important_list)).copy()
                G = nx.compose(G, g)
            except Exception as e:
                print(f"Skipping {gexf_path}: {e}")
                continue
        if G.number_of_nodes() == 0:
            continue
        centrality = nx.average_clustering(G, weight="weight")
        results.append({"time_period": t, "clustering_coefficient": centrality})
    return pd.DataFrame(results)


if __name__ == "__main__":
    Main(rolling_periods=1)
    #Main(rolling_periods=3)
    Main(rolling_periods=5)
