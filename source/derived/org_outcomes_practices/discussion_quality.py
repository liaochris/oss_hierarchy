import random
from pathlib import Path
import networkx as nx
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from source.lib.helpers import CleanDirs, ImputeTimePeriod, LoadGlobalSettings, LoadImportanceSpecifications, LoadGlobals
from source.derived.org_outcomes_practices.helpers import AddTypeBroad, ApplyRolling, ConcatStatsByTimePeriod, FilterOnImportant, LoadBotList, LoadFilteredImportantMembers
from source.lib.JMSLab.SaveData import SaveData

_globals              = LoadGlobalSettings()
_constants            = LoadGlobals("source/derived/org_outcomes_practices/constants.json")
TIME_PERIOD           = _globals["time_period_months"]
ROLLING_PERIODS       = _globals["rolling_periods"]
N_JOBS                = _globals["n_jobs"]
RUN_EXTENSIONS        = _globals["run_extensions"]
PR_REVIEW_DATA_START  = pd.Timestamp(_constants["pr_review_data_start_date"])

_importance_params = LoadImportanceSpecifications()
PRIMARY_SUBSET    = "all"
EXTENSION_SUBSETS = list(_importance_params.keys())

INDIR            = Path("drive/output/derived/action_data/repo_actions")
INDIR_BOT        = Path("output/derived/create_bot_list")
INDIR_INTERACTIONS = Path("drive/output/derived/graph_structure/interactions")
INDIR_GRAPHS     = Path("drive/output/derived/graph_structure/graphs")
INDIR_TEXT       = Path("drive/output/derived/action_data/cleaned_text")
INDIR_LIB        = Path("source/lib")
INDIR_IMPORTANT  = Path("output/derived/graph_structure/important_members")
OUTDIR           = Path("drive/output/derived/org_outcomes_practices/repo_discussion_quality")
LOG_OUTDIR       = Path("output/derived/org_outcomes_practices/repo_discussion_quality")


def Main():
    CleanOutputs()
    bot_list   = LoadBotList(INDIR_BOT)
    repo_files = [f.stem for f in INDIR.glob("*.parquet") if not f.stem.startswith("._")]
    random.shuffle(repo_files)
    subsets = [PRIMARY_SUBSET] + (EXTENSION_SUBSETS if RUN_EXTENSIONS else [])
    for subset in subsets:
        Parallel(n_jobs=N_JOBS)(
            delayed(ProcessRepo)(repo_name, subset, bot_list, RUN_EXTENSIONS)
            for repo_name in repo_files
        )


def CleanOutputs():
    if not OUTDIR.exists():
        return
    for subset_dir in OUTDIR.iterdir():
        if subset_dir.is_dir():
            CleanDirs([
                subset_dir / f"rolling{ROLLING_PERIODS}",
                LOG_OUTDIR / subset_dir.name / f"rolling{ROLLING_PERIODS}",
            ])


def ProcessRepo(repo_name, contributor_subset, bot_list, extensions=True):
    infile       = INDIR / f"{repo_name}.parquet"
    infile_graph = INDIR_INTERACTIONS / f"{repo_name}.parquet"
    infile_text  = INDIR_TEXT / f"{repo_name}.parquet"

    if not (infile.exists() and infile_graph.exists() and infile_text.exists()):
        return

    outdir = OUTDIR / contributor_subset / f"rolling{ROLLING_PERIODS}"
    outdir.mkdir(parents=True, exist_ok=True)

    df_actions, df_interactions, df_text = LoadAndImpute(infile, infile_graph, infile_text)
    df_actions = AddTypeBroad(df_actions)

    df_conversation_blocks = BuildConversationBlocks(df_interactions, df_text)
    df_conversation_blocks = pd.merge(
        df_conversation_blocks, df_actions[["action_id", "type"]], on="action_id", how="left"
    )

    if contributor_subset != PRIMARY_SUBSET:
        if not (INDIR_IMPORTANT / f"{repo_name}.csv").exists():
            return
        df_important = LoadFilteredImportantMembers(repo_name, INDIR_IMPORTANT, INDIR_LIB, contributor_subset)
        df_important_members = df_important[["time_period", "important_actors"]]
        df_actions             = FilterOnImportant(df_actions, df_important_members)
        df_conversation_blocks = FilterOnImportant(df_conversation_blocks, df_important_members)
        if df_actions.empty or df_conversation_blocks.empty:
            return
        df_clustering = CalculateNetworkClustering(repo_name, bot_list, df_important_members)
    else:
        df_clustering = CalculateNetworkClustering(repo_name, bot_list, pd.DataFrame())

    df_eligible_senders = df_conversation_blocks[df_conversation_blocks["actor_id"].notna()]

    stats = PrimaryCalculations(df_conversation_blocks, df_eligible_senders, df_text, bot_list)
    if extensions:
        stats += ExtensionCalculations(df_conversation_blocks, df_eligible_senders, df_text, df_actions, bot_list, df_clustering)

    df_combined = ConcatStatsByTimePeriod(*stats)
    if not df_combined.empty:
        log_dir = LOG_OUTDIR / contributor_subset / f"rolling{ROLLING_PERIODS}"
        log_dir.mkdir(parents=True, exist_ok=True)
        SaveData(df_combined, ["time_period"], outdir / f"{repo_name}.parquet", log_dir / f"{repo_name}.log")


def PrimaryCalculations(df_conversation_blocks, df_eligible_senders, df_text, bot_list):
    return [
        CalculateResponseRate(df_conversation_blocks, bot_list),
        CalculateResponseTime(df_eligible_senders, bot_list),
        CalculateTextSentiment(df_text, bot_list),
    ]


def ExtensionCalculations(df_conversation_blocks, df_eligible_senders, df_text, df_actions, bot_list, df_clustering):
    return [
        CalculateResponseRate(df_conversation_blocks, bot_list, by_type=True, include_overall=False),
        CalculateResponseTime(df_eligible_senders, bot_list, by_type=True, include_overall=False),
        CalculateTextSentiment(df_text, bot_list, by_type=True, include_overall=False),
        PercentPullsMergedReviewed(df_actions),
        CalculateAvgPRDiscCounts(df_actions, include_opener=True),
        CalculateAvgPRDiscCounts(df_actions, include_opener=False),
        df_clustering,
    ]


def LoadAndImpute(infile, infile_graph, infile_text):
    df_actions      = pd.read_parquet(infile)
    df_interactions = pd.read_parquet(infile_graph)
    df_text         = pd.read_parquet(infile_text)
    df_text["actor_id"]    = pd.to_numeric(df_text["actor_id"])
    df_actions["actor_id"] = pd.to_numeric(df_actions["actor_id"])
    return (
        ImputeTimePeriod(df_actions, TIME_PERIOD),
        ImputeTimePeriod(df_interactions, TIME_PERIOD),
        df_text,
    )


def BuildConversationBlocks(df_interactions, df_text):
    """Build reply-chain structure for measuring response rates and times."""
    df_interactions = pd.merge(df_interactions, df_text[["action_id", "cleaned_text"]], on="action_id", how="left")
    df_interactions["message"] = [
        {"cleaned_text": c, "created_at": t, "text": x}
        for c, t, x in zip(df_interactions["cleaned_text"], df_interactions["created_at"], df_interactions["text"])
    ]
    df_interactions["sender_block"] = (
        (df_interactions["sender"] != df_interactions.groupby("discussion_id")["sender"].shift())
        .astype(int).groupby(df_interactions["discussion_id"]).cumsum()
    )
    grouped = (
        df_interactions.groupby(["discussion_id", "sender_block", "sender"], as_index=False)
        .agg({"message": list, "time_period": "first", "created_at": "first", "action_id": "first"})
        .rename(columns={"sender": "actor_id", "message": "self_data"})
    )
    grouped["recipient_block"] = grouped["sender_block"] - 1
    grouped["response_block"]  = grouped["sender_block"] + 1
    return (
        grouped.merge(
            grouped[["discussion_id", "sender_block", "self_data"]],
            left_on=["discussion_id", "recipient_block"], right_on=["discussion_id", "sender_block"],
            how="left", suffixes=("", "_recipient"),
        ).rename(columns={"self_data_recipient": "recipient_data"})
        .merge(
            grouped[["discussion_id", "sender_block", "self_data"]],
            left_on=["discussion_id", "response_block"], right_on=["discussion_id", "sender_block"],
            how="left", suffixes=("", "_response"),
        ).rename(columns={"self_data_response": "response_data"})
        .drop(columns=["sender_block_response", "sender_block_recipient"])
        .assign(
            recipient_block=lambda d: d["recipient_block"].where(d["recipient_data"].notna()),
            response_block =lambda d: d["response_block"].where(d["response_data"].notna()),
            actor_id=lambda d: pd.to_numeric(d["actor_id"], errors="coerce"),
        )
    )


def _ResponseRateCore(df, bot_list, by_type=False, include_overall=True):
    # PR review events (except review comments) don't receive direct replies
    df = df[df["type"].apply(lambda t: "pull request review" not in t or t == "pull request review comment")
            & ~df["actor_id"].isin(bot_list)]
    if df.empty:
        return pd.DataFrame({"response_rate": [np.nan]}) if include_overall else pd.DataFrame()
    overall = {"response_rate": df["response_data"].notna().mean()} if include_overall else {}
    if not by_type:
        return pd.DataFrame([overall])
    by_t = df.groupby("type")["response_data"].apply(lambda r: r.notna().mean()).rename(
        lambda c: f"response_rate_{c.replace(' ', '_')}"
    )
    return pd.DataFrame([{**overall, **by_t.to_dict()}])

def CalculateResponseRate(df, bot_list, by_type=False, include_overall=True):
    return ApplyRolling(df, ROLLING_PERIODS, _ResponseRateCore, bot_list=bot_list, by_type=by_type, include_overall=include_overall, time_period=TIME_PERIOD)


def _ResponseTimeCore(df_eligible_senders, bot_list, by_type=False, include_overall=True):
    df = df_eligible_senders[df_eligible_senders["response_data"].notna() & ~df_eligible_senders["actor_id"].isin(bot_list)].copy()
    if df.empty:
        return pd.DataFrame({"mean_days_to_respond": [np.nan]}) if include_overall else pd.DataFrame()
    df["respond_time"]  = df["response_data"].apply(lambda msgs: min((m["created_at"] for m in msgs if m.get("created_at")), default=pd.NaT))
    df["last_msg_time"] = df["self_data"].apply(lambda msgs: max((m["created_at"] for m in msgs if m.get("created_at")), default=pd.NaT))
    df = df[df["respond_time"].notna() & df["last_msg_time"].notna()]
    if df.empty:
        return pd.DataFrame({"mean_days_to_respond": [np.nan]}) if include_overall else pd.DataFrame()
    df["days_to_respond"] = (pd.to_datetime(df["respond_time"]) - pd.to_datetime(df["last_msg_time"])).dt.total_seconds() / 86400
    overall = {"mean_days_to_respond": df["days_to_respond"].mean()} if include_overall else {}
    if not by_type:
        return pd.DataFrame([overall])
    by_t = df.groupby("type")["days_to_respond"].mean().rename(lambda c: f"mean_days_to_respond_{c.replace(' ', '_')}")
    last_comment_avg = df.groupby("discussion_id").tail(1)["days_to_respond"].mean()
    return pd.DataFrame([{**overall, **by_t.to_dict(), "mean_days_to_respond_to_last_comment": last_comment_avg}])

def CalculateResponseTime(df_eligible_senders, bot_list, by_type=False, include_overall=True):
    return ApplyRolling(df_eligible_senders, ROLLING_PERIODS, _ResponseTimeCore, bot_list=bot_list, by_type=by_type, include_overall=include_overall, time_period=TIME_PERIOD)


def _SentimentCore(df_text, bot_list, by_type=False, include_overall=True):
    # Sentiment columns (pos, neg, compound) may be NaN for empty/unparseable text; .mean() ignores NaN
    df = df_text[~df_text["actor_id"].isin(bot_list)].copy()
    if df.empty:
        return pd.DataFrame({"pos_sentiment_avg": [np.nan], "neg_sentiment_avg": [np.nan], "ov_sentiment_avg": [np.nan]}) if include_overall else pd.DataFrame()
    overall = df[["pos", "neg", "compound"]].mean().rename(
        {"pos": "pos_sentiment_avg", "neg": "neg_sentiment_avg", "compound": "ov_sentiment_avg"}
    ).to_dict() if include_overall else {}
    if not by_type:
        return pd.DataFrame([overall])
    by_t = (
        df.groupby("type")[["pos", "neg", "compound"]].mean()
        .rename(columns={"pos": "pos_sentiment_avg", "neg": "neg_sentiment_avg", "compound": "ov_sentiment_avg"})
    )
    by_t_flat = {f"{t}_{metric}".replace(" ", "_"): val for t, row in by_t.iterrows() for metric, val in row.items()}
    return pd.DataFrame([{**overall, **by_t_flat}])

def CalculateTextSentiment(df_text, bot_list, by_type=False, include_overall=True):
    return ApplyRolling(df_text, ROLLING_PERIODS, _SentimentCore, bot_list=bot_list, by_type=by_type, include_overall=include_overall, time_period=TIME_PERIOD)


def _PercentPullsMergedReviewedCore(df_actions):
    df_actions = df_actions[df_actions["time_period"] >= PR_REVIEW_DATA_START]
    if df_actions.empty:
        return pd.DataFrame()
    merged = df_actions[df_actions["type"] == "pull request merged"][["repo_name", "thread_number"]].drop_duplicates()
    indicators = (
        df_actions[df_actions["type"].eq("pull request review approved") | df_actions["type_broad"].eq("pull request review")]
        .assign(has_approved=lambda d: d["type"].eq("pull request review approved"),
                has_review=lambda d: d["type_broad"].eq("pull request review"))
        .groupby(["repo_name", "thread_number"])[["has_approved", "has_review"]].max().reset_index()
    )
    merged = merged.merge(indicators, on=["repo_name", "thread_number"], how="left").fillna(False)
    if merged.empty:
        return pd.DataFrame({"pct_merged_with_approved": [np.nan], "pct_merged_with_review": [np.nan]})
    n = merged["thread_number"].nunique()
    return pd.DataFrame({
        "pct_merged_with_approved": [merged["has_approved"].sum() / n if n else np.nan],
        "pct_merged_with_review":   [merged["has_review"].sum() / n if n else np.nan],
    })

def PercentPullsMergedReviewed(df_actions):
    return ApplyRolling(df_actions, ROLLING_PERIODS, _PercentPullsMergedReviewedCore, time_period=TIME_PERIOD)


def _AvgPRDiscCountsCore(df_actions, include_opener=True):
    df = df_actions.copy()
    suffix = ""
    if not include_opener:
        df["opener_id"] = pd.to_numeric(df["opener_id"])
        df = df[df["actor_id"] != df["opener_id"]]
        suffix = "exclude_opener_"
    avg_counts = (
        df[df["type_broad"].isin(["pull request comment", "pull request review comment", "pull request review"])]
        .groupby(["thread_number", "type_broad"]).size().unstack(fill_value=0)
    )
    avg_counts["all_disc"] = avg_counts.sum(axis=1)
    return (avg_counts.mean().to_frame().T.reset_index(drop=True)
            .rename(columns={
                "pull request comment":        f"avg_pull_request_comment_{suffix}count",
                "pull request review comment": f"avg_pull_request_review_comment_{suffix}count",
                "pull request review":         f"avg_pull_request_review_{suffix}count",
                "all_disc":                    f"avg_all_disc_{suffix}count",
            }))

def CalculateAvgPRDiscCounts(df_actions, include_opener=True):
    return ApplyRolling(df_actions, ROLLING_PERIODS, _AvgPRDiscCountsCore, include_opener=include_opener, time_period=TIME_PERIOD)


def CalculateNetworkClustering(repo_name, bot_list, df_important_members):
    results = []
    folder_dates = [pd.to_datetime(f.name, format="%Y%m") for f in INDIR_GRAPHS.iterdir() if f.is_dir()]
    for t in sorted(folder_dates):
        window_start   = t - pd.DateOffset(months=(ROLLING_PERIODS - 1) * TIME_PERIOD)
        window_folders = [f for f in INDIR_GRAPHS.iterdir()
                          if f.is_dir() and window_start <= pd.to_datetime(f.name, format="%Y%m") <= t]
        G = nx.Graph()
        for folder in window_folders:
            gexf_path = folder / f"{repo_name}.gexf"
            if not gexf_path.exists():
                continue
            try:
                g = nx.read_gexf(gexf_path)
                g = nx.relabel_nodes(g, int)
                g = g.subgraph(set(g.nodes) - set(bot_list)).copy()
                if not df_important_members.empty:
                    imp_series = df_important_members.loc[
                        df_important_members["time_period"] == pd.to_datetime(folder.name, format="%Y%m"),
                        "important_actors",
                    ]
                    g = g.subgraph({actor for sublist in imp_series for actor in sublist}).copy()
                G = nx.compose(G, g)
            except Exception:
                pass
        if G.number_of_nodes() == 0:
            continue
        results.append({"time_period": t, "clustering_coefficient": nx.average_clustering(G, weight="weight")})
    return pd.DataFrame(results)


if __name__ == "__main__":
    Main()
