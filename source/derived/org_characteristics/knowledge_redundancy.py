from pathlib import Path
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
import random
from source.lib.helpers import ImputeTimePeriod, ConcatStatsByTimePeriod, LoadFilteredImportantMembers, FilterOnImportant, ApplyRolling
import yaml

TIME_PERIOD = 6  # months

INDIR_LIB = Path("source/lib")
INDIR = Path("drive/output/derived/problem_level_data/repo_actions")
INDIR_BOT = Path("output/derived/create_bot_list")
INDIR_GRAPH = Path("drive/output/derived/graph_structure/interactions")
INDIR_IMPORTANT = Path('drive/output/derived/graph_structure/important_members')
INDIR_TEXT = Path("drive/output/derived/problem_level_data/cleaned_text")
INDIR_TEXT_MINILM = Path("drive/output/derived/problem_level_data/text_variance/minilm")
INDIR_TEXT_MPNET = Path("drive/output/derived/problem_level_data/text_variance/mpnet")
OUTDIR_AGG_MINILM = Path("drive/output/derived/org_characteristics/repo_knowledge_redundancy/minilm")
OUTDIR_AGG_MPNET = Path("drive/output/derived/org_characteristics/repo_knowledge_redundancy/mpnet")


def Main(rolling_periods=1):
    repo_files = [f.stem for f in INDIR.glob("*.parquet")]
    random.shuffle(repo_files)
    Parallel(n_jobs=8)(delayed(ProcessRepo)(repo_name, rolling_periods) for repo_name in repo_files)

    for repo_name in repo_files:
        ProcessRepo(repo_name, rolling_periods) 


def ProcessRepo(repo_name, rolling_periods=1):
    infile = INDIR / f"{repo_name}.parquet"
    infile_graph = INDIR_GRAPH / f"{repo_name}.parquet"
    infile_text_dispersion = INDIR_TEXT_MINILM / f"{repo_name}.parquet"

    if not infile.exists() or not infile_graph.exists() or not infile_text_dispersion.exists():
        print(f"⚠️ Skipping {repo_name}, missing inputs for {repo_name}")
        return

    bot_list = pd.to_numeric(pd.read_parquet(INDIR_BOT / "bot_list.parquet")["actor_id"]).unique().tolist()

    MODEL_ID = "sentence-transformers/all-mpnet-base-v2"
    OUTDIR_AGG = OUTDIR_AGG_MINILM if MODEL_ID == "sentence-transformers/all-MiniLM-L12-v2" else OUTDIR_AGG_MPNET
    OUTDIR_AGG.mkdir(parents=True, exist_ok=True)

    for contributor_subset in ["all", "important_thresh", "important_topk"]:
        df_all = pd.read_parquet(infile)
        df_all["actor_id"] = pd.to_numeric(df_all["actor_id"])
        df_all = ImputeTimePeriod(df_all, TIME_PERIOD)

        outfile_agg = OUTDIR_AGG / contributor_subset / f"rolling{rolling_periods}" / f"{repo_name}.parquet"  
        (OUTDIR_AGG / contributor_subset /  f"rolling{rolling_periods}" ).mkdir(parents=True, exist_ok=True)
        if outfile_agg.exists():
            print(f"Skipping {repo_name}, outputs already exist.")
            continue
        print(f"Processing {repo_name}...")

        if contributor_subset != "all":
            if not (INDIR_IMPORTANT / f"{repo_name}.parquet").exists():
                continue
            df_filtered_important = LoadFilteredImportantMembers(repo_name, INDIR_IMPORTANT, INDIR_LIB, contributor_subset)
            df_filtered_important_members = df_filtered_important[["time_period", "important_actors"]]
            df_all = FilterOnImportant(df_all, df_filtered_important_members)
            if df_all.empty:
                continue

        df_all["type_broad"] = df_all["type"].apply(
            lambda x: "pull request review"
            if x.startswith("pull request review") and x != "pull request review comment"
            else x
        )
        df_all["type_issue_pr"] = df_all["type"].apply(lambda x: "issue" if x.startswith("issue") else "pull request")

        df_text_dispersion_results = pd.read_parquet(infile_text_dispersion).drop(
            columns=["same_author_pairs", "diff_author_pairs"]
        )

        stats = [
            CalculateMemberStatsPerProblem(df_all, bot_list, rolling_periods),
            CalculateProjectHHI(df_all, rolling_periods=rolling_periods),
            CalculateProjectProblemHHI(df_all, rolling_periods=rolling_periods),
            ActorIssuePRMix(df_all, bot_list, rolling_periods=rolling_periods),
            AverageTypeCount(df_all, bot_list, rolling_periods=rolling_periods),
            df_text_dispersion_results.set_index('time_period')[['cos_sim_same_actor','cos_sim_diff_actor','text_sim_ratio']].rolling(rolling_periods, min_periods = 1).mean().reset_index(),
            PercentPullsMergedReviewed(df_all, rolling_periods=rolling_periods),
            CalculateAvgPRDiscCounts(df_all, rolling_periods=rolling_periods),
            CalculateAvgPRDiscCounts(df_all, include_opener=False, rolling_periods=rolling_periods),
        ]


        df_combined = ConcatStatsByTimePeriod(*stats)
        if not df_combined.empty:
            df_combined.to_parquet(outfile_agg)

# -------------------
# Metric Functions (no grouping by time_period inside cores)
# -------------------

def _MemberStatsPerProblemCore(df_all, bot_list):
    df_problem = df_all[~df_all['actor_id'].isin(bot_list)].groupby(['thread_number'], as_index=False).agg(
        avg_members_per_problem=('actor_id', 'nunique')
    )
    df_problem['pct_members_multiple'] = (df_problem['avg_members_per_problem'] > 1).astype(int)
    return pd.DataFrame({
        'avg_members_per_problem': [df_problem['avg_members_per_problem'].mean()],
        'pct_members_multiple': [df_problem['pct_members_multiple'].mean()]
    })

def CalculateMemberStatsPerProblem(df_all, bot_list, rolling_periods=1):
    return ApplyRolling(df_all, rolling_periods, _MemberStatsPerProblemCore, bot_list=bot_list)


def _ProjectHHICore(df_all):
    def activity_shares(df, actor_col='actor_id', time_col='time_period', type_col='type_broad'):
        counts = df.groupby([time_col, actor_col, type_col]).size().rename('count')
        totals = df.groupby([time_col, type_col]).size().rename('total_count')
        return (counts.reset_index()
                      .merge(totals.reset_index(), on=[time_col, type_col])
                      .assign(share=lambda d: d['count'] / d['total_count']))

    # baseline shares excluding reopened
    df_share = activity_shares(df_all[~df_all['type_broad'].str.endswith('reopened')])

    # add discussion comments
    df_comments = df_all[df_all['type_broad'].str.endswith('comment')].assign(type_broad='discussion comment')
    df_share = pd.concat([df_share, activity_shares(df_comments)])

    # compute HHI per (time_period, type_broad)
    df_share['share_sq'] = df_share['share'] ** 2
    df_hhi = df_share.groupby(['time_period', 'type_broad']).agg(
        hhi=('share_sq', 'sum'),
        total_count=('total_count', 'first')
    ).reset_index()

    # weighted averages across periods → one row
    agg = (df_hhi.groupby('type_broad', group_keys=False)
                  .agg(weighted_hhi=('hhi', lambda x: np.average(x, weights=df_hhi.loc[x.index, 'total_count']))))

    return agg.T.rename(columns=lambda c: f"proj_hhi_{c.replace(' ', '_')}").reset_index(drop=True)


def CalculateProjectHHI(df_all, rolling_periods=1):
    return ApplyRolling(df_all, rolling_periods, _ProjectHHICore)


def _ProjectProblemHHICore(df_all):
    def problem_activity_shares(df):
        counts = df.groupby(['repo_name', 'thread_number', 'actor_id', 'type_broad']).size().rename('count')
        totals = df.groupby(['repo_name', 'thread_number', 'type_broad']).size().rename('total_count')
        return (counts.reset_index()
                      .merge(totals.reset_index(), on=['repo_name', 'thread_number', 'type_broad'])
                      .assign(share=lambda d: d['count'] / d['total_count']))

    # discussion + review events
    df_disc = df_all[df_all['type_broad'].str.endswith(('comment', 'review'))]
    df_share = problem_activity_shares(df_disc)

    # add explicit discussion comment grouping
    df_comments = df_all[df_all['type_broad'].str.endswith('comment')].assign(type_broad='discussion comment')
    df_share = pd.concat([df_share, problem_activity_shares(df_comments)])

    # HHI per problem
    df_probs = (df_share.assign(share_sq=df_share['share']**2)
                        .groupby(['repo_name', 'thread_number', 'type_broad'])
                        .agg(problem_hhi=('share_sq', 'sum'),
                             total_count=('total_count', 'first'))
                        .reset_index())

    # weighted averages across problems → one row
    agg = (df_probs.groupby('type_broad', group_keys=False)
                   .agg(weighted_prob_hhi=('problem_hhi',
                                           lambda x: np.average(x, weights=df_probs.loc[x.index, 'total_count']))))

    return agg.T.rename(columns=lambda c: f"proj_prob_hhi_{c.replace(' ', '_')}").reset_index(drop=True)

def CalculateProjectProblemHHI(df_all, rolling_periods=1):
    return ApplyRolling(df_all, rolling_periods, _ProjectProblemHHICore)

def _ActorIssuePRMixCore(df, bot_list):
    actor_df = df[~df['actor_id'].isin(bot_list)]
    if actor_df.empty:
        return pd.DataFrame()

    actor_types = (
        actor_df.groupby(['repo_name', 'actor_id'])['type_issue_pr']
        .unique()
        .apply(lambda x: 'issue_only' if set(x) == {'issue'}
               else 'pr_only' if set(x) == {'pull request'}
               else 'issue_and_pr')
    )

    shares = (actor_types.reset_index(name='category')
              .groupby('category').size())
    shares = shares / shares.sum()

    return (shares.rename(lambda c: f"share_{c}")
                  .to_frame().T.reset_index(drop=True))


def ActorIssuePRMix(df_all, bot_list, rolling_periods=1):
    return ApplyRolling(df_all, rolling_periods, _ActorIssuePRMixCore, bot_list=bot_list)


def _AverageTypeCountCore(df_all, bot_list):
    avg_types = (
        df_all[(~df_all['type_broad'].str.endswith('reopened'))
               & ~(df_all['actor_id'].isin(bot_list))]
        .groupby(['repo_name', 'actor_id'])['type_broad'].nunique()
        .groupby('repo_name').mean()
    )
    return pd.DataFrame({'avg_unique_types': [avg_types.mean()]})


def AverageTypeCount(df_all, bot_list, rolling_periods=1):
    return ApplyRolling(df_all, rolling_periods, _AverageTypeCountCore, bot_list=bot_list)


def _PercentPullsMergedReviewedCore(df_all):
    # no pr data pre 2020-07
    df_all = df_all[df_all['time_period'] >= '2020-07-01']
    if df_all.empty:
        return pd.DataFrame()
    merged = df_all[df_all['type'] == "pull request merged"][['repo_name', 'thread_number']].drop_duplicates()
    
    indicators = (
        df_all[df_all['type'].eq("pull request review approved") |
               df_all['type_broad'].eq("pull request review")]
        .assign(has_approved=lambda d: d['type'].eq("pull request review approved"),
                has_review=lambda d: d['type_broad'].eq("pull request review"))
        .groupby(['repo_name', 'thread_number'])[['has_approved', 'has_review']].max()
        .reset_index()
    )

    merged = merged.merge(indicators, on=['repo_name', 'thread_number'], how='left').fillna(False)

    if merged.empty:
        return pd.DataFrame({'pct_merged_with_approved': [np.nan],
                             'pct_merged_with_review': [np.nan]})

    n_merged = merged['thread_number'].nunique()
    n_with_approved = merged['has_approved'].sum()
    n_with_review = merged['has_review'].sum()

    return pd.DataFrame({
        'pct_merged_with_approved': [n_with_approved / n_merged if n_merged > 0 else np.nan],
        'pct_merged_with_review': [n_with_review / n_merged if n_merged > 0 else np.nan]
    })


def PercentPullsMergedReviewed(df_all, rolling_periods=1):
    return ApplyRolling(df_all, rolling_periods, _PercentPullsMergedReviewedCore)


def _AvgPRDiscCountsCore(df_all, include_opener=True):
    df = df_all.copy()
    suffix = ""
    if not include_opener:
        df['opener_id'] = pd.to_numeric(df['opener_id'])
        df = df[df['actor_id'] != df['opener_id']]
        suffix = "exclude_opener_"

    avg_counts = (
        df[df['type_broad'].isin(['pull request comment',
                                  'pull request review comment',
                                  'pull request review'])]
        .groupby(['thread_number', 'type_broad']).size().unstack(fill_value=0)
    )
    avg_counts['all_disc'] = avg_counts.sum(axis=1)

    return (avg_counts.mean().to_frame().T.reset_index(drop=True)
            .rename(columns={
                'pull request comment': f'avg_pull_request_comment_{suffix}count',
                'pull request review comment': f'avg_pull_request_review_comment_{suffix}count',
                'pull request review': f'avg_pull_request_review_{suffix}count',
                'all_disc': f'avg_all_disc_{suffix}count'
            }))


def CalculateAvgPRDiscCounts(df_all, include_opener=True, rolling_periods=1):
    return ApplyRolling(df_all, rolling_periods, _AvgPRDiscCountsCore, include_opener=include_opener)


def ExportYaml(categories, outpath):
    yaml_dict = {
        category: {
            "main_covariates": main_covariates,
            "other_covariates": []
        }
        for category, main_covariates in categories.items()
    }
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with open(outpath, "w") as f:
        yaml.safe_dump(yaml_dict, f, sort_keys=False)
    print(f"📝 Exported YAML → {outpath}")


if __name__ == "__main__":
    Main(rolling_periods=1)
    Main(rolling_periods=3)
    Main(rolling_periods=5)
