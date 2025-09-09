from source.lib.helpers import ImputeTimePeriod
from pathlib import Path
import numpy as np
import pandas as pd
time_period = 6

from source.lib.helpers import ImputeTimePeriod
from pathlib import Path
import numpy as np
import pandas as pd
from joblib import Parallel, delayed

time_period = 6

INDIR = Path('drive/output/derived/repo_level_data/repo_actions')
INDIR_GRAPH = Path('drive/output/derived/graph_structure/interactions')
OUTDIR_RESPONSE = Path('drive/output/derived/repo_level_data/repo_actions_responses')
OUTDIR_AGG = Path('drive/output/derived/repo_level_data/repo_knowledge_redundancy')


def ProcessRepo(repo_name, indir=INDIR, indir_graph=INDIR_GRAPH, outdir_response=OUTDIR_RESPONSE, outdir_agg=OUTDIR_AGG):
    infile = indir / f"{repo_name}.parquet"
    infile_graph = indir_graph / f"{repo_name}.parquet"
    outfile_response = outdir_response / f"{repo_name}.parquet"
    outfile_agg = outdir_agg / f"{repo_name}.parquet"

    if outfile_response.exists() and outfile_agg.exists():
        print(f"Skipping {repo_name}, outputs already exist.")
        return

    print(f"Processing {repo_name}...")

    df_all = pd.read_parquet(infile)
    df_interactions = pd.read_parquet(infile_graph)

    df_all = ImputeTimePeriod(df_all, time_period)
    df_all['type_broad'] = df_all['type'].apply(
        lambda x: 'pull request review' if x.startswith('pull request review') and 
        x != 'pull request review comment' else x)
    df_all['type_issue_pr'] = df_all['type'].apply(
        lambda x: 'issue' if x.startswith('issue') else 'pull request')

    df_all = CreateRecipientResponseDict(df_interactions, df_all)

    df_problem_member_stats = CalculateMemberStatsPerProblem(df_all)
    df_project_hhi = CalculateProjectHHI(df_all)
    df_project_problem_hhi = CalculateProjectProblemHHI(df_all)
    df_issue_pr_split = ActorTypeMix(df_all)
    df_avg_type = AverageTypeCount(df_all)
    df_pr_merge_stats = PercentPullsMergedReviewed(df_all)
    avg_pr_counts = CalculateAvgPRDiscCounts(df_all)

    df_combined_knowledge_redundancy = ConcatStatsByTimePeriod(
        df_problem_member_stats,
        df_project_hhi,
        df_project_problem_hhi,
        df_issue_pr_split,
        df_avg_type,
        df_pr_merge_stats,
        avg_pr_counts
    )

    df_combined_knowledge_redundancy.to_parquet(outfile_response)
    df_all.to_parquet(outfile_agg)


def ProcessAllRepos(n_jobs=4):
    repo_files = [f.stem for f in INDIR.glob("*.parquet")]
    Parallel(n_jobs=n_jobs)(
        delayed(ProcessRepo)(repo_name) for repo_name in repo_files
    )

def CreateRecipientResponseDict(df_interactions, df_all):
    df_interactions['text'] = df_interactions['text'].apply(
        lambda x: str(x).strip() if not pd.isnull(x) else x)
    for shift_val, colname, actor_col in [
        (-1, "response_data", "sender"),
        (1, "recipient_data", "receiver")
    ]:
        if actor_col == "sender":
            shifted = df_interactions.groupby("discussion_id")[["text", "sender", "created_at"]].shift(shift_val)
        else:
            shifted = df_interactions.groupby("discussion_id")[["text", "created_at"]].shift(shift_val)
            shifted["receiver"] = df_interactions["receiver"]

        df_interactions[colname] = shifted.to_dict(orient="records")
        mask = shifted.isna().all(axis=1)
        df_interactions.loc[mask, colname] = pd.NA
        df_interactions[colname] = df_interactions[colname].apply(
            lambda d: {k: v for k, v in d.items() if pd.notna(v)} if isinstance(d, dict) else d
        )
    df_interactions = df_interactions.rename(columns={"sender": "actor_id"}).drop(columns=["receiver","origin","time_period","opener_id","text"])
    df_interactions['actor_id'] = pd.to_numeric(df_interactions['actor_id'], errors = 'coerce')
    df_all['actor_id'] = pd.to_numeric(df_all['actor_id'], errors = 'coerce')
    df_all = pd.merge(df_all, df_interactions[['actor_id','action_id','response_data','recipient_data']], how = 'left')
    return df_all

### IS a drop in 2024 there for everyone??
def IndividualActivityShares(df, actor_col='actor_id', time_col='time_period', type_col='type_broad'):
    actor_counts = df.groupby([time_col, actor_col, type_col]).size().rename('count')
    type_totals = df.groupby([time_col, type_col]).size().rename('total_count')
    return (
        actor_counts
        .reset_index()
        .merge(type_totals.reset_index(), on=[time_col, type_col])
        .assign(share=lambda d: d['count'] / d['total_count'])
    )

def CalculateProjectHHI(df_all):
    df_all_not_reopened = df_all[~df_all['type_broad'].str.endswith('reopened')]
    df_indiv_activity_share = IndividualActivityShares(df_all_not_reopened)

    df_all_comments = df_all[df_all['type_broad'].str.endswith('comment')].assign()
    df_all_comments['type_comments'] = 'discussion comment'
    df_indiv_activity_share = pd.concat([
        df_indiv_activity_share,
        IndividualActivityShares(df_all_comments, type_col = 'type_comments').rename(
            columns={'type_comments':'type_broad'}
        )
    ])
    df_indiv_activity_share['share_sq'] = df_indiv_activity_share['share']**2
    df_project_hhi = df_indiv_activity_share.groupby(['time_period','type_broad'])['share_sq'].sum()
    return (
        df_project_hhi.reset_index(name = 'hhi_project')
        .pivot(index="time_period", columns="type_broad", values="hhi_project")
        .rename(columns=lambda c: f"proj_hhi_{c.replace(' ','_')}")
        .reset_index()
    )


def IndividualProblemActivityShares(df, actor_col='actor_id', problem_col='thread_number', project_col='repo_name', type_col='type_broad', time_col = 'time_period'):
    actor_counts = df.groupby([project_col, problem_col, time_col, actor_col, type_col]).size().rename('count')
    type_totals = df.groupby([project_col, problem_col, time_col, type_col]).size().rename('total_count')
    return (
        actor_counts
        .reset_index()
        .merge(type_totals.reset_index(), on=[project_col, problem_col, type_col, time_col])
        .assign(share=lambda d: d['count'] / d['total_count'])
    )

def CalculateProjectProblemHHI(df_all):
    df_all_disc = df_all[(df_all['type_broad'].str.endswith('comment')) |
        (df_all['type_broad'].str.endswith('review'))]
    df_indiv_prob_activity_share = IndividualProblemActivityShares(df_all_disc)
    df_all_comments = df_all[df_all['type_broad'].str.endswith('comment')].assign()
    df_all_comments['type_comments'] = 'discussion comment'

    df_indiv_prob_activity_share = pd.concat([
        df_indiv_prob_activity_share,
        IndividualProblemActivityShares(df_all_comments, type_col='type_comments')
    ])
    df_project_problem_hhi = (
        df_indiv_prob_activity_share
        .assign(share_sq=lambda d: d['share']**2)
        .groupby(['repo_name','time_period','thread_number','type_broad'], as_index = False)
        .agg(problem_hhi=('share_sq','sum'), total_count=('total_count','first'))
        .groupby(['repo_name','time_period','type_broad'])
        .apply(lambda g: np.average(g['problem_hhi'], weights=g['total_count']))
        .reset_index(name='hhi_project_problem')
    )
    return (
        df_project_problem_hhi
        .pivot(index="time_period", columns="type_broad", values="hhi_project_problem")
        .rename(columns=lambda c: f"proj_prob_hhi_{c.replace(' ','_')}")
        .reset_index()
    )


def ActorTypeMix(df):
    actor_types = (
        df.groupby(['repo_name', 'time_period', 'actor_id'])['type_issue_pr']
        .unique()
        .apply(lambda x: 'issue_only' if set(x) == {'issue'}
               else 'pr_only' if set(x) == {'pull request'}
               else 'both')
    )

    shares = (
        actor_types.reset_index(name='category')
        .groupby(['repo_name', 'time_period', 'category'])
        .size()
        .groupby(level=[0,1])
        .apply(lambda g: g / g.sum())
    )

    return (
        shares.droplevel([0,1])
        .reset_index(name='share')
        .pivot(index="time_period", columns="category", values="share")
        .rename(columns=lambda c: f"share_{c}")
        .reset_index()
    )

def AverageTypeCount(df_all):
    return (
        df_all.groupby(['repo_name', 'time_period', 'actor_id'])['type_broad']
        .nunique()  
        .groupby(['repo_name', 'time_period'])
        .mean()   
        .reset_index(name='avg_unique_types')
    )


def PercentPullsMergedReviewed(df_all):
    merged = df_all[df_all['type'] == "pull request merged"][
        ['repo_name','thread_number','time_period']].drop_duplicates()
    indicators = (
        df_all[df_all['type'].eq("pull request review approved") | df_all['type_broad'].eq("pull request review")]
        .assign(has_approved=lambda d: d['type'].eq("pull request review approved"),
                has_review=lambda d: d['type_broad'].eq("pull request review"))
        .groupby(['repo_name','thread_number','time_period'])[['has_approved','has_review']]
        .max()
        .reset_index()
    )

    percents = (
        merged.merge(indicators, on=['repo_name','thread_number','time_period'], how='left')
        .fillna(False)
        .groupby('time_period')
        .agg(n_merged=('thread_number','nunique'),
            n_with_approved=('has_approved','sum'),
            n_with_review=('has_review','sum'))
        .assign(pct_with_approved=lambda d: d['n_with_approved'] / d['n_merged'],
                pct_with_review=lambda d: d['n_with_review'] / d['n_merged'])
        .reset_index()
        .drop(columns=['n_merged','n_with_approved','n_with_review'])
    )
    return percents


def CalculateAvgPRDiscCounts(df_all):
    avg_pr_counts = (
        df_all[df_all['type_broad'].isin(['pull request comment','pull request review comment','pull request review'])]
        .groupby(['thread_number','time_period','type_broad'])
        .size()
        .unstack(fill_value=0)
        .assign(all_disc=lambda d: d.sum(axis=1))
        .groupby('time_period')
        .mean()
        .reset_index()
        .rename(columns={
            'pull request comment': 'avg_pull_request_comment_count',
            'pull request review comment': 'avg_pull_request_review_comment_count',
            'pull request review': 'avg_pull_request_review_count',
            'all_disc': 'avg_all_disc_count'
        })
    )
    return avg_pr_counts

def CalculateMemberStatsPerProblem(df_all):
    df_problem = df_all.groupby(['thread_number', 'time_period'], as_index=False).agg(
        avg_members_per_problem=('actor_id', 'nunique')
    )
    df_problem['pct_members_multiple'] = (df_problem['avg_members_per_problem']>1).astype(int)
    return df_problem.groupby(['time_period'], as_index = False).agg({
        'avg_members_per_problem': 'mean',
        'pct_members_multiple': 'mean'
    })

def ConcatStatsByTimePeriod(*dfs):
    dfs_with_index = [df.set_index("time_period") for df in dfs]
    return pd.concat(dfs_with_index, axis=1)

def Main():
    ProcessAllRepos(n_jobs=8)


if __name__ == "__main__":
    Main()


# BELOW I AHVE SOME NOTES ABOUT ADDING COMMIT DATA but I THINK I WILL LEAVE AS IS
# 3. Add commits to actions, (action_id is commit_sha, commit_date is created_at, 'commit_message_full' is text, type is 'pull request commit'  (see more detalis bwlow)
## MIGHT NEED T OGENERATE AN ID FOR PR REVIEW THREADS
## EXCLUDE COMMITS from REBASING (committer_name != committer_login, or some NA stuff)
# MAYBE PUT SUPPLEMENTARY DATA IN A JSON (changes, etc)



