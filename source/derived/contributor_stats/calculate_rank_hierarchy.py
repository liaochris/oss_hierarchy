import os
import pandas as pd
from pathlib import Path
import numpy as np
import sys
import glob
import warnings
import random
from pandarallel import pandarallel
from source.lib.JMSLab import autofill
from source.lib.helpers import *
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from glob import glob 
import datetime
import itertools
import time
from multiprocessing import pool
import shutil

def Main():
    pandarallel.initialize(progress_bar = True)

    indir_committers = Path('drive/output/scrape/link_committers_profile')
    indir = Path('drive/output/derived/contributor_stats/contributor_data')
    outdir = Path('drive/output/derived/contributor_stats/contributor_rank_hierarchy')
    
    time_period = int(sys.argv[1])
    rolling_window = int(sys.argv[2])

    committers_match = CleanCommittersInfo(indir_committers)
    print("committers_match made")
    df_committers = ReadCommitterData(indir, committers_match, time_period, rolling_window)
    df_committers_rank = RankContributors(df_committers)
    df_ranked_activity_share = CreateRankSharePanel(df_committers_rank)
    df_hierarchy = AboveRankActivity(df_ranked_activity_share)
    
    df_committers_rank.to_parquet(outdir / f'contributor_rank_major_months{time_period}_window{rolling_window}D.parquet')
    df_ranked_activity_share.to_parquet(outdir / f'contributions_by_rank_major_months{time_period}_window{rolling_window}D.parquet')
    df_hierarchy.to_parquet(outdir / f'hierarchy_measure_major_months{time_period}_window{rolling_window}D.parquet')

def CleanCommittersInfo(indir_committers):
    # TODO: edit file so it can handle pushes
    df_committers_info = pd.concat([pd.read_csv(indir_committers / 'committers_info_pr.csv', index_col = 0, engine = 'pyarrow').dropna(),
                                    pd.read_csv(indir_committers / 'committers_info_push.csv', index_col = 0, engine = 'pyarrow').dropna()])
    df_committers_info['committer_info'] = df_committers_info['committer_info'].parallel_apply(literal_eval)
    df_committers_info['commit_repo'] = df_committers_info['commit_repo'].parallel_apply(literal_eval)
    # TODO: handle cleaning so that it can handle the other cases
    df_committers_info = df_committers_info[df_committers_info['committer_info'].parallel_apply(lambda x: len(x)==4)]
    df_committers_info['repo'] = df_committers_info['commit_repo'].parallel_apply(lambda x: list(set([ele.split("_")[1] for ele in x])))
    df_committers_info = df_committers_info.explode('repo')
    df_committers_info['actor_name'] = df_committers_info['committer_info'].parallel_apply(lambda x: x[0])
    df_committers_info['actor_id'] = df_committers_info['committer_info'].parallel_apply(lambda x: x[1])
    df_committers_info['type'] = df_committers_info['committer_info'].parallel_apply(lambda x: x[2])
    committers_match = df_committers_info[['name','email','user_type', 'type','actor_name','actor_id','repo']].drop_duplicates()
    committers_match.rename({'actor_id':'commit_author_id'}, axis = 1, inplace = True)
    return committers_match

def ReadCommitterData(indir, committers_match, time_period, rolling_window):
    df_committers = pd.read_parquet(indir / f'major_contributors_major_months{time_period}_window{rolling_window}D_samplefull.parquet', engine = 'pyarrow')
    df_committers = pd.merge(df_committers, committers_match[['commit_author_id','type']], how = 'left',
             left_on = ['actor_id'], right_on = ['commit_author_id'])
    df_committers['user_type'] = df_committers.parallel_apply(lambda x: x['user_type'] if pd.isnull(x['type']) else x['type'], axis = 1)
    df_committers.drop('type', axis = 1, inplace = True)
    return df_committers

def RankContributors(df_committers):
    user_cols = ['repo_name', 'actor_id', 'time_period', 'user_type']
    active_user_cols = ['issue_number','linked_pr_issue_number','issue_comments','linked_pr_issue_comments', 'own_issue_comments']
    developer_pr_cols = ['pr','pr_commits','pr_commit_additions','pr_commit_deletions','pr_commit_changes_total','pr_commit_files_changed_count']
    developer_push_cols = ['push_commits','push_commit_additions','push_commit_deletions','push_commit_changes_total','push_commit_files_changed_count']
    developer_cols = developer_pr_cols + developer_push_cols
    reviewer_cols = ['pr_reviews','pr_review_comments','prs_merged','issues_closed']
    df_committers_rank = df_committers[user_cols + active_user_cols + developer_cols + reviewer_cols].drop_duplicates()
    df_committers_rank['active_user'] =  (df_committers_rank[active_user_cols]>0).apply(any, axis = 1).astype(int)
    df_committers_rank['developer_pr'] =  (df_committers_rank[developer_pr_cols]>0).apply(any, axis = 1).astype(int)
    df_committers_rank['developer_push'] =  (df_committers_rank[developer_push_cols]>0).apply(any, axis = 1).astype(int)
    df_committers_rank['developer'] = df_committers_rank[['developer_pr','developer_push']].apply(any, axis = 1).astype(int)
    df_committers_rank['maintainer'] = df_committers_rank[reviewer_cols].apply(any, axis = 1).astype(int)
    df_committers_rank['rank'] = df_committers_rank.apply(
        lambda x: 'maintainer' if x['maintainer']==1 else 'developer' if x['developer'] == 1 else 'active user' if x['active_user'] == 1 else np.nan, axis = 1)
    return df_committers_rank

def CreateRankSharePanel(df_committers_rank):
    df_committers_rank['commits'] = df_committers_rank['pr_commits'] + df_committers_rank['push_commits']
    df_ranked_agg_actvity = df_committers_rank.assign(user_count = 1).groupby(['repo_name','time_period', 'rank'])\
        .agg({'user_count': 'sum', 'issue_number': 'sum', 'linked_pr_issue_number':'sum', 'own_issue_comments': 'sum',
              'issue_comments': 'sum', 'linked_pr_issue_comments':'sum','pr': 'sum', 
              'commits': 'sum','pr_commits':'sum','push_commits':'sum', 'pr_reviews': 'sum', 'pr_review_comments':'sum', 'prs_merged': 'sum',
              'issues_closed': 'sum'})\
        .rename({'issue_number':'opened_issues'}, axis = 1)
    df_agg_activity = df_ranked_agg_actvity.reset_index(level = 2).drop('rank', axis = 1).groupby(level = [0,1]).sum().add_prefix('total_')
    df_ranked_activity_share = df_ranked_agg_actvity.reset_index(level = 2).join(df_agg_activity, how = 'left',on = ['repo_name','time_period'])
    df_ranked_activity_share[df_ranked_agg_actvity.columns]/df_ranked_activity_share[df_agg_activity.columns]
    activity_share = df_ranked_activity_share[df_ranked_agg_actvity.columns].values/df_ranked_activity_share[df_agg_activity.columns].values
    df_ranked_activity_share[[f"share_{col}" for col in df_ranked_agg_actvity.columns]] = activity_share
    return df_ranked_activity_share

def AboveRankActivity(df_ranked_activity_share):
    share_activeuser = df_ranked_activity_share.query('rank != "active user"')\
        .groupby(level = [0, 1])[['share_opened_issues','share_issue_comments']].sum()
    total_activeuser_activities = df_ranked_activity_share.groupby(level = [0, 1])[['opened_issues','issue_comments']].sum()
    df_activeuser_activities = share_activeuser.join(total_activeuser_activities, how = 'outer')
    for col in ['opened_issues','issue_comments']:
        df_activeuser_activities.loc[df_activeuser_activities.query(f'{col} == 0').index, f'share_{col}'] = np.nan
        df_activeuser_activities.loc[df_activeuser_activities.query(f'{col} != 0 & share_{col}.isna()').index, f'share_{col}'] = 0
    
    share_maintainer = df_ranked_activity_share.query('rank != "developer" & rank != "active user"')\
        .groupby(level = [0, 1])[['share_commits', 'share_pr']].sum()
    total_maintainer_activities = df_ranked_activity_share.groupby(level = [0, 1])[['commits','pr']].sum()
    df_maintainer_activities = share_maintainer.join(total_maintainer_activities, how = 'left')
    for col in ['commits','pr']:
        df_maintainer_activities.loc[df_maintainer_activities.query(f'{col} == 0').index, f'share_{col}'] = np.nan
        df_maintainer_activities.loc[df_maintainer_activities.query(f'{col} != 0 & share_{col}.isna()').index, f'share_{col}'] = 0
    
    df_shares = df_activeuser_activities.join(df_maintainer_activities, how = 'outer')
    no_info = df_shares.parallel_apply(lambda x: x.fillna(0).sum() == 0, axis = 1)
    df_shares = df_shares[~no_info]
    
    # WEIGHTS FROM GORTMAKER
    df_shares['opened_issues_hr'] = df_shares['opened_issues'] * .5
    df_shares['issue_comments_hr'] = df_shares['issue_comments'] * .07
    df_shares['commits_hr'] = df_shares['commits'] * .10
    df_shares['pr_hr'] = df_shares['pr'] * .77
    hour_cols = ['opened_issues_hr','issue_comments_hr','commits_hr','pr_hr']
    share_cols = [f"share_{col.replace("_hr","")}" for col in hour_cols] 
    df_shares['project_hierarchy_rank'] = (df_shares[hour_cols].fillna(0).values * df_shares[share_cols].fillna(0).values).sum(axis = 1)/df_shares[hour_cols].sum(axis = 1)
    df_shares['active_user_hierarchy_rank'] = (df_shares[hour_cols[:2]].fillna(0).values * df_shares[share_cols[:2]].fillna(0).values).sum(axis = 1)/df_shares[hour_cols[:2]].sum(axis = 1)
    df_shares['developer_hierarchy_rank'] = (df_shares[hour_cols[2:]].fillna(0).values * df_shares[share_cols[2:]].fillna(0).values).sum(axis = 1)/df_shares[hour_cols[2:]].sum(axis = 1)
    df_hierarchy = df_shares.reset_index()
    
    return df_hierarchy


if __name__ == '__main__':
    Main()


