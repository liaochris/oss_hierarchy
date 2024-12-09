
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
from source.lib.helpers import *

warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', None)
pandarallel.initialize(progress_bar = True)

def Main():
    indir_committers_info = Path('drive/output/scrape/link_committers_profile')
    indir_committers_departure = Path('drive/output/derived/contributor_stats/departed_contributors')
    indir_committers_rank = Path('drive/output/derived/contributor_stats/contributor_rank_hierarchy')
    indir_project_outcomes = Path('drive/output/derived/project_outcomes')
    outdir_panel = indir_project_outcomes

    time_period = int(sys.argv[1])
    rolling_window = int(sys.argv[2])
    criteria_col = sys.argv[3]
    criteria_pct = int(sys.argv[4])
    general_pct = int(sys.argv[5])
    consecutive_periods = int(sys.argv[6])
    post_period_length = int(sys.argv[7])
    decline_type = sys.argv[8]
    decline_stat = float(sys.argv[9])

    departure_candidates_rank_rolling = GenerateDepartureCandidateRank(indir_committers_departure, indir_committers_rank, time_period, rolling_window, consecutive_periods, post_period_length, criteria_col, criteria_pct, general_pct, decline_type, decline_stat)
    df_repo_panel_stats_hierarchy = GenerateRepoHierarchyStats(indir_project_outcomes, indir_committers_rank, time_period, rolling_window)
    df_repo_sample = ConstructRepoContributorPanel(departure_candidates_rank_rolling, df_repo_panel_stats_hierarchy, time_period)
    df_repo_sample.to_csv(outdir_panel / f'project_panel_major_months{time_period}_window{rolling_window}D_criteria_{criteria_col}_{criteria_pct}pct_general{general_pct}pct_consecutive{consecutive_periods}_post_period{post_period_length}{decline_type}_{decline_stat}.csv') 

def ConstructRepoContributorPanel(departure_candidates_rank_rolling, df_repo_panel_stats_hierarchy, time_period):
    df_repo_sample = FilterForTreatedOnce(departure_candidates_rank_rolling, df_repo_panel_stats_hierarchy)
    df_repo_sample = AddCovariates(df_repo_sample)
    df_repo_sample = AddAlwaysActiveIndicators(df_repo_sample, time_period)
    return df_repo_sample


def FilterForTreatedOnce(departure_candidates_rank_rolling, df_repo_panel_stats_hierarchy):
    repo_appears_once = departure_candidates_rank_rolling[['repo_name','actor_id']].drop_duplicates()\
        ['repo_name'].value_counts().reset_index()\
        .query('count==1')\
        ['repo_name'].tolist()
    departure_repos = departure_candidates_rank_rolling['repo_name'].unique().tolist()
    all_repos = df_repo_panel_stats_hierarchy['repo_name'].unique().tolist()
    repo_never_appears = [repo for repo in all_repos if repo not in departure_repos]
    df_repo_one_treatment = df_repo_panel_stats_hierarchy[
        df_repo_panel_stats_hierarchy['repo_name'].isin(repo_never_appears + repo_appears_once)]
    treated_date = departure_candidates_rank_rolling[departure_candidates_rank_rolling['repo_name'].isin(repo_appears_once)]\
        .query('time_period == final_period')\
        [['repo_name','final_period','rank','rank_ffilled','rolling_3period_rank','rolling_6period_rank']].drop_duplicates()
    df_repo_sample = pd.merge(df_repo_one_treatment, treated_date, how = 'left', on = ['repo_name'])
    return df_repo_sample

def AddCovariates(df_repo_sample):
    for col in [['opened_issues','closed_issues','opened_prs']]:
        df_repo_sample[col] = df_repo_sample[col].fillna(0)
    df_repo_sample['treatment'] = df_repo_sample.parallel_apply(
        lambda x: 0 if pd.isnull(x['final_period']) else int(x['time_period']>x['final_period']), axis = 1)
    df_repo_sample['active_all'] = df_repo_sample.apply(
        lambda x: x['opened_issues']>0 and x['opened_prs']>0, axis = 1).astype(int)
    df_repo_sample['mean_activity_all'] = df_repo_sample.groupby('repo_name')['active_all'].transform('mean')
    df_repo_sample['periods_all'] = df_repo_sample.groupby('repo_name')['active_all'].transform('sum')
    time_index_dict_rev = df_repo_sample['time_period'].sort_values().drop_duplicates().reset_index(drop = True).to_dict()
    time_index_dict = {v: k for (k, v) in time_index_dict_rev.items()}
    df_repo_sample['time_index'] = df_repo_sample['time_period'].apply(lambda x: time_index_dict[x])
    return df_repo_sample


def AddAlwaysActiveIndicators(df_repo_sample, time_period):
    active_2019onwards = df_repo_sample.query('time_period>="2019-01-01" & time_period < "2023-07-01"')\
        .groupby('repo_name')[['active_all']].sum().query(f'active_all == {54/time_period}').index.tolist()
    active_2018onwards = df_repo_sample.query('time_period>="2018-01-01" & time_period < "2023-07-01"')\
        .groupby('repo_name')[['active_all']].sum().query(f'active_all  == {66/time_period}').index.tolist()
    active_2017onwards = df_repo_sample.query('time_period>="2017-01-01" & time_period < "2023-07-01"')\
        .groupby('repo_name')[['active_all']].sum().query(f'active_all == {78/time_period}').index.tolist()
    df_repo_sample['2017_sample'] = df_repo_sample['repo_name'].isin(active_2017onwards).astype(int)
    df_repo_sample['2018_sample'] = df_repo_sample['repo_name'].isin(active_2018onwards).astype(int)
    df_repo_sample['2019_sample'] = df_repo_sample['repo_name'].isin(active_2019onwards).astype(int)
    return df_repo_sample


def GetConsecutiveSum(df):
    gb = df.groupby((df['active_all'] != df['active_all'].shift()).cumsum())
    df['consecutive_periods'] = gb['periods_all'].cumsum()
    df.loc[df['active_all'] == 0, 'consecutive_periods'] = 0
    return df

def GenerateDepartureCandidateRank(indir_committers_departure, indir_committers_rank, time_period, rolling_window, consecutive_periods, post_period_length, criteria_col, criteria_pct, general_pct, decline_type, decline_stat):
    departure_candidates = pd.read_parquet(indir_committers_departure / f'contributors_major_months{time_period}_window{rolling_window}D_criteria_{criteria_col}_{criteria_pct}pct_general{general_pct}pct_consecutive{consecutive_periods}_post_period{post_period_length}_{decline_type}_{decline_stat}.parquet')
    df_committers_rank = pd.read_parquet(indir_committers_rank / f'contributor_rank_major_months{time_period}_window{rolling_window}D.parquet')
    departure_candidates_rank = pd.merge(departure_candidates, df_committers_rank[['repo_name','actor_id','time_period','user_type','rank']],
                                        how = 'left')
    departure_candidates_rank['rank_ffilled'] = departure_candidates_rank.groupby(['repo_name','actor_id'])['rank'].ffill()
    rank_dict = { 'active user': 1, 'developer': 2, 'maintainer': 3}
    rank_dict_inv = {v: k for k, v in rank_dict.items()}
    departure_candidates_rank['rank_numeric'] = departure_candidates_rank['rank'].parallel_apply(lambda x: rank_dict.get(x, np.nan))
    departure_candidates_rank_rolling = departure_candidates_rank
    for periods in [3, 6]:
        rolling_rank = departure_candidates_rank_rolling.groupby(['repo_name','actor_id']).rolling(periods, min_periods = 1)\
            ['rank_numeric'].max()\
            .rename(f'rolling_{periods}period_rank_numeric')\
            .reset_index().set_index('level_2')\
            .drop(['repo_name','actor_id'],axis = 1)
        departure_candidates_rank_rolling = departure_candidates_rank_rolling.join(rolling_rank, how = 'left')
        departure_candidates_rank_rolling[f'rolling_{periods}period_rank'] = departure_candidates_rank_rolling[f'rolling_{periods}period_rank_numeric'].parallel_apply(lambda x: rank_dict_inv.get(x, np.nan))
    return departure_candidates_rank_rolling

def GenerateRepoHierarchyStats(indir_project_outcomes, indir_committers_rank, time_period, rolling_window):
    df_repo_panel_stats = pd.read_parquet(indir_project_outcomes / f'project_outcomes_major_months{time_period}.parquet')
    df_shares = pd.read_parquet(indir_committers_rank / f'hierarchy_measure_major_months{time_period}_window{rolling_window}D.parquet')
    df_repo_panel_stats_hierarchy = pd.merge(df_repo_panel_stats, 
                                            df_shares[['repo_name','time_period','project_hierarchy_rank','active_user_hierarchy_rank','developer_hierarchy_rank']],
                                            how = 'left')
    return df_repo_panel_stats_hierarchy

if __name__ == '__main__':
    Main()



