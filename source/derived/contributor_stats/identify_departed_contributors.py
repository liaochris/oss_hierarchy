import pandas as pd
import os
import glob
import numpy as np
from pathlib import Path
from ast import literal_eval
from pandarallel import pandarallel
import concurrent.futures
import itertools
import sys
import time

pd.set_option('display.max_columns', None)


def Main():
    pandarallel.initialize(progress_bar = True)
    indir = Path('drive/output/derived/contributor_stats/contributor_data')
    indir_truck = Path('drive/output/scrape/get_weekly_truck_factor')
    indir_committers = Path('drive/output/scrape/link_committers_profile')
    outdir = Path('drive/output/derived/contributor_stats/departed_contributors')
    
    criteria_col_list = ['comments', 'issue_comments', 'commits', 'pr_review_comments', 'pr_commits',
                         'commits_lt100','pr_commits_lt100']
    
    criteria_pct_list = [75, 90]
    consecutive_periods_major_months_dict = {2: [3, 9, 18],
                                            3: [3, 6, 12],
                                            6: [2, 3, 4, 5, 6]}

    post_periods_major_months_dict = {2: [3, 6, 12],
                                    3: [2, 4, 8],
                                    6: [2, 4]}
    decline_type_list = ['threshold_mean', 'threshold_gap_qty'] #['threshold_mean','threshold_pct']
    decline_threshold_list = [0.2, 0.1, 0]
    decline_gap_qty_list = [0, 5]

    time_period = int(sys.argv[1])
    rolling_window = int(sys.argv[2])
    df_truckfactors_uq = GetUniqueTruckFactor(indir_truck)
    committers_merge_map = CleanCommittersInfo(indir_committers)
    truckfactor_factor_id = pd.merge(df_truckfactors_uq, committers_merge_map, how = 'left', left_on = ['repo','authors_list'], right_on = ['repo','name'])
    truck_indivs = truckfactor_factor_id[['repo','commit_author_id']].rename({'commit_author_id':'actor_id'}, axis = 1).drop_duplicates()

    identifiers = ['repo_name','actor_id','time_period']

    df_contributors = GetContributorData(indir, time_period, rolling_window)
    
    num_contributors = ContributorCount(df_contributors)

    summary_cols = ['time_period','rolling_window','criteria_col','criteria_pct',
            'consecutive_periods','post_period_length','decline_type','decline_stat',
            'total_contributors','total_contributors_consecutive_criteria', 'total_major_contributors','total_final_candidates', 
            'num_total_projects','num_projects_with_one_departure','truck_factor_pct_departure','truck_factor_pct_all']
    df_contributor_stats = pd.DataFrame(columns = summary_cols)

    for criteria_col in criteria_col_list:
        for criteria_pct in criteria_pct_list:
            criteria_analysis_col = f'{criteria_col}_{criteria_pct}th_pct'
            criteria_cols = [criteria_col, criteria_analysis_col]
            analysis_cols = identifiers + criteria_cols
            df_analysis = df_contributors[analysis_cols]
            
            potential_major_contributors = df_analysis.query(f'{criteria_col}>{criteria_analysis_col}')\
                [['actor_id','repo_name']].drop_duplicates()
            consecutive_periods_list = consecutive_periods_major_months_dict[time_period]
            post_period_length_list = post_periods_major_months_dict[time_period]

            df_potential = ImputeConsecutivePeriods(df_analysis, potential_major_contributors, criteria_col, criteria_analysis_col)
            for consecutive_periods in consecutive_periods_list:
                has_consecutive_periods = df_potential.query(f'consecutive_periods>={consecutive_periods}')[['actor_id','repo_name']].drop_duplicates()
                df_potential_consecutive = pd.merge(df_potential, has_consecutive_periods)
                num_consecutive_periods = ContributorCount(df_potential_consecutive)
                
                major_contributors = df_potential_consecutive[['actor_id','repo_name', 'consecutive_periods', 'time_period']]\
                    .sort_values('consecutive_periods', ascending = False)\
                    .drop_duplicates(['actor_id','repo_name'])\
                    .reset_index(drop = True)\
                    .rename({'time_period':'final_period'}, axis = 1)
                
                for post_period_length in post_period_length_list:
                    if post_period_length <= consecutive_periods:
                        for decline_type in decline_type_list:
                            decline_stat_list = decline_threshold_list if decline_type == "threshold_mean" else decline_gap_qty_list
                            for decline_stat in decline_stat_list:

                                make_major_contributors = 0
                                if post_period_length == min(post_period_length_list) and decline_type == decline_type_list[0] and decline_stat == min(decline_stat_list):
                                    make_major_contributors = 1

                                print("analyzing potential departure candidates")
                                df_candidates = GetCandidates(major_contributors, df_potential_consecutive, post_period_length, criteria_analysis_col, criteria_col, decline_type, decline_stat)
                                filename = f'departed_contributors_major_months{time_period}_window{rolling_window}D_criteria_{criteria_col}_{criteria_pct}pct_consecutive{consecutive_periods}_post_period{post_period_length}'
                                decline_suffix = f"_{decline_type}_{decline_stat}.parquet" 
                                filename = filename + decline_suffix
                                df_candidates.to_parquet(outdir / filename)

                                if make_major_contributors == 1:
                                    filename = f'major_contributors_major_months{time_period}_window{rolling_window}D_criteria_{criteria_col}_{criteria_pct}pct_consecutive{consecutive_periods}.parquet'
                                    major_contributors.to_parquet(outdir / filename)

                                if df_candidates.shape[0] == 0:
                                    num_final_contributors = repo_count = uq_candidates = uq_candidates_all = one_departure_repos = pct_truck_factor_dep = pct_truck_factor = 0
                                else:
                                    num_final_contributors = ContributorCount(df_candidates)
                                    repo_count = df_candidates[['repo_name']].drop_duplicates().shape[0]
                                    uq_candidates = df_candidates[['actor_id','repo_name']].drop_duplicates()
                                    uq_candidates_all = major_contributors[['actor_id','repo_name']].drop_duplicates()
                                    one_departure_repos = np.sum(uq_candidates['repo_name'].value_counts()==1)

                                    pct_truck_factor_dep = GetTruckFactorPct(df_candidates, truck_indivs)
                                    pct_truck_factor = GetTruckFactorPct(major_contributors, truck_indivs)
                                
                                append_index = df_contributor_stats.shape[0]
                                df_contributor_stats.loc[append_index, summary_cols] = \
                                    [time_period, rolling_window, criteria_col, criteria_pct, consecutive_periods,
                                    post_period_length, decline_type, decline_stat, num_contributors, num_consecutive_periods,
                                    uq_candidates_all.shape[0], num_final_contributors, repo_count, one_departure_repos, pct_truck_factor_dep, pct_truck_factor]

                                print(f"exported {filename}")
    df_contributor_stats.to_csv(outdir / f'departed_contributors_specification_summary_major_months{time_period}_window{rolling_window}D.csv', index = False)


def GetContributorData(indir, time_period, rolling_window):
    df_contributors = pd.read_parquet(indir / f'major_contributors_major_months{time_period}_window{rolling_window}D_samplefull.parquet')
    last_tp = df_contributors['time_period'].max()
    df_contributors = df_contributors[df_contributors['time_period']<last_tp]
    return df_contributors
    
def ContributorCount(df):
    return df[['actor_id','repo_name']].drop_duplicates().shape[0]

def ImputeConsecutivePeriods(df_analysis, potential_major_contributors, criteria_col, criteria_analysis_col):
    df_potential = pd.merge(df_analysis, potential_major_contributors, on = ['actor_id','repo_name'])

    all_actors = df_potential[['repo_name','actor_id']].drop_duplicates()
    time_periods_uq = df_potential['time_period'].unique().tolist()
    all_actors['time_period'] = [time_periods_uq for i in range(all_actors.shape[0])]
    all_actors = all_actors.explode('time_period')
    df_potential['first_period'] = df_potential.groupby(['repo_name','actor_id'])['time_period'].transform('min')
    df_potential = pd.merge(all_actors, df_potential, how = 'left')
    df_potential.sort_values(['repo_name','actor_id','time_period'],inplace = True)
    df_potential['grouped_index'] = df_potential.groupby(['repo_name','actor_id']).cumcount()+1
    df_potential[['first_period']] = df_potential.groupby(['repo_name','actor_id'])[['first_period']].ffill().bfill()
    df_potential = df_potential.query('time_period >= first_period')
    df_potential[criteria_col] = df_potential[criteria_col].fillna(0)

    df_potential = df_potential.assign(criteria_exceed = (df_potential[criteria_col]>df_potential[criteria_analysis_col]).astype(int))
    df_potential['criteria_exceed'] = df_potential['criteria_exceed'].fillna(0)
    df_potential = df_potential.sort_values(['actor_id', 'repo_name', 'time_period'])
    group_keys = (
        (df_potential['actor_id'] != df_potential['actor_id'].shift()) |
        (df_potential['repo_name'] != df_potential['repo_name'].shift()) |
        (df_potential['criteria_exceed'] == 0)
    ).cumsum()
    df_potential['consecutive_periods'] = df_potential.groupby(group_keys)['criteria_exceed'].cumsum()
    df_potential.loc[df_potential['criteria_exceed'] == 0, 'consecutive_periods'] = 0
    return df_potential

def GetCandidates(major_contributors, df_potential_consecutive, post_period_length, criteria_analysis_col, criteria_col, decline_type, decline_stat):
    df_candidates_all = pd.merge(df_potential_consecutive, major_contributors.rename({'consecutive_periods':'total_consecutive_periods'}, axis = 1))
    df_candidates_all.sort_values(['repo_name','actor_id','time_period'], inplace = True)    

    df_candidates_all['post_final_period'] = df_candidates_all['time_period']>df_candidates_all['final_period']
    df_candidates_all['sum_post_final_period'] = df_candidates_all.groupby(['repo_name','actor_id'])['post_final_period'].transform('sum')

    if decline_type == "threshold_mean":
        df_candidates_all = df_candidates_all.query(f'sum_post_final_period>={post_period_length}')
        df_candidates_all['active_indicator'] = (df_candidates_all[criteria_analysis_col].notna()) & (df_candidates_all['post_final_period'])
        df_candidates_all['sum_active'] = df_candidates_all.groupby(['repo_name','actor_id'])['active_indicator'].transform('sum')
        df_candidates_all = df_candidates_all.query('sum_active>0')
        df_candidates_all['grouped_index'] = df_candidates_all.groupby(['repo_name','actor_id']).cumcount()+1
        df_candidates_final_index = df_candidates_all.query('consecutive_periods == total_consecutive_periods')[['repo_name','actor_id','grouped_index']]
        df_candidates_final_index['final_index'] = df_candidates_final_index['grouped_index']
        df_candidates_all = pd.merge(df_candidates_all, df_candidates_final_index.drop('grouped_index', axis = 1))

        prior_periods = df_candidates_all.query("time_period <= final_period & final_index-grouped_index < total_consecutive_periods").groupby(['repo_name','actor_id'])[criteria_col].mean().reset_index()
        post_periods = df_candidates_all.query(f"time_period > final_period & grouped_index - final_index <= {post_period_length}").groupby(['repo_name','actor_id'])[criteria_col].mean().reset_index()

        df_candidates_all = df_candidates_all.merge(post_periods.rename({criteria_col:f'post_periods_{criteria_col}'}, axis = 1))\
            .merge(prior_periods.rename({criteria_col:f'prior_periods_{criteria_col}'}, axis = 1))

        df_candidates = df_candidates_all.query(f"post_periods_{criteria_col} <= prior_periods_{criteria_col} * {decline_stat}")
    elif decline_type == "threshold_gap_qty":      
        # max gap is post_period  
        gap_range = post_period_length
        threshold_qty = decline_stat

        df_candidates_all['below_qty'] = df_candidates_all[criteria_col]<=threshold_qty
        final_period_index = df_candidates_all.query('time_period > final_period').groupby(['repo_name','actor_id']).head(1)\
            [['repo_name','actor_id','grouped_index']].rename({'grouped_index':'first_post_period_index'}, axis = 1)
        df_candidates_all = pd.merge(df_candidates_all, final_period_index)
        df_candidates_all.sort_values(['repo_name','actor_id','time_period'],inplace = True)

        for gap in range(gap_range+1):
            df_candidates_gap = df_candidates_all.query(
                f'grouped_index>=first_post_period_index+{gap}').groupby(['repo_name','actor_id'])['below_qty'].mean()\
                .reset_index().rename({'below_qty':f'below_qty_mean_gap{gap}'}, axis = 1)
            df_candidates_all = pd.merge(df_candidates_all, df_candidates_gap)
        df_candidates_all[[col for col in df_candidates_all.columns if "gap" in col]]
        keep = df_candidates_all[[col for col in df_candidates_all.columns if "gap" in col]].apply(lambda x: any(x==1), axis = 1)
        df_candidates = df_candidates_all[keep]

    df_candidates = PostProcessCleaning(df_candidates)

    return df_candidates

def PostProcessCleaning(df_candidates):
    dropcols = ['post_final_period', 'sum_post_final_period', 'first_post_period_index']
    df_candidates = df_candidates.drop([col for col in dropcols if col in df_candidates.columns], axis = 1)
    df_candidates['consecutive_periods'] = df_candidates['consecutive_periods'].fillna(0)
    df_candidates['total_consecutive_periods'] = df_candidates.groupby(['repo_name','actor_id'])['total_consecutive_periods'].transform('ffill')
    df_candidates['total_consecutive_periods'] = df_candidates.groupby(['repo_name','actor_id'])['total_consecutive_periods'].transform('bfill')
    return df_candidates

def GetUniqueTruckFactor(indir_truck):
    df_truckfactor = pd.read_csv(indir_truck / 'truckfactor.csv')
    df_truckfactor['repo'] = df_truckfactor['repo_name'].apply(lambda x: str(x).replace('drive/output/scrape/get_weekly_truck_factor/truckfactor_','').replace('.csv','').replace("_","/",1))
    df_truckfactors_uq = df_truckfactor[['repo','authors']].drop_duplicates().dropna()
    df_truckfactors_uq['authors_list'] = df_truckfactors_uq['authors'].apply(lambda x: x.split(" | "))
    df_truckfactors_uq = df_truckfactors_uq.explode('authors_list')[['repo','authors_list']].drop_duplicates()
    return df_truckfactors_uq

def CleanCommittersInfo(indir_committers):
    df_committers_info = pd.concat([pd.read_csv(indir_committers / 'committers_info_pr.csv', index_col = 0).dropna(),
                                    pd.read_csv(indir_committers / 'committers_info_push.csv', index_col = 0).dropna()])
    df_committers_info['committer_info'] = df_committers_info['committer_info'].apply(literal_eval)
    df_committers_info['commit_repo'] = df_committers_info['commit_repo'].apply(literal_eval)
    # TODO: handle cleaning so that it can handle the other cases
    df_committers_info = df_committers_info[df_committers_info['committer_info'].apply(lambda x: len(x)==4)]
    df_committers_info['repo'] = df_committers_info['commit_repo'].apply(lambda x: list(set([ele.split("_")[1] for ele in x])))
    df_committers_info = df_committers_info.explode('repo')
    df_committers_info['actor_name'] = df_committers_info['committer_info'].apply(lambda x: x[0])
    df_committers_info['actor_id'] = df_committers_info['committer_info'].apply(lambda x: x[1])
    committers_match = df_committers_info[['name','email','user_type','actor_name','actor_id','repo']].drop_duplicates()
    committers_match.rename({'actor_id':'commit_author_id'}, axis = 1, inplace = True)
    return committers_match

def GetTruckFactorPct(df_contributors, truck_indivs):
    uq_contributors = df_contributors[['actor_id','repo_name']].drop_duplicates()
    all_candidate_repos = df_contributors['repo_name'].unique().tolist()
    truck_indivs_repo = truck_indivs[truck_indivs['repo'].isin(all_candidate_repos)]
    truck_indivs_repo['actor_id'] = pd.to_numeric(truck_indivs_repo['actor_id'])
    truck_merged = pd.merge(truck_indivs_repo, uq_contributors.assign(presence=1), how = 'left', left_on = ['repo','actor_id'], right_on = ['repo_name','actor_id'])
    pct_truck_factor = np.mean(truck_merged['presence'].notna())
    return pct_truck_factor

if __name__ == '__main__':
    Main()


