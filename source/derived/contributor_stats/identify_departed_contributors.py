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

pd.set_option('display.max_columns', None)


def Main():
    pandarallel.initialize(progress_bar = True)
    indir = Path('drive/output/derived/contributor_stats/contributor_data')
    indir_truck = Path('drive/output/scrape/get_weekly_truck_factor')
    indir_committers = Path('drive/output/scrape/link_committers_profile')
    outdir = Path('drive/output/derived/contributor_stats/departed_contributors')
    
    global max_workers
    max_workers = 4

    time_period = int(sys.argv[1])
    rolling_window = int(sys.argv[2])
    criteria_col = sys.argv[3]
    criteria_pct = int(sys.argv[4])
    general_pct = int(sys.argv[5])
    consecutive_periods = int(sys.argv[6])
    post_period_length = int(sys.argv[7])
    decline_type = sys.argv[8]
    decline_stat = float(sys.argv[9])
    make_major_contributors = = int(sys.argv[10])

    df_truckfactors_uq = GetUniqueTruckFactor(indir_truck)
    committers_merge_map = CleanCommittersInfo(indir_committers)
    truckfactor_factor_id = pd.merge(df_truckfactors_uq, committers_merge_map, how = 'left', left_on = ['repo','authors_list'], right_on = ['repo','name'])
    truck_indivs = truckfactor_factor_id[['repo','commit_author_id']].rename({'commit_author_id':'actor_id'}, axis = 1).drop_duplicates()

    identifiers = ['repo_name','actor_id','time_period']

    df_contributors = GetContributorData(indir, time_period, rolling_window)
    num_contributors = ContributorCount(df_contributors)

    criteria_analysis_col = f'{criteria_col}_{criteria_pct}th_pct'
    criteria_general_col = f'general_{criteria_col}_{general_pct}th_pct'
                            
    criteria_cols = [criteria_col, criteria_analysis_col, criteria_general_col]
    if decline_type == 'threshold_pct':
        criteria_analysis_decline_cols = f'{criteria_col}_{int(100*decline_stat)}th_pct'
        criteria_cols.extend(criteria_analysis_decline_cols)

    analysis_cols = identifiers + criteria_cols
    df_analysis = df_contributors[analysis_cols]
    
    potential_major_contributors = df_analysis.query(f'{criteria_col}>{criteria_analysis_col} & {criteria_col}>{criteria_general_col}')\
        [['actor_id','repo_name']].drop_duplicates()
    df_potential = pd.merge(df_analysis, potential_major_contributors, on = ['actor_id','repo_name'])
    df_potential = df_potential.assign(criteria_exceed = (df_potential[criteria_col]>df_potential[criteria_analysis_col]).astype(int))
    df_potential = df_potential.groupby(['actor_id','repo_name']).parallel_apply(GetConsecutiveSum).reset_index(drop = True)

    has_consecutive_periods = df_potential.query(f'consecutive_periods>={consecutive_periods}')[['actor_id','repo_name']].drop_duplicates()
    df_potential_consecutive = pd.merge(df_potential, has_consecutive_periods)
    num_consecutive_periods = ContributorCount(df_potential_consecutive)
    
    major_contributors = df_potential_consecutive[['actor_id','repo_name', 'consecutive_periods', 'time_period']]\
        .sort_values('consecutive_periods', ascending = False)\
        .drop_duplicates(['actor_id','repo_name'])\
        .reset_index(drop = True)\
        .rename({'time_period':'final_period'}, axis = 1)
    
    
    print("analyzing potential departure candidates")
    df_candidates = ParalellizeGetCandidates(major_contributors, df_potential_consecutive, post_period_length, criteria_analysis_col, criteria_col, decline_type, decline_stat)
    if type(df_candidates) == type(None):
        exit

    num_final_contributors = ContributorCount(df_candidates)

    repo_count = df_candidates[['repo_name']].drop_duplicates().shape[0]
    uq_candidates = df_candidates[['actor_id','repo_name']].drop_duplicates()
    uq_candidates_all = major_contributors[['actor_id','repo_name']].drop_duplicates()
    one_departure_repos = np.sum(uq_candidates['repo_name'].value_counts()==1)

    pct_truck_factor_dep = GetTruckFactorPct(df_candidates, truck_indivs)
    pct_truck_factor = GetTruckFactorPct(major_contributors, truck_indivs)

    summary_cols = ['time_period','rolling_window','criteria_col','criteria_pct','general_pct',
            'consecutive_periods','post_period_length','decline_type','decline_stat',
            'total_contributors','total_contributors_consecutive_criteria','total_final_candidates',
            'num_total_projects','num_projects_with_one_departure','truck_factor_pct_departure','truck_factor_pct_all']
    
    if 'departed_contributors_specification_summary.csv' not in os.listdir(outdir):
        df_contributor_stats = pd.DataFrame(columns = summary_cols)
    else:
        df_contributor_stats = pd.read_csv(outdir / 'departed_contributors_specification_summary.csv')
    append_index = df_contributor_stats.shape[0]
    df_contributor_stats.loc[append_index, summary_cols] = \
        [time_period, rolling_window, criteria_col, criteria_pct, general_pct, consecutive_periods,
         post_period_length, decline_type, decline_stat, num_contributors, num_consecutive_periods,
        num_final_contributors, repo_count, one_departure_repos, pct_truck_factor_dep, pct_truck_factor]
    df_contributor_stats.to_csv(outdir / f'departed_contributors_specification_summary.csv')

    filename = f'departed_contributors_major_months{time_period}_window{rolling_window}D_criteria_{criteria_col}_{criteria_pct}pct_general{general_pct}pct_consecutive{consecutive_periods}_post_period{post_period_length}'
    decline_suffix = f"threshold_mean_{decline_stat}" if decline_type == 'threshold_mean' else f"threshold_pct_{decline_stat}"
    decline_suffix = f"{decline_suffix}.parquet"
    filename = filename + decline_suffix
    df_candidates.to_parquet(outdir / filename)

    if make_major_contributors == 1:
        filename = f'major_contributors_major_months{time_period}_window{rolling_window}D_criteria_{criteria_col}_{criteria_pct}pct_general{general_pct}pct_consecutive{consecutive_periods}.parquet'
        major_contributors.to_parquet(outdir / filename)
    
    print(f"exported {filename}")


def GetContributorData(indir, time_period, rolling_window):
    df_contributors = pd.read_parquet(indir / f'major_contributors_major_months{time_period}_window{rolling_window}D_samplefull.parquet')
    return df_contributors
    
def ContributorCount(df):
    return df[['actor_id','repo_name']].drop_duplicates().shape[0]

def GetConsecutiveSum(df):
    gb = df.groupby((df['criteria_exceed'] != df['criteria_exceed'].shift()).cumsum())
    df['consecutive_periods'] = gb['criteria_exceed'].cumsum()
    df.loc[df['criteria_exceed'] == 0, 'consecutive_periods'] = 0
    return df

def ProcessCandidate(i, major_contributors, df_potential_consecutive, post_period_length, criteria_analysis_col, criteria_col, decline_type, decline_stat):
    final_period = major_contributors.loc[i, 'final_period']
    total_consecutive_periods = major_contributors.loc[i, 'consecutive_periods']
    repo_name = major_contributors.loc[i, 'repo_name']
    actor_id = major_contributors.loc[i, 'actor_id']
    df_potential_consecutive_subset = df_potential_consecutive[
        df_potential_consecutive.apply(
            lambda x: x['repo_name'] == repo_name and x['actor_id'] == actor_id, axis=1
        )
    ]
    df_candidate = pd.merge(df_potential_consecutive_subset, major_contributors.loc[[i]].drop('consecutive_periods', axis=1))
    final_periods = df_candidate.query('time_period>final_period').head(post_period_length)
    prior_periods = df_candidate.query('time_period<=final_period').sort_values('time_period').tail(total_consecutive_periods)
    if final_periods.shape[0] >= post_period_length:
        if final_periods[criteria_analysis_col].isna().sum() != post_period_length:  # project is still active
            if decline_type == "threshold_mean":
                pre_period_mean = prior_periods[criteria_col].mean()
                post_period_mean = final_periods[criteria_col].mean()
                if pre_period_mean * decline_stat >= post_period_mean:
                    return df_candidate
            if decline_type == "threshold_pct":
                decline_analysis_col = f'{criteria_col}_{int(decline_stat * 100)}th_pct'
                final_periods_fulfill = final_periods.query(f'{criteria_col}<{decline_analysis_col}')
                if final_periods_fulfill.shape[0] == post_period_length:
                    return df_candidate
    return None

def ParalellizeGetCandidates(major_contributors, df_potential_consecutive, post_period_length, criteria_analysis_col, criteria_col, decline_type, decline_stat):
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(
            ProcessCandidate,
            major_contributors.index.tolist(),
            itertools.repeat(major_contributors),
            itertools.repeat(df_potential_consecutive),
            itertools.repeat(post_period_length),
            itertools.repeat(criteria_analysis_col),
            itertools.repeat(criteria_col),
            itertools.repeat(decline_type),
            itertools.repeat(decline_stat)
        ))
    df_candidate_values = [res for res in results if res is not None]
    if len(df_candidate_values)>0:
        df_candidates = pd.concat(df_candidate_values, ignore_index=True)
    else:
        df_candidates = None
    return df_candidates
    

def GetUniqueTruckFactor(indir_truck):
    df_truckfactor = pd.read_csv(indir_truck / 'truckfactor.csv')

    df_truckfactor['repo'] = df_truckfactor['repo_name'].apply(lambda x: str(x).replace('drive/output/scrape/get_weekly_truck_factor/truckfactor_','').replace('.csv','').replace("_","/",1))
    df_truckfactors_uq = df_truckfactor[['repo','authors']].drop_duplicates().dropna()
    df_truckfactors_uq['authors_list'] = df_truckfactors_uq['authors'].apply(lambda x: x.split(" | "))
    df_truckfactors_uq = df_truckfactors_uq.explode('authors_list')[['repo','authors_list']].drop_duplicates()
    return df_truckfactors_uq

def CleanCommittersInfo(indir_committers):
    # TODO: edit file so it can handle pushes
    df_committers_info = pd.concat([pd.read_csv(indir_committers / 'committers_info_pr.csv', index_col = 0).dropna(),
                                    pd.read_csv(indir_committers / 'committers_info_push.csv', index_col = 0).dropna()])
    df_committers_info['committer_info'] = df_committers_info['committer_info'].apply(literal_eval)
    df_committers_info['commit_repo'] = df_committers_info['commit_repo'].parallel_apply(literal_eval)
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
    pct_truck_factor = 1-np.mean(truck_merged['presence'].isna())
    return pct_truck_factor

if __name__ == '__main__':
    Main()


