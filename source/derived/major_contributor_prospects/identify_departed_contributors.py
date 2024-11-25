import pandas as pd
import os
import glob
import numpy as np
from pathlib import Path
from ast import literal_eval
from pandarallel import pandarallel
import concurrent.futures
import itertools

pd.set_option('display.max_columns', None)


def Main():
    pandarallel.initialize(progress_bar = True)
    indir = Path('drive/output/derived/major_contributor_prospects/contributor_data')
    indir_truck = Path('drive/output/scrape/get_weekly_truck_factor')
    indir_committers = Path('drive/output/scrape/link_committers_profile')
    outdir = Path('drive/output/derived/major_contributor_prospects/departed_contributors')
    
    major_months_list = [2]
    rolling_window_list = [367, 732, 1828] 
    consecutive_periods_major_months_dict = {2: [3, 9, 18],
                                             3: [3, 6, 12],
                                             6: [3, 6]}
    issue_col = 'issue_comments'
    commit_col = 'commits'
    criteria_col_list = [issue_col, commit_col]  
    criteria_pct_list = [75, 90]
    general_pct_list = [25] 
    
    post_periods_major_months_dict = {2: [3, 6, 12],
                                      3: [2, 4, 8],
                                      6: [2, 4]}
    decline_threshold_list = [.1, .2] 
    decline_pct_list = [.1, .25]  
    
    df_truckfactors_uq = GetUniqueTruckFactor(indir_truck)
    committers_merge_map = CleanCommittersInfo(indir_committers)
    truckfactor_factor_id = pd.merge(df_truckfactors_uq, committers_merge_map, how = 'left', left_on = ['repo','authors_list'], right_on = ['repo','name'])
    truck_indivs = truckfactor_factor_id[['repo','commit_author_id']].rename({'commit_author_id':'actor_id'}, axis = 1).drop_duplicates()

    identifiers = ['repo_name','actor_id','time_period']
    
    df_contributor_stats = pd.DataFrame(columns = ['major_months','window','criteria_col','criteria_pct','general_pct',
                                                   'consecutive_periods','post_period_length','decline_type','decline_stat',
                                                   'total_contributors','total_contributors_consecutive_criteria','total_final_candidates',
                                                   'num_total_projects','num_projects_with_one_departure', 'truck_factor_pct'])
    
    for major_months in major_months_list:
        for window in rolling_window_list:
            df_contributors = GetContributorData(indir, major_months, window)
            num_contributors = ContributorCount(df_contributors)
            for criteria_col in criteria_col_list:
                for criteria_pct in criteria_pct_list:
                    for general_pct in general_pct_list: 
                        criteria_analysis_col = f'{criteria_col}_{criteria_pct}th_pct'
                        criteria_general_col = f'general_{criteria_col}_{general_pct}th_pct'
                            
                        criteria_cols = [criteria_col, criteria_analysis_col, criteria_general_col]
                        criteria_analysis_decline_cols = [f'{criteria_col}_{int(100*decline_pct)}th_pct' for decline_pct in decline_pct_list]
                        criteria_cols.extend(criteria_analysis_decline_cols)

                        analysis_cols = identifiers + criteria_cols
                        df_analysis = df_contributors[analysis_cols]
    
                        potential_major_contributors = df_analysis.query(f'{criteria_col}>{criteria_analysis_col} & {criteria_col}>{criteria_general_col}')\
                            [['actor_id','repo_name']].drop_duplicates()
                        df_potential = pd.merge(df_analysis, potential_major_contributors, on = ['actor_id','repo_name'])
                        df_potential = df_potential.assign(criteria_exceed = (df_potential[criteria_col]>df_potential[criteria_analysis_col]).astype(int))
                        df_potential = df_potential.groupby(['actor_id','repo_name']).parallel_apply(GetConsecutiveSum).reset_index(drop = True)
    
                        consecutive_periods_list = consecutive_periods_major_months_dict[major_months]
                        for consecutive_periods in consecutive_periods_list:
                            has_consecutive_periods = df_potential.query(f'consecutive_periods>={consecutive_periods}')[['actor_id','repo_name']].drop_duplicates()
                            df_potential_consecutive = pd.merge(df_potential, has_consecutive_periods)
                            num_consecutive_periods = ContributorCount(df_potential_consecutive)
    
                            departure_candidates = df_potential_consecutive[['actor_id','repo_name', 'consecutive_periods', 'time_period']]\
                                .sort_values('consecutive_periods', ascending = False)\
                                .drop_duplicates(['actor_id','repo_name'])\
                                .reset_index(drop = True)\
                                .rename({'time_period':'final_period'}, axis = 1)
                            
                            post_period_length_list = post_periods_major_months_dict[major_months]
                            for post_period_length in post_period_length_list:
                                for decline_type in ['threshold_mean','threshold_pct']:
                                    decline_stat_list = decline_threshold_list if decline_type == "threshold_mean" else decline_pct_list
                                    for decline_stat in decline_stat_list:
                                        print("analyzing potential departure candidates")
                                        df_candidates = ParalellizeGetCandidates(departure_candidates, df_potential_consecutive, post_period_length, criteria_analysis_col, criteria_col, decline_type, decline_stat)
                                        if type(df_candidates) == type(None):
                                            continue
                                        num_final_contributors = ContributorCount(df_candidates)
    
                                        append_index = df_contributor_stats.shape[0]
                                        repo_count = df_candidates[['repo_name']].drop_duplicates().shape[0]
                                        uq_candidates = df_candidates[['actor_id','repo_name']].drop_duplicates()
                                        major_contributors = df_potential_consecutive[['actor_id','repo_name']].drop_duplicates()
                                        one_departure_repos = np.sum(uq_candidates['repo_name'].value_counts()==1)
                                        
                                        all_candidate_repos = df_candidates['repo_name'].unique().tolist()
                                        truck_indivs_repo = truck_indivs[truck_indivs['repo'].isin(all_candidate_repos)]
                                        truck_indivs_repo['actor_id'] = pd.to_numeric(truck_indivs_repo['actor_id'])
                                        truck_merged = pd.merge(truck_indivs_repo, uq_candidates.assign(presence=1), how = 'left', left_on = ['repo','actor_id'], right_on = ['repo_name','actor_id'])
                                        pct_truck_factor = 1-np.mean(truck_merged['presence'].isna())
                                        pct_truck_factor = 1-np.mean(truck_merged['presence'].isna())
                                        
                                        df_contributor_stats.loc[append_index, 
                                            ['major_months','window','criteria_col','criteria_pct','general_pct',
                                             'consecutive_periods','post_period_length','decline_type','decline_stat',
                                             'total_contributors','total_contributors_consecutive_criteria','total_final_candidates',
                                             'num_total_projects','num_projects_with_one_departure','truck_factor_pct']] = \
                                            [major_months, window, criteria_col, criteria_pct, general_pct, consecutive_periods,
                                             post_period_length, decline_type, decline_stat, num_contributors, num_consecutive_periods,
                                             num_final_contributors, repo_count, one_departure_repos, pct_truck_factor]
    
                                        filename = f'contributors_major_months{major_months}_window{window}D_criteria_{criteria_col}_{criteria_pct}pct_general{general_pct}pct_consecutive{consecutive_periods}_post_period{post_period_length}'
                                        decline_suffix = f"threshold_mean_{decline_stat}" if decline_type == 'threshold_mean' else f"threshold_pct_{decline_stat}"
                                        decline_suffix = f"{decline_suffix}.parquet"
                                        filename = filename + decline_suffix
                                        df_candidates.to_parquet(outdir / filename)
                                        print(f"exported {filename}")
    
        df_contributor_stats.to_csv(outdir / f'contributor_stats_summary_major_months{major_months}.csv')

def GetContributorData(indir, major_months, window):
    df_contributors = pd.read_parquet(indir / f'major_contributors_major_months{major_months}_window{window}D_samplefull.parquet')
    return df_contributors
    
def ContributorCount(df):
    return df[['actor_id','repo_name']].drop_duplicates().shape[0]

def GetConsecutiveSum(df):
    gb = df.groupby((df['criteria_exceed'] != df['criteria_exceed'].shift()).cumsum())
    df['consecutive_periods'] = gb['criteria_exceed'].cumsum()
    df.loc[df['criteria_exceed'] == 0, 'consecutive_periods'] = 0
    return df

def ProcessCandidate(i, departure_candidates, df_potential_consecutive, post_period_length, criteria_analysis_col, criteria_col, decline_type, decline_stat):
    final_period = departure_candidates.loc[i, 'final_period']
    total_consecutive_periods = departure_candidates.loc[i, 'consecutive_periods']
    repo_name = departure_candidates.loc[i, 'repo_name']
    actor_id = departure_candidates.loc[i, 'actor_id']
    df_potential_consecutive_subset = df_potential_consecutive[
        df_potential_consecutive.apply(
            lambda x: x['repo_name'] == repo_name and x['actor_id'] == actor_id, axis=1
        )
    ]
    df_candidate = pd.merge(df_potential_consecutive_subset, departure_candidates.loc[[i]].drop('consecutive_periods', axis=1))
    final_periods = df_candidate.query('time_period>final_period').head(post_period_length)
    prior_periods = df_candidate.query('time_period<=final_period').sort_values('time_period').tail(total_consecutive_periods)
    if final_periods.shape[0] >= post_period_length:
        if final_periods[criteria_analysis_col].isna().sum() != post_period_length:  # project is still active
            if decline_type == "threshold_mean":
                pre_period_mean = prior_periods[criteria_col].mean()
                post_period_mean = final_periods[criteria_col].mean()
                if pre_period_mean * decline_stat > post_period_mean:
                    return df_candidate
            if decline_type == "threshold_pct":
                decline_analysis_col = f'{criteria_col}_{int(decline_stat * 100)}th_pct'
                final_periods_fulfill = final_periods.query(f'{criteria_col}<{decline_analysis_col}')
                if final_periods_fulfill.shape[0] == post_period_length:
                    return df_candidate
    return None

def ParalellizeGetCandidates(departure_candidates, df_potential_consecutive, post_period_length, criteria_analysis_col, criteria_col, decline_type, decline_stat):
    with concurrent.futures.ProcessPoolExecutor(max_workers=12) as executor:
        results = list(executor.map(
            ProcessCandidate,
            departure_candidates.index.tolist(),
            itertools.repeat(departure_candidates),
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
    df_truckfactor = pd.concat([pd.read_csv(file).assign(repo_name = file) for file in indir_truck.glob('*.csv')]).drop('Unnamed: 0', axis = 1).reset_index(drop = True)
    df_truckfactor['repo'] = df_truckfactor['repo_name'].apply(lambda x: str(x).replace('drive/output/scrape/get_weekly_truck_factor/truckfactor_','').replace('.csv','').replace("_","/",1))
    df_truckfactors_uq = df_truckfactor[['repo','authors']].drop_duplicates().dropna()
    df_truckfactors_uq['authors_list'] = df_truckfactors_uq['authors'].apply(lambda x: x.split(" | "))
    df_truckfactors_uq = df_truckfactors_uq.explode('authors_list')[['repo','authors_list']].drop_duplicates()
    return df_truckfactors_uq

def GetUniqueCommitters(indir_committers):
    df_committers_profile = pd.concat([pd.read_csv(indir_committers / 'committers_info_push.csv', index_col = 0),
                                       pd.read_csv(indir_committers / 'committers_info_pr.csv', index_col = 0)]).reset_index(drop = True)
    # this is just a temporary quick solution - need a better solution
    df_committers_profile.dropna(inplace = True)
    df_committers_profile['committer_info'] = df_committers_profile['committer_info'].parallel_apply(ast.literal_eval)
    df_committers_profile['actor_id'] = df_committers_profile['committer_info'].apply(lambda x: x[1])
    committers_merge_map = df_committers_profile[['name','repo','actor_id']].drop_duplicates()
    return committers_merge_map


def CleanCommittersInfo(indir_committers_info):
    # TODO: edit file so it can handle pushes
    df_committers_info = pd.concat([pd.read_csv(indir_committers_info / 'committers_info_pr.csv', index_col = 0).dropna(),
                                    pd.read_csv(indir_committers_info / 'committers_info_push.csv', index_col = 0).dropna()])
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


if __name__ == '__main__':
    Main()


