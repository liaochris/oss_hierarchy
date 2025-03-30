import os
import pandas as pd
from pathlib import Path
from ast import literal_eval
import numpy as np
import wrds
pd.set_option('display.max_columns', None)
from rapidfuzz import fuzz
import re
from source.scrape.get_linkedin_profiles.docs.get_linkedin_profiles import *
from joblib import Parallel, delayed
from datetime import datetime
import textwrap

def Main():
    indir_locations = Path("drive/output/scrape/get_standardized_locations")
    indir_profiles = Path('drive/output/scrape/get_linkedin_profiles')
    indir_departures = Path('drive/output/derived/contributor_stats/departed_contributors')
    outdir = Path('drive/output/analysis/linkedin_departure_validation')
    outdir_es = Path('output/analysis/linkedin_departure_validation/event_studies')
    github_profile_locations = pd.read_csv(indir_locations / "standardized_locations_github.csv", index_col = 0)
    linkedin_profile_locations = pd.read_csv(indir_locations / "standardized_locations_linkedin.csv", index_col = 0)

    departed_committers = pd.read_csv(indir_profiles / 'departed_github_profiles.csv', index_col = 0)
    departed_linkedin_profiles = pd.read_parquet(indir_profiles / 'departed_linkedin_profiles.parquet')
    departed_linkedin_positions = pd.read_parquet(indir_profiles / 'departed_linkedin_positions.parquet')

    departed_full = GenerateDepartedFullDataset(departed_committers, github_profile_locations, 
                                         linkedin_profile_locations, departed_linkedin_profiles, departed_linkedin_positions)
    
    departure_validation_cols = ['time_period','rolling_window','criteria_col','criteria_pct','consecutive_periods',
                                'post_period_length','decline_type','decline_stat', 'departure_count', 'linkedin_match_pct', 'departure_occurred_pct']
    df_departure_linkedin_validation = pd.DataFrame(columns = departure_validation_cols)
    for time_period in [2, 3, 6]:
        for rolling_window in [732, 1828]:
            spec_summary_file = indir_departures / f'departed_contributors_specification_summary_major_months{time_period}_window{rolling_window}D.csv'
            df_specifications = pd.read_csv(spec_summary_file).query('criteria_col == "commits"')

            specification_results = [SummarizeSpecification(df_specifications, spec_idx, time_period, rolling_window, departed_full, indir_departures) 
                                    for spec_idx in df_specifications.index]

            specification_results_df = pd.DataFrame(specification_results, columns=departure_validation_cols)
            df_departure_linkedin_validation = pd.concat([df_departure_linkedin_validation, specification_results_df], ignore_index=True)

            RunEventStudies(df_specifications=df_specifications, indir_departures=indir_departures, departed_full=departed_full,
                            time_period=time_period, rolling_window=rolling_window, outdir= outdir_es,
                            pre_period=5, post_period=5, time_fe=False, individual_fe=False)

    df_departure_linkedin_validation.to_csv(outdir / 'linkedin_validation_stats.csv', index = False)


def GenerateDepartedFullDataset(departed_committers, github_profile_locations, linkedin_profile_locations, departed_linkedin_profiles, departed_linkedin_positions):

    committers_clean, github_locs, linkedin_locs = PreprocessLocations(
        departed_committers, github_profile_locations, linkedin_profile_locations)

    merged_committers, relevant_positions = MergeAndMatchProfiles(
        committers_clean, github_locs, linkedin_locs,
        departed_linkedin_profiles, departed_linkedin_positions)

    matched_committers_linkedin = FetchWRDSCompanyMatches(
        merged_committers, relevant_positions)

    departed_full = FetchLinkedinJobChanges(matched_committers_linkedin)

    return departed_full


def PreprocessLocations(departed_committers, github_locs, linkedin_locs):
    committers_clean = departed_committers[['repo_name', 'actor_id', 'name_clean', 'linkedin_url', 'company', 'location']].copy()
    committers_clean['name_clean'] = committers_clean['name_clean'].replace('[nan]', np.nan).apply(
        lambda x: literal_eval(x) if pd.notnull(x) else x)

    for col in ['standardized_location_address', 'standardized_location_raw']:
        github_locs[col] = github_locs[col].apply(lambda x: literal_eval(x) if pd.notnull(x) else x)
        linkedin_locs[col] = linkedin_locs[col].apply(lambda x: literal_eval(x) if pd.notnull(x) else x)

    return committers_clean, github_locs, linkedin_locs

def MergeAndMatchProfiles(committers_clean, github_locs, linkedin_locs, linkedin_profiles, linkedin_positions):
    committers_loc = pd.merge(committers_clean, github_locs.drop('standardized_location', axis=1), left_index=True, right_index=True)
    committers_loc.reset_index(inplace=True)
    committers_loc = committers_loc.explode('name_clean').rename(columns={'name_clean': 'fullname'})

    linkedin_loc = pd.merge(linkedin_profiles, linkedin_locs.drop('standardized_location', axis=1), how='left')
    linkedin_loc = linkedin_loc.rename(columns={'standardized_location_address':'standardized_linkedin_address',
                                                'standardized_location_raw':'raw_linkedin_address'})[['user_id','fullname','standardized_linkedin_address','raw_linkedin_address','profile_linkedin_url']]

    merged = pd.merge(committers_loc, linkedin_loc, on='fullname')

    merged['exact_match'] = merged.apply(lambda x:
        isinstance(x['standardized_location_address'], list) and
        isinstance(x['standardized_linkedin_address'], list) and
        bool(set(x['standardized_location_address']).intersection(x['standardized_linkedin_address'])), axis=1)

    merged_filtered = merged[merged['exact_match'] | merged['company'].notna()]
    relevant_positions = linkedin_positions[linkedin_positions['user_id'].isin(merged_filtered['user_id'])]

    return merged_filtered, relevant_positions


def FetchWRDSCompanyMatches(merged_committers, relevant_positions):
    params = {'company_list': tuple(relevant_positions['rcid'].dropna().unique())}
    db = wrds.Connection(wrds_username=os.environ['WRDS_USERNAME'])
    company_mappings = db.raw_sql("SELECT rcid, company FROM revelio.company_mapping WHERE rcid IN %(company_list)s", params=params)
    db.close()

    company_matches = pd.merge(relevant_positions, company_mappings, on='rcid')[['user_id','company']].drop_duplicates()
    company_list = company_matches.groupby('user_id')['company'].apply(list).reset_index()

    merged = pd.merge(merged_committers, company_list, on='user_id', how='left', suffixes=('', '_linkedin'))
    merged['linkedin_company_matches'] = merged.apply(lambda x: [
        (comp, fuzz.ratio(x['company'], comp)) for comp in x['company_linkedin']] if type(x['company_linkedin']) == list and pd.notnull(x['company']) else np.nan, axis=1)

    merged['company_match'] = merged['linkedin_company_matches'].apply(
        lambda x: any(score > 45 for _, score in x) if isinstance(x, list) else False)
    merged_filtered = merged[merged['company_match'] | merged['linkedin_company_matches'].isna()]

    linkedin_urls = merged_filtered.apply(
        lambda x: x['linkedin_url'] if pd.notnull(x['linkedin_url']) else x['profile_linkedin_url'], axis=1)
    pattern = r'^(?:https?:\/\/)?(?:www\.)?linkedin\.com/(.*)$'
    linkedin_urls = linkedin_urls.apply(lambda x: re.sub(pattern, r'linkedin.com/\1', x) if pd.notnull(x) else x)

    merged_filtered['linkedin_url'] = linkedin_urls

    return merged_filtered[['repo_name', 'actor_id', 'linkedin_url', 'user_id']]


def QueryForLinkedinPositions(committers_df, db):
    params = {'user_ids': tuple(committers_df['user_id'].dropna().unique())}
    return db.raw_sql("SELECT user_id, startdate, enddate FROM revelio.positions WHERE user_id IN %(user_ids)s", params=params)

def FetchLinkedinJobChanges(committers_linkedin):
    db = wrds.Connection(wrds_username=os.environ.get('WRDS_USERNAME'))
    linkedin_positions = QueryForLinkedinPositions(committers_linkedin.dropna(), db)
    db.close()

    for col in ['startdate', 'enddate']:
        linkedin_positions[col] = pd.to_datetime(linkedin_positions[col])

    job_changes = linkedin_positions.groupby('user_id').agg(list).reset_index()

    departed_full = pd.merge(committers_linkedin, job_changes, on='user_id', how='left')

    for col in ['startdate', 'enddate']:
        departed_full[col] = departed_full[col].apply(lambda x: x if isinstance(x, list) and x else np.nan)

    return departed_full

def SummarizeSpecification(df_specifications, spec_idx, time_period, rolling_window, departed_full, indir_departures):
    spec = df_specifications.loc[spec_idx]
    criteria_pct, consecutive_periods = spec['criteria_pct'], spec['consecutive_periods']
    post_period_length, decline_type, decline_stat = spec['post_period_length'], spec['decline_type'], spec['decline_stat']
    decline_stat = int(decline_stat) if decline_stat == 0 or decline_type == "threshold_gap_qty" else decline_stat
    filepath = indir_departures / f'departed_contributors_major_months{time_period}_window{rolling_window}D_criteria_commits_{criteria_pct}pct_consecutive{consecutive_periods}_post_period{post_period_length}_{decline_type}_{decline_stat}.parquet'
    df_departed = pd.read_parquet(filepath)
    departure_df = MergeWithLinkedinDepartures(df_departed, decline_type, departed_full)
    linkedin_match_pct = 1 - departure_df['user_id'].isna().mean()
    departure_occurred_pct = departure_df.query('~user_id.isna()')['corresponding_departure'].mean()
    return [time_period, rolling_window, 'commits', criteria_pct, consecutive_periods, post_period_length, decline_type, decline_stat, len(departure_df), linkedin_match_pct, departure_occurred_pct]

def MergeWithLinkedinDepartures(df_departed, decline_type, departed_full):
    departure_df = CleanDepartures(df_departed, decline_type)
    departure_df = pd.merge(departure_df, departed_full, how='left')
    departure_df['startdate_dep'] = departure_df.apply(lambda x: any(x['time_range'][0] <= ele.date() <= x['time_range'][1]  for ele in x['startdate'] if pd.notnull(ele)) if isinstance(x['startdate'], list) else False, axis=1)
    departure_df['enddate_dep'] = departure_df.apply(lambda x: any(x['time_range'][0] <= ele.date() <= x['time_range'][1] for ele in x['enddate'] if pd.notnull(ele)) if isinstance(x['enddate'], list) else False, axis=1)
    departure_df['corresponding_departure'] = departure_df['startdate_dep'] | departure_df['enddate_dep']
    return departure_df

def CleanDepartures(df_departed, decline_type):
    if decline_type == "threshold_gap_qty":
        df_departed = df_departed.query('below_qty_mean_gap0 == 1 | below_qty_mean_gap1 == 1')

    df_departed = pd.merge(df_departed, df_departed.query('time_period == final_period')\
                           [['repo_name','actor_id','grouped_index']].rename({'grouped_index':'final_index'}, axis = 1))
    if decline_type == "threshold_gap_qty":
        df_departed['final_index'] = df_departed.apply(
            lambda x: x['final_index'] if x['below_qty_mean_gap0'] == 1 else x['final_index']+1, axis = 1)
    df_departed = pd.merge(df_departed.drop('final_period', axis = 1), df_departed.query('grouped_index == final_index')[['actor_id','repo_name','time_period']]\
                          .rename({'time_period':'final_period'}, axis = 1))
    df_departed['first_post_period_index'] = df_departed['final_index'] + 1
    df_departed['relative_time'] = (df_departed['grouped_index'] - df_departed['final_index'])-1

        
    df_departed['time_period'] = pd.to_datetime(df_departed['time_period'])
    df_departure_range = df_departed.query('relative_time == -1 | relative_time == 0')\
        .groupby(['repo_name','actor_id']).agg({'time_period':list}).reset_index()
    df_departure_range['time_range'] = df_departure_range['time_period'].apply(lambda x: [x[0].date(), x[1].date()])
    df_departure_range = df_departure_range[df_departure_range['time_range'].apply(lambda x: x[0]<datetime(2023, 1, 1).date())]
    
    return df_departure_range.drop('time_period', axis = 1)

def pairwise(lst):
    return [[lst[i], lst[i+1]] for i in range(len(lst) - 1)]

def PlotDepartureEventStudy(df_departure_es, time_fe, individual_fe, pre_period, post_period, filename, spec_description=""):
    df_departure_es['corresponding_departure_og'] = df_departure_es['corresponding_departure']
    title_supp = spec_description
    
    if time_fe:
        df_departure_es['corresponding_departure'] -= df_departure_es.groupby('time_period')['corresponding_departure'].transform('mean')
        title_supp += "\nTime FE"
    if individual_fe:
        df_departure_es['corresponding_departure'] -= df_departure_es.groupby(['repo_name', 'actor_id'])['corresponding_departure'].transform('mean')
        title_supp += "\nIndividual FE"
    if individual_fe and time_fe:
        df_departure_es['corresponding_departure'] += df_departure_es['corresponding_departure_og']

    plt.figure(figsize=(12, 7))
    
    sns.lineplot(
        x='relative_time', 
        y='corresponding_departure', 
        data=df_departure_es.query(f'relative_time >= -{pre_period} & relative_time <= {post_period}'),
        errorbar=('ci', 95),
        linewidth=2
    )

    plt.axvline(x=0, color='red', linestyle='--', linewidth=1.5)

    full_title = 'Proportion of Contributor Departures aligning with LinkedIn Job Departures' + title_supp
    wrapped_title = "\n".join(textwrap.wrap(full_title, width=70))
    
    plt.title(wrapped_title, fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Relative Time (Periods)', fontsize=14)
    plt.ylabel('Corresponding Departure Rate', fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.6)
    sns.despine()
    plt.tight_layout(pad=2.0)
    
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close()

def RunEventStudies(df_specifications, indir_departures, departed_full, 
                    time_period, rolling_window, outdir,
                    pre_period=5, post_period=5, time_fe=False, individual_fe=False):
    
    for idx, spec in df_specifications.iterrows():
        df_departure_es, filename, spec_description = PrepareEventStudyData(
            spec, indir_departures, departed_full, time_period, rolling_window, outdir
        )

        PlotDepartureEventStudy(df_departure_es=df_departure_es, time_fe=time_fe, individual_fe=individual_fe,
                                pre_period=pre_period, post_period=post_period, filename=filename, spec_description=spec_description)

def PrepareEventStudyData(spec, indir_departures, departed_full, time_period, rolling_window, outdir):
    criteria_pct = spec['criteria_pct']
    consecutive_periods = spec['consecutive_periods']
    post_period_length = spec['post_period_length']
    decline_type = spec['decline_type']
    decline_stat = spec['decline_stat']
    decline_stat_fmt = int(decline_stat) if decline_stat == 0 or decline_type == "threshold_gap_qty" else decline_stat
    spec_file = (f"departed_contributors_major_months{time_period}_window{rolling_window}D_"
                 f"criteria_commits_{criteria_pct}pct_consecutive{consecutive_periods}_"
                 f"post_period{post_period_length}_{decline_type}_{decline_stat_fmt}.parquet")
    df_departed = pd.read_parquet(indir_departures / spec_file)
    df_departure_range = CleanDepartures(df_departed, decline_type)
    df_departure_range = pd.merge(df_departure_range, departed_full, how='left')
    
    # Precompute date lists for startdate and enddate
    df_departure_range['startdate_dates'] = df_departure_range['startdate'].apply(
        lambda lst: [ele.date() for ele in lst if pd.notnull(ele)] if isinstance(lst, list) else [])
    df_departure_range['enddate_dates'] = df_departure_range['enddate'].apply(
        lambda lst: [ele.date() for ele in lst if pd.notnull(ele)] if isinstance(lst, list) else [])
    
    # Now use these precomputed columns in your comparisons:
    df_departure_range['startdate_dep'] = df_departure_range.apply(
        lambda x: any(x['time_range'][0] <= d <= x['time_range'][1] for d in x['startdate_dates'])
                  if x['startdate_dates'] else False, axis=1)
    df_departure_range['enddate_dep'] = df_departure_range.apply(
        lambda x: any(x['time_range'][0] <= d <= x['time_range'][1] for d in x['enddate_dates'])
                  if x['enddate_dates'] else False, axis=1)
    df_departure_range['corresponding_departure'] = df_departure_range['startdate_dep'] | df_departure_range['enddate_dep']

    all_timerange_groups = np.sort(df_departure_range['time_range'].explode().unique())
    pairwise_groups = list(pairwise(all_timerange_groups))
    df_departure_es = df_departure_range[~df_departure_range['startdate'].isna()].copy()
    df_departure_es['all_timerange_groups'] = [pairwise_groups] * len(df_departure_es)
    df_departure_es = df_departure_es.explode('all_timerange_groups')
    
    # Precompute again for exploded data
    df_departure_es['startdate_dates'] = df_departure_es['startdate'].apply(
        lambda lst: [ele.date() for ele in lst if pd.notnull(ele)] if isinstance(lst, list) else [])
    df_departure_es['enddate_dates'] = df_departure_es['enddate'].apply(
        lambda lst: [ele.date() for ele in lst if pd.notnull(ele)] if isinstance(lst, list) else [])
    
    df_departure_es['startdate_dep'] = df_departure_es.apply(
        lambda x: any(x['all_timerange_groups'][0].date() <= d <= x['all_timerange_groups'][1].date() for d in x['startdate_dates'])
                  if x['startdate_dates'] else False, axis=1)
    df_departure_es['enddate_dep'] = df_departure_es.apply(
        lambda x: any(x['all_timerange_groups'][0].date() <= d <= x['all_timerange_groups'][1].date() for d in x['enddate_dates'])
                  if x['enddate_dates'] else False, axis=1)
    df_departure_es['corresponding_departure'] = df_departure_es['startdate_dep'] | df_departure_es['enddate_dep']
    
    df_departure_es['departure_date'] = df_departure_es.apply(lambda x: x['time_range'][0] == x['all_timerange_groups'][0], axis=1)
    df_departure_es['time_period'] = df_departure_es['time_range'].apply(lambda x: x[0])
    df_departure_es.sort_values(['actor_id', 'time_period'], inplace=True)
    df_departure_es['relative_time'] = df_departure_es.groupby('actor_id').cumcount()
    departure_time = df_departure_es[df_departure_es['departure_date']][['repo_name', 'actor_id', 'relative_time']].rename(
        columns={'relative_time': 'treatment_time'})
    df_departure_es = df_departure_es.merge(departure_time, how='left', on=['repo_name', 'actor_id'])
    df_departure_es['relative_time'] -= df_departure_es['treatment_time']
    
    filename = f"{outdir}/event_study_commits{criteria_pct}_consec{consecutive_periods}_post{post_period_length}_{decline_type}_{decline_stat_fmt}.png"
    spec_description = (f"\nCriteria: {criteria_pct}% Commits | Consecutive Periods: {consecutive_periods} | Post-Period: {post_period_length}\n"
                        f"Decline Type: {decline_type} | Stat: {decline_stat_fmt}")
    
    return df_departure_es, filename, spec_description
