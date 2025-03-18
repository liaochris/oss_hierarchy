import pandas as pd
from pathlib import Path
from source.lib.helpers import *
from source.lib.JMSLab.SaveData import SaveData

def Main():
    indir_abandoned = Path('drive/output/derived/project_outcomes/abandoned_projects')
    indir_departures = Path('drive/output/derived/contributor_stats/departed_contributors')
    outdir_departures_filtered = Path('drive/output/derived/contributor_stats/filtered_departed_contributors')

    for time_period in [2,3,6]:
        for rolling_window in [732, 1828]:
            contribution_histories = pd.read_parquet(f'drive/output/derived/contributor_stats/contributor_data/major_contributors_major_months{time_period}_window{rolling_window}D_samplefull.parquet')
            df_specifications = pd.read_csv(indir_departures / f'departed_contributors_specification_summary_major_months{time_period}_window{rolling_window}D.csv')
            for idx in df_specifications.index:
                df_departure_range = FilterSpecification(df_specifications, idx, time_period, rolling_window, indir_departures, indir_abandoned, outdir_departures_filtered, contribution_histories)
                
def FilterSpecification(df_specifications, idx, time_period, rolling_window, indir_departures, indir_abandoned, outdir_departures_filtered,
                        contribution_histories):
    criteria_pct = df_specifications.loc[idx,'criteria_pct']
    consecutive_periods = df_specifications.loc[idx,'consecutive_periods']
    post_period_length = df_specifications.loc[idx,'post_period_length']
    decline_type = df_specifications.loc[idx,'decline_type']
    decline_stat = df_specifications.loc[idx,'decline_stat']
    criteria_col = df_specifications.loc[idx,'criteria_col']    
    
    if decline_stat == 0 or decline_type == "threshold_gap_qty":
        decline_stat = int(decline_stat)
    
    df_departed = pd.read_parquet(indir_departures / f'departed_contributors_major_months{time_period}_window{rolling_window}D_criteria_{criteria_col}_{criteria_pct}pct_consecutive{consecutive_periods}_post_period{post_period_length}_{decline_type}_{decline_stat}.parquet')
    df_departure_range = CleanDepartures(df_departed, decline_type)
    df_departure_range = LabelScrapedAbandonment(df_departure_range, indir_abandoned)
    df_departure_range = LabelDataExtractedAbandonment(df_departure_range, indir_abandoned, time_period)
    
    df_departure_range['repo_count'] = df_departure_range.groupby('repo_name')['actor_id'].transform('count')
    df_departure_range['last_pre_period'] = df_departure_range['time_range'].apply(lambda x: x[0])
    df_departure_range['treatment_period'] = df_departure_range['time_range'].apply(lambda x: x[1])
    
    df_departure_range = FilterOutStillParticipating(df_departure_range, contribution_histories)
    SaveData(df_departure_range, ['repo_name','time_period','actor_id'],
             outdir_departures_filtered / f'filtered_departed_contributors_major_months{time_period}_window{rolling_window}D_criteria_{criteria_col}_{criteria_pct}pct_consecutive{consecutive_periods}_post_period{post_period_length}_{decline_type}_{decline_stat}.parquet',
             outdir_departures_filtered / 'data_manifest.log')

    return df_departure_range

def CleanDepartures(df_departed, decline_type):
    if decline_type == "threshold_gap_qty":
        df_departed = df_departed.query('below_qty_mean_gap0 == 1')

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
    df_departure_range = df_departure_range.drop('time_period', axis = 1) 
    return df_departure_range

def LabelScrapedAbandonment(df_departure_range, indir_abandoned):
    df_abandoned_scraped = pd.read_csv(indir_abandoned / 'scraped_abandoned_repo_data.csv', index_col = 0).query('status == "abandoned"')
    df_abandoned_scraped['abandoned_date'] = pd.to_datetime(df_abandoned_scraped['abandoned_date']).apply(lambda x: x.date())
    
    df_departure_range = pd.merge(df_departure_range, df_abandoned_scraped[['repo_name','abandoned_date']], how = 'left')
    df_departure_range['abandoned_scraped'] = df_departure_range.apply(
            lambda x: not pd.isnull(x['abandoned_date']) and x['time_range'][0]<=x['abandoned_date']<=x['time_range'][1], axis = 1)
    return df_departure_range

def LabelDataExtractedAbandonment(df_departure_range, indir_abandoned, time_period):
    for consecutive in [2, 3, 4]:
        for permanent in [True, False]:
            df_abandoned_data = pd.read_parquet(indir_abandoned / f'abandoned_projects_consecutive_req{consecutive}_permanent{permanent}.parquet')
            df_abandoned_data['abandoned_date'] = pd.to_datetime(df_abandoned_data['abandoned_date']).apply(lambda x: x.date())
            abandoned_date_col = f'abandoned_date_consecutive_req{consecutive}_permanent{permanent}'
            df_departure_range = pd.merge(df_departure_range, 
                                          df_abandoned_data.rename({'abandoned_date':abandoned_date_col}, axis = 1), how = 'left')
            df_departure_range[f'abandoned_consecutive_req{consecutive}_permanent{permanent}'] = df_departure_range.apply(
                lambda x: not pd.isnull(x[abandoned_date_col]) and x[abandoned_date_col] == x['time_range'][1], axis = 1)
            for periods_after in [1, 2, 3]:
                df_departure_range[f'abandoned_within_{periods_after}periods_consecutive_req{consecutive}_permanent{permanent}'] = df_departure_range.apply(
                    lambda x: not pd.isnull(x[abandoned_date_col]) and x[abandoned_date_col] <= (x['time_range'][1] + pd.DateOffset(months=periods_after*time_period)).date(), axis = 1)
    
    return df_departure_range

def FilterOutStillParticipating(df_departure_range, contribution_histories):
    contribution_departure = pd.merge(contribution_histories, df_departure_range[['repo_name','actor_id','treatment_period']])
    present_one_after = contribution_departure.query('time_period>treatment_period')[['repo_name','actor_id']].drop_duplicates().assign(present_one_after = 1)
    present_after = contribution_departure.query('time_period>=treatment_period')[['repo_name','actor_id']].drop_duplicates().assign(present_after = 1)
    df_departure_range = pd.merge(df_departure_range, present_one_after, how = 'left').merge(present_after, how = 'left')
    df_departure_range[['present_one_after','present_after']] = df_departure_range[['present_one_after','present_after']].fillna(0)
    return df_departure_range