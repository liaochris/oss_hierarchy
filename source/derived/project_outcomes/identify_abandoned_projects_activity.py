import pandas as pd
import numpy as np
from pathlib import Path


def Main():
    indir = Path('drive/output/derived/contributor_stats/contributor_data')
    outdir = Path('drive/output/derived/project_outcomes/abandoned_projects')
    time_period = 6
    rolling_window = 732

    contribution_histories = pd.read_parquet(indir / f'major_contributors_major_months{time_period}_window{rolling_window}D_samplefull.parquet')

    repo_panel_full = GenerateRepoPanel(contribution_histories)

    repo_panel_full['abandoned_criteria'] = ((repo_panel_full['issues_closed'] == 0) & \
        (repo_panel_full['commits'] == 0)).astype(int)
    repo_panel_full = ConsecuiveAbandonedPeriods(repo_panel_full)

    consecutive_periods_list = np.arange(2, 5, 1)
    permanent_departure_options = [True, False]
    df_specs = pd.DataFrame(columns = ['consecutive periods','permanent departure','project count'])
    for consecutive_periods in consecutive_periods_list:
        for permanent_departure in permanent_departure_options:
            df_abandoned = GetAbandonedProjects(repo_panel_full, consecutive_periods, permanent_departure)
            df_specs.loc[df_specs.shape[0]+1] = [consecutive_periods, permanent_departure, df_abandoned.shape[0]]
            print(df_abandoned.shape)
            df_abandoned.to_parquet(outdir / f'abandoned_projects_consecutive_req{consecutive_periods}_permanent{str(permanent_departure)}.parquet')
    df_specs.to_csv(outdir / 'abandoned_projects_specifications.csv')

def GenerateRepoPanel(contribution_histories):
    id_cols = ['user_type','repo_name','actor_id','time_period']
    val_cols = [col for col in contribution_histories.columns if 'pct' not in col and col not in id_cols]
    repo_panel = contribution_histories.groupby(['repo_name','time_period'])[val_cols].sum().reset_index()
    repo_panel['first_period'] = repo_panel.groupby('repo_name')['time_period'].transform('min')

    repo_panel_full = MakeRepoPanelBalanced(repo_panel, contribution_histories)
    repo_panel_full['first_period'] = repo_panel_full.groupby('repo_name')['first_period'].transform('ffill')
    repo_panel_full['first_period'] = repo_panel_full.groupby('repo_name')['first_period'].transform('bfill')
    repo_panel_full = repo_panel_full.query('time_period>=first_period')\
        .drop('first_period', axis = 1)\
        .sort_values(['repo_name','time_period'])\
        .fillna(0)
    
    repo_panel_full['numeric_time_period'] = repo_panel_full.groupby('repo_name').cumcount()
    repo_panel_full['max_time_period'] = repo_panel_full.groupby('repo_name')['time_period'].transform('max')

    return repo_panel_full

def MakeRepoPanelBalanced(repo_panel, contribution_histories):
    time_periods = contribution_histories['time_period'].sort_values().unique().tolist()
    df_balanced = contribution_histories[['repo_name']].drop_duplicates()
    df_balanced['time_period'] = [time_periods for i in range(df_balanced.shape[0])]
    df_balanced = df_balanced.explode('time_period')
    repo_panel_full = pd.merge(df_balanced[['repo_name','time_period']].query(f'time_period <= "2023-01-01"'), 
        repo_panel, how = 'left')
    return repo_panel_full

def ConsecuiveAbandonedPeriods(repo_panel_full):
    group_keys = (
        (repo_panel_full['repo_name'] != repo_panel_full['repo_name'].shift()) |
        (repo_panel_full['abandoned_criteria'] == 0)
    ).cumsum()
    repo_panel_full['consecutive_periods'] = repo_panel_full.groupby(group_keys)['abandoned_criteria'].cumsum()
    repo_panel_full.loc[repo_panel_full['abandoned_criteria'] == 0, 'consecutive_periods'] = 0
    return repo_panel_full

def GetAbandonedProjects(repo_panel_full, consecutive_periods, permanent_departure):
    max_consecutive_periods = repo_panel_full.query(f'consecutive_periods >= {consecutive_periods}')\
        .groupby('repo_name')[['time_period']].max().reset_index()\
        .rename({'time_period':'max_consecutive_time_period'}, axis = 1)
    repo_panel_full = pd.merge(repo_panel_full, max_consecutive_periods, how = 'left')

    abandoned_project_candidates = repo_panel_full.query(f'consecutive_periods >= {consecutive_periods} & \
        max_consecutive_time_period == max_time_period')
    abandoned_project_candidates['time_period_gap'] = abandoned_project_candidates.groupby('repo_name')\
        ['numeric_time_period'].transform('diff')

    if permanent_departure:
        abandoned_project_list = abandoned_project_candidates.groupby('repo_name')[['time_period_gap']].mean()\
            .query('time_period_gap == 1 | time_period_gap.isna()').index.tolist()
        abandoned_project_candidates = abandoned_project_candidates[abandoned_project_candidates['repo_name'].isin(abandoned_project_list)]
    
    df_abandoned_dates = abandoned_project_candidates.query('time_period == max_consecutive_time_period')\
        [['repo_name','consecutive_periods','numeric_time_period']]\
        .rename({'consecutive_periods':'end_consecutive_periods'}, axis = 1)
    df_abandoned_dates['abandoned_numeric_time_period'] = df_abandoned_dates['numeric_time_period']-df_abandoned_dates['end_consecutive_periods']+1
    df_abandoned = pd.merge(repo_panel_full, df_abandoned_dates[['repo_name','abandoned_numeric_time_period']])\
        .query('abandoned_numeric_time_period == numeric_time_period')\
        [['repo_name','time_period']].rename({'time_period':'abandoned_date'}, axis = 1)
    
    return df_abandoned

if __name__ == '__main__':
    Main()
