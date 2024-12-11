import os
import pandas as pd
from pathlib import Path
import sys

def Main():
    indir = Path('drive/output/derived/contributor_stats/contributor_data')
    outdir = Path('drive/output/derived/contributor_stats/minor_contributors')

    time_period = int(sys.argv[1])
    rolling_window = int(sys.argv[2])
    criteria_col = sys.argv[3]
    criteria_pct = int(sys.argv[4])
    
    df_contributors = pd.read_parquet(indir / f'major_contributors_major_months{time_period}_window{rolling_window}D_samplefull.parquet')
    min_appearances = 3 # how to let this vary with "major months", one idea below
    # min_appearances = 18/time_period 

    contributor_appearances = df_contributors[['actor_id','repo_name']].value_counts()\
    .reset_index().query(f'count>={min_appearances}')\
    [['actor_id','repo_name', 'count']]

    df_freq_appearances = pd.merge(df_contributors, contributor_appearances)
    df_freq_appearances[f'{criteria_col}_thresh_below'] = \
        df_freq_appearances.apply(lambda x: (x[criteria_col] <= x[f'{criteria_col}_25th_pct']) and x[criteria_col] > 0, axis = 1)
    df_freq_appearances[f'pct_{criteria_col}_thresh_below'] = df_freq_appearances.groupby(['repo_name', 'actor_id'])[f'{criteria_col}_thresh_below'].transform('mean')
    
    df_freq_unimp = df_freq_appearances.query(f'pct_{criteria_col}_thresh_below == 1')
    df_freq_unimp['final_period'] = df_freq_unimp.groupby(['repo_name', 'actor_id'])['time_period'].transform('max')
    
    df_freq_unimp[['repo_name','actor_id','time_period', 'final_period']].to_csv(
        outdir / f'minor_contributors_major_months{time_period}_window{rolling_window}D_criteria_{criteria_col}_{criteria_pct}pct.csv')

if __name__ == '__main__':
    Main()

