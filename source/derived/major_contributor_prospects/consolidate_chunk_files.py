import pandas as pd
import glob
from pathlib import Path

def Main():
    indir = Path('drive/output/derived/major_contributor_prospects/intermediary_files')
    outdir = Path('drive/output/derived/major_contributor_prospects')
    rolling_window_list = ['732D','1828D','367D','3654D']
    time_period_months = [6, 3, 12, 2]

    for window in rolling_window_list:
        for major_months in time_period_months:
            relevant_files = indir.glob(f'major_contributors_major_months{major_months}_window{window}_samplefull_chunk*.parquet')
            df_major_contributors = pd.concat([pd.read_parquet(file) for file in relevant_files])
            df_major_contributors.to_parquet(outdir / f'major_contributors_major_months{major_months}_window{window}_samplefull.parquet')

if __name__ == '__main__':
    Main()


