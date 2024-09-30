#!/usr/bin/env python3
import os
import pandas as pd
from datetime import datetime

def Main():
    LOWER_BOUND = datetime(2018,7,1)
    UPPER_BOUND = datetime(2023,9,1)
    NUM_MONTHS = (UPPER_BOUND.year - LOWER_BOUND.year) * 12 + UPPER_BOUND.month - LOWER_BOUND.month - 1

    results_monthly_downloads = pd.read_csv("drive/output/scrape/pypi_monthly_downloads/pypi_monthly_downloads.csv")
    results_monthly_downloads['month'] = pd.to_datetime(results_monthly_downloads['month'])

    results_monthly_downloads_filt = results_monthly_downloads[
        (results_monthly_downloads['num_downloads']>=1000) & (results_monthly_downloads['month']>LOWER_BOUND) &
        (results_monthly_downloads['month']<UPPER_BOUND)]
    results_monthly_downloads_filt['count'] = results_monthly_downloads_filt.groupby('project')['month'].transform('count')

    downloaded_packages = results_monthly_downloads_filt[results_monthly_downloads_filt['count'] == NUM_MONTHS]['project'].drop_duplicates()
    downloaded_packages.to_csv('source/scrape/pypi_monthly_downloads/data/popular_python_packages.csv', index = False)

if __name__ == '__main__':
    Main()