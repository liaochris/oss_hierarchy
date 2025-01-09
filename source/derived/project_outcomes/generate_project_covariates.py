
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

### I want to merge in df_ranked_activity_share

def Main():
    warnings.filterwarnings("ignore")
    pd.set_option('display.max_columns', None)
    pandarallel.initialize(progress_bar = True)

    indir_data = Path('drive/output/derived/contributor_stats/contributor_data')
    outdir_data = Path('drive/output/derived/project_outcomes')

    time_period = int(sys.argv[1])
    rolling_window = 732
    agg_cols = ['issues_opened','helping_issue_comments','commits','prs_opened','prs_merged','issues_closed']

    df_contributor_panel = pd.read_parquet(indir_data / f"major_contributors_major_months{time_period}_window{rolling_window}D_samplefull.parquet")
    df_contributor_panel = CleanContributorPanel(df_contributor_panel)
    df_contributor_selected_panel = AddActivityCategories(df_contributor_panel, agg_cols)
    
    df_repo_size = CalculateSize(df_contributor_panel)
    df_repo_hhi = CalculateHHI(df_contributor_panel, agg_cols)
    df_repo_overlap = CalculateOverlap(df_contributor_panel)
    
    df_repo_panel = pd.merge(df_repo_size, df_repo_hhi, how = 'outer').merge(df_repo_overlap, how = 'outer')
    df_repo_panel_full = MakeBalanced(df_repo_panel)

    df_repo_panel_full.to_parquet(outdir_data / f'project_covariates_major_months{time_period}.parquet')
    df_contributor_selected_panel.to_parquet(outdir_data / f'contributor_covariates_major_months{time_period}.parquet')

def CleanContributorPanel(df_contributor_panel):
    val_cols = [col for col in df_contributor_panel.columns if 'pct' in col]
    df_contributor_panel = df_contributor_panel.drop(val_cols, axis = 1).reset_index(drop = True)
    df_contributor_panel = df_contributor_panel.rename({'issue_number':'issues_opened','pr':'prs_opened'}, axis = 1)

    return df_contributor_panel

def AddActivityCategories(df_contributor_panel, agg_cols):
    df_contributor_panel['problem_identification'] = df_contributor_panel.parallel_apply(
        lambda x: x['issues_opened']>0 or x['own_issue_comments']>0, axis = 1)
    df_contributor_panel['problem_discussion'] = df_contributor_panel.parallel_apply(
        lambda x: x['helping_issue_comments']>0 or x['pr_comments']>0, axis = 1)
    df_contributor_panel['problem_solving'] = df_contributor_panel.parallel_apply(
        lambda x: x['prs_opened']>0 or x['commits']>0, axis = 1)
    df_contributor_panel['solution_incorporation'] = df_contributor_panel.parallel_apply(
        lambda x: x['pr_reviews']>0 or x['pr_review_comments']>0 or x['prs_merged']>0 or x['issues_closed']>0, axis = 1)
    for col in agg_cols:
        df_contributor_panel[f"{col}_share"] = df_contributor_panel[col]/df_contributor_panel.groupby(['repo_name','time_period'])[col].transform('sum')

    df_contributor_selected_panel = df_contributor_panel[['repo_name','time_period','actor_id', 'problem_identification',
                                                        'problem_discussion','problem_solving','solution_incorporation'] + [f"{col}_share" for col in agg_cols]]
    return df_contributor_selected_panel

def CalculateSize(df_contributor_panel):
    df_repo_size = df_contributor_panel.groupby(['repo_name','time_period']).agg({
        'actor_id':'count','problem_identification': 'sum','problem_discussion':'sum','problem_solving':'sum',
        'solution_incorporation':'sum'}).rename({
        'actor_id':"contributor_count","problem_identification":"problem_identifier_count","problem_discussion":"problem_discusser_count",
        "problem_solving":"problem_solver_count","solution_incorporation":"solution_incorporator_count"}, axis = 1).reset_index()
    return df_repo_size
    
# CalculateSpan
# how??

def CalculateHHI(df_contributor_panel, agg_cols):
    for col in agg_cols:
        df_contributor_panel[f"{col}_hhi"] = df_contributor_panel.assign(share_sq = lambda x: x[f"{col}_share"]**2)\
            .groupby(['repo_name','time_period'])['share_sq'].transform('sum')
        df_contributor_panel.loc[df_contributor_panel.query(f"{col}_share.isna()").index,f"{col}_hhi"] = np.nan
    for col in agg_cols:
        df_contributor_panel[f"{col}_hhi_missing"] = df_contributor_panel[f"{col}_hhi"].isna()
    df_repo_hhi = df_contributor_panel[['repo_name','time_period'] + [f"{col}_hhi" for col in agg_cols] + [f"{col}_hhi_missing" for col in agg_cols]]\
        .drop_duplicates(['repo_name','time_period'])

    return df_repo_hhi


def CalculateOverlap(df_contributor_panel):
    norm_dict = {"solve_and_incorporate":"incorporate","solve_and_incorporate_and_discuss":"incorporate",
                 "solve_and_discuss":"solve"}
    df_contributor_panel['incorporate'] = df_contributor_panel.groupby('repo_name')\
        ['solution_incorporation'].transform('sum')
    df_contributor_panel['solve'] = df_contributor_panel.groupby('repo_name')\
        ['problem_solving'].transform('sum')
    df_contributor_panel['solve_and_incorporate'] = df_contributor_panel.parallel_apply(
        lambda x: x['solution_incorporation'] and x['problem_solving'], axis = 1)
    df_contributor_panel['solve_and_incorporate_and_discuss'] = df_contributor_panel.parallel_apply(
        lambda x: x['solution_incorporation'] and x['problem_solving'] and x['problem_discussion'], axis = 1)
    df_contributor_panel['solve_and_discuss'] = df_contributor_panel.parallel_apply(
        lambda x: x['problem_solving'] and x['problem_discussion'], axis = 1)
    
    for col in ['solve_and_incorporate','solve_and_incorporate_and_discuss','solve_and_discuss']:
        df_contributor_panel[col] = df_contributor_panel.groupby('repo_name')\
            [col].transform('sum')
        df_contributor_panel[col] = df_contributor_panel[col]/df_contributor_panel[norm_dict[col]]

    df_repo_overlap = df_contributor_panel.groupby(['repo_name','time_period'])\
        [['solve_and_incorporate','solve_and_incorporate_and_discuss','solve_and_discuss']].mean().reset_index()
    return df_repo_overlap

def MakeBalanced(df_repo_panel):
    df_repo_panel['first_period'] = df_repo_panel.groupby('repo_name')['time_period'].transform('min')
    df_repo_panel['final_period'] = df_repo_panel.groupby('repo_name')['time_period'].transform('max')
    time_periods = df_repo_panel['time_period'].unique().tolist()
    df_balanced = df_repo_panel[['repo_name']].drop_duplicates()
    df_balanced['time_period'] = [time_periods for i in range(df_balanced.shape[0])]
    df_balanced = df_balanced.explode('time_period')
    df_repo_panel_full = pd.merge(df_balanced, df_repo_panel, how = 'left')
    df_repo_panel_full[['first_period','final_period']] = df_repo_panel_full.groupby(['repo_name'])[['first_period','final_period']].ffill()
    df_repo_panel_full = df_repo_panel_full.query('time_period >= first_period')
    df_repo_panel_full[[col for col in df_repo_panel_full.columns if 'hhi' not in col]] = df_repo_panel_full[[col for col in df_repo_panel_full.columns if 'hhi' not in col]].fillna(0)

    return df_repo_panel_full
    
if __name__ == '__main__':
    Main()


