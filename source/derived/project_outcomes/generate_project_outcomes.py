
import pandas as pd
from pathlib import Path
import numpy as np
import sys
import warnings
from pandarallel import pandarallel
from source.lib.helpers import *
from source.lib.JMSLab.SaveData import SaveData


def Main():
    warnings.filterwarnings("ignore")
    pd.set_option('display.max_columns', None)
    pandarallel.initialize(progress_bar = True)

    indir_data = Path('drive/output/derived/data_export')
    indir_contributors = Path('drive/output/derived/contributor_stats/contributor_data')
    indir_graph = Path('drive/output/derived/graph_structure')
    outdir_data = Path('drive/output/derived/project_outcomes')
    outdir_log = Path('output/derived/project_outcomes')

    SECONDS_IN_DAY = 86400
    closing_day_options = [30, 60, 90, 180, 360]

    time_period = int(sys.argv[1])
    df_issue = pd.read_parquet(indir_data / 'df_issue.parquet', columns=[
        'repo_name', 'issue_number', 'created_at', 
        'issue_action', 'type', 'issue_comment_id'
    ])
    df_pr = pd.read_parquet(indir_data / 'df_pr.parquet', columns=[
        'repo_name', 'pr_number', 'created_at','actor_id',
        'pr_action', 'pr_merged_by_id', 'pr_merged_by_type', 
        'type', 'pr_review_id', 'pr_review_state'
    ])
    
    df_issue['created_at'] = pd.to_datetime(df_issue['created_at'])
    df_pr['created_at'] = pd.to_datetime(df_pr['created_at'])
    
    df_issue_selected = df_issue[df_issue['created_at']>='2015-01-01']
    df_pr_selected = df_pr[df_pr['created_at']>='2015-01-01']
    
    df_issue_selected = ImputeTimePeriod(df_issue_selected, time_period)
    df_pr_selected = ImputeTimePeriod(df_pr_selected, time_period)

    df_contributor_panel = pd.read_parquet(indir_contributors / f"major_contributors_major_months{time_period}_window732D_samplefull.parquet")
    val_cols = [col for col in df_contributor_panel.columns if 'pct' in col]
    df_contributor_panel = df_contributor_panel.drop(val_cols, axis = 1)

    unique_times = df_issue_selected['time_period'].drop_duplicates().sort_values().unique()
    time_index_map = {t: i + 1 for i, t in enumerate(unique_times)}

    indir_departed = "drive/output/derived/contributor_stats/filtered_departed_contributors"
    rolling_window = 732
    criteria_pct = 75
    consecutive_periods = 3
    post_periods = 2
    df_clean_departures = CleanDepartedContributors(indir_departed, time_period, rolling_window, criteria_pct, consecutive_periods, post_periods)

    df_repo_panel_dept = ConstructRepoPanel(df_contributor_panel, df_issue_selected, df_pr_selected, df_clean_departures, time_period, SECONDS_IN_DAY, closing_day_options,
                                            outdir_data, outdir_log, "", time_index_map=time_index_map)
    
    df_graph_classifications = pd.read_parquet(indir_graph / 'contributor_characteristics.parquet').query('time_period.dt.year>=2015')
    df_graph_data = pd.merge(df_graph_classifications, df_clean_departures, on = ['repo_name'], how = 'left')
    df_graph_data['actor_id'] = pd.to_numeric(df_graph_data['actor_id'])
    df_graph_data['time_index'] = df_graph_data['time_period'].apply(lambda x: time_index_map.get(x, np.nan))
    df_graph_data = pd.merge(df_graph_data, df_repo_panel_dept[['repo_name','all_treatment_group']].drop_duplicates())
    
    important_contributors = GetImportantContributors(df_graph_data)

    ConstructOutcomesPanel(important_contributors, df_contributor_panel, df_issue_selected, df_pr_selected, df_clean_departures, time_period,
                           SECONDS_IN_DAY, closing_day_options, outdir_data, outdir_log, "_imp", time_index_map)

    everyone_pre_departure = GetEveryonePreDeparture(df_graph_data)
    ConstructOutcomesPanel(everyone_pre_departure, df_contributor_panel,df_issue_selected, df_pr_selected, df_clean_departures, time_period,
                           SECONDS_IN_DAY, closing_day_options, outdir_data, outdir_log,  "_all", time_index_map)

    everyone = GetEveryone(df_graph_data)
    ConstructOutcomesPanel(everyone, df_contributor_panel,df_issue_selected, df_pr_selected, df_clean_departures, time_period,
                           SECONDS_IN_DAY, closing_day_options, outdir_data, outdir_log,  "_alltime", time_index_map)

    unimportant_contributors = GetUnimportantContributors(everyone_pre_departure, important_contributors)
    ConstructOutcomesPanel(unimportant_contributors, df_contributor_panel, df_issue_selected, df_pr_selected, df_clean_departures, time_period,
                           SECONDS_IN_DAY, closing_day_options, outdir_data, outdir_log, "_unimp", time_index_map)

    ### MODIFY NEW TO BE BALANCED
    departed_contributors = GetDepartedContributors(df_graph_data)
    new_contributors = GetNewContributors(df_graph_data, everyone_pre_departure, departed_contributors)
    ConstructOutcomesPanel(new_contributors, df_contributor_panel, df_issue_selected, df_pr_selected, df_clean_departures, time_period,
                           SECONDS_IN_DAY, closing_day_options, outdir_data, outdir_log, "_new", time_index_map)

    df_contributor_panel_departed = df_contributor_panel.merge(
        departed_contributors, left_on=["repo_name", "actor_id"], right_on=["repo_name", "departed_actor_id"]
    )
    ConstructRepoPanel(df_contributor_panel_departed, df_issue_selected, df_pr_selected, df_clean_departures, time_period,
                    SECONDS_IN_DAY, closing_day_options, outdir_data, outdir_log, "_departed", time_index_map)

def ConstructRepoPanel(df_contributor_panel, df_issue_selected, df_pr_selected, df_clean_departures, time_period, SECONDS_IN_DAY, closing_day_options, 
                       outdir_data, outdir_log, filename_suffix, time_index_map):
    df_repo_panel = AggregateRepoPanel(df_contributor_panel)
    df_repo_panel_full = MakeBalancedPanel(df_repo_panel)

    df_issues_sans_comments = CreateIssueSansCommentsStats(df_issue_selected)
    df_issues = CreateFullIssueDatasetWithComments(df_issue_selected, df_issues_sans_comments, SECONDS_IN_DAY, closing_day_options)
    df_issues_stats = CreateIssueStats(df_issues)

    df_prs_sans_reviews = CreatePRSansReviewsStats(df_pr_selected)
    df_prs_complete = CreateFullPRDatasetWithReviews(df_pr_selected, df_prs_sans_reviews, SECONDS_IN_DAY, closing_day_options)
    df_prs_stats = CreatePRStats(df_prs_complete)
    df_stats = pd.merge(df_issues_stats, df_prs_stats, how = 'outer')
    df_repo_panel_stats = pd.merge(df_repo_panel_full, df_stats, how = 'left')

    df_repo_panel_dept = ImputeTreatmentPeriodsForControls(df_clean_departures, df_repo_panel_stats)
    df_repo_panel_dept = AddTimePeriodData(df_repo_panel_dept, time_index_map)
    
    SaveData(df_repo_panel_dept,
             ['repo_name','time_period'],
             outdir_data / f'project_outcomes_major_months{time_period}{filename_suffix}.parquet',
             outdir_log / f'project_outcomes_major_months{time_period}{filename_suffix}.log')
    return df_repo_panel_dept

def AggregateRepoPanel(df_contributor_panel):
    df_contributor_panel = df_contributor_panel.drop(['pr_opener'], axis=1) \
        .rename(columns={'issue_number': 'issues_opened','linked_pr_issue_number': 'issues_opened_with_linked_pr',
                         'pr': 'prs_opened'})
    df_aggregated = df_contributor_panel.drop('actor_id', axis = 1).groupby(['repo_name', 'time_period']).agg(['sum', 'mean']).reset_index()
    df_aggregated.columns = [col[0] if col[1] in ['sum', ''] else f"avg_{col[0]}" 
                             for col in df_aggregated.columns.to_flat_index()]
    
    grouping_columns = ['repo_name', 'time_period']
    aggregation_columns = [col for col in df_contributor_panel.columns 
                           if col not in grouping_columns + ['actor_id', 'pr_opener']]
    
    df_contributor_counts = (df_contributor_panel.groupby(grouping_columns).apply(
        lambda grp: pd.Series({"contributor_count": grp['actor_id'].nunique(),
                               **{f"cc_{col}": (grp[col] != 0).sum() for col in aggregation_columns}})).reset_index())
    df_repo_panel = df_aggregated.merge(df_contributor_counts, on=grouping_columns, how='left')
    return df_repo_panel

def MakeBalancedPanel(df_repo_panel):
    df_repo_panel['first_period'] = df_repo_panel.groupby('repo_name')['time_period'].transform('min')
    df_repo_panel['final_period'] = df_repo_panel.groupby('repo_name')['time_period'].transform('max')
    unique_time_periods = np.sort(df_repo_panel['time_period'].unique()).tolist()
    df_balanced_panel = df_repo_panel[['repo_name']].drop_duplicates().copy()
    df_balanced_panel['time_period'] = [unique_time_periods for _ in range(len(df_balanced_panel))]
    df_balanced_panel = df_balanced_panel.explode('time_period')
    df_balanced_panel['time_period'] = pd.to_datetime(df_balanced_panel['time_period'])
    df_full_panel = df_balanced_panel.merge(df_repo_panel, on=['repo_name', 'time_period'], how='left')
    df_full_panel[['first_period', 'final_period']] = df_full_panel.groupby('repo_name')[['first_period', 'final_period']].ffill()
    df_full_panel = df_full_panel.query('time_period >= first_period').fillna(0)
    return df_full_panel


def RemoveDuplicates(df, query, keepcols, duplicatecols, newcolname):
    df_uq = df.query(query).sort_values('created_at', ascending = True)[keepcols]\
        .drop_duplicates(duplicatecols)
    df_uq[newcolname] = 1
    return df_uq

def CreateIssueSansCommentsStats(df_issue_selected):
    issue_keepcols = ['repo_name','issue_number','time_period', 'created_at']
    issue_duplicatecols = ['repo_name','issue_number']
    df_opened_issues = RemoveDuplicates(df_issue_selected, 'issue_action == "opened"', issue_keepcols, issue_duplicatecols, 'opened_issue')
    df_closed_issues = RemoveDuplicates(df_issue_selected, 'issue_action == "closed"', issue_keepcols, issue_duplicatecols, 'closed_issue')\
        .rename({'time_period':'closed_time_period','created_at':'closed_at'}, axis = 1)
    ## TODO: how many closed issues are unlinked
    df_issues_sans_comments = pd.merge(df_opened_issues, df_closed_issues, how = 'left')
    return df_issues_sans_comments

def CreateFullIssueDatasetWithComments(df_issue_selected, df_issues_sans_comments, SECONDS_IN_DAY, closing_day_options):
    ic_keepcols = ['issue_number','issue_comment_id','repo_name','time_period', 'created_at']
    ic_duplicatecols = ['repo_name','issue_number','time_period', 'created_at']
    df_issue_comments = RemoveDuplicates(df_issue_selected, 'type == "IssueCommentEvent"',ic_keepcols, ic_duplicatecols, 'issue_comments')\
        .groupby(['repo_name','issue_number'])['issue_comments'].sum()\
        .reset_index()
    # TODO: how many unlinked issues by issue comments
    df_issues = pd.merge(df_issues_sans_comments, df_issue_comments, how = 'left')
    for col in ['closed_issue','issue_comments']:    
        df_issues[col] = df_issues[col].fillna(0)
    df_issues['days_to_close'] = (df_issues['closed_at'] - df_issues['created_at']).apply(lambda x: x.total_seconds()/SECONDS_IN_DAY)
    for day in closing_day_options:
        df_issues[f'closed_in_{day}_days'] = pd.to_numeric(df_issues['days_to_close']<day).astype(int)
    return df_issues
    
def CreateIssueStats(df_issues):
    df_issues_stats = df_issues.groupby(['repo_name','time_period'])\
        .agg({'closed_issue':'mean', 'closed_in_30_days':'mean', 
              'closed_in_60_days':'mean','closed_in_90_days':'mean',
              'closed_in_180_days':'mean', 'closed_in_360_days':'mean'})
    df_issues_stats.columns = df_issues_stats.columns.to_flat_index()
    df_issues_stats = df_issues_stats.reset_index()\
        .rename(columns = {('closed_issue','mean'): 'p_issues_closed',
            ('closed_in_30_days', 'mean'): 'p_issues_closed_30d',
            ('closed_in_60_days', 'mean'): 'p_issues_closed_60d',
            ('closed_in_90_days', 'mean'): 'p_issues_closed_90d',
            ('closed_in_180_days', 'mean'): 'p_issues_closed_180d',
            ('closed_in_360_days', 'mean'): 'p_issues_closed_360d'})

    return df_issues_stats

def CreatePRSansReviewsStats(df_pr_selected):
    pr_keepcols = ['repo_name','pr_number','time_period', 'created_at']
    pr_merge_keepcols = ['repo_name','pr_number','time_period', 'created_at', 'pr_merged_by_type', 'pr_merged_by_id']
    pr_idcols = ['repo_name','pr_number']
    
    df_opened_prs = RemoveDuplicates(df_pr_selected,'pr_action == "opened"', pr_keepcols, pr_idcols, 'opened_pr')
    df_closed_prs = RemoveDuplicates(df_pr_selected,'pr_action == "closed" & pr_merged_by_id.isna()', pr_keepcols, pr_idcols, 'closed_unmerged_pr')\
        .rename({'time_period':'closed_unmerged_time_period','created_at':'closed_unmerged_at'}, axis = 1)
    df_merged_prs = RemoveDuplicates(df_pr_selected,'pr_action=="closed" & ~pr_merged_by_id.isna()',pr_merge_keepcols, pr_idcols,'merged_pr')\
        .rename({'time_period':'merged_time_period','created_at':'merged_at'}, axis = 1)

    df_prs_sans_reviews = pd.merge(df_opened_prs, df_closed_prs, how = 'left').merge(df_merged_prs, how = 'left')
    return df_prs_sans_reviews

def CreateFullPRDatasetWithReviews(df_pr_selected, df_prs_sans_reviews, SECONDS_IN_DAY, closing_day_options):
    pr_review_keepcols = ['repo_name','pr_number','time_period', 'created_at','pr_review_id','pr_review_state']
    pr_review_idcols = ['repo_name','pr_number','pr_review_id']
    df_pr_reviews = RemoveDuplicates(df_pr_selected,'type == "PullRequestReviewEvent"',pr_review_keepcols,pr_review_idcols, 'pr_review')
    
    for col in ['commented','approved','changes_requested']:
        df_pr_reviews[f'review_state_{col}'] = pd.to_numeric(df_pr_reviews['pr_review_state']==col).astype(int)
    df_pr_review_stats = df_pr_reviews.groupby(['repo_name','pr_number'])\
        [['review_state_commented','review_state_approved','review_state_changes_requested']].sum().reset_index()
    df_pr_review_stats['pr_reviews'] = df_pr_review_stats[['review_state_commented','review_state_approved','review_state_changes_requested']].sum(axis = 1)
    
    df_prs_complete = pd.merge(df_prs_sans_reviews, df_pr_review_stats, how = 'left')
    for col in ['closed_unmerged_pr', 'merged_pr','review_state_commented',
                'review_state_approved','review_state_changes_requested']:    
        df_prs_complete[col] = df_prs_complete[col].fillna(0)
    df_prs_complete['days_to_merge'] = (df_prs_complete['merged_at'] - df_prs_complete['created_at']).apply(lambda x: x.total_seconds()/SECONDS_IN_DAY)
    df_prs_complete['closed_at'] = df_prs_complete['merged_at'].fillna(df_prs_complete['closed_unmerged_at'])
    df_prs_complete['days_to_close'] = (df_prs_complete['closed_at'] - df_prs_complete['created_at']).apply(lambda x: x.total_seconds()/SECONDS_IN_DAY)
    for day in closing_day_options:
        df_prs_complete[f'merged_in_{day}_days'] = pd.to_numeric(df_prs_complete['days_to_merge']<day).astype(int)
        df_prs_complete[f'closed_in_{day}_days'] = pd.to_numeric(df_prs_complete['days_to_close']<day).astype(int)
    
    df_prs_complete['closed_pr'] = df_prs_complete.apply(lambda x: int(x['closed_unmerged_pr'] == 1 or x['merged_pr'] == 1), axis = 1)
    
    df_pr_closer_id = df_pr_selected.query('pr_action == "closed" & pr_merged_by_id.isna()')[['repo_name','pr_number','actor_id']].rename(columns={'actor_id':'pr_closed_by_id'})
    df_prs_complete = pd.merge(df_prs_complete, df_pr_closer_id, how = 'left', on=['repo_name','pr_number'])
    df_prs_complete['pr_closed_by_id'] = df_prs_complete['pr_closed_by_id'].fillna(df_prs_complete['pr_merged_by_id'])

    return df_prs_complete

def CreatePRStats(df_prs_complete):
    # Aggregate repo stats
    df_repo_stats = df_prs_complete.groupby(['repo_name', 'time_period'])\
        .agg({'closed_unmerged_pr': ['sum', 'mean'],
              'merged_pr': ['sum', 'mean'],
              'review_state_commented': 'mean',
              'review_state_approved': 'mean',
              'review_state_changes_requested': 'mean',
              'merged_in_30_days': 'mean',
              'merged_in_60_days': 'mean',
              'merged_in_90_days': 'mean',
              'merged_in_180_days': 'mean',
              'merged_in_360_days': 'mean',
              'closed_pr': ['sum', 'mean'],
              'pr_reviews': ['sum', 'mean']}).reset_index()
    df_repo_stats.columns = [
        col[0] if col[1] in ['sum', ''] else f"p_{col[0]}" 
        for col in df_repo_stats.columns.to_flat_index()
    ]
    df_repo_stats = df_repo_stats.rename(columns={
        'closed_unmerged_pr': 'prs_closed_unmerged',
        'merged_pr': 'prs_merged',
        'closed_pr': 'prs_closed',
        'pr_reviews': 'pr_reviews'
    })
    
    name_map = {'closed_pr': 'prs_closed', 'merged_pr': 'prs_merged', 'closed_unmerged_pr': 'prs_closed_unmerged'}
    actor_cols = list(name_map.keys())
    df_actor = df_prs_complete.groupby(['repo_name', 'time_period', 'pr_closed_by_id'])[actor_cols].sum().reset_index()
    df_actor_agg = df_actor.groupby(['repo_name', 'time_period'])\
        .agg({col: ['mean', lambda s: (s != 0).sum()] for col in actor_cols})\
        .reset_index()
    df_actor_agg.columns = ['repo_name', 'time_period'] + [f"avg_{name_map[col]}" if i % 2 == 0 else f"cc_{name_map[col]}"
                                                           for col in actor_cols for i in (0, 1)]
    df_prs_stats = df_repo_stats.merge(df_actor_agg, on=['repo_name', 'time_period'], how='left')
    return df_prs_stats

def CleanDepartedContributors(indir_departed, time_period, rolling_window, criteria_pct, consecutive_periods, post_periods):
    file_path = Path(indir_departed) / f"filtered_departed_contributors_major_months{time_period}_window{rolling_window}D_criteria_commits_{criteria_pct}pct_consecutive{consecutive_periods}_post_period{post_periods}_threshold_gap_qty_0.parquet"
    df_dep = pd.read_parquet(file_path)
    df_dep['treatment_period'] = pd.to_datetime(df_dep['treatment_period'])

    df_clean = df_dep.loc[
        (df_dep['present_one_after'] == 0) &
        (df_dep['treatment_period'].dt.year < 2023) &
        (~df_dep['abandoned_scraped']) &
        (~df_dep['abandoned_consecutive_req3_permanentTrue'])
    ].copy()

    df_clean['repo_count'] = df_clean.groupby('repo_name')['repo_name'].transform('count')
    df_clean = df_clean.loc[df_clean['repo_count'] == 1].copy()
    df_clean['departed_actor_id'] = df_clean['actor_id']
    df_clean['abandoned_date'] = pd.to_datetime(df_clean['abandoned_date_consecutive_req2_permanentTrue'])
    df_clean_departures = df_clean[['repo_name', 'treatment_period', 'abandoned_date','departed_actor_id']]
  
    return df_clean_departures

def ImputeTreatmentPeriodsForControls(df_clean_departures, df_repo_panel_stats, seed = 42):
    np.random.seed(seed)

    df_repo_panel_dept = df_repo_panel_stats.merge(df_clean_departures, on=['repo_name'], how='left')
    treatment_counts = df_clean_departures['treatment_period'].value_counts(normalize=True)
    treatment_options = treatment_counts.index.tolist()
    treatment_probs = treatment_counts.values.tolist()
    sampleTreatment = lambda: np.random.choice(treatment_options, p=treatment_probs)
    
    df_repo_panel_dept.sort_values(['repo_name','time_period'], inplace = True)
    repo_treatment = {}
    for repo, group in df_repo_panel_dept.groupby('repo_name'):
        existing = group['treatment_period'].dropna().unique()
        repo_treatment[repo] = existing[0] if len(existing) > 0 else sampleTreatment()
    df_repo_panel_dept['control_treatment_period'] = df_repo_panel_dept['repo_name'].map(repo_treatment)
    df_repo_panel_dept['all_treatment_period'] = df_repo_panel_dept['treatment_period'].fillna(df_repo_panel_dept['control_treatment_period'])
    return df_repo_panel_dept

def AddTimePeriodData(df_repo_panel_dept, time_index_map):
    for col in ['time_period','treatment_period']:
        df_repo_panel_dept[col] = pd.to_datetime(df_repo_panel_dept[col])

    df_repo_panel_dept['time_index'] = df_repo_panel_dept['time_period'].map(time_index_map)
    df_repo_panel_dept['treatment_group'] = df_repo_panel_dept['treatment_period'].apply(lambda x: time_index_map.get(x, np.nan))
    df_repo_panel_dept['all_treatment_group'] = df_repo_panel_dept['all_treatment_period'].apply(lambda x: time_index_map.get(x, np.nan))

    df_repo_panel_dept = df_repo_panel_dept[~df_repo_panel_dept['time_period'].isna()]
    return df_repo_panel_dept

def ConstructOutcomesPanel(df_subset, base_panel, df_issue_selected, df_pr_selected, df_clean_departures, time_period,
                       SECONDS_IN_DAY, closing_day_options, outdir_data, outdir_log, suffix, time_index_map):
    merged_panel = base_panel.merge(df_subset, on=['repo_name', 'actor_id'])
    ConstructRepoPanel(merged_panel, df_issue_selected, df_pr_selected, df_clean_departures, time_period,
                       SECONDS_IN_DAY, closing_day_options, outdir_data, outdir_log, suffix, time_index_map)

def GetImportantContributors(df_graph_data):
    return df_graph_data.query(
        '(time_index < all_treatment_group & ~important.isna() & actor_id != departed_actor_id)'
    )[["repo_name", "actor_id"]].drop_duplicates()

def GetEveryonePreDeparture(df_graph_data):
    return df_graph_data.query(
        '(time_index < all_treatment_group & actor_id != departed_actor_id) '
    )[["repo_name", "actor_id"]].drop_duplicates()

def GetEveryone(df_graph_data):
    return df_graph_data.query(
        '(actor_id != departed_actor_id) '
    )[["repo_name", "actor_id"]].drop_duplicates()

def GetUnimportantContributors(everyone_pre, important):
    return everyone_pre.merge(important, on=["repo_name", "actor_id"], how="left", indicator=True) \
                        .query("_merge=='left_only'").drop(columns="_merge")

def GetDepartedContributors(df_graph_data):
    return df_graph_data[["repo_name", "departed_actor_id"]].drop_duplicates()

def GetNewContributors(df_graph_data, everyone_pre, departed):
    new = df_graph_data[["repo_name", "actor_id"]].drop_duplicates() \
        .merge(everyone_pre, on=["repo_name", "actor_id"], how="left", indicator=True) \
        .query("_merge=='left_only'").drop(columns="_merge")
    new = new.merge(departed, on="repo_name") \
        .query("actor_id != departed_actor_id")[["repo_name", "actor_id"]].drop_duplicates()
    return new


if __name__ == '__main__':
    Main()


