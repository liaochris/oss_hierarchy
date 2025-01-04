import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np
from dateutil.relativedelta import relativedelta
pd.set_option('display.max_columns', None)
import seaborn as sns
import wrds
import time
from ast import literal_eval
import requests
from bs4 import BeautifulSoup

def Main():
    indir_committers = Path('drive/output/scrape/link_committers_profile')
    outdir_committers = indir_committers
    df_committers_info = pd.concat([pd.read_csv(indir_committers / 'committers_info_pr.csv', index_col = 0).dropna(),
                                    pd.read_csv(indir_committers / 'committers_info_push.csv', index_col = 0).dropna()])

    df_committers_info = CleanCommittersInfo(df_committers_info)
    df_aggregated_committers = AggregateCommitters(df_committers_info)
    df_aggregated_committers = FillInMissingGitHubData(df_aggregated_committers)

    df_profiles = df_aggregated_committers[['profile_url']].drop_duplicates()

    for i in df_profiles.index:
        print(f"Processing Index: {i}")
        profile_url = df_profiles.loc[i, 'profile_url']
        scrape_result = ScrapeGitHubProfile(profile_url)
        attempts = 0
        while isinstance(scrape_result, bool) and attempts < 5:
            time.sleep(2)
            scrape_result = ScrapeGitHubProfile(profile_url)
            attempts+=1
        df_profiles.loc[i, 'profile_data'] = [scrape_result]

    df_profile_data = pd.merge(df_aggregated_committers, df_profiles)
    df_profile_data.to_csv(outdir_committers / 'committers_info.csv')

def CleanCommittersInfo(df_committers_info):
    df_committers_info = SubsetComittersInfo(df_committers_info)
    df_committers_info = ParseCommittersInfo(df_committers_info)
    df_committers_info = CleanCommittersVars(df_committers_info)

    return df_committers_info

def SubsetComittersInfo(df_committers_info):
    df_committers_info['committer_info'] = df_committers_info['committer_info'].apply(literal_eval)
    df_committers_info['commit_repo'] = df_committers_info['commit_repo'].apply(literal_eval)
    df_committers_info['committer_info_length'] = df_committers_info['committer_info'].apply(len)
    df_committers_info = df_committers_info[df_committers_info['committer_info_length'].isin([1,2,4])]
    return df_committers_info

def ParseCommittersInfo(df_committers_info):
    df_committers_info['actor_name'] = df_committers_info.apply(
        lambda x: x['committer_info'][0] if x['committer_info_length'] in [1,4] else x['committer_info'][1], axis = 1)
    df_committers_info['actor_id'] = df_committers_info.apply(
        lambda x: x['committer_info'][1] if x['committer_info_length'] == 4 else 
        x['committer_info'][0] if x['committer_info_length'] == 2 else np.nan, axis = 1)
    df_committers_info['repo_name'] = df_committers_info['commit_repo'].apply(lambda x: list(set([ele.split("_")[1] for ele in x])))
    df_committers_info = df_committers_info.explode('repo_name')

    return df_committers_info
    
def CleanCommittersVars(df_committers_info):
    df_committers_info['human_name'] = df_committers_info['name'].apply(lambda x: CleanName(x))
    df_committers_info['actor_name'] = df_committers_info['actor_name'].fillna('')
    df_committers_info['email_address'] = df_committers_info['email'].apply(lambda x: np.nan if pd.isnull(x) or x.endswith("@users.noreply.github.com") else x)

    return df_committers_info
    
def CleanName(name):
    if pd.isnull(name):
        return name
    name = name.title()
    if len(name.split(" "))<2:
        return np.nan
    return name

def AggregateCommitters(df_committers_info):
    df_aggregated_committers = df_committers_info[['actor_id','actor_name','repo_name','human_name','email_address','name','email']].drop_duplicates()\
        .groupby(['actor_id','actor_name','repo_name'])\
        .agg({'human_name':'unique','email_address':'unique','name':'unique','email':'unique'})\
        .reset_index()
    df_aggregated_committers = CleanAggregatedCommittersVars(df_aggregated_committers)
    
    return df_aggregated_committers

def CleanAggregatedCommittersVars(df_aggregated_committers):
    df_aggregated_committers['actor_id'] = pd.to_numeric(df_aggregated_committers['actor_id'], errors = 'coerce')\
    .replace('',np.nan)
    df_aggregated_committers['actor_name'] = df_aggregated_committers['actor_name'].apply(
        lambda x: x if x.replace("_","").replace("-","").replace("[","").replace("]","").isalnum() else np.nan)
    df_aggregated_committers['actor_name'] = df_aggregated_committers['actor_name'].replace('' ,np.nan)

    for col in ['human_name','email_address']:
        df_aggregated_committers[col] = df_aggregated_committers[col].apply(DropNAList)
    df_aggregated_committers.rename({'name':'commit_name','email':'commit_email'}, axis = 1, inplace = True)
    df_aggregated_committers.dropna(subset = ['actor_id','actor_name'], thresh = 1, inplace = True)

    return df_aggregated_committers

def DropNAList(lst):
    return [x for x in lst if not pd.isnull(x)]

def FillInMissingGitHubData(df_aggregated_committers):
    df_aggregated_committers['actor_name'] = df_aggregated_committers.apply(
        lambda x: x['actor_name'] if not pd.isnull(x['actor_name']) else GetGitHubAPIData(x['actor_id'], 'login'), axis = 1)
    df_aggregated_committers['actor_id'] = df_aggregated_committers.apply(
        lambda x: x['actor_id'] if not pd.isnull(x['actor_id']) else GetGitHubAPIData(x['actor_name'], 'id'), axis = 1)
    df_aggregated_committers.dropna(subset = ['actor_id','actor_name'], thresh = 2, inplace = True)
    df_aggregated_committers['profile_url'] = df_aggregated_committers['actor_name'].apply(
        lambda x: f"https://github.com/{x}")

    return df_aggregated_committers


def GetGitHubAPIData(user_attr, extract_attr):
    if extract_attr == 'login':
        url = f"https://api.github.com/user/{user_attr}"
    if extract_attr == 'id':
        url = f"https://api.github.com/users/{user_attr}"
    response = requests.get(url)
    if response.status_code != 200:
        return np.nan
    data = response.json()
    return data.get(extract_attr, np.nan)

def ScrapeGitHubProfile(profile_url):
    response = requests.get(profile_url)
    if response.status_code != 200:
        return False
    
    soup = BeautifulSoup(response.text, 'html.parser')
    name = ParseGitHubProfile(soup, '.p-name')
    company = ParseGitHubProfile(soup, '.p-org')
    location = ParseGitHubProfile(soup, '.p-label')
    bio = ParseGitHubProfile(soup, '.user-profile-bio')
    
    linkedin_url = None
    for link_tag in FilterForLinks(soup.select('a[href]')):
        if "linkedin.com" in href.lower():
            linkedin_url = href
            break
    
    blog_url = None
    blog_link_tags = soup.select('a.Link--primary[rel="nofollow me"]')
    for link_tag in FilterForLinks(blog_link_tags):
        if "linkedin.com" not in href.lower():
            blog_url = href
            break

    return {
        "name": name,
        "company": company,
        "location": location,
        "bio": bio,
        "linkedin_url": linkedin_url,
        "blog_url": blog_url
    }

def ParseGitHubProfile(soup, attr)
    tag = soup.select_one(attr)
    val = tag.get_text(strip=True) if tag else None
    return val
    
def FilterForLinks(lst):
    return [ele['href'] for ele in lst]