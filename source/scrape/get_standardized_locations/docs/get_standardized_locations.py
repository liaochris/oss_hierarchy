import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np
from dateutil.relativedelta import relativedelta
pd.set_option('display.max_columns', None)
import seaborn as sns
import time
from ast import literal_eval
from geopy.geocoders import GoogleV3
from geopy.extra.rate_limiter import RateLimiter
import os

def Main():
    indir_profile = Path("drive/output/scrape/get_linkedin_profiles")
    outdir = Path("drive/output/scrape/get_standardized_locations")

    df_departed_committers = pd.read_csv(indir_profile / 'departed_github_profiles.csv')
    df_linkedin_profiles = pd.read_parquet(indir_profile / 'departed_linkedin_profiles.parquet')

    geolocator = GoogleV3(api_key=os.environ['GMAPS_TOKEN'])
    github_profile_locations = df_departed_committers[['location']].drop_duplicates()
    github_profile_locations = GetStandardizedLocation(github_profile_locations, 'location')
    github_profile_locations.to_csv(outdir / "standardized_locations_github.csv")

    linkedin_profile_locations = df_linkedin_profiles[['user_location','user_country']].drop_duplicates()\
        .query('~user_location.isna()')
    linkedin_profile_locations['user_complete_location'] = linkedin_profile_locations.apply(
        lambda x: x['user_location']+", " + x['user_country'] if 
        not pd.isnull(x['user_country']) and not x['user_location'].endswith(x['user_country']) else
        x['user_location'], axis = 1)
    linkedin_profile_locations = GetStandardizedLocation(linkedin_profile_locations, 'user_complete_location')
    linkedin_profile_locations.to_csv(outdir / "standardized_locations_linkedin.csv")

def GetStandardizedLocation(df, loc_col):    
    df['standardized_location'] = df[loc_col].apply(
        lambda x: GetLocation(x))
    df['standardized_location_address'] = df['standardized_location'].apply(
        lambda x: [loc.address for loc in x]  if type(x) == list else np.nan)
    df['standardized_location_raw'] = df['standardized_location'].apply(
        lambda x: [loc.raw for loc in x]  if type(x) == list else np.nan)
    return df

def GetLocation(location):
    try:
        return geolocator.geocode(location, exactly_one = False)
    except:
        return np.nan