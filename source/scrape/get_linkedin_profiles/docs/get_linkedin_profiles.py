import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np
from dateutil.relativedelta import relativedelta
pd.set_option('display.max_columns', None)
import seaborn as sns
import wrds
import re
from ast import literal_eval
import os

def Main():
    indir_departures = Path('drive/output/derived/contributor_stats/departed_contributors')
    indir_committers = Path('drive/output/scrape/link_committers_profile')
    outdir_profiles = Path('drive/output/scrape/get_linkedin_profiles')

    df_all_departures = AggregateAllDepartures(indir_departures)
    all_committer_info = pd.read_csv(indir_committers / 'committers_info.csv', index_col = 0)
    all_committer_info = CleanInfo(all_committer_info)
    
    departure_committer_info = pd.merge(df_all_departures, all_committer_info, how = 'left')
    print("Step 1: linkedin on Profile")
    print("Linkedin Obtained: {:.2f}%".format(100*(1-departure_committer_info['linkedin_url'].isna().mean())))

    departure_committer_info = ParseBioName(departure_committer_info)
    print("Linkedin or valid name obtained: {:.2f}%".format(
    100*(1-departure_committer_info[['linkedin_url','name_clean']].isna().all(axis=1).mean())))

    derived_commit_names_dict = GetCleanCommitNameMap(departure_committer_info)
    departure_committer_info['name_clean'] = departure_committer_info.apply(
        lambda x: [x['name_clean']] if not type(x['name_clean']) != float else derived_commit_names_dict.get(x['profile_url'], np.nan), axis = 1)
    print("Linkedin or valid name obtained: {:.2f}%".format(
        100*(1-departure_committer_info[['linkedin_url','name_clean']].isna().all(axis=1).mean())))

    departure_committer_info.to_csv(outdir_profiles / 'departed_github_profiles.csv')

    db = wrds.Connection(wrds_username=os.environ["WRDS_USERNAME"])
    df_linkedin_profiles = QueryForLinkedinProfiles(departure_committer_info, db, outdir_profiles)
    df_linkedin_profiles.to_parquet(outdir_profiles / 'departed_linkedin_profiles.parquet')
    db.close()

    db = wrds.Connection(wrds_username=os.environ["WRDS_USERNAME"])
    df_positions = QueryForLinkedinPositions(df_linkedin_profiles, db)
    df_positions.to_parquet(outdir_profiles / 'departed_linkedin_positions.parquet')
    db.close()



def AggregateAllDepartures(indir_departures):
    # later decide if I want to expand to issue comments - but this is not supported by lit
    non_gap_files = list(set(indir_departures.glob("*commits*.parquet")) - set(indir_departures.glob("*commits*threshold_gap_qty*.parquet")))
    gap_files = list(set(indir_departures.glob("*commits*threshold_gap_qty*.parquet")))

    df_all_departures_gap = pd.concat([pd.read_parquet(file) for file in gap_files])
    df_all_departures_nongap = pd.concat([pd.read_parquet(file) for file in non_gap_files])

    df_all_departures_gap_uq = df_all_departures_nongap[['repo_name','actor_id']].drop_duplicates()
    df_all_departures_nongap_uq = df_all_departures_gap.query(
        'below_qty_mean_gap0 == 1 | below_qty_mean_gap1 == 1')[['repo_name','actor_id']].drop_duplicates()
    df_all_departures = pd.concat([df_all_departures_gap_uq, df_all_departures_nongap_uq]).drop_duplicates()

    return df_all_departures

def CleanInfo(all_committer_info):
    all_committer_info = all_committer_info.drop_duplicates(['repo_name','actor_id'])
    for col in ['human_name', 'email_address', 'commit_name', 'commit_email']:
        all_committer_info[col] = all_committer_info[col].apply(literal_eval)
    all_committer_info['profile_data'] = all_committer_info['profile_data'].apply(lambda x: literal_eval(x)[0] if type(x) != float else x)
    
    for col in ['name','company','location','bio','linkedin_url','blog_url']:
        all_committer_info[col] = all_committer_info['profile_data'].apply(lambda x: x[col] if type(x) == dict else np.nan)
    all_committer_info.drop('profile_data',axis = 1, inplace = True)
    all_committer_info['name'] = all_committer_info['name'].replace("",np.nan)
    
    return all_committer_info

def NameFilter(departure_committer_info, name_col):
    departure_committer_info[name_col] = departure_committer_info[name_col].apply(
        lambda x: x if not pd.isnull(x) else np.nan)
    departure_committer_info[name_col] = departure_committer_info[name_col].apply(
        lambda x: x if type(x) != float and len(x.split(" "))>=2 else np.nan)
    departure_committer_info[name_col] = departure_committer_info[name_col].apply(
        lambda x: x if type(x) != float and sum([len(name)>=2 for name in x.split(" ")])>=2 else np.nan)
    return departure_committer_info

def NameCleaner(departure_committer_info):
    departure_committer_info_unparseable = departure_committer_info[departure_committer_info['name'].apply(lambda x: type(x) == float)]
    departure_committer_info = departure_committer_info[departure_committer_info['name'].apply(lambda x: type(x) != float)]
    pattern = r'\([^)]*\)|\[[^\]]*\]|"[^"]*"|\'[^\']*\'|<[^>]*>|{[^}]*}|\`[^)]*\`'
    departure_committer_info['name_clean'] = departure_committer_info['name'].apply(lambda x: re.sub(pattern, '', x))
    # 1) Remove everything from '@' to the end of the line (including '@')
    #    e.g., "Hello @someDomain.com" -> "Hello "
    departure_committer_info['name_clean'] = departure_committer_info['name_clean'].apply(lambda x: re.sub(r'@.*', '', x))
    # 2) Remove certain punctuation/symbols: 
    #    asterisks (*), exclamation marks (!), colons (:), semicolons (;), 
    #    equals (=), carets (^), periods (.), tildes (~), underscores (_).
    departure_committer_info['name_clean'] = departure_committer_info['name_clean'].apply(lambda x: re.sub(r'[!\*\:\;\=\^\.\~_]+', '', x))
    # 3a) Remove a broad range of emojis (including flags).
    #     This pattern targets many common Unicode emoji blocks:
    #        - U+1F600–U+1F64F  (Emoticons)
    #        - U+1F300–U+1F5FF  (Misc Symbols and Pictographs)
    #        - U+1F680–U+1F6FF  (Transport and Map)
    #        - U+1F1E0–U+1F1FF  (Flags)
    #     If you only want to remove ASCII emoticons like :-) or :D, 
    #     replace this with a simpler pattern.
    emoji_pattern = re.compile("["                   
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags
                               "(╯°□）︵♫"
                               "]+", flags=re.UNICODE)
    departure_committer_info['name_clean'] = departure_committer_info['name_clean'].apply(lambda x: emoji_pattern.sub(r'', x))
    # 3b) (Optional) Remove ASCII-style emoticons if needed:
    #     e.g., :-) :D ;P etc.
    departure_committer_info['name_clean'] = departure_committer_info['name_clean'].apply(
        lambda x: re.sub(r'(?::|;|=|8)(?:-)?(?:\)|\(|D|P)', '', x))
    # 4) Remove ", MBA", ", PhD", "Ph. D.", ", DVM", etc., including variants 
    #    with different capitalization and optional periods.  
    #    This pattern tries to catch:
    #       - Comma + optional space + "MBA" (with optional dots and varying case)
    #       - Comma + optional space + "PhD" (with optional dots/space)
    #       - "PhD" / "Ph. D." standalone
    #       - Comma + optional space + "DVM" (with optional dots)
    #    Expand/adjust if you need more thorough matching of variants.
    degrees_pattern = re.compile(
        r"(?ix)"                  # ignore case + verbose
        r"(,\s*M\.?\s*B\.?\s*A\.?)"  # Matches ", MBA" with or without dots
        r"|(,\s*P\.?\s*H\.?\s*D\.?)"  # Matches ", PhD" with or without dots
        r"|(P\.?\s*H\.?\s*D\.?)"     # Matches "PhD" or "Ph. D."
        r"|(,\s*D\.?\s*V\.?\s*M\.?)" # Matches ", DVM"
    )
    departure_committer_info['name_clean'] = departure_committer_info['name_clean'].apply(lambda x:  degrees_pattern.sub("",x))
    # 5) Remove all exponent (superscript/subscript) characters in typical Unicode blocks:
    #    - \u00B2-\u00B3, \u00B9  (², ³, ¹)
    #    - \u2070-\u209F         (the "Superscripts and Subscripts" block)
    departure_committer_info['name_clean'] = departure_committer_info['name_clean'].apply(lambda x: re.sub(r'[\u00B2-\u00B3\u00B9\u2070-\u209F]+', '', x))
    # 6) Collapse extra whitespace
    departure_committer_info['name_clean'] = departure_committer_info['name_clean'].apply(lambda x: re.sub(r'\s+', ' ', x).strip())
    # 7) Reorder names that appear like Last Name, First Name, except when its Suffix, Name
    pattern = re.compile(
    r'^\s*(?P<lastname>[A-Za-z]+(?:[ \-][A-Za-z]+)*)\s*,\s*'
    r'(?P<firstname>[A-Za-z]+(?:[ \-][A-Za-z]+)*)(?:\s+(?P<suffix>[IVXLCDM]+))?\s*$',
    re.IGNORECASE
    )
    departure_committer_info['name_clean'] = departure_committer_info['name_clean'].apply(
        lambda x: x if not pattern.match(x) else f"{pattern.match(x).group('firstname')} {pattern.match(x).group('lastname')} {match.group('suffix')}" 
    if pattern.match(x).group('suffix') else f"{pattern.match(x).group('firstname')} {pattern.match(x).group('lastname')}")
    # 8) Only keep words that do not have any numbers
    departure_committer_info['name_clean'] = departure_committer_info['name_clean'].apply(
        lambda x: " ".join([word for word in x.split(" ") if not any(char.isdigit() for char in word)]))
    departure_committer_info = pd.concat([departure_committer_info, departure_committer_info_unparseable])
    return departure_committer_info

def ParseBioName(departure_committer_info):
    departure_committer_info = NameFilter(departure_committer_info, 'name')
    departure_committer_info = NameCleaner(departure_committer_info)
    departure_committer_info = NameFilter(departure_committer_info, 'name_clean')
    return departure_committer_info

def GetCleanCommitNameMap(departure_committer_info):
    derived_commit_names = departure_committer_info[['profile_url','human_name']].explode('human_name')\
        .dropna().rename({'human_name':'name'}, axis = 1)
    derived_commit_names = NameCleaner(derived_commit_names).drop('name', axis = 1)
    derived_commit_names = NameFilter(derived_commit_names, 'name_clean')
    derived_commit_names['name_clean'] = derived_commit_names['name_clean'].replace('',np.nan)
    derived_commit_names = derived_commit_names.drop_duplicates().dropna()
    derived_commit_names_dict = derived_commit_names.groupby('profile_url').agg({'name_clean':list}).to_dict()['name_clean']
    return derived_commit_names_dict

def QueryForLinkedinProfiles(departure_committer_info, db, outdir_profiles):
    query_names = departure_committer_info.query('linkedin_url.isna()')['name_clean'].explode().dropna().unique()
    query_names = tuple([name for name in query_names if name.replace(" ","").isalnum()])
    params = {"fullname":query_names}
    query = f"""SELECT * FROM revelio.individual_user WHERE fullname IN %(fullname)s"""

    df_linkedin_profiles = db.raw_sql(query, params = params)

    return df_linkedin_profiles

def QueryForLinkedinPositions(df_linkedin_profiles, db):
    revelio_userid = tuple(df_linkedin_profiles['user_id'].astype(int).unique().tolist())
    params = {"revelio_userid_list" : revelio_userid}
    userid_query = f"""SELECT * FROM revelio.individual_positions WHERE User_id IN %(revelio_userid_list)s"""

    df_positions = db.raw_sql(userid_query, params = params)
    return df_positions
