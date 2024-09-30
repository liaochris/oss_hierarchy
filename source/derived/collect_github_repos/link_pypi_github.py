#!/usr/bin/env python3

import json
from pandarallel import pandarallel
import pandas as pd
from pathlib import Path
import os
import sys
sys.path.append('source/lib/JMSLab')
from SaveData import SaveData
pandarallel.initialize(progress_bar = True)

def Main():
    pypi_json_dir = 'drive/output/scrape/pypi_site_info'
    python_packages = os.listdir(pypi_json_dir)

    python_package_info = pd.Series(python_packages).parallel_apply(lambda package: GetPackageInfo(pypi_json_dir, package))
    df_linked = BuildLinkedDataframe(python_packages, python_package_info)

    df_linked.to_csv('output/derived/collect_github_repos/linked_pypi_github.csv')

def GetPackageInfo(pypi_json_dir, package):
    with open(Path(pypi_json_dir) / package, 'r') as raw_json:
        url_val, license_val = "Unavailable", "Unavailable"
        try:
            json_file = json.load(raw_json)
        except:
            return url_val, license_val

        # add homepage
        try:
            all_urls = json_file['info']['project_urls']
            all_urls = list(set(["/".join(ele.split("/")[:5]) for ele in all_urls.values() if ele.startswith('https://github.com/')]))
            if len(all_urls)>1:
                url_val = "Unavailable"
            else:
                url_val = all_urls[0]
        except:
            url_val = "Unavailable"
    
        # add license
        try:
            license_val = " | ".join([ele for ele in json_file['info']['classifiers'] if 'License' in ele])
        except:
            license_val = "Unavailable"
    return url_val, license_val

def BuildLinkedDataframe(python_packages, python_package_info):
    df_linked = pd.concat([ pd.Series(python_packages), pd.DataFrame(python_package_info)], axis = 1)
    df_linked.columns =  ['package', 'data']
    df_linked['package'] = df_linked['package'].apply(lambda x: x.replace('.json', ''))
    df_linked['homepage'] = df_linked['data'].apply(lambda x: x[0])
    df_linked['license'] = df_linked['data'].apply(lambda x: x[1])

    df_linked['license'] = df_linked['license'].replace('', 'Unavailable')
    df_linked['github repository'] = df_linked['homepage'].apply(lambda x: 'Unavailable' if 'https://github.com/' not in x else x.replace('https://github.com/',''))
    df_linked['github repository'] = df_linked['github repository'].apply(lambda x: "Unavailable" if "/" not in x else x)

    df_linked['license'] = df_linked['license'].apply(lambda x: [ele.split(" :: ")[-1].strip() for ele in x.split("|")])

    return df_linked[['package', 'github repository', 'license']]

if __name__ == '__main__':
    Main()
