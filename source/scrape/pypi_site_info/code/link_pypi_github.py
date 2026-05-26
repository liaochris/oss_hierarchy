#!/usr/bin/env python3

import json
from pandarallel import pandarallel
import pandas as pd
from pathlib import Path
import os
from source.lib.JMSLab.SaveData import SaveData
pandarallel.initialize(progress_bar = True)

def Main():
    pypi_json_dir = 'drive/output/scrape/pypi_site_info'
    OUTDIR = Path('output/scrape/pypi_site_info')
    python_packages = os.listdir(pypi_json_dir)

    python_package_info = pd.Series(python_packages).parallel_apply(lambda package: GetPackageInfo(pypi_json_dir, package))
    df_linked = BuildLinkedDataframe(python_packages, python_package_info)

    SaveData(df_linked, ['package'],
            OUTDIR / 'linked_pypi_github.csv',
            OUTDIR  / 'linked_pypi_github.log')

def GetHomepage(json_file):
    try:
        all_urls = json_file['info']['project_urls']
        all_urls = list(set(["/".join(ele.split("/")[:5]) for ele in all_urls.values() if ele.startswith('https://github.com/')]))
        if len(all_urls) != 1:
            return "Unavailable"
        return all_urls[0] if all_urls else "Unavailable"
    except:
        return "Unavailable"

def GetLicense(json_file):
    try:
        return " | ".join([ele for ele in json_file['info']['classifiers'] if 'License' in ele])
    except:
        return "Unavailable"

def GetPackageInfo(pypi_json_dir, package):
    with open(Path(pypi_json_dir) / package, 'r') as raw_json:
        try:
            json_file = json.load(raw_json)
        except:
            return "Unavailable", "Unavailable"

        url_val = GetHomepage(json_file)
        license_val = GetLicense(json_file)
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

    df_linked['license'] = df_linked['license'].apply(lambda x: "|".join([ele.split(" :: ")[-1].strip() for ele in x.split("|")]))

    return df_linked[['package', 'github repository', 'license']]

if __name__ == '__main__':
    Main()
