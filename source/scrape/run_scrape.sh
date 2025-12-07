#!/usr/bin/env bash

if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo "ERROR: GOOGLE_APPLICATION_CREDENTIALS is not set."
    exit 1
fi

python source/scrape/pypi_downloads/code/query_monthly_pip_downloads.py
python source/scrape/pypi_downloads/code/get_popular_packages.py

bash source/scrape/pypi_site_info/code/get_library_json.sh > output/scrape/pypi_site_info/get_library_json.log 2>&1
python source/scrape/pypi_site_info/code/link_pypi_github.py

python source/scrape/extract_github_data/code/query_all_repo_identities.py
python source/scrape/extract_github_data/code/group_repo_identities.py
python source/scrape/extract_github_data/code/query_github_data.py

python source/scrape/extract_governance_data/code/collect_governance_data.py