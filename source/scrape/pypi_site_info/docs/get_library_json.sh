#! /usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

PACKAGES="$(cat "output/scrape/pypi_monthly_downloads/popular_python_packages.csv" | cut -d"," -f2)"

# download jsons in
for p in $PACKAGES; do 
    if [ -f "drive/output/scrape/pypi_site_info/$p.json" ]; then
        echo "File drive/output/scrape/pypi_site_info/$p.json already exists"
    else
        curl -s "https://pypi.org/pypi/$p/json" >> "drive/output/scrape/pypi_site_info/$p.json"
    fi
done