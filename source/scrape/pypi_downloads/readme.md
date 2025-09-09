### Overview
This folder contains monthly download data on python libraries from PyPi. 

### Source
Chris Liao queried this data using Google BigQuery on September 29, 2024. 

### When/where obtained & original form of files
In order to access the Google Cloud BigQuery data for python downloads, I used the Python Client for Google BigQuery. 
1. First, I followed the Quick Start [documentation setup steps](https://cloud.google.com/python/docs/reference/bigquery/latest)
   - For step 3, the BigQuery API is automatically enabled for new projects
   - For step 4, we're using `Client-Provided Authentication` so create a service account, download the `.json` file with your keys and then run `export GOOGLE_APPLICATION_CREDENTIALS=<<downloaded_json_location>>`
2. I then installed `google-cloud-bigquery` in my virtual environment
3. I ran the script `source/scrape/docs/query_monthly_pip_downloads.py` to obtain monthly downloads data 

### Description
- `drive/source/scrape/pypi_monthly_downloads.csv` contains the monthly download data 
- `docs/query_monthly_pip_downloads.py` queries Google BQ for PyPI Download data
- `docs/get_popular_packages.py` extracts the popular python libraries (10k+ downloads in every month between July 2018 and September 2023)
- `data/popular_python_packages.csv` contains the extracted popular lbiraries
- `docs/pypistats_about.py` contains the about page for [pypistats.org](pypistats.org)

### Terms of Use
Official TOU cannot be found on the [website](pypistats.org) but the service is open source. 

### Notes
- `data/popular_python_packages.csv` is used by `source/scrape/pypi_site_info` 