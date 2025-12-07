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

### Description
- `code/query_monthly_pip_downloads.py` queries Google BQ for PyPI Download data
- `code/get_popular_packages.py` extracts the popular python libraries 
  - See [this link](https://pypistats.org/faqs#:~:text=Why%20are%20there,dropped%20%20%20%20%20%20%20%20download%20records.) for why I consider only post July 201 downloads
- `data/popular_python_packages.csv` contains the extracted popular lbiraries
- `docs/pypistats_about.pdf` contains the about page for [pypistats.org](pypistats.org)

### Terms of Use
Official TOU cannot be found on the [website](pypistats.org) but the service is open source. 

### Run order
1. Set up `GOOGLE_APPLICATION_CREDENTIALS` following above
2. Run `source/scrape/pypi_downloads/code/query_monthly_pip_downloads.py`
3. Run `source/scrape/pypi_downloads/code/get_popular_packages.py`