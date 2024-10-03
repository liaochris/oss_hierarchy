### Overview
This folder contains event-level data on a few thousand top GitHub repositories. 

### Source
Chris Liao queried this data using Google BigQuery on October 1, 2024.

### When/where obtained & original form of files
In order to access the Google Cloud BigQuery data for python downloads, I used the Python Client for Google BigQuery. 
1. First, I followed the Quick Start [documentation setup steps](https://cloud.google.com/python/docs/reference/bigquery/latest)
   - For step 3, the BigQuery API is automatically enabled for new projects
   - For step 4, we're using `Client-Provided Authentication` so [create a service account](https://cloud.google.com/iam/docs/service-accounts-create#iam-service-accounts-create-console), create a new key, download the `.json` file with your key and then run `export GOOGLE_APPLICATION_CREDENTIALS=<<downloaded_json_location>>`
2. I then installed `google-cloud-bigquery` in my virtual environment
3. I ran the script `source/scrape/docs/query_github_data.py` to obtain event-level GitHub data

### Description
- `docs/query_github_data.py` queries Google BQ for GitHub data
- `docs/README.md` contains the README.md of the gharchive.org repository
- `docs/LICENSE.md` contains the LICENSE.md of the gharchive.org repository

### Terms of Use
From the [GitHub Archive repository website](https://github.com/igrigorik/gharchive.org) readme:

```
MIT, for code and documentation in this repository, see LICENSE.md.

www.gharchive.org website content (gh-pages branch) is also released under CC-BY-4.0, which gives you permission to use the content for almost any purpose but does not grant you any trademark permissions, so long as you note the license and give credit, such as follows:

Content based on www.gharchive.org used under the CC-BY-4.0 license.
Note this repository does not contain the GH Archive dataset (event archives/data). The dataset includes material that may be subject to third party rights.
```

### Notes
- Uses `output/derived/collect_github_repos/linked_pypi_github.csv` to filter GitHub repositories
- Total cost of queries does not exceed Google BQ's $300 dollars in credit. 