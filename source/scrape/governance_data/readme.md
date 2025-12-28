### Overview
This folder contains code to scrape github repositories to check whether they had particular governnance files. 

### Source
Chris Liao queried this data using `git` on INSERT DATE.

### When/where obtained & original form of files
Scraped by cloning repositories using `git clone` and then querying the logs. 

### Description
- `code/collect_governance_data.py` groups repo name and ID pairs into GitHub repos and identifies forks
- `docs/Github_TOU.md` contains the terms of usage for GitHub.

### Terms of Use
Per the docs, don't use the data to spam people. 

### Run Order
1. Run `source/scrape/extract_governance_data/code/collect_governance_data.py`