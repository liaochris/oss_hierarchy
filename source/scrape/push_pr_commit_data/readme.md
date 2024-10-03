### Overview
This folder contains code that scrapes GitHub and queries the Github API for commit sha's of pushes and pull requests

### Source
Chris Liao scraped this data starting October 2nd, 2024. The process 

### When/where obtained & original form of files
To obtain information on commits for pushes, I queried GitHub's API. To do so, you need a GitHub account and a [personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens). 

To obtain information on commits for pull requests, I scraped GitHub. 

### Description
- `docs/get_pull_request_commits.py` gets commit SHAs for all pull requests in my data
- `docs/get_push_commits.py` gets commit SHAs for all pushes in my data that don't already have complete commit SHA data
- `docs/github_tou.pdf` has a saved version of GitHub's TOU from Oct 2, 2024

### Terms of Use
According to Github

> You may use information from our Service for the following reasons, regardless of whether the information was scraped, collected through our API, or obtained otherwise:
> 
> Researchers may use public, non-personal information from the Service for research purposes, only if any publications resulting from that research are open access.
> Archivists may use public information from the Service for archival purposes.
> Scraping refers to extracting information from our Service via an automated process, such as a bot or webcrawler. Scraping does not refer to the collection of information through our API. Please see Section H of our Terms of Service for our API Terms.
> 
> You may not use information from the Service (whether scraped, collected through our API, or obtained otherwise) for spamming purposes, including for the purposes of sending unsolicited emails to users or selling personal information, such as to recruiters, headhunters, and job boards.



### Notes
- `docs/get_pull_request_commits.py` can be run concurrently on multiple virtual machines that share the same drive. I do so in order to finish the task in a reasonable amount of time. 