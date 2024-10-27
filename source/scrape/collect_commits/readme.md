### Overview
This folder contains data on commits that were made in pull requests for the repositories I am interested in

### Source
Chris Liao cloned repositories from Github in October 2024

### When/where obtained & original form of files
The data was obtained using the git log from the cloned repositories

### Description
- `docs/get_commit_data.py` queries the Github API to retrieve information for committers based off the commit and repository information.
- `docs/github_tou.pdf` has a saved version of the TOU for Github's API from October 2021, 2024

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
- `docs/get_commit_data.py` can be run concurrently on multiple virtual machines that share the same drive. I do so in order to finish the task in a reasonable amount of time.
- Running this task requires significant free hard drive space, as the cloned repositories can be quite large