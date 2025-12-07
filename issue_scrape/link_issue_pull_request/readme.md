### Overview
This folder contains code that scrapes GitHub to link issues to pull requests and vice versa

### Source
Chris Liao scraped this data in October. 

### When/where obtained & original form of files
To obtain information on the issues that were solved by a pull request, and the issue a pull request was solving, I scraped Github's issue and pull request pages. 

### Description
- `docs/link_using_issue.py` gets the pull request linked to each issue for the repositories that are part of my study
- `docs/link_using_pull_request.py` gets the issue linked to each pull request for the repositories that are part of my study
- `docs/github_tou.pdf` has a saved version of GitHub's TOU from Oct 2, 2024

### Terms of Use
According to Github

> You may use information from our Service for the following reasons, regardless of whether the information was scraped, collected through our API, or obtained otherwise:
Researchers may use public, non-personal information from the Service for research purposes, only if any publications resulting from that research are open access.
Archivists may use public information from the Service for archival purposes.
Scraping refers to extracting information from our Service via an automated process, such as a bot or webcrawler. Scraping does not refer to the collection of information through our API. Please see Section H of our Terms of Service for our API Terms.
You may not use information from the Service (whether scraped, collected through our API, or obtained otherwise) for spamming purposes, including for the purposes of sending unsolicited emails to users or selling personal information, such as to recruiters, headhunters, and job boards.


### Notes
- `docs/link_using_issue.py` and `docs/link_using_pull_request.py` can be run concurrently on multiple virtual machines that share the same drive. I do so in order to finish the task in a reasonable amount of time. 