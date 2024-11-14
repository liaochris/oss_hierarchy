### Overview
This folder contains data on the GitHub usernames for individuals who made commits in the repositories I am interested in.

### Source
Chris Liao queried the GitHub API starting October 21 to obtain this data. 

### When/where obtained & original form of files
To obtain information on commits for pushes and pull requests, I queried GitHub's API. To do so, you need a GitHub account and a [personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens). I also set the environment variables `PRIMARY_GITHUB_USERNAME` to my GitHub username and `PRIMARY_GITHUB_TOKEN` to my GitHub personal access token. I use `BACKUP_GITHUB_USERNAME` and `BACKUP_GITHUB_TOKEN` to increase my rate limit request from 5,000/hour to 10,000/hour.

### Description
- `docs/get_committers_profile.py` queries the Github API to retrieve information for committers based off the commit and repository information.
- `docs/github_api_tou.pdf` has a saved version of the TOU for Github's API from October 2021, 2024

### Terms of Use
According to Github

> Short version: You agree to these Terms of Service, plus this Section H, when using any of GitHub's APIs (Application Provider Interface), including use of the API through a third party product that accesses GitHub.
Abuse or excessively frequent requests to GitHub via the API may result in the temporary or permanent suspension of your Account's access to the API. GitHub, in our sole discretion, will determine abuse or excessive usage of the API. We will make a reasonable attempt to warn you via email prior to suspension.
You may not share API tokens to exceed GitHub's rate limitations. You may not use the API to download data or Content from GitHub for spamming purposes, including for the purposes of selling GitHub users' personal information, such as to recruiters, headhunters, and job boards.
All use of the GitHub API is subject to these Terms of Service and the GitHub Privacy Statement.
GitHub may offer subscription-based access to our API for those Users who require high-throughput access or access that would result in resale of GitHub's Service.

