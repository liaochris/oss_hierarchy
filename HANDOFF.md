revise architecture by ensuring that run + extended s always mutually exclusive in configs, and that extended includes run + extended...

## Duplicate "pull request opened" events per thread (fix upstream)
Some PR threads carry multiple "pull request opened" events from distinct actors (apparent reopens / mis-attributed events), so member-level open counts (distinct threads per actor) sum to more than the repo's distinct-thread open count, pushing sum(prob_open) > 1 in the staged model.
Currently hotfixed downstream in `source/derived/model_prediction/build_event_time_member_panel.py` (SeparateActionTypes keeps only the earliest open per thread).
The real fix belongs upstream in `source/scrape/extract_github_data/code/transform_event_to_repo_level.py` so every consumer of the action data sees one opener per thread; once fixed there, remove the downstream hotfix. 