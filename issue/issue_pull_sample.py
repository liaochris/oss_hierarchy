import pandas as pd
from pathlib import Path

INDIR_ACTIONS = Path("drive/output/derived/problem_level_data/repo_actions")

df = pd.read_csv("issue/repo_name_list.csv", index_col=0)
repo_names = df["repo_name"].str.replace("/", "_", n=1)

def LoadRepoActions(INDIR_ACTIONS, repo_name):
    parquet_path = INDIR_ACTIONS / f"{repo_name}.parquet"
    if not parquet_path.exists():
        return None
    return pd.read_parquet(parquet_path)

repo_action_frames = [
    repo_action_frame
    for repo_name in repo_names
    if (repo_action_frame := LoadRepoActions(INDIR_ACTIONS, repo_name)) is not None
]
print(f"NUMBER OF REPOS {len(repo_names)}")
print(f"NUMBER OF REPOS WITH ACTIONS {len(repo_action_frames)}")
df_all_actions = pd.concat(repo_action_frames, ignore_index=True)

DROP_COLS = ['actor_id','assignees','requested_reviewers','discussion_id','opener_id','action_id']
df_all_issues = df_all_actions.drop(columns=DROP_COLS).query('type == "issue opened"')
df_all_prs = df_all_actions.drop(columns=DROP_COLS).query('type == "pull request opened"')

df_all_issues['url'] = df_all_issues.apply(lambda row: f"github.com/{row['repo_name']}/issues/{row['thread_number']}", axis = 1)
df_all_prs['url'] = df_all_prs.apply(lambda row: f"github.com/{row['repo_name']}/pulls/{row['thread_number']}", axis = 1)

df_sampled_issues = df_all_issues.sample(n=1000, random_state=1)
df_sampled_prs = df_all_prs.sample(n=1000, random_state=1)

df_sampled_issues.to_csv("issue/sampled_issues.csv", index=False)
df_sampled_prs.to_csv("issue/sampled_prs.csv", index=False)

