#!/usr/bin/env python
# coding: utf-8

from source.scrape.collect_commits.docs.get_commit_data_helpers import getCommitData

def Main():
    pandarallel.initialize(progress_bar=True)
    warnings.filterwarnings("ignore")

    push_indir = Path('drive/output/derived/filter_nonunique_push')
    commits_outdir = Path('drive/output/scrape/collect_commits/push')

    df_push_commits = pd.read_csv(push_indir / 'non_unique_push_complete.csv')
    df_push_commits['commit_list'] = df_push_commits['commit_list'].parallel_apply(ast.literal_eval)
    
    github_repos = df_push_commits['repo_name'].unique().tolist()
    github_repos = [library for library in github_repos if "/" in library]
    random.shuffle(github_repos)

    push_commit_cols = ['repo_name', 'push_id', 'commit_list', 'push_before', 'push_head', 'commit_list_length', 'commit_groups']

    for library in github_repos:
        print(library)
        df_library = df_push_commits[df_push_commits['repo_name'] == library]
        getCommitData(library, commits_outdir, df_library, push_commit_cols, 'push')
            
    print("Done!")

if __name__ == '__main__':   
    Main()
        
