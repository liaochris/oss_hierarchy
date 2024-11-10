#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
import ast
import pandas as pd
import numpy as np
from pygit2 import Object, Repository, GIT_SORT_TIME
from pygit2 import init_repository, Patch
from pandarallel import pandarallel
import subprocess
import warnings
import os
import multiprocessing
import time
import random
import shutil

def createCommitGroupPush(push_before, push_head, commit_list, commit_list_length):
    try:
        if commit_list_length == 1:
            return [[push_before, push_head]]
        else:
            avail_commits = len(commit_list)
            lst_result = [[push_before, commit_list[0].split("/")[-1]]]
            for i in range(avail_commits-1):
                lst_result.append([commit_list[i].split("/")[-1], commit_list[i+1].split("/")[-1]])
            return lst_result
    except:
        return [[]]
        
def createCommitGroupPr(commit_list, parent_commit):
    try:
        if type(commit_list) != list and type(commit_list) != type(pd.Series()) and type(commit_list) != np.ndarray:
            commit_list = ast.literal_eval(commit_list)
        if len(commit_list) == 0:
            return [[]]
        elif len(commit_list) == 1:
            return [[parent_commit, commit_list[0]]]
        else:
            avail_commits = len(commit_list)
            lst_result = [[parent_commit, commit_list[0]]]
            for i in range(avail_commits-1):
                lst_result.append([commit_list[i], commit_list[i+1]])
            return lst_result
    except:
        return np.nan

def getHead(commit_list, pull_number, cloned_repo_location):
    try:
        if type(commit_list) != list and type(commit_list) != type(pd.Series())  and type(commit_list) != np.ndarray:
            commit = ast.literal_eval(commit_list)[0]
        else:
            commit = commit_list[0]
        pull_fetch = subprocess.Popen(["git","fetch", "origin", f"pull/{pull_number}/head"], cwd = cloned_repo_location,
                                      shell=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()
        result = subprocess.run(["git","show", f"{commit}^"], cwd = cloned_repo_location, capture_output = True, text = True).stdout[7:47]
        return result
    except Exception as e:
        return np.nan


def returnCommitStats(x, repo):
    try:
        if len(x) < 2:
            return []
        if x[0] == [] or x[1] == []:
            return []
        commit_parent_sha = x[0]
        commit_head_sha = x[1]
        commit_parent = repo.get(commit_parent_sha)
        commit_head = repo.get(commit_head_sha)
        if type(commit_parent) != type(None) and type(commit_head) != type(None):
            diff = repo.diff(commit_parent, commit_head, context_lines=0, interhunk_lines=0)
            commit_sha = commit_head_sha
            commit_author_name = commit_head.author.name
            commit_author_email = commit_head.author.email
            committer_author_name = commit_head.committer.name
            committer_author_email = commit_head.committer.email
            commit_message = commit_head.message
            commit_additions = diff.stats.insertions
            commit_deletions = diff.stats.deletions
            commit_changes_total = commit_additions + commit_deletions
            commit_files_changed_count = diff.stats.files_changed
            commit_time = commit_head.commit_time
            commit_file_changes = []
            for obj in diff:
                if type(obj) == Patch:
                    additions = 0
                    deletions = 0
                    for hunk in obj.hunks:
                      for line in hunk.lines:
                        # The new_lineno represents the new location of the line after the patch. If it's -1, the line has been deleted.
                        if line.new_lineno == -1: 
                            deletions += 1
                        # Similarly, if a line did not previously have a place in the file, it's been added fresh. 
                        if line.old_lineno == -1: 
                            additions += 1
                    commit_file_changes.append({'file':obj.delta.new_file.path,
                                                'additions': additions,
                                                'deletions': deletions,
                                                'total': additions + deletions})
            return [commit_sha, commit_author_name, commit_author_email, committer_author_name, committer_author_email,
                    commit_message, commit_additions, commit_deletions, commit_changes_total, commit_files_changed_count,
                    commit_file_changes, commit_time]
        return []
    except:
        return []
    

def cleanCommitData(library, df_library, lib_renamed, github_repos_loc,
                    df_commit_cols, commit_type, commits_outdir):
    subprocess.Popen(["git", "clone", f"git@github.com:{library}.git", f"{lib_renamed}"], cwd = commits_outdir / github_repos_loc).communicate()
    print(f"COPYING {lib_renamed}")
    subprocess.Popen(["cp", "-r", f"{lib_renamed}", f"{lib_renamed}_temp"], cwd = commits_outdir / github_repos_loc).communicate()
    print(f"Finished cloning {library}")

    global repo
    df_library.reset_index(drop = True, inplace = True)
    df_library_index = list(df_library.index)
    df_library_chunks = [df_library_index[x:x+3000] for x in range(0, len(df_library_index), 3000)]

    print(len(df_library_index))
    commit_data = pd.Series()
    i = 0
    df_commit_groups = pd.DataFrame(columns = df_commit_cols)
    for chunk in df_library_chunks:
        start = time.time()
        cloned_repo_location = Path(commits_outdir / github_repos_loc / lib_renamed)
        repo = Repository(cloned_repo_location)
        df_library_chunk = df_library.loc[chunk]
        if commit_type == "pr":
            df_library_chunk['parent_commit'] = df_library_chunk.parallel_apply(lambda x: getHead(x['commit_list'], x['pr_number'], cloned_repo_location) if not pd.isnull(x['pr_number']) else np.nan, axis = 1)
            print(f"finished getting parent commits for {library}, chunk {i+1}")
            df_library_chunk['commit_groups'] = df_library_chunk.parallel_apply(lambda x: createCommitGroupPr(x['commit_list'], x['parent_commit']) if not pd.isnull(x['parent_commit']) else np.nan, axis = 1)
            id_col = 'pr_number'
        if commit_type == "push":
            id_col = 'push_id'
            df_library_chunk['commit_groups'] = df_library_chunk.apply(lambda x: createCommitGroupPush(x['push_before'], x['push_head'], x['commit_list'], x['commit_list_length']) , axis = 1)
        df_commit_groups_chunk = df_library_chunk[df_commit_cols].explode('commit_groups')
        df_commit_groups = pd.concat([df_commit_groups, df_commit_groups_chunk])
        commit_data_chunk = df_commit_groups_chunk['commit_groups'].parallel_apply(lambda x: returnCommitStats(x, repo) if type(x) != float else x)
        commit_data = pd.concat([commit_data, commit_data_chunk])
        commit_data_chunk.to_csv(f'drive/temp/{lib_renamed}{i+2}_{commit_type}.csv')
        print(f"NOW REMOVING {lib_renamed}")
        while lib_renamed in os.listdir(commits_outdir / github_repos_loc):
            try:
                shutil.rmtree(commits_outdir / github_repos_loc / lib_renamed)
            except Exception as e:
                print(e)
        if i != len(df_library_chunks)-1:
            print(f"COPYING {lib_renamed}_temp to {lib_renamed}")
            subprocess.Popen(["cp", "-r", f"{lib_renamed}_temp", f"{lib_renamed}"], cwd = commits_outdir / github_repos_loc).communicate()    
        i+=1
        end = time.time()
        print(f"FINISHED CHUNK {i+1} of {lib_renamed}")
        print(end - start)

    print(f"NOW REMOVING {lib_renamed}_temp")
    while f"{lib_renamed}_temp" in os.listdir(commits_outdir / github_repos_loc):
        try:
            shutil.rmtree(commits_outdir / github_repos_loc / f"{lib_renamed}_temp")
        except Exception as e:
            print(e)

    commit_data_colnames = ['commit sha', 'commit author name', 'commit author email', 'committer name',
                            'commmitter email', 'commit message', 'commit additions', 'commit deletions',
                            'commit changes total', 'commit files changed count', 'commit file changes', 
                            'commit time']
    df_commit = pd.DataFrame(commit_data.apply(lambda x: [np.nan]* len(commit_data_colnames) if type(x) == float else x).tolist(),
                             columns = commit_data_colnames)

    # In[ ]:
    df_commit_final = pd.concat([df_commit_groups.reset_index(drop = True), df_commit], axis = 1)
    df_commit_final[id_col] = pd.to_numeric(df_commit_final[id_col])

    df_commit_final = df_commit_final.dropna()

    return df_commit_final

def getCommitData(library, commits_outdir, df_library, df_commit_cols, commit_type):
    print(library)
    lib_name = library.split("/")[1]
    lib_renamed = library.replace("/","_")
    github_repos_loc = 'github_repos'
    if f'commits_{commit_type}_{lib_renamed}.parquet' not in os.listdir(commits_outdir):
        try:
            print(f"Starting {library}")
            start = time.time()
            if lib_renamed not in os.listdir(commits_outdir / github_repos_loc):
                df_commit_final = cleanCommitData(library, df_library, lib_renamed, github_repos_loc,
                                                  df_commit_cols, commit_type, commits_outdir)
                df_commit_final.to_parquet(commits_outdir / f'commits_{commit_type}_{lib_renamed}.parquet')
                end = time.time()
                print(f"{library} completed in {end - start}")
            else:
                print(f"skipping {lib_renamed} because it's a concurrent process")
            return "success"
        except Exception as e:
            print(e)
            return f"failure, {str(e)}"
    return 'success'

