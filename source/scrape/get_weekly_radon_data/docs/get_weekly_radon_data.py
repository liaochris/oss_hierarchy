import os
import subprocess
import json
from pathlib import Path
import pandas as pd
import numpy as np
from pygit2 import Repository, enums
import random
import shutil
import multiprocess
from source.scrape.collect_commits.docs.get_commit_data_pr import importPullRequestData
import warnings
from pandarallel import pandarallel

def main():
    warnings.filterwarnings("ignore")
    pandarallel.initialize(progress_bar = True)

    pr_indir = Path('drive/output/scrape/push_pr_commit_data/pull_request_data')
    outdir = Path('drive/output/scrape/get_weekly_radon_data')
    (outdir / 'github_repos').mkdir(parents=True, exist_ok=True)
    (outdir / 'radon_aggregate').mkdir(parents=True, exist_ok=True)
    (outdir / 'radon_detailed').mkdir(parents=True, exist_ok=True)

    df_pull_request = importPullRequestData(pr_indir)
    github_repos = df_pull_request['repo_name'].unique().tolist()
    github_repos = [library for library in github_repos if "/" in library]
    random.shuffle(github_repos)

    with multiprocess.Pool(8) as pool:
        for result in pool.imap(get_radon_scorecard, github_repos):
            print(result)

    print("Done!")


def get_radon_scorecard(library):
    outdir = Path('drive/output/scrape/get_weekly_radon_data')
    lib_renamed = library.replace("/", "_")
    repo_path = outdir / 'github_repos' / lib_renamed

    agg_path = outdir / f'radon_aggregate/radon_aggregate_{lib_renamed}.csv'
    detail_path = outdir / f'radon_detailed/radon_detailed_{lib_renamed}.csv'

    if agg_path.exists() and detail_path.exists():
        return f"{library} already processed"

    try:
        if not repo_path.exists():
            subprocess.run(["git", "clone", f"https://github.com/{library}.git", lib_renamed], cwd=outdir / 'github_repos')

        df_agg, df_detail = analyze_repo_radon(repo_path, library, lib_renamed)

        df_agg.to_csv(agg_path, index=False)
        df_detail.to_csv(detail_path, index=False)

        shutil.rmtree(repo_path)
        return f"{library} success"

    except Exception as e:
        print(f"{library} failed: {e}")
        return f"{library} failure"


def get_radon_json(cmd, path):
    result = subprocess.run(cmd, cwd=path, capture_output=True, text=True)
    if result.returncode == 0:
        try:
            return json.loads(result.stdout)
        except Exception as e:
            print("JSON parse error:", e)
    else:
        print("Radon error:", result.stderr)
    return {}


def analyze_repo_radon(repo_path, library, lib_renamed):
    repo = Repository(repo_path)
    latest_commit = repo[repo.head.target]
    data_commits = [[commit.commit_time, str(commit.id)] for commit in repo.walk(latest_commit.id, enums.SortMode.TIME)]
    df_commits = pd.DataFrame(data_commits, columns=['time', 'commit_sha'])

    df_commits['date'] = pd.to_datetime(df_commits['time'], unit='s')
    df_commits['week'] = df_commits['date'].dt.isocalendar().week
    df_commits['year'] = df_commits['date'].dt.year
    df_commits = df_commits.sort_values('date', ascending=True).drop_duplicates(['week', 'year'])

    aggregate_data = []
    detailed_data = []

    for _, row in df_commits.iterrows():
        commit_sha = row['commit_sha']
        dt_str = row['date'].strftime('%Y-%m-%d')

        subprocess.run(["git", "reset", "--hard", commit_sha], cwd=repo_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # 1. Cyclomatic Complexity
        cc_json = get_radon_json(['radon', 'cc', '-s', '-j', '.'], repo_path)
        scores, grades = [], []

        for file, blocks in cc_json.items():
            for block in blocks:
                if block != "error":
                    scores.append(block['complexity'])
                    grades.append(block['rank'])
                    detailed_data.append({
                        'commit': commit_sha,
                        'date': dt_str,
                        'file': file,
                        'function': block['name'],
                        'line': block['lineno'],
                        'complexity': block['complexity'],
                        'grade': block['rank']
                    })
                else:
                    detailed_data.append({
                        'commit': commit_sha,
                        'date': dt_str,
                        'file': file,
                        'error': 1
                    })

        cc_stats = {
            'total_blocks': len(scores),
            'avg_complexity': np.mean(scores) if scores else 0,
            'median_complexity': np.median(scores) if scores else 0,
            'std_complexity': np.std(scores) if scores else 0,
            'max_complexity': max(scores) if scores else 0,
            'min_complexity': min(scores) if scores else 0,
            'percent_high_complexity': (sum(1 for g in grades if g in ['D', 'E', 'F']) / len(grades) * 100) if grades else 0
        }

        for grade in 'ABCDEF':
            cc_stats[f'grade_{grade}'] = grades.count(grade)

        # 2. Maintainability Index
        mi_json = get_radon_json(['radon', 'mi', '-j', '.'], repo_path)
        mi_scores = [score for score in mi_json.values() if isinstance(score, (int, float))]

        mi_stats = {
            'avg_mi': np.mean(mi_scores) if mi_scores else 0,
            'median_mi': np.median(mi_scores) if mi_scores else 0,
            'std_mi': np.std(mi_scores) if mi_scores else 0,
            'min_mi': min(mi_scores) if mi_scores else 0,
            'max_mi': max(mi_scores) if mi_scores else 0,
            'percent_mi_gt_70': (sum(1 for s in mi_scores if s > 70) / len(mi_scores) * 100) if mi_scores else 0,
            'percent_mi_lt_20': (sum(1 for s in mi_scores if s < 20) / len(mi_scores) * 100) if mi_scores else 0
        }

        # 3. Halstead Metrics
        hal_json = get_radon_json(['radon', 'hal', '-j', '.'], repo_path)
        effort_scores = []
        time_scores = []
        bug_scores = []
        volume_scores = []
        difficulty_scores = []

        for file, file_data in hal_json.items():
            for func_data in file_data.get("functions", []):
                metrics = func_data[1]
                if len(metrics) >= 12:
                    effort = metrics[9]
                    time = metrics[10]
                    bugs = metrics[11]
                    volume = metrics[7]
                    difficulty = metrics[8]

                    if effort > 0:
                        effort_scores.append(effort)
                    if time > 0:
                        time_scores.append(time)
                    if bugs > 0:
                        bug_scores.append(bugs)
                    if volume > 0:
                        volume_scores.append(volume)
                    if difficulty > 0:
                        difficulty_scores.append(difficulty)
        hal_stats = {
            'avg_effort': np.mean(effort_scores) if effort_scores else 0,
            'max_effort': max(effort_scores) if effort_scores else 0,
            'avg_time': np.mean(time_scores) if time_scores else 0,
            'avg_bugs': np.mean(bug_scores) if bug_scores else 0,
            'avg_volume': np.mean(volume_scores) if volume_scores else 0,
            'avg_difficulty': np.mean(difficulty_scores) if difficulty_scores else 0
        }

        row_data = {
            'commit': commit_sha,
            'date': dt_str,
            **cc_stats,
            **mi_stats,
            **hal_stats
        }
        aggregate_data.append(row_data)

    df_agg = pd.DataFrame(aggregate_data)
    df_detail = pd.DataFrame(detailed_data)
    return df_agg, df_detail


if __name__ == '__main__':
    main()
