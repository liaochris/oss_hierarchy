#!/usr/bin/env python3
import os
from google.cloud import bigquery
from pathlib import Path
import pandas as pd
import numpy as np
from source.lib.JMSLab.SaveData import SaveData


def CreateDataset(client, dataset_name):
    dataset_id = f"{client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, timeout=30)

    print(f"Created dataset {client.project}.{dataset.dataset_id}")


def LoadTableToDataset(client, dataset_name, table_name, indir_github_projects):
    df_github_projects = pd.read_csv(
        Path(indir_github_projects) / "repo_id_history_latest.csv", index_col=False
    ).query('repo_name_latest != "ERROR"')
    df_github_projects = df_github_projects[["repo_name"]].drop_duplicates()

    github_project_ref = client.dataset(dataset_name).table(table_name)
    load_data_job = client.load_table_from_dataframe(
        df_github_projects, github_project_ref
    )
    load_results = load_data_job.result()

    if load_results.errors == None:
        print("Data loaded successfully")
    else:
        print(f"Error Log: {load_results.errors}")


def GetRawGitHubData(
    client, github_projects_name, project_name, dataset_name, github_data_name
):
    github_data_config = bigquery.QueryJobConfig(
        destination=f"{project_name}.{dataset_name}.{github_data_name}"
    )
    github_raw_sql = f"""
    SELECT *
    FROM `githubarchive.month.20*`
    WHERE (_TABLE_SUFFIX BETWEEN '1501' AND '2412') AND repo.name in 
    (SELECT repo_name FROM `{project_name}.{dataset_name}.{github_projects_name}`)
    """
    github_data_query = client.query(github_raw_sql, job_config=github_data_config)
    github_data_query.result()


def GetWatchData(client, project_name, dataset_name, github_data_name):
    watch_data_name = "watch_data"
    watch_data_config = bigquery.QueryJobConfig(
        destination=f"{project_name}.{dataset_name}.{watch_data_name}"
    )
    watch_data_sql = f"""
    SELECT
      `type`,
      `created_at`,
      repo.id AS `repo_id`,
      repo.name AS `repo_name`,
      actor.id AS `actor_id`,
      actor.login AS `actor_login`,
      org.id AS `org_id`,
      org.login AS `org_login`,
    FROM
        `{project_name}.{dataset_name}.{github_data_name}`
    WHERE
        type = "WatchEvent"
    """
    watch_data_query = client.query(watch_data_sql, job_config=watch_data_config)
    watch_data_query.result()

    GetSubsetData(client, project_name, dataset_name, "watch_data")


def GetReleaseData(client, project_name, dataset_name, github_data_name):
    release_data_name = "release_data"
    release_data_config = bigquery.QueryJobConfig(
        destination=f"{project_name}.{dataset_name}.{release_data_name}"
    )
    release_data_sql = f"""
    SELECT
      `type`,
      `created_at`,
      repo.id AS `repo_id`,
      repo.name AS `repo_name`,
      actor.id AS `actor_id`,
      actor.login AS `actor_login`,
      org.id AS `org_id`,
      org.login AS `org_login`,
      JSON_VALUE(`payload`, '$.action') AS `release_action`,
      JSON_VALUE(`payload`, '$.release.tag_name') AS `release_tag_name`,
      JSON_VALUE(`payload`, '$.release.name') AS `release_name`,
      JSON_VALUE(`payload`, '$.release.body') AS `release_body`,
    FROM
        `{project_name}.{dataset_name}.{github_data_name}`
    WHERE
        type = "ReleaseEvent"
    """
    release_data_query = client.query(release_data_sql, job_config=release_data_config)
    release_data_query.result()

    GetSubsetData(client, project_name, dataset_name, "release_data")


def GetPushData(client, project_name, dataset_name, github_data_name):
    push_data_name = "push_data"
    push_data_config = bigquery.QueryJobConfig(
        destination=f"{project_name}.{dataset_name}.{push_data_name}"
    )
    push_data_sql = f"""
    SELECT
      `type`,
      `created_at`,
      repo.id AS `repo_id`,
      repo.name AS `repo_name`,
      actor.id AS `actor_id`,
      actor.login AS `actor_login`,
      org.id AS `org_id`,
      org.login AS `org_login`,
      JSON_VALUE(`payload`, '$.push_id') AS `push_id`,
      JSON_VALUE(`payload`, '$.size') AS `push_size`,
      JSON_VALUE(`payload`, '$.distinct_size') AS `push_size_distinct`,
      JSON_VALUE(`payload`, '$.before') AS `push_before`,
      JSON_VALUE(`payload`, '$.head') AS `push_head`,
      JSON_VALUE(`payload`, '$.ref') AS `push_ref`,
      COALESCE( ARRAY(
        SELECT
          JSON_VALUE(ele, '$.url')
        FROM
          UNNEST(JSON_QUERY_ARRAY(`payload`, '$.commits')) AS `ele`), ARRAY(
        SELECT
          JSON_VALUE(ele, '$.url')
        FROM
          UNNEST(JSON_QUERY_ARRAY(`payload`, '$.commits')) AS `ele`)) AS `commit_urls`
      FROM
        `{project_name}.{dataset_name}.{github_data_name}`
      WHERE
        type = "PushEvent" 
    """
    push_data_query = client.query(push_data_sql, job_config=push_data_config)
    push_data_query.result()

    GetSubsetData(client, project_name, dataset_name, "push_data")


def GetPullRequestReviewData(client, project_name, dataset_name, github_data_name):
    pull_request_review_name = "pull_request_review_data"
    pull_request_review_config = bigquery.QueryJobConfig(
        destination=f"{project_name}.{dataset_name}.{pull_request_review_name}"
    )
    pull_request_review_sql = f"""
				SELECT
						`type`,
						`created_at`,
						repo.id AS `repo_id`,
						repo.name AS `repo_name`,
						actor.id AS `actor_id`,
						actor.login AS `actor_login`,
						org.id AS `org_id`,
						org.login AS `org_login`,
						JSON_VALUE(`payload`, '$.action') AS `pr_review_action`,
						JSON_VALUE(`payload`, '$.review.id') AS `pr_review_id`,
						JSON_VALUE(`payload`, '$.review.body') AS `pr_review_body`,
						JSON_VALUE(`payload`, '$.review.commit_id') AS `pr_review_commit_id`,
						JSON_VALUE(`payload`, '$.review.author_association') AS `pr_review_author_association`,
						JSON_VALUE(`payload`, '$.review.state') AS `pr_review_state`,
						JSON_VALUE(`payload`, '$.pull_request.number') AS `pr_number`,
						JSON_VALUE(`payload`, '$.pull_request.id') AS `pr_id`,
						JSON_VALUE(`payload`, '$.pull_request.node_id') AS `pr_node_id`,
						JSON_VALUE(`payload`, '$.pull_request.title') AS `pr_title`,
						JSON_VALUE(`payload`, '$.pull_request.state') AS `pr_state`,
						JSON_VALUE(`payload`, '$.pull_request.locked') AS `pr_locked`,
						JSON_VALUE(`payload`, '$.pull_request.body') AS `pr_body`,
						JSON_VALUE(`payload`, '$.pull_request.issue_url') AS `pr_issue_url`,
						JSON_VALUE(`payload`, '$.pull_request.merged_at') AS `pr_merged_at`,
						JSON_VALUE(`payload`, '$.pull_request.closed_at') AS `pr_closed_at`,
						JSON_VALUE(`payload`, '$.pull_request.updated_at') AS `pr_updated_at`,
						JSON_VALUE(`payload`, '$.pull_request.commits') AS `pr_commits`,
						JSON_VALUE(`payload`, '$.pull_request.additions') AS `pr_additions`,
						JSON_VALUE(`payload`, '$.pull_request.deletions') AS `pr_deletions`,
						JSON_VALUE(`payload`, '$.pull_request.changed_files') AS `pr_changed_files`,
						JSON_VALUE(`payload`, '$.pull_request.author_association') AS `pr_author_association`,
						JSON_VALUE(`payload`, '$.pull_request.assignee') AS `pr_assignee`,
						JSON_QUERY(`payload`, '$.pull_request.assignees') AS `pr_assignees`,
						JSON_QUERY(`payload`, '$.pull_request.requested_reviewers') AS `pr_requested_reviewers`,
						JSON_QUERY(`payload`, '$.pull_request.requested_teams') AS `pr_requested_teams`,
						JSON_QUERY(`payload`, '$.pull_request.labels') AS `pr_labels`,
						JSON_VALUE(`payload`, '$.pull_request.ref') AS `pr_ref`,
						JSON_VALUE(`payload`, '$.pull_request.user.id') AS `pr_user_id`,
						JSON_VALUE(`payload`, '$.pull_request.user.login') AS `pr_user_login`,
						JSON_VALUE(`payload`, '$.pull_request.merged_by.id') AS `pr_merged_by_id`,
						JSON_VALUE(`payload`, '$.pull_request.merged_by.login') AS `pr_merged_by_login`,
				FROM
				  `{project_name}.{dataset_name}.{github_data_name}`
				WHERE
						type = "PullRequestReviewEvent" 

    """
    pull_request_review_query = client.query(
        pull_request_review_sql, job_config=pull_request_review_config
    )
    pull_request_review_query.result()

    GetSubsetData(client, project_name, dataset_name, "pull_request_review_data")


def GetPullRequestReviewCommentData(
    client, project_name, dataset_name, github_data_name
):
    pull_request_review_comment_name = "pull_request_review_comment_data"
    pull_request_review_comment_config = bigquery.QueryJobConfig(
        destination=f"{project_name}.{dataset_name}.{pull_request_review_comment_name}"
    )
    pull_request_review_comment_sql = f"""
				SELECT
						`type`,
						`created_at`,
						repo.id AS `repo_id`,
						repo.name AS `repo_name`,
						actor.id AS `actor_id`,
						actor.login AS `actor_login`,
						org.id AS `org_id`,
						org.login AS `org_login`,
                        JSON_VALUE(`payload`, '$.comment.position') AS `pr_review_comment_position`,
                        JSON_VALUE(`payload`, '$.comment.diff_hunk') AS `pr_review_comment_diff_hunk`,
                        JSON_VALUE(`payload`, '$.comment.path') AS `pr_review_comment_path`,
                        JSON_VALUE(`payload`, '$.comment.original_position') AS `pr_review_comment_original_position`,
                        JSON_VALUE(`payload`, '$.comment.original_commit_id') AS `pr_review_comment_original_commit_id`,
                        JSON_VALUE(`payload`, '$.comment.pull_request_review_id') AS `pr_review_id`,
                        JSON_VALUE(`payload`, '$.comment.reactions') AS `pr_review_comment_reactions`,
						JSON_VALUE(`payload`, '$.comment.author_association') AS `pr_review_comment_author_association`,
						JSON_VALUE(`payload`, '$.comment.body') AS `pr_review_comment_body`,
						JSON_VALUE(`payload`, '$.comment.user.site_admin') AS `pr_review_comment_site_admin`,
						JSON_VALUE(`payload`, '$.comment.commit_id') AS `pr_review_comment_commit_id`,
						JSON_VALUE(`payload`, '$.comment.id') AS `pr_review_comment_id`,
						JSON_VALUE(`payload`, '$.action') AS `pr_review_comment_action`,
						JSON_VALUE(`payload`, '$.pull_request.number') AS `pr_number`,
						JSON_VALUE(`payload`, '$.pull_request.id') AS `pr_id`,
						JSON_VALUE(`payload`, '$.pull_request.node_id') AS `pr_node_id`,
						JSON_VALUE(`payload`, '$.pull_request.title') AS `pr_title`,
						JSON_VALUE(`payload`, '$.pull_request.state') AS `pr_state`,
						JSON_VALUE(`payload`, '$.pull_request.locked') AS `pr_locked`,
						JSON_VALUE(`payload`, '$.pull_request.body') AS `pr_body`,
						JSON_VALUE(`payload`, '$.pull_request.issue_url') AS `pr_issue_url`,
						JSON_VALUE(`payload`, '$.pull_request.merged_at') AS `pr_merged_at`,
						JSON_VALUE(`payload`, '$.pull_request.closed_at') AS `pr_closed_at`,
						JSON_VALUE(`payload`, '$.pull_request.updated_at') AS `pr_updated_at`,
						JSON_VALUE(`payload`, '$.pull_request.commits') AS `pr_commits`,
						JSON_VALUE(`payload`, '$.pull_request.additions') AS `pr_additions`,
						JSON_VALUE(`payload`, '$.pull_request.deletions') AS `pr_deletions`,
						JSON_VALUE(`payload`, '$.pull_request.changed_files') AS `pr_changed_files`,
						JSON_VALUE(`payload`, '$.pull_request.author_association') AS `pr_author_association`,
						JSON_VALUE(`payload`, '$.pull_request.assignee') AS `pr_assignee`,
						JSON_QUERY(`payload`, '$.pull_request.assignees') AS `pr_assignees`,
						JSON_QUERY(`payload`, '$.pull_request.requested_reviewers') AS `pr_requested_reviewers`,
						JSON_QUERY(`payload`, '$.pull_request.requested_teams') AS `pr_requested_teams`,
						JSON_QUERY(`payload`, '$.pull_request.labels') AS `pr_labels`,
						JSON_VALUE(`payload`, '$.pull_request.ref') AS `pr_ref`,
						JSON_VALUE(`payload`, '$.pull_request.user.id') AS `pr_user_id`,
						JSON_VALUE(`payload`, '$.pull_request.user.login') AS `pr_user_login`,
						JSON_VALUE(`payload`, '$.pull_request.merged_by.id') AS `pr_merged_by_id`,
						JSON_VALUE(`payload`, '$.pull_request.merged_by.login') AS `pr_merged_by_login`,
				FROM
				  `{project_name}.{dataset_name}.{github_data_name}`
				WHERE
						type = "PullRequestReviewCommentEvent"
		    """
    pull_request_review_comment_query = client.query(
        pull_request_review_comment_sql, job_config=pull_request_review_comment_config
    )
    pull_request_review_comment_query.result()

    GetSubsetData(
        client, project_name, dataset_name, "pull_request_review_comment_data"
    )


def GetPullRequestData(client, project_name, dataset_name, github_data_name):
    pull_request_name = "pull_request_data"
    pull_request_config = bigquery.QueryJobConfig(
        destination=f"{project_name}.{dataset_name}.{pull_request_name}"
    )
    pull_request_sql = f"""
				SELECT
						`type`,
						`created_at`,
						repo.id AS `repo_id`,
						repo.name AS `repo_name`,
						actor.id AS `actor_id`,
						actor.login AS `actor_login`,
						org.id AS `org_id`,
						org.login AS `org_login`,
						JSON_VALUE(`payload`, '$.id') AS `pr_id`,
						JSON_VALUE(`payload`, '$.node_id') AS `pr_node_id`,
						JSON_VALUE(`payload`, '$.title') AS `pr_title`,
						JSON_VALUE(`payload`, '$.state') AS `pr_state`,
						JSON_VALUE(`payload`, '$.locked') AS `pr_locked`,
						JSON_VALUE(`payload`, '$.number') AS `pr_number`,
						JSON_VALUE(`payload`, '$.body') AS `pr_body`,
						JSON_VALUE(`payload`, '$.issue_url') AS `pr_issue_url`,
						JSON_VALUE(`payload`, '$.merged_at') AS `pr_merged_at`,
						JSON_VALUE(`payload`, '$.closed_at') AS `pr_closed_at`,
						JSON_VALUE(`payload`, '$.updated_at') AS `pr_updated_at`,
						JSON_VALUE(`payload`, '$.commits') AS `pr_commits`,
						JSON_VALUE(`payload`, '$.additions') AS `pr_additions`,
						JSON_VALUE(`payload`, '$.deletions') AS `pr_deletions`,
						JSON_VALUE(`payload`, '$.changed_files') AS `pr_changed_files`,
						JSON_VALUE(`payload`, '$.author_association') AS `pr_author_association`,
						JSON_VALUE(`payload`, '$.assignee') AS `pr_assignee`,
						JSON_QUERY(`payload`, '$.pull_request.assignees') AS `pr_assignees`,
						JSON_QUERY(`payload`, '$.pull_request.requested_reviewers') AS `pr_requested_reviewers`,
						JSON_QUERY(`payload`, '$.pull_request.requested_teams') AS `pr_requested_teams`,
						JSON_QUERY(`payload`, '$.pull_request.labels') AS `pr_labels`,
						JSON_VALUE(`payload`, '$.ref') AS `pr_ref`,
						JSON_VALUE(`payload`, '$.action') AS `pr_action`,
						JSON_VALUE(`payload`, '$.pull_request.merged_by.login') AS `pr_merged_by_login`,
						JSON_VALUE(`payload`, '$.pull_request.merged_by.id') AS `pr_merged_by_id`,
						JSON_VALUE(`payload`, '$.pull_request.merged_by.type') AS `pr_merged_by_type`,
						JSON_VALUE(`payload`, '$.pull_request.merged_by.site_admin') AS `pr_merged_by_site_admin`,
						
						JSON_VALUE(`payload`, '$.pull_request.patch_url') AS `pr_patch_url`,
						JSON_VALUE(`payload`, '$.pull_request.commits_url') AS `pr_commits_url`,
				FROM
				  `{project_name}.{dataset_name}.{github_data_name}`
				WHERE
						type = "PullRequestEvent"
		    """
    pull_request_query = client.query(pull_request_sql, job_config=pull_request_config)
    pull_request_query.result()

    GetSubsetData(client, project_name, dataset_name, "pull_request_data")


def GetIssueData(client, project_name, dataset_name, github_data_name):
    issue_name = "issue_data"
    issue_config = bigquery.QueryJobConfig(
        destination=f"{project_name}.{dataset_name}.{issue_name}"
    )
    issue_sql = f"""
				SELECT
						`type`,
						`created_at`,
						repo.id AS `repo_id`,
						repo.name AS `repo_name`,
						actor.id AS `actor_id`,
						actor.login AS `actor_login`,
						org.id AS `org_id`,
						org.login AS `org_login`,
						JSON_VALUE(`payload`, '$.action') AS `issue_action`,
						JSON_VALUE(`payload`, '$.issue.title') AS `issue_title`,
						JSON_QUERY(`payload`, '$.issue.labels') AS `issue_labels`,
						JSON_VALUE(`payload`, '$.issue.assignee') AS `issue_assignee`,
						JSON_QUERY(`payload`, '$.issue.assignees') AS `issue_assignees`,
						JSON_VALUE(`payload`, '$.issue.comments') AS `issue_comment_count`,
						JSON_VALUE(`payload`, '$.issue.body') AS `issue_body`,
						JSON_VALUE(`payload`, '$.issue.reactions') AS `issue_reactions`,
						JSON_VALUE(`payload`, '$.issue.state_reason') AS `issue_reason`,
						JSON_VALUE(`payload`, '$.issue.user.login') AS `issue_user_login`,
						JSON_VALUE(`payload`, '$.issue.user.type') AS `issue_user_type`,
						JSON_VALUE(`payload`, '$.issue.user.id') AS `issue_user_id`,
						JSON_VALUE(`payload`, '$.issue.author_association') AS `issue_author_association`,
						JSON_VALUE(`payload`, '$.issue.updated_at') AS `issue_updated_at`,
						JSON_VALUE(`payload`, '$.issue.closed_at') AS `issue_closed_at`,
						JSON_VALUE(`payload`, '$.issue.id') AS `issue_id`,
						JSON_VALUE(`payload`, '$.issue.number') AS `issue_number`,
						JSON_VALUE(`payload`, '$.issue.state') AS `issue_state`,
						JSON_VALUE(`payload`, '$.issue.locked') AS `issue_locked`,
				FROM
				  `{project_name}.{dataset_name}.{github_data_name}`
				WHERE
						type = "IssuesEvent"
		    """
    issue_query = client.query(issue_sql, job_config=issue_config)
    issue_query.result()

    GetSubsetData(client, project_name, dataset_name, "issue_data")


def GetIssueCommentData(client, project_name, dataset_name, github_data_name):
    issue_comment_name = "issue_comment_data"
    issue_comment_config = bigquery.QueryJobConfig(
        destination=f"{project_name}.{dataset_name}.{issue_comment_name}"
    )
    issue_comment_sql = f"""
				SELECT
						`type`,
						`created_at`,
						repo.id AS `repo_id`,
						repo.name AS `repo_name`,
						actor.id AS `actor_id`,
						actor.login AS `actor_login`,
						org.id AS `org_id`,
						org.login AS `org_login`,
						JSON_VALUE(`payload`, '$.issue.number') AS `issue_number`,
						JSON_VALUE(`payload`, '$.issue.created_at') AS `issue_created_at`,
						JSON_VALUE(`payload`, '$.issue.closed_at') AS `issue_closed_at`,
						SPLIT(JSON_VALUE(`payload`, '$.comment.issue_url'),' ')[ARRAY_LENGTH(SPLIT(JSON_VALUE(`payload`, '$.comment.issue_url'),' '))-1] AS `issue_id`,
						COALESCE(
								SPLIT(JSON_VALUE(`payload`, '$.comment.html_url'),'-')[ARRAY_LENGTH(SPLIT(JSON_VALUE(`payload`, '$.comment.html_url'),'-'))-1], 
								SPLIT(JSON_VALUE(`payload`, '$.comment.id'),'-')[ARRAY_LENGTH(SPLIT(JSON_VALUE(`payload`, '$.comment.id'),'-'))-1]) AS `issue_comment_id`,
						JSON_VALUE(`payload`, '$.comment.body') AS `issue_comment_body`,
						JSON_VALUE(`payload`, '$.comment.reactions') AS `issue_comment_reactions`,
						JSON_VALUE(`payload`, '$.issue.user.login') AS `issue_user_login`,
						JSON_VALUE(`payload`, '$.issue.user.id') AS `issue_user_id`,
						JSON_VALUE(`payload`, '$.issue.user.type') AS `issue_user_type`,
						JSON_VALUE(`payload`, '$.issue.author_association') AS `issue_author_association`,
						JSON_VALUE(`payload`, '$.comment.author_association') AS `actor_repo_association`,
						JSON_VALUE(`payload`, '$.comment.user.type') AS `actor_type`,
						JSON_VALUE(`payload`, '$.comment.user.site_admin') AS `actor_site_admin_status`,
						JSON_VALUE(`payload`, '$.issue.pull_request') AS `issue_pull_request`,
						JSON_QUERY(`payload`, '$.issue.labels') AS `latest_issue_labels`,
						JSON_VALUE(`payload`, '$.issue.locked') AS `latest_issue_locked`,
						JSON_VALUE(`payload`, '$.issue.state') AS `latest_issue_state`,
						JSON_VALUE(`payload`, '$.issue.assignee') AS `latest_issue_assignee`,
						JSON_QUERY(`payload`, '$.issue.assignees') AS `latest_issue_assignees`,
						JSON_VALUE(`payload`, '$.issue.comments') AS `latest_issue_comments`,
				FROM
				  `{project_name}.{dataset_name}.{github_data_name}`
				WHERE
						type = "IssueCommentEvent"
				"""
    issue_comment_query = client.query(
        issue_comment_sql, job_config=issue_comment_config
    )
    issue_comment_query.result()

    GetSubsetData(client, project_name, dataset_name, "issue_comment_data")


def GetForkData(client, project_name, dataset_name, github_data_name):
    fork_name = "fork_data"
    fork_config = bigquery.QueryJobConfig(
        destination=f"{project_name}.{dataset_name}.{fork_name}"
    )
    fork_sql = f"""
				SELECT
						`type`,
						`created_at`,
						repo.id AS `repo_id`,
						repo.name AS `repo_name`,
						actor.id AS `actor_id`,
						actor.login AS `actor_login`,
						org.id AS `org_id`,
						org.login AS `org_login`,
				FROM
				  `{project_name}.{dataset_name}.{github_data_name}`
				WHERE
						type = "ForkEvent"
				"""
    fork_query = client.query(fork_sql, job_config=fork_config)
    fork_query.result()

    GetSubsetData(client, project_name, dataset_name, "fork_data")


def GetDeleteData(client, project_name, dataset_name, github_data_name):
    delete_name = "delete_data"
    delete_config = bigquery.QueryJobConfig(
        destination=f"{project_name}.{dataset_name}.{delete_name}"
    )
    delete_sql = f"""
				SELECT
						`type`,
						`created_at`,
						repo.id AS `repo_id`,
						repo.name AS `repo_name`,
						actor.id AS `actor_id`,
						actor.login AS `actor_login`,
						org.id AS `org_id`,
						org.login AS `org_login`,
						JSON_VALUE(`payload`, '$.ref_type') AS `event_type`,
						JSON_VALUE(`payload`, '$.ref') AS `event_ref`,
						JSON_VALUE(`payload`, '$.pusher_type') AS `event_pusher_type`,
				FROM
				  `{project_name}.{dataset_name}.{github_data_name}`
				WHERE
						type = "DeleteEvent"
				"""
    delete_query = client.query(delete_sql, job_config=delete_config)
    delete_query.result()

    GetSubsetData(client, project_name, dataset_name, "delete_data")


def GetCreateData(client, project_name, dataset_name, github_data_name):
    create_name = "create_data"
    create_config = bigquery.QueryJobConfig(
        destination=f"{project_name}.{dataset_name}.{create_name}"
    )
    create_sql = f"""
				SELECT
						`type`,
						`created_at`,
						repo.id AS `repo_id`,
						repo.name AS `repo_name`,
						actor.id AS `actor_id`,
						actor.login AS `actor_login`,
						org.id AS `org_id`,
						org.login AS `org_login`,
						JSON_VALUE(`payload`, '$.ref_type') AS `event_type`,
						COALESCE(JSON_VALUE(`payload`, '$.ref'),JSON_VALUE(`payload`, '$.object_name')) AS `event_ref`,
						JSON_VALUE(`payload`, '$.master_branch') AS `repo_master_branch`,
						JSON_VALUE(`payload`, '$.pusher_type') AS `event_pusher_type`,
				FROM
				  `{project_name}.{dataset_name}.{github_data_name}`
				WHERE
						type = "CreateEvent"
				"""
    create_query = client.query(create_sql, job_config=create_config)
    create_query.result()

    GetSubsetData(client, project_name, dataset_name, "create_data")


def GetSubsetData(client, project_name, dataset_name, subset_data_name):
    outdir = f"drive/output/scrape/extract_github_data/{subset_data_name}"
    outdir_log = f"output/scrape/extract_github_data"
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    if not os.path.exists(outdir_log):
        os.mkdir(outdir_log)

    file_counter = 0
    for subset_year in np.arange(2015, 2025, 1):
        for subset_month in np.arange(1, 13, 1):
            subset_date_sql = f"""
			SELECT *
			FROM
				`{project_name}.{dataset_name}.{subset_data_name}`
			WHERE
				EXTRACT(MONTH FROM created_at) = {subset_month} AND EXTRACT(YEAR FROM created_at) = {subset_year}
			"""

            subset_date_query = client.query(subset_date_sql)
            df_subset = subset_date_query.to_dataframe()
            SaveData(
                df_subset.reset_index(),
                ["index"],
                f"{outdir}/{subset_data_name.replace('_data','')}_{subset_year}_{subset_month}.csv",
                f"{outdir_log}/{subset_data_name}.log",
                append=file_counter != 0,
            )
            file_counter += 1


def Main():
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ.keys():
        print("Need to set up GOOGLE_APPLICATION_CREDENTIALS environment variable")
        return

    indir_github_projects = "output/scrape/extract_github_data"
    project_name = "serene-bazaar-470016-h8"  # changes whenever I use a new BQ email
    dataset_name = "source"
    github_projects_name = "github_repositories"
    github_data_name = "github_data"

    client = bigquery.Client(project=project_name)
    CreateDataset(client, dataset_name)
    LoadTableToDataset(client, "source", github_projects_name, indir_github_projects)
    GetRawGitHubData(
        client, github_projects_name, project_name, dataset_name, github_data_name
    )

    GetWatchData(client, project_name, dataset_name, github_data_name)
    GetReleaseData(client, project_name, dataset_name, github_data_name)
    GetPushData(client, project_name, dataset_name, github_data_name)
    GetPullRequestReviewData(client, project_name, dataset_name, github_data_name)
    GetPullRequestReviewCommentData(
        client, project_name, dataset_name, github_data_name
    )
    GetPullRequestData(client, project_name, dataset_name, github_data_name)
    GetIssueData(client, project_name, dataset_name, github_data_name)
    GetIssueCommentData(client, project_name, dataset_name, github_data_name)
    GetForkData(client, project_name, dataset_name, github_data_name)
    GetDeleteData(client, project_name, dataset_name, github_data_name)
    GetCreateData(client, project_name, dataset_name, github_data_name)


if __name__ == "__main__":
    Main()
