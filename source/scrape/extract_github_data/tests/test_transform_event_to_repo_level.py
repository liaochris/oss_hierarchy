import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from source.scrape.extract_github_data.code import transform_event_to_repo_level as mod


KEEP_COLS = [
    "type",
    "created_at",
    "repo_id",
    "repo_name",
    "actor_id",
    "actor_login",
    "issue_number",
    "issue_body",
    "issue_title",
    "issue_action",
    "issue_state",
    "issue_comment_id",
    "issue_user_id",
    "issue_comment_body",
    "latest_issue_assignees",
    "latest_issue_assignee",
    "latest_issue_labels",
    "actor_type",
]


def MakeIssueRow(created_at, issue_user_id, repo_name="Owner/Repo"):
    return {
        "type": "IssuesEvent",
        "created_at": created_at,
        "repo_id": "1",
        "repo_name": repo_name,
        "actor_id": "10",
        "actor_login": "alice",
        "issue_number": "5",
        "issue_body": "body",
        "issue_title": "title",
        "issue_action": "opened",
        "issue_state": "open",
        "issue_comment_id": "",
        "issue_user_id": issue_user_id,
        "issue_comment_body": "",
        "latest_issue_assignees": "",
        "latest_issue_assignee": "",
        "latest_issue_labels": "",
        "actor_type": "User",
    }


class TestTransformEventToRepoLevel(unittest.TestCase):
    def test_prepare_source_chunk_filters_unknown_repo_and_uses_nullable_ints(self):
        df = pd.DataFrame(
            [
                MakeIssueRow("2024-01-01T00:00:00Z", "12"),
                MakeIssueRow("2024-01-01T01:00:00Z", "", repo_name="bad-format"),
            ]
        )

        out = mod.PrepareSourceChunk(df, KEEP_COLS, {"owner/repo": "owner/repo"})

        self.assertEqual(len(out), 1)
        self.assertEqual(out.loc[0, "safe_repo_name"], "owner___repo")
        self.assertEqual(out.loc[0, "repo_name"], "owner/repo")
        self.assertEqual(str(out["issue_number"].dtype), "Int64")
        self.assertEqual(str(out["issue_user_id"].dtype), "Int64")
        self.assertEqual(int(out.loc[0, "issue_user_id"]), 12)

    def test_process_category_uses_available_workers_for_all_parallel_phases(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fake_paths = [root / "one.csv", root / "two.csv"]
            for path in fake_paths:
                path.write_text("repo_name\n", encoding="utf-8")

            cfg = mod.CATEGORIES[0]
            with mock.patch.object(mod, "OUTDIR", root / "out"), \
                 mock.patch.object(mod, "OUTDIR_LOG", root / "logs"), \
                 mock.patch.object(mod, "STAGE_ROOT", root / "stage"), \
                 mock.patch.object(mod, "DiscoverPaths", return_value=fake_paths), \
                 mock.patch.object(mod, "BuildRepoParts", return_value=["repo_a", "repo_b"]) as build_mock, \
                 mock.patch.object(mod, "MergeRepoParts") as merge_mock, \
                 mock.patch.object(mod, "RunParallel") as parallel_mock, \
                 mock.patch.object(mod, "CombineFlaggedParts"):
                mod.ProcessCategory(cfg, {}, max_workers=8)

            self.assertEqual(build_mock.call_args[0][5], 2)
            self.assertEqual(merge_mock.call_args[0][5], 2)
            self.assertEqual(parallel_mock.call_args[0][2], 2)

    def test_stage_source_file_retries_with_python_engine_after_empty_write(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            parts_dir = root / "parts"
            stale_part = parts_dir / "owner___repo" / "00007_00001.parquet"
            stale_part.parent.mkdir(parents=True, exist_ok=True)
            stale_part.write_text("stale", encoding="utf-8")

            with mock.patch.object(
                mod,
                "StageSourceFileOnce",
                side_effect=[(0, set(), 5), (2, {"owner___repo"}, 5)],
            ) as stage_once:
                repo_names = mod.StageSourceFile(root / "input.csv", KEEP_COLS, {}, parts_dir, 7, 100)

            self.assertEqual(repo_names, ["owner___repo"])
            self.assertFalse(stale_part.exists())
            self.assertFalse(stage_once.call_args_list[0].kwargs["force_python"])
            self.assertTrue(stage_once.call_args_list[1].kwargs["force_python"])

    def test_stage_source_file_retries_with_python_engine_after_parser_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            parts_dir = root / "parts"
            stale_part = parts_dir / "owner___repo" / "00003_00001.parquet"
            stale_part.parent.mkdir(parents=True, exist_ok=True)
            stale_part.write_text("stale", encoding="utf-8")

            with mock.patch.object(
                mod,
                "StageSourceFileOnce",
                side_effect=[pd.errors.ParserError("bad csv"), (2, {"owner___repo"}, 5)],
            ) as stage_once:
                repo_names = mod.StageSourceFile(root / "input.csv", KEEP_COLS, {}, parts_dir, 3, 100)

            self.assertEqual(repo_names, ["owner___repo"])
            self.assertFalse(stale_part.exists())
            self.assertFalse(stage_once.call_args_list[0].kwargs["force_python"])
            self.assertTrue(stage_once.call_args_list[1].kwargs["force_python"])

    def test_stage_and_merge_keep_nullable_integer_schema(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            csv_path = root / "input.csv"
            parts_dir = root / "parts"
            export_dir = root / "export"
            parts_dir.mkdir()
            export_dir.mkdir()

            pd.DataFrame(
                [
                    MakeIssueRow("2024-01-01T00:00:00Z", "99"),
                    MakeIssueRow("2024-01-01T01:00:00Z", ""),
                ]
            ).to_csv(csv_path, index=False)

            repo_names = mod.StageSourceFile(csv_path, KEEP_COLS, {"owner/repo": "owner/repo"}, parts_dir, 1, 1)
            mod.MergeOneRepoParts("owner___repo", str(parts_dir), str(export_dir), KEEP_COLS)

            out = pd.read_parquet(export_dir / "owner___repo.parquet")
            self.assertEqual(repo_names, ["owner___repo"])
            self.assertEqual(len(out), 2)
            self.assertEqual(out.loc[0, "issue_user_id"], 99)
            self.assertTrue(pd.isna(out.loc[1, "issue_user_id"]))

    def test_combine_flagged_parts_streams_without_concat(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            parts_dir = root / "parts"
            out_csv = root / "combined.csv"
            parts_dir.mkdir()

            pd.DataFrame({"col": [1]}).to_parquet(parts_dir / "one.parquet", index=False)
            pd.DataFrame({"col": [2]}).to_parquet(parts_dir / "two.parquet", index=False)

            with mock.patch.object(pd, "concat", side_effect=AssertionError("concat should not be used")):
                mod.CombineFlaggedParts(parts_dir, out_csv)

            out = pd.read_csv(out_csv)
            self.assertEqual(out["col"].tolist(), [1, 2])


if __name__ == "__main__":
    unittest.main()
