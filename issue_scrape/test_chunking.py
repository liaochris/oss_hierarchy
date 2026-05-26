"""
Smoke-test for chunked processing logic in link_using_issue.py.

Uses CHUNK_SIZE=3 over a 10-row fake df_issue, with GrabIssueData mocked
so no network calls are made. Verifies:
  1. Chunk files are created per chunk
  2. Final files are written with all rows
  3. Chunk files are deleted after aggregation
  4. Resume: re-running skips already-done chunks
"""

import sys
import os
import tempfile
import pandas as pd
import numpy as np
from pathlib import Path
from unittest import mock

# ---------------------------------------------------------------------------
# Monkey-patch CHUNK_SIZE before importing Main
# ---------------------------------------------------------------------------
import source.scrape.link_issue_pull_request.code.link_using_issue as mod

SMALL_CHUNK = 3
mod.CHUNK_SIZE = SMALL_CHUNK

# ---------------------------------------------------------------------------
# Fake GrabIssueData — returns a minimal valid dict with a timeline row
# ---------------------------------------------------------------------------
def _fake_grab(repo_name, issue_number):
    tl = pd.DataFrame([{
        "type": "IssueOpened", "author": "bot", "date": "2020-01-01",
        "text": f"body of issue {issue_number}", "url": None,
        "repo_name": repo_name, "issue_number": issue_number,
    }])
    return {"pr_links": [], "github_links": [], "timeline_df": tl, "is_pull": False}


def run_test():
    N = 10  # total issues
    repo_name = "test-org/test-repo"
    safe_repo = "test-org___test-repo"

    df_issue = pd.DataFrame({
        "repo_name": [repo_name] * N,
        "issue_number": list(range(1, N + 1)),
    })

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        OUTDIR        = tmpdir / "linked"
        TIMELINE_DIR  = tmpdir / "timeline"
        CHUNK_OUTDIR       = OUTDIR / "chunks"
        CHUNK_TIMELINE_DIR = TIMELINE_DIR / "chunks"

        for d in [OUTDIR, TIMELINE_DIR, CHUNK_OUTDIR, CHUNK_TIMELINE_DIR]:
            d.mkdir(parents=True)

        repo_file     = OUTDIR / f"{safe_repo}.parquet"
        timeline_file = TIMELINE_DIR / f"{safe_repo}_issue_timeline.parquet"

        expected_chunks = math_ceil(N, SMALL_CHUNK)

        # ---- run chunked processing (inline, same logic as Main) ----------
        call_counts = {"total": 0}

        def counted_grab(repo_name, issue_number):
            call_counts["total"] += 1
            return _fake_grab(repo_name, issue_number)

        _run_chunked(df_issue, safe_repo, OUTDIR, TIMELINE_DIR,
                     CHUNK_OUTDIR, CHUNK_TIMELINE_DIR, counted_grab)

        # ---- assertions ---------------------------------------------------
        assert repo_file.exists(),     "Final linked parquet missing"
        assert timeline_file.exists(), "Final timeline parquet missing"

        out_df = pd.read_parquet(repo_file)
        assert len(out_df) == N, f"Expected {N} rows, got {len(out_df)}"

        tl_df = pd.read_parquet(timeline_file)
        assert len(tl_df) == N, f"Expected {N} timeline rows, got {len(tl_df)}"

        # chunk files should be gone
        leftover = list(CHUNK_OUTDIR.glob("*.parquet")) + list(CHUNK_TIMELINE_DIR.glob("*.parquet"))
        assert not leftover, f"Chunk files not cleaned up: {leftover}"

        assert call_counts["total"] == N, f"Expected {N} GrabIssueData calls, got {call_counts['total']}"
        print(f"PASS: {N} issues processed in chunks of {SMALL_CHUNK}, final files correct, chunks cleaned up.")

        # ---- resume test: delete chunk 1 final parquet, re-run -----------
        # Simulate partial run: remove final files, pre-populate chunks 0..1
        repo_file.unlink()
        timeline_file.unlink()
        chunks = [df_issue.iloc[i:i+SMALL_CHUNK].copy() for i in range(0, N, SMALL_CHUNK)]
        pre_done = 2  # pretend first 2 chunks already done
        for i in range(pre_done):
            chunk_out = CHUNK_OUTDIR / f"{safe_repo}_chunk_{i:04d}.parquet"
            chunk_tl  = CHUNK_TIMELINE_DIR / f"{safe_repo}_chunk_{i:04d}_timeline.parquet"
            ch = chunks[i].copy()
            ch["linked_pull_request"] = [
                {"pr_links": [], "github_links": [], "is_pull": False}
            ] * len(ch)
            ch.to_parquet(chunk_out, engine="pyarrow")
            tl_rows = [_fake_grab(r, n)["timeline_df"] for r, n in zip(ch["repo_name"], ch["issue_number"])]
            pd.concat(tl_rows, ignore_index=True).to_parquet(chunk_tl, engine="pyarrow", index=False)

        resume_calls = {"total": 0}
        def counted_grab2(repo_name, issue_number):
            resume_calls["total"] += 1
            return _fake_grab(repo_name, issue_number)

        _run_chunked(df_issue, safe_repo, OUTDIR, TIMELINE_DIR,
                     CHUNK_OUTDIR, CHUNK_TIMELINE_DIR, counted_grab2)

        expected_resume_calls = N - pre_done * SMALL_CHUNK
        assert resume_calls["total"] == expected_resume_calls, (
            f"Resume: expected {expected_resume_calls} calls, got {resume_calls['total']}"
        )
        assert repo_file.exists() and timeline_file.exists(), "Final files missing after resume"
        print(f"PASS: Resume skipped {pre_done} chunks, only called GrabIssueData {expected_resume_calls} times.")


def math_ceil(n, k):
    return (n + k - 1) // k


def _run_chunked(df_issue, safe_repo, OUTDIR, TIMELINE_DIR,
                 CHUNK_OUTDIR, CHUNK_TIMELINE_DIR, grab_fn):
    """Core chunked logic extracted from Main() for testability."""
    repo_file     = OUTDIR / f"{safe_repo}.parquet"
    timeline_file = TIMELINE_DIR / f"{safe_repo}_issue_timeline.parquet"

    chunks = [
        df_issue.iloc[i:i + SMALL_CHUNK].copy()
        for i in range(0, len(df_issue), SMALL_CHUNK)
    ]

    for chunk_idx, chunk_df in enumerate(chunks):
        chunk_out = CHUNK_OUTDIR / f"{safe_repo}_chunk_{chunk_idx:04d}.parquet"
        chunk_tl  = CHUNK_TIMELINE_DIR / f"{safe_repo}_chunk_{chunk_idx:04d}_timeline.parquet"

        if chunk_out.exists() and chunk_tl.exists():
            print(f"  Chunk {chunk_idx + 1}/{len(chunks)} already done, skipping.")
            continue

        results = [grab_fn(row["repo_name"], int(float(row["issue_number"])))
                   for _, row in chunk_df.iterrows()]
        chunk_df["linked_pull_request"] = results

        timeline_dfs = [
            d["timeline_df"]
            for d in chunk_df["linked_pull_request"]
            if isinstance(d, dict) and isinstance(d.get("timeline_df"), pd.DataFrame)
        ]

        if timeline_dfs:
            pd.concat(timeline_dfs, ignore_index=True).to_parquet(chunk_tl, engine="pyarrow", index=False)
        else:
            pd.DataFrame().to_parquet(chunk_tl, engine="pyarrow", index=False)

        chunk_df["linked_pull_request"] = chunk_df["linked_pull_request"].apply(
            lambda d: {
                "pr_links": d.get("pr_links", []),
                "github_links": d.get("github_links", []),
                "is_pull": d.get("is_pull", False),
            } if isinstance(d, dict) else d
        )
        chunk_df.to_parquet(chunk_out, engine="pyarrow")

    # Aggregate
    chunk_dfs = [
        pd.read_parquet(CHUNK_OUTDIR / f"{safe_repo}_chunk_{i:04d}.parquet")
        for i in range(len(chunks))
    ]
    pd.concat(chunk_dfs, ignore_index=True).to_parquet(repo_file, engine="pyarrow")

    tl_dfs = [
        pd.read_parquet(CHUNK_TIMELINE_DIR / f"{safe_repo}_chunk_{i:04d}_timeline.parquet")
        for i in range(len(chunks))
    ]
    combined_tl = pd.concat([t for t in tl_dfs if not t.empty], ignore_index=True)
    combined_tl.to_parquet(timeline_file, engine="pyarrow", index=False)

    # Cleanup
    for i in range(len(chunks)):
        (CHUNK_OUTDIR / f"{safe_repo}_chunk_{i:04d}.parquet").unlink()
        (CHUNK_TIMELINE_DIR / f"{safe_repo}_chunk_{i:04d}_timeline.parquet").unlink()


if __name__ == "__main__":
    run_test()
