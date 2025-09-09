import os
import glob
import shutil
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def IsDatetimeLike(value: str) -> bool:
    try:
        datetime.fromisoformat(value)
        return True
    except Exception:
        return False


def BuildRepoRenameLookup(repo_latest_path: Path) -> pd.DataFrame:
    df = pd.read_csv(repo_latest_path)
    df = df[df["latest_repo_name"] != "ERROR"]
    return df[["repo_name", "latest_repo_name"]].drop_duplicates()


def ProcessRepoGroup(repo, gdf, staging_dir, keep_cols, category_name, batch_id):
    safe_name = repo.replace("/", "_")
    if IsDatetimeLike(safe_name):
        print(f"[{category_name}] ⚠️ Repo looks datetime-like: {repo} -> {safe_name}")

    table = pa.Table.from_pandas(gdf, preserve_index=False)

    # normalize schema: replace null-only cols with string
    fields = []
    for c in keep_cols:
        if c in table.schema.names:
            f = table.schema.field(c)
            if pa.types.is_null(f.type):
                fields.append(pa.field(c, pa.string()))
            else:
                fields.append(f)
        else:
            fields.append(pa.field(c, pa.string()))
    full_schema = pa.schema(fields)

    out_path = staging_dir / f"{safe_name}__batch{batch_id}.parquet"
    pq.write_table(table.cast(full_schema, safe=False), out_path)

    return repo, len(gdf), out_path


def BuildByRepo(globs_list,
                repo_lookup_df: pd.DataFrame,
                staging_dir: Path,
                category_name: str,
                keep_cols: list[str],
                casts: dict[str, pa.DataType],
                ts_col: str,
                dedup_subset: list[str] | None = None,
                text_cols: tuple[str, ...] = (),
                batch_size: int = 40,
                parallel: bool = True,
                max_workers: int = 4):

    staging_dir.mkdir(parents=True, exist_ok=True)
    paths = NonEmptyPaths(globs_list)
    repos_seen = set()

    for i in range(0, len(paths), batch_size):
        batch = paths[i:i + batch_size]
        dfs = []

        for p in batch:
            for chunk in pd.read_csv(
                p,
                chunksize=50_000,
                dtype=str,
                encoding="utf-8",
                on_bad_lines="skip"
            ):
                for c in keep_cols:
                    if c not in chunk.columns:
                        chunk[c] = pd.NA
                for c in text_cols:
                    if c in chunk.columns:
                        chunk[c] = chunk[c].astype("string")

                dfs.append(chunk[keep_cols])

        if not dfs:
            continue

        df = pd.concat(dfs, ignore_index=True)
        del dfs

        # casting numeric cols
        for c, t in casts.items():
            if c in df.columns:
                if pa.types.is_integer(t) or pa.types.is_floating(t):
                    df[c] = pd.to_numeric(df[c], errors="coerce")

        if ts_col in df.columns:
            df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
            df = df.dropna(subset=[ts_col])
            df["date"] = df[ts_col].dt.strftime("%Y-%m-%d")

        # repo rename mapping
        df = df.merge(repo_lookup_df, on="repo_name", how="left")
        if "latest_repo_name" in df.columns:
            df["repo_name"] = df["latest_repo_name"].fillna(df["repo_name"])
            df = df.drop(columns=["latest_repo_name"])

        if dedup_subset:
            df = df.drop_duplicates(subset=dedup_subset)

        if df.empty:
            continue

        if parallel:
            futures = []
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                for repo, gdf in df.groupby("repo_name"):
                    if not isinstance(repo, str) or not repo.strip():
                        continue
                    repos_seen.add(repo)
                    futures.append(executor.submit(
                        ProcessRepoGroup, repo, gdf, staging_dir, keep_cols, category_name, i
                    ))

                for f in as_completed(futures):
                    repo, nrows, path = f.result()
                    print(f"[{category_name}] staged repo: {repo} ({nrows} rows) → {path}")
        else:
            for repo, gdf in df.groupby("repo_name"):
                if not isinstance(repo, str) or not repo.strip():
                    continue
                repos_seen.add(repo)
                repo, nrows, path = ProcessRepoGroup(repo, gdf, staging_dir, keep_cols, category_name, i)
                print(f"[{category_name}] staged repo: {repo} ({nrows} rows) → {path}")

        del df

    return repos_seen

def CompactRepos(staging_dir: Path, final_dir: Path, repo_names: set[str]):
    final_dir.mkdir(parents=True, exist_ok=True)

    for repo in sorted(repo_names):  # sorted for stable output order
        safe_name = repo.replace("/", "_")
        parts = list(staging_dir.glob(f"{safe_name}__batch*.parquet"))
        if not parts:
            print(f"[compact] skipped {repo} (no parts)")
            continue

        dfs = [pq.read_table(p).to_pandas() for p in parts]
        combined = pd.concat(dfs, ignore_index=True)

        out_path = final_dir / f"{safe_name}.parquet"
        combined.to_parquet(out_path, index=False)

        print(f"[compact] wrote {repo} → {out_path} ({len(combined)} rows)")

def RepoPath(out_dir: Path, repo_name: str) -> Path:
    return out_dir / f"{repo_name.replace('/', '_')}.parquet"


def NonEmptyPaths(globs_list):
    paths = [p for g in globs_list for p in glob.glob(g)]
    return [p for p in paths if Path(p).exists() and Path(p).stat().st_size > 0]


def Zip(category_dir: Path):
    archive = category_dir.parent / f"{category_dir.name}.zip"
    shutil.make_archive(str(archive.with_suffix("")), "zip", root_dir=category_dir.parent, base_dir=category_dir.name)


def Main():
    indir_repo_match = Path("output/scrape/extract_github_data")
    outdir_root = Path("drive/output/derived/repo_level_data")
    outdir_root.mkdir(parents=True, exist_ok=True)

    repo_lookup_df = BuildRepoRenameLookup(indir_repo_match / "repo_id_history_latest.csv")

    categories = [
        {
            "name": "pr",
            "globs": [
                "drive/output/scrape/extract_github_data/pull_request_data/*.csv",
                "drive/output/scrape/extract_github_data/pull_request_review_data/*.csv",
                "drive/output/scrape/extract_github_data/pull_request_review_comment_data/*.csv",
            ],
            "keep": [
                "type", "created_at", "repo_id", "repo_name", "actor_id", "actor_login",
                "pr_number", "pr_title", "pr_body", "pr_action",
                "pr_merged_by_id", "pr_merged_by_type", "pr_label",
                "pr_review_action", "pr_review_id", "pr_review_state", "pr_review_body",
                "pr_review_comment_body", "pr_review_comment_position",
                "pr_review_comment_original_position",
                "pr_review_comment_original_commit_id", "pr_review_comment_commit_id",
                "pr_review_comment_path",
                "pr_assignee", "pr_assignees", "pr_requested_reviewers",
                "date",
            ],
            "casts": {
                "repo_id": pa.int64(),
                "pr_number": pa.int64(),
                "pr_review_comment_position": pa.float64(),
                "pr_review_comment_original_position": pa.float64(),
            },
            "ts_col": "created_at",
            "text_cols": (
                "pr_body", "pr_title", "pr_review_body", "pr_review_comment_body",
                "pr_review_state", "pr_review_action", "pr_review_comment_path",
            ),
            ],
            "keep": [
                "type", "created_at", "repo_id", "repo_name", "actor_id", "actor_login",
                "issue_number", "issue_body", "issue_title", "issue_action", "issue_state",
                "issue_comment_id", "issue_user_id", "issue_comment_body",
                "latest_issue_assignees", "latest_issue_assignee", "latest_issue_labels", "actor_type",
                "date",
            ],
            "casts": {},
            "ts_col": "created_at",
            "text_cols": ("issue_body", "issue_title", "issue_comment_body"),
            "batch_size": 10,
        }
    ]

    for cfg in categories:
        if cfg['name'] != "issue": continue
        staging_dir = outdir_root / f"{cfg['name']}_staging"
        final_dir = outdir_root / cfg["name"]

        repos_seen = BuildByRepo(
            globs_list=cfg["globs"],
            repo_lookup_df=repo_lookup_df,
            staging_dir=staging_dir,
            category_name=cfg["name"],
            keep_cols=cfg["keep"],
            casts=cfg["casts"],
            ts_col=cfg["ts_col"],
            text_cols=cfg["text_cols"],
            batch_size=cfg["batch_size"],
            parallel=True,
            max_workers=8
        )

        CompactRepos(staging_dir, final_dir, repos_seen)
        Zip(final_dir)

        shutil.rmtree(staging_dir, ignore_errors=True)
        print(f"[cleanup] removed staging dir: {staging_dir}")


if __name__ == "__main__":
    Main()
