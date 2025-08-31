import os
import csv
import glob
import shutil
import tempfile
from pathlib import Path
from datetime import datetime

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from source.lib.helpers import ImputeTimePeriodPL

import pandas as pd

def ReadWithPandas(paths, keep_cols, text_cols):
    frames = []
    for p in paths:
        df = pd.read_csv(
            p,
            engine="python",          # more tolerant parser
            sep=",",                  # explicit CSV
            quotechar='"',            # handle long multi-line quoted text
            doublequote=True,         # allow "" inside quoted fields
            escapechar="\\",          # keep backslashes as escape
            on_bad_lines="skip",      # drop only malformed rows
            encoding="utf-8",
            keep_default_na=False,    # keep TRUE/FALSE, 0/1, etc. as literal text
        )

        # Make sure all keep_cols exist
        for c in keep_cols:
            if c not in df.columns:
                df[c] = pd.NA

        # Force text columns to string dtype
        for c in text_cols:
            if c in df.columns:
                df[c] = df[c].astype("string")

        frames.append(df[keep_cols])

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=keep_cols)

def IsDatetimeLike(value: str) -> bool:
    try:
        datetime.fromisoformat(value)
        return True
    except Exception:
        return False


def Main():
    indir_repo_match = Path("output/scrape/extract_github_data")
    outdir_root = Path("drive/output/derived/data_export")
    outdir_root.mkdir(parents=True, exist_ok=True)

    repo_df = pl.read_csv(indir_repo_match / "repo_id_history_filtered.csv")
    repo_lookup_df = BuildRepoRenameLookup(repo_df)

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
                "pr_review_comment_path", 'pr_assignee', 'pr_assignees',
                'pr_requested_reviewers'
            ],
            "casts": {
                "repo_id": pl.Int64,
                "pr_number": pl.Int64,
                "pr_review_comment_position": pl.Float64,
                "pr_review_comment_original_position": pl.Float64,
            },
            "ts_col": "created_at",
            "dedup": None,
            "text_cols": ("pr_body", "pr_title", "pr_review_body", "pr_review_comment_body"),
            "batch_size": 10,
        },
        {
            "name": "issue",
            "globs": [
                "drive/output/scrape/extract_github_data/issue_data/*.csv",
                "drive/output/scrape/extract_github_data/issue_comment_data/*.csv",
            ],
            "keep": [
                "type", "created_at", "repo_id", "repo_name", "actor_id", "actor_login",
                "issue_number", "issue_body", "issue_title", "issue_action", "issue_state",
                "issue_comment_id", "issue_user_id", "issue_comment_body",
                'latest_issue_assignees',
                "latest_issue_assignee", "latest_issue_labels", "actor_type",
            ],
            "casts": {
                "actor_id": pl.Int64,
                "repo_id": pl.Int64,
            },
            "ts_col": "created_at",
            "dedup": None,
            "text_cols": ("issue_body", "issue_title", "issue_comment_body"),
            "batch_size": 10,
        },
        {
            "name": "pr_commit",
            "globs": ["drive/output/scrape/collect_commits/pr/*"],
            "keep": [
                "repo_name", "commit sha", "commit author name", "commit author email",
                "commit additions", "commit deletions", "commit changes total",
                "commit files changed count", "commit file changes", "commit time", "pr_number",
            ],
            "casts": {
                "commit additions": pl.Int64,
                "commit deletions": pl.Int64,
                "commit changes total": pl.Int64,
                "commit files changed count": pl.Int64,
                "commit file changes": pl.Utf8,
            },
            "ts_col": "commit time",
            "dedup": ["commit sha"],
            "text_cols": ("commit file changes",),
            "batch_size": 30,
        },
        {
            "name": "push_commit",
            "globs": ["drive/output/scrape/collect_commits/push/*"],
            "keep": [
                "repo_name", "commit sha", "commit author name", "commit author email",
                "commit additions", "commit deletions", "commit changes total",
                "commit files changed count", "commit file changes", "commit time", "push_id",
            ],
            "casts": {
                "commit additions": pl.Int64,
                "commit deletions": pl.Int64,
                "commit changes total": pl.Int64,
                "commit files changed count": pl.Int64,
                "commit file changes": pl.Utf8,
            },
            "ts_col": "commit time",
            "dedup": ["commit sha"],
            "text_cols": ("commit file changes",),
            "batch_size": 30,
        },
    ]

    for cfg in categories:
        if cfg['name'] not in ["issue","pr"]:
            continue
        out_dir = outdir_root / cfg["name"]
        BuildByRepo(
            globs_list=cfg["globs"],
            repo_lookup_df=repo_lookup_df,
            out_dir=out_dir,
            category_name=cfg["name"],
            keep_cols=cfg["keep"],
            casts=cfg["casts"],
            ts_col=cfg["ts_col"],
            dedup_subset=cfg["dedup"],
            text_cols=cfg["text_cols"],
            batch_size=cfg["batch_size"],
        )
        Zip(out_dir)


def BuildRepoRenameLookup(repo_df: pl.DataFrame) -> pl.DataFrame:
    repo_df = repo_df.with_columns(
        pl.col("last_seen").str.strptime(pl.Datetime, strict=False).alias("last_seen")
    )
    latest = (
        repo_df.sort("last_seen")
        .group_by("repo_group")
        .agg(pl.col("repo_name").last().alias("repo_name_latest"))
    )
    return repo_df.select("repo_name", "repo_group").join(
        latest, on="repo_group", how="left"
    ).select("repo_name", "repo_name_latest")


def BuildByRepo(globs_list,
                repo_lookup_df: pl.DataFrame,
                out_dir: Path,
                category_name: str,
                keep_cols: list[str],
                casts: dict[str, pl.DataType],
                ts_col: str,
                dedup_subset: list[str] | None,
                text_cols: tuple[str, ...],
                batch_size: int = 40):
    out_dir.mkdir(parents=True, exist_ok=True)
    repo_lookup_lf = repo_lookup_df.lazy()
    writers, schemata = {}, {}

    paths = NonEmptyPaths(globs_list)
    for i in range(0, len(paths), batch_size):
        batch = paths[i:i + batch_size]

        schema_overrides = {c: pl.Utf8 for c in text_cols if c in keep_cols}

        def MakeCsvReader():
            return pl.scan_csv(
                batch,
                infer_schema_length=10_000,
                encoding="utf8-lossy",
                schema_overrides=schema_overrides,
            )

        def BuildPipeline(make_reader):
            lf = make_reader()
            names = set(lf.collect_schema().names())
            have = [c for c in keep_cols if c in names]

            missing = []
            for c in keep_cols:
                if c not in names:
                    if c in schema_overrides:  # force text cols to Utf8
                        missing.append(pl.lit(None, dtype=pl.Utf8).alias(c))
                    elif c in casts:  # if we know a numeric cast is expected
                        missing.append(pl.lit(None, dtype=casts[c].to_arrow()).alias(c))
                    else:  # fallback to Utf8
                        missing.append(pl.lit(None, dtype=pl.Utf8).alias(c))

            lf = lf.select([pl.col(c) for c in have] + missing)
            
            lf = EnsureTypes(lf, casts, keep_cols)
            if ts_col in keep_cols:
                lf = AddDate(lf, ts_col)
                lf = ImputeTimePeriodPL(lf, ts_col, months=6)
            lf = NormalizeRepo(lf, repo_lookup_lf, "repo_name")
            lf = lf.unique(subset=dedup_subset) if dedup_subset else lf
            return lf

        try:
            df = BuildPipeline(MakeCsvReader).collect()
        except Exception:
            parquet_path = CleanCsvToParquet(batch, keep_cols, text_cols)
            try:
                def MakeParquetReader():
                    return pl.scan_parquet(parquet_path)
                df = BuildPipeline(MakeParquetReader).collect()
            finally:
                try:
                    os.remove(parquet_path)
                except Exception:
                    pass

        if df.height == 0:
            continue

        for repo, gdf in df.partition_by("repo_name_latest", as_dict=True).items():
            repo = (str(repo) if repo else "").strip()
            if not repo:
                continue

            safe_name = (
                repo.replace("/", "_").replace(",", "").replace("(", "")
                .replace(")", "").replace('"', "").replace("'", "")
            )

            if IsDatetimeLike(safe_name):
                print(f"[{category_name}] ⚠️ Repo looks datetime-like: {repo} -> {safe_name}")
                print(f"[{category_name}] Batch paths: {batch}")

            table_cols = [c for c in keep_cols if c in gdf.columns]
            if "date" in gdf.columns:
                table_cols.append("date")
            if "time_period" in gdf.columns:
                table_cols.append("time_period")
            if "repo_name_latest" not in table_cols and "repo_name_latest" in gdf.columns:
                table_cols.append("repo_name_latest")
            if "sanitized" in gdf.columns:
                table_cols.append("sanitized")
            table = gdf.select(table_cols).to_arrow()
            print(f"[{category_name}] Starting {repo}")
            WritePerRepoBatch(repo, table, out_dir, writers, schemata, keep_cols)
            print(f"[{category_name}] Exported {repo}")

    CloseWriters(writers)


def CleanCsvToParquet(csv_paths, keep_cols, text_cols) -> str:
    df_pd = ReadWithPandas(csv_paths, keep_cols, text_cols)

    fd, parquet_path = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    df_pd.to_parquet(parquet_path, index=False)
    return parquet_path


def EnsureTypes(lf, casts, keep_cols):
    if not casts:
        return lf
    cols = [pl.col(c).cast(t, strict=False) for c, t in casts.items() if c in keep_cols]
    return lf.with_columns(cols) if cols else lf


def AddDate(lf, ts_col):
    return (
        lf.with_columns(pl.col(ts_col).str.strptime(pl.Datetime, strict=False))
          .filter(pl.col(ts_col).is_not_null())
          .with_columns(pl.col(ts_col).dt.strftime("%Y-%m-%d").alias("date"))
    )


def NormalizeRepo(lf, repo_lookup_lf, repo_col):
    if repo_col not in lf.collect_schema().names():
        return lf.with_columns(pl.lit(None, dtype=pl.Utf8).alias("repo_name_latest"))
    lf = lf.join(repo_lookup_lf, left_on=repo_col, right_on="repo_name", how="left")
    return lf.with_columns(pl.coalesce([pl.col("repo_name_latest"), pl.col(repo_col)]).alias("repo_name_latest"))


def WritePerRepoBatch(repo, table, out_dir, writers, schemata, keep_cols: list[str]):
    path = RepoPath(out_dir, repo)
    if repo not in writers:
        # Build schema from keep_cols. Use table types if present, else fallback to Utf8
        fields = []
        for c in keep_cols:
            if c in table.schema.names:
                fields.append(table.schema.field(c))
            else:
                fields.append(pa.field(c, pa.string()))
        full_schema = pa.schema(fields)

        path.parent.mkdir(parents=True, exist_ok=True)
        writers[repo] = pq.ParquetWriter(str(path), full_schema)
        schemata[repo] = full_schema

        # align first table to full schema
        table = AlignToSchema(table, full_schema)

    elif not table.schema.equals(schemata[repo]):
        # Align later tables to the locked schema
        table = AlignToSchema(table, schemata[repo])

    writers[repo].write_table(table)

def CloseWriters(writers):
    for w in writers.values():
        try:
            w.close()
        except Exception:
            pass


def RepoPath(out_dir: Path, repo_name: str) -> Path:
    safe_name = (
        repo_name.replace("/", "_").replace(",", "").replace("(", "")
        .replace(")", "").replace('"', "").replace("'", "")
    )
    return out_dir / f"{safe_name}.parquet"


def AlignToSchema(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
    arrays = []
    for f in target_schema:
        if f.name in table.column_names:
            col = table[f.name]
            if not col.type.equals(f.type):
                try:
                    col = col.cast(f.type)
                except Exception:
                    col = pa.nulls(table.num_rows, type=f.type)
            arrays.append(col)
        else:
            arrays.append(pa.nulls(table.num_rows, type=f.type))
    return pa.Table.from_arrays(arrays, schema=target_schema)


def CombineCategoryToSingleFile(category_dir: Path, out_path: Path) -> Path | None:
    files = list(Path(category_dir).glob("*.parquet"))
    if not files:
        return None
    def CastNumeric(lf: pl.LazyFrame) -> pl.LazyFrame:
        schema = lf.collect_schema()
        numerics = [n for n, dt in zip(schema.names(), schema.dtypes()) if dt.is_numeric()]
        return lf.with_columns(pl.col(numerics).cast(pl.Float64, strict=False)) if numerics else lf
    lf_all = pl.concat([CastNumeric(pl.scan_parquet(str(f))) for f in files], how="vertical_relaxed")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lf_all.collect(streaming=True).write_parquet(out_path)
    return out_path


def Zip(category_dir: Path):
    archive = category_dir.parent / f"{category_dir.name}.zip"
    shutil.make_archive(str(archive.with_suffix("")), "zip", root_dir=category_dir.parent, base_dir=category_dir.name)


def NonEmptyPaths(globs_list):
    paths = [p for g in globs_list for p in glob.glob(g)]
    return [p for p in paths if Path(p).exists() and Path(p).stat().st_size > 0]


def LazyFrameLooksEmpty(lf):
    try:
        return lf.head(1).collect().height == 0
    except Exception:
        try:
            return lf.limit(1).collect(streaming=True).height == 0
        except Exception:
            return True


if __name__ == "__main__":
    Main()
