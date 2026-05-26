#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import argparse
import os
import pyarrow.parquet as pq


def is_parquet_ok(path: Path) -> tuple[bool, str]:
    """
    Returns (ok, reason). Uses ParquetFile metadata read which is cheap and
    reliably fails on truncated/corrupt files.
    """
    try:
        # This reads footer/metadata; should fail fast for truncation/corruption.
        pf = pq.ParquetFile(str(path))
        _ = pf.schema_arrow  # touch schema to force metadata access
        # Optional extra sanity check: ensure at least 1 row group metadata can be accessed
        if pf.num_row_groups is None:
            return False, "num_row_groups is None"
        return True, "ok"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def scan_and_remove(root: Path, dry_run: bool, recursive: bool) -> int:
    pattern = "**/*.parquet" if recursive else "*.parquet"
    paths = sorted(root.glob(pattern))

    if not paths:
        print(f"No parquet files found under: {root}")
        return 0

    bad = []
    for p in paths:
        ok, reason = is_parquet_ok(p)
        if not ok:
            try:
                size = p.stat().st_size
            except OSError:
                size = -1
            bad.append((p, size, reason))

    if not bad:
        print(f"All good. Checked {len(paths)} parquet files; none are broken.")
        return 0

    print(f"Found {len(bad)} broken parquet files (out of {len(paths)} checked):")
    for p, size, reason in bad:
        print(f" - {p}  ({size} bytes)  -> {reason}")

    if dry_run:
        print("\nDry run enabled; not deleting anything.")
        return len(bad)

    print("\nDeleting broken files...")
    deleted = 0
    for p, _, _ in bad:
        try:
            p.unlink()
            deleted += 1
        except Exception as e:
            print(f" ! Failed to delete {p}: {e}")

    print(f"Deleted {deleted}/{len(bad)} broken parquet files.")
    return deleted


def main():
    ap = argparse.ArgumentParser(description="Remove broken/truncated parquet files.")
    ap.add_argument("dir", type=str, help="Directory containing parquet files (e.g., chunks dir)")
    ap.add_argument("--recursive", action="store_true", help="Recurse into subdirectories")
    ap.add_argument("--dry-run", action="store_true", help="Only report, do not delete")
    args = ap.parse_args()

    root = Path(args.dir).expanduser().resolve()
    if not root.exists():
        raise SystemExit(f"Directory does not exist: {root}")
    if not root.is_dir():
        raise SystemExit(f"Not a directory: {root}")

    scan_and_remove(root, dry_run=args.dry_run, recursive=args.recursive)


if __name__ == "__main__":
    main()