from pathlib import Path
import pandas as pd
from collections import defaultdict


def CollectParquetFiles(input_dir):
    return [f for f in input_dir.iterdir() if f.suffix == ".parquet"]


def WriteProjectShards(parquet_files, temp_dir):
    project_shards = defaultdict(list)
    for parquet_file in parquet_files:
        df = pd.read_parquet(parquet_file)
        for project, project_df in df.groupby("project"):
            shard_path = temp_dir / f"{project}_{len(project_shards[project])}.parquet"
            project_df.to_parquet(shard_path, index=False, engine="pyarrow", compression="snappy")
            project_shards[project].append(shard_path)
    return project_shards


def MergeAndWriteProjects(project_shards, output_dir):
    for project, shards in project_shards.items():
        dfs = [pd.read_parquet(shard) for shard in shards]
        final_df = pd.concat(dfs, ignore_index=True)
        output_path = output_dir / f"{project}.parquet"
        final_df.to_parquet(output_path, index=False, engine="pyarrow", compression="snappy")


def CleanupTempFiles(project_shards, temp_dir):
    for shards in project_shards.values():
        for shard in shards:
            shard.unlink()
    temp_dir.rmdir()


def SplitParquetsByProjectSingleFile(input_dir, output_dir, temp_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = CollectParquetFiles(input_dir)
    project_shards = WriteProjectShards(parquet_files, temp_dir)
    MergeAndWriteProjects(project_shards, output_dir)
    CleanupTempFiles(project_shards, temp_dir)

    print(f"✅ Done! One parquet per project in {output_dir}")


def Main():
    input_dir = Path("drive/output/scrape/pypi_downloads/pypi_package_downloads")
    output_dir = Path("drive/output/scrape/pypi_downloads/pypi_package_downloads_package_level")
    temp_dir = Path("__tmp_split__")

    SplitParquetsByProjectSingleFile(input_dir, output_dir, temp_dir)


if __name__ == "__main__":
    Main()
