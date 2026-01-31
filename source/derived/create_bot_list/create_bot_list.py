import polars as pl
from pathlib import Path
from source.lib.JMSLab.SaveData import SaveData

INDIR_ISSUE = Path('drive/output/scrape/extract_github_data/repo_level_data/issue')
INDIR_PR = Path('drive/output/scrape/extract_github_data/repo_level_data/pr')
OUTDIR = Path('output/derived/create_bot_list')

def Main():
    OUTDIR.mkdir(parents=True, exist_ok=True)

    files = list(INDIR_ISSUE.glob("*.parquet")) + list(INDIR_PR.glob("*.parquet"))

    if not files:
        print("No parquet files found")
        return

    df = pl.scan_parquet(files).select(['actor_id', 'actor_login']).unique().collect()

    bots_df = df.filter(
        df['actor_login'].str.ends_with('[bot]') |
        df['actor_login'].str.ends_with('bot')
    ).to_pandas()

    SaveData(bots_df, ['actor_id'], OUTDIR / 'bot_list.parquet', OUTDIR / 'bot_list.log')

if __name__ == "__main__":
    Main()
