import polars as pl
from pathlib import Path
from source.lib.JMSLab.SaveData import SaveData

INDIR_ISSUE = Path('drive/output/scrape/extract_github_data/repo_level_data/issue')
INDIR_PR = Path('drive/output/scrape/extract_github_data/repo_level_data/pr')
OUTDIR = Path('output/derived/create_bot_list')

def Main():
    OUTDIR.mkdir(parents=True, exist_ok=True)

    files = [f for f in INDIR_ISSUE.glob("*.parquet") if not f.name.startswith('._')] + \
            [f for f in INDIR_PR.glob("*.parquet") if not f.name.startswith('._')]

    dfs = []
    for f in files:
        df = pl.read_parquet(f, columns=['actor_id', 'actor_login'])
        df = df.with_columns(pl.col('actor_id').cast(pl.Int64, strict=False))
        dfs.append(df)

    df = pl.concat(dfs, how='vertical').unique(subset=['actor_id', 'actor_login'])

    bots_df = df.filter(
        pl.col('actor_login').str.ends_with('[bot]') |
        pl.col('actor_login').str.ends_with('bot')
    ).to_pandas().drop_duplicates()

    SaveData(bots_df, ['actor_id', 'actor_login'], OUTDIR / 'bot_list.parquet', OUTDIR / 'bot_list.log')

if __name__ == "__main__":
    Main()
