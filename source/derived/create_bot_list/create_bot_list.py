import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

def ReadParquetSubset(filepath):
    try:
        return pd.read_parquet(filepath, columns=['actor_id','actor_login']).drop_duplicates()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return pd.DataFrame(columns=['actor_id','actor_login'])

def main():
    input_dirs = [
        Path('drive/output/derived/problem_level_data/issue'),
        Path('drive/output/derived/problem_level_data/pr')
    ]
    outdir = Path('output/derived/create_bot_list')
    outdir.mkdir(parents=True, exist_ok=True)
    outfile = outdir / 'bot_list.parquet'

    files = []
    for d in input_dirs:
        files.extend(d.glob("*.parquet"))

    with ProcessPoolExecutor() as executor:
        results = list(executor.map(ReadParquetSubset, files))

    all_df = pd.concat(results, ignore_index=True).drop_duplicates()
    bots_df = all_df[all_df['actor_login'].str.endswith('[bot]')]

    bots_df.to_parquet(outfile, index=False)
    print(f"Saved bot list with {len(bots_df)} rows to {outfile}")

if __name__ == "__main__":
    main()
