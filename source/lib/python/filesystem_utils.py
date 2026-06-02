import hashlib
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def WriteContentHash(log_dir, outfile, pattern="*.log"):
    named_content_hashes = []
    for log_file in sorted(Path(log_dir).rglob(pattern)):
        if log_file.name == "sconscript.log" or log_file.name.startswith("._"):
            continue
        for content_hash in re.findall(r"MD5 hash:\s*([0-9a-f]+)", log_file.read_text()):
            named_content_hashes.append(f"{log_file.name}:{content_hash}")
    aggregate_hash = hashlib.md5()
    for named_hash in sorted(named_content_hashes):
        aggregate_hash.update(named_hash.encode())
    Path(outfile).parent.mkdir(parents=True, exist_ok=True)
    Path(outfile).write_text(aggregate_hash.hexdigest())


# SINGLE-USE EXCEPTION: scrape entry point (write_scrape_hashes.py) hashes raw
# scrape parquets that have no SaveData logs, so it can't use WriteContentHash.
def WriteDirectoryHash(directory, outfile, pattern="*.parquet"):
    files = sorted(Path(directory).rglob(pattern))

    def _stat(f):
        return f.name.encode() + str(f.stat().st_mtime).encode()

    with ThreadPoolExecutor() as pool:
        chunks = list(pool.map(_stat, files))

    h = hashlib.md5()
    for chunk in chunks:
        h.update(chunk)
    Path(outfile).parent.mkdir(parents=True, exist_ok=True)
    Path(outfile).write_text(h.hexdigest())


def CleanDirs(dirs, patterns=("*.parquet", "*.log")):
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        for pattern in patterns:
            for f in d.glob(pattern):
                if f.name != "sconscript.log":
                    f.unlink(missing_ok=True)
