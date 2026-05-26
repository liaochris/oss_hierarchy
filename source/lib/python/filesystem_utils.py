import hashlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


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
