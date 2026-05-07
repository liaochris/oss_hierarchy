def CleanDirs(dirs, patterns=("*.parquet", "*.log")):
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        for pattern in patterns:
            for f in d.glob(pattern):
                if f.name != "sconscript.log":
                    f.unlink(missing_ok=True)
