# Coding Conventions

## Naming

- **Functions**: CamelCase (except internal helpers nested inside functions)
- **Variables**: snake_case (except global variables and paths use CAPITALIZED)
- **Paths**: Always `Path` objects from `pathlib`, named `INDIR_*` or `OUTDIR_*` (or `INDIR`/`OUTDIR`)
- **Typing**: Don't implement typing
- **Comments**: Don't write any code comments.
- **Organization**: All functions should have a `Main()` function that takes no input, and relies on helper functions throughout the rest of the script, unless it is purely a helper script.
- **PYTHONPATH**: Assume `export PYTHONPATH=.` is done, so you should always import project-specific dependencies from the root.
- **ORDERING**: Functions should be in the order they are called, with `Main()` first

