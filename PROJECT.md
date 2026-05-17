## Project Structure

- `source/lib/`
  Shared helpers and shared config. Canonical shared config files currently live here:
  `project_config.json`, `pc_groups.json`, and `practice_display.csv`.
- `source/analysis/`
  Analysis pipelines and their local helpers. `source/analysis/analyze_forest/helpers.R`
  is the current analysis-local helper hub for forest follow-on analysis.
- `source/derived/`
  Derived-data pipelines and their local helpers. `source/derived/org_outcomes_practices/helpers.py`
  is the main derived-local helper hub.
- `drive/output/`
  Large data artifacts.
- `output/`
  Smaller outputs, logs, and review artifacts.

## Environment

- Always activate the conda environment before running scripts or SCons:

```bash
source activate org_resilience
```

- Python entrypoints assume `PYTHONPATH=.`:

```bash
export PYTHONPATH=.
```

## Helper Model

- **Script-local**: logic used by one script only.
- **Subsystem-local**: shared inside one subsystem such as `source/analysis/analyze_forest` or `source/derived/org_outcomes_practices`.
- **Section-local**: shared across multiple subsystems inside one top-level section.
- **Repo-shared**: shared across top-level sections; belongs in `source/lib`.

Promotion rule:
- one script: keep local
- multiple scripts in one subsystem: subsystem-local helper
- multiple subsystems in one section: section-local helper is allowed
- multiple top-level sections: move to `source/lib`

## SCons Conventions

- Keep the current explicit builder system: `env.Python(target, source)` and `env.R(target, source)`.
- In every `SConscript`, keep runtime code dependencies explicit:
  - script file
  - helper deps
  - shared config deps
  - data deps
- If a build script directly imports a repo-local Python helper or directly `source()`s an R helper, add that file to `source=[...]`.
- Prefer small shared dependency lists near the top of a `SConscript` over ad hoc repeated arrays.

