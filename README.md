# OSS Hierarchy

This repository contains the data pipelines, analysis code, paper source, and
supporting documentation for a research project on organizational hierarchy,
contributor behavior, and resilience in open-source software projects.

The project is organized around reproducible SCons pipelines. Source scripts
live under `source/`, generated artifacts live under `output/` or
`drive/output/`, and project-specific setup instructions live in
[`SETUP.md`](SETUP.md).

## Repository Structure

- `source/lib/`
  Shared helpers and shared configuration. Canonical shared config files
  currently live here, including `project_config.json`, `pc_groups.json`, and
  `practice_display.csv`.
- `source/derived/`
  Derived-data pipelines and local helpers. The main derived-local helper hub is
  `source/derived/org_outcomes_practices/helpers.py`.
- `source/analysis/`
  Analysis pipelines and local helpers. `source/analysis/analyze_forest/helpers.R`
  is the current analysis-local helper hub for forest follow-on analysis.
- `source/paper/`
  Paper source, SCons rules for paper builds, and related TeX inputs.
- `drive/output/`
  Large output artifacts that are too large or too shared-workflow-specific for
  normal source control.
- `output/`
  Smaller generated outputs, logs, and review artifacts.
  
## Workflow Essentials

Before running scripts or SCons, activate the project environment:

```bash
source activate org_resilience
export PYTHONPATH=.
```

Use SCons from the repository root to build outputs:

```bash
scons
```

To build one target and its dependencies:

```bash
scons path/to/target
```

See [`SETUP.md`](SETUP.md) for prerequisites, dependency installation, SCons
details, and executable configuration.

## Development Conventions

Helpers should live at the narrowest level where they are genuinely shared:

- script-local logic stays in the script that uses it;
- subsystem-local helpers live inside one subsystem, such as
  `source/analysis/analyze_forest`;
- section-local helpers can be shared across multiple subsystems inside one
  top-level section;
- repo-shared helpers used across top-level sections belong in `source/lib`.

Promotion rule:

- one script: keep the helper local;
- multiple scripts in one subsystem: use a subsystem-local helper;
- multiple subsystems in one section: use a section-local helper if helpful;
- multiple top-level sections: move the helper to `source/lib`.

SCons files should keep runtime dependencies explicit. If a build script imports
a repo-local Python helper or `source()`s an R helper, add that helper to the
target's `source=[...]` list. Prefer small shared dependency lists near the top
of each `SConscript` over repeated ad hoc arrays.

## Important Docs

- [`SETUP.md`](SETUP.md) - environment, dependencies, and SCons usage.
- [`DATA_APPENDIX.MD`](DATA_APPENDIX.MD) - action-level data construction
  documentation.
- [`HANDOFF.md`](HANDOFF.md) - current handoff notes and implementation context.
- [`oop_event_study_forest.md`](oop_event_study_forest.md) - event-study forest
  OOP/package refactor todo list.
