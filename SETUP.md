# Setup

This document describes how to prepare the local environment and run the SCons
build system for this project.

## Prerequisites

- Python, preferably through
  [Anaconda](https://www.anaconda.com/products/individual).
  - Python is used to run [SCons](https://scons.org).
  - Custom SCons builders are also written in Python.
- [git](https://git-scm.com/downloads) for version control.
- [git-lfs](https://git-lfs.github.com/) for large tracked files when the
  project uses them.
- [R](https://www.r-project.org/) for R analysis pipelines.
- [LyX](https://www.lyx.org/Download) and a working TeX installation for paper
  builds that require them.

Some older or auxiliary pipelines may also require specialized tools such as
Stata or Matlab. Check the relevant `SConscript` and script source before
running those targets.

## Environment

Always activate the conda environment before running scripts or SCons:

```bash
source activate org_resilience
```

Python entrypoints assume the repository root is on `PYTHONPATH`:

```bash
export PYTHONPATH=.
```

## Dependencies

Install Python dependencies with:

```bash
python -m pip install -r source/lib/requirements.txt
```

Install R dependencies with:

```bash
Rscript source/lib/requirements.r
```

## Data and Output Locations

- `source/` contains source scripts and small source-controlled inputs.
- `output/` contains generated outputs, logs, and review artifacts.
- `drive/output/` contains large generated artifacts.
- `temp/` may be used for temporary or intermediate files and is not expected to
  be under version control.

Generated output directories should generally mirror the relevant `source/`
subtree. For example, code in `source/analysis/foo/` should write to a matching
location under `output/analysis/foo/` or `drive/output/analysis/foo/`.

When linking or copying large shared data, work from a local offline copy rather
than a live synced directory whenever there is any risk of overwriting shared
artifacts.

## Running SCons

Compile the default project targets from the repository root:

```bash
scons
```

Build a specific target and all dependencies needed for it:

```bash
scons path/to/target
```

SCons uses custom builders defined through the project template infrastructure.
The common builder pattern is:

```python
target = ["#output/derived/example/result.csv"]
source = [
    "#source/derived/example/build_result.py",
    "#source/derived/example/helpers.py",
    "#source/lib/project_config.json",
]
env.Python(target, source)
```

The script should be the first item in `source`. Add every runtime code
dependency that the script imports or sources, along with shared config and data
dependencies that affect the target.

## SCons Conventions

- Keep the current explicit builder system: `env.Python(target, source)` and
  `env.R(target, source)`.
- In every `SConscript`, keep runtime code dependencies explicit:
  - script file;
  - helper dependencies;
  - shared config dependencies;
  - data dependencies.
- If a build script directly imports a repo-local Python helper or directly
  `source()`s an R helper, add that file to `source=[...]`.
- Prefer small shared dependency lists near the top of a `SConscript` over ad
  hoc repeated arrays.

## Executables

SCons program executable names are configured by the JMSLab builder
infrastructure. Default executable names live in:

```text
source/lib/JMSLab/builders/executables.yml
```

To use a custom executable name or path, define an environment variable named
`JMSLAB_EXE_PROGRAM`. For example, on Windows:

```bat
SET JMSLAB_EXE_STATA=StataSE.exe
SET JMSLAB_EXE_STATA=C:\Program Files (x86)\Stata16\StataSE.exe
```

## Helper Placement

Helpers should live at the narrowest useful sharing level:

- script-local: logic used by one script only;
- subsystem-local: shared inside one subsystem, such as
  `source/analysis/analyze_forest` or
  `source/derived/org_outcomes_practices`;
- section-local: shared across multiple subsystems inside one top-level section;
- repo-shared: shared across top-level sections and stored in `source/lib`.

Promotion rule:

- one script: keep local;
- multiple scripts in one subsystem: use a subsystem-local helper;
- multiple subsystems in one section: use a section-local helper if useful;
- multiple top-level sections: move to `source/lib`.
