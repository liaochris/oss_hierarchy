# CLAUDE.md

## Reference Docs
- `PROJECT.md` — project structure, environment setup, helper model, SCons conventions
- `DATA_APPENDIX.md` — action-level data construction documentation
- `/code-review` skill — apply after runing large chunks of code to ensure code follows coding conventions (naming, SaveData, SConscript deps, helper placement)

## Workflow
- Always activate `source activate org_resilience && export PYTHONPATH=.` before running python code. 
- Whenever using bash commands, always use "" around the file directories. 
- For multi-file changes, plan first and get approval before writing code. Changes cascade through SCons dependencies.
- When the user points to existing code as a model, match its patterns exactly.
- When the user says "yes" or "do it" — execute without re-summarizing the plan.
