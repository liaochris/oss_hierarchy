# GitHub Issue Classification and Reach Estimation Prompt

## Context

You are analyzing GitHub issues to understand:
1. How issues are resolved (or not resolved)
2. What kinds of frictions they represent

You are given:
- A CSV file representing the full issue timeline (chronological)

You must not infer facts that are not supported by the inputs.

---

## Input Specification

### Issue Timeline CSV
Each row represents an event in the issue discussion.

Columns:
- author
- type (IssueOpened, IssueComment, ClosedEvent, LabeledEvent, etc.)
- text
- url
- date

The **first row** is always the original issue post.

## Core Issue Classification 

### Step 1: Resolution Found
Determine whether the issue was substantively resolved.

- Yes: The original problem was answered, fixed, or definitively addressed
- No: The issue ended without resolution


### Step 2: Resolution Mechanism (if resolved)
If resolved, classify the mechanism:
- Code change (commit or pull request)
- Discussion-only (explanation, configuration guidance)

Code changes take precedence over discussion if both are present. 


### Step 3: Resolution Artifact (if resolved)
Extract the specific artifact that resolved the issue:
- Commit
- Pull request
- Issue comment

For each artifact, provide:
- Type
- URL (exact string from input)
- Verbatim solution text (commit message, PR title, or comment text)


### Step 4: Non-Resolution Reason (if unresolved)
If unresolved, classify why:
- No responder
- Asker ghosted
- Responder ghosted
- Resolution impossible or explicitly out of scope

Do not count automated bot replies, replies about state issues or “I have this too” as substantive responses.



## Output Format

Return a single JSON object with the following top-level fields:

- resolution_found
- resolution_mechanism
- resolution_artifacts
- non_resolution_reason

