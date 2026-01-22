# GitHub Issue Classification and Reach Estimation Prompt

## Context

You are analyzing GitHub issues to understand:
1. What kinds of frictions they represent
2. How many downstream users could plausibly be affected

You are given:
- A CSV file representing the full issue timeline (chronological)
- Optionally, a git diff corresponding to a commit or pull request

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


### Code Diff (Optional)
A unified diff for a commit or pull request, if one exists.

## Core Issue Classification 

## Part 2: Technical and Reach-Relevant Classification (Categories 7–10)

### Step 1: Backwards-Incompatible Change
Does the issue describe something that used to work but no longer does?

- Yes – internal change (this project or same organization)
- Yes – external dependency change
- No

Do not classify user misuse as backwards incompatibility.

### Step 2: Ground Truth: Software Broken
Assuming correct usage and unchanged code, would the software still fail?

- Yes: The software itself is broken
- No: The issue arises from incorrect usage or configuration


### Step 3: Feature Solution Status 
If the issue proposes a feature or enhancement:
- Implemented
- Not implemented
- Not applicable

Consider similar implementations as implemented.


### Step 4: Reach Definition (Search Engine Specification)

You must define how to identify downstream projects that could be affected by this issue.

Provide answers to three fields for this step:
1. **File path regex** (to identify relevant files)
2. **Within-file regex** (to identify relevant usage)
3. **Conditions** (OS, hardware, optional arguments, etc.)

Guidelines:
- Regex must be precise (avoid partial or ambiguous matches)
- Prefer exact function calls, parameters, or imports
- If only certain platforms are affected, state this explicitly, and format it the text as f"{PLATFORM(s)} only" 
- If the hardware condition can be determined within the code (ie: if someone is using a GPU and calls `.cuda()`), this should be included in **Within-file regex**
- If no filtering is needed for a particular field (for example, if the bug applies to any matching code, irrespective of other conditions such as OS or hardware) then the output for that field should be "**"

Do NOT estimate counts. Only define how affected code would be detected.

---

Each field must be filled or set to null where not applicable.


## Output Format

Return a single JSON object with the following top-level fields:

- backwards_incompatible
- software_broken
- feature_solution_status
- reach_specification, which is a nested json with three keys:  **File path regex**, **Within-file regex** and **Conditions** 



