# GitHub Issue Technical Classification and Reach Estimation Prompt

## Context

You are analyzing GitHub issues to understand:
1. What kinds of frictions they represent
2. How many downstream users could plausibly be affected

This prompt focuses on **technical classification and reach estimation**. 

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

**If you receive the message "No code diff available for this issue"**, classify based on the CSV timeline alone. Infer feature_solution_status from discussion evidence rather than code changes.

### Edge Cases
- If the CSV is malformed, extract what information you can
- If the issue is reopened multiple times, focus on the final state
- If you cannot determine a field value with confidence, use `null`

---

## Technical and Reach-Relevant Classification

### Step 1: Backwards-Incompatible Change
Does the issue describe something that used to work but no longer does?

Use exactly one of these strings:
- `"yes_internal"`: Yes – internal change (this project or same organization)
- `"yes_external"`: Yes – external dependency change
- `"no"`: No backwards incompatibility

**Important**: Do not classify user misuse as backwards incompatibility.

### Step 2: Ground Truth: Software Broken
Assuming correct usage and unchanged code, would the software still fail?

Use exactly one of these lowercase strings:
- `"yes"`: The software itself is broken
- `"no"`: The issue arises from incorrect usage or configuration


### Step 3: Feature Solution Status
If the issue proposes a feature or enhancement:

Use exactly one of these strings:
- `"implemented"`: The feature was implemented (or a similar implementation was added)
- `"not_implemented"`: The feature was not implemented
- `"not_applicable"`: This is not a feature/enhancement issue (e.g., it's a bug or question)


### Step 4: Reach Definition (Search Specification)

You must define **how to identify downstream projects that could be affected by this issue**.

This step specifies **how affected code would be detected**, not how many users are affected. You are creating a search specification that could be used to find affected code.

#### Core Concept

By definition, for a project to be affected by an issue in a software project, it must:
1. **Use the software** (detected via imports, dependencies, or command invocations)
2. **Use the affected component/command** (if the issue is specific to certain APIs or CLI commands)
3. **Run on the affected system** (if the issue is platform/hardware-specific)

There are three primary ways downstream code uses this software:
- Imports and uses it in `.py` scripts or other code files
- Calls it within scripts (bash, bat, shell scripts, etc.)
- Lists it in dependency/installation files (requirements.txt, setup.py, package.json, etc.)

#### Output Structure

The `reach_specification` field is a nested JSON object with **exactly three top-level keys**:

```json
"reach_specification": {
  "software_filtering_regex": {
    "file_path_regex": "string or **",
    "within_file_regex": "string or **",
    "conditions": "string or **"
  },
  "command_filtering_regex": {
    "file_path_regex": "string or **",
    "within_file_regex": "string or **",
    "conditions": "string or **"
  },
  "system_filtering_regex_or_condition": {
    "file_path_regex": "string or **",
    "within_file_regex": "string or **",
    "conditions": "string or **"
  }
}
```

#### Filter Definitions

**1. software_filtering_regex** *(always required)*
   - **Purpose**: Identifies whether downstream code uses this software at all
   - **file_path_regex**: Regex matching file paths where software usage might appear (e.g., `".*\\.(py|sh|txt)$"` for Python files, shell scripts, or requirements files)
   - **within_file_regex**: Regex matching import statements, dependency declarations, or package references
     - Combine multiple detection methods using OR logic: `(import torch|torch==|pip install torch)`
     - This should capture: imports in code, dependency file entries, command-line installations
   - **conditions**: Usually `"**"` (not applicable for basic software usage detection)

**2. command_filtering_regex** *(if issue affects specific CLI commands/APIs only)*
   - **Purpose**: Narrows down to code using specific commands, subcommands, flags, functions, or classes
   - **file_path_regex**: Where command usage would appear (code files, shell scripts, etc.)
   - **within_file_regex**: Specific CLI invocations, function calls, or API usage patterns
   - **conditions**: `"**"` unless there's a conceptual constraint
   - **If not applicable**: Set all three fields to `"**"`

**3. system_filtering_regex_or_condition** *(if issue is platform/hardware-specific only)*
   - **Two approaches**:

   **Approach A - Detectable in code:**
   - **file_path_regex**: Where platform-specific code appears
   - **within_file_regex**: Platform checks, hardware API calls (e.g., `\\.cuda\\(\\)|if.*Windows|platform\\.system`)
   - **conditions**: `"**"`

   **Approach B - Not reliably detectable in code:**
   - **file_path_regex**: `"**"`
   - **within_file_regex**: `"**"`
   - **conditions**: Explicit system requirement (e.g., `"Windows only"`, `"macOS and Linux only"`, `"CUDA GPU required"`)
   - **Note**: Use Approach B when the system requirement cannot be consistently detected in code. When conditions is set to anything other than `"**"`, it means code analysis alone is insufficient.

   - **If not applicable**: Set all three fields to `"**"`

#### Guidelines for Writing Regexes

* **Precision**: Regexes must be precise to avoid false positives. Avoid overly broad patterns.
* **No comment matching**: Ensure regexes don't match commented-out code
* **Relative paths**: Support both relative and absolute paths
* **Exact matches**: Prefer exact imports, function names with parameters/parentheses, specific flags
* **Wildcard notation**: Use exactly the string `"**"` (two asterisks) to indicate "not applicable" or "no filtering needed"
* **No prevalence estimation**: Do NOT estimate how many users are affected—only define how to detect them

#### Complete Examples

**Example 1: Import-based bug affecting specific function**
```json
"reach_specification": {
  "software_filtering_regex": {
    "file_path_regex": ".*\\.py$",
    "within_file_regex": "(import torch|from torch|torch==|requirements.*torch)",
    "conditions": "**"
  },
  "command_filtering_regex": {
    "file_path_regex": ".*\\.py$",
    "within_file_regex": "\\.leaky_relu\\(",
    "conditions": "**"
  },
  "system_filtering_regex_or_condition": {
    "file_path_regex": "**",
    "within_file_regex": "**",
    "conditions": "**"
  }
}
```

**Example 2: CLI command with specific flag, Windows-only**
```json
"reach_specification": {
  "software_filtering_regex": {
    "file_path_regex": ".*\\.(sh|bat|ps1|py)$",
    "within_file_regex": "(skvideo|scikit-video|pip install.*scikit-video)",
    "conditions": "**"
  },
  "command_filtering_regex": {
    "file_path_regex": ".*\\.(sh|bat|ps1|py)$",
    "within_file_regex": "skvideo.*ffmpeg",
    "conditions": "**"
  },
  "system_filtering_regex_or_condition": {
    "file_path_regex": "**",
    "within_file_regex": "**",
    "conditions": "Windows only"
  }
}
```

**Example 3: General bug affecting all users of the package**
```json
"reach_specification": {
  "software_filtering_regex": {
    "file_path_regex": ".*\\.(py|txt)$",
    "within_file_regex": "(import elasticsearch_dsl|from elasticsearch_dsl|elasticsearch-dsl-py==)",
    "conditions": "**"
  },
  "command_filtering_regex": {
    "file_path_regex": "**",
    "within_file_regex": "**",
    "conditions": "**"
  },
  "system_filtering_regex_or_condition": {
    "file_path_regex": "**",
    "within_file_regex": "**",
    "conditions": "**"
  }
}
``` 

## Output Format

Return a single JSON object with exactly these top-level fields:

```json
{
  "backwards_incompatible": "yes_internal" | "yes_external" | "no",
  "software_broken": "yes" | "no",
  "feature_solution_status": "implemented" | "not_implemented" | "not_applicable",
  "reach_specification": {
    "software_filtering_regex": {
      "file_path_regex": "string or **",
      "within_file_regex": "string or **",
      "conditions": "string or **"
    },
    "command_filtering_regex": {
      "file_path_regex": "string or **",
      "within_file_regex": "string or **",
      "conditions": "string or **"
    },
    "system_filtering_regex_or_condition": {
      "file_path_regex": "string or **",
      "within_file_regex": "string or **",
      "conditions": "string or **"
    }
  }
}
```

### Complete Example Outputs:

**Example 1: Platform-specific bug**
```json
{
  "backwards_incompatible": "no",
  "software_broken": "yes",
  "feature_solution_status": "not_applicable",
  "reach_specification": {
    "software_filtering_regex": {
      "file_path_regex": ".*\\.(py|txt)$",
      "within_file_regex": "(import librealsense|pyrealsense2|pip install.*pyrealsense)",
      "conditions": "**"
    },
    "command_filtering_regex": {
      "file_path_regex": ".*\\.py$",
      "within_file_regex": "rs\\.pipeline\\(\\)",
      "conditions": "**"
    },
    "system_filtering_regex_or_condition": {
      "file_path_regex": "**",
      "within_file_regex": "**",
      "conditions": "Intel RealSense D415 camera required"
    }
  }
}
```

**Example 2: General enhancement not implemented**
```json
{
  "backwards_incompatible": "no",
  "software_broken": "no",
  "feature_solution_status": "not_implemented",
  "reach_specification": {
    "software_filtering_regex": {
      "file_path_regex": ".*\\.(py|txt)$",
      "within_file_regex": "(import checkov|from checkov|checkov==)",
      "conditions": "**"
    },
    "command_filtering_regex": {
      "file_path_regex": "**",
      "within_file_regex": "**",
      "conditions": "**"
    },
    "system_filtering_regex_or_condition": {
      "file_path_regex": "**",
      "within_file_regex": "**",
      "conditions": "**"
    }
  }
}
```

### Requirements:
- Output MUST be valid JSON
- Do NOT wrap in markdown code blocks
- Do NOT include backticks (```)
- Do NOT include explanations, comments, or extra text outside the JSON
- Response must start with `{` and end with `}`
- Use exact string values as specified (lowercase, underscores for multi-word values)
- All three filter objects in reach_specification must be present with all three fields each
- Use exactly `"**"` (string with two asterisks) for non-applicable fields

