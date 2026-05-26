# GitHub Issue Reach Estimation Prompt

## Context

You are analyzing GitHub issues to estimate:
**How many downstream users could plausibly be affected by this issue**

This prompt focuses on **reach estimation through regex-based code search patterns**.

You are given:

* A CSV file representing the full issue timeline (chronological)
* The issue classification from the previous phase (issue_type, resolution info, etc.)
* Optionally, a git diff corresponding to a commit or pull request

You must not infer facts that are not supported by the inputs.

---

## Input Specification

### Issue Timeline CSV

Each row represents an event in the issue discussion.

Columns:
- type (IssueTitle, IssueOpened, IssueComment, ClosedEvent, LabeledEvent, MentionedByPR, etc.)
- author
- date
- text
- url
- repo_name
- issue_number

The **first row** (after the header) is always `IssueTitle` containing the issue title.
The **second row** is always `IssueOpened` containing the original issue post text.

**CSV Format:** Rows are newline-separated; everything else is in raw CSV formatting (i.e., `\n` within quoted fields appears as literal `\n`, not an actual line break).

---

### Code Diff (Optional)

A unified diff for a commit or pull request, if one exists.

If you receive the message:

```
No code diff available for this issue
```

* Classify based on the CSV timeline alone, as no code changes are available to analyze.

---

### Edge Cases

* For reopened issues, focus on the final resolution state
* If the CSV is malformed or information is missing, extract what you can and use `null` where needed

---

## Reach Estimation

### Critical: Regex Specificity Guidelines

Your regex patterns should capture projects that would **DEFINITELY** be affected by this issue if no code change was made.

**Be precise about the scope of the issue:**

1. **Function-level specificity:**
   - If the **entire function** is broken, match any usage of that function
   - If only **certain parameters** cause the issue, match patterns that include those parameters
   - Example: If `foo(x=None)` breaks but `foo(x=1)` works, prefer `foo\([^)]*None` over just `foo\(`

   **Important: Parameters may be defined before the function call**
   - Users often assign values to variables before passing them to functions
   - Example: `var = None; foo(x=var)` should also be captured when `foo(x=None)` breaks
   - Consider patterns that match:
     - Direct parameter values: `foo\([^)]*None`
     - Variable assignments that could contain the problematic value: `=\s*None.*foo\(` (same file context)
   - If the breaking value is commonly assigned to a variable first, you may need multiple patterns or broader matching
   - When the parameter value is ambiguous (could be any variable), prefer matching the function usage broadly

2. **Configuration-level specificity:**
   - If the issue only affects certain configurations, include patterns that detect those configs
   - Example: If only `training=True` causes the bug, match `training\s*=\s*True`

3. **Version/environment specificity:**
   - For CUDA issues, match `.cuda()` patterns
   - For async issues, match `async` or `await` patterns
   - For specific Python version issues, note in conditions

4. **Bias toward precision over recall:**
   - It is better to capture projects that are DEFINITELY affected
   - Overly broad patterns create false positives
   - When in doubt, add specificity
   - However, if a parameter could realistically be assigned to a variable first, consider whether broader matching is warranted

**Examples of good specificity:**
- Issue: `batch_norm` fails only when `running_var=None`
  - Good: `batch_norm\([^)]*running_var\s*=\s*None`
  - Also good: Match both direct `None` and variable patterns
  - Bad: Just `batch_norm` (too broad)

- Issue: `DocType` equality check broken (missing `__ne__`)
  - Good: `DocType` + any `!=` comparison in same file
  - Acceptable: Just `DocType` (all users affected when comparing)

- Issue: Function fails when parameter is empty list `[]`
  - Consider: Users often do `my_list = []; func(my_list)`
  - May need to match `func\(` broadly if the problematic value is commonly passed via variable

---

### Step 1: Reach Definition (Search Specification)

**Populates:** `reach_specification`

Define **how to detect downstream projects that could be affected by this issue using code search**.

You are **not** estimating how many projects are affected.
You are specifying **the search conditions under which a project would be considered affected**.

Assume you have a search engine that can determine whether a project contained a given piece of code or text on a given day.

Use **detectable code patterns** wherever possible.
Use **explicit system conditions only when system dependence is *not reliably detectable from code***.

---

### Reach Matching Rule

A downstream project is considered affected if **all applicable criteria below are satisfied**:

1. The software corresponding to this issue is used
2. The specific command / API / feature involved in the issue is used (if applicable)
3. The project runs in an affected system environment (if applicable)

Each criterion corresponds to one component of `reach_specification`.

---

## Reach Specification Structure (Authoritative)

```json
"reach_specification": {
  "software_filtering_regex": {
    "file_within_pairs": [
      { "file_path_regex": "...", "within_file_regex": "..." }
    ]
  },
  "command_filtering_regex": {
    "file_within_pairs": [
      { "file_path_regex": "...", "within_file_regex": "..." }
    ]
  },
  "system_filtering_regex_or_condition": {
    "file_within_pairs": [
      { "file_path_regex": "...", "within_file_regex": "..." }
    ],
    "conditions": "..."
  }
}
```

---

## Definitions (Binding)

### `file_within_pairs`

A list (possibly empty) of **atomic detection rules**. Each of the **atomic detection rules** are combined using OR and an **atomic detection rules** is true of both `file_path_regex` and `within_file_regex` are both true. 

Each rule matches if **both**:

* `file_path_regex` matches a file path (e.g., `.*\\.py$`, `requirements\\.txt`, `Dockerfile`, `\\.github/workflows/.*\\.ya?ml$`)
* `within_file_regex` matches text **inside that file** (e.g., imports, dependency entries, function calls, CLI invocations)

---

### `conditions`

* Exists **only** for system-level constraints
* Describes, in plain text, the **subset of user systems required for users to be affected**
* MUST NOT be used for software-level or command-level filtering

**Format:**

* `"**"` — No system constraint; issue affects all systems (default)
* `"{system} only"` — Issue only affects users on that system

**Valid examples:**

* `"**"` (all systems)
* `"windows only"`
* `"linux only"`
* `"macos only"`
* `"arm64 only"`
* `"python 2 only"`

Even if repository code contains system checks, you **cannot** infer the user's actual runtime environment from code alone.
If system dependence is not reliably and commonly inferable from code (e.g., operating system), do **not** attempt code-based filtering and use a textual condition instead.
CUDA-style hardware dependence **is** an example of system dependence that is commonly and reliably detectable from code.

---

## How to Populate Each Component

### 4.1 Software Filtering Regex (REQUIRED)

**Purpose:** Detect whether a project uses this software at all.

Populate:

* `file_within_pairs` with one or more rules matching imports, dependency declarations, or install commands

Rules:

* This component is **always required**
* `file_within_pairs` must be **non-empty**
* Include package names *and* import/module names (including aliases)
* Bias toward broad recall
* Do **not** reference specific commands or APIs
* Do **not** include `conditions`

---

### 4.2 Command Filtering Regex (ONLY IF ISSUE IS COMMAND / API SPECIFIC)

**Purpose:** Detect usage of a specific command, API, or feature implicated in the issue.

Populate:

* If the issue affects the software in general: set `file_within_pairs` to `[]`
* If the issue is command/API specific: populate `file_within_pairs` with precise detection rules

Rules:

* Match semantic identifiers (function names, decorators, CLI commands, flags)
* Do **not** include general software imports
* Do **not** include `conditions`

---

### 4.3 System Filtering Regex or Condition (ONLY IF ISSUE IS SYSTEM-SPECIFIC)

**Purpose:** Restrict matches to projects affected only under certain system conditions.

If the issue is **not** system-specific:

* Set `file_within_pairs` to `[]`
* Set `conditions` to `"**"`

If the issue **is** system-specific, choose **exactly one**:

#### A. System Dependence Reliably Detectable From Code

Use this **only if**:

* The dependence is fundamental (not defensive or optional)
* Standard, common, explicit code patterns exist
* Those patterns reliably imply affected users (e.g., CUDA usage)

Then:

* Populate `file_within_pairs`
* Set `conditions` to `"**"`

#### B. System Dependence NOT Reliably Detectable From Code

Use this when:

* Repository code does not reliably imply the user’s runtime system
* The dependence is conceptual or environmental (e.g., operating system)

Then:

* Set `file_within_pairs` to `[]`
* Set `conditions` to a short plain-text requirement, framed as `"{system} only"` (e.g., `"windows only"`)

---

### Validation Rules (Hard Constraints)

Before producing output, ensure:

* `software_filtering_regex.file_within_pairs` is non-empty
* `command_filtering_regex.file_within_pairs` exists (may be empty)
* `conditions` appears **only** in `system_filtering_regex_or_condition`
* If `conditions` is not `"**"`, then `file_within_pairs` must be empty
* All three top-level components exist

---

### Complete Examples with CSV Input

These examples show complete CSV inputs and the expected classification output.

**Note:** All examples include all rows from the CSV except LabeledEvent, AssignedEvent, and UnassignedEvent rows, which have been omitted for clarity. Rows are newline-separated; `\n` within quoted fields appears as literal `\n`.

---

**Example 1: Bug with specific API** (elasticsearch-dsl-py #373)

CSV Input:
```
type,author,date,text,url,repo_name,issue_number
IssueTitle,0x64746b,2016-03-21T17:19:22Z,Equality check on documents behaves unexpectedly,https://github.com/elastic/elasticsearch-dsl-py/issues/373,elastic/elasticsearch-dsl-py,373
IssueOpened,0x64746b,2016-03-21T17:19:22Z,"```\nIn [1]: from elasticsearch_dsl import DocType, String\n\nIn [2]: class Author(DocType):\n    name = String()\n   ...:     \n\nIn [3]: a = Author(name='George Orwell')\n\nIn [4]: b = Author(name='George Orwell')\n\nIn [5]: a == b\nOut[5]: True\n\nIn [6]: a != b\nOut[6]: True\n```\n\nAm I holding it wrong?  \ndtk",https://github.com/elastic/elasticsearch-dsl-py/issues/373,elastic/elasticsearch-dsl-py,373
IssueComment,honzakral,2016-03-21T17:23:59Z,"No, you are not - it's just that we have defined `__eq__` but not `__ne__`, my bad.",,elastic/elasticsearch-dsl-py,373
ClosedEvent,honzakral,2016-03-21T17:45:11Z,Closed by honzakral in a903a1,https://github.com/elastic/elasticsearch-dsl-py/commit/b97ed033e7676069ff8f933ae433246e8aa903a1,elastic/elasticsearch-dsl-py,373
```

Expected Output (raw JSON, no code blocks):

{"reach_specification":{"software_filtering_regex":{"file_within_pairs":[{"file_path_regex":".*\\.py$","within_file_regex":"(from elasticsearch_dsl|import elasticsearch_dsl)"},{"file_path_regex":"requirements.*\\.txt$","within_file_regex":"elasticsearch-dsl"}]},"command_filtering_regex":{"file_within_pairs":[{"file_path_regex":".*\\.py$","within_file_regex":"DocType"}]},"system_filtering_regex_or_condition":{"file_within_pairs":[],"conditions":"**"}}}

---

**Example 2: CPU/GPU inconsistency bug** (pytorch #65520)

CSV Input:
```
type,author,date,text,url,repo_name,issue_number
IssueTitle,ilovepytorch,2021-09-23T05:41:38Z,Improve input checking for running_var of nn.functional.batch_norm on CPU/GPU,https://github.com/pytorch/pytorch/issues/65520,pytorch/pytorch,65520
IssueOpened,ilovepytorch,2021-09-23T05:41:38Z,"## 🐛 Bug\n\nAll the input parameters for the `torch.nn.functional.batch_norm` API are the same, but their behaviors on GPU and CPU are different. CPU runs successfully silently while GPU throws an internal assertion failure. I guess it is because the input validation of `running_var` of `batch_norm`(<https://pytorch.org/docs/stable/generated/torch.nn.functional.batch_norm.html>) is not consistent on CPU and GPU backends.\n\n## To Reproduce\n\n```\nimport torch\ndef cpu():\n  input = torch.rand(1, 64, 16, 16, dtype=torch.float32)\n  running_mean = torch.rand(64, dtype=torch.float32)\n  running_var = None\n  weight = torch.rand(64, dtype=torch.float32)\n  res = torch.nn.functional.batch_norm(input,running_mean,running_var,weight,training=True)\ndef gpu():\n  input = torch.rand(1, 64, 16, 16, dtype=torch.float32).cuda()\n  running_mean = torch.rand(64, dtype=torch.float32).cuda()\n  running_var = None\n  weight = torch.rand(64, dtype=torch.float32).cuda()\n  res = torch.nn.functional.batch_norm(input,running_mean,running_var,weight,training=True)\ncpu()\nprint(""CPU pass"")\ngpu()\n```\n\n## Expected behavior\n\nOn CPU, the script silently runs without throwing any warning message. I think it should raise some exception like invalid argument instead.\n\n## Environment\n\n* PyTorch Version: 1.9.0\n* OS: Ubuntu\n* CUDA/cuDNN version: cuDNN 11.1\n* GPU: RTX3090",https://github.com/pytorch/pytorch/issues/65520,pytorch/pytorch,65520
```

Expected Output (raw JSON, no code blocks):

{"reach_specification":{"software_filtering_regex":{"file_within_pairs":[{"file_path_regex":".*\\.py$","within_file_regex":"import torch"},{"file_path_regex":"requirements.*\\.txt$","within_file_regex":"torch"}]},"command_filtering_regex":{"file_within_pairs":[{"file_path_regex":".*\\.py$","within_file_regex":"batch_norm"}]},"system_filtering_regex_or_condition":{"file_within_pairs":[{"file_path_regex":".*\\.py$","within_file_regex":"\\.cuda\\(\\)"}],"conditions":"**"}}}

---

**Example 3: Question resolved by user** (mutagen #355)

CSV Input:
```
type,author,date,text,url,repo_name,issue_number
IssueTitle,bll123,2018-10-13T16:59:08Z,mid3v2 command line : colon in tag data,https://github.com/quodlibet/mutagen/issues/355,quodlibet/mutagen,355
IssueOpened,bll123,2018-10-13T16:59:08Z,"The tag's : character is getting re-mapped and quotes are added.  \nIs this what it is supposed to do?  \nI think I recall the : character being somewhat special.\n\n```\nbll-tecra:bll$ mid3v2 --UFID 'http://example.com=abc-def' waltz.mp3\nbll-tecra:bll$ mutagen-inspect waltz.mp3\n-- waltz.mp3\n- MPEG 1 layer 3, 128000 bps (CBR, LAME 3.99.1+, -b 128), 44100 Hz, 2 chn, 20.85 seconds (audio/mp3)\nUFID=http='//example.com=abc-def'\n```",https://github.com/quodlibet/mutagen/issues/355,quodlibet/mutagen,355
IssueComment,bll123,2018-10-13T20:48:49Z,"This is what MusicBrainz Picard puts in:\n\n`UFID=http://musicbrainz.org='2409871f-6cde-42a1-a017-722b86f87e68'`",,quodlibet/mutagen,355
IssueComment,bll123,2018-10-13T21:07:22Z,"Looks like this is the proper way to do it:\n\n`mid3v2 -e --delete-frames=UFID --UFID ""http\://example.com:abc-def"" test.mp3`",,quodlibet/mutagen,355
ClosedEvent,bll123,2018-10-13T21:07:22Z,Closed by bll123 in ,,quodlibet/mutagen,355
```

Expected Output (raw JSON, no code blocks):

{"reach_specification":{"software_filtering_regex":{"file_within_pairs":[{"file_path_regex":".*\\.py$","within_file_regex":"(import mutagen|from mutagen)"},{"file_path_regex":"requirements.*\\.txt$","within_file_regex":"mutagen"}]},"command_filtering_regex":{"file_within_pairs":[{"file_path_regex":".*","within_file_regex":"mid3v2"}]},"system_filtering_regex_or_condition":{"file_within_pairs":[],"conditions":"**"}}}

---

**Example 4: Windows-specific bug** (scikit-video #21)

CSV Input (showing key rows):
```
type,author,date,text,url,repo_name,issue_number
IssueTitle,grovduck,2017-02-10T22:59:21Z,Windows executable path support,https://github.com/scikit-video/scikit-video/issues/21,scikit-video/scikit-video,21
IssueOpened,grovduck,2017-02-10T22:59:21Z,"Great package - it's very useful for my research. Unfortunately, I'm using Windows and initially had problems with skvideo bonking when calling 'ffmpeg' and associated executables. I did an ugly hack like this:\n\n```\nif os.name == ""nt"":\n    ffprobe_exe += "".exe""\n    ffmpeg_exe += "".exe""\n```\n\nI'd be happy to write a PR, but thought you might have an idea of how you wanted to implement this.",,scikit-video/scikit-video,21
IssueComment,beyondmetis,2017-03-05T22:31:43.000Z,"Windows should be supported now, based on the information you provided. Let me know if you have problems.",,scikit-video/scikit-video,21
IssueComment,grovduck,2017-03-08T19:07:37.000Z,"I just tested against 87089a9 and it works like a charm.",,scikit-video/scikit-video,21
ClosedEvent,beyondmetis,2018-04-22T16:21:41.000Z,beyondmetis closed this as completed,,scikit-video/scikit-video,21
```

Expected Output (raw JSON, no code blocks):

{"reach_specification":{"software_filtering_regex":{"file_within_pairs":[{"file_path_regex":".*\\.py$","within_file_regex":"(import skvideo|from skvideo)"},{"file_path_regex":"requirements.*\\.txt$","within_file_regex":"(scikit-video|sk-video)"}]},"command_filtering_regex":{"file_within_pairs":[]},"system_filtering_regex_or_condition":{"file_within_pairs":[],"conditions":"windows only"}}}

---

## Output Format

Return a single JSON object with exactly this structure:

```json
{
  "reach_specification": {
    "software_filtering_regex": {
      "file_within_pairs": [{ "file_path_regex": "string", "within_file_regex": "string" }]
    },
    "command_filtering_regex": {
      "file_within_pairs": [{ "file_path_regex": "string", "within_file_regex": "string" }]
    },
    "system_filtering_regex_or_condition": {
      "file_within_pairs": [{ "file_path_regex": "string", "within_file_regex": "string" }],
      "conditions": "string"
    }
  }
}
```

### Output Requirements

Your response must be a single line of valid JSON with **NO additional formatting**.

**Required format:**

* Start with `{` and end with `}`
* Do NOT add markdown code blocks
* Do NOT add backticks
* Do NOT add explanations, comments, or any text outside the JSON object
* Do NOT add newlines or pretty-printing — output compact JSON on a single line

**String values:**

* Use exact strings as specified (e.g., `"**"`, `"windows only"`)
* For null values, use JSON `null` (not the string `"null"`)

**Example of correct format:**
`{"reach_specification":{"software_filtering_regex":{"file_within_pairs":[{"file_path_regex":".*\\.py$","within_file_regex":"import example"}]},"command_filtering_regex":{"file_within_pairs":[]},"system_filtering_regex_or_condition":{"file_within_pairs":[],"conditions":"**"}}}`
