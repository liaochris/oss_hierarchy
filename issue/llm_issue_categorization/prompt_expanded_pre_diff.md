# GitHub Issue Classification Prompt

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
- type (IssueTitle, IssueOpened, IssueComment, ClosedEvent, LabeledEvent, MentionedByPR, etc.)
- author
- date
- text
- url
- repo_name
- issue_number

The **first row** (after the header) is always IssueTitle containing the issue title.
The **second row** is always IssueOpened containing the original issue post text.

### Edge Cases
- For reopened issues, focus on the final resolution state
- If CSV is malformed or information is missing, extract what you can and use null values where needed

---

## Core Issue Classification

### Step 1: Issue Type
Classify the issue type based on the original issue post (IssueOpened row, which is the second row after the header).

Use one of: `bug`, `enhancement`, `question`
- `bug`: Code is not working as the user intended
- `enhancement`: A feature request or improvement proposal
- `question`: Asking how to use a function or seeking clarification

### Step 2: Resolution Found
Classify whether the issue was substantively resolved.

Use one of: `yes`, `no`
- `yes`: The original problem was answered, fixed, or definitively addressed
- `no`: The issue ended without resolution


### Step 3: Resolution Type
If resolution_found is `yes`, classify the resolution mechanism. Otherwise use `null`.

Use one of: `code`, `discussion`, `null`
- `code`: Resolved via commit or pull request. Even if the specific commit/PR is not clearly referenced, use this if the discussion indicates code changes were made.
- `discussion`: Resolved via explanation, configuration guidance, or comment with no code changes needed
- `null`: If resolution_found is `no`

**Priority rule**: Code takes precedence. If code was written AND discussion occurred, classify as `code`. Use `discussion` only when the problem was resolved entirely through explanation without any code changes. 


### Step 4: Resolution Artifact (if resolved)
Extract the specific artifact that resolved the issue.

#### Decision Tree for Identifying Resolution:

1. **Auto-closed by PR/commit** (resolution_type = `code`):
   - Look for text like "Closed by @user in #123" or "Closed by @user in abc1234"
   - The closing PR/commit is always the resolution

2. **PR/commit mentioned but didn't auto-close** (resolution_type = `code`):
   - Look for surrounding comments that confirm it solved the problem
   - Example: "Fixed in #123" followed by manual close
   - The mentioned PR/commit is the resolution only if confirmed by discussion

3. **Resolved through discussion** (resolution_type = `discussion`):
   - Problem addressed via explanation or instructions with no code changes
   - The explaining comment is the resolution

4. **Cannot determine artifact**:
   - Thread appears resolved but no clear artifact is identifiable
   - Set resolution to `null` 

#### Output Structure:

When resolution_found is `yes`, provide a resolution object:
```json
"resolution": {
  "type": "pull_request" | "commit" | "comment",
  "url": "exact URL string from CSV",
  "solution": "verbatim text: PR title, commit message, or comment text"
}
```

When resolution_found is `no` OR cannot identify the artifact, set to `null`.


### Step 5: Resolution Inability
If resolution_found is `no`, classify why the issue was not resolved.

Use one of:
- `no_responder`: No one from the project responded substantively
- `asker_ghosted`: Asker stopped responding before resolution
- `responder_ghosted`: Responder(s) stopped engaging before resolution
- `impossible_or_out_of_scope`: Resolution explicitly stated as impossible or out of scope

If resolution_found is `yes`, set this field to `null`.

**Note**: Do not count automated bot replies (like this issue is stale and is being closed), "I have this too" comments, or state-change-only events as substantive responses.

---

## Output Format

Return a single JSON object with exactly these top-level fields:

```json
{
  "issue_type": "bug" | "enhancement" | "question",
  "resolution_found": "yes" | "no",
  "resolution_type": "code" | "discussion" | null,
  "resolution": {
    "type": "pull_request" | "commit" | "comment",
    "url": "string",
    "solution": "string"
  } | null,
  "resolution_inability": "no_responder" | "asker_ghosted" | "responder_ghosted" | "impossible_or_out_of_scope" | null
}
```

### Complete Examples with CSV Input

These examples show complete CSV inputs (multiple rows) and the expected classification output.

**Note:** All examples include all rows from the CSV except LabeledEvent, AssignedEvent, and UnassignedEvent rows, which have been omitted for clarity. The provided CSV's are newline separated by rows; everything else is in raw csv formatting. 

---

**Example 1: Bug resolved by code (commit)**

CSV Input:
```
type,author,date,text,url,repo_name,issue_number
IssueTitle,0x64746b,2016-03-21T17:19:22Z,Equality check on documents behaves unexpectedly,https://github.com/elastic/elasticsearch-dsl-py/issues/373,elastic/elasticsearch-dsl-py,373
IssueOpened,0x64746b,2016-03-21T17:19:22Z,"```\nIn [1]: from elasticsearch_dsl import DocType, String\n\nIn [2]: class Author(DocType):\n    name = String()\n   ...:     \n\nIn [3]: a = Author(name='George Orwell')\n\nIn [4]: b = Author(name='George Orwell')\n\nIn [5]: a == b\nOut[5]: True\n\nIn [6]: a != b\nOut[6]: True\n```\n\nAm I holding it wrong?  \ndtk",https://github.com/elastic/elasticsearch-dsl-py/issues/373,elastic/elasticsearch-dsl-py,373
IssueComment,honzakral,2016-03-21T17:23:59Z,"No, you are not - it's just that we have defined `__eq__` but not `__ne__`, my bad.",,elastic/elasticsearch-dsl-py,373
ClosedEvent,honzakral,2016-03-21T17:45:11Z,Closed by honzakral in a903a1,https://github.com/elastic/elasticsearch-dsl-py/commit/b97ed033e7676069ff8f933ae433246e8aa903a1,elastic/elasticsearch-dsl-py,373
```

Expected Output (raw JSON, no code blocks):

{"issue_type":"bug","resolution_found":"yes","resolution_type":"code","resolution":{"type":"commit","url":"https://github.com/elastic/elasticsearch-dsl-py/commit/b97ed033e7676069ff8f933ae433246e8aa903a1","solution":"Closed by honzakral in a903a1"},"resolution_inability":null}

---

**Example 2: Enhancement resolved by code (commit)**

CSV Input:
```
type,author,date,text,url,repo_name,issue_number
IssueTitle,d-kleine,2022-08-21T12:45:53Z,"Leaky ReLU - ""negative slope"" naming issue",https://github.com/pytorch/pytorch/issues/83821,pytorch/pytorch,83821
IssueOpened,d-kleine,2022-08-21T12:45:53Z,"### 📚 The doc issue\n\n[PyTorch Leaky ReLU documentation](https://pytorch.org/docs/stable/generated/torch.nn.LeakyReLU.html#torch.nn.LeakyReLU)\n\nAs the author in [this](https://stackoverflow.com/questions/68867228/where-is-the-negative-slope-in-a-leakyrelu) post critizises, the naming ""negative slope"" for the parameter in Leaky ReLU is somewhat confusing as it is not a ""negative"" slope (in the narrow sense, see [here](https://image.shutterstock.com/image-vector/types-slope-negative-600w-1884989446.jpg)) but refers to the negative part (quadrant III in a [cartesian coordinate system](https://upload.wikimedia.org/wikipedia/commons/thumb/1/1a/Cartesian_coordinates_2D.svg/450px-Cartesian_coordinates_2D.svg.png) of the slope of the input activated by the Leaky ReLU function.\n\nFor comparison, in TensorFlow/Keras the parameter is called alpha:\n\n> alpha | Float >= 0. Negative slope coefficient. Default to 0.3.\n\n[TensorFlow Leaky ReLU documentation](https://www.tensorflow.org/api_docs/python/tf/keras/layers/LeakyReLU#args)\n\n### Suggest a potential alternative/fix\n\n* Adaption from TF: `alpha`\n* Update documentation with more precise information, that `alpha` refers to the negative part (quadrant III in a cartesian coordinate system) of the slope\n\ncc [@svekars](https://github.com/svekars) [@holly1238](https://github.com/holly1238) [@albanD](https://github.com/albanD) [@mruberry](https://github.com/mruberry) [@jbschlosser](https://github.com/jbschlosser) [@walterddr](https://github.com/walterddr) [@kshitij12345](https://github.com/kshitij12345) [@saketh-are](https://github.com/saketh-are)",https://github.com/pytorch/pytorch/issues/83821,pytorch/pytorch,83821
IssueComment,albanD,2022-08-23T14:31:56Z,"Changing the name would be quite challenging I'm afraid due to BC reasons.  \nBut we can update the doc to make it clearer for sure,",,pytorch/pytorch,83821
MentionedByPR,albanD,2023-02-03T23:46:00Z,Improve leaky relu doc,https://github.com/pytorch/pytorch/pull/94090,pytorch/pytorch,83821
ClosedEvent,pytorchmergebot,2023-02-14T17:58:58Z,Closed by pytorchmergebot in 30b207,https://github.com/pytorch/pytorch/commit/b7e1477e9b69a80114cbc992216cf57adf30b207,pytorch/pytorch,83821
```

Expected Output (raw JSON, no code blocks):

{"issue_type":"enhancement","resolution_found":"yes","resolution_type":"code","resolution":{"type":"commit","url":"https://github.com/pytorch/pytorch/commit/b7e1477e9b69a80114cbc992216cf57adf30b207","solution":"Closed by pytorchmergebot in 30b207"},"resolution_inability":null}

---

**Example 3: Question resolved by discussion**

CSV Input:
```
type,author,date,text,url,repo_name,issue_number
IssueTitle,heckad,2019-06-29T20:58:46Z,How to apply function to each element?,https://github.com/JulienPalard/Pipe/issues/42,JulienPalard/Pipe,42
IssueOpened,heckad,2019-06-29T20:58:46Z,"I want to convert elements to int. It's like `map(int, [1,2,3])`. How to do it?",https://github.com/JulienPalard/Pipe/issues/42,JulienPalard/Pipe,42
IssueComment,JulienPalard,2019-06-30T09:52:13Z,"We're using the name `select` for this:\n\n```\n>>> ""123"" | select(int) | add\n6\n```\n\nI choose `select` for its similarity with SQL select, and I were having a where:\n\n```\n>>> range(10) | where(lambda x: x % 2 == 0) | select(lambda x: x ** 2) | concat\n'0, 4, 16, 36, 64'\n```\n\nbeing the equivalent of:\n\n```\n>>> [x ** 2 for x in range(10) if x % 2 == 0]\n[0, 4, 16, 36, 64]\n```",,JulienPalard/Pipe,42
ClosedEvent,JulienPalard,2019-06-30T09:52:13Z,Closed by JulienPalard in ,,JulienPalard/Pipe,42
```

Expected Output (raw JSON, no code blocks):

{"issue_type":"question","resolution_found":"yes","resolution_type":"discussion","resolution":{"type":"comment","url":"","solution":"We're using the name `select` for this:\n\n```\n>>> \"123\" | select(int) | add\n6\n```\n\nI choose `select` for its similarity with SQL select, and I were having a where:\n\n```\n>>> range(10) | where(lambda x: x % 2 == 0) | select(lambda x: x ** 2) | concat\n'0, 4, 16, 36, 64'\n```\n\nbeing the equivalent of:\n\n```\n>>> [x ** 2 for x in range(10) if x % 2 == 0]\n[0, 4, 16, 36, 64]\n```"},"resolution_inability":null}

---

**Example 4: Unresolved enhancement (no_responder)**

CSV Input:
```
type,author,date,text,url,repo_name,issue_number
IssueTitle,ankur7,2023-08-17T19:08:06Z,Provide an interface to parse the graph of connected resources,https://github.com/bridgecrewio/checkov/issues/5451,bridgecrewio/checkov,5451
IssueOpened,ankur7,2023-08-17T19:08:06Z,"**Describe the issue**  \nWe want to have custom checks which span multiple resources connected together.  \nTo achieve this we can go the YAML way, but with more complex logic it will be better to use Python checks.  \nWe need a Graph Parser so that we can traverse from one component to another connected component\n\n**Examples**  \nLet's assume we have the below components\n\n1. EC2 instance\n2. Security group: Attached to the EC2 instance\n3. Inbound Rule: Part of the Security Group  \n   Now we want to identify if any Inbound rule allows traffic from IP 0.0.0.0/0 then this should EC2 instance should be flagged.  \n   However, if the EC2 instance has a tag with key:exempt value:True then this should not be flagged.  \n   There are other cases like this for which we feel Python checks will be easier to implement\n\n**Desktop (please complete the following information):**\n\n* OS: Linux\n* Checkov Version [e.g. 22]\n\n**Additional context**  \nAdd any other context about the problem here (e.g. code snippets).",https://github.com/bridgecrewio/checkov/issues/5451,bridgecrewio/checkov,5451
IssueComment,stale,2024-02-16T03:32:36Z,"Thanks for contributing to Checkov! We've automatically marked this issue as stale to keep our issues list tidy, because it has not had any activity for 6 months. It will be closed in 14 days if no further activity occurs. Commenting on this issue will remove the stale tag. If you want to talk through the issue or help us understand the priority and context, feel free to add a comment or join us in the Checkov slack channel at codifiedsecurity.slack.com  \nThanks!",,bridgecrewio/checkov,5451
IssueComment,stale,2024-04-14T14:21:12Z,"Closing issue due to inactivity. If you feel this is in error, please re-open, or reach out to the community via slack: codifiedsecurity.slack.com Thanks!",,bridgecrewio/checkov,5451
ClosedEvent,stale,2024-04-14T14:21:21Z,Closed by stale in ,,bridgecrewio/checkov,5451
```

Expected Output (raw JSON, no code blocks):

{"issue_type":"enhancement","resolution_found":"no","resolution_type":null,"resolution":null,"resolution_inability":"no_responder"}

---

**Example 5: Bug unresolved (no_responder)**

CSV Input:
```
type,author,date,text,url,repo_name,issue_number
IssueTitle,Yoshihito-M,2022-04-11T02:17:10Z,Raspberry pi 4 does not detect T265,https://github.com/IntelRealSense/librealsense/issues/10383,IntelRealSense/librealsense,10383
IssueOpened,Yoshihito-M,2022-04-11T02:17:10Z,"### Info\n\n| Camera Model | T265 |  \n| Operating System & Version | Ubuntu 20.04.4 LTS |  \n| Kernel Version (Linux Only) | 5.4.0-1058-raspi |  \n| Platform | Raspberry Pi 4 Model B 8G |  \n| SDK Version | v2.50.0 |\n\n### Issue Description\n\nI conected T265 to USB 3.0 port of Rappberry pi and `lsusb` shown the following correct message:  \n`Bus 001 Device 005: ID 03e7:2150 Intel Myriad VPU [Movidius Neural Compute Stick`\n\nHowever, when I started realsense viewer, it said the following message:  \n`Connect a realsense camera or add source`\n\nAs a side note, when I used D455, realsense viewer shown some data( for example, pointcloud)\n\nI would like to know how to fix the trouble mentioned above.  \nPlease give me some advice.",https://github.com/realsenseai/librealsense/issues/10383,IntelRealSense/librealsense,10383
IssueComment,Ntstr,2022-05-31T16:02:52Z,"Hello,\n\ndid you find a solution?  \nI happen to have exactly the same problem and I would be interested to know if you managed to get out of it and how.",,IntelRealSense/librealsense,10383
```

Expected Output (raw JSON, no code blocks):

{"issue_type":"bug","resolution_found":"no","resolution_type":null,"resolution":null,"resolution_inability":"no_responder"}

### Output Requirements:

Your response must be a single line of valid JSON with NO additional formatting:

**Required format:**
- Start with `{` and end with `}`
- Do NOT add markdown code blocks (no ``` before or after)
- Do NOT add backticks
- Do NOT add explanations, comments, or any text outside the JSON object
- Do NOT add newlines or pretty-printing - output compact JSON on a single line

**String values:**
- Use exact lowercase strings as specified in each step (e.g., `"bug"`, `"yes"`, `"code"`)
- Multi-word values use underscores (e.g., `"asker_ghosted"`, `"no_responder"`)
- For null values, use JSON `null` (not the string `"null"`)

**Example of correct format:**
`{"issue_type":"bug","resolution_found":"yes","resolution_type":"code","resolution":{"type":"commit","url":"https://...","solution":"Fixed bug"},"resolution_inability":null}`

**Example of INCORRECT format (do NOT do this):**
```json
{
  "issue_type": "bug",
  ...
}
```