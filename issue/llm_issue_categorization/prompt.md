## **Task Prompt (Revised)**

### **Task**

You are given a CSV of events from a GitHub issue discussion. 
The first event in each GitHub issue discussion is an inquiry. 
Your task is to determine **whether the original inquiry was resolved** and, if so, **determine how it was resolved and extract the actual resolution(s)** that resolved it.
A **resolution** is the **substantive action, explanation, configuration, or code change** that fixes or answers the original problem. 

### **Data**
Use **only the provided CSV**. Do not infer or assume information that is not contaiined in the csv. 
The csv contains, in each row, a series of actions that are taking place in a GitHub issue discussion, in descending order from oldest to most recent.
The csv contains in the 
 - "author" column: the person responsible for the action in that row
 - "type" column: the type of action that was done
 - "text" column: details about the action that occurred
 - "url" column: For some actions like referenced pull requests or commits, a link to the pull request or comit url
 - "date" column: The date of the action


## **Output Format**

Return **one JSON object** with **exactly four fields**:

```json
{
  "solution_status": "",
  "solution_status_reason": "",
  "solvers": [],
  "solutions": []
}
```
I will now describe what each of the fields means in more detail and how you can identify each. 

## **Field Definitions**

### **1. `solution_status`**

* `"yes"` → the inquiry was resolved
* `"no"` → the inquiry was not resolved

Here are some ways an inquiry can be resolved
- An issue was closed as a result of either a commit or a pull request. For example, this would be the case if an issue closing is associated with a commit or pull in the url. 
- One or more issue comments following the original inquiry directly address the problem raised by the author of the original inquiry in a conceptual manner. Sometimes, the author of the original inquiry will make it clear in followup queries that their inquiry was addressed, although this may not always happena nd need not happen for th eissue comment to directly address the problem
- One or more issue comments following the original inquiry state that a code change has been made to address the inquiry. Sometimes, a commit or pull request may be mentioned shortly before or after this comment, as can be ascertained by the date column. 
- One or more issue comments describes the motivation for a policy and why changes will not be made, despite the user's inquiry. 

Here are some ways an inquiry is not resolved
- If throughout the entire conversation, the only comments are clarifying questions about what the inquiry is, or additional comments by users stating that they are encountering the same problems
- If nobody responds to the inquiry



### **2. `solution_status_reason`**

If `solution_status` is `"yes"`, choose **one**:

* `"discussion"` → resolved via conceptual explanation in an issue comment. Importantly, it must be clear throughout the discussion that the original inquiry was resolved without any code changes. 
* `"code change"` → resolved via a code change, either explicitly mentioned as via a commit, PR, merge,  milestone, or automated code-linked closure through commit pushes and pull merges. Importantly, just because code changes are attempted do not mean that the inquiry has been resolved. It must be clear either through the automated closure of the issue, or through the contextual discussions in issue comments before and after commit and pull mentioned events that the proposed solutions or changes actually resolve the problem. Sometimes, it is also the case that the solution is referenced via the pull # or commit SHA in a discussion, rather than explicitly mentioned in the timeline. 

If `solution_status` is `"no"`, choose **one**:

* `"no responder"`
* `"asker ghosted"`
* `"responder ghosted"`

No responder means that none of the followup comments or actions, if any, are responsible for addressing the inquiry. 
Asker ghosted means that at some point, the discussion ceased because the user who made the inquiry stopped responding, and the lack of response was not because a resolution to the discussion had been reached.
Responder ghosted means that at some point, someone had responded to try and make headway towards a resolution, but they had eventually stopped responding.
The three categories are mutually exclusive. 


### **3. `solvers`**

A list of people from the authors column who have solved a problem. 

Include a person **if and only if** they:

* authored a **discussion comment that contributes substantial solution value** 
* were the person responsible for a ClosedEvent **resolving commit or pull request** 

**Substantial solution value means**:

* introducing a key idea, fix, configuration, or explanation that is required for resolution
* not merely confirming, thanking, rephrasing, or restating another person’s solution

**Do NOT include** people who only:

* confirmed the solution worked
* thanked another user
* closed the issue without contributing the solution
* summarized or repeated someone else’s solution


### **4. `solutions`**

* If `solution_status` is `"yes"` → a list of solution objects
* If `solution_status` is `"no"` → an empty list (`[]`)

Each solution object:

```json
{
  "type": "",
  "url": "",
  "solution_text": ""
}
```

## **Allowed `solutions.type` Values**

* `"commit"`
* `"pull_request"`
* `"discussion_solution"`

---

## **Rules for `solutions`**

* The **resolving commit or pull request must be included if it exists** - the one responsible for the issue closing

  * It **takes precedence** over discussion
* Multiple discussion solutions are allowed **only if**:

  * they each add **distinct, substantive solution value**
* Each solution:

  * must be authored by exactly one person
  * must have a **full URL**
  * must contain **verbatim solution text** that is either part of, or the whole text column for a row
* Every person in `solvers` **must correspond to at least one solution**
* Every solution’s author **must appear in `solvers`**


## **Resolution Precedence**

1. **Code-Based Resolution (Highest Priority)**

   * If a commit or pull request closes or resolves the inquiry:

     * `solution_status = "yes"`
     * `solution_status_reason = "code change"`
     * Include **only the resolving commit(s) / PR(s)**

2. **Discussion-Based Resolution**

   * Only if no resolving code artifact exists:

     * extract **all comments that contribute substantial solution value**
     * set `solution_status_reason = "discussion"`

3. **Unresolved**

   * Only if **no actionable solution exists**
   * `solutions` must be `[]`

---

## **Critical Clarifications (Read Carefully)**

### **Resolver Focus (Highest Priority Rule)**

* The **resolver** is the **person or artifact that actually closes or resolves the inquiry**.
* If the issue is closed by:

  * a **commit**, or
  * a **pull request**
    → that **closing commit / PR is the solution**, even if similar ideas appeared earlier in discussion.
This will be indicated by event_type == "ClosedEvent" and the url either containing "/commit/" or "/pull/"
* **Do NOT include precursor discussion, speculation, or partial ideas** that occur before the resolving commit/PR unless they are the *only* resolution.

---

### **Full Fidelity Extraction Requirement**

* All URLs **must be complete, full GitHub URLs** (no truncation, no ellipses) from the url column. 
* `solution_text` **must contain the full verbatim solution content**, not summarie from the text column. 

  * For commits / PRs: include the **exact commit message or PR title text** that explains the fix from the text column.
  * For discussion solutions: include the **exact text of the comment(s)** that provide the actionable solution, from the text column. 


## **Strict Prohibitions**

* Do NOT infer solutions
* Do NOT infer solver identity
* Do NOT truncate URLs or solution text
* Do NOT credit maintainers by default
* Do NOT include precursor discussion once a resolving commit/PR exists
* Do NOT override a resolved inquiry with later tangential discussion
* Do NOT include explanations outside the JSON

