## **Task Prompt (Revised)**

### **Task**

You are given a CSV of events from a GitHub issue, discussion, or pull request page.

Your task is to determine **whether the original inquiry was resolved** and, if so, **extract the actual solution(s)** that resolved it.

A **solution** is the **substantive action, explanation, configuration, or code change** that fixes or answers the original problem — **not confirmation that it worked**.

Use **only the provided CSV**. Do not infer or assume information.

---

## **Critical Clarifications (Read Carefully)**

### **Resolver Focus (Highest Priority Rule)**

* The **resolver** is the **person or artifact that actually closes or resolves the inquiry**.
* If the issue is closed by:

  * a **commit**, or
  * a **pull request**
    → that **closing commit / PR is the solution**, even if similar ideas appeared earlier in discussion.
* **Do NOT include precursor discussion, speculation, or partial ideas** that occur before the resolving commit/PR unless they are the *only* resolution.

---

### **Full Fidelity Extraction Requirement**

* All URLs **must be complete, full GitHub URLs** (no truncation, no ellipses).
* `solution_text` **must contain the full verbatim solution content**, not summaries.

  * For commits / PRs: include the **exact commit message or PR description text** that explains the fix.
  * For discussion solutions: include the **exact text of the comment(s)** that provide the actionable solution.

---

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

---

## **Field Definitions**

### **1. `solution_status`**

* `"yes"` → the inquiry was resolved
* `"no"` → the inquiry was not resolved

---

### **2. `solution_status_reason`**

If `solution_status` is `"yes"`, choose **one**:

* `"discussion"` → resolved via explanation, configuration, or guidance in comments
* `"code change"` → resolved via a commit, PR, merge, release, milestone, or automated code-linked closure

If `solution_status` is `"no"`, choose **one**:

* `"no responder / responder ghosted"`
* `"asker ghosted"`

---

### **3. `solvers`**

A list of **GitHub profile URLs of all individuals who substantively contributed to the resolution**.

Include a person **if and only if** they:

* authored a **resolving commit or pull request**, or
* authored a **discussion comment that contributes substantial solution value**

**Substantial solution value means**:

* introducing a key idea, fix, configuration, or explanation that is required for resolution
* not merely confirming, thanking, rephrasing, or restating another person’s solution

**Do NOT include** people who only:

* confirmed the solution worked
* thanked another user
* closed the issue without contributing the solution
* summarized or repeated someone else’s solution

---

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

---

## **Allowed `solutions.type` Values**

* `"commit"`
* `"pull_request"`
* `"discussion_solution"`

---

## **Rules for `solutions`**

* The **resolving commit or pull request must be included if it exists**

  * It **takes precedence** over discussion
* Multiple discussion solutions are allowed **only if**:

  * they each add **distinct, substantive solution value**
* Each solution:

  * must be authored by exactly one person
  * must have a **full URL**
  * must contain **verbatim solution text**
* Every person in `solvers` **must correspond to at least one solution**
* Every solution’s author **must appear in `solvers`**

---

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

## **Strict Prohibitions**

* Do NOT infer solutions
* Do NOT infer solver identity
* Do NOT truncate URLs or solution text
* Do NOT credit maintainers by default
* Do NOT include precursor discussion once a resolving commit/PR exists
* Do NOT override a resolved inquiry with later tangential discussion
* Do NOT include explanations outside the JSON

