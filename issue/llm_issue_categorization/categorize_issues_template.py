import json
import pandas as pd
from source.scrape.link_issue_pull_request.code.link_using_issue import GrabIssueData
import time
from pathlib import Path
from ollama import Client as OllamaClient
import re
import requests
import os

PROMPT_PATH = Path("issue/llm_issue_categorization/prompt_expanded_pre_diff.md")
OLLAMA_MODEL = "gpt-oss:120b-cloud"
#OLLAMA_MODEL = "glm-4.6:cloud"

def ReadPrompt(path):
    return path.read_text(encoding="utf-8") if path.exists() else ""

def CleanJSONResponse(response_text):
    """
    Remove markdown code block wrapper from JSON responses.

    Handles formats like:
    1. ```json\n{...}\n```
    2. ```\n{...}\n```
    3. {...} (plain JSON)
    4. {\n  "key": "value"\n} (JSON with literal newlines - already valid)

    Note: JSON with escaped characters like \n, \\s, \\( is valid JSON
    and will be correctly parsed by json.loads() without modification.
    """
    text = response_text.strip()

    # Check if wrapped in ```json or ```
    if text.startswith("```json"):
        text = text[7:]  # Remove ```json
    elif text.startswith("```"):
        text = text[3:]  # Remove ```

    # Remove trailing ```
    if text.endswith("```"):
        text = text[:-3]

    return text.strip()

def FormatCSVForLLM(csv_path):
    """
    Format CSV with rows on separate lines but \n visible within quoted fields.
    This matches the format shown in the prompt examples.
    """
    import csv as csv_module
    import io

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv_module.reader(f)
        rows = list(reader)

    lines = []
    for row in rows:
        formatted_fields = []
        for field in row:
            # If field contains newlines, commas, or quotes, it needs to be quoted
            if '\n' in field or ',' in field or '"' in field or '\r' in field:
                # Escape quotes by doubling them
                escaped = field.replace('"', '""')
                # Show \n instead of actual newlines within the field
                escaped = escaped.replace('\n', '\\n')
                escaped = escaped.replace('\r', '')
                formatted_fields.append(f'"{escaped}"')
            else:
                formatted_fields.append(field)
        lines.append(','.join(formatted_fields))

    return '\n'.join(lines)

df = pd.read_csv('issue/sampled_issues_annotated.csv').head(30)
df["linked_pull_request"] = df.apply(
    lambda x: GrabIssueData(x["repo_name"], int(float(x["thread_number"]))),
    axis=1
)

for _ in range(10):
    if df[df['linked_pull_request'].apply(lambda x: type(x) == str)].empty:
        break
    df["linked_pull_request"] = df.apply(
        lambda x: GrabIssueData(x["repo_name"], int(float(x["thread_number"])))
        if isinstance(x["linked_pull_request"], str) 
        else x["linked_pull_request"],
        axis=1
    )
    time.sleep(5)

timeline_dfs = [
    d["timeline_df"]
    for d in df["linked_pull_request"]
    if isinstance(d, dict) and isinstance(d.get("timeline_df"), pd.DataFrame)
]

out_dir = Path("issue/llm_issue_categorization/output")
out_dir.mkdir(parents=True, exist_ok=True)


prompt_body = ReadPrompt(PROMPT_PATH)
client = OllamaClient()

# GitHub API configuration
GITHUB_TOKEN = os.environ.get("BACKUP4_GITHUB_TOKEN", "")
GITHUB_API_BASE = "https://api.github.com"

def ParseResolutionURL(url):
    if not url or not isinstance(url, str):
        return None

    pr_match = re.match(r'https://github\.com/([^/]+)/([^/]+)/pull/(\d+)', url)
    if pr_match:
        return {
            "type": "pull",
            "owner": pr_match.group(1),
            "repo": pr_match.group(2),
            "number": pr_match.group(3)
        }

    commit_match = re.match(r'https://github\.com/([^/]+)/([^/]+)/commit/([0-9a-fA-F]+)', url)
    if commit_match:
        return {
            "type": "commit",
            "owner": commit_match.group(1),
            "repo": commit_match.group(2),
            "sha": commit_match.group(3)
        }

    return None

def FetchGitHubDiff(parsed_url, token, retry_delay=120):
    if not parsed_url or not token:
        return None

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3.diff"
    }

    if parsed_url["type"] == "pull":
        api_url = f"{GITHUB_API_BASE}/repos/{parsed_url['owner']}/{parsed_url['repo']}/pulls/{parsed_url['number']}"
    elif parsed_url["type"] == "commit":
        api_url = f"{GITHUB_API_BASE}/repos/{parsed_url['owner']}/{parsed_url['repo']}/commits/{parsed_url['sha']}"
    else:
        return None

    try:
        response = requests.get(api_url, headers=headers)

        if response.status_code == 403:
            remaining = int(response.headers.get("X-RateLimit-Remaining", "1"))
            if remaining == 0:
                reset_time = int(response.headers.get("X-RateLimit-Reset", "0"))
                now = int(time.time())
                wait = max(1, reset_time - now)
                print(f"Rate limit hit. Sleeping {wait} seconds...")
                time.sleep(wait)
                return FetchGitHubDiff(parsed_url, token, retry_delay)

        response.raise_for_status()
        return response.text

    except Exception as e:
        print(f"Error fetching diff from {api_url}: {e}")
        return None

rows = []
def FindClosedBy(timeline_df, repo_name):
    pat = re.compile(r'Closed by\s+[^\s]+\s+in\s+([0-9a-fA-F]{6}|#\d+)\b')
    if "text" not in timeline_df.columns:
        return None
    for idx, cell in timeline_df["text"].astype(str).items():
        if not cell:
            continue
        m = pat.search(cell)
        if not m:
            continue
        token = m.group(1)

        # Get the full URL from the url column if available
        row_url = timeline_df.loc[idx, "url"] if "url" in timeline_df.columns else None

        if token.startswith("#"):
            num = token.lstrip("#")
            solution = token
            url = row_url if row_url else f"https://github.com/{repo_name}/pull/{num}"
        else:
            solution = token
            # Use the full commit URL from the CSV, not the 6-char hash
            url = row_url if row_url else f"https://github.com/{repo_name}/commit/{token}"

        return {"solution": solution, "url": url, "text": cell, "row_index": idx}
    return None

rows = []
out_dir = Path(out_dir)
for timeline_df in timeline_dfs:
    repo_safe = timeline_df.iloc[0]["repo_name"].replace("/", "_")
    issue_number = int(timeline_df.iloc[0]["issue_number"])
    csv_path = out_dir / f"{repo_safe}_{issue_number}_timeline.csv"
    csv_path.write_text(timeline_df.to_csv(index=False), encoding="utf-8")

    messages = [
        {"role": "user", "content": prompt_body},
        {"role": "user", "content": f"CSV below\n{FormatCSVForLLM(csv_path)}"},
    ]

    try:
        response = client.chat(OLLAMA_MODEL, messages=messages, stream=False)
        cleaned_response = CleanJSONResponse(response.message.content)
        parsed = json.loads(cleaned_response)
    except:
        response = client.chat(OLLAMA_MODEL, messages=messages, stream=False)
        cleaned_response = CleanJSONResponse(response.message.content)
        parsed = json.loads(cleaned_response)

    name_parts = csv_path.stem.replace("_timeline", "").split("_")
    issue_number = int(name_parts[-1])
    repo_name = "/".join(name_parts[:-1])

    detected = FindClosedBy(timeline_df, repo_name)
    if detected:
        parsed["resolution_found"] = "yes"
        parsed["resolution_type"] = "code"
        parsed.setdefault("resolution", {})
        parsed["resolution"]["solution"] = detected["solution"]
        parsed["resolution"]["url"] = detected["url"]
        parsed["resolution"]["text"] = detected["text"]

    rows.append({
        "repo_name": repo_name,
        "issue_number": issue_number,
        "issue_type": parsed.get("issue_type"),
        "resolution_found": parsed.get("resolution_found"),
        "resolution_type": parsed.get("resolution_type"),
        "resolution_solution": parsed.get("resolution", {}).get("solution") if parsed.get("resolution") else None,
        "resolution_url": parsed.get("resolution", {}).get("url") if parsed.get("resolution") else None,
        "resolution_text": parsed.get("resolution", {}).get("text") if parsed.get("resolution") else None,
        "resolution_inability": parsed.get("resolution_inability"),
    })

df_issue_analysis_precode = pd.DataFrame(rows)

# Load the expanded prompt that includes diff handling
prompt_with_diff_path = Path("issue/llm_issue_categorization/prompt_expanded_with_diff.md")
prompt_with_diff_body = ReadPrompt(prompt_with_diff_path)

# Process ALL rows with the expanded prompt
rows_with_diffs = []
for idx, row in df_issue_analysis_precode.iterrows():
    repo_name = row["repo_name"]
    issue_number = row["issue_number"]
    repo_safe = repo_name.replace("/", "_")
    csv_path = out_dir / f"{repo_safe}_{issue_number}_timeline.csv"

    resolution_url = row.get("resolution_url")
    diff_text = None
    diff_filename = None

    if pd.notna(resolution_url) and resolution_url:
        parsed_url = ParseResolutionURL(resolution_url)

        if parsed_url:
            diff_text = FetchGitHubDiff(parsed_url, GITHUB_TOKEN)

            if diff_text:
                if parsed_url["type"] == "pull":
                    diff_filename = f"{repo_safe}_{issue_number}_pr{parsed_url['number']}_diff.txt"
                else:
                    sha_short = parsed_url["sha"][:7]
                    diff_filename = f"{repo_safe}_{issue_number}_{sha_short}_diff.txt"

                diff_path = out_dir / diff_filename
                diff_path.write_text(diff_text, encoding="utf-8")
                print(f"Saved diff: {diff_filename}")
            else:
                print(f"Could not fetch diff for {resolution_url}")
        else:
            print(f"Could not parse URL: {resolution_url}")

    # Run LLM with expanded prompt (CSV always, diff if available)
    messages = [
        {"role": "user", "content": prompt_with_diff_body},
        {"role": "user", "content": f"CSV below\n{FormatCSVForLLM(csv_path)}"}
    ]

    if diff_text:
        messages.append({"role": "user", "content": f"Diff below\n{diff_text}"})
    else:
        messages.append({"role": "user", "content": "No code diff available for this issue."})

    try:
        response = client.chat(OLLAMA_MODEL, messages=messages, stream=False)
        cleaned_response = CleanJSONResponse(response.message.content)
        parsed = json.loads(cleaned_response)
    except:
        response = client.chat(OLLAMA_MODEL, messages=messages, stream=False)
        cleaned_response = CleanJSONResponse(response.message.content)
        parsed = json.loads(cleaned_response)


    enhanced_row = row.to_dict()
    enhanced_row.update({
        "backwards_incompatible": parsed.get("backwards_incompatible"),
        "software_broken": parsed.get("software_broken"),
        "feature_solution_status": parsed.get("feature_solution_status"),
        "reach_specification": json.dumps(parsed.get("reach_specification", {})),
        "diff_filename": diff_filename if diff_filename else None
    })

    rows_with_diffs.append(enhanced_row)

    status = "with diff" if diff_text else "without diff"
    print(f"Processed {repo_name}#{issue_number} {status}")


df_issue_analysis_with_diff = pd.DataFrame(rows_with_diffs)

# Save the final results
df_issue_analysis_with_diff.to_csv("issue/sampled_issues_llm_categorization_output.csv", index=False)
