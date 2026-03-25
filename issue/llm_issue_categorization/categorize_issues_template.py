import json
import pandas as pd
from source.scrape.link_issue_pull_request.code.link_using_issue import GrabIssueData
import time
from pathlib import Path
from ollama import Client as OllamaClient
import re
import requests
import os
import csv as csv_module
from collections import Counter
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

PROMPT_PATH = Path("issue/llm_issue_categorization/prompt_expanded_pre_diff.md")
PROMPT_WITH_DIFF_PATH = Path("issue/llm_issue_categorization/prompt_expanded_with_diff.md")
OUTDIR = Path("issue/llm_issue_categorization/output")
INFILE = Path("issue/sampled_issues_annotated.csv")

OLLAMA_MODELS = [
    "qwen3-coder:480b-cloud",
    "glm-4.6:cloud",
    "gpt-oss:120b-cloud",
    "gemini-3-flash-preview:cloud"
]

GITHUB_TOKEN = os.environ.get("BACKUP4_GITHUB_TOKEN", "")
GITHUB_API_BASE = "https://api.github.com"

VALID_ISSUE_TYPES = {"bug", "enhancement", "question"}
VALID_RESOLUTION_FOUND = {"yes", "no"}
VALID_RESOLUTION_TYPES = {"code", "discussion", None}
VALID_RESOLUTION_INABILITY = {"responder_dropped", "asker_ghosted", "impossible_or_out_of_scope", None}
VALID_BACKWARDS_INCOMPATIBLE = {"yes_internal", "yes_external", "no"}
VALID_SOFTWARE_BROKEN = {"yes", "no", None}
VALID_FEATURE_SOLUTION_STATUS = {"implemented", "not_implemented", None}


def Main():
    df = pd.read_csv(INFILE).head(300)
    df = FetchLinkedPullRequests(df)

    timeline_dfs = [
        d["timeline_df"]
        for d in df["linked_pull_request"]
        if isinstance(d, dict) and isinstance(d.get("timeline_df"), pd.DataFrame)
    ]

    OUTDIR.mkdir(parents=True, exist_ok=True)

    prompt_body = ReadPrompt(PROMPT_PATH)
    prompt_with_diff_body = ReadPrompt(PROMPT_WITH_DIFF_PATH)
    client = OllamaClient()

    for model in OLLAMA_MODELS:
        print(f"Processing with model: {model}")
        rows = ProcessPreDiffPhase(timeline_dfs, prompt_body, client, model)
        df_issue_analysis_precode = pd.DataFrame(rows)

        rows_with_diffs = ProcessWithDiffPhase(
            df_issue_analysis_precode,
            prompt_with_diff_body,
            client,
            model
        )
        df_issue_analysis_with_diff = pd.DataFrame(rows_with_diffs)

        model_safe = model.replace(":", "_").replace("/", "_")
        output_file = Path(f"issue/sampled_issues_llm_categorization_output_{model_safe}.csv")
        df_issue_analysis_with_diff.to_csv(output_file, index=False)
        print(f"Saved results for model {model} to {output_file}")

    AggregateModelResults()
    EvaluateLLMAccuracy()


def FetchLinkedPullRequests(df):
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

    return df


def ReadPrompt(path):
    return path.read_text(encoding="utf-8")


def ProcessPreDiffPhase(timeline_dfs, prompt_body, client, model):
    rows = []
    for timeline_df in timeline_dfs:
        if timeline_df.empty: continue
        repo_safe = timeline_df.iloc[0]["repo_name"].replace("/", "_")
        issue_number = int(timeline_df.iloc[0]["issue_number"])
        csv_path = OUTDIR / f"{repo_safe}_{issue_number}_timeline.csv"
        csv_path.write_text(timeline_df.to_csv(index=False), encoding="utf-8")

        messages = [
            {"role": "user", "content": prompt_body},
            {"role": "user", "content": f"CSV below\n{FormatCSVForLLM(csv_path)}"},
        ]

        parsed = CallLLMWithRetry(client, model, messages)

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

        ValidatePreDiffResponse(parsed)

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
            "backwards_incompatible": parsed.get("backwards_incompatible"),
            "software_broken": parsed.get("software_broken"),
            "feature_solution_status": parsed.get("feature_solution_status"),
        })

    return rows


def FormatCSVForLLM(csv_path):
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv_module.reader(f)
        rows = list(reader)

    lines = []
    for row in rows:
        formatted_fields = []
        for field in row:
            if '\n' in field or ',' in field or '"' in field or '\r' in field:
                escaped = field.replace('"', '""')
                escaped = escaped.replace('\n', '\\n')
                escaped = escaped.replace('\r', '')
                formatted_fields.append(f'"{escaped}"')
            else:
                formatted_fields.append(field)
        lines.append(','.join(formatted_fields))

    return '\n'.join(lines)


def CallLLMWithRetry(client, model, messages):
    try:
        response = client.chat(model, messages=messages, stream=False)
        cleaned_response = CleanJSONResponse(response.message.content)
        return json.loads(cleaned_response)
    except:
        response = client.chat(model, messages=messages, stream=False)
        cleaned_response = CleanJSONResponse(response.message.content)
        return json.loads(cleaned_response)


def CleanJSONResponse(response_text):
    text = response_text.strip()

    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]

    if text.endswith("```"):
        text = text[:-3]

    return text.strip()


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

        row_url = timeline_df.loc[idx, "url"] if "url" in timeline_df.columns else None

        if token.startswith("#"):
            num = token.lstrip("#")
            solution = token
            url = row_url if row_url else f"https://github.com/{repo_name}/pull/{num}"
        else:
            solution = token
            url = row_url if row_url else f"https://github.com/{repo_name}/commit/{token}"

        return {"solution": solution, "url": url, "text": cell, "row_index": idx}
    return None


def ValidatePreDiffResponse(parsed):
    issue_type = parsed.get("issue_type")
    resolution_found = parsed.get("resolution_found")
    resolution_type = parsed.get("resolution_type")
    resolution_inability = parsed.get("resolution_inability")
    backwards_incompatible = parsed.get("backwards_incompatible")
    software_broken = parsed.get("software_broken")
    feature_solution_status = parsed.get("feature_solution_status")

    assert issue_type in VALID_ISSUE_TYPES, f"Invalid issue_type: {issue_type}"
    assert resolution_found in VALID_RESOLUTION_FOUND, f"Invalid resolution_found: {resolution_found}"

    if resolution_found == "yes":
        assert resolution_type in {"code", "discussion"}, f"Invalid resolution_type when resolved: {resolution_type}"
        assert resolution_inability is None, f"resolution_inability must be null when resolved"
    else:
        assert resolution_type is None, f"resolution_type must be null when not resolved"
        assert resolution_inability in {"responder_dropped", "asker_ghosted", "impossible_or_out_of_scope"}, f"Invalid resolution_inability: {resolution_inability}"

    # Backwards incompatible is always required
    assert backwards_incompatible in VALID_BACKWARDS_INCOMPATIBLE, f"Invalid backwards_incompatible: {backwards_incompatible}"

    # software_broken: required for bugs/questions, must be null for enhancements
    if issue_type == "enhancement":
        assert software_broken is None, f"software_broken must be null for enhancements, got: {software_broken}"
    else:
        assert software_broken in {"yes", "no"}, f"software_broken required for bugs/questions, got: {software_broken}"

    # feature_solution_status: required for enhancements, must be null for bugs/questions
    if issue_type == "enhancement":
        assert feature_solution_status in {"implemented", "not_implemented"}, \
            f"feature_solution_status required for enhancements, got: {feature_solution_status}"
    else:
        assert feature_solution_status is None, \
            f"feature_solution_status must be null for bugs/questions, got: {feature_solution_status}"


def ProcessWithDiffPhase(df_issue_analysis_precode, prompt_with_diff_body, client, model):
    rows_with_diffs = []
    for idx, row in df_issue_analysis_precode.iterrows():
        repo_name = row["repo_name"]
        issue_number = row["issue_number"]
        repo_safe = repo_name.replace("/", "_")
        csv_path = OUTDIR / f"{repo_safe}_{issue_number}_timeline.csv"

        resolution_url = row.get("resolution_url")
        diff_text = None
        diff_filename = None
        include_diff_in_prompt = False

        # Try to fetch and save diff if resolution_url exists
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

                    diff_path = OUTDIR / diff_filename
                    diff_path.write_text(diff_text, encoding="utf-8")
                    print(f"Saved diff: {diff_filename}")
                else:
                    print(f"Could not fetch diff for {resolution_url}")
            else:
                print(f"Could not parse URL: {resolution_url}")

        base_messages = [
            {"role": "user", "content": prompt_with_diff_body},
            {"role": "user", "content": f"CSV below\n{FormatCSVForLLM(csv_path)}"}
        ]

        if diff_text:
            messages_with_diff = list(base_messages) + [{"role": "user", "content": f"Diff below\n{diff_text}"}]
            include_diff_in_prompt = True
        else:
            messages_with_diff = list(base_messages) + [{"role": "user", "content": "No code diff available for this issue."}]
            include_diff_in_prompt = False


        try:
            parsed = CallLLMWithRetry(client, model, messages_with_diff)
        except Exception as e:
            err_str = str(e)
            if diff_text and ("prompt too long" in err_str.lower()):
                print(f"LLM call failed for {repo_name}#{issue_number} with diff due to prompt size; retrying without diff. Error: {err_str}")
                messages_without_diff = list(base_messages) + [{"role": "user", "content": "No code diff available for this issue."}]
                try:
                    parsed = CallLLMWithRetry(client, model, messages_without_diff)
                    diff_filename = None
                    include_diff_in_prompt = False
                except Exception as e2:
                    print(f"Retry without diff also failed for {repo_name}#{issue_number}. Error: {e2}")
                    raise
            else:
                print(f"LLM call failed for {repo_name}#{issue_number} and is not recognized as a prompt-size issue. Error: {err_str}")
                raise

        if "system_filtering_regex_or_condition" in parsed:
            parsed['reach_specification']['system_filtering_regex_or_condition'] = parsed['system_filtering_regex_or_condition']
            del parsed['system_filtering_regex_or_condition']

        ValidateWithDiffResponse(parsed)

        enhanced_row = row.to_dict()
        enhanced_row.update({
            "reach_specification": json.dumps(parsed.get("reach_specification", {})),
            "diff_filename": diff_filename if diff_filename and include_diff_in_prompt else None
        })

        rows_with_diffs.append(enhanced_row)

        status = "with diff" if include_diff_in_prompt else "without diff"
        print(f"Processed {repo_name}#{issue_number} {status}")

    return rows_with_diffs


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


def ValidateWithDiffResponse(parsed):
    reach_specification = parsed.get("reach_specification")

    assert reach_specification is not None, "reach_specification must be present"
    assert "software_filtering_regex" in reach_specification, "software_filtering_regex required"
    assert "command_filtering_regex" in reach_specification, "command_filtering_regex required"
    assert "system_filtering_regex_or_condition" in reach_specification, "system_filtering_regex_or_condition required"

    software_filter = reach_specification["software_filtering_regex"]
    assert "file_within_pairs" in software_filter, "software_filtering_regex.file_within_pairs required"
    assert len(software_filter["file_within_pairs"]) > 0, "software_filtering_regex.file_within_pairs must be non-empty"

    command_filter = reach_specification["command_filtering_regex"]
    assert "file_within_pairs" in command_filter, "command_filtering_regex.file_within_pairs required"

    system_filter = reach_specification["system_filtering_regex_or_condition"]
    assert "file_within_pairs" in system_filter, "system_filtering_regex_or_condition.file_within_pairs required"
    assert "conditions" in system_filter, "system_filtering_regex_or_condition.conditions required"

    if system_filter["conditions"] != "**":
        assert len(system_filter["file_within_pairs"]) == 0, "file_within_pairs must be empty when conditions is not '**'"


def FormatModelSummary(row, model_dfs, category):
    lines = []
    repo_name = row["repo_name"]
    issue_number = int(float(row["thread_number"]))

    for model, df in model_dfs.items():
        match = df[(df["repo_name"] == repo_name) & (df["issue_number"] == issue_number)]
        if not match.empty:
            value = match.iloc[0].get(category, "N/A")
            if pd.isna(value):
                value = "None"
            lines.append(f"{model}: {value}")

    return "\n".join(lines)


def AggregateModelResults():
    df_base = pd.read_csv(INFILE).head(300)

    model_dfs = {}
    for model in OLLAMA_MODELS:
        model_safe = model.replace(":", "_").replace("/", "_")
        output_file = Path(f"issue/sampled_issues_llm_categorization_output_{model_safe}.csv")
        model_dfs[model] = pd.read_csv(output_file)

    column_mappings = [
        ("issue_type", [("issue_type", "issue_type_summary")]),
        ("resolution_found", [("resolution_found", "resolution_found_summary")]),
        ("resolution_type", [("resolution_type", "resolution_type_summary")]),
        ("resolution", [
            ("resolution_solution", "resolution_solution_summary"),
            ("resolution_url", "resolution_url_summary"),
            ("resolution_text", "resolution_text_summary")
        ]),
        ("resolution_inability", [("resolution_inability", "resolution_inability_summary")]),
        ("backwards_incompatible", [("backwards_incompatible", "backwards_incompatible_summary")]),
        ("software_broken", [("software_broken", "software_broken_summary")]),
        ("solution_status", [("feature_solution_status", "feature_solution_status_summary")]),
        ("reach_specification", [("reach_specification", "reach_specification_summary")]),
    ]

    for input_col, mappings in column_mappings:
        for model_col, summary_col in mappings:
            df_base[summary_col] = df_base.apply(
                lambda row, mc=model_col: FormatModelSummary(row, model_dfs, mc),
                axis=1
            )

    original_cols = list(pd.read_csv(INFILE, nrows=0).columns)
    ordered_cols = []
    for col in original_cols:
        ordered_cols.append(col)
        for input_col, mappings in column_mappings:
            if col == input_col:
                for _, summary_col in mappings:
                    ordered_cols.append(summary_col)

    df_base = df_base[ordered_cols]

    output_path = Path("issue/sampled_issues_llm_aggregated_results.csv")
    df_base.to_csv(output_path, index=False)
    print(f"Saved aggregated results to {output_path}")

    return df_base


def GetModalPrediction(summary_text):
    if pd.isna(summary_text) or not summary_text:
        return None

    predictions = []
    for line in str(summary_text).strip().split("\n"):
        if ": " in line:
            value = line.split(": ", 1)[1].strip()
            if value and value.lower() != "none" and value.lower() != "n/a":
                predictions.append(value.lower())

    if not predictions:
        return None

    counter = Counter(predictions)
    modal_value, _ = counter.most_common(1)[0]
    return modal_value



def EvaluateLLMAccuracy():
    aggregated_path = Path("issue/sampled_issues_llm_aggregated_results.csv")

    df = pd.read_csv(aggregated_path)

    # Excludes resolution and reach_specification
    eval_columns = [
        ("issue_type", "issue_type_summary", "issue_type"),
        ("resolution_found", "resolution_found_summary", "resolution_found"),
        ("resolution_type", "resolution_type_summary", "resolution_type"),
        ("resolution_inability", "resolution_inability_summary", "resolution_inability"),
        ("backwards_incompatible", "backwards_incompatible_summary", "backwards_incompatible"),
        ("software_broken", "software_broken_summary", "software_broken"),
        ("solution_status", "feature_solution_status_summary", "feature_solution_status"),
    ]

    all_y_true = []
    all_y_pred = []

    print("\n" + "=" * 70)
    print("LLM ACCURACY EVALUATION (vs Modal LLM Prediction)")
    print("=" * 70)

    for gt_col, summary_col, display_name in eval_columns:
        df[f"{display_name}_modal"] = df[summary_col].apply(GetModalPrediction)

        mask = (df['issue_type'].notna()) & (df['issue_type'] != "Chinese")

        if display_name == "software_broken":
            mask = mask & (df['issue_type'].str.lower() != 'enhancement')
        if display_name == "feature_solution_status":
            mask = mask & (df['issue_type'].str.lower() == 'enhancement')

        mask = mask & df[gt_col].notna() & df[f"{display_name}_modal"].notna()

        subset = df[mask].copy()

        y_true = subset[gt_col].astype(str).str.lower().str.strip().tolist()
        y_pred = subset[f"{display_name}_modal"].tolist()

        accuracy = accuracy_score(y_true, y_pred)

        labels = list(set(y_true) | set(y_pred))
        f1 = f1_score(y_true, y_pred, average='macro', zero_division=0, labels=labels)
        precision = precision_score(y_true, y_pred, average='macro', zero_division=0, labels=labels)
        recall = recall_score(y_true, y_pred, average='macro', zero_division=0, labels=labels)

        print(f"\n{display_name.upper()}")
        print(f"  N observations: {len(subset)}")
        print(f"  Accuracy:       {accuracy:.4f}")
        print(f"  F1 (macro):     {f1:.4f}")
        print(f"  Precision:      {precision:.4f}")
        print(f"  Recall:         {recall:.4f}")

        all_y_true.extend([(display_name, v) for v in y_true])
        all_y_pred.extend([(display_name, v) for v in y_pred])

    if all_y_true and all_y_pred:
        y_true_overall = [f"{col}:{val}" for col, val in all_y_true]
        y_pred_overall = [f"{col}:{val}" for col, val in all_y_pred]

        overall_accuracy = accuracy_score(y_true_overall, y_pred_overall)
        labels_overall = list(set(y_true_overall) | set(y_pred_overall))
        overall_f1 = f1_score(y_true_overall, y_pred_overall, average='macro', zero_division=0, labels=labels_overall)
        overall_precision = precision_score(y_true_overall, y_pred_overall, average='macro', zero_division=0, labels=labels_overall)
        overall_recall = recall_score(y_true_overall, y_pred_overall, average='macro', zero_division=0, labels=labels_overall)

        print("\n" + "-" * 70)
        print("OVERALL (ALL COLUMNS COMBINED)")
        print(f"  N observations: {len(all_y_true)}")
        print(f"  Accuracy:       {overall_accuracy:.4f}")
        print(f"  F1 (macro):     {overall_f1:.4f}")
        print(f"  Precision:      {overall_precision:.4f}")
        print(f"  Recall:         {overall_recall:.4f}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    Main()
