#!/usr/bin/env python3
from pathlib import Path
import re
import json
from bs4 import BeautifulSoup
import pandas as pd
from markdownify import markdownify as md

INCLUDE_MENTIONED_BY = False


def NormalizeUrl(url):
    if not url:
        return ""
    return ("https://github.com" + url) if url.startswith("/") else url


def TryLoadJson(raw_text):
    try:
        return json.loads(raw_text)
    except (json.JSONDecodeError, TypeError):      
        line_comment_pattern = r"//.*?$"
        text_without_comments = re.sub(line_comment_pattern, "", raw_text, flags=re.MULTILINE)
        trailing_comma_pattern = r",\s*([\]}])"
        sanitized_json_text = re.sub(trailing_comma_pattern, r"\1", text_without_comments)

        try:
            return json.loads(sanitized_json_text)
        except (json.JSONDecodeError, TypeError):
            return None

def FindTimelineEvents(html_text):
    soup = BeautifulSoup(html_text or "", "html.parser")
    data_blocks = []

    # 1) existing script JSON pattern (keeps current behavior)
    GITHUB_DATA_EXTRACTOR = re.compile(r'\{.*?"(?:__typename|timelineItems)".*\}', re.DOTALL)
    for s in soup.find_all("script"):
        content = s.string or ""
        match = GITHUB_DATA_EXTRACTOR.search(content)
        if match:
            loaded = TryLoadJson(match.group(0))
            if loaded:
                data_blocks.append(loaded)

    return data_blocks

def HtmlToMarkdown(html_text):
    return md(html_text or "", heading_style="ATX")

def ExtractRefFromNode(node):
    if not isinstance(node, dict):
        return "", "", "" 

    url = NormalizeUrl(node.get("url") or node.get("resourcePath") or "")
    if not url:
        return "", "", "" 

    typename = (node.get("__typename") or "").lower()

    ref_type = (
        "pull" if "pull" in typename or "/pull/" in url
        else "commit" if "commit" in typename or "/commit/" in url
        else "issue" if "issue" in typename or "/issues/" in url
        else ""
    )

    title_html = (
        node.get("pullTitleHTML")
        or node.get("messageHeadlineHTML")
        or node.get("titleHTML")
        or node.get("message")
        or node.get("title")
        or ""
    )

    title = HtmlToMarkdown(title_html) if title_html else ""

    return ref_type, url, title


def MentionContextFromUrl(u):
    if not u:
        return ("", "")
    if "/pull/" in u:
        return ("pull", u)
    if "/commit/" in u:
        return ("commit", u)
    if "/issues/" in u:
        return ("issue", u)
    return ("", u if u.startswith("http") else "")


def BuildEvent(evtype, text, date, person, url,
               label="", close_reason="", assignee="", assigner="",
               ref_type="", ref_url="", ref_title="", 
               milestone="", target_name=""):
    return {
        "event_type": evtype,
        "text": text,
        "date": date,
        "person": person,
        "url": url,
        "label": label,
        "close_reason": close_reason,
        "assignee": assignee,
        "assigner": assigner,
        "ref_type": ref_type,
        "ref_url": ref_url,
        "ref_title": ref_title,
        "milestone": milestone,
        "target_name": target_name,
    }


def ExtractTimelineNodes(j):
    timeline_edges = []
    for q in j.get("payload", {}).get("preloadedQueries", []):
        if q.get("queryName") == "IssueViewerViewQuery":
            timeline_edges = (q.get("result", {}).get("data", {}).get("repository", {})\
                .get("issue", {}).get("frontTimelineItems", {}).get("edges", [])
            )
    issue_data = q.get("result", {}).get("data", {}).get("repository", {}).get("issue", {})
    events = [BuildEvent("IssueOpened", HtmlToMarkdown(issue_data.get("bodyHTML") or ""), issue_data.get("createdAt", ""),
                         issue_data.get("author", {}).get("login", ""), issue_data.get("url"))]
    for edge in timeline_edges:
        timeline_node = edge.get("node")
        if not isinstance(timeline_node, dict):
            continue

        typename = timeline_node.get("__typename", "")
        created_at = timeline_node.get("createdAt", "")

        actor_node = timeline_node.get("actor") or timeline_node.get("author") or {}
        actor_login = actor_node.get("login") if isinstance(actor_node, dict) else ""
        node_url = timeline_node.get("url") or timeline_node.get("resourcePath") or ""

        if typename == "IssueComment":
            text_md = HtmlToMarkdown(timeline_node['bodyHTML'])
            events.append(BuildEvent(typename, text_md, created_at, actor_login, node_url))
            continue

        if typename == "AssignedEvent":
            assignee_node = timeline_node.get("assignee") or {}
            assignee_login = assignee_node.get("login") if isinstance(assignee_node, dict) else ""
            events.append(BuildEvent(typename, "", created_at, actor_login, node_url, assignee=assignee_login, assigner=actor_login))
            continue

        if typename == "MilestonedEvent":
            milestone_title = timeline_node.get("milestoneTitle") or ""
            events.append(BuildEvent(typename, f"added to milestone: {milestone_title}", created_at, actor_login, node_url, milestone=milestone_title))
            continue

        if typename == "DemilestonedEvent":
            milestone_title = timeline_node.get("milestoneTitle") or ""
            events.append(BuildEvent(typename, f"removed from milestone: {milestone_title}", created_at, actor_login, node_url,
                milestone=milestone_title))
            continue

        if typename in ("ReferencedEvent", "CrossReferencedEvent"):
            subject_node = timeline_node.get("innerSource")  or timeline_node.get("commit") or timeline_node.get("subject") or {}
            ref_type, ref_url, ref_title = ExtractRefFromNode(subject_node)

            event_type = (
                "MentionedByCommit" if ref_type == "commit"
                else "MentionedByIssue" if ref_type == "issue"
                else "MentionedByPR" if ref_type == "pr" 
                else "ReferencedEvent"
            )

            text_md = ref_title or ref_url or ""

            events.append(BuildEvent(event_type, text_md, created_at, actor_login, node_url,
                ref_type=ref_type, ref_url=ref_url, ref_title=ref_title))
            continue

        if typename == "ClosedEvent":
            closer_node = timeline_node.get("closer") or {}
            ref_type, ref_url, ref_title = ExtractRefFromNode(closer_node)

            events.append(BuildEvent(typename, "", created_at, actor_login, node_url, 
                ref_type=ref_type, ref_url=ref_url, ref_title=ref_title,))
            continue

        if typename == "LabeledEvent":
            label = timeline_node['label']['name']
            events.append(BuildEvent(typename, label, created_at, actor_login, node_url))
            continue

    return events


def HtmlTextToTimeline(html_text):
    events = []
    for j in FindTimelineEvents(html_text):
        events.extend(ExtractTimelineNodes(j))
    return pd.DataFrame(events)

input_dir = Path("issue/llm_issue_categorization/raw")
output_dir = Path("issue/llm_issue_categorization/output")
output_dir.mkdir(parents=True, exist_ok=True)

for html_file in input_dir.glob("*.html"):
    with html_file.open("r", encoding="utf-8") as f:
        html_text = f.read()

    df_timeline = HtmlTextToTimeline(html_text)

    out_path = output_dir / f"{html_file.stem}.csv"
    df_timeline.to_csv(out_path, index=False)