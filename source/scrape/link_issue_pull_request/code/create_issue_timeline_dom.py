#!/usr/bin/env python3
from pathlib import Path
import re
import json
from bs4 import BeautifulSoup
import pandas as pd
from markdownify import markdownify as md
from source.scrape.link_issue_pull_request.code.create_issue_timeline_json import HtmlToMarkdown
INCLUDE_MENTIONED_BY = False

def GetDate(entry):
    relative_times = entry.find_all("relative-time")
    date = None
    if relative_times: 
        date = min(rt["datetime"] for rt in relative_times if rt.has_attr("datetime"))
    return date

def HtmlTextToTimelineDOM(html_text):
    timeline = []
    timeline_entries = []
    soup = BeautifulSoup(html_text or "", "html.parser")
    timeline_container = soup.find(attrs={"data-testid": "issue-timeline-container"})

    if timeline_container is not None:
        timeline_entries = list(timeline_container.find_all(recursive=False))

    for entry in timeline_entries:
        # only consider divs whose class starts with LayoutHelpers-module__timelineElement
        classes = entry.get("class") or []
        if entry.name == "div" and any(re.compile(r"^LayoutHelpers-module__timelineElement").search(c or "") for c in classes):
            event_type = "IssueComment"
            author_tag_list = entry.find_all("a", class_=re.compile(r"AuthorName"))
            author = None
            if author_tag_list:
                author = author_tag_list[0].get_text(strip=True)

            date = GetDate(entry)


            p_tags = entry.find_all("p")
            combined_p_html = "\n".join(str(p) for p in p_tags)
            text_md = HtmlToMarkdown(combined_p_html) if combined_p_html else ""

            timeline.append({"type": event_type, "author": author, "date": date, "text": text_md, "url": None})
        elif entry.name == "section" and entry.get("aria-label", "") == "Events":
            for subentry in entry.find_all("div", class_=re.compile(r"LayoutHelpers-module__timelineElement")):
                author_tag_list = subentry.find_all("a", class_=re.compile(r"eventActorLink"))
                author = None
                if author_tag_list:
                    author = author_tag_list[0].get_text(strip=True)
                
                text = subentry.find(class_ = re.compile(r"timelineBodyContent")).text
                url = None
                relative_times = subentry.find_all("relative-time")

                date = GetDate(subentry)
                if "mentioned this" in text:
                    text_md = text.replace("mentioned this", f" mentioned this")
                    mentioned_url_attr = subentry.find_all("section", attrs={"aria-label": re.compile(r"Issues mentioned")})[0].find_all("a")

                    for i, mentioned_url in enumerate(mentioned_url_attr):
                        url = mentioned_url.get("href")
                        event_type = "MentionedByIssue" if "issues" in url else "MentionedByPR"
                        timeline.append({"type": event_type, "author": author, "date": date, 
                                         "text": text_md + f"\n{mentioned_url.text}", "url": url})

                elif "closed this" in text:
                    text_md ="[" + text.replace("closed this", f" closed this", )
                    text_md = text_md.replace("on ", " on ", -1)
                    event_type = "ClosedEvent"
                    
                    url_list = subentry.find_all("a", href=True, attrs={"class": re.compile(r"closerLink")})
                    if url_list:
                        url = url_list[0].get("href")
                elif " commit that references" in text:
                    text.replace("mentioned this", " mentioned this")
                    commit_url_list = subentry.find_all("a", href=True, attrs={"class": re.compile(r"ReferencedEventInner-module__commitHashLink")})

                    for i, commit_url in enumerate(commit_url_list):
                        url = commit_url.get("href")
                        commit_text = commit_url.get("aria-label")
                        event_type = "MentionedByCommit"
                        timeline.append({"type": event_type, "author": author, "date": date, 
                                         "text": text_md + f"\n{commit_text}", "url": url})
                elif "milestone " in text:
                    text_md = text.replace("added ", " added ")
                    event_type = "MilestoneEvent"
                elif "added " in text:
                    text_md = text.replace("added ", " added ")
                    event_type = "LabelEvent"
                elif "assigned " in text:
                    text_md = text.replace("assigned ", " assigned ")
                    text_md = text_md.replace("unassigned ", " unassigned ")
                    event_type = "AssigneeEvent"
                if "mentioned this" not in text:
                    timeline.append({"type": event_type, "author": author, "date": date, "text": text_md, "url": url})

    return pd.DataFrame(timeline)