#!/usr/bin/env python3
from pathlib import Path
import re
import json
import csv
import html
from bs4 import BeautifulSoup, NavigableString, Tag

INDIR = Path("issue/llm_issue_categorization/raw")
OUTDIR = Path("issue/llm_issue_categorization/output")
INCLUDE_MENTIONED_BY = False


def CleanHtmlToText(html_snip):
    return html.unescape(BeautifulSoup(html_snip or "", "html.parser").get_text(" ", strip=True))


def NormalizeUrl(url):
    if not url:
        return ""
    return ("https://github.com" + url) if url.startswith("/") else url


def ExtractPageCanonical(html_text):
    s = BeautifulSoup(html_text or "", "html.parser")
    link = s.select_one("link[rel='canonical']")
    href = NormalizeUrl(link["href"]) if link and link.has_attr("href") else ""
    if "/pull/" in href:
        ptype = "pull"
    elif "/commit/" in href:
        ptype = "commit"
    elif "/issues/" in href:
        ptype = "issue"
    else:
        ptype = ""
    return (href, ptype)


def TryLoadJson(candidate):
    try:
        return json.loads(candidate)
    except Exception:
        c = re.sub(r"//.*?$", "", candidate, flags=re.MULTILINE)
        c = re.sub(r",\s*([\]}])", r"\1", c)
        try:
            return json.loads(c)
        except Exception:
            return None


def FindJsonBlocks(html_text):
    soup = BeautifulSoup(html_text or "", "html.parser")
    out = []
    for s in soup.find_all("script"):
        t = s.string or ""
        if not t or ("__typename" not in t and "timelineItems" not in t):
            continue
        i = t.find("{")
        if i == -1:
            continue
        sub = t[i:]
        for end in range(len(sub), 0, -1):
            cand = sub[:end]
            loaded = TryLoadJson(cand)
            if loaded is not None:
                out.append(loaded)
                break
    return out


def HtmlToMarkdownLite(html_snip):
    if not html_snip:
        return ""
    soup = BeautifulSoup(html_snip, "html.parser")

    def node_text(n):
        if isinstance(n, NavigableString):
            return str(n)
        if not isinstance(n, Tag):
            return ""
        name = n.name.lower()
        if name in ("strong", "b"):
            return f"**{''.join(node_text(c) for c in n.children).strip()}**"
        if name in ("em", "i"):
            return f"*{''.join(node_text(c) for c in n.children).strip()}*"
        if name == "a":
            href = n.get("href") or ""
            inner = ''.join(node_text(c) for c in n.children).strip() or href
            href = NormalizeUrl(href)
            return f"[{inner}]({href})" if href else inner
        if name == "code":
            if n.parent and getattr(n.parent, "name", "").lower() == "pre":
                return ''.join(node_text(c) for c in n.children)
            return f"`{''.join(node_text(c) for c in n.children).strip()}`"
        if name == "pre":
            code = n.find("code")
            block = code.get_text() if code else n.get_text()
            block = block.rstrip("\n")
            return "\n```\n" + block + "\n```\n"
        if name == "br":
            return "\n"
        if name in ("p", "div"):
            inner = ''.join(node_text(c) for c in n.children).strip()
            return inner + ("\n\n" if inner else "")
        if name in ("ul", "ol"):
            lines = []
            is_ordered = (name == "ol")
            for idx, li in enumerate(n.find_all("li", recursive=False), start=1):
                prefix = f"{idx}. " if is_ordered else "- "
                li_text = ''.join(node_text(c) for c in li.children).strip().replace("\n", " ")
                lines.append(prefix + li_text)
            return "\n".join(lines) + ("\n\n" if lines else "")
        return ''.join(node_text(c) for c in n.children)

    md = ''.join(node_text(c) for c in soup.children).strip()
    md = re.sub(r"\n{3,}", "\n\n", md)
    return md


def MarkdownFromNode(node):
    if not node:
        return ""
    if isinstance(node, dict):
        body = node.get("body")
        if isinstance(body, str) and body.strip():
            return body
        alt = node.get("bodyHTML") or node.get("message") or node.get("title") or node.get("messageHeadlineHTML") or node.get("body_html") or node.get("pullTitleHTML") or node.get("titleHTML")
        if isinstance(alt, str) and alt.strip():
            return HtmlToMarkdownLite(alt)
        return ""
    if isinstance(node, str):
        if "<" in node and ">" in node:
            return HtmlToMarkdownLite(node)
        return node
    return ""


def ExtractRefFromNode(node):
    if not isinstance(node, dict):
        return ("", "", "", "")
    url = node.get("url") or node.get("html_url") or node.get("resourcePath") or node.get("href") or ""
    for k in ("innerSource", "source", "subject", "reference", "target", "closer", "pullRequest", "pull_request", "commit"):
        nested = node.get(k)
        if isinstance(nested, dict):
            nested_url = nested.get("url") or nested.get("html_url") or nested.get("resourcePath") or nested.get("href")
            if nested_url:
                url = nested_url
                node = nested
                break
    url = NormalizeUrl(url)
    title = ""
    title_candidates = [node.get("pullTitleHTML"), node.get("titleHTML"), node.get("nameHTML"), node.get("title"), node.get("name"), node.get("message"), node.get("messageHeadlineHTML"), node.get("bodyHTML"), node.get("messageBodyHTML"), node.get("body")]
    for cand in title_candidates:
        if isinstance(cand, str) and cand.strip():
            title = CleanHtmlToText(cand)
            break
    number = ""
    nfield = node.get("number")
    if isinstance(nfield, (int, str)):
        number = str(nfield)
    if not number and url:
        m = re.search(r"/(?:pull|pulls|issues)/(\d+)", url)
        if m:
            number = m.group(1)
    typename = (node.get("__typename") or "").lower() if isinstance(node.get("__typename"), str) else ""
    if "/pull/" in url or typename.startswith("pull"):
        rtype = "pull"
    elif "/commit/" in url or typename.startswith("commit"):
        rtype = "commit"
    elif "/issues/" in url or typename.startswith("issue"):
        rtype = "issue"
    else:
        rtype = ""
    return (rtype, url or "", title or "", number or "")


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
               ref_type="", ref_url="", ref_title="", ref_number="",
               milestone="", target_name="", mentioned_by_type="", mentioned_by_url=""):
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
        "ref_number": ref_number,
        "milestone": milestone,
        "target_name": target_name,
        "mentioned_by_type": mentioned_by_type,
        "mentioned_by_url": mentioned_by_url,
    }


def ExtractEventsFromJson(j):
    events = []

    def rec(o, context_mention_url=""):
        if isinstance(o, dict):
            ctx = o.get("url") or o.get("html_url") or o.get("resourcePath") or context_mention_url or ""
            ctx = NormalizeUrl(ctx)
            tn = o.get("__typename", "") or ""
            if tn == "IssueComment" or o.get("bodyHTML") or o.get("body"):
                actor = o.get("author") or o.get("actor") or {}
                person = actor.get("login") if isinstance(actor, dict) else ""
                text_md = MarkdownFromNode(o) or MarkdownFromNode(o.get("bodyHTML") or o.get("body") or "")
                events.append(BuildEvent("Comment", text_md, o.get("createdAt") or "", person, o.get("url") or ctx))
            if "ClosedEvent" in tn:
                actor = o.get("actor") or {}
                person = actor.get("login") if isinstance(actor, dict) else ""
                closer = o.get("closer") or {}
                close_reason_md = MarkdownFromNode(o) or MarkdownFromNode(closer) or MarkdownFromNode(o.get("bodyHTML") or o.get("closingComment") or o.get("closingCommitMessage") or "")
                ref_t, ref_u, ref_title, ref_n = ExtractRefFromNode(closer)
                events.append(BuildEvent("ClosedEvent", close_reason_md, o.get("createdAt") or "", person, o.get("url") or "", close_reason=CleanHtmlToText(o.get("bodyHTML") or o.get("closingComment") or o.get("closingCommitMessage") or ""), ref_type=ref_t, ref_url=ref_u, ref_title=ref_title, ref_number=ref_n))
            if "MilestonedEvent" in tn or "Milestoned" in tn:
                actor = o.get("actor") or {}
                person = actor.get("login") if isinstance(actor, dict) else ""
                mnode = o.get("milestone") or {}
                mtitle = mnode.get("title") or o.get("milestoneTitle") or ""
                events.append(BuildEvent("Milestoned", f"added to milestone: {mtitle}", o.get("createdAt") or "", person, o.get("url") or "", milestone=mtitle))
            if "DemilestonedEvent" in tn or "Demilestoned" in tn:
                actor = o.get("actor") or {}
                person = actor.get("login") if isinstance(actor, dict) else ""
                mnode = o.get("milestone") or {}
                mtitle = mnode.get("title") or o.get("milestoneTitle") or ""
                events.append(BuildEvent("Demilestoned", f"removed from milestone: {mtitle}", o.get("createdAt") or "", person, o.get("url") or "", milestone=mtitle))
            if "ReferencedEvent" in tn or "CrossReferencedEvent" in tn:
                actor = o.get("actor") or {}
                person = actor.get("login") if isinstance(actor, dict) else ""
                subject_node = o.get("innerSource") or o.get("source") or o.get("commit") or o.get("subject") or o.get("reference") or o.get("target") or {}
                ref_t, ref_u, ref_title, ref_n = ExtractRefFromNode(subject_node if isinstance(subject_node, dict) else {})
                mention_type, mention_url = ("", "")
                if INCLUDE_MENTIONED_BY:
                    mention_type, mention_url = MentionContextFromUrl(ctx)
                text_md = ref_title or ref_u or MarkdownFromNode(o) or ""
                evtype = "MentionedByIssueOrPR" if ref_t in ("pull", "issue") else "MentionedByCommit" if ref_t == "commit" else "ReferencedEvent"
                events.append(BuildEvent(evtype, text_md, o.get("createdAt") or "", person, ref_u or o.get("url") or "", ref_type=ref_t, ref_url=ref_u, ref_title=ref_title, ref_number=ref_n, mentioned_by_type=mention_type, mentioned_by_url=mention_url))
            if "LabeledEvent" in tn:
                actor = o.get("actor") or {}
                label = (o.get("label") or {}).get("name") or o.get("labelName") or ""
                events.append(BuildEvent("LabelAdded", f"label added: {label}", o.get("createdAt") or "", actor.get("login") if isinstance(actor, dict) else "", o.get("url") or "", label=label))
            if "UnlabeledEvent" in tn:
                actor = o.get("actor") or {}
                label = (o.get("label") or {}).get("name") or o.get("labelName") or ""
                events.append(BuildEvent("LabelRemoved", f"label removed: {label}", o.get("createdAt") or "", actor.get("login") if isinstance(actor, dict) else "", o.get("url") or "", label=label))
            for v in o.values():
                rec(v, context_mention_url=ctx)
        elif isinstance(o, list):
            for it in o:
                rec(it, context_mention_url=context_mention_url)

    rec(j)
    return events


def ParseDomForEvents(html_text):
    page_canonical, page_type = ExtractPageCanonical(html_text)
    soup = BeautifulSoup(html_text or "", "html.parser")

    def _normalize_author_link(anchor):
        if not anchor:
            return ("", "")
        profile_span = anchor.select_one("[class^='row-module__eventProfileReference'], [class*='eventProfileReference']")
        author = profile_span.get_text(strip=True) if profile_span and profile_span.get_text(strip=True) else anchor.get_text(" ", strip=True)
        href = anchor.get("href", "") or ""
        return (author, NormalizeUrl(href))

    def _find_pr_anchor_and_href(container):
        a = container.select_one("a[class^='IssueLink-module__issueLinkAnchor'], a[class*='IssueLink-module__issueLinkAnchor'], a[href*='/pull/'], a[href*='/commit/'], a[href*='/issues/']")
        if not a:
            a = container.select_one("a[data-hovercard-url], a[data-url]")
        href = a.get("href") or a.get("data-url") or a.get("data-hovercard-url") if a else ""
        return (a, NormalizeUrl(href))

    events = []
    for c in soup.select(".js-comment-container, .timeline-comment, .js-timeline-item, .Comment, .Timeline-Item"):
        actor_anchor = c.select_one("a[class^='row-module__eventActorLink'], a[class*='row-module__eventActorLink'], a.author, a.Link--primary, .author")
        author, author_url = _normalize_author_link(actor_anchor)
        rt = c.select_one("relative-time, time-ago, time, .js-relative-date")
        date = rt["datetime"] if rt and rt.has_attr("datetime") else ""
        body_elem = c.select_one("[class^='row-module__timelineBodyContent'], [class*='row-module__timelineBodyContent'], .comment-body, .markdown-body, .js-comment-body")
        parent_html = str(body_elem) if body_elem else ""
        parent_text_md = MarkdownFromNode(parent_html) if parent_html else ""
        lower = c.get_text("").lower()

        cross_list = c.select_one("[class^='CrossReferencedEvent-module__crossReferencesList'], [class*='CrossReferencedEvent-module__crossReferencesList'], .cross-references, section[aria-label='Issues mentioned']")
        if cross_list:
            for li in cross_list.select("li, [class^='IssueLink-module__issueContainer'], [class*='IssueLink-module__issueContainer'], a"):
                pr_link_elem, pr_href = _find_pr_anchor_and_href(li)
                if not pr_link_elem:
                    possible = li.find(lambda tag: tag.name == "a" and ("/pull/" in (tag.get("href") or "") or "/commit/" in (tag.get("href") or "") or "/issues/" in (tag.get("href") or "")))
                    if possible:
                        pr_link_elem = possible
                        pr_href = NormalizeUrl(possible.get("href") or "")
                if not pr_link_elem and not pr_href:
                    continue
                title_elem = pr_link_elem.select_one("bdi, .markdown-title, .IssueLink-module__issueTitleContainer--jwBZJ") if pr_link_elem else None
                ref_title = CleanHtmlToText(str(title_elem)) if title_elem else CleanHtmlToText(pr_link_elem.get_text(" ", strip=True) if pr_link_elem else li.get_text(" ", strip=True))
                repo_span = li.select_one("[class^='IssueLink-module__repoIssueNumber'], [class*='repoIssueNumber'], .IssueLink-module__repoIssueNumber--c52hS, .IssueLink-module__repoIssueNumber")
                span_text = repo_span.get_text(strip=True) if repo_span else ""
                if span_text:
                    ref_title = f"{ref_title} {span_text.strip()}".strip()
                ref_number = ""
                if span_text:
                    mspan = re.search(r"(\d+)$", span_text)
                    if mspan:
                        ref_number = mspan.group(1)
                if not ref_number:
                    m = re.search(r"/(?:pull|pulls|issues)/(\d+)", pr_href or "")
                    if m:
                        ref_number = m.group(1)
                ref_type = "pull" if "/pull/" in pr_href else "commit" if "/commit/" in pr_href else "issue" if "/issues/" in pr_href else ""
                mention_type, mention_url = MentionContextFromUrl(pr_href) if INCLUDE_MENTIONED_BY else ("", "")
                evtype = "MentionedByIssueOrPR" if ref_type in ("pull", "issue") else "MentionedByCommit"
                text_md = ref_title or parent_text_md or CleanHtmlToText(li.get_text())
                events.append(BuildEvent(evtype, text_md, date, author, pr_href, ref_type=ref_type, ref_url=pr_href, ref_title=ref_title, ref_number=ref_number, mentioned_by_type=mention_type, mentioned_by_url=mention_url))
            continue

        if "closed" in lower or "merged" in lower:
            pr_link = c.select_one("a[class^='IssueLink-module__issueLinkAnchor'], a[class*='IssueLink-module__issueLinkAnchor'], a[href*='/pull/'], a[href*='/commit/']")
            ref_url = NormalizeUrl(pr_link.get("href")) if pr_link else ""
            ref_title = CleanHtmlToText(pr_link.get_text(" ", strip=True)) if pr_link else ""
            ref_number = ""
            m = re.search(r"/pull/(\d+)", ref_url or "")
            if m:
                ref_number = m.group(1)
            ref_type = "pull" if "/pull/" in ref_url else "commit" if "/commit/" in ref_url else ""
            text_md = parent_text_md or CleanHtmlToText(c.get_text())
            events.append(BuildEvent("ClosedEvent", text_md, date, author, author_url, close_reason=CleanHtmlToText(parent_text_md or c.get_text()), ref_type=ref_type, ref_url=ref_url, ref_title=ref_title, ref_number=ref_number))
            continue

        if "added to milestone" in lower or ("added this to the" in lower and "milestone" in lower):
            m_elems = c.select(".milestone-link, a[href*='/milestone/'], .milestone")
            mname = m_elems[0].get_text(strip=True) if m_elems else ""
            events.append(BuildEvent("Milestoned", f"added to milestone: {mname}", date, author, author_url, milestone=mname))
            continue

        if "removed from milestone" in lower or ("removed this from the" in lower and "milestone" in lower):
            m_elems = c.select(".milestone-link, a[href*='/milestone/'], .milestone")
            mname = m_elems[0].get_text(strip=True) if m_elems else ""
            events.append(BuildEvent("Demilestoned", f"removed from milestone: {mname}", date, author, author_url, milestone=mname))
            continue

        if "assigned" in lower or "unassigned" in lower:
            ass_link = c.select_one("a.assignee, .assignee a, a[href*='/'] .css-truncate-target, a[href*='/']")
            assignee = ass_link.get_text(strip=True) if ass_link else ""
            by_link = c.select_one("a.author, a.Link--primary, a[class^='row-module__eventActorLink'], a[class*='row-module__eventActorLink']")
            assigner = by_link.get_text(strip=True) if by_link else ""
            action = "Assigned" if "assigned" in lower and "unassigned" not in lower else "Unassigned"
            events.append(BuildEvent(action, CleanHtmlToText(c.get_text()), date, author, author_url, assignee=assignee, assigner=assigner))
            continue

        if ("labeled this" in lower or ("added the" in lower and "label" in lower)) and "label" in lower:
            labels = [le.get_text(strip=True) for le in c.select(".IssueLabel, .labels .IssueLabel, .Label")]
            events.append(BuildEvent("LabelAdded", f"label added: {', '.join(labels)}", date, author, author_url, label=", ".join(labels)))
            continue

        if (("removed the" in lower and "label" in lower) or "unlabeled" in lower):
            labels = [le.get_text(strip=True) for le in c.select(".IssueLabel, .labels .IssueLabel, .Label")]
            events.append(BuildEvent("LabelRemoved", f"label removed: {', '.join(labels)}", date, author, author_url, label=", ".join(labels)))
            continue

        if parent_text_md.strip():
            events.append(BuildEvent("Comment", parent_text_md, date, author, author_url))

    soup_top = BeautifulSoup(html_text or "", "html.parser")
    sidebar_labels = [l.get_text(strip=True) for l in soup_top.select(".js-issue-labels .IssueLabel, .labels .IssueLabel, .gh-header-meta .IssueLabel")]
    if sidebar_labels:
        events.append(BuildEvent("LabelSnapshot", ", ".join(sidebar_labels), "", "", "", label=", ".join(sidebar_labels)))
    sidebar_m = soup_top.select_one(".gh-header-meta .milestone a, .js-issue-milestone .milestone a, a[href*='/milestone/']")
    if sidebar_m:
        mname = sidebar_m.get_text(strip=True)
        events.append(BuildEvent("MilestoneSnapshot", mname, "", "", "", milestone=mname))
    return events


def DedupeEvents(events):
    seen = set()
    out = []
    for e in events:
        key = (e.get("event_type"), (e.get("text") or "")[:200], e.get("date"), e.get("person"), e.get("ref_type"), e.get("ref_url"), e.get("milestone"), e.get("mentioned_by_type"), e.get("mentioned_by_url"))
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def WriteCsv(events, html_path, out_dir):
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / (html_path.stem + ".csv")
    fields = ["event_type", "text", "date", "person", "url", "label", "close_reason", "assignee", "assigner", "ref_type", "ref_url", "ref_title", "ref_number", "milestone", "target_name", "mentioned_by_type", "mentioned_by_url"]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for ev in events:
            w.writerow({k: ev.get(k, "") for k in fields})
    print("Wrote", len(events), "rows ->", out_path)


def ProcessHtmlFileToCsv(html_path, out_dir):
    html_text = html_path.read_text(encoding="utf-8", errors="ignore")
    events = []
    for j in FindJsonBlocks(html_text):
        events.extend(ExtractEventsFromJson(j))
    if not events:
        events = ParseDomForEvents(html_text)
    events = DedupeEvents(events)
    WriteCsv(events, html_path, out_dir)


def main():
    for p in INDIR.glob("*.html"):
        try:
            ProcessHtmlFileToCsv(p, OUTDIR)
        except Exception as e:
            print("ERROR", p, e)


if __name__ == "__main__":
    main()
