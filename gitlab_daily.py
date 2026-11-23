#!/usr/bin/env python3
"""
gitlab_daily.py - Auto-generate daily notes from GitLab data

Uses GitLab GraphQL API for efficient data fetching, with REST fallback
for issue-MR links. Supports plain markdown and Obsidian formatting.

Requires the `glab` CLI to be installed and authenticated.

Usage:
    python gitlab_daily.py                     # Generate today's note (plain)
    python gitlab_daily.py --format obsidian   # Obsidian callouts + frontmatter
    python gitlab_daily.py --keep              # Save a snapshot on first run
    python gitlab_daily.py --init              # Create a config file
    python gitlab_daily.py --stdout            # Print to stdout
"""

import json
import re
import subprocess
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

CONFIG_FILENAME = ".daily-note.json"
STATE_FILENAME = ".daily-note-state.json"
CACHE_DIR_NAME = ".daily-note-cache"
CACHE_TTL = 300  # 5 minutes
STORE_TTL = 30 * 24 * 3600  # per-MR thread store
SNIPPET_LEN = 80
PER_PAGE = 100
RATE_LIMIT_PER_MINUTE = 100  # gitlab.com burst limit per user

DEFAULT_CONFIG = {
    "base_dir": "daily-note",
    "tags": "",
    "gitlab_group": "",
    "on_hold_patterns": [],
    "on_hold_label": "On Hold",
    "stale_days": 7,
    "backlog_days": 14,
}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(config_path=None):
    if config_path is None:
        config_path = Path(CONFIG_FILENAME)
    else:
        config_path = Path(config_path)

    config = dict(DEFAULT_CONFIG)
    if config_path.exists():
        with open(config_path) as f:
            user_config = json.load(f)
        config.update(user_config)
        print(f"Loaded config from {config_path}", file=sys.stderr)
    else:
        print(f"No config found at {config_path}, using defaults", file=sys.stderr)
        print(f"Run with --init to generate a config file", file=sys.stderr)
    return config


def generate_config(config_path=None):
    if config_path is None:
        config_path = Path(CONFIG_FILENAME)
    else:
        config_path = Path(config_path)

    if config_path.exists():
        print(f"Config already exists at {config_path}", file=sys.stderr)
        return

    example = dict(DEFAULT_CONFIG)
    example["_comment"] = {
        "base_dir": "Directory where daily notes are saved (relative to script location)",
        "tags": "Frontmatter/tags line prepended to each note (leave empty to omit)",
        "gitlab_group": "GitLab group path for --team mode (e.g. 'my-org/my-project')",
        "on_hold_patterns": "Strings to match in MR/issue paths for 'on hold' items",
        "on_hold_label": "Section heading for on-hold items",
        "stale_days": "Days of inactivity before an MR counts as stale; a failed build on a stale MR is not shown in Act Now",
        "backlog_days": "Days of inactivity before an MR with no milestone moves to the folded Backlog block",
    }

    with open(config_path, "w") as f:
        json.dump(example, f, indent=2)
    print(f"Generated config at {config_path}", file=sys.stderr)
    print(f"Edit it to customize your daily notes.", file=sys.stderr)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

class FileCache:
    def __init__(self, base_dir, ttl=CACHE_TTL):
        self.cache_dir = Path(base_dir) / CACHE_DIR_NAME
        self.ttl = ttl

    def get(self, key):
        path = self.cache_dir / f"{key}.json"
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > self.ttl:
            return None
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    def set(self, key, data):
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        path = self.cache_dir / f"{key}.json"
        with open(path, "w") as f:
            json.dump(data, f)

    def clear(self):
        if self.cache_dir.exists():
            for p in self.cache_dir.iterdir():
                p.unlink()


# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

MR_FIELDS = """
    iid title webUrl draft
    reference
    projectId
    createdAt updatedAt
    conflicts
    shouldBeRebased
    mergeStatusEnum
    approved
    author { username id }
    approvedBy { nodes { username } }
    headPipeline { status }
    userNotesCount
    description
    milestone { title dueDate }
"""

MAIN_QUERY = """
query {
    currentUser {
        username
        id
        todos(state: pending, first: 100) {
            nodes {
                id action body createdAt
                targetUrl
                target {
                    ... on MergeRequest {
                        iid title webUrl state
                        headPipeline { status }
                    }
                    ... on Issue {
                        iid title webUrl state
                    }
                }
            }
        }
        authoredMergeRequests(state: opened, first: 100) {
            nodes { %s }
        }
        assignedMergeRequests(state: opened, first: 100) {
            nodes { %s }
        }
    }
}
""" % (MR_FIELDS, MR_FIELDS)

MERGED_QUERY = """
query {
    currentUser {
        authoredMergeRequests(state: merged, sort: UPDATED_DESC, first: 50) {
            nodes {
                iid title webUrl mergedAt updatedAt reference projectId
            }
        }
    }
}
"""

ISSUES_QUERY_TEMPLATE = """
{{
    issues(assigneeUsernames: ["{username}"], state: opened, first: 100) {{
        nodes {{
            iid title webUrl projectId createdAt
            labels {{ nodes {{ title }} }}
        }}
    }}
}}
"""


# Matches issue refs: `group/project#42`, `project#42` (same namespace) or `#42` (same project).
# Group 1: optional project path. Group 2: issue number.
_ISSUE_REF_RE = re.compile(r'(?<![\w/.-])((?:[\w.-]+/)*[\w.-]+)?#(\d+)\b')
_ISSUE_URL_RE = re.compile(r'(.*)/-/(?:issues|work_items)/(\d+)')


def _issue_ref_path(own_path, proj_part):
    """Full project path of an issue ref found in an MR of project `own_path`."""
    if not proj_part:
        return own_path
    if "/" in proj_part:
        return proj_part
    return f"{own_path.rsplit('/', 1)[0]}/{proj_part}"


def _issue_url_key(url):
    """Normalize an issue URL to (project_base, iid).

    GitLab serves the same issue under both /-/issues/N and /-/work_items/N;
    the GraphQL API returns the work_items form. Comparing raw URL strings
    misses duplicates, so key on the project base and iid instead.
    """
    m = _ISSUE_URL_RE.match(url)
    if m:
        return (m.group(1), m.group(2))
    return (url, "")

# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------

_MERGE_STATUS_MAP = {
    "MERGEABLE": "can_be_merged",
    "BROKEN_STATUS": "cannot_be_merged",
    "CHECKING": "checking",
    "UNCHECKED": "unchecked",
    "CI_MUST_PASS": "cannot_be_merged",
    "CI_STILL_RUNNING": "checking",
    "DISCUSSIONS_NOT_RESOLVED": "cannot_be_merged",
    "DRAFT_STATUS": "cannot_be_merged",
    "EXTERNAL_STATUS_CHECKS": "checking",
    "NOT_APPROVED": "cannot_be_merged",
    "NOT_OPEN": "cannot_be_merged",
    "POLICIES_DENIED": "cannot_be_merged",
}

_PIPELINE_STATUS_MAP = {
    "RUNNING": "running",
    "PENDING": "pending",
    "SUCCESS": "success",
    "FAILED": "failed",
    "CANCELED": "canceled",
    "CREATED": "created",
    "MANUAL": "manual",
    "WAITING_FOR_RESOURCE": "pending",
    "PREPARING": "pending",
    "SCHEDULED": "pending",
}


def _gid_to_int(gid):
    if isinstance(gid, int):
        return gid
    if isinstance(gid, str) and "/" in gid:
        return int(gid.rsplit("/", 1)[-1])
    return int(gid)


def _normalize_milestone(ms):
    return {"title": ms["title"], "due_date": ms.get("dueDate")} if ms else None


def _snippet(body):
    """First line of a note, short enough for one list line."""
    first = next((l.strip() for l in (body or "").splitlines() if l.strip()), "")
    if first.startswith("```suggestion"):
        return "code suggestion"
    first = first.replace("[", "(").replace("]", ")").replace("|", "/")
    return first if len(first) <= SNIPPET_LEN else first[:SNIPPET_LEN - 3] + "..."


def _normalize_mr(node):
    pipeline = node.get("headPipeline") or {}
    author = dict(node.get("author") or {})
    if author.get("id") is not None:
        author["id"] = _gid_to_int(author["id"])
    raw_status = pipeline.get("status", "")
    ref = node.get("reference", "")
    project_path = node.get("webUrl", "").split("/-/")[0].rstrip("/")
    repo_name = project_path.rsplit("/", 1)[-1] if project_path else "unknown"
    full_ref = f"{repo_name}{ref}" if ref else ""

    return {
        "iid": node["iid"],
        "title": node["title"],
        "web_url": node["webUrl"],
        "draft": node.get("draft", False),
        "work_in_progress": node.get("draft", False),
        "has_conflicts": node.get("conflicts", False),
        "should_be_rebased": node.get("shouldBeRebased", False),
        "merge_status": _MERGE_STATUS_MAP.get(node.get("mergeStatusEnum", ""), ""),
        "approved": node.get("approved", False),
        "approved_by": [a["username"] for a in (node.get("approvedBy") or {}).get("nodes", [])],
        "head_pipeline": {"status": _PIPELINE_STATUS_MAP.get(raw_status, raw_status.lower())} if raw_status else None,
        "project_id": _gid_to_int(node.get("projectId", 0)),
        "created_at": node.get("createdAt", ""),
        "updated_at": node.get("updatedAt", ""),
        "merged_at": node.get("mergedAt", ""),
        "user_notes_count": node.get("userNotesCount", 0),
        "references": {"full": full_ref},
        "author": author,
        "description": node.get("description", "") or "",
        "milestone": _normalize_milestone(node.get("milestone")),
    }


def _normalize_issue(node):
    labels = [n["title"] for n in (node.get("labels") or {}).get("nodes", [])]
    return {
        "iid": node["iid"],
        "title": node["title"],
        "web_url": node["webUrl"],
        "project_id": _gid_to_int(node.get("projectId", 0)),
        "created_at": node.get("createdAt", ""),
        "labels": labels,
    }


def _normalize_todo(node):
    target = node.get("target") or {}
    pipeline = target.get("headPipeline") or {}
    return {
        "id": _gid_to_int(node["id"]),
        "action_name": node.get("action", ""),
        "body": node.get("body", ""),
        "created_at": node.get("createdAt", ""),
        "target_url": node.get("targetUrl", ""),
        "target": {
            "iid": target.get("iid"),
            "title": target.get("title", node.get("body", "")),
            "web_url": target.get("webUrl", node.get("targetUrl", "")),
            "state": target.get("state", ""),
            "head_pipeline": {"status": _PIPELINE_STATUS_MAP.get(pipeline.get("status", ""), "")} if pipeline else None,
        },
    }


def _serialize_discussions(mr_discussions):
    """Convert sets to lists for JSON serialization."""
    result = {}
    for url, disc in mr_discussions.items():
        d = dict(disc)
        d["pending_authors"] = list(d.get("pending_authors", []))
        result[url] = d
    return result


def _deserialize_discussions(data):
    """Convert lists back to sets after JSON load."""
    result = {}
    for url, disc in data.items():
        d = dict(disc)
        d["pending_authors"] = set(d.get("pending_authors", []))
        result[url] = d
    return result


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

class PlainFormatter:
    """Plain markdown output."""

    FAILED_LABEL = "FAILED"
    APPROVED_LABEL = "approved"

    def _section(self, kind, title, content_lines, level=2, folded=False):
        """One block of the note: a heading here, a callout in Obsidian."""
        return [f"{'#' * level} {title}", *content_lines, ""]

    @staticmethod
    def _thread_lines(sync, mr, kinds):
        """One sub-line per thread that waits on me, with a link to the note."""
        out = []
        for t in sync.mr_discussions.get(mr["web_url"], {}).get("open_threads", []):
            if t["kind"] not in kinds:
                continue
            where = f" on `{t['path'].rsplit('/', 1)[-1]}`" if t.get("path") else ""
            out.append(f"    - @{t['author']}{where}: [{t['body'] or 'open'}]({mr['web_url']}#note_{t['note_id']})")
        return out

    def _review_line(self, sync, mr):
        ms = f" | milestone {mr['milestone']['title']}" if mr.get("milestone") else ""
        author = mr.get("author", {}).get("username", "?")
        return f"- [ ] [!{mr['iid']}]({mr['web_url']}): {mr['title']} — by @{author}{ms}"

    def _pipeline_status(self, mr):
        pipeline = mr.get("head_pipeline") or {}
        status = pipeline.get("status", "")
        labels = {
            "failed": "FAILED", "running": "running", "pending": "pending",
            "canceled": "canceled", "created": "pending", "manual": "manual",
        }
        return labels.get(status, "")

    def _approval_text(self, mr):
        if mr.get("draft") or not mr.get("approved_by"):
            return ""
        return f"{self.APPROVED_LABEL} by " + ", ".join("@" + a for a in mr["approved_by"])

    def format_mr_line(self, sync, mr):
        line = f"- [ ] [!{mr['iid']}]({mr['web_url']}): {mr['title']}"
        annotations = []

        if mr.get("milestone"):
            annotations.append(f"milestone {mr['milestone']['title']}")

        pipeline = self._pipeline_status(mr)
        if pipeline:
            annotations.append(pipeline)

        days = sync._get_staleness(mr)
        if days >= 3:
            annotations.append(f"idle {days}d")

        if mr.get("has_conflicts", False):
            annotations.append("conflicts")
        if mr.get("should_be_rebased", False):
            annotations.append("needs rebase")

        approval = self._approval_text(mr)
        if approval:
            annotations.append(approval)

        disc = sync.mr_discussions.get(mr["web_url"])
        if disc:
            if disc["pending"] > 0:
                authors = ", ".join("@" + a for a in sorted(disc["pending_authors"]))
                annotations.append(f"{disc['pending']} pending ({authors})")
            if disc.get("to_close", 0) > 0:
                annotations.append(f"{disc['to_close']} to close")
            if disc.get("comments", 0) > 0:
                annotations.append(f"{disc['comments']} comment(s)")
            if disc["answered"] > 0:
                annotations.append(f"{disc['answered']} answered")

        if annotations:
            line += f" — {' | '.join(annotations)}"
        return line

    def _format_issue_line(self, issue):
        labels = issue.get("labels", [])
        priority_labels = [l for l in labels if l.lower() in ["high", "critical", "urgent", "bug"]]
        label_text = f" [{', '.join(priority_labels)}]" if priority_labels else ""
        return f"- [ ] [#{issue['iid']}]({issue['web_url']}): {issue['title']}{label_text}"

    def _todo_issue_line(self, sync, issue):
        line = self._format_issue_line(issue)
        states = [m.get("state") for m in sync.issue_all_mrs.get(issue["web_url"], [])]
        statuses = [f"{states.count(s)} MR {s}" for s in ("merged", "closed") if states.count(s)]
        if statuses:
            line += f" — {', '.join(statuses)}"
        return line

    @staticmethod
    def _nav_line(sync, date):
        def link(d, label):
            return f"[[{sync.base_dir}/{d.strftime('%Y/%m-%b/%Y-%m-%d')}|{label}]]"
        return f"<< {link(date - timedelta(days=1), 'Yesterday')} | {link(date + timedelta(days=1), 'Tomorrow')} >>"

    def _render_header(self, sync, lines, date, stats):
        tags = sync.config.get("tags", "")
        if tags:
            lines += [tags, ""]
        lines += [f"# {date.strftime('%A, %B %d, %Y')}", self._nav_line(sync, date), ""]
        lines.append("## Summary")
        lines.append(
            f"- {stats['pending_threads']} pending threads"
            f" | {stats['to_close_threads']} to close"
            f" | {stats['build_failures']} build failures"
            f" | {stats['conflicts']} conflicts"
        )
        lines.append(
            f"- {stats['ready_count']} ready to merge"
            f" | {stats['stale_count']} need a ping"
            f" | {len(sync.mrs)} total open MRs"
            f" | avg age {stats['avg_age']}d"
        )
        if sync.recently_merged:
            lines.append(f"- {len(sync.recently_merged)} recently merged")
        if sync.team_mrs:
            lines.append(f"- {len(sync.team_mrs)} team MRs to review")
        lines.append("")

    def format_markdown(self, sync, date, todos, diff=None):
        cats = sync.categorize_mrs(todos)
        lines = []
        self._render_header(sync, lines, date, cats["stats"])

        if cats["review_requested"]:
            cl = [self._review_line(sync, mr) for mr in cats["review_requested"]]
            lines += self._section("todo", "Review Owed", cl)

        # Act Now: threads that wait on me, then builds that just failed
        cl = []
        for mr in cats["act_now"]:
            cl.append(self.format_mr_line(sync, mr))
            cl += self._thread_lines(sync, mr, ("pending", "comment"))
        cl += [self.format_mr_line(sync, mr) for mr in cats["failed"]]
        if cl:
            lines += self._section("danger", "Act Now", cl)

        if cats["close_mrs"]:
            cl = []
            for mr in cats["close_mrs"]:
                disc = sync.mr_discussions[mr["web_url"]]
                cl.append(f"- [ ] [!{mr['iid']}]({mr['web_url']}): {mr['title']} — {disc['to_close']} to close")
                cl += self._thread_lines(sync, mr, ("to_close",))
            lines += self._section("warning", "Threads to Close", cl)

        # Approved comes before new work: a rebase here is a merge
        for key, kind, title in (
            ("approved", "tip", "Approved"),
            ("todo_issues", "example", "To Do"),
            ("in_progress", "note", "In Progress"),
        ):
            if not cats[key]:
                continue
            if key == "todo_issues":
                cl = [self._todo_issue_line(sync, issue) for issue in cats[key]]
            else:
                cl = [self.format_mr_line(sync, mr) for mr in cats[key]]
            lines += self._section(kind, title, cl)

        lines += ["---", ""]
        self._render_issues(sync, lines, cats)
        if cats["backlog"]:
            days = sync.config.get("backlog_days", 14)
            cl = [self.format_mr_line(sync, mr) for mr in cats["backlog"]]
            lines += self._section(
                "abstract", f"Backlog ({len(cl)} MRs idle {days}d or more, no milestone)", cl, folded=True)
        self._render_on_hold(sync, lines, cats)
        self._render_team(sync, lines)
        self._render_changelog(sync, lines, diff)
        lines += ["## Notes", "- "]
        return "\n".join(lines)

    def _render_issues(self, sync, lines, cats):
        """Issues whose MRs still move. Each MR gets one full line; later mentions are short."""
        backlog_urls = {mr["web_url"] for mr in cats["backlog"]}
        live_urls = {mr["web_url"] for mr in sync.mrs} - backlog_urls
        shown = set()
        blocks = []

        for issue in cats["issues_with_mrs"]:
            linked = sync.issue_to_mrs.get(issue["web_url"], [])
            live = [mr for mr in linked if mr["web_url"] in live_urls]
            if not live:
                continue
            block = [f"**[#{issue['iid']}]({issue['web_url']}): {issue['title']}**"]
            block += [self.format_mr_line(sync, mr) for mr in live if mr["web_url"] not in shown]
            again = [f"[!{mr['iid']}]({mr['web_url']})" for mr in live if mr["web_url"] in shown]
            if again:
                block.append(f"- also listed above: {', '.join(again)}")
            parked = sum(1 for mr in linked if mr["web_url"] in backlog_urls)
            if parked:
                block.append(f"- {parked} more in the backlog")
            shown.update(mr["web_url"] for mr in live)
            blocks.append(block)

        orphans = [mr for mr in cats["waiting"] if mr["web_url"] not in shown]
        if orphans:
            block = ["**No linked issue:**"]
            by_repo = defaultdict(list)
            for mr in orphans:
                by_repo[sync._get_repo_short_name(mr)].append(mr)
            for repo in sorted(by_repo):
                block.append(f"*{repo}:*")
                block += [self.format_mr_line(sync, mr) for mr in by_repo[repo]]
            blocks.append(block)

        if not blocks:
            return
        lines += ["## Issues", ""]
        for block in blocks:
            lines += [*block, ""]

    def _render_on_hold(self, sync, lines, cats):
        on_hold_issues = [i for i in sync.filter_relevant_issues() if sync._is_on_hold(i)]
        if not cats["on_hold_mrs"] and not on_hold_issues:
            return
        cl = [self.format_mr_line(sync, mr) for mr in cats["on_hold_mrs"]]
        cl += [self._format_issue_line(issue) for issue in on_hold_issues]
        lines += self._section("quote", sync.config.get("on_hold_label", "On Hold"), cl)

    @staticmethod
    def _ci_passed(mr):
        pipeline = mr.get("head_pipeline") or {}
        status = pipeline.get("status", "")
        return status in ("success", "")

    def _render_team_mr_line(self, sync, mr, show_author=True):
        author = mr.get("author", {}).get("username", "?")
        title = mr["title"]
        if mr.get("draft") and not title.startswith("Draft:"):
            title = f"Draft: {title}"
        by = f" — by @{author}" if show_author else ""
        line = f"- [ ] [!{mr['iid']}]({mr['web_url']}): {title}{by}"

        annotations = []
        pipeline = self._pipeline_status(mr)
        if pipeline:
            annotations.append(pipeline)

        days = sync._get_staleness(mr)
        if days >= 3:
            annotations.append(f"idle {days}d")

        approved_by = mr.get("approved_by", [])
        if approved_by:
            names = ", ".join("@" + a for a in approved_by)
            annotations.append(f"approved by {names}")

        notes = mr.get("user_notes_count", 0)
        if notes > 0:
            annotations.append(f"{notes} note(s)")

        if annotations:
            line += " | " + " | ".join(annotations)
        return line

    def _group_by_milestone(self, team_mrs):
        groups = defaultdict(list)
        for mr in team_mrs:
            ms = mr.get("milestone")
            ms_title = ms["title"] if ms else "No milestone"
            groups[ms_title].append(mr)
        def sort_key(title):
            if title == "No milestone":
                return (2, "")
            for mr in team_mrs:
                ms = mr.get("milestone")
                if ms and ms["title"] == title and ms.get("due_date"):
                    return (0, ms["due_date"])
            return (1, title)
        return sorted(groups.items(), key=lambda x: sort_key(x[0]))

    def _render_team(self, sync, lines):
        if not sync.team_mrs:
            return
        lines += ["---", ""]
        non_draft = [mr for mr in sync.team_mrs if not mr.get("draft")]
        drafts = [mr for mr in sync.team_mrs if mr.get("draft")]
        ready = [mr for mr in non_draft if self._ci_passed(mr)]
        ci_pending = [mr for mr in non_draft if not self._ci_passed(mr)]

        milestones = []
        for ms_title, mrs in self._group_by_milestone(ready):
            by_author = defaultdict(list)
            for mr in mrs:
                by_author[mr.get("author", {}).get("username", "?")].append(mr)
            cl = []
            for author in sorted(by_author):
                cl.append(f"**@{author}** ({len(by_author[author])})")
                cl += [self._render_team_mr_line(sync, mr, show_author=False) for mr in by_author[author]]
            due = next((mr["milestone"].get("due_date") for mr in mrs if mr.get("milestone")), None)
            suffix = f", due {due}" if due else ""
            milestones.append((f"{ms_title} ({len(mrs)}{suffix})", cl))

        others = []
        if ci_pending:
            others.append((f"CI Pending ({len(ci_pending)})", [self._render_team_mr_line(sync, mr) for mr in ci_pending]))
        if drafts:
            others.append((f"Drafts ({len(drafts)})", [self._render_team_mr_line(sync, mr) for mr in drafts]))
        self._emit_team(lines, milestones, others)

    def _emit_team(self, lines, milestones, others):
        lines.append("## To Review")
        for title, cl in milestones + others:
            lines += self._section("", title, cl, level=3)

    def _render_changelog(self, sync, lines, diff):
        if sync.recently_merged:
            lines.append("## Recently Merged")
            for mr in sync.recently_merged:
                merged_at = mr.get("merged_at", "")[:10]
                lines.append(f"- [!{mr['iid']}]({mr['web_url']}): {mr['title']} ({merged_at})")
            lines.append("")

        if diff and diff["days_back"] <= 2:
            has_changes = (
                diff["new_mrs"] or diff["merged_mrs"] or diff["gone_mr_urls"]
                or diff["new_issues"] or diff["resolved_issues"]
            )
            if has_changes:
                label = "Yesterday" if diff["days_back"] == 1 else f"{diff['prev_date'].strftime('%b %d')}"
                lines.append(f"## Changes Since {label}")
                for mr in diff.get("merged_mrs", []):
                    lines.append(f"- Merged: [!{mr['iid']}]({mr['web_url']}): {mr['title']}")
                if diff["gone_mr_urls"]:
                    lines.append(f"- {len(diff['gone_mr_urls'])} MR(s) closed/removed")
                for mr in diff.get("new_mrs", []):
                    lines.append(f"- New: [!{mr['iid']}]({mr['web_url']}): {mr['title']}")
                if diff["resolved_issues"]:
                    lines.append(f"- {len(diff['resolved_issues'])} issue(s) resolved")
                for issue in diff.get("new_issues", []):
                    lines.append(f"- New issue: [#{issue['iid']}]({issue['web_url']}): {issue['title']}")
                lines.append("")


class ObsidianFormatter(PlainFormatter):
    """Obsidian-flavored output with callouts, frontmatter, and styled status."""

    FAILED_LABEL = "**`FAILED`**"
    APPROVED_LABEL = "**approved**"

    def _section(self, kind, title, content_lines, level=2, folded=False):
        fold = "-" if folded else ""
        return [f"> [!{kind}]{fold} {title}", *(f"> {line}" for line in content_lines), ""]

    def _pipeline_status(self, mr):
        pipeline = mr.get("head_pipeline") or {}
        status = pipeline.get("status", "")
        mapping = {
            "failed": "**`FAILED`**", "running": "`running`", "pending": "`pending`",
            "canceled": "`canceled`", "created": "`pending`", "manual": "`manual`",
        }
        return mapping.get(status, "")

    def _render_header(self, sync, lines, date, stats):
        lines += [
            "---",
            "type: daily-note",
            f"date: {date.strftime('%Y-%m-%d')}",
            f"open_mrs: {len(sync.mrs)}",
            f"pending_threads: {stats['pending_threads']}",
            f"to_close: {stats['to_close_threads']}",
            f"build_failures: {stats['build_failures']}",
            f"conflicts: {stats['conflicts']}",
            f"ready_to_merge: {stats['ready_count']}",
            f"stale: {stats['stale_count']}",
            f"avg_mr_age: {stats['avg_age']}",
        ]
        if sync.recently_merged:
            lines.append(f"recently_merged: {len(sync.recently_merged)}")
        lines += ["---", ""]

        tags = sync.config.get("tags", "")
        if tags:
            lines += [tags, ""]
        lines += [f"# {date.strftime('%A, %B %d, %Y')}", self._nav_line(sync, date), ""]

        total = len(sync.mrs)
        if total > 0:
            filled = round(stats["ready_count"] / total * 20)
            bar = "=" * filled + "-" * (20 - filled)
            lines.append(f"`[{bar}]` {stats['ready_count']}/{total} ready to merge")
        lines.append("")

        lines += [
            "| Pending | To close | Failures | Conflicts | Stale | Avg age |",
            "|:---:|:---:|:---:|:---:|:---:|:---:|",
            f"| {stats['pending_threads']} | {stats['to_close_threads']}"
            f" | {stats['build_failures']} | {stats['conflicts']}"
            f" | {stats['stale_count']} | {stats['avg_age']}d |",
            "",
        ]

    def _emit_team(self, lines, milestones, others):
        for title, cl in milestones:
            lines += self._section("info", f"To Review: {title}", cl)
        for title, cl in others:
            lines += self._section("abstract", title, cl)


FORMATTERS = {"plain": PlainFormatter, "obsidian": ObsidianFormatter}


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class GitLabSync:
    def __init__(self, config=None, auto_cleanup=True, format="plain"):
        self.config = config or DEFAULT_CONFIG
        self.todos = []
        self.mrs = []
        self.issues = []
        self.recently_merged = []
        self.auto_cleanup = auto_cleanup
        self.base_dir = Path(self.config["base_dir"])
        self.username = None
        self.user_id = None
        self.mr_discussions = {}
        self.failed_calls = 0
        self.store_hits = 0
        self.review_state = {}
        self._call_times = deque()
        self.issue_to_mrs = defaultdict(list)
        self.issue_all_mrs = defaultdict(list)
        self.mr_to_issues = defaultdict(list)
        self.team_mrs = []
        self.on_hold_patterns = [p.lower() for p in self.config.get("on_hold_patterns", [])]
        self.include_team = False
        self.formatter = FORMATTERS[format]()
        self.cache = FileCache(self.base_dir)

    # -- Paths --

    def get_daily_note_path(self, date, suffix=""):
        folder = self.base_dir / date.strftime("%Y") / date.strftime("%m-%b")
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f"{date.strftime('%Y-%m-%d')}{suffix}.md"

    # -- Note merging --

    def read_existing_note(self, filepath):
        if not filepath.exists():
            return None
        with open(filepath, "r") as f:
            content = f.read()
        notes_match = re.search(r"^## Notes\s*\n(.*)$", content, re.MULTILINE | re.DOTALL)
        if notes_match:
            return notes_match.group(1).strip()
        return None

    def merge_with_existing(self, new_content, existing_notes):
        if not existing_notes:
            return new_content
        notes_pattern = r"^## Notes\s*\n- \s*$"
        if re.search(notes_pattern, new_content, re.MULTILINE):
            new_content = re.sub(
                notes_pattern, lambda _: f"## Notes\n{existing_notes}",
                new_content, flags=re.MULTILINE,
            )
        return new_content

    # -- Shell helpers --

    def _throttle(self):
        """Keep glab calls under the per-minute limit; sleep when the window is full."""
        now = time.monotonic()
        while self._call_times and now - self._call_times[0] >= 60:
            self._call_times.popleft()
        if len(self._call_times) >= RATE_LIMIT_PER_MINUTE:
            wait = 60 - (now - self._call_times.popleft())
            print(f"Rate limit window full, waiting {wait:.0f}s", file=sys.stderr)
            time.sleep(wait)
        self._call_times.append(time.monotonic())

    def run_command(self, cmd, retries=5, retry_delay=5):
        for attempt in range(retries):
            self._throttle()
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                return result.stdout.strip()
            except subprocess.CalledProcessError as e:
                rate_limited = "HTTP 429" in e.stderr
                transient = rate_limited or "timed out" in e.stderr
                if transient and attempt < retries - 1:
                    delay = retry_delay * 2**attempt
                    reason = "Rate limited" if rate_limited else "Timeout"
                    print(f"{reason}, retrying in {delay}s ({attempt + 1}/{retries - 1}): {' '.join(cmd)}", file=sys.stderr)
                    time.sleep(delay)
                    continue
                if transient:
                    self.failed_calls += 1
                print(f"Error running command: {' '.join(cmd)}", file=sys.stderr)
                print(f"Error: {e.stderr}", file=sys.stderr)
                return None
        return None

    def run_graphql(self, query, retries=3, retry_delay=5):
        # glab exits 1 when the response has "errors" but still prints the body.
        # GitLab puts partial data next to per-field timeout errors; keep it.
        cmd = ["glab", "api", "graphql", "-f", f"query={query}"]
        for attempt in range(retries):
            self._throttle()
            result = subprocess.run(cmd, capture_output=True, text=True)
            try:
                body = json.loads(result.stdout)
            except json.JSONDecodeError:
                body = {}
            errors = body.get("errors") or []
            timeouts = [e for e in errors if "Timeout" in str(e.get("message", ""))]
            if timeouts and attempt < retries - 1:
                print(f"GraphQL timeout on {len(timeouts)} fields, retrying ({attempt + 1}/{retries - 1})", file=sys.stderr)
                time.sleep(retry_delay)
                continue
            for err in errors:
                if err not in timeouts:
                    print(f"GraphQL error: {err.get('message', err)}", file=sys.stderr)
            if timeouts:
                print(f"GraphQL timeout on {len(timeouts)} fields, using partial data", file=sys.stderr)
            data = body.get("data")
            if not data:
                print("Error running command: glab api graphql", file=sys.stderr)
                print(f"Error: {result.stderr}", file=sys.stderr)
            return data

    # -- Todo management --

    def dismiss_todo(self, todo_id):
        print(f"Dismissing todo {todo_id}...", file=sys.stderr)
        output = self.run_command(
            ["glab", "api", "-X", "POST", f"todos/{todo_id}/mark_as_done"]
        )
        return output is not None

    # -- Data fetching --

    def _cache_set(self, key, data, failed_before):
        """Skip the cache when calls were lost to throttling, so the next run refetches."""
        lost = self.failed_calls - failed_before
        if lost:
            print(f"  {lost} calls lost to throttling, not caching {key}", file=sys.stderr)
            return
        self.cache.set(key, data)

    def fetch_main(self):
        cached = self.cache.get("main")
        if cached:
            print("Using cached main data...", file=sys.stderr)
            self.username = cached["username"]
            self.user_id = cached["user_id"]
            self.todos = cached["todos"]
            self.mrs = cached["mrs"]
            self.mr_discussions = _deserialize_discussions(cached["mr_discussions"])
            print(f"User: {self.username} | {len(self.todos)} todos | {len(self.mrs)} MRs", file=sys.stderr)
            return

        print("Fetching user, todos, and MRs (GraphQL)...", file=sys.stderr)
        data = self.run_graphql(MAIN_QUERY)
        if not data:
            sys.exit("GraphQL main query failed, not writing an empty note")

        user = data["currentUser"]
        self.username = user["username"]
        self.user_id = _gid_to_int(user["id"])

        # Fall back to long-lived identity cache when GraphQL times out username.
        identity_cache = FileCache(self.base_dir, ttl=7 * 24 * 3600)
        if not self.username:
            cached_id = identity_cache.get("identity")
            if cached_id:
                self.username = cached_id["username"]
                self.user_id = self.user_id or cached_id["user_id"]
                print(f"User.username timed out, using cached identity: {self.username}", file=sys.stderr)
        if self.username:
            identity_cache.set("identity", {"username": self.username, "user_id": self.user_id})

        print(f"User: {self.username} (ID: {self.user_id})", file=sys.stderr)

        todos_data = user.get("todos") or {"nodes": []}
        self.todos = [_normalize_todo(t) for t in todos_data["nodes"] if t is not None]
        print(f"Found {len(self.todos)} todos", file=sys.stderr)

        authored = user.get("authoredMergeRequests") or {"nodes": []}
        assigned = user.get("assignedMergeRequests") or {"nodes": []}
        mr_fields_timed_out = user.get("authoredMergeRequests") is None or user.get("assignedMergeRequests") is None
        if mr_fields_timed_out:
            stale = FileCache(self.base_dir, ttl=STORE_TTL).get("main")
            if not stale:
                sys.exit("MR fields timed out and no earlier MR data is cached, not writing an empty note")
            self.mrs = stale["mrs"]
            self.mr_discussions = _deserialize_discussions(stale["mr_discussions"])
            print(f"Warning: MR fields timed out, using {len(self.mrs)} MRs from an earlier run", file=sys.stderr)
            return
        mrs_by_key = {}
        for node in authored["nodes"]:
            if node is None:
                continue
            mr = _normalize_mr(node)
            mrs_by_key[(mr["project_id"], mr["iid"])] = mr
        for node in assigned["nodes"]:
            if node is None:
                continue
            mr = _normalize_mr(node)
            mrs_by_key[(mr["project_id"], mr["iid"])] = mr

        self.mrs = list(mrs_by_key.values())
        print(f"Found {len(self.mrs)} MRs", file=sys.stderr)

        failed_before = self.failed_calls
        self._process_discussions()

        self._cache_set("main", {
            "username": self.username,
            "user_id": self.user_id,
            "todos": self.todos,
            "mrs": self.mrs,
            "mr_discussions": _serialize_discussions(self.mr_discussions),
        }, failed_before)

    def _fetch_rest_discussions(self, project_id, iid):
        """Fetch every discussion page of one MR."""
        result = []
        page = 1
        while True:
            output = self.run_command([
                "glab", "api",
                f"projects/{project_id}/merge_requests/{iid}/discussions?per_page={PER_PAGE}&page={page}",
            ])
            if not output:
                break
            batch = json.loads(output)
            if not isinstance(batch, list):
                break
            result.extend(batch)
            if len(batch) < PER_PAGE:
                break
            page += 1
        return result

    _THREAD_COUNTS = ("pending", "answered", "to_close", "resolved", "comments")

    def _classify_threads(self, discussions):
        """Count one MR's threads by who has to act next."""
        counts = {k: 0 for k in self._THREAD_COUNTS}
        counts["pending_authors"] = set()
        counts["open_threads"] = []

        def waits_on_me(kind, authored):
            pos = authored[0].get("position") or {}
            counts["open_threads"].append({
                "kind": kind,
                "author": authored[-1]["author"]["username"],
                "note_id": authored[-1].get("id"),
                "path": pos.get("new_path") or pos.get("old_path") or "",
                "body": _snippet(authored[-1].get("body")),
            })

        for disc in discussions:
            notes = [n for n in disc.get("notes", []) if not n.get("system", False)]
            authored = [n for n in notes if (n.get("author") or {}).get("username")]
            if not authored:
                continue
            first = authored[0]["author"]["username"]
            last = authored[-1]["author"]["username"]
            resolvable = [n for n in notes if n.get("resolvable", False)]
            if not resolvable:
                # Plain comment: mine to answer unless I spoke last
                if last != self.username:
                    counts["comments"] += 1
                    counts["pending_authors"].add(last)
                    waits_on_me("comment", authored)
            elif all(n.get("resolved", False) for n in resolvable):
                counts["resolved"] += 1
            elif last == self.username:
                counts["answered"] += 1
            elif first == self.username:
                counts["to_close"] += 1
                waits_on_me("to_close", authored)
            else:
                counts["pending"] += 1
                counts["pending_authors"].add(last)
                waits_on_me("pending", authored)
        return counts

    # -- Per-MR thread store: refetch an MR only when it moved or still waits on me --

    def _store(self):
        if not hasattr(self, "_thread_store"):
            self._store_cache = FileCache(self.base_dir, ttl=STORE_TTL)
            self._thread_store = self._store_cache.get("threads") or {}
        return self._thread_store

    def _save_store(self, keep=None):
        store = self._store()
        if keep:
            for url in [u for u in store if u not in keep]:
                del store[url]
        self._store_cache.set("threads", store)

    def _threads_for(self, mr, open_kinds=("pending", "comment", "to_close")):
        """Thread counts of one MR, from the store when nothing can have changed."""
        store = self._store()
        entry = store.get(mr["web_url"])
        if entry and entry["updated_at"] == mr.get("updated_at") and not entry["open"]:
            self.store_hits += 1
            counts = dict(entry["counts"])
            counts["pending_authors"] = set(counts["pending_authors"])
            return counts
        failed_before = self.failed_calls
        discussions = self._fetch_rest_discussions(mr["project_id"], mr["iid"])
        counts = self._classify_threads(discussions)
        if self.failed_calls == failed_before:
            stored = dict(counts)
            stored["pending_authors"] = sorted(counts["pending_authors"])
            store[mr["web_url"]] = {
                "updated_at": mr.get("updated_at"),
                "open": any(t["kind"] in open_kinds for t in counts["open_threads"]),
                "counts": stored,
            }
        return counts

    def _process_discussions(self):
        print(f"Fetching discussions for {len(self.mrs)} own MRs...", file=sys.stderr)
        for mr in self.mrs:
            counts = self._threads_for(mr)
            if any(counts[k] for k in self._THREAD_COUNTS):
                self.mr_discussions[mr["web_url"]] = counts
        self._save_store()
        print(f"  {self.store_hits} unchanged MRs read from the store", file=sys.stderr)

        discussed = sum(
            1 for v in self.mr_discussions.values()
            if v["pending"] > 0 or v["answered"] > 0 or v["to_close"] > 0 or v.get("comments", 0) > 0
        )
        print(f"  {discussed} MRs with active discussions", file=sys.stderr)

    def fetch_recently_merged(self, since_date):
        cached = self.cache.get("merged")
        if cached:
            print("Using cached merged MRs...", file=sys.stderr)
            self.recently_merged = cached
            return

        print("Fetching recently merged MRs (GraphQL)...", file=sys.stderr)
        data = self.run_graphql(MERGED_QUERY)
        if not data:
            return

        since_iso = since_date.strftime("%Y-%m-%dT00:00:00Z")
        nodes = data["currentUser"]["authoredMergeRequests"]["nodes"]
        self.recently_merged = [
            _normalize_mr(n) for n in nodes
            if n.get("mergedAt", "") >= since_iso
        ]
        print(f"Found {len(self.recently_merged)} recently merged MRs", file=sys.stderr)
        self.cache.set("merged", self.recently_merged)

    def fetch_issues(self):
        if not self.username:
            return

        cached = self.cache.get("issues")
        if cached:
            print("Using cached issues...", file=sys.stderr)
            self.issues = cached
            return

        print("Fetching issues (GraphQL)...", file=sys.stderr)
        query = ISSUES_QUERY_TEMPLATE.format(username=self.username)
        data = self.run_graphql(query)
        if not data:
            return

        self.issues = [_normalize_issue(n) for n in data["issues"]["nodes"]]
        print(f"Found {len(self.issues)} issues", file=sys.stderr)
        self.cache.set("issues", self.issues)

    def fetch_mr_linked_issues(self):
        """Discover open issues referenced in MR descriptions but not assigned to the user.

        Parses each user MR's description for issue refs (`namespace/project#N` or `#N`)
        and fetches any issue not already in self.issues. New issues are appended and
        cached separately so build_issue_mr_links can link them.
        """
        if not self.mrs:
            return

        cached = self.cache.get("linked_issues")
        known_urls = {i["web_url"] for i in self.issues}
        if cached is not None:
            print("Using cached linked issues...", file=sys.stderr)
            known_keys = {_issue_url_key(u) for u in known_urls}
            added = 0
            for issue in cached:
                key = _issue_url_key(issue["web_url"])
                if key not in known_keys:
                    self.issues.append(issue)
                    known_keys.add(key)
                    added += 1
            if added:
                print(f"  Added {added} linked issues from cache", file=sys.stderr)
            return

        print("Discovering issues linked in MR descriptions...", file=sys.stderr)
        discovered = []
        known_keys = {_issue_url_key(u) for u in known_urls}
        failed_before = self.failed_calls

        for mr in self.mrs:
            description = mr.get("description", "") or ""
            if not description:
                continue
            project_url = mr["web_url"].split("/-/")[0].rstrip("/")
            gitlab_base = "/".join(project_url.split("/", 3)[:3]) + "/"
            own_path = project_url.removeprefix(gitlab_base)

            for m in _ISSUE_REF_RE.finditer(description):
                proj_part = m.group(1)
                issue_num = m.group(2)
                ref_path = _issue_ref_path(own_path, proj_part)
                issue_url = f"{gitlab_base}{ref_path}/-/issues/{issue_num}"
                if _issue_url_key(issue_url) in known_keys:
                    continue
                encoded = ref_path.replace("/", "%2F")
                output = self.run_command(
                    ["glab", "api", f"projects/{encoded}/issues/{issue_num}"]
                )
                if not output:
                    continue
                data = json.loads(output)
                if data.get("state") != "opened":
                    continue
                # The API returns the canonical work_items URL, which may already
                # be known under a different surface form than issue_url above.
                if _issue_url_key(data["web_url"]) in known_keys:
                    continue
                raw_labels = data.get("labels", [])
                labels = raw_labels if isinstance(raw_labels, list) and all(isinstance(l, str) for l in raw_labels) else []
                issue = {
                    "iid": str(data["iid"]),
                    "title": data["title"],
                    "web_url": data["web_url"],
                    "project_id": data["project_id"],
                    "created_at": data.get("created_at", ""),
                    "labels": labels,
                }
                discovered.append(issue)
                known_keys.add(_issue_url_key(data["web_url"]))

        if discovered:
            print(f"  Discovered {len(discovered)} linked issues from MR descriptions", file=sys.stderr)
            self.issues.extend(discovered)
            # Invalidate issue_mr_links so it rebuilds with the new issues included.
            self.cache.set("issue_mr_links", None)
        else:
            print("  No new linked issues found", file=sys.stderr)

        self._cache_set("linked_issues", discovered, failed_before)

    def build_issue_mr_links(self):
        if not self.issues:
            return

        cached = self.cache.get("issue_mr_links")
        if cached:
            print("Using cached issue-MR links...", file=sys.stderr)
            mr_by_url = {mr["web_url"]: mr for mr in self.mrs}
            for issue_url, links in cached.get("issue_to_mrs", {}).items():
                for mr_url in links:
                    if mr_url in mr_by_url:
                        self.issue_to_mrs[issue_url].append(mr_by_url[mr_url])
                        self.mr_to_issues[mr_url].append(
                            next((i for i in self.issues if i["web_url"] == issue_url), {})
                        )
            for issue_url, mrs in cached.get("issue_all_mrs", {}).items():
                self.issue_all_mrs[issue_url] = mrs
            return

        print(f"Fetching issue-MR links ({len(self.issues)} issues)...", file=sys.stderr)
        failed_before = self.failed_calls
        mr_url_set = {mr["web_url"] for mr in self.mrs}
        mr_by_url = {mr["web_url"]: mr for mr in self.mrs}

        cache_issue_to_mrs = {}
        cache_issue_all_mrs = {}

        for issue in self.issues:
            project_id = issue["project_id"]
            iid = issue["iid"]
            output = self.run_command(
                ["glab", "api", f"projects/{project_id}/issues/{iid}/related_merge_requests"]
            )
            if not output:
                continue
            related = json.loads(output)
            all_mrs_for_issue = []
            linked_mr_urls = []
            for rel in related:
                url = rel.get("web_url", "")
                all_mrs_for_issue.append({"web_url": url, "state": rel.get("state", "")})
                if url in mr_url_set:
                    self.issue_to_mrs[issue["web_url"]].append(mr_by_url[url])
                    self.mr_to_issues[url].append(issue)
                    linked_mr_urls.append(url)
            self.issue_all_mrs[issue["web_url"]] = all_mrs_for_issue
            cache_issue_all_mrs[issue["web_url"]] = all_mrs_for_issue
            if linked_mr_urls:
                cache_issue_to_mrs[issue["web_url"]] = linked_mr_urls

        linked = sum(1 for v in self.issue_to_mrs.values() if v)
        print(f"  {linked} issues linked to open MRs", file=sys.stderr)

        self._cache_set("issue_mr_links", {
            "issue_to_mrs": cache_issue_to_mrs,
            "issue_all_mrs": cache_issue_all_mrs,
        }, failed_before)

    def _fetch_all_group_mrs(self):
        """Fetch all open MRs from configured groups (cached, shared by team + discussions).

        Uses GraphQL to get approvals + note counts inline.
        """
        if hasattr(self, "_all_group_mrs"):
            return self._all_group_mrs

        cached = self.cache.get("all_group_mrs")
        if cached:
            print("Using cached group MRs...", file=sys.stderr)
            self._all_group_mrs = cached
            return cached

        if not self.username:
            self._all_group_mrs = []
            return []

        groups = self.config.get("gitlab_groups", [])
        if not groups:
            single = self.config.get("gitlab_group", "")
            if single:
                groups = [single]
        if not groups:
            self._all_group_mrs = []
            return []

        all_mrs = []
        for group in groups:
            cursor = ""
            while True:
                query = f'''{{
                    group(fullPath: "{group}") {{
                        mergeRequests(state: opened, includeSubgroups: true, first: 100{cursor}) {{
                            pageInfo {{ hasNextPage endCursor }}
                            nodes {{
                                iid title webUrl draft
                                approved
                                approvedBy {{ nodes {{ username }} }}
                                reviewers {{ nodes {{ username }} }}
                                userNotesCount
                                author {{ username id }}
                                reference projectId createdAt updatedAt
                                milestone {{ title dueDate }}
                                headPipeline {{ status }}
                            }}
                        }}
                    }}
                }}'''
                data = self.run_graphql(query)
                if not data or not data.get("group"):
                    break
                page = data["group"]["mergeRequests"]
                for node in page["nodes"]:
                    mr = _normalize_mr(node)
                    mr["reviewers"] = [r["username"] for r in (node.get("reviewers") or {}).get("nodes", [])]
                    all_mrs.append(mr)
                info = page.get("pageInfo") or {}
                if not info.get("hasNextPage"):
                    break
                cursor = f', after: "{info["endCursor"]}"'

        # Exclude own MRs
        other_mrs = [
            mr for mr in all_mrs
            if mr.get("author", {}).get("id") != self.user_id
        ]
        self._all_group_mrs = other_mrs
        self.cache.set("all_group_mrs", other_mrs)
        return other_mrs

    def fetch_team_mrs(self):
        """Fetch team MRs for the review list (--team only). Includes drafts."""
        all_group = self._fetch_all_group_mrs()
        self.team_mrs = list(all_group)
        print(f"Found {len(self.team_mrs)} team MRs to review", file=sys.stderr)

    def fetch_team_discussions(self):
        """Fetch discussions on group MRs where I participated (always runs)."""
        all_group = self._fetch_all_group_mrs()
        if not all_group:
            return

        own_urls = {mr["web_url"] for mr in self.mrs}
        participated = [
            mr for mr in all_group
            if mr.get("user_notes_count", 0) > 0 and mr["web_url"] not in own_urls
        ]
        if not participated:
            return

        print(f"Fetching discussions for {len(participated)} team MRs I commented on...", file=sys.stderr)
        for mr in participated:
            counts = self._threads_for(mr, open_kinds=("to_close",))
            if counts["to_close"]:
                # On someone else's MR only the threads I opened matter
                self.mr_discussions[mr["web_url"]] = {
                    "pending": 0, "answered": 0, "to_close": counts["to_close"],
                    "resolved": 0, "comments": 0, "pending_authors": set(),
                    "open_threads": [t for t in counts["open_threads"] if t["kind"] == "to_close"],
                }
        self._save_store(keep={mr["web_url"] for mr in self.mrs} | {mr["web_url"] for mr in all_group})

        # GitLab's own reviewer state: "unreviewed" comes back when the author asks again
        for mr in all_group:
            if self.username not in mr.get("reviewers", []) or self.username in mr.get("approved_by", []):
                continue
            output = self.run_command(
                ["glab", "api", f"projects/{mr['project_id']}/merge_requests/{mr['iid']}/reviewers"]
            )
            for r in json.loads(output) if output else []:
                if (r.get("user") or {}).get("username") == self.username:
                    self.review_state[mr["web_url"]] = r.get("state", "")

        team_discussed = sum(
            1 for mr in all_group
            if mr["web_url"] in self.mr_discussions
        )
        if team_discussed:
            print(f"  {team_discussed} team MRs with threads to close", file=sys.stderr)

    # -- Helpers --

    def _is_on_hold(self, item):
        if not self.on_hold_patterns:
            return False
        full_ref = item.get("references", {}).get("full", "").lower()
        web_url = item.get("web_url", "").lower()
        title = item.get("title", "").lower()
        return any(
            p in full_ref or p in web_url or p in title
            for p in self.on_hold_patterns
        )

    @staticmethod
    def _get_repo_short_name(item):
        ref = item.get("references", {}).get("full", "")
        parts = ref.split("/")
        if len(parts) >= 2:
            last = parts[-1]
            repo = re.sub(r"[!#]\d+$", "", last)
            return repo
        url = item.get("web_url", "")
        match = re.search(r"/([^/]+)/-/merge_requests/", url)
        if match:
            return match.group(1)
        return "unknown"

    def _get_staleness(self, mr):
        updated = mr.get("updated_at", "")
        if not updated:
            return 0
        updated_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - updated_dt).days

    def _get_mr_age(self, mr):
        created = mr.get("created_at", "")
        if not created:
            return 0
        created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - created_dt).days

    def get_mr_category(self, mr):
        if mr.get("has_conflicts", False):
            return "has_conflicts"
        elif mr.get("work_in_progress", False) or mr.get("draft", False):
            return "draft"
        elif mr.get("merge_status") == "can_be_merged":
            if mr.get("approved", False) or mr.get("approved_by"):
                return "ready_to_merge"
            else:
                return "needs_approval"
        else:
            return "needs_review"

    def filter_relevant_issues(self):
        return [
            i for i in self.issues
            if not any(s in [l.lower() for l in i.get("labels", [])]
                       for s in ["done", "blocked", "wontfix", "duplicate"])
        ]

    # -- MR categorization (shared by both formatters) --

    @staticmethod
    def _milestone_first(mr):
        ms = mr.get("milestone")
        return (0, ms.get("due_date") or "9999") if ms else (1, "")

    def categorize_mrs(self, todos):
        STALE_DAYS = self.config.get("stale_days", 7)
        BACKLOG_DAYS = self.config.get("backlog_days", 14)
        act_now = []
        failed = []
        approved = []
        in_progress = []
        waiting = []
        backlog = []
        on_hold_mrs = []
        act_now_urls = set()

        conflict_mrs = [mr for mr in self.mrs if mr.get("has_conflicts", False)]

        for mr in self.mrs:
            if self._is_on_hold(mr):
                on_hold_mrs.append(mr)

        active_mr_urls = {mr["web_url"] for mr in self.mrs if not self._is_on_hold(mr)}

        # Pending threads and unanswered comments first
        for mr in self.mrs:
            if mr["web_url"] not in active_mr_urls:
                continue
            disc = self.mr_discussions.get(mr["web_url"], {})
            if disc.get("pending", 0) > 0 or disc.get("comments", 0) > 0:
                act_now.append(mr)
                act_now_urls.add(mr["web_url"])

        # A failed build counts only on an MR that still moves
        for mr in self.mrs:
            if mr["web_url"] in act_now_urls or mr["web_url"] not in active_mr_urls:
                continue
            status = (mr.get("head_pipeline") or {}).get("status")
            if status == "failed" and self._get_staleness(mr) < STALE_DAYS:
                failed.append(mr)
                act_now_urls.add(mr["web_url"])

        for mr in self.mrs:
            if mr["web_url"] in act_now_urls or mr["web_url"] not in active_mr_urls:
                continue
            category = self.get_mr_category(mr)
            has_approval = not mr.get("draft") and bool(mr.get("approved_by"))

            if category == "ready_to_merge" or has_approval:
                approved.append(mr)
            elif not mr.get("milestone") and self._get_staleness(mr) >= BACKLOG_DAYS:
                backlog.append(mr)
            elif category == "draft":
                in_progress.append(mr)
            else:
                waiting.append(mr)

        # Threads to close (own MRs + team MRs I commented on)
        close_mrs = [
            mr for mr in self.mrs
            if mr["web_url"] in active_mr_urls
            and self.mr_discussions.get(mr["web_url"], {}).get("to_close", 0) > 0
        ]
        own_urls = {mr["web_url"] for mr in self.mrs}
        for mr in getattr(self, "_all_group_mrs", []):
            if mr["web_url"] in own_urls:
                continue
            if self.mr_discussions.get(mr["web_url"], {}).get("to_close", 0) > 0:
                close_mrs.append(mr)

        # Reviews: team MRs where GitLab says my review is still owed (first ask or a re-request)
        review_requested = [
            mr for mr in getattr(self, "_all_group_mrs", [])
            if self.username in mr.get("reviewers", [])
            and self.username not in mr.get("approved_by", [])
            and self.review_state.get(mr["web_url"], "unreviewed") in ("unreviewed", "review_started")
        ]
        review_requested.sort(key=lambda m: (self._milestone_first(m), m.get("created_at", "")))

        # To Do: issues with no open MR (work not started yet)
        filtered_issues = self.filter_relevant_issues()
        active_issues = [i for i in filtered_issues if not self._is_on_hold(i)]
        all_mr_urls = {mr["web_url"] for mr in self.mrs}
        todo_issues = [
            i for i in active_issues
            if not any(mr["web_url"] in all_mr_urls for mr in self.issue_to_mrs.get(i["web_url"], []))
        ]

        # Issues with linked MRs (reference section)
        issues_with_mrs = [
            i for i in active_issues
            if any(mr["web_url"] in all_mr_urls for mr in self.issue_to_mrs.get(i["web_url"], []))
        ]

        # Smart sorting
        act_now.sort(key=self._milestone_first)
        approved.sort(key=lambda m: (self._milestone_first(m), -len(m.get("approved_by", []))))
        in_progress.sort(key=lambda m: (self._milestone_first(m), self._get_staleness(m)))
        waiting.sort(key=lambda m: (self._milestone_first(m), self._get_staleness(m)))
        backlog.sort(key=lambda m: self._get_staleness(m))

        # Stats
        pending_threads = sum(1 for v in self.mr_discussions.values() if v["pending"] > 0)
        to_close_threads = sum(1 for v in self.mr_discussions.values() if v.get("to_close", 0) > 0)
        ready_count = sum(1 for mr in self.mrs if self.get_mr_category(mr) == "ready_to_merge")
        stale_count = sum(
            1 for mr in self.mrs
            if self.get_mr_category(mr) in ("needs_approval", "needs_review")
            and self._get_staleness(mr) >= STALE_DAYS
        )
        avg_age = round(sum(self._get_mr_age(mr) for mr in self.mrs) / len(self.mrs)) if self.mrs else 0

        return {
            "act_now": act_now,
            "failed": failed,
            "backlog": backlog,
            "approved": approved,
            "close_mrs": close_mrs,
            "review_requested": review_requested,
            "in_progress": in_progress,
            "waiting": waiting,
            "todo_issues": todo_issues,
            "issues_with_mrs": issues_with_mrs,
            "on_hold_mrs": on_hold_mrs,
            "stats": {
                "pending_threads": pending_threads,
                "to_close_threads": to_close_threads,
                "build_failures": len(failed),
                "conflicts": len(conflict_mrs),
                "ready_count": ready_count,
                "stale_count": stale_count,
                "avg_age": avg_age,
            },
        }

    # -- Todo categorization --

    def categorize_todos(self):
        categories = {
            "review_submitted": [], "build_failed": [], "unmergeable": [],
            "assigned": [], "needs_action": [],
        }
        recent_cutoff = datetime.now(timezone.utc) - timedelta(days=3)
        stale_todos = []

        for todo in self.todos:
            created = datetime.fromisoformat(todo["created_at"].replace("Z", "+00:00"))
            target = todo.get("target", {})
            if target and target.get("state", "") in ["closed", "merged"]:
                stale_todos.append(todo)
                continue
            if todo["action_name"] == "build_failed":
                head_pipeline = (target or {}).get("head_pipeline") or {}
                pipeline_status = head_pipeline.get("status", "")
                if pipeline_status == "success":
                    stale_todos.append(todo)
                    continue
                if self._is_on_hold(target or {}):
                    continue
                categories["build_failed"].append(todo)
                continue
            if todo["action_name"] == "unmergeable":
                categories["unmergeable"].append(todo)
                continue
            if created < recent_cutoff:
                continue
            action = todo["action_name"]
            if action in categories:
                categories[action].append(todo)
            else:
                categories["needs_action"].append(todo)

        if self.auto_cleanup and stale_todos:
            print(f"\nCleaning up {len(stale_todos)} stale todos...", file=sys.stderr)
            for todo in stale_todos:
                self.dismiss_todo(todo["id"])

        if stale_todos:
            print(f"\nFound {len(stale_todos)} stale todos (merged/closed)", file=sys.stderr)
        for cat, items in categories.items():
            if items:
                print(f"  {cat}: {len(items)} items", file=sys.stderr)

        return categories

    # -- JSON state for diff --

    def _state_path(self):
        return self.base_dir / STATE_FILENAME

    def _load_state(self):
        path = self._state_path()
        if not path.exists():
            return None
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    def _baseline(self, date):
        """State of the last run before `date`: the one today's diff compares to."""
        state = self._load_state()
        if state and state.get("date") == date.strftime("%Y-%m-%d"):
            return state.get("prev")
        return state

    def _save_state(self, date):
        prev = self._baseline(date)
        state = {
            "date": date.strftime("%Y-%m-%d"),
            "mr_urls": [mr["web_url"] for mr in self.mrs],
            "issue_urls": [i["web_url"] for i in self.issues],
            "prev": prev and {k: prev[k] for k in ("date", "mr_urls", "issue_urls") if k in prev},
        }
        path = self._state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(state, f, indent=2)

    def compute_diff(self, date):
        prev_state = self._baseline(date)
        if not prev_state or not prev_state.get("date"):
            return None

        prev_date = datetime.strptime(prev_state["date"], "%Y-%m-%d")
        days_back = (date - prev_date).days
        if days_back <= 0 or days_back > 7:
            return None

        since_iso = prev_date.strftime("%Y-%m-%dT00:00:00Z")
        prev_mr_urls = set(prev_state.get("mr_urls", []))
        current_mr_urls = {mr["web_url"] for mr in self.mrs}
        gone_mr_urls = prev_mr_urls - current_mr_urls

        merged_mrs = [mr for mr in self.recently_merged if mr["web_url"] in gone_mr_urls]

        prev_issue_urls = set(prev_state.get("issue_urls", []))
        current_issue_urls = {i["web_url"] for i in self.issues}
        gone_issue_urls = prev_issue_urls - current_issue_urls
        resolved_issues = [(url.rsplit("/", 1)[-1], url) for url in gone_issue_urls]

        new_mrs = [mr for mr in self.mrs if mr.get("created_at", "") >= since_iso]
        new_issues = [i for i in self.issues if i.get("created_at", "") >= since_iso]

        return {
            "days_back": days_back,
            "prev_date": prev_date,
            "new_mrs": new_mrs,
            "merged_mrs": merged_mrs,
            "gone_mr_urls": gone_mr_urls - {mr["web_url"] for mr in merged_mrs},
            "new_issues": new_issues,
            "resolved_issues": resolved_issues,
        }

    # -- Orchestration --

    def generate_daily_note(self, date=None):
        if date is None:
            date = datetime.now()

        self.fetch_main()
        self.fetch_recently_merged(date - timedelta(days=3))
        self.fetch_issues()
        self.fetch_mr_linked_issues()
        self.build_issue_mr_links()

        print("\nChecking threads on group MRs...", file=sys.stderr)
        self.fetch_team_discussions()

        if self.include_team:
            print("\nFetching team MRs to review...", file=sys.stderr)
            self.fetch_team_mrs()

        print("\nCategorizing todos...", file=sys.stderr)
        todo_categories = self.categorize_todos()

        print("\nComputing diff...", file=sys.stderr)
        diff = self.compute_diff(date)

        print("\nGenerating markdown...", file=sys.stderr)
        result = self.formatter.format_markdown(self, date, todo_categories, diff)
        print(f"Generated {len(result)} characters", file=sys.stderr)

        self._save_state(date)
        return result

    def save_daily_note(self, date=None, keep=False):
        if date is None:
            date = datetime.now()

        filepath = self.get_daily_note_path(date)
        print(f"\nSaving to: {filepath}", file=sys.stderr)

        existing_notes = self.read_existing_note(filepath)
        if existing_notes:
            print("Preserving existing notes section", file=sys.stderr)

        new_content = self.generate_daily_note(date)
        final_content = self.merge_with_existing(new_content, existing_notes)

        with open(filepath, "w") as f:
            f.write(final_content)
        abs_path = filepath.resolve()
        link = f"\033]8;;file://{abs_path}\a{filepath}\033]8;;\a"
        print(f"Done: {link}", file=sys.stderr)

        if keep:
            snapshot_path = self.get_daily_note_path(date, "-snapshot")
            if not snapshot_path.exists():
                with open(snapshot_path, "w") as f:
                    f.write(final_content)
                print(f"Snapshot: {snapshot_path}", file=sys.stderr)

        return filepath


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate daily notes from GitLab data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="First run: use --init to generate a .daily-note.json config file.",
    )
    parser.add_argument("--init", action="store_true", help="Generate a starter config file and exit")
    parser.add_argument("--config", default=None, help=f"Path to config file (default: {CONFIG_FILENAME})")
    parser.add_argument("--format", choices=["plain", "obsidian"], default="plain", help="Output format (default: plain)")
    parser.add_argument("--team", action="store_true", help="Include team MRs for review")
    parser.add_argument("--no-cleanup", action="store_true", help="Don't auto-dismiss stale todos")
    parser.add_argument("--stdout", action="store_true", help="Print to stdout instead of saving")
    parser.add_argument("--keep", action="store_true", help="Save a snapshot of the first run of the day")
    args = parser.parse_args()

    if args.init:
        generate_config(args.config)
        sys.exit(0)

    config = load_config(args.config)
    sync = GitLabSync(config=config, auto_cleanup=not args.no_cleanup, format=args.format)
    sync.include_team = args.team

    if args.stdout:
        note = sync.generate_daily_note()
        print(note)
    else:
        filepath = sync.save_daily_note(keep=args.keep)
