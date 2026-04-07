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
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

CONFIG_FILENAME = ".daily-note.json"
STATE_FILENAME = ".daily-note-state.json"
CACHE_DIR_NAME = ".daily-note-cache"
CACHE_TTL = 300  # 5 minutes

DEFAULT_CONFIG = {
    "base_dir": "daily-note",
    "tags": "",
    "gitlab_group": "",
    "on_hold_patterns": [],
    "on_hold_label": "On Hold",
    "stale_days": 7,
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
        "stale_days": "Days of inactivity before an MR is flagged for pinging reviewer",
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
    discussions(first: 100) {
        nodes {
            resolvable resolved
            notes(first: 100) {
                nodes {
                    system resolvable resolved
                    author { username }
                }
            }
        }
    }
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


def _normalize_mr(node):
    pipeline = node.get("headPipeline") or {}
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
        "author": node.get("author") or {},
        "_discussions_raw": (node.get("discussions") or {}).get("nodes", []),
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

    def _pipeline_status(self, mr):
        pipeline = mr.get("head_pipeline") or {}
        status = pipeline.get("status", "")
        labels = {
            "failed": "FAILED", "running": "running", "pending": "pending",
            "canceled": "canceled", "created": "pending", "manual": "manual",
        }
        return labels.get(status, "")

    def _approval_text(self, sync, mr):
        info = sync.mr_approvals.get(mr["web_url"])
        if not info:
            return ""
        if info["approved_by"]:
            names = ", ".join("@" + a for a in info["approved_by"])
            if info["approvals_left"] > 0:
                return f"approved by {names}, needs {info['approvals_left']} more"
            return f"approved by {names}"
        if info["approvals_required"] > 0:
            return f"needs {info['approvals_required']} approval(s)"
        return ""

    def format_mr_line(self, sync, mr):
        line = f"- [ ] [!{mr['iid']}]({mr['web_url']}): {mr['title']}"
        annotations = []

        pipeline = self._pipeline_status(mr)
        if pipeline:
            annotations.append(pipeline)

        days = sync._get_staleness(mr)
        if days >= 3:
            annotations.append(f"idle {days}d")

        if mr.get("should_be_rebased", False):
            annotations.append("needs rebase")

        approval = self._approval_text(sync, mr)
        if approval:
            annotations.append(approval)

        disc = sync.mr_discussions.get(mr["web_url"])
        if disc:
            if disc["pending"] > 0:
                authors = ", ".join("@" + a for a in sorted(disc["pending_authors"]))
                annotations.append(f"{disc['pending']} pending ({authors})")
            if disc.get("to_close", 0) > 0:
                annotations.append(f"{disc['to_close']} to close")
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

    def format_markdown(self, sync, date, todos, diff=None):
        lines = []
        cats = sync.categorize_mrs(todos)

        # Header
        tags = sync.config.get("tags", "")
        if tags:
            lines.append(tags)
            lines.append("")
        lines.append(f"# {date.strftime('%A, %B %d, %Y')}")

        prev_date = date - timedelta(days=1)
        next_date = date + timedelta(days=1)
        base = str(sync.base_dir)
        for d, label in [(prev_date, "Yesterday"), (next_date, "Tomorrow")]:
            pass
        prev_link = f"{base}/{prev_date.strftime('%Y')}/{prev_date.strftime('%m-%b')}/{prev_date.strftime('%Y-%m-%d')}"
        next_link = f"{base}/{next_date.strftime('%Y')}/{next_date.strftime('%m-%b')}/{next_date.strftime('%Y-%m-%d')}"
        lines.append(f"<< [[{prev_link}|Yesterday]] | [[{next_link}|Tomorrow]] >>")
        lines.append("")

        # Summary
        lines.append("## Summary")
        lines.append(
            f"- {cats['stats']['pending_threads']} pending threads"
            f" | {cats['stats']['to_close_threads']} to close"
            f" | {cats['stats']['build_failures']} build failures"
            f" | {cats['stats']['conflicts']} conflicts"
        )
        lines.append(
            f"- {cats['stats']['ready_count']} ready to merge"
            f" | {cats['stats']['stale_count']} need a ping"
            f" | {len(sync.mrs)} total open MRs"
            f" | avg age {cats['stats']['avg_age']}d"
        )
        if sync.recently_merged:
            lines.append(f"- {len(sync.recently_merged)} recently merged")
        if sync.team_mrs:
            lines.append(f"- {len(sync.team_mrs)} team MRs to review")
        lines.append("")

        # Act Now
        if cats["act_now"] or todos.get("build_failed") or cats["conflict_mrs"]:
            lines.append("## Act Now")
            for mr in cats["act_now"]:
                if sync.mr_discussions.get(mr["web_url"], {}).get("pending", 0) > 0:
                    lines.append(self.format_mr_line(sync, mr))
            if todos.get("build_failed"):
                for todo in todos["build_failed"]:
                    title = todo["target"]["title"]
                    url = todo["target_url"]
                    lines.append(f"- [ ] [{title}]({url}) — FAILED")
            for mr in cats["conflict_mrs"]:
                if mr["web_url"] not in cats["act_now_urls"]:
                    lines.append(self.format_mr_line(sync, mr))
            lines.append("")

        # Threads to Close
        if cats["close_mrs"]:
            lines.append("## Threads to Close")
            for mr in cats["close_mrs"]:
                disc = sync.mr_discussions[mr["web_url"]]
                line = f"- [ ] [!{mr['iid']}]({mr['web_url']}): {mr['title']}"
                line += f" — {disc['to_close']} to close"
                if disc.get("pending", 0) > 0:
                    line += f" | {disc['pending']} pending"
                lines.append(line)
            lines.append("")

        # To Do
        if cats["todo_issues"]:
            lines.append("## To Do")
            for issue in cats["todo_issues"]:
                line = self._format_issue_line(issue)
                all_issue_mrs = sync.issue_all_mrs.get(issue["web_url"], [])
                merged = sum(1 for m in all_issue_mrs if m.get("state") == "merged")
                closed = sum(1 for m in all_issue_mrs if m.get("state") == "closed")
                statuses = []
                if merged:
                    statuses.append(f"{merged} MR merged")
                if closed:
                    statuses.append(f"{closed} MR closed")
                if statuses:
                    line += f" — {', '.join(statuses)}"
                lines.append(line)
            lines.append("")

        # Ping Reviewer
        if cats["ping_reviewer"]:
            lines.append("## Ping Reviewer")
            for mr in cats["ping_reviewer"]:
                lines.append(self.format_mr_line(sync, mr))
            lines.append("")

        # In Progress
        if cats["in_progress"]:
            lines.append("## In Progress")
            for mr in cats["in_progress"]:
                lines.append(self.format_mr_line(sync, mr))
            lines.append("")

        # Approved
        if cats["approved"]:
            lines.append("## Approved")
            for mr in cats["approved"]:
                lines.append(self.format_mr_line(sync, mr))
            lines.append("")

        lines.append("---")
        lines.append("")

        # Issues (reference: all issues with linked MRs)
        self._render_issues(sync, lines, cats)

        # On Hold
        self._render_on_hold(sync, lines, cats)

        # Team MRs
        self._render_team(sync, lines)

        # Recently Merged + Changes
        self._render_changelog(sync, lines, diff)

        lines.append("## Notes")
        lines.append("- ")
        return "\n".join(lines)

    def _render_issues(self, sync, lines, cats):
        issues_with_mrs = cats["issues_with_mrs"]
        waiting = cats["waiting"]

        if not waiting and not issues_with_mrs:
            return

        lines.append("## Issues")
        lines.append("")

        all_mr_urls = {mr["web_url"] for mr in sync.mrs}
        shown_mr_urls = set()

        # Issues with linked MRs
        first_issue = True
        for issue in issues_with_mrs:
            linked_mrs = [
                mr for mr in sync.issue_to_mrs.get(issue["web_url"], [])
                if mr["web_url"] in all_mr_urls
            ]
            if not linked_mrs:
                continue
            if not first_issue:
                lines.append("")
            first_issue = False
            lines.append(f"**[#{issue['iid']}]({issue['web_url']}): {issue['title']}**")
            for mr in linked_mrs:
                lines.append(self.format_mr_line(sync, mr))
                shown_mr_urls.add(mr["web_url"])

        # MRs with no linked issue
        remaining_mrs = [mr for mr in waiting if mr["web_url"] not in shown_mr_urls]
        if remaining_mrs:
            if issues_with_mrs:
                lines.append("")
            lines.append("**No linked issue:**")
            repo_groups = defaultdict(list)
            for mr in remaining_mrs:
                repo = sync._get_repo_short_name(mr)
                repo_groups[repo].append(mr)
            for repo in sorted(repo_groups.keys()):
                lines.append(f"*{repo}:*")
                for mr in repo_groups[repo]:
                    lines.append(self.format_mr_line(sync, mr))

        lines.append("")

    def _render_on_hold(self, sync, lines, cats):
        on_hold_issues = [i for i in sync.filter_relevant_issues() if sync._is_on_hold(i)]
        if not cats["on_hold_mrs"] and not on_hold_issues:
            return
        label = sync.config.get("on_hold_label", "On Hold")
        lines.append(f"## {label}")
        for mr in cats["on_hold_mrs"]:
            lines.append(self.format_mr_line(sync, mr))
        for issue in on_hold_issues:
            lines.append(self._format_issue_line(issue))
        lines.append("")

    def _render_team(self, sync, lines):
        if not sync.team_mrs:
            return
        lines.append("---")
        lines.append("")
        lines.append("## To Review")
        repo_groups = defaultdict(list)
        for mr in sync.team_mrs:
            repo = sync._get_repo_short_name(mr)
            repo_groups[repo].append(mr)
        for repo in sorted(repo_groups.keys()):
            if len(repo_groups) > 1:
                lines.append(f"*{repo}:*")
            for mr in repo_groups[repo]:
                author = mr.get("author", {}).get("username", "?")
                line = f"- [ ] [!{mr['iid']}]({mr['web_url']}): {mr['title']} — by @{author}"
                days = sync._get_staleness(mr)
                if days >= 3:
                    line += f" | idle {days}d"
                lines.append(line)
        lines.append("")

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

    def _pipeline_status(self, mr):
        pipeline = mr.get("head_pipeline") or {}
        status = pipeline.get("status", "")
        mapping = {
            "failed": "**`FAILED`**", "running": "`running`", "pending": "`pending`",
            "canceled": "`canceled`", "created": "`pending`", "manual": "`manual`",
        }
        return mapping.get(status, "")

    def _approval_text(self, sync, mr):
        info = sync.mr_approvals.get(mr["web_url"])
        if not info:
            return ""
        if info["approved_by"]:
            names = ", ".join("@" + a for a in info["approved_by"])
            if info["approvals_left"] > 0:
                return f"**approved** by {names}, needs {info['approvals_left']} more"
            return f"**approved** by {names}"
        if info["approvals_required"] > 0:
            return f"needs {info['approvals_required']} approval(s)"
        return ""

    @staticmethod
    def _callout(kind, title, content_lines):
        result = [f"> [!{kind}] {title}"]
        for line in content_lines:
            result.append(f"> {line}")
        result.append("")
        return result

    def format_markdown(self, sync, date, todos, diff=None):
        lines = []
        cats = sync.categorize_mrs(todos)
        stats = cats["stats"]

        # Dataview frontmatter
        lines.append("---")
        lines.append("type: daily-note")
        lines.append(f"date: {date.strftime('%Y-%m-%d')}")
        lines.append(f"open_mrs: {len(sync.mrs)}")
        lines.append(f"pending_threads: {stats['pending_threads']}")
        lines.append(f"to_close: {stats['to_close_threads']}")
        lines.append(f"build_failures: {stats['build_failures']}")
        lines.append(f"conflicts: {stats['conflicts']}")
        lines.append(f"ready_to_merge: {stats['ready_count']}")
        lines.append(f"stale: {stats['stale_count']}")
        lines.append(f"avg_mr_age: {stats['avg_age']}")
        if sync.recently_merged:
            lines.append(f"recently_merged: {len(sync.recently_merged)}")
        lines.append("---")
        lines.append("")

        # Tags
        tags = sync.config.get("tags", "")
        if tags:
            lines.append(tags)
            lines.append("")

        # Header + nav
        lines.append(f"# {date.strftime('%A, %B %d, %Y')}")
        prev_date = date - timedelta(days=1)
        next_date = date + timedelta(days=1)
        base = str(sync.base_dir)
        prev_link = f"{base}/{prev_date.strftime('%Y')}/{prev_date.strftime('%m-%b')}/{prev_date.strftime('%Y-%m-%d')}"
        next_link = f"{base}/{next_date.strftime('%Y')}/{next_date.strftime('%m-%b')}/{next_date.strftime('%Y-%m-%d')}"
        lines.append(f"<< [[{prev_link}|Yesterday]] | [[{next_link}|Tomorrow]] >>")
        lines.append("")

        # Progress bar
        total = len(sync.mrs)
        if total > 0:
            pct = stats["ready_count"] / total
            bar_len = 20
            filled = round(pct * bar_len)
            bar = "=" * filled + "-" * (bar_len - filled)
            lines.append(f"`[{bar}]` {stats['ready_count']}/{total} ready to merge")
        lines.append("")

        # Summary table
        lines.append("| Pending | To close | Failures | Conflicts | Stale | Avg age |")
        lines.append("|:---:|:---:|:---:|:---:|:---:|:---:|")
        lines.append(
            f"| {stats['pending_threads']} | {stats['to_close_threads']}"
            f" | {stats['build_failures']} | {stats['conflicts']}"
            f" | {stats['stale_count']} | {stats['avg_age']}d |"
        )
        lines.append("")

        # Act Now
        if cats["act_now"] or todos.get("build_failed") or cats["conflict_mrs"]:
            cl = []
            for mr in cats["act_now"]:
                if sync.mr_discussions.get(mr["web_url"], {}).get("pending", 0) > 0:
                    cl.append(self.format_mr_line(sync, mr))
            if todos.get("build_failed"):
                for todo in todos["build_failed"]:
                    cl.append(f"- [ ] [{todo['target']['title']}]({todo['target_url']}) — **`FAILED`**")
            for mr in cats["conflict_mrs"]:
                if mr["web_url"] not in cats["act_now_urls"]:
                    cl.append(self.format_mr_line(sync, mr))
            lines.extend(self._callout("danger", "Act Now", cl))

        # Threads to Close
        if cats["close_mrs"]:
            cl = []
            for mr in cats["close_mrs"]:
                disc = sync.mr_discussions[mr["web_url"]]
                line = f"- [ ] [!{mr['iid']}]({mr['web_url']}): {mr['title']}"
                line += f" — {disc['to_close']} to close"
                if disc.get("pending", 0) > 0:
                    line += f" | {disc['pending']} pending"
                cl.append(line)
            lines.extend(self._callout("warning", "Threads to Close", cl))

        # To Do
        if cats["todo_issues"]:
            cl = []
            for issue in cats["todo_issues"]:
                line = self._format_issue_line(issue)
                all_issue_mrs = sync.issue_all_mrs.get(issue["web_url"], [])
                merged = sum(1 for m in all_issue_mrs if m.get("state") == "merged")
                closed = sum(1 for m in all_issue_mrs if m.get("state") == "closed")
                statuses = []
                if merged:
                    statuses.append(f"{merged} MR merged")
                if closed:
                    statuses.append(f"{closed} MR closed")
                if statuses:
                    line += f" — {', '.join(statuses)}"
                cl.append(line)
            lines.extend(self._callout("example", "To Do", cl))

        # Ping Reviewer
        if cats["ping_reviewer"]:
            cl = [self.format_mr_line(sync, mr) for mr in cats["ping_reviewer"]]
            lines.extend(self._callout("warning", "Ping Reviewer", cl))

        # In Progress
        if cats["in_progress"]:
            cl = [self.format_mr_line(sync, mr) for mr in cats["in_progress"]]
            lines.extend(self._callout("note", "In Progress", cl))

        # Approved
        if cats["approved"]:
            cl = [self.format_mr_line(sync, mr) for mr in cats["approved"]]
            lines.extend(self._callout("tip", "Approved", cl))

        lines.append("---")
        lines.append("")

        # Issues (reference section)
        self._render_issues(sync, lines, cats)

        on_hold_issues = [i for i in sync.filter_relevant_issues() if sync._is_on_hold(i)]
        if cats["on_hold_mrs"] or on_hold_issues:
            label = sync.config.get("on_hold_label", "On Hold")
            cl = []
            for mr in cats["on_hold_mrs"]:
                cl.append(self.format_mr_line(sync, mr))
            for issue in on_hold_issues:
                cl.append(self._format_issue_line(issue))
            lines.extend(self._callout("quote", label, cl))

        if sync.team_mrs:
            lines.append("---")
            lines.append("")
            cl = []
            repo_groups = defaultdict(list)
            for mr in sync.team_mrs:
                repo = sync._get_repo_short_name(mr)
                repo_groups[repo].append(mr)
            for repo in sorted(repo_groups.keys()):
                if len(repo_groups) > 1:
                    cl.append(f"*{repo}:*")
                for mr in repo_groups[repo]:
                    author = mr.get("author", {}).get("username", "?")
                    line = f"- [ ] [!{mr['iid']}]({mr['web_url']}): {mr['title']} — by @{author}"
                    days = sync._get_staleness(mr)
                    if days >= 3:
                        line += f" | idle {days}d"
                    cl.append(line)
            lines.extend(self._callout("info", "To Review", cl))

        self._render_changelog(sync, lines, diff)

        lines.append("## Notes")
        lines.append("- ")
        return "\n".join(lines)


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
        self.mr_approvals = {}
        self.issue_to_mrs = defaultdict(list)
        self.issue_all_mrs = defaultdict(list)
        self.mr_to_issues = defaultdict(list)
        self.team_mrs = []
        self.on_hold_patterns = [p.lower() for p in self.config.get("on_hold_patterns", [])]
        self.include_team = False
        self.formatter = FORMATTERS[format]()
        self.cache = FileCache(self.base_dir)

    # -- Paths --

    def get_daily_note_path(self, date):
        year = date.strftime("%Y")
        month = date.strftime("%m-%b")
        filename = date.strftime("%Y-%m-%d.md")
        folder = self.base_dir / year / month
        folder.mkdir(parents=True, exist_ok=True)
        return folder / filename

    def get_snapshot_path(self, date):
        year = date.strftime("%Y")
        month = date.strftime("%m-%b")
        filename = date.strftime("%Y-%m-%d-snapshot.md")
        folder = self.base_dir / year / month
        folder.mkdir(parents=True, exist_ok=True)
        return folder / filename

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
                notes_pattern, f"## Notes\n{existing_notes}",
                new_content, flags=re.MULTILINE,
            )
        return new_content

    # -- Shell helpers --

    def run_command(self, cmd):
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            print(f"Error running command: {' '.join(cmd)}", file=sys.stderr)
            print(f"Error: {e.stderr}", file=sys.stderr)
            return None

    def run_graphql(self, query):
        cmd = ["glab", "api", "graphql", "-f", f"query={query}"]
        output = self.run_command(cmd)
        if not output:
            return None
        data = json.loads(output)
        if "errors" in data:
            for err in data["errors"]:
                print(f"GraphQL error: {err.get('message', err)}", file=sys.stderr)
            return None
        return data.get("data")

    # -- Todo management --

    def dismiss_todo(self, todo_id):
        print(f"Dismissing todo {todo_id}...", file=sys.stderr)
        output = self.run_command(
            ["glab", "api", "-X", "POST", f"todos/{todo_id}/mark_as_done"]
        )
        return output is not None

    # -- Data fetching --

    def fetch_main(self):
        cached = self.cache.get("main")
        if cached:
            print("Using cached main data...", file=sys.stderr)
            self.username = cached["username"]
            self.user_id = cached["user_id"]
            self.todos = cached["todos"]
            self.mrs = cached["mrs"]
            self.mr_discussions = _deserialize_discussions(cached["mr_discussions"])
            self.mr_approvals = cached["mr_approvals"]
            print(f"User: {self.username} | {len(self.todos)} todos | {len(self.mrs)} MRs", file=sys.stderr)
            return

        print("Fetching user, todos, and MRs (GraphQL)...", file=sys.stderr)
        data = self.run_graphql(MAIN_QUERY)
        if not data:
            print("GraphQL main query failed", file=sys.stderr)
            return

        user = data["currentUser"]
        self.username = user["username"]
        self.user_id = _gid_to_int(user["id"])
        print(f"User: {self.username} (ID: {self.user_id})", file=sys.stderr)

        self.todos = [_normalize_todo(t) for t in user["todos"]["nodes"]]
        print(f"Found {len(self.todos)} todos", file=sys.stderr)

        mrs_by_key = {}
        for node in user["authoredMergeRequests"]["nodes"]:
            mr = _normalize_mr(node)
            mrs_by_key[(mr["project_id"], mr["iid"])] = mr
        for node in user["assignedMergeRequests"]["nodes"]:
            mr = _normalize_mr(node)
            mrs_by_key[(mr["project_id"], mr["iid"])] = mr

        self.mrs = list(mrs_by_key.values())
        print(f"Found {len(self.mrs)} MRs", file=sys.stderr)

        self._process_discussions()
        self._process_approvals()

        self.cache.set("main", {
            "username": self.username,
            "user_id": self.user_id,
            "todos": self.todos,
            "mrs": self.mrs,
            "mr_discussions": _serialize_discussions(self.mr_discussions),
            "mr_approvals": self.mr_approvals,
        })

    def _process_discussions(self):
        print(f"Processing discussions from {len(self.mrs)} MRs...", file=sys.stderr)
        for mr in self.mrs:
            raw_discussions = mr.pop("_discussions_raw", [])
            if not raw_discussions:
                continue

            pending = answered = to_close = resolved = 0
            pending_authors = set()

            for disc in raw_discussions:
                if not disc.get("resolvable", False):
                    continue
                if disc.get("resolved", False):
                    resolved += 1
                    continue

                notes = disc.get("notes", {}).get("nodes", [])
                non_system = [n for n in notes if not n.get("system", False)]
                if not non_system:
                    continue

                first_author = non_system[0].get("author", {}).get("username", "")
                last_author = non_system[-1].get("author", {}).get("username", "")

                if first_author == self.username:
                    if last_author == self.username:
                        answered += 1
                    else:
                        to_close += 1
                else:
                    if last_author == self.username:
                        answered += 1
                    else:
                        pending += 1
                        pending_authors.add(last_author)

            if pending or answered or to_close or resolved:
                self.mr_discussions[mr["web_url"]] = {
                    "pending": pending, "answered": answered, "to_close": to_close,
                    "resolved": resolved, "pending_authors": pending_authors,
                }

        discussed = sum(
            1 for v in self.mr_discussions.values()
            if v["pending"] > 0 or v["answered"] > 0 or v["to_close"] > 0
        )
        print(f"  {discussed} MRs with active discussions", file=sys.stderr)

    def _process_approvals(self):
        print("Processing approvals...", file=sys.stderr)
        for mr in self.mrs:
            if mr.get("draft", False):
                continue
            approved_by = mr.get("approved_by", [])
            self.mr_approvals[mr["web_url"]] = {
                "approved_by": approved_by,
                "approvals_required": 1 if not approved_by else 0,
                "approvals_left": 0 if approved_by else 1,
            }
        approved_count = sum(1 for v in self.mr_approvals.values() if v["approved_by"])
        print(f"  {approved_count} MRs have approvals", file=sys.stderr)

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

        self.cache.set("issue_mr_links", {
            "issue_to_mrs": cache_issue_to_mrs,
            "issue_all_mrs": cache_issue_all_mrs,
        })

    def fetch_team_mrs(self):
        if not self.username:
            return

        groups = self.config.get("gitlab_groups", [])
        if not groups:
            single = self.config.get("gitlab_group", "")
            if single:
                groups = [single]
        if not groups:
            print("No gitlab_group(s) configured, skipping team MRs", file=sys.stderr)
            return

        cached = self.cache.get("team")
        if cached:
            print("Using cached team MRs...", file=sys.stderr)
            self.team_mrs = cached
            return

        all_mrs = []
        for group in groups:
            encoded_group = group.replace("/", "%2F")
            output = self.run_command(
                ["glab", "api",
                 f"groups/{encoded_group}/merge_requests?state=opened&per_page=100&scope=all"]
            )
            if output:
                all_mrs.extend(json.loads(output))

        self.team_mrs = [
            mr for mr in all_mrs
            if mr.get("author", {}).get("id") != self.user_id
            and not mr.get("draft", False)
            and not mr.get("work_in_progress", False)
        ]
        print(f"Found {len(self.team_mrs)} team MRs to review", file=sys.stderr)
        self.cache.set("team", self.team_mrs)

    # -- Helpers --

    def _is_on_hold(self, item):
        if not self.on_hold_patterns:
            return False
        full_ref = item.get("references", {}).get("full", "").lower()
        web_url = item.get("web_url", "").lower()
        return any(p in full_ref or p in web_url for p in self.on_hold_patterns)

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

    def categorize_mrs(self, todos):
        STALE_DAYS = self.config.get("stale_days", 7)
        act_now = []
        approved = []
        ping_reviewer = []
        in_progress = []
        waiting = []
        on_hold_mrs = []
        act_now_urls = set()

        conflict_mrs = [mr for mr in self.mrs if mr.get("has_conflicts", False)]

        for mr in self.mrs:
            if self._is_on_hold(mr):
                on_hold_mrs.append(mr)

        active_mr_urls = {mr["web_url"] for mr in self.mrs if not self._is_on_hold(mr)}

        # Pending threads first
        for mr in self.mrs:
            if mr["web_url"] not in active_mr_urls:
                continue
            disc = self.mr_discussions.get(mr["web_url"], {})
            if disc.get("pending", 0) > 0:
                act_now.append(mr)
                act_now_urls.add(mr["web_url"])

        build_failure_mr_urls = set()
        for todo in todos.get("build_failed", []):
            target = todo.get("target", {})
            if target.get("web_url"):
                build_failure_mr_urls.add(target["web_url"])

        for mr in conflict_mrs:
            if mr["web_url"] not in act_now_urls and mr["web_url"] in active_mr_urls:
                act_now.append(mr)
                act_now_urls.add(mr["web_url"])

        excluded_urls = act_now_urls | build_failure_mr_urls

        for mr in self.mrs:
            if mr["web_url"] in excluded_urls or mr["web_url"] not in active_mr_urls:
                continue
            category = self.get_mr_category(mr)
            days_idle = self._get_staleness(mr)
            has_approval = bool(self.mr_approvals.get(mr["web_url"], {}).get("approved_by"))

            if category == "ready_to_merge" or has_approval:
                approved.append(mr)
            elif category == "draft":
                in_progress.append(mr)
            elif category in ("needs_approval", "needs_review") and days_idle >= STALE_DAYS:
                ping_reviewer.append(mr)
            else:
                waiting.append(mr)

        # Threads to close
        close_mrs = [
            mr for mr in self.mrs
            if mr["web_url"] in active_mr_urls
            and self.mr_discussions.get(mr["web_url"], {}).get("to_close", 0) > 0
        ]

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
        approved.sort(
            key=lambda m: len(self.mr_approvals.get(m["web_url"], {}).get("approved_by", [])),
            reverse=True,
        )
        ping_reviewer.sort(key=lambda m: self._get_staleness(m), reverse=True)
        waiting.sort(key=lambda m: self._get_staleness(m), reverse=True)

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
            "act_now_urls": act_now_urls,
            "approved": approved,
            "close_mrs": close_mrs,
            "ping_reviewer": ping_reviewer,
            "in_progress": in_progress,
            "waiting": waiting,
            "todo_issues": todo_issues,
            "issues_with_mrs": issues_with_mrs,
            "on_hold_mrs": on_hold_mrs,
            "conflict_mrs": conflict_mrs,
            "stats": {
                "pending_threads": pending_threads,
                "to_close_threads": to_close_threads,
                "build_failures": len(todos.get("build_failed", [])),
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
            if todo["action_name"] in ["build_failed", "unmergeable"]:
                categories[todo["action_name"]].append(todo)
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

    def _save_state(self, date):
        state = {
            "date": date.strftime("%Y-%m-%d"),
            "mr_urls": [mr["web_url"] for mr in self.mrs],
            "issue_urls": [i["web_url"] for i in self.issues],
        }
        path = self._state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(state, f, indent=2)

    def compute_diff(self, date):
        prev_state = self._load_state()
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
        self.build_issue_mr_links()

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
        link = f"\033]8;;file://{abs_path}\033\\{filepath}\033]8;;\033\\"
        print(f"Done: {link}", file=sys.stderr)

        if keep:
            snapshot_path = self.get_snapshot_path(date)
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
