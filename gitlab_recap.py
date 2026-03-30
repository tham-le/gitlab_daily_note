#!/usr/bin/env python3
"""
gitlab_recap.py - Generate a recap of a team member's GitLab contributions

Fetches MRs, issues, reviews, and comments for a given user.
Uses GraphQL where possible, REST for events.

Usage:
    python gitlab_recap.py @username                # Last 6 months
    python gitlab_recap.py @username --months 12    # Last 12 months
    python gitlab_recap.py @username --stdout       # Print to stdout
"""

import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from gitlab_common import (
    run_command, run_graphql, parse_json_list, gid_to_int,
    get_repo_short_name, group_by_repo, add_months,
)


# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

USER_QUERY_TEMPLATE = """
{{
    users(usernames: ["{username}"]) {{
        nodes {{ id username name }}
    }}
}}
"""

MRS_QUERY_TEMPLATE = """
{{
    user(username: "{username}") {{
        authoredMergeRequests(
            {state_filter}
            updatedAfter: "{since}"
            updatedBefore: "{until}"
            first: 100
        ) {{
            nodes {{
                iid title webUrl state
                createdAt mergedAt updatedAt
                reference projectId
                author {{ username id }}
            }}
        }}
        reviewRequestedMergeRequests(
            {state_filter}
            updatedAfter: "{since}"
            updatedBefore: "{until}"
            first: 100
        ) {{
            nodes {{
                iid title webUrl state
                createdAt mergedAt updatedAt
                reference projectId
                author {{ username id }}
            }}
        }}
    }}
}}
"""

ISSUES_QUERY_TEMPLATE = """
{{
    issues(
        assigneeUsernames: ["{username}"]
        updatedAfter: "{since}"
        updatedBefore: "{until}"
        first: 100
    ) {{
        nodes {{
            iid title webUrl state
            createdAt closedAt
        }}
    }}
}}
"""


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_user(username):
    query = USER_QUERY_TEMPLATE.format(username=username)
    data = run_graphql(query)
    if not data:
        return None
    nodes = data.get("users", {}).get("nodes", [])
    if not nodes:
        return None
    user = nodes[0]
    return {
        "id": gid_to_int(user["id"]),
        "username": user["username"],
        "name": user.get("name", user["username"]),
    }


def fetch_authored_and_reviewed(username, since, until):
    """Fetch authored + reviewed MRs across all states."""
    authored = {}
    reviewed = {}

    for state in ["opened", "merged", "closed"]:
        state_filter = f"state: {state}"
        query = MRS_QUERY_TEMPLATE.format(
            username=username, state_filter=state_filter,
            since=since, until=until,
        )
        data = run_graphql(query)
        if not data or not data.get("user"):
            continue

        user_data = data["user"]
        for node in user_data.get("authoredMergeRequests", {}).get("nodes", []):
            mr = _normalize_mr(node)
            authored[(mr["project_id"], mr["iid"])] = mr

        for node in user_data.get("reviewRequestedMergeRequests", {}).get("nodes", []):
            mr = _normalize_mr(node)
            author = mr.get("author", {}).get("username", "")
            # Exclude self-authored from reviews
            if author != username:
                reviewed[(mr["project_id"], mr["iid"])] = mr

    return list(authored.values()), list(reviewed.values())


def fetch_issues(username, since, until):
    query = ISSUES_QUERY_TEMPLATE.format(
        username=username, since=since, until=until,
    )
    data = run_graphql(query)
    if not data:
        return []
    return [_normalize_issue(n) for n in data["issues"]["nodes"]]


def fetch_events(user_id, since_iso):
    """Fetch user events via REST (no GraphQL equivalent)."""
    all_events = []
    page = 1
    while True:
        output = run_command(
            ["glab", "api", f"users/{user_id}/events?per_page=100&page={page}"]
        )
        events = parse_json_list(output)
        if not events:
            break

        oldest = events[-1].get("created_at", "")
        for event in events:
            if event.get("created_at", "") >= since_iso:
                all_events.append(event)

        if oldest < since_iso or len(events) < 100:
            break
        page += 1

    return all_events


_project_cache = {}


def resolve_project_name(project_id):
    if project_id in _project_cache:
        return _project_cache[project_id]
    output = run_command(["glab", "api", f"projects/{project_id}?simple=true"])
    if output:
        data = json.loads(output)
        if isinstance(data, dict):
            name = data.get("path", "unknown")
            _project_cache[project_id] = name
            return name
    _project_cache[project_id] = "unknown"
    return "unknown"


# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------

def _normalize_mr(node):
    ref = node.get("reference", "")
    project_path = node.get("webUrl", "").split("/-/")[0].rstrip("/")
    repo_name = project_path.rsplit("/", 1)[-1] if project_path else "unknown"
    return {
        "iid": node["iid"],
        "title": node["title"],
        "web_url": node["webUrl"],
        "state": node.get("state", "").lower(),
        "created_at": node.get("createdAt", ""),
        "merged_at": node.get("mergedAt", ""),
        "updated_at": node.get("updatedAt", ""),
        "project_id": gid_to_int(node.get("projectId", 0)),
        "references": {"full": f"{repo_name}{ref}"},
        "author": node.get("author") or {},
    }


def _normalize_issue(node):
    return {
        "iid": node["iid"],
        "title": node["title"],
        "web_url": node["webUrl"],
        "state": node.get("state", "").lower(),
        "created_at": node.get("createdAt", ""),
        "closed_at": node.get("closedAt", ""),
        "references": {"full": ""},
    }


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def format_mr(mr):
    merged_at = mr.get("merged_at") or ""
    date_str = f" ({merged_at[:10]})" if merged_at else ""
    return f"- [!{mr['iid']}]({mr['web_url']}): {mr['title']}{date_str}"


def format_issue(issue):
    state = issue.get("state", "")
    return f"- [#{issue['iid']}]({issue['web_url']}): {issue['title']} [{state}]"


def format_reviewed_mr(mr):
    author = mr.get("author", {}).get("username", "?")
    state = mr.get("state", "")
    return f"- [!{mr['iid']}]({mr['web_url']}): {mr['title']} (by @{author}) [{state}]"


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_recap(user, mrs, reviewed_mrs, issues, events, since_date, until_date):
    name = user["name"]
    username = user["username"]

    merged = sorted(
        [mr for mr in mrs if mr.get("state") == "merged" and mr.get("merged_at")],
        key=lambda m: m["merged_at"],
    )
    opened = [mr for mr in mrs if mr.get("state") == "opened"]
    closed_mrs = [mr for mr in mrs if mr.get("state") == "closed"]

    # Group merged by month
    merged_by_month = defaultdict(list)
    for mr in merged:
        merged_by_month[mr["merged_at"][:7]].append(mr)

    closed_issues = [i for i in issues if i.get("state") == "closed"]
    open_issues = [i for i in issues if i.get("state") == "opened"]

    comment_events = [e for e in events if e.get("action_name") == "commented on"]
    comments_by_project = defaultdict(int)
    for e in comment_events:
        comments_by_project[resolve_project_name(e.get("project_id", 0))] += 1

    repos = set(get_repo_short_name(mr) for mr in mrs)
    period = f"{since_date.strftime('%B %Y')} — {until_date.strftime('%B %Y')}"

    lines = [
        f"# {name} (@{username}) — Recap",
        f"*{period}*",
        "",
        "## Summary",
        f"- {len(merged)} MRs merged | {len(opened)} MRs open | {len(closed_mrs)} MRs closed",
        f"- {len(reviewed_mrs)} MRs reviewed",
        f"- {len(closed_issues)} issues closed | {len(open_issues)} issues open",
        f"- {len(comment_events)} comments across {len(comments_by_project)} projects",
        f"- {len(repos)} repos: {', '.join(sorted(repos))}",
        "",
    ]

    if merged:
        lines.append(f"## Shipped ({len(merged)} MRs merged)")
        lines.append("")
        for month_key in sorted(merged_by_month.keys()):
            month_mrs = merged_by_month[month_key]
            month_label = datetime.strptime(month_key, "%Y-%m").strftime("%B %Y")
            lines.append(f"### {month_label} ({len(month_mrs)} MRs)")
            groups = group_by_repo(month_mrs)
            for repo, repo_mrs in groups.items():
                lines.append(f"**{repo}:**")
                for mr in repo_mrs:
                    lines.append(format_mr(mr))
            lines.append("")

    if opened:
        lines.append(f"## In Progress ({len(opened)} MRs open)")
        groups = group_by_repo(opened)
        for repo, repo_mrs in groups.items():
            lines.append(f"**{repo}:**")
            for mr in repo_mrs:
                lines.append(format_mr(mr))
        lines.append("")

    if reviewed_mrs:
        lines.append(f"## Reviews ({len(reviewed_mrs)} MRs reviewed)")
        groups = group_by_repo(reviewed_mrs)
        for repo, repo_mrs in groups.items():
            lines.append(f"**{repo}:**")
            for mr in repo_mrs:
                lines.append(format_reviewed_mr(mr))
        lines.append("")

    if issues:
        lines.append(f"## Issues ({len(closed_issues)} closed, {len(open_issues)} open)")
        if closed_issues:
            lines.append("**Closed:**")
            for issue in closed_issues:
                lines.append(format_issue(issue))
        if open_issues:
            lines.append("**Open:**")
            for issue in open_issues:
                lines.append(format_issue(issue))
        lines.append("")

    if comments_by_project:
        lines.append(f"## Comment Activity ({len(comment_events)} comments)")
        for project in sorted(comments_by_project, key=comments_by_project.get, reverse=True):
            count = comments_by_project[project]
            lines.append(f"- **{project}:** {count} comments")
        lines.append("")

    return "\n".join(lines)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate a recap of a team member's GitLab contributions"
    )
    parser.add_argument("user", help="GitLab username (with or without @)")
    parser.add_argument("--months", type=int, default=6, help="Months to look back (default: 6)")
    parser.add_argument("--stdout", action="store_true", help="Print to stdout")
    args = parser.parse_args()

    username = args.user.lstrip("@")

    print(f"Looking up @{username}...", file=sys.stderr)
    user = fetch_user(username)
    if not user:
        print(f"User @{username} not found", file=sys.stderr)
        sys.exit(1)
    print(f"Found: {user['name']} (ID: {user['id']})", file=sys.stderr)

    now = datetime.now(timezone.utc)
    sy, sm = add_months(now.year, now.month, -args.months)
    since = f"{sy}-{sm:02d}-01T00:00:00Z"
    until = now.strftime("%Y-%m-%dT23:59:59Z")
    since_date = datetime(sy, sm, 1)

    print(f"Fetching data from {since[:10]} to {until[:10]}...", file=sys.stderr)

    print("  MRs authored + reviewed (GraphQL)...", file=sys.stderr)
    mrs, reviewed_mrs = fetch_authored_and_reviewed(username, since, until)
    print(f"    {len(mrs)} authored, {len(reviewed_mrs)} reviewed", file=sys.stderr)

    print("  Issues (GraphQL)...", file=sys.stderr)
    issues = fetch_issues(username, since, until)
    print(f"    {len(issues)} issues", file=sys.stderr)

    print("  Events (REST)...", file=sys.stderr)
    events = fetch_events(user["id"], since)
    print(f"    {len(events)} events", file=sys.stderr)

    content = generate_recap(user, mrs, reviewed_mrs, issues, events, since_date, now)

    if args.stdout:
        print(content)
    else:
        filename = f"recap-{username}-{now.strftime('%Y-%m-%d')}.md"
        filepath = Path(filename)
        filepath.write_text(content)
        print(f"Saved to {filepath}", file=sys.stderr)


if __name__ == "__main__":
    main()
