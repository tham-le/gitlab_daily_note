#!/usr/bin/env python3
"""
gitlab_recap.py - Generate a recap of a team member's GitLab contributions

Fetches MRs, issues, reviews, and comments for a given user.

Usage:
    python gitlab_recap.py @Olimarmite                # Last 6 months
    python gitlab_recap.py @Olimarmite --months 12    # Last 12 months
    python gitlab_recap.py @Olimarmite --stdout       # Print to stdout
"""

import json
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from pathlib import Path


def run_command(cmd):
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error: {' '.join(cmd)}", file=sys.stderr)
        print(f"  {e.stderr}", file=sys.stderr)
        return None


def parse_json_list(output):
    if not output:
        return []
    data = json.loads(output)
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


def fetch_user(username):
    output = run_command(["glab", "api", f"users?username={username}"])
    if not output:
        return None
    data = json.loads(output)
    if not isinstance(data, list) or not data:
        return None
    return data[0]


def fetch_paginated(url_template, since, until):
    """Fetch all pages for a given API endpoint"""
    all_items = {}
    page = 1
    while True:
        url = f"{url_template}&updated_after={since}&updated_before={until}&per_page=100&page={page}"
        output = run_command(["glab", "api", url])
        items = parse_json_list(output)
        if not items:
            break
        for item in items:
            all_items[item["id"]] = item
        if len(items) < 100:
            break
        page += 1
    return list(all_items.values())


def fetch_all_mrs(user_id, since, until):
    """Fetch all MRs authored by user (any state)"""
    all_mrs = {}
    for state in ["merged", "opened", "closed"]:
        mrs = fetch_paginated(
            f"merge_requests?author_id={user_id}&state={state}&scope=all",
            since, until,
        )
        for mr in mrs:
            all_mrs[mr["id"]] = mr
    return list(all_mrs.values())


def fetch_reviewed_mrs(user_id, since, until):
    """Fetch MRs where user was a reviewer (not author)"""
    all_mrs = {}
    for state in ["merged", "opened", "closed"]:
        mrs = fetch_paginated(
            f"merge_requests?reviewer_id={user_id}&state={state}&scope=all",
            since, until,
        )
        for mr in mrs:
            # Exclude self-authored
            if mr.get("author", {}).get("id") != user_id:
                all_mrs[mr["id"]] = mr
    return list(all_mrs.values())


def fetch_issues(user_id, since, until):
    """Fetch issues authored or assigned to user"""
    all_issues = {}
    for param in [f"author_id={user_id}", f"assignee_id={user_id}"]:
        issues = fetch_paginated(
            f"issues?{param}&scope=all", since, until,
        )
        for issue in issues:
            all_issues[issue["id"]] = issue
    return list(all_issues.values())


def fetch_events(user_id, since_date):
    """Fetch user events (comments, pushes, etc.) via events API"""
    all_events = []
    page = 1
    while True:
        output = run_command(
            ["glab", "api", f"users/{user_id}/events?per_page=100&page={page}"]
        )
        events = parse_json_list(output)
        if not events:
            break

        # Events are sorted newest first — stop when we pass our date range
        oldest = events[-1].get("created_at", "")
        for event in events:
            created = event.get("created_at", "")
            if created >= since_date:
                all_events.append(event)

        if oldest < since_date or len(events) < 100:
            break
        page += 1

    return all_events


def get_repo_short_name(item):
    ref = item.get("references", {}).get("full", "")
    parts = ref.split("/")
    if len(parts) >= 2:
        return re.sub(r"[!#]\d+$", "", parts[-1])
    return "unknown"


_project_cache = {}


def resolve_project_name(project_id):
    """Resolve project ID to short name, cached"""
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


def group_by_repo(items):
    groups = defaultdict(list)
    for item in items:
        groups[get_repo_short_name(item)].append(item)
    return dict(sorted(groups.items()))


def format_mr(mr):
    iid = mr["iid"]
    url = mr["web_url"]
    title = mr["title"]
    merged_at = mr.get("merged_at")
    date_str = ""
    if merged_at:
        date_str = f" ({merged_at[:10]})"
    return f"- [!{iid}]({url}): {title}{date_str}"


def format_issue(issue):
    iid = issue["iid"]
    url = issue["web_url"]
    title = issue["title"]
    state = issue.get("state", "")
    return f"- [#{iid}]({url}): {title} [{state}]"


def format_reviewed_mr(mr):
    iid = mr["iid"]
    url = mr["web_url"]
    title = mr["title"]
    author = mr.get("author", {}).get("username", "?")
    state = mr.get("state", "")
    return f"- [!{iid}]({url}): {title} (by @{author}) [{state}]"


def generate_recap(user, mrs, reviewed_mrs, issues, events, since_date, until_date):
    name = user.get("name", user["username"])
    username = user["username"]

    # Categorize MRs
    merged = sorted(
        [mr for mr in mrs if mr.get("state") == "merged" and mr.get("merged_at")],
        key=lambda m: m["merged_at"],
    )
    opened = [mr for mr in mrs if mr.get("state") == "opened"]
    closed = [mr for mr in mrs if mr.get("state") == "closed"]

    # Group merged by month
    merged_by_month = defaultdict(list)
    for mr in merged:
        month_key = mr["merged_at"][:7]
        merged_by_month[month_key].append(mr)

    # Categorize issues
    closed_issues = [i for i in issues if i.get("state") == "closed"]
    open_issues = [i for i in issues if i.get("state") == "opened"]

    # Count events by type
    comment_events = [e for e in events if e.get("action_name") == "commented on"]
    # Group comments by project
    comments_by_project = defaultdict(int)
    for e in comment_events:
        comments_by_project[resolve_project_name(e.get("project_id", 0))] += 1

    # Repos touched (from all MRs)
    repos = set(get_repo_short_name(mr) for mr in mrs)

    period = f"{since_date.strftime('%B %Y')} — {until_date.strftime('%B %Y')}"

    lines = [
        f"# {name} (@{username}) — Recap",
        f"*{period}*",
        "",
        "## Summary",
        f"- {len(merged)} MRs merged | {len(opened)} MRs open | {len(closed)} MRs closed",
        f"- {len(reviewed_mrs)} MRs reviewed",
        f"- {len(closed_issues)} issues closed | {len(open_issues)} issues open",
        f"- {len(comment_events)} comments across {len(comments_by_project)} projects",
        f"- {len(repos)} repos: {', '.join(sorted(repos))}",
        "",
    ]

    # Merged by month
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

    # Still open
    if opened:
        lines.append(f"## In Progress ({len(opened)} MRs open)")
        groups = group_by_repo(opened)
        for repo, repo_mrs in groups.items():
            lines.append(f"**{repo}:**")
            for mr in repo_mrs:
                lines.append(format_mr(mr))
        lines.append("")

    # Reviews
    if reviewed_mrs:
        lines.append(f"## Reviews ({len(reviewed_mrs)} MRs reviewed)")
        groups = group_by_repo(reviewed_mrs)
        for repo, repo_mrs in groups.items():
            lines.append(f"**{repo}:**")
            for mr in repo_mrs:
                lines.append(format_reviewed_mr(mr))
        lines.append("")

    # Issues
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

    # Comment activity
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
    parser.add_argument(
        "user",
        help="GitLab username (with or without @)",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=6,
        help="Number of months to look back (default: 6)",
    )
    parser.add_argument("--stdout", action="store_true", help="Print to stdout")
    args = parser.parse_args()

    username = args.user.lstrip("@")

    # Look up user
    print(f"Looking up @{username}...", file=sys.stderr)
    user = fetch_user(username)
    if not user:
        print(f"User @{username} not found", file=sys.stderr)
        sys.exit(1)
    print(f"Found: {user['name']} (ID: {user['id']})", file=sys.stderr)

    # Date range
    now = datetime.now(timezone.utc)
    since_date = now - relativedelta(months=args.months)
    since = since_date.strftime("%Y-%m-01T00:00:00Z")
    until = now.strftime("%Y-%m-%dT23:59:59Z")

    # Fetch data
    print(f"Fetching data from {since[:10]} to {until[:10]}...", file=sys.stderr)

    print("  MRs authored...", file=sys.stderr)
    mrs = fetch_all_mrs(user["id"], since, until)
    print(f"    {len(mrs)} MRs", file=sys.stderr)

    print("  MRs reviewed...", file=sys.stderr)
    reviewed_mrs = fetch_reviewed_mrs(user["id"], since, until)
    print(f"    {len(reviewed_mrs)} MRs", file=sys.stderr)

    print("  Issues...", file=sys.stderr)
    issues = fetch_issues(user["id"], since, until)
    print(f"    {len(issues)} issues", file=sys.stderr)

    print("  Events (comments, activity)...", file=sys.stderr)
    events = fetch_events(user["id"], since)
    print(f"    {len(events)} events", file=sys.stderr)

    # Generate
    content = generate_recap(user, mrs, reviewed_mrs, issues, events, since_date, now)

    if args.stdout:
        print(content)
    else:
        filename = f"recap-{username}-{now.strftime('%Y-%m-%d')}.md"
        filepath = Path(filename)
        filepath.write_text(content)
        print(f"✓ Saved to {filepath}", file=sys.stderr)


if __name__ == "__main__":
    main()
