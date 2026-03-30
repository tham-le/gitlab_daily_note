#!/usr/bin/env python3
"""
gitlab_monthly_resume.py - Generate a monthly resume from GitLab data

Generates a markdown summary of MRs and issues for a given month.
Requires the `glab` CLI to be installed and authenticated.

Usage:
    python gitlab_monthly_resume.py                  # Current month
    python gitlab_monthly_resume.py --last           # Previous month
    python gitlab_monthly_resume.py --month 2026-03  # Specific month
    python gitlab_monthly_resume.py --stdout         # Print to stdout
"""

import json
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

CONFIG_FILENAME = ".daily-note.json"


def load_config():
    config_path = Path(CONFIG_FILENAME)
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {}


def run_command(cmd):
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error: {' '.join(cmd)}", file=sys.stderr)
        print(f"  {e.stderr}", file=sys.stderr)
        return None


def fetch_user_info():
    output = run_command(["glab", "api", "user"])
    if not output:
        print("Could not get user info", file=sys.stderr)
        sys.exit(1)
    data = json.loads(output)
    if not isinstance(data, dict) or "id" not in data:
        print(f"Unexpected user response: {str(data)[:200]}", file=sys.stderr)
        sys.exit(1)
    return data["id"], data.get("username", "unknown")


def fetch_mrs(user_id, since, until):
    """Fetch authored + assigned MRs updated during the month"""
    mrs_by_id = {}

    for param in [f"author_id={user_id}", f"assignee_id={user_id}"]:
        output = run_command(
            [
                "glab",
                "api",
                f"merge_requests?{param}&scope=all&per_page=100"
                f"&updated_after={since}&updated_before={until}",
            ]
        )
        if not output:
            continue
        data = json.loads(output)
        if not isinstance(data, list):
            continue
        for mr in data:
            if isinstance(mr, dict):
                mrs_by_id[mr["id"]] = mr

    return list(mrs_by_id.values())


def fetch_issues(user_id, since, until):
    """Fetch assigned issues updated during the month"""
    output = run_command(
        [
            "glab",
            "api",
            f"issues?assignee_id={user_id}&scope=all&per_page=100"
            f"&updated_after={since}&updated_before={until}",
        ]
    )
    if not output:
        return []
    data = json.loads(output)
    if not isinstance(data, list):
        return []
    return [i for i in data if isinstance(i, dict)]


def get_repo_short_name(item):
    ref = item.get("references", {}).get("full", "")
    parts = ref.split("/")
    if len(parts) >= 2:
        return re.sub(r"[!#]\d+$", "", parts[-1])
    return "unknown"


def categorize_mrs(mrs, since, until):
    """Split MRs into: merged this month, opened this month, carried over"""
    merged = []
    opened = []
    carried = []

    for mr in mrs:
        merged_at = mr.get("merged_at")
        created_at = mr.get("created_at", "")
        state = mr.get("state", "")

        was_merged_this_month = (
            state == "merged" and merged_at and since <= merged_at < until
        )
        was_opened_this_month = since <= created_at < until

        if was_merged_this_month:
            merged.append(mr)
        elif was_opened_this_month and state != "merged":
            opened.append(mr)
        elif state == "opened":
            carried.append(mr)

    return merged, opened, carried


def categorize_issues(issues, since, until):
    """Split issues into: closed this month, opened this month, carried over"""
    closed = []
    opened = []
    carried = []

    for issue in issues:
        closed_at = issue.get("closed_at")
        created_at = issue.get("created_at", "")
        state = issue.get("state", "")

        was_closed_this_month = (
            state == "closed" and closed_at and since <= closed_at < until
        )
        was_opened_this_month = since <= created_at < until

        if was_closed_this_month:
            closed.append(issue)
        elif was_opened_this_month and state != "closed":
            opened.append(issue)
        elif state == "opened":
            carried.append(issue)

    return closed, opened, carried


def format_mr(mr):
    iid = mr["iid"]
    url = mr["web_url"]
    title = mr["title"]
    return f"- [!{iid}]({url}): {title}"


def format_issue(issue):
    iid = issue["iid"]
    url = issue["web_url"]
    title = issue["title"]
    return f"- [#{iid}]({url}): {title}"


def group_by_repo(items):
    groups = defaultdict(list)
    for item in items:
        groups[get_repo_short_name(item)].append(item)
    return dict(sorted(groups.items()))


def render_grouped(items, formatter):
    lines = []
    groups = group_by_repo(items)
    for repo, repo_items in groups.items():
        lines.append(f"**{repo}:**")
        for item in repo_items:
            lines.append(formatter(item))
    return lines


def generate_resume(year, month):
    # Date range
    if month == 12:
        next_year, next_month = year + 1, 1
    else:
        next_year, next_month = year, month + 1

    since = f"{year}-{month:02d}-01T00:00:00Z"
    until = f"{next_year}-{next_month:02d}-01T00:00:00Z"
    month_name = datetime(year, month, 1).strftime("%B %Y")

    print(f"Generating resume for {month_name}...", file=sys.stderr)

    # Fetch data
    user_id, username = fetch_user_info()
    print(f"User: {username} (ID: {user_id})", file=sys.stderr)

    print("Fetching MRs...", file=sys.stderr)
    mrs = fetch_mrs(user_id, since, until)
    print(f"  {len(mrs)} MRs", file=sys.stderr)

    print("Fetching issues...", file=sys.stderr)
    issues = fetch_issues(user_id, since, until)
    print(f"  {len(issues)} issues", file=sys.stderr)

    # Categorize
    merged, opened_mrs, carried_mrs = categorize_mrs(mrs, since, until)
    closed, opened_issues, carried_issues = categorize_issues(issues, since, until)

    # Also count MRs that were both opened and merged this month
    merged_ids = {mr["id"] for mr in merged}
    opened_and_merged = [mr for mr in merged if since <= mr.get("created_at", "") < until]

    # Build markdown
    lines = [f"# {month_name}", ""]

    # Summary
    lines.append("## Summary")
    lines.append(f"- {len(merged)} MRs merged | {len(opened_mrs)} MRs opened | {len(carried_mrs)} MRs carried over")
    lines.append(f"- {len(closed)} issues closed | {len(opened_issues)} issues opened | {len(carried_issues)} issues carried over")
    lines.append("")

    # Merged
    if merged:
        lines.append(f"## Shipped ({len(merged)} MRs merged)")
        lines.extend(render_grouped(merged, format_mr))
        lines.append("")

    # Opened MRs (not yet merged)
    if opened_mrs:
        lines.append(f"## In Progress ({len(opened_mrs)} MRs opened)")
        lines.extend(render_grouped(opened_mrs, format_mr))
        lines.append("")

    # Carried over MRs
    if carried_mrs:
        lines.append(f"## Carried Over ({len(carried_mrs)} MRs)")
        lines.extend(render_grouped(carried_mrs, format_mr))
        lines.append("")

    # Issues closed
    if closed:
        lines.append(f"## Issues Closed ({len(closed)})")
        lines.extend(render_grouped(closed, format_issue))
        lines.append("")

    # Issues opened
    if opened_issues:
        lines.append(f"## Issues Opened ({len(opened_issues)})")
        lines.extend(render_grouped(opened_issues, format_issue))
        lines.append("")

    # Issues carried over
    if carried_issues:
        lines.append(f"## Issues Carried Over ({len(carried_issues)})")
        lines.extend(render_grouped(carried_issues, format_issue))
        lines.append("")

    return "\n".join(lines)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate monthly GitLab resume")
    parser.add_argument(
        "--month",
        help="Month to generate (YYYY-MM), defaults to current month",
    )
    parser.add_argument(
        "--last", "--last-month",
        action="store_true",
        help="Generate for previous month",
    )
    parser.add_argument("--stdout", action="store_true", help="Print to stdout")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    if args.month:
        try:
            dt = datetime.strptime(args.month, "%Y-%m")
        except ValueError:
            print("Invalid format, use YYYY-MM", file=sys.stderr)
            sys.exit(1)
        year, month = dt.year, dt.month
    elif args.last:
        if now.month == 1:
            year, month = now.year - 1, 12
        else:
            year, month = now.year, now.month - 1
    else:
        year, month = now.year, now.month

    content = generate_resume(year, month)

    if args.stdout:
        print(content)
    else:
        config = load_config()
        base_dir = Path(config.get("base_dir", "monthly-resume"))
        filepath = base_dir / f"{year}-{month:02d}-resume.md"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(content)
        print(f"✓ Saved to {filepath}", file=sys.stderr)


if __name__ == "__main__":
    main()
