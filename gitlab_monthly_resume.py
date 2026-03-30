#!/usr/bin/env python3
"""
gitlab_monthly_resume.py - Generate a monthly resume from GitLab data

Generates a markdown summary of MRs and issues for a given month.
Uses GraphQL for efficient data fetching.

Usage:
    python gitlab_monthly_resume.py                  # Current month
    python gitlab_monthly_resume.py --last           # Previous month
    python gitlab_monthly_resume.py --month 2026-03  # Specific month
    python gitlab_monthly_resume.py --stdout         # Print to stdout
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

from gitlab_common import (
    load_config, run_graphql, gid_to_int,
    get_repo_short_name, render_grouped, add_months,
)


# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

MRS_QUERY_TEMPLATE = """
{{
    currentUser {{
        id username
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
            }}
        }}
        assignedMergeRequests(
            {state_filter}
            updatedAfter: "{since}"
            updatedBefore: "{until}"
            first: 100
        ) {{
            nodes {{
                iid title webUrl state
                createdAt mergedAt updatedAt
                reference projectId
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
            labels {{ nodes {{ title }} }}
        }}
    }}
}}
"""


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_user_and_mrs(since, until):
    """Fetch user info + all MRs (any state) in one call per state."""
    mrs_by_key = {}
    username = None

    for state in ["opened", "merged", "closed"]:
        state_filter = f"state: {state}"
        query = MRS_QUERY_TEMPLATE.format(
            state_filter=state_filter, since=since, until=until
        )
        data = run_graphql(query)
        if not data:
            continue

        user = data["currentUser"]
        if not username:
            username = user["username"]

        for source in ["authoredMergeRequests", "assignedMergeRequests"]:
            for node in user[source]["nodes"]:
                mr = _normalize_mr(node)
                mrs_by_key[(mr["project_id"], mr["iid"])] = mr

    return username, list(mrs_by_key.values())


def fetch_issues(username, since, until):
    query = ISSUES_QUERY_TEMPLATE.format(
        username=username, since=since, until=until
    )
    data = run_graphql(query)
    if not data:
        return []
    return [_normalize_issue(n) for n in data["issues"]["nodes"]]


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
# Categorization
# ---------------------------------------------------------------------------

def categorize_mrs(mrs, since, until):
    merged, opened, carried = [], [], []
    for mr in mrs:
        merged_at = mr.get("merged_at") or ""
        created_at = mr.get("created_at", "")
        state = mr.get("state", "")

        if state == "merged" and merged_at and since <= merged_at < until:
            merged.append(mr)
        elif since <= created_at < until and state != "merged":
            opened.append(mr)
        elif state == "opened":
            carried.append(mr)

    return merged, opened, carried


def categorize_issues(issues, since, until):
    closed, opened, carried = [], [], []
    for issue in issues:
        closed_at = issue.get("closed_at") or ""
        created_at = issue.get("created_at", "")
        state = issue.get("state", "")

        if state == "closed" and closed_at and since <= closed_at < until:
            closed.append(issue)
        elif since <= created_at < until and state != "closed":
            opened.append(issue)
        elif state == "opened":
            carried.append(issue)

    return closed, opened, carried


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def format_mr(mr):
    return f"- [!{mr['iid']}]({mr['web_url']}): {mr['title']}"


def format_issue(issue):
    return f"- [#{issue['iid']}]({issue['web_url']}): {issue['title']}"


def generate_resume(year, month):
    ny, nm = add_months(year, month, 1)
    since = f"{year}-{month:02d}-01T00:00:00Z"
    until = f"{ny}-{nm:02d}-01T00:00:00Z"
    month_name = datetime(year, month, 1).strftime("%B %Y")

    print(f"Generating resume for {month_name}...", file=sys.stderr)

    print("Fetching MRs (GraphQL)...", file=sys.stderr)
    username, mrs = fetch_user_and_mrs(since, until)
    print(f"  {len(mrs)} MRs", file=sys.stderr)

    print("Fetching issues (GraphQL)...", file=sys.stderr)
    issues = fetch_issues(username, since, until)
    print(f"  {len(issues)} issues", file=sys.stderr)

    merged, opened_mrs, carried_mrs = categorize_mrs(mrs, since, until)
    closed, opened_issues, carried_issues = categorize_issues(issues, since, until)

    lines = [f"# {month_name}", ""]

    # Summary
    lines.append("## Summary")
    lines.append(f"- {len(merged)} MRs merged | {len(opened_mrs)} MRs opened | {len(carried_mrs)} MRs carried over")
    lines.append(f"- {len(closed)} issues closed | {len(opened_issues)} issues opened | {len(carried_issues)} issues carried over")
    lines.append("")

    if merged:
        lines.append(f"## Shipped ({len(merged)} MRs merged)")
        lines.extend(render_grouped(merged, format_mr))
        lines.append("")

    if opened_mrs:
        lines.append(f"## In Progress ({len(opened_mrs)} MRs opened)")
        lines.extend(render_grouped(opened_mrs, format_mr))
        lines.append("")

    if carried_mrs:
        lines.append(f"## Carried Over ({len(carried_mrs)} MRs)")
        lines.extend(render_grouped(carried_mrs, format_mr))
        lines.append("")

    if closed:
        lines.append(f"## Issues Closed ({len(closed)})")
        lines.extend(render_grouped(closed, format_issue))
        lines.append("")

    if opened_issues:
        lines.append(f"## Issues Opened ({len(opened_issues)})")
        lines.extend(render_grouped(opened_issues, format_issue))
        lines.append("")

    if carried_issues:
        lines.append(f"## Issues Carried Over ({len(carried_issues)})")
        lines.extend(render_grouped(carried_issues, format_issue))
        lines.append("")

    return "\n".join(lines)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate monthly GitLab resume")
    parser.add_argument("--month", help="Month to generate (YYYY-MM), defaults to current month")
    parser.add_argument("--last", "--last-month", action="store_true", help="Generate for previous month")
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
        year, month = add_months(now.year, now.month, -1)
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
        print(f"Saved to {filepath}", file=sys.stderr)


if __name__ == "__main__":
    main()
