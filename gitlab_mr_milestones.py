#!/usr/bin/env python3
"""
gitlab_mr_milestones.py - Count a user's merged MRs per milestone.

Usage:
    python gitlab_mr_milestones.py                  # current glab user, all time
    python gitlab_mr_milestones.py @username
    python gitlab_mr_milestones.py --repo my-repo    # filter to one repo
    python gitlab_mr_milestones.py --json
    python gitlab_mr_milestones.py --md > out.md     # full MR list, one section per milestone
"""

import argparse
import json
import re
import sys
from collections import defaultdict

from gitlab_common import run_graphql

MRS_QUERY_TEMPLATE = """
{{
    user(username: "{username}") {{
        authoredMergeRequests(state: merged, first: 100{after}) {{
            pageInfo {{ hasNextPage endCursor }}
            nodes {{
                iid
                title
                webUrl
                mergedAt
                milestone {{ title dueDate }}
            }}
        }}
    }}
}}
"""


def fetch_merged_mrs(username):
    mrs = []
    cursor = None
    while True:
        after = f', after: "{cursor}"' if cursor else ""
        query = MRS_QUERY_TEMPLATE.format(username=username, after=after)
        data = run_graphql(query)
        if not data or not data.get("user"):
            break
        conn = data["user"]["authoredMergeRequests"]
        mrs.extend(conn["nodes"])
        page_info = conn["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
    return mrs


def get_repo_short_name(web_url):
    match = re.search(r"/([^/]+)/-/merge_requests/", web_url)
    return match.group(1) if match else "unknown"


def group_by_milestone(mrs, repo_filter=None):
    groups = defaultdict(list)
    for mr in mrs:
        repo = get_repo_short_name(mr["webUrl"])
        if repo_filter and repo != repo_filter:
            continue
        milestone = mr.get("milestone") or {}
        title = milestone.get("title") or "(no milestone)"
        groups[title].append(mr)
    return groups


def sort_key(title):
    # Sort semver-like milestone titles (e.g. "0.28.0") numerically; push
    # "(no milestone)" to the end.
    parts = re.findall(r"\d+", title)
    if not parts:
        return (1, [], title)
    return (0, [int(p) for p in parts], title)


def render_markdown(username, groups, ordered_titles, repo_filter=None):
    total = sum(len(v) for v in groups.values())
    scope = f" ({repo_filter})" if repo_filter else ""
    lines = [f"# Merged MRs by Milestone{scope}", ""]
    lines.append(f"Author: @{username}. Total merged: {total}.")
    lines.append("")
    lines.append("| Milestone | Merged |")
    lines.append("|-----------|--------|")
    for title in ordered_titles:
        lines.append(f"| {title} | {len(groups[title])} |")
    lines.append("")
    for title in ordered_titles:
        mrs = sorted(groups[title], key=lambda m: m.get("mergedAt") or "", reverse=True)
        lines.append(f"## {title} ({len(mrs)})")
        lines.append("")
        for mr in mrs:
            repo = get_repo_short_name(mr["webUrl"])
            date = (mr.get("mergedAt") or "")[:10]
            lines.append(f"- [{repo}!{mr['iid']}]({mr['webUrl']}): {mr['title']} ({date})")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main():
    parser = argparse.ArgumentParser(
        description="Count a user's merged MRs per milestone"
    )
    parser.add_argument("user", nargs="?", help="GitLab username (with or without @), default: the glab user")
    parser.add_argument("--repo", help="Filter to a single repo (short name, e.g. my-repo)")
    parser.add_argument("--json", action="store_true", help="Output JSON instead of a table")
    parser.add_argument("--md", action="store_true", help="Output Markdown with the full MR list per milestone")
    args = parser.parse_args()

    if args.user:
        username = args.user.lstrip("@")
    else:
        data = run_graphql("{ currentUser { username } }")
        if not data or not data.get("currentUser"):
            sys.exit("Could not read the current user from glab; pass a username")
        username = data["currentUser"]["username"]

    print(f"Fetching merged MRs authored by @{username}...", file=sys.stderr)
    mrs = fetch_merged_mrs(username)
    print(f"  {len(mrs)} merged MRs found", file=sys.stderr)

    groups = group_by_milestone(mrs, repo_filter=args.repo)
    ordered_titles = sorted(groups, key=sort_key)

    if args.json:
        result = {title: len(groups[title]) for title in ordered_titles}
        print(json.dumps(result, indent=2))
        return

    if args.md:
        print(render_markdown(username, groups, ordered_titles, repo_filter=args.repo))
        return

    total = sum(len(v) for v in groups.values())
    width = max((len(t) for t in ordered_titles), default=10)
    print(f"\nMerged MRs by milestone for @{username}"
          + (f" ({args.repo})" if args.repo else "") + ":\n")
    for title in ordered_titles:
        count = len(groups[title])
        print(f"  {title.ljust(width)}  {count}")
    print(f"\n  {'TOTAL'.ljust(width)}  {total}")


if __name__ == "__main__":
    main()
