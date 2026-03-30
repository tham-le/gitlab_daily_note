#!/usr/bin/env python3
"""
gitlab_daily_obsidian.py - Obsidian-flavored daily notes from GitLab data

Same data fetching as gitlab_daily_graphql.py, but with Obsidian-specific
formatting: callouts, dataview frontmatter, and styled status annotations.

Requires the `glab` CLI to be installed and authenticated.

Usage:
    python gitlab_daily_obsidian.py              # Generate today's note
    python gitlab_daily_obsidian.py --keep       # Save a snapshot on first run of the day
    python gitlab_daily_obsidian.py --init       # Create a config file with defaults
    python gitlab_daily_obsidian.py --stdout     # Print to stdout instead of saving
"""

import sys
from collections import defaultdict
from datetime import timedelta

from gitlab_daily_graphql import (
    GitLabSync,
    load_config,
    generate_config,
    CONFIG_FILENAME,
)


class ObsidianSync(GitLabSync):
    """GitLabSync with Obsidian-specific formatting."""

    # -- Status formatting -------------------------------------------------

    def _status_tag(self, text):
        """Wrap status text for visual distinction in Obsidian."""
        mapping = {
            "FAILED": "**`FAILED`**",
            "running": "`running`",
            "pending": "`pending`",
            "canceled": "`canceled`",
            "manual": "`manual`",
        }
        return mapping.get(text, f"`{text}`")

    def _get_pipeline_status(self, mr):
        pipeline = mr.get("head_pipeline") or {}
        status = pipeline.get("status", "")
        labels = {
            "failed": "FAILED",
            "running": "running",
            "pending": "pending",
            "canceled": "canceled",
            "created": "pending",
            "manual": "manual",
        }
        raw = labels.get(status, "")
        return self._status_tag(raw) if raw else ""

    def _get_approval_text(self, mr):
        info = self.mr_approvals.get(mr["web_url"])
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

    # -- Callout helpers ---------------------------------------------------

    @staticmethod
    def _callout(kind, title, lines):
        """Build an Obsidian callout block.

        kind: danger, warning, tip, info, note, example, quote
        """
        result = [f"> [!{kind}] {title}"]
        for line in lines:
            result.append(f"> {line}")
        result.append("")
        return result

    # -- Main formatting ---------------------------------------------------

    def format_markdown(self, date, todos, diff=None):
        lines = []

        # Pre-compute stats
        conflict_mrs = [mr for mr in self.mrs if mr.get("has_conflicts", False)]
        pending_discussion_count = sum(
            1 for v in self.mr_discussions.values() if v["pending"] > 0
        )
        ready_count = sum(1 for mr in self.mrs if self.get_mr_category(mr) == "ready_to_merge")
        STALE_DAYS = self.config.get("stale_days", 7)
        stale_count = sum(
            1 for mr in self.mrs
            if self.get_mr_category(mr) in ("needs_approval", "needs_review")
            and self._get_staleness(mr) >= STALE_DAYS
        )
        to_close_thread_count = sum(
            1 for v in self.mr_discussions.values() if v.get("to_close", 0) > 0
        )

        # --- Dataview frontmatter ---
        lines.append("---")
        lines.append(f"type: daily-note")
        lines.append(f"date: {date.strftime('%Y-%m-%d')}")
        lines.append(f"open_mrs: {len(self.mrs)}")
        lines.append(f"pending_threads: {pending_discussion_count}")
        lines.append(f"to_close: {to_close_thread_count}")
        lines.append(f"build_failures: {len(todos.get('build_failed', []))}")
        lines.append(f"conflicts: {len(conflict_mrs)}")
        lines.append(f"ready_to_merge: {ready_count}")
        lines.append(f"stale: {stale_count}")
        if self.recently_merged:
            lines.append(f"recently_merged: {len(self.recently_merged)}")
        lines.append("---")
        lines.append("")

        # --- Tags ---
        tags = self.config.get("tags", "")
        if tags:
            lines.append(tags)
            lines.append("")

        # --- Header with navigation ---
        lines.append(f"# {date.strftime('%A, %B %d, %Y')}")

        prev_date = date - timedelta(days=1)
        next_date = date + timedelta(days=1)
        base = str(self.base_dir)
        prev_link = f"{base}/{prev_date.strftime('%Y')}/{prev_date.strftime('%m-%b')}/{prev_date.strftime('%Y-%m-%d')}"
        next_link = f"{base}/{next_date.strftime('%Y')}/{next_date.strftime('%m-%b')}/{next_date.strftime('%Y-%m-%d')}"
        lines.append(f"<< [[{prev_link}|Yesterday]] | [[{next_link}|Tomorrow]] >>")
        lines.append("")

        # --- Summary as progress bar ---
        total = len(self.mrs)
        if total > 0:
            approved_pct = ready_count / total
            bar_len = 20
            filled = round(approved_pct * bar_len)
            bar = "=" * filled + "-" * (bar_len - filled)
            lines.append(f"`[{bar}]` {ready_count}/{total} ready to merge")
        lines.append("")
        lines.append(f"| Pending threads | To close | Build failures | Conflicts | Stale |")
        lines.append(f"|:---:|:---:|:---:|:---:|:---:|")
        lines.append(f"| {pending_discussion_count} | {to_close_thread_count} | {len(todos.get('build_failed', []))} | {len(conflict_mrs)} | {stale_count} |")
        lines.append("")

        # --- Categorize MRs (same logic as parent) ---
        act_now = []
        quick_wins = []
        ping_reviewer = []
        in_progress = []
        waiting = []
        on_hold_mrs = []
        act_now_urls = set()

        for mr in self.mrs:
            if self._is_on_hold(mr):
                on_hold_mrs.append(mr)

        active_mr_urls = {mr["web_url"] for mr in self.mrs if not self._is_on_hold(mr)}

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
                quick_wins.append(mr)
            elif category == "draft":
                in_progress.append(mr)
            elif category in ("needs_approval", "needs_review") and days_idle >= STALE_DAYS:
                ping_reviewer.append(mr)
            else:
                waiting.append(mr)

        # --- Act Now (danger callout) ---
        has_act_now = act_now or todos.get("build_failed") or conflict_mrs
        if has_act_now:
            callout_lines = []

            pending_thread_mrs = [
                mr for mr in act_now
                if self.mr_discussions.get(mr["web_url"], {}).get("pending", 0) > 0
            ]
            for mr in pending_thread_mrs:
                callout_lines.append(self.format_mr_line(mr))

            if todos.get("build_failed"):
                for todo in todos["build_failed"]:
                    title = todo["target"]["title"]
                    url = todo["target_url"]
                    callout_lines.append(f"- [ ] [{title}]({url}) — **`FAILED`**")

            non_pending_conflicts = [
                mr for mr in conflict_mrs
                if mr["web_url"] not in act_now_urls
            ]
            for mr in non_pending_conflicts:
                callout_lines.append(self.format_mr_line(mr))

            lines.extend(self._callout("danger", "Act Now", callout_lines))

        # --- Approved (tip callout) ---
        if quick_wins:
            callout_lines = [self.format_mr_line(mr) for mr in quick_wins]
            lines.extend(self._callout("tip", "Approved", callout_lines))

        # --- Threads to Close (warning callout) ---
        close_mrs = [
            mr for mr in self.mrs
            if mr["web_url"] in active_mr_urls
            and self.mr_discussions.get(mr["web_url"], {}).get("to_close", 0) > 0
        ]
        if close_mrs:
            callout_lines = []
            for mr in close_mrs:
                disc = self.mr_discussions[mr["web_url"]]
                line = f"- [ ] [!{mr['iid']}]({mr['web_url']}): {mr['title']}"
                line += f" — {disc['to_close']} to close"
                if disc.get("pending", 0) > 0:
                    line += f" | {disc['pending']} pending"
                callout_lines.append(line)
            lines.extend(self._callout("warning", "Threads to Close", callout_lines))

        # --- Ping Reviewer (warning callout) ---
        if ping_reviewer:
            callout_lines = [
                self.format_mr_line(mr)
                for mr in sorted(ping_reviewer, key=lambda m: self._get_staleness(m), reverse=True)
            ]
            lines.extend(self._callout("warning", "Ping Reviewer", callout_lines))

        # --- In Progress (note callout) ---
        if in_progress:
            callout_lines = [self.format_mr_line(mr) for mr in in_progress]
            lines.extend(self._callout("note", "In Progress", callout_lines))

        lines.append("---")
        lines.append("")

        # --- Waiting on Others ---
        filtered_issues = self.filter_relevant_issues()
        active_issues = [i for i in filtered_issues if not self._is_on_hold(i)]

        if waiting or active_issues:
            lines.append("## Waiting on Others")
            lines.append("")

            shown_mr_urls = set()
            shown_issue_urls = set()
            waiting_urls = {mr["web_url"] for mr in waiting}

            first_issue = True
            for issue in active_issues:
                linked_mrs = [
                    mr for mr in self.issue_to_mrs.get(issue["web_url"], [])
                    if mr["web_url"] in waiting_urls
                ]
                if not linked_mrs:
                    continue
                shown_issue_urls.add(issue["web_url"])
                if not first_issue:
                    lines.append("")
                first_issue = False
                lines.append(f"**[#{issue['iid']}]({issue['web_url']}): {issue['title']}**")
                for mr in linked_mrs:
                    lines.append(self.format_mr_line(mr))
                    shown_mr_urls.add(mr["web_url"])

            remaining_mrs = [mr for mr in waiting if mr["web_url"] not in shown_mr_urls]
            if remaining_mrs:
                if shown_issue_urls:
                    lines.append("")
                    lines.append("**No linked issue:**")
                repo_groups = defaultdict(list)
                for mr in remaining_mrs:
                    repo = self._get_repo_short_name(mr)
                    repo_groups[repo].append(mr)
                for repo in sorted(repo_groups.keys()):
                    lines.append(f"*{repo}:*")
                    for mr in repo_groups[repo]:
                        lines.append(self.format_mr_line(mr))

            all_mr_urls = {mr["web_url"] for mr in self.mrs}
            orphan_issues = [
                i for i in active_issues
                if i["web_url"] not in shown_issue_urls
                and not any(mr["web_url"] in all_mr_urls for mr in self.issue_to_mrs.get(i["web_url"], []))
            ]
            if orphan_issues:
                lines.append("")
                lines.append("**Issues (no open MR):**")
                for issue in orphan_issues:
                    line = self._format_issue_line(issue)
                    all_issue_mrs = self.issue_all_mrs.get(issue["web_url"], [])
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

        # --- On Hold (quote callout) ---
        on_hold_issues = [i for i in self.filter_relevant_issues() if self._is_on_hold(i)]
        if on_hold_mrs or on_hold_issues:
            label = self.config.get("on_hold_label", "On Hold")
            callout_lines = []
            for mr in on_hold_mrs:
                callout_lines.append(self.format_mr_line(mr))
            for issue in on_hold_issues:
                callout_lines.append(self._format_issue_line(issue))
            lines.extend(self._callout("quote", label, callout_lines))

        # --- To Review (team MRs) ---
        if self.team_mrs:
            lines.append("---")
            lines.append("")
            callout_lines = []
            repo_groups = defaultdict(list)
            for mr in self.team_mrs:
                repo = self._get_repo_short_name(mr)
                repo_groups[repo].append(mr)
            for repo in sorted(repo_groups.keys()):
                if len(repo_groups) > 1:
                    callout_lines.append(f"*{repo}:*")
                for mr in repo_groups[repo]:
                    author = mr.get("author", {}).get("username", "?")
                    line = f"- [ ] [!{mr['iid']}]({mr['web_url']}): {mr['title']} — by @{author}"
                    days = self._get_staleness(mr)
                    if days >= 3:
                        line += f" | idle {days}d"
                    callout_lines.append(line)
            lines.extend(self._callout("info", "To Review", callout_lines))

        # --- Recently Merged / Changes ---
        if self.recently_merged:
            lines.append("## Recently Merged")
            for mr in self.recently_merged:
                merged_at = mr.get("merged_at", "")[:10]
                lines.append(
                    f"- [!{mr['iid']}]({mr['web_url']}): {mr['title']} ({merged_at})"
                )
            lines.append("")

        if diff and diff["days_back"] <= 2:
            has_changes = (
                diff["new_mrs"] or diff["merged_mrs"] or diff["gone_mr_urls"]
                or diff["new_issues"] or diff["resolved_issues"]
            )
            if has_changes:
                label = "Yesterday" if diff["days_back"] == 1 else f"{diff['prev_date'].strftime('%b %d')}"
                lines.append(f"## Changes Since {label}")
                if diff["merged_mrs"]:
                    for mr in diff["merged_mrs"]:
                        lines.append(
                            f"- Merged: [!{mr['iid']}]({mr['web_url']}): {mr['title']}"
                        )
                if diff["gone_mr_urls"]:
                    lines.append(f"- {len(diff['gone_mr_urls'])} MR(s) closed/removed")
                if diff["new_mrs"]:
                    for mr in diff["new_mrs"]:
                        lines.append(
                            f"- New: [!{mr['iid']}]({mr['web_url']}): {mr['title']}"
                        )
                if diff["resolved_issues"]:
                    lines.append(f"- {len(diff['resolved_issues'])} issue(s) resolved")
                if diff["new_issues"]:
                    for issue in diff["new_issues"]:
                        lines.append(
                            f"- New issue: [#{issue['iid']}]({issue['web_url']}): {issue['title']}"
                        )
                lines.append("")

        lines.append("## Notes")
        lines.append("- ")

        return "\n".join(lines)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate Obsidian daily notes from GitLab data (GraphQL + Obsidian formatting)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="First run: use --init to generate a .daily-note.json config file.",
    )
    parser.add_argument("--init", action="store_true", help="Generate a starter config file and exit")
    parser.add_argument("--config", default=None, help=f"Path to config file (default: {CONFIG_FILENAME})")
    parser.add_argument("--team", action="store_true", help="Include team MRs for review")
    parser.add_argument("--no-cleanup", action="store_true", help="Don't auto-dismiss stale todos")
    parser.add_argument("--stdout", action="store_true", help="Print to stdout instead of saving")
    parser.add_argument("--keep", action="store_true", help="Save a snapshot of the first run of the day")
    args = parser.parse_args()

    if args.init:
        generate_config(args.config)
        sys.exit(0)

    config = load_config(args.config)
    sync = ObsidianSync(config=config, auto_cleanup=not args.no_cleanup)
    sync.include_team = args.team

    if args.stdout:
        note = sync.generate_daily_note()
        print(note)
    else:
        filepath = sync.save_daily_note(keep=args.keep)
