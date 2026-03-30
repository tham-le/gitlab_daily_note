#!/usr/bin/env python3
"""
gitlab_daily_note.py - Auto-generate Obsidian daily notes from GitLab data

Generates an action-based daily note from your GitLab MRs, issues, and todos.
Requires the `glab` CLI to be installed and authenticated.

Usage:
    python gitlab_daily_note.py              # Generate today's note
    python gitlab_daily_note.py --keep       # Save a snapshot on first run of the day
    python gitlab_daily_note.py --init       # Create a config file with defaults
    python gitlab_daily_note.py --stdout     # Print to stdout instead of saving
"""

import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

CONFIG_FILENAME = ".daily-note.json"


DEFAULT_CONFIG = {
    "base_dir": "daily-note",
    "tags": "",
    "gitlab_group": "",
    "on_hold_patterns": [],
    "on_hold_label": "On Hold",
    "stale_days": 7,
}


def load_config(config_path=None):
    """Load config from file, falling back to defaults"""
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
    """Generate a starter config file"""
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
        "tags": "Obsidian frontmatter/tags line prepended to each note (leave empty to omit)",
        "gitlab_group": "GitLab group path for --team mode (e.g. 'my-org/my-project')",
        "on_hold_patterns": "Strings to match in MR/issue paths for 'on hold' items (e.g. ['client-name', 'legacy'])",
        "on_hold_label": "Section heading for on-hold items",
        "stale_days": "Days of inactivity before an MR is flagged for pinging reviewer",
    }

    with open(config_path, "w") as f:
        json.dump(example, f, indent=2)
    print(f"Generated config at {config_path}", file=sys.stderr)
    print(f"Edit it to customize your daily notes.", file=sys.stderr)


class GitLabSync:
    def __init__(self, config=None, auto_cleanup=True):
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
        self.issue_to_mrs = defaultdict(list)  # issue web_url -> [mr objects]
        self.issue_all_mrs = defaultdict(list)  # issue web_url -> all related MRs (inc. merged/closed)
        self.mr_to_issues = defaultdict(list)  # mr web_url -> [issue objects]
        self.team_mrs = []  # MRs from team to review
        self.on_hold_patterns = [p.lower() for p in self.config.get("on_hold_patterns", [])]
        self.include_team = False

    def get_daily_note_path(self, date):
        """Get the correct path for daily note based on date"""
        year = date.strftime("%Y")
        month = date.strftime("%m-%b")
        filename = date.strftime("%Y-%m-%d.md")

        folder = self.base_dir / year / month
        folder.mkdir(parents=True, exist_ok=True)

        return folder / filename

    def get_snapshot_path(self, date):
        """Get the path for the first-run snapshot"""
        year = date.strftime("%Y")
        month = date.strftime("%m-%b")
        filename = date.strftime("%Y-%m-%d-snapshot.md")

        folder = self.base_dir / year / month
        folder.mkdir(parents=True, exist_ok=True)

        return folder / filename

    def read_existing_note(self, filepath):
        """Read existing note and extract the Notes section"""
        if not filepath.exists():
            return None

        with open(filepath, "r") as f:
            content = f.read()

        notes_match = re.search(
            r"^## Notes\s*\n(.*)$", content, re.MULTILINE | re.DOTALL
        )
        if notes_match:
            return notes_match.group(1).strip()

        return None

    def merge_with_existing(self, new_content, existing_notes):
        """Merge new generated content with existing notes"""
        if not existing_notes:
            return new_content

        notes_pattern = r"^## Notes\s*\n- \s*$"
        if re.search(notes_pattern, new_content, re.MULTILINE):
            new_content = re.sub(
                notes_pattern,
                f"## Notes\n{existing_notes}",
                new_content,
                flags=re.MULTILINE,
            )

        return new_content

    def run_command(self, cmd):
        """Run command and return output"""
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            print(f"Error running command: {' '.join(cmd)}", file=sys.stderr)
            print(f"Error: {e.stderr}", file=sys.stderr)
            return None

    def dismiss_todo(self, todo_id):
        """Mark a todo as done"""
        print(f"Dismissing todo {todo_id}...", file=sys.stderr)
        output = self.run_command(
            ["glab", "api", "-X", "POST", f"todos/{todo_id}/mark_as_done"]
        )
        return output is not None

    def fetch_todos(self):
        """Get pending todos from GitLab"""
        output = self.run_command(["glab", "api", "todos?state=pending&per_page=100"])
        if output:
            self.todos = json.loads(output)
            print(f"Found {len(self.todos)} todos", file=sys.stderr)
        else:
            print("No todos fetched", file=sys.stderr)

    def fetch_user_info(self):
        """Fetch and cache current user info"""
        if self.username:
            return True
        user_info = self.run_command(["glab", "api", "user"])
        if not user_info:
            print("Could not get user info", file=sys.stderr)
            return False
        user_data = json.loads(user_info)
        self.user_id = user_data["id"]
        self.username = user_data.get("username", "unknown")
        print(f"User: {self.username} (ID: {self.user_id})", file=sys.stderr)
        return True

    def fetch_my_mrs(self):
        """Get your open MRs using API (authored + assigned)"""
        if not self.fetch_user_info():
            return

        mrs_by_id = {}

        for param in [f"author_id={self.user_id}", f"assignee_id={self.user_id}"]:
            output = self.run_command(
                [
                    "glab",
                    "api",
                    f"merge_requests?{param}&state=opened&scope=all&per_page=100",
                ]
            )
            if output:
                mrs = json.loads(output)
                if isinstance(mrs, list):
                    for mr in mrs:
                        if isinstance(mr, dict):
                            mrs_by_id[mr["id"]] = mr

        self.mrs = list(mrs_by_id.values())
        print(f"Found {len(self.mrs)} MRs", file=sys.stderr)

    def fetch_recently_merged(self, since_date):
        """Fetch MRs merged since a given date"""
        if not self.fetch_user_info():
            return

        since_iso = since_date.strftime("%Y-%m-%dT00:00:00Z")
        output = self.run_command(
            [
                "glab",
                "api",
                f"merge_requests?author_id={self.user_id}&state=merged&scope=all"
                f"&updated_after={since_iso}&per_page=50&order_by=updated_at",
            ]
        )

        if output:
            mrs = json.loads(output)
            if not isinstance(mrs, list):
                print(f"Unexpected response for recently merged MRs: {str(mrs)[:200]}", file=sys.stderr)
                return
            # Filter to only those actually merged after since_date
            self.recently_merged = [
                mr for mr in mrs
                if isinstance(mr, dict) and mr.get("merged_at") and mr["merged_at"] >= since_iso
            ]
            print(f"Found {len(self.recently_merged)} recently merged MRs", file=sys.stderr)
        else:
            print("No recently merged MRs fetched", file=sys.stderr)

    def fetch_mr_discussions(self):
        """Fetch discussion threads for MRs that have notes"""
        mrs_with_notes = [mr for mr in self.mrs if mr.get("user_notes_count", 0) > 0]
        print(f"Fetching discussions for {len(mrs_with_notes)} MRs with notes...", file=sys.stderr)

        for mr in mrs_with_notes:
            project_id = mr["project_id"]
            iid = mr["iid"]
            output = self.run_command(
                ["glab", "api", f"projects/{project_id}/merge_requests/{iid}/discussions?per_page=100"]
            )
            if not output:
                continue

            discussions = json.loads(output)
            if not isinstance(discussions, list):
                continue
            pending = 0
            answered = 0
            resolved = 0
            to_close = 0
            pending_authors = set()

            for disc in discussions:
                notes = disc.get("notes", [])
                if not notes:
                    continue

                if notes[0].get("system", False):
                    continue

                if not notes[0].get("resolvable", False):
                    continue

                if all(n.get("resolved", False) for n in notes if n.get("resolvable")):
                    resolved += 1
                    continue

                first_author = notes[0].get("author", {}).get("username", "")
                last_note = notes[-1]
                last_author = last_note.get("author", {}).get("username", "")

                if first_author == self.username:
                    # I opened this thread
                    if last_author == self.username:
                        answered += 1  # waiting for colleague response
                    else:
                        to_close += 1  # colleague replied, I need to resolve
                else:
                    # Colleague opened this thread
                    if last_author == self.username:
                        answered += 1  # I replied, waiting for them
                    else:
                        pending += 1   # I need to answer
                        pending_authors.add(last_author)

            self.mr_discussions[mr["web_url"]] = {
                "pending": pending,
                "answered": answered,
                "to_close": to_close,
                "resolved": resolved,
                "pending_authors": pending_authors,
            }

        discussed = sum(1 for v in self.mr_discussions.values() if v["pending"] > 0 or v["answered"] > 0 or v["to_close"] > 0)
        print(f"  {discussed} MRs with active discussions", file=sys.stderr)

    def fetch_mr_approvals(self):
        """Fetch approval info for non-draft open MRs"""
        non_draft_mrs = [
            mr for mr in self.mrs
            if not mr.get("work_in_progress", False) and not mr.get("draft", False)
        ]
        print(f"Fetching approvals for {len(non_draft_mrs)} non-draft MRs...", file=sys.stderr)

        for mr in non_draft_mrs:
            project_id = mr["project_id"]
            iid = mr["iid"]
            output = self.run_command(
                ["glab", "api", f"projects/{project_id}/merge_requests/{iid}/approvals"]
            )
            if not output:
                continue

            data = json.loads(output)
            approved_by = [
                a.get("user", {}).get("username", "?")
                for a in data.get("approved_by", [])
            ]
            approvals_required = data.get("approvals_required", 0)
            approvals_left = data.get("approvals_left", 0)

            self.mr_approvals[mr["web_url"]] = {
                "approved_by": approved_by,
                "approvals_required": approvals_required,
                "approvals_left": approvals_left,
            }

        approved_count = sum(1 for v in self.mr_approvals.values() if v["approved_by"])
        print(f"  {approved_count} MRs have approvals", file=sys.stderr)

    def fetch_team_mrs(self):
        """Fetch open MRs from the group that need review (not yours, not approved)"""
        if not self.fetch_user_info():
            return

        groups = self.config.get("gitlab_groups", [])
        # Backwards compat: single string gitlab_group
        if not groups:
            single = self.config.get("gitlab_group", "")
            if single:
                groups = [single]
        if not groups:
            print("No gitlab_group(s) configured, skipping team MRs", file=sys.stderr)
            return

        all_mrs = []
        for group in groups:
            encoded_group = group.replace("/", "%2F")
            output = self.run_command(
                [
                    "glab", "api",
                    f"groups/{encoded_group}/merge_requests"
                    f"?state=opened&per_page=100&scope=all",
                ]
            )
            if output:
                all_mrs.extend(json.loads(output))

        # Exclude own MRs and drafts
        candidates = [
            mr for mr in all_mrs
            if mr.get("author", {}).get("id") != self.user_id
            and not mr.get("draft", False)
            and not mr.get("work_in_progress", False)
        ]

        self.team_mrs = candidates
        print(f"Found {len(self.team_mrs)} team MRs to review", file=sys.stderr)

    def build_issue_mr_links(self):
        """Link issues to MRs via GitLab related_merge_requests API"""
        mr_url_set = {mr["web_url"] for mr in self.mrs}
        mr_by_url = {mr["web_url"]: mr for mr in self.mrs}

        print(f"Fetching related MRs for {len(self.issues)} issues...", file=sys.stderr)
        for issue in self.issues:
            project_id = issue["project_id"]
            iid = issue["iid"]
            output = self.run_command(
                ["glab", "api", f"projects/{project_id}/issues/{iid}/related_merge_requests"]
            )
            if not output:
                continue

            related_mrs = json.loads(output)
            if not isinstance(related_mrs, list):
                continue
            for related in related_mrs:
                if not isinstance(related, dict):
                    continue
                url = related.get("web_url", "")
                self.issue_all_mrs[issue["web_url"]].append(related)
                if url in mr_url_set:
                    mr = mr_by_url[url]
                    self.issue_to_mrs[issue["web_url"]].append(mr)
                    self.mr_to_issues[url].append(issue)

        linked = sum(1 for v in self.issue_to_mrs.values() if v)
        print(f"  {linked} issues linked to open MRs", file=sys.stderr)

    @staticmethod
    def _get_repo_short_name(item):
        """Extract short repo name from references.full"""
        ref = item.get("references", {}).get("full", "")
        # e.g. my-org/my-project/apps/my-app!12 -> my-app
        parts = ref.split("/")
        if len(parts) >= 2:
            last = parts[-1]
            # Strip the !iid or #iid suffix
            repo = re.sub(r"[!#]\d+$", "", last)
            return repo
        return "unknown"

    def fetch_issues(self):
        """Get assigned issues using API"""
        if not self.fetch_user_info():
            return

        output = self.run_command(
            [
                "glab",
                "api",
                f"issues?assignee_id={self.user_id}&state=opened&scope=all&per_page=100",
            ]
        )

        if output:
            self.issues = json.loads(output)
            print(f"Found {len(self.issues)} issues", file=sys.stderr)
        else:
            print("No issues fetched", file=sys.stderr)

    def filter_relevant_issues(self):
        """Filter issues to only show actionable ones"""
        filtered = []

        for issue in self.issues:
            labels = [label.lower() for label in issue.get("labels", [])]
            if any(
                skip in labels for skip in ["done", "blocked", "wontfix", "duplicate"]
            ):
                continue
            filtered.append(issue)

        return filtered

    def categorize_todos(self):
        """Group todos by type and relevance"""
        categories = {
            "review_submitted": [],
            "build_failed": [],
            "unmergeable": [],
            "assigned": [],
            "needs_action": [],
        }

        recent_cutoff = datetime.now(timezone.utc) - timedelta(days=3)
        stale_todos = []

        for todo in self.todos:
            created = datetime.fromisoformat(todo["created_at"].replace("Z", "+00:00"))

            target = todo.get("target", {})
            if target:
                state = target.get("state", "")
                if state in ["closed", "merged"]:
                    stale_todos.append(todo)
                    continue

            if todo["action_name"] in ["build_failed", "unmergeable"]:
                action = todo["action_name"]
                categories[action].append(todo)
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
            print(
                f"\nFound {len(stale_todos)} stale todos (merged/closed)",
                file=sys.stderr,
            )
        for cat, items in categories.items():
            if items:
                print(f"  {cat}: {len(items)} items", file=sys.stderr)

        return categories

    def _is_on_hold(self, item):
        """Check if an item matches any on-hold pattern"""
        if not self.on_hold_patterns:
            return False
        full_ref = item.get("references", {}).get("full", "").lower()
        return any(pattern in full_ref for pattern in self.on_hold_patterns)

    def _find_previous_note(self, date, max_lookback=7):
        """Find the most recent previous daily note, looking back up to max_lookback days"""
        for days_back in range(1, max_lookback + 1):
            prev_date = date - timedelta(days=days_back)
            # Try standard path first
            filepath = self.get_daily_note_path(prev_date)
            if filepath.exists():
                return filepath, prev_date
            # Try legacy format with day-of-week suffix (e.g. 2026-03-04-Wed.md)
            folder = filepath.parent
            prefix = prev_date.strftime("%Y-%m-%d")
            for f in folder.glob(f"{prefix}*.md"):
                if "snapshot" not in f.name:
                    return f, prev_date
        return None, None

    def compute_diff_from_previous(self, date):
        """Compare current state with previous note to find changes"""
        filepath, prev_date = self._find_previous_note(date)
        if not filepath:
            return None

        with open(filepath, "r") as f:
            content = f.read()

        days_back = (date - prev_date).days

        # Use created_at to find genuinely new items (not just "missing from old note")
        since_iso = prev_date.strftime("%Y-%m-%dT00:00:00Z")
        new_mrs = [
            mr for mr in self.mrs
            if mr.get("created_at", "") >= since_iso
        ]

        # Extract MR URLs from previous note to detect gone/merged
        prev_mr_urls = set(re.findall(
            r"\[!(\d+)\]\((https://[^)]+/merge_requests/\d+)\)",
            content,
        ))
        prev_mr_url_set = {url for _, url in prev_mr_urls}
        current_mr_urls = {mr["web_url"] for mr in self.mrs}
        gone_mr_urls = prev_mr_url_set - current_mr_urls

        merged_mrs = [mr for mr in self.recently_merged if mr["web_url"] in gone_mr_urls]

        # Extract issue URLs to detect resolved
        prev_issue_urls = set(re.findall(
            r"\[#(\d+)\]\((https://[^)]+/work_items/\d+)\)",
            content,
        ))
        prev_issue_url_set = {url for _, url in prev_issue_urls}
        current_issue_urls = {issue["web_url"] for issue in self.issues}
        gone_issue_urls = prev_issue_url_set - current_issue_urls

        resolved_issues_info = []
        for iid_str, url in prev_issue_urls:
            if url in gone_issue_urls:
                resolved_issues_info.append((iid_str, url))

        new_issues = [
            i for i in self.issues
            if i.get("created_at", "") >= since_iso
        ]

        return {
            "days_back": days_back,
            "prev_date": prev_date,
            "new_mrs": new_mrs,
            "merged_mrs": merged_mrs,
            "gone_mr_urls": gone_mr_urls - {mr["web_url"] for mr in merged_mrs},
            "new_issues": new_issues,
            "resolved_issues": resolved_issues_info,
        }

    def generate_daily_note(self, date=None):
        """Generate Obsidian daily note"""
        if date is None:
            date = datetime.now()

        print("Fetching todos...", file=sys.stderr)
        self.fetch_todos()
        print("\nFetching MRs...", file=sys.stderr)
        self.fetch_my_mrs()
        print("\nFetching recently merged...", file=sys.stderr)
        self.fetch_recently_merged(date - timedelta(days=3))
        print("\nFetching issues...", file=sys.stderr)
        self.fetch_issues()
        print("\nFetching MR discussions...", file=sys.stderr)
        self.fetch_mr_discussions()
        print("\nFetching MR approvals...", file=sys.stderr)
        self.fetch_mr_approvals()
        print("\nLinking issues to MRs...", file=sys.stderr)
        self.build_issue_mr_links()

        if self.include_team:
            print("\nFetching team MRs to review...", file=sys.stderr)
            self.fetch_team_mrs()

        print("\nCategorizing todos...", file=sys.stderr)
        todo_categories = self.categorize_todos()

        print("\nComputing diff from previous note...", file=sys.stderr)
        diff = self.compute_diff_from_previous(date)

        print("\nGenerating markdown...", file=sys.stderr)
        result = self.format_markdown(date, todo_categories, diff)
        print(f"Generated {len(result)} characters", file=sys.stderr)
        return result

    def get_mr_category(self, mr):
        """Determine MR category"""
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

    def _get_staleness(self, mr):
        """Return days since last MR activity"""
        updated = mr.get("updated_at", "")
        if not updated:
            return 0
        updated_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - updated_dt
        return delta.days

    def _get_pipeline_status(self, mr):
        """Return pipeline status text"""
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
        return labels.get(status, "")

    def _get_approval_text(self, mr):
        """Return approval annotation for an MR"""
        info = self.mr_approvals.get(mr["web_url"])
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

    def format_mr_line(self, mr):
        """Format a single MR line with all annotations"""
        line = f"- [ ] [!{mr['iid']}]({mr['web_url']}): {mr['title']}"

        annotations = []

        # Pipeline status
        pipeline = self._get_pipeline_status(mr)
        if pipeline:
            annotations.append(pipeline)

        # Staleness
        days = self._get_staleness(mr)
        if days >= 3:
            annotations.append(f"idle {days}d")

        # Approval info
        approval = self._get_approval_text(mr)
        if approval:
            annotations.append(approval)

        # Discussion threads
        disc = self.mr_discussions.get(mr["web_url"])
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

    def format_markdown(self, date, todos, diff=None):
        """Format as Obsidian markdown"""
        lines = []
        shown_urls = set()

        # Header
        tags = self.config.get("tags", "")
        if tags:
            lines.append(tags)
            lines.append("")
        lines.append(f"# {date.strftime('%A, %B %d, %Y')}")

        prev_date = date - timedelta(days=1)
        next_date = date + timedelta(days=1)

        prev_year = prev_date.strftime("%Y")
        prev_month = prev_date.strftime("%m-%b")
        prev_filename = prev_date.strftime("%Y-%m-%d")

        next_year = next_date.strftime("%Y")
        next_month = next_date.strftime("%m-%b")
        next_filename = next_date.strftime("%Y-%m-%d")

        base = str(self.base_dir)
        lines.append(
            f"<< [[{base}/{prev_year}/{prev_month}/{prev_filename}|Yesterday]] | "
            f"[[{base}/{next_year}/{next_month}/{next_filename}|Tomorrow]] >>"
        )
        lines.append("")

        # Pre-compute categories for summary and later use
        conflict_mrs = [mr for mr in self.mrs if mr.get("has_conflicts", False)]
        pending_discussion_count = sum(
            1 for v in self.mr_discussions.values() if v["pending"] > 0
        )
        ready_count = sum(1 for mr in self.mrs if self.get_mr_category(mr) == "ready_to_merge")
        stale_count = sum(
            1 for mr in self.mrs
            if self.get_mr_category(mr) in ("needs_approval", "needs_review")
            and self._get_staleness(mr) >= 7
        )
        to_close_thread_count = sum(
            1 for v in self.mr_discussions.values() if v["to_close"] > 0
        )

        lines.append("## Summary")
        lines.append(f"- {pending_discussion_count} pending threads | {to_close_thread_count} to close | {len(todos['build_failed'])} build failures | {len(conflict_mrs)} conflicts")
        lines.append(f"- {ready_count} ready to merge | {stale_count} need a ping | {len(self.mrs)} total open MRs")
        if self.recently_merged:
            lines.append(f"- {len(self.recently_merged)} recently merged")
        if self.team_mrs:
            lines.append(f"- {len(self.team_mrs)} team MRs to review")
        lines.append("")

        # Categorize ALL MRs by action (what should I do?)
        STALE_DAYS = self.config.get("stale_days", 7)
        act_now = []       # pending threads, build failures, conflicts
        quick_wins = []    # ready to merge, approved
        ping_reviewer = [] # idle 7+ days waiting for review/approval
        in_progress = []   # drafts
        waiting = []       # everything else (blocked on others)
        on_hold_mrs = []   # on hold

        # Track MR URLs shown in act_now to avoid duplication
        act_now_urls = set()

        # Separate on-hold MRs first
        for mr in self.mrs:
            if self._is_on_hold(mr):
                on_hold_mrs.append(mr)

        active_mr_urls = {mr["web_url"] for mr in self.mrs if not self._is_on_hold(mr)}

        # Pending threads — FIRST priority
        for mr in self.mrs:
            if mr["web_url"] not in active_mr_urls:
                continue
            disc = self.mr_discussions.get(mr["web_url"], {})
            if disc.get("pending", 0) > 0:
                act_now.append(mr)
                act_now_urls.add(mr["web_url"])

        # Build failures — also track their MR web_urls to exclude from other buckets
        build_failure_mr_urls = set()
        for todo in todos.get("build_failed", []):
            target = todo.get("target", {})
            if target.get("web_url"):
                build_failure_mr_urls.add(target["web_url"])

        # Add conflict MRs not already in act_now
        for mr in conflict_mrs:
            if mr["web_url"] not in act_now_urls and mr["web_url"] in active_mr_urls:
                act_now.append(mr)
                act_now_urls.add(mr["web_url"])

        # URLs to exclude from other buckets
        excluded_urls = act_now_urls | build_failure_mr_urls

        # Categorize remaining active MRs
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

        # --- Act Now ---
        has_act_now = act_now or todos.get("build_failed") or conflict_mrs
        if has_act_now:
            lines.append("## Act Now")

            # Pending threads first
            pending_thread_mrs = [
                mr for mr in act_now
                if self.mr_discussions.get(mr["web_url"], {}).get("pending", 0) > 0
            ]
            if pending_thread_mrs:
                for mr in pending_thread_mrs:
                    lines.append(self.format_mr_line(mr))

            # Build failures
            if todos.get("build_failed"):
                for todo in todos["build_failed"]:
                    title = todo["target"]["title"]
                    url = todo["target_url"]
                    lines.append(f"- [ ] [{title}]({url}) — FAILED")

            # Conflicts not already shown as pending thread
            non_pending_conflicts = [
                mr for mr in conflict_mrs
                if mr["web_url"] not in act_now_urls
            ]
            if non_pending_conflicts:
                for mr in non_pending_conflicts:
                    lines.append(self.format_mr_line(mr))

            lines.append("")

        # --- Approved ---
        if quick_wins:
            lines.append("## Approved")
            for mr in quick_wins:
                lines.append(self.format_mr_line(mr))
            lines.append("")

        # --- Threads to Close (I opened, colleague replied, I need to resolve) ---
        close_mrs = [
            mr for mr in self.mrs
            if mr["web_url"] in active_mr_urls
            and self.mr_discussions.get(mr["web_url"], {}).get("to_close", 0) > 0
        ]
        if close_mrs:
            lines.append("## Threads to Close")
            for mr in close_mrs:
                disc = self.mr_discussions[mr["web_url"]]
                line = f"- [ ] [!{mr['iid']}]({mr['web_url']}): {mr['title']}"
                line += f" — {disc['to_close']} to close"
                if disc.get("pending", 0) > 0:
                    line += f" | {disc['pending']} pending"
                lines.append(line)
            lines.append("")

        # --- Ping Reviewer ---
        if ping_reviewer:
            lines.append("## Ping Reviewer")
            for mr in sorted(ping_reviewer, key=lambda m: self._get_staleness(m), reverse=True):
                lines.append(self.format_mr_line(mr))
            lines.append("")

        # --- In Progress ---
        if in_progress:
            lines.append("## In Progress")
            for mr in in_progress:
                lines.append(self.format_mr_line(mr))
            lines.append("")

        lines.append("---")
        lines.append("")

        # --- Waiting (blocked on others) + Issues (unified) ---
        filtered_issues = self.filter_relevant_issues()
        active_issues = [
            i for i in filtered_issues
            if not self._is_on_hold(i)
        ]

        if waiting or active_issues:
            lines.append("## Waiting on Others")
            lines.append("")

            # 1. Issues with their linked MRs
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

            # 2. Remaining MRs grouped by repo
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

            # 3. Orphan issues (no linked open MR at all)
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
                    all_mrs = self.issue_all_mrs.get(issue["web_url"], [])
                    merged = sum(1 for m in all_mrs if m.get("state") == "merged")
                    closed = sum(1 for m in all_mrs if m.get("state") == "closed")
                    statuses = []
                    if merged:
                        statuses.append(f"{merged} MR merged")
                    if closed:
                        statuses.append(f"{closed} MR closed")
                    if statuses:
                        line += f" — {', '.join(statuses)}"
                    lines.append(line)

            lines.append("")

        # --- On Hold ---
        on_hold_issues = [
            i for i in self.filter_relevant_issues()
            if self._is_on_hold(i)
        ]
        if on_hold_mrs or on_hold_issues:
            label = self.config.get("on_hold_label", "On Hold")
            lines.append(f"## {label}")
            for mr in on_hold_mrs:
                lines.append(self.format_mr_line(mr))
            for issue in on_hold_issues:
                lines.append(self._format_issue_line(issue))
            lines.append("")

        # --- To Review (team MRs) ---
        if self.team_mrs:
            lines.append("---")
            lines.append("")
            lines.append("## To Review")
            repo_groups = defaultdict(list)
            for mr in self.team_mrs:
                repo = self._get_repo_short_name(mr)
                repo_groups[repo].append(mr)
            for repo in sorted(repo_groups.keys()):
                if len(repo_groups) > 1:
                    lines.append(f"*{repo}:*")
                for mr in repo_groups[repo]:
                    author = mr.get("author", {}).get("username", "?")
                    line = f"- [ ] [!{mr['iid']}]({mr['web_url']}): {mr['title']} — by @{author}"
                    days = self._get_staleness(mr)
                    if days >= 3:
                        line += f" | idle {days}d"
                    lines.append(line)
            lines.append("")

        # --- Changes / Recently Merged (reference, at bottom) ---
        changelog_lines = []

        if self.recently_merged:
            changelog_lines.append("## Recently Merged")
            for mr in self.recently_merged:
                merged_at = mr.get("merged_at", "")[:10]
                changelog_lines.append(
                    f"- [!{mr['iid']}]({mr['web_url']}): {mr['title']} ({merged_at})"
                )
            changelog_lines.append("")

        if diff and diff["days_back"] <= 2:
            has_changes = (
                diff["new_mrs"] or diff["merged_mrs"] or diff["gone_mr_urls"]
                or diff["new_issues"] or diff["resolved_issues"]
            )
            if has_changes:
                label = "Yesterday" if diff["days_back"] == 1 else f"{diff['prev_date'].strftime('%b %d')}"
                changelog_lines.append(f"## Changes Since {label}")
                if diff["merged_mrs"]:
                    for mr in diff["merged_mrs"]:
                        changelog_lines.append(
                            f"- Merged: [!{mr['iid']}]({mr['web_url']}): {mr['title']}"
                        )
                if diff["gone_mr_urls"]:
                    changelog_lines.append(f"- {len(diff['gone_mr_urls'])} MR(s) closed/removed")
                if diff["new_mrs"]:
                    for mr in diff["new_mrs"]:
                        changelog_lines.append(
                            f"- New: [!{mr['iid']}]({mr['web_url']}): {mr['title']}"
                        )
                if diff["resolved_issues"]:
                    changelog_lines.append(f"- {len(diff['resolved_issues'])} issue(s) resolved")
                if diff["new_issues"]:
                    for issue in diff["new_issues"]:
                        changelog_lines.append(
                            f"- New issue: [#{issue['iid']}]({issue['web_url']}): {issue['title']}"
                        )
                changelog_lines.append("")

        lines.extend(changelog_lines)

        lines.append("## Notes")
        lines.append("- ")

        return "\n".join(lines)

    def _format_issue_line(self, issue):
        """Format a single issue line"""
        labels = issue.get("labels", [])
        priority_labels = [
            l
            for l in labels
            if l.lower() in ["high", "critical", "urgent", "bug"]
        ]
        label_text = (
            f" [{', '.join(priority_labels)}]" if priority_labels else ""
        )
        return f"- [ ] [#{issue['iid']}]({issue['web_url']}): {issue['title']}{label_text}"

    def save_daily_note(self, date=None, keep=False):
        """Generate and save daily note, preserving existing notes"""
        if date is None:
            date = datetime.now()

        filepath = self.get_daily_note_path(date)
        print(f"\nSaving to: {filepath}", file=sys.stderr)

        # Read existing notes
        existing_notes = self.read_existing_note(filepath)
        if existing_notes:
            print("Preserving existing notes section", file=sys.stderr)

        # Generate new content
        new_content = self.generate_daily_note(date)

        # Merge with existing notes
        final_content = self.merge_with_existing(new_content, existing_notes)

        # Write to file
        with open(filepath, "w") as f:
            f.write(final_content)

        print(f"✓ Daily note saved to {filepath}", file=sys.stderr)

        # Save a snapshot on first run of the day
        if keep:
            snapshot_path = self.get_snapshot_path(date)
            if not snapshot_path.exists():
                with open(snapshot_path, "w") as f:
                    f.write(final_content)
                print(f"✓ Snapshot saved to {snapshot_path}", file=sys.stderr)
            else:
                print(f"  Snapshot already exists, skipping", file=sys.stderr)

        return filepath


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate Obsidian daily notes from GitLab data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="First run: use --init to generate a .daily-note.json config file.",
    )
    parser.add_argument(
        "--init",
        action="store_true",
        help="Generate a starter config file and exit",
    )
    parser.add_argument(
        "--config",
        default=None,
        help=f"Path to config file (default: {CONFIG_FILENAME})",
    )
    parser.add_argument(
        "--team",
        action="store_true",
        help="Include team MRs assigned to you for review (requires gitlab_group in config)",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Don't auto-dismiss stale todos for merged/closed MRs",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print to stdout instead of saving to file",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Save a snapshot of the first run of the day",
    )
    args = parser.parse_args()

    if args.init:
        generate_config(args.config)
        sys.exit(0)

    config = load_config(args.config)
    sync = GitLabSync(config=config, auto_cleanup=not args.no_cleanup)
    sync.include_team = args.team

    if args.stdout:
        note = sync.generate_daily_note()
        print(note)
    else:
        filepath = sync.save_daily_note(keep=args.keep)
