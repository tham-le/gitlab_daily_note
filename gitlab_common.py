"""
gitlab_common.py - Shared utilities for GitLab daily note scripts.

Provides: config loading, shell/GraphQL helpers, caching, and formatting utils.
"""

import json
import re
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

CONFIG_FILENAME = ".daily-note.json"
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


# ---------------------------------------------------------------------------
# Shell / API helpers
# ---------------------------------------------------------------------------

def run_command(cmd):
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error: {' '.join(cmd)}", file=sys.stderr)
        print(f"  {e.stderr}", file=sys.stderr)
        return None


def run_graphql(query):
    cmd = ["glab", "api", "graphql", "-f", f"query={query}"]
    output = run_command(cmd)
    if not output:
        return None
    data = json.loads(output)
    if "errors" in data:
        for err in data["errors"]:
            print(f"GraphQL error: {err.get('message', err)}", file=sys.stderr)
        return None
    return data.get("data")


def parse_json_list(output):
    if not output:
        return []
    data = json.loads(output)
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


def fetch_paginated(url_template, since, until):
    """Fetch all pages for a given REST API endpoint."""
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


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def gid_to_int(gid):
    if isinstance(gid, int):
        return gid
    if isinstance(gid, str) and "/" in gid:
        return int(gid.rsplit("/", 1)[-1])
    return int(gid)


def get_repo_short_name(item):
    """Extract short repo name from references.full or web_url."""
    ref = item.get("references", {}).get("full", "")
    parts = ref.split("/")
    if len(parts) >= 2:
        return re.sub(r"[!#]\d+$", "", parts[-1])
    url = item.get("web_url", "")
    match = re.search(r"/([^/]+)/-/(?:merge_requests|issues|work_items)/", url)
    if match:
        return match.group(1)
    return "unknown"


def group_by_repo(items):
    groups = defaultdict(list)
    for item in items:
        groups[get_repo_short_name(item)].append(item)
    return dict(sorted(groups.items()))


def format_mr_link(mr):
    return f"- [!{mr['iid']}]({mr['web_url']}): {mr['title']}"


def format_issue_link(issue):
    return f"- [#{issue['iid']}]({issue['web_url']}): {issue['title']}"


def render_grouped(items, formatter):
    lines = []
    groups = group_by_repo(items)
    for repo, repo_items in groups.items():
        lines.append(f"**{repo}:**")
        for item in repo_items:
            lines.append(formatter(item))
    return lines


# ---------------------------------------------------------------------------
# Month arithmetic (avoids python-dateutil dependency)
# ---------------------------------------------------------------------------

def add_months(year, month, delta):
    """Add delta months to year/month. Returns (year, month)."""
    month += delta
    while month > 12:
        month -= 12
        year += 1
    while month < 1:
        month += 12
        year -= 1
    return year, month
