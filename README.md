# gitlab-daily-note

Auto-generate daily notes from your GitLab MRs, issues, and todos.

Generates an action-based daily note with sections like **Act Now**, **Approved**, **Threads to Close**, **Ping Reviewer**, **Waiting on Others**, etc. — so you know what to work on first.

## Setup

1. Install and authenticate [`glab`](https://gitlab.com/gitlab-org/cli)
2. Run `python gitlab_daily.py --init` to generate a `.daily-note.json` config
3. Edit the config:
   ```json
   {
     "base_dir": "daily-note",
     "tags": "",
     "gitlab_group": "my-org/my-project",
     "on_hold_patterns": [],
     "on_hold_label": "On Hold",
     "stale_days": 7
   }
   ```
4. Run `python gitlab_daily.py`

## Usage

```
python gitlab_daily.py                          # Plain markdown
python gitlab_daily.py --format obsidian        # Obsidian callouts + frontmatter
python gitlab_daily.py --keep                   # Save a snapshot on first run of the day
python gitlab_daily.py --team                   # Include team MRs to review
python gitlab_daily.py --stdout                 # Print to stdout instead of saving
python gitlab_daily.py --no-cleanup             # Don't auto-dismiss stale todos
python gitlab_daily.py --config path            # Use a custom config file
```

## How it works

Uses GitLab **GraphQL API** to batch MR data (discussions, approvals, pipeline, rebase status) into a single call. Falls back to REST for issue-MR links (GitLab GraphQL limitation).

**API caching**: Responses are cached for 5 minutes in `.daily-note-cache/`. Re-runs within that window are instant.

**Change tracking**: A JSON state file tracks MR/issue URLs between runs, so the "Changes Since Yesterday" section works without parsing previous markdown.

## Output formats

### Plain (`--format plain`, default)

Standard markdown with `##` headings. Works in any markdown viewer.

### Obsidian (`--format obsidian`)

Adds Obsidian-specific features:

- **Callouts** — `[!danger]` Act Now, `[!tip]` Approved, `[!warning]` Ping Reviewer, `[!note]` In Progress, `[!quote]` On Hold
- **Dataview frontmatter** — YAML fields for querying across notes
- **Progress bar** — ASCII bar showing ready-to-merge ratio
- **Summary table** — stats in a formatted table with avg MR age

Example Dataview query:
~~~
```dataview
TABLE build_failures, pending_threads, open_mrs, avg_mr_age
FROM "daily-note"
WHERE type = "daily-note"
SORT date DESC
LIMIT 7
```
~~~

## Output sections

| Section | What it means |
|---|---|
| **Act Now** | Pending review threads, build failures, merge conflicts |
| **Threads to Close** | Threads you opened where a colleague replied — resolve them |
| **To Do** | Issues with no open MR — work you haven't started yet |
| **Ping Reviewer** | MRs idle 7+ days waiting on review (sorted oldest first) |
| **In Progress** | Your draft MRs |
| **Approved** | Approved MRs ready to merge (sorted by most approvals) |
| **Issues** | All issues with their linked MRs (reference section) |
| **On Hold** | MRs/issues matching `on_hold_patterns` |
| **To Review** | Team MRs needing your review (`--team` flag), grouped by milestone and author. Drafts in a separate section. |
| **Recently Merged** | MRs merged in the last 3 days |
| **Changes Since Yesterday** | New/merged/closed MRs and issues since last run |

## MR annotations

Each MR line can show: `FAILED` | `running` | `idle Xd` | `needs rebase` | `approved by @name` | `X pending (@author)` | `X to close` | `X answered`

## Config options

| Key | Description |
|---|---|
| `base_dir` | Directory for daily notes (relative to script) |
| `tags` | Frontmatter line prepended to each note |
| `gitlab_group` | GitLab group path for `--team` mode |
| `gitlab_groups` | Array of group paths (overrides `gitlab_group`) |
| `on_hold_patterns` | Strings to match for on-hold items |
| `on_hold_label` | Section heading for on-hold items |
| `stale_days` | Days before flagging an MR for pinging (default: 7) |

## Legacy scripts

Previous versions are preserved in `old_scripts/` for reference.
