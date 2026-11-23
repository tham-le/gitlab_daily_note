# gitlab-daily-note

Auto-generate daily notes from your GitLab MRs, issues, and todos.

Generates an action-based daily note with sections like **Act Now**, **Approved**, **Threads to Close**, etc. — so you know what to work on first.

## Setup

Needs Python 3.9 or newer (standard library only, tested on 3.14) and the [`glab`](https://gitlab.com/gitlab-org/cli) CLI. Works with gitlab.com and self-hosted GitLab: the scripts talk to whatever host `glab` is logged in to.

1. Install `glab` and run `glab auth login` (add `--hostname gitlab.example.com` for a self-hosted instance)
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

Uses the GitLab **GraphQL API** for MRs, todos, issues and approvals, and REST for MR discussions and issue-MR links. REST calls are paced to gitlab.com's limit of 100 requests per minute per user and retried on HTTP 429. A run that lost calls to throttling is not cached, so the next run refetches.

**API caching**: Responses are cached for 5 minutes in `.daily-note-cache/`. Re-runs within that window are instant.

**Thread store**: the threads of each MR are kept in `.daily-note-cache/threads.json` for 30 days. An MR is refetched only when its `updatedAt` changed or it still has a thread that waits on you.

**Change tracking**: A JSON state file tracks MR/issue URLs between runs, so the "Changes Since Yesterday" section works without parsing previous markdown.

## Output formats

### Plain (`--format plain`, default)

Standard markdown with `##` headings. Works in any markdown viewer.

### Obsidian (`--format obsidian`)

Adds Obsidian-specific features:

- **Callouts** — `[!danger]` Act Now, `[!tip]` Approved, `[!note]` In Progress, `[!quote]` On Hold
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

A line is in the top blocks only when you can act on it today. The rest is folded or counted.

| Section | What it means |
|---|---|
| **Review Owed** | Team MRs where you are a reviewer and GitLab's reviewer state is still unreviewed (first ask, or the author asked again). An MR you already reviewed is not listed. |
| **Act Now** | Your MRs with threads that wait on you, one sub-line per thread with a link to the note; then builds that failed on an MR touched in the last `stale_days` |
| **Threads to Close** | Threads you opened where a colleague replied, with a link to each |
| **Approved** | Approved MRs. One that says `needs rebase` is one rebase from a merge |
| **To Do** | Issues with no open MR |
| **In Progress** | Your draft MRs that still move |
| **Issues** | Issues and their MRs that still move. Each MR has one full line |
| **Backlog** | Folded. MRs idle `backlog_days` or more with no milestone |
| **On Hold** | MRs/issues matching `on_hold_patterns` |
| **To Review** | Team MRs needing your review (`--team` flag), grouped by milestone and author |
| **Recently Merged** | MRs merged in the last 3 days |
| **Changes Since Yesterday** | New/merged/closed MRs and issues since last run |

Inside a block, MRs with a milestone come first, by due date.

## MR annotations

Each MR line can show: `milestone X` | `FAILED` | `running` | `idle Xd` | `conflicts` | `needs rebase` | `approved by @name` | `X pending (@author)` | `X to close` | `X answered`

## Config options

| Key | Description |
|---|---|
| `base_dir` | Directory for daily notes (relative to script) |
| `tags` | Frontmatter line prepended to each note |
| `gitlab_group` | GitLab group path for `--team` mode |
| `gitlab_groups` | Array of group paths (overrides `gitlab_group`) |
| `on_hold_patterns` | Strings to match for on-hold items |
| `on_hold_label` | Section heading for on-hold items |
| `stale_days` | A failed build on an MR idle this long is not shown in Act Now (default: 7) |
| `backlog_days` | Idle days before an MR with no milestone moves to Backlog (default: 14) |

## Other scripts

All use the same `glab` login.

```
python gitlab_monthly_resume.py --last          # MRs and issues of last month
python gitlab_recap.py @username --months 6     # A user's MRs, issues, reviews and comments
python gitlab_mr_milestones.py --md > out.md    # Your merged MRs grouped by milestone
```

## Tests

`python3 test_gitlab_daily.py` covers the glab retry and pacing, the thread classification, the review filter and the section buckets.

## Firefox extension

`firefox-ext/` puts the same sections on top of the gitlab.com dashboard pages (`/dashboard/merge_requests`, `/dashboard/work_items`, ...): GitLab's own list is regrouped into blocks (to do without MR, issues with MRs, on hold, ...; for MR lists: act now, ping reviewer, in progress, approved, ...) with a header row per block, and each row gets a chip with the section and the annotations (idle days, needs rebase, pending threads, approved by, linked MR). A small "Tasks" pill bottom right shows the fetch time or an error; click it to refetch. It reads the API with your browser session, caches for 5 minutes in the page's localStorage; Refresh forces a fetch. Load it from `about:debugging` > This Firefox > Load Temporary Add-on > `firefox-ext/manifest.json`. On-hold patterns, stale days and the merged window are the `CONFIG` constant at the top of `content.js`. It only runs on gitlab.com; for a self-hosted instance change the two URLs in `manifest.json`. Test: `cd firefox-ext && node --test`.

## License

MIT, see [LICENSE](LICENSE).
