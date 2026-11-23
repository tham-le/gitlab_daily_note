#!/usr/bin/env python3
"""Checks for the glab call layer and thread classification. Run: python3 test_gitlab_daily.py"""
import subprocess
import types
import unittest
from unittest import mock

import gitlab_daily as gd


def _sync():
    sync = gd.GitLabSync(config=dict(gd.DEFAULT_CONFIG))
    sync.username = "me"
    return sync


def _fail(stderr):
    def fake_run(cmd, **kw):
        raise subprocess.CalledProcessError(1, cmd, output="", stderr=stderr)
    return fake_run


class RunCommand(unittest.TestCase):
    def test_429_retried_with_backoff(self):
        sync = _sync()
        calls = []

        def fake_run(cmd, **kw):
            calls.append(cmd)
            if len(calls) < 3:
                raise subprocess.CalledProcessError(1, cmd, output="", stderr="glab: HTTP 429\n")
            return types.SimpleNamespace(stdout="[]\n", stderr="")

        with mock.patch.object(gd.subprocess, "run", fake_run), mock.patch.object(gd.time, "sleep") as sleep:
            self.assertEqual(sync.run_command(["glab", "api", "x"]), "[]")
        self.assertEqual(len(calls), 3)
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [5, 10])
        self.assertEqual(sync.failed_calls, 0)

    def test_404_not_retried_not_counted(self):
        sync = _sync()
        with mock.patch.object(gd.subprocess, "run", _fail("glab: 404 Not found (HTTP 404)\n")), \
                mock.patch.object(gd.time, "sleep") as sleep:
            self.assertIsNone(sync.run_command(["glab", "api", "x"]))
        sleep.assert_not_called()
        self.assertEqual(sync.failed_calls, 0)

    def test_exhausted_429_counts_as_lost(self):
        sync = _sync()
        with mock.patch.object(gd.subprocess, "run", _fail("glab: HTTP 429\n")), \
                mock.patch.object(gd.time, "sleep"):
            self.assertIsNone(sync.run_command(["glab", "api", "x"]))
        self.assertEqual(sync.failed_calls, 1)

    def test_throttle_waits_when_window_full(self):
        sync = _sync()
        clock = [1000.0]
        with mock.patch.object(gd.time, "monotonic", lambda: clock[0]), \
                mock.patch.object(gd.time, "sleep") as sleep:
            for _ in range(gd.RATE_LIMIT_PER_MINUTE):
                sync._throttle()
                clock[0] += 0.1
            sleep.assert_not_called()
            sync._throttle()
        self.assertAlmostEqual(sleep.call_args.args[0], 60 - 0.1 * gd.RATE_LIMIT_PER_MINUTE)


class ClassifyThreads(unittest.TestCase):
    @staticmethod
    def thread(*authors, resolvable=True, resolved=False):
        return {"notes": [
            {"author": {"username": a}, "resolvable": resolvable, "resolved": resolved, "system": False}
            for a in authors
        ]}

    def test_counts(self):
        counts = _sync()._classify_threads([
            self.thread("bob"),                    # bob waits on me
            self.thread("bob", "me"),              # answered
            self.thread("me", "bob"),              # my thread, bob replied, mine to close
            self.thread("bob", resolved=True),
            self.thread("bob", resolvable=False),  # plain comment
            self.thread("me", resolvable=False),   # my own comment, nothing to do
            {"notes": [{"system": True, "author": {"username": "bot"}, "resolvable": False}]},
        ])
        self.assertEqual(
            {k: counts[k] for k in gd.GitLabSync._THREAD_COUNTS},
            {"pending": 1, "answered": 1, "to_close": 1, "resolved": 1, "comments": 1},
        )
        self.assertEqual(counts["pending_authors"], {"bob"})

    def test_open_threads_point_at_the_last_note(self):
        t = {"notes": [
            {"id": 1, "author": {"username": "bob"}, "resolvable": True, "resolved": False, "system": False,
             "body": "first", "position": {"new_path": "src/a.rs"}},
            {"id": 2, "author": {"username": "eve"}, "resolvable": True, "resolved": False, "system": False,
             "body": "```suggestion:-0+0\nx\n```"},
        ]}
        counts = _sync()._classify_threads([t])
        self.assertEqual(counts["open_threads"], [
            {"kind": "pending", "author": "eve", "note_id": 2, "path": "src/a.rs", "body": "code suggestion"},
        ])


class Reviews(unittest.TestCase):
    def test_only_a_review_still_owed_is_listed(self):
        sync = _sync()
        mk = lambda i: {"iid": i, "title": "t", "web_url": f"https://x/-/merge_requests/{i}", "reviewers": ["me"]}
        sync._all_group_mrs = [mk(1), mk(2), mk(3)]
        sync.review_state = {mk(2)["web_url"]: "reviewed", mk(3)["web_url"]: "unreviewed"}
        self.assertEqual([m["iid"] for m in sync.categorize_mrs({})["review_requested"]], [1, 3])


class Buckets(unittest.TestCase):
    @staticmethod
    def mr(iid, days_idle, **kw):
        at = (gd.datetime.now(gd.timezone.utc) - gd.timedelta(days=days_idle)).isoformat()
        return {"iid": iid, "title": f"t{iid}", "web_url": f"https://x/-/merge_requests/{iid}",
                "updated_at": at, "created_at": at, **kw}

    def test_old_failure_and_old_mr_leave_the_top(self):
        sync = _sync()
        failed = {"status": "failed"}
        sync.mrs = [
            self.mr(1, 1, head_pipeline=failed),
            self.mr(2, 40, head_pipeline=failed),
            self.mr(3, 40, milestone={"title": "1.0", "due_date": None}),
            self.mr(4, 40, approved_by=["bob"]),
            self.mr(5, 2, draft=True),
        ]
        cats = sync.categorize_mrs({})
        ids = lambda key: [m["iid"] for m in cats[key]]
        self.assertEqual(ids("failed"), [1])
        self.assertEqual(ids("backlog"), [2])
        self.assertEqual(ids("waiting"), [3])
        self.assertEqual(ids("approved"), [4])
        self.assertEqual(ids("in_progress"), [5])

    def test_assigned_mr_in_the_group_keeps_its_threads_and_shows_once(self):
        sync = _sync()
        mr = self.mr(1, 1, user_notes_count=3)
        sync.mrs = [mr]
        counts = {"pending": 1, "answered": 0, "to_close": 1, "resolved": 0, "comments": 0,
                  "pending_authors": {"bob"}, "open_threads": []}
        sync.mr_discussions = {mr["web_url"]: dict(counts)}
        sync._all_group_mrs = [dict(mr)]
        with mock.patch.object(sync, "_threads_for", return_value=dict(counts, pending=0)), \
             mock.patch.object(sync, "_save_store"), mock.patch.object(sync, "run_command", return_value="[]"):
            sync.fetch_team_discussions()
        cats = sync.categorize_mrs({})
        self.assertEqual([m["iid"] for m in cats["act_now"]], [1])
        self.assertEqual([m["iid"] for m in cats["close_mrs"]], [1])


class Notes(unittest.TestCase):
    def test_backslashes_in_notes_are_kept(self):
        notes = r"path C:\temp\x, regex \d+"
        self.assertEqual(_sync().merge_with_existing("x\n## Notes\n- ", notes), f"x\n## Notes\n{notes}")


class IssueRefs(unittest.TestCase):
    def test_refs_resolve_to_the_right_project(self):
        text = "Fixes other-repo#12, see g/sub/p2#3 and #7. Color #ff0000, mr/5#note_1"
        paths = [gd._issue_ref_path("g/sub/p", m.group(1)) + "#" + m.group(2) for m in gd._ISSUE_REF_RE.finditer(text)]
        self.assertEqual(paths, ["g/sub/other-repo#12", "g/sub/p2#3", "g/sub/p#7"])


class Diff(unittest.TestCase):
    def test_second_run_of_the_day_still_compares_to_yesterday(self):
        import tempfile
        from datetime import datetime
        sync = _sync()
        sync.base_dir = gd.Path(tempfile.mkdtemp())
        mr = lambda iid: {"iid": iid, "title": "t", "web_url": f"u/{iid}", "created_at": ""}
        sync.mrs = [mr(1)]
        sync._save_state(datetime(2026, 1, 1, 9))
        sync.mrs = [mr(1), mr(2)]
        for hour in (9, 17):
            today = datetime(2026, 1, 2, hour)
            diff = sync.compute_diff(today)
            self.assertEqual(diff["days_back"], 1)
            self.assertEqual(diff["gone_mr_urls"], set())
            sync._save_state(today)


if __name__ == "__main__":
    unittest.main()
