import { test } from "node:test";
import assert from "node:assert/strict";
import "./logic.js";

const { categorize, classifyThreads, shortRef } = globalThis.gitlabTasks;

const note = (user, extra = {}) => ({ author: { username: user }, resolvable: true, resolved: false, ...extra });
const ME = "me";

test("classifyThreads follows the daily note rules", () => {
  const c = classifyThreads(
    [
      { notes: [note("rev")] },                                    // pending
      { notes: [note("rev"), note(ME)] },                          // answered
      { notes: [note(ME), note("rev")] },                          // to close
      { notes: [note("rev", { resolved: true })] },                // resolved
      { notes: [note("rev", { resolvable: false })] },             // unanswered comment
      { notes: [note("bot", { system: true }), note("rev2")] },    // system note ignored, pending
      { notes: [note("rev"), note(ME), { system: true, author: null }] }, // trailing authorless note ignored
    ],
    ME,
  );
  assert.deepEqual(c, { pending: 2, answered: 2, to_close: 1, resolved: 1, comments: 1, pending_authors: ["rev", "rev2"] });
});

test("categorize sorts MRs and issues into the sections", () => {
  const now = Date.parse("2026-10-20T12:00:00Z");
  const mr = (iid, extra = {}) => ({
    iid, title: `mr ${iid}`, web_url: `https://gitlab.com/g/p/-/merge_requests/${iid}`,
    references: { full: `g/p!${iid}` }, author: { username: ME }, draft: false, has_conflicts: false,
    detailed_merge_status: "not_approved", created_at: "2026-10-01T00:00:00Z", updated_at: "2026-10-19T00:00:00Z",
    ...extra,
  });
  const mrs = [
    mr(1),                                              // waiting
    mr(2, { draft: true }),                             // in progress
    mr(3, { updated_at: "2026-10-01T00:00:00Z" }),      // ping (19d idle)
    mr(4, { has_conflicts: true }),                     // act now
    mr(5),                                              // act now (pending thread) + to close
    mr(6),                                              // approved
    mr(7, { title: "VPN relay" }),                      // on hold
    mr(8),                                              // act now (build failed)
  ];
  const reviewing = [mr(9, { author: { username: "other" } }), mr(10, { author: { username: "other" } })];
  const url = (iid) => `https://gitlab.com/g/p/-/merge_requests/${iid}`;
  const issue = (iid, extra = {}) => ({ iid, title: `issue ${iid}`, web_url: `https://gitlab.com/g/p/-/issues/${iid}`, references: { full: `g/p#${iid}` }, labels: [], ...extra });
  const cats = categorize(
    {
      me: ME,
      mrs,
      reviewing,
      issues: [issue(1), issue(2), issue(3, { labels: ["Done"] })],
      issueMrs: { [issue(2).web_url]: [{ web_url: url(1) }, { web_url: "https://gitlab.com/g/p/-/merge_requests/99" }] },
      threads: { [url(5)]: { pending: 1, to_close: 1, pending_authors: ["rev"] } },
      approvals: { [url(6)]: ["rev"], [url(10)]: [ME] },
      failedUrls: new Set([url(8)]),
      merged: [mr(11, { merged_at: "2026-10-18T00:00:00Z" })],
    },
    { on_hold_patterns: ["vpn"], stale_days: 7 },
    now,
  );
  const iids = (list) => list.map((x) => x.iid);
  assert.deepEqual(iids(cats.act_now), [4, 5, 8]);
  assert.deepEqual(iids(cats.close), [5]);
  assert.deepEqual(iids(cats.approved), [6]);
  assert.deepEqual(iids(cats.in_progress), [2]);
  assert.deepEqual(iids(cats.ping), [3]);
  assert.deepEqual(iids(cats.waiting), [1]);
  assert.deepEqual(iids(cats.on_hold), [7]);
  assert.deepEqual(iids(cats.review_requested), [9]);
  assert.deepEqual(iids(cats.todo_issues), [1]);
  assert.deepEqual(iids(cats.issues_with_mrs), [2]);
  assert.deepEqual(iids(cats.issues_with_mrs[0].mrs), [1]);
  assert.deepEqual(mrs[2].tags, ["idle 19d"]);
  assert.deepEqual(mrs[4].tags, ["idle 1d", "1 pending (@rev)", "1 to close"]);
  assert.deepEqual(mrs[7].tags, ["FAILED", "idle 1d"]);
  assert.deepEqual(reviewing[0].tags, ["by @other", "idle 1d"]);
  assert.deepEqual(cats.merged[0].tags, ["merged 2d ago"]);
  assert.equal(shortRef(mrs[0]), "p!1");
  assert.equal(mrs[3].section, "act_now");
  assert.equal(mrs[4].section, "act_now");
  assert.equal(cats.merged[0].section, "merged");
  assert.deepEqual(cats.issues_with_mrs[0].tags, ["p!1"]);
  assert.deepEqual(cats.todo_issues[0].tags, ["no MR yet"]);
});
