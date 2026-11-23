// Pure logic shared by the content script and the test. No fetch, no DOM.
// Classic script: exposes globalThis.gitlabTasks so content.js can use it.

const SKIP_LABELS = ["done", "blocked", "wontfix", "duplicate"];
// detailed_merge_status values that add nothing next to the other tags.
const QUIET_STATUS = new Set(["mergeable", "not_approved", "draft_status", "unchecked", "checking", "conflict"]);

// Same rules as gitlab_daily.py _process_discussions, on REST discussion objects.
function classifyThreads(discussions, me) {
  const c = { pending: 0, answered: 0, to_close: 0, resolved: 0, comments: 0, pending_authors: [] };
  for (const disc of discussions) {
    const notes = (disc.notes || []).filter((n) => !n.system && n.author?.username);
    if (!notes.length) continue;
    const first = notes[0].author.username;
    const last = notes[notes.length - 1].author.username;
    const resolvable = notes.filter((n) => n.resolvable);
    if (!resolvable.length) {
      if (last !== me) {
        c.comments++;
        c.pending_authors.push(last);
      }
      continue;
    }
    if (resolvable.every((n) => n.resolved)) {
      c.resolved++;
    } else if (last === me) {
      c.answered++;
    } else if (first === me) {
      c.to_close++;
    } else {
      c.pending++;
      c.pending_authors.push(last);
    }
  }
  c.pending_authors = [...new Set(c.pending_authors)];
  return c;
}

function shortRef(item) {
  const full = item.references?.full || "";
  return full.slice(full.lastIndexOf("/") + 1) || item.web_url;
}

const users = (list) => list.map((u) => "@" + u).join(", ");

function tagsFor(mr, t, approvedBy, failed, me) {
  const tags = [];
  if (mr.author?.username && mr.author.username !== me) tags.push("by @" + mr.author.username);
  if (failed) tags.push("FAILED");
  if (mr.has_conflicts) tags.push("conflicts");
  if (mr.idle > 0) tags.push(`idle ${mr.idle}d`);
  const status = mr.detailed_merge_status;
  if (status && !QUIET_STATUS.has(status)) tags.push(status.replace(/_/g, " "));
  if (approvedBy.length) tags.push("approved by " + users(approvedBy));
  if (t.pending) tags.push(`${t.pending} pending (${users(t.pending_authors)})`);
  if (t.comments) tags.push(`${t.comments} unanswered comment${t.comments > 1 ? "s" : ""}`);
  if (t.to_close) tags.push(`${t.to_close} to close`);
  if (t.answered) tags.push(`${t.answered} answered`);
  return tags;
}

// data: { me, mrs, reviewing, issues, issueMrs, threads, approvals, failedUrls, merged }
// cfg: { on_hold_patterns, stale_days }
function categorize(data, cfg, now = Date.now()) {
  const { me, threads, approvals, failedUrls } = data;
  const days = (iso) => Math.floor((now - Date.parse(iso)) / 86400000);
  const onHold = (x) => {
    const hay = `${x.web_url} ${x.title} ${x.references?.full || ""}`.toLowerCase();
    return cfg.on_hold_patterns.some((p) => hay.includes(p.toLowerCase()));
  };
  const out = {
    act_now: [], review_requested: [], close: [], todo_issues: [], ping: [], in_progress: [],
    waiting: [], issues_with_mrs: [], on_hold: [], approved: [], merged: data.merged,
  };
  const put = (key, x) => {
    x.section ??= key;
    out[key].push(x);
  };

  const decorate = (mr) => {
    const t = threads[mr.web_url] || {};
    const approvedBy = approvals[mr.web_url] || [];
    mr.idle = days(mr.updated_at);
    mr.tags = tagsFor(mr, t, approvedBy, failedUrls.has(mr.web_url), me);
    return { t, approvedBy };
  };

  for (const mr of data.mrs) {
    const { t, approvedBy } = decorate(mr);
    if (onHold(mr)) {
      put("on_hold", mr);
      continue;
    }
    if (t.pending || t.comments || mr.has_conflicts || failedUrls.has(mr.web_url)) put("act_now", mr);
    else if (approvedBy.length) put("approved", mr);
    else if (mr.draft) put("in_progress", mr);
    else if (mr.idle >= cfg.stale_days) put("ping", mr);
    else put("waiting", mr);
    if (t.to_close) put("close", mr);
  }

  for (const mr of data.reviewing) {
    if (mr.author?.username === me) continue;
    const { t, approvedBy } = decorate(mr);
    if (!approvedBy.includes(me)) put("review_requested", mr);
    if (t.to_close) put("close", mr);
  }

  const ownOpen = new Map(data.mrs.map((mr) => [mr.web_url, mr]));
  for (const issue of data.issues) {
    if ((issue.labels || []).some((l) => SKIP_LABELS.includes(l.toLowerCase()))) continue;
    issue.mrs = (data.issueMrs[issue.web_url] || []).map((m) => ownOpen.get(m.web_url)).filter(Boolean);
    issue.tags = issue.mrs.length ? issue.mrs.map(shortRef) : ["no MR yet"];
    if (onHold(issue)) put("on_hold", issue);
    else if (issue.mrs.length) put("issues_with_mrs", issue);
    else put("todo_issues", issue);
  }

  for (const mr of out.merged) {
    mr.section = "merged";
    mr.tags = [`merged ${days(mr.merged_at)}d ago`];
  }

  out.approved.sort((a, b) => approvals[b.web_url].length - approvals[a.web_url].length);
  out.ping.sort((a, b) => b.idle - a.idle);
  out.waiting.sort((a, b) => b.idle - a.idle);
  out.review_requested.sort((a, b) => a.created_at.localeCompare(b.created_at));
  out.merged.sort((a, b) => b.merged_at.localeCompare(a.merged_at));
  return out;
}

globalThis.gitlabTasks = { classifyThreads, shortRef, tagsFor, categorize };
