// Runs on gitlab.com/dashboard/*: groups GitLab's own list into blocks and adds a chip on each row.
(async () => {
  const { categorize, classifyThreads, shortRef } = globalThis.gitlabTasks;

  const CONFIG = { on_hold_patterns: [], stale_days: 7, done_days: 7, cache_minutes: 5, parallel: 4 };
  const SECTIONS = [
    ["act_now", "Act now", "danger"],
    ["review_requested", "Review requested", "warn"],
    ["close", "Threads to close", "warn"],
    ["todo_issues", "To do: issues without an MR", "todo"],
    ["ping", "Ping reviewer", "warn"],
    ["in_progress", "In progress", ""],
    ["waiting", "Waiting on others", ""],
    ["issues_with_mrs", "Issues with MRs", ""],
    ["on_hold", "On hold", ""],
    ["approved", "Approved, ready to merge", "ok"],
    ["merged", `Merged, last ${CONFIG.done_days} days`, "ok"],
  ];
  const TITLE = Object.fromEntries(SECTIONS.map(([k, title]) => [k, title]));
  const KIND = Object.fromEntries(SECTIONS.map(([k, , kind]) => [k, kind]));
  // GitLab's own badge classes, so chips follow its theme.
  const BADGE = { danger: "badge-danger", warn: "badge-warning", ok: "badge-success", todo: "badge-info", "": "badge-muted" };
  const CACHE_KEY = "gitlab-tasks";

  // ---- data ----
  let requests = 0;

  // A few requests at a time, so GitLab's own page requests are not starved or rate limited.
  let active = 0;
  const waiting = [];
  const acquire = () => new Promise((go) => (active < CONFIG.parallel ? (active++, go()) : waiting.push(go)));
  const release = () => {
    const next = waiting.shift();
    if (next) next();
    else active--;
  };
  const sleep = (ms) => new Promise((go) => setTimeout(go, ms));

  async function fetchJson(url, path) {
    for (let attempt = 0; ; attempt++) {
      const r = await fetch(url, { credentials: "include" });
      status(`${++requests} requests…`);
      if (r.status === 429 && attempt < 2) {
        await sleep(1000 * (Number(r.headers.get("retry-after")) || 2));
        continue;
      }
      if (!r.ok) throw new Error(`${r.status} on ${path}`);
      return [r, await r.json()];
    }
  }

  // GET with the page's gitlab.com session. Follows x-next-page for list endpoints.
  async function get(path, params = {}) {
    const url = new URL("/api/v4/" + path, location.origin);
    for (const [k, v] of Object.entries({ per_page: 100, ...params })) url.searchParams.set(k, v);
    await acquire();
    try {
      const out = [];
      for (let page = 1; ; page++) {
        url.searchParams.set("page", page);
        const [r, body] = await fetchJson(url, path);
        if (!Array.isArray(body)) return body;
        out.push(...body);
        if (!r.headers.get("x-next-page")) return out;
      }
    } finally {
      release();
    }
  }

  const byUrl = (...lists) => [...new Map(lists.flat().map((x) => [x.web_url, x])).values()];

  async function load() {
    requests = 0;
    const me = (await get("user")).username;
    const since = new Date(Date.now() - CONFIG.done_days * 86400000).toISOString();
    const [assigned, created, reviewing, issues, todos, mergedRaw] = await Promise.all([
      get("merge_requests", { scope: "assigned_to_me", state: "opened" }),
      get("merge_requests", { scope: "created_by_me", state: "opened" }),
      get("merge_requests", { scope: "all", state: "opened", reviewer_username: me }),
      get("issues", { scope: "assigned_to_me", state: "opened" }),
      get("todos", { action: "build_failed", state: "pending" }),
      get("merge_requests", { scope: "created_by_me", state: "merged", updated_after: since }),
    ]);
    const mrs = byUrl(assigned, created);
    const threads = {};
    const approvals = {};
    await Promise.all(
      byUrl(mrs, reviewing).map(async (mr) => {
        const base = `projects/${mr.project_id}/merge_requests/${mr.iid}/`;
        const [disc, appr] = await Promise.all([get(base + "discussions"), get(base + "approvals")]);
        threads[mr.web_url] = classifyThreads(disc, me);
        approvals[mr.web_url] = appr.approved_by.map((a) => a.user.username);
      }),
    );
    const issueMrs = {};
    await Promise.all(
      issues.map(async (i) => {
        issueMrs[i.web_url] = await get(`projects/${i.project_id}/issues/${i.iid}/related_merge_requests`);
      }),
    );
    const failedUrls = new Set(todos.map((t) => t.target?.web_url).filter(Boolean));
    const merged = mergedRaw.filter((mr) => mr.merged_at >= since);
    const cats = categorize({ me, mrs, reviewing, issues, issueMrs, threads, approvals, failedUrls, merged }, CONFIG);
    return { me, cats, at: Date.now() };
  }

  // Same-origin localStorage: one fetch per cache window across dashboard tabs.
  function cached() {
    try {
      const data = JSON.parse(localStorage.getItem(CACHE_KEY));
      return data && Date.now() - data.at < CONFIG.cache_minutes * 60000 ? data : null;
    } catch {
      return null;
    }
  }

  // ---- status pill (click = refetch) ----
  const el = (tag, cls, text) => {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  };
  const pill = el("button", "gt-pill", "Tasks");
  document.body.append(pill);
  const status = (text, err = false) => {
    pill.textContent = `Tasks: ${text}`;
    pill.classList.toggle("gt-err", err);
  };

  // ---- chips on GitLab's own list rows ----
  const key = (url) => url.split(/[?#]/)[0].replace("/-/work_items/", "/-/issues/");
  let items = new Map();

  function annotate() {
    if (!items.size) return;
    const seen = new Set();
    const sel = 'a[href*="/-/merge_requests/"], a[href*="/-/issues/"], a[href*="/-/work_items/"]';
    for (const a of document.querySelectorAll(sel)) {
      const k = key(a.href);
      const x = items.get(k);
      if (!x || seen.has(k)) continue;
      seen.add(k);
      // New MR dashboard rows are div cells; the old issuable list is li.
      const row = a.closest('[data-testid="merge-request"], li, tr') || a.parentElement;
      const chip = el("span", `gt-chip gl-badge badge badge-pill ${BADGE[KIND[x.section]]}`, TITLE[x.section]);
      if (x.tags?.length) chip.append(el("small", "", " " + x.tags.join(" | ")));
      const old = row.querySelector(".gt-chip");
      if (old?.className === chip.className && old.textContent === chip.textContent) continue;
      old?.remove();
      // Next to GitLab's comment count when the row has one, else after the link.
      const meta = row.querySelector('li[data-testid="issuable-comments"], [role="cell"] > div:has(svg[data-testid="comments-icon"])');
      (meta || a).after(chip);
    }
  }

  // ---- group GitLab's own lists into blocks ----
  // Rows stay GitLab's (Vue keeps its handlers); only an inline CSS order and header rows are added.
  // Works on the old ul.issuable-list and on the MR dashboard's grid tables (div[role="table"]).
  function rowSection(row) {
    const a = [...row.querySelectorAll("a[href]")].find((a) => items.has(key(a.href)));
    if (!a) return -1;
    const i = SECTIONS.findIndex(([k]) => k === items.get(key(a.href)).section);
    return i < 0 ? SECTIONS.length : i;
  }

  function groupList(list) {
    const counts = new Map();
    const sig = [...list.children]
      .filter((row) => !row.classList.contains("gt-header"))
      .map((row) => {
        if (row.querySelector('[role="columnheader"]')) {
          row.style.order = -1;
          return "h";
        }
        const i = rowSection(row);
        if (i < 0) return "";
        row.style.order = i;
        counts.set(i, (counts.get(i) || 0) + 1);
        return i;
      })
      .join(",");
    if (sig === list.dataset.gtSig && list.querySelector(".gt-header")) return;
    list.dataset.gtSig = sig;
    list.querySelectorAll(".gt-header").forEach((h) => h.remove());
    for (const [i, n] of counts) {
      const h = el(list.tagName === "UL" ? "li" : "div", "gt-header gl-col-span-full", `${SECTIONS[i]?.[1] || "Other"} (${n})`);
      h.style.order = i;
      list.prepend(h);
    }
  }

  function group() {
    if (!items.size) return;
    document.querySelectorAll('ul.issuable-list, div[role="table"]').forEach(groupList);
  }

  let scheduled = false;
  new MutationObserver(() => {
    if (scheduled) return;
    scheduled = true;
    setTimeout(() => {
      scheduled = false;
      group();
      annotate();
    }, 200);
  }).observe(document.body, { childList: true, subtree: true });

  function show(data) {
    status(`fetched ${new Date(data.at).toLocaleTimeString()}`);
    const all = SECTIONS.flatMap(([k]) => data.cats[k]);
    items = new Map(all.map((x) => [key(x.web_url), x]));
    group();
    annotate();
    console.info(`[gitlab-tasks] ${items.size} items, ${document.querySelectorAll(".gt-chip").length} rows matched on this page`);
  }

  // GitLab's own list and counts queries first; ours start once its rows are on screen (10 s cap).
  async function pageListRendered() {
    status("waiting for the page…");
    for (let i = 0; i < 40 && !document.querySelector('ul.issuable-list li, [data-testid="merge-request"]'); i++) await sleep(250);
    await sleep(1000);
  }

  async function refresh(force) {
    const hit = !force && cached();
    if (hit) return show(hit);
    pill.disabled = true;
    try {
      if (!force) await pageListRendered();
      const data = await load();
      localStorage.setItem(CACHE_KEY, JSON.stringify(data));
      show(data);
    } catch (e) {
      status(e.message.startsWith("401") ? "not logged in to gitlab.com" : `failed, ${e.message}`, true);
    } finally {
      pill.disabled = false;
    }
  }

  pill.onclick = () => refresh(true);
  refresh(false);
})();
