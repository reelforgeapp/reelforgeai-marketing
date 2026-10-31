// ReelForge Partner Intelligence dashboard (no build step).
// Creator content is untrusted: everything is rendered with textContent via h(), never innerHTML.
"use strict";

const API = "/rpi/api/v1";
const state = { selected: new Set(JSON.parse(sessionStorage.getItem("rpi.selected") || "[]")) };

// ---------------------------------------------------------------- helpers
function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else if (k === "class") el.className = v;
    else if (k === "style") el.style.cssText = v; // CSSOM is allowed under the page CSP; style attributes are not
    else el.setAttribute(k, v === true ? "" : v);
  }
  for (const c of children.flat()) {
    if (c === null || c === undefined || c === false) continue;
    el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}
const key = () => sessionStorage.getItem("rpi.key") || "";
async function api(method, path, body) {
  const res = await fetch(API + path, {
    method, headers: { "X-RPI-Key": key(), ...(body ? { "content-type": "application/json" } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let data; try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (!res.ok) throw new Error(`${res.status}: ${typeof data === "string" ? data : JSON.stringify(data.detail ?? data)}`);
  return data;
}
function flash(msg, error = false) {
  const f = document.getElementById("flash");
  f.replaceChildren(h("div", { class: "msg" + (error ? " error" : "") }, msg));
  setTimeout(() => f.replaceChildren(), error ? 9000 : 4000);
}
const money = v => {
  if (v === null || v === undefined) return "—";
  const n = Number(v);
  return (n < 0 ? "−$" : "$") + Math.abs(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
};
const num = v => v === null || v === undefined ? "—" : Number(v).toLocaleString();
const pct = v => v === null || v === undefined ? "—" : (Number(v) * 100).toFixed(1) + "%";
const date = v => v ? new Date(v).toLocaleDateString() : "—";
function tag(kind, provider) {
  if (provider === "fixture") return h("span", { class: "tag synthetic" }, "synthetic");
  const map = { observed: "observed", first_party_measured: "observed", third_party_modeled: "modeled", content_inferred: "inferred",
    manual_submitted: "assumption", assumption: "assumption", estimate: "modeled", actual: "observed", computed: "computed", unavailable: "unavailable" };
  const label = { third_party_modeled: "modeled estimate", content_inferred: "AI/content inferred", first_party_measured: "creator-authorized",
    manual_submitted: "creator-submitted (unverified)" }[kind] || kind;
  return h("span", { class: "tag " + (map[kind] || "assumption") }, label || "unknown");
}
const pill = v => h("span", { class: "pill " + (v || "") }, v || "—");
function table(cols, rows, onRow) {
  return h("table", {},
    h("thead", {}, h("tr", {}, cols.map(c => h("th", { class: c.num ? "num" : "" }, c.label)))),
    h("tbody", {}, rows.map(r => h("tr", { onclick: onRow ? () => onRow(r) : null, style: onRow ? "cursor:pointer" : null },
      cols.map(c => h("td", { class: c.num ? "num" : "" }, c.render ? c.render(r) : (r[c.key] ?? "—")))))));
}
const card = (k, v, extra) => h("div", { class: "card" }, h("div", { class: "k" }, k), h("div", { class: "v" }, v), extra || null);
const panel = (title, ...body) => h("div", { class: "panel" }, title ? h("h2", {}, title) : null, ...body);
function field(label, input) { return h("label", {}, label, input); }
function saveSelected() { sessionStorage.setItem("rpi.selected", JSON.stringify([...state.selected])); }
function setView(title, ...nodes) {
  document.getElementById("title").textContent = title;
  document.getElementById("view").replaceChildren(...nodes);
  for (const a of document.querySelectorAll("#nav a")) a.classList.toggle("active", location.hash.startsWith(a.getAttribute("href")));
}

// ---------------------------------------------------------------- views
const views = {
  async dashboard() {
    const d = await api("GET", "/dashboard/executive");
    const rc = d.research_costs;
    setView("Executive dashboard",
      h("div", { class: "grid" },
        card("Discovered creators", num(d.total_discovered)), card("Qualified prospects", num(d.qualified_prospects)),
        card("Active negotiations", num(d.active_negotiations)), card("Active partnerships", num(d.active_partnerships)),
        card("Partnership spend (month)", money(d.monthly_partnership_spend_usd)),
        card("New paying subscribers (month)", num(d.new_paying_subscribers_this_month)),
        card("Campaign contribution profit", money(d.campaign_contribution_profit_usd),
          d.profit_reports_complete === false ? h("div", { class: "notice" }, "Incomplete: some cost categories missing") : null),
        card("Research spend (month)", money(rc.month_to_date_spend_usd))),
      panel("Cost per stage (research spend only)", h("div", { class: "grid" },
        card("Per discovered creator", money(rc.cost_per_discovered_creator_usd)),
        card("Per qualified prospect", money(rc.cost_per_qualified_prospect_usd)),
        card("Per outreach-ready prospect", money(rc.cost_per_outreach_ready_prospect_usd)),
        card("Per secured partnership", money(rc.cost_per_secured_partnership_usd)),
        card("Per paying customer", money(rc.research_cost_per_paying_customer_usd)))),
      panel("Top partners (observed ledger)", d.top_partners.length ? table([
        { label: "Campaign", key: "campaign" }, { label: "Creator", key: "creator" },
        { label: "Paying", key: "paying_customers", num: true },
        { label: "Net revenue", render: r => money(r.net_revenue_usd), num: true },
        { label: "Profit", render: r => money(r.campaign_profit_usd), num: true },
        { label: "Complete", render: r => r.complete ? "yes" : h("span", { class: "muted" }, "no: " + r.missing.join("; ")) },
      ], d.top_partners) : h("p", { class: "muted" }, "No campaigns yet.")),
      panel("Action items", h("ul", {},
        h("li", {}, `${d.action_items.drafts_awaiting_review} outreach drafts awaiting review`),
        h("li", {}, `${d.action_items.identity_matches_to_review} identity matches to review`),
        h("li", {}, `${d.action_items.follow_ups_due} follow-ups due`))),
      panel("Alerts", d.alerts.length ? table([{ label: "Kind", key: "kind" }, { label: "Severity", key: "severity" },
        { label: "Message", key: "message" }, { label: "When", render: r => date(r.created_at) }], d.alerts)
        : h("p", { class: "muted" }, "No open alerts.")));
  },

  async discovery() {
    const strategies = ["niche", "competitor_sponsorship", "emerging", "audience_first", "lookalike"];
    const checks = strategies.map(s => h("label", { style: "flex-direction:row;gap:4px;align-items:center" },
      h("input", { type: "checkbox", value: s, checked: s === "niche" }), s));
    const preset = h("select", {}, ["english_based", "quality", "quality_broad", "none"].map(p => h("option", { value: p }, p)));
    const kw = h("input", { placeholder: "blank = preset hashtags and topics; words are matched as hashtags", size: 40 });
    const minS = h("input", { type: "number", value: 10000 }), maxS = h("input", { type: "number", value: 150000 });
    const countries = h("input", { value: "US,CA,GB,AU,NZ,IE", size: 18 });
    const max = h("input", { type: "number", value: 25 }), budget = h("input", { type: "number", step: "0.5", value: 10 });
    const aq = h("input", { placeholder: "audience description (SparkToro, opt-in)", size: 40 });
    const out = h("div");
    const body = () => ({
      strategies: checks.map(c => c.firstChild).filter(i => i.checked).map(i => i.value),
      max_candidates: Number(max.value), budget_usd: budget.value, audience_query: aq.value || null, preset: preset.value,
      criteria: { ...(kw.value ? { keywords: kw.value.split(",").map(s => s.trim()).filter(Boolean) } : {}),
        min_subscribers: Number(minS.value), max_subscribers: Number(maxS.value),
        audience_countries: countries.value.split(",").map(s => s.trim()).filter(Boolean) },
    });
    const estimate = async () => {
      const e = await api("POST", "/discovery/estimate", body());
      out.replaceChildren(panel("Estimated cost", h("p", {}, `Total ${e.total_credits} credits (${money(e.total_usd)}) via ${e.primary_provider || "no search provider configured"}`),
        table([{ label: "Strategy", key: "strategy" }, { label: "Provider", key: "provider" }, { label: "Credits", key: "credits", num: true },
          { label: "USD", render: r => money(r.usd), num: true }, { label: "Basis", key: "basis" }], e.lines),
        h("pre", {}, JSON.stringify(e.criteria, null, 2))));
    };
    const run = async () => {
      const b = body(); b.inline = b.max_candidates <= 200;
      const r = await api("POST", "/discovery/runs", b);
      flash(r.status === "queued" ? `Queued job ${r.job_id}` : `Run ${r.status}: ${JSON.stringify(r.progress?.summary || {})}`);
      views.discovery();
    };
    const runs = await api("GET", "/discovery/runs");
    setView("Creator discovery",
      panel("New search", h("div", { class: "row" }, ...checks), h("div", { class: "row" },
        field("Preset", preset), field("Keywords", kw), field("Min subscribers", minS), field("Max subscribers", maxS),
        field("Main audience countries", countries),
        field("Max candidates", max), field("Job budget (USD)", budget), field("Audience-first query", aq)),
        h("p", { class: "muted" }, "The preset sets hashtags and topics, English, audience location, median views, engagement, upload frequency and a contact email as search filters, so only creators passing them are pulled. Fields above override it. audience_first uses paid SparkToro credits and only runs when selected."),
        h("div", { class: "row" }, h("button", { class: "secondary", onclick: () => estimate().catch(e => flash(e.message, true)) }, "Estimate cost"),
          h("button", { onclick: () => run().catch(e => flash(e.message, true)) }, "Run discovery"))),
      out,
      panel("Search history", table([
        { label: "When", render: r => date(r.created_at) }, { label: "Strategies", render: r => r.strategies.join(", ") },
        { label: "Found", key: "total_found", num: true }, { label: "New", key: "new_creators", num: true },
        { label: "Duplicates", key: "duplicate_count", num: true }, { label: "Prelim. qualified", key: "preliminary_qualified", num: true },
        { label: "Cost", render: r => money(r.actual_cost_usd), num: true },
        { label: "Next-stage estimate", render: r => money(r.estimated_enrichment_cost_usd), num: true },
        { label: "Warnings", render: r => (r.warnings || []).join("; ") || "—" },
      ], runs, r => { location.hash = "#/run/" + r.id; })));
  },

  async run(id) {
    const d = await api("GET", "/discovery/runs/" + id);
    setView("Discovery run",
      panel("Summary", h("pre", {}, JSON.stringify({ niche_distribution: d.run.niche_distribution, strategy_breakdown: d.run.strategy_breakdown,
        warnings: d.run.warnings }, null, 2))),
      panel("Candidates", table([
        { label: "", render: r => h("input", { type: "checkbox", checked: state.selected.has(r.creator_id),
          onclick: e => { e.stopPropagation(); e.target.checked ? state.selected.add(r.creator_id) : state.selected.delete(r.creator_id); saveSelected(); } }) },
        { label: "Creator", key: "display_name" }, { label: "Strategy", key: "strategy" }, { label: "Source", key: "source_provider" },
        { label: "Subscribers", render: r => r.subscribers == null ? "—" : Number(r.subscribers).toLocaleString(), num: true },
        { label: "Score", render: r => r.preliminary_score == null ? "—" : Number(r.preliminary_score).toFixed(2), num: true },
        { label: "Duplicate", render: r => r.was_duplicate ? "yes" : "" },
        { label: "Why included", render: r => r.inclusion_reasons.join("; ") }, { label: "Missing", render: r => r.missing_fields.join(", ") || "—" },
      ], d.candidates, r => { location.hash = "#/creator/" + r.creator_id; })));
  },

  async creators() {
    const status = h("select", {}, h("option", { value: "" }, "any status"),
      ...["discovered", "preliminary_qualified", "deep_analysis", "recommended", "contact_identified", "outreach_approved", "contacted",
        "negotiating", "active_partnership", "rejected", "do_not_contact"].map(s => h("option", { value: s }, s)));
    const rec = h("select", {}, h("option", { value: "" }, "any recommendation"),
      ...["recommend", "investigate", "hold", "reject_niche_mismatch", "reject_suspect_engagement", "reject_audience_geography", "needs_data"].map(s => h("option", { value: s }, s)));
    const q = h("input", { placeholder: "name or handle" });
    const list = h("div");
    const load = async () => {
      const params = new URLSearchParams(Object.entries({ status: status.value, recommendation: rec.value, q: q.value }).filter(([, v]) => v));
      const rows = await api("GET", "/creators?" + params);
      list.replaceChildren(table([
        { label: "", render: r => h("input", { type: "checkbox", checked: state.selected.has(r.id),
          onclick: e => { e.stopPropagation(); e.target.checked ? state.selected.add(r.id) : state.selected.delete(r.id); saveSelected(); } }) },
        { label: "Creator", render: r => [r.display_name || r.handle, " ", r.snapshot_provider === "fixture" ? tag(null, "fixture") : null] },
        { label: "Status", key: "pipeline_status" }, { label: "Stage", key: "funnel_stage", num: true },
        { label: "Subscribers", render: r => num(r.subscribers), num: true },
        { label: "Niche", render: r => r.niche_relevance === null ? "—" : Number(r.niche_relevance).toFixed(2), num: true },
        { label: "Score", render: r => r.overall === null ? "—" : Number(r.overall).toFixed(2), num: true },
        { label: "Confidence", render: r => r.confidence === null ? "—" : Number(r.confidence).toFixed(2), num: true },
        { label: "Recommendation", render: r => pill(r.recommendation) }, { label: "Risk", render: r => pill(r.risk_level) },
      ], rows, r => { location.hash = "#/creator/" + r.id; }));
    };
    const research = stage => async () => {
      if (!state.selected.size) return flash("Select creators first", true);
      const r = await api("POST", "/research", { creator_ids: [...state.selected], stage, inline: state.selected.size <= 25 });
      flash(r.status === "queued" ? `Queued job ${r.job_id}` : `Research ${r.status}`); load();
    };
    setView("Creators", panel(null, h("div", { class: "row" }, status, rec, q,
      h("button", { class: "secondary", onclick: () => load().catch(e => flash(e.message, true)) }, "Filter"),
      h("button", { onclick: () => research("screen")().catch(e => flash(e.message, true)) }, "Screen selected (stage 2)"),
      h("button", { onclick: () => research("deep")().catch(e => flash(e.message, true)) }, "Deep analysis (stages 3-4)"),
      h("button", { class: "secondary", onclick: () => { location.hash = "#/compare"; } }, "Compare selected"))), list);
    await load();
  },

  async creator(id) {
    const [p, campaigns, deals] = await Promise.all([api("GET", "/creators/" + id),
      api("GET", "/campaigns").then(cs => cs.filter(x => x.creator_id === id)).catch(() => []),
      api("GET", "/deals?creator_id=" + id).catch(() => [])]);
    const c = p.creator, snap = p.snapshots[0] || {}, q = p.qualification, cls = p.classification;
    const campName = h("input", { value: `${c.display_name || "Creator"} partnership`, size: 30 });
    const refCode = h("input", { placeholder: "their Reditus ?ref= code", pattern: "[A-Za-z0-9_-]{2,60}" });
    const campDeal = h("select", {}, h("option", { value: "" }, "no deal yet"),
      ...deals.map(d => h("option", { value: d.id }, `${d.structure} · ${d.status}`)));
    const target = h("select", {}, ...["preliminary_qualified", "deep_analysis", "recommended", "contact_identified", "outreach_approved",
      "contacted", "replied", "negotiating", "agreement_pending", "active_partnership", "campaign_completed", "under_performance_review",
      "re_engagement_candidate", "rejected", "do_not_contact"].map(s => h("option", { value: s }, s)));
    const reason = h("input", { placeholder: "reason" });
    const note = h("textarea", { placeholder: "Note, sponsorship request (add fee in data), negotiation update…" });
    const noteKind = h("select", {}, ...["note", "sponsorship_request", "negotiation", "call", "manual_analytics"].map(s => h("option", { value: s }, s)));
    const fee = h("input", { type: "number", placeholder: "requested fee USD" });
    setView(c.display_name || "Creator",
      p.accounts.some(a => a.platform_account_id.startsWith("UCFIXTURE")) ? h("div", { class: "notice" }, "SYNTHETIC fixture creator (development data).") : null,
      h("div", { class: "grid" },
        card("Status", c.pipeline_status), card("Funnel stage", c.funnel_stage),
        card("Subscribers", num(snap.subscribers), snap.provider ? tag(snap.measurement, snap.provider) : null),
        card("Recommendation", q ? pill(q.recommendation) : "—"),
        card("Score / confidence", q ? `${(q.overall ?? 0).toFixed?.(2) ?? q.overall} / ${Number(q.confidence).toFixed(2)}` : "—")),
      p.partner_scout_history ? h("div", { class: "notice" }, p.partner_scout_history.summary +
        ". Drafts for this creator are written as a follow-up.") : null,
      p.external_conversation ? h("div", { class: "notice" }, p.external_conversation.summary +
        ". RPI will not cold-email them; continue the conversation from the inbox.") : null,
      q?.components?.commercial_fit?.flags?.length
        ? h("div", { class: "notice" }, q.components.commercial_fit.flags.map(f => f[0].toUpperCase() + f.slice(1)).join("; ")) : null,
      q?.components?.audience_fit?.geography?.english_speaking_share != null
        ? h("div", { class: "notice" }, `${pct(q.components.audience_fit.geography.english_speaking_share)} of the audience is in ` +
          `English-speaking countries (US, UK, CA, AU, NZ, IE); a recommendation needs ${pct(q.components.audience_fit.geography.required)}.`) : null,
      panel("Identity and channels", table([{ label: "Platform", key: "platform" }, { label: "Account id", key: "platform_account_id" },
        { label: "Handle", key: "handle" }, { label: "Link", render: r => r.url ? h("a", { href: r.url, target: "_blank", rel: "noopener noreferrer" }, "open") : "—" }], p.accounts),
        p.identity_matches.length ? h("p", { class: "muted" }, `${p.identity_matches.length} identity match record(s); review under Administration.`) : null),
      q ? panel("Qualification (deterministic, " + q.scoring_version + ")",
        table([{ label: "Component", render: r => r[0] }, { label: "Value", render: r => r[1].value === null ? "excluded / missing" : Number(r[1].value).toFixed(2), num: true },
          { label: "Weight", render: r => r[1].weight, num: true }, { label: "Basis", render: r => tag(r[1].basis, r[1].provider) },
          { label: "Detail", render: r => r[1].detail }], Object.entries(q.components || {})),
        q.missing_data?.length ? h("p", {}, "Missing: " + q.missing_data.join(", ")) : null,
        q.excluded_sources?.length ? h("div", { class: "notice" }, "Excluded by data policy: " + q.excluded_sources.join("; ")) : null) : null,
      q ? panel("Audience fit by ICP", table([{ label: "ICP", key: "name" }, { label: "Fit", render: r => r.fit === null ? "not evaluable" : r.fit.toFixed(2), num: true },
        { label: "Completeness", render: r => pct(r.completeness), num: true },
        { label: "Components", render: r => r.components.map(x => h("div", {}, tag(x.basis), " ", x.name, ": ", x.detail)) },
        { label: "Missing", render: r => r.missing.join(", ") || "—" }], q.audience_fit || [])) : null,
      panel("Audience estimates", p.audience.length ? table([{ label: "Dimension", key: "dimension" }, { label: "Bucket", key: "bucket" },
        { label: "Share", render: r => pct(r.share), num: true }, { label: "Basis", render: r => tag(r.measurement, r.provider) },
        { label: "Source", key: "provider" }], p.audience) : h("p", { class: "muted" }, "No audience data. Nothing has been estimated or filled in.")),
      cls ? panel("Content evidence (" + cls.method + (cls.model ? ", " + cls.model : "") + ")", tag("content_inferred"),
        h("p", {}, "Audience problem: ", cls.audience_problem || "—"),
        table([{ label: "Video", render: r => r.url ? h("a", { href: r.url, target: "_blank", rel: "noopener noreferrer" }, r.excerpt || r.ref) : (r.excerpt || r.ref) },
          { label: "Labels", render: r => (r.labels || []).join(", ") }, { label: "Note", render: r => r.note || "—" }], cls.evidence || []),
        (cls.uncertainty_reasons || []).length ? h("p", { class: "muted" }, "Uncertainty: " + cls.uncertainty_reasons.join("; ")) : null) : null,
      panel("Sponsorship and promotion evidence", p.sponsorships.length ? table([{ label: "Brand", key: "brand_name" },
        { label: "Type", render: r => pill(r.relationship_type) }, { label: "Source", key: "source" },
        { label: "Evidence", render: r => (r.evidence || []).map(e => h("div", {}, `${e.signal}: `, e.excerpt || "")) },
        { label: "Link", render: r => r.evidence_url ? h("a", { href: r.evidence_url, target: "_blank", rel: "noopener noreferrer" }, "video") : "—" }],
        p.sponsorships) : h("p", { class: "muted" }, "No evidence found. A brand mention alone is never treated as a paid sponsorship.")),
      panel("Risk screening (not a forensic determination)", p.risk_signals.length ? table([{ label: "Signal", key: "signal_type" },
        { label: "Severity", render: r => pill(r.severity) }, { label: "Detail", key: "detail" }], p.risk_signals)
        : h("p", { class: "muted" }, "No flags in available data.")),
      panel("Contacts", p.contacts.length ? table([{ label: "Type", key: "contact_type" }, { label: "Value", key: "value" },
        { label: "Source", key: "source" }, { label: "Verification", key: "verification_status" }], p.contacts)
        : h("p", { class: "muted" }, "No contact on file. Contacts are fetched only after qualification and never guessed.")),
      panel("Campaign tracking",
        campaigns.length ? table([{ label: "Campaign", key: "name" }, { label: "Referral code", key: "referral_code" },
          { label: "Window (days)", key: "attribution_window_days", num: true }, { label: "Status", key: "status" }], campaigns)
          : h("p", { class: "muted" }, "Not tracked yet. When they join the affiliate program, enter the code from their Reditus link " +
            "(reelforgeai.io/?ref=CODE). Signups through that link, including earlier ones, are then attributed to them."),
        h("div", { class: "row", style: "margin-top:10px" }, field("Name", campName), field("Reditus referral code", refCode),
          field("Deal", campDeal),
          h("button", { onclick: () => refCode.value.trim() ? api("POST", "/campaigns", { creator_id: id, name: campName.value,
            referral_code: refCode.value.trim(), deal_id: campDeal.value || null })
            .then(r => { flash(`Tracking ${r.referral_code}` + (r.earlier_signups_attributed ? `; ${r.earlier_signups_attributed} earlier signup(s) attributed` : "")); views.creator(id); })
            .catch(e => flash(e.message, true)) : flash("Enter their Reditus referral code", true) }, "Start tracking"))),
      panel("Actions", h("div", { class: "row" }, target, reason,
        h("button", { onclick: () => api("POST", `/crm/creators/${id}/transition`, { status: target.value, reason: reason.value || null })
          .then(() => { flash("Status updated"); views.creator(id); }).catch(e => flash(e.message, true)) }, "Move"),
        h("button", { class: "secondary", onclick: () => api("POST", `/outreach/creators/${id}/drafts`, {})
          .then(d => { flash(d.validation.ok ? "Draft created" : "Draft created with validation errors"); location.hash = "#/outreach"; })
          .catch(e => flash(e.message, true)) }, "Draft outreach"),
        h("button", { class: "secondary", onclick: () => { state.selected.add(id); saveSelected(); flash("Added to comparison"); } }, "Add to compare")),
        h("div", { class: "row", style: "margin-top:10px" }, noteKind, fee),
        note, h("button", { onclick: () => api("POST", `/crm/creators/${id}/notes`, { kind: noteKind.value, body: note.value,
          data: fee.value ? { fee_usd: fee.value } : {} }).then(() => views.creator(id)).catch(e => flash(e.message, true)) }, "Add note")),
      panel("History", table([{ label: "When", render: r => date(r.created_at) }, { label: "From", key: "from_status" },
        { label: "To", key: "to_status" }, { label: "Actor", key: "actor" }, { label: "Reason", key: "reason" }], p.pipeline_events),
        table([{ label: "When", render: r => date(r.created_at) }, { label: "Kind", key: "kind" }, { label: "Note", key: "body" },
          { label: "Author", key: "author" }], p.notes)));
  },

  async compare() {
    const ids = [...state.selected];
    if (!ids.length) return setView("Partner comparison", panel(null, h("p", { class: "muted" }, "Select creators in Creators or a discovery run.")));
    const rows = await api("GET", "/creators/compare?ids=" + ids.join(","));
    setView("Partner comparison", panel(null, table([
      { label: "Creator", key: "name" }, { label: "Status", key: "status" },
      { label: "Subscribers", render: r => [num(r.subscribers), " ", r.subscribers_source === "fixture" ? tag(null, "fixture") : null], num: true },
      { label: "Niche", render: r => r.niche_relevance?.toFixed?.(2) ?? "—", num: true },
      { label: "Best ICP fit", render: r => { const f = (r.audience_fit || []).filter(x => x.fit !== null).sort((a, b) => b.fit - a.fit)[0];
        return f ? `${f.fit.toFixed(2)} (${pct(f.completeness)} complete)` : "—"; } },
      { label: "Risk", render: r => pill(r.risk_level) }, { label: "Score", render: r => r.overall?.toFixed?.(2) ?? "—", num: true },
      { label: "Confidence", render: r => r.confidence?.toFixed?.(2) ?? "—", num: true }, { label: "Recommendation", render: r => pill(r.recommendation) },
      { label: "Promotions", render: r => r.sponsorships.map(s => `${s.brand} (${s.type})`).join(", ") || "—" },
      { label: "Requested terms", render: r => r.requested_fee_notes.length ? money(r.requested_fee_notes[0].fee_usd) : "—" },
      { label: "Missing", render: r => (r.missing_data || []).join(", ") || "—" },
    ], rows, r => { location.hash = "#/creator/" + r.creator_id; })),
    h("button", { class: "secondary", onclick: () => { state.selected.clear(); saveSelected(); views.compare(); } }, "Clear selection"));
  },

  async profit() {
    const inp = (v, step = "any") => h("input", { type: "number", value: v, step, style: "width:100px" });
    const vLo = inp(10000), vBase = inp(20000), vHi = inp(30000);
    const cLo = inp(0.004), cBase = inp(0.01), cHi = inp(0.02);
    const pLo = inp(0.01), pBase = inp(0.03), pHi = inp(0.06);
    const fee = inp(500), rate = inp(""), months = inp(""), roi = inp(0), incr = inp(1), other = inp(0);
    const kind = h("select", {}, ...["fixed", "affiliate_only", "fixed_plus_recurring", "reduced_fixed_limited_commission", "fixed_plus_bonus"].map(k => h("option", { value: k }, k)));
    const actuals = h("input", { type: "checkbox", checked: true });
    const out = h("div");
    const go = async () => {
      const r = (lo, b, hi) => ({ low: lo.value, base: b.value, high: hi.value });
      const res = await api("POST", "/profit/forecast", {
        expected_views: r(vLo, vBase, vHi), click_through_rate: r(cLo, cBase, cHi), visitor_to_paid_rate: r(pLo, pBase, pHi),
        use_ledger_actuals: actuals.checked, required_roi: roi.value, incrementality: incr.value, other_campaign_costs: other.value,
        deal: { kind: kind.value, upfront_fee: fee.value || "0", commission_rate: rate.value || null, commission_months: months.value ? Number(months.value) : null,
          ...(kind.value === "fixed_plus_bonus" ? { bonus_threshold_customers: "5", bonus_per_customer_usd: "40", bonus_cap_usd: fee.value } : {}) },
        compare_structures: true,
      });
      const sc = res.scenarios;
      const grid = res.sensitivity;
      out.replaceChildren(
        h("div", { class: "notice" }, res.basis.statement),
        panel("Scenarios", table([{ label: "Metric", render: r => r[0] },
          ...["pessimistic", "base", "optimistic"].map(s => ({ label: s, render: r => r[1](sc[s]), num: true }))], [
          ["Paying customers", s => s.customers], ["Contribution / customer", s => money(s.contribution_per_customer)],
          ["Commission / customer", s => money(s.commission_per_customer)], ["Campaign profit", s => money(s.campaign_profit)],
          ["Contribution per $ acquisition", s => s.contribution_per_acquisition_dollar ?? "—"],
          ["Payback month", s => s.payback_month ?? "not within horizon"], ["Break-even customers", s => s.break_even_customers ?? "never"],
          ["Max justified fee", s => money(s.max_justified_upfront_fee)], ["Creator payout", s => money(s.creator_payout)]])),
        panel("Sensitivity: profit by churn (rows) × conversion (columns)", h("table", { class: "heat" },
          h("thead", {}, h("tr", {}, h("th", {}, "churn ×"), ...grid.conversion_multipliers.map(c => h("th", { class: "num" }, "conv ×" + c)))),
          h("tbody", {}, grid.campaign_profit.map((row, i) => h("tr", {}, h("th", {}, grid.churn_multipliers[i]),
            ...row.map(v => h("td", { class: Number(v) < 0 ? "neg" : "pos" }, money(v)))))))),
        res.assumption_simulation ? panel("Assumption simulation", tag("assumption"), h("p", {}, res.assumption_simulation.warning),
          h("p", {}, `Share of runs profitable: ${pct(res.assumption_simulation.share_of_runs_profitable)} · P10 ${money(res.assumption_simulation.p10)} · P50 ${money(res.assumption_simulation.p50)} · P90 ${money(res.assumption_simulation.p90)}`)) : null,
        panel("Deal structures (risk-adjusted)", h("p", { class: "muted" }, res.deal_comparison.method), table([
          { label: "Structure", render: r => r.deal.label }, { label: "Pessimistic", render: r => money(r.profit.pessimistic), num: true },
          { label: "Base", render: r => money(r.profit.base), num: true }, { label: "Optimistic", render: r => money(r.profit.optimistic), num: true },
          { label: "Risk-adjusted", render: r => money(r.risk_adjusted_profit), num: true }, { label: "Creator payout (base)", render: r => money(r.creator_payout_base), num: true },
          { label: "Needs signed agreement", render: r => r.requires_documented_agreement ? "yes" : "no" }], res.deal_comparison.ranking),
          h("p", { class: "muted" }, res.deal_comparison.caveat)),
        panel("Input basis", table([{ label: "Input", render: r => r[0] }, { label: "Basis", render: r => tag(r[1]) }], Object.entries(res.basis.inputs)),
          res.notes.length ? h("p", { class: "muted" }, res.notes.join(" · ")) : null));
    };
    setView("Profitability Lab",
      panel("Campaign assumptions (low / base / high)", h("div", { class: "row" },
        field("Expected views", h("span", {}, vLo, vBase, vHi)), field("Click-through", h("span", {}, cLo, cBase, cHi)),
        field("Visit → paid", h("span", {}, pLo, pBase, pHi))), h("div", { class: "row" },
        field("Deal", kind), field("Upfront fee", fee), field("Commission rate", rate), field("Commission months", months),
        field("Required ROI", roi), field("Incrementality", incr), field("Other costs", other),
        field("Use ledger actuals", actuals), h("button", { onclick: () => go().catch(e => flash(e.message, true)) }, "Forecast"))),
      out);
    await go();
  },

  async crm() {
    const board = await api("GET", "/crm/board");
    setView("Partnership CRM", h("div", { class: "kanban" }, Object.entries(board).map(([status, items]) =>
      h("div", { class: "column" }, h("h3", {}, `${status.replaceAll("_", " ")} (${items.length})`),
        items.map(i => h("div", { class: "item", onclick: () => { location.hash = "#/creator/" + i.id; } },
          h("div", {}, i.display_name || "—"), h("div", { class: "muted" }, i.next_action ? `${i.next_action} · ${date(i.next_action_due)}` : (i.recommendation || ""))))))));
  },

  async outreach() {
    const [drafts, settings] = await Promise.all([api("GET", "/outreach/drafts"), api("GET", "/outreach/settings")]);
    const testTo = await api("GET", "/outreach/test-recipients").then(r => r.recipients).catch(() => []);
    const detail = h("div");
    const show = d => {
      const subject = h("input", { value: d.subject, size: 80 }), body = h("textarea", {}); body.value = d.body_text;
      const to = h("select", {}, ...testTo.map(a => h("option", { value: a }, a)));
      const when = h("input", { type: "datetime-local" });
      const scheduleTo = v => api("POST", `/outreach/drafts/${d.id}/schedule`, { send_at: v })
        .then(() => { flash(v ? `Scheduled for ${new Date(v).toLocaleString()}` : "Schedule cleared"); views.outreach(); })
        .catch(e => flash(e.message, true));
      detail.replaceChildren(panel(`Draft for ${d.display_name} (${d.status})`,
        d.validation?.ok ? null : h("div", { class: "notice" }, "Validation: " + (d.validation?.errors || []).join("; ")),
        d.grounding?.prior_contact ? h("div", { class: "notice" }, d.grounding.prior_contact.summary) : null,
        h("p", { class: "muted" }, "Grounded in: " + (d.grounding?.videos || []).map(v => v.title).join(" · ")),
        field("Subject", subject), field("Body", body),
        h("div", { class: "row" },
          h("button", { class: "secondary", disabled: d.status !== "draft", onclick: () => api("PUT", `/outreach/drafts/${d.id}`, { subject: subject.value, body: body.value })
            .then(v => { flash(v.ok ? "Saved, valid" : "Saved, still invalid: " + v.errors.join("; "), !v.ok); views.outreach(); }).catch(e => flash(e.message, true)) }, "Save edits"),
          h("button", { disabled: d.status !== "draft", onclick: () => api("POST", `/outreach/drafts/${d.id}/approve`).then(() => { flash("Approved"); views.outreach(); }).catch(e => flash(e.message, true)) }, "Approve"),
          h("button", { class: "danger", disabled: !["draft", "approved"].includes(d.status), onclick: () => api("POST", `/outreach/drafts/${d.id}/reject?reason=operator`).then(() => views.outreach()).catch(e => flash(e.message, true)) }, "Reject"),
          h("button", { disabled: d.status !== "approved" || !settings.sending_enabled, onclick: () => api("POST", `/outreach/drafts/${d.id}/send`).then(() => { flash("Sent"); views.outreach(); }).catch(e => flash(e.message, true)) }, "Send")),
        d.status === "approved" ? h("div", { class: "row" },
          d.scheduled_send_at ? h("p", {}, `Scheduled for ${new Date(d.scheduled_send_at).toLocaleString()} (sent by RPI within the send window).`)
            : h("p", { class: "muted" }, "Not scheduled. Send now, or pick a time:"),
          field("Send at (your time)", when),
          h("button", { class: "secondary", onclick: () => when.value ? scheduleTo(new Date(when.value).toISOString()) : flash("Pick a date and time", true) }, "Schedule"),
          d.scheduled_send_at ? h("button", { class: "secondary", onclick: () => scheduleTo(null) }, "Clear schedule") : null) : null,
        d.failure_reason ? h("div", { class: "notice" }, d.failure_reason) : null,
        h("div", { class: "row" },
          testTo.length ? field("Test send to", to) : h("p", { class: "muted" }, "Set RPI_TEST_RECIPIENTS to enable test sends."),
          h("button", { class: "secondary", disabled: !testTo.length, onclick: () => api("POST", `/outreach/drafts/${d.id}/test-send`, { to: to.value })
            .then(r => flash(`Test sent to ${r.to}. Check that inbox; the creator was not emailed.`)).catch(e => flash(e.message, true)) },
            "Send test to me"))));
    };
    setView("Outreach",
      h("div", { class: settings.sending_enabled ? "panel" : "notice" }, settings.sending_enabled
        ? `Sending enabled by ${settings.enabled_by} · limit ${settings.daily_send_limit}/day · ${settings.min_days_between_contacts}-day contact spacing`
        : "Sending is disabled (research and drafts only). An admin can enable it under Administration once sender identity and postal address are set."),
      panel("Drafts", table([{ label: "Created", render: r => date(r.created_at) }, { label: "Creator", key: "display_name" },
        { label: "Subject", key: "subject" }, { label: "Status", render: r => pill(r.status) },
        { label: "Scheduled", render: r => r.scheduled_send_at ? new Date(r.scheduled_send_at).toLocaleString() : "—" },
        { label: "Valid", render: r => r.validation?.ok ? "yes" : "no" },
        { label: "Generator", key: "generator" }], drafts, show)), detail);
  },

  async autopilot() {
    const a = await api("GET", "/autopilot");
    const local = v => v ? new Date(v).toLocaleString() : "—";
    const check = (v) => { const i = h("input", { type: "checkbox" }); i.checked = !!v; return i; };
    const n = (v, min, max) => h("input", { type: "number", min, max, step: "1", value: v });
    const f = { enabled: check(a.enabled), auto_approve: check(a.auto_approve), weekly_credit_cap: n(a.weekly_credit_cap, 0, 100000),
      target_queue: n(a.target_queue, 0, 100), send_time: h("input", { type: "time", value: a.send_time }),
      discover_per_run: n(a.discover_per_run, 1, 100), screen_per_run: n(a.screen_per_run, 0, 100), analyze_per_run: n(a.analyze_per_run, 0, 50) };
    const save = () => api("PUT", "/autopilot", { enabled: f.enabled.checked, auto_approve: f.auto_approve.checked,
      weekly_credit_cap: Number(f.weekly_credit_cap.value), target_queue: Number(f.target_queue.value), send_time: f.send_time.value,
      discover_per_run: Number(f.discover_per_run.value), screen_per_run: Number(f.screen_per_run.value),
      analyze_per_run: Number(f.analyze_per_run.value), presets: a.presets })
      .then(() => { flash("Autopilot settings saved"); views.autopilot(); }).catch(e => flash(e.message, true));
    const run = a.last_run || {};
    const research = run.research;
    const list = (title, items) => items && items.length ? h("div", {}, h("strong", {}, title), h("ul", {}, items.map(i => h("li", {}, typeof i === "string" ? i : `${i.creator}: ${local(i.send_at)}`)))) : null;
    const w = a.send_window;
    setView("Autopilot",
      h("div", { class: a.enabled ? "panel" : "notice" }, a.enabled
        ? `Autopilot is on${a.auto_approve ? " and approves its own drafts" : "; its drafts wait for your approval"}. Research spends at most ${num(a.weekly_credit_cap)} CreatorDB credits in any 7 days. Replies are not read: answer them from your inbox and move the creator in the CRM.`
        : "Autopilot is off. Turn it on below to research, draft, approve and schedule without you. Sends still go through every Outreach rule and the daily limit."),
      h("div", { class: "grid" },
        card("Messages waiting", `${num(a.queue)} / ${num(a.target_queue)}`, h("div", { class: "muted" }, "research runs when below target")),
        card("Credits used (7 days)", num(a.credits_used_7d)), card("Credits left (7 days)", num(a.credits_left_7d)),
        card("Qualified, not yet screened", num(a.candidates_waiting)),
        card("Next search", a.next_search ? `${a.next_search.preset} from #${Number(a.next_search.page_token || 0) + 1}` : "all presets searched")),
      panel("Settings (admin)", h("div", { class: "row" },
        field("On", f.enabled), field("Approve its own drafts", f.auto_approve), field("Weekly credit allowance", f.weekly_credit_cap),
        field("Keep this many messages waiting", f.target_queue), field(`Send time (${w.timezone})`, f.send_time)),
        h("div", { class: "row" }, field("Creators per search", f.discover_per_run), field("Screen per run (3 credits each)", f.screen_per_run),
          field("Deep-analyze per run (up to 20 credits each)", f.analyze_per_run),
          h("button", { onclick: save }, "Save"),
          h("button", { class: "secondary", onclick: () => api("POST", "/autopilot/run").then(() => flash("Autopilot run queued; refresh in a few minutes")).catch(e => flash(e.message, true)) }, "Run now")),
        h("p", { class: "muted" }, `Runs daily at 07:30 ${w.timezone}. Sends go out ${w.weekdays_only ? "on weekdays " : ""}inside ${w.window}, at most the Outreach daily limit per day.`)),
      panel("Scheduled sends", a.upcoming.length ? table([{ label: "Creator", key: "creator" }, { label: "Sends", render: r => local(r.send_at) },
        { label: "Approved by", key: "approved_by" }], a.upcoming) : h("p", { class: "muted" }, "Nothing scheduled.")),
      panel(`Last run ${a.last_run_at ? "(" + local(a.last_run_at) + ")" : ""}`, a.last_run_at ? h("div", {},
        list("Drafted", run.drafted), list("Approved", run.approved), list("Scheduled", run.scheduled), list("Skipped", run.skipped),
        h("p", {}, "Research: " + (typeof research === "object" && research
          ? `${research.preset || "no search"}: ${num(research.screened)} screened, ${num(research.deep_analyzed)} deep-analyzed, ${num(research.recommended)} recommended, ${num(research.credits)} credits${research.stopped ? " (" + research.stopped + ")" : ""}`
          : (research || "—")))) : h("p", { class: "muted" }, "Not run yet.")));
  },

  async campaigns() {
    const model = h("select", {}, h("option", { value: "last_touch" }, "last touch (affiliate program)"), h("option", { value: "first_touch" }, "first touch"));
    const out = h("div");
    const load = async () => {
      const [camps, rep, coh] = await Promise.all([api("GET", "/campaigns"), api("GET", "/reports/partners?attribution_model=" + model.value),
        api("GET", "/reports/cohorts?attribution_model=" + model.value)]);
      out.replaceChildren(
        panel("Partner profitability (observed ledger)", rep.length ? table([
          { label: "Campaign", key: "campaign" }, { label: "Creator", key: "creator" }, { label: "Signups", key: "signups", num: true },
          { label: "Paying", key: "paying_customers", num: true }, { label: "Active", key: "active_customers", num: true },
          { label: "Net revenue", render: r => money(r.net_revenue_usd), num: true },
          { label: "Variable costs", render: r => [money(r.variable_costs_usd), Number(r.variable_costs_estimated_usd) ? h("div", {}, tag("estimate"), " ", money(r.variable_costs_estimated_usd)) : null], num: true },
          { label: "Contribution", render: r => money(r.contribution_usd), num: true },
          { label: "Acquisition", render: r => Object.entries(r.acquisition_costs_usd).map(([k, v]) => h("div", {}, `${k}: ${money(v)}`)) },
          { label: "Profit", render: r => money(r.campaign_profit_usd), num: true },
          { label: "Per $", render: r => r.contribution_per_acquisition_dollar ?? "—", num: true },
          { label: "Status", render: r => r.complete ? "complete" : h("span", { class: "muted" }, r.statement) }], rep) : h("p", { class: "muted" }, "No campaigns yet.")),
        panel("Subscriber cohorts", coh.cohorts.length ? table([{ label: "Cohort", key: "cohort" }, { label: "Customers", key: "customers", num: true },
          { label: "Retention by month", render: r => r.retention.map(pct).join(" · ") },
          { label: "Cumulative contribution / customer", render: r => r.cumulative_contribution_per_customer_usd.map(money).join(" · ") }], coh.cohorts)
          : h("p", { class: "muted" }, coh.note)),
        panel("Campaigns", table([{ label: "Name", key: "name" }, { label: "Creator", key: "display_name" }, { label: "Referral code", key: "referral_code" },
          { label: "UTM campaign", key: "utm_campaign" }, { label: "Window (days)", key: "attribution_window_days", num: true }, { label: "Status", key: "status" }], camps)));
    };
    setView("Campaign performance", panel(null, h("div", { class: "row" }, field("Attribution model", model),
      h("button", { class: "secondary", onclick: () => load().catch(e => flash(e.message, true)) }, "Refresh"))), out);
    await load();
  },

  async admin() {
    const [prov, budgets, jobs, recon, compliance] = await Promise.all([api("GET", "/admin/providers"), api("GET", "/admin/budgets"),
      api("GET", "/jobs"), api("GET", "/reports/reconciliation").catch(() => null), api("GET", "/admin/compliance")]);
    const amt = h("input", { type: "number", step: "1", placeholder: "new limit USD" }), why = h("input", { placeholder: "reason" });
    const bsel = h("select", {}, ...budgets.budgets.map(b => h("option", { value: b.id }, `${b.scope} ${b.period} ${b.provider || ""}`)));
    setView("Administration",
      panel(`Providers (RPI ${prov.version})`, table([{ label: "Provider", key: "provider" }, { label: "Configured", render: r => r.configured ? "yes" : "no" },
        { label: "Capabilities", render: r => (r.capabilities || r.models || []).join(", ") || "—" },
        { label: "Circuit", render: r => r.health?.circuit_state || "—" }, { label: "Detail", render: r => r.health?.detail || r.detail || "—" },
        { label: "Last 24h", render: r => r.last_24h ? `${r.last_24h.calls} calls, ${r.last_24h.errors} errors, ${r.last_24h.denied} denied` : "—" },
        { label: "Terms verified", render: r => r.policy_verified === undefined ? "—" : (r.policy_verified ? "yes" : "no") }], prov.providers)),
      panel("Budgets", table([{ label: "Scope", key: "scope" }, { label: "Period", key: "period" }, { label: "Provider", key: "provider" },
        { label: "Limit", render: r => money(r.limit_usd), num: true }, { label: "Spent", render: r => money(r.spent_usd), num: true },
        { label: "Alert at", render: r => pct(r.alert_ratio), num: true }], budgets.budgets),
        h("div", { class: "row", style: "margin-top:10px" }, bsel, amt, why, h("button", { onclick: () => api("POST", `/admin/budgets/${bsel.value}/change-requests`,
          { requested_limit_usd: amt.value, reason: why.value }).then(r => { flash(r.applied ? "Budget updated" : "Increase requested; an admin must approve"); views.admin(); })
          .catch(e => flash(e.message, true)) }, "Request change")),
        budgets.change_requests.length ? table([{ label: "Requested", render: r => money(r.requested_limit_usd) }, { label: "By", key: "requested_by" },
          { label: "Reason", key: "reason" }, { label: "Status", key: "status" },
          { label: "", render: r => r.status === "pending" ? h("button", { onclick: e => { e.stopPropagation(); api("POST", `/admin/budget-requests/${r.id}`, { approve: true })
            .then(() => views.admin()).catch(er => flash(er.message, true)); } }, "Approve") : "" }], budgets.change_requests) : null),
      panel("Jobs", table([{ label: "Created", render: r => date(r.created_at) }, { label: "Type", key: "job_type" }, { label: "Status", render: r => pill(r.status) },
        { label: "Spent", render: r => money(r.spent_usd), num: true }, { label: "Budget", render: r => money(r.budget_usd), num: true },
        { label: "Error", key: "error" }, { label: "", render: r => ["paused_budget", "failed"].includes(r.status) ? h("button", { onclick: e => { e.stopPropagation();
          api("POST", `/jobs/${r.id}/resume`).then(() => flash("Resumed")).catch(er => flash(er.message, true)); } }, "Resume") : "" }], jobs)),
      recon ? panel("Ledger reconciliation", h("pre", {}, JSON.stringify(recon, null, 2))) : null,
      panel("Data licensing policies", table([{ label: "Provider", render: r => r[0] }, { label: "Verified", render: r => r[1].verified ? `yes (${r[1].verified_on})` : "no — conservative defaults" },
        { label: "Derived metrics", render: r => r[1].derived_metrics }, { label: "Training", render: r => r[1].model_training },
        { label: "Max storage (days)", render: r => r[1].max_storage_days ?? "—" }, { label: "Notes", render: r => r[1].notes.join(" ") }], Object.entries(compliance))),
      panel("Retention", h("button", { class: "secondary", onclick: () => api("POST", "/admin/retention/run?dry_run=true")
        .then(r => flash("Would delete: " + JSON.stringify(r.expired_provenance))).catch(e => flash(e.message, true)) }, "Dry run")));
  },
};

// ---------------------------------------------------------------- routing
async function route() {
  const [, name = "dashboard", arg] = (location.hash || "#/dashboard").split("/");
  if (!key()) return setView("Connect", panel(null, h("p", {}, "Enter an RPI API key (X-RPI-Key) above to continue.")));
  try { await (views[name] || views.dashboard)(arg); }
  catch (e) { flash(e.message, true); if (String(e.message).startsWith("401")) sessionStorage.removeItem("rpi.key"); }
}
document.getElementById("save-key").addEventListener("click", () => {
  const v = document.getElementById("key").value.trim();
  if (v) sessionStorage.setItem("rpi.key", v);
  document.getElementById("key").value = "";
  route();
});
window.addEventListener("hashchange", route);
route();
