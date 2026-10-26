#!/usr/bin/env bun
// cc-lb actual-CLI E2E driver: Codex CLI (real binary) -> owned plaintext tee -> cc-lb -> real OpenAI.
//
// Scenarios (one fresh private CODEX_HOME per session, one fixture copy per session):
//   http          supports_websockets=false, turn1 coding tool loop + `codex exec resume <thread_id>`
//   ws            supports_websockets=true,  turn1 coding tool loop + `codex exec resume <thread_id>`
//   ws_reconnect  supports_websockets=true,  turn1 only; the owned tee injects ONE labeled WS drop
//                 right after the first response.completed that carries output, then observes the
//                 client's next generation request (new connection? full replay? stale prev id?).
//
// Paid-bound enforcement lives in the tee because cc-lb's WS relay (crates/cc-lb-server/src/app.rs
// relay_responses_websocket) has no usage/limit hook: every POST /v1/responses and every WS
// `response.create` is admitted only while slice attempts < 16 and driver-computed USD < $3;
// otherwise the tee refuses locally (labeled) and kills the client.
//
// Evidence is metadata-only: hashes/presence/counts, never raw headers, bodies or key values.
// Requires Bun (Bun.serve WebSocket server + header-capable WebSocket client).
//
// Pricing comes only from the cc-lb runtime price catalog (ctx.costCatalog / ctx.priceCatalogPath);
// an unpriced model or any ctx.blocked entry => slice BLOCKED before any CLI launch.
// Optional env:
//   CODEX_QA_MODEL (default gpt-5.1-codex), CODEX_QA_SCENARIOS (comma list, default http,ws,ws_reconnect)

import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  appendFileSync,
  chmodSync,
  cpSync,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  realpathSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import {
  assertBudget,
  budgetStatus,
  finishEvidence,
  observabilityBaseline,
  prepareSlice,
  recordResource,
  scanCredentialFiles,
  waitForRequestEvents,
} from "../qa-runtime.mjs";

if (typeof Bun === "undefined") {
  console.error("run.mjs requires Bun (bun run codex/run.mjs)");
  process.exit(2);
}

const SLICE_DIR = dirname(fileURLToPath(import.meta.url));
const ROOT = dirname(SLICE_DIR);
const FIXTURE = join(SLICE_DIR, "fixture");
const RUN_ID = new Date().toISOString().replace(/[:.]/g, "-");
const RUN_DIR = join(SLICE_DIR, "runs", RUN_ID);
const CONTRACT = JSON.parse(readFileSync(join(ROOT, "contract.json"), "utf8"));
const ARTIFACT = CONTRACT.artifact;
const BOUNDS = CONTRACT.paid_bounds;
// gpt-5.1-codex was rejected by the provider (model_not_found) for this key; gpt-5.2 has real
// generations through the same key/endpoint (omp-gpt slice). Pass CODEX_QA_MODEL explicitly.
const MODEL = process.env.CODEX_QA_MODEL || "gpt-5.1-codex";
const PORTS = [18500, 18501, 18502];
const SCENARIOS = (process.env.CODEX_QA_SCENARIOS || "http,ws,ws_reconnect").split(",").map((s) => s.trim()).filter(Boolean);
// Parent-approved revision may raise the slice attempt cap (e.g. 32) via CODEX_QA_ATTEMPT_CAP;
// USD cap stays at the contract value. Earlier attempts are retained through driver-budget.json.
const ATTEMPT_CAP_OVERRIDE = process.env.CODEX_QA_ATTEMPT_CAP ? Number(process.env.CODEX_QA_ATTEMPT_CAP) : null;
if (ATTEMPT_CAP_OVERRIDE != null && !(Number.isInteger(ATTEMPT_CAP_OVERRIDE) && ATTEMPT_CAP_OVERRIDE > 0 && ATTEMPT_CAP_OVERRIDE <= 64))
  throw new Error("CODEX_QA_ATTEMPT_CAP must be an integer in 1..64 (Codex-only qa-runtime limitOverrides bound)");
const SLICE_ATTEMPTS = ATTEMPT_CAP_OVERRIDE ?? BOUNDS.generation_attempts_per_slice;
const SLICE_USD = BOUNDS.usd_ceiling_per_slice; // 3
const TURN_MS = BOUNDS.wall_seconds_per_cli_run * 1000; // 120s
// Sized from the observed gpt-5.2 loop: WS turn1 used 5 frames (1 generate:false prewarm + 4
// generations), HTTP turn1 used 4; the turn-2 task allows 5 tool calls (~6 generations + prewarm).
// 12 per 2-turn session; reconnect (single turn) 6. Upper bound per full rerun: 12+12+6 = 30.
const SCENARIO_ATTEMPT_CAP = { http: 12, ws: 12, ws_reconnect: 6 };
const MIN_REMAINING_TO_START = 3;
const MIN_REMAINING_FOR_RESUME = 3;
const EVENT_DEADLINE_MS = 30_000;
const UNPRICED_ATTEMPT_USD = 0.05; // conservative charge for an attempt that ended without usage
const INJECTED_DROP_LABEL = "cc-lb-qa-injected-ws-drop";
const LOCAL_STOP_LABEL = "cc-lb-qa-local-budget-stop";
const LOSS_COUNTERS = CONTRACT.observability_guard.loss_counters;

const PROMPT_TURN1 =
  "You are working in a tiny Node.js project in the current directory. Use your shell tool to read " +
  "src/cart.js and test/cart.test.js, fix the bug that makes the subtotal test fail with the smallest " +
  "possible edit to src/cart.js, then run `node --test` and report the pass/fail counts. Use at most 3 " +
  "tool calls. Do not touch files outside this directory.";
// Turn 2 asks for genuinely new behavior (the fixture already has applyDiscount and its basic test):
// out-of-range percents must throw RangeError, 0 and 100 stay valid, and new boundary tests must be added.
// codex/hidden-check.mjs (never copied into the project) asserts the behavior independently.
const PROMPT_TURN2 =
  "Continue in the same project. Change applyDiscount in src/cart.js so it throws a RangeError when percent " +
  "is below 0 or above 100; 0 and 100 must remain valid. Keep every existing test, add tests in " +
  "test/cart.test.js for a negative percent and for a percent above 100 (both expecting RangeError), run " +
  "`node --test`, and summarize both of your changes in two sentences. Use at most 5 tool calls.";
const HIDDEN_CHECK = join(SLICE_DIR, "hidden-check.mjs");
const MIN_TESTS = { 1: 3, 2: 5 };

// ---------------------------------------------------------------------------------------------
// helpers

const sha = (v) => createHash("sha256").update(v).digest("hex");
const short = (v) => (v == null ? null : sha(String(v)).slice(0, 12));
const now = () => Date.now();
const tryJson = (t) => {
  try {
    return JSON.parse(t);
  } catch {
    return undefined;
  }
};
let SECRET_VALUES = [];
const redact = (s) => {
  let out = String(s);
  for (const v of SECRET_VALUES) if (v && v.length >= 8) out = out.split(v).join("[REDACTED]");
  return out
    .replace(/\b(sk-[A-Za-z0-9_-]{4})[A-Za-z0-9_-]{8,}/g, "$1…[REDACTED]")
    .replace(/\b(cclb_[A-Za-z0-9]{4})[A-Za-z0-9_-]{8,}/g, "$1…[REDACTED]")
    .replace(/(bearer\s+)[A-Za-z0-9._-]{8,}/gi, "$1[REDACTED]");
};
const writePrivate = (path, content) => {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, content, { mode: 0o600 });
  chmodSync(path, 0o600);
};
const appendPrivate = (path, line) => {
  if (!existsSync(path)) writePrivate(path, "");
  appendFileSync(path, line);
};
const fileSha = (path) => sha(readFileSync(path));
const walk = (dir, out = []) => {
  if (!existsSync(dir)) return out;
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    let st;
    try {
      st = statSync(p);
    } catch {
      continue;
    }
    if (st.isDirectory()) walk(p, out);
    else out.push(p);
  }
  return out;
};

// ---------------------------------------------------------------------------------------------
// pricing / budget

function loadPricing(ctx) {
  const key = ctx.costCatalog?.prices?.[MODEL]?.key;
  if (!ctx.costCatalog?.modelPriced || key !== MODEL) return { ok: false, reason: `model ${MODEL} not priced in cc-lb runtime catalog` };
  const entry = tryJson(readFileSync(ctx.priceCatalogPath, "utf8"))?.[key];
  if (!entry || entry.litellm_provider !== "openai" || typeof entry.input_cost_per_token !== "number" || typeof entry.output_cost_per_token !== "number")
    return { ok: false, reason: `catalog entry ${key} lacks native openai input/output cost` };
  const cached = typeof entry.cache_read_input_token_cost === "number" ? entry.cache_read_input_token_cost : entry.input_cost_per_token;
  return {
    ok: true,
    catalog_key: key,
    input_per_mtok: entry.input_cost_per_token * 1e6,
    cached_input_per_mtok: cached * 1e6,
    output_per_mtok: entry.output_cost_per_token * 1e6,
    cached_price_source: typeof entry.cache_read_input_token_cost === "number" ? "cache_read_input_token_cost" : "input_cost_per_token (no cache price in catalog)",
    source: ctx.priceCatalogPath,
    as_of: statSync(ctx.priceCatalogPath).mtime.toISOString(),
    label: "driver-computed from cc-lb runtime catalog (tee gate only; not cc-lb cost evidence)",
    catalog_model: MODEL,
    catalog_installed_by_server: ctx.costCatalog.installedByServer ?? null,
    catalog_provenance: ctx.costCatalog.provenance ?? null,
  };
}

function usageCost(pricing, u) {
  if (!u) return null;
  const input = u.input_tokens ?? 0;
  const cached = u.cached_tokens ?? 0;
  const output = u.output_tokens ?? 0;
  return ((input - cached) * pricing.input_per_mtok + cached * pricing.cached_input_per_mtok + output * pricing.output_per_mtok) / 1e6;
}

// usd = gate total = usageUsd (priced from provider-reported usage at catalog prices) +
// reserveUsd (conservative charge for attempts that ended without usage, e.g. provider 404s;
// a gate reserve, NOT billed provider cost).
const budget = {
  attempts: 0,
  usd: 0,
  usageUsd: 0,
  reserveUsd: 0,
  legacyUsd: 0,
  lastCost: 0,
  open: new Map(), // attemptKey -> {scenario, priced:false}
  refusals: [],
};

// Driver-side tally survives QA_RESUME reruns: WS attempts never reach cc-lb's server budget,
// so the slice cap must include attempts/cost from earlier driver processes.
const DRIVER_BUDGET_PATH = join(SLICE_DIR, "driver-budget.json");
function persistBudget() {
  writePrivate(
    DRIVER_BUDGET_PATH,
    JSON.stringify({ updated_at: new Date().toISOString(), run_id: RUN_ID, model: MODEL, attempts: budget.attempts, usd: budget.usd, usage_usd: budget.usageUsd, reserve_usd: budget.reserveUsd, legacy_usd: budget.legacyUsd, last_cost: budget.lastCost, refusals: budget.refusals.length }, null, 2),
  );
}
async function seedBudget(ctx) {
  const saved = existsSync(DRIVER_BUDGET_PATH) ? tryJson(readFileSync(DRIVER_BUDGET_PATH, "utf8")) ?? {} : {};
  const server = await budgetStatus(ctx);
  // Budget files written before the usage/reserve split carry only a gate total: keep it whole
  // as legacy gate USD (counted against the cap, attributed to neither billed usage nor reserve).
  const legacy = saved.usage_usd == null && saved.reserve_usd == null;
  const savedUsage = saved.usage_usd ?? 0;
  const savedReserve = saved.reserve_usd ?? 0;
  const savedLegacy = legacy ? saved.usd ?? 0 : saved.legacy_usd ?? 0;
  budget.attempts = Math.max(saved.attempts ?? 0, server.attempts ?? 0);
  budget.usageUsd = savedUsage;
  budget.reserveUsd = savedReserve;
  budget.legacyUsd = savedLegacy;
  budget.usd = Math.max(savedUsage + savedReserve + savedLegacy, server.costUsd ?? 0);
  // A previous model's 404 reserve must not become the next model's cost estimate.
  budget.lastCost = saved.model === MODEL ? saved.last_cost ?? 0 : 0;
  budget.seed = {
    saved_model: saved.model ?? null,
    saved_attempts: saved.attempts ?? 0,
    saved_usage_usd: savedUsage,
    saved_reserve_usd: savedReserve,
    saved_legacy_unsplit_gate_usd: savedLegacy,
    server_attempts: server.attempts,
    server_cost_usd: server.costUsd,
    server_cost_known: server.costKnown,
  };
  persistBudget();
}

// ---------------------------------------------------------------------------------------------
// wire summaries (metadata only)

function summarizeHeaders(h) {
  const names = [...h.keys()].sort();
  const val = (k) => h.get(k);
  return {
    names,
    authorization_present: h.has("authorization"),
    authorization_scheme: val("authorization")?.split(" ")[0] ?? null,
    authorization_sha12: short(val("authorization")),
    x_api_key_present: h.has("x-api-key"),
    chatgpt_account_id_present: h.has("chatgpt-account-id"),
    session_id_present: h.has("session_id") || h.has("session-id"),
    session_id_sha12: short(val("session_id") ?? val("session-id")),
    conversation_id_present: h.has("conversation_id"),
    x_codex_turn_state_present: h.has("x-codex-turn-state"),
    x_codex_turn_state_sha12: short(val("x-codex-turn-state")),
    openai_beta: val("openai-beta") ?? null,
    originator: val("originator") ?? null,
    version: val("version") ?? null,
    user_agent: val("user-agent") ? redact(val("user-agent")).slice(0, 160) : null,
    content_encoding: val("content-encoding") ?? null,
  };
}

function summarizeInputItem(it) {
  if (!it || typeof it !== "object") return { type: typeof it };
  const s = { type: it.type ?? (it.role ? "message" : "unknown") };
  if (it.role) s.role = it.role;
  if (it.call_id) s.call_id_sha12 = short(it.call_id);
  if (it.name) s.name = it.name;
  if (it.id) s.id_sha12 = short(it.id);
  if (it.type === "reasoning") s.encrypted_content_present = Boolean(it.encrypted_content);
  return s;
}

function summarizeGenBody(obj, rawBytes) {
  if (!obj || typeof obj !== "object") return { parsed: false, bytes: rawBytes?.length ?? 0 };
  const input = Array.isArray(obj.input) ? obj.input : [];
  const items = input.map(summarizeInputItem);
  return {
    parsed: true,
    bytes: rawBytes?.length ?? null,
    body_sha12: rawBytes ? sha(rawBytes).slice(0, 12) : null,
    frame_type: obj.type ?? null,
    model: obj.model ?? null,
    stream: obj.stream ?? null,
    store: obj.store ?? null,
    generate: obj.generate ?? null,
    previous_response_id_present: Boolean(obj.previous_response_id),
    previous_response_id: obj.previous_response_id ?? null, // provider-issued id, not a secret
    prompt_cache_key_present: Boolean(obj.prompt_cache_key),
    prompt_cache_key_sha12: short(obj.prompt_cache_key),
    instructions_sha12: short(obj.instructions),
    max_output_tokens: obj.max_output_tokens ?? null,
    reasoning: obj.reasoning ? { effort: obj.reasoning.effort ?? null, summary: obj.reasoning.summary ?? null } : null,
    include: obj.include ?? null,
    tool_choice: obj.tool_choice ?? null,
    parallel_tool_calls: obj.parallel_tool_calls ?? null,
    tools: Array.isArray(obj.tools) ? obj.tools.map((t) => t.name ?? t.type) : null,
    input_count: input.length,
    input_items: items,
    has_user_message: items.some((i) => i.type === "message" && i.role === "user"),
    function_call_ids: items.filter((i) => i.type === "function_call" || i.type === "custom_tool_call" || i.type === "local_shell_call").map((i) => i.call_id_sha12),
    function_call_output_ids: items.filter((i) => /_output$/.test(i.type)).map((i) => i.call_id_sha12),
  };
}

function summarizeProviderEvent(ev) {
  if (!ev || typeof ev !== "object" || !ev.type) return null;
  const r = ev.response;
  const out = { type: ev.type };
  if (r?.id) out.response_id = r.id;
  if (ev.type === "response.completed" || ev.type === "response.failed" || ev.type === "response.incomplete") {
    out.status = r?.status ?? null;
    const u = r?.usage;
    out.usage = u
      ? {
          input_tokens: u.input_tokens ?? null,
          cached_tokens: u.input_tokens_details?.cached_tokens ?? null,
          output_tokens: u.output_tokens ?? null,
          reasoning_tokens: u.output_tokens_details?.reasoning_tokens ?? null,
          total_tokens: u.total_tokens ?? null,
        }
      : null;
    out.output = Array.isArray(r?.output)
      ? r.output.map((o) => ({ type: o.type, name: o.name ?? null, call_id_sha12: short(o.call_id) }))
      : null;
    out.store = r?.store ?? null;
    out.model = r?.model ?? null;
    out.previous_response_id = r?.previous_response_id ?? null;
    out.error = r?.error ? { code: r.error.code ?? null, type: r.error.type ?? null } : null;
    out.incomplete_details = r?.incomplete_details ?? null;
  }
  if (ev.type === "error") out.error = { code: ev.error?.code ?? ev.code ?? null, type: ev.error?.type ?? null, status: ev.status ?? null };
  return out;
}

async function readSse(stream, onEvent) {
  const reader = stream.getReader();
  const dec = new TextDecoder();
  let buf = "";
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let idx;
      while ((idx = buf.search(/\r?\n\r?\n/)) >= 0) {
        const chunk = buf.slice(0, idx);
        buf = buf.slice(idx).replace(/^\r?\n\r?\n/, "");
        const data = chunk.split(/\r?\n/).filter((l) => l.startsWith("data:")).map((l) => l.slice(5).trimStart()).join("\n");
        if (data && data !== "[DONE]") onEvent(tryJson(data));
      }
    }
    return { ended: "clean" };
  } catch (e) {
    return { ended: "error", error: String(e?.name ?? e) };
  }
}

// ---------------------------------------------------------------------------------------------
// owned plaintext tee (client -> tee -> cc-lb proxy); labeled local refusals and injected WS drop

const HOP = new Set([
  "host", "connection", "upgrade", "sec-websocket-key", "sec-websocket-version",
  "sec-websocket-extensions", "sec-websocket-protocol", "content-length", "transfer-encoding", "keep-alive",
]);

function startTee(targetOrigin, pricing) {
  const target = new URL(targetOrigin);
  const wsOrigin = `${target.protocol === "https:" ? "wss" : "ws"}://${target.host}`;
  let S = null; // current scenario state
  let connSeq = 0;

  const log = (e) => {
    if (!S) return;
    const rec = { ts: new Date().toISOString(), scenario: S.name, turn: S.turn, ...e };
    S.wire.push(rec);
    appendPrivate(S.wirePath, redact(JSON.stringify(rec)) + "\n");
  };

  const admit = (transport, attemptKey) => {
    const reason =
      budget.attempts >= SLICE_ATTEMPTS ? `slice attempts ${budget.attempts}/${SLICE_ATTEMPTS}`
      : S.attempts >= S.attemptCap ? `scenario attempts ${S.attempts}/${S.attemptCap}`
      : budget.usd + Math.max(budget.lastCost * 1.5, UNPRICED_ATTEMPT_USD) > SLICE_USD ? `projected usd ${budget.usd.toFixed(4)} would exceed ${SLICE_USD}`
      : null;
    if (reason) {
      const refusal = { label: LOCAL_STOP_LABEL, injected: true, forwarded: false, transport, reason };
      budget.refusals.push({ scenario: S.name, ...refusal });
      log({ event: "local_refusal", ...refusal });
      S.killClient?.(LOCAL_STOP_LABEL);
      return false;
    }
    budget.attempts += 1;
    S.attempts += 1;
    budget.open.set(attemptKey, { scenario: S.name, priced: false });
    persistBudget();
    return true;
  };

  const settle = (attemptKey, summary) => {
    const a = budget.open.get(attemptKey);
    if (!a || a.priced) return;
    a.priced = true;
    const cost = summary?.usage ? usageCost(pricing, summary.usage) : null;
    const charged = cost ?? Math.max(budget.lastCost, UNPRICED_ATTEMPT_USD);
    budget.usd += charged;
    if (cost != null) {
      budget.lastCost = cost;
      budget.usageUsd += cost;
    } else budget.reserveUsd += charged;
    log({ event: "attempt_settled", attempt: attemptKey, driver_usd: Number(charged.toFixed(6)), priced_from_usage: cost != null });
    persistBudget();
    if (budget.usd >= SLICE_USD) S?.killClient?.(`${LOCAL_STOP_LABEL}: usd cap reached`);
  };

  const onProviderEvent = (transport, conn, attemptKey, ev) => {
    const s = summarizeProviderEvent(ev);
    if (!s) return null;
    S.eventTypes[s.type] = (S.eventTypes[s.type] ?? 0) + 1;
    if (s.type === "response.created" || s.type === "response.completed" || s.type === "response.failed" || s.type === "response.incomplete" || s.type === "error")
      log({ event: "provider_event", transport, conn, attempt: attemptKey, ...s });
    if (s.type === "response.completed" || s.type === "response.failed" || s.type === "response.incomplete" || s.type === "error") {
      S.responses.push({ transport, conn, attempt: attemptKey, ...s });
      settle(attemptKey, s);
    }
    return s;
  };

  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    idleTimeout: 255,
    async fetch(req, srv) {
      const url = new URL(req.url);
      if (!S) return new Response("tee idle", { status: 503 });
      const headers = summarizeHeaders(req.headers);
      if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
        const id = ++connSeq;
        const fwd = {};
        for (const [k, v] of req.headers) if (!HOP.has(k)) fwd[k] = v;
        const upstream = new WebSocket(`${wsOrigin}${url.pathname}${url.search}`, { headers: fwd });
        const opened = await new Promise((resolve) => {
          upstream.addEventListener("open", () => resolve(true), { once: true });
          upstream.addEventListener("error", () => resolve(false), { once: true });
          upstream.addEventListener("close", () => resolve(false), { once: true });
        });
        if (!opened) {
          log({ event: "ws_upgrade_failed", conn: id, path: url.pathname, headers });
          return new Response(JSON.stringify({ error: { type: "tee_upstream_ws_failed" } }), { status: 502, headers: { "content-type": "application/json" } });
        }
        log({ event: "ws_open", conn: id, path: url.pathname, query_present: Boolean(url.search), headers });
        S.wsConns.push({ id, opened_at: now(), path: url.pathname, headers });
        if (srv.upgrade(req, { data: { id, upstream, scenario: S, attempt: null } })) return undefined;
        upstream.close();
        return new Response("upgrade failed", { status: 400 });
      }

      const raw = ["GET", "HEAD"].includes(req.method) ? null : new Uint8Array(await req.arrayBuffer());
      const isGen = req.method === "POST" && /\/responses\/?$/.test(url.pathname);
      let parsed;
      if (raw && !headers.content_encoding) parsed = tryJson(new TextDecoder().decode(raw));
      else if (raw && headers.content_encoding === "zstd" && typeof Bun.zstdDecompressSync === "function") {
        try {
          parsed = tryJson(new TextDecoder().decode(Bun.zstdDecompressSync(raw)));
        } catch {}
      }
      const attemptKey = isGen ? `${S.name}:http:${S.attempts + 1}` : null;
      const reqSummary = isGen ? summarizeGenBody(parsed, raw) : null;
      let httpEntry = null;
      if (isGen) {
        if (!admit("http", attemptKey)) {
          return new Response(JSON.stringify({ error: { type: LOCAL_STOP_LABEL, message: "local QA paid-bound refusal (not provider)" } }), { status: 400, headers: { "content-type": "application/json" } });
        }
        httpEntry = { transport: "http", attempt: attemptKey, ts: now(), headers, ...reqSummary };
        S.requests.push(httpEntry);
      }
      const fwd = new Headers();
      for (const [k, v] of req.headers) if (!HOP.has(k)) fwd.set(k, v);
      const started = now();
      let res;
      try {
        res = await fetch(`${target.origin}${url.pathname}${url.search}`, { method: req.method, headers: fwd, body: raw ?? undefined });
      } catch (e) {
        log({ event: "http", method: req.method, path: url.pathname, attempt: attemptKey, error: String(e?.name ?? e) });
        if (attemptKey) settle(attemptKey, null);
        return new Response("tee upstream error", { status: 502 });
      }
      const rh = new Headers(res.headers);
      rh.delete("content-length");
      rh.delete("content-encoding");
      rh.delete("transfer-encoding");
      if (httpEntry) httpEntry.http_status = res.status;
      const respMeta = {
        status: res.status,
        content_type: res.headers.get("content-type"),
        request_id_header_present: res.headers.has("x-request-id") || res.headers.has("request-id"),
        cc_lb_header_names: [...res.headers.keys()].filter((k) => /cc-lb|x-codex|openai|ratelimit/i.test(k)).sort(),
      };
      log({ event: "http", method: req.method, path: url.pathname, attempt: attemptKey, ms_to_headers: now() - started, headers, request: reqSummary, response: respMeta });
      if (!res.body) {
        if (attemptKey) settle(attemptKey, null);
        return new Response(null, { status: res.status, headers: rh });
      }
      const [toClient, toTap] = res.body.tee();
      const scen = S;
      if (respMeta.content_type?.includes("event-stream")) {
        readSse(toTap, (ev) => {
          if (S === scen) onProviderEvent("http", null, attemptKey, ev);
        }).then((end) => {
          if (S === scen) log({ event: "http_stream_end", attempt: attemptKey, ...end });
          if (attemptKey) settle(attemptKey, null);
        });
      } else {
        new Response(toTap).text().then((t) => {
          const j = tryJson(t);
          if (attemptKey) {
            if (S === scen) log({ event: "http_body", attempt: attemptKey, status: res.status, error: j?.error ? { type: j.error.type ?? null, code: j.error.code ?? null } : null, response_id: j?.id ?? null });
            settle(attemptKey, j?.usage ? summarizeProviderEvent({ type: "response.completed", response: j }) : null);
          }
        });
      }
      return new Response(toClient, { status: res.status, headers: rh });
    },
    websocket: {
      open(ws) {
        const d = ws.data;
        d.upstream.addEventListener("message", (ev) => {
          // Forward the original frame unchanged (text stays text, binary stays binary).
          ws.send(ev.data);
          if (S !== d.scenario) return;
          const t = typeof ev.data === "string" ? ev.data : new TextDecoder().decode(ev.data);
          const s = onProviderEvent("ws", d.id, d.attempt, tryJson(t));
          if (s?.type === "response.completed") {
            const f = S.fault;
            if (f?.armed && !f.fired && Array.isArray(s.output) && s.output.length > 0) {
              f.fired = true;
              f.fired_at = now();
              f.dropped_conn = d.id;
              f.after_response_id = s.response_id;
              f.dropped_output = s.output;
              log({ event: "injected_ws_drop", label: INJECTED_DROP_LABEL, injected: true, conn: d.id, after_response_id: s.response_id, output: s.output });
              d.dropped = true;
              try {
                d.upstream.close(1000, INJECTED_DROP_LABEL);
              } catch {}
              if (typeof ws.terminate === "function") ws.terminate();
              else ws.close(1011, INJECTED_DROP_LABEL);
            }
          }
        });
        d.upstream.addEventListener("close", (ev) => {
          if (S === d.scenario) log({ event: "ws_close", dir: "proxy->client", conn: d.id, code: ev.code, injected: Boolean(d.dropped) });
          try {
            ws.close();
          } catch {}
        });
      },
      message(ws, message) {
        const d = ws.data;
        const t = typeof message === "string" ? message : new TextDecoder().decode(message);
        const obj = tryJson(t);
        if (S === d.scenario && obj?.type === "response.create") {
          const attemptKey = `${S.name}:ws:${S.attempts + 1}`;
          if (!admit("ws", attemptKey)) {
            ws.close(1008, LOCAL_STOP_LABEL);
            try {
              d.upstream.close(1000, LOCAL_STOP_LABEL);
            } catch {}
            return;
          }
          d.attempt = attemptKey;
          const summary = summarizeGenBody(obj, new TextEncoder().encode(t));
          S.requests.push({ transport: "ws", conn: d.id, attempt: attemptKey, ts: now(), after_injected_drop: Boolean(S.fault?.fired), headers: S.wsConns.find((c) => c.id === d.id)?.headers ?? null, ...summary });
          log({ event: "ws_frame", dir: "client->proxy", conn: d.id, attempt: attemptKey, request: summary });
        } else if (S === d.scenario) {
          log({ event: "ws_frame", dir: "client->proxy", conn: d.id, frame_type: obj?.type ?? "non-json", bytes: t.length });
        }
        d.upstream.send(message);
      },
      close(ws, code) {
        const d = ws.data;
        if (S === d.scenario) log({ event: "ws_close", dir: "client->proxy", conn: d.id, code, injected: Boolean(d.dropped) });
        try {
          d.upstream.close();
        } catch {}
      },
    },
  });

  return {
    port: server.port,
    url: `http://127.0.0.1:${server.port}`,
    begin(state) {
      S = state;
    },
    setTurn(turn) {
      if (S) S.turn = turn;
    },
    end() {
      S = null;
    },
    stop() {
      server.stop(true);
    },
  };
}

// ---------------------------------------------------------------------------------------------
// cc-lb observation

async function scrapeMetrics(ctx) {
  const u = new URL(ctx.metricsUrl);
  if (u.pathname === "/" || u.pathname === "") u.pathname = "/metrics";
  try {
    const t = await (await fetch(u)).text();
    const series = {};
    for (const line of t.split("\n")) {
      if (!line || line.startsWith("#")) continue;
      const m = line.match(/^([^\s{]+)(\{[^}]*\})?\s+(\S+)/);
      if (!m) continue;
      const [, name, labels = "", value] = m;
      if (!/websocket|responses|request|generation|upstream|usage|cost|limit|quota/i.test(name) && !LOSS_COUNTERS.some((c) => name.includes(c))) continue;
      series[name + labels] = Number(value);
    }
    return { ok: true, series };
  } catch (e) {
    return { ok: false, error: String(e?.name ?? e) };
  }
}

function metricsDelta(before, after) {
  if (!before?.ok || !after?.ok) return { ok: false };
  const delta = {};
  for (const [k, v] of Object.entries(after.series)) {
    const d = v - (before.series[k] ?? 0);
    if (Number.isFinite(d) && d !== 0) delta[k] = d;
  }
  const loss = Object.fromEntries(Object.entries(after.series).filter(([k]) => LOSS_COUNTERS.some((c) => k.includes(c))));
  return { ok: true, nonzero_delta: delta, loss_counters_after: loss };
}

const EVENT_KEYS = /^(request_id|event_id|event_kind|source_kind|status|model|upstream_id|upstream_name|principal_id|api_key_id|key_id|client_key_id|thread_id|observed_session_id|session_id_source|input_tokens|output_tokens|cache_read_input_tokens|cache_creation_input_tokens|thinking_tokens|reasoning_effort|service_tier|cost_[a-z_0-9]*|stream|store|previous_response_id[a-z_]*|upstream_response_id|response_id|path|method|error_code|upstream_error_type|duration_ms|ttfb_ms|attempt[a-z_]*|ts_ms)$/;

// ---------------------------------------------------------------------------------------------
// Codex execution

function codexConfig({ teeUrl, projectReal, websockets }) {
  return [
    `model_provider = "cc_lb_qa"`,
    `model = "${MODEL}"`,
    `model_reasoning_effort = "low"`,
    `approval_policy = "never"`,
    `sandbox_mode = "workspace-write"`,
    `check_for_update_on_startup = false`,
    `cli_auth_credentials_store = "file"`,
    `mcp_oauth_credentials_store = "file"`,
    ``,
    // Real feature names from `codex features list` (0.163): stop the curated/remote plugin
    // clone into CODEX_HOME/.tmp (an extra GitHub fetch whose docs tripped the credential scan).
    `[features]`,
    `plugins = false`,
    `remote_plugin = false`,
    `apps = false`,
    ``,
    `[analytics]`,
    `enabled = false`,
    ``,
    `[sandbox_workspace_write]`,
    `network_access = false`,
    // TMPDIR is the session-private S.tmp; zsh heredocs (used by shell apply_patch) need it writable.
    `exclude_tmpdir_env_var = false`,
    `exclude_slash_tmp = true`,
    ``,
    `[projects."${projectReal}"]`,
    `trust_level = "trusted"`,
    ``,
    `[model_providers.cc_lb_qa]`,
    `name = "cc-lb-qa"`,
    `base_url = "${teeUrl}/v1"`,
    `wire_api = "responses"`,
    `env_key = "CCLB_QA_PROXY_KEY"`,
    `supports_websockets = ${websockets}`,
    `requires_openai_auth = false`,
    `request_max_retries = 0`,
    `stream_max_retries = 1`,
    ``,
  ].join("\n");
}

// Benign-doc filter: only documentation files inside Codex's own plugin clone/cache trees.
// Everything else (auth.json, *.env, secret*, OAuth/token JSON, keyrings, any non-doc file) stays a hit.
const BENIGN_PLUGIN_DOC_RE = /\/\.codex\/(?:\.tmp\/plugins-clone-[^/]+|plugins|\.tmp\/plugins)\/.+\.(?:md|mdx|markdown|txt|rst)$/i;
const REAL_CRED_NAME_RE = /(?:^|\/)(?:auth\.json|[^/]*\.env|\.env[^/]*|[^/]*secret[^/]*|[^/]*credentials?\.json|[^/]*oauth[^/]*\.json|[^/]*token[^/]*\.json|\.netrc|[^/]*keyring[^/]*|[^/]*\.pem|[^/]*\.key)$/i;
async function credentialScan(dirs) {
  const hits = [];
  for (const d of dirs)
    for (const h of await scanCredentialFiles(d)) {
      const p = typeof h === "string" ? h : h?.path ?? "";
      if (BENIGN_PLUGIN_DOC_RE.test(p) && !REAL_CRED_NAME_RE.test(p)) {
        BENIGN_DOC_HITS.add(relative(ROOT, p));
        continue;
      }
      hits.push(typeof h === "string" ? relative(ROOT, h) : h);
    }
  return hits;
}
const BENIGN_DOC_HITS = new Set();

function runFixtureTests(project, scenarioDir, tag) {
  const r = spawnSync("node", ["--test"], { cwd: project, timeout: 30_000, encoding: "utf8", env: { PATH: process.env.PATH, HOME: join(scenarioDir, "home") } });
  const out = `${r.stdout ?? ""}\n${r.stderr ?? ""}`;
  writePrivate(join(scenarioDir, `fixture-tests-${tag}.txt`), redact(out));
  // Node's TAP reporter prints "# tests N"; the spec reporter (default in newer Node) prints "ℹ tests N".
  const num = (k) => Number(out.match(new RegExp(`^(?:# |ℹ )${k} (\\d+)`, "m"))?.[1] ?? NaN);
  const hiddenRun = spawnSync("node", [HIDDEN_CHECK, tag.replace(/\D/g, ""), project], { cwd: dirname(HIDDEN_CHECK), timeout: 30_000, encoding: "utf8", env: { PATH: process.env.PATH, HOME: join(scenarioDir, "home") } });
  const hidden = tryJson((hiddenRun.stdout ?? "").trim().split("\n").pop()) ?? { ok: false, checks: [], error: redact(`${hiddenRun.stderr ?? ""}`).slice(-400) };
  writePrivate(join(scenarioDir, `hidden-check-${tag}.json`), JSON.stringify(hidden, null, 2));
  return { exit_code: r.status, signal: r.signal, tests: num("tests"), pass: num("pass"), fail: num("fail"), hidden };
}

function fixtureDiff(project) {
  const changed = [];
  for (const f of walk(project)) {
    const rel = relative(project, f);
    const orig = join(FIXTURE, rel);
    if (!existsSync(orig)) changed.push({ path: rel, kind: "added", sha12: fileSha(f).slice(0, 12) });
    else if (fileSha(orig) !== fileSha(f)) changed.push({ path: rel, kind: "modified", sha12: fileSha(f).slice(0, 12) });
  }
  return changed;
}

function sessionFiles(codexHome, threadId) {
  return walk(join(codexHome, "sessions"))
    .filter((f) => threadId && f.includes(threadId))
    .map((f) => ({ path: relative(ROOT, f), lines: readFileSync(f, "utf8").split("\n").filter(Boolean).length, sha12: fileSha(f).slice(0, 12) }));
}

function runCodex({ S, args, cwd, tag }) {
  const env = {
    PATH: process.env.PATH,
    HOME: S.home,
    CODEX_HOME: S.codexHome,
    TMPDIR: S.tmp,
    LANG: "en_US.UTF-8",
    USER: process.env.USER ?? "qa",
    TERM: "dumb",
    XDG_CONFIG_HOME: join(S.home, ".config"),
    XDG_DATA_HOME: join(S.home, ".local", "share"),
    XDG_CACHE_HOME: join(S.home, ".cache"),
    XDG_STATE_HOME: join(S.home, ".local", "state"),
    CCLB_QA_PROXY_KEY: S.clientKey,
    NO_COLOR: "1",
  };
  const stdoutPath = join(S.dir, `codex-${tag}.jsonl`);
  const stderrPath = join(S.dir, `codex-${tag}.stderr.txt`);
  writePrivate(stdoutPath, "");
  writePrivate(stderrPath, "");
  return new Promise((resolve) => {
    const started = now();
    const child = spawn(CODEX.path, args, { cwd, env, stdio: ["ignore", "pipe", "pipe"] });
    let timedOut = false;
    let killedBy = null;
    S.killClient = (why) => {
      if (child.exitCode != null || killedBy) return;
      killedBy = why;
      child.kill("SIGTERM");
      setTimeout(() => child.exitCode == null && child.kill("SIGKILL"), 3000);
    };
    const timer = setTimeout(() => {
      timedOut = true;
      S.killClient("driver wall deadline");
    }, TURN_MS);
    let outBuf = "";
    const events = [];
    child.stdout.on("data", (b) => {
      appendFileSync(stdoutPath, redact(b.toString()));
      outBuf += b.toString();
      let i;
      while ((i = outBuf.indexOf("\n")) >= 0) {
        const line = outBuf.slice(0, i);
        outBuf = outBuf.slice(i + 1);
        const j = tryJson(line);
        if (j) events.push(j);
      }
    });
    let errText = "";
    child.stderr.on("data", (b) => {
      appendFileSync(stderrPath, redact(b.toString()));
      errText += b.toString();
    });
    child.on("close", (code, signal) => {
      clearTimeout(timer);
      S.killClient = null;
      const tail = tryJson(outBuf);
      if (tail) events.push(tail);
      resolve({
        tag,
        exit_code: code,
        signal,
        timed_out: timedOut,
        killed_by: killedBy,
        wall_ms: now() - started,
        cwd: relative(ROOT, cwd),
        argv_shape: args.map((a) => (a === PROMPT_TURN1 || a === PROMPT_TURN2 ? "<prompt>" : a)),
        ...summarizeCodexJsonl(events),
        stderr_transport_lines: errText
          .split("\n")
          .filter((l) => /websocket|web socket|fallback|falling back|reconnect|retry|stream disconnected|error/i.test(l))
          .slice(0, 30)
          .map((l) => redact(l).slice(0, 300)),
        stdout_path: relative(ROOT, stdoutPath),
        stderr_path: relative(ROOT, stderrPath),
      });
    });
  });
}

function summarizeCodexJsonl(events) {
  const out = { jsonl_event_counts: {}, thread_ids: [], tool_calls: [], file_changes: [], agent_messages: [], turn_usage: [], errors: [] };
  for (const e of events) {
    out.jsonl_event_counts[e.type] = (out.jsonl_event_counts[e.type] ?? 0) + 1;
    if (e.type === "thread.started" && e.thread_id) out.thread_ids.push(e.thread_id);
    if (e.type === "turn.completed") out.turn_usage.push(e.usage ?? null);
    if (e.type === "turn.failed" || e.type === "error") out.errors.push(redact(JSON.stringify(e.error ?? e.message ?? e)).slice(0, 400));
    if (e.type === "item.completed" && e.item) {
      const it = e.item;
      if (it.type === "command_execution")
        out.tool_calls.push({ type: it.type, command: redact(it.command ?? "").slice(0, 300), exit_code: it.exit_code ?? null, status: it.status ?? null, output_tail: redact(it.aggregated_output ?? "").slice(-400) });
      else if (it.type === "file_change") out.file_changes.push({ status: it.status ?? null, changes: (it.changes ?? []).map((c) => ({ path: c.path, kind: c.kind })) });
      else if (it.type === "agent_message") out.agent_messages.push(redact(it.text ?? "").slice(0, 600));
      else if (it.type === "mcp_tool_call" || it.type === "web_search") out.tool_calls.push({ type: it.type, status: it.status ?? null });
    }
  }
  out.thread_id = out.thread_ids[0] ?? null;
  return out;
}

// ---------------------------------------------------------------------------------------------
// scenario orchestration

const CODEX = { path: null, version: null, sha256: null };

function resolveCodex() {
  const which = spawnSync("which", ["codex"], { encoding: "utf8" });
  const p = which.stdout.trim();
  CODEX.path = realpathSync(p);
  CODEX.version = spawnSync(CODEX.path, ["--version"], { encoding: "utf8" }).stdout.trim();
  CODEX.sha256 = fileSha(CODEX.path);
}

function upstreamHost(ctx) {
  const u = ctx.upstreams?.find((x) => x.id === ctx.upstreamId) ?? ctx.upstreams?.[0] ?? null;
  return u ? { id: u.id, name: u.name, provider: u.provider, kind: u.kind, host: u.host ?? null } : null;
}

async function observedRun(ctx, S, tee, turn, args, cwd) {
  tee.setTurn(turn);
  const label = `codex-${S.name}-turn${turn}`;
  const creds = await credentialScan([S.home]);
  if (creds.length) return { blocked: `credential-like files present in private home before launch: ${JSON.stringify(creds)}` };
  try {
    await assertBudget(ctx);
  } catch (e) {
    if (e.code === "BUDGET_BLOCKED") return { blocked: `server-side budget: ${e.message}` };
    throw e;
  }
  const reqBefore = S.requests.length;
  const respBefore = S.responses.length;
  const refusalsBefore = budget.refusals.length;
  const metricsBefore = await scrapeMetrics(ctx);
  const baseline = await observabilityBaseline(ctx, label);
  const run = await runCodex({ S, args, cwd, tag: `turn${turn}` });
  const turnReqs = S.requests.slice(reqBefore);
  const turnResps = S.responses.slice(respBefore);
  const httpGens = turnReqs.filter((r) => r.transport === "http").length;
  const wsGens = turnReqs.filter((r) => r.transport === "ws").length;
  // WS turns expect events too (contract); absence after the full deadline is the observability finding.
  const waited = await waitForRequestEvents(ctx, { baseline, minCount: Math.max(1, httpGens + wsGens), deadlineMs: EVENT_DEADLINE_MS, label });
  const events = {
    baseline_cursor: baseline.eventCursor,
    cursor_source: baseline.cursorSource,
    final_cursor: waited.finalCursor,
    timed_out: waited.timedOut,
    invalidated: waited.invalidated,
    drop_increases: waited.dropIncreases,
    critical_drop_increases: waited.criticalDropIncreases,
    expected_min: httpGens + wsGens,
    count: waited.events.length,
    events: waited.events.map((ev) => Object.fromEntries(Object.entries(ev).filter(([k]) => EVENT_KEYS.test(k)))),
    event_field_names: [...new Set(waited.events.flatMap((ev) => Object.keys(ev)))].sort(),
  };
  const metricsAfter = await scrapeMetrics(ctx);
  // Collector convention: <evidenceDir>/<label>.stdout/.stderr (already redacted copies).
  for (const [src, ext] of [[run.stdout_path, "stdout"], [run.stderr_path, "stderr"]])
    writePrivate(join(ctx.evidenceDir, `${label}.${ext}`), readFileSync(join(ROOT, src), "utf8"));
  return {
    turn,
    label,
    run,
    fixture_tests: runFixtureTests(cwd, S.dir, `turn${turn}`),
    fixture_changes: fixtureDiff(cwd),
    session_files: sessionFiles(S.codexHome, run.thread_id ?? S.threadId),
    observed_transport: { http_generation_requests: httpGens, ws_generation_frames: wsGens, ws_connections_total: S.wsConns.length },
    requests: turnReqs,
    responses: turnResps,
    local_refusals: budget.refusals.slice(refusalsBefore),
    server_429: turnReqs.filter((q) => q.http_status === 429).map((q) => q.attempt),
    cc_lb_events: events,
    metrics: metricsDelta(metricsBefore, metricsAfter),
    post_scan_credential_names: await credentialScan([S.home]),
  };
}

// Budget refusals/kills and cc-lb limit 429s are QA-setup bounds: BLOCKED, never client/proxy evidence.
function budgetBlock(t) {
  if (t.local_refusals.length || String(t.run.killed_by ?? "").startsWith(LOCAL_STOP_LABEL))
    return { verdict: "BLOCKED", reasons: [`BLOCKED-budget: tee refused/killed at slice attempts ${budget.attempts}, driver usd ${budget.usd.toFixed(4)}`], budget_stop: t.local_refusals };
  if (t.server_429.length) return { verdict: "BLOCKED", reasons: [`BLOCKED-budget: cc-lb principal limit 429 on ${t.server_429.join(",")} (QA setup bound; parent assesses)`] };
  if (t.cc_lb_events.invalidated) return { verdict: "BLOCKED", reasons: [`observability invalidated: drop counters increased ${JSON.stringify(t.cc_lb_events.drop_increases)}`] };
  // Provider rejected the model for this credential on every attempt: access prerequisite, not client/proxy evidence.
  const anyCompleted = t.responses.some((x) => x.type === "response.completed");
  const wsModelMissing = t.responses.filter((x) => x.error?.code === "model_not_found").length;
  const http404 = t.requests.filter((q) => q.transport === "http" && q.http_status === 404).length;
  const upstream404Events = t.cc_lb_events.events.filter((e) => e.status === 404 && e.upstream_id).length;
  if (!anyCompleted && t.requests.length > 0 && wsModelMissing + http404 === t.requests.length)
    return { verdict: "BLOCKED", reasons: [`BLOCKED-provider-access: every attempt rejected by provider for model ${MODEL} (ws model_not_found ${wsModelMissing}, http 404 ${http404}, cc-lb upstream 404 events ${upstream404Events})`] };
  return null;
}

function judgeTurn(t, transport, { expectResume, priorThread, minTests }) {
  const blocked = budgetBlock(t);
  if (blocked) return blocked;
  const reasons = [];
  const r = t.run;
  if (r.exit_code !== 0) reasons.push(`codex exit ${r.exit_code}${r.signal ? ` signal ${r.signal}` : ""}${r.killed_by ? ` killed: ${r.killed_by}` : ""}`);
  if (!r.tool_calls.some((c) => c.type === "command_execution")) reasons.push("no actual shell tool call observed");
  if (t.fixture_tests.exit_code !== 0 || !(t.fixture_tests.fail === 0)) reasons.push(`fixture tests not passing (${JSON.stringify(t.fixture_tests)})`);
  if (minTests && !(t.fixture_tests.tests >= minTests)) reasons.push(`expected >=${minTests} fixture tests, saw ${t.fixture_tests.tests}`);
  if (!t.fixture_tests.hidden?.ok)
    reasons.push(`hidden behavior check failed: ${(t.fixture_tests.hidden?.checks ?? []).filter((c) => !c.ok).map((c) => `${c.name} (${c.detail})`).join("; ") || t.fixture_tests.hidden?.error || "no result"}`);
  if (t.turn === 2 && !t.fixture_changes.some((c) => c.path === "test/cart.test.js")) reasons.push("turn 2 did not modify test/cart.test.js");
  if (transport === "http" && (t.observed_transport.ws_generation_frames > 0 || t.observed_transport.http_generation_requests === 0))
    reasons.push(`expected HTTP only, observed ${JSON.stringify(t.observed_transport)}`);
  if (transport === "ws" && (t.observed_transport.http_generation_requests > 0 || t.observed_transport.ws_generation_frames === 0))
    reasons.push(`expected WS only (no silent HTTP fallback), observed ${JSON.stringify(t.observed_transport)}`);
  // Codex WS sends a generate:false prewarm `response.create`; the provider completes it with
  // input usage and 0 output. That is a control completion, judged separately: input>0, no output.
  // Every real generation must still report nonzero input AND output.
  const prewarmAttempts = new Set(t.requests.filter((q) => q.generate === false).map((q) => q.attempt));
  const completed = t.responses.filter((x) => x.type === "response.completed");
  const realCompleted = completed.filter((x) => !prewarmAttempts.has(x.attempt));
  const prewarmCompleted = completed.filter((x) => prewarmAttempts.has(x.attempt));
  t.prewarm = { requests: prewarmAttempts.size, completed: prewarmCompleted.map((x) => ({ attempt: x.attempt, input: x.usage?.input_tokens ?? null, output: x.usage?.output_tokens ?? null, output_items: x.output?.length ?? null })) };
  if (!realCompleted.length) reasons.push("no real (generate!=false) provider response.completed observed");
  if (completed.some((x) => !/^resp_/.test(x.response_id ?? ""))) reasons.push("response id not provider-issued resp_*");
  if (realCompleted.some((x) => !(x.usage?.input_tokens > 0 && x.usage?.output_tokens > 0))) reasons.push("real generation completed without nonzero input+output usage");
  if (prewarmCompleted.some((x) => !(x.usage?.input_tokens > 0) || (x.output?.length ?? 0) > 0)) reasons.push("generate:false prewarm completion lacks input usage or produced output");
  if (t.responses.some((x) => x.type !== "response.completed")) reasons.push(`non-completed provider terminal events: ${t.responses.filter((x) => x.type !== "response.completed").map((x) => x.type).join(",")}`);
  if (expectResume) {
    if (r.thread_id && r.thread_id !== priorThread) reasons.push("resumed thread id differs from turn1 thread id");
    if (!t.requests.some((q) => q.function_call_ids.length > 0 || q.previous_response_id_present)) reasons.push("resumed turn request carried neither prior tool history nor previous_response_id");
  }
  if (transport === "http" && t.cc_lb_events?.count < t.observed_transport.http_generation_requests)
    reasons.push(`cc-lb committed events ${t.cc_lb_events?.count} < tee HTTP generations ${t.observed_transport.http_generation_requests}`);
  return { verdict: reasons.length ? "FAIL" : "PASS", reasons };
}

// ---------------------------------------------------------------------------------------------
// ActualEvidenceCollector view (coverage.json driver_manifest)

const SCENARIO_IDS = { http: "codex.http_stateless_tools", ws: "codex.ws_tools", ws_reconnect: "codex.ws_reconnect_replay" };

function toolKind(cmd) {
  if (/node\s+--test|npm\s+(run\s+)?test/.test(cmd)) return "exec";
  if (/apply_patch|>\s*\S|sed\s+-i|perl\s+-pi|tee\s/.test(cmd)) return "edit";
  if (/\b(cat|sed\s+-n|head|tail|ls|rg|grep|nl|find|wc)\b/.test(cmd)) return "read";
  return "exec";
}

function collectorView({ scenarios, pricing, ctx }) {
  const runs = [];
  const blocked = [];
  const cache = [];
  for (const s of scenarios) {
    const sid = SCENARIO_IDS[s.scenario];
    if (s.verdict === "BLOCKED" || s.verdict === "NOT_RUN")
      blocked.push({ scenarioId: sid, kind: (s.reasons ?? []).some((r) => /budget/i.test(r)) ? "budget" : "capability", prerequisite: (s.reasons ?? []).join("; "), ref: s.tee_wire_path ?? null });
    if (s.resume_verdict?.verdict === "BLOCKED")
      blocked.push({ scenarioId: "codex.session_resume", kind: s.resume_verdict.reasons.some((r) => /budget/i.test(r)) ? "budget" : "capability", prerequisite: s.resume_verdict.reasons.join("; "), ref: s.tee_wire_path ?? null });
    for (const t of s.turns ?? []) {
      const ids = [sid, ...(t.turn === 2 ? ["codex.session_resume"] : [])];
      const wire = t.requests.map((q, i) => {
        const resp = t.responses.find((x) => x.attempt === q.attempt && x.type === "response.completed") ?? t.responses.find((x) => x.attempt === q.attempt);
        const h = q.headers ?? {};
        return {
          source: "proxy_wire_capture",
          transport: q.transport === "ws" ? "websocket" : "http",
          connectionId: q.conn ?? null,
          fullReplay: !q.previous_response_id_present && q.has_user_message,
          prewarmControl: q.generate === false,
          generation: i + 1,
          attempt: q.attempt,
          apiFamily: "openai-responses",
          endpoint: "/v1/responses",
          stream: q.stream,
          store: q.store,
          previousResponseIdPresent: q.previous_response_id_present,
          sessionTurnHeaderPresent: Boolean(h.x_codex_turn_state_present || h.session_id_present),
          httpStatus: q.transport === "ws" ? 101 : q.http_status ?? null,
          providerResponseIdPresent: /^resp_/.test(resp?.response_id ?? ""),
          providerResponseIdSha256: resp?.response_id ? sha(resp.response_id) : null,
          providerTerminalEvent: resp?.type ?? null,
          providerUsage: resp?.usage ? { input: resp.usage.input_tokens, cached: resp.usage.cached_tokens, output: resp.usage.output_tokens, reasoning: resp.usage.reasoning_tokens } : null,
          promptCacheKeySha12: q.prompt_cache_key_sha12,
          afterInjectedDrop: Boolean(q.after_injected_drop),
          forwarded: true,
          ref: s.tee_wire_path,
        };
      });
      for (const r of t.local_refusals) wire.push({ source: "proxy_wire_capture", transport: r.transport === "ws" ? "websocket" : "http", forwarded: false, injected: true, label: r.label, reason: r.reason, ref: s.tee_wire_path });
      const driverCostUsd = t.responses.reduce((n, x) => n + (usageCost(pricing, x.usage) ?? 0), 0);
      const run = {
        label: t.label,
        scenarioIds: ids,
        kind: "cli",
        sessionId: t.run.thread_id ?? s.client_session_id ?? null,
        resumeOfSessionId: t.turn === 2 ? s.client_session_id : null,
        resumeSelector: t.turn === 2 ? "exact_id" : null,
        turn: t.turn,
        exitCode: t.run.exit_code,
        timedOut: t.run.timed_out,
        wallSeconds: t.run.wall_ms / 1000,
        toolCalls: [
          ...t.run.tool_calls.map((c) => ({ name: c.type === "command_execution" ? "shell" : c.type, command: c.command ?? null, kind: c.type === "command_execution" ? toolKind(c.command ?? "") : "exec", ok: c.status === "completed" && (c.exit_code ?? 0) === 0 })),
          ...t.run.file_changes.map((c) => ({ name: "apply_patch", kind: "edit", ok: c.status === "completed", paths: c.changes.map((x) => x.path) })),
        ],
        fixtureTest: { exitCode: t.fixture_tests.exit_code, tests: t.fixture_tests.tests, pass: t.fixture_tests.pass, fail: t.fixture_tests.fail, outputRef: relative(ROOT, join(RUN_DIR, s.scenario, `fixture-tests-turn${t.turn}.txt`)) },
        wire,
        ccLbEvents: { count: t.cc_lb_events.count, timedOut: t.cc_lb_events.timed_out, invalidated: t.cc_lb_events.invalidated, ref: `evidence/events-${t.label}.jsonl` },
        metrics: { before: t.label, after: `${t.label}-after` },
        driverCostUsd: Number(driverCostUsd.toFixed(6)),
        driverCostLabel: pricing.label,
      };
      if (s.supports_websockets_config)
        run.budgetTap = { enforced: true, generationFrames: t.observed_transport.ws_generation_frames, attemptsCap: SLICE_ATTEMPTS, usdCap: SLICE_USD, deadlineSecs: TURN_MS / 1000, refusals: t.local_refusals.length };
      runs.push(run);
    }
    if (s.cache && s.turns?.length >= 2) {
      const first = s.turns[0].responses.find((x) => x.type === "response.completed" && x.usage);
      cache.push({
        scenarioId: "x.cache_usage_cost.openai",
        provider: "openai",
        sourceScenario: sid,
        thresholdTokens: s.cache.provider_cache_threshold_tokens,
        prefixTokensEstimate: first?.usage?.input_tokens ?? null,
        unchangedPrefix: s.cache.instructions_stable && s.cache.client_prompt_cache_key_stable,
        runLabels: [s.turns[0].label, s.turns[s.turns.length - 1].label],
        verdict: s.cache.verdict,
      });
    }
  }
  return {
    client: { name: "codex", version: CODEX.version, binary: CODEX.path, sha256: CODEX.sha256 },
    configGuard: {
      fakeOrDirectFallback: false,
      providerBaseUrlIsProxy: true,
      providerBaseUrlPath: "codex base_url -> owned plaintext tee (forward/refuse only) -> cc-lb proxy",
      realAuthCopied: false,
      credentialFileNamesScanned: true,
      credentialFileHits: scenarios.flatMap((s) => (s.turns ?? []).flatMap((t) => t.post_scan_credential_names)),
      benignPluginDocHitsExcluded: [...BENIGN_DOC_HITS],
      benignFilter: "only .md/.mdx/.markdown/.txt/.rst under .codex/{.tmp/plugins-clone-*,plugins,.tmp/plugins}/ and not auth.json/*.env/secret/credentials/oauth|token json/netrc/keyring/pem/key",
    },
    sharedPrincipalsUnmodified: true,
    trace: { otlpConfigured: Boolean(ctx.trace?.otlpConfigured), traceIds: ctx.trace?.traceIds ?? [] },
    runs,
    blocked,
    cache,
  };
}

function cacheEvidence(S) {
  const keys = new Set(S.requests.map((q) => q.prompt_cache_key_sha12).filter(Boolean));
  const completed = S.responses.filter((x) => x.type === "response.completed" && x.usage);
  return {
    upstream_forwarding: "NOT_OBSERVABLE (no capture past cc-lb)",
    client_prompt_cache_key_present_all: S.requests.length > 0 && S.requests.every((q) => q.prompt_cache_key_present),
    client_prompt_cache_key_distinct_sha12: [...keys],
    client_prompt_cache_key_stable: keys.size === 1,
    instructions_stable: new Set(S.requests.map((q) => q.instructions_sha12)).size === 1,
    provider_cache_threshold_tokens: 1024,
    cached_tokens_by_response: completed.map((x) => ({ response_id: x.response_id, input: x.usage.input_tokens, cached: x.usage.cached_tokens })),
    later_request_hit: completed.slice(1).some((x) => x.usage.cached_tokens > 0),
    verdict: completed.slice(1).some((x) => x.usage.cached_tokens > 0)
      ? "PASS (cached_tokens>0 on later same-session request)"
      : completed.length > 1
        ? "MISS_OBSERVED (not a defect)"
        : "NOT_APPLICABLE (fewer than two completed responses)",
  };
}

function judgeReconnect(S, t1) {
  const f = S.fault;
  const blocked = budgetBlock(t1);
  if (blocked) return { ...blocked, fault: f };
  if (!f.fired) return { verdict: "BLOCKED", reasons: ["injected drop never fired: no response.completed with output on WS"], fault: f };
  const droppedIds = new Set(S.responses.filter((x) => x.conn === f.dropped_conn).map((x) => x.response_id).filter(Boolean));
  const next = S.requests.find((q) => q.ts > f.fired_at);
  if (!next) return { verdict: "BLOCKED", reasons: ["client made no generation request after injected drop"], fault: f, turn_exit: t1.run.exit_code };
  const callIds = (f.dropped_output ?? []).filter((o) => o.call_id_sha12).map((o) => o.call_id_sha12);
  const fullReplay =
    next.has_user_message &&
    callIds.every((c) => next.function_call_ids.includes(c)) &&
    callIds.every((c) => next.function_call_output_ids.includes(c));
  const stalePrev = next.previous_response_id_present && droppedIds.has(next.previous_response_id);
  const newConn = next.transport === "ws" && next.conn !== f.dropped_conn;
  const after = S.responses.filter((x) => x.attempt === next.attempt);
  const realAfter = after.find((x) => x.type === "response.completed" && /^resp_/.test(x.response_id ?? "") && x.usage?.output_tokens > 0);
  const reasons = [];
  if (!newConn) reasons.push(next.transport === "http" ? "client fell back to HTTP after WS drop (layer: client transport)" : "next request reused dropped connection");
  if (stalePrev) reasons.push("next request carried previous_response_id from the dropped connection");
  if (!stalePrev && !fullReplay) reasons.push("next request lacks full replay (user message + dropped function_call + its output)");
  if (!realAfter) reasons.push(`no real provider completion after reconnect (${after.map((x) => x.type + (x.error ? JSON.stringify(x.error) : "")).join(",") || "none"})`);
  if (t1.run.exit_code !== 0) reasons.push(`codex exit ${t1.run.exit_code}`);
  return {
    verdict: reasons.length ? "FAIL" : "PASS",
    reasons,
    label: INJECTED_DROP_LABEL,
    fault: { dropped_conn: f.dropped_conn, after_response_id: f.after_response_id, dropped_output: f.dropped_output },
    next_request: {
      transport: next.transport,
      conn: next.conn ?? null,
      new_connection: newConn,
      previous_response_id_present: next.previous_response_id_present,
      previous_response_id_from_dropped_conn: stalePrev,
      input_count: next.input_count,
      input_items: next.input_items,
      full_replay: fullReplay,
      store: next.store,
    },
    post_reconnect_provider: after,
  };
}

async function runScenario(ctx, tee, name) {
  const websockets = name !== "http";
  const dir = join(RUN_DIR, name);
  const S = {
    name,
    dir,
    turn: 0,
    home: join(dir, "home"),
    codexHome: join(dir, "home", ".codex"),
    tmp: join(dir, "tmp"),
    project: join(dir, "project"),
    wirePath: join(dir, "tee-wire.jsonl"),
    clientKey: ctx.__clientKeyValue,
    attempts: 0,
    attemptCap: SCENARIO_ATTEMPT_CAP[name],
    wire: [],
    requests: [],
    responses: [],
    wsConns: [],
    eventTypes: {},
    fault: name === "ws_reconnect" ? { armed: true, fired: false, label: INJECTED_DROP_LABEL, rule: "drop WS (terminate) right after first response.completed with non-empty output" } : null,
    killClient: null,
  };
  const remaining = SLICE_ATTEMPTS - budget.attempts;
  // Start only when the whole per-session cap fits, so a session is never cut mid-loop by the slice cap.
  if (remaining < Math.max(MIN_REMAINING_TO_START, S.attemptCap) || budget.usd >= SLICE_USD)
    return { scenario: name, verdict: "BLOCKED", reasons: [`BLOCKED-budget before start: remaining slice attempts ${remaining} < session cap ${S.attemptCap}, gate usd ${budget.usd.toFixed(4)}`] };

  for (const d of [S.codexHome, S.tmp]) mkdirSync(d, { recursive: true, mode: 0o700 });
  cpSync(FIXTURE, S.project, { recursive: true });
  const projectReal = realpathSync(S.project);
  writePrivate(join(S.codexHome, "config.toml"), codexConfig({ teeUrl: tee.url, projectReal, websockets }));
  await recordResource(ctx, { kind: "scratch-dir", id: dir, path: dir, delete: `rm -rf '${dir}'`, note: `codex ${name} private home + fixture copy` });

  const preScan = await credentialScan([S.home]);
  if (preScan.length) return { scenario: name, verdict: "BLOCKED", reasons: [`credential-like names in fresh home: ${JSON.stringify(preScan)}`] };

  tee.begin(S);
  const result = { scenario: name, supports_websockets_config: websockets, project: relative(ROOT, projectReal), turns: [] };
  try {
    const t1 = await observedRun(ctx, S, tee, 1, ["exec", "--json", "--skip-git-repo-check", "-C", projectReal, PROMPT_TURN1], projectReal);
    if (t1.blocked) return { ...result, verdict: "BLOCKED", reasons: [t1.blocked] };
    result.turns.push(t1);
    S.threadId = t1.run.thread_id;
    result.client_session_id = S.threadId;

    if (name === "ws_reconnect") {
      result.reconnect = judgeReconnect(S, t1);
      result.verdict = result.reconnect.verdict;
      result.reasons = result.reconnect.reasons;
    } else {
      const transport = websockets ? "ws" : "http";
      const j1 = judgeTurn(t1, transport, { minTests: MIN_TESTS[1] });
      result.turn1_verdict = j1;
      if (!S.threadId) {
        result.resume_verdict = { verdict: "BLOCKED", reasons: ["turn1 JSONL emitted no thread.started thread_id; refusing --last"] };
      } else if (SLICE_ATTEMPTS - budget.attempts < MIN_REMAINING_FOR_RESUME || S.attemptCap - S.attempts < MIN_REMAINING_FOR_RESUME || budget.usd >= SLICE_USD) {
        result.resume_verdict = { verdict: "BLOCKED", reasons: ["paid bound exhausted before resume"] };
      } else {
        const sessBefore = sessionFiles(S.codexHome, S.threadId);
        const t2 = await observedRun(ctx, S, tee, 2, ["exec", "resume", "--json", "--skip-git-repo-check", S.threadId, PROMPT_TURN2], projectReal);
        if (t2.blocked) result.resume_verdict = { verdict: "BLOCKED", reasons: [t2.blocked] };
        else {
          result.turns.push(t2);
          const j2 = judgeTurn(t2, transport, { expectResume: true, priorThread: S.threadId, minTests: MIN_TESTS[2] });
          const sessAfter = t2.session_files;
          const appended = sessBefore.length === 1 && sessAfter.length === 1 && sessAfter[0].path === sessBefore[0].path && sessAfter[0].lines > sessBefore[0].lines;
          if (j2.verdict !== "BLOCKED") {
            if (!appended) j2.reasons.push(`session rollout not appended in place (before ${JSON.stringify(sessBefore)}, after ${JSON.stringify(sessAfter)})`);
            j2.verdict = j2.reasons.length ? "FAIL" : "PASS";
          }
          result.resume_verdict = j2;
        }
      }
      const verdicts = [j1.verdict, result.resume_verdict.verdict];
      result.verdict = verdicts.includes("FAIL") ? "FAIL" : verdicts.includes("BLOCKED") ? "BLOCKED" : "PASS";
      if (websockets) result.ws_observability_note = "cc-lb WS relay records no usage/events/limits; see slice finding ws-observability";
    }
    result.cache = cacheEvidence(S);
    result.tee_event_type_counts = S.eventTypes;
    result.attempts_used = S.attempts;
    result.tee_wire_path = relative(ROOT, S.wirePath);
    return result;
  } finally {
    tee.end();
  }
}

// ---------------------------------------------------------------------------------------------
// main

async function main() {
  mkdirSync(RUN_DIR, { recursive: true, mode: 0o700 });
  const binarySha = existsSync(ARTIFACT.path) ? fileSha(ARTIFACT.path) : null;
  resolveCodex();
  const base = {
    slice: "codex",
    run_id: RUN_ID,
    binary_sha256: binarySha,
    binary_sha256_expected: ARTIFACT.expected_sha256,
    client: { binary: CODEX.path, version: CODEX.version, sha256: CODEX.sha256 },
    model: MODEL,
    bounds: { attempts: SLICE_ATTEMPTS, attempts_source: ATTEMPT_CAP_OVERRIDE != null ? "CODEX_QA_ATTEMPT_CAP (parent-approved revision)" : "contract paid_bounds", contract_attempts: BOUNDS.generation_attempts_per_slice, usd: SLICE_USD, wall_seconds_per_run: TURN_MS / 1000, scenario_caps: SCENARIO_ATTEMPT_CAP },
    max_output_tokens: "client-limited: Codex 0.163 exposes no max_output_tokens config; not set, wire value recorded per request",
  };
  if (binarySha !== ARTIFACT.expected_sha256) {
    const out = { ...base, verdict: "BLOCKED", reasons: ["target binary sha mismatch/missing"], scenarios: SCENARIOS.map((s) => ({ scenario: s, verdict: "NOT_RUN" })) };
    writePrivate(join(RUN_DIR, "summary.json"), JSON.stringify(out, null, 2));
    console.log(JSON.stringify({ verdict: out.verdict, reasons: out.reasons, summary: relative(ROOT, join(RUN_DIR, "summary.json")) }));
    return;
  }

  const ctx = await prepareSlice({
    name: "codex",
    ports: PORTS,
    provider: "openai",
    model: MODEL,
    resume: process.env.QA_RESUME === "1",
    ...(ATTEMPT_CAP_OVERRIDE != null ? { limitOverrides: { maxGenerationAttempts: ATTEMPT_CAP_OVERRIDE } } : {}),
  });
  if (ctx.binarySha256 && ctx.binarySha256 !== ARTIFACT.expected_sha256) throw new Error("runtime binary sha mismatch");
  ctx.__clientKeyValue = ctx.clientKeyPath ? readFileSync(ctx.clientKeyPath, "utf8").trim() : ctx.clientKey;
  if (ctx.clientKeyPath && (statSync(ctx.clientKeyPath).mode & 0o077) !== 0) throw new Error("client key file is not 0600");
  if (!ctx.__clientKeyValue) throw new Error("prepareSlice returned no client key");
  SECRET_VALUES = [ctx.__clientKeyValue];
  const pricing = loadPricing(ctx);
  if (!pricing.ok || ctx.blocked?.length) {
    const reasons = [...(ctx.blocked ?? []), pricing.ok ? null : `cost gate: ${pricing.reason}`].filter(Boolean);
    const out = { ...base, verdict: "BLOCKED", reasons, scenarios: SCENARIOS.map((s) => ({ scenario: s, verdict: "NOT_RUN" })) };
    writePrivate(join(RUN_DIR, "summary.json"), JSON.stringify(out, null, 2));
    await finishEvidence(ctx, out);
    console.log(JSON.stringify({ verdict: out.verdict, reasons, summary: relative(ROOT, join(RUN_DIR, "summary.json")) }));
    return;
  }

  await seedBudget(ctx);
  const proxyOrigin = new URL(ctx.proxyUrl).origin;
  const tee = startTee(proxyOrigin, pricing);
  await recordResource(ctx, { kind: "in-process-listener", id: `codex-tee-127.0.0.1:${tee.port}-pid${process.pid}`, pid: process.pid, port: tee.port, delete: `kill ${process.pid} (listener ends with driver process)` });
  await recordResource(ctx, { kind: "scratch-dir", id: RUN_DIR, path: RUN_DIR, delete: `rm -rf '${RUN_DIR}'`, note: "codex run evidence" });

  const upstream = upstreamHost(ctx);
  const scenarios = [];
  try {
    for (const name of SCENARIOS) {
      try {
        scenarios.push(await runScenario(ctx, tee, name));
      } catch (e) {
        scenarios.push({ scenario: name, verdict: "FAIL", reasons: [`driver error: ${redact(String(e?.stack ?? e)).slice(0, 600)}`], layer: "driver" });
      }
    }
  } finally {
    tee.stop();
  }

  const wsRuns = scenarios.filter((s) => s.supports_websockets_config && s.turns?.length);
  const wsFrames = wsRuns.flatMap((s) => s.turns).reduce((n, t) => n + t.observed_transport.ws_generation_frames, 0);
  const wsEvents = wsRuns.flatMap((s) => s.turns).reduce((n, t) => n + (t.cc_lb_events?.count ?? 0), 0);
  const findings = [];
  if (wsFrames > 0)
    findings.push({
      id: "ws-observability",
      verdict: wsEvents === 0 ? "FAIL" : "INFO",
      layer: "cc-lb observability/limits",
      evidence: { tee_ws_generation_frames: wsFrames, cc_lb_committed_events_delta: wsEvents, source: "crates/cc-lb-server/src/app.rs relay_responses_websocket (opaque frame pump, no usage/event/limit hook)" },
      note: "Fix is architectural; requires approval. Not double-counted per scenario.",
    });

  const providerIds = scenarios.flatMap((s) => (s.turns ?? []).flatMap((t) => t.responses.map((r) => r.response_id))).filter(Boolean);
  const verdicts = scenarios.map((s) => s.verdict);
  const summary = {
    ...base,
    principal_id: ctx.principalId,
    key_id: ctx.keyId ?? ctx.clientKeyId ?? null,
    upstream_id: ctx.upstreamId,
    real_provider_host: upstream,
    api_family_and_endpoint: "OpenAI Responses /v1/responses (HTTP SSE POST and WS response.create)",
    pricing: { ...pricing, note: "driver-computed USD only; cc_lb_tokens_and_cost comes from committed events cost_* fields when present" },
    budget: {
      attempts_used: budget.attempts,
      driver_gate_usd: Number(budget.usd.toFixed(6)),
      driver_usage_priced_usd: Number(budget.usageUsd.toFixed(6)),
      driver_unpriced_reserve_usd: Number(budget.reserveUsd.toFixed(6)),
      driver_legacy_unsplit_gate_usd: Number(budget.legacyUsd.toFixed(6)),
      reserve_note: "reserve = conservative gate charge for attempts without provider usage (e.g. prior gpt-5.1-codex model_not_found 404s); not billed provider cost",
      seed: budget.seed,
      resumed_runtime: process.env.QA_RESUME === "1",
      refusals: budget.refusals,
      cc_lb_budget_status: await budgetStatus(ctx).catch((e) => ({ error: redact(String(e?.message ?? e)) })),
    },
    provider_response_ids: providerIds,
    scenarios,
    findings,
    ...collectorView({ scenarios, pricing, ctx }),
    verdict: verdicts.every((v) => v === "PASS") && !findings.some((f) => f.verdict === "FAIL") ? "PASS" : verdicts.includes("FAIL") || findings.some((f) => f.verdict === "FAIL") ? "FAIL" : "PARTIAL",
  };
  writePrivate(join(RUN_DIR, "summary.json"), redact(JSON.stringify(summary, null, 2)));
  await finishEvidence(ctx, summary);
  writePrivate(join(SLICE_DIR, "runs", "latest.json"), JSON.stringify({ run_id: RUN_ID, driver_summary: relative(ROOT, join(RUN_DIR, "summary.json")), runtime_summary: "codex/summary.json", verdict: summary.verdict }, null, 2));
  console.log(JSON.stringify({ verdict: summary.verdict, scenarios: scenarios.map((s) => ({ s: s.scenario, v: s.verdict })), attempts: budget.attempts, usage_usd: summary.budget.driver_usage_priced_usd, reserve_usd: summary.budget.driver_unpriced_reserve_usd, summary: relative(ROOT, join(RUN_DIR, "summary.json")) }));
}

main().catch((e) => {
  console.error(redact(String(e?.stack ?? e)));
  process.exit(1);
});
