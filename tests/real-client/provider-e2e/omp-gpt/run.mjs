#!/usr/bin/env node
// omp-gpt slice driver: actual OMP CLI -> local observation tap -> cc-lb -> real OpenAI.
// Scenarios: stateless tools (+ session resume), stateful store:true / previous_response_id
// (PI_OPENAI_STATEFUL=1, + session resume). Nothing in the client wire payload is rewritten;
// the tap forwards bytes unchanged and records redacted request/response shape only.
import { spawn, spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import {
  chmodSync,
  cpSync,
  createWriteStream,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  realpathSync,
  statSync,
  writeFileSync,
} from "node:fs";
import http from "node:http";
import net from "node:net";
import { delimiter, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import zlib from "node:zlib";

import {
  admin,
  assertBudget,
  clientEnvBase,
  finishEvidence,
  observabilityBaseline,
  prepareSlice,
  recordResource,
  waitForRequestEvents,
} from "../qa-runtime.mjs";
import { buildCacheProof, loadCatalogPrice } from "./cache-proof.mjs";

const SLICE = "omp-gpt";
const SLICE_DIR = dirname(fileURLToPath(import.meta.url));
const FIXTURE_DIR = join(SLICE_DIR, "fixture");
const EVIDENCE_DIR = join(SLICE_DIR, "evidence");
const WORK_DIR = join(SLICE_DIR, "work");
const PORTS = [18510, 18511, 18512];
const PROVIDER = "openai";
const EXPECTED_MODEL = "gpt-5.2";
const EXPECTED_PROVIDER_HOST = "api.openai.com";
const CLIENT_PROVIDER_ID = "cclb";
const BINARY_PATH = resolve(SLICE_DIR, "../../../../target/debug/cc-lb");
const EXPECTED_BINARY_SHA256 = "dcde4343277cfc977bdfa77df768ea475d990d86058a879264fc58bb2a4e71a4";
const EXPECTED_OMP_VERSION_PREFIX = "18.12.5";

// paid_bounds from contract.json
const MAX_GENERATION_ATTEMPTS = 16;
const RUN_WALL_MS = 120_000;
const OMP_MAX_TIME_SECONDS = 115;
const MAX_OUTPUT_TOKENS = 2000;
const USD_CEILING_SLICE = 3;
const EVENT_DEADLINE_MS = 30_000;
const RESPONSE_PARSE_CAP = 8 * 1024 * 1024;
const CREDENTIAL_NAME_RE = /(auth|oauth|credential|token|keyring|keychain|secret|\.pem$|\.p12$|api[-_]?key|\.key$)/i;
const SESSION_HEADER_RE = /(session|turn|conversation|thread|affinity|request-id|client-request)/i;

// Generation budget: 16 attempts per slice across 4 CLI runs. Turn 1 targets <=4 generations
// (parallel reads, one edit, one test run, answer); turn 2 targets <=2 (one test run, answer).
const TURN1_PROMPT = [
  "You are working in a tiny JavaScript project in the current directory. Be economical with tool calls.",
  "In a single step, use the read tool on src/stats.js and test/stats.test.js (README.md states the contract:",
  "mean([]) must throw RangeError('mean of empty list')).",
  "Then make the smallest edit to src/stats.js that satisfies it, run `node --test` once with the bash tool,",
  "and reply with a one-sentence summary. Do not create new files.",
].join(" ");

const TURN2_PROMPT = [
  "Continuing this session: do not edit anything. Run `node --test` once with the bash tool,",
  "then explain in two sentences what you changed in the previous turn and why the tests now pass.",
].join(" ");

const BEHAVIOR_CHECK = [
  "import { mean, sum } from './src/stats.js';",
  "const out = { sumEmpty: sum([]), meanOk: mean([2, 4, 6]) };",
  "try { mean([]); out.meanEmpty = 'no-throw'; } catch (e) { out.meanEmpty = e.name + ':' + e.message; }",
  "console.log(JSON.stringify(out));",
].join("\n");

const RUNS = [
  { id: "stateless-t1", scenario: "stateless", turn: 1, stateful: false, prompt: TURN1_PROMPT },
  { id: "stateless-t2", scenario: "stateless", turn: 2, stateful: false, prompt: TURN2_PROMPT },
  { id: "stateful-t1", scenario: "stateful", turn: 1, stateful: true, prompt: TURN1_PROMPT },
  { id: "stateful-t2", scenario: "stateful", turn: 2, stateful: true, prompt: TURN2_PROMPT },
];

const nowIso = () => new Date().toISOString();
const sha256Hex = (data) => createHash("sha256").update(data).digest("hex");
const shortHash = (value) => sha256Hex(String(value)).slice(0, 12);
const truncate = (value, max) => (typeof value === "string" && value.length > max ? `${value.slice(0, max)}…` : value);

function ensureDir(path, mode) {
  mkdirSync(path, { recursive: true, mode });
  if (mode !== undefined) chmodSync(path, mode);
}

function writeJson(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function parseBody(body) {
  if (body === undefined || body === null) return null;
  if (typeof body === "string") {
    try {
      return JSON.parse(body);
    } catch {
      return { unparsed_text: truncate(body, 500) };
    }
  }
  if (Buffer.isBuffer(body)) return parseBody(body.toString("utf8"));
  return body;
}

function decodeBody(buffer, encoding) {
  const enc = String(encoding ?? "").trim().toLowerCase();
  if (!enc || enc === "identity") return buffer;
  if (enc === "gzip" || enc === "x-gzip") return zlib.gunzipSync(buffer);
  if (enc === "deflate") return zlib.inflateSync(buffer);
  if (enc === "br") return zlib.brotliDecompressSync(buffer);
  if (enc === "zstd") return zlib.zstdDecompressSync(buffer);
  throw new Error(`unsupported content-encoding ${enc}`);
}

function findExecutable(name) {
  for (const dir of String(process.env.PATH ?? "").split(delimiter)) {
    if (!dir) continue;
    const candidate = join(dir, name);
    try {
      if (statSync(candidate).isFile()) return candidate;
    } catch {
      // continue
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// Observation tap (local, owned, pass-through; no payload rewriting)
// ---------------------------------------------------------------------------

function cumulativeItemHashes(items) {
  const hash = createHash("sha256");
  return items.map((item) => {
    hash.update(JSON.stringify(item));
    hash.update("\n");
    return hash.copy().digest("hex").slice(0, 12);
  });
}

function summarizeRequest(req, rawBody) {
  const headerNames = Object.keys(req.headers).map((name) => name.toLowerCase()).sort();
  const summary = {
    method: req.method,
    path: req.url,
    header_names: headerNames,
    authorization_present: Boolean(req.headers.authorization),
    session_turn_header_names: headerNames.filter((name) => SESSION_HEADER_RE.test(name)),
    content_encoding: req.headers["content-encoding"] ?? null,
    body_bytes: rawBody.length,
  };
  if (rawBody.length === 0) return { summary, json: null };
  let json = null;
  try {
    json = JSON.parse(decodeBody(rawBody, req.headers["content-encoding"]).toString("utf8"));
  } catch (error) {
    summary.body_parse_error = String(error?.message ?? error);
    return { summary, json: null };
  }
  const input = Array.isArray(json.input) ? json.input : null;
  const inputTypes = {};
  for (const item of input ?? []) {
    const key = item?.type ?? (item?.role ? `message:${item.role}` : "unknown");
    inputTypes[key] = (inputTypes[key] ?? 0) + 1;
  }
  Object.assign(summary, {
    model: json.model ?? null,
    stream: json.stream ?? "absent",
    store: Object.hasOwn(json, "store") ? json.store : "absent",
    previous_response_id_present: typeof json.previous_response_id === "string" && json.previous_response_id.length > 0,
    previous_response_id_hash: json.previous_response_id ? shortHash(json.previous_response_id) : null,
    max_output_tokens: json.max_output_tokens ?? json.max_tokens ?? json.max_completion_tokens ?? "absent",
    prompt_cache_key_present: typeof json.prompt_cache_key === "string",
    prompt_cache_retention: json.prompt_cache_retention ?? "absent",
    reasoning: json.reasoning ? { effort: json.reasoning.effort ?? null, summary: json.reasoning.summary ?? null } : "absent",
    include: json.include ?? "absent",
    instructions_present: typeof json.instructions === "string",
    instructions_sha12: typeof json.instructions === "string" ? shortHash(json.instructions) : null,
    tools_sha12: Array.isArray(json.tools) ? shortHash(JSON.stringify(json.tools)) : null,
    // Cumulative hash of leading native input items: entry k hashes items[0..k] (redacted, comparable).
    input_leading_sha12: input ? cumulativeItemHashes(input) : [],
    input_kind: input ? "array" : typeof json.input,
    input_item_count: input ? input.length : null,
    input_types: inputTypes,
    tools: Array.isArray(json.tools) ? json.tools.map((tool) => tool?.name ?? tool?.type ?? "unknown") : [],
    tool_choice: typeof json.tool_choice === "string" ? json.tool_choice : json.tool_choice ? "object" : "absent",
    parallel_tool_calls: json.parallel_tool_calls ?? "absent",
    service_tier: json.service_tier ?? "absent",
    top_level_keys: Object.keys(json).sort(),
  });
  return { summary, json };
}

function extractUsage(usage) {
  if (!usage || typeof usage !== "object") return null;
  return {
    input_tokens: usage.input_tokens ?? null,
    cached_tokens: usage.input_tokens_details?.cached_tokens ?? null,
    output_tokens: usage.output_tokens ?? null,
    reasoning_tokens: usage.output_tokens_details?.reasoning_tokens ?? null,
    total_tokens: usage.total_tokens ?? null,
  };
}

function absorbResponseObject(target, response) {
  if (!response || typeof response !== "object") return;
  if (typeof response.id === "string") target.provider_response_id = response.id;
  if (response.status) target.response_status = response.status;
  if (Object.hasOwn(response, "store")) target.echoed_store = response.store;
  if (Object.hasOwn(response, "previous_response_id")) {
    target.echoed_previous_response_id_present = typeof response.previous_response_id === "string";
    target.echoed_previous_response_id_hash = response.previous_response_id ? shortHash(response.previous_response_id) : null;
  }
  if (response.model) target.response_model = response.model;
  if (response.usage) target.usage = extractUsage(response.usage);
  if (response.error) {
    target.error = { type: response.error.type ?? null, code: response.error.code ?? null, message: truncate(response.error.message ?? null, 200) };
  }
  if (response.incomplete_details) target.incomplete_details = response.incomplete_details;
}

function summarizeResponse(status, headers, buffer, truncated) {
  const contentType = String(headers["content-type"] ?? "");
  const summary = {
    status,
    content_type: contentType,
    cclb_request_id: headers["request-id"] ?? headers["x-request-id"] ?? null,
    header_names: Object.keys(headers).map((name) => name.toLowerCase()).sort(),
    sse: contentType.includes("text/event-stream"),
    parse_truncated: truncated,
    event_type_counts: {},
    provider_response_id: null,
  };
  let text;
  try {
    text = decodeBody(buffer, headers["content-encoding"]).toString("utf8");
  } catch (error) {
    summary.body_parse_error = String(error?.message ?? error);
    return summary;
  }
  if (summary.sse) {
    for (const block of text.split(/\r?\n\r?\n/)) {
      const data = block
        .split(/\r?\n/)
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice(5).trimStart())
        .join("\n");
      if (!data || data === "[DONE]") continue;
      let event;
      try {
        event = JSON.parse(data);
      } catch {
        summary.event_type_counts.unparsed = (summary.event_type_counts.unparsed ?? 0) + 1;
        continue;
      }
      const type = event?.type ?? "untyped";
      summary.event_type_counts[type] = (summary.event_type_counts[type] ?? 0) + 1;
      if (event?.response) absorbResponseObject(summary, event.response);
      if (type === "error" || event?.error) {
        const err = event.error ?? event;
        summary.error = { type: err.type ?? null, code: err.code ?? null, message: truncate(err.message ?? null, 200) };
      }
    }
    summary.terminal_event = ["response.completed", "response.incomplete", "response.failed", "response.done"].find(
      (type) => summary.event_type_counts[type],
    ) ?? null;
  } else {
    try {
      const json = JSON.parse(text);
      if (json?.object === "response" || typeof json?.id === "string") absorbResponseObject(summary, json);
      if (json?.error) {
        summary.error = { type: json.error.type ?? null, code: json.error.code ?? null, message: truncate(json.error.message ?? null, 200) };
      }
    } catch {
      summary.body_text_excerpt_bytes = text.length;
    }
  }
  return summary;
}

function isGenerationRequest(method, url) {
  if (method !== "POST") return false;
  const path = String(url).split("?")[0];
  return /\/responses$/.test(path) || /\/chat\/completions$/.test(path);
}

// Opt-in correlation only: bodies and all non-tracing headers stay untouched.
function requestTrace(prefix) {
  const traceId = randomUUID().replaceAll("-", "");
  const parentSpanId = randomUUID().replaceAll("-", "").slice(0, 16);
  return {
    request_id: `${prefix}-${traceId}`,
    traceparent: `00-${traceId}-${parentSpanId}-01`,
    trace_id: traceId,
    parent_span_id: parentSpanId,
    trace_flags: "01",
  };
}

function startTap({ targetHost, targetPort, state, requestTracePrefix }) {
  // Records commit when the upstream body ends, which can be after the client already exited on the
  // terminal SSE event. `pending` lets the run loop wait for every started request to commit.
  const commit = (record) => {
    if (record.committed) return;
    record.committed = true;
    state.records.push(record);
    state.log.write(`${JSON.stringify(record)}\n`);
    state.pending -= 1;
    if (state.pending === 0) for (const resolve of state.idleWaiters.splice(0)) resolve();
  };
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("error", () => res.destroy());
    req.on("end", () => {
      const rawBody = Buffer.concat(chunks);
      const startedAt = Date.now();
      if (!state.socketIds.has(req.socket)) state.socketIds.set(req.socket, `tap-conn-${state.nextSocketId++}`);
      const record = {
        seq: state.seq++,
        run: state.currentRun,
        at: nowIso(),
        connection_id: state.socketIds.get(req.socket),
        generation: isGenerationRequest(req.method, req.url),
        forwarded: true,
        injected_locally: false,
      };
      state.pending += 1;
      const { summary } = summarizeRequest(req, rawBody);
      record.request = summary;
      if (record.generation) {
        if (state.attempts >= MAX_GENERATION_ATTEMPTS) {
          record.forwarded = false;
          record.injected_locally = true;
          record.injected_reason = "driver_generation_attempt_budget_exhausted";
          commit(record);
          res.writeHead(503, { "content-type": "application/json", "x-qa-injected": "driver-budget" });
          res.end(JSON.stringify({ error: { type: "driver_generation_attempt_budget_exhausted", injected_locally: true } }));
          state.onBudgetExhausted?.();
          return;
        }
        state.attempts += 1;
        record.attempt_number = state.attempts;
      }
      const headers = { ...req.headers, host: `${targetHost}:${targetPort}` };
      if (requestTracePrefix !== undefined) {
        record.qa_trace = requestTrace(requestTracePrefix);
        headers["request-id"] = record.qa_trace.request_id;
        headers.traceparent = record.qa_trace.traceparent;
      }
      const upstream = http.request(
        { host: targetHost, port: targetPort, method: req.method, path: req.url, headers },
        (upRes) => {
          res.writeHead(upRes.statusCode ?? 502, upRes.headers);
          const captured = [];
          let capturedBytes = 0;
          let truncated = false;
          upRes.on("data", (chunk) => {
            res.write(chunk);
            if (capturedBytes + chunk.length <= RESPONSE_PARSE_CAP) {
              captured.push(chunk);
              capturedBytes += chunk.length;
            } else {
              truncated = true;
            }
          });
          upRes.on("end", () => {
            res.end();
            record.duration_ms = Date.now() - startedAt;
            record.response = summarizeResponse(upRes.statusCode ?? 0, upRes.headers, Buffer.concat(captured), truncated);
            commit(record);
          });
          upRes.on("error", (error) => {
            record.duration_ms = Date.now() - startedAt;
            record.response = { status: upRes.statusCode ?? 0, stream_error: String(error?.message ?? error) };
            commit(record);
            res.destroy(error);
          });
        },
      );
      upstream.on("error", (error) => {
        record.duration_ms = Date.now() - startedAt;
        record.response = { status: 0, connect_error: String(error?.message ?? error) };
        commit(record);
        if (!res.headersSent) res.writeHead(502, { "content-type": "application/json", "x-qa-injected": "tap-connect-error" });
        res.end(JSON.stringify({ error: { type: "tap_upstream_connect_error", injected_locally: true } }));
      });
      upstream.end(rawBody);
    });
  });
  server.on("upgrade", (req, socket, head) => {
    const record = {
      seq: state.seq++,
      run: state.currentRun,
      at: nowIso(),
      generation: true,
      upgrade: true,
      forwarded: true,
      injected_locally: false,
      request: { method: req.method, path: req.url, header_names: Object.keys(req.headers).sort() },
    };
    if (state.attempts >= MAX_GENERATION_ATTEMPTS) {
      record.forwarded = false;
      record.injected_locally = true;
      record.injected_reason = "driver_generation_attempt_budget_exhausted";
      state.records.push(record);
      state.log.write(`${JSON.stringify(record)}\n`);
      socket.end("HTTP/1.1 503 Service Unavailable\r\nx-qa-injected: driver-budget\r\ncontent-length: 0\r\n\r\n");
      state.onBudgetExhausted?.();
      return;
    }
    state.attempts += 1;
    record.attempt_number = state.attempts;
    if (requestTracePrefix !== undefined) record.qa_trace = requestTrace(requestTracePrefix);
    state.records.push(record);
    state.log.write(`${JSON.stringify(record)}\n`);
    const upstream = net.connect(targetPort, targetHost, () => {
      const lines = [`${req.method} ${req.url} HTTP/${req.httpVersion}`];
      for (let i = 0; i < req.rawHeaders.length; i += 2) {
        const name = req.rawHeaders[i];
        if (record.qa_trace && /^(request-id|traceparent)$/i.test(name)) continue;
        const value = name.toLowerCase() === "host" ? `${targetHost}:${targetPort}` : req.rawHeaders[i + 1];
        lines.push(`${name}: ${value}`);
      }
      if (record.qa_trace) {
        lines.push(`request-id: ${record.qa_trace.request_id}`, `traceparent: ${record.qa_trace.traceparent}`);
      }
      upstream.write(`${lines.join("\r\n")}\r\n\r\n`);
      if (head?.length) upstream.write(head);
      upstream.pipe(socket);
      socket.pipe(upstream);
    });
    upstream.on("error", () => socket.destroy());
    socket.on("error", () => upstream.destroy());
  });
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve(server));
  });
}

// ---------------------------------------------------------------------------
// cc-lb observation helpers (admin API, metrics)
// ---------------------------------------------------------------------------


function summarizeEvent(event) {
  return {
    request_id: event.request_id ?? null,
    ts_ms: event.ts_ms ?? null,
    event_kind: event.event_kind ?? null,
    request_kind: event.request_kind ?? null,
    principal_id: event.principal_id ?? null,
    key_id: event.key_id ?? null,
    upstream_id: event.upstream_id ?? null,
    upstream_name: event.upstream_name ?? null,
    model: event.model ?? null,
    status: event.status ?? null,
    input_tokens: event.input_tokens ?? null,
    output_tokens: event.output_tokens ?? null,
    cache_read_input_tokens: event.cache_read_input_tokens ?? null,
    thinking_tokens: event.thinking_tokens ?? null,
    cost_usd_micros: event.cost_usd_micros ?? null,
    cost_input_micros: event.cost_input_micros ?? null,
    cost_output_micros: event.cost_output_micros ?? null,
    cost_cache_read_micros: event.cost_cache_read_micros ?? null,
    service_tier: event.service_tier ?? null,
    client_app: event.client_app ?? null,
    session_id_source: event.session_id_source ?? null,
    observed_session_id_present: Boolean(event.observed_session_id),
    thread_id_present: Boolean(event.thread_id),
    upstream_request_id_present: Boolean(event.upstream_request_id),
    error_code: event.error_code ?? null,
    upstream_error_type: event.upstream_error_type ?? null,
    upstream_error_message: truncate(event.upstream_error_message ?? null, 200),
    duration_ms: event.duration_ms ?? null,
    upstream_ttfb_ms: event.upstream_ttfb_ms ?? null,
    retry_overhead_ms: event.retry_overhead_ms ?? null,
    internal_error_count: Array.isArray(event.internal_errors) ? event.internal_errors.length : 0,
  };
}


// Real-provider host comes from the runtime-created upstream (baseUrl/host), confirmed live via admin.
async function upstreamHost(ctx) {
  const entry = (ctx.upstreams ?? []).find((upstream) => upstream.id === ctx.upstreamId) ?? null;
  const response = await admin(ctx, `/admin/v1/upstreams/${encodeURIComponent(ctx.upstreamId)}`, "GET");
  const body = parseBody(response.body);
  return {
    admin_status: response.status,
    hosts: entry?.host ? [entry.host] : [],
    provider: entry?.provider ?? body?.provider ?? null,
    kind: entry?.kind ?? null,
    name: entry?.name ?? body?.name ?? null,
    admin_id_matches: body?.id === ctx.upstreamId,
  };
}

// ---------------------------------------------------------------------------
// Client home, credential guard, OMP process
// ---------------------------------------------------------------------------

// The runtime writes the local principal key to a 0600 file and ledgers it; OMP reads it via `!cat`.
function resolveClientKeyPath(ctx) {
  const path = ctx.clientKeyPath;
  if (typeof path !== "string" || !existsSync(path)) throw new Error("prepareSlice returned no client key file");
  const mode = statSync(path).mode & 0o777;
  if (mode !== 0o600) throw new Error(`client key file mode ${mode.toString(8)} is not 600`);
  return path;
}

function writeClientHome(home, tapPort, keyPath, model) {
  const agentDir = join(home, ".omp", "agent");
  ensureDir(agentDir, 0o700);
  for (const sub of ["tmp", ".cache", ".config", ".local/share", ".local/state"]) ensureDir(join(home, sub), 0o700);
  const yaml = [
    "providers:",
    `  ${CLIENT_PROVIDER_ID}:`,
    `    baseUrl: http://127.0.0.1:${tapPort}/v1`,
    "    api: openai-responses",
    `    apiKey: "!cat '${keyPath}'"`,
    "    models:",
    `      - id: ${model}`,
    `        name: ${model} via cc-lb`,
    "        api: openai-responses",
    "        reasoning: true",
    "        input: [text]",
    "        contextWindow: 128000",
    `        maxTokens: ${MAX_OUTPUT_TOKENS}`,
    "",
  ].join("\n");
  writeFileSync(join(agentDir, "models.yml"), yaml, { mode: 0o600 });
}

function credentialScan(home) {
  const hits = [];
  const walk = (dir, depth) => {
    if (depth > 8) return;
    let entries;
    try {
      entries = readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const full = join(dir, entry.name);
      if (CREDENTIAL_NAME_RE.test(entry.name)) hits.push(full.slice(home.length + 1));
      if (entry.isDirectory()) walk(full, depth + 1);
    }
  };
  walk(home, 0);
  const modelsYml = readFileSync(join(home, ".omp", "agent", "models.yml"), "utf8");
  const providers = [...modelsYml.matchAll(/^ {2}([A-Za-z0-9_-]+):\s*$/gm)].map((match) => match[1]);
  return { credential_like_names: hits, configured_providers: providers, ok: hits.length === 0 && providers.join(",") === CLIENT_PROVIDER_ID };
}

// Shared fresh-env base (no inherited auth/provider vars), re-rooted at the per-scenario home so
// stateless and stateful sessions never share OMP agent state.
function clientEnv(home, stateful) {
  const env = { ...clientEnvBase({ clientHome: home }), PI_CODING_AGENT_DIR: join(home, ".omp", "agent") };
  if (stateful) env.PI_OPENAI_STATEFUL = "1";
  return env;
}

function textOf(value, depth = 0) {
  if (depth > 6 || value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map((item) => textOf(item, depth + 1)).join("\n");
  if (typeof value === "object") {
    if (typeof value.text === "string") return value.text;
    return Object.values(value).map((item) => textOf(item, depth + 1)).join("\n");
  }
  return "";
}

function testCounts(text) {
  const counts = {};
  for (const match of String(text).matchAll(/(?:^|\n)\s*(?:#|ℹ)\s*(tests|pass|fail|cancelled|skipped|todo)\s+(\d+)/g)) {
    counts[match[1]] = Number(match[2]);
  }
  return Object.keys(counts).length ? counts : null;
}

function summarizeToolArgs(name, args) {
  if (!args || typeof args !== "object") return null;
  if (name === "bash") return { command: truncate(args.command ?? args.cmd ?? null, 300) };
  const out = {};
  for (const key of ["path", "file", "file_path", "pattern", "glob", "paths"]) if (args[key] !== undefined) out[key] = truncate(JSON.stringify(args[key]), 200);
  out.arg_keys = Object.keys(args).sort();
  return out;
}

function parseOmpOutput(stdout) {
  const result = { session_id: null, session_line: null, tool_calls: [], assistant_messages: [], event_type_counts: {}, unparsed_lines: 0, agent_end: null };
  const pending = new Map();
  for (const line of stdout.split("\n")) {
    if (!line.trim()) continue;
    let event;
    try {
      event = JSON.parse(line);
    } catch {
      result.unparsed_lines += 1;
      continue;
    }
    const type = event.type ?? "untyped";
    result.event_type_counts[type] = (result.event_type_counts[type] ?? 0) + 1;
    if (type === "session") {
      result.session_id = event.id ?? null;
      result.session_line = { id: event.id ?? null, version: event.version ?? null, cwd: event.cwd ?? null, timestamp: event.timestamp ?? null };
    } else if (type === "tool_execution_start") {
      const call = { tool: event.toolName, call_id_hash: event.toolCallId ? shortHash(event.toolCallId) : null, args: summarizeToolArgs(event.toolName, event.args) };
      pending.set(event.toolCallId, call);
      result.tool_calls.push(call);
    } else if (type === "tool_execution_end") {
      const call = pending.get(event.toolCallId) ?? { tool: event.toolName, args: null };
      if (!pending.has(event.toolCallId)) result.tool_calls.push(call);
      call.is_error = Boolean(event.isError);
      const text = textOf(event.result);
      call.result_bytes = text.length;
      if (event.toolName === "bash") {
        call.test_counts = testCounts(text);
        call.result_excerpt = truncate(text, 600);
      }
    } else if (type === "message_end" && event.message?.role === "assistant") {
      const message = event.message;
      const meta = {};
      for (const [key, value] of Object.entries(message)) {
        if (key === "content") continue;
        if (value === null || ["string", "number", "boolean"].includes(typeof value)) meta[key] = typeof value === "string" ? truncate(value, 200) : value;
      }
      meta.usage = message.usage ?? null;
      meta.content_types = Array.isArray(message.content) ? message.content.map((part) => part?.type ?? "unknown") : typeof message.content;
      meta.text_excerpt = truncate(textOf(Array.isArray(message.content) ? message.content.filter((part) => part?.type === "text") : ""), 400);
      result.assistant_messages.push(meta);
    } else if (type === "agent_end") {
      result.agent_end = { is_terminal: event.isTerminal ?? null, yielded: event.yielded ?? null, message_count: Array.isArray(event.messages) ? event.messages.length : null };
    }
  }
  return result;
}

function runOmp({ ompBin, args, env, cwd, stdoutPath, stderrPath, state }) {
  return new Promise((resolve) => {
    const child = spawn(ompBin, args, { cwd, env, stdio: ["ignore", "pipe", "pipe"] });
    const stdoutFile = createWriteStream(stdoutPath);
    const stderrFile = createWriteStream(stderrPath);
    const stdout = [];
    let timedOut = false;
    let budgetKilled = false;
    const kill = () => {
      if (child.exitCode !== null || child.signalCode !== null) return;
      child.kill("SIGTERM");
      setTimeout(() => {
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      }, 5000).unref();
    };
    const timer = setTimeout(() => {
      timedOut = true;
      kill();
    }, RUN_WALL_MS);
    state.onBudgetExhausted = () => {
      budgetKilled = true;
      kill();
    };
    state.activeChild = child;
    child.stdout.on("data", (chunk) => {
      stdout.push(chunk);
      stdoutFile.write(chunk);
    });
    child.stderr.on("data", (chunk) => stderrFile.write(chunk));
    child.on("close", (code, signal) => {
      clearTimeout(timer);
      state.onBudgetExhausted = null;
      state.activeChild = null;
      stdoutFile.end();
      stderrFile.end();
      resolve({ rc: code, signal, timed_out: timedOut, budget_killed: budgetKilled, stdout: Buffer.concat(stdout).toString("utf8") });
    });
    child.on("error", (error) => {
      clearTimeout(timer);
      state.onBudgetExhausted = null;
      state.activeChild = null;
      resolve({ rc: null, signal: null, spawn_error: String(error?.message ?? error), timed_out: false, budget_killed: false, stdout: "" });
    });
  });
}

function fixtureCheck(projectDir, label) {
  const tests = spawnSync(process.execPath, ["--test", "--test-reporter=tap"], { cwd: projectDir, encoding: "utf8", timeout: 60_000, env: { PATH: process.env.PATH ?? "", HOME: projectDir } });
  const outputRef = `evidence/${label}.fixture-test.txt`;
  writeFileSync(join(SLICE_DIR, outputRef), `$ node --test --test-reporter=tap  (cwd ${projectDir})\nrc=${tests.status}\n${tests.stdout ?? ""}${tests.stderr ?? ""}`);
  const out = { command: "node --test --test-reporter=tap", node_test_rc: tests.status, output_ref: outputRef, node_test_counts: testCounts(`${tests.stdout}\n${tests.stderr}`) };
  const behavior = spawnSync(process.execPath, ["--input-type=module", "-e", BEHAVIOR_CHECK], { cwd: projectDir, encoding: "utf8", timeout: 30_000, env: { PATH: process.env.PATH ?? "", HOME: projectDir } });
  let observed = null;
  try {
    observed = JSON.parse(behavior.stdout.trim().split("\n").at(-1));
  } catch {
    observed = { rc: behavior.status, stderr_excerpt: truncate(behavior.stderr, 300) };
  }
  out.behavior = observed;
  out.mean_contract_ok = observed?.meanEmpty === "RangeError:mean of empty list" && observed?.meanOk === 4 && observed?.sumEmpty === 0;
  out.test_file_unchanged = sha256Hex(readFileSync(join(projectDir, "test", "stats.test.js"))) === sha256Hex(readFileSync(join(FIXTURE_DIR, "test", "stats.test.js")));
  out.ok = tests.status === 0 && out.node_test_counts?.fail === 0 && out.mean_contract_ok && out.test_file_unchanged;
  return out;
}

function sessionFiles(sessionDir) {
  const files = [];
  const walk = (dir) => {
    let entries;
    try {
      entries = readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else files.push(full.slice(sessionDir.length + 1));
    }
  };
  walk(sessionDir);
  return files.sort();
}

// ---------------------------------------------------------------------------
// Judgment
// ---------------------------------------------------------------------------

function judgeRun(run, ctx, hostInfo) {
  const problems = [];
  const blocked = [];
  if (run.process.spawn_error) blocked.push(`spawn_error:${run.process.spawn_error}`);
  if (run.process.budget_killed) blocked.push("driver_generation_attempt_budget_exhausted");
  if (run.process.timed_out) problems.push("client_wall_deadline_exceeded");
  if (run.tap_settled === false) problems.push(`tap_records_unsettled:${run.tap_pending_after_settle}`);
  if (run.process.rc !== 0 && !run.process.budget_killed) problems.push(`omp_rc=${run.process.rc} signal=${run.process.signal}`);
  if (!run.omp.session_id) problems.push("no_session_id_emitted");
  if (run.expected_session_id && run.omp.session_id !== run.expected_session_id) problems.push("resumed_session_id_mismatch");
  const generation = run.tap.filter((record) => record.generation);
  if (generation.length === 0) problems.push("no_generation_request_observed");
  if (generation.some((record) => record.upgrade)) problems.push("unexpected_websocket_upgrade_on_openai_responses");
  for (const record of generation) {
    const status = record.response?.status;
    if (status === 429) blocked.push(`http_429_principal_or_upstream_limit:seq${record.seq}:${record.response?.error?.type ?? ""}`);
    else if (!(status >= 200 && status < 300)) problems.push(`non_2xx:${status}:${record.response?.error?.type ?? record.response?.connect_error ?? ""}`);
    else {
      if (!String(record.response?.provider_response_id ?? "").startsWith("resp_")) problems.push(`missing_provider_response_id:seq${record.seq}`);
      const usage = record.response?.usage;
      if (!(usage?.input_tokens > 0 && usage?.output_tokens > 0)) problems.push(`missing_nonzero_usage:seq${record.seq}`);
      if (record.request?.stream === true && !record.response?.terminal_event) problems.push(`stream_without_terminal_event:seq${record.seq}`);
    }
    if (record.request?.model && record.request.model !== ctx.model) problems.push(`client_model_mismatch:${record.request.model}`);
    if (record.request?.max_output_tokens !== MAX_OUTPUT_TOKENS) problems.push(`max_output_tokens_observed=${record.request?.max_output_tokens}`);
  }
  if (!run.events.complete) problems.push(`events_missing:${run.events.missing.length}`);
  if (run.events.invalidated) problems.push(`observability_invalidated_drop_counters:${JSON.stringify(run.events.drop_increases)}`);
  if (run.forwarded_generation_without_request_id?.length) problems.push(`generation_without_cclb_request_id:${run.forwarded_generation_without_request_id.join(",")}`);
  for (const event of run.events.summaries) {
    if (event.upstream_id && ctx.upstreamId && event.upstream_id !== ctx.upstreamId) problems.push(`unexpected_upstream:${event.upstream_id}`);
    if (event.status >= 200 && event.status < 300 && event.cost_usd_micros === null) blocked.push("cost_missing_on_success_event");
  }
  const realHost = hostInfo.hosts.length > 0 && hostInfo.hosts.every((host) => !/^(127\.|localhost|\[::1\]|0\.0\.0\.0)/.test(host));
  if (!realHost) blocked.push(`upstream_host_not_real:${hostInfo.hosts.join(",")}`);
  const toolNames = new Set(run.omp.tool_calls.map((call) => call.tool));
  if (!toolNames.has("bash")) problems.push("bash_tool_not_used");
  if (run.turn === 1) {
    if (!toolNames.has("read")) problems.push("read_tool_not_used");
    if (!toolNames.has("edit") && !toolNames.has("write")) problems.push("no_edit_or_write_tool_used");
  } else if (toolNames.has("edit") || toolNames.has("write")) {
    problems.push("turn2_edited_despite_explanation_only_prompt");
  }
  if (!run.omp.tool_calls.some((call) => call.tool === "bash" && call.test_counts)) problems.push("no_test_run_observed_in_bash_output");
  if (!run.fixture.ok) problems.push("fixture_contract_not_met_after_turn");
  return { verdict: blocked.length ? "BLOCKED" : problems.length ? "FAIL" : "PASS", problems, blocked };
}

function judgeState(scenario, runs) {
  const generation = runs.flatMap((run) => run.tap.filter((record) => record.generation && !record.upgrade));
  const observed = generation.map((record) => ({
    run: record.run,
    seq: record.seq,
    status: record.response?.status ?? null,
    store: record.request?.store,
    previous_response_id_present: record.request?.previous_response_id_present ?? false,
    previous_matches_prior_observed_response: record.previous_matches_prior_observed_response ?? null,
    echoed_store: record.response?.echoed_store ?? null,
    echoed_previous_response_id_present: record.response?.echoed_previous_response_id_present ?? null,
    input_item_count: record.request?.input_item_count ?? null,
  }));
  if (generation.length === 0) return { verdict: "NOT_RUN", observed, problems: ["no generation requests"] };
  if (scenario === "stateless") {
    const problems = observed.filter((row) => row.previous_response_id_present || row.store === true).map((row) => `stateful_field_in_stateless:seq${row.seq}`);
    return { verdict: problems.length ? "FAIL" : "PASS", observed, problems };
  }
  const chained = observed.filter((row) => row.previous_response_id_present);
  const problems = [];
  if (chained.length === 0) {
    return { verdict: "BLOCKED", observed, problems: ["client_limited: OMP emitted no previous_response_id with PI_OPENAI_STATEFUL=1"] };
  }
  for (const row of chained) {
    if (!(row.status >= 200 && row.status < 300)) problems.push(`chained_request_non_2xx:seq${row.seq}:${row.status}`);
    if (row.store !== true) problems.push(`chained_request_store_not_true:seq${row.seq}:${row.store}`);
    if (row.previous_matches_prior_observed_response !== true) problems.push(`previous_response_id_not_a_prior_observed_response:seq${row.seq}`);
    if (row.status >= 200 && row.status < 300 && row.echoed_previous_response_id_present !== true) problems.push(`provider_did_not_echo_previous_response_id:seq${row.seq}`);
  }
  const firstStore = observed[0]?.store;
  if (firstStore !== true) problems.push(`first_stateful_request_store=${firstStore}`);
  const failedChain = chained.find((row) => !(row.status >= 200 && row.status < 300));
  if (failedChain) {
    const fallback = observed.find((row) => row.seq > failedChain.seq && !row.previous_response_id_present);
    if (fallback) problems.push(`stateless_fallback_after_chain_failure:seq${fallback.seq}`);
  }
  const byRun = {};
  for (const row of observed) {
    byRun[row.run] ??= { requests: 0, chained: 0, first_request_chained: null };
    byRun[row.run].requests += 1;
    if (byRun[row.run].first_request_chained === null) byRun[row.run].first_request_chained = row.previous_response_id_present;
    if (row.previous_response_id_present) byRun[row.run].chained += 1;
  }
  // Continuation proof is judged from the observed turn-2 (resumed process) requests.
  const turn2 = byRun["stateful-t2"];
  if (!turn2) return { verdict: problems.length ? "FAIL" : "NOT_RUN", observed, problems: [...problems, "turn2 not run"], by_run: byRun };
  if (turn2.chained === 0 && problems.length === 0) {
    return { verdict: "BLOCKED", observed, problems: ["client_limited: resumed turn-2 requests carried no previous_response_id"], by_run: byRun };
  }
  return { verdict: problems.length ? "FAIL" : "PASS", observed, problems, chained_count: chained.length, by_run: byRun };
}

// ---------------------------------------------------------------------------
// Collector manifest projection (coverage.json driver_manifest). Every field is derived from
// observed artifacts; missing observations stay null/absent so the collector reports INCOMPLETE.
// ---------------------------------------------------------------------------

const SCENARIO_IDS = {
  stateless: "omp-gpt.stateless_tools",
  stateful: "omp-gpt.stateful_previous_response",
  continue: "omp-gpt.session_continue",
  freshRouting: "x.fresh_principal_routing.openai",
  cache: "x.cache_usage_cost.openai",
  observability: "x.observability",
};
const TOOL_KIND = { read: "read", grep: "read", glob: "read", edit: "edit", write: "edit", bash: "exec" };
const SESSION_ONLY_HEADER_RE = /(session|turn|conversation|thread|affinity)/i;

function loadPrice(ctx) {
  // Prefer the bare OpenAI catalog key over reseller/region aliases (e.g. azure/gpt-5.2).
  const keys = ctx.costCatalog?.matchedKeys?.[ctx.model] ?? [];
  const key = keys.includes(ctx.model) ? ctx.model : keys[0];
  if (!key || !ctx.costCatalog?.path) return null;
  try {
    const entry = JSON.parse(readFileSync(ctx.costCatalog.path, "utf8"))[key];
    if (typeof entry?.input_cost_per_token !== "number" || typeof entry?.output_cost_per_token !== "number") return null;
    return {
      key,
      input: entry.input_cost_per_token,
      output: entry.output_cost_per_token,
      cacheRead: typeof entry.cache_read_input_token_cost === "number" ? entry.cache_read_input_token_cost : null,
    };
  } catch {
    return null;
  }
}

// OpenAI usage: input_tokens includes cached tokens; output_tokens includes reasoning tokens.
function usageCostUsd(usage, price) {
  if (!price || !usage || typeof usage.input_tokens !== "number" || typeof usage.output_tokens !== "number") return null;
  const cached = typeof usage.cached_tokens === "number" ? usage.cached_tokens : 0;
  const cacheRate = price.cacheRead ?? price.input;
  return (usage.input_tokens - cached) * price.input + cached * cacheRate + usage.output_tokens * price.output;
}

function blockedKind(reason) {
  const text = String(reason);
  if (/client_limited/.test(text)) return "client_limited";
  if (/cost|pric|catalog/.test(text)) return "pricing";
  if (/budget|attempt|429|usd_ceiling/.test(text)) return "budget";
  if (/credential|key file/.test(text)) return "credential";
  return "capability";
}

function manifestWire(run, price) {
  return (run.tap_digest ?? []).map((record) => ({
    seq: record.seq,
    transport: record.upgrade ? "websocket" : "http",
    connectionId: record.connection_id,
    // Observed full replay: no previous_response_id while prior assistant/tool items are resent.
    fullReplay: record.generation
      ? !record.previous_response_id_present && Object.keys(record.input_types ?? {}).some((type) => /function_call|message:assistant|reasoning/.test(type))
      : null,
    generation: record.generation,
    endpoint: record.path ?? null,
    apiFamily: "openai_responses",
    stream: record.stream,
    store: record.store,
    previousResponseIdPresent: record.previous_response_id_present,
    sessionTurnHeaderPresent: (record.session_turn_header_names ?? []).some((name) => SESSION_ONLY_HEADER_RE.test(name)),
    sessionTurnHeaderNames: (record.session_turn_header_names ?? []).filter((name) => SESSION_ONLY_HEADER_RE.test(name)),
    thoughtSignatureReplayedPresent: null,
    functionCallReplayed: Object.keys(record.input_types ?? {}).some((type) => type === "function_call"),
    httpStatus: record.status,
    providerResponseIdPresent: typeof record.provider_response_id === "string" && record.provider_response_id.startsWith("resp_"),
    providerResponseIdSha256: record.provider_response_id ? sha256Hex(record.provider_response_id) : null,
    providerUsage: record.usage
      ? { input: record.usage.input_tokens, cached: record.usage.cached_tokens, output: record.usage.output_tokens, reasoning: record.usage.reasoning_tokens }
      : null,
    driverCostUsd: usageCostUsd(record.usage, price),
    upstreamId: record.upstream_id,
    ...(record.qa_trace ? { qaTrace: record.qa_trace } : {}),
    forwarded: record.forwarded,
    injected: record.injected_locally,
    source: "proxy_wire_capture",
    ref: `evidence/${run.id}.tap.json#seq=${record.seq}`,
  }));
}

function runScenarioIds(run) {
  const ids = [SCENARIO_IDS[run.scenario], SCENARIO_IDS.continue, SCENARIO_IDS.freshRouting, SCENARIO_IDS.observability];
  return ids;
}

function manifestRun(run, price) {
  const wire = manifestWire(run, price);
  const costs = wire.map((frame) => frame.driverCostUsd).filter((value) => typeof value === "number");
  const generationFrames = wire.filter((frame) => frame.generation).length;
  return {
    label: run.id,
    scenarioIds: runScenarioIds(run),
    kind: "cli",
    sessionId: run.omp?.session_id ?? null,
    resumeOfSessionId: run.turn > 1 ? run.expected_session_id ?? null : null,
    resumeSelector: run.turn > 1 ? "exact_id" : null,
    turn: run.turn,
    exitCode: run.process?.rc ?? null,
    timedOut: Boolean(run.process?.timed_out),
    wallSeconds: run.wall_seconds ?? null,
    cliOutputRef: `evidence/${run.id}.omp.jsonl`,
    toolCalls: (run.omp?.tool_calls ?? []).map((call) => ({
      name: call.tool,
      kind: TOOL_KIND[call.tool] ?? "other",
      ok: call.is_error === undefined ? null : !call.is_error,
    })),
    fixtureTest: run.fixture ? { command: run.fixture.command, exitCode: run.fixture.node_test_rc, outputRef: run.fixture.output_ref, counts: run.fixture.node_test_counts } : null,
    wire,
    metrics: { before: `${run.id}-before`, after: `${run.id}-after` },
    budgetTap: { enforced: true, generationFrames, attemptsCap: MAX_GENERATION_ATTEMPTS, usdCap: USD_CEILING_SLICE, deadlineSecs: RUN_WALL_MS / 1000 },
    driverCostUsd: price && costs.length === generationFrames && generationFrames > 0 ? costs.reduce((a, b) => a + b, 0) : null,
    verdict: run.verdict ?? null,
    problems: run.problems ?? [],
    blockedReasons: run.blocked ?? [],
  };
}

function configGuard(observed) {
  const homes = observed?.homes ?? [];
  const credentialNames = new Set();
  const homeChecks = homes.map((home) => {
    const yml = readFileSync(join(home, ".omp", "agent", "models.yml"), "utf8");
    const scan = credentialScan(home);
    for (const name of scan.credential_like_names) credentialNames.add(name);
    const baseUrls = [...yml.matchAll(/baseUrl:\s*(\S+)/g)].map((match) => match[1]);
    return { home, configured_providers: scan.configured_providers, base_urls: baseUrls, credential_like_names: scan.credential_like_names };
  });
  const tapBase = observed?.tapPort ? `http://127.0.0.1:${observed.tapPort}/v1` : null;
  const onlyCclb = homeChecks.length > 0 && homeChecks.every((check) => check.configured_providers.join(",") === CLIENT_PROVIDER_ID);
  const viaTapToProxy = homeChecks.length > 0 && homeChecks.every((check) => check.base_urls.length === 1 && check.base_urls[0] === tapBase);
  const guard = {
    fakeOrDirectFallback: !(onlyCclb && viaTapToProxy),
    providerBaseUrlIsProxy: onlyCclb && viaTapToProxy,
    providerBaseUrlPath: tapBase ? `${tapBase} (local pass-through capture tap) -> ${observed.proxyUrl}` : null,
    credentialFileNamesScanned: [...credentialNames].sort(),
    realAuthCopied: false,
    homes: homeChecks,
    ref: "evidence/config-guard.json",
  };
  writeJson(join(EVIDENCE_DIR, "config-guard.json"), guard);
  return guard;
}

function traceInfo(ctx) {
  let text = "";
  try {
    text = readFileSync(ctx.configPath, "utf8");
  } catch {
    // config unreadable: report as not configured below with ref
  }
  return { otlpConfigured: /otlp|opentelemetry|otel/i.test(text), collectorRef: ctx.configPath ?? null };
}

function manifestAssertions(summary, ctx) {
  const assertions = [];
  const ref = "evidence/driver-summary.json";
  const push = (scenarioId, name, expected, observed, pass) => assertions.push({ scenarioId, name, expected, observed, pass, ref });
  for (const scenario of ["stateless", "stateful"]) {
    const result = summary.scenarios?.[scenario];
    if (!result) continue;
    const scenarioId = SCENARIO_IDS[scenario];
    for (const run of summary.driverRuns.filter((item) => item.scenario === scenario && item.process)) {
      push(scenarioId, `run_judgment:${run.id}`, "PASS (cli completed, tools, fixture test, events, usage, cost)", { verdict: run.verdict, problems: run.problems, blocked: run.blocked }, run.verdict === "PASS");
    }
    if (scenario === "stateless") {
      push(scenarioId, "stateless", "no previous_response_id and store!=true on every generation request", result.native_state.problems, result.native_state.verdict === "PASS");
    } else {
      push(scenarioId, "stateful_chain", "turn1 store:true; resumed turn2 chained request store:true + previous_response_id of a prior observed response, echoed by provider", { by_run: result.native_state.by_run, problems: result.native_state.problems }, result.native_state.verdict === "PASS");
      const upstreams = new Set(
        summary.driverRuns
          .filter((run) => run.scenario === "stateful")
          .flatMap((run) => (run.tap_digest ?? []).filter((record) => record.generation && record.forwarded).map((record) => record.upstream_id)),
      );
      push(scenarioId, "same_owner_upstream", ctx.upstreamId, [...upstreams], upstreams.size === 1 && upstreams.has(ctx.upstreamId));
    }
    push(SCENARIO_IDS.continue, `session_resume_exact:${scenario}`, "turn2 resumed via -r <exact turn1 session id> with same session-dir; one session file", result.session_continue, result.session_continue.verdict === "PASS");
  }
  return assertions;
}

// Cache proof is shared with the offline post-processor (cache-proof.mjs) so live and re-derived
// evidence use identical pairing, basis and cost arithmetic.
function manifestCache(summary, ctx, records) {
  if (!records?.length) return { entries: [], proof: null };
  const events = new Map();
  for (const run of summary.driverRuns) for (const event of run.events?.summaries ?? []) if (event.request_id) events.set(event.request_id, event);
  const catalogPrice = loadCatalogPrice(ctx.costCatalog?.path, ctx.model);
  const proof = buildCacheProof(records, { events, price: catalogPrice });
  writeJson(join(EVIDENCE_DIR, "cache-proof.json"), proof);
  return { entries: proof.entries, proof };
}

function manifestBlocked(summary) {
  const blocked = [];
  const ref = "evidence/driver-summary.json";
  const allScenarios = [SCENARIO_IDS.stateless, SCENARIO_IDS.stateful, SCENARIO_IDS.continue];
  for (const reason of summary.blockers ?? []) {
    for (const scenarioId of allScenarios) blocked.push({ scenarioId, kind: blockedKind(reason), prerequisite: reason, ref });
  }
  for (const run of summary.driverRuns ?? []) {
    const reasons = run.verdict === "BLOCKED" ? [...(run.blocked ?? []), ...(run.reason ? [run.reason] : [])] : [];
    for (const reason of reasons) blocked.push({ scenarioId: SCENARIO_IDS[run.scenario], kind: blockedKind(reason), prerequisite: `${run.id}: ${reason}`, ref });
  }
  const stateful = summary.scenarios?.stateful?.native_state;
  if (stateful?.verdict === "BLOCKED") {
    for (const reason of stateful.problems ?? []) blocked.push({ scenarioId: SCENARIO_IDS.stateful, kind: blockedKind(reason), prerequisite: reason, ref });
  }
  return blocked;
}

async function finalize(ctx, summary, observed) {
  summary.finished_at ??= nowIso();
  const price = loadPrice(ctx);
  summary.driverPrice = price ? { catalogKey: price.key, inputPerToken: price.input, outputPerToken: price.output, cacheReadPerToken: price.cacheRead } : null;
  summary.configGuard = observed ? configGuard(observed) : null;
  summary.trace = traceInfo(ctx);
  summary.principalFresh = Boolean(ctx.principalId);
  // The driver issues only admin GETs; it never mutates any principal.
  summary.sharedPrincipalsUnmodified = true;
  summary.runs = summary.driverRuns.filter((run) => run.process).map((run) => manifestRun(run, price));
  summary.assertions = manifestAssertions(summary, ctx);
  summary.blocked = manifestBlocked(summary);
  const cacheResult = manifestCache(summary, ctx, observed?.records);
  summary.cache = cacheResult.entries;
  summary.cacheFirstMisses = cacheResult.proof?.firstMisses ?? [];
  summary.cacheNotes = cacheResult.proof?.notes ?? [];
  summary.faults = [];
  writeJson(join(EVIDENCE_DIR, "driver-summary.json"), summary);
  await finishEvidence(ctx, summary);
  return summary;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const otlpEndpoint = process.env.QA_OTLP_ENDPOINT || undefined;
  const requestTracePrefix = process.env.QA_REQUEST_TRACE_PREFIX || undefined;
  if (requestTracePrefix !== undefined && !/^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$/.test(requestTracePrefix)) {
    throw new Error("QA_REQUEST_TRACE_PREFIX must be 1..96 ASCII letters, digits, dots, underscores, or hyphens, starting with a letter or digit");
  }
  ensureDir(EVIDENCE_DIR);
  ensureDir(WORK_DIR, 0o700);
  const startedAt = nowIso();
  const summary = { slice: SLICE, started_at: startedAt, scenarios: {}, driverRuns: [], blockers: [] };

  const localSha = sha256Hex(readFileSync(BINARY_PATH));
  // QA_RESUME=1 reuses the runtime's verified, ledgered cc-lb process for this slice (no restart/delete).
  const ctx = await prepareSlice({
    name: SLICE,
    ports: PORTS,
    provider: PROVIDER,
    model: EXPECTED_MODEL,
    pricedModels: [EXPECTED_MODEL],
    resume: process.env.QA_RESUME === "1",
    ...(otlpEndpoint === undefined ? {} : { otlpEndpoint }),
  });
  summary.binary_sha256 = { local: localSha, runtime: ctx.binarySha256 ?? null, expected: EXPECTED_BINARY_SHA256 };
  summary.principal_id = ctx.principalId;
  summary.upstream_id = ctx.upstreamId;
  summary.model = ctx.model;
  summary.key_id = ctx.keyId ?? null;
  if (ctx.model !== EXPECTED_MODEL) summary.blockers.push(`runtime_model_unexpected:${ctx.model}`);
  for (const reason of ctx.blocked ?? []) summary.blockers.push(`runtime:${reason}`);
  summary.proxy_url = ctx.proxyUrl;
  if (localSha !== EXPECTED_BINARY_SHA256 || (ctx.binarySha256 && ctx.binarySha256 !== EXPECTED_BINARY_SHA256)) {
    summary.blockers.push("binary_sha256_mismatch");
    summary.verdict = "BLOCKED";
    return finalize(ctx, summary, null);
  }

  const ompPath = findExecutable("omp");
  if (!ompPath) throw new Error("omp not found on PATH");
  const helpHome = join(WORK_DIR, "version-home");
  ensureDir(helpHome, 0o700);
  await recordResource(ctx, { kind: "scratch-directory", id: helpHome, delete: `rm -rf '${helpHome}'`, created_at: nowIso() });
  const versionOut = spawnSync(ompPath, ["--version"], { encoding: "utf8", timeout: 30_000, env: { HOME: helpHome, PATH: process.env.PATH ?? "", TERM: "dumb", NO_COLOR: "1" } });
  const ompVersion = String(versionOut.stdout ?? "").trim();
  writeFileSync(join(EVIDENCE_DIR, "omp-version.txt"), `$ ${ompPath} --version\nrc=${versionOut.status}\n${versionOut.stdout ?? ""}${versionOut.stderr ?? ""}`);
  summary.client = {
    name: "omp",
    binaryPath: ompPath,
    realpath: realpathSync(ompPath),
    version: ompVersion,
    versionCommandRef: "evidence/omp-version.txt",
    expectedVersionPrefix: EXPECTED_OMP_VERSION_PREFIX,
  };
  if (!ompVersion.includes(EXPECTED_OMP_VERSION_PREFIX)) summary.blockers.push(`omp_version_unexpected:${ompVersion}`);

  const hostInfo = await upstreamHost(ctx);
  summary.real_provider_upstream = hostInfo;
  if (!hostInfo.hosts.includes(EXPECTED_PROVIDER_HOST)) summary.blockers.push(`upstream_host_not_${EXPECTED_PROVIDER_HOST}:${hostInfo.hosts.join(",") || "none"}`);
  if (summary.blockers.length) {
    summary.verdict = "BLOCKED";
    return finalize(ctx, summary, null);
  }

  const proxy = new URL(ctx.proxyUrl);
  const tapLog = createWriteStream(join(EVIDENCE_DIR, "tap.jsonl"), { flags: "a" });
  const state = { seq: 0, attempts: 0, records: [], currentRun: null, log: tapLog, onBudgetExhausted: null, activeChild: null, socketIds: new WeakMap(), nextSocketId: 1, pending: 0, idleWaiters: [] };
  const tap = await startTap({ targetHost: proxy.hostname, targetPort: Number(proxy.port || 80), state, requestTracePrefix });
  const tapPort = tap.address().port;
  summary.observation_tap = { kind: "local owned pass-through HTTP tap (no payload rewrite)", listen: `127.0.0.1:${tapPort}`, target: `${proxy.hostname}:${proxy.port}` };
  await recordResource(ctx, { kind: "in-process-listener", id: `omp-gpt-tap 127.0.0.1:${tapPort} pid ${process.pid}`, delete: "exits with run.mjs process", created_at: nowIso() });

  const keyPath = resolveClientKeyPath(ctx);

  process.on("SIGINT", () => {
    state.activeChild?.kill("SIGTERM");
    process.exitCode = 130;
  });

  const sessions = {};
  let costMicros = 0;
  let stopReason = null;

  for (const spec of RUNS) {
    const runRecord = { id: spec.id, scenario: spec.scenario, turn: spec.turn, stateful_env: spec.stateful };
    summary.driverRuns.push(runRecord);
    if (stopReason) {
      runRecord.verdict = "NOT_RUN";
      runRecord.reason = stopReason;
      continue;
    }
    if (!sessions[spec.scenario]) {
      const base = join(WORK_DIR, spec.scenario);
      const home = join(base, "home");
      const projectDir = join(base, "project");
      const sessionDir = join(base, "sessions");
      ensureDir(base, 0o700);
      ensureDir(sessionDir, 0o700);
      cpSync(FIXTURE_DIR, projectDir, { recursive: true });
      writeClientHome(home, tapPort, keyPath, ctx.model);
      sessions[spec.scenario] = { home, projectDir, sessionDir, sessionId: null };
      await recordResource(ctx, { kind: "scratch-directory", id: base, delete: `rm -rf '${base}'`, created_at: nowIso() });
    }
    const session = sessions[spec.scenario];
    if (spec.turn > 1 && !session.sessionId) {
      runRecord.verdict = "NOT_RUN";
      runRecord.reason = "turn1 produced no session id";
      continue;
    }

    // Turn 1: home must hold no credential-like names at all. Turn 2: only names the client itself
    // created during turn 1 (recorded right after turn 1) are tolerated; anything else blocks.
    const scan = credentialScan(session.home);
    const tolerated = new Set(session.clientCreatedCredentialNames ?? []);
    scan.client_created_tolerated = scan.credential_like_names.filter((name) => tolerated.has(name));
    scan.unexpected = scan.credential_like_names.filter((name) => !tolerated.has(name));
    scan.ok = scan.unexpected.length === 0 && scan.configured_providers.join(",") === CLIENT_PROVIDER_ID;
    runRecord.credential_scan = scan;
    if (!scan.ok) {
      runRecord.verdict = "BLOCKED";
      runRecord.reason = "credential_guard";
      stopReason = "credential_guard";
      continue;
    }

    const args = [
      "-p",
      "--mode", "json",
      "--model", `${CLIENT_PROVIDER_ID}/${ctx.model}`,
      "--session-dir", session.sessionDir,
      "--cwd", session.projectDir,
      "--tools", "read,bash,edit,grep,glob",
      "--thinking", "low",
      "--max-time", String(OMP_MAX_TIME_SECONDS),
      "--auto-approve",
      "--no-lsp",
      "--no-extensions",
      "--no-skills",
      "--no-rules",
      "--no-title",
      "--no-pty",
    ];
    if (spec.turn > 1) args.push("-r", session.sessionId);
    args.push(spec.prompt);
    runRecord.command = { binary: ompPath, args: args.slice(0, -1), prompt_sha12: shortHash(spec.prompt), env_keys: Object.keys(clientEnv(session.home, spec.stateful)).sort(), pi_openai_stateful: spec.stateful ? "1" : "unset" };

    try {
      runRecord.budget_before = await assertBudget(ctx);
    } catch (error) {
      if (error?.code !== "BUDGET_BLOCKED") throw error;
      runRecord.verdict = "BLOCKED";
      runRecord.reason = `budget:${error.message}`;
      stopReason = "budget_blocked";
      continue;
    }
    const baseline = await observabilityBaseline(ctx, `${spec.id}-before`);
    state.currentRun = spec.id;
    runRecord.started_at = nowIso();
    const runStartedMs = Date.now();
    const processResult = await runOmp({
      ompBin: ompPath,
      args,
      env: clientEnv(session.home, spec.stateful),
      cwd: session.projectDir,
      stdoutPath: join(EVIDENCE_DIR, `${spec.id}.omp.jsonl`),
      stderrPath: join(EVIDENCE_DIR, `${spec.id}.omp.stderr`),
      state,
    });
    runRecord.finished_at = nowIso();
    runRecord.wall_seconds = (Date.now() - runStartedMs) / 1000;
    // Wait (bounded) for every request started during this run to commit its record; the client may
    // exit on the terminal SSE event before the upstream body ends.
    const settled = state.pending === 0 || (await Promise.race([
      new Promise((resolve) => state.idleWaiters.push(() => resolve(true))),
      new Promise((resolve) => setTimeout(() => resolve(false), EVENT_DEADLINE_MS)),
    ]));
    runRecord.tap_settled = settled;
    runRecord.tap_pending_after_settle = state.pending;
    state.currentRun = null;

    const tapRecords = state.records.filter((record) => record.run === spec.id);
    for (const record of tapRecords) {
      if (record.request?.previous_response_id_present) {
        const earlier = state.records
          .filter((other) => other.seq < record.seq && other.response?.provider_response_id)
          .map((other) => shortHash(other.response.provider_response_id));
        record.previous_matches_prior_observed_response = earlier.includes(record.request.previous_response_id_hash);
      }
    }

    const forwardedGeneration = tapRecords.filter((record) => record.generation && record.forwarded && !record.upgrade);
    runRecord.forwarded_generation_without_request_id = forwardedGeneration.filter((record) => !record.response?.cclb_request_id).map((record) => record.seq);
    const requestIds = forwardedGeneration.filter((record) => record.response?.cclb_request_id).map((record) => record.response.cclb_request_id);
    const eventWait = await waitForRequestEvents(ctx, {
      baseline,
      expectedRequestIds: requestIds,
      minCount: Math.max(requestIds.length, 1),
      deadlineMs: EVENT_DEADLINE_MS,
      label: spec.id,
    });
    const omp = parseOmpOutput(processResult.stdout);
    delete processResult.stdout;
    if (spec.turn === 1 && omp.session_id) session.sessionId = omp.session_id;
    if (spec.turn === 1) session.clientCreatedCredentialNames = credentialScan(session.home).credential_like_names;

    const eventSummaries = eventWait.events.map(summarizeEvent);
    writeJson(join(EVIDENCE_DIR, `${spec.id}.events.json`), eventSummaries);
    writeJson(join(EVIDENCE_DIR, `${spec.id}.tap.json`), tapRecords);

    Object.assign(runRecord, {
      process: processResult,
      omp,
      expected_session_id: spec.turn > 1 ? session.sessionId : null,
      session_files: sessionFiles(session.sessionDir),
      tap: tapRecords,
      events: {
        baseline_cursor: baseline.eventCursor,
        cursor_source: baseline.cursorSource,
        final_cursor: eventWait.finalCursor,
        complete: !eventWait.timedOut && (eventWait.missing ?? []).length === 0,
        missing: eventWait.missing ?? [],
        drop_increases: eventWait.dropIncreases ?? [],
        critical_drop_increases: eventWait.criticalDropIncreases ?? [],
        invalidated: Boolean(eventWait.invalidated),
        summaries: eventSummaries,
      },
      fixture: fixtureCheck(session.projectDir, spec.id),
      generation_attempts_total_after_run: state.attempts,
    });

    for (const event of eventSummaries) if (typeof event.cost_usd_micros === "number") costMicros += event.cost_usd_micros;
    Object.assign(runRecord, judgeRun(runRecord, ctx, hostInfo));
    // Keep summary compact: tap/event detail lives in per-run files.
    const upstreamByRequestId = new Map(eventSummaries.map((event) => [event.request_id, event.upstream_id]));
    runRecord.tap_digest = tapRecords.map((record) => ({
      seq: record.seq,
      connection_id: record.connection_id ?? null,
      upgrade: Boolean(record.upgrade),
      generation: record.generation,
      forwarded: record.forwarded,
      injected_locally: record.injected_locally,
      path: record.request?.path,
      stream: record.request?.stream,
      store: record.request?.store,
      previous_response_id_present: record.request?.previous_response_id_present ?? null,
      session_turn_header_names: record.request?.session_turn_header_names ?? [],
      max_output_tokens: record.request?.max_output_tokens ?? null,
      input_item_count: record.request?.input_item_count ?? null,
      input_types: record.request?.input_types ?? {},
      instructions_sha12: record.request?.instructions_sha12 ?? null,
      tools_sha12: record.request?.tools_sha12 ?? null,
      input_leading_sha12: record.request?.input_leading_sha12 ?? [],
      previous_matches_prior_observed_response: record.previous_matches_prior_observed_response ?? null,
      status: record.response?.status ?? null,
      cclb_request_id: record.response?.cclb_request_id ?? null,
      ...(record.qa_trace ? { qa_trace: record.qa_trace } : {}),
      upstream_id: upstreamByRequestId.get(record.response?.cclb_request_id) ?? null,
      provider_response_id: record.response?.provider_response_id ?? null,
      usage: record.response?.usage ?? null,
      echoed_store: record.response?.echoed_store ?? null,
      echoed_previous_response_id_present: record.response?.echoed_previous_response_id_present ?? null,
    }));
    delete runRecord.tap;

    if (runRecord.blocked?.includes("cost_missing_on_success_event")) stopReason = "cost_blocked_missing_pricing";
    if (costMicros / 1e6 > USD_CEILING_SLICE) stopReason = "usd_ceiling_reached";
    if (state.attempts >= MAX_GENERATION_ATTEMPTS) stopReason = "generation_attempt_budget_reached";
  }

  for (const scenario of ["stateless", "stateful"]) {
    const runs = summary.driverRuns.filter((run) => run.scenario === scenario && run.tap_digest);
    const runIds = new Set(runs.map((run) => run.id));
    const records = state.records.filter((record) => runIds.has(record.run));
    const state_ = judgeState(scenario, runs.map((run) => ({ tap: records.filter((record) => record.run === run.id) })));
    const turn1 = summary.driverRuns.find((run) => run.scenario === scenario && run.turn === 1);
    const turn2 = summary.driverRuns.find((run) => run.scenario === scenario && run.turn === 2);
    const continuation = !turn2 || turn2.verdict === "NOT_RUN" || !turn2.omp
      ? { verdict: "NOT_RUN" }
      : {
          verdict: turn2.omp.session_id && turn2.omp.session_id === turn1?.omp?.session_id && turn2.session_files.filter((file) => file.endsWith(".jsonl")).length === 1 ? "PASS" : "FAIL",
          turn1_session_id: turn1?.omp?.session_id ?? null,
          turn2_session_id: turn2.omp.session_id,
          session_files: turn2.session_files,
        };
    summary.scenarios[scenario] = {
      tools: { verdict: turn1?.verdict ?? "NOT_RUN", turn2_verdict: turn2?.verdict ?? "NOT_RUN" },
      native_state: state_,
      session_continue: continuation,
    };
  }

  summary.generation_attempts = state.attempts;
  summary.cost_usd = costMicros / 1e6;
  summary.stop_reason = stopReason;
  summary.finished_at = nowIso();
  const verdicts = Object.values(summary.scenarios).flatMap((scenario) => [scenario.tools.verdict, scenario.tools.turn2_verdict, scenario.native_state.verdict, scenario.session_continue.verdict]);
  summary.verdict = verdicts.includes("FAIL") ? "FAIL" : verdicts.includes("BLOCKED") || summary.blockers.length ? "BLOCKED" : verdicts.includes("NOT_RUN") ? "NOT_RUN" : "PASS";

  tapLog.end();
  await new Promise((resolve) => tap.close(resolve));
  return finalize(ctx, summary, { tapPort, proxyUrl: ctx.proxyUrl, homes: Object.values(sessions).map((session) => session.home), records: state.records });
}

main()
  .then((summary) => {
    console.log(JSON.stringify({ slice: SLICE, verdict: summary.verdict, scenarios: Object.fromEntries(Object.entries(summary.scenarios ?? {}).map(([key, value]) => [key, { tools: value.tools, native_state: value.native_state?.verdict, session_continue: value.session_continue?.verdict }])), attempts: summary.generation_attempts, cost_usd: summary.cost_usd, stop_reason: summary.stop_reason }, null, 2));
  })
  .catch((error) => {
    console.error(`omp-gpt driver error: ${error?.stack ?? error}`);
    process.exitCode = 1;
  });
