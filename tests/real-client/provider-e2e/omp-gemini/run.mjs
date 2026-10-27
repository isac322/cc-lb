#!/usr/bin/env node
// omp-gemini slice driver: actual OMP CLI -> local passive observation tap -> cc-lb -> real Gemini API.
//
// Scenarios (contract.json slices[omp-gemini]):
//   gemini3-tools      Gemini 3 (gemini-3-pro-preview) real coding tool loop; thoughtSignature on replayed
//                      functionCall parts asserted from the observed client request bodies.
//   session-continue   Turn 2 resumes the exact turn-1 session id (`-r <id>`, same --session-dir) and must
//                      replay turn-1 functionCall parts with their thoughtSignature unchanged.
//   flashlite-control  Same task + same transport on gemini-3.5-flash-lite; request shape must match the
//                      Gemini 3 turn-1 shape apart from the model; tokens/cost compared.
//
// The tap is a local owned pass-through: it never rewrites bodies or adds headers (Host is set to the
// cc-lb authority, which is what OMP would send when pointed at cc-lb directly). It only records redacted
// structure (keys, part kinds, signature presence + sha256 prefix). Locally answered requests (driver
// attempt budget) are labeled `injected_locally: true` with an `x-qa-injected` response header.
// cachedContent is NOT native OMP coverage; it is a separate server-contract probe outside this driver.
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { cpSync, createWriteStream, existsSync, mkdirSync, readdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import http from "node:http";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import zlib from "node:zlib";

import {
  CLIENT_LIMITS,
  EXPECTED_BINARY_SHA256,
  BINARY_PATH,
  admin,
  assertBudget,
  budgetStatus,
  finishEvidence,
  observabilityBaseline,
  prepareSlice,
  recordResource,
  redactText,
  runClient,
  scanCredentialFiles,
  sha256File,
  waitForRequestEvents,
} from "../qa-runtime.mjs";

const SLICE = "omp-gemini";
const SLICE_DIR = dirname(fileURLToPath(import.meta.url));
const FIXTURE_DIR = join(SLICE_DIR, "fixture");
const PORTS = [18520, 18521, 18522];
const PROVIDER = "gemini";
// Gemini 3 family primary. Override with QA_GEMINI_PRIMARY_MODEL (allowlisted ids only; never a guessed retry).
const PRIMARY_MODEL_ALLOWLIST = ["gemini-3-pro-preview", "gemini-3.1-pro-preview"];
const PRIMARY_MODEL = process.env.QA_GEMINI_PRIMARY_MODEL ?? "gemini-3-pro-preview";
if (!PRIMARY_MODEL_ALLOWLIST.includes(PRIMARY_MODEL)) throw new Error(`QA_GEMINI_PRIMARY_MODEL must be one of ${PRIMARY_MODEL_ALLOWLIST.join(",")}`);
const CONTROL_MODEL = "gemini-3.5-flash-lite";
const CLIENT_PROVIDER_ID = "cclb-gemini";
const EXPECTED_OMP_VERSION = "18.12.2";
const EXPECTED_PROVIDER_HOST = "generativelanguage.googleapis.com";
const THINKING_LEVEL = "low"; // supported by both models' google-level thinking efforts
const MAX_OUTPUT_TOKENS = CLIENT_LIMITS.maxOutputTokens;
const RUN_TIMEOUT_MS = CLIENT_LIMITS.turnTimeoutSecs * 1000;
const OMP_MAX_TIME = "115"; // seconds; help: "--max-time=<value> (e.g., 600, 10m, 1h)"
const EVENT_DEADLINE_MS = 30_000;
const RESPONSE_PARSE_CAP = 8 * 1024 * 1024;
const OMP_TOOLS = "read,bash,edit,write";
// Per-attempt id: fresh project/home/session dirs and unique evidence labels (first attempt keeps bare labels).
const ATTEMPT = process.env.QA_ATTEMPT_ID ?? new Date().toISOString().replace(/[-:.]/g, "").slice(0, 15);
const LABEL_SUFFIX = process.env.QA_RESUME === "1" ? `-r${ATTEMPT}` : "";

// Per-run forwarded-request caps; sum == CLIENT_LIMITS.maxGenerationAttempts (16).
// Every forwarded request counts (generation or not); replay/error/cancel included.
const SCENARIO = {
  tools: "omp-gemini.gemini3_tools_thought_signature",
  resume: "omp-gemini.session_continue",
  lite: "omp-gemini.flash_lite_transport",
};
const RUNS = [
  { id: "gemini3-t1", session: "gemini3", model: PRIMARY_MODEL, turn: 1, cap: 6, prompt: "TURN1", scenarioIds: [SCENARIO.tools] },
  { id: "gemini3-t2", session: "gemini3", model: PRIMARY_MODEL, turn: 2, cap: 5, prompt: "TURN2", scenarioIds: [SCENARIO.resume] },
  { id: "flashlite-t1", session: "flashlite", model: CONTROL_MODEL, turn: 1, cap: 5, prompt: "TURN1", scenarioIds: [SCENARIO.lite] },
];

const PROMPTS = {
  TURN1: [
    "You are working in a tiny JavaScript project in the current directory. Be economical with tool calls.",
    "First use the read tool on src/slugify.js and test/slugify.test.js (both in one step).",
    "Then make the smallest edit to src/slugify.js so every existing test passes; do not modify the tests.",
    "Run `node --test` once with the bash tool and reply with a one-sentence summary.",
  ].join(" "),
  TURN2: [
    "Continuing this session: extend slugify with an optional second argument `{ maxLength }` that truncates",
    "the slug to at most maxLength characters and never leaves a trailing dash.",
    "Add one test for it to test/slugify.test.js, run `node --test` once with the bash tool,",
    "and reply with a two-sentence summary of both changes in this session.",
  ].join(" "),
};

// Hidden driver checks (local fixture evaluation, not provider calls).
const BEHAVIOR_T1 = [
  "import { slugify } from './src/slugify.js';",
  "console.log(JSON.stringify({ a: slugify('Hello World'), b: slugify('  Hello,  World!  '), c: slugify('Crème brûlée'), d: slugify('--Já  vu--') }));",
].join("\n");
const EXPECTED_T1 = { a: "hello-world", b: "hello-world", c: "creme-brulee", d: "ja-vu" };
const BEHAVIOR_T2 = [
  "import { slugify } from './src/slugify.js';",
  "const out = {};",
  "try { out.cut = slugify('Hello big world', { maxLength: 9 }); } catch (e) { out.cut = 'throw:' + e.message; }",
  "try { out.dash = slugify('Hello big world', { maxLength: 6 }); } catch (e) { out.dash = 'throw:' + e.message; }",
  "try { out.none = slugify('Hello World'); } catch (e) { out.none = 'throw:' + e.message; }",
  "console.log(JSON.stringify(out));",
].join("\n");
const EXPECTED_T2 = { cut: "hello-big", dash: "hello", none: "hello-world" };

const nowIso = () => new Date().toISOString();
const sha256Hex = (data) => createHash("sha256").update(data).digest("hex");
const hash12 = (value) => sha256Hex(String(value)).slice(0, 12);
const truncate = (value, max) => (typeof value === "string" && value.length > max ? `${value.slice(0, max)}…` : value);
const ensureDir = (path) => mkdirSync(path, { recursive: true, mode: 0o700 });
const writeJson = (path, value) => writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);

function parseBody(body) {
  if (body === undefined || body === null) return null;
  if (Buffer.isBuffer(body)) return parseBody(body.toString("utf8"));
  if (typeof body === "string") {
    try {
      return JSON.parse(body);
    } catch {
      return { unparsed_text: truncate(redactText(body), 300) };
    }
  }
  return body;
}

function decodeBody(buffer, encoding) {
  const enc = String(encoding ?? "").trim().toLowerCase();
  if (!enc || enc === "identity") return buffer;
  if (enc === "gzip" || enc === "x-gzip") return zlib.gunzipSync(buffer);
  if (enc === "deflate") return zlib.inflateSync(buffer);
  if (enc === "br") return zlib.brotliDecompressSync(buffer);
  if (enc === "zstd" && typeof zlib.zstdDecompressSync === "function") return zlib.zstdDecompressSync(buffer);
  throw new Error(`unsupported content-encoding ${enc}`);
}

// ---------------------------------------------------------------------------
// Gemini request/response structure (no values, no secrets)
// ---------------------------------------------------------------------------

const GEN_PATH_RE = /\/models\/([^/:?]+):(streamGenerateContent|generateContent)/;

function splitUrl(rawUrl) {
  const url = new URL(rawUrl, "http://tap.invalid");
  const queryKeys = [...url.searchParams.keys()].sort();
  const match = url.pathname.match(GEN_PATH_RE);
  return {
    pathname_template: match ? url.pathname.replace(`/models/${match[1]}:`, "/models/{model}:") : url.pathname.replace(/\/models\/[^/:?]+/, "/models/{model}"),
    model: match ? decodeURIComponent(match[1]) : null,
    method_kind: match ? match[2] : null,
    query_keys: queryKeys,
    alt: url.searchParams.get("alt"),
    key_query_param_present: url.searchParams.has("key"),
  };
}

function partKind(part) {
  if (!part || typeof part !== "object") return "unknown";
  for (const kind of ["functionCall", "functionResponse", "inlineData", "fileData", "executableCode", "codeExecutionResult"]) {
    if (part[kind] !== undefined) return kind;
  }
  if (typeof part.text === "string") return part.thought === true ? "thought_text" : "text";
  return `other:${Object.keys(part).sort().join("+")}`;
}

function summarizeContents(contents) {
  const out = {
    count: 0,
    roles: [],
    model_turns_with_function_call: 0,
    function_call_parts: 0,
    function_call_parts_with_signature: 0,
    first_function_call_in_turn_signed: [],
    function_call_signature_hashes: [],
    function_call_names: [],
    text_parts_with_signature: 0,
    function_response_parts: 0,
    thought_parts: 0,
    part_kind_counts: {},
    // Per model turn (wire order), one entry per functionCall part: {signed, hash}. Required by judgeReplayedTurns.
    model_turn_calls: [],
  };
  if (!Array.isArray(contents)) return out;
  out.count = contents.length;
  for (const content of contents) {
    out.roles.push(content?.role ?? null);
    const parts = Array.isArray(content?.parts) ? content.parts : [];
    let firstCallSeen = false;
    const turnCalls = [];
    for (const part of parts) {
      const kind = partKind(part);
      out.part_kind_counts[kind] = (out.part_kind_counts[kind] ?? 0) + 1;
      const signed = typeof part?.thoughtSignature === "string" && part.thoughtSignature.length > 0;
      if (kind === "functionCall") {
        out.function_call_parts += 1;
        out.function_call_names.push(part.functionCall?.name ?? null);
        turnCalls.push({ signed, hash: signed ? hash12(part.thoughtSignature) : null });
        if (signed) {
          out.function_call_parts_with_signature += 1;
          out.function_call_signature_hashes.push(hash12(part.thoughtSignature));
        }
        if (!firstCallSeen) {
          firstCallSeen = true;
          out.model_turns_with_function_call += 1;
          out.first_function_call_in_turn_signed.push(signed);
        }
      } else if (kind === "functionResponse") {
        out.function_response_parts += 1;
      } else if (kind === "thought_text") {
        out.thought_parts += 1;
      } else if (kind === "text" && signed) {
        out.text_parts_with_signature += 1;
      }
    }
    if (turnCalls.length) out.model_turn_calls.push(turnCalls);
  }
  return out;
}

function summarizeRequest(req, rawBody) {
  const url = splitUrl(req.url ?? "/");
  const headerNames = Object.keys(req.headers).map((name) => name.toLowerCase()).sort();
  const summary = {
    method: req.method,
    ...url,
    header_names: headerNames,
    auth_kind: {
      x_goog_api_key: headerNames.includes("x-goog-api-key"),
      authorization: headerNames.includes("authorization"),
      key_query_param: url.key_query_param_present,
    },
    user_agent_family: truncate(String(req.headers["user-agent"] ?? "").split(/[\s/]/)[0] || null, 40),
    body_bytes: rawBody.length,
    body_sha12: rawBody.length ? sha256Hex(rawBody).slice(0, 12) : null,
  };
  let body = null;
  if (rawBody.length) {
    try {
      body = JSON.parse(rawBody.toString("utf8"));
    } catch {
      summary.body_parse_error = true;
    }
  }
  if (body && typeof body === "object") {
    const gen = body.generationConfig ?? {};
    const thinking = gen.thinkingConfig ?? null;
    const declarations = (Array.isArray(body.tools) ? body.tools : []).flatMap((tool) => (Array.isArray(tool?.functionDeclarations) ? tool.functionDeclarations : []));
    summary.top_level_keys = Object.keys(body).sort();
    summary.generation_config_keys = Object.keys(gen).sort();
    summary.max_output_tokens = gen.maxOutputTokens ?? null;
    summary.thinking_config = thinking ? { keys: Object.keys(thinking).sort(), thinkingLevel: thinking.thinkingLevel ?? null, thinkingBudget: thinking.thinkingBudget ?? null, includeThoughts: thinking.includeThoughts ?? null } : null;
    summary.tool_kinds = (Array.isArray(body.tools) ? body.tools : []).map((tool) => Object.keys(tool ?? {}).sort().join("+"));
    summary.function_declaration_names = declarations.map((decl) => decl?.name ?? null).sort();
    summary.tool_config_keys = body.toolConfig ? Object.keys(body.toolConfig).sort() : null;
    summary.system_instruction = body.systemInstruction ? { present: true, sha12: hash12(JSON.stringify(body.systemInstruction)) } : { present: false };
    summary.cached_content_present = body.cachedContent !== undefined;
    summary.model_field = body.model ?? null;
    summary.contents = summarizeContents(body.contents);
  }
  summary.shape_fingerprint = hash12(
    JSON.stringify({
      method: summary.method,
      path: summary.pathname_template,
      query: summary.query_keys,
      headers: headerNames.filter((name) => !["content-length", "transfer-encoding", "connection"].includes(name)),
      top: summary.top_level_keys ?? null,
      gen: summary.generation_config_keys ?? null,
      max: summary.max_output_tokens ?? null,
      thinking: summary.thinking_config ?? null,
      tools: summary.tool_kinds ?? null,
      decl: summary.function_declaration_names ?? null,
      toolConfig: summary.tool_config_keys ?? null,
      sys: summary.system_instruction?.present ?? null,
      cached: summary.cached_content_present ?? null,
    }),
  );
  return summary;
}

function absorbResponseObject(obj, acc) {
  if (!obj || typeof obj !== "object") return;
  if (obj.error) {
    acc.error = { code: obj.error.code ?? null, status: obj.error.status ?? null, message: truncate(redactText(String(obj.error.message ?? "")), 300) };
    return;
  }
  if (obj.responseId) acc.response_ids.add(obj.responseId);
  if (obj.modelVersion) acc.model_versions.add(obj.modelVersion);
  if (obj.usageMetadata) acc.usage = obj.usageMetadata;
  for (const candidate of Array.isArray(obj.candidates) ? obj.candidates : []) {
    if (candidate?.finishReason) acc.finish_reasons.add(candidate.finishReason);
    for (const part of Array.isArray(candidate?.content?.parts) ? candidate.content.parts : []) {
      const kind = partKind(part);
      acc.part_kind_counts[kind] = (acc.part_kind_counts[kind] ?? 0) + 1;
      if (typeof part?.thoughtSignature === "string" && part.thoughtSignature.length > 0) {
        acc.signature_hashes.push(hash12(part.thoughtSignature));
        if (kind === "functionCall") acc.function_call_signature_hashes.push(hash12(part.thoughtSignature));
        acc.signature_on_kinds[kind] = (acc.signature_on_kinds[kind] ?? 0) + 1;
      }
    }
  }
}

function summarizeResponse(status, headers, buffer, truncated) {
  const contentType = String(headers["content-type"] ?? "");
  const out = {
    status,
    content_type: contentType,
    request_id: headers["request-id"] ?? headers["x-request-id"] ?? null,
    header_names: Object.keys(headers).map((name) => name.toLowerCase()).sort(),
    sse: contentType.includes("text/event-stream"),
    parse_truncated: truncated,
  };
  const acc = { response_ids: new Set(), model_versions: new Set(), finish_reasons: new Set(), usage: null, part_kind_counts: {}, signature_hashes: [], function_call_signature_hashes: [], signature_on_kinds: {}, error: null };
  try {
    const text = decodeBody(buffer, headers["content-encoding"]).toString("utf8");
    if (out.sse) {
      let events = 0;
      for (const block of text.split(/\r?\n\r?\n/)) {
        const data = block.split(/\r?\n/).filter((line) => line.startsWith("data:")).map((line) => line.slice(5).trimStart()).join("\n");
        if (!data || data === "[DONE]") continue;
        events += 1;
        try {
          absorbResponseObject(JSON.parse(data), acc);
        } catch {
          out.sse_unparsed_events = (out.sse_unparsed_events ?? 0) + 1;
        }
      }
      out.sse_events = events;
    } else if (text.length) {
      const parsed = parseBody(text);
      if (Array.isArray(parsed)) parsed.forEach((item) => absorbResponseObject(item, acc));
      else absorbResponseObject(parsed, acc);
      if (parsed?.unparsed_text) out.unparsed_excerpt = parsed.unparsed_text;
    }
  } catch (error) {
    out.decode_error = String(error?.message ?? error);
  }
  out.response_ids = [...acc.response_ids];
  out.model_versions = [...acc.model_versions];
  out.finish_reasons = [...acc.finish_reasons];
  out.usage = acc.usage;
  out.part_kind_counts = acc.part_kind_counts;
  out.signature_hashes = acc.signature_hashes;
  out.signature_on_kinds = acc.signature_on_kinds;
  out.function_call_signature_hashes = acc.function_call_signature_hashes;
  out.error = acc.error;
  return out;
}

function classifyFailure(response) {
  if (!response || (response.status >= 200 && response.status < 300)) return null;
  const message = `${response.error?.status ?? ""} ${response.error?.message ?? ""}`;
  if ((response.status === 404 || response.status === 400) && /(not found|not supported|unsupported|is not available|unknown model)/i.test(message)) return "MODEL_UNSUPPORTED";
  if (response.status === 429) return "RATE_OR_LIMIT_REJECTED";
  // cc-lb limit engine could not price the request (observed: Gemini path-model → "model unknown"); deterministic, never retried.
  if (response.status === 503 && /cost limits unavailable/i.test(message)) return "CCLB_COST_LIMIT_UNAVAILABLE";
  if (response.status === 401 || response.status === 403) return "AUTH_REJECTED";
  if (response.status === 400 && /thought[_ ]?signature/i.test(message)) return "THOUGHT_SIGNATURE_REJECTED";
  return `HTTP_${response.status}`;
}

// ---------------------------------------------------------------------------
// Local passive tap
// ---------------------------------------------------------------------------

function startTap({ targetHost, targetPort, state }) {
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("error", () => res.destroy());
    req.on("end", () => {
      const rawBody = Buffer.concat(chunks);
      const startedAt = Date.now();
      const run = state.currentRun;
      const record = { seq: state.seq++, run: run?.id ?? null, at: nowIso(), forwarded: true, injected_locally: false, request: summarizeRequest(req, rawBody) };
      let done = false;
      const finish = () => {
        if (done) return;
        done = true;
        state.records.push(record);
        state.log.write(`${JSON.stringify(record)}\n`);
      };
      const refuse = (reason) => {
        record.forwarded = false;
        record.injected_locally = true;
        record.injected_reason = reason;
        finish();
        res.writeHead(503, { "content-type": "application/json", "x-qa-injected": reason });
        res.end(JSON.stringify({ error: { code: 503, status: "UNAVAILABLE", message: `local QA driver refused: ${reason}`, injected_locally: true } }));
      };
      if (!run) return refuse("no_active_driver_run");
      if (run.stopReason) return refuse(run.stopReason);
      if (state.attempts >= CLIENT_LIMITS.maxGenerationAttempts) return refuse("driver_slice_attempt_budget_exhausted");
      if (state.driverCostUsd >= CLIENT_LIMITS.sliceCostUsd) return refuse("driver_slice_usd_cap_reached");
      if (run.attempts >= run.cap) return refuse("driver_run_attempt_cap_reached");
      state.attempts += 1;
      run.attempts += 1;
      record.attempt_number = state.attempts;
      record.run_attempt_number = run.attempts;
      const headers = { ...req.headers, host: `${targetHost}:${targetPort}` };
      const upstream = http.request({ host: targetHost, port: targetPort, method: req.method, path: req.url, headers }, (upRes) => {
        res.writeHead(upRes.statusCode ?? 502, upRes.headers);
        const captured = [];
        let capturedBytes = 0;
        let truncated = false;
        upRes.on("data", (chunk) => {
          res.write(chunk);
          if (capturedBytes + chunk.length <= RESPONSE_PARSE_CAP) {
            captured.push(chunk);
            capturedBytes += chunk.length;
          } else truncated = true;
        });
        const complete = (interrupted) => {
          if (done) return;
          record.duration_ms = Date.now() - startedAt;
          record.response = summarizeResponse(upRes.statusCode ?? 0, upRes.headers, Buffer.concat(captured), truncated);
          if (interrupted) record.response.interrupted_before_end = true;
          record.failure_class = classifyFailure(record.response);
          record.driver_cost_usd = driverCostUsd(record.response.usage, state.prices[record.request.model]);
          if (typeof record.driver_cost_usd === "number") state.driverCostUsd += record.driver_cost_usd;
          else if (record.response.usage) state.driverCostUnknown = true;
          // Model-unsupported is classified once; never retried under a guessed model id.
          if (record.failure_class === "MODEL_UNSUPPORTED") run.stopReason = "model_unsupported_no_retry";
          if (record.failure_class === "RATE_OR_LIMIT_REJECTED") run.stopReason = "limit_rejected_no_retry";
          if (record.failure_class === "CCLB_COST_LIMIT_UNAVAILABLE") run.stopReason = "cclb_cost_limit_unavailable_no_retry";
          finish();
        };
        upRes.on("end", () => {
          res.end();
          complete(false);
        });
        upRes.on("close", () => complete(true));
        upRes.on("error", (error) => {
          if (!done) record.stream_error = String(error?.message ?? error);
          complete(true);
          res.destroy(error);
        });
      });
      upstream.on("error", (error) => {
        if (done) return;
        record.duration_ms = Date.now() - startedAt;
        record.response = { status: 0, connect_error: String(error?.message ?? error) };
        finish();
        if (res.headersSent) return res.destroy();
        res.writeHead(502, { "content-type": "application/json", "x-qa-injected": "tap_connect_error" });
        res.end(JSON.stringify({ error: { code: 502, message: "tap could not reach cc-lb", injected_locally: true } }));
      });
      res.on("close", () => {
        if (!res.writableFinished) upstream.destroy();
      });
      upstream.end(rawBody);
    });
  });
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve(server));
  });
}

// ---------------------------------------------------------------------------
// Client home / OMP
// ---------------------------------------------------------------------------

function writeClientHome(home, tapPort, keyPath) {
  const agentDir = join(home, ".omp", "agent");
  ensureDir(agentDir);
  for (const sub of ["tmp", ".cache", ".config", ".local/share", ".local/state"]) ensureDir(join(home, sub));
  const model = (id, efforts) => [
    `      - id: ${id}`,
    `        name: ${id} via cc-lb`,
    "        api: google-generative-ai",
    "        reasoning: true",
    "        input: [text]",
    "        contextWindow: 1048576",
    `        maxTokens: ${MAX_OUTPUT_TOKENS}`,
    "        thinking:",
    "          mode: google-level",
    `          efforts: [${efforts.join(", ")}]`,
  ];
  const yaml = [
    "providers:",
    `  ${CLIENT_PROVIDER_ID}:`,
    `    baseUrl: http://127.0.0.1:${tapPort}/v1beta`,
    "    api: google-generative-ai",
    "    auth: apiKey",
    `    apiKey: "!cat '${keyPath}'"`,
    "    models:",
    ...model(PRIMARY_MODEL, ["low", "high"]),
    ...model(CONTROL_MODEL, ["minimal", "low", "medium", "high"]),
    "",
  ].join("\n");
  writeFileSync(join(agentDir, "models.yml"), yaml, { mode: 0o600 });
  // Keep the run to the coding loop: no memory backend, no compaction side requests.
  writeFileSync(join(agentDir, "config.yml"), ["memory:", "  backend: off", "compaction:", "  enabled: false", ""].join("\n"), { mode: 0o600 });
  return agentDir;
}

function clientEnv(home) {
  return {
    PATH: process.env.PATH ?? "/usr/bin:/bin",
    HOME: home,
    USER: process.env.USER ?? "qa",
    LANG: "C.UTF-8",
    TERM: "dumb",
    NO_COLOR: "1",
    TMPDIR: join(home, "tmp"),
    XDG_CONFIG_HOME: join(home, ".config"),
    XDG_CACHE_HOME: join(home, ".cache"),
    XDG_DATA_HOME: join(home, ".local", "share"),
    XDG_STATE_HOME: join(home, ".local", "state"),
    PI_CODING_AGENT_DIR: join(home, ".omp", "agent"),
  };
}

function configuredProviders(agentDir) {
  const yaml = readFileSync(join(agentDir, "models.yml"), "utf8");
  return {
    providers: [...yaml.matchAll(/^ {2}([A-Za-z0-9_-]+):\s*$/gm)].map((match) => match[1]),
    base_urls: [...yaml.matchAll(/baseUrl:\s*(\S+)/g)].map((match) => match[1]),
  };
}

function textOf(value, depth = 0) {
  if (depth > 6 || value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map((item) => textOf(item, depth + 1)).join("\n");
  if (typeof value === "object") return typeof value.text === "string" ? value.text : Object.values(value).map((item) => textOf(item, depth + 1)).join("\n");
  return "";
}

function testCounts(text) {
  const counts = {};
  for (const match of String(text).matchAll(/(?:^|\n)\s*(?:#|ℹ)\s*(tests|pass|fail|cancelled|skipped|todo)\s+(\d+)/g)) counts[match[1]] = Number(match[2]);
  return Object.keys(counts).length ? counts : null;
}

function summarizeToolArgs(name, args) {
  if (!args || typeof args !== "object") return null;
  if (name === "bash") return { command: truncate(args.command ?? args.cmd ?? null, 300) };
  const out = { arg_keys: Object.keys(args).sort() };
  for (const key of ["path", "file", "file_path", "paths"]) if (args[key] !== undefined) out[key] = truncate(JSON.stringify(args[key]), 200);
  return out;
}

function parseOmpOutput(stdout) {
  const result = { session_id: null, session_line: null, tool_calls: [], assistant_messages: [], event_type_counts: {}, unparsed_lines: 0, agent_end: null };
  const pending = new Map();
  for (const line of String(stdout).split("\n")) {
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
      const call = { tool: event.toolName, call_id_hash: event.toolCallId ? hash12(event.toolCallId) : null, args: summarizeToolArgs(event.toolName, event.args) };
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
      const content = Array.isArray(message.content) ? message.content : [];
      result.assistant_messages.push({
        provider: message.provider ?? null,
        api: message.api ?? null,
        model: message.model ?? null,
        stop_reason: message.stopReason ?? null,
        response_id: message.responseId ?? null,
        error_message: truncate(message.errorMessage ?? null, 300),
        usage: message.usage ?? null,
        content_types: content.map((part) => part?.type ?? "unknown"),
        tool_calls_with_signature: content.filter((part) => part?.type === "toolCall" && typeof part.thoughtSignature === "string" && part.thoughtSignature.length > 0).length,
        tool_calls: content.filter((part) => part?.type === "toolCall").length,
        text_excerpt: truncate(textOf(content.filter((part) => part?.type === "text")), 400),
      });
    } else if (type === "agent_end") {
      result.agent_end = { is_terminal: event.isTerminal ?? null, yielded: event.yielded ?? null, message_count: Array.isArray(event.messages) ? event.messages.length : null };
    }
  }
  return result;
}

// Session file evidence: OMP's persisted toolCall thoughtSignature presence (client state that is replayed).
function sessionFileEvidence(sessionDir, sessionId) {
  const files = [];
  const walk = (dir) => {
    for (const entry of existsSync(dir) ? readdirSync(dir, { withFileTypes: true }) : []) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.name.endsWith(".jsonl")) files.push(full);
    }
  };
  walk(sessionDir);
  const matching = files.filter((file) => !sessionId || file.includes(sessionId));
  const out = { session_dir_jsonl_files: files.map((file) => relative(sessionDir, file)), matching_files: matching.map((file) => relative(sessionDir, file)), lines: 0, tool_calls: 0, tool_calls_with_signature: 0, signature_hashes: [] };
  for (const file of matching) {
    for (const line of readFileSync(file, "utf8").split("\n")) {
      if (!line.trim()) continue;
      out.lines += 1;
      let entry;
      try {
        entry = JSON.parse(line);
      } catch {
        continue;
      }
      const visit = (value, depth) => {
        if (depth > 8 || !value || typeof value !== "object") return;
        if (Array.isArray(value)) return value.forEach((item) => visit(item, depth + 1));
        if (value.type === "toolCall") {
          out.tool_calls += 1;
          if (typeof value.thoughtSignature === "string" && value.thoughtSignature.length > 0) {
            out.tool_calls_with_signature += 1;
            out.signature_hashes.push(hash12(value.thoughtSignature));
          }
        }
        for (const child of Object.values(value)) visit(child, depth + 1);
      };
      visit(entry, 0);
    }
  }
  return out;
}

function nodeEval(projectDir, script) {
  const result = spawnSync(process.execPath, ["--input-type=module", "-e", script], { cwd: projectDir, encoding: "utf8", timeout: 30_000, env: { PATH: process.env.PATH ?? "", HOME: projectDir } });
  try {
    return { rc: result.status, value: JSON.parse(String(result.stdout).trim().split("\n").pop()) };
  } catch {
    return { rc: result.status, value: null, stderr_excerpt: truncate(result.stderr, 300) };
  }
}

function fixtureCheck(projectDir, turn) {
  const tests = spawnSync(process.execPath, ["--test", "--test-reporter=tap"], { cwd: projectDir, encoding: "utf8", timeout: 60_000, env: { PATH: process.env.PATH ?? "", HOME: projectDir } });
  const testFile = readFileSync(join(projectDir, "test", "slugify.test.js"), "utf8");
  const originalTest = readFileSync(join(FIXTURE_DIR, "test", "slugify.test.js"), "utf8");
  const originalAssertions = originalTest.split("\n").filter((line) => line.includes("assert.equal("));
  const out = {
    node_test_rc: tests.status,
    node_test_counts: testCounts(`${tests.stdout}\n${tests.stderr}`),
    output_text: `${tests.stdout ?? ""}\n${tests.stderr ?? ""}`,
    original_assertions_preserved: originalAssertions.every((line) => testFile.includes(line.trim())),
    source_changed: readFileSync(join(projectDir, "src", "slugify.js"), "utf8") !== readFileSync(join(FIXTURE_DIR, "src", "slugify.js"), "utf8"),
    turn1_behavior: nodeEval(projectDir, BEHAVIOR_T1),
  };
  out.turn1_ok = out.node_test_rc === 0 && out.original_assertions_preserved && out.source_changed && JSON.stringify(out.turn1_behavior.value) === JSON.stringify(EXPECTED_T1);
  if (turn >= 2) {
    out.turn2_behavior = nodeEval(projectDir, BEHAVIOR_T2);
    out.test_mentions_max_length = /maxLength/.test(testFile);
    out.test_count_grew = (out.node_test_counts?.tests ?? 0) > originalAssertions.length;
    out.turn2_ok = out.turn1_ok && out.test_mentions_max_length && out.test_count_grew && JSON.stringify(out.turn2_behavior.value) === JSON.stringify(EXPECTED_T2);
  }
  return out;
}

// ---------------------------------------------------------------------------
// cc-lb evidence helpers
// ---------------------------------------------------------------------------

// Driver-side cost (provider usage x cc-lb catalog prices); a guard + cross-check, never labelled cc-lb cost.
function loadPrices(ctx, models) {
  const prices = {};
  let catalog = {};
  try {
    catalog = JSON.parse(readFileSync(ctx.priceCatalogPath, "utf8"));
  } catch {
    return prices;
  }
  for (const model of models) {
    // cc-lb contract (cc-lb-pricing lib.rs lookup + catalog_parser.rs): exact catalog key == request model id
    // (normalize_model_id only special-cases one Claude id). Provider-prefixed keys (vertex_ai/…, gemini/…) do NOT price it.
    const key = Object.hasOwn(catalog, model) ? model : null;
    const row = key ? catalog[key] : null;
    if (row && typeof row.input_cost_per_token === "number" && typeof row.output_cost_per_token === "number") {
      prices[model] = {
        key,
        input: row.input_cost_per_token,
        output: row.output_cost_per_token,
        cache_read: typeof row.cache_read_input_token_cost === "number" ? row.cache_read_input_token_cost : row.input_cost_per_token,
        reasoning: typeof row.output_cost_per_reasoning_token === "number" ? row.output_cost_per_reasoning_token : row.output_cost_per_token,
      };
    }
  }
  return prices;
}

function driverCostUsd(usage, price) {
  if (!usage || !price) return null;
  const prompt = Number(usage.promptTokenCount) || 0;
  const cached = Number(usage.cachedContentTokenCount) || 0;
  const output = Number(usage.candidatesTokenCount) || 0;
  const thoughts = Number(usage.thoughtsTokenCount) || 0;
  return Math.max(prompt - cached, 0) * price.input + cached * price.cache_read + output * price.output + thoughts * price.reasoning;
}

function summarizeEvent(event) {
  return {
    request_id: event.request_id ?? null,
    ts_ms: event.ts_ms ?? null,
    event_kind: event.event_kind ?? null,
    source_kind: event.source_kind ?? null,
    principal_id: event.principal_id ?? null,
    key_id: event.key_id ?? null,
    upstream_id: event.upstream_id ?? null,
    upstream_name: event.upstream_name ?? null,
    model: event.model ?? null,
    status: event.status ?? null,
    error_code: event.error_code ?? null,
    upstream_error_type: event.upstream_error_type ?? null,
    upstream_error_message: truncate(redactText(String(event.upstream_error_message ?? "")), 200) || null,
    input_tokens: event.input_tokens ?? null,
    output_tokens: event.output_tokens ?? null,
    cache_read_input_tokens: event.cache_read_input_tokens ?? null,
    thinking_tokens: event.thinking_tokens ?? null,
    cost_usd_micros: event.cost_usd_micros ?? null,
    cost_input_micros: event.cost_input_micros ?? null,
    cost_output_micros: event.cost_output_micros ?? null,
    cost_cache_read_micros: event.cost_cache_read_micros ?? null,
    client_app: event.client_app ?? null,
    session_id_source: event.session_id_source ?? null,
    observed_session_id_present: Boolean(event.observed_session_id),
    duration_ms: event.duration_ms ?? null,
    upstream_ttfb_ms: event.upstream_ttfb_ms ?? null,
  };
}

async function upstreamHosts(ctx) {
  const hosts = new Set();
  for (const upstream of ctx.upstreams ?? []) if (upstream.host) hosts.add(upstream.host);
  const response = await admin(ctx, `/admin/v1/upstreams/${encodeURIComponent(ctx.upstreamId)}`, "GET");
  const body = parseBody(response.body);
  // Only base-URL fields count as provider hosts (other URL-valued fields, e.g. docs links, are ignored).
  const visit = (value, depth) => {
    if (depth > 6 || value === null || typeof value !== "object") return;
    for (const [key, child] of Object.entries(value)) {
      if (typeof child === "string" && /base_?url/i.test(key) && /^https?:\/\//.test(child)) {
        try {
          hosts.add(new URL(child).host);
        } catch {}
      } else visit(child, depth + 1);
    }
  };
  visit(body, 0);
  return { admin_status: response.status, hosts: [...hosts] };
}

// Correlate tap records with committed events by request-id (cc-lb echoes `request-id`).
function correlate(records, events) {
  const byId = new Map(events.map((event) => [event.request_id, event]));
  return records
    .filter((record) => record.forwarded)
    .map((record) => {
      const event = byId.get(record.response?.request_id);
      const usage = record.response?.usage ?? null;
      return {
        seq: record.seq,
        request_id: record.response?.request_id ?? null,
        status_tap: record.response?.status ?? null,
        status_event: event?.status ?? null,
        provider_usage: usage ? { prompt: usage.promptTokenCount ?? null, cached: usage.cachedContentTokenCount ?? null, candidates: usage.candidatesTokenCount ?? null, thoughts: usage.thoughtsTokenCount ?? null, total: usage.totalTokenCount ?? null } : null,
        cclb: event ? { input: event.input_tokens, output: event.output_tokens, cache_read: event.cache_read_input_tokens, thinking: event.thinking_tokens, cost_usd_micros: event.cost_usd_micros } : null,
        event_found: Boolean(event),
      };
    });
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// thoughtSignature replay predicate (shared with the offline adjudicator; pure, no I/O)
//
// Contract source: OMP v18.12.5 packages/ai/src/providers/google-shared.ts (commit d5cd750):
//   SKIP_THOUGHT_SIGNATURE (l.165); resolveThoughtSignature keeps a valid same-provider/model signature on any
//   toolCall (l.176-177, 326); public Gemini sets requiresSkipThoughtSignature, so EVERY unsigned functionCall
//   gets the marker (l.327-330, 340-341). Text/thinking never get the marker.
// Strict QA predicate per model turn with >=1 functionCall, in wire order:
//   first call  : must carry a provider-returned (known) signature; marker / unsigned / unknown => FAIL
//                 (a marker on the first call means the genuine signature was lost or rejected by OMP).
//   later calls : marker OR a known provider signature; unsigned or unknown non-marker => FAIL.
// "known" = sha256-12 of a thoughtSignature the provider returned on a functionCall earlier in this session.
// No claim is made that Google officially sanctions the marker; this only checks OMP's documented behaviour.
// ---------------------------------------------------------------------------

export const SKIP_THOUGHT_SIGNATURE = "skip_thought_signature_validator";
export const SKIP_THOUGHT_SIGNATURE_HASH12 = hash12(SKIP_THOUGHT_SIGNATURE); // parent observed d78f0532165d

export function judgeReplayedTurns(turns, knownHashes) {
  const known = knownHashes instanceof Set ? knownHashes : new Set(knownHashes ?? []);
  const rows = (turns ?? []).map((calls, turn) => {
    const violations = [];
    let markers = 0;
    let genuine = 0;
    calls.forEach((call, index) => {
      const marker = Boolean(call?.signed) && call.hash === SKIP_THOUGHT_SIGNATURE_HASH12;
      const isKnown = Boolean(call?.signed) && !marker && known.has(call.hash);
      if (marker) markers += 1;
      if (isKnown) genuine += 1;
      if (index === 0) {
        if (!call?.signed) violations.push("first_unsigned");
        else if (marker) violations.push("first_is_skip_marker");
        else if (!isKnown) violations.push("first_signature_unknown_or_corrupt");
      } else if (!call?.signed) violations.push(`sibling${index}_unsigned`);
      else if (!marker && !isKnown) violations.push(`sibling${index}_signature_unknown`);
    });
    return { turn, calls: calls.length, genuine, markers, violations };
  });
  return { ok: rows.length > 0 && rows.every((row) => row.calls > 0 && row.violations.length === 0), rows };
}

// Rebuild per-turn call groups from a legacy (flat) request summary using externally proven per-turn
// functionCall counts (e.g. OMP session JSONL assistant toolCall counts). Returns {turns} or {inconclusive}.
export function regroupFlatCalls(contents, perTurnCounts) {
  const k = contents?.model_turns_with_function_call ?? 0;
  const counts = (perTurnCounts ?? []).slice(0, k);
  const total = counts.reduce((a, b) => a + b, 0);
  const flat = contents?.function_call_signature_hashes ?? [];
  if (counts.length !== k) return { inconclusive: `session_has_${perTurnCounts?.length ?? 0}_tool_turns_request_has_${k}` };
  if (total !== contents.function_call_parts) return { inconclusive: `count_mismatch_session_${total}_wire_${contents.function_call_parts}` };
  // Flat hashes only list signed parts; 1:1 alignment is proven only when every part was signed.
  if (contents.function_call_parts_with_signature !== contents.function_call_parts || flat.length !== total) {
    return { inconclusive: `unsigned_parts_present_alignment_unprovable_${contents.function_call_parts_with_signature}/${contents.function_call_parts}` };
  }
  const turns = [];
  let offset = 0;
  for (const count of counts) {
    turns.push(flat.slice(offset, offset + count).map((hash) => ({ signed: true, hash })));
    offset += count;
  }
  const firstSigned = contents.first_function_call_in_turn_signed ?? [];
  if (firstSigned.length !== k || !firstSigned.every(Boolean)) return { inconclusive: "first_call_signed_flags_disagree" };
  return { turns };
}

export function responseCallHashes(record) {
  return record?.response?.function_call_signature_hashes ?? record?.response?.signature_hashes ?? [];
}

function signatureReplayAssertion(runRecords, priorResponseHashes) {
  const generation = runRecords.filter((r) => r.forwarded && r.request?.method_kind);
  const seen = new Set(priorResponseHashes);
  const rows = [];
  for (const record of generation) {
    const contents = record.request.contents ?? {};
    if ((contents.function_call_parts ?? 0) > 0) {
      const judged = judgeReplayedTurns(contents.model_turn_calls ?? [], seen);
      rows.push({ seq: record.seq, model_turns_with_function_call: contents.model_turns_with_function_call, function_call_parts: contents.function_call_parts, ok: judged.ok, turns: judged.rows });
    }
    for (const h of responseCallHashes(record)) seen.add(h);
  }
  return { requests_with_function_call_history: rows.length, rows, ok: rows.length > 0 && rows.every((row) => row.ok), response_hashes_seen: seen.size, marker_hash12: SKIP_THOUGHT_SIGNATURE_HASH12 };
}

function shapeComparison(primaryFirst, controlFirst) {
  if (!primaryFirst || !controlFirst) return { ok: false, reason: "missing_first_request" };
  const pick = (r) => ({
    method: r.method,
    pathname_template: r.pathname_template,
    query_keys: r.query_keys,
    header_names: r.header_names.filter((n) => !["content-length", "transfer-encoding", "connection"].includes(n)),
    auth_kind: r.auth_kind,
    top_level_keys: r.top_level_keys,
    generation_config_keys: r.generation_config_keys,
    max_output_tokens: r.max_output_tokens,
    thinking_config: r.thinking_config,
    tool_kinds: r.tool_kinds,
    function_declaration_names: r.function_declaration_names,
    tool_config_keys: r.tool_config_keys,
    system_instruction_present: r.system_instruction?.present,
    cached_content_present: r.cached_content_present,
    contents_roles: r.contents?.roles,
    contents_part_kinds: r.contents?.part_kind_counts,
  });
  const a = pick(primaryFirst);
  const b = pick(controlFirst);
  const differing = Object.keys(a).filter((key) => JSON.stringify(a[key]) !== JSON.stringify(b[key]));
  return {
    ok: differing.length === 0 && primaryFirst.model === PRIMARY_MODEL && controlFirst.model === CONTROL_MODEL,
    models: [primaryFirst.model, controlFirst.model],
    shape_fingerprints: [primaryFirst.shape_fingerprint, controlFirst.shape_fingerprint],
    differing_fields: differing,
    system_instruction_sha12: [primaryFirst.system_instruction?.sha12 ?? null, controlFirst.system_instruction?.sha12 ?? null],
  };
}

function runTotals(correlated) {
  const sum = (pick) => correlated.reduce((acc, row) => acc + (Number(pick(row)) || 0), 0);
  const costs = correlated.map((row) => row.cclb?.cost_usd_micros);
  return {
    forwarded: correlated.length,
    provider_prompt_tokens: sum((r) => r.provider_usage?.prompt),
    provider_cached_tokens: sum((r) => r.provider_usage?.cached),
    provider_output_tokens: sum((r) => r.provider_usage?.candidates),
    provider_thought_tokens: sum((r) => r.provider_usage?.thoughts),
    cclb_input_tokens: sum((r) => r.cclb?.input),
    cclb_output_tokens: sum((r) => r.cclb?.output),
    cclb_cache_read_tokens: sum((r) => r.cclb?.cache_read),
    cclb_cost_usd_micros: costs.every((c) => typeof c === "number") ? sum((r) => r.cclb?.cost_usd_micros) : null,
  };
}

function evaluateRun(run, ctx, hostInfo) {
  const problems = [];
  const blocked = [];
  const forwarded = run.tap.filter((record) => record.forwarded);
  const injected = run.tap.filter((record) => record.injected_locally);
  if (run.process.timedOut) problems.push("client_timeout");
  if (run.process.code !== 0) problems.push(`client_exit_${run.process.code ?? run.process.signal}`);
  if (!run.omp.session_id) problems.push("no_session_id");
  if (forwarded.length === 0) problems.push("no_forwarded_requests");
  for (const record of forwarded) {
    const status = record.response?.status ?? 0;
    if (record.failure_class === "MODEL_UNSUPPORTED") blocked.push(`model_unsupported:${record.request?.model}`);
    else if (record.failure_class === "RATE_OR_LIMIT_REJECTED") blocked.push("limit_or_rate_rejected_429");
    else if (record.failure_class === "CCLB_COST_LIMIT_UNAVAILABLE") blocked.push(`pricing_unavailable_in_cclb_catalog:${record.request?.model}`);
    else if (status < 200 || status >= 300) problems.push(`http_${status}:${record.failure_class}`);
    if (record.request?.model && record.request.model !== run.spec.model) problems.push(`client_model_mismatch:${record.request.model}`);
    if (record.request?.max_output_tokens !== MAX_OUTPUT_TOKENS) problems.push(`max_output_tokens_observed=${record.request?.max_output_tokens}`);
    if (record.request?.cached_content_present) problems.push("unexpected_cachedContent_from_client");
    if (status >= 200 && status < 300) {
      if (!record.response?.response_ids?.length) problems.push(`no_provider_response_id:seq${record.seq}`);
      const usage = record.response?.usage;
      if (!usage || !(Number(usage.promptTokenCount) > 0) || !(Number(usage.totalTokenCount) > 0)) problems.push(`no_nonzero_usage:seq${record.seq}`);
    }
  }
  if (injected.length) problems.push(`locally_refused_requests:${injected.map((record) => record.injected_reason).join(",")}`);
  if (!run.events.complete) problems.push(`events_incomplete:missing=${run.events.missing.length}`);
  if (run.events.invalidated) problems.push("observability_invalidated_drop_counter_increase");
  for (const event of run.events.summaries) {
    if (event.upstream_id && event.upstream_id !== ctx.upstreamId) problems.push(`unexpected_upstream:${event.upstream_id}`);
    if (event.principal_id && event.principal_id !== ctx.principalId) problems.push(`unexpected_principal:${event.principal_id}`);
    if (event.status >= 200 && event.status < 300 && event.cost_usd_micros === null) blocked.push("cost_missing_on_success_event");
  }
  for (const row of run.correlated) if (!row.event_found) problems.push(`event_missing_for_request:${row.request_id}`);
  if (!(hostInfo.hosts.length > 0 && hostInfo.hosts.every((host) => host === EXPECTED_PROVIDER_HOST))) problems.push(`provider_host_not_real:${hostInfo.hosts.join(",")}`);
  const tools = new Set(run.omp.tool_calls.map((call) => call.tool));
  if (run.spec.turn === 1 && !tools.has("read")) problems.push("no_read_tool_call");
  if (!(tools.has("edit") || tools.has("write"))) problems.push("no_edit_tool_call");
  const bashTests = run.omp.tool_calls.filter((call) => call.tool === "bash" && /node\s+--test/.test(call.args?.command ?? ""));
  if (bashTests.length === 0) problems.push("no_node_test_bash_call");
  if (run.spec.turn === 1 && !run.fixture.turn1_ok) problems.push("fixture_turn1_check_failed");
  if (run.spec.turn === 2 && !run.fixture.turn2_ok) problems.push("fixture_turn2_check_failed");
  return { problems: [...new Set(problems)], blocked: [...new Set(blocked)] };
}

function verdictOf(problems, blocked, prerequisiteBlocked = []) {
  if (prerequisiteBlocked.length || blocked.length) return "BLOCKED";
  return problems.length ? "FAIL" : "PASS";
}

// ---------------------------------------------------------------------------
// Collector-shaped evidence (evidence/coverage.json driver_manifest)
// ---------------------------------------------------------------------------

const SESSION_HEADER_RE = /(session|turn|conversation|thread|affinity)/i;

function toolKind(name) {
  if (["read", "grep", "glob", "find"].includes(name)) return "read";
  if (["edit", "write"].includes(name)) return "edit";
  if (name === "bash") return "exec";
  return "other";
}

function collectorRun(result, ctx) {
  const eventsById = new Map((result.events?.summaries ?? []).map((event) => [event.request_id, event]));
  return {
    label: result.label,
    scenarioIds: result.spec.scenarioIds,
    kind: "cli",
    model: result.spec.model,
    sessionId: result.omp?.session_id ?? null,
    resumeOfSessionId: result.spec.turn > 1 ? result.session?.expected_id ?? null : null,
    resumeSelector: result.spec.turn > 1 ? "exact_id" : null,
    turn: result.spec.turn,
    exitCode: result.process?.code ?? null,
    timedOut: result.process?.timedOut ?? null,
    wallSeconds: typeof result.process?.durationMs === "number" ? result.process.durationMs / 1000 : null,
    cliOutputRef: `evidence/${result.label}.stdout`,
    toolCalls: (result.omp?.tool_calls ?? []).map((call) => ({ name: call.tool, kind: toolKind(call.tool), ok: call.is_error === undefined ? null : !call.is_error })),
    fixtureTest: result.fixture ? { command: "node --test --test-reporter=tap", exitCode: result.fixture.node_test_rc, outputRef: `evidence/${result.label}-fixture-test.txt` } : null,
    wire: (result.tap ?? []).map((record) => {
      const request = record.request ?? {};
      const response = record.response ?? {};
      const usage = response.usage ?? null;
      const firstResponseId = response.response_ids?.[0] ?? null;
      const event = eventsById.get(response.request_id);
      return {
        seq: record.seq,
        transport: "http",
        connectionId: null,
        fullReplay: null,
        generation: Boolean(request.method_kind),
        endpoint: request.pathname_template ?? null,
        apiFamily: request.method_kind === "streamGenerateContent" ? "gemini_stream" : request.method_kind === "generateContent" ? "gemini_generate" : null,
        model: request.model ?? null,
        stream: request.method_kind === "streamGenerateContent" ? true : request.method_kind === "generateContent" ? false : null,
        store: null,
        previousResponseIdPresent: false,
        sessionTurnHeaderPresent: (request.header_names ?? []).some((name) => SESSION_HEADER_RE.test(name)),
        functionCallReplayed: (request.contents?.function_call_parts ?? 0) > 0,
        thoughtSignatureReplayedPresent: (request.contents?.function_call_parts ?? 0) > 0 && request.contents.first_function_call_in_turn_signed.every(Boolean) && request.contents.function_call_parts_with_signature > 0,
        cachedContentPresent: request.cached_content_present ?? null,
        maxOutputTokens: request.max_output_tokens ?? null,
        thinkingLevel: request.thinking_config?.thinkingLevel ?? null,
        httpStatus: response.status ?? (record.injected_locally ? 503 : null),
        providerResponseIdPresent: Boolean(firstResponseId),
        providerResponseIdSha256: firstResponseId ? sha256Hex(firstResponseId) : null,
        providerUsage: usage ? { input: usage.promptTokenCount ?? null, cached: usage.cachedContentTokenCount ?? 0, output: usage.candidatesTokenCount ?? null, reasoning: usage.thoughtsTokenCount ?? 0 } : null,
        requestId: response.request_id ?? null,
        upstreamId: event?.upstream_id ?? null,
        forwarded: record.forwarded,
        injected: record.injected_locally,
        injectedReason: record.injected_reason ?? null,
        source: "proxy_wire_capture",
        ref: `evidence/tap.jsonl#seq=${record.seq}`,
      };
    }),
    metrics: { before: `${result.label}-before`, after: `${result.label}-after` },
    budgetTap: { enforced: true, generationFrames: (result.tap ?? []).filter((record) => record.forwarded).length, attemptsCap: CLIENT_LIMITS.maxGenerationAttempts, runCap: result.spec.cap, usdCap: CLIENT_LIMITS.sliceCostUsd, deadlineSecs: CLIENT_LIMITS.turnTimeoutSecs },
    driverCostUsd: (result.tap ?? []).every((record) => !record.response?.usage || typeof record.driver_cost_usd === "number")
      ? (result.tap ?? []).reduce((acc, record) => acc + (record.driver_cost_usd ?? 0), 0)
      : null,
    principalId: ctx.principalId,
    keyId: ctx.keyId ?? null,
    verdictInputs: { problems: result.problems ?? [], blocked: result.blocked ?? [] },
    ref: `evidence/${result.label}.run.json`,
  };
}

function blockedEntry(scenarioId, reason, ref) {
  const kind = /cost|pric/.test(reason) ? "pricing" : /model_unsupported|client_limited/.test(reason) ? "capability" : /budget|cap_reached|limit|429/.test(reason) ? "budget" : /credential|home_not_clean/.test(reason) ? "credential" : "capability";
  return { scenarioId, kind, prerequisite: reason, ref };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const summary = { slice: SLICE, started_at: nowIso(), scenarios: {}, runs: [], blocked: [], notes: [] };
  summary.notes.push("cachedContent: not native OMP coverage; separate raw-HTTP server-contract probe outside actual-CLI totals.");
  summary.notes.push("Tap: local owned pass-through, no payload rewrite; Host set to cc-lb authority; locally refused requests labeled injected_locally.");

  const localSha = await sha256File(BINARY_PATH);
  // QA_RESUME=1: recover THIS slice's owned, still-running runtime (prepareSlice resume:true; never a new DB).
  const resume = process.env.QA_RESUME === "1";
  const ctx = await prepareSlice({ name: SLICE, artifact: { path: BINARY_PATH, expected_sha256: EXPECTED_BINARY_SHA256 }, ports: PORTS, provider: PROVIDER, model: PRIMARY_MODEL, pricedModels: [PRIMARY_MODEL, CONTROL_MODEL], resume });
  summary.resume = resume ? { resumed: true, attempt: ATTEMPT, recovered: ctx.recovered ?? null } : { resumed: false, attempt: ATTEMPT };
  const evidenceDir = ctx.evidenceDir;
  ensureDir(evidenceDir);
  summary.binary_sha256 = { local: localSha, runtime: ctx.binarySha256 ?? null, expected: EXPECTED_BINARY_SHA256 };
  summary.principal_id = ctx.principalId;
  summary.key_id = ctx.keyId ?? null;
  summary.upstream_id = ctx.upstreamId;
  summary.server_limits = ctx.limits?.server ?? null;
  summary.cost_catalog = ctx.costCatalog ?? null;
  const prerequisiteBlocked = [];
  if (localSha !== EXPECTED_BINARY_SHA256 || ctx.binarySha256 !== EXPECTED_BINARY_SHA256) prerequisiteBlocked.push("binary_sha256_mismatch");
  // Exact-key pricing precheck per cc-lb lookup contract; an unpriced valid model is BLOCKED(pricing), never FAIL.
  const exactPriced = loadPrices(ctx, [PRIMARY_MODEL, CONTROL_MODEL]);
  summary.pricing_precheck = {
    contract: "cc-lb-pricing lookup: exact catalog key == model id",
    models: Object.fromEntries([PRIMARY_MODEL, CONTROL_MODEL].map((model) => [model, exactPriced[model] ? { key: exactPriced[model].key } : null])),
    helper_matched_keys: ctx.costCatalog?.matchedKeys ?? null,
  };
  for (const model of [PRIMARY_MODEL, CONTROL_MODEL]) if (!exactPriced[model]) prerequisiteBlocked.push(`pricing_unavailable_exact_catalog_key:${model}`);
  if (Array.isArray(ctx.blocked) && ctx.blocked.length) prerequisiteBlocked.push(...ctx.blocked.map((item) => `runtime:${item}`));
  if (!Array.isArray(ctx.limits?.server) || ctx.limits.server.length === 0) prerequisiteBlocked.push("server_limits_missing");

  const ompPath = String(spawnSync("/bin/sh", ["-c", "command -v omp"], { encoding: "utf8", env: { PATH: process.env.PATH ?? "" } }).stdout || "").trim();
  const ompBinary = ompPath ? realpathSync(ompPath) : null;
  const ompVersion = spawnSync(ompBinary ?? "omp", ["--version"], { encoding: "utf8", timeout: 30_000, env: { PATH: process.env.PATH ?? "", HOME: ctx.clientHome } });
  const versionText = String(ompVersion.stdout || "").trim();
  writeFileSync(join(evidenceDir, "omp-version.txt"), `${versionText}\n`);
  summary.client = { name: "omp", binaryPath: ompBinary, launcherPath: ompPath || null, version: versionText, expectedVersion: EXPECTED_OMP_VERSION, versionCommandRef: "evidence/omp-version.txt" };
  if (!versionText.includes(EXPECTED_OMP_VERSION)) summary.notes.push(`omp_version_differs:${versionText}`);
  if (!ompBinary) prerequisiteBlocked.push("omp_binary_not_found");

  const hostInfo = await upstreamHosts(ctx);
  summary.real_provider_host = hostInfo;
  if (!hostInfo.hosts.includes(EXPECTED_PROVIDER_HOST)) prerequisiteBlocked.push(`provider_host_unexpected:${hostInfo.hosts.join(",")}`);

  if (prerequisiteBlocked.length) {
    for (const id of Object.values(SCENARIO)) {
      summary.scenarios[id] = { verdict: "BLOCKED", reasons: prerequisiteBlocked };
      for (const reason of prerequisiteBlocked) summary.blocked.push(blockedEntry(id, reason, "summary.json#prerequisites"));
    }
    summary.prerequisites = prerequisiteBlocked;
    summary.verdict = "BLOCKED";
    writeJson(join(evidenceDir, "omp-gemini-summary.json"), summary);
    await finishEvidence(ctx, summary);
    return summary;
  }

  const proxy = new URL(ctx.proxyUrl);
  const tapLog = createWriteStream(join(evidenceDir, "tap.jsonl"), { flags: "a", mode: 0o600 });
  const state = { seq: 0, attempts: 0, records: [], currentRun: null, log: tapLog, prices: loadPrices(ctx, [PRIMARY_MODEL, CONTROL_MODEL]), driverCostUsd: 0, driverCostUnknown: false };
  if (resume) {
    // Prior attempts on this principal count toward the same slice bounds.
    const prior = await budgetStatus(ctx);
    state.attempts = Number(prior.attempts) || 0;
    state.driverCostUsd = Number(prior.costUsd) || 0;
    summary.resume.prior_budget = { attempts: prior.attempts, costUsd: prior.costUsd, costKnown: prior.costKnown };
  }
  summary.driver_prices = state.prices;
  for (const model of [PRIMARY_MODEL, CONTROL_MODEL]) if (!state.prices[model]) summary.notes.push(`driver_price_missing:${model} (driver USD guard falls back to cc-lb principal cost_usd limit only)`);
  const tap = await startTap({ targetHost: proxy.hostname, targetPort: Number(proxy.port || 80), state });
  const tapPort = tap.address().port;
  const configGuard = {
    fakeOrDirectFallback: false,
    providerBaseUrlIsProxy: true,
    providerBaseUrlNote: "models.yml baseUrl is the local passive tap, which forwards unchanged to the cc-lb proxy listener only",
    credentialFileNamesScanned: [],
    realAuthCopied: false,
    scans: [],
    ref: "work/<session> client homes under home/<session>/.omp/agent/models.yml",
  };
  summary.configGuard = configGuard;
  summary.observation_tap = { kind: "local owned passive pass-through tap", listen: `127.0.0.1:${tapPort}`, target: `${proxy.hostname}:${proxy.port}` };
  await recordResource(ctx, { kind: "in-process-listener", id: `omp-gemini-tap 127.0.0.1:${tapPort} pid ${process.pid}`, delete: "Exits with run.mjs process", created_at: nowIso() });

  // Fresh per-attempt project/home/session dirs so a resumed attempt never reuses an edited fixture or session.
  const workRoot = join(ctx.root, "work", ATTEMPT);
  const sessions = {};
  const runResults = {};
  const runList = [];
  let budgetBlockedReason = null;
  try {
    for (const spec of RUNS) {
      if (!sessions[spec.session]) {
        const base = join(workRoot, spec.session);
        const home = join(ctx.clientHome, ATTEMPT, spec.session);
        const projectDir = join(base, "project");
        const sessionDir = join(base, "omp-sessions");
        ensureDir(projectDir);
        ensureDir(sessionDir);
        cpSync(FIXTURE_DIR, projectDir, { recursive: true });
        const agentDir = writeClientHome(home, tapPort, ctx.clientKeyPath);
        sessions[spec.session] = { home, agentDir, projectDir, sessionDir, sessionId: null };
        await recordResource(ctx, { kind: "scratch-directory", id: base, delete: `rm -rf '${base}'`, created_at: nowIso() });
        await recordResource(ctx, { kind: "client-home", id: home, delete: `rm -rf '${home}'`, created_at: nowIso() });
      }
      const session = sessions[spec.session];
      const result = { id: spec.id, label: `${spec.id}${LABEL_SUFFIX}`, spec: { session: spec.session, model: spec.model, turn: spec.turn, cap: spec.cap, scenarioIds: spec.scenarioIds } };
      runResults[spec.id] = result;
      runList.push(result);
      if (spec.turn > 1 && !session.sessionId) {
        result.verdict = "BLOCKED";
        result.reasons = ["turn1_session_id_missing"];
        continue;
      }
      const credentialFiles = await scanCredentialFiles(session.home);
      const providers = configuredProviders(session.agentDir);
      result.preflight = { credential_like_files: credentialFiles.map((hit) => ({ path: relative(session.home, hit.path), size: hit.size })), configured: providers };
      configGuard.credentialFileNamesScanned.push(...credentialFiles.map((hit) => `${spec.id}:${relative(session.home, hit.path)}`));
      configGuard.scans.push({ run: spec.id, hits: credentialFiles.length, providers: providers.providers, base_urls: providers.base_urls });
      const baseUrlOk = providers.base_urls.every((url) => url.startsWith(`http://127.0.0.1:${tapPort}/`));
      if ((spec.turn === 1 && credentialFiles.length) || providers.providers.join(",") !== CLIENT_PROVIDER_ID || !baseUrlOk) {
        result.verdict = "BLOCKED";
        result.reasons = ["client_home_not_clean_or_extra_provider"];
        configGuard.fakeOrDirectFallback = providers.providers.join(",") !== CLIENT_PROVIDER_ID;
        configGuard.providerBaseUrlIsProxy = configGuard.providerBaseUrlIsProxy && baseUrlOk;
        continue;
      }
      if (budgetBlockedReason) {
        result.verdict = "BLOCKED";
        result.reasons = [budgetBlockedReason];
        continue;
      }
      try {
        result.budget_before = await assertBudget(ctx);
      } catch (error) {
        budgetBlockedReason = `budget:${redactText(String(error.message))}`;
        result.verdict = "BLOCKED";
        result.reasons = [budgetBlockedReason];
        continue;
      }
      const args = [
        "-p",
        "--mode", "json",
        "--model", `${CLIENT_PROVIDER_ID}/${spec.model}`,
        "--thinking", THINKING_LEVEL,
        "--session-dir", session.sessionDir,
        "--cwd", session.projectDir,
        "--tools", OMP_TOOLS,
        "--no-title",
        "--no-lsp",
        "--no-extensions",
        "--no-skills",
        "--no-rules",
        "--no-pty",
        "--auto-approve",
        "--max-time", OMP_MAX_TIME,
      ];
      if (spec.turn > 1) args.push("-r", session.sessionId);
      args.push(PROMPTS[spec.prompt]);
      result.argv = ["omp", ...args.slice(0, -1), "<prompt>"];
      result.prompt_sha12 = hash12(PROMPTS[spec.prompt]);

      const label = `${spec.id}${LABEL_SUFFIX}`;
      result.label = label;
      const baseline = await observabilityBaseline(ctx, `${label}-before`);
      const tapStart = state.records.length;
      state.currentRun = { id: label, cap: spec.cap, attempts: 0, stopReason: null };
      result.started_at = nowIso();
      const processResult = await runClient(ctx, { cmd: "omp", args, cwd: session.projectDir, env: clientEnv(session.home), label, timeoutMs: RUN_TIMEOUT_MS, enforceBudget: false });
      result.ended_at = nowIso();
      const runState = state.currentRun;
      state.currentRun = null;
      // Let in-flight tap records settle (bounded; responses finished before the client exited).
      for (let i = 0; i < 20 && state.records.length - tapStart < runState.attempts; i += 1) await new Promise((r) => setTimeout(r, 100));
      const tapRecords = state.records.slice(tapStart).filter((record) => record.run === label);
      const requestIds = tapRecords.filter((record) => record.forwarded && record.response?.request_id).map((record) => record.response.request_id);
      const wait = await waitForRequestEvents(ctx, { baseline, expectedRequestIds: requestIds, minCount: requestIds.length || 1, deadlineMs: EVENT_DEADLINE_MS, label });

      const omp = parseOmpOutput(processResult.stdout);
      if (spec.turn === 1) session.sessionId = omp.session_id;
      result.process = { code: processResult.code, signal: processResult.signal, timedOut: processResult.timedOut, durationMs: processResult.durationMs, stderr_warnings: processResult.stderrWarnings };
      result.omp = omp;
      result.session = { expected_id: spec.turn > 1 ? session.sessionId : null, observed_id: omp.session_id, same_as_turn1: spec.turn > 1 ? omp.session_id === session.sessionId : null, file: sessionFileEvidence(session.sessionDir, omp.session_id) };
      result.tap = tapRecords;
      result.tap_stop_reason = runState.stopReason;
      result.events = {
        baseline_cursor: baseline.eventCursor,
        final_cursor: wait.finalCursor,
        complete: !wait.timedOut && wait.missing.length === 0,
        missing: wait.missing,
        invalidated: wait.invalidated,
        drop_increases: wait.dropIncreases,
        summaries: wait.events.map(summarizeEvent),
      };
      result.correlated = correlate(tapRecords, wait.events);
      result.totals = runTotals(result.correlated);
      result.fixture = fixtureCheck(session.projectDir, spec.turn);
      writeFileSync(join(evidenceDir, `${label}-fixture-test.txt`), redactText(result.fixture.output_text ?? ""));
      delete result.fixture.output_text;
      result.slice_attempts_after_run = state.attempts;
      const evaluation = evaluateRun(result, ctx, hostInfo);
      result.problems = evaluation.problems;
      result.blocked = evaluation.blocked;
      if (spec.turn > 1 && !result.session.same_as_turn1) result.problems.push("resumed_session_id_differs");
      writeJson(join(evidenceDir, `${label}.run.json`), result);
      if (result.blocked.some((reason) => reason.startsWith("model_unsupported"))) {
        // No guessed retries: later turns of this session are not attempted.
        session.sessionId = null;
      }
    }
  } finally {
    tap.close();
    tapLog.end();
  }

  // Scenario judgments (ids from evidence/coverage.json)
  const g1 = runResults["gemini3-t1"];
  const g2 = runResults["gemini3-t2"];
  const fl = runResults["flashlite-t1"];
  const ran = (run) => Boolean(run && run.process);
  const responseHashesFrom = (run) => (run?.tap ?? []).flatMap((record) => responseCallHashes(record));
  const assertions = [];
  const assert = (scenarioId, name, expected, observed, pass, ref) => assertions.push({ scenarioId, name, expected, observed, pass, ref });
  const notRun = (scenarioId, run, fallback) => {
    const reasons = run?.reasons ?? fallback;
    const verdict = run?.verdict ?? (reasons.length ? "BLOCKED" : "NOT_RUN");
    summary.scenarios[scenarioId] = { verdict, reasons };
    if (verdict === "BLOCKED") for (const reason of reasons) summary.blocked.push(blockedEntry(scenarioId, reason, run ? `evidence/${run.label}.run.json` : "summary.json"));
  };
  const conclude = (scenarioId, run, problems, blocked, extra) => {
    summary.scenarios[scenarioId] = { verdict: verdictOf(problems, blocked), problems, blocked, ...extra };
    for (const reason of blocked) summary.blocked.push(blockedEntry(scenarioId, reason, `evidence/${run.label}.run.json`));
  };

  if (ran(g1)) {
    const sig = signatureReplayAssertion(g1.tap, []);
    g1.signature_replay = sig;
    const problems = [...g1.problems];
    if (!sig.ok) problems.push("thoughtSignature_not_replayed_on_functionCall_within_turn1");
    if (!g1.session.file.tool_calls_with_signature) problems.push("session_file_has_no_signed_toolCall");
    assert(SCENARIO.tools, "thought_signature_replayed", "per model turn: first functionCall carries a provider-returned signature; later calls carry that kind of signature or OMP's skip_thought_signature_validator marker (judgeReplayedTurns)", { rows: sig.rows, requests: sig.requests_with_function_call_history, marker_hash12: sig.marker_hash12 }, sig.ok, "evidence/tap.jsonl");
    assert(SCENARIO.tools, "fixture_test", EXPECTED_T1, g1.fixture.turn1_behavior?.value ?? null, g1.fixture.turn1_ok, `evidence/${g1.label}-fixture-test.txt`);
    conclude(SCENARIO.tools, g1, problems, g1.blocked, { signature_replay: sig, totals: g1.totals });
  } else notRun(SCENARIO.tools, g1, []);

  if (ran(g2)) {
    const sig = signatureReplayAssertion(g2.tap, responseHashesFrom(g1));
    g2.signature_replay = sig;
    const firstRequest = g2.tap.find((record) => record.forwarded && record.request?.method_kind);
    const problems = [...g2.problems];
    const t1Hashes = responseHashesFrom(g1);
    const firstContents = firstRequest?.request?.contents;
    const replaysTurn1 = Boolean(firstContents && firstContents.function_call_parts > 0 && firstContents.function_call_signature_hashes.some((h) => t1Hashes.includes(h)));
    if (!replaysTurn1) problems.push("turn2_first_request_does_not_replay_turn1_signed_functionCall");
    if (!sig.ok) problems.push("thoughtSignature_replay_assertion_failed_turn2");
    assert(SCENARIO.resume, "session_resume_exact", g2.session.expected_id, g2.session.observed_id, g2.session.same_as_turn1 === true, `evidence/${g2.label}.stdout`);
    assert(SCENARIO.resume, "turn1_history_replayed_with_signature", true, { replaysTurn1, first_request_contents: firstContents ?? null }, replaysTurn1, "evidence/tap.jsonl");
    assert(SCENARIO.resume, "fixture_test", EXPECTED_T2, g2.fixture.turn2_behavior?.value ?? null, g2.fixture.turn2_ok === true, `evidence/${g2.label}-fixture-test.txt`);
    conclude(SCENARIO.resume, g2, problems, g2.blocked, { resumed_session_id_matches: g2.session.same_as_turn1, first_request_contents: firstContents ?? null, signature_replay: sig, totals: g2.totals });
  } else notRun(SCENARIO.resume, g2, ran(g1) ? [] : ["gemini3_turn1_not_run"]);

  if (ran(fl) && ran(g1)) {
    const firstOf = (run) => run.tap.find((record) => record.forwarded && record.request?.method_kind)?.request ?? null;
    const shape = shapeComparison(firstOf(g1), firstOf(fl));
    const problems = [...fl.problems];
    if (!shape.ok) problems.push(`request_shape_differs:${shape.differing_fields.join(",") || shape.reason || "model"}`);
    const perToken = (totals) => {
      const tokens = totals.cclb_input_tokens + totals.cclb_output_tokens;
      return totals.cclb_cost_usd_micros !== null && tokens > 0 ? totals.cclb_cost_usd_micros / tokens : null;
    };
    const comparison = {
      primary: { model: PRIMARY_MODEL, ...g1.totals, cost_micros_per_token: perToken(g1.totals) },
      control: { model: CONTROL_MODEL, ...fl.totals, cost_micros_per_token: perToken(fl.totals) },
      control_cheaper_per_token: perToken(g1.totals) !== null && perToken(fl.totals) !== null ? perToken(fl.totals) < perToken(g1.totals) : null,
      control_cheaper_total: g1.totals.cclb_cost_usd_micros !== null && fl.totals.cclb_cost_usd_micros !== null ? fl.totals.cclb_cost_usd_micros < g1.totals.cclb_cost_usd_micros : null,
      control_signature_observed: responseHashesFrom(fl).length > 0,
    };
    assert(SCENARIO.lite, "request_shape_equal_except_model", { models: [PRIMARY_MODEL, CONTROL_MODEL], differing_fields: [] }, { models: shape.models, differing_fields: shape.differing_fields }, shape.ok, "evidence/tap.jsonl");
    // Cost comparison is reported only when both are priced; it never decides transport proof.
    if (comparison.control_cheaper_per_token === null) summary.blocked.push(blockedEntry(`${SCENARIO.lite}#cost_comparison`, "cost_comparison_unpriced", `evidence/${fl.label}.run.json`));
    else assert(SCENARIO.lite, "control_cheaper_per_token", true, { primary: comparison.primary.cost_micros_per_token, control: comparison.control.cost_micros_per_token }, comparison.control_cheaper_per_token, "summary.json#scenarios");
    conclude(SCENARIO.lite, fl, problems, fl.blocked, { shape, comparison });
  } else notRun(SCENARIO.lite, fl, ran(g1) ? [] : ["gemini3_turn1_not_run_no_shape_baseline"]);

  // Driver scenario verdicts counted by the collector (it judges from runs[] + assertions[]).
  for (const [scenarioId, scenario] of Object.entries(summary.scenarios)) {
    if (!Object.values(SCENARIO).includes(scenarioId)) continue;
    assert(scenarioId, "driver_scenario_verdict", "PASS", { verdict: scenario.verdict, problems: scenario.problems ?? [], blocked: scenario.blocked ?? scenario.reasons ?? [] }, scenario.verdict === "PASS", "summary.json#scenarios");
  }
  // A prerequisite block (e.g. pre-provider pricing 503) leaves protocol assertions unproven, not failed.
  for (const item of assertions) {
    const scenario = summary.scenarios[item.scenarioId];
    if (scenario?.verdict === "BLOCKED" && item.pass === false) {
      item.pass = null;
      item.unprovenBecause = scenario.blocked ?? scenario.reasons ?? [];
    }
  }
  summary.assertions = assertions;
  summary.budget_after = await budgetStatus(ctx).catch((error) => ({ error: redactText(String(error.message)) }));
  summary.slice_forwarded_attempts = state.attempts;
  summary.driver_cost_usd_total = state.driverCostUnknown ? null : state.driverCostUsd;
  summary.runDetails = runList.map((run) => ({ id: run.id, label: run.label, ref: `evidence/${run.label}.run.json`, verdict: run.verdict ?? null, reasons: run.reasons ?? [], problems: run.problems ?? [], blocked: run.blocked ?? [] }));
  summary.runs = runList.filter((run) => run.process).map((run) => collectorRun(run, ctx));
  const verdicts = Object.values(summary.scenarios).map((scenario) => scenario.verdict);
  summary.verdict = verdicts.every((v) => v === "PASS") ? "PASS" : verdicts.includes("FAIL") ? "FAIL" : verdicts.includes("BLOCKED") ? "BLOCKED" : "PARTIAL";
  summary.required_evidence_index = {
    binary_sha256: "binary_sha256",
    client_binary_and_current_version: "client",
    client_session_id: "runs[].sessionId/resumeOfSessionId (detail: evidence/<label>.run.json session)",
    client_tool_calls_and_fixture_test_output: "runs[].toolCalls + runs[].fixtureTest (detail: evidence/<label>.run.json omp.tool_calls, fixture)",
    API_family_and_endpoint: "runs[].wire[].apiFamily/endpoint",
    observed_transport: "runs[].wire[].transport (HTTP SSE via passive tap)",
    principal_id: "principal_id",
    key_id: "key_id",
    upstream_id: "upstream_id + runs[].wire[].upstreamId",
    real_provider_host: "real_provider_host",
    model: "runs[].wire[].model + evidence/<label>.run.json events.summaries[].model",
    stream: "runs[].wire[].stream",
    store: "not applicable (Gemini generateContent)",
    previous_response_id_presence: "not applicable (Gemini generateContent); runs[].wire[].previousResponseIdPresent=false",
    session_turn_header_presence: "runs[].wire[].sessionTurnHeaderPresent",
    HTTP_status: "runs[].wire[].httpStatus + evidence/<label>.run.json correlated[].status_event",
    provider_response_id: "runs[].wire[].providerResponseIdPresent/providerResponseIdSha256",
    provider_input_cached_output_reasoning_tokens: "runs[].wire[].providerUsage",
    cc_lb_tokens_and_cost: "evidence/<label>.run.json correlated[].cclb + totals; runtime budget",
    Gemini_replayed_function_call_thoughtSignature_presence: "runs[].wire[].thoughtSignatureReplayedPresent + assertions[] + scenarios.*.signature_replay",
  };
  summary.principalFresh = Boolean(ctx.principalId);
  summary.sharedPrincipalsUnmodified = true; // driver only reads/writes this slice's fresh SQLite principal
  summary.trace = { otlpConfigured: false, collectorRef: null };
  summary.cache = [];
  summary.notes.push("cache[]: omitted; Gemini implicit-cache minimum token threshold for these models not verified here, so no cache claim is made. Per-request cached tokens remain in runs[].wire[].providerUsage.cached.");
  summary.finished_at = nowIso();
  writeJson(join(evidenceDir, "omp-gemini-summary.json"), summary);
  await finishEvidence(ctx, summary);
  return summary;
}

// Run only when executed directly; importing (offline adjudicator) reuses the predicate without side effects.
const invokedDirectly = (() => {
  try {
    return Boolean(process.argv[1]) && realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url));
  } catch {
    return false;
  }
})();
if (invokedDirectly) {
  main()
    .then((summary) => {
      console.log(JSON.stringify({ slice: SLICE, verdict: summary.verdict, scenarios: Object.fromEntries(Object.entries(summary.scenarios).map(([id, s]) => [id, s.verdict])) }));
    })
    .catch((error) => {
      console.error(redactText(String(error?.stack ?? error)));
      process.exitCode = 1;
    });
}
