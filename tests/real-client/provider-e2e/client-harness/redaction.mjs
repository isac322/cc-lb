import { createHash, randomBytes } from "node:crypto";

const INTERNAL = Symbol("redaction-context");
const REDACTED = "[REDACTED]";
const OPAQUE = "[OPAQUE]";
const SAFE_ERROR = "unsafe artifact rejected";
const MAX_DECODE_ROUNDS = 4;

const SECRET_KEY = /authorization|cookie|password|passwd|credential|client[_-]?secret|private[_-]?key|api[_-]?key|access[_-]?key|access.?token|refresh.?token|id.?token|bearer|secret|csrf|jwt|signature|(?:^|[_-])(?:auth|sig|key|token)(?:$|[_-])|oauth|enrollment|mint/i;
const IDENTITY_KEY = /(?:account|workspace|project|resource|principal|tenant|correlation|request|user|org|organization)(?:[_-]?id)?$/i;
const OPAQUE_KEY = /^(?!binary_sha256$)(?:bytes|buffer|audio|file|blob|binary|raw|stdout|stderr|wire|chunk|content_bytes|rawContent|contentBytes|contentHash|content_hash|raw_hash|raw_sha256)(?:Data|Content|Hash|Name|Path|Transcript|_hash|_sha256|_path|_name)?$/i;
const URL_KEY = /(?:url|uri|href|endpoint|path|location|capability)$/i;
const SAFE_CAPTURE_KEYS = new Set([
  "id", "kind", "type", "name", "version", "source_revision", "binary_sha256", "provenance", "synthetic",
  "scenario", "scenarioId", "policyVersion", "surfaceIds", "surface_id", "transport", "method", "status",
  "statusCode", "headers", "query", "request", "response", "expected", "frames", "frame", "events", "event",
  "termination", "terminal", "cancelled", "cancel", "sequence", "timestamp", "timestamp_ms", "duration_ms",
  "byteLength", "contentType", "encoding", "mediaType", "metadata", "error", "message", "name", "code",
  "client", "dependencies", "disposition", "result", "feature", "issue", "owner", "reason", "nextProof",
  "method", "path", "url", "uri", "data", "event", "eventId", "messageId", "final", "complete", "more",
  // These containers are emitted only after the structured reconstruction below.
  "body", "payload", "content",
  "source", "provider", "model", "role", "tool", "toolName", "finishReason", "usage", "count", "length",
]);

const CAPTURE_HEADER_NAMES = new Set([
  "accept", "accept-encoding", "accept-language", "authorization", "proxy-authorization", "cookie", "set-cookie",
  "content-type", "content-length", "connection", "transfer-encoding", "host", "user-agent", "te",
  "grpc-status", "grpc-message", "grpc-encoding", "grpc-accept-encoding", "x-api-key", "x-goog-api-key",
  "x-order", "x-test", "x-private-bin", "x-request-id", "x-account-id", "x-workspace-id", "x-project-id",
  "x-resource-id", "x-principal-id",
]);
const CAPTURE_QUERY_NAMES = new Set([
  "alt", "model", "stream", "api-version", "version", "token", "key", "signature", "sig",
  "account_id", "workspace_id", "project_id", "resource_id", "principal_id", "same", "x-test",
]);

function safeEntryName(name, context, synthetic, kind) {
  if (synthetic) return replaceSecrets(name, context);
  const lower = name.toLowerCase();
  const known = kind === "header" ? CAPTURE_HEADER_NAMES : CAPTURE_QUERY_NAMES;
  if (known.has(lower)) return name;
  const prefix = kind === "header" ? "x-redacted-" : "redacted_query_";
  if (new RegExp(`^${prefix}[a-f0-9]{16}$`).test(name)) return name;
  return `${prefix}${digest(stateOf(context), `${kind}-name`, name)}`;
}

function validCapturedEntryName(name, kind) {
  const known = kind === "header" ? CAPTURE_HEADER_NAMES : CAPTURE_QUERY_NAMES;
  return known.has(name.toLowerCase()) || (kind === "header" ? /^x-redacted-[a-f0-9]{16}$/ : /^redacted_query_[a-f0-9]{16}$/).test(name);
}

const ACTION_REASONS = new Set([
  "INVALID_ARGUMENT", "INVALID_INPUT", "UNSUPPORTED_ACTION", "ACTION_APPROVAL_REQUIRED", "MODE_REQUIRED",
  "CHATGPT_CONTRACT_APPROVAL_REQUIRED", "INVALID_PRINCIPAL_REFERENCE", "INVALID_BOUND", "INVALID_LISTENER",
  "PROMPT_REQUIRED", "THREAD_REQUIRED", "PRINCIPAL_UNAVAILABLE", "PROCESS_FAILED", "PROCESS_DEADLINE",
  "OUTPUT_LIMIT", "CLOUD_CUSTOM_HOST_REJECTED", "CLIENT_AUTH_REQUIRED", "CLIENT_OPERATION_DENIED",
  "CLIENT_ARGUMENT_REJECTED", "CLIENT_EXIT_FAILURE", "TURN_NOT_OBSERVED", "JSONL_INVALID", "EVENT_ORDER_INVALID",
  "TURN_FAILED", "TURN_INCOMPLETE", "RESUME_THREAD_NOT_OBSERVED", "RESUME_THREAD_MISMATCH",
  "TOOL_EXECUTION_NOT_OBSERVED", "ACTION_OBSERVED", "CLOUD_LIST_NOT_OBSERVED", "DRIVER_OPERATION_FAILED",
  "HARNESS_UNAVAILABLE", "UNSAFE_PATH", "CONFIG_REQUIRED", "UNSAFE_LEDGER", "EXECUTABLE_REQUIRED",
  "CLIENT_HASH_MISMATCH", "ISOLATION_NOT_READY", "ARTIFACT_REJECTED", "CLIENT_DISPATCH_UNOBSERVED",
  "CLIENT_DEADLINE", "CLIENT_OUTPUT_LIMIT", "CLIENT_PROCESS_FAILED", "AUTH_SETUP_REQUIRED_DISPATCH_UNKNOWN",
  "INVALID_CLIENT_JSON", "CLIENT_ERROR_UNCORRELATED", "SEMANTIC_COMPLETION_UNOBSERVED", "TOOL_EXECUTION_UNOBSERVED",
  "NATIVE_STRUCTURED_RESULT_MATCH", "STRUCTURED_RESULT_MISMATCH", "INVALID_EXPECTED_JSON",
  "MISSING_TRIGGER_AUTH_CONTRACT", "UNSAFE_PERMISSIONS", "HOME_NOT_EMPTY", "CLIENT_PIN_MISMATCH", "INVALID_ENDPOINT",
  "ISOLATION_OS_SANDBOX_UNAVAILABLE", "ISOLATION_SANDBOX_PREFLIGHT_FAILED", "ISOLATION_LOOPBACK_SCOPE_NOT_ENFORCED",
]);
const ACTION_FIXED = Object.freeze({
  provenance: "captured", schema: "qaccept-action-v1", observed_upstream_kind: null,
  wireEvidence: "NOT-OBSERVED", liveFeatureAcceptance: "BLOCKED", runtimeAcceptance: "NOT-RUN",
  providerAcceptance: "NOT-RUN", fullAcceptance: "NOT-RUN", fullClientAcceptance: false, closes_acceptance: false,
});

function exactKeys(value, required, optional = []) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(SAFE_ERROR);
  if (required.some((key) => !Object.hasOwn(value, key))) throw new Error(SAFE_ERROR);
  const allowed = new Set([...required, ...optional]);
  if (Object.keys(value).some((key) => !allowed.has(key))) throw new Error(SAFE_ERROR);
}

// Execution-only action evidence has a finite contract, not permission to persist
// arbitrary captured output. Neither a consumer PASS nor this schema grants live acceptance.
function validateActionEvidence(record) {
  exactKeys(record, [...Object.keys(ACTION_FIXED), "action", "status", "consumer", "expected_upstream_kind"], ["client", "client_transport"]);
  for (const [key, value] of Object.entries(ACTION_FIXED)) if (record[key] !== value) throw new Error(SAFE_ERROR);
  if (!["C005", "C004", "C054", "A002", "A006", null].includes(record.action)) throw new Error(SAFE_ERROR);
  if (!["PASS", "FAIL", "BLOCKED"].includes(record.status)) throw new Error(SAFE_ERROR);
  if (!["openai_oauth", "gemini_oauth", "gemini_api_key", null].includes(record.expected_upstream_kind)) throw new Error(SAFE_ERROR);
  if ("client_transport" in record && !["gemini-api-key-native", "enterprise-gateway-bearer-native"].includes(record.client_transport)) throw new Error(SAFE_ERROR);
  exactKeys(record.consumer, ["status", "reason"], ["completed", "tool_count", "structured_match", "more", "result_count"]);
  if (record.consumer.status !== record.status) throw new Error(SAFE_ERROR);
  if (typeof record.consumer.reason !== "string" || !record.consumer.reason.startsWith("QACCEPT_") || !ACTION_REASONS.has(record.consumer.reason.slice(8))) throw new Error(SAFE_ERROR);
  for (const key of ["completed", "structured_match", "more"]) if (key in record.consumer && typeof record.consumer[key] !== "boolean") throw new Error(SAFE_ERROR);
  for (const key of ["tool_count", "result_count"]) if (key in record.consumer && (!Number.isInteger(record.consumer[key]) || record.consumer[key] < 0 || record.consumer[key] > 10000)) throw new Error(SAFE_ERROR);
  const output = { ...record, consumer: { ...record.consumer } };
  if ("client" in record) {
    exactKeys(record.client, ["name", "version", "binary_sha256"], ["source_revision"]);
    if (!["codex", "agy"].includes(record.client.name) || typeof record.client.version !== "string" || !/^\d+\.\d+\.\d+$/.test(record.client.version)) throw new Error(SAFE_ERROR);
    if (typeof record.client.binary_sha256 !== "string" || !/^[a-f0-9]{64}$/i.test(record.client.binary_sha256)) throw new Error(SAFE_ERROR);
    if ("source_revision" in record.client && (typeof record.client.source_revision !== "string" || !/^[a-f0-9]{40}$/i.test(record.client.source_revision))) throw new Error(SAFE_ERROR);
    output.client = { ...record.client };
  }
  return output;
}

function stateOf(context) {
  if (!context || !context[INTERNAL]) throw new TypeError("redaction context required");
  return context[INTERNAL];
}

function text(value) {
  return typeof value === "string" ? value : String(value ?? "");
}

function canonical(value) {
  return text(value).normalize("NFKC");
}

function identityNamespace(key) {
  return key.toLowerCase().replace(/^x-/, "").replace(/[_-]?id$/, "").replace(/^organization$/, "org");
}

function digest(state, namespace, value) {
  return createHash("sha256")
    .update(state.epoch)
    .update("\0")
    .update(state.scenarioId)
    .update("\0")
    .update(state.policyVersion)
    .update("\0")
    .update(namespace)
    .update("\0")
    .update(canonical(value))
    .digest("hex")
    .slice(0, 16);
}

function pseudonym(value, context, namespace = "identity") {
  const state = stateOf(context);
  const raw = canonical(value);
  if (raw.startsWith(`syn_${namespace}_`) && /^syn_[a-z-]+_[a-f0-9]{16}$/.test(raw)) return raw;
  const key = `${namespace}\0${raw}`;
  let result = state.identityMap.get(key);
  if (!result) {
    result = `syn_${namespace}_${digest(state, namespace, raw)}`;
    state.identityMap.set(key, result);
  }
  return result;
}

function registerSecret(value, context) {
  if (typeof value !== "string" || value.length === 0) return;
  const state = stateOf(context);
  const raw = value.normalize("NFKC");
  if (raw.length < 3) return;
  state.secrets.add(raw);
  const authorization = /^(?:Bearer|Basic)\s+(.+)$/i.exec(raw);
  if (authorization) registerSecret(authorization[1], context);
  for (const match of raw.matchAll(/(?:^|;\s*)[^=;\s]+=([^;]+)/g)) {
    if (match[1] !== raw) registerSecret(match[1], context);
  }
  let decoded = raw;
  for (let i = 0; i < MAX_DECODE_ROUNDS; i += 1) {
    try {
      const next = decodeURIComponent(decoded);
      if (next === decoded) break;
      decoded = next;
      if (decoded.length >= 3) state.secrets.add(decoded);
    } catch {
      break;
    }
  }
  if (/^[A-Za-z0-9+/=_-]{8,}$/.test(raw)) {
    try {
      const base = Buffer.from(raw.replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
      if (/^[\x20-\x7e\r\n]+$/.test(base) && base.length >= 3) {
        state.secrets.add(base);
        state.secrets.add(encodeURIComponent(base));
      }
    } catch {
      // Not base64; the original value remains registered.
    }
  }
}

// Discovery is a separate pass: an alias appearing before its credential field
// must be removed too. Raw values and this index never leave the context.
function discoverSecrets(value, context, key = "") {
  if (typeof value === "string") {
    if (SECRET_KEY.test(key)) registerSecret(value, context);
    if (IDENTITY_KEY.test(key)) pseudonym(value, context, identityNamespace(key));
    if (!SECRET_KEY.test(key) && key) {
      const decoded = decodeStructured(value);
      if (decoded) discoverSecrets(decoded.value, context);
    }
    const parsed = parseJson(value);
    if (parsed !== undefined) discoverSecrets(parsed, context);
    for (const line of value.split(/\r?\n/)) {
      if (!/^data\s*:/i.test(line)) continue;
      const payload = parseJson(line.replace(/^data\s*:\s*/i, ""));
      if (payload !== undefined) discoverSecrets(payload, context);
    }
    if (looksLikeUrl(value)) {
      try {
        const url = new URL(value);
        registerSecret(url.username, context);
        registerSecret(url.password, context);
        for (const [name, item] of url.searchParams) if (SECRET_KEY.test(name)) registerSecret(item, context);
      } catch { /* Invalid URLs are suppressed by the sanitizer. */ }
    }
    return;
  }
  if (!value || typeof value !== "object" || isBinary(value)) return;
  if (Array.isArray(value)) {
    for (const item of value) {
      if (Array.isArray(item) && item.length === 2 && typeof item[0] === "string") discoverSecrets(item[1], context, SECRET_KEY.test(key) ? key : item[0]);
      else discoverSecrets(item, context, key);
    }
    return;
  }
  for (const [name, item] of Object.entries(value)) {
    if (name === "frames") {
      for (const frame of reconstructLogicalMessages(item)) discoverSecrets(frame, context);
    } else discoverSecrets(item, context, SECRET_KEY.test(key) ? key : name);
  }
}

function replaceSecrets(value, context) {
  if (typeof value !== "string") return value;
  const state = stateOf(context);
  let result = value;
  const values = [...state.secrets].sort((a, b) => b.length - a.length);
  for (const secret of values) {
    const variants = [secret, encodeURIComponent(secret), encodeURIComponent(encodeURIComponent(secret)), Buffer.from(secret).toString("base64"), Buffer.from(secret).toString("base64url")];
    for (const variant of variants) if (variant.length >= 3 && result.includes(variant)) result = result.split(variant).join(REDACTED);
  }
  for (const [key, alias] of state.identityMap) {
    const raw = key.slice(key.indexOf("\0") + 1);
    if (raw.length < 3) continue;
    for (const variant of [raw, encodeURIComponent(raw), Buffer.from(raw).toString("base64"), Buffer.from(raw).toString("base64url")]) {
      if (variant.length >= 3 && result.includes(variant)) result = result.split(variant).join(alias);
    }
  }
  return result;
}

function looksLikeUrl(value) {
  return /^(?:https?|wss?|ftp):\/\//i.test(value) || /^(?:https?|wss?):\/\//i.test(value);
}

function redactPath(pathname, context, { synthetic = false } = {}) {
  const pieces = pathname.split("/");
  return pieces.map((piece, index) => {
    if (!piece || index === 0) return piece;
    if (/^(?:v\d+(?:beta)?|api|chat|responses|completions|models|events|stream|health|status|accounts|workspaces|projects|resources|users|tenants)$/i.test(piece)) return piece;
    const parent = pieces[index - 1]?.replace(/s$/, "");
    if (/^(?:account|workspace|project|resource|user|tenant)$/.test(parent)) return pseudonym(decodePathPart(piece), context, parent);
    if (!synthetic || /^(?:sk-|token-|secret-|sig-)/i.test(piece)) return pseudonym(decodePathPart(piece), context, "path");
    return replaceSecrets(piece, context);
  }).join("/");
}

function decodePathPart(value) {
  try { return decodeURIComponent(value); } catch { return value; }
}

export function sanitizeUrl(value, context, { synthetic = false } = {}) {
  const input = text(value);
  const decoded = decodePathPart(input);
  if (decoded !== input && looksLikeUrl(decoded)) return encodeURIComponent(sanitizeUrl(decoded, context, { synthetic }));
  const absolute = /^[a-z][a-z0-9+.-]*:\/\//i.test(input);
  try {
    const url = new URL(input, "https://capture.invalid");
    if (!/^(?:https?|wss?):$/.test(url.protocol)) return OPAQUE;
    registerSecret(decodePathPart(url.username), context);
    registerSecret(decodePathPart(url.password), context);
    url.username = "";
    url.password = "";
    if (!synthetic) url.host = "capture.invalid";
    url.pathname = redactPath(url.pathname, context, { synthetic });
    const sanitized = sanitizeQuery([...url.searchParams], context, { synthetic });
    url.search = "";
    for (const [key, val] of sanitized) url.searchParams.append(key, text(val));
    url.hash = "";
    return replaceSecrets(absolute ? url.toString() : `${url.pathname}${url.search}`, context);
  } catch {
    return OPAQUE;
  }
}

function sanitizeString(value, context, options = {}) {
  const { key = "", synthetic = false, structured = false } = options;
  let result = text(value);
  if (SECRET_KEY.test(key)) {
    registerSecret(result, context);
    return REDACTED;
  }
  if (IDENTITY_KEY.test(key)) return pseudonym(result, context, identityNamespace(key));
  if (URL_KEY.test(key) || looksLikeUrl(result)) return sanitizeUrl(result, context, { synthetic });
  result = replaceSecrets(result, context);
  const encoded = decodeStructured(result);
  if (encoded) {
    const clean = typeof encoded.value === "string"
      ? sanitizeUrl(encoded.value, context, { synthetic })
      : JSON.stringify(sanitizeAny(encoded.value, context, { synthetic, known: true, key: "" }));
    return encoded.encoding === "url" ? encodeURIComponent(clean) : Buffer.from(clean).toString(encoded.encoding);
  }

  if (structured || /^(?:body|data|payload|message|text|value|error)$/i.test(key)) {
    const parsed = parseJson(result);
    if (parsed !== undefined) {
      const clean = sanitizeAny(parsed, context, { synthetic, known: true, key });
      const serialized = JSON.stringify(clean);
      return serialized === JSON.stringify(parsed) ? result : serialized;
    }
    if (/(?:^|\n)data:/i.test(result)) {
      const lines = result.split(/\r?\n/).map((line) => {
        if (!/^data\s*:/i.test(line)) return synthetic || line === "" ? line : /^event:\s*(?:message|error|done)$/.test(line) ? line : "";
        const payload = line.replace(/^data\s*:\s*/i, "");
        const parsedPayload = parseJson(payload);
        if (parsedPayload === undefined) return `data: ${payload === "[DONE]" ? "[DONE]" : synthetic ? replaceSecrets(payload, context) : REDACTED}`;
        return `data: ${JSON.stringify(sanitizeAny(parsedPayload, context, { synthetic, known: true, key: "data" }))}`;
      });
      return lines.join("\n");
    }
  }
  if (!synthetic && !/^(?:kind|type|method|role|encoding|mediaType|contentType|event|termination|provenance|transport)$/.test(key)) return REDACTED;
  if (!synthetic && !/^(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS|json|sse|ws-text|ws-binary|grpc|audio|base64|utf8|captured|synthetic|descriptor-only|assistant|user|system|tool|message|response|error|done|cancel|cancelled|terminal|eof|complete|application\/[a-z0-9.+-]+|text\/[a-z0-9.+-]+)$/.test(result)) return REDACTED;
  return result;
}

function parseJson(value) {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  if (!(trimmed.startsWith("{") || trimmed.startsWith("["))) return undefined;
  try { return JSON.parse(trimmed); } catch { return undefined; }
}

function decodeStructured(value) {
  try {
    const decoded = decodeURIComponent(value);
    if (decoded !== value) {
      const parsed = parseJson(decoded);
      if (parsed !== undefined) return { value: parsed, encoding: "url" };
      if (looksLikeUrl(decoded)) return { value: decoded, encoding: "url" };
    }
  } catch { /* Not URL encoding. */ }
  if (/^[A-Za-z0-9+/_-]+={0,2}$/.test(value) && value.length >= 8) {
    const decoded = Buffer.from(value, "base64").toString("utf8");
    const parsed = parseJson(decoded);
    if (parsed !== undefined) return { value: parsed, encoding: /[-_]/.test(value) ? "base64url" : "base64" };
  }
  return undefined;
}

function opaqueMetadata(value, key = "opaque") {
  let byteLength;
  let contentType;
  if (typeof value === "string") byteLength = Buffer.byteLength(value);
  else if (value instanceof Uint8Array) byteLength = value.byteLength;
  else if (value && typeof value === "object") {
    if (Number.isSafeInteger(value.byteLength)) byteLength = value.byteLength;
    else if (Array.isArray(value.data)) byteLength = value.data.length;
    else if (typeof value.data === "string") byteLength = Buffer.byteLength(value.data);
    contentType = value.contentType ?? value.mimeType ?? value.mediaType;
  }
  const result = { kind: key || "opaque" };
  if (Number.isSafeInteger(byteLength) && byteLength >= 0) result.byteLength = byteLength;
  if (typeof contentType === "string" && /^(?:audio|video|image|application|text)\/[a-z0-9.+-]+$/i.test(contentType)) result.contentType = contentType;
  return result;
}

function isBinary(value) {
  return value instanceof Uint8Array || value instanceof ArrayBuffer || (value && typeof value === "object" && value.type === "Buffer" && Array.isArray(value.data));
}

export function sanitizeHeaders(entries, context, options = {}) {
  const synthetic = Boolean(options.synthetic);
  const list = Array.isArray(entries)
    ? entries
    : entries && typeof entries === "object" ? Object.entries(entries) : [];
  const result = [];
  for (const entry of list) {
    if (!Array.isArray(entry) || entry.length < 2) continue;
    const name = text(entry[0]);
    const value = entry[1];
    const outputName = safeEntryName(name, context, synthetic, "header");
    if (SECRET_KEY.test(name) || /^(?:cookie|set-cookie|authorization|proxy-authorization|x-goog-api-key)$/i.test(name)) {
      registerSecret(typeof value === "string" ? value : JSON.stringify(value), context);
      result.push([outputName, REDACTED]);
      continue;
    }
    if (/-bin$/i.test(name)) {
      result.push([outputName, opaqueMetadata(value, "grpc-metadata-bin")]);
      continue;
    }
    result.push([outputName, sanitizeAny(value, context, { synthetic, known: true, key: name })]);
  }
  return result;
}

export function sanitizeQuery(entries, context, options = {}) {
  const synthetic = Boolean(options.synthetic);
  const list = Array.isArray(entries)
    ? entries
    : typeof entries === "string" ? [...new URLSearchParams(entries)]
    : entries && typeof entries === "object" ? Object.entries(entries) : [];
  const result = [];
  for (const entry of list) {
    if (!Array.isArray(entry) || entry.length < 2) continue;
    const key = text(entry[0]);
    const value = entry[1];
    const outputName = safeEntryName(key, context, synthetic, "query");
    if (SECRET_KEY.test(key) || /^(?:sig|signature|expires|x-amz-[^]+)$/i.test(key)) {
      registerSecret(typeof value === "string" ? value : JSON.stringify(value), context);
      result.push([outputName, REDACTED]);
    } else if (IDENTITY_KEY.test(key)) {
      result.push([outputName, pseudonym(value, context, identityNamespace(key))]);
    } else {
      result.push([outputName, sanitizeAny(value, context, { synthetic, known: true, key })]);
    }
  }
  return result;
}

function sanitizeLogicalMessage(data, kind, context, synthetic, frame) {
  if (isBinary(data) || /^(?:ws-binary|audio|binary|file|grpc)$/.test(kind)) {
    // Generated framing is an explicit fixture-author assertion, never inferred
    // from a captured transport or from the presence of a base64-looking value.
    if (synthetic && frame?.syntheticContent === true && frame.encoding === "base64" && typeof data === "string") {
      const decoded = Buffer.from(data, "base64");
      if (decoded.toString("base64") !== data || replaceSecrets(data, context) !== data || replaceSecrets(decoded.toString("utf8"), context) !== decoded.toString("utf8")) return opaqueMetadata(data, kind || "binary");
      return data;
    }
    return opaqueMetadata(data, kind || "binary");
  }
  if (typeof data === "string") return sanitizeString(data, context, { synthetic, structured: true, key: "data" });
  return sanitizeAny(data, context, { synthetic, known: true, key: "data" });
}

function logicalFrameGroups(frames) {
  if (!Array.isArray(frames)) return [];
  const groups = [];
  const active = new Map();
  frames.forEach((frame, index) => {
    if (!frame || typeof frame !== "object") throw new Error("invalid frame");
    const websocket = /^ws-/.test(frame.kind ?? "");
    const control = /^ws-(?:ping|pong|close)$/.test(frame.kind ?? "") || Number(frame.opcode) >= 8;
    const key = websocket ? "websocket" : `${frame.kind}:${frame.messageId ?? frame.eventId ?? frame.id ?? ""}`;
    let group = control ? null : active.get(key);
    if (!group) {
      group = { frames: [], indices: [], message: { ...frame } };
      groups.push(group);
    } else group.message.data = joinFrameData(group.message.data, frame.data);
    group.frames.push(frame);
    group.indices.push(index);
    const partial = frame.final === false || frame.fin === false || frame.more === true || frame.complete === false;
    if (!control && partial) active.set(key, group);
    else if (!control) active.delete(key);
    for (const field of ["final", "fin", "more", "complete"]) if (field in frame) group.message[field] = frame[field];
  });
  return groups;
}

export function reconstructLogicalMessages(frames) {
  return logicalFrameGroups(frames).map(({ message }) => {
    const { more, complete, continuation, start, chunkSizes, chunk_sizes, ...logical } = message;
    return logical;
  });
}

function sanitizeFrame(frame, context, synthetic, replacementData) {
  const clean = {};
  for (const [key, value] of Object.entries(frame)) {
    if (key === "data" || key === "body" || key === "payload") clean[key] = key === "data" && replacementData !== undefined ? replacementData : sanitizeLogicalMessage(value, frame.kind, context, synthetic, frame);
    else if (key === "headers" || key === "metadata") clean[key] = sanitizeHeaders(value, context, { synthetic });
    else {
      const item = sanitizeAny(value, context, { synthetic, known: true, key });
      if (item !== undefined) clean[key] = item;
    }
  }
  if (typeof clean.data === "string") clean.byteLength = clean.encoding === "base64" ? Buffer.from(clean.data, "base64").byteLength : Buffer.byteLength(clean.data);
  else if (clean.data && typeof clean.data === "object" && !("byteLength" in clean.data)) clean.byteLength = Buffer.byteLength(JSON.stringify(clean.data));
  return clean;
}

function sanitizePhysicalFrames(frames, context) {
  const output = new Array(frames.length);
  for (const group of logicalFrameGroups(frames)) {
    if (group.frames.length === 1) {
      output[group.indices[0]] = sanitizeFrame(group.frames[0], context, true);
      continue;
    }
    if (group.frames.some((frame) => typeof frame.data !== "string" || frame.encoding === "base64" || /^(?:ws-binary|audio|binary|file|grpc)$/.test(frame.kind))) throw new Error("unrepresentable fragmented message");
    const raw = group.message.data;
    const clean = sanitizeLogicalMessage(raw, group.message.kind, context, true, group.message);
    if (typeof clean !== "string") throw new Error("unrepresentable fragmented message");
    if (clean !== raw && group.frames.some((frame) => "chunkSizes" in frame || "chunk_sizes" in frame)) throw new Error("unrepresentable rewritten chunk boundaries");
    const points = Array.from(clean);
    let offset = 0;
    group.frames.forEach((frame, i) => {
      const end = i === group.frames.length - 1 ? points.length : Math.min(points.length, offset + Array.from(frame.data).length);
      const data = clean === raw ? frame.data : points.slice(offset, end).join("");
      offset = end;
      output[group.indices[i]] = sanitizeFrame(frame, context, true, data);
    });
  }
  return output;
}

function joinFrameData(left, right) {
  if (typeof left === "string" && typeof right === "string") return left + right;
  if (Array.isArray(left) && Array.isArray(right)) return [...left, ...right];
  return right === undefined ? left : `${left ?? ""}${right}`;
}

function sanitizeAny(value, context, options = {}) {
  const { synthetic = false, known = false, key = "" } = options;
  if (value === null || typeof value === "number" || typeof value === "boolean") return value;
  if (typeof value === "string") {
    if (!synthetic && !known) return undefined;
    return sanitizeString(value, context, { key, synthetic, structured: known });
  }
  if (isBinary(value)) return opaqueMetadata(value, key || "binary");
  if (Array.isArray(value)) {
    if (!synthetic && value.length > 0 && value.every((item) => typeof item === "number")) return { kind: "opaque-array", byteLength: value.length };
    const output = [];
    for (const item of value) {
      const clean = sanitizeAny(item, context, { synthetic, known: true, key });
      if (clean !== undefined) output.push(clean);
    }
    return output;
  }
  if (typeof value !== "object") return undefined;
  if (OPAQUE_KEY.test(key)) return opaqueMetadata(value, key);
  const output = {};
  for (const [rawKey, rawValue] of Object.entries(value)) {
    const childKey = text(rawKey);
    if (childKey === "__proto__" || childKey === "constructor" || childKey === "prototype") continue;
    if (SECRET_KEY.test(childKey)) {
      registerSecret(typeof rawValue === "string" ? rawValue : JSON.stringify(rawValue), context);
      output[childKey] = REDACTED;
      continue;
    }
    if (IDENTITY_KEY.test(childKey) && (typeof rawValue === "string" || typeof rawValue === "number")) {
      output[childKey] = pseudonym(rawValue, context, identityNamespace(childKey));
      continue;
    }
    if (OPAQUE_KEY.test(childKey)) {
      output[childKey] = opaqueMetadata(rawValue, childKey);
      continue;
    }
    if (childKey === "headers") {
      output[childKey] = sanitizeHeaders(rawValue, context, { synthetic });
      continue;
    }
    if (childKey === "query" || childKey === "searchParams") {
      output[childKey] = sanitizeQuery(rawValue, context, { synthetic });
      continue;
    }
    if (URL_KEY.test(childKey) && childKey !== "path") {
      output[childKey] = sanitizeUrl(rawValue, context, { synthetic });
      continue;
    }
    if (childKey === "path") {
      output[childKey] = sanitizeUrl(rawValue, context, { synthetic });
      continue;
    }
    if (childKey === "frames" || childKey === "events") {
      const frames = Array.isArray(rawValue) ? rawValue : [];
      output[childKey] = synthetic
        ? sanitizePhysicalFrames(frames, context)
        : reconstructLogicalMessages(frames).map((frame) => sanitizeFrame(frame, context, false));
      continue;
    }
    if (/^(?:body|data|payload|content)$/.test(childKey) && !synthetic) {
      const parsed = typeof rawValue === "string" ? parseJson(rawValue) : rawValue;
      output[childKey] = parsed && typeof parsed === "object"
        ? sanitizeAny(parsed, context, { synthetic: false, known: true, key: "" })
        : opaqueMetadata(rawValue, childKey);
      continue;
    }
    if (!synthetic && !SAFE_CAPTURE_KEYS.has(childKey)) continue;
    const child = sanitizeAny(rawValue, context, { synthetic, known: synthetic || SAFE_CAPTURE_KEYS.has(childKey), key: childKey });
    if (child !== undefined) output[childKey] = child;
  }
  if (Array.isArray(output.headers) && "body" in output) {
    const length = Buffer.byteLength(typeof output.body === "string" ? output.body : JSON.stringify(output.body));
    output.headers = output.headers.map(([name, value]) => [name, /^content-length$/i.test(name) ? String(length) : value]);
  }
  return output;
}

export function createRedactionContext({ scenarioId = "unknown", policyVersion = "qharness-v1" } = {}) {
  const state = {
    scenarioId: canonical(scenarioId),
    policyVersion: canonical(policyVersion),
    epoch: randomBytes(32).toString("hex"),
    identityMap: new Map(),
    secrets: new Set(),
  };
  const context = { scenarioId: state.scenarioId, policyVersion: state.policyVersion };
  Object.defineProperty(context, INTERNAL, { value: state, enumerable: false });
  return Object.freeze(context);
}

export function sanitizeRecord(record, context) {
  try {
    stateOf(context);
    if (!record || typeof record !== "object") return { kind: "metadata", value: null, provenance: "captured" };
    if (record.schema === "qaccept-action-v1") return validateActionEvidence(record);
    const synthetic = record.provenance === "synthetic" || record.synthetic === true;
    discoverSecrets(record, context);
    const output = sanitizeAny(record, context, { synthetic, known: true, key: "record" });
    if (!output || typeof output !== "object") return { kind: "metadata", provenance: synthetic ? "synthetic" : "captured" };
    output.provenance = synthetic ? "synthetic" : "captured";
    if (synthetic) output.synthetic = true;
    return output;
  } catch {
    throw new Error("record sanitization failed");
  }
}

function decodedArtifactForms(source) {
  const forms = new Set([source]);
  let frontier = [source];
  for (let round = 0; round < MAX_DECODE_ROUNDS; round += 1) {
    const next = [];
    const add = (value) => {
      if (typeof value === "string" && !forms.has(value)) { forms.add(value); next.push(value); }
    };
    for (const value of frontier) {
      add(value.replace(/\\u\{([a-f0-9]{1,6})\}|\\u([a-f0-9]{4})|\\x([a-f0-9]{2})/gi, (_, point, unit, byte) => {
        const n = Number.parseInt(point ?? unit ?? byte, 16);
        return n <= 0x10ffff ? String.fromCodePoint(n) : "";
      }));
      try { add(decodeURIComponent(value)); } catch { /* Not URL encoding. */ }
      try {
        const parsed = JSON.parse(value);
        const visit = (item) => {
          if (typeof item === "string") add(item);
          else if (item && typeof item === "object") {
            if (Array.isArray(item.frames)) for (const frame of reconstructLogicalMessages(item.frames)) if (typeof frame.data === "string") add(frame.data);
            for (const child of Object.values(item)) visit(child);
          }
        };
        visit(parsed);
      } catch { /* Plain text/SSE is scanned directly. */ }
      for (const match of value.matchAll(/[A-Za-z0-9+/_-]{8,}={0,2}/g)) {
        const token = match[0];
        const decoded = Buffer.from(token, "base64").toString("utf8");
        if (decoded && !decoded.includes("\ufffd")) add(decoded);
      }
    }
    frontier = next;
    if (frontier.length === 0) break;
  }
  return forms;
}

function checkArtifactStructure(value, allowed, captured = false, key = "") {
  if (!value || typeof value !== "object") return;
  if (value.schema === "qaccept-action-v1") {
    validateActionEvidence(value);
    return;
  }
  if (Array.isArray(value)) {
    if (/^(?:headers|metadata|query|searchParams)$/.test(key) && value.length === 2 && typeof value[0] === "string" && SECRET_KEY.test(value[0]) && value[1] !== REDACTED && !allowed.has(value[1])) throw new Error(SAFE_ERROR);
    if (captured && /^(?:headers|metadata|query|searchParams)$/.test(key) && value.length === 2 && typeof value[0] === "string" && !validCapturedEntryName(value[0], /^(?:headers|metadata)$/.test(key) ? "header" : "query")) throw new Error(SAFE_ERROR);
    for (const item of value) checkArtifactStructure(item, allowed, captured, key);
    return;
  }
  const isCaptured = captured || value.provenance === "captured";
  for (const [key, item] of Object.entries(value)) {
    if (SECRET_KEY.test(key) && item !== REDACTED && !allowed.has(item)) throw new Error(SAFE_ERROR);
    if (OPAQUE_KEY.test(key)) {
      if (!item || typeof item !== "object" || Array.isArray(item) || isBinary(item) || item.kind !== key) throw new Error(SAFE_ERROR);
      for (const field of Object.keys(item)) if (!["kind", "byteLength", "contentType"].includes(field)) throw new Error(SAFE_ERROR);
      if ("byteLength" in item && (!Number.isSafeInteger(item.byteLength) || item.byteLength < 0)) throw new Error(SAFE_ERROR);
      if ("contentType" in item && (typeof item.contentType !== "string" || !/^(?:audio|video|image|application|text)\/[a-z0-9.+-]+$/i.test(item.contentType))) throw new Error(SAFE_ERROR);
    }
    if (isCaptured && !SAFE_CAPTURE_KEYS.has(key) && !SECRET_KEY.test(key) && !IDENTITY_KEY.test(key) && !OPAQUE_KEY.test(key) && !URL_KEY.test(key)) throw new Error(SAFE_ERROR);
    checkArtifactStructure(item, allowed, isCaptured, key);
  }
}

// This is a regression tripwire for planted values and invalid artifact shapes,
// not proof that arbitrary unknown secrets are absent. Captured input must first
// pass the fail-closed structured sanitizer; callers must not persist raw input.
export function assertSafeArtifact(bytes, { sentinels = [], allowedSyntheticTokens = [] } = {}) {
  const source = typeof bytes === "string" ? bytes : Buffer.from(bytes ?? []).toString("utf8");
  const allowed = new Set(allowedSyntheticTokens.map(text));
  let parsed;
  try { parsed = JSON.parse(source); } catch { /* Raw sanitized SSE/text is supported. */ }
  checkArtifactStructure(parsed, allowed);
  const forms = decodedArtifactForms(source);
  for (const sentinelValue of sentinels) {
    const sentinel = text(sentinelValue);
    if (!sentinel || allowed.has(sentinel)) continue;
    for (const form of forms) if (form.includes(sentinel)) throw new Error(SAFE_ERROR);
  }
  return true;
}

export { REDACTED, OPAQUE };
export default {
  createRedactionContext,
  sanitizeRecord,
  assertSafeArtifact,
  sanitizeUrl,
  sanitizeHeaders,
  sanitizeQuery,
  reconstructLogicalMessages,
};
