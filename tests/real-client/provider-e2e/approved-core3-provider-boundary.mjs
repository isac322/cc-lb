// Parent-executed only. Importing this module starts no listener or paid request.
// QA for the parent: raw HTTP path/query/header/body and compressed SSE equality;
// retry-per-reservation; missing/refusing guard; create(generate:false); fragmented
// text/binary WS; ambiguous created/completed; disconnect/full-hold; occupied port.
// Seal QA: idempotence, missing/changed guard, new HTTP/WS denial without closing
// retained listeners, and continued settlement of already-reserved responses.
import http from 'node:http';
import https from 'node:https';
import net from 'node:net';
import { createHash, randomUUID } from 'node:crypto';
import { brotliDecompressSync, gunzipSync, inflateSync, createInflateRaw, constants as zlibConstants } from 'node:zlib';
import path from 'node:path';
import { rootCertificates } from 'node:tls';

const BRANDED = new WeakSet();
const PINNED_NEGATIVE_PROBES = new WeakMap();
const BODY_LIMIT = 32 * 1024 * 1024;
const CAPTURE_LIMIT = 8 * 1024 * 1024;
const RECEIPT_LIMIT = 8192;
const HTTP_DISPATCH_ERROR_CODES = new Set([
  'ERR_INVALID_ARG_TYPE', 'ERR_INVALID_ARG_VALUE', 'ERR_INVALID_HTTP_TOKEN', 'ERR_HTTP_INVALID_HEADER_VALUE',
  'ERR_INVALID_PROTOCOL', 'ERR_INVALID_URL', 'ERR_HTTP_HEADERS_SENT', 'ERR_STREAM_WRITE_AFTER_END',
  'ERR_STREAM_DESTROYED', 'ERR_TLS_CERT_ALTNAME_INVALID', 'ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT',
  'EPIPE', 'ENOTFOUND',
]);
const safeHttpDispatchErrorCode = error => {
  let code;
  try { code = error?.code; } catch { return 'UNKNOWN'; }
  return typeof code === 'string' && HTTP_DISPATCH_ERROR_CODES.has(code) ? code : 'UNKNOWN';
};
const PROTOCOLS = new Set(['openai-responses', 'google-generate-content', 'anthropic-messages']);
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const hash = value => createHash('sha256').update(value).digest('hex');
const number = value => Number.isSafeInteger(value) && value >= 0;
const fail = reason => { throw new Error(`BLOCKED:${reason}`); };
const requireValue = (condition, reason) => { if (!condition) fail(reason); };
const utf8 = bytes => new TextDecoder('utf-8', { fatal: true }).decode(bytes);

export function isProviderBoundaryRelay(value) { return object(value) && BRANDED.has(value); }

function endpoint(pathname, protocol) {
  if (protocol === 'openai-responses' && /(?:^|\/)responses(?:\/compact)?\/?$/.test(pathname)) return /\/responses\/compact\/?$/.test(pathname) ? '/responses/compact' : '/responses';
  if (protocol === 'anthropic-messages' && /(?:^|\/)messages\/?$/.test(pathname)) return '/messages';
  if (protocol === 'google-generate-content') {
    if (/\/models\/[^/:]+:(?:generateContent|streamGenerateContent)$/.test(pathname)) return '/models/{model}:generateContent';
    if (/(?:^|\/)(?:v1internal:)?(?:generateContent|streamGenerateContent)$/.test(pathname)) return '/{codeassist}:generateContent';
  }
  return null;
}
function metadataRoute(pathname) {
  // An explicit route allowlist, not an assumed zero-cost generation result.
  return /(?:^|\/)models(?:\/[^/]+)?\/?$/.test(pathname) ? '/models/{optional-model}' : null;
}
function requestPath(raw) {
  requireValue(typeof raw === 'string' && raw.startsWith('/') && !raw.startsWith('//') && !/[\r\n#]/.test(raw), 'origin_form_path_required');
  const mark = raw.indexOf('?');
  return { raw, pathname: mark < 0 ? raw : raw.slice(0, mark), query: mark >= 0 };
}
function pathEvidence(p, protocol) {
  return { endpoint: endpoint(p.pathname, protocol) ?? metadataRoute(p.pathname) ?? 'unrecognized', path_sha256: hash(p.pathname), query_present: p.query, path_query_policy: 'VERBATIM_ON_ACTUAL_WRITE_ATTEMPT_ONLY' };
}
function outboundHeaders(raw, authority) {
  const result = [];
  let found = false;
  for (let index = 0; index < raw.length; index += 2) {
    if (raw[index].toLowerCase() === 'host') { result.push(raw[index], authority); found = true; }
    else result.push(raw[index], raw[index + 1]);
  }
  if (!found) result.push('Host', authority);
  return result;
}
function numericUsage(raw) {
  if (!object(raw)) return null;
  const keys = ['input_tokens', 'output_tokens', 'total_tokens', 'cached_tokens', 'reasoning_tokens', 'cache_creation_input_tokens', 'cache_read_input_tokens', 'promptTokenCount', 'candidatesTokenCount', 'totalTokenCount', 'cachedContentTokenCount', 'thoughtsTokenCount', 'toolUsePromptTokenCount'];
  const result = {};
  for (const key of keys) if (number(raw[key])) result[key] = raw[key];
  for (const key of ['input_tokens_details', 'output_tokens_details', 'cache_creation']) {
    if (!object(raw[key])) continue;
    const nested = {};
    for (const field of ['cached_tokens', 'reasoning_tokens', 'ephemeral_5m_input_tokens', 'ephemeral_1h_input_tokens']) if (number(raw[key][field])) nested[field] = raw[key][field];
    if (Object.keys(nested).length) result[key] = nested;
  }
  return Object.keys(result).length ? result : null;
}
function positiveUsage(raw, protocol) {
  if (!object(raw)) return false;
  if (protocol === 'google-generate-content') return number(raw.promptTokenCount) && number(raw.candidatesTokenCount ?? 0) && (raw.promptTokenCount + (raw.candidatesTokenCount ?? 0) + (raw.thoughtsTokenCount ?? 0)) > 0;
  return number(raw.input_tokens) && number(raw.output_tokens) && (raw.input_tokens + raw.output_tokens + (raw.cache_creation_input_tokens ?? 0) + (raw.cache_read_input_tokens ?? 0)) > 0;
}
function observedZeroPrewarmUsage(reservation, raw) {
  if (!reservation.exactWsPrewarm || !object(raw) || !Object.hasOwn(raw, 'input_tokens') || !Object.hasOwn(raw, 'output_tokens') || raw.input_tokens !== 0 || raw.output_tokens !== 0) return false;
  const allObservedCountsZero = (value, depth) => {
    if (typeof value === 'number') return Number.isSafeInteger(value) && value === 0;
    if (!object(value) || depth > 8) return false;
    return Object.values(value).every(child => allObservedCountsZero(child, depth + 1));
  };
  // No absent count is synthesized. Optional cache/reasoning/write details,
  // when actually present, must themselves contain only explicit integer zero.
  return allObservedCountsZero(raw, 0);
}
function parseJson(bytes) {
  const parsed = JSON.parse(utf8(bytes));
  requireValue(object(parsed), 'json_object_required');
  return parsed;
}
function actualModel(body, p, protocol) {
  const candidates = [];
  if (typeof body.model === 'string') candidates.push(body.model);
  if (protocol === 'google-generate-content') {
    const match = p.pathname.match(/\/models\/([^/:]+):(?:generateContent|streamGenerateContent)$/);
    if (match) candidates.push(decodeURIComponent(match[1]));
    if (object(body.request) && typeof body.request.model === 'string') candidates.push(body.request.model);
  }
  requireValue(candidates.length > 0 && candidates.every(value => value.length > 0 && value === candidates[0]), 'actual_model_unknown_or_ambiguous');
  return candidates[0];
}
function bodyView(body, protocol) {
  // Billing view only: the original bytes, including the CodeAssist envelope,
  // are always forwarded, never reconstructed from this object.
  if (protocol === 'google-generate-content' && Object.hasOwn(body, 'request')) requireValue(object(body.request), 'invalid_codeassist_request_envelope');
  return body;
}
function observedToolMetadata(body, protocol) {
  const wrappedGoogle = protocol === 'google-generate-content' && Object.hasOwn(body, 'request') && object(body.request);
  const effective = wrappedGoogle ? body.request : body;
  const rootPath = wrappedGoogle ? '$.request.tools' : '$.tools';
  const entries = [], containers = [], unknownShapes = [];
  let truncated = false;
  const googleKinds = new Set([
    'functionDeclarations', 'function_declarations', 'googleSearch', 'google_search',
    'googleSearchRetrieval', 'google_search_retrieval', 'urlContext', 'url_context',
    'codeExecution', 'code_execution', 'enterpriseWebSearch', 'enterprise_web_search',
    'retrieval', 'computerUse', 'computer_use', 'googleMaps', 'google_maps',
    'fileSearch', 'file_search',
  ]);
  const observedLabel = value => {
    if (typeof value !== 'string' || !/^[A-Za-z0-9_][A-Za-z0-9_.:-]{0,127}$/.test(value)) return null;
    if (/^(?:sk[-_]|AIza|ya29[.]|gh[pousr]_|github_pat_|xox[abprs]-|glpat-|hf_|AKIA|ASIA|eyJ|bearer)/i.test(value)) return null;
    if (/[A-Za-z0-9]{32,}/.test(value) || /^\d{12,}$/.test(value)) return null;
    return value;
  };
  function unknownShape(location, reason) {
    if (unknownShapes.length >= 256) { truncated = true; return; }
    unknownShapes.push(Object.freeze({ path: location, reason }));
  }
  function literalToolsShape(owner) {
    if (!Object.hasOwn(owner, 'tools')) return 'ABSENT';
    if (owner.tools === null) return 'NULL';
    return Array.isArray(owner.tools) ? 'ARRAY' : 'INVALID_NON_ARRAY';
  }
  function visit(tools, arrayPath, depth, sourcePath, sourceKind) {
    if (depth > 16) { truncated = true; unknownShape(arrayPath, 'NAMESPACE_DEPTH_LIMIT'); return; }
    for (let index = 0; index < tools.length; index++) {
      if (entries.length >= 256) { truncated = true; unknownShape(arrayPath, 'TOOL_METADATA_ENTRY_LIMIT'); return; }
      const tool = tools[index];
      const location = `${arrayPath}[${index}]`;
      if (!object(tool)) {
        unknownShape(location, 'TOOL_ENTRY_NOT_OBJECT');
        entries.push(Object.freeze({ source_path: sourcePath, source_kind: sourceKind, path: location, depth, object: false }));
        continue;
      }
      if (protocol === 'google-generate-content') {
        const kinds = [];
        let unknownKeys = 0;
        for (const key of Object.keys(tool)) {
          if (googleKinds.has(key)) kinds.push(key);
          else unknownKeys++;
        }
        // Only Google tool-container KEY KINDS are observations. Declaration
        // names, schemas, retrieval config, credentials and key values are not.
        entries.push(Object.freeze({ source_path: sourcePath, source_kind: sourceKind, path: location, depth, object: true, google_object_key_kinds: Object.freeze(kinds), unrecognized_object_key_count: unknownKeys }));
        continue;
      }
      const typePresent = Object.hasOwn(tool, 'type'), namePresent = Object.hasOwn(tool, 'name');
      const type = typePresent ? observedLabel(tool.type) : null;
      const name = namePresent ? observedLabel(tool.name) : null;
      const nestedPresent = Object.hasOwn(tool, 'tools');
      entries.push(Object.freeze({
        source_path: sourcePath, source_kind: sourceKind, path: location, depth, object: true, type, name,
        type_present: typePresent, name_present: namePresent,
        type_value_omitted: typePresent && type === null, name_value_omitted: namePresent && name === null,
        nested_tools_present: nestedPresent, nested_tools_array: nestedPresent && Array.isArray(tool.tools),
      }));
      // Recurse only an exact namespace tag's literal tools array. Function
      // schemas, content/user strings and arbitrary nested tools keys are not
      // recognized as tool declarations.
      if (tool.type === 'namespace') {
        const nestedShape = literalToolsShape(tool);
        if (nestedShape === 'ARRAY') visit(tool.tools, `${location}.tools`, depth + 1, sourcePath, sourceKind);
        else unknownShape(`${location}.tools`, `NAMESPACE_TOOLS_${nestedShape}`);
      }
    }
  }
  const present = Object.hasOwn(effective, 'tools');
  const array = present && Array.isArray(effective.tools);
  const rootShape = literalToolsShape(effective);
  containers.push(Object.freeze({ source_path: rootPath, source_kind: 'ROOT_TOOLS', shape: rootShape, tools_count: array ? effective.tools.length : null }));
  if (rootShape === 'INVALID_NON_ARRAY') unknownShape(rootPath, 'ROOT_TOOLS_INVALID_NON_ARRAY');
  if (array) visit(effective.tools, rootPath, 0, rootPath, 'ROOT_TOOLS');
  const inputArray = protocol === 'openai-responses' && Object.hasOwn(effective, 'input') && Array.isArray(effective.input);
  let additionalItemsObserved = 0;
  // Source-supported serde shape: a DIRECT input item tagged additional_tools,
  // developer role, and its own literal tools array. This receipt observes only
  // this decoded request; it is not a provider-QA or search-enabled/free claim.
  if (inputArray) for (let index = 0; index < effective.input.length; index++) {
    const item = effective.input[index];
    if (!object(item) || !Object.hasOwn(item, 'type') || item.type !== 'additional_tools') continue;
    if (containers.length >= 256) { truncated = true; unknownShape('$.input', 'ADDITIONAL_TOOLS_CONTAINER_LIMIT'); break; }
    additionalItemsObserved++;
    const itemPath = `$.input[${index}]`, sourcePath = `${itemPath}.tools`;
    const shape = literalToolsShape(item);
    const rolePresent = Object.hasOwn(item, 'role');
    const role = rolePresent && ['developer', 'system', 'user', 'assistant', 'tool'].includes(item.role) ? item.role : null;
    const roleMatches = rolePresent && item.role === 'developer';
    containers.push(Object.freeze({
      source_path: sourcePath, source_kind: 'TYPED_ADDITIONAL_TOOLS', shape,
      tools_count: shape === 'ARRAY' ? item.tools.length : null,
      item_path: itemPath, item_type: 'additional_tools', role_present: rolePresent,
      role, role_matches_source_contract: roleMatches,
    }));
    if (!roleMatches) unknownShape(itemPath, 'ADDITIONAL_TOOLS_ROLE_MISSING_OR_UNSUPPORTED');
    if (shape !== 'ARRAY') unknownShape(sourcePath, `ADDITIONAL_TOOLS_${shape}`);
    if (roleMatches && shape === 'ARRAY') visit(item.tools, sourcePath, 0, sourcePath, 'TYPED_ADDITIONAL_TOOLS');
  }
  return Object.freeze({
    container_path: rootPath, present, array, entries: Object.freeze(entries), truncated,
    containers: Object.freeze(containers), unknown: truncated || unknownShapes.length > 0, unknown_shapes: Object.freeze(unknownShapes),
    additional_tools_input_array_present: inputArray, additional_tools_items_observed: additionalItemsObserved,
    interpretation: 'OBSERVED_FIELDS_ONLY_NOT_HOSTED_TOOL_CLASSIFICATION',
    label_policy: 'BOUNDED_IDENTIFIER_ONLY_CREDENTIAL_SHAPED_VALUES_OMITTED',
    pricing_authority: 'SHARED_GUARD_ACTUAL_UNMODIFIED_BODY',
  });
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0, finished = false;
    const chunks = [];
    const finish = error => { if (finished) return; finished = true; if (error) reject(error); else resolve(Buffer.concat(chunks, size)); };
    req.on('data', chunk => {
      size += chunk.length;
      if (size > BODY_LIMIT) { chunks.length = 0; finish(new Error('BLOCKED:request_body_32mib_limit')); req.destroy(); }
      else if (!finished) chunks.push(chunk);
    });
    req.once('end', () => finish());
    req.once('aborted', () => finish(new Error('BLOCKED:request_interrupted')));
    req.once('error', () => finish(new Error('BLOCKED:request_read_failed')));
  });
}
function sseObjects(text) {
  const result = [];
  for (const block of text.split(/\r?\n\r?\n/)) {
    const payload = block.split(/\r?\n/).filter(line => line.startsWith('data:')).map(line => line.slice(5).replace(/^ /, '')).join('\n');
    if (!payload || payload === '[DONE]') continue;
    result.push(JSON.parse(payload));
  }
  return result;
}
function completeHttpResult(text, contentType, protocol, route) {
  const items = /event-stream/i.test(contentType) ? sseObjects(text) : [JSON.parse(text)];
  const list = items.flatMap(item => Array.isArray(item) ? item : [item]);
  let usage = null, id = null, tier, terminal = false, bad = false, anthropicStart = null;
  for (const item of list) {
    if (!object(item)) { bad = true; continue; }
    const value = object(item.response) ? item.response : item;
    if (item.error || value.error || ['response.failed', 'response.incomplete', 'error'].includes(item.type)) bad = true;
    if (protocol === 'openai-responses') {
      if (typeof value.id === 'string') { if (id && id !== value.id) bad = true; id = value.id; }
      if (item.type === 'response.completed' && value.status === 'completed' || value.object === 'response' && value.status === 'completed' || route === '/responses/compact' && value.object === 'response.compaction') {
        terminal = true; usage = value.usage; tier = value.service_tier;
      }
    } else if (protocol === 'anthropic-messages') {
      if (item.type === 'message_start' && object(item.message)) { anthropicStart = item.message.usage; id = item.message.id ?? null; }
      if (item.type === 'message_delta' && object(item.usage) && object(anthropicStart)) usage = { ...anthropicStart, ...item.usage };
      if (item.type === 'message_stop') terminal = true;
      if (value.type === 'message' && value.stop_reason != null) { terminal = true; usage = value.usage; id = value.id ?? null; }
    } else {
      if (object(value.usageMetadata)) usage = value.usageMetadata;
      if (typeof value.responseId === 'string') { if (id && id !== value.responseId) bad = true; id = value.responseId; }
      if (Array.isArray(value.candidates) && value.candidates.length > 0 && value.candidates.every(candidate => typeof candidate.finishReason === 'string' && candidate.finishReason !== 'FINISH_REASON_UNSPECIFIED')) terminal = true;
    }
  }
  return { usage, id, tier, complete: terminal && !bad && positiveUsage(usage, protocol) };
}

// A frame decoder retains original wire frames, including masking, fragmentation,
// binary/text opcodes and extension bytes. No JSON reserialization touches wire.
class FrameDecoder {
  constructor(masked, onMessage, onControl) { this.masked = masked; this.onMessage = onMessage; this.onControl = onControl; this.buffer = Buffer.alloc(0); this.parts = []; this.frames = []; this.size = 0; this.opcode = null; this.compressed = false; }
  push(chunk) {
    requireValue(this.buffer.length + chunk.length <= BODY_LIMIT + 65536, 'ws_buffer_limit');
    this.buffer = this.buffer.length ? Buffer.concat([this.buffer, chunk]) : chunk;
    for (;;) {
      if (this.buffer.length < 2) return;
      const first = this.buffer[0], second = this.buffer[1], opcode = first & 15, final = !!(first & 128), masked = !!(second & 128);
      requireValue(masked === this.masked && !(first & 48), 'invalid_ws_frame');
      let length = second & 127, offset = 2;
      if (length === 126) { if (this.buffer.length < 4) return; length = this.buffer.readUInt16BE(2); offset = 4; }
      else if (length === 127) { if (this.buffer.length < 10) return; const wide = this.buffer.readBigUInt64BE(2); requireValue(wide <= BigInt(BODY_LIMIT), 'ws_message_32mib_limit'); length = Number(wide); offset = 10; }
      const keyOffset = offset;
      if (masked) offset += 4;
      requireValue(length <= BODY_LIMIT, 'ws_message_32mib_limit');
      if (this.buffer.length < offset + length) return;
      const frame = this.buffer.subarray(0, offset + length);
      const payload = Buffer.from(frame.subarray(offset));
      if (masked) for (let index = 0; index < payload.length; index++) payload[index] ^= frame[keyOffset + index % 4];
      this.buffer = this.buffer.subarray(offset + length);
      if (opcode >= 8) {
        requireValue([8, 9, 10].includes(opcode) && final && !(first & 64) && length <= 125, 'unknown_ws_control');
        this.onControl(frame, opcode); continue;
      }
      requireValue([0, 1, 2].includes(opcode), 'unknown_ws_opcode');
      if (opcode !== 0) { requireValue(this.opcode === null, 'interleaved_ws_message'); this.opcode = opcode; this.compressed = !!(first & 64); }
      else requireValue(this.opcode !== null && !(first & 64), 'invalid_ws_continuation');
      this.size += length;
      requireValue(this.size <= BODY_LIMIT, 'ws_message_32mib_limit');
      this.parts.push(payload); this.frames.push(frame);
      if (final) {
        const message = { bytes: Buffer.concat(this.parts, this.size), frames: this.frames, opcode: this.opcode, compressed: this.compressed };
        this.parts = []; this.frames = []; this.size = 0; this.opcode = null; this.compressed = false;
        this.onMessage(message);
      }
    }
  }
}

export async function createProviderBoundaryRelay({ ctx, port, targetBaseUrl, protocol, models, recordResource, getGuard, tlsFixture } = {}) {
  requireValue(typeof Bun !== 'undefined', 'bun_runtime_required');
  requireValue(ctx && path.isAbsolute(ctx.root ?? '') && path.isAbsolute(ctx.evidenceDir ?? '') && path.relative(ctx.root, ctx.evidenceDir) !== '..' && !path.relative(ctx.root, ctx.evidenceDir).startsWith(`..${path.sep}`), 'existing_owned_context_required');
  requireValue(Number.isSafeInteger(port) && port > 1024 && port < 65536, 'exact_owned_port_required');
  requireValue(PROTOCOLS.has(protocol) && typeof recordResource === 'function' && typeof getGuard === 'function', 'relay_contract_required');
  requireValue(typeof targetBaseUrl === 'string' && !/[#\\\s]/.test(targetBaseUrl) && !/^https:\/\/[^/?#]*@/i.test(targetBaseUrl), 'target_url_userinfo_fragment_or_whitespace_forbidden');
  let target;
  try { target = new URL(targetBaseUrl); } catch { fail('invalid_target_url'); }
  requireValue(target.protocol === 'https:' && !target.username && !target.password && !target.hash, 'https_target_without_userinfo_or_fragment_required');
  const rawSuffix = targetBaseUrl.match(/^https:\/\/[^/?#]+(.*)$/i)?.[1];
  requireValue(rawSuffix !== undefined, 'literal_https_target_required');
  const targetPath = requestPath(rawSuffix.startsWith('/') ? rawSuffix : `/${rawSuffix}`);
  const allowedModels = new Set(Array.isArray(models) ? models : models instanceof Set ? [...models] : []);
  requireValue(allowedModels.size > 0 && [...allowedModels].every(model => typeof model === 'string' && model.length > 0), 'approved_models_required');
  const connectionHostname = target.hostname.startsWith('[') && target.hostname.endsWith(']') ? target.hostname.slice(1, -1) : target.hostname;
  const fixtureOnly = tlsFixture !== undefined;
  if (fixtureOnly) {
    requireValue(object(tlsFixture) && Object.keys(tlsFixture).length === 1 && Object.hasOwn(tlsFixture, 'ca') && typeof tlsFixture.ca === 'string' &&
      tlsFixture.ca.includes('-----BEGIN CERTIFICATE-----') && tlsFixture.ca.includes('-----END CERTIFICATE-----') && Buffer.byteLength(tlsFixture.ca) <= 1024 * 1024, 'explicit_local_fixture_ca_required');
    requireValue(['localhost', '127.0.0.1', '::1'].includes(connectionHostname), 'fixture_ca_only_for_literal_loopback_target');
  } else requireValue(Array.isArray(rootCertificates) && rootCertificates.length > 0, 'bundled_public_trust_roots_required');
  const tlsCa = fixtureOnly ? tlsFixture.ca : rootCertificates;
  const sourceScope = fixtureOnly ? 'synthetic-provider-boundary-fixture' : 'provider-boundary';
  const tlsTrustScope = fixtureOnly ? 'EXPLICIT_LOCAL_FIXTURE_CA' : 'EXPLICIT_BUNDLED_PUBLIC_ROOTS_WITHOUT_ENV_EXTRA_CA';
  const epoch = randomUUID();
  let sequence = 0, closing = false, fatal = false, captureHeld = 0;
  let sealed = false, sealAttempted = false, sealGuard = null, guardSealedObserved = false, guardSealConfirmed = false, sealIdentityInvalidated = false, sealFailure = null;
  const probeOwner = Symbol('private-pinned-negative-probe-owner');
  let activeNegativeProbe = null;
  const receipts = [], attempts = [], tlsReceipts = [], reservations = new Map(), sockets = new Set(), requests = new Set();
  const counts = { blocked: 0, metadata_requests: 0, ws_upgrade_write_attempts: 0, websocket_transport_controls: 0, provider_response_receipts: 0, actual_boundary_write_attempts: 0, actualgeneration_writeattempts: 0, settlement_failures: 0 };
  const receipt = entry => { if (receipts.length >= RECEIPT_LIMIT) { fatal = true; return; } receipts.push({ synthetic_fixture_only: fixtureOnly, source_scope: sourceScope, ...entry }); };
  const safeBlock = (kind, p, reason) => { counts.blocked++; receipt({ decision: 'BLOCKED', kind, ...(p ? pathEvidence(p, protocol) : {}), reason }); };
  function sealMetadata() {
    let sameGuard = false;
    try { sameGuard = sealed && sealGuard !== null && getGuard() === sealGuard; } catch { /* Missing identity is not successful sealing. */ }
    if (sealed && sealAttempted && sealGuard !== null && !sameGuard) sealIdentityInvalidated = true;
    const unchanged = sameGuard && !sealIdentityInvalidated;
    const complete = sealed && unchanged && guardSealConfirmed;
    return {
      sealed, future_generation_admission_denied: sealed,
      sealed_guard_identity_unchanged: unchanged, guard_sealed: guardSealedObserved, guard_seal_persisted: guardSealConfirmed,
      sealing_complete: complete, seal_state: !sealed ? 'OPEN' : complete ? 'SEALED' : 'BLOCKED',
      seal_reason: !sealed || complete ? null : sealFailure ?? (!unchanged ? 'sealed_guard_identity_changed_or_absent' : 'shared_guard_seal_unconfirmed'),
      admission_denial_scope: complete ? 'THIS_RELAY_AND_IDENTICAL_SHARED_GUARD' : sealed ? 'THIS_RELAY_ONLY_SHARED_GUARD_UNCONFIRMED' : 'NOT_SEALED',
    };
  }
  function seal() {
    // This local gate flips synchronously BEFORE any caller-supplied guard
    // method. Sealing never closes a listener/socket or interrupts paid work.
    sealed = true;
    if (!sealAttempted) {
      sealAttempted = true;
      try {
        sealGuard = getGuard();
        if (!sealGuard || typeof sealGuard.seal !== 'function') sealFailure = 'shared_guard_seal_unavailable';
        else {
          const result = sealGuard.seal();
          if (result && typeof result.then !== 'function') guardSealedObserved = result.sealed === true;
          if (!result || typeof result.then === 'function' || result.ok !== true || result.sealed !== true || result.seal_persisted !== true) sealFailure = 'shared_guard_seal_not_synchronously_durably_confirmed';
          else guardSealConfirmed = true;
        }
      } catch { sealFailure = 'shared_guard_seal_failed'; }
    }
    const state = sealMetadata();
    return { ok: state.sealing_complete, state: state.seal_state, reason: state.seal_reason, ...state };
  }
  function beginPinnedNegativeProbe({ model, maxOutputTokens } = {}) {
    requireValue(protocol === 'google-generate-content' && !sealed && !closing && !fatal && activeNegativeProbe === null, 'negative_probe_operation_not_available');
    requireValue(allowedModels.has(model) && Number.isSafeInteger(maxOutputTokens) && maxOutputTokens > 0, 'negative_probe_original_arguments_required');
    const guard = getGuard();
    requireValue(guard && typeof guard.reserve === 'function' && typeof guard.settleNonGeneration === 'function', 'negative_probe_durable_guard_required');
    requireValue(typeof ctx.upstreamId === 'string' && ctx.upstreamId.length > 0, 'negative_probe_actual_upstream_identity_required');
    const handle = Object.freeze(Object.create(null));
    const operation = { owner: probeOwner, guard, model, maxOutputTokens, upstreamId: ctx.upstreamId, targetOrigin: target.origin, forwards: 0, first: null, consumed: false };
    PINNED_NEGATIVE_PROBES.set(handle, operation);
    activeNegativeProbe = operation;
    return handle;
  }
  function trackPinnedProbeForward(reservation, parsed, p) {
    const operation = activeNegativeProbe;
    if (!operation || !reservation) return null;
    operation.forwards++;
    const view = object(parsed?.request) ? parsed.request : parsed;
    const exactKeys = object(view) && Object.keys(view).length === 3 && ['contents', 'generationConfig', 'max_tokens'].every(key => Object.hasOwn(view, key));
    const config = view?.generationConfig;
    const candidate = exactKeys && view.contents === 'invalid-not-array' && view.max_tokens === operation.maxOutputTokens && object(config) && Object.keys(config).length === 1 && config.maxOutputTokens === 8 &&
      reservation.model === operation.model && reservation.guard === operation.guard && getGuard() === operation.guard && ctx.upstreamId === operation.upstreamId && target.origin === operation.targetOrigin && endpoint(p.pathname, protocol) !== null;
    const record = { reservation, candidate, complete: false, rawInvalidArgument: false, tls: null, transportError: false };
    if (operation.forwards === 1) operation.first = record;
    return record;
  }
  function settlePinnedNegativeProbe(handle, proof) {
    const operation = object(handle) ? PINNED_NEGATIVE_PROBES.get(handle) : null;
    if (!operation || operation.owner !== probeOwner || operation.consumed) return { ok: false, reason: 'negative_probe_handle_invalid_or_consumed' };
    operation.consumed = true;
    if (activeNegativeProbe === operation) activeNegativeProbe = null;
    const record = operation.first;
    const event = object(proof?.event) ? proof.event : null;
    const trustedProof = object(proof) && proof.kind === 'pinned_negative_quota_probe' && proof.providerAttempts === 1 && proof.status === 400 && proof.errorStatus === 'INVALID_ARGUMENT' && proof.providerEvents === 1 &&
      proof.costKnown === true && proof.attemptsDelta === 1 && proof.costDeltaMicros === 0 && proof.drops === 0 && proof.failoverDelta === 0 &&
      event && typeof event.request_id === 'string' && event.request_id.length > 0 && event.request_id.length <= 4096 && event.status === 400 && event.error_code === 'upstream_4xx' &&
      event.upstream_id === operation.upstreamId && event.cost_usd_micros === 0 && event.input_tokens === 0 && event.output_tokens === 0;
    if (closing || fatal || getGuard() !== operation.guard || ctx.upstreamId !== operation.upstreamId || operation.targetOrigin !== target.origin || operation.forwards !== 1 || !record?.candidate || !record.complete || !record.rawInvalidArgument || record.transportError ||
      record.reservation.guard !== operation.guard || !record.tls?.secure_connect_observed || record.tls.certificate_authorized !== true || record.tls.hostname_verified !== true || record.tls.status !== 400 || !trustedProof) {
      return { ok: false, reason: 'negative_probe_evidence_incomplete_or_ambiguous_full_hold' };
    }
    try {
      const result = operation.guard.settleNonGeneration({ id: record.reservation.id, proof });
      if (!result || typeof result.then === 'function' || result.ok !== true) return { ok: false, reason: 'negative_probe_guard_proof_refused_full_hold' };
      record.reservation.state = 'SETTLED'; record.reservation.nonGenerationProven = true;
      return { ok: true, reason: null };
    } catch { return { ok: false, reason: 'negative_probe_settlement_failed_full_hold' }; }
  }
  function admission(body, model, p, transport) {
    requireValue(!sealed && !closing && !fatal && attempts.length < RECEIPT_LIMIT && receipts.length < RECEIPT_LIMIT - 2, 'relay_sealed_closed_or_evidence_bound');
    requireValue(allowedModels.has(model), 'actual_model_not_approved');
    const id = `provider-boundary:${epoch}:${++sequence}`;
    const observedTools = observedToolMetadata(body, protocol);
    receipt({ kind: 'generation_admission_tools_observed', admission_id_sha256: hash(id), reservation_id_sha256: null, provider_forward_attempted: false, observation_scope: 'DECODED_NATIVE_BODY_BEFORE_BUDGET_ADMISSION', observed_tools: observedTools });
    const guard = getGuard();
    requireValue(guard && typeof guard.reserve === 'function' && typeof guard.settle === 'function', 'durable_guard_absent');
    let result;
    try { result = guard.reserve({ id, model, body, source: `provider-boundary:${protocol}:${transport}:${endpoint(p.pathname, protocol)}`, transport }); }
    catch {
      receipt({ kind: 'generation_admission', decision: 'BLOCKED', admission_id_sha256: hash(id), reservation_id_sha256: null, reason: 'guard_reservation_failed' });
      fail('durable_reservation_failed');
    }
    const admitted = result && typeof result.then !== 'function' && result.ok === true;
    const refusal = typeof result?.reason === 'string' && /^[a-z0-9_]{1,160}$/.test(result.reason) ? result.reason : 'durable_reservation_refused';
    receipt({ kind: 'generation_admission', decision: admitted ? 'DURABLY_RESERVED' : 'BLOCKED', admission_id_sha256: hash(id), reservation_id_sha256: admitted ? hash(id) : null, reason: admitted ? null : refusal });
    requireValue(admitted, 'durable_reservation_refused');
    const reservation = { id, guard, model, id_sha256: hash(id), state: 'IN_FLIGHT', attempted: false, writeAttemptCount: 0, observedProviderStatus: null, exactWsPrewarm: transport === 'responses-ws' && object(body) && body.type === 'response.create' && body.generate === false, bound: number(result.bound_micros) ? result.bound_micros : null };
    reservations.set(id, reservation);
    return reservation;
  }
  function markUnknown(reservation, status = null, reason = 'terminal_outcome_unknown_full_hold') {
    if (!reservation || reservation.state === 'SETTLED' || reservation.state === 'UNKNOWN') return;
    reservation.state = 'UNKNOWN'; reservation.unknownReason = reason;
    const observed = Number.isSafeInteger(status) && status >= 100 && status <= 599 ? status : reservation.observedProviderStatus;
    const format = { 'openai-responses': 'openai', 'google-generate-content': 'gemini', 'anthropic-messages': 'anthropic' }[protocol];
    try {
      // Null is not zero usage. The guard durably changes only the outcome
      // classification to UNKNOWN and retains its complete maximum hold.
      const result = reservation.guard.settle({ id: reservation.id, usage: null, format, providerStatus: observed ?? null });
      if (!result || typeof result.then === 'function' || result.reason === 'budget_settlement_persistence_failed') {
        reservation.unknownTransitionFailed = true; counts.settlement_failures++;
      }
    } catch { reservation.unknownTransitionFailed = true; counts.settlement_failures++; }
  }
  function settle(reservation, usage, status, tier) {
    if (reservation.state === 'SETTLED') return;
    if (closing) { markUnknown(reservation, status, 'relay_closed_before_usage_settlement_full_hold'); return; }
    if (!(positiveUsage(usage, protocol) || observedZeroPrewarmUsage(reservation, usage)) || !Number.isSafeInteger(status) || status < 200 || status >= 300) { markUnknown(reservation, status, 'terminal_usage_missing_or_invalid_full_hold'); return; }
    try {
      const format = { 'openai-responses': 'openai', 'google-generate-content': 'gemini', 'anthropic-messages': 'anthropic' }[protocol];
      const result = reservation.guard.settle({ id: reservation.id, usage, format, providerStatus: status, ...(tier == null ? {} : { serviceTier: tier }) });
      if (result && typeof result.then !== 'function' && result.ok === true) { reservation.state = 'SETTLED'; reservation.usage = numericUsage(usage); }
      else { counts.settlement_failures++; markUnknown(reservation, status, 'raw_usage_settlement_refused_full_hold'); }
    } catch { counts.settlement_failures++; markUnknown(reservation, status, 'raw_usage_settlement_error_full_hold'); }
  }
  function attempted(reservation, p, kind, bytes) {
    requireValue(!sealed && !closing && !fatal && attempts.length < RECEIPT_LIMIT && tlsReceipts.length < RECEIPT_LIMIT, 'relay_sealed_stopping_or_evidence_bound');
    counts.actual_boundary_write_attempts++;
    if (reservation) { reservation.attempted = true; reservation.writeAttemptCount++; counts.actualgeneration_writeattempts++; }
    const entry = { kind, reservation_id_sha256: reservation?.id_sha256 ?? null, reserved_bound_micros: reservation?.bound ?? null, ...pathEvidence(p, protocol), source_scope: sourceScope, synthetic_fixture_only: fixtureOnly, provider_received: false, request_bytes: bytes.length, request_sha256: hash(bytes), forwarded_sha256: hash(bytes), hash_byte_equality: true };
    attempts.push(entry);
    return entry;
  }
  function requestOptions(req, p) {
    const options = { protocol: 'https:', hostname: connectionHostname, port: target.port || 443, ca: tlsCa, rejectUnauthorized: true, method: req.method, path: p.raw, headers: outboundHeaders(req.rawHeaders, target.host), setHost: false, agent: false };
    if (net.isIP(connectionHostname) === 0) options.servername = connectionHostname;
    return options;
  }
  function trackTls(request, p, kind, reservation) {
    const entry = { kind, ...pathEvidence(p, protocol), source_scope: sourceScope, synthetic_fixture_only: fixtureOnly, tls_trust_scope: tlsTrustScope, reservation_id_sha256: reservation?.id_sha256 ?? null, secure_connect_observed: false, certificate_authorized: null, hostname_verified: null, tls_protocol: null, status: null, transport_error: false };
    tlsReceipts.push(entry);
    request.once('socket', socket => {
      sockets.add(socket); socket.once('close', () => sockets.delete(socket));
      socket.once('secureConnect', () => {
        entry.secure_connect_observed = true;
        entry.certificate_authorized = socket.authorized === true;
        // https.request's default certificate AND hostname verifier remains
        // enabled; no custom checkServerIdentity or insecure TLS override exists.
        entry.hostname_verified = socket.authorized === true;
        const negotiated = socket.getProtocol();
        entry.tls_protocol = ['TLSv1.2', 'TLSv1.3'].includes(negotiated) ? negotiated : 'OTHER';
      });
    });
    request.once('response', response => { entry.status = response.statusCode; });
    request.once('upgrade', response => { entry.status = response.statusCode; });
    request.once('error', () => { entry.transport_error = true; });
    return entry;
  }
  function localBlocked(res, reason = 'provider_boundary_blocked') {
    if (res.destroyed) return;
    if (!res.headersSent) res.writeHead(503, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ error: { type: 'provider_boundary_blocked', message: `BLOCKED:${reason}` } }));
  }
  function capture() {
    let chunks = [], size = 0, valid = true;
    return {
      add(bytes) {
        // Reserve capture, concatenation and UTF-8/private-decode headroom.
        if (!valid) return;
        if (captureHeld + bytes.length * 6 > CAPTURE_LIMIT) { valid = false; captureHeld -= size * 6; chunks = []; size = 0; return; }
        chunks.push(Buffer.from(bytes)); size += bytes.length; captureHeld += bytes.length * 6;
      },
      decode(encoding) {
        if (!valid) return null;
        const bytes = Buffer.concat(chunks, size);
        const options = { maxOutputLength: Math.max(1, Math.min(Math.floor((CAPTURE_LIMIT - captureHeld) / 6) + size, Math.floor(CAPTURE_LIMIT / 6))) };
        if (!encoding || encoding === 'identity') return utf8(bytes);
        if (encoding === 'gzip') return utf8(gunzipSync(bytes, options));
        if (encoding === 'deflate') return utf8(inflateSync(bytes, options));
        if (encoding === 'br') return utf8(brotliDecompressSync(bytes, options));
        return null;
      },
      release() { captureHeld -= size * 6; chunks = []; size = 0; valid = false; },
    };
  }
  async function handle(req, res) {
    let p, reservation, up, parsedBody = null, probeRecord = null, phase = 'READ_BODY';
    try {
      requireValue(!sealed, 'relay_sealed_new_http_denied');
      p = requestPath(req.url);
      const generation = req.method === 'POST' && endpoint(p.pathname, protocol) !== null;
      const metadata = ['GET', 'HEAD'].includes(req.method) && metadataRoute(p.pathname) !== null;
      requireValue(generation || metadata, 'unknown_post_control_or_metadata_route');
      const body = await readBody(req);
      requireValue(!sealed && !req.aborted && !res.destroyed && !closing, 'relay_sealed_or_downstream_closed_before_reservation');
      if (metadata) requireValue(body.length === 0, 'metadata_body_not_allowed');
      else {
        parsedBody = parseJson(body);
        phase = 'ADMISSION';
        reservation = admission(bodyView(parsedBody, protocol), actualModel(parsedBody, p, protocol), p, 'http');
      }
      requireValue(!sealed && !closing && !fatal, 'relay_sealed_or_stopping');
      if (!metadata) probeRecord = trackPinnedProbeForward(reservation, parsedBody, p);
      requireValue(!sealed, 'relay_sealed_before_https_write');
      const entry = attempted(reservation, p, metadata ? 'non_generation_metadata_https' : 'generation_https', body);
      if (metadata) counts.metadata_requests++;
      phase = 'REQUEST_OPTIONS';
      const options = requestOptions(req, p);
      phase = 'HTTPS_REQUEST';
      up = https.request(options);
      phase = 'REQUEST_HANDLERS';
      const tlsEntry = trackTls(up, p, metadata ? 'non_generation_metadata_https' : 'generation_https', reservation);
      if (probeRecord) probeRecord.tls = tlsEntry;
      requests.add(up);
      up.once('close', () => requests.delete(up));
      let ended = false, interrupted = false;
      const abort = () => { if (!ended) { interrupted = true; markUnknown(reservation, null, 'downstream_http_closed_outcome_unknown_full_hold'); up.destroy(); } };
      res.once('close', abort);
      up.once('response', response => {
        counts.provider_response_receipts++;
        entry.received_response = true; entry.provider_status = response.statusCode;
        if (reservation) reservation.observedProviderStatus = response.statusCode;
        const captured = capture(), digest = createHash('sha256');
        let responseBytes = 0, released = false;
        const release = () => { if (!released) { released = true; captured.release(); } };
        res.writeHead(response.statusCode, response.statusMessage, response.rawHeaders);
        response.on('data', chunk => { responseBytes += chunk.length; digest.update(chunk); captured.add(chunk); if (!res.write(chunk)) response.pause(); });
        res.on('drain', () => response.resume());
        response.once('aborted', () => { interrupted = true; markUnknown(reservation, response.statusCode, 'provider_http_aborted_full_hold'); release(); res.destroy(); });
        response.once('error', () => { interrupted = true; markUnknown(reservation, response.statusCode, 'provider_http_error_full_hold'); release(); res.destroy(); });
        response.once('close', () => { if (!ended) { interrupted = true; markUnknown(reservation, response.statusCode, 'provider_http_closed_before_end_full_hold'); release(); } });
        response.once('end', () => {
          ended = true;
          const complete = !interrupted && response.complete && !res.destroyed && !closing;
          if (probeRecord) probeRecord.complete = complete;
          try {
            if (probeRecord?.candidate && complete && response.statusCode === 400) {
              const text = captured.decode(String(response.headers['content-encoding'] ?? '').toLowerCase());
              if (text !== null) {
                const result = parseJson(Buffer.from(text));
                probeRecord.rawInvalidArgument = object(result.error) && result.error.status === 'INVALID_ARGUMENT';
              }
            }
            // Every forward inside a pinned negative operation keeps its hold
            // unless the once-only operation proof validates the exact 400.
            if (reservation && !probeRecord && complete && response.statusCode >= 200 && response.statusCode < 300) {
              const text = captured.decode(String(response.headers['content-encoding'] ?? '').toLowerCase());
              if (text !== null) {
                const result = completeHttpResult(text, String(response.headers['content-type'] ?? ''), protocol, endpoint(p.pathname, protocol));
                if (result.id) entry.provider_response_id_sha256 = hash(result.id);
                if (result.complete) settle(reservation, result.usage, response.statusCode, result.tier);
              }
            }
          } catch { /* Incomplete/opaque usage keeps the complete maximum hold. */ }
          finally { release(); markUnknown(reservation, response.statusCode, 'provider_http_terminal_without_settlement_full_hold'); }
          const responseHash = digest.digest('hex');
          receipt({ kind: metadata ? 'non_generation_metadata_response' : 'generation_https_response', reservation_id_sha256: reservation?.id_sha256 ?? null, ...pathEvidence(p, protocol), status: response.statusCode, response_bytes: responseBytes, response_sha256: responseHash, forwarded_sha256: responseHash, hash_byte_equality: true, complete, settlement: reservation?.state ?? 'NOT_A_GENERATION_RESERVATION' });
          if (!res.destroyed) { if (response.rawTrailers.length) res.addTrailers(response.rawTrailers); res.end(); }
        });
      });
      up.once('error', () => { markUnknown(reservation, null, 'provider_http_transport_error_full_hold'); if (probeRecord) probeRecord.transportError = true; entry.transport_error = true; localBlocked(res, 'provider_tls_or_transport_failure_full_hold'); });
      phase = 'UPSTREAM_TRAILERS';
      if (req.rawTrailers.length) up.addTrailers(req.rawTrailers);
      phase = 'UPSTREAM_END';
      up.end(body);
    } catch (error) {
      receipt({ kind: 'http_dispatch_failure', phase, error_code: safeHttpDispatchErrorCode(error), reservation_id_sha256: reservation?.id_sha256 ?? null });
      if (probeRecord) probeRecord.transportError = true;
      markUnknown(reservation, null, 'http_forward_failed_or_interrupted_full_hold');
      safeBlock('http', p, sealed ? 'sealed_future_http_write_denied' : reservation ? 'after_reservation_full_hold' : 'unrecognized_or_unadmitted_request');
      if (up) up.destroy();
      localBlocked(res);
    }
  }
  const server = http.createServer((req, res) => { handle(req, res).catch(() => { fatal = true; localBlocked(res); }); });
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  server.on('clientError', (_error, socket) => socket.destroy());
  server.on('upgrade', (req, downstream, downstreamHead) => {
    let p, upstreamRequest, upstreamSocket, dead = false, transportClosing = false;
    let providerDecoder = null, providerCaptureCharge = 0, accountingOpaque = false;
    let inflateClient = null, clientNoContextTakeover = false, compressionNegotiated = false;
    let outgoingQueue = Promise.resolve(), queuedClientBytes = 0;
    const laneDefault = Symbol('internal-unlabeled-stream');
    const lanes = new Map(), byResponse = new Map();
    const wsReservations = new Set();
    const unknownWsHolds = reason => { for (const reserved of wsReservations) markUnknown(reserved, null, reason); };
    function lane(value) {
      const outer = value.stream_id, inner = object(value.response) ? value.response.stream_id : undefined;
      requireValue(outer === undefined || typeof outer === 'string' && outer.length > 0, 'invalid_stream_lane');
      requireValue(inner === undefined || typeof inner === 'string' && inner.length > 0, 'invalid_stream_lane');
      requireValue(outer === undefined || inner === undefined || outer === inner, 'ambiguous_stream_lane');
      const key = outer ?? inner ?? laneDefault;
      if (!lanes.has(key)) lanes.set(key, { key, pending: new Set(), active: new Set(), ambiguous: accountingOpaque });
      return lanes.get(key);
    }
    const laneEvidence = value => value.key === laneDefault ? null : hash(value.key);
    const releaseProviderCapture = () => {
      captureHeld -= providerCaptureCharge; providerCaptureCharge = 0;
      if (providerDecoder) { providerDecoder.buffer = Buffer.alloc(0); providerDecoder.parts = []; providerDecoder.frames = []; providerDecoder.size = 0; }
    };
    function opaqueAccounting(reason) {
      accountingOpaque = true;
      for (const stream of lanes.values()) stream.ambiguous = true;
      releaseProviderCapture();
      receipt({ kind: 'responses_ws_opaque_accounting', reason, settlement: 'ALL_UNRESOLVED_FULL_HOLD' });
    }
    const stop = reason => {
      if (dead) return; dead = true;
      unknownWsHolds('websocket_stopped_outcome_unknown_full_hold');
      releaseProviderCapture(); inflateClient?.destroy();
      safeBlock('responses_ws', p, reason);
      upstreamRequest?.destroy(); upstreamSocket?.destroy(); downstream.destroy();
      // No timeout, close, error or interruption releases any reservation.
    };
    function writeRaw(socket, frames, source, generative = false) {
      if (generative) requireValue(!sealed, 'sealed_ws_generative_write_denied');
      requireValue(socket && !socket.destroyed && !dead && !closing, 'websocket_not_open');
      for (const frame of frames) {
        if (generative) requireValue(!sealed, 'sealed_ws_generative_frame_write_denied');
        if (!socket.write(frame)) {
          source.pause();
          socket.once('drain', () => { if (!dead && !closing) source.resume(); });
        }
      }
    }
    function inflateOutgoing(bytes) {
      requireValue(compressionNegotiated, 'unnegotiated_ws_compression');
      if (!inflateClient) {
        inflateClient = createInflateRaw();
        inflateClient.on('error', () => stop('client_ws_compression_error_full_hold'));
      }
      const decoder = inflateClient;
      return new Promise((resolve, reject) => {
        const chunks = [];
        let size = 0, finished = false;
        const cleanup = () => { decoder.removeListener('data', data); decoder.removeListener('error', error); };
        const error = () => { if (!finished) { finished = true; cleanup(); reject(new Error('BLOCKED:invalid_compressed_ws_message')); } };
        const data = chunk => {
          size += chunk.length;
          if (size > BODY_LIMIT) { error(); decoder.destroy(); return; }
          chunks.push(chunk);
        };
        decoder.on('data', data); decoder.once('error', error);
        decoder.write(bytes); decoder.write(Buffer.from([0, 0, 255, 255]));
        decoder.flush(zlibConstants.Z_SYNC_FLUSH, () => {
          if (finished) return;
          finished = true; cleanup();
          if (clientNoContextTakeover) { decoder.destroy(); inflateClient = null; }
          resolve(Buffer.concat(chunks, size));
        });
      });
    }
    async function outgoing(message) {
      requireValue(!sealed && !dead && !closing && !transportClosing, 'websocket_sealed_or_closed_before_admission');
      const inspected = message.compressed ? await inflateOutgoing(message.bytes) : message.bytes;
      requireValue(!sealed && !dead && !closing && !transportClosing, 'websocket_sealed_or_closed_during_private_decode');
      const value = parseJson(inspected);
      requireValue(value.type === 'response.create' || value.type === 'response.steer', 'unknown_billable_ws_control');
      let model, transport, stream, parent = null;
      if (value.type === 'response.create') {
        stream = lane(value); model = actualModel(value, p, protocol); transport = 'responses-ws';
      } else {
        requireValue(Object.keys(value).every(key => ['type', 'previous_response_id', 'input', 'model'].includes(key)), 'unsupported_steer_fields');
        requireValue(typeof value.previous_response_id === 'string' && value.previous_response_id.length > 0, 'steer_actual_parent_required');
        parent = byResponse.get(value.previous_response_id);
        requireValue(parent && allowedModels.has(parent.model) && !parent.lane.ambiguous && !accountingOpaque && parent.pendingSteers.size === 0, 'steer_previous_model_or_lane_ambiguous');
        stream = parent.lane; model = parent.model;
        requireValue(value.model === undefined || value.model === model, 'explicit_steer_model_mismatch');
        requireValue(![...stream.pending].some(record => record.previousResponseId === value.previous_response_id), 'steer_competes_with_explicit_create');
        transport = 'responses-ws-steer';
      }
      const reserved = admission(value, model, p, transport);
      wsReservations.add(reserved);
      reserved.lane = stream; reserved.transport = transport;
      reserved.previousResponseId = typeof value.previous_response_id === 'string' ? value.previous_response_id : null;
      reserved.pendingSteers = new Set();
      if (parent) parent.pendingSteers.add(reserved);
      stream.pending.add(reserved);
      if (transport === 'responses-ws' && [...stream.pending].filter(record => record.transport === 'responses-ws').length > 1) stream.ambiguous = true;
      requireValue(!sealed && upstreamSocket && !upstreamSocket.destroyed && !dead && !closing, 'websocket_sealed_or_closed_before_write_attempt');
      const entry = attempted(reserved, p, 'responses_ws_data_message', message.bytes);
      entry.stream_lane_sha256 = laneEvidence(stream); entry.stream_lane = null;
      entry.binary = message.opcode === 2; entry.fragment_count = message.frames.length; entry.compressed = message.compressed;
      const wireHash = createHash('sha256');
      let wireBytes = 0;
      for (const frame of message.frames) { wireHash.update(frame); wireBytes += frame.length; }
      entry.wire_bytes = wireBytes; entry.wire_sha256 = wireHash.digest('hex');
      entry.forwarded_wire_sha256 = entry.wire_sha256;
      entry.hash_equality_scope = 'UNCHANGED_SUBMITTED_BUFFER_NOT_PROVIDER_RECEIPT';
      entry.generate_false = value.type === 'response.create' && value.generate === false;
      requireValue(!sealed, 'relay_sealed_before_ws_generative_write');
      writeRaw(upstreamSocket, message.frames, downstream, true);
    }
    function queueOutgoing(message) {
      queuedClientBytes += message.bytes.length;
      requireValue(queuedClientBytes <= BODY_LIMIT, 'queued_ws_request_32mib_limit');
      outgoingQueue = outgoingQueue.then(() => outgoing(message)).catch(() => {
        if (sealed) {
          safeBlock('responses_ws', p, 'sealed_future_generation_write_denied');
          for (const reserved of wsReservations) if (!reserved.attempted) markUnknown(reserved, null, 'sealed_before_ws_write_full_hold');
          // A sealed denial does not destroy an existing socket or interrupt
          // an already-paid response arriving in the opposite direction.
        } else stop('unknown_or_unadmitted_ws_data_full_hold');
      }).finally(() => { queuedClientBytes -= message.bytes.length; });
    }
    function incoming(message) {
      const evidence = { kind: 'responses_ws_provider_message', response_bytes: message.bytes.length, response_sha256: hash(message.bytes), binary: message.opcode === 2, compressed: message.compressed, stream_lane: null };
      if (message.compressed) { opaqueAccounting('compressed_provider_usage_not_used_for_release'); receipt(evidence); return; }
      try {
        const value = parseJson(message.bytes);
        let stream = lane(value);
        const response = object(value.response) ? value.response : null;
        const id = response?.id ?? value.response_id;
        // A provider snapshot name is an observation, never a catalog lookup,
        // alias heuristic or replacement for the bound request's pricing model.
        const observedResponseModel = typeof response?.model === 'string' ? response.model : null;
        if (response && Object.hasOwn(response, 'model')) {
          evidence.observed_response_model = observedResponseModel !== null && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/.test(observedResponseModel) ? observedResponseModel : null;
          evidence.observed_response_model_present = true;
        }
        evidence.stream_lane_sha256 = laneEvidence(stream);
        const wireLanePresent = value.stream_id !== undefined || response?.stream_id !== undefined;
        if (!wireLanePresent && typeof id === 'string' && byResponse.has(id)) stream = byResponse.get(id).lane;
        if (typeof value.type === 'string' && ['response.created', 'response.completed', 'response.failed', 'response.incomplete', 'error', 'response.steer.accepted', 'response.steer.pending', 'response.steer.failed'].includes(value.type)) evidence.event = value.type;
        if (typeof id === 'string' && id.length) evidence.provider_response_id_sha256 = hash(id);
        if (value.type === 'error') {
          for (const item of lanes.values()) item.ambiguous = true;
          if (typeof id === 'string' && byResponse.has(id)) markUnknown(byResponse.get(id), null, 'provider_ws_error_full_hold');
          else unknownWsHolds('provider_ws_unattributed_error_full_hold');
        }
        if (['response.steer.accepted', 'response.steer.pending', 'response.steer.failed'].includes(value.type)) {
          const control = object(value.steer) ? value.steer : null;
          const parent = typeof control?.previous_response_id === 'string' ? byResponse.get(control.previous_response_id) : null;
          if (!wireLanePresent && parent) stream = parent.lane;
          const candidates = parent ? [...parent.pendingSteers] : [];
          if (!control || typeof control.id !== 'string' || candidates.length !== 1 || parent.lane !== stream) {
            stream.ambiguous = true;
            if (parent) parent.lane.ambiguous = true;
          } else {
            const reserved = candidates[0];
            if (reserved.steerId && reserved.steerId !== control.id) stream.ambiguous = true;
            reserved.steerId = control.id;
            evidence.steer_id_sha256 = hash(control.id);
            if (value.type === 'response.steer.failed') { reserved.terminalBad = true; markUnknown(reserved, null, 'provider_ws_steer_failed_full_hold'); }
          }
        }
        if (value.type === 'response.created' && typeof id === 'string' && id.length && response) {
          const previous = response.previous_response_id;
          const parent = typeof previous === 'string' ? byResponse.get(previous) : null;
          const explicit = [...lanes.values()].flatMap(item => [...item.pending]).filter(record => record.transport === 'responses-ws' && record.previousResponseId === (previous ?? null));
          const steers = parent ? [...parent.pendingSteers] : [];
          let reserved = null;
          if (steers.length === 1 && explicit.length === 0 && !steers[0].terminalBad) reserved = steers[0];
          else if (steers.length === 0 && explicit.length === 1) reserved = explicit[0];
          if (!wireLanePresent && parent && reserved && parent.lane === reserved.lane) stream = parent.lane;
          if (reserved && reserved.lane.key === laneDefault && stream.key !== laneDefault && reserved.lane.pending.size === 1 && reserved.lane.active.size === 0) {
            reserved.lane.pending.delete(reserved); stream.pending.add(reserved); reserved.lane = stream;
          }
          if (!reserved || reserved.lane !== stream || stream.ambiguous || byResponse.has(id)) {
            stream.ambiguous = true;
            if (reserved) reserved.lane.ambiguous = true;
            evidence.association = 'AMBIGUOUS_OR_UNBOUND_FULL_HOLD';
          } else {
            // Exact created-ID/request/lane binding fixes the approved pricing
            // model at reserved.model, including when the provider reports a
            // dated snapshot instead of the request alias or omits its model.
            reserved.observedResponseModel = observedResponseModel;
            stream.pending.delete(reserved); stream.active.add(reserved);
            reserved.responseId = id; byResponse.set(id, reserved);
            if (parent) parent.pendingSteers.delete(reserved);
          }
        }
        if (['response.completed', 'response.failed', 'response.incomplete'].includes(value.type) && typeof id === 'string') {
          const reserved = byResponse.get(id);
          if (reserved && reserved.lane === stream && !stream.ambiguous && !accountingOpaque && !reserved.terminalBad) {
            if (observedResponseModel !== null) reserved.observedResponseModel = observedResponseModel;
            if (value.type === 'response.completed' && response?.status === 'completed') settle(reserved, response.usage, 200, response.service_tier);
            else { reserved.terminalBad = true; markUnknown(reserved, null, 'provider_ws_failed_or_incomplete_full_hold'); }
            stream.active.delete(reserved);
            evidence.settlement = reserved.state; evidence.usage = numericUsage(response?.usage);
            if (reserved.state === 'UNKNOWN') reserved.terminalBad = true;
          } else {
            if (reserved) markUnknown(reserved, null, 'provider_ws_terminal_association_ambiguous_full_hold');
            evidence.association = 'AMBIGUOUS_OR_UNBOUND_FULL_HOLD';
          }
        }
      } catch { opaqueAccounting('provider_message_correlation_unknown'); evidence.accounting = 'OPAQUE_FULL_HOLD'; }
      receipt(evidence);
    }
    try {
      requireValue(!sealed, 'relay_sealed_new_ws_upgrade_denied');
      p = requestPath(req.url);
      requireValue(protocol === 'openai-responses' && req.method === 'GET' && endpoint(p.pathname, protocol) === '/responses' && String(req.headers.upgrade ?? '').toLowerCase() === 'websocket', 'unrecognized_ws_upgrade');
      requireValue(!sealed && getGuard() && !closing && !fatal && tlsReceipts.length < RECEIPT_LIMIT && receipts.length < RECEIPT_LIMIT - 2, 'relay_sealed_guard_absent_or_evidence_bound_no_ws_upgrade');
      // The upgrade handshake itself cannot generate. Transport ping/pong/close
      // are explicit non-generation controls; unknown data controls are blocked.
      const clientDecoder = new FrameDecoder(true, queueOutgoing, (frame, opcode) => { if (opcode === 8) { transportClosing = true; unknownWsHolds('client_ws_close_observed_full_hold'); } counts.websocket_transport_controls++; receipt({ kind: 'ws_transport_control', direction: 'client', opcode }); writeRaw(upstreamSocket, [frame], downstream); });
      providerDecoder = new FrameDecoder(false, incoming, (_frame, opcode) => { if (opcode === 8) { transportClosing = true; unknownWsHolds('provider_ws_close_observed_full_hold'); } counts.websocket_transport_controls++; receipt({ kind: 'ws_transport_control', direction: 'provider', opcode }); });
      function providerBytes(bytes) {
        counts.provider_response_receipts++;
        const digest = hash(bytes);
        writeRaw(downstream, [bytes], upstreamSocket);
        receipt({ kind: 'responses_ws_provider_wire_chunk', bytes: bytes.length, response_sha256: digest, forwarded_sha256: digest, hash_byte_equality: true });
        if (accountingOpaque) return;
        // All response decoding across all connections shares this cap. Wire
        // forwarding does not stop when private accounting reaches the bound.
        if (captureHeld + bytes.length * 6 > CAPTURE_LIMIT) { opaqueAccounting('shared_capture_8mib_bound'); return; }
        captureHeld += bytes.length * 6; providerCaptureCharge += bytes.length * 6;
        try {
          providerDecoder.push(bytes);
          if (accountingOpaque) return;
          const retained = providerDecoder.buffer.length + providerDecoder.parts.reduce((sum, value) => sum + value.length, 0) + providerDecoder.frames.reduce((sum, value) => sum + value.length, 0);
          const nextCharge = retained * 3;
          captureHeld += nextCharge - providerCaptureCharge; providerCaptureCharge = nextCharge;
          if (captureHeld > CAPTURE_LIMIT) opaqueAccounting('shared_capture_8mib_bound');
        } catch { opaqueAccounting('provider_ws_private_decoder_failure'); }
      }
      downstream.pause();
      requireValue(!sealed, 'relay_sealed_before_ws_upgrade_https_write');
      counts.actual_boundary_write_attempts++; counts.ws_upgrade_write_attempts++;
      upstreamRequest = https.request(requestOptions(req, p));
      trackTls(upstreamRequest, p, 'non_generation_ws_upgrade', null);
      requests.add(upstreamRequest);
      upstreamRequest.once('close', () => requests.delete(upstreamRequest));
      upstreamRequest.once('upgrade', (response, socket, head) => {
        upstreamSocket = socket;
        sockets.add(socket); socket.once('close', () => sockets.delete(socket));
        counts.provider_response_receipts++;
        receipt({ kind: 'non_generation_ws_upgrade_response', ...pathEvidence(p, protocol), status: response.statusCode, stream_lane: null });
        if (dead || closing) { socket.destroy(); return; }
        if (response.statusCode !== 101) { stop('provider_upgrade_not_101'); return; }
        const extensions = String(response.headers['sec-websocket-extensions'] ?? '');
        compressionNegotiated = /(?:^|,)\s*permessage-deflate(?:;|,|$)/i.test(extensions);
        clientNoContextTakeover = /(?:^|;)\s*client_no_context_takeover(?:;|,|$)/i.test(extensions);
        let raw = `HTTP/${response.httpVersion} ${response.statusCode} ${response.statusMessage}\r\n`;
        for (let index = 0; index < response.rawHeaders.length; index += 2) raw += `${response.rawHeaders[index]}: ${response.rawHeaders[index + 1]}\r\n`;
        downstream.write(raw + '\r\n');
        downstream.on('data', bytes => { try { clientDecoder.push(bytes); } catch { stop('unknown_or_unadmitted_ws_data_full_hold'); } });
        socket.on('data', bytes => { try { providerBytes(bytes); } catch { stop('provider_ws_forward_failure_full_hold'); } });
        socket.once('error', () => stop('provider_ws_transport_error_full_hold'));
        socket.once('close', () => { unknownWsHolds('provider_ws_socket_closed_outcome_unknown_full_hold'); releaseProviderCapture(); inflateClient?.destroy(); if (!dead) { dead = true; downstream.destroy(); } });
        try { if (head.length) providerBytes(head); if (downstreamHead.length) clientDecoder.push(downstreamHead); } catch { stop('initial_ws_data_unadmitted_full_hold'); }
        if (!dead) downstream.resume();
      });
      upstreamRequest.once('response', response => {
        counts.provider_response_receipts++;
        receipt({ kind: 'non_generation_ws_upgrade_rejection', ...pathEvidence(p, protocol), status: response.statusCode });
        response.resume(); stop('provider_upgrade_rejected');
      });
      upstreamRequest.once('error', () => stop('provider_ws_tls_failure'));
      downstream.once('error', () => stop('downstream_ws_error_full_hold'));
      downstream.once('close', () => { unknownWsHolds('downstream_ws_socket_closed_outcome_unknown_full_hold'); releaseProviderCapture(); inflateClient?.destroy(); if (!dead) { dead = true; upstreamRequest.destroy(); upstreamSocket?.destroy(); } });
      receipt({ kind: 'non_generation_ws_upgrade_attempt', ...pathEvidence(p, protocol), source_scope: sourceScope, stream_lane: null, provider_received: false });
      upstreamRequest.end();
    } catch { stop(sealed ? 'sealed_new_ws_upgrade_denied' : 'unrecognized_or_guardless_ws_upgrade'); }
  });
  server.on('error', () => { fatal = true; });
  // The ledger is durable BEFORE bind. EADDRINUSE fails without touching the
  // pre-existing owner; close() only sees sockets accepted by this instance.
  await recordResource(ctx, { kind: 'listener', id: `127.0.0.1:${port}/tcp`, pid: process.pid, role: fixtureOnly ? 'synthetic_provider_boundary_http_ws_fixture' : 'actual_provider_boundary_http_ws', relay_identity: epoch, synthetic_fixture_only: fixtureOnly, delete: 'After explicit cleanup approval and matching relay identity: await owned providerBoundary.close(); never kill a port owner' });
  await new Promise((resolve, reject) => {
    const onError = () => reject(new Error('BLOCKED:exact_owned_port_bind_failed_existing_owner_untouched'));
    server.once('error', onError);
    server.listen({ host: '127.0.0.1', port, exclusive: true }, () => { server.removeListener('error', onError); resolve(); });
  });
  const upstreamBaseUrl = `http://127.0.0.1:${port}${targetPath.raw}`;
  const relay = {
    upstreamBaseUrl, port,
    guardIdentity() { return getGuard(); },
    sealGenerativeForwards: seal,
    beginPinnedNegativeProbe,
    settlePinnedNegativeProbe,
    metadata() {
      const sealState = sealMetadata();
      const reservationRows = [...reservations.values()];
      const reservationIds = new Set(reservationRows.map(value => value.id_sha256));
      const generationAttempts = attempts.filter(entry => entry.reservation_id_sha256 !== null);
      const allBoundsKnown = reservationRows.every(value => number(value.bound));
      const boundSum = allBoundsKnown ? reservationRows.reduce((sum, value) => sum + value.bound, 0) : null;
      return {
        schema: 'provider-boundary-relay-v1', protocol, port, source_scope: sourceScope,
        synthetic_fixture_only: fixtureOnly, tls_trust_scope: tlsTrustScope,
        tls_verification_policy: { reject_unauthorized: true, default_hostname_verification: true, explicit_ca_only: true, environment_extra_ca_accepted: false, system_trust_modified: false },
        source_original_url: { protocol: 'https:', host: target.hostname, port: Number(target.port || 443), origin_sha256: hash(target.origin), ...pathEvidence(targetPath, protocol), raw_query_excluded: true, auth_excluded: true },
        owned_override: { protocol: 'http:', host: '127.0.0.1', port, ...pathEvidence(targetPath, protocol), upstream_seed_only: true },
        endpoint_deviation: { local_upstream_override: true, source_original_origin_preserved_as_tls_target: true, custom_prefix_present: !/^\/(?:v1|v1beta|v1internal)?\/?$/.test(target.pathname), default_url_coverage_claimed: false, native_direct_provider_bypass_coverage: 'NOT_CLAIMED' },
        guard_identity_present: !!getGuard(), closed: closing, blocked: fatal || sealed && !sealState.sealing_complete,
        ...sealState,
        tls_receipts: tlsReceipts.map(entry => ({ ...entry })),
        tls_summary: { request_attempts: tlsReceipts.length, secure_connect_observed: tlsReceipts.filter(entry => entry.secure_connect_observed).length, certificate_and_hostname_verified: tlsReceipts.filter(entry => entry.certificate_authorized === true && entry.hostname_verified === true).length, provider_status_received: tlsReceipts.filter(entry => entry.status !== null).length },
        transport_policy: { redirects: 'NEVER_FOLLOW', automatic_request_retry: 'DISABLED', https_agent_reuse: false, pricing_scope: 'REFERENCE_USD_NOT_PROVIDER_INVOICE' },
        counters: { ...counts }, capture_bytes_held: captureHeld, capture_limit_bytes: CAPTURE_LIMIT, request_body_limit_bytes: BODY_LIMIT,
        attempts: attempts.map(entry => ({ ...entry })), receipts: receipts.map(entry => ({ ...entry })),
        reservation_attempt_matches: reservationRows.map(value => ({ reservation_id_sha256: value.id_sha256, reserved_bound_micros: value.bound, state: value.state, actualgeneration_writeattempts: value.writeAttemptCount, unique_actual_write_match: value.writeAttemptCount === 1, unattempted_full_hold: value.writeAttemptCount === 0 && value.state !== 'SETTLED' })),
        generation_attempt_reservation_join: {
          generation_attempts: generationAttempts.length, generation_counter_matches_attempt_records: counts.actualgeneration_writeattempts === generationAttempts.length,
          reservation_records: reservationRows.length, successful_reservation_bound_sum_micros: number(boundSum) ? boundSum : null, all_reservation_bounds_known: allBoundsKnown && number(boundSum),
          unmatched_generation_attempts: generationAttempts.filter(entry => !reservationIds.has(entry.reservation_id_sha256)).length,
          duplicate_reservation_write_matches: reservationRows.filter(value => value.writeAttemptCount > 1).length,
          all_successful_reservations_have_one_write_attempt: reservationRows.every(value => value.writeAttemptCount === 1),
        },
        outstanding: [...reservations.values()].filter(value => value.state !== 'SETTLED').map(value => ({ reservation_id_sha256: value.id_sha256, state: value.state, hold_policy: 'FULL_HOLD', attempted: value.attempted, bound_micros: value.bound, reason: value.unknownReason ?? null, unknown_transition_failed: value.unknownTransitionFailed === true })),
        settled: [...reservations.values()].filter(value => value.state === 'SETTLED').map(value => ({ reservation_id_sha256: value.id_sha256, usage: value.usage ?? null, settlement_kind: value.nonGenerationProven ? 'PINNED_NON_GENERATION_PROOF' : value.exactWsPrewarm && value.usage?.input_tokens === 0 && value.usage?.output_tokens === 0 ? 'EXPLICIT_ZERO_PREWARM_RAW_USAGE' : 'POSITIVE_RAW_USAGE' })),
        provider_received_claim: 'NONE_FROM_WRITE_OR_SEND; received_response_is_separate',
      };
    },
    async close() {
      if (closing) return;
      closing = true;
      for (const reserved of reservations.values()) markUnknown(reserved, null, 'owned_relay_closed_provider_outcome_unknown_full_hold');
      for (const request of requests) request.destroy();
      for (const socket of sockets) socket.destroy();
      await new Promise(resolve => server.close(() => resolve()));
      // Closing the owned transport intentionally retains every unresolved hold.
    },
  };
  BRANDED.add(relay);
  return Object.freeze(relay);
}
