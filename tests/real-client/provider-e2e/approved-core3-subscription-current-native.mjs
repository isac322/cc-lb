// Metadata-only helper: importing this module performs no I/O or execution.
import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { lstat, open } from 'node:fs/promises';
import path from 'node:path';
import { TextDecoder } from 'node:util';

export const CURRENT_NATIVE_ARTIFACT_ROOT = '/tmp/cc-lb-actual-cli-e2e-20261020/approved-core3-codex-shape-additional-tools-2026-10-22T13-21-26-226Z';
const SUMMARY_PATH = `${CURRENT_NATIVE_ARTIFACT_ROOT}/summary.json`;
const MAX_BYTES = 2 * 1024 * 1024;
const HASH = /^[a-f0-9]{64}$/;
const MODEL = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$/;
const OVERRIDE_FLAGS = ['CODEX_QA_MODEL_passed', 'generated_config_model_present', 'argv_model_flag_present'];
const CLASSIFICATION = 'SETUP_SHAPE_PROBE_ONLY_NOT_PROVIDER_QA';
const RESULT = 'CAPTURED_HTTP_AND_WS_SHAPES';
const CASE_RESULT = 'CAPTURED_TERMINAL_REJECTION_EXPECTED_NOT_CODING_SUCCESS';
const fail = (reason) => { throw new Error(`BLOCKED:current_native_${reason}`); };
const object = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const validHash = (value) => typeof value === 'string' && HASH.test(value);
const validString = (value, max) => typeof value === 'string' && value.length > 0 && value.length <= max && value.trim() === value && !/[\u0000-\u001f\u007f]/.test(value);

function validatePath(value) {
  // Exact lexical allowlist, including the filename, precedes any filesystem I/O.
  // Neither dot segments, sibling artifacts, credential paths nor aliases qualify.
  if (value !== SUMMARY_PATH) fail('evidence_path_invalid');
  return value;
}

function modelFromCases(cases) {
  if (!Array.isArray(cases) || cases.length !== 2) fail('http_ws_cases_required');
  const seen = new Set();
  let model;
  for (const entry of cases) {
    if (!object(entry) || !['http', 'ws'].includes(entry.name) || seen.has(entry.name)) fail('http_ws_cases_required');
    seen.add(entry.name);
    if (entry.result !== CASE_RESULT || entry.capture_parse_errors !== 0 || entry.timeout !== false || entry.signal !== null || entry.exit_code !== 1 || !Array.isArray(entry.terminal_error_codes) || entry.terminal_error_codes.length === 0 || entry.terminal_error_codes.some((code) => code !== 'setup_shape_probe_complete')) fail('case_setup_result_invalid');
    if (entry.supports_websockets_config !== (entry.name === 'ws') || !Number.isSafeInteger(entry.websocket_handshakes) || (entry.name === 'http' ? entry.websocket_handshakes !== 0 : entry.websocket_handshakes < 1)) fail('case_transport_invalid');
    if (!Array.isArray(entry.argv_shape) || entry.argv_shape.length === 0 || entry.argv_shape.some((arg) => typeof arg !== 'string')) fail('argv_metadata_invalid');
    // Cross-check the redacted argv projection rather than trusting its boolean
    // override receipt when it contradicts visible model/config flags.
    for (let index = 0; index < entry.argv_shape.length; index += 1) {
      const arg = entry.argv_shape[index];
      if (arg === '-m' || arg.startsWith('-m=') || arg === '--model' || arg.startsWith('--model=') || /^--config=(?:model|model_provider)\s*=/.test(arg) || ((arg === '-c' || arg === '--config') && /^(?:model|model_provider)\s*=/.test(entry.argv_shape[index + 1] ?? ''))) fail('model_override_detected');
    }
    if (!Array.isArray(entry.captures) || entry.captures.length === 0) fail('captures_missing');
    for (const capture of entry.captures) {
      if (!object(capture) || capture.transport !== entry.name || capture.pathname !== '/v1/responses') fail('capture_metadata_invalid');
      if (!object(capture.model) || capture.model.present !== true || typeof capture.model.value !== 'string' || !MODEL.test(capture.model.value)) fail('capture_model_invalid');
      if (model !== undefined && model !== capture.model.value) fail('captured_models_conflict');
      model = capture.model.value;
    }
  }
  return model;
}

export function parseCurrentNativeModelEvidence(raw, options = {}) {
  if (!object(options)) fail('options_invalid');
  const { expectedSha256, sourcePath } = options;
  if (!validHash(expectedSha256)) fail('expected_sha256_invalid');
  validatePath(sourcePath);
  if (!(typeof raw === 'string' || raw instanceof Uint8Array)) fail('metadata_bytes_invalid');
  const length = typeof raw === 'string' ? Buffer.byteLength(raw, 'utf8') : raw.byteLength;
  if (length === 0 || length > MAX_BYTES) fail('metadata_size_invalid');
  const bytes = typeof raw === 'string' ? Buffer.from(raw, 'utf8') : raw;
  // Verify the exact supplied bytes before attempting UTF-8 or JSON decoding.
  if (createHash('sha256').update(bytes).digest('hex') !== expectedSha256) fail('metadata_sha256_mismatch');
  let summary;
  try { summary = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); } catch { fail('metadata_json_invalid'); }
  if (!object(summary)) fail('metadata_object_required');
  if (summary.classification !== CLASSIFICATION || summary.result !== RESULT) fail('setup_shape_result_required');
  if (!object(summary.model_override)) fail('override_metadata_missing');
  if (OVERRIDE_FLAGS.some((flag) => summary.model_override[flag] !== false)) fail('model_override_detected');
  if (!object(summary.safety) || summary.safety.actual_credentials_loaded !== false || summary.safety.provider_forwarding !== false || summary.safety.fresh_synthetic_auth_only !== true) fail('local_synthetic_safety_required');
  const cli = summary.cli;
  if (!object(cli) || !validString(cli.path, 4096) || !path.isAbsolute(cli.path) || path.normalize(cli.path) !== cli.path || !validString(cli.version, 200) || !validHash(cli.sha256)) fail('cli_identity_invalid');
  const model = modelFromCases(summary.cases);
  // Tool metadata is intentionally not interpreted. Root tools absence is not
  // evidence of disabled search, a free prewarm, a price or bounded tool loops.
  return Object.freeze({
    model,
    cli: Object.freeze({ path: cli.path, version: cli.version, sha256: cli.sha256 }),
    provenance: Object.freeze({
      kind: 'LOCAL_SYNTHETIC_SETUP_NOT_AUTHENTICATED_SESSION',
      evidence_path: sourcePath,
      evidence_sha256: expectedSha256,
    }),
    no_model_override: true,
    current_user_session_default_proven: false,
    provider_qa_verified: false,
  });
}

export async function loadCurrentNativeModelEvidence(options = {}) {
  if (!object(options)) fail('options_invalid');
  const { path: filePath, sha256 } = options;
  validatePath(filePath);
  if (!validHash(sha256)) fail('expected_sha256_invalid');
  let handle;
  try {
    // /tmp itself can be the platform's standard alias. The ledger-owned base
    // and approved artifact root must both be actual directories, not symlinks.
    for (const directory of [path.dirname(CURRENT_NATIVE_ARTIFACT_ROOT), CURRENT_NATIVE_ARTIFACT_ROOT]) {
      const stat = await lstat(directory);
      if (!stat.isDirectory() || stat.isSymbolicLink()) fail('evidence_directory_invalid');
    }
    const before = await lstat(filePath);
    if (!before.isFile() || before.isSymbolicLink() || before.size === 0 || before.size > MAX_BYTES) fail('evidence_file_invalid');
    handle = await open(filePath, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    const opened = await handle.stat();
    if (!opened.isFile() || opened.dev !== before.dev || opened.ino !== before.ino || opened.size !== before.size) fail('evidence_file_changed');
    // Read a fixed upper bound plus one byte, not an unbounded readFile after a
    // racy size check. A file growing beyond the bound is rejected.
    const bytes = Buffer.alloc(MAX_BYTES + 1);
    let length = 0;
    while (length < bytes.length) {
      const { bytesRead } = await handle.read(bytes, length, bytes.length - length, length);
      if (bytesRead === 0) break;
      length += bytesRead;
    }
    if (length === 0 || length > MAX_BYTES) fail('metadata_size_invalid');
    const after = await handle.stat();
    if (after.size !== opened.size || after.mtimeMs !== opened.mtimeMs || after.ctimeMs !== opened.ctimeMs || length !== opened.size) fail('evidence_file_changed');
    return parseCurrentNativeModelEvidence(bytes.subarray(0, length), { expectedSha256: sha256, sourcePath: filePath });
  } catch (error) {
    // Only our fixed reason codes survive. OS/JSON errors never include paths,
    // raw metadata, credential contents or untrusted text in the caller error.
    if (error instanceof Error && /^BLOCKED:current_native_[a-z0-9_]+$/.test(error.message)) throw error;
    fail('evidence_read_failed');
  } finally {
    if (handle) {
      try { await handle.close(); } catch { fail('evidence_close_failed'); }
    }
  }
}
