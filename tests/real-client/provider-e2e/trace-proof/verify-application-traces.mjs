#!/usr/bin/env node
// Offline correlation only. This program never contacts the SUT or any network service.
import { watch } from 'node:fs';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { performance } from 'node:perf_hooks';

const BASE = '/tmp/cc-lb-actual-cli-e2e-20261020';
const BINARY_SHA256 = 'afb27555110c2072a6997adc76901c9d3d7cb0db1265a02496f22621d0b420cb';
const DEFAULTS = Object.freeze({
  'native-summary': `${BASE}/fixed-afb27555110c-trace-r5/omp-gpt/summary.json`,
  'gemini-summary': `${BASE}/fixed-afb27555110c-trace-r4/gemini-trace-control/summary.json`,
  capture: `${BASE}/trace-proof/actual-spans.json`,
  out: `${BASE}/trace-proof/application-trace-proof.json`,
  'deadline-ms': '45000',
});
const SOURCE_REFS = Object.freeze({
  root: 'crates/cc-lb-observability/src/trace_layer.rs:41-86',
  routes: 'crates/cc-lb-server/src/app.rs:2296-2315',
  handle: 'crates/cc-lb-engine/src/lifecycle.rs:2970-3005',
  attempt: 'crates/cc-lb-engine/src/lifecycle.rs:4668-4702',
  nativeWriter: `${BASE}/fixed-afb27555110c-trace-r5/omp-gpt/run.mjs:896-928,1317-1346`,
  receiver: `${BASE}/trace-proof/otlp-receiver.mjs:252-305,390-395`,
});
const HEX32 = /^(?!0{32}$)[0-9a-f]{32}$/;
const HEX16 = /^(?!0{16}$)[0-9a-f]{16}$/;
const isObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const nonempty = (value) => typeof value === 'string' && value.length > 0;

class ProofError extends Error {
  constructor(code, verdict = 'FAIL') { super(code); this.code = code; this.verdict = verdict; }
}
function requireValue(condition, code, verdict = 'FAIL') {
  if (!condition) throw new ProofError(code, verdict);
}
function parseOptions(args) {
  const options = { ...DEFAULTS };
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index];
    requireValue(typeof flag === 'string' && flag.startsWith('--') && Object.hasOwn(DEFAULTS, flag.slice(2)), 'unsupported_cli_option');
    const value = args[index + 1];
    requireValue(nonempty(value) && !value.startsWith('--'), 'missing_cli_option_value');
    options[flag.slice(2)] = value;
  }
  requireValue(/^\d+$/.test(options['deadline-ms']), 'invalid_deadline_ms');
  options.deadlineMs = Number(options['deadline-ms']);
  requireValue(Number.isSafeInteger(options.deadlineMs) && options.deadlineMs > 0 && options.deadlineMs <= 45000, 'deadline_ms_must_be_between_1_and_45000');
  for (const key of ['native-summary', 'gemini-summary', 'capture', 'out']) options[key] = path.resolve(options[key]);
  requireValue(!['native-summary', 'gemini-summary', 'capture'].some((key) => options[key] === options.out), 'output_must_not_overwrite_evidence');
  return options;
}
async function loadJson(file, label) {
  let text;
  try { text = await readFile(file, 'utf8'); }
  catch (error) { throw new ProofError(`${label}_${error?.code === 'ENOENT' ? 'missing' : 'unreadable'}`, 'INCOMPLETE'); }
  try { return JSON.parse(text); }
  catch { throw new ProofError(`${label}_invalid_json`, 'INCOMPLETE'); }
}
function traceExpectation(trace, code) {
  requireValue(isObject(trace), `${code}_trace_metadata_missing`, 'INCOMPLETE');
  requireValue(nonempty(trace.request_id) && HEX32.test(trace.trace_id) && HEX16.test(trace.parent_span_id), `${code}_trace_metadata_invalid`);
  requireValue(trace.trace_flags === '01' && trace.traceparent === `00-${trace.trace_id}-${trace.parent_span_id}-01`, `${code}_traceparent_mismatch`);
  return { requestId: trace.request_id, traceId: trace.trace_id, callerParentSpanId: trace.parent_span_id };
}
function nativeRoute(endpoint) {
  const route = typeof endpoint === 'string' ? endpoint.split('?')[0] : '';
  // These templates come from proxy_route_template, not observed-name guessing.
  if (route === '/v1/responses') return '/v1/responses';
  if (route === '/v1/chat/completions') return '/v1/{*path}';
  throw new ProofError('native_generation_endpoint_not_supported_by_known_writer');
}
function collectExpectations(native, gemini, options) {
  const expected = [];
  const runs = [];
  requireValue(isObject(native) && Array.isArray(native.runs) && native.runs.length > 0, 'native_runs_missing', 'INCOMPLETE');
  requireValue(native.binary_sha256?.local === BINARY_SHA256 && native.binary_sha256?.runtime === BINARY_SHA256 && native.binary_sha256?.expected === BINARY_SHA256, 'native_binary_sha256_mismatch');
  requireValue(nonempty(native.upstream_id), 'native_upstream_id_missing', 'INCOMPLETE');
  requireValue(native.verdict === 'PASS', 'native_summary_not_pass');
  const labels = new Set();
  for (const [runIndex, run] of native.runs.entries()) {
    requireValue(isObject(run) && nonempty(run.label) && !labels.has(run.label), 'native_run_label_invalid_or_duplicate');
    labels.add(run.label);
    requireValue(run.kind === 'cli' && Array.isArray(run.wire), 'native_run_wire_missing', 'INCOMPLETE');
    const generation = run.wire.filter((wire) => wire?.generation === true);
    const actual = generation.filter((wire) => wire.forwarded === true && wire.injected === false);
    requireValue(actual.length > 0, 'native_run_has_no_actual_generation_wires', 'INCOMPLETE');
    const runKey = `native:${run.label}`;
    runs.push({ run: runKey, source: 'native_cli', cliCoverage: true, summaryRef: `${options['native-summary']}#/runs/${runIndex}`, expectedCount: actual.length, excludedNonGenerationWireCount: run.wire.length - generation.length, excludedUnforwardedOrInjectedGenerationWireCount: generation.length - actual.length });
    for (const wire of actual) {
      const wireIndex = run.wire.indexOf(wire);
      const trace = traceExpectation(wire.qaTrace, 'native_wire');
      requireValue(wire.transport === 'http', 'native_generation_transport_not_http');
      requireValue(wire.upstreamId === native.upstream_id, 'native_wire_upstream_id_mismatch');
      requireValue(Number.isInteger(wire.httpStatus) && wire.httpStatus >= 200 && wire.httpStatus < 300, 'native_wire_not_successful');
      const driverRun = native.driverRuns?.find((item) => item.id === run.label);
      const digest = driverRun?.tap_digest?.find((item) => item.seq === wire.seq);
      requireValue(isObject(digest), 'native_driver_generation_digest_missing', 'INCOMPLETE');
      requireValue(digest.generation === true && digest.forwarded === true && digest.injected_locally === false && digest.upgrade === false, 'native_driver_generation_digest_mismatch');
      requireValue(digest.cclb_request_id === trace.requestId && digest.upstream_id === wire.upstreamId, 'native_echoed_request_id_or_upstream_mismatch');
      const rawTrace = traceExpectation(digest.qa_trace, 'native_driver');
      requireValue(rawTrace.requestId === trace.requestId && rawTrace.traceId === trace.traceId && rawTrace.callerParentSpanId === trace.callerParentSpanId, 'native_manifest_driver_trace_mismatch');
      expected.push({ ...trace, run: runKey, expectedUpstreamId: wire.upstreamId, expectedRootName: `POST ${nativeRoute(wire.endpoint)}`, expectedRoute: nativeRoute(wire.endpoint), method: 'POST', metadataRef: `${options['native-summary']}#/runs/${runIndex}/wire/${wireIndex}`, wireRef: nonempty(wire.ref) ? wire.ref : null });
    }
  }
  requireValue(isObject(gemini) && gemini.source === 'RAW' && gemini.CLIcoverage === false && gemini.verdict === 'PASS', 'gemini_raw_control_classification_or_verdict_mismatch');
  requireValue(gemini.binary?.sha256 === BINARY_SHA256 && gemini.binary?.expected === BINARY_SHA256 && gemini.binary?.match === true, 'gemini_binary_sha256_mismatch');
  const trace = gemini.expectedTrace;
  requireValue(isObject(trace) && nonempty(trace.requestId) && HEX32.test(trace.traceId) && HEX16.test(trace.parentSpanId), 'gemini_expected_trace_missing_or_invalid', 'INCOMPLETE');
  requireValue(gemini.request?.method === 'POST' && /^\/v1beta\/models\/[^/?]+:generateContent$/.test(gemini.request?.route ?? '') && gemini.paidPostInvocations === 1, 'gemini_generation_request_metadata_mismatch');
  requireValue(nonempty(gemini.guard?.upstreamId), 'gemini_upstream_id_missing', 'INCOMPLETE');
  requireValue(gemini.response?.status === 200 && gemini.response?.echoedRequestId === trace.requestId, 'gemini_echoed_request_id_or_status_mismatch');
  requireValue(gemini.observability?.event?.request_id === trace.requestId && gemini.observability?.event?.upstream_id === gemini.guard.upstreamId && gemini.observability?.matchedEventCount === 1 && gemini.observability?.committedEventCount === 1, 'gemini_committed_event_correlation_mismatch');
  const geminiRun = 'gemini:raw-protocol-control';
  runs.push({ run: geminiRun, source: 'raw_gemini_protocol_control', cliCoverage: false, summaryRef: options['gemini-summary'], expectedCount: 1 });
  expected.push({ run: geminiRun, requestId: trace.requestId, traceId: trace.traceId, callerParentSpanId: trace.parentSpanId, expectedUpstreamId: gemini.guard.upstreamId, expectedRootName: 'POST /v1beta/models/{model}:generateContent', expectedRoute: '/v1beta/models/{model}:generateContent', method: 'POST', metadataRef: `${options['gemini-summary']}#/expectedTrace`, wireRef: `${options['gemini-summary']}#/request` });
  requireValue(new Set(expected.map((item) => item.requestId)).size === expected.length, 'duplicate_expected_request_id');
  requireValue(new Set(expected.map((item) => item.traceId)).size === expected.length, 'duplicate_expected_trace_id');
  return { expected, runs };
}
function spanReference(span, index, capture) {
  return { name: span.name, spanId: span.spanId, traceId: span.traceId, parentSpanId: span.parentSpanId ?? null, startTimeUnixNano: span.startTimeUnixNano, endTimeUnixNano: span.endTimeUnixNano, serviceName: span.resource?.serviceName ?? null, captureRef: `${capture}#/spans/${index}` };
}
function inspectRequest(expected, spans, capture) {
  const checks = [];
  const check = (name, pass, verdict = 'FAIL') => checks.push({ name, pass, ...(pass ? {} : { verdict }) });
  const selected = spans.map((span, index) => ({ span, index })).filter(({ span }) => isObject(span) && (span.traceId === expected.traceId || span.attributes?.['cc_lb.request.id'] === expected.requestId));
  const traceSpans = selected.filter(({ span }) => span.traceId === expected.traceId);
  check('application_spans_received', selected.length > 0, 'INCOMPLETE');
  check('request_id_never_recorded_on_wrong_trace', selected.every(({ span }) => span.traceId === expected.traceId));
  check('all_correlated_spans_have_cc_lb_resource', selected.every(({ span }) => span.resource?.serviceName === 'cc-lb'));
  check('all_correlated_span_ids_well_formed', selected.every(({ span }) => HEX16.test(span.spanId) && (span.parentSpanId == null || HEX16.test(span.parentSpanId))));
  check('all_correlated_timestamps_well_formed_and_end_not_before_start', selected.every(({ span }) => {
    if (!/^[1-9]\d*$/.test(span.startTimeUnixNano ?? '') || !/^[1-9]\d*$/.test(span.endTimeUnixNano ?? '')) return false;
    return BigInt(span.endTimeUnixNano) >= BigInt(span.startTimeUnixNano);
  }));
  const byId = new Map(traceSpans.map((entry) => [entry.span.spanId, entry]));
  check('trace_span_ids_unique', byId.size === traceSpans.length);
  const roots = traceSpans.filter(({ span }) => span.name === expected.expectedRootName && span.attributes?.['cc_lb.request.id'] === expected.requestId);
  check('exact_source_derived_http_root_present_once', roots.length === 1, roots.length === 0 ? 'INCOMPLETE' : 'FAIL');
  const root = roots[0]?.span;
  check('http_root_has_caller_trace_id_and_parent_span_id', Boolean(root && root.traceId === expected.traceId && root.parentSpanId === expected.callerParentSpanId), root ? 'FAIL' : 'INCOMPLETE');
  check('http_root_method_and_route_match_source', Boolean(root && root.attributes?.['http.request.method'] === expected.method && root.attributes?.['http.route'] === expected.expectedRoute), root ? 'FAIL' : 'INCOMPLETE');
  function descendant(span, ancestorId) {
    if (!ancestorId) return false;
    const visited = new Set([span.spanId]);
    let parentId = span.parentSpanId;
    while (parentId) {
      if (parentId === ancestorId) return true;
      if (visited.has(parentId)) return false;
      visited.add(parentId);
      const parent = byId.get(parentId)?.span;
      if (!parent) return false;
      parentId = parent.parentSpanId;
    }
    return false;
  }
  const handles = traceSpans.filter(({ span }) => span.name === 'proxy.handle' && span.attributes?.['cc_lb.request.id'] === expected.requestId);
  check('proxy_handle_with_same_request_id_present_once', handles.length === 1, handles.length === 0 ? 'INCOMPLETE' : 'FAIL');
  const handle = handles[0]?.span;
  check('proxy_handle_descends_from_http_root', Boolean(handle && root && descendant(handle, root.spanId)), 'INCOMPLETE');
  const attempts = traceSpans.filter(({ span }) => span.name === 'proxy.upstream_attempt');
  check('proxy_upstream_attempt_present', attempts.length > 0, 'INCOMPLETE');
  check('all_attempts_have_expected_upstream_id', attempts.every(({ span }) => span.attributes?.['cc_lb.upstream.id'] === expected.expectedUpstreamId));
  check('all_attempts_descend_from_proxy_handle', Boolean(handle && attempts.length > 0 && attempts.every(({ span }) => descendant(span, handle.spanId))), 'INCOMPLETE');
  check('all_trace_spans_connect_to_http_root_without_cycles_or_missing_parents', Boolean(root && traceSpans.every(({ span }) => span.spanId === root.spanId || descendant(span, root.spanId))), 'INCOMPLETE');
  const failures = checks.filter((item) => !item.pass);
  const verdict = failures.some((item) => item.verdict === 'FAIL') ? 'FAIL' : failures.length > 0 ? 'INCOMPLETE' : 'PASS';
  return { ...expected, verdict, correlatedSpanCount: selected.length, checks, spans: selected.map(({ span, index }) => spanReference(span, index, capture)) };
}
function inspectCapture(snapshot, metadata, capture) {
  requireValue(isObject(snapshot) && snapshot.version === 1 && Array.isArray(snapshot.spans) && isObject(snapshot.stats), 'receiver_capture_schema_invalid', 'INVALID');
  const stats = {};
  for (const key of ['exportRequests', 'acceptedSpans', 'rejectedSpans', 'decodeErrors', 'oversizedRequests']) {
    requireValue(Number.isSafeInteger(snapshot.stats[key]) && snapshot.stats[key] >= 0, `receiver_${key}_invalid`, 'INVALID');
    stats[key] = snapshot.stats[key];
  }
  const captureChecks = [
    { name: 'no_rejected_spans', pass: stats.rejectedSpans === 0 },
    { name: 'no_decode_errors', pass: stats.decodeErrors === 0 },
    { name: 'no_oversized_requests', pass: stats.oversizedRequests === 0 },
    { name: 'accepted_span_counter_matches_capture_length', pass: stats.acceptedSpans === snapshot.spans.length },
    { name: 'real_exports_received', pass: stats.exportRequests > 0 },
  ];
  const requests = metadata.expected.map((item) => inspectRequest(item, snapshot.spans, capture));
  const runs = metadata.runs.map((run) => {
    const proofs = requests.filter((request) => request.run === run.run);
    return { ...run, verdict: proofs.every((proof) => proof.verdict === 'PASS') ? 'PASS' : proofs.some((proof) => proof.verdict === 'FAIL') ? 'FAIL' : 'INCOMPLETE', provenCount: proofs.filter((proof) => proof.verdict === 'PASS').length, requestIds: proofs.map((proof) => proof.requestId) };
  });
  const verdict = captureChecks.some((check) => !check.pass) ? 'INVALID' : requests.some((request) => request.verdict === 'FAIL') ? 'FAIL' : requests.every((request) => request.verdict === 'PASS') ? 'PASS' : 'INCOMPLETE';
  const selectedIndices = new Set(requests.flatMap((request) => request.spans.map((span) => span.captureRef)));
  return { verdict, captureStats: stats, captureChecks, capturedSpanCount: snapshot.spans.length, correlatedSpanCount: selectedIndices.size, excludedUnrelatedSpanCount: snapshot.spans.length - selectedIndices.size, expectedRequestCount: metadata.expected.length, provenRequestCount: requests.filter((request) => request.verdict === 'PASS').length, runs, requests };
}
async function awaitCapture(options, metadata) {
  const start = performance.now();
  let epoch = 0;
  let wake;
  let watcherError = false;
  let watcher;
  let latest;
  let deadlineReached = false;
  const notify = () => { epoch += 1; wake?.(); };
  try {
    watcher = watch(path.dirname(options.capture), (eventType, filename) => {
      if (filename === null || filename.toString() === path.basename(options.capture)) notify();
    });
    watcher.on('error', () => { watcherError = true; notify(); });
  } catch { throw new ProofError('capture_directory_watch_unavailable', 'INCOMPLETE'); }
  try {
    while (true) {
      const observedEpoch = epoch;
      try { latest = inspectCapture(await loadJson(options.capture, 'capture'), metadata, options.capture); }
      catch (error) {
        if (!(error instanceof ProofError)) throw error;
        latest = { verdict: error.verdict, error: error.code, expectedRequestCount: metadata.expected.length, provenRequestCount: 0, runs: metadata.runs.map((run) => ({ ...run, verdict: error.verdict, provenCount: 0 })), requests: metadata.expected.map((expected) => ({ ...expected, verdict: error.verdict, checks: [{ name: 'capture_available_and_valid', pass: false, verdict: error.verdict }], spans: [] })) };
      }
      if (latest.verdict === 'PASS' || latest.verdict === 'INVALID' || latest.verdict === 'FAIL') break;
      if (watcherError) { latest.watchError = 'capture_directory_watch_failed'; break; }
      const remaining = options.deadlineMs - (performance.now() - start);
      if (remaining <= 0) { deadlineReached = true; break; }
      if (epoch !== observedEpoch) continue;
      await new Promise((resolve) => {
        const timer = setTimeout(() => { deadlineReached = true; finish(); }, remaining);
        function finish() { clearTimeout(timer); wake = undefined; resolve(); }
        wake = finish;
        if (epoch !== observedEpoch || watcherError) finish();
      });
      if (deadlineReached) {
        // One final read closes the deadline/event race; timer expiry is never proof.
        try { latest = inspectCapture(await loadJson(options.capture, 'capture'), metadata, options.capture); }
        catch (error) { if (!(error instanceof ProofError)) throw error; latest.error = error.code; }
        break;
      }
    }
  } finally { watcher.close(); }
  return { ...latest, wait: { mechanism: 'fs.watch_capture_directory_atomic_rename', deadlineMs: options.deadlineMs, deadlineReached, elapsedMs: Math.round(performance.now() - start) } };
}

let options;
try {
  options = parseOptions(process.argv.slice(2));
  const [native, gemini] = await Promise.all([loadJson(options['native-summary'], 'native_summary'), loadJson(options['gemini-summary'], 'gemini_summary')]);
  const metadata = collectExpectations(native, gemini, options);
  const result = await awaitCapture(options, metadata);
  const output = { version: 1, generatedAt: new Date().toISOString(), purpose: 'actual application generation trace correlation; not native Gemini CLI coverage', binarySha256: BINARY_SHA256, inputs: { nativeSummary: options['native-summary'], geminiSummary: options['gemini-summary'], capture: options.capture }, sourceRefs: SOURCE_REFS, ...result };
  await writeFile(options.out, `${JSON.stringify(output, null, 2)}\n`, { mode: 0o600 });
  console.log(JSON.stringify({ verdict: output.verdict, expectedRequestCount: output.expectedRequestCount, provenRequestCount: output.provenRequestCount, captureStats: output.captureStats ?? null, out: options.out }));
  process.exitCode = output.verdict === 'PASS' ? 0 : output.verdict === 'INCOMPLETE' ? 2 : 1;
} catch (error) {
  const verdict = error instanceof ProofError ? error.verdict : 'INCOMPLETE';
  const code = error instanceof ProofError ? error.code : 'validator_io_or_internal_error';
  const output = { version: 1, generatedAt: new Date().toISOString(), verdict, error: code, sourceRefs: SOURCE_REFS };
  if (options) {
    output.inputs = { nativeSummary: options['native-summary'], geminiSummary: options['gemini-summary'], capture: options.capture };
    try { await writeFile(options.out, `${JSON.stringify(output, null, 2)}\n`, { mode: 0o600 }); }
    catch { output.outputWriteFailed = true; }
  }
  console.error(JSON.stringify({ verdict, error: code, ...(options ? { out: options.out } : {}), ...(output.outputWriteFailed ? { outputWriteFailed: true } : {}) }));
  process.exitCode = verdict === 'INCOMPLETE' ? 2 : 1;
}
