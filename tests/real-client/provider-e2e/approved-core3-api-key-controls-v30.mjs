// Parent-imported, scratch-only adapters. Construction imports declarations only;
// the parent explicitly invokes module.main() after its execution/freeze gate.
import { createHash, randomUUID } from 'node:crypto';
import path from 'node:path';
import { ClientRequest } from 'node:http';
import { rootCertificates, TLSSocket, checkServerIdentity } from 'node:tls';
import { isProviderBoundaryRelay } from './approved-core3-provider-boundary.mjs';

const must = (ok, code) => { if (!ok) throw new Error(`BLOCKED:${code}`); };
const hash = (value) => createHash('sha256').update(value).digest('hex');
const quote = JSON.stringify;
const url = (source) => `data:text/javascript;base64,${Buffer.from(source).toString('base64')}`;
const own = (object, key) => Object.prototype.hasOwnProperty.call(object, key);
const CONTROL_BOUNDARIES = new WeakSet();
const NODE_REQUEST_END = ClientRequest.prototype.end;
const PUBLIC_TLS_CA = Object.freeze([...rootCertificates]);
export function isApiKeyControlBoundary(value) {
  return value !== null && typeof value === 'object' && CONTROL_BOUNDARIES.has(value);
}
function wireBytes(value) {
  must(Buffer.isBuffer(value) || typeof value === 'string', 'control_original_wire_bytes_required');
  return Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf8');
}
function createControlBoundary({ plan, maxForwards, getGuard, getContext, isReady }) {
  const reservations = new Map();
  const attempts = [];
  const tlsReceipts = [];
  const usedRequests = new WeakSet();
  const observedSockets = new WeakMap();
  let sequence = 0;
  let actualWrites = 0;
  let blocked = false;
  let sealed = false;
  let sealAttempted = false;
  let sealGuard = null;
  let guardSealedObserved = false;
  let guardSealConfirmed = false;
  let sealIdentityInvalidated = false;
  let sealFailure = null;
  const nonnegative = (value) => Number.isSafeInteger(value) && value >= 0;
  function sealMetadata() {
    const same = sealed && sealGuard !== null && getGuard() === sealGuard;
    if (sealed && sealAttempted && sealGuard !== null && !same) sealIdentityInvalidated = true;
    const unchanged = same && !sealIdentityInvalidated;
    let currentPersisted = false;
    try {
      const current = unchanged && guardSealConfirmed ? sealGuard.snapshot() : null;
      currentPersisted = current?.sealed === true && current.seal_persisted === true;
    } catch { /* Never promote an unreadable current seal to proof. */ }
    const complete = sealed && unchanged && guardSealConfirmed && currentPersisted;
    return { sealed, future_generation_admission_denied: sealed,
      sealed_guard_identity_unchanged: unchanged, guard_sealed: guardSealedObserved,
      guard_seal_persisted: guardSealConfirmed && currentPersisted, sealing_complete: complete,
      seal_state: !sealed ? 'OPEN' : complete ? 'SEALED' : 'BLOCKED',
      seal_reason: !sealed || complete ? null : sealFailure ?? (!unchanged ? 'sealed_guard_identity_changed_or_absent' : 'shared_guard_seal_unconfirmed'),
      admission_denial_scope: complete ? 'THIS_RELAY_AND_IDENTICAL_SHARED_GUARD' : sealed ? 'THIS_RELAY_ONLY_SHARED_GUARD_UNCONFIRMED' : 'NOT_SEALED' };
  }
  function sealGenerativeForwards() {
    // Latch before touching the shared guard. Never close listeners or sockets.
    sealed = true;
    if (!sealAttempted) {
      sealAttempted = true;
      sealGuard = getGuard();
      try {
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
  function unknown(record) {
    if (!record) return;
    record.state = 'UNKNOWN';
    try { record.guard.settle({ id: record.id, usage: null, format: record.format, providerStatus: null }); }
    catch { blocked = true; }
  }
  function reserve(body, model, source, rawWireBody) {
    must(!sealed && !blocked && isReady() && getGuard() && getContext(), 'control_sealed_or_guard_absent_no_forward');
    must(reservations.size < maxForwards, 'control_original_forward_cap_exhausted');
    const bytes = wireBytes(rawWireBody);
    const guard = getGuard();
    const id = `${plan.name}:https:${++sequence}`;
    const admitted = guard.reserve({ id, model, body, source });
    must(admitted && typeof admitted.then !== 'function' && admitted.ok === true, 'control_durable_reservation_refused');
    const record = { id, guard, idSha256: hash(id), bound: admitted.bound_micros,
      wireHash: hash(bytes), wireLength: bytes.length, wireKind: Buffer.isBuffer(rawWireBody) ? 'buffer' : 'string',
      wireString: typeof rawWireBody === 'string' ? rawWireBody : null,
      format: model.startsWith('gemini-') ? 'gemini' : 'openai', writes: 0, state: 'IN_FLIGHT' };
    reservations.set(id, record);
    if (!nonnegative(record.bound)) { blocked = true; unknown(record); throw new Error('BLOCKED:control_reservation_bound_unknown'); }
    return id;
  }
  function trackTls(request, record, host) {
    const receipt = { reservation_id_sha256: record.idSha256, source_scope: 'provider-boundary', target_host: host, target_port: 443,
      secure_connect_observed: false, reused_same_observed_socket: false,
      certificate_authorized: null, hostname_verified: null, tls_protocol: null, status: null, transport_error: false };
    tlsReceipts.push(receipt);
    let boundSocket = null;
    const bind = (socket) => {
      if (boundSocket === socket) return;
      boundSocket = socket;
      if (!(socket instanceof TLSSocket)) { receipt.transport_error = true; return; }
      const prior = observedSockets.get(socket);
      if (prior) {
        receipt.secure_connect_observed = prior.secure_connect_observed;
        receipt.reused_same_observed_socket = true;
        receipt.certificate_authorized = prior.certificate_authorized;
        receipt.hostname_verified = prior.hostname_verified && prior.host === host;
        receipt.tls_protocol = prior.tls_protocol;
        return;
      }
      socket.once('secureConnect', () => {
        const protocol = socket.getProtocol();
        const observed = { host, secure_connect_observed: true,
          certificate_authorized: socket.authorized === true,
          hostname_verified: socket.authorized === true && socket.servername === host,
          tls_protocol: ['TLSv1.2', 'TLSv1.3'].includes(protocol) ? protocol : 'OTHER' };
        observedSockets.set(socket, observed);
        receipt.secure_connect_observed = observed.secure_connect_observed;
        receipt.certificate_authorized = observed.certificate_authorized;
        receipt.hostname_verified = observed.hostname_verified;
        receipt.tls_protocol = observed.tls_protocol;
      });
    };
    request.once('socket', bind);
    if (request.socket) bind(request.socket);
    request.once('response', (response) => { receipt.status = response.statusCode; });
    request.once('error', () => { receipt.transport_error = true; });
  }
  function writeReservedBody(id, request, actualBody) {
    const record = reservations.get(id);
    try {
      must(record && record.state === 'IN_FLIGHT' && !sealed && !blocked && isReady() && getGuard() === record.guard, 'control_sealed_or_changed_guard_no_body_write');
      const snapshot = record.guard.snapshot();
      const durableRecord = snapshot.records?.find((value) => value.id_sha256 === record.idSha256);
      must(snapshot.sealed !== true && durableRecord?.status === 'IN_FLIGHT' && durableRecord.reserved_bound_micros === record.bound, 'control_durable_reservation_not_open_for_write');
      must(request instanceof ClientRequest && request.protocol === 'https:' && !request.destroyed && !request.writableEnded && !usedRequests.has(request) && record.writes === 0, 'control_authentic_unique_https_request_required');
      const bytes = wireBytes(actualBody);
      must((Buffer.isBuffer(actualBody) ? 'buffer' : 'string') === record.wireKind && (record.wireKind !== 'string' || actualBody === record.wireString) && bytes.length === record.wireLength && hash(bytes) === record.wireHash, 'control_original_wire_body_changed');
      const host = request.getHeader('host');
      must(host === 'api.openai.com' || host === 'generativelanguage.googleapis.com', 'control_pinned_actual_provider_host_required');
      trackTls(request, record, host);
      const attempt = { reservation_id_sha256: record.idSha256, reserved_bound_micros: record.bound,
        source_scope: 'provider-boundary', kind: 'generation_https', request_bytes: bytes.length,
        request_sha256: record.wireHash, forwarded_sha256: record.wireHash,
        hash_byte_equality: true, provider_received: false };
      // Count the authentic end-call seam, not reservation or request creation.
      attempts.push(attempt);
      record.writes += 1;
      actualWrites += 1;
      usedRequests.add(request);
      // A throwing authentic end invocation remains a write ATTEMPT with an
      // unknown provider outcome; never erase it or refund its durable hold.
      return NODE_REQUEST_END.call(request, actualBody);
    } catch {
      unknown(record);
      throw new Error('BLOCKED:control_reserved_body_write_refused_full_hold');
    }
  }
  function settle(id, usage, format, providerStatus, complete, serviceTier) {
    const record = reservations.get(id);
    must(record, 'control_unreserved_settlement');
    const valid = complete && record.writes === 1 && positiveUsage(usage, format);
    const result = record.guard.settle({ id, usage: valid ? usage : null, format,
      providerStatus: valid ? providerStatus : null, ...(serviceTier == null ? {} : { serviceTier }) });
    const actual = result?.records?.find((value) => value.id_sha256 === record.idSha256);
    record.state = actual?.status ?? (result?.ok === true ? 'SETTLED' : 'UNKNOWN');
    return result;
  }
  const controller = Object.freeze({
    guardIdentity: getGuard,
    sealGenerativeForwards,
    metadata() {
      const rows = [...reservations.values()];
      const ids = new Set(rows.map((value) => value.idSha256));
      const boundsKnown = rows.every((value) => nonnegative(value.bound));
      const sum = boundsKnown ? rows.reduce((total, value) => total + value.bound, 0) : null;
      const allBoundsKnown = boundsKnown && nonnegative(sum);
      let guardRows = null;
      try { guardRows = getGuard()?.snapshot()?.records ?? null; } catch { /* No invented guard agreement. */ }
      const states = new Map((Array.isArray(guardRows) ? guardRows : []).map((value) => [value.id_sha256, value]));
      const sealState = sealMetadata();
      return { schema: 'api-key-control-boundary-v1', source_scope: 'provider-boundary',
        source_pid: process.pid, source_name: plan.name, source_root: plan.root, case: plan.case,
        source_slice_usd_cap: 3, selected_reference_slice_usd_cap: 6, campaign_usd_cap: 30,
        guard_identity_present: !!getGuard(), blocked: blocked || sealed && !sealState.sealing_complete, ...sealState,
        tls_trust_scope: 'EXPLICIT_BUNDLED_PUBLIC_ROOTS_WITHOUT_ENV_EXTRA_CA',
        tls_verification_policy: { reject_unauthorized: true, default_hostname_verification: true,
          explicit_ca_only: true, environment_extra_ca_accepted: false, system_trust_modified: false },
        original_trust_scope_changed: 'EXPLICIT_BUNDLED_PUBLIC_ROOTS_REPLACE_ORIGINAL_AMBIENT_TRUST',
        tls_receipts: tlsReceipts.map((value) => ({ ...value })),
        tls_summary: { request_attempts: tlsReceipts.length,
          secure_connect_observed: tlsReceipts.filter((value) => value.secure_connect_observed).length,
          certificate_and_hostname_verified: tlsReceipts.filter((value) => value.certificate_authorized === true && value.hostname_verified === true).length,
          provider_status_received: tlsReceipts.filter((value) => value.status !== null).length },
        transport_policy: { redirects: 'NEVER_FOLLOW', automatic_request_retry: 'ORIGINAL_CC_LB_RETRIES_EACH_RESERVED',
          https_agent_reuse: 'ORIGINAL_NODE_AGENT_RETAINED_OBSERVED_SOCKET_IDENTITY_REQUIRED', pricing_scope: 'REFERENCE_USD_NOT_PROVIDER_INVOICE' },
        counters: { actual_boundary_write_attempts: actualWrites, actualgeneration_writeattempts: actualWrites }, attempts: attempts.map((value) => ({ ...value })),
        reservation_attempt_matches: rows.map((value) => ({ reservation_id_sha256: value.idSha256,
          reserved_bound_micros: value.bound, state: states.get(value.idSha256)?.status ?? value.state,
          actualgeneration_writeattempts: value.writes, unique_actual_write_match: value.writes === 1,
          unattempted_full_hold: value.writes === 0 && (states.get(value.idSha256)?.status ?? value.state) !== 'SETTLED' })),
        generation_attempt_reservation_join: { generation_attempts: attempts.length,
          generation_counter_matches_attempt_records: actualWrites === attempts.length,
          reservation_records: rows.length, successful_reservation_bound_sum_micros: allBoundsKnown ? sum : null,
          all_reservation_bounds_known: allBoundsKnown,
          unmatched_generation_attempts: attempts.filter((value) => !ids.has(value.reservation_id_sha256)).length,
          duplicate_reservation_write_matches: rows.filter((value) => value.writes > 1).length,
          all_successful_reservations_have_one_write_attempt: rows.every((value) => value.writes === 1) },
        guard_record_join: { available: Array.isArray(guardRows),
          exact_successful_reservation_set: Array.isArray(guardRows) && guardRows.length === rows.length && rows.every((value) => states.has(value.idSha256)),
          reserved_bounds_match: Array.isArray(guardRows) && rows.every((value) => states.get(value.idSha256)?.reserved_bound_micros === value.bound) },
        provider_received_claim: 'NONE_FROM_WRITE_OR_SEND; received_response_is_separate',
        retention: { listeners_closed: false, sockets_closed_for_cleanup: false, files_deleted: false } };
    },
  });
  CONTROL_BOUNDARIES.add(controller);
  return { controller, reserve, writeReservedBody, settle };
}
const PROFILE = Object.freeze({
  faults: { source: 'faults/run.mjs', forwards: 16 },
  'native-http-cancel': { source: 'approved-core3-native-http-cancel.mjs', forwards: 3, caseForwards: 16 },
  'native-http-cancel-concurrent': { source: 'approved-core3-native-http-cancel.mjs', forwards: 3, caseForwards: 16 },
  'gemini-accounting-control': { source: 'approved-core3-controls.mjs', forwards: 1 },
  'anthropic-regression': { source: 'approved-core3-anthropic-regression.mjs', forwards: 1 },
});
function once(source, from, to, label) {
  must(typeof source === 'string' && source.split(from).length === 2, `control_source_drift:${label}`);
  return source.replace(from, to);
}
// Verify the campaign-generated declarations before importing any data module.
// These checks do not rewrite ports or accept legacy-source alternatives.
function freshPortDeclarations(plan, config, source) {
  const offset = config.portOffset;
  // The v30 campaign epoch uses offset 1000; every port below is still derived
  // from the same original base ports and checked line-exactly in the source.
  must([500, 600, 700, 800, 900, 1000, 1100].includes(offset), 'control_fresh_manifest_port_offset_required');
  const line = (expected, label) => {
    must(source.split('\n').filter((value) => value === expected).length === 1, `control_source_drift:${label}`);
  };
  const mapped = (ports) => ports.map((port) => port + offset);
  const selected = (ports) => must(JSON.stringify(plan.ports) === JSON.stringify(mapped(ports)), 'control_plan_port_declarations_mismatch');
  let listeners = [];
  if (plan.native === 'faults') {
    selected([18540, 18541, 18542]);
    line(`  if (raw !== '${offset}') throw new Error('QA_FAULT_PORT_OFFSET must match approved core3 epoch');`, 'fault_fresh_offset_validator');
    line('const PORTS = [18540, 18541, 18542].map((p) => p + PORT_OFFSET);', 'fault_runtime_ports');
    line('const RELAY_A = 18543 + PORT_OFFSET;', 'fault_relay_a');
    line('const RELAY_B = 18544 + PORT_OFFSET;', 'fault_relay_b');
    listeners = mapped([18543, 18544]);
  } else if (plan.native.startsWith('native-http-cancel')) {
    const concurrent = plan.native.endsWith('-concurrent');
    selected(concurrent ? [18590, 18591, 18592] : [18550, 18551, 18552]);
    line('const DRIVER_PORTS = CONCURRENT_MODE ? [18590, 18591, 18592] : [18550, 18551, 18552];', 'cancel_fresh_disjoint_driver_ports');
    line(`const RELAY_PORT = CONCURRENT_MODE ? ${18593 + offset} : ${18553 + offset};`, 'cancel_fresh_relay_ports');
    line(`const TAP_PORT = CONCURRENT_MODE ? ${18594 + offset} : ${18554 + offset};`, 'cancel_fresh_tap_ports');
    listeners = mapped(concurrent ? [18593, 18594] : [18553, 18554]);
  } else if (plan.native === 'anthropic-regression') {
    selected([18570, 18571, 18572]);
    line(`  ctx = await prepareSlice({ name: 'anthropic-regression', ports: [${18570 + offset}, ${18571 + offset}, ${18572 + offset}], provider: 'anthropic', model });`, 'anthropic_fresh_runtime_ports');
  } else {
    selected([18580, 18581, 18582]);
    line('  const ports = [18580 + offset, 18581 + offset, 18582 + offset];', 'gemini_runtime_port_expression');
  }
  return { manifestOffset: offset, runtimePorts: [...plan.ports], localRelayAndTapPorts: listeners };
}
function positiveUsage(usage, format) {
  if (!usage || typeof usage !== 'object' || Array.isArray(usage)) return false;
  const integer = (value) => Number.isSafeInteger(value) && value >= 0;
  if (format === 'gemini') {
    const thoughts = usage.thoughtsTokenCount ?? 0;
    return integer(usage.promptTokenCount) && usage.promptTokenCount > 0
      && integer(usage.candidatesTokenCount) && integer(thoughts)
      && Number.isSafeInteger(usage.candidatesTokenCount + thoughts)
      && usage.candidatesTokenCount + thoughts > 0;
  }
  const input = format === 'anthropic'
    ? [usage.input_tokens, usage.cache_read_input_tokens ?? 0, usage.cache_creation_input_tokens ?? 0]
    : [usage.input_tokens];
  return input.every(integer) && input.reduce((sum, value) => sum + value, 0) > 0
    && Number.isSafeInteger(input.reduce((sum, value) => sum + value, 0))
    && integer(usage.output_tokens) && usage.output_tokens > 0;
}
// This parser is settlement-only. It never rewrites forwarded bytes or serializes
// provider bodies; malformed, incomplete and rejected results retain their hold.
function rawResult(text, format) {
  let usage = null;
  let serviceTier;
  let completed = format !== 'openai';
  let invalid = false;
  const absorb = (object, sse) => {
    if (!object || typeof object !== 'object' || object.error || object.response?.error) { invalid = true; return; }
    const response = object.response && typeof object.response === 'object' ? object.response : object;
    if (format === 'openai') {
      if (sse && object.type === 'response.completed') completed = true;
      if (!sse) completed = response.status === 'completed';
      if (response.usage) usage = response.usage;
      if (response.service_tier != null) serviceTier = response.service_tier;
      if (sse && ['response.failed', 'response.incomplete', 'error'].includes(object.type)) invalid = true;
    } else if (format === 'gemini') {
      if (object.usageMetadata) usage = object.usageMetadata;
    } else if (object.usage) usage = object.usage;
  };
  try {
    if (/^(?:event|data):/m.test(text.slice(0, 256))) {
      let count = 0;
      for (const line of text.split('\n')) {
        if (!line.startsWith('data:')) continue;
        const data = line.slice(5).trim();
        if (!data || data === '[DONE]') continue;
        count += 1;
        absorb(JSON.parse(data), true);
      }
      if (!count) invalid = true;
    } else absorb(JSON.parse(text), false);
  } catch { invalid = true; }
  return { usage: !invalid && completed ? usage : null, serviceTier, complete: !invalid && completed };
}
function helperModule(key, runtime) {
  const names = Object.keys(runtime).filter((name) => /^[A-Za-z_$][\w$]*$/.test(name) && name !== 'default');
  must(names.includes('prepareSlice'), 'control_runtime_prepare_missing');
  return `const p=globalThis[Symbol.for(${quote(key)})];if(!p)throw Error('BLOCKED:private_control_bridge_missing');\n`
    + names.map((name) => {
      if (name === 'prepareSlice') return 'export const prepareSlice=(...args)=>p.prepareSlice(...args);';
      if (name === 'redactText') return 'export const redactText=(value)=>p.redactText(value);';
      if (name === 'redact') return 'export const redact=(value)=>p.redact(value);';
      if (name === 'BINARY_PATH') return 'export const BINARY_PATH=p.artifact.path;';
      if (name === 'EXPECTED_BINARY_SHA256') return 'export const EXPECTED_BINARY_SHA256=p.artifact.expected_sha256;';
      return `export const ${name}=p.runtime.${name};`;
    }).join('\n');
}
const FAULT_TAIL = `const deadline = setTimeout(async () => {
  summary.blocked.push({ scenarioId: '*', kind: 'budget', prerequisite: \`run deadline \${RUN_DEADLINE_MS}ms exceeded\`, ref: 'evidence/fault-summary.json' });
  process.exitCode = await finalize(3);
  process.exit();
}, RUN_DEADLINE_MS);

main()
  .then((code) => finalize(code))
  .catch(async (error) => {
    summary.fatal = redactText(error.stack ?? error.message).slice(0, 1500);
    console.log(\`FATAL: \${redactText(error.message)}\`);
    return finalize(1);
  })
  .then((code) => { clearTimeout(deadline); process.exitCode = code; });`;
const CANCEL_TAIL = `const deadline = setTimeout(async () => {
  summary.blocked.push({ scenarioId: 'x.cancel_release', kind: 'budget', prerequisite: \`run deadline \${RUN_DEADLINE_MS}ms exceeded\`, scope: 'case', ref: REF });
  summary.verdict = 'BLOCKED';
  process.exitCode = await finalize(3);
  process.exit();
}, RUN_DEADLINE_MS);

main()
  .then(() => finalize(0))
  .catch(async (error) => { summary.fatal = redactText(error.stack ?? error.message).slice(0, 1500); summary.verdict = 'BLOCKED'; console.log(\`FATAL: \${redactText(error.message)}\`); return finalize(1); })
  .then((code) => { clearTimeout(deadline); process.exitCode = code; });`;
function parentMain(source, tail, label) {
  source = once(source, 'async function main() {', 'async function originalMain() {', `${label}_main_declaration`);
  const retained = once(tail, '  process.exit();\n', '', `${label}_deadline_retains_parent_and_listeners`);
  const body = once(retained, '\nmain()\n', '\nreturn originalMain()\n', `${label}_tail_invocation`);
  return once(source, tail, `export async function main() {\n${body}\n}`, `${label}_direct_main`);
}

/**
 * prepared = {runtime, prepareGuard: async (actualContext) => durableGuard,
 * getProviderBoundary: () => brandedRelay}; the getter is mandatory for raw controls.
 * pinned.sources contains the fresh epoch's verified source text, keyed by the
 * names in PROFILE, plus faults/relay.mjs. config.cancellationGuard, if supplied,
 * replaces only config.pinnedCancellationGuard's exact declaration. config includes
 * expectedSha, epoch, otlpEndpoint; plan.ports are final, already-owned ports.
 */
export async function buildGuardedApiKeyControlModule(plan, config, pinned, prepared, privateValues) {
  const profile = PROFILE[plan?.native];
  must(profile && plan.case === `api-key-${plan.native}` && plan.name === plan.native, 'unknown_api_key_control_profile');
  must(plan.caps?.case_forwards === (profile.caseForwards ?? profile.forwards) && plan.caps.slice_usd === 6 && plan.caps.campaign_usd === 30, 'control_approved_caps_required');
  // The selected cap30 contract keeps the historical $3 per-slice bound; the
  // historical $15 campaign stays pinned in the campaign manifest, not here.
  must(pinned?.contract?.paid_bounds?.usd_ceiling_per_slice === 3 && pinned.contract.paid_bounds.usd_ceiling_campaign === 30, 'control_selected_contract_caps_required');
  must(prepared?.runtime && typeof prepared.prepareGuard === 'function' && Array.isArray(privateValues), 'control_guard_preparer_required');
  must(path.isAbsolute(plan.epoch) && config?.epoch === plan.epoch && plan.root === path.join(plan.epoch, plan.name), 'control_owned_epoch_required');
  must(Array.isArray(plan.ports) && plan.ports.length === 3 && plan.ports.every((port) => Number.isSafeInteger(port) && port > 1024 && port < 65536), 'control_owned_ports_required');
  must(typeof plan.model === 'string' && plan.model.length > 0 && /^[0-9a-f]{64}$/.test(config.expectedSha ?? ''), 'control_model_and_sha_required');
  const runtime = prepared.runtime;
  const artifact = pinned.artifact;
  must(artifact && Object.isFrozen(artifact) && typeof artifact.path === 'string' && path.isAbsolute(artifact.path) && /^[0-9a-f]{64}$/.test(artifact.expected_sha256 ?? '') && artifact.expected_sha256 === plan.expectedSha && artifact.expected_sha256 === config.expectedSha, 'control_pinned_paid_artifact_required');
  must(pinned.contract.artifact?.path === artifact.path && pinned.contract.artifact.expected_sha256 === artifact.expected_sha256 && pinned.manifest?.binary?.path === artifact.path && pinned.manifest.binary.expected_sha256 === artifact.expected_sha256, 'control_pinned_artifact_pair_mismatch');
  must(runtime.CLIENT_LIMITS?.sliceCostUsd === 6 && runtime.CLIENT_LIMITS.campaignCostUsd === 30 && await runtime.sha256File(artifact.path) === artifact.expected_sha256, 'control_runtime_reference_caps_or_sha_mismatch');
  const getSource = (name) => {
    must(own(pinned.sources ?? {}, name) && typeof pinned.sources[name] === 'string', `control_pinned_source_missing:${name}`);
    return pinned.sources[name];
  };
  const portExpectations = freshPortDeclarations(plan, config, getSource(profile.source));
  const rawBoundary = ['gemini-accounting-control', 'anthropic-regression'].includes(plan.native);
  must(!rawBoundary || typeof prepared.getProviderBoundary === 'function', 'raw_control_provider_boundary_getter_required');
  must(!rawBoundary || (Number.isSafeInteger(plan.provider_boundary_port) && plan.provider_boundary_port > 1024 && plan.provider_boundary_port < 65536), 'raw_control_notified_provider_boundary_port_required');
  if (rawBoundary) portExpectations.providerBoundaryPort = plan.provider_boundary_port;
  const key = `cc-lb-approved-api-key-controls-${randomUUID()}`;
  let preparing = false;
  let preparedOnce = false;
  const secrets = privateValues;
  const scrub = (value) => {
    let text = String(value ?? '');
    for (const secret of secrets) if (typeof secret === 'string' && secret.length) text = text.split(secret).join('[REDACTED]');
    return runtime.redactText(text);
  };
  must(rawBoundary || PUBLIC_TLS_CA.length > 0, 'control_bundled_public_trust_roots_required');
  const control = rawBoundary ? null : createControlBoundary({ plan, maxForwards: profile.forwards,
    getGuard: () => bridge.guard, getContext: () => bridge.ctx, isReady: () => preparedOnce });
  const ompIdentity = plan.native.startsWith('native-http-cancel') ? await import('./approved-core3-subscription-native-v30.mjs') : null;
  const bridge = {
    runtime, artifact, ctx: null, guard: null, providerBoundary: null,
    controller: control?.controller ?? null, tlsCa: PUBLIC_TLS_CA, checkServerIdentity,
    options: Object.freeze({ epoch: plan.epoch, kind: 'gemini' }),
    approvedLauncher: () => ompIdentity.assertVerifiedOmpClient(prepared.approvedClient, pinned.contract),
    versionEnvironment: (home) => ompIdentity.ompVersionProbeEnvironment(home),
    get approvedClientHome() { ompIdentity.assertVerifiedOmpClient(prepared.approvedClient, pinned.contract); return prepared.approvedClient.probe.home; },
    assertVersion(result) { ompIdentity.assertVerifiedOmpClient(prepared.approvedClient, pinned.contract); return ompIdentity.assertApprovedOmpVersion(prepared.approvedClient.identity, result); },
    redactText: scrub,
    redact: (value) => JSON.parse(scrub(JSON.stringify(runtime.redact(value)))),
    async prepareSlice(options) {
      must(!preparing && !preparedOnce && options?.name === plan.name && options.model === plan.model && options.resume !== true, 'control_fresh_prepare_identity_required');
      preparing = true;
      try {
        const ctx = await runtime.prepareSlice({ ...options, artifact });
        bridge.ctx = ctx;
        for (const value of [ctx.clientKey, ctx._adminToken]) if (typeof value === 'string' && value.length && !secrets.includes(value)) secrets.push(value);
        must(ctx.root === plan.root && ctx.binaryPath === artifact.path && ctx.binarySha256 === artifact.expected_sha256 && ctx.expectedBinarySha256 === artifact.expected_sha256 && JSON.stringify(ctx.ports) === JSON.stringify(plan.ports), 'control_prepared_identity_mismatch');
        must(ctx.limits?.client?.sliceCostUsd === 6 && ctx.limits.client.campaignCostUsd === 30, 'control_prepared_caps_changed');
        must(Array.isArray(ctx.blocked) && ctx.blocked.length === 0, 'control_runtime_blocked');
        const guard = await prepared.prepareGuard(ctx);
        must(guard && typeof guard.reserve === 'function' && typeof guard.settle === 'function', 'control_durable_guard_missing');
        must(rawBoundary || (typeof guard.snapshot === 'function' && typeof guard.seal === 'function'), 'control_shared_guard_snapshot_and_seal_required');
        bridge.guard = guard;
        if (rawBoundary) {
          bridge.providerBoundary = prepared.getProviderBoundary();
          bridge.assertRawBoundary(ctx);
        }
        preparedOnce = true;
        return ctx;
      } finally { preparing = false; }
    },
    reserve(body, model, source, rawWireBody) {
      must(control, 'raw_control_source_reservation_forbidden');
      return control.reserve(body, model, source, rawWireBody);
    },
    writeReservedBody(id, request, actualBody) {
      must(control, 'raw_control_source_body_write_forbidden');
      return control.writeReservedBody(id, request, actualBody);
    },
    settle(id, usage, format, providerStatus, complete, serviceTier) {
      must(control, 'raw_control_source_settlement_forbidden');
      return control.settle(id, usage, format, providerStatus, complete, serviceTier);
    },
    settleText(id, text, format, status, complete) {
      const result = rawResult(text, format);
      return bridge.settle(id, result.usage, format, status, complete && result.complete, result.serviceTier);
    },
    assertRawBoundary(ctx, expectedKind, expectedHost) {
      must(rawBoundary && ctx === bridge.ctx && bridge.guard, 'raw_control_actual_prepared_context_required');
      const relay = prepared.getProviderBoundary();
      must(isProviderBoundaryRelay(relay) && relay === bridge.providerBoundary && relay.guardIdentity() === bridge.guard, 'raw_control_branded_provider_guard_identity_required');
      const kind = plan.native === 'gemini-accounting-control' ? 'gemini_api_key' : 'anthropic_api_key';
      const protocol = kind === 'gemini_api_key' ? 'google-generate-content' : 'anthropic-messages';
      const upstream = ctx.upstreams?.find((value) => value.id === ctx.upstreamId);
      must(ctx.upstreams?.length === 1 && upstream?.kind === kind && (expectedKind == null || expectedKind === kind), 'raw_control_original_api_key_kind_required');
      must(upstream.baseUrl === relay.upstreamBaseUrl && new URL(upstream.baseUrl).hostname === '127.0.0.1' && new URL(upstream.baseUrl).protocol === 'http:' && new URL(upstream.baseUrl).port === String(plan.provider_boundary_port), 'raw_control_seeded_local_boundary_required');
      const metadata = relay.metadata();
      must(metadata.schema === 'provider-boundary-relay-v1' && metadata.protocol === protocol && metadata.source_scope === 'provider-boundary' && metadata.port === relay.port && relay.port === plan.provider_boundary_port && !metadata.closed && !metadata.blocked, 'raw_control_provider_boundary_metadata_mismatch');
      must(metadata.owned_override?.upstream_seed_only === true && metadata.owned_override.host === '127.0.0.1' && metadata.owned_override.port === relay.port && metadata.source_original_url?.protocol === 'https:' && Number.isSafeInteger(metadata.source_original_url.port) && metadata.source_original_url.port > 0 && metadata.source_original_url.port < 65536 && metadata.endpoint_deviation?.default_url_coverage_claimed === false && metadata.endpoint_deviation.local_upstream_override === true, 'raw_control_honest_override_scope_required');
      must((expectedHost == null || metadata.source_original_url.host === expectedHost) && (kind !== 'gemini_api_key' || metadata.source_original_url.host === 'generativelanguage.googleapis.com'), 'raw_control_original_google_host_required');
      return true;
    },
    providerBoundaryScope() {
      bridge.assertRawBoundary(bridge.ctx);
      const metadata = bridge.providerBoundary.metadata();
      // Configuration scope only: actual TLS receipts and outcomes belong to the
      // relay's post-execution evidence, never preconstructed adapter evidence.
      return { source_scope: 'provider-boundary', source_original_url: metadata.source_original_url,
        owned_override: metadata.owned_override, endpoint_deviation: metadata.endpoint_deviation };
    },
  };
  const sourceMap = {};
  const remember = (name, transformed) => {
    sourceMap[name] = { original: path.join(plan.epoch, name), originalSha256: hash(getSource(name)), transformedSha256: hash(transformed) };
    return transformed;
  };
  const helperUrl = url(remember('qa-runtime.mjs', helperModule(key, runtime)));
  let source = getSource(profile.source);
  if (source.startsWith('#!/usr/bin/env node\n')) source = once(source, '#!/usr/bin/env node\n', '', 'control_shebang');
  source = `const API_KEY_PRIVATE=globalThis[Symbol.for(${quote(key)})];if(!API_KEY_PRIVATE)throw Error('BLOCKED:private_control_bridge_missing');\n${source}`;
  if (plan.native === 'faults') {
    const offset = plan.ports[0] - 18540;
    must([500, 600, 700, 800, 900, 1000].includes(offset) && config.portOffset === offset && JSON.stringify(plan.ports) === JSON.stringify([18540, 18541, 18542].map((port) => port + offset)), 'fault_owned_epoch_port_offset_required');
    source = once(source, "from '../qa-runtime.mjs'", `from ${quote(helperUrl)}`, 'fault_private_runtime');
    source = once(source, 'const HERE = path.dirname(fileURLToPath(import.meta.url));', `const HERE = ${quote(path.join(plan.epoch, 'faults'))};`, 'fault_static_owned_root');
    source = once(source, "const SLICE = process.env.QA_FAULT_SLICE ?? 'faults';", `const SLICE = ${quote(plan.name)};`, 'fault_fresh_slice');
    source = once(source, "  const raw = process.env.QA_FAULT_PORT_OFFSET ?? '0';", `  const raw = ${quote(String(offset))};`, 'fault_fixed_epoch_offset');
    source = once(source, "const OPENAI_MODEL = process.env.QA_FAULT_OPENAI_MODEL ?? 'gpt-4.1-mini';", `const OPENAI_MODEL = ${quote(plan.model)};`, 'fault_pinned_model');
    source = once(source, 'const USD_CEILING = Math.min(3, CLIENT_LIMITS.sliceCostUsd);', 'const USD_CEILING = Math.min(6, CLIENT_LIMITS.sliceCostUsd);', 'fault_new_slice_cap_only');
    source = once(source, "const RESUME = process.env.QA_RESUME === '1';", 'const RESUME = false;', 'fault_no_historical_resume');
    let relay = getSource('faults/relay.mjs');
    relay = `const API_KEY_PRIVATE=globalThis[Symbol.for(${quote(key)})];\n${relay}`;
    relay = once(relay, '    forwardCount += 1;', `    const reservationBody = JSON.parse(body.toString('utf8'));
    const reservationModel = entry.target === 'gemini' ? decodeURIComponent(url.pathname.match(/\\/models\\/([^/:]+)(?::|$)/)?.[1] ?? '') : reservationBody.model;
    const reservationId = API_KEY_PRIVATE.reserve(reservationBody, reservationModel, 'provider-boundary:fault_real_https_retry_or_failover', body);`, 'fault_reserve_true_https_only');
    relay = once(relay, '    let finished = false;', '    let finished = false;\n    let providerComplete = false;', 'fault_track_provider_end');
    relay = once(relay, "        up.on('end', () => { if (!broken) res.end(); done(); });", "        up.on('end', () => { providerComplete = true; if (!broken) res.end(); done(); });", 'fault_complete_not_partial');
    relay = once(relay, "    Object.assign(entry, summarizeProviderBody(Buffer.concat(capture).toString('utf8'), res.getHeader('content-type')));", `    API_KEY_PRIVATE.settleText(reservationId, Buffer.concat(capture).toString('utf8'), entry.target === 'gemini' ? 'gemini' : 'openai', entry.status, providerComplete && !entry.error && !entry.downstream_closed_early && !entry.stream_break_after_bytes && captured === entry.response_bytes);
    Object.assign(entry, summarizeProviderBody(Buffer.concat(capture).toString('utf8'), res.getHeader('content-type')));`, 'fault_raw_complete_usage_settlement');
    relay = once(relay, '        path: `${url.pathname}${url.search}`, headers, timeout: 120_000,', '        path: `${url.pathname}${url.search}`, headers, timeout: 120_000, ca: API_KEY_PRIVATE.tlsCa, rejectUnauthorized: true, checkServerIdentity: API_KEY_PRIVATE.checkServerIdentity,', 'fault_explicit_public_ca_and_default_hostname_verification');
    relay = once(relay, '      upstream.end(body);', '      API_KEY_PRIVATE.writeReservedBody(reservationId, upstream, body);\n      forwardCount += 1;', 'fault_actual_client_request_end_seam');
    source = once(source, "from './relay.mjs'", `from ${quote(url(remember('faults/relay.mjs', relay)))}`, 'fault_guarded_relay_import');
    source = once(source, 'async function finalize(code) {', 'async function finalize(code) {\n  summary.provider_boundary_seal = API_KEY_PRIVATE.controller.sealGenerativeForwards();', 'fault_seal_before_final_evidence');
    source = once(source, '  if (relay) await relay.close();', '  if (relay) summary.provider_boundary_seal = API_KEY_PRIVATE.controller.sealGenerativeForwards();', 'fault_retain_real_listener_and_durably_seal');
    source = parentMain(source, FAULT_TAIL, 'fault');
  } else if (plan.native.startsWith('native-http-cancel')) {
    source = once(source, "from './qa-runtime.mjs'", `from ${quote(helperUrl)}`, 'cancel_private_runtime');
    source = once(source, "  const ompPath = findExecutable('omp');", '  const ompPath = API_KEY_PRIVATE.approvedLauncher();', 'cancel_approved_omp_launcher');
    source = once(source, "env: { PATH: process.env.PATH ?? '/usr/bin:/bin', HOME: '/nonexistent-qa-home', TERM: 'dumb', NO_COLOR: '1' }", 'env: API_KEY_PRIVATE.versionEnvironment(API_KEY_PRIVATE.approvedClientHome)', 'cancel_cloud_key_free_version_probe');
    source = once(source, "  const version = (ver.stdout ?? '').trim();", "  const version = (ver.stdout ?? '').trim();\n  API_KEY_PRIVATE.assertVersion(ver);", 'cancel_exact_approved_version_gate');
    source = once(source, "    ompChild = spawn(ompPath, args, { cwd: project, env, stdio: ['ignore', 'pipe', 'pipe'] });", "    if (ompPath !== API_KEY_PRIVATE.approvedLauncher()) throw Error('BLOCKED:approved_omp_launch_mismatch');\n    ompChild = spawn(ompPath, args, { cwd: project, env, stdio: ['ignore', 'pipe', 'pipe'] });", 'cancel_approved_identity_before_native_spawn');
    source = once(source, "const SLICE = process.env.QA_NATIVE_CANCEL_SLICE ?? 'native-http-cancel';", `const SLICE = ${quote(plan.name)};`, 'cancel_fresh_slice');
    source = once(source, "const RESUME = process.env.QA_NATIVE_CANCEL_RESUME === '1';", 'const RESUME = false;', 'cancel_no_historical_resume');
    source = once(source, "const LIMIT_KIND = process.env.QA_NATIVE_CANCEL_LIMIT_KIND ?? 'cost_usd';", `const LIMIT_KIND = ${quote(plan.native.endsWith('-concurrent') ? 'concurrent' : 'cost_usd')};`, 'cancel_exact_selected_mode');
    source = once(source, "const MODEL = process.env.QA_NATIVE_CANCEL_MODEL ?? 'gpt-4.1-mini';", `const MODEL = ${quote(plan.model)};`, 'cancel_pinned_model');
    source = once(source, 'path.dirname(new URL(import.meta.url).pathname)', quote(plan.epoch), 'cancel_static_owned_artifact_root');
    if (config.cancellationGuard) {
      must(path.isAbsolute(config.pinnedCancellationGuard ?? '') && path.isAbsolute(config.cancellationGuard), 'cancel_fresh_guard_paths_required');
      source = once(source, `const CAMPAIGN_GUARD = ${quote(config.pinnedCancellationGuard)};`, `const CAMPAIGN_GUARD = ${quote(config.cancellationGuard)};`, 'cancel_fresh_parent_math_guard');
    }
    source = once(source, '    forwards += 1;', `    let reservationId;
    try { reservationId = API_KEY_PRIVATE.reserve(JSON.parse(bodyText), MODEL, 'provider-boundary:native_cancel_real_https_readiness_native_followup', raw); }
    catch { return inject(400, 'durable_outbound_budget_guard'); }`, 'cancel_all_true_https_max_three');
    source = once(source, "    Object.assign(entry, providerSummary(Buffer.concat(capture).toString('utf8')));", `    API_KEY_PRIVATE.settleText(reservationId, Buffer.concat(capture).toString('utf8'), 'openai', entry.status, providerFinished && !entry.provider_aborted_at && !entry.error);
    Object.assign(entry, providerSummary(Buffer.concat(capture).toString('utf8')));`, 'cancel_real_complete_usage_or_full_hold');
    source = once(source, "      const up = https.request({ hostname: 'api.openai.com', port: 443, method: req.method, path: req.url, headers, timeout: 120_000 }, (pr) => {", "      const up = https.request({ hostname: 'api.openai.com', port: 443, method: req.method, path: req.url, headers, timeout: 120_000, ca: API_KEY_PRIVATE.tlsCa, rejectUnauthorized: true, checkServerIdentity: API_KEY_PRIVATE.checkServerIdentity }, (pr) => {", 'cancel_explicit_public_ca_and_default_hostname_verification');
    source = once(source, '      up.end(raw);', '      API_KEY_PRIVATE.writeReservedBody(reservationId, up, raw);\n      forwards += 1;', 'cancel_actual_client_request_end_seam');
    source = once(source, 'async function finalize(code) {', 'async function finalize(code) {\n  summary.provider_boundary_seal = API_KEY_PRIVATE.controller.sealGenerativeForwards();', 'cancel_seal_before_final_evidence');
    source = once(source, '  if (tap) await tap.close();\n  if (relay) await relay.close();', '  if (tap || relay) summary.provider_boundary_seal = API_KEY_PRIVATE.controller.sealGenerativeForwards();', 'cancel_retain_real_tap_and_relay_and_durably_seal');
    source = parentMain(source, CANCEL_TAIL, 'cancel');
  } else if (plan.native === 'gemini-accounting-control') {
    source = once(source, '  const R = await import(pathToFileURL(helperFile));', `  const R = await import(${quote(helperUrl)});`, 'gemini_guarded_runtime_only');
    source = once(source, "  const model = 'gemini-3.1-pro-preview';", `  const model = ${quote(plan.model)};`, 'gemini_pinned_model');
    source = once(source, "  const otlpEndpoint = process.env.QA_OTLP_ENDPOINT;", `  const otlpEndpoint = ${quote(config.otlpEndpoint ?? null)};`, 'gemini_owned_trace_endpoint');
    source = once(source, 'ctx.limits.client.sliceCostUsd === 3', 'ctx.limits.client.sliceCostUsd === 6', 'gemini_new_reference_slice_cap');
    source = once(source, 'budget.costUsd + worstCaseUsd <= 3', 'budget.costUsd + worstCaseUsd <= 6', 'gemini_new_preflight_slice_cap');
    source = once(source, "    must(ctx.upstreams.length === 1 && upstream?.host === 'generativelanguage.googleapis.com' && upstream?.kind === 'gemini_api_key', 'real_native_google_only');", "    must(API_KEY_PRIVATE.assertRawBoundary(ctx, 'gemini_api_key', 'generativelanguage.googleapis.com'), 'real_native_google_only_via_owned_provider_boundary');", 'gemini_honest_original_host_and_local_override');
    source = once(source, 'nativeProviderHost: upstream.host, upstreamKind: upstream.kind,', 'nativeProviderHost: API_KEY_PRIVATE.providerBoundaryScope().source_original_url.host, configuredUpstreamHost: upstream.host, upstreamKind: upstream.kind,', 'gemini_separate_source_and_configured_hosts');
    source = once(source, "    const baseline = await R.observabilityBaseline(ctx, 'gemini-accounting-before');", "    summary.provider_boundary = API_KEY_PRIVATE.providerBoundaryScope();\n    const baseline = await R.observabilityBaseline(ctx, 'gemini-accounting-before');", 'gemini_metadata_only_boundary_scope');
    source = once(source, "run(options(process.argv.slice(2))).catch((error) => { console.error(error.message); process.exitCode = 2; });", 'export async function main() { return run(API_KEY_PRIVATE.options); }', 'gemini_parent_main_closed_options');
  } else {
    source = once(source, "from './qa-runtime.mjs'", `from ${quote(helperUrl)}`, 'anthropic_private_runtime');
    source = once(source, "const model = 'claude-haiku-4-5-20251001';", `export async function main() {\nconst model = ${quote(plan.model)};`, 'anthropic_parent_main');
    source = once(source, "name: 'anthropic-regression'", `name: ${quote(plan.name)}`, 'anthropic_fresh_slice');
    source = once(source, '    const safe = { verdict, checks,', '    const safe = { provider_boundary: API_KEY_PRIVATE.providerBoundaryScope(), verdict, checks,', 'anthropic_metadata_only_boundary_scope');
    source += '\n}\n';
  }
  must(!source.includes('import.meta.url'), 'control_unmapped_data_url_path');
  remember(profile.source, source);
  globalThis[Symbol.for(key)] = bridge;
  try {
    const module = await import(url(source));
    must(typeof module.main === 'function', 'control_main_export_missing');
    return { module, bridgeKey: key, transformedSourceHash: hash(source), originalSource: path.join(plan.epoch, profile.source),
      ...(rawBoundary ? {} : { providerBoundaryController: control.controller }),
      sourceMap, portExpectations, provider_boundary_sourceScope: { source_scope: 'provider-boundary', implementation: rawBoundary ? 'branded_actual_https_relay' : 'branded_api_key_control_actual_https_end_controller', raw_client_boundary_reservations: false, default_url_direct_bypass_coverage: 'NOT_CLAIMED' },
      originalSourceSliceUsd: 3, selectedReferenceSliceUsd: 6,
      coverage: { profile: plan.native, forwardCap: profile.forwards, cumulativeCaseForwardCap: profile.caseForwards ?? profile.forwards, reservation: 'every_true_provider_https_forward',
        settlement: 'complete_positive_original_vendor_usage_only', unknown: 'full_durable_hold_no_timeout_release',
        originalWireAndAssertionsRetained: true, rawOnly: ['faults', 'gemini-accounting-control', 'anthropic-regression'].includes(plan.native) } };
  } catch {
    delete globalThis[Symbol.for(key)];
    throw new Error('BLOCKED:pinned_api_key_control_load_failed_no_secret_details');
  }
}
