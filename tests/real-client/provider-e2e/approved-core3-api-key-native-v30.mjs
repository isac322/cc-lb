#!/usr/bin/env bun
// Parent-only orchestration. Default/--plan reads metadata only. No vendor call on import.
// Fresh in-memory helpers select the newly authorized $6 ceiling; historical $3 files stay untouched.
import { createHash } from 'node:crypto';
import { accessSync, constants, createReadStream, existsSync, lstatSync, realpathSync, statSync } from 'node:fs';
import { readFile, writeFile, readdir, realpath } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { isDeepStrictEqual } from 'node:util';
import { validateRevisionExecutionRelease, preflightApprovedOmpClient } from './approved-core3-subscription-native-v30.mjs';

const BASE = path.dirname(fileURLToPath(import.meta.url));
const CAP30_CONTRACT = path.join(BASE, 'approved-core3-cap30-contract.json');
const CAP30_MANIFEST = path.join(BASE, 'approved-core3-cap30-manifest.json');
const LEDGER = '/tmp/cc-lb-actual-cli-e2e-20261020-ledger.json';
const hash = (bytes) => createHash('sha256').update(bytes).digest('hex');
async function hashFile(file) {
  const state = createHash('sha256');
  for await (const chunk of createReadStream(file)) state.update(chunk);
  return state.digest('hex');
}
const must = (ok, code) => { if (!ok) throw new Error(`BLOCKED:${code}`); };
const json = async (file) => JSON.parse(await readFile(file, 'utf8'));
const data = (text) => `data:text/javascript;base64,${Buffer.from(text).toString('base64')}`;
const once = (text, from, to, label) => { must(text.split(from).length === 2, `api_key_helper_source_drift:${label}`); return text.replace(from, to); };
// Current Codex selection comes only from pinned no-override client observation.
// Historical assistant fixture models remain metadata, not user-selected defaults.
// Paid release and model-specific pricing/prewarm contracts are separate gates.
const CASES = Object.freeze({
  'api-key-codex': { name: 'codex', native: 'codex', kind: 'openai_api_key', provider: 'openai', legacy_fixture_model: 'gpt-5.2', forwards: 32, ports: 18500, observer: 18503, nativeSuite: true, current_native: true },
  'api-key-omp-gpt': { name: 'omp-gpt', native: 'omp-gpt', kind: 'openai_api_key', provider: 'openai', model: 'gpt-5.2', forwards: 16, ports: 18510, observer: 18513, nativeSuite: true },
  'api-key-omp-gemini': { name: 'omp-gemini', native: 'omp-gemini', kind: 'gemini_api_key', provider: 'gemini', model: 'gemini-3.1-pro-preview', forwards: 16, ports: 18520, observer: 18523, nativeSuite: true },
  'api-key-agy': { name: 'agy', native: 'agy', kind: 'gemini_api_key', provider: 'gemini', model: 'gemini-3.1-pro-preview', forwards: 16, ports: 18530, observer: 18533, nativeSuite: true },
  'api-key-faults': { name: 'faults', native: 'faults', kind: 'mixed_api_key', provider: 'openai', model: 'gpt-4.1-mini', forwards: 16, ports: 18540, nativeSuite: false },
  'api-key-native-http-cancel': { name: 'native-http-cancel', native: 'native-http-cancel', kind: 'openai_api_key', provider: 'openai', model: 'gpt-4.1-mini', forwards: 16, ports: 18550, nativeSuite: false },
  'api-key-native-http-cancel-concurrent': { name: 'native-http-cancel-concurrent', native: 'native-http-cancel-concurrent', kind: 'openai_api_key', provider: 'openai', model: 'gpt-4.1-mini', forwards: 16, ports: 18590, nativeSuite: false },
  'api-key-gemini-accounting-control': { name: 'gemini-accounting-control', native: 'gemini-accounting-control', kind: 'gemini_api_key', provider: 'gemini', model: 'gemini-3.1-pro-preview', forwards: 1, ports: 18580, nativeSuite: false },
  'api-key-anthropic-regression': { name: 'anthropic-regression', native: 'anthropic-regression', kind: 'anthropic_api_key', provider: 'anthropic', model: 'claude-haiku-4-5-20251001', forwards: 1, ports: 18570, nativeSuite: false },
});
const MODULES = ['approved-core3-api-key-native-v30.mjs', 'approved-core3-api-key-gpt-v30.mjs', 'approved-core3-api-key-controls-v30.mjs', 'approved-core3-provider-boundary.mjs', 'approved-core3-subscription-native-v30.mjs', 'approved-core3-subscription-contract-v30.mjs', 'approved-core3-subscription-current-native.mjs', 'approved-core3-subscription-budget-v30.mjs', 'approved-core3-oracle.mjs'];
const integer = (n) => Number.isSafeInteger(n) && n >= 0;
function checkNonsecretConfig(config) {
  const allowed = new Set(['schema', 'case', 'epoch', 'expectedSha', 'sliceCostUsd', 'model', 'models', 'nativeModelEvidence', 'controlModel', 'geminiPrimary', 'agyModelLabel', 'agyAuxModels', 'openaiModel', 'geminiModel', 'pricingEligibility', 'campaignGuard', 'cancellationGuard', 'pinnedCancellationGuard', 'otlpEndpoint', 'capture', 'authorization']);
  must(config && typeof config === 'object' && !Array.isArray(config) && Object.keys(config).every((key) => allowed.has(key)), 'nonsecret_api_key_config_fields_required');
  must(config.schema === 'approved-core3-api-key-config-v1', 'explicit_api_key_config_schema_required');
  const walk = (value) => {
    if (typeof value === 'string') must(!/^(?:sk[-_]|AIza|ya29\.|eyJ[\w-]*\.|Bearer\s)/i.test(value), 'credential_value_forbidden_in_config');
    else if (value && typeof value === 'object') for (const nested of Object.values(value)) walk(nested);
  };
  walk(config);
}

// Approved epoch port offsets for v30 API-key epochs.
export const APPROVED_PORT_OFFSETS = Object.freeze([500, 600, 700, 800, 900, 1000, 1100]);
// Pure resource plan for one API case at one approved offset, derived only from
// CASES and the original port formulas. It plans ports and labels; it grants no
// run permission and never waives a model, pricing, finance or source gate.
export function apiKeyCaseResources(caseName, offset) {
  const spec = Object.hasOwn(CASES, caseName) ? CASES[caseName] : null;
  must(spec, 'known_actual_api_key_profile_required');
  must(APPROVED_PORT_OFFSETS.includes(offset), 'new_epoch_port_offset_required');
  const needsOwnedBoundary = spec.nativeSuite || ['gemini-accounting-control', 'anthropic-regression'].includes(spec.native);
  const providerBoundaryPort = needsOwnedBoundary ? 20500 + offset + Object.keys(CASES).indexOf(caseName) * 10 : null;
  const auxiliaryPorts = spec.observer !== undefined ? [spec.observer + offset]
    : spec.native === 'faults' ? [18543 + offset, 18544 + offset]
    : spec.native === 'native-http-cancel' ? [18553 + offset, 18554 + offset]
    : spec.native === 'native-http-cancel-concurrent' ? [18593 + offset, 18594 + offset] : [];
  if (providerBoundaryPort !== null) auxiliaryPorts.push(providerBoundaryPort);
  return {
    case: caseName, name: spec.name, native: spec.native, port_offset: offset,
    ports: [spec.ports + offset, spec.ports + offset + 1, spec.ports + offset + 2],
    observer_port: spec.observer === undefined ? null : spec.observer + offset,
    auxiliary_ports: auxiliaryPorts, provider_boundary_port: providerBoundaryPort,
    provider_boundary_mode: needsOwnedBoundary ? 'owned_original_target_relay' : 'existing_true_https_relay_hooks',
    collector: { port: 18563 + offset, control_port: 18564 + offset },
    required_original_scenarios: spec.native === 'codex' ? ['http', 'ws', 'ws_reconnect', 'exact_session_resume'] : spec.native === 'omp-gpt' ? ['HTTP stateless coding', 'HTTP stateful previous_response_id', 'exact_session_continue'] : spec.native === 'omp-gemini' ? ['coding_tools_thoughtSignature', 'exact_session_continue', 'lower_cost_transport_control'] : spec.native === 'agy' ? ['native_coding_tools', 'thoughtSignature_continuity', 'exact_session_continue'] : ['all original raw/control assertions'],
  };
}
export const API_KEY_CASE_NAMES = Object.freeze(Object.keys(CASES));

export async function apiKeyNativePlan(config) {
  checkNonsecretConfig(config);
  const spec = CASES[config.case]; must(spec, 'known_actual_api_key_profile_required');
  if (spec.current_native) {
    must(!Object.hasOwn(config, 'model') && process.env.CODEX_QA_MODEL === undefined, 'current_native_model_override_forbidden');
    must(config.nativeModelEvidence && typeof config.nativeModelEvidence.path === 'string' && /^[0-9a-f]{64}$/.test(config.nativeModelEvidence.sha256 ?? ''), 'current_native_evidence_required');
  }
  const epoch = path.resolve(config.epoch ?? '');
  const canonicalBase = realpathSync(BASE);
  const epochParent = path.dirname(epoch);
  const epochName = path.basename(epoch);
  must((epochParent === BASE || epochParent === canonicalBase) && /^fixed-[0-9a-f]{12}-core3(?:-[a-z0-9-]{1,16})?$/.test(epochName), 'owned_new_epoch_required');
  const epochStat = lstatSync(epoch);
  must(epochStat.isDirectory() && !epochStat.isSymbolicLink(), 'owned_new_epoch_required');
  const canonicalEpoch = realpathSync(epoch);
  must(path.dirname(canonicalEpoch) === canonicalBase && path.basename(canonicalEpoch) === epochName, 'owned_new_epoch_required');
  const manifest = await json(path.join(epoch, 'fixed-epoch.json'));
  must(/^[0-9a-f]{64}$/.test(config.expectedSha ?? '') && manifest.binary?.expected_sha256 === config.expectedSha, 'new_binary_manifest_pin_required');
  must(APPROVED_PORT_OFFSETS.includes(manifest.port_offset), 'new_epoch_port_offset_required');
  must(config.sliceCostUsd === 6, 'explicit_new_six_usd_cap_required');
  let currentNative = null;
  if (spec.current_native) {
    const helperFile = path.join(BASE, 'approved-core3-subscription-current-native.mjs');
    must(config.authorization?.runner_sha256?.['approved-core3-subscription-current-native.mjs'] === hash(await readFile(helperFile)), 'reviewed_current_native_helper_pin_required');
    const helper = await import(pathToFileURL(helperFile));
    currentNative = await helper.loadCurrentNativeModelEvidence(config.nativeModelEvidence);
    must(currentNative.no_model_override === true && currentNative.current_user_session_default_proven === false && currentNative.provider_qa_verified === false, 'scoped_current_native_observation_required');
  }
  const model = currentNative?.model ?? config.model ?? (spec.native === 'faults' ? config.openaiModel : undefined) ?? spec.model;
  must(typeof model === 'string' && /^[A-Za-z0-9._-]+$/.test(model), 'exact_observed_or_legacy_model_required');
  if (['omp-gpt', 'gemini-accounting-control', 'anthropic-regression'].includes(spec.native)) must(model === spec.model, 'legacy_fixed_control_model_preserved');
  const models = config.models ?? (currentNative ? [model] : undefined);
  must(Array.isArray(models) && models.length > 0 && new Set(models).size === models.length && models.includes(model) && models.every((m) => typeof m === 'string' && /^[A-Za-z0-9._-]+$/.test(m)), 'complete_native_and_auxiliary_model_list_required');
  if (currentNative) must(models.length === 1 && models[0] === currentNative.model, 'exact_observed_current_native_catalog_binding_required');
  if (spec.native === 'omp-gemini') must(models.includes(config.controlModel ?? 'gemini-3.5-flash-lite'), 'original_gemini_low_cost_control_model_required');
  if (spec.native === 'agy') must(Array.isArray(config.agyAuxModels) && config.agyAuxModels.length > 0 && config.agyAuxModels.every((m) => models.includes(m)), 'all_original_agy_auxiliary_models_required');
  if (spec.native === 'faults') must(models.includes(model) && models.includes(config.geminiModel ?? 'gemini-2.5-flash-lite'), 'both_original_faults_provider_models_required');
  must(config.pricingEligibility && typeof config.pricingEligibility === 'object' && models.every((m) => config.pricingEligibility[m]), 'source_validated_protocol_and_tier_eligibility_required');
  const resources = apiKeyCaseResources(config.case, manifest.port_offset);
  return { case: config.case, ...spec, model, models: [...models], epoch, root: path.join(epoch, spec.name), ports: resources.ports, observer_port: resources.observer_port, portOffset: resources.port_offset,
    expectedSha: config.expectedSha, caps: { slice_usd: 6, campaign_usd: 30, case_forwards: spec.forwards, original_source_slice_usd: 3 },
    native_model_evidence: currentNative,
    native_model_selection: currentNative ? 'OBSERVED_CLIENT_SELECTION_NO_ASSISTANT_OVERRIDE' : 'LEGACY_FIXED_CONTROL_SOURCE',
    auxiliary_ports: resources.auxiliary_ports,
    provider_boundary_port: resources.provider_boundary_port, provider_boundary_mode: resources.provider_boundary_mode,
    baseUrl: spec.provider === 'openai' ? 'https://api.openai.com' : spec.provider === 'gemini' ? 'https://generativelanguage.googleapis.com' : 'https://api.anthropic.com',
    ledger: LEDGER, state: 'NOT_RUN', all_original_scenarios_retained: true,
    required_original_scenarios: resources.required_original_scenarios,
    hard_bound: 'source-validated catalog max input/output and eligible tier prices; actual native body before every guarded forward, no payload rewrite',
    parent_gate: 'No execution until reviewed source hashes, new-binary gates, exact resources and fresh whole-root financial snapshot are approved. Single paid campaign process at a time.',
  };
}

async function pinnedInputs(plan, config) {
  must(!Object.hasOwn(config, 'artifact'), 'api_key_artifact_config_override_forbidden');
  const manifest = await json(path.join(plan.epoch, 'fixed-epoch.json'));
  const needed = ['qa-runtime.mjs', 'contract.json'];
  if (plan.nativeSuite) needed.push(`${plan.native}/run.mjs`);
  if (plan.native === 'omp-gpt') needed.push('omp-gpt/cache-proof.mjs');
  if (plan.native === 'codex') needed.push('codex/hidden-check.mjs');
  if (plan.native === 'faults') needed.push('faults/run.mjs', 'faults/relay.mjs');
  if (plan.native.startsWith('native-http-cancel')) needed.push('approved-core3-native-http-cancel.mjs');
  if (plan.native === 'gemini-accounting-control') needed.push('approved-core3-controls.mjs');
  if (plan.native === 'anthropic-regression') needed.push('approved-core3-anthropic-regression.mjs');
  const sources = {};
  for (const rel of needed) {
    const bytes = await readFile(path.join(plan.epoch, rel));
    must(hash(bytes) === manifest.files?.[rel]?.sha256, `pinned_source_changed:${rel}`);
    sources[rel] = bytes.toString('utf8');
  }
  // The cap30 manifest is trusted only through the parent's external raw SHA.
  const contractBytes = await readFile(CAP30_CONTRACT);
  const revisionManifestBytes = await readFile(CAP30_MANIFEST);
  must(/^[0-9a-f]{64}$/.test(config.authorization?.revision_manifest_sha256 ?? '') && hash(revisionManifestBytes) === config.authorization.revision_manifest_sha256, 'cap30_trusted_external_manifest_sha_required');
  const revisionManifest = JSON.parse(revisionManifestBytes);
  must(!Object.hasOwn(revisionManifest.source_sha256 ?? {}, 'approved-core3-cap30-manifest.json') && revisionManifest.source_sha256?.['approved-core3-cap30-contract.json'] === hash(contractBytes), 'cap30_contract_manifest_hash_mismatch');
  const contract = JSON.parse(contractBytes);
  validateRevisionExecutionRelease(contract, revisionManifest);
  must(typeof contract.artifact?.path === 'string' && path.isAbsolute(contract.artifact.path) && /^[0-9a-f]{64}$/.test(contract.artifact.expected_sha256 ?? '') && contract.artifact.expected_sha256 === plan.expectedSha, 'pinned_paid_artifact_required');
  const artifact = Object.freeze({ path: contract.artifact.path, expected_sha256: contract.artifact.expected_sha256 });
  must(manifest.binary?.path === artifact.path && manifest.binary.expected_sha256 === artifact.expected_sha256, 'pinned_paid_artifact_manifest_pair_mismatch');
  must(contract.paid_bounds?.usd_ceiling_per_slice === 3 && contract.paid_bounds.usd_ceiling_campaign === 30 && contract.paid_bounds.generation_attempts_per_slice === 16 && contract.paid_attempt_overrides?.codex?.max_generation_attempts === 32, 'historical_original_caps_or_binary_changed');
  must(await hashFile(artifact.path) === artifact.expected_sha256, 'pinned_paid_artifact_sha_mismatch');
  const auth = config.authorization ?? {};
  for (const name of MODULES) must(auth.runner_sha256?.[name] === hash(await readFile(path.join(BASE, name))), `reviewed_adapter_pin_required:${name}`);
  must(auth.parent_new_binary_green === true && auth.parent_safety_review_green === true && auth.serial_campaign_execution === true && auth.allow_native_api_key_generation === true && auth.allow_owned_provider_boundary === true, 'explicit_parent_binary_safety_serial_paid_boundary_authorization_required');
  if (plan.current_native) {
    must(process.env.CODEX_QA_MODEL === undefined && !Object.hasOwn(config, 'model'), 'current_native_model_override_forbidden');
    let actualCli = null;
    for (const directory of (process.env.PATH ?? '').split(path.delimiter)) {
      if (!directory) continue;
      const candidate = path.join(directory, 'codex');
      try { accessSync(candidate, constants.X_OK); if (statSync(candidate).isFile()) { actualCli = realpathSync(candidate); break; } } catch {}
    }
    const observedCli = plan.native_model_evidence.cli;
    must(actualCli !== null && actualCli === realpathSync(observedCli.path) && await hashFile(actualCli) === observedCli.sha256, 'current_native_observed_client_identity_stale');
  }
  must(auth.notified_root === plan.root && JSON.stringify(auth.notified_ports) === JSON.stringify(plan.ports) && (plan.observer_port === null || auth.notified_native_port === plan.observer_port), 'exact_owned_resources_not_notified');
  must(JSON.stringify(auth.notified_auxiliary_ports) === JSON.stringify(plan.auxiliary_ports), 'exact_native_or_relay_auxiliary_ports_not_notified');
  must(auth.notified_provider_boundary_port === plan.provider_boundary_port, 'exact_provider_boundary_port_not_notified');
  must(!existsSync(path.join(plan.root, 'runtime/storage.sqlite')) && !existsSync(path.join(plan.root, 'prepare.json')) && !existsSync(path.join(plan.root, 'outbound-budget.json')), 'fresh_api_key_runtime_only_existing_evidence_preserved');
  if (plan.native === 'codex' || config.otlpEndpoint !== undefined) must(config.otlpEndpoint === `http://127.0.0.1:${18563 + plan.portOffset}` && config.capture === path.join(plan.epoch, 'approved-core3-spans.json') && auth.parent_collector_ledgered === true && existsSync(config.capture), 'owned_current_collector_required');
  return { manifest, sources, contract, artifact };
}

async function privateRuntime(plan, pinned, boundaryResolver) {
  const boundaryExports = plan.provider_boundary_port === null ? '' : '\nlet approvedBoundaryResolver = null;\nexport function installProviderBoundaryResolver(value) { if (approvedBoundaryResolver || typeof value !== "function") throw Error("BLOCKED:one_real_boundary_resolver_required"); approvedBoundaryResolver = value; }';
  let source = once(pinned.sources['qa-runtime.mjs'], 'export const ROOT = path.dirname(fileURLToPath(import.meta.url));', `export const ROOT = ${JSON.stringify(plan.epoch)};${boundaryExports}`, 'owned_static_root');
  source = once(source, '  sliceCostUsd: 3,', '  sliceCostUsd: 6,', 'new_selected_six_only');
  source = once(source, '  campaignCostUsd: 15, // contract C655: 4 CLI slices + 1 fault slice, each $3 / 16 generations', '  campaignCostUsd: 30, // contract C655: 4 CLI slices + 1 fault slice, each $3 / 16 generations', 'new_campaign_cap_v30');
  if (plan.provider_boundary_port !== null) source = once(source, '    const baseUrl = u.baseUrl ?? cred.baseUrl;', '    if (!approvedBoundaryResolver) throw Error("BLOCKED:provider_boundary_resolver_missing");\n    const baseUrl = await approvedBoundaryResolver(ctx, u, cred, u.baseUrl ?? cred.baseUrl);', 'actual_upstream_original_url_boundary');
  const runtime = await import(data(source));
  must(pinned.artifact.expected_sha256 === plan.expectedSha && await runtime.sha256File(pinned.artifact.path) === pinned.artifact.expected_sha256, 'current_binary_changed_before_runtime');
  if (plan.provider_boundary_port !== null) runtime.installProviderBoundaryResolver(boundaryResolver);
  return { runtime, sourceHash: hash(source) };
}
function nativeEnvironment(plan, config) {
  const env = { QA_RESUME: undefined };
  if (config.otlpEndpoint) env.QA_OTLP_ENDPOINT = config.otlpEndpoint;
  if (plan.native === 'codex') Object.assign(env, { CODEX_QA_ATTEMPT_CAP: '32', CODEX_QA_SCENARIOS: 'http,ws,ws_reconnect' });
  if (plan.native === 'omp-gemini') env.QA_GEMINI_PRIMARY_MODEL = config.geminiPrimary ?? plan.model;
  if (plan.native === 'agy') Object.assign(env, { AGY_MODEL_LABEL: config.agyModelLabel ?? 'gemini-3.1-pro-low', AGY_WIRE_MODEL: plan.model, AGY_PRICED_MODELS: config.agyAuxModels.join(',') });
  if (plan.native === 'faults') Object.assign(env, { QA_FAULT_SLICE: 'faults', QA_FAULT_PORT_OFFSET: String(plan.portOffset), QA_FAULT_REQUEST_CAP: '24', QA_FAULT_OPENAI_MODEL: plan.model, QA_FAULT_GEMINI_MODEL: config.geminiModel ?? 'gemini-2.5-flash-lite', QA_FAULT_ONLY: undefined });
  if (plan.native.startsWith('native-http-cancel')) Object.assign(env, { QA_NATIVE_CANCEL_SLICE: plan.name, QA_NATIVE_CANCEL_MODEL: plan.model, QA_NATIVE_CANCEL_LIMIT_KIND: plan.native.endsWith('concurrent') ? 'concurrent' : 'cost_usd', QA_NATIVE_CANCEL_RESUME: undefined });
  return env;
}
async function readEvents(ctx, runtime) {
  const events = new Map(); let cursor = 0;
  for (let page = 0; page < 50; page += 1) {
    const query = new URLSearchParams({ since_cursor: String(cursor), limit: '500', principal_id: ctx.principalId });
    const response = await runtime.admin(ctx, `/admin/v1/events/delta?${query}`);
    must(response.status === 200 && Array.isArray(response.body?.events), 'bounded_committed_event_read_failed');
    for (const event of response.body.events) {
      must(typeof event.request_id === 'string' && event.request_id.length > 0, 'committed_request_identity_missing');
      const prior = events.get(event.request_id);
      must(!prior || JSON.stringify(prior) === JSON.stringify(event), 'conflicting_duplicate_committed_request_rows');
      events.set(event.request_id, event);
    }
    cursor = response.body.next_cursor;
    if (response.body.exhausted) return [...events.values()];
  }
  throw new Error('BLOCKED:committed_event_page_bound_reached');
}

// Release bookkeeping needs complete request identities, owner/key scope, counts and
// raw usage agreement. Guard charges are conservative CEIL/tier maxima; cc-lb costs
// use per-component FLOOR. They are not required to be numerically equal or invoices.
export function reconcileGuardedEvents(outbound, events, ctx) {
  try {
    must(Array.isArray(events) && outbound?.attempts > 0 && Array.isArray(outbound.records) && outbound.records.length === outbound.attempts && outbound.unresolved === false, 'nonempty_complete_outbound_evidence_required');
    const rows = events.filter((event) => event.upstream_id && event.error_code !== 'limit_rejected');
    must(rows.length === outbound.attempts, 'guard_and_committed_provider_request_counts_differ');
    const allowed = new Set(ctx.upstreams.map((upstream) => upstream.id));
    const ids = new Set();
    const expected = new Map(); const observed = new Map();
    const empty = () => ({ requests: 0n, input: 0n, output: 0n, cached: 0n, written: 0n, reasoning: 0n });
    const add = (groups, model, values) => {
      const group = groups.get(model) ?? empty(); group.requests += 1n;
      for (const key of ['input', 'output', 'cached', 'written', 'reasoning']) { must(integer(values[key]), 'unsafe_usage_counter'); group[key] += BigInt(values[key]); }
      groups.set(model, group);
    };
    let storedMicros = 0n;
    for (const event of rows) {
      must(typeof event.request_id === 'string' && !ids.has(event.request_id), 'duplicate_committed_request_identity');
      ids.add(event.request_id);
      must(event.principal_id === ctx.principalId && (event.key_id ?? event.api_key_id ?? event.client_key_id) === ctx.keyId && allowed.has(event.upstream_id), 'committed_identity_or_owner_scope_mismatch');
      must(integer(event.input_tokens) && integer(event.output_tokens) && integer(event.cost_usd_micros), 'committed_usage_or_cost_missing');
      must((event.status >= 200 && event.status < 300) || (event.status === 400 && event.error_code === 'upstream_4xx' && event.input_tokens === 0 && event.output_tokens === 0 && event.cost_usd_micros === 0), 'unsettled_provider_rejection_or_cancellation');
      const written = event.cache_creation_input_tokens ?? ((event.cache_creation_input_tokens_5m ?? 0) + (event.cache_creation_input_tokens_1h ?? 0));
      add(observed, event.model, { input: event.input_tokens, output: event.output_tokens, cached: event.cache_read_input_tokens ?? 0, written, reasoning: event.thinking_tokens ?? 0 });
      storedMicros += BigInt(event.cost_usd_micros);
    }
    for (const record of outbound.records) {
      must(record.status === 'SETTLED', 'outbound_record_not_settled');
      if (!record.usage) {
        must(record.provider_status === 400 && record.charged_micros === 0 && /^[0-9a-f]{64}$/.test(record.non_generation_proof_sha256 ?? ''), 'missing_raw_usage_without_validated_negative_proof');
        add(expected, record.model, { input: 0, output: 0, cached: 0, written: 0, reasoning: 0 });
        continue;
      }
      const u = record.usage;
      must([u.input_tokens, u.output_tokens, u.cache_read_input_tokens, u.cache_creation_input_tokens, u.reasoning_tokens].every(integer), 'raw_guarded_usage_missing');
      const input = u.uncached_input_tokens ?? (u.input_tokens - u.cache_read_input_tokens - u.cache_creation_input_tokens);
      add(expected, record.model, { input, output: u.output_tokens, cached: u.cache_read_input_tokens, written: u.cache_creation_input_tokens, reasoning: u.reasoning_tokens });
    }
    must(expected.size === observed.size, 'raw_and_committed_model_sets_differ');
    for (const [model, group] of expected) {
      const actual = observed.get(model);
      must(actual && Object.keys(group).every((key) => group[key] === actual[key]), 'raw_and_committed_usage_or_model_counts_differ');
    }
    must(integer(outbound.spent_micros) && storedMicros <= BigInt(outbound.spent_micros) && storedMicros <= BigInt(Number.MAX_SAFE_INTEGER), 'stored_cost_exceeds_conservative_guard_charge');
    return { matched: true, requests: rows.length, committed_catalog_micros: Number(storedMicros), guard_conservative_charge_micros: outbound.spent_micros, exact_raw_usage_aggregates: true, per_turn_identity: 'Codex WS checked separately with trusted spans; aggregate reconciliation does not invent a per-turn join', cost_comparison: 'stored component-floor cost bounded by guarded ceil/eligible-tier charge; exact pricing and invoice proof are separate' };
  } catch (error) {
    return { matched: false, reason: typeof error.message === 'string' && error.message.startsWith('BLOCKED:') ? error.message : 'BLOCKED:guarded_event_reconciliation_incomplete', committed_catalog_micros: null };
  }
}

export function nativeCommandCompletionEvidence(plan, summary, mainReturned) {
  const result = { native_command_finished: false, main_returned: mainReturned === true, receipt_scope: 'Original native child exit/close callbacks, not socket-idle polling; retained SUT is excluded', receipts: [] };
  if (!mainReturned || !summary) return result;
  const ended = (receipt) => receipt && (Number.isInteger(receipt.code) || typeof receipt.signal === 'string' && receipt.signal.length > 0);
  if (plan.native === 'codex') result.receipts = (summary.scenarios ?? []).flatMap((scenario) => (scenario.turns ?? []).map((turn) => ({ code: turn.run?.exit_code, signal: turn.run?.signal, source: `${scenario.scenario}:turn${turn.turn}:run.close` })));
  else if (['omp-gpt', 'omp-gemini'].includes(plan.native)) result.receipts = (summary.runs ?? []).filter((run) => run.kind === 'cli').map((run) => ({ code: run.exitCode, source: `${run.label}:original_child_close` }));
  else if (plan.native === 'agy') {
    result.receipts = (summary.turns ?? []).map((turn) => ({ code: turn.exit?.code, signal: turn.exit?.signal, source: `turn${turn.turn}:original_child_exit`, survivors: turn.survivors }));
    if (result.receipts.some((receipt) => !Array.isArray(receipt.survivors) || receipt.survivors.length !== 0)) return result;
  } else if (plan.native.startsWith('native-http-cancel')) result.receipts = [{ code: summary.native_cancel_evidence?.exit?.code, signal: summary.native_cancel_evidence?.exit?.signal, source: 'original_native_cancel_child_exit' }];
  else {
    result.native_command_finished = plan.native === 'faults' ? summary.kind === 'server_contract_checks' && summary.cli_coverage === false
      : plan.native === 'gemini-accounting-control' ? summary.source === 'RAW' && summary.CLIcoverage === false
      : plan.native === 'anthropic-regression' ? summary.cliCoverage === false : false;
    result.receipt_scope = 'Named raw-only control returned after its awaited requests; no native CLI is launched by this source';
    return result;
  }
  result.native_command_finished = result.receipts.length > 0 && result.receipts.every(ended);
  return result;
}

// OMP-GPT pricing eligibility must be the contract-sourced gpt-5.2 entry (the
// campaign loader validated its full shape before the epoch contract was pinned).
// It is this project's user-confirmed Standard account scope, never a universal
// API default. Other cases keep their own sourced config unchanged.
export function contractSourcedPricingEligibility(plan, config, contract) {
  if (plan.case !== 'api-key-omp-gpt') return null;
  const sourced = contract?.api_key_pricing_eligibility;
  must(sourced?.schema === 'approved-core3-api-key-pricing-eligibility-v1' && sourced.account_scope?.universal_default_claimed === false && sourced.account_scope.key_value_read === false && sourced.account_scope.scope === 'this_project_only', 'gpt52_contract_pricing_eligibility_required');
  must(isDeepStrictEqual(plan.models, ['gpt-5.2']) && isDeepStrictEqual(Object.keys(config.pricingEligibility ?? {}), ['gpt-5.2']) && isDeepStrictEqual(config.pricingEligibility['gpt-5.2'], sourced.models?.['gpt-5.2']), 'gpt52_pricing_eligibility_not_contract_sourced');
  return { account_scope: { ...sourced.account_scope }, model: 'gpt-5.2', eligible_tiers: [...sourced.models['gpt-5.2'].eligible_tiers], omitted_tier: sourced.models['gpt-5.2'].omitted_tier, citation_count: sourced.models['gpt-5.2'].citations.length };
}

export async function runGuardedApiKeyNative(config) {
  must(typeof Bun !== 'undefined', 'bun_required_for_original_native_runners');
  const plan = await apiKeyNativePlan(config);
  const pinned = await pinnedInputs(plan, config);
  const sourcedEligibility = contractSourcedPricingEligibility(plan, config, pinned.contract);
  // Fail before the reservation write, credentials or SUT. OMP siblings share
  // the same artifact-backed identity; non-OMP cases keep their existing gates.
  const approvedClient = ['omp-gpt', 'omp-gemini', 'native-http-cancel', 'native-http-cancel-concurrent'].includes(plan.native)
    ? await preflightApprovedOmpClient(plan, pinned.contract) : null;
  const shared = await import(pathToFileURL(path.join(BASE, 'approved-core3-subscription-native-v30.mjs')));
  const { createOutboundBudgetGuard } = await import(pathToFileURL(path.join(BASE, 'approved-core3-subscription-budget-v30.mjs')));
  const boundaryModule = await import(pathToFileURL(path.join(BASE, 'approved-core3-provider-boundary.mjs')));
  must(typeof shared.buildGuardedNativeModule === 'function' && typeof shared.reconcileReferenceReservation === 'function', 'frozen_shared_native_and_reference_closure_exports_required');
  const rootGuard = await shared.verifyRootCampaignGuard(config);
  const reservationFile = path.join(plan.epoch, `approved-core3-api-key-${plan.name}-reservation.json`);
  const record = { case: plan.case, slice: plan.name, credential_kind: plan.kind, at: new Date().toISOString(), state: 'ACTIVE', slice_cap_micros: 6000000, charge_usd: 6, root_guard: rootGuard, original_source_slice_usd: 3, reason: 'Fresh authorized catalog-list maximum exposure hold; not an invoice or expected spend.' };
  await writeFile(reservationFile, `${JSON.stringify(record, null, 2)}\n`, { mode: 0o600, flag: 'wx' });
  const previousEnv = {}; const env = nativeEnvironment(plan, config);
  for (const [key, value] of Object.entries(env)) { previousEnv[key] = process.env[key]; if (value === undefined) delete process.env[key]; else process.env[key] = value; }
  let native; let ctx; let guard; let runtime; let providerBoundary = null; let failure = null; let runtimeSourceHash = null; let mainReturned = false;
  const privateValues = [];
  const evidence = { case: plan.case, kind: plan.kind, binary_sha256: plan.expectedSha, state: 'NOT_RUN', caps: plan.caps, source_original_usd_cap_preserved: 3, root_guard: rootGuard, real_paid_funding_or_invoice_state: 'UNVERIFIED', no_native_payload_rewrite: true };
  if (sourcedEligibility) evidence.pricing_eligibility_provenance = { source: 'cap30 contract api_key_pricing_eligibility', ...sourcedEligibility, universal_default_claimed: false };
  if (approvedClient) evidence.approved_client_identity = { ...approvedClient.identity, observed_version: approvedClient.version, preflight: approvedClient.probe };
  evidence.native_model_expectation = { selection: plan.native_model_selection, expected_catalog_slug: plan.model, observation: plan.native_model_evidence, client_override_policy: plan.current_native ? 'FORBIDDEN' : 'UNCHANGED_LEGACY_CONTROL', wire_model_or_tool_progress_verified: false };
  try {
    const boundaryResolver = async (seedCtx, upstream, credential, originalTargetUrl) => {
      must(plan.provider_boundary_port !== null && !providerBoundary && credential.kind === plan.kind && upstream.provider === plan.provider, 'exact_single_original_api_key_target_required');
      if (typeof credential.apiKey === 'string' && credential.apiKey.length && !privateValues.includes(credential.apiKey)) privateValues.push(credential.apiKey);
      must(typeof originalTargetUrl === 'string', 'source_original_target_url_missing_no_guessed_default');
      const original = new URL(originalTargetUrl);
      const expectedHost = { openai: 'api.openai.com', gemini: 'generativelanguage.googleapis.com', anthropic: 'api.anthropic.com' }[plan.provider];
      must(original.protocol === 'https:' && original.hostname === expectedHost && (!original.port || original.port === '443'), 'approved_real_provider_origin_required');
      const protocol = { openai: 'openai-responses', gemini: 'google-generate-content', anthropic: 'anthropic-messages' }[plan.provider];
      providerBoundary = await boundaryModule.createProviderBoundaryRelay({ ctx: seedCtx, port: plan.provider_boundary_port, targetBaseUrl: originalTargetUrl, protocol, models: plan.models, recordResource: runtime.recordResource, getGuard: () => guard });
      must(boundaryModule.isProviderBoundaryRelay(providerBoundary), 'real_provider_boundary_brand_required');
      return providerBoundary.upstreamBaseUrl;
    };
    ({ runtime, sourceHash: runtimeSourceHash } = await privateRuntime(plan, pinned, boundaryResolver));
    const prepareGuard = async (actualCtx) => {
      must(!guard && actualCtx.name === plan.name && actualCtx.binaryPath === pinned.artifact.path && actualCtx.binarySha256 === pinned.artifact.expected_sha256 && actualCtx.expectedBinarySha256 === pinned.artifact.expected_sha256, 'one_guard_for_exact_owned_runtime_required');
      must(Array.isArray(actualCtx.blocked) && actualCtx.blocked.length === 0 && actualCtx.costCatalog?.modelPriced === true && actualCtx.costCatalog.installedByServer === true, 'runtime_readiness_and_installed_price_catalog_required');
      ctx = actualCtx;
      const catalog = JSON.parse(await readFile(ctx.priceCatalogPath, 'utf8'));
      const prices = { ...(ctx.costCatalog?.prices ?? {}) };
      for (const model of plan.models) {
        const protocol = config.pricingEligibility[model]?.protocol;
        const provider = protocol === 'openai-responses' ? 'openai' : protocol === 'google-generate-content' ? 'gemini' : protocol === 'anthropic-messages' ? 'anthropic' : null;
        must(provider, 'source_validated_protocol_required');
        const key = provider === 'gemini' ? `gemini/${model}` : model;
        must(catalog[key]?.litellm_provider === provider, 'exact_native_catalog_provider_required');
        prices[model] = { ...(prices[model] ?? {}), key };
      }
      ctx.costCatalog = { ...ctx.costCatalog, prices };
      guard = createOutboundBudgetGuard({ ctx, plan, catalog, models: plan.models, campaignBudgetBefore: { conservativePriorMicros: rootGuard.conservativePriorMicros, outstandingMaxOutboundMicros: rootGuard.outstandingMaxOutboundMicros }, pricingEligibility: config.pricingEligibility });
      const checked = guard.preflight(); must(checked.ok, `catalog_hard_budget_setup:${checked.reason}`);
      if (plan.provider_boundary_port !== null) must(boundaryModule.isProviderBoundaryRelay(providerBoundary) && providerBoundary.guardIdentity() === guard, 'actual_provider_boundary_and_guard_identity_required');
      return guard;
    };
    if (plan.nativeSuite) {
      ctx = await runtime.prepareSlice({ name: plan.name, ports: plan.ports, provider: plan.provider, model: plan.model, pricedModels: plan.models, ...(config.otlpEndpoint ? { otlpEndpoint: config.otlpEndpoint } : {}), ...(plan.native === 'codex' ? { limitOverrides: { maxGenerationAttempts: 32 } } : {}), resume: false, artifact: pinned.artifact });
      await prepareGuard(ctx);
      must(ctx.upstreams.length === 1 && ctx.upstreams[0].kind === plan.kind, 'actual_single_api_key_upstream_required');
      for (const value of [ctx.clientKey, ctx._adminToken, ctx._masterKey]) if (typeof value === 'string' && value) privateValues.push(value);
      if (plan.native === 'omp-gpt') {
        const adapter = await import(pathToFileURL(path.join(BASE, 'approved-core3-api-key-gpt-v30.mjs')));
        native = await adapter.buildGuardedApiKeyGptModule(plan, config, pinned, { runtime, ctx, providerBoundary, approvedClient }, privateValues, guard);
      } else native = await shared.buildGuardedNativeModule(plan, config, pinned, { runtime, ctx, providerBoundary, approvedClient }, privateValues, guard);
    } else {
      const adapter = await import(pathToFileURL(path.join(BASE, 'approved-core3-api-key-controls-v30.mjs')));
      native = await adapter.buildGuardedApiKeyControlModule(plan, { ...config, portOffset: plan.portOffset }, pinned, { runtime, prepareGuard, getProviderBoundary: () => providerBoundary, approvedClient }, privateValues);
    }
    try { await native.module.main(); mainReturned = true; } catch { failure = 'native_or_control_execution_failed_no_secret_details'; }
    const providerBoundaries = providerBoundary ? [providerBoundary] : native.providerBoundaryController ? [native.providerBoundaryController] : [];
    for (const boundary of providerBoundaries) boundary.sealGenerativeForwards();
    if (!ctx && native?.bridgeKey) ctx = globalThis[Symbol.for(native.bridgeKey)]?.ctx;
    must(ctx && guard, 'actual_transport_guard_not_initialized');
    const summary = await json(path.join(ctx.root, 'summary.json'));
    const commandCompletion = nativeCommandCompletionEvidence(plan, summary, mainReturned);
    evidence.native_command_completion = commandCompletion;
    const events = await readEvents(ctx, runtime);
    const outbound = guard.snapshot();
    const reconciliation = reconcileGuardedEvents(outbound, events, ctx);
    const committedMicros = reconciliation.committed_catalog_micros;
    const held = outbound.total_held_micros;
    const costSafe = integer(held) && outbound.bound_breach === false && outbound.unsafe_integer_cost_evidence === false && held <= 6000000;
    const settled = costSafe && reconciliation.matched && integer(committedMicros) && committedMicros <= held;
    const originalVerdict = summary.verdict ?? ((summary.verdicts && Object.values(summary.verdicts).every((v) => v === 'PASS')) || (summary.scenarios && Object.values(summary.scenarios).every((v) => v?.verdict === 'PASS')) ? 'PASS' : 'INCOMPLETE');
    let trustedWs = { verdict: 'NOT_APPLICABLE', reason: 'Not the native Codex three-scenario WS suite.' };
    if (plan.native === 'codex') {
      const oracle = await import(pathToFileURL(path.join(BASE, 'approved-core3-oracle.mjs')));
      const capture = existsSync(config.capture) ? await json(config.capture) : null;
      trustedWs = oracle.wsProof(summary, events, capture);
    }
    const admissionBlocked = providerBoundaries.some((boundary) => (boundary.metadata().counters?.blocked ?? 0) > 0) || outbound.blocked === true;
    evidence.state = admissionBlocked ? 'BLOCKED' : originalVerdict === 'PASS' && settled && !failure && ['PASS', 'NOT_APPLICABLE'].includes(trustedWs.verdict) ? 'PASS' : originalVerdict === 'FAIL' || trustedWs.verdict === 'FAIL' || failure ? 'FAIL' : 'INCOMPLETE';
    evidence.native_or_control = { original_verdict: originalVerdict, summary: path.join(ctx.root, 'summary.json'), all_original_scenarios_required: plan.required_original_scenarios, transformed_source_sha256: native.transformedSourceHash, original_source: native.originalSource, source_map: native.sourceMap ?? null, trusted_ws_turn_identity: trustedWs };
    evidence.guard_source_scope = { boundary: 'provider-boundary', implementation: plan.provider_boundary_mode, provider_boundary_write_attempts_observed: true, provider_received_count_claimed_from_writes: false, default_url_coverage_claimed: false, original_native_prewarm_preserved: true, note: 'Every actual paid HTTPS request or generative WS control requires its own durable hold. Write attempts are not provider-received proof; raw response/TLS receipts are separate. Original target path/query and native payload are preserved; local upstream override is disclosed.' };
    evidence.provider_boundary = providerBoundary?.metadata() ?? native.coverage ?? null;
    const referenceClosure = await shared.reconcileReferenceReservation({ record, guard, providerBoundaries, nativeCommandFinished: commandCompletion.native_command_finished, observedCostMicros: committedMicros });
    Object.assign(record, referenceClosure.record);
    record.native_command_completion = commandCompletion;
    evidence.cost = { selected_reference_slice_usd: 6, original_source_usd_cap: 3, committed_catalog_cost_micros: committedMicros, outbound, reconciliation, settled, reference_exposure_reconciled: referenceClosure.reconciled, reference_closure: referenceClosure.proof, reference_closure_reason: referenceClosure.reason, conservative_charge_usd: record.charge_usd, conservative_charge_micros: record.charge_micros ?? 6000000, unknown_hold_micros_retained: outbound.unknown_hold_micros, in_flight_hold_micros_retained: outbound.in_flight_hold_micros, catalog_list_equivalent_only: true, provider_invoice_or_credit_verified: false };
    evidence.failure = failure;
  } catch (error) {
    failure = typeof error?.message === 'string' && error.message.startsWith('BLOCKED:') ? error.message : 'BLOCKED:api_key_runtime_or_guard_preparation_failed_no_secret_details';
    evidence.state = 'BLOCKED'; evidence.failure = failure;
    record.state = 'RECONCILIATION_REQUIRED'; record.reason = 'Parent must reconcile unexecuted/unknown work explicitly; no automatic release of selected full hold.';
  } finally {
    const retainedBoundary = providerBoundary ?? native?.providerBoundaryController;
    if (retainedBoundary) {
      const sealed = retainedBoundary.sealGenerativeForwards();
      evidence.final_provider_boundary_seal = sealed;
      evidence.provider_boundary = retainedBoundary.metadata();
    }
    evidence.runtime_helper_transformed_sha256 = runtimeSourceHash;
    record.finished_at = new Date().toISOString();
    await writeFile(reservationFile, `${JSON.stringify(record, null, 2)}\n`, { mode: 0o600 });
    const output = path.join(plan.epoch, `approved-core3-api-key-${plan.name}-evidence.json`);
    await writeFile(output, `${JSON.stringify(evidence, null, 2)}\n`, { mode: 0o600, flag: 'wx' });
    if (native?.bridgeKey) delete globalThis[Symbol.for(native.bridgeKey)];
    for (const [key, value] of Object.entries(previousEnv)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
    console.log(JSON.stringify({ case: plan.case, state: evidence.state, evidence: output, reservation: reservationFile, shared_outbound_guard: Boolean(guard), controller_pid: process.pid, retained_provider_boundary: Boolean(providerBoundary || native?.providerBoundaryController), executed_by_author: false }));
  }
  return evidence;
}

export function apiKeyConfigContract() {
  return {
    kind: 'approved-core3-api-key-config-contract', runtime_config_schema: 'approved-core3-api-key-config-v1',
    execution: 'NOT_RUN; parent only; no adapters, keys or providers accessed by plan mode',
    model_selection_status: 'Current Codex uses pinned no-override observation only; other controls retain unchanged legacy source. Authenticated-session defaults and pricing are not proved.',
    run_lifecycle: 'Parent-owned long-lived controller: native command completion seals future generations but retains SUT/listeners. No automatic cleanup or process.exit.',
    profiles: Object.keys(CASES), expectedSha: 'Parent supplies the rebuilt SHA256',
    epoch: `${BASE}/fixed-<sha12>-core3`, sliceCostUsd: 6,
    models: 'Codex: exactly the observed slug, no aliases or old-model fallback. Other controls: complete original primary/control/auxiliary list.',
    pricingEligibility: 'Per model documented API-key protocol/account eligible tiers, omitted tier semantics and citations; subscription Standard is not an API-key pricing proof',
    nativeModelEvidence: { path: 'Parent-pinned regular no-override SETUP summary.json', sha256: 'Exact raw metadata SHA256; used for expected catalog/guard identity, never written into client configuration' },
    approved_client_identity: 'OMP profiles use the single contract.approved_client_identity launcher/realpath/SHA/version observation. An isolated cloud-key-free --version preflight must pass before reservation, credential access or SUT setup; no config override or version fallback.',
    required_unexecuted_current_adapter_gate: 'Parent NoProvider recapture of the actual generated configuration/env/argv and selected wire model; metadata/pure units alone are not this gate',
    prewarm_policy: 'Only source-eligible exact WS generate:false may have output bound 0; input/cache hold remains. Never inject prompt_cache_options or disable native warmup.',
    unresolved_pricing: 'Hosted tool fees and recursive input need sourced hard limits; missing dimensions are BLOCKED without disabling tools or injecting max_tool_calls.',
    campaignGuard: `${BASE}/evidence/approved-core3-<fresh-root-finance>.json`,
    cancellationGuard: 'Separate fresh native-cancel arithmetic proof; also supply pinnedCancellationGuard naming the exact old literal in the verified fresh source',
    authorization: {
      runner_sha256: Object.fromEntries(MODULES.map((name) => [name, 'Parent-reviewed SHA256'])),
      campaign_guard_sha256: 'Fresh canonical whole-root financial snapshot SHA256',
      revision_manifest_sha256: 'Parent-supplied raw SHA256 of approved-core3-cap30-manifest.json; must equal the file bytes',
      parent_new_binary_green: 'Required true only after actual parent verification',
      parent_safety_review_green: 'Required true only after shared guard and adapters receive Safety GREEN',
      serial_campaign_execution: 'Required true; parent runs one paid command at a time',
      allow_native_api_key_generation: 'Required explicit parent authorization',
      allow_owned_provider_boundary: 'Required explicit parent authorization for the guarded private upstream override',
      notified_root: 'Exact profile root', notified_ports: 'Exact proxy/admin/metrics ports',
      notified_native_port: 'Exact native tap where applicable', parent_collector_ledgered: 'Required true for traced/WS cases',
      notified_auxiliary_ports: 'Exact native observer/relay/tap port array from plan, including empty array for no auxiliary listener',
      notified_provider_boundary_port: 'Exact new relay port, or null when original true-HTTPS hooks are retained',
    },
    caps: { historical_source_usd: 3, selected_new_reference_slice_usd: 6, global_reference_usd: 30, invoice_or_paid_credit_verified: false },
    source_scope: 'Every actual paid provider-boundary write is reserved; client counts alone are not provider dispatch proof. Original-target/local-override deviation is disclosed, not default-URL proof.',
    no_secret_config_values: true,
  };
}
const cliEntryPath = process.argv[1] ? await realpath(path.resolve(process.argv[1])).catch(() => null) : null;
if (import.meta.url.startsWith('file:') && cliEntryPath !== null && cliEntryPath === await realpath(fileURLToPath(import.meta.url)).catch(() => null)) {
  const args = process.argv.slice(2);
  const run = args.includes('--run');
  const fileIndex = args.indexOf('--config');
  const valid = args.every((arg, i) => ['--run', '--plan', '--config'].includes(arg) || i === fileIndex + 1);
  if (!valid || (run && fileIndex < 0)) throw new Error('BLOCKED:usage_--plan_or_--run_--config_nonsecret_json');
  if (fileIndex < 0) console.log(JSON.stringify(apiKeyConfigContract(), null, 2));
  else {
    const file = path.resolve(args[fileIndex + 1] ?? '');
    const canonicalBase = realpathSync(BASE);
    const canonicalFile = realpathSync(file);
    must(canonicalFile.startsWith(`${canonicalBase}/`) && canonicalFile.endsWith('.json') && statSync(canonicalFile).isFile(), 'owned_nonsecret_config_file_required');
    const config = await json(file);
    if (run) {
      try {
        const evidence = await runGuardedApiKeyNative(config);
        if (evidence.state !== 'PASS') process.exitCode = 2;
      } catch (error) {
        const code = typeof error?.message === 'string' && error.message.startsWith('BLOCKED:') ? error.message.slice(8).replace(/[^A-Za-z0-9_.:-]/g, '_') : 'native_run_failed';
        console.error(JSON.stringify({ state: 'BLOCKED', phase: 'run', code }));
        process.exitCode = 2;
      }
    } else {
      try {
        console.log(JSON.stringify(await apiKeyNativePlan(config), null, 2));
      } catch (error) {
        const code = typeof error?.message === 'string' && error.message.startsWith('BLOCKED:') ? error.message.slice(8).replace(/[^A-Za-z0-9_.:-]/g, '_') : 'native_plan_failed';
        console.error(JSON.stringify({ state: 'BLOCKED', phase: 'plan', code }));
        process.exitCode = 2;
      }
  }
}
}
