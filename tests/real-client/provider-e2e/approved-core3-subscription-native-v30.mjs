#!/usr/bin/env bun
// Parent-only real native CLI subscription QA. Default/--plan is read-only.
// Never serializes a provider token, shared grant, local admin token or client key.
import { createHash, randomUUID } from 'node:crypto';
import { spawn, execFile } from 'node:child_process';
import { existsSync, appendFileSync, watch, lstatSync, readFileSync, realpathSync, statSync, accessSync, constants, createReadStream } from 'node:fs';
import { readFile, writeFile, readdir, lstat, realpath, mkdir, rename } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { isDeepStrictEqual } from 'node:util';
import { subscriptionRuntimePlan, prepareAccessOnlySubscriptionRuntime } from './approved-core3-subscription-contract-v30.mjs';
import { BROKER_SOURCE_CONTRACT, sourcePreflight, readBrokerAccess, descriptorForAccess } from './approved-core3-subscription-source.mjs';
import { createOutboundBudgetGuard } from './approved-core3-subscription-budget-v30.mjs';
import { isProviderBoundaryRelay } from './approved-core3-provider-boundary.mjs';
import { loadCurrentNativeModelEvidence } from './approved-core3-subscription-current-native.mjs';
import { isApiKeyControlBoundary } from './approved-core3-api-key-controls-v30.mjs';

const BASE = '/tmp/cc-lb-actual-cli-e2e-20261020/final-gpt-direct-base-gpt30w-20251025/financial-root-gpt30x-canonical';
const LOCAL_ROOT = path.dirname(fileURLToPath(import.meta.url));
const CAP30_CONTRACT = path.join(LOCAL_ROOT, 'approved-core3-cap30-contract.json');
const CAP30_MANIFEST = path.join(LOCAL_ROOT, 'approved-core3-cap30-manifest.json');
const HISTORICAL_CAP25_CONTRACT = path.join(BASE, 'approved-core3-cap25-contract.json');
const HISTORICAL_CAP25_MANIFEST = path.join(BASE, 'approved-core3-cap25-manifest.json');
const LEDGER = '/tmp/cc-lb-actual-cli-e2e-20261020-ledger.json';
const must = (ok, code) => { if (!ok) throw new Error(`BLOCKED:${code}`); };
const sha = (value) => createHash('sha256').update(value).digest('hex');
const once = (text, old, replacement, label) => { must(text.split(old).length === 2, `pinned_native_source_drift:${label}`); return text.replace(old, replacement); };
const json = async (file) => { try { return JSON.parse(await readFile(file, 'utf8')); } catch (error) { if (error.code === 'ENOENT') return null; throw new Error('BLOCKED:invalid_nonsecret_qa_json'); } };
const quote = (value) => JSON.stringify(value);

// Reuse the pinned recorder and its existing filesystem ledger lock. Only the
// established owned ROOT/ledger projections change, not locking or redaction.
async function recordHarnessResource(ledgerPath, owner, entry) {
  let source = await readFile(path.join(BASE, 'qa-runtime.mjs'), 'utf8');
  must(sha(source) === '04a189e56d8cb380ff961154be75eff38c9fc442e3ed44e46c4eadef255d96a7', 'resource_ledger_helper_source_changed');
  source = once(source, 'export const ROOT = path.dirname(fileURLToPath(import.meta.url));', `export const ROOT = ${quote(BASE)};`, 'resource_ledger_owned_root');
  source = once(source, "export const LEDGER_PATH = '/tmp/cc-lb-actual-cli-e2e-20261020-ledger.json';", `export const LEDGER_PATH = ${quote(ledgerPath)};`, 'resource_ledger_owned_path');
  const recorder = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
  return recorder.recordResource({ name: owner }, entry);
}
export const NATIVE_SUBSCRIPTION_CASES = Object.freeze({
  'codex-http': { native: 'codex', kind: 'openai_oauth', slice: 'codex-http-subscription', ports: 19500, scenario: 'http', original_case_forward_cap: 12 },
  'codex-ws': { native: 'codex', kind: 'openai_oauth', slice: 'codex-ws-subscription', ports: 19503, scenario: 'ws', original_case_forward_cap: 12 },
  'codex-ws-reconnect': { native: 'codex', kind: 'openai_oauth', slice: 'codex-ws-reconnect-subscription', ports: 19506, scenario: 'ws_reconnect', original_case_forward_cap: 6 },
  'omp-gemini': { native: 'omp-gemini', kind: 'gemini_oauth', slice: 'omp-gemini-subscription', ports: 19510, original_case_forward_cap: 16 },
  agy: { native: 'agy', kind: 'gemini_oauth', slice: 'agy-subscription', ports: 19530, original_case_forward_cap: 16 },
});
const KNOWN_NATIVE_MODELS = Object.freeze({
  openai_oauth: ['gpt-5.1-codex', 'gpt-5.2', 'gpt-5.2-codex'],
  gemini_oauth: ['gemini-3-pro-preview', 'gemini-3.1-pro-preview', 'gemini-3.1-flash-lite-preview', 'gemini-3.5-flash-lite'],
});
// Fixture checks run the Node test runner, never the controller runtime: the
// v30 controller is Bun, so process.execPath is Bun there. This is the same pin
// as the actual run receipt `runtimes.node`; the omp-gpt adapter imports it.
export const FIXTURE_NODE = Object.freeze({
  path: '/nix/store/fbx2kg04dr8vqk7axzv16ywarbk8hwxy-nodejs-slim-26.12.0/bin/node',
  sha256: '9bd70ab029f5d30632f44b6f3ae060ad848c35a9c7f84a3fd931c0d23370d727',
});
// Synchronous so a transformed fixtureCheck re-verifies the bytes immediately
// before each test spawn; any mismatch throws before anything is spawned.
export function pinnedFixtureNode() {
  let stat = null;
  try { stat = lstatSync(FIXTURE_NODE.path); } catch { /* reported below */ }
  must(stat?.isFile() === true && (stat.mode & 0o111) !== 0, 'pinned_fixture_node_not_executable');
  must(sha(readFileSync(FIXTURE_NODE.path)) === FIXTURE_NODE.sha256, 'pinned_fixture_node_sha_mismatch');
  return FIXTURE_NODE.path;
}
// One approved OMP identity, sourced from the SHA-pinned contract and the
// original version observation. Never independently repin a version literal.
export function validateApprovedOmpIdentity(identity) {
  const exact = (value, keys) => value && typeof value === 'object' && !Array.isArray(value) && isDeepStrictEqual(Object.keys(value).sort(), [...keys].sort());
  must(exact(identity, ['schema', 'launcher', 'realpath', 'sha256', 'version', 'provenance']) && identity.schema === 'approved-core3-omp-client-v1', 'approved_omp_identity_required');
  must([identity.launcher, identity.realpath].every((file) => typeof file === 'string' && path.isAbsolute(file) && !file.includes('\0')) && /^[0-9a-f]{64}$/.test(identity.sha256 ?? '') && /^omp\/[0-9]+\.[0-9]+\.[0-9]+$/.test(identity.version ?? ''), 'approved_omp_identity_invalid');
  const proof = identity.provenance;
  must(exact(proof, ['receipt_path', 'receipt_sha256', 'version_path', 'version_sha256'])
    && [proof.receipt_path, proof.version_path].every((file) => typeof file === 'string' && path.isAbsolute(file) && !file.includes('\0'))
    && [proof.receipt_sha256, proof.version_sha256].every((value) => /^[0-9a-f]{64}$/.test(value ?? '')), 'approved_omp_identity_provenance_invalid');
  return identity;
}
export function verifyApprovedOmpFileIdentity(identity) {
  validateApprovedOmpIdentity(identity);
  let resolved;
  try {
    resolved = realpathSync(identity.launcher);
    must(resolved === identity.realpath && realpathSync(identity.realpath) === identity.realpath, 'approved_omp_realpath_mismatch');
    accessSync(identity.launcher, constants.X_OK);
    must(statSync(resolved).isFile(), 'approved_omp_not_executable');
  } catch (error) {
    if (error.message?.startsWith('BLOCKED:')) throw error;
    throw new Error('BLOCKED:approved_omp_not_executable');
  }
  let digest;
  try { digest = sha(readFileSync(resolved)); } catch { throw new Error('BLOCKED:approved_omp_not_executable'); }
  must(digest === identity.sha256, 'approved_omp_sha_mismatch');
  return identity.launcher;
}
export function assertApprovedOmpVersion(identity, result) {
  validateApprovedOmpIdentity(identity);
  must(!result?.error && result?.status === 0 && result.signal == null, 'approved_omp_version_probe_failed');
  must(typeof result.stdout === 'string' && result.stdout.trim() === identity.version, 'approved_omp_version_mismatch');
  return identity.version;
}
const VERIFIED_OMP_CLIENTS = new WeakSet();
// Builders and native launch sites accept only a receipt produced by this
// preflight in this process; a caller-supplied lookalike is not a verification.
export function assertVerifiedOmpClient(client, contract) {
  must(client && VERIFIED_OMP_CLIENTS.has(client) && isDeepStrictEqual(client.identity, contract?.approved_client_identity), 'approved_omp_preflight_required');
  verifyApprovedOmpFileIdentity(client.identity);
  must(client.version === client.identity.version, 'approved_omp_version_mismatch');
  return client.identity.launcher;
}
export function ompVersionProbeEnvironment(home) {
  return { HOME: home, TMPDIR: home, PATH: '/usr/bin:/bin', TERM: 'dumb', NO_COLOR: '1' };
}
// OMP --version runs before money reservation, credential access or SUT setup.
// Only this passive artifact check and a cloud-key-free version subprocess are
// allowed here. The private home/process are ledgered, retained, never cleaned.
export async function preflightApprovedOmpClient(plan, contract, { ledgerPath = LEDGER } = {}) {
  const identity = JSON.parse(JSON.stringify(validateApprovedOmpIdentity(contract?.approved_client_identity)));
  verifyApprovedOmpFileIdentity(identity);
  const proof = identity.provenance;
  let receiptBytes; let versionBytes; let receipt;
  try {
    receiptBytes = await readFile(proof.receipt_path);
    versionBytes = await readFile(proof.version_path);
    receipt = JSON.parse(receiptBytes);
  } catch { throw new Error('BLOCKED:approved_omp_observation_unavailable'); }
  must(sha(receiptBytes) === proof.receipt_sha256 && sha(versionBytes) === proof.version_sha256, 'approved_omp_observation_sha_mismatch');
  must(receipt.omp_pre?.launcher === identity.launcher && (receipt.omp_pre.binary ?? receipt.omp_pre.realpath) === identity.realpath
    && receipt.omp_pre.sha256 === identity.sha256 && receipt.omp_post?.binary === identity.realpath && receipt.omp_post.sha256 === identity.sha256, 'approved_omp_observation_identity_mismatch');
  must(versionBytes.toString('utf8') === `$ ${identity.launcher} --version\nrc=0\n${identity.version}\n`, 'approved_omp_observation_version_mismatch');
  must(typeof plan.epoch === 'string' && path.isAbsolute(plan.epoch) && (await lstat(plan.epoch)).isDirectory(), 'approved_omp_probe_owned_epoch_required');
  const home = path.join(plan.epoch, `approved-core3-omp-version-home-${randomUUID()}`);
  const command = [identity.launcher, '--version'];
  const record = (entry) => recordHarnessResource(ledgerPath, 'approved-omp-client-preflight', entry);
  await record({ kind: 'scratch-directory', id: home, delete: `After explicit cleanup approval and exact ownership verification: rm -rf -- '${home}'` });
  await mkdir(home, { mode: 0o700 });
  await record({ kind: 'version_probe_process_plan', id: home, command, delete: 'Finite --version subprocess exits naturally; verify exact PID/command before any user-authorized cleanup' });
  const result = await new Promise((resolve) => {
    let recorded = Promise.resolve();
    const child = execFile(identity.launcher, ['--version'], { cwd: home, env: ompVersionProbeEnvironment(home), timeout: 30_000, maxBuffer: 65536, encoding: 'utf8' }, async (error, stdout) => {
      try { await recorded; } catch { error = error ?? new Error('ledger write failed'); }
      resolve({ error: error != null, status: error ? null : 0, signal: error?.signal ?? null, stdout, pid: child.pid ?? null });
    });
    // The command was ledgered before spawn. Await the actual PID record before
    // declaring the probe complete; a failed PID write cannot produce success.
    child.once('spawn', () => {
      recorded = record({ kind: 'process', id: `omp --version pid ${child.pid}`, pid: child.pid, command, home, delete: 'Finite version probe; verify PID/command before explicit cleanup' });
      recorded.catch(() => child.kill('SIGTERM'));
    });
  });
  assertApprovedOmpVersion(identity, result);
  verifyApprovedOmpFileIdentity(identity);
  Object.freeze(identity.provenance);
  Object.freeze(identity);
  const client = Object.freeze({ identity, version: identity.version, probe: Object.freeze({ home, pid: result.pid, status: result.status }) });
  VERIFIED_OMP_CLIENTS.add(client);
  return client;
}
// Only the fixture test-runner selection and the turn-1 pass predicate change.
// nodeEval, prompts, tools, model and request handling stay verbatim.
function pinOmpGeminiFixtureTests(source) {
  const pinned = once(source, 'const tests = spawnSync(process.execPath, ["--test", "--test-reporter=tap"],', 'const tests = spawnSync(SUBSCRIPTION_PRIVATE.fixtureNode(), ["--test", "--test-reporter=tap"],', 'omp_fixture_tests_pinned_node');
  return once(pinned, '  out.turn1_ok = out.node_test_rc === 0 && ', '  out.turn1_ok = out.node_test_rc === 0 && Number.isSafeInteger(out.node_test_counts?.tests) && out.node_test_counts.tests >= originalAssertions.length && out.node_test_counts.fail === 0 && ', 'omp_fixture_tests_counted_pass');
}

// configured != received != verified. The trace summary reports only whether the
// SUT config actually rendered for this run carries the exact requested OTLP
// endpoint (qa-runtime renderConfig writes `otlp_endpoint = <JSON string>`).
// Span receipt and the canonical trace verifier are separate evidence.
function otlpTraceSummary(runnerCtx, config) {
  const unproven = { received_claimed: false, canonical_verifier_run: false };
  const expected = config?.otlpEndpoint;
  if (expected === undefined) return { otlpConfigured: false, collectorRef: null, reason: 'otlp_endpoint_not_requested', ...unproven };
  let text;
  try { text = readFileSync(runnerCtx?.configPath, 'utf8'); } catch { return { otlpConfigured: false, collectorRef: null, reason: 'sut_config_unreadable', ...unproven }; }
  const lines = text.split('\n').filter((line) => /^otlp_endpoint\s*=/.test(line));
  const configured = typeof expected === 'string' && lines.length === 1 && lines[0] === `otlp_endpoint = ${JSON.stringify(expected)}`;
  return { otlpConfigured: configured, collectorRef: configured ? config.capture ?? null : null, reason: configured ? null : 'sut_config_otlp_endpoint_mismatch', ...unproven };
}
// Replaces only the original hardcoded trace summary line; run.mjs is unchanged.
function pinOmpGeminiTraceSummary(source) {
  return once(source, '  summary.trace = { otlpConfigured: false, collectorRef: null };', '  summary.trace = SUBSCRIPTION_PRIVATE.traceSummary(ctx);', 'omp_trace_summary_from_rendered_config');
}
const CURRENT_NATIVE_PROFILE = Object.freeze({
  selection: 'PINNED_LOCAL_METADATA_OBSERVATION',
  historical_harness_source: 'UNCHANGED_FIXED_RAW_EVIDENCE',
  no_model_override: true,
  authenticated_session_provider_qa_proven: false,
});
const CURRENT_NATIVE_CONFIG_CONTRACT = Object.freeze({
  evidence: 'config.nativeModelEvidence { path, sha256 } loaded through shared metadata-only helper',
  model: 'Codex derives expected/catalog model from pinned metadata; config.model is forbidden',
  source: 'historical fixed/raw harness remains unchanged',
  no_model_override: 'generated config model line, CODEX_QA_MODEL env, and --model argv are absent',
  authenticated_session_provider_qa_proven: false,
});
async function modelPlan(config, spec) {
  if (spec.native === 'codex') {
    must(!Object.hasOwn(config, 'model'), 'codex_current_native_config_model_override_forbidden');
    const nativeModelEvidence = await loadCurrentNativeModelEvidence(config.nativeModelEvidence ?? {});
    must(nativeModelEvidence.no_model_override === true && nativeModelEvidence.current_user_session_default_proven === false && nativeModelEvidence.provider_qa_verified === false, 'codex_current_native_observation_contract_invalid');
    const model = nativeModelEvidence.model;
    must(typeof model === 'string' && model.length > 0, 'codex_current_native_observed_model_required');
    must(config.authorization?.confirmed_native_models?.includes(model), 'parent_native_model_availability_evidence_required');
    return { models: [model], nativeModelEvidence, profile: Object.freeze({ ...CURRENT_NATIVE_PROFILE, config_contract: CURRENT_NATIVE_CONFIG_CONTRACT }) };
  }
  must(typeof config.model === 'string' && KNOWN_NATIVE_MODELS[spec.kind].includes(config.model), 'known_official_native_model_required');
  const extras = spec.native === 'omp-gemini' ? [config.controlModel] : spec.native === 'agy' ? config.auxiliaryModels : [];
  must(Array.isArray(extras) && extras.every((model) => KNOWN_NATIVE_MODELS[spec.kind].includes(model)), 'known_native_control_and_auxiliary_models_required');
  if (spec.native === 'omp-gemini') must(['gemini-3-pro-preview', 'gemini-3.1-pro-preview'].includes(config.model), 'original_omp_primary_model_contract');
  const models = [...new Set([config.model, ...extras])];
  must(models.every((model) => config.authorization?.confirmed_native_models?.includes(model)), 'parent_native_model_availability_evidence_required');
  return { models, nativeModelEvidence: null, profile: null };
}

// New v30 executions select exactly the $6 slice. Historical records keep their
// recorded 3/6 interpretation in recordedSliceCapMicros and the budget library.
function selectedReferenceSliceUsd(config) {
  const cap = config.sliceCostUsd ?? 6;
  must(cap === 6, 'approved_reference_slice_cap_required');
  return cap;
}

function recordedSliceCapMicros({ reservation, outbound, summary, budget, driver } = {}) {
  const caps = [];
  // Missing creation metadata is historical $3 evidence, never today's $6.
  if (reservation) caps.push(reservation.slice_cap_micros ?? 3000000);
  if (outbound) caps.push(outbound.slice_usd_cap_micros ?? 3000000);
  if (summary?.limits?.client?.sliceCostUsd !== undefined) caps.push(summary.limits.client.sliceCostUsd * 1000000);
  for (const record of [budget, driver]) if (record?.slice_usd_cap_micros !== undefined) caps.push(record.slice_usd_cap_micros);
  if (caps.length === 0) return 3000000;
  must(caps.every((cap) => [3000000, 6000000].includes(cap)) && new Set(caps).size === 1, 'conflicting_or_unsupported_recorded_slice_caps');
  return caps[0];
}

const referenceInteger = (value) => Number.isSafeInteger(value) && value >= 0;
const referenceHash = (value) => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
const REFERENCE_STATUSES = new Set(['SETTLED', 'IN_FLIGHT', 'UNKNOWN']);
const API_KEY_REFERENCE_SLICES = new Set(['codex', 'omp-gpt', 'omp-gemini', 'agy', 'faults', 'native-http-cancel', 'native-http-cancel-concurrent', 'gemini-accounting-control', 'anthropic-regression']);

function referenceSliceForCase(caseName) {
  if (Object.hasOwn(NATIVE_SUBSCRIPTION_CASES, caseName)) return NATIVE_SUBSCRIPTION_CASES[caseName].slice;
  const slice = typeof caseName === 'string' && caseName.startsWith('api-key-') ? caseName.slice(8) : null;
  return API_KEY_REFERENCE_SLICES.has(slice) ? slice : null;
}

// Recompute live exposure; cumulative reservation bounds are identity evidence,
// never a substitute for held cost or a multiplier for future requests.
function referenceGuardEvidence(record, outbound) {
  const cap = record?.slice_cap_micros ?? 3000000;
  if (![3000000, 6000000].includes(cap) || !record || !outbound ||
      outbound.schema !== 'outbound-budget-v1' || outbound.case !== record.case ||
      referenceSliceForCase(record.case) !== record.slice ||
      (outbound.slice !== undefined && outbound.slice !== record.slice) ||
      outbound.slice_usd_cap_micros !== cap || outbound.sealed !== true ||
      outbound.seal_persisted !== true || outbound.bound_breach !== false ||
      outbound.unsafe_integer_cost_evidence !== false || outbound.unsupported_cost_dimension === true || !Array.isArray(outbound.records) ||
      !referenceInteger(outbound.attempts) || outbound.attempts !== outbound.records.length) return null;
  const records = new Map();
  let spent = 0, inFlight = 0, unknown = 0, boundSum = 0, unresolved = false;
  for (const row of outbound.records) {
    if (!row || !referenceHash(row.id_sha256) || records.has(row.id_sha256) ||
        !REFERENCE_STATUSES.has(row.status) || !referenceInteger(row.reserved_bound_micros) ||
        !referenceInteger(row.charged_micros) || row.charged_micros > row.reserved_bound_micros ||
        row.bound_breach === true || row.unsupported_cost_dimension === true) return null;
    records.set(row.id_sha256, row);
    boundSum += row.reserved_bound_micros;
    if (row.status === 'SETTLED') spent += row.charged_micros;
    else {
      unresolved = true;
      if (row.status === 'IN_FLIGHT') inFlight += row.reserved_bound_micros;
      else unknown += Math.max(row.reserved_bound_micros, row.charged_micros);
    }
    if (![spent, inFlight, unknown, boundSum].every(referenceInteger)) return null;
  }
  const held = spent + inFlight + unknown;
  if (!referenceInteger(held) || held > cap || outbound.unresolved !== unresolved ||
      outbound.spent_micros !== spent || outbound.in_flight_hold_micros !== inFlight ||
      outbound.unknown_hold_micros !== unknown || outbound.total_held_micros !== held) return null;
  return { cap, records, boundSum, held, unresolved, ids: [...records.keys()].sort() };
}

// Distinct from COMPLETED_NATIVE: a pinned, audited version-gate run with all
// possible dispatch writers stopped. Raw INCOMPLETE/unknown usage stay intact.
const ZERO_POLICY = Object.freeze({
  id: 'f3b-openai-api-key-omp-gpt-version-gate-v1',
  manifest: '4897b4601609411d9e40627133122a1d8ff530fce6b8b85d2e999e0fd5ea19bc',
  contract: '54cba19ac357aa1c91e8fb09323497e492285d89c071cf076cbedda1ca0d7687',
  binary: 'dcde4343277cfc977bdfa77df768ea475d990d86058a879264fc58bb2a4e71a4',
  commit: 'f3b8044ea427fab13cfafffae01c7d4189507684',
  tree: 'c793b0ebda2668fa560a959ca6c09b2707f9a94b',
  archive: 'a35339b8402de317822b0203bd5b858a92a276b5256a98369390fbaeca9298f4',
  // Immutable compiled-source namespace, not an arbitrary relocation switch.
  compiled_root: '/tmp/cc-lb-actual-cli-e2e-20261020/after-counttokens-f3b8044e-39bbfeb283',
  runtime: 'fe7d0a3cb403d620898404037567c13977ad10976f00868857fc2dba10efbbf5',
  module: '9f179ae0750e778bbf315cc9746d639dbc549826982919766a1928c80346036e',
  creator: 'dae1d2693457e7485f57ec0b2f7d846a46cd49a9737e6648701cb817f56edff0',
  // Sorted relative-role -> byte-SHA map of all 55 original epoch files.
  // Namespace metadata may be isolated in QA; audited source bytes may not.
  epoch_roles: '9d94f65d63097450b7af79aa26e4b03c44da47216d19ac09fae80fdc2f3d6fa6',
});
const ZERO_REQUIRED = ['creation_receipt', 'final_config', 'preceding_guard', 'epoch_manifest', 'selected_contract', 'selected_source_manifest', 'runtime_config', 'prepare', 'slice_ledger', 'resources', 'reservation_preimage', 'outbound', 'summary', 'budget', 'driver_summary', 'version', 'run_evidence', 'terminal_receipt', 'terminal_appendix', 'scheduler_receipt'];
const ZERO_OPTIONAL = ['creator_source', 'ready_source', 'prepare_source'];
const ZERO_LOADED = new WeakSet();
const zeroCanonical = (value) => JSON.stringify(value, (_, item) => item && typeof item === 'object' && !Array.isArray(item) ? Object.fromEntries(Object.keys(item).sort().map((key) => [key, item[key]])) : item);
const zeroEqual = (a, b) => zeroCanonical(a) === zeroCanonical(b);
const zeroExact = (value, keys) => value && typeof value === 'object' && !Array.isArray(value) && zeroEqual(Object.keys(value).sort(), [...keys].sort());
const zeroEmpty = (value) => Array.isArray(value) && value.length === 0;
function zeroFreeze(value) {
  if (value && typeof value === 'object') { for (const child of Object.values(value)) zeroFreeze(child); Object.freeze(value); }
  return value;
}
function zeroTarget(binding) { return path.join(binding.epoch, 'approved-core3-api-key-omp-gpt-reservation.json'); }
function zeroBackup(binding) { return path.join(binding.epoch, 'evidence', `approved-core3-reference-preimage-${binding.artifacts.reservation_preimage.sha256}.json`); }
function zeroBindingId(binding) {
  const original = structuredClone(binding);
  original.artifacts.reservation_preimage.path = zeroTarget(binding);
  return sha(zeroCanonical(original));
}
async function zeroRegularPath(file, root) {
  must(typeof file === 'string' && path.isAbsolute(file) && path.resolve(file) === file && file.startsWith(`${root}/`), 'pinned_zero_path_invalid');
  const relative = path.relative(root, file);
  let current = root;
  for (const part of relative.split(path.sep)) {
    current = path.join(current, part);
    must(!(await lstat(current)).isSymbolicLink(), 'pinned_zero_symlink_rejected');
  }
  must(await realpath(file) === path.join(await realpath(root), relative), 'pinned_zero_alias_rejected');
  must((await lstat(file)).isFile(), 'pinned_zero_regular_file_required');
}
// Bounded named roles only; no SQLite/key reads, recursive directory reads,
// source imports, process probes, network calls, or caller boolean overrides.
export async function loadPinnedRunSealedZeroEvidence(actualBinding) {
  const binding = structuredClone(actualBinding);
  must(zeroExact(binding, ['schema', 'policy_id', 'case', 'slice', 'epoch', 'reservation_created_at', 'slice_cap_micros', 'controller_pid', 'sut_pid', 'collector_pid', 'relay_port', 'relay_resource_identity', 'upstream_id', 'optional_auxiliary_roles', 'required_artifact_roles', 'artifacts']), 'pinned_zero_binding_shape');
  must(binding.schema === 'pinned-run-sealed-zero-binding-v1' && binding.policy_id === ZERO_POLICY.id && binding.case === 'api-key-omp-gpt' && binding.slice === 'omp-gpt' && binding.slice_cap_micros === 6000000, 'pinned_zero_policy_unsupported');
  must(typeof binding.epoch === 'string' && path.dirname(binding.epoch) === BASE && /^fixed-dcde4343277c-core3-[a-z0-9-]+$/.test(path.basename(binding.epoch)), 'pinned_zero_epoch_invalid');
  must([binding.controller_pid, binding.sut_pid, binding.collector_pid].every((pid) => Number.isSafeInteger(pid) && pid > 0) && new Set([binding.controller_pid, binding.sut_pid, binding.collector_pid]).size === 3 && Number.isSafeInteger(binding.relay_port) && binding.relay_port > 1024 && binding.relay_port < 65536, 'pinned_zero_process_tuple_invalid');
  must(typeof binding.reservation_created_at === 'string' && Number.isFinite(Date.parse(binding.reservation_created_at)) && [binding.relay_resource_identity, binding.upstream_id].every((value) => typeof value === 'string' && /^[0-9a-f-]{36}$/.test(value)), 'pinned_zero_run_tuple_invalid');
  must(zeroEqual(binding.required_artifact_roles, ZERO_REQUIRED) && zeroEqual(binding.optional_auxiliary_roles, ZERO_OPTIONAL), 'pinned_zero_role_contract_invalid');
  const roles = Object.keys(binding.artifacts ?? {});
  must(ZERO_REQUIRED.every((role) => roles.includes(role)) && roles.every((role) => [...ZERO_REQUIRED, ...ZERO_OPTIONAL].includes(role)), 'pinned_zero_artifact_roles_invalid');
  const documents = {}, dependencies = {}, originalPaths = new Set();
  const read = async (descriptor, root = BASE, max = 8 * 1024 * 1024, retain = true) => {
    must(zeroExact(descriptor, ['path', 'sha256']) && referenceHash(descriptor.sha256), 'pinned_zero_descriptor_invalid');
    await zeroRegularPath(descriptor.path, root);
    must((await lstat(descriptor.path)).size <= max, 'pinned_zero_artifact_too_large');
    if (retain) {
      const bytes = await readFile(descriptor.path);
      must(sha(bytes) === descriptor.sha256, 'pinned_zero_artifact_hash_mismatch');
      dependencies[descriptor.path] = descriptor.sha256;
      return bytes;
    }
    const digest = createHash('sha256');
    for await (const chunk of createReadStream(descriptor.path)) digest.update(chunk);
    must(digest.digest('hex') === descriptor.sha256, 'pinned_zero_artifact_hash_mismatch');
    dependencies[descriptor.path] = descriptor.sha256;
  };
  const fixed = {
    epoch_manifest: 'fixed-epoch.json', selected_contract: 'contract.json', selected_source_manifest: 'approved-core3-cap25-manifest.json',
    runtime_config: 'omp-gpt/runtime/cc-lb.toml', prepare: 'omp-gpt/prepare.json', slice_ledger: 'omp-gpt/slice-ledger.json',
    resources: 'omp-gpt/resources.jsonl', outbound: 'omp-gpt/outbound-budget.json', summary: 'omp-gpt/summary.json',
    budget: 'omp-gpt/budget.json', driver_summary: 'omp-gpt/evidence/driver-summary.json', version: 'omp-gpt/evidence/omp-version.txt',
    run_evidence: 'approved-core3-api-key-omp-gpt-evidence.json',
  };
  for (const role of roles) {
    const descriptor = binding.artifacts[role];
    if (fixed[role]) must(descriptor.path === path.join(binding.epoch, fixed[role]), 'pinned_zero_role_path_mismatch');
    else if (role === 'reservation_preimage') must([zeroTarget(binding), zeroBackup(binding)].includes(descriptor.path), 'pinned_zero_predecessor_path_mismatch');
    else must(typeof descriptor.path === 'string' && path.dirname(descriptor.path) === path.join(BASE, 'evidence'), 'pinned_zero_audit_path_mismatch');
    const bytes = await read(descriptor);
    if (role === 'reservation_preimage') must(((await lstat(descriptor.path)).mode & 0o777) === 0o600, 'pinned_zero_preimage_mode_invalid');
    const physical = await realpath(descriptor.path);
    must(!originalPaths.has(physical), 'pinned_zero_duplicate_artifact');
    originalPaths.add(physical);
    documents[role] = role === 'resources' ? bytes.toString('utf8').trim().split('\n').map((line) => JSON.parse(line))
      : ['runtime_config', 'version', ...ZERO_OPTIONAL].includes(role) ? bytes.toString('utf8') : JSON.parse(bytes);
  }
  // Bounded named logs of the original run; hash only, never print or parse.
  for (const channel of ['stdout', 'stderr']) {
    const log = documents.creation_receipt.run?.[channel];
    must(log && typeof log.path === 'string' && path.dirname(log.path) === path.join(BASE, 'evidence') && path.basename(log.path).endsWith(`.run.${channel}.log`) && referenceInteger(log.bytes) && log.bytes <= 8 * 1024 * 1024, 'pinned_zero_run_log_descriptor_invalid');
    const physical = await realpath(log.path);
    must(!originalPaths.has(physical), 'pinned_zero_duplicate_artifact');
    originalPaths.add(physical);
    await read({ path: log.path, sha256: log.sha256 }, BASE, 8 * 1024 * 1024, false);
    must((await lstat(log.path)).size === log.bytes, 'pinned_zero_run_log_size_mismatch');
  }
  must(binding.artifacts.selected_contract.sha256 === ZERO_POLICY.contract && binding.artifacts.selected_source_manifest.sha256 === ZERO_POLICY.manifest, 'pinned_zero_unknown_source_snapshot');
  const manifest = documents.epoch_manifest;
  const original = documents.selected_source_manifest.source_sha256;
  must(Object.keys(original ?? {}).length === 50 && !Object.hasOwn(original, 'approved-core3-cap25-manifest.json') && original['approved-core3-api-key-native-v25.mjs'] === ZERO_POLICY.creator, 'pinned_zero_creator_source_unsupported');
  must(Object.keys(manifest.files ?? {}).length === 55 && manifest.epoch === path.basename(binding.epoch) && zeroEqual(manifest.approved_core3?.source_sha256, original), 'pinned_zero_epoch_source_set_invalid');
  const rolePins = Object.fromEntries(Object.keys(manifest.files).sort().map((role) => [role, manifest.files[role].sha256]));
  must(sha(JSON.stringify(rolePins)) === ZERO_POLICY.epoch_roles, 'pinned_zero_unaudited_source_roles');
  const sourceRoot = await realpath(BASE);
  for (const [relative, pin] of Object.entries(original)) {
    const descriptor = manifest.approved_core3.source_descriptors?.[relative];
    must(descriptor?.source === path.join(sourceRoot, relative) && descriptor.source_sha256 === pin, 'pinned_zero_source_alias_mismatch');
  }
  for (const [relative, pin] of Object.entries(manifest.files)) {
    must(typeof relative === 'string' && relative !== '' && !path.isAbsolute(relative) && path.normalize(relative) === relative && !relative.split('/').includes('..') && referenceHash(pin.sha256), 'pinned_zero_source_role_invalid');
    await read({ path: path.join(binding.epoch, relative), sha256: pin.sha256 });
  }
  const contract = documents.selected_contract, compiled = contract.compiled_artifact_binding;
  must(compiled?.source_commit === ZERO_POLICY.commit && compiled.source_tree === ZERO_POLICY.tree && compiled.source_archive?.sha256 === ZERO_POLICY.archive && contract.artifact?.expected_sha256 === ZERO_POLICY.binary && zeroEqual(compiled, manifest.compiled_artifact_binding) && zeroEqual(contract.artifact, manifest.binary), 'pinned_zero_compiled_policy_mismatch');
  for (const role of ['compiled_witness', 'compiled_pair', 'source_index_before', 'source_index_after', 'source_archive', 'overlay_equivalence']) await read(compiled[role], ZERO_POLICY.compiled_root, role === 'source_archive' ? 128 * 1024 * 1024 : 8 * 1024 * 1024, false);
  await read({ path: contract.artifact.path, sha256: ZERO_POLICY.binary }, ZERO_POLICY.compiled_root, 512 * 1024 * 1024, false);
  const oracle = await import('./approved-core3-oracle-v30.mjs');
  await oracle.validateCompiledArtifact(compiled, contract.artifact, ZERO_POLICY.binary);
  const capture = documents.terminal_appendix.final_capture;
  must(capture?.path === path.join(binding.epoch, 'approved-core3-spans.json'), 'pinned_zero_capture_path_mismatch');
  documents.final_capture = JSON.parse(await read({ path: capture.path, sha256: capture.sha256 }));
  const catalogPath = documents.slice_ledger.paths?.priceCatalog;
  must(catalogPath === path.join(binding.epoch, 'omp-gpt/runtime/price-catalog.json'), 'pinned_zero_catalog_path_mismatch');
  const provenance = documents.prepare.costCatalog?.provenance;
  must(provenance?.path === catalogPath && referenceHash(provenance.sha256), 'pinned_zero_catalog_provenance_missing');
  documents.catalog = JSON.parse(await read({ path: catalogPath, sha256: provenance.sha256 }, BASE, 16 * 1024 * 1024));
  const preceding = documents.preceding_guard.source_files;
  must(preceding && Object.keys(preceding).length === 74 && !Object.hasOwn(preceding, path.relative(BASE, zeroTarget(binding))), 'pinned_zero_prior_snapshot_missing');
  for (const [relative, digest] of Object.entries(preceding)) {
    must(typeof relative === 'string' && !path.isAbsolute(relative) && !relative.split('/').includes('..') && (['budget.json', 'driver-budget.json', 'summary.json', 'outbound-budget.json'].includes(path.basename(relative)) || /^approved-core3-(api-key|subscription)-[a-z0-9-]+-reservation\.json$/.test(path.basename(relative))), 'pinned_zero_prior_role_invalid');
    await read({ path: path.join(BASE, relative), sha256: digest });
  }
  const loaded = zeroFreeze({ binding, documents, dependencies, canonical_root: sourceRoot });
  ZERO_LOADED.add(loaded);
  const result = validatePinnedRunSealedZeroEvidence(documents.reservation_preimage, loaded);
  must(result.valid, result.reason);
  return loaded;
}
// Pure predicate shared by the zero successor writer and every root reader.
// Source-derived audit scope is separate from unchanged raw bypass/TLS claims.
export function validatePinnedRunSealedZeroEvidence(record, loaded) {
  try {
    const { binding: b, documents: d } = loaded;
    const a = b.artifacts, p = d.reservation_preimage, c = d.creation_receipt, e = d.run_evidence, s = d.summary, t = d.terminal_receipt, x = d.terminal_appendix, o = d.outbound;
    const check = (condition, reason) => must(condition, `pinned_zero_${reason}`);
    check(b.policy_id === ZERO_POLICY.id && b.case === 'api-key-omp-gpt' && b.slice === 'omp-gpt' && b.slice_cap_micros === 6000000, 'policy_unsupported');
    check(p.case === b.case && p.slice === b.slice && p.credential_kind === 'openai_api_key' && p.at === b.reservation_created_at && p.slice_cap_micros === b.slice_cap_micros && p.charge_micros === b.slice_cap_micros && p.charge_usd === 6 && p.state === 'RECONCILIATION_REQUIRED' && p.unresolved === true && p.reference_closure === null, 'predecessor_invalid');
    const completion = { native_command_finished: false, main_returned: true, receipt_scope: 'Original native child exit/close callbacks, not socket-idle polling; retained SUT is excluded', receipts: [] };
    check(zeroEqual(p.native_command_completion, completion) && zeroEqual(e.native_command_completion, completion) && e.state === 'INCOMPLETE' && e.real_paid_funding_or_invoice_state === 'UNVERIFIED', 'completion_rewritten');
    check(c.kind === 'gptmeta1-omp-gpt-single-actual-run' && c.run_count === 1 && c.oracle_count === 0 && c.retries === 0 && c.source_or_config_edits_after_freeze === 0 && c.source50_pre_post_match === true && c.source_consumption_complete === true && c.cleanup === false, 'creator_execution_invalid');
    check(c.checks?.some((row) => row.name === 'fresh_case_root_pre' && row.pass === true) && c.checks.every((row) => row.pass === true), 'creator_preconditions_missing');
    const sourceMap = d.selected_source_manifest.source_sha256;
    for (const rows of [c.source50_pre, c.source50_post, t.original_source50?.pre, t.original_source50?.post]) {
      check(Array.isArray(rows) && rows.length === 50 && new Set(rows.map((row) => row.name)).size === 50 && rows.every((row) => row.expected === sourceMap[row.name] && row.actual === row.expected), 'source50_mismatch');
    }
    check(t.epoch_file_hashes?.length === 54 && new Set(t.epoch_file_hashes.map((row) => row.rel)).size === 54 && t.epoch_file_hashes.every((row) => row.rel !== 'credentials.json' && row.expected === d.epoch_manifest.files[row.rel]?.sha256 && row.actual === row.expected && row.match === true), 'terminal_epoch_roles_mismatch');
    check(t.original_source50.match === true && c.epoch_manifest_pre?.sha256 === a.epoch_manifest.sha256 && c.epoch_manifest_pre.path === a.epoch_manifest.path && c.final_config?.path === a.final_config.path && c.final_config.sha256 === a.final_config.sha256 && c.final_config_frozen_sha256 === a.final_config.sha256, 'creator_artifact_lineage_mismatch');
    const config = d.final_config, manifest = d.epoch_manifest, offset = manifest.port_offset;
    check([500, 600, 700, 800, 900, 1000].includes(offset) && config.case === b.case && config.epoch === b.epoch && config.expectedSha === ZERO_POLICY.binary && config.sliceCostUsd === 6 && zeroEqual(config.models, ['gpt-5.2']), 'config_run_identity_mismatch');
    check(['parent_new_binary_green', 'parent_safety_review_green', 'serial_campaign_execution', 'allow_native_api_key_generation', 'allow_owned_provider_boundary'].every((key) => config.authorization?.[key] === true), 'original_run_authorization_mismatch');
    check(config.authorization.notified_root === path.join(b.epoch, b.slice) && zeroEqual(config.authorization.notified_ports, [18510 + offset, 18511 + offset, 18512 + offset]) && config.authorization.notified_native_port === 18513 + offset && config.authorization.notified_provider_boundary_port === b.relay_port && zeroEqual(config.authorization.notified_auxiliary_ports, [18513 + offset, b.relay_port]), 'original_resource_plan_mismatch');
    check(c.compiled?.source_commit === ZERO_POLICY.commit && c.compiled.source_tree === ZERO_POLICY.tree && c.compiled.binary_sha256 === ZERO_POLICY.binary && c.compiled.compiled_witness_sha256 === d.selected_contract.compiled_artifact_binding.compiled_witness.sha256 && c.artifact?.path === d.selected_contract.artifact.path && c.artifact.sha256 === ZERO_POLICY.binary, 'creator_compiled_identity_mismatch');
    check(s.model === 'gpt-5.2' && ['local', 'runtime', 'expected'].every((key) => s.binary_sha256?.[key] === ZERO_POLICY.binary), 'native_binary_identity_mismatch');
    check(config.authorization?.runner_sha256?.['approved-core3-api-key-native-v25.mjs'] === ZERO_POLICY.creator && config.authorization.revision_manifest_sha256 === a.selected_source_manifest.sha256 && config.authorization.campaign_guard_sha256 === a.preceding_guard.sha256 && config.campaignGuard === a.preceding_guard.path && p.root_guard?.sha256 === a.preceding_guard.sha256 && c.guard?.sha256 === a.preceding_guard.sha256, 'creator_guard_lineage_mismatch');
    check(d.preceding_guard.campaignCapMicros === 25000000 && d.preceding_guard.plannedSubscriptionSliceMaxMicros === 6000000 && o.campaign_before_micros === d.preceding_guard.conservativePriorMicros, 'original_budget_lineage_mismatch');
    check(zeroEqual(e.root_guard, p.root_guard), 'stored_root_guard_mismatch');
    check(c.run?.pid === b.controller_pid && zeroEqual(c.run.command.slice(1), [path.join(BASE, 'approved-core3-api-key-native-v25.mjs'), '--run', '--config', a.final_config.path]), 'controller_identity_mismatch');
    check(d.prepare.pid === b.sut_pid && d.prepare.model === 'gpt-5.2' && d.prepare.provider === 'openai' && d.prepare.slice === b.slice && d.slice_ledger.pid === b.sut_pid && d.slice_ledger.paths?.root === path.join(b.epoch, b.slice), 'sut_identity_mismatch');
    check(d.prepare.binary?.sha256 === ZERO_POLICY.binary && d.prepare.binary.expected === ZERO_POLICY.binary && d.prepare.binary.match === true && d.prepare.binary.path === d.selected_contract.artifact.path && zeroEqual(d.prepare.upstreams, d.slice_ledger.upstreams), 'sut_artifact_mismatch');
    check(d.prepare.principalId === s.principal_id && d.prepare.keyId === s.key_id && d.prepare.upstreams.length === 1 && d.prepare.upstreams[0].id === b.upstream_id && s.upstream_id === b.upstream_id, 'owner_identity_mismatch');
    check(d.slice_ledger.principalId === d.prepare.principalId && d.slice_ledger.keyId === d.prepare.keyId && d.resources.filter((row) => row.kind === 'local_proxy_key_file').length === 1 && d.resources.some((row) => row.kind === 'local_proxy_key_file' && row.key_id === d.prepare.keyId && row.id === path.join(b.epoch, b.slice, 'client/key')), 'local_key_owner_mismatch');
    const resource = d.resources.filter((row) => row.kind === 'listener' && row.role === 'actual_provider_boundary_http_ws');
    check(resource.length === 1 && resource[0].pid === b.controller_pid && resource[0].id === `127.0.0.1:${b.relay_port}/tcp` && resource[0].relay_identity === b.relay_resource_identity && resource[0].synthetic_fixture_only === false && b.relay_port === 20510 + offset, 'boundary_resource_mismatch');
    check(d.resources.filter((row) => row.kind === 'process').length === 1 && d.resources.some((row) => row.kind === 'process' && row.pid === b.sut_pid && row.config === a.runtime_config.path), 'extra_runtime_writer');
    check(e.runtime_helper_transformed_sha256 === ZERO_POLICY.runtime && e.native_or_control?.transformed_source_sha256 === ZERO_POLICY.module && e.native_or_control.original_source === path.join(b.epoch, 'omp-gpt/run.mjs') && e.native_or_control.summary === a.summary.path, 'generated_source_policy_mismatch');
    check(s.verdict === 'BLOCKED' && s.client?.expectedVersionPrefix === '18.12.5' && typeof s.client.version === 'string' && !s.client.version.includes(s.client.expectedVersionPrefix) && zeroEqual(s.blockers, [`omp_version_unexpected:${s.client.version}`]) && zeroEmpty(s.driverRuns) && zeroEmpty(s.runs) && zeroExact(s.scenarios, []), 'generation_scenario_launched');
    check(d.driver_summary.verdict === s.verdict && zeroEqual(d.driver_summary.blockers, s.blockers) && zeroEmpty(d.driver_summary.driverRuns) && zeroEmpty(d.driver_summary.runs) && zeroExact(d.driver_summary.scenarios, []), 'driver_terminal_mismatch');
    check(d.version === `$ ${s.client.binaryPath} --version\nrc=0\n${s.client.version}\n` && zeroEqual(t.causal_terminal?.native_command_completion, completion) && t.causal_terminal.driver_runs === 0 && t.causal_terminal.runs === 0 && zeroEqual(t.causal_terminal.blockers, s.blockers), 'version_terminal_mismatch');
    const current = referenceGuardEvidence(p, o);
    check(zeroExact(o, ['schema', 'case', 'slice_usd_cap_micros', 'campaign_usd_cap_micros', 'case_forward_cap', 'codex_aggregate_forward_cap', 'attempts', 'spent_micros', 'in_flight_hold_micros', 'unknown_hold_micros', 'total_held_micros', 'campaign_before_micros', 'unsafe_integer_cost_evidence', 'total_held_micros_exact', 'unresolved', 'bound_breach', 'sealed', 'seal_persisted', 'unsupported_cost_dimension', 'blocked', 'reason', 'catalog_sha256', 'pricing_basis', 'real_paid_funding_or_invoice_state', 'actual_subscription_quota_or_credit_state', 'records']), 'unknown_guard_schema');
    check(current && o.campaign_usd_cap_micros === 25000000 && o.attempts === 0 && zeroEmpty(o.records) && o.spent_micros === 0 && o.in_flight_hold_micros === 0 && o.unknown_hold_micros === 0 && o.total_held_micros === 0 && o.total_held_micros_exact === '0' && o.unresolved === false && o.blocked === false && o.unsupported_cost_dimension === false, 'guard_not_complete_zero');
    check(o.real_paid_funding_or_invoice_state === 'UNVERIFIED' && o.actual_subscription_quota_or_credit_state === 'UNVERIFIED', 'raw_invoice_claim_changed');
    check(zeroEqual(e.cost?.outbound, o) && zeroEqual(t.boundary?.outbound, o) && d.budget.attempts === 0 && d.budget.costUsd === 0 && d.budget.costKnown === true && zeroEmpty(d.budget.unpricedRequestIds), 'persisted_guard_disagrees');
    check(e.cost.committed_catalog_cost_micros === null && e.cost.settled === false && e.cost.provider_invoice_or_credit_verified === false, 'raw_usage_claim_changed');
    const boundary = e.provider_boundary;
    check(zeroExact(boundary, ['schema', 'protocol', 'port', 'source_scope', 'synthetic_fixture_only', 'tls_trust_scope', 'tls_verification_policy', 'source_original_url', 'owned_override', 'endpoint_deviation', 'guard_identity_present', 'closed', 'blocked', 'sealed', 'future_generation_admission_denied', 'sealed_guard_identity_unchanged', 'guard_sealed', 'guard_seal_persisted', 'sealing_complete', 'seal_state', 'seal_reason', 'admission_denial_scope', 'tls_receipts', 'tls_summary', 'transport_policy', 'counters', 'capture_bytes_held', 'capture_limit_bytes', 'request_body_limit_bytes', 'attempts', 'receipts', 'reservation_attempt_matches', 'generation_attempt_reservation_join', 'outstanding', 'settled', 'provider_received_claim']), 'unknown_boundary_schema');
    check(boundary?.schema === 'provider-boundary-relay-v1' && boundary.protocol === 'openai-responses' && boundary.source_scope === 'provider-boundary' && boundary.synthetic_fixture_only === false && boundary.port === b.relay_port && boundary.guard_identity_present === true && boundary.blocked === false && boundary.closed === false, 'boundary_identity_invalid');
    check(zeroEqual(boundary, t.boundary.stored) && zeroEqual(e.final_provider_boundary_seal, t.boundary.final_seal), 'boundary_lifetime_mismatch');
    const sealFields = ['sealed', 'future_generation_admission_denied', 'sealed_guard_identity_unchanged', 'guard_sealed', 'guard_seal_persisted', 'sealing_complete'];
    check(sealFields.every((key) => boundary[key] === true && e.final_provider_boundary_seal[key] === true) && e.final_provider_boundary_seal.ok === true && boundary.seal_state === 'SEALED', 'boundary_not_sealed');
    check(['attempts', 'receipts', 'tls_receipts', 'reservation_attempt_matches', 'outstanding', 'settled'].every((key) => zeroEmpty(boundary[key]))
      && zeroExact(boundary.counters, ['blocked', 'metadata_requests', 'ws_upgrade_write_attempts', 'websocket_transport_controls', 'provider_response_receipts', 'actual_boundary_write_attempts', 'actualgeneration_writeattempts', 'settlement_failures']) && Object.values(boundary.counters).every((value) => value === 0)
      && zeroExact(boundary.tls_summary, ['request_attempts', 'secure_connect_observed', 'certificate_and_hostname_verified', 'provider_status_received']) && Object.values(boundary.tls_summary).every((value) => value === 0), 'any_provider_attempt');
    const join = boundary.generation_attempt_reservation_join;
    check(join?.generation_attempts === 0 && join.reservation_records === 0 && join.successful_reservation_bound_sum_micros === 0 && join.unmatched_generation_attempts === 0 && join.duplicate_reservation_write_matches === 0 && ['generation_counter_matches_attempt_records', 'all_reservation_bounds_known', 'all_successful_reservations_have_one_write_attempt'].every((key) => join[key] === true), 'reservation_join_invalid');
    check(boundary.source_original_url?.protocol === 'https:' && boundary.source_original_url.host === 'api.openai.com' && boundary.source_original_url.port === 443 && boundary.owned_override?.host === '127.0.0.1' && boundary.owned_override.port === b.relay_port && boundary.endpoint_deviation?.default_url_coverage_claimed === false && boundary.endpoint_deviation.native_direct_provider_bypass_coverage === 'NOT_CLAIMED', 'audit_scope_widened');
    check(boundary.tls_trust_scope === 'EXPLICIT_BUNDLED_PUBLIC_ROOTS_WITHOUT_ENV_EXTRA_CA' && zeroEqual(boundary.tls_verification_policy, { reject_unauthorized: true, default_hostname_verification: true, explicit_ca_only: true, environment_extra_ca_accepted: false, system_trust_modified: false }) && zeroEqual(boundary.transport_policy, { redirects: 'NEVER_FOLLOW', automatic_request_retry: 'DISABLED', https_agent_reuse: false, pricing_scope: 'REFERENCE_USD_NOT_PROVIDER_INVOICE' }), 'raw_tls_or_transport_claim_changed');
    const selectedCatalog = [{ model: 'gpt-5.2', key: 'gpt-5.2', entry: d.catalog['gpt-5.2'] }];
    check(sha(JSON.stringify(selectedCatalog)) === o.catalog_sha256, 'selected_catalog_digest_mismatch');
    const db = t.safe_db;
    check(Array.isArray(db?.upstream_rows_including_deleted) && db.upstream_rows_including_deleted.length === 1 && db.upstream_rows_including_deleted[0].id === b.upstream_id && db.upstream_rows_including_deleted[0].kind === 'openai_api_key' && db.upstream_rows_including_deleted[0].base_url === `http://127.0.0.1:${b.relay_port}/` && db.upstream_rows_including_deleted[0].enabled === 1 && db.upstream_rows_including_deleted[0].warmup_enabled === 0 && db.upstream_rows_including_deleted[0].deleted_at === null, 'foreign_or_warm_upstream');
    check(db.counts?.upstream_oauth_token_v1 === 0 && db.counts.upstream_api_key_secret_v1 === 1 && ['cache_keepalive_sessions', 'cache_keepalive_turns', 'cache_keepalive_decisions', 'request_events_v1'].every((key) => db.counts[key] === 0), 'unaccounted_dispatch_source');
    const sentinel = '737562736372697074696f6e2d707265666572656e6365000000000000000000';
    check(['plugin_chains_v2', 'wasm_registry_v2', 'wasm_blobs_v2'].every((key) => db.counts[key] === 1) && db.plugin_safe_details?.length === 1 && db.router_metadata?.length === 1, 'unknown_plugin_inventory');
    const plugin = db.plugin_safe_details[0], router = db.router_metadata[0];
    check(plugin.id === '00000000-0000-0000-0000-000000000002' && Buffer.from(plugin.sha256, 'utf8').toString('hex') === sentinel && plugin.plugin_name === 'subscription-preference' && plugin.plugin_version === null && plugin.status === 'active' && plugin.size_bytes === 0 && plugin.blob_length_bytes === 0 && router.slot === 'router' && router.order_value === 0 && router.plugin_name === plugin.plugin_name && router.plugin_version === null && router.status === 'active', 'builtin_policy_mismatch');
    const scheduler = d.scheduler_receipt;
    check(scheduler.active_jobs === 0 && scheduler.jobs_by_queue_status?.length === 1 && scheduler.jobs_by_queue_status[0].job_type === 'cron' && scheduler.jobs_by_queue_status[0].status === 'Done' && scheduler.jobs_by_queue_status[0].n === 125, 'scheduler_dispatch_possible');
    check(scheduler.files?.length === 3 && scheduler.files.every((file, index) => file.path === path.join(b.epoch, `omp-gpt/runtime/storage.scheduler.sqlite${['', '-wal', '-shm'][index]}`)) && scheduler.readonly_sqlite_mode === true, 'scheduler_path_mismatch');
    check(['credentials_read', 'provider_requests', 'processes_started', 'signals_sent', 'financial_changes'].every((key) => scheduler[key] === 0), 'scheduler_audit_scope_changed');
    const taskCounts = { api_key_usage_poll: 5, oauth_usage_poll: 29, pool_quota_snapshot: 29, prompt_cache_purge: 2, upstream_affinity_purge: 2, usage_rollup: 58 };
    check(scheduler.task_kind_counts_from_nonpayload_idempotency_key?.length === 6 && scheduler.task_kind_counts_from_nonpayload_idempotency_key.every((row) => row.status === 'Done' && taskCounts[row.task_kind] === row.n) && new Set(scheduler.task_kind_counts_from_nonpayload_idempotency_key.map((row) => row.task_kind)).size === 6, 'scheduler_job_policy_unknown');
    check(t.status === 'EVIDENCE_PRESERVED_THREE_OWNED_PROCESSES_STOPPED_PORTS_FREE' && t.financial_post_unchanged === true && zeroEqual(t.reservation_unchanged, p) && t.targets?.length === 3 && t.signals?.length === 3 && t.after?.processes?.length === 3, 'terminal_writer_proof_missing');
    const pids = [b.sut_pid, b.controller_pid, b.collector_pid];
    for (const pid of pids) {
      const target = t.targets.find((row) => row.pid === pid), signal = t.signals.find((row) => row.pid === pid), after = t.after.processes.find((row) => row.pid === pid);
      check(target && signal && after && signal.signals_sent === 1 && signal.stop_verified === true && signal.status === 'DISAPPEARED_AFTER_SINGLE_SIGTERM' && signal.identity?.pid === pid && signal.identity.alive === true && signal.identity.start_utc === target.start && signal.identity.command === target.command && zeroEmpty(signal.identity.children) && after.alive === false && zeroEmpty(after.children) && Date.parse(signal.observed_at) > Date.parse(signal.signal_sent_at), 'writer_not_terminal');
    }
    check(t.targets.find((row) => row.pid === b.controller_pid).command === c.run.command.join(' ') && t.targets.find((row) => row.pid === b.sut_pid).command === `${d.selected_contract.artifact.path} serve --config ${a.runtime_config.path}`, 'stopped_command_mismatch');
    const sutResource = d.resources.find((row) => row.kind === 'process' && row.pid === b.sut_pid);
    check(Date.parse(p.at) >= Date.parse(c.run.started_at) && Date.parse(p.at) < Date.parse(sutResource.created_at) && Date.parse(p.finished_at) >= Date.parse(sutResource.created_at), 'reservation_creation_lifetime_mismatch');
    check(Date.parse(t.targets.find((row) => row.pid === b.controller_pid).start) === Math.floor(Date.parse(c.run.started_at) / 1000) * 1000
      && Date.parse(t.targets.find((row) => row.pid === b.sut_pid).start) === Math.floor(Date.parse(sutResource.created_at) / 1000) * 1000
      && c.collector_pre?.pid === b.collector_pid && t.targets.find((row) => row.pid === b.collector_pid).command === c.collector_pre.ps.trim().split(/\s+/).slice(2).join(' ')
      && Date.parse(t.targets.find((row) => row.pid === b.collector_pid).start) < Date.parse(c.created_at), 'process_start_or_reuse_mismatch');
    const ports = [18510 + offset, 18511 + offset, 18512 + offset, 18513 + offset, b.relay_port, 18563 + offset, 18564 + offset].map(String);
    check(zeroEqual(t.targets.find((row) => row.pid === b.sut_pid).ports, [18510 + offset, 18511 + offset, 18512 + offset])
      && zeroEqual(t.targets.find((row) => row.pid === b.controller_pid).ports, [b.relay_port])
      && zeroEqual(t.targets.find((row) => row.pid === b.collector_pid).ports, [18563 + offset, 18564 + offset]), 'terminal_resource_plan_mismatch');
    check(zeroEqual(Object.keys(t.after.ports).sort(), ports.sort()) && Object.values(t.after.ports).every(zeroEmpty), 'ports_not_terminal');
    check(x.audit_receipt?.path === a.terminal_receipt.path && x.audit_receipt.sha256 === a.terminal_receipt.sha256 && x.prior_financial_files_checked === Object.keys(d.preceding_guard.source_files).length && zeroEmpty(x.prior_financial_changed) && x.new_case_four_financial_files_unchanged === true && x.reservation_release_performed === false, 'terminal_appendix_mismatch');
    const immutableRoles = ['reservation_preimage', 'outbound', 'budget', 'summary', 'run_evidence', 'prepare', 'slice_ledger', 'resources', 'driver_summary', 'version', 'creation_receipt'];
    const immutable = immutableRoles.map((role) => ({ path: role === 'reservation_preimage' ? zeroTarget(b) : a[role].path, sha256: a[role].sha256 }));
    immutable.push(c.run.stdout, c.run.stderr);
    check(t.original_files_post?.length === 14 && new Set(t.original_files_post.map((row) => row.path)).size === 14, 'historical_files_inventory_mismatch');
    for (const descriptor of immutable) {
      const row = t.original_files_post.find((item) => item.path === descriptor.path);
      check(row?.sha256 === descriptor.sha256 && row.byte_unchanged === true, 'historical_files_changed');
    }
    const captureRow = t.original_files_post.find((row) => row.path === x.final_capture?.path);
    check(captureRow?.sha256 === x.final_capture.sha256 && typeof captureRow.byte_unchanged === 'boolean', 'final_capture_row_mismatch');
    check(c.evidence_file?.path === path.join(loaded.canonical_root, path.relative(BASE, a.run_evidence.path)) && c.evidence_file.sha256 === a.run_evidence.sha256, 'original_result_file_mismatch');
    const capture = d.final_capture;
    const allowedNames = new Set(['GET /admin/health', 'POST /admin/v1/upstreams', 'POST /admin/v1/principals', 'POST /admin/v1/principals/{id}/keys', 'GET /admin/v1/upstreams/{id}', 'GET /admin/v1/events/delta', 'scheduler.job']);
    check(capture.spans?.length === 132 && capture.spans.filter((span) => span.name === 'scheduler.job').length === 125 && capture.spans.every((span) => allowedNames.has(span.name)) && capture.stats?.acceptedSpans === 132 && capture.stats.rejectedSpans === 0 && capture.stats.decodeErrors === 0, 'capture_activity_unaccounted');
    if (record.reference_closure?.kind === 'PINNED_RUN_SEALED_ZERO') {
      const proof = record.reference_closure;
      check(zeroExact(proof, ['schema', 'kind', 'case', 'slice', 'slice_cap_micros', 'native_command_finished', 'terminal_reason', 'reference_bound_micros', 'observed_cost_micros', 'actual_invoice_protection_claimed', 'source_scope', 'predecessor_sha256', 'binding_sha256', 'proof_inputs', 'audit_scope']), 'closure_shape_invalid');
      check(proof.schema === 'pinned-run-sealed-zero-reference-closure-v1' && proof.terminal_reason === 'NO_POSSIBLE_PROVIDER_DISPATCH' && proof.native_command_finished === false && proof.observed_cost_micros === null && proof.actual_invoice_protection_claimed === false && proof.reference_bound_micros === 0 && proof.case === b.case && proof.slice === b.slice && proof.slice_cap_micros === b.slice_cap_micros && proof.predecessor_sha256 === a.reservation_preimage.sha256 && proof.binding_sha256 === zeroBindingId(b) && zeroEqual(proof.proof_inputs, b), 'closure_binding_invalid');
      check(proof.source_scope === 'audited_original_run_and_terminal_writer_lifecycle' && zeroEqual(proof.audit_scope, zeroAuditScope()), 'closure_audit_scope_invalid');
      const expected = { ...p, state: 'SETTLED', charge_micros: 0, charge_usd: 0, unresolved: false, reference_closure: proof };
      check(zeroEqual(record, expected), 'successor_changed_original_metadata');
    } else check(zeroEqual(record, p), 'record_not_exact_preimage');
    return { valid: true, reason: null, reference_bound_micros: 0 };
  } catch (error) { return { valid: false, reason: error.message?.startsWith('BLOCKED:') ? error.message.slice(8) : 'pinned_zero_evidence_incomplete' }; }
}
function zeroAuditScope() {
  return {
    policy_id: ZERO_POLICY.id, trust_model: 'TRUSTED_PARENT_HARNESS_AND_COMPILED_SUT',
    observed_evidence_scope: ['PINNED_ORIGINAL_RUN_TUPLE', 'SEALED_ZERO_BOUNDARY_AND_GUARD_SNAPSHOTS', 'EXACT_UPSTREAM_AND_BUILTIN_DB_METADATA', 'CLOSED_SCHEDULER_DB_PLUS_WAL_METADATA', 'THREE_OWNED_WRITERS_TERMINATED_AND_PORTS_FREE'],
    source_model_policy: { financial_creator_sha256: ZERO_POLICY.creator, compiled_commit: ZERO_POLICY.commit, compiled_binary_sha256: ZERO_POLICY.binary, source_roles_sha256: ZERO_POLICY.epoch_roles, routing: 'EXPLICIT_SOME_BASE_URL_PRECEDES_DEFAULT', bootstrap: 'SKIP_PREFLIGHT_AND_NO_WARMUP_SOURCE_DERIVED', builtin: 'UUID_OR_SENTINEL_NORMALIZES_TO_COMPILED_RUST_FILTER_NO_WASMTIME', scheduler: 'CASE_DERIVED_PATH_CRON_ONLY_RESTRICTED_UPSTREAM_LOOKUPS' },
    financial_creator: 'ORIGINAL_API_SOURCE50_PIN', external_launcher_original_hash_attestation: 'NOT_CLAIMED',
    scheduler_preabsence: 'NOT_CLAIMED', builtin_version_and_capabilities: 'SOURCE_DERIVED_NOT_ADMIN_OBSERVED',
    historical_syscall_coverage: 'NOT_CLAIMED', provider_invoice_or_usage_zero: 'NOT_CLAIMED', runtime_completion: 'INCOMPLETE_UNCHANGED',
  };
}
function zeroReferenceProof(loaded) {
  const b = loaded.binding;
  return {
    schema: 'pinned-run-sealed-zero-reference-closure-v1', kind: 'PINNED_RUN_SEALED_ZERO',
    case: b.case, slice: b.slice, slice_cap_micros: b.slice_cap_micros, native_command_finished: false,
    terminal_reason: 'NO_POSSIBLE_PROVIDER_DISPATCH', reference_bound_micros: 0,
    observed_cost_micros: null, actual_invoice_protection_claimed: false,
    source_scope: 'audited_original_run_and_terminal_writer_lifecycle',
    predecessor_sha256: b.artifacts.reservation_preimage.sha256, binding_sha256: zeroBindingId(b),
    proof_inputs: b, audit_scope: zeroAuditScope(),
  };
}
async function zeroReaderEvidence(record, target) {
  if (record?.reference_closure?.kind !== 'PINNED_RUN_SEALED_ZERO') return null;
  try {
    const input = record.reference_closure.proof_inputs;
    if (zeroTarget(input) !== target) return null;
    return await loadPinnedRunSealedZeroEvidence(input);
  } catch { return null; } // Invalid/missing proof retains the creation-time cap.
}

// Explicit operator only. Never called by normal run/plan, and no new CLI mode.
// Main must separately authorize the exact target/preimage after source review.
export async function applyPinnedRunSealedZeroClosure(actualBinding, { reservationPath = zeroTarget(actualBinding), expectedPreimageSha256, expectedSnapshot, ledgerPath = LEDGER } = {}) {
  actualBinding = structuredClone(actualBinding);
  must(reservationPath === zeroTarget(actualBinding) && expectedPreimageSha256 === actualBinding.artifacts?.reservation_preimage?.sha256, 'pinned_zero_operator_target_mismatch');
  must([zeroTarget(actualBinding), zeroBackup(actualBinding)].includes(actualBinding.artifacts.reservation_preimage.path), 'pinned_zero_operator_predecessor_alias');
  await zeroRegularPath(reservationPath, BASE);
  const id = zeroBindingId(actualBinding);
  const currentBytes = await readFile(reservationPath), current = JSON.parse(currentBytes);
  if (current.reference_closure?.kind === 'PINNED_RUN_SEALED_ZERO') {
    must(current.reference_closure.binding_sha256 === id, 'pinned_zero_conflicting_proof');
    const loaded = await zeroReaderEvidence(current, reservationPath);
    must(loaded && validatedReferenceClosure(current, loaded.documents.outbound, current.slice, loaded), 'pinned_zero_existing_closure_invalid');
    return { applied: false, idempotent: true, released_micros: 0, record: current };
  }
  must(sha(currentBytes) === expectedPreimageSha256, 'pinned_zero_operator_preimage_changed');
  must(zeroExact(expectedSnapshot, Object.keys(expectedSnapshot ?? {})), 'pinned_zero_expected_snapshot_required');
  const beforeSnapshot = await rootFinancialSnapshot();
  must(zeroEqual(beforeSnapshot, expectedSnapshot), 'pinned_zero_financial_snapshot_changed');
  const before = await rootFinancialMinimum(beforeSnapshot);
  const loaded = await loadPinnedRunSealedZeroEvidence(actualBinding);
  must(zeroEqual(current, loaded.documents.reservation_preimage), 'pinned_zero_operator_record_mismatch');
  const directory = path.join(actualBinding.epoch, 'evidence');
  const backupPath = zeroBackup(actualBinding), proofPath = path.join(directory, `approved-core3-pinned-run-zero-proof-${id}.json`);
  // One exclusive intent per canonical reservation, not per proposed proof.
  const lockPath = path.join(directory, `approved-core3-pinned-run-zero-intent-${sha(reservationPath)}.json`);
  const stagingPath = path.join(directory, `approved-core3-pinned-run-zero-successor-${id}.json`);
  await recordHarnessResource(ledgerPath, 'pinned-run-sealed-zero-operator', { kind: 'financial_closure_audit_artifacts', id, paths: [backupPath, proofPath, lockPath, stagingPath], target: reservationPath, delete: 'Retain original/successor audit evidence. Never delete the canonical financial target; any audit-artifact cleanup needs explicit user approval.' });
  await mkdir(directory, { recursive: true, mode: 0o700 });
  // An interrupted attempt leaves the original full hold and an exclusive
  // intent, never a silently resumed or concurrently rewritten successor.
  await writeFile(lockPath, JSON.stringify({ schema: 'pinned-run-zero-intent-v1', target: reservationPath, preimage_sha256: expectedPreimageSha256, binding_sha256: id, pid: process.pid }, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  const exclusive = async (file, bytes) => {
    try { await writeFile(file, bytes, { flag: 'wx', mode: 0o600 }); }
    catch (error) {
      if (error.code !== 'EEXIST') throw error;
      await zeroRegularPath(file, BASE);
      must(((await lstat(file)).mode & 0o777) === 0o600 && sha(await readFile(file)) === sha(bytes), 'pinned_zero_preserved_artifact_conflict');
    }
  };
  await exclusive(backupPath, currentBytes);
  const persistedBinding = structuredClone(actualBinding);
  persistedBinding.artifacts.reservation_preimage.path = backupPath;
  const preserved = await loadPinnedRunSealedZeroEvidence(persistedBinding);
  const reconciled = reconcileReferenceReservation({ record: current, nativeCommandFinished: false, observedCostMicros: null, pinnedRunZero: preserved });
  must(reconciled.reconciled, reconciled.reason ?? 'pinned_zero_successor_invalid');
  const proofBytes = Buffer.from(JSON.stringify(reconciled.proof, null, 2) + '\n');
  await exclusive(proofPath, proofBytes);
  await exclusive(stagingPath, Buffer.from(JSON.stringify(reconciled.record, null, 2) + '\n'));
  must(sha(await readFile(reservationPath)) === expectedPreimageSha256 && zeroEqual(await rootFinancialSnapshot(), beforeSnapshot), 'pinned_zero_compare_and_swap_changed');
  // Re-read all proof dependencies before the single atomic financial change.
  for (const [file, digest] of Object.entries(preserved.dependencies)) {
    await zeroRegularPath(file, file.startsWith(`${ZERO_POLICY.compiled_root}/`) ? ZERO_POLICY.compiled_root : BASE);
    const hash = createHash('sha256');
    for await (const chunk of createReadStream(file)) hash.update(chunk);
    must(hash.digest('hex') === digest, 'pinned_zero_dependency_changed');
  }
  must(sha(await readFile(reservationPath)) === expectedPreimageSha256 && zeroEqual(await rootFinancialSnapshot(), beforeSnapshot), 'pinned_zero_compare_and_swap_changed');
  await rename(stagingPath, reservationPath);
  const afterSnapshot = await rootFinancialSnapshot();
  const targetKey = path.relative(BASE, reservationPath);
  must(zeroEqual(Object.keys(afterSnapshot).sort(), Object.keys(beforeSnapshot).sort()) && Object.keys(beforeSnapshot).every((file) => file === targetKey || afterSnapshot[file] === beforeSnapshot[file]), 'pinned_zero_unrelated_finance_changed');
  const after = await rootFinancialMinimum(afterSnapshot);
  must(before.minimumMicros === after.minimumMicros + actualBinding.slice_cap_micros, 'pinned_zero_release_not_exact_cap');
  must(before.rows.every((row) => row.slice === path.relative(BASE, path.join(actualBinding.epoch, actualBinding.slice)) || zeroEqual(row, after.rows.find((post) => post.slice === row.slice))), 'pinned_zero_unrelated_hold_changed');
  return { applied: true, idempotent: false, released_micros: actualBinding.slice_cap_micros, before_micros: before.minimumMicros, after_micros: after.minimumMicros, preimage: { path: backupPath, sha256: expectedPreimageSha256 }, proof: { path: proofPath, sha256: sha(proofBytes) }, record: reconciled.record };
}

// Shared by the writer and root reader. A later settlement may reduce live holds,
// but a new reservation, changed original bound or increased hold invalidates it.
function validatedReferenceClosure(record, outbound, slice = record?.slice, pinnedRunZero = null) {
  const proof = record?.reference_closure;
  if (proof?.kind === 'PINNED_RUN_SEALED_ZERO') {
    return ZERO_LOADED.has(pinnedRunZero) && slice === record.slice && zeroEqual(outbound, pinnedRunZero.documents.outbound)
      && validatePinnedRunSealedZeroEvidence(record, pinnedRunZero).valid ? proof : null;
  }
  const current = referenceGuardEvidence(record, outbound);
  if (!proof || !current || slice !== record.slice ||
      proof.schema !== 'provider-boundary-reference-closure-v1' ||
      proof.source_scope !== 'actual_provider_boundary' ||
      proof.case !== record.case || proof.slice !== record.slice ||
      proof.slice_cap_micros !== current.cap || proof.native_command_finished !== true ||
      proof.future_generation_sealed !== true || proof.guard_seal_persisted !== true ||
      proof.counter_consistent !== true || proof.actual_invoice_protection_claimed !== false ||
      proof.guard_attempts !== outbound.attempts ||
      !referenceInteger(proof.actual_generation_write_attempts) ||
      proof.actual_generation_write_attempts > proof.guard_attempts ||
      proof.reservation_bound_sum_micros !== current.boundSum ||
      !referenceInteger(proof.held_at_seal_micros) || current.held > proof.held_at_seal_micros ||
      !referenceInteger(proof.reference_bound_micros) || proof.reference_bound_micros > current.cap ||
      proof.held_at_seal_micros > proof.reference_bound_micros ||
      !Array.isArray(proof.reservation_ids_sha256) ||
      JSON.stringify(proof.reservation_ids_sha256) !== JSON.stringify(current.ids) ||
      !Array.isArray(proof.reservation_bounds) || proof.reservation_bounds.length !== current.ids.length ||
      !(proof.observed_cost_micros === null || referenceInteger(proof.observed_cost_micros)) ||
      proof.reference_bound_micros !== Math.max(proof.observed_cost_micros ?? 0, proof.held_at_seal_micros) ||
      record.charge_micros !== proof.reference_bound_micros ||
      record.charge_usd !== record.charge_micros / 1000000 ||
      !['SETTLED', 'RECONCILED_BOUNDED_UNKNOWN'].includes(record.state) ||
      (record.state === 'SETTLED' && (current.unresolved || proof.observed_cost_micros === null)) ||
      ((current.unresolved || proof.observed_cost_micros === null) && record.unresolved !== true)) return null;
  let writeAttempts = 0;
  for (let index = 0; index < current.ids.length; index++) {
    const row = proof.reservation_bounds[index];
    const live = current.records.get(current.ids[index]);
    if (!row || row.id_sha256 !== current.ids[index] ||
        row.reserved_bound_micros !== live.reserved_bound_micros ||
        ![0, 1].includes(row.actual_generation_write_attempts) ||
        (row.actual_generation_write_attempts === 0 && live.status === 'SETTLED')) return null;
    writeAttempts += row.actual_generation_write_attempts;
  }
  return writeAttempts === proof.actual_generation_write_attempts ? proof : null;
}

function safeReferenceBoundaryMetadata(value) {
  const result = {
    schema: ['provider-boundary-relay-v1', 'api-key-control-boundary-v1'].includes(value?.schema) ? value.schema : null,
    synthetic_fixture_only: value?.synthetic_fixture_only === true,
  };
  for (const key of ['sealed', 'future_generation_admission_denied', 'sealed_guard_identity_unchanged', 'guard_sealed', 'guard_seal_persisted', 'sealing_complete']) result[key] = typeof value?.[key] === 'boolean' ? value[key] : null;
  const safeNumber = (number) => referenceInteger(number) ? number : null;
  result.counters = { actualgeneration_writeattempts: safeNumber(value?.counters?.actualgeneration_writeattempts) };
  result.reservation_attempt_matches = Array.isArray(value?.reservation_attempt_matches) ? value.reservation_attempt_matches.map((row) => ({
    reservation_id_sha256: referenceHash(row?.reservation_id_sha256) ? row.reservation_id_sha256 : null,
    reserved_bound_micros: safeNumber(row?.reserved_bound_micros),
    state: REFERENCE_STATUSES.has(row?.state) ? row.state : null,
    actualgeneration_writeattempts: safeNumber(row?.actualgeneration_writeattempts),
    unique_actual_write_match: typeof row?.unique_actual_write_match === 'boolean' ? row.unique_actual_write_match : null,
    unattempted_full_hold: typeof row?.unattempted_full_hold === 'boolean' ? row.unattempted_full_hold : null,
  })) : null;
  const join = value?.generation_attempt_reservation_join;
  result.generation_attempt_reservation_join = {};
  for (const key of ['generation_attempts', 'reservation_records', 'successful_reservation_bound_sum_micros', 'unmatched_generation_attempts', 'duplicate_reservation_write_matches']) result.generation_attempt_reservation_join[key] = safeNumber(join?.[key]);
  for (const key of ['generation_counter_matches_attempt_records', 'all_reservation_bounds_known', 'all_successful_reservations_have_one_write_attempt']) result.generation_attempt_reservation_join[key] = typeof join?.[key] === 'boolean' ? join[key] : null;
  return result;
}

// Synchronous financial closure only: no artifact writes, invoice claims, or
// native PASS inference. Unfinished commands retain full holds unless the
// distinct offline PINNED_RUN_SEALED_ZERO source/lifecycle proof is complete.
export function reconcileReferenceReservation({ record, guard, providerBoundaries, nativeCommandFinished, observedCostMicros, pinnedRunZero = null } = {}) {
  const metadata = [];
  let outbound = null;
  const cap = record?.slice_cap_micros ?? 3000000;
  const currentRecordMicros = referenceInteger(record?.charge_micros) ? record.charge_micros
    : typeof record?.charge_usd === 'number' && Number.isFinite(record.charge_usd) && record.charge_usd >= 0 && referenceInteger(Math.ceil(record.charge_usd * 1000000)) ? Math.ceil(record.charge_usd * 1000000) : 0;
  const failure = (reason) => {
    try { const current = guard?.snapshot?.(); if (current && typeof current.then !== 'function') outbound = current; } catch { /* Keep the last available conservative evidence. */ }
    const charge = Math.max(3000000, referenceInteger(cap) ? cap : 0, currentRecordMicros,
      referenceInteger(observedCostMicros) ? observedCostMicros : 0,
      referenceInteger(outbound?.total_held_micros) ? outbound.total_held_micros : 0,
      referenceInteger(outbound?.spent_micros) ? outbound.spent_micros : 0);
    const next = { ...record, state: 'RECONCILIATION_REQUIRED', charge_micros: charge, charge_usd: charge / 1000000, unresolved: true, reference_closure: null };
    return { record: next, proof: null, metadata, reconciled: false, reason };
  };
  try {
    if (pinnedRunZero !== null) {
      if (nativeCommandFinished !== false || observedCostMicros !== null || !ZERO_LOADED.has(pinnedRunZero)) return failure('pinned_zero_loaded_evidence_required');
      outbound = pinnedRunZero.documents.outbound;
      const valid = validatePinnedRunSealedZeroEvidence(record, pinnedRunZero);
      if (!valid.valid) return failure(valid.reason);
      const proof = record.reference_closure?.kind === 'PINNED_RUN_SEALED_ZERO' ? record.reference_closure : zeroReferenceProof(pinnedRunZero);
      const next = { ...record, state: 'SETTLED', charge_micros: 0, charge_usd: 0, unresolved: false, reference_closure: proof };
      if (!validatedReferenceClosure(next, outbound, record.slice, pinnedRunZero)) return failure('pinned_zero_successor_invalid');
      return { record: next, proof, metadata, reconciled: true, reason: 'NO_POSSIBLE_PROVIDER_DISPATCH' };
    }
    if (typeof guard?.snapshot !== 'function') return failure('reference_guard_snapshot_required');
    outbound = guard.snapshot();
    if (!outbound || typeof outbound.then === 'function') return failure('reference_guard_snapshot_required');
    if (nativeCommandFinished !== true) return failure('native_command_not_finished');
    if (!(observedCostMicros === null || referenceInteger(observedCostMicros))) return failure('observed_reference_cost_invalid');
    if (![3000000, 6000000].includes(cap)) return failure('recorded_reference_cap_invalid');
    if (!Array.isArray(providerBoundaries) || providerBoundaries.length === 0 ||
        new Set(providerBoundaries).size !== providerBoundaries.length) return failure('actual_provider_boundaries_required');
    for (const boundary of providerBoundaries) {
      if (!(isProviderBoundaryRelay(boundary) || isApiKeyControlBoundary(boundary)) ||
          typeof boundary.guardIdentity !== 'function' || boundary.guardIdentity() !== guard ||
          typeof boundary.sealGenerativeForwards !== 'function' || typeof boundary.metadata !== 'function') return failure('actual_provider_guard_identity_required');
    }
    for (const boundary of providerBoundaries) {
      const sealed = boundary.sealGenerativeForwards();
      if (!sealed || typeof sealed.then === 'function' || sealed.ok !== true ||
          sealed.sealing_complete !== true || boundary.guardIdentity() !== guard) return failure('provider_boundary_seal_unconfirmed');
    }
    if (typeof guard.seal !== 'function') return failure('reference_guard_seal_required');
    const seal = guard.seal();
    if (!seal || typeof seal.then === 'function' || seal.ok !== true ||
        seal.sealed !== true || seal.seal_persisted !== true) return failure('reference_guard_seal_unconfirmed');
    for (const boundary of providerBoundaries) {
      if (boundary.guardIdentity() !== guard) return failure('actual_provider_guard_identity_required');
      const value = boundary.metadata();
      if (!value || typeof value.then === 'function') return failure('provider_boundary_metadata_invalid');
      metadata.push(safeReferenceBoundaryMetadata(value));
      if ((isProviderBoundaryRelay(boundary) && value.schema !== 'provider-boundary-relay-v1') ||
          (isApiKeyControlBoundary(boundary) && value.schema !== 'api-key-control-boundary-v1') ||
          value.synthetic_fixture_only === true ||
          !['sealed', 'future_generation_admission_denied', 'sealed_guard_identity_unchanged', 'guard_sealed', 'guard_seal_persisted', 'sealing_complete'].every((key) => value[key] === true)) return failure('actual_provider_boundary_seal_evidence_invalid');
    }
    if (!providerBoundaries.every((boundary) => boundary.guardIdentity() === guard)) return failure('actual_provider_guard_identity_required');
    outbound = guard.snapshot();
    const current = referenceGuardEvidence(record, outbound);
    if (!current) return failure('reference_guard_evidence_invalid');
    const matched = new Map();
    let actualWrites = 0;
    for (const value of metadata) {
      const rows = value.reservation_attempt_matches;
      const join = value.generation_attempt_reservation_join;
      const writes = value.counters.actualgeneration_writeattempts;
      if (!Array.isArray(rows) || !referenceInteger(writes) ||
          join.generation_attempts !== writes || join.generation_counter_matches_attempt_records !== true ||
          join.reservation_records !== rows.length || join.all_reservation_bounds_known !== true ||
          join.unmatched_generation_attempts !== 0 || join.duplicate_reservation_write_matches !== 0 ||
          join.all_successful_reservations_have_one_write_attempt !== rows.every((row) => row.actualgeneration_writeattempts === 1)) return failure('provider_generation_counter_join_invalid');
      let rowWrites = 0, boundSum = 0;
      for (const row of rows) {
        const live = current.records.get(row.reservation_id_sha256);
        if (!live || matched.has(row.reservation_id_sha256) ||
            row.reserved_bound_micros !== live.reserved_bound_micros || row.state !== live.status ||
            ![0, 1].includes(row.actualgeneration_writeattempts) ||
            row.unique_actual_write_match !== (row.actualgeneration_writeattempts === 1) ||
            row.unattempted_full_hold !== (row.actualgeneration_writeattempts === 0 && row.state !== 'SETTLED') ||
            (row.actualgeneration_writeattempts === 0 && !['IN_FLIGHT', 'UNKNOWN'].includes(live.status))) return failure('provider_reservation_identity_join_invalid');
        matched.set(row.reservation_id_sha256, row);
        rowWrites += row.actualgeneration_writeattempts;
        boundSum += row.reserved_bound_micros;
      }
      if (!referenceInteger(boundSum) || boundSum !== join.successful_reservation_bound_sum_micros ||
          rowWrites !== writes) return failure('provider_reservation_counter_sum_invalid');
      actualWrites += writes;
    }
    if (matched.size !== current.records.size || !referenceInteger(actualWrites)) return failure('provider_guard_reservations_not_exhaustive');
    const charge = Math.max(observedCostMicros ?? 0, current.held);
    if (charge > cap) return failure('reference_charge_exceeds_recorded_cap');
    const proof = {
      schema: 'provider-boundary-reference-closure-v1', source_scope: 'actual_provider_boundary',
      case: record.case, slice: record.slice, slice_cap_micros: cap,
      native_command_finished: true, future_generation_sealed: true, guard_seal_persisted: true,
      counter_consistent: true, guard_attempts: outbound.attempts, actual_generation_write_attempts: actualWrites,
      reservation_ids_sha256: current.ids, reservation_bound_sum_micros: current.boundSum,
      reservation_bounds: current.ids.map((id) => ({ id_sha256: id, reserved_bound_micros: current.records.get(id).reserved_bound_micros, actual_generation_write_attempts: matched.get(id).actualgeneration_writeattempts })),
      held_at_seal_micros: current.held, reference_bound_micros: charge,
      observed_cost_micros: observedCostMicros, actual_invoice_protection_claimed: false,
    };
    const unresolved = current.unresolved || observedCostMicros === null;
    const next = { ...record, charge_micros: charge, charge_usd: charge / 1000000,
      state: unresolved ? 'RECONCILED_BOUNDED_UNKNOWN' : 'SETTLED', unresolved, reference_closure: proof };
    if (!validatedReferenceClosure(next, outbound)) return failure('stored_reference_closure_invalid');
    return { record: next, proof, metadata, reconciled: true, reason: null };
  } catch {
    try { const current = guard?.snapshot?.(); if (current && typeof current.then !== 'function') outbound = current; } catch { /* Retain the last known safe full hold. */ }
    return failure('reference_closure_unconfirmed');
  }
}

export async function nativeSubscriptionPlan(config) {
  const spec = NATIVE_SUBSCRIPTION_CASES[config.case];
  must(spec, 'original_native_case_required');
  must(config.kind === spec.kind, 'native_case_actual_oauth_kind_required');
  const base = subscriptionRuntimePlan(config);
  const manifest = await json(path.join(base.epoch, 'fixed-epoch.json'));
  must(manifest?.binary?.expected_sha256 === config.expectedSha, 'new_binary_manifest_required');
  const offset = manifest.port_offset;
  must([500, 600, 700, 800, 900, 1000].includes(offset), 'new_epoch_port_offset_required');
  const first = spec.ports + offset;
  const root = path.join(base.epoch, spec.slice);
  const modelSelection = await modelPlan(config, spec);
  return { ...base, case: config.case, name: spec.slice, root, native: spec.native,
    ports: [first, first + 1, first + 2], observer_port: first + 50, provider_boundary_port: first + 120,
    native_source: path.join(base.epoch, spec.native, 'run.mjs'),
    required_original_cases: Object.keys(NATIVE_SUBSCRIPTION_CASES),
    models: modelSelection.models, native_model_evidence: modelSelection.nativeModelEvidence,
    current_native_profile: modelSelection.profile,
    caps: { slice_usd: selectedReferenceSliceUsd(config), campaign_usd: 30, slice_forwards: 16, case_forwards: spec.original_case_forward_cap },
    original_scenarios_retained: true, raw_http_generation_substitute: false,
    ledger: LEDGER, source_status: BROKER_SOURCE_CONTRACT.current_blockers,
    commands: {
      source_preflight: `bun ${BASE}/approved-core3-subscription-native-v30.mjs --source-preflight --config <nonsecret-parent-config.json>`,
      source_getaccess_import_native_settlement_one_memory_process: `bun ${BASE}/approved-core3-subscription-native-v30.mjs --run --config <nonsecret-parent-config.json>`,
      evidence: `${root}/subscription-evidence.json`,
    },
  };
}

async function pinnedInputs(plan, config) {
  must(!Object.hasOwn(config, 'artifact'), 'subscription_artifact_config_override_forbidden');
  const manifest = await json(path.join(plan.epoch, 'fixed-epoch.json'));
  const fixturePins = Object.keys(manifest.files ?? {}).filter((name) => name.startsWith(`${plan.native}/fixture/`) || name === `${plan.native}/hidden-check.mjs`);
  must(fixturePins.length > 0, 'original_native_fixture_pins_missing');
  const names = ['qa-runtime.mjs', `${plan.native}/run.mjs`, 'contract.json', ...fixturePins];
  const sources = {};
  for (const name of names) {
    const text = await readFile(path.join(plan.epoch, name), 'utf8');
    must(sha(text) === manifest.files?.[name]?.sha256, `epoch_source_hash_mismatch:${name}`);
    sources[name] = text;
  }
  // The cap30 manifest is trusted only through the parent's external raw SHA.
  const revisionContractText = await readFile(CAP30_CONTRACT, 'utf8');
  const revisionManifestBytes = await readFile(CAP30_MANIFEST);
  must(/^[0-9a-f]{64}$/.test(config.authorization?.revision_manifest_sha256 ?? '') && sha(revisionManifestBytes) === config.authorization.revision_manifest_sha256, 'cap30_trusted_external_manifest_sha_required');
  const revisionManifest = JSON.parse(revisionManifestBytes);
  must(!Object.hasOwn(revisionManifest.source_sha256 ?? {}, 'approved-core3-cap30-manifest.json') && revisionManifest.source_sha256?.['approved-core3-cap30-contract.json'] === sha(revisionContractText), 'cap30_contract_manifest_hash_mismatch');
  const contract = JSON.parse(revisionContractText);
  validateRevisionExecutionRelease(contract, revisionManifest);
  must(typeof contract.artifact?.path === 'string' && path.isAbsolute(contract.artifact.path) && contract.artifact.expected_sha256 === config.expectedSha, 'current_new_binary_artifact_invalid');
  const artifact = Object.freeze({ path: contract.artifact.path, expected_sha256: contract.artifact.expected_sha256 });
  must(manifest.binary?.path === artifact.path && manifest.binary.expected_sha256 === artifact.expected_sha256, 'current_new_binary_manifest_pair_mismatch');
  must(sha(await readFile(artifact.path)) === artifact.expected_sha256, 'current_new_binary_sha_mismatch');
  must(contract.paid_bounds?.generation_attempts_per_slice === 16 && [3, 6].includes(contract.paid_bounds.usd_ceiling_per_slice) && contract.paid_bounds.usd_ceiling_campaign === 30, 'pinned_source_reference_bounds_unsupported');
  // These are scratch executable artifacts, not a new repository convention. Parent pins their
  // exact reviewed bytes in its nonsecret handoff before any source value is retrieved.
  const runnerPins = config.authorization?.runner_sha256;
  for (const name of ['approved-core3-subscription-contract-v30.mjs', 'approved-core3-subscription-source.mjs', 'approved-core3-subscription-native-v30.mjs', 'approved-core3-subscription-current-native.mjs', 'approved-core3-subscription-budget-v30.mjs', 'approved-core3-provider-boundary.mjs', 'approved-core3-api-key-controls-v30.mjs']) {
    must(runnerPins?.[name] === sha(await readFile(path.join(BASE, name))), `reviewed_subscription_runner_sha_required:${name}`);
  }
  if (plan.native === 'codex') {
    must(!Object.hasOwn(config, 'model') && process.env.CODEX_QA_MODEL === undefined, 'codex_current_native_model_override_forbidden');
    const eligibility = config.pricingEligibility?.[plan.models[0]];
    must(eligibility && eligibility.protocol === 'openai-responses', 'codex_current_native_pricing_contract_unproven');
    const observedCli = plan.native_model_evidence.cli;
    let launchCli = null;
    for (const directory of (process.env.PATH ?? '').split(path.delimiter)) {
      if (!directory) continue;
      try {
        const candidate = path.join(directory, 'codex');
        const stat = await lstat(candidate);
        if ((stat.mode & 0o111) === 0) continue;
        launchCli = await realpath(candidate);
        break;
      } catch { /* Only inspect the next PATH candidate; no launcher is executed. */ }
    }
    must(launchCli === observedCli.path && sha(await readFile(launchCli)) === observedCli.sha256, 'codex_current_native_cli_identity_mismatch');
  }
  must(config.authorization.parent_new_binary_green === true && config.authorization.application_access_only_qa_passed === true, 'parent_new_binary_and_access_only_green_required');
  must(config.authorization.serial_campaign_execution === true, 'parent_serial_campaign_execution_required');
  must(config.authorization.notified_root === plan.root && JSON.stringify(config.authorization.notified_ports) === JSON.stringify(plan.ports) && config.authorization.notified_native_port === plan.observer_port, 'exact_native_resources_not_notified');
  must(config.authorization.notified_provider_boundary_port === plan.provider_boundary_port && config.authorization.allow_owned_provider_boundary === true, 'exact_provider_boundary_resources_not_notified');
  must(config.authorization.allow_native_subscription_generation === true, 'parent_native_paid_generation_authorization_required');
  must(config.authorization.codex_default_url_qa_green === true || plan.kind !== 'openai_oauth', 'codex_default_url_red_gate_pending');
  if (['codex-ws', 'codex-ws-reconnect'].includes(plan.case)) must(config.otlpEndpoint === `http://127.0.0.1:${18563 + manifest.port_offset}` && config.capture === path.join(plan.epoch, 'approved-core3-spans.json') && config.authorization.parent_collector_ledgered === true && existsSync(config.capture), 'existing_trusted_epoch_collector_required_for_ws_turn_identity');
  must(!existsSync(plan.root), 'fresh_subscription_slice_only_existing_evidence_preserved');
  return { manifest, sources, contract, artifact };
}

// Conservative campaign accounting reuses each epoch slice's original budget/driver budget.
// Unfinished commands retain their recorded creation-time cap; legacy records stay $3.
const RESERVATION_FILE = /^approved-core3-(subscription|api-key)-(.+)-reservation\.json$/;

function reservationSlice(file, value) {
  const match = RESERVATION_FILE.exec(path.basename(file));
  must(match && value && typeof value.slice === 'string' && /^[a-z0-9-]+$/.test(value.slice) && match[2] === value.slice, 'reservation_filename_and_slice_disagree');
  if (value.case !== undefined) must(typeof value.case === 'string' && value.case.startsWith('api-key-') === (match[1] === 'api-key'), 'reservation_case_namespace_disagrees');
  return value.slice;
}

export async function subscriptionCampaignBudget(epoch) {
  let totalMicros = 0;
  let priorFound = false;
  const slices = [];
  const reservations = new Map();
  for (const name of (await readdir(epoch)).filter((name) => RESERVATION_FILE.test(name))) {
    const record = await json(path.join(epoch, name));
    const slice = reservationSlice(name, record);
    must(!reservations.has(slice), 'duplicate_reservation_namespaces_for_same_slice');
    reservations.set(slice, record);
  }
  for (const entry of await readdir(epoch, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const root = path.join(epoch, entry.name);
    const b = await json(path.join(root, 'budget.json'));
    const d = await json(path.join(root, 'driver-budget.json'));
    const s = await json(path.join(root, 'summary.json'));
    const outbound = await json(path.join(root, 'outbound-budget.json'));
    const r = reservations.get(entry.name);
    if (!b && !d && !s && !outbound && !r) continue;
    const legacyEarlyStop = Math.max(Number(b?.costUsd) || 0, Number(d?.usd) || 0);
    const cancellationReserve = Number(s?.out_of_band_provider_cost?.conservative_upper_bound_usd) || 0;
    const capMicros = recordedSliceCapMicros({ reservation: r, outbound, summary: s, budget: b, driver: d });
    const unknown = b?.costKnown === false && Number(b?.attempts) > 0;
    const closure = validatedReferenceClosure(r, outbound, entry.name, await zeroReaderEvidence(r, path.join(epoch, `approved-core3-api-key-${entry.name}-reservation.json`)));
    const closedBeforeRuntime = r?.state === 'CLOSED_BEFORE_RUNTIME_CREATION' && r.runtime_creation_attempted === false && r.charge_micros === 0 && r.charge_usd === 0 && !b && !d && !s && !outbound;
    const reservationMicros = r?.charge_micros !== undefined ? r.charge_micros : Math.ceil((Number(r?.charge_usd) || 0) * 1e6);
    must(referenceInteger(reservationMicros) && (!r || r.charge_micros === undefined || r.charge_usd === reservationMicros / 1e6), 'campaign_typed_charge_invalid');
    const heldMicros = outbound?.total_held_micros ?? 0;
    must(referenceInteger(heldMicros), 'campaign_outbound_hold_invalid');
    must(!unknown || closure || reservationMicros >= capMicros, `unreconciled_campaign_cost:${entry.name}`);
    const chargeMicros = closure
      ? Math.max(Math.ceil((Number(b?.costUsd) || 0) * 1e6), heldMicros, reservationMicros)
      : Math.max(Math.ceil((legacyEarlyStop + cancellationReserve) * 1e6), heldMicros, reservationMicros, r && capMicros === 6000000 && !closedBeforeRuntime ? capMicros : 0);
    must(referenceInteger(chargeMicros) && (entry.name === 'prior-campaign-baseline' || chargeMicros <= capMicros), 'campaign_slice_ceiling_invalid');
    slices.push({ slice: entry.name, charge_usd: chargeMicros / 1e6, charge_micros: chargeMicros, slice_cap_micros: capMicros, cost_unknown: unknown || outbound?.unresolved === true, reference_closure_valid: Boolean(closure), unsettled: (unknown && !closure) || r?.state === 'ACTIVE' });
    if (entry.name === 'prior-campaign-baseline') priorFound = true;
    totalMicros += chargeMicros;
    reservations.delete(entry.name);
  }
  // Both API-key and subscription prepare failures retain their orphan reserve.
  for (const [slice, r] of reservations) {
    const capMicros = recordedSliceCapMicros({ reservation: r });
    const recordedCharge = r.charge_micros !== undefined ? r.charge_micros : Math.ceil(Number(r.charge_usd) * 1e6);
    must(referenceInteger(recordedCharge) && recordedCharge <= capMicros && (r.charge_micros === undefined || r.charge_usd === recordedCharge / 1e6), 'campaign_reservation_invalid');
    const closedBeforeRuntime = r.state === 'CLOSED_BEFORE_RUNTIME_CREATION' && r.runtime_creation_attempted === false && r.charge_micros === 0 && r.charge_usd === 0;
    const chargeMicros = capMicros === 6000000 && !closedBeforeRuntime ? capMicros : recordedCharge;
    totalMicros += chargeMicros;
    slices.push({ slice, charge_usd: chargeMicros / 1e6, charge_micros: chargeMicros, slice_cap_micros: capMicros, unsettled: r.state === 'ACTIVE' });
  }
  must(priorFound, 'parent_prior_campaign_baseline_required');
  must(referenceInteger(totalMicros) && totalMicros <= 30000000, 'campaign_ceiling_exceeded');
  return { conservative_usd: totalMicros / 1e6, conservative_micros: totalMicros, slices, campaign_usd_cap: 30, accounting_role: 'Additional epoch preflight; mandatory root recomputation and actual per-forward admission remain required' };
}

// Root guard covers historical/native-core/API-key campaigns as well as this epoch.
// Parent supplies a fresh reconciled guard; this module verifies its complete live
// financial-artifact snapshot rather than silently trusting an old epoch baseline.
export async function rootFinancialSnapshot() {
  const files = {};
  const capture = async (file) => {
    try { const st = await lstat(file); must(st.isFile() && !st.isSymbolicLink(), 'financial_artifact_must_be_regular'); files[path.relative(BASE, file)] = sha(await readFile(file)); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
  };
  const slice = async (directory) => {
    for (const name of ['budget.json', 'driver-budget.json', 'summary.json', 'outbound-budget.json']) await capture(path.join(directory, name));
  };
  for (const entry of await readdir(BASE, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const directory = path.join(BASE, entry.name);
    if (/^fixed-[0-9a-f]{12}(?:-[a-z0-9-]{1,32})?$/.test(entry.name)) {
      for (const child of await readdir(directory, { withFileTypes: true })) {
        if (child.isDirectory()) await slice(path.join(directory, child.name));
        else if (RESERVATION_FILE.test(child.name)) await capture(path.join(directory, child.name));
      }
    } else await slice(directory);
  }
  return Object.fromEntries(Object.entries(files).sort(([a], [b]) => a.localeCompare(b)));
}

export async function rootFinancialMinimum(snapshot) {
  const groups = new Map();
  const group = (name) => { if (!groups.has(name)) groups.set(name, {}); return groups.get(name); };
  const micros = (value) => {
    must(typeof value === 'number' && Number.isFinite(value) && value >= 0, 'financial_amount_unknown_or_negative');
    const result = Math.ceil(value * 1000000);
    must(Number.isSafeInteger(result), 'financial_amount_overflow');
    return result;
  };
  for (const [relative, expected] of Object.entries(snapshot)) {
    const file = path.resolve(BASE, relative);
    must(file.startsWith(`${BASE}/`), 'financial_snapshot_path_escape');
    const bytes = await readFile(file);
    must(sha(bytes) === expected, 'financial_snapshot_changed_during_recomputation');
    const value = JSON.parse(bytes);
    if (path.basename(path.dirname(relative)) === 'prior-campaign-baseline') continue; // Historical aggregate, not another paid slice.
    if (RESERVATION_FILE.test(path.basename(relative))) {
      const slice = reservationSlice(relative, value);
      const target = group(path.join(path.dirname(relative), slice));
      must(!target.reservation, 'duplicate_reservation_namespaces_for_same_slice');
      target.reservation = value;
      if (value.reference_closure?.kind === 'PINNED_RUN_SEALED_ZERO') must(relative === path.relative(BASE, file), 'pinned_zero_financial_source_alias');
      target.reservationPath = file;
    } else group(path.dirname(relative))[path.basename(relative)] = value;
  }
  const rows = [];
  for (const [slice, files] of groups) {
    const b = files['budget.json'], d = files['driver-budget.json'], s = files['summary.json'], outbound = files['outbound-budget.json'], r = files.reservation;
    const sliceCapMicros = recordedSliceCapMicros({ reservation: r, outbound, summary: s, budget: b, driver: d });
    const attempts = Math.max(Number(b?.attempts) || 0, Number(d?.attempts) || 0, Number(outbound?.attempts) || 0, Number(s?.budget?.attempts_used) || 0, Number(s?.budget?.attempts) || 0, Number(s?.slice_forwarded_attempts) || 0, Number(s?.generationRequestsObserved) || 0);
    const summaryCosts = [s?.budget?.costUsd, s?.budget?.driver_usage_priced_usd, s?.budget?.cc_lb_budget_status?.costUsd, s?.budget_after?.costUsd, s?.driver_cost_usd_total].filter((value) => value !== null && value !== undefined);
    const committed = Math.max(b?.costUsd == null ? 0 : micros(b.costUsd), ...summaryCosts.map(micros));
    // Old lastCost*1.5, reserve_usd and unsplit legacy_usd are NOT worst-case
    // prices. Unclassified legacy/reserve exposure retains the whole slice.
    const driverKnown = d?.usage_usd == null ? 0 : micros(d.usage_usd);
    const legacyUnknown = Boolean(d && attempts > 0 && (d.usage_usd == null || Number(d.reserve_usd) > 0 || Number(d.legacy_usd) > 0));
    const committedUnknown = Boolean(b && attempts > 0 && (b.costKnown !== true || (Array.isArray(b.unpricedRequestIds) && b.unpricedRequestIds.length > 0)));
    const summaryUnknown = !b && !d && attempts > 0;
    let outboundHeld = 0;
    if (outbound) {
      for (const key of ['spent_micros', 'in_flight_hold_micros', 'unknown_hold_micros', 'total_held_micros']) must(Number.isSafeInteger(outbound[key]) && outbound[key] >= 0, 'outbound_financial_snapshot_invalid');
      must(outbound.total_held_micros === outbound.spent_micros + outbound.in_flight_hold_micros + outbound.unknown_hold_micros, 'outbound_hold_components_disagree');
      outboundHeld = outbound.total_held_micros;
    }
    const reservationActive = Boolean(r && ['ACTIVE', 'RECONCILIATION_REQUIRED'].includes(r.state));
    const cancellationHold = s?.out_of_band_provider_cost?.conservative_upper_bound_usd == null ? 0 : micros(s.out_of_band_provider_cost.conservative_upper_bound_usd);
    const closure = validatedReferenceClosure(r, outbound, path.basename(slice), await zeroReaderEvidence(r, files.reservationPath));
    const closedBeforeRuntime = r?.state === 'CLOSED_BEFORE_RUNTIME_CREATION' && r.runtime_creation_attempted === false && r.charge_micros === 0 && r.charge_usd === 0 && !b && !d && !s && !outbound;
    const closureRequired = !closedBeforeRuntime && Boolean(r && (r.reference_closure !== undefined || r.charge_micros !== undefined || r.state === 'RECONCILED_BOUNDED_UNKNOWN' || r.state === 'CLOSED_BEFORE_RUNTIME_CREATION' || sliceCapMicros === 6000000));
    const unclassified = legacyUnknown || committedUnknown || summaryUnknown || reservationActive || cancellationHold > 0 || outbound?.unresolved === true || outbound?.bound_breach === true || outbound?.unsafe_integer_cost_evidence === true || (closureRequired && !closure);
    const known = Math.max(committed, driverKnown, Number(outbound?.spent_micros) || 0);
    // Typed micro amounts are authoritative; USD is display-only. Re-multiplying
    // that display can round a valid exact dual up by one micro.
    const reserve = r ? (referenceInteger(r.charge_micros) ? r.charge_micros : micros(r.charge_usd)) : 0;
    const held = closedBeforeRuntime ? 0 : closure ? Math.max(known, outboundHeld, reserve)
      : Math.max(known + cancellationHold, outboundHeld, reserve, unclassified ? sliceCapMicros : 0);
    must(Number.isSafeInteger(held) && held >= 0, 'financial_minimum_overflow');
    rows.push({ slice, slice_cap_micros: sliceCapMicros, known_catalog_micros: known, conservative_held_micros: held, unknown_or_in_flight: unclassified || r?.unresolved === true, reference_closure_valid: Boolean(closure), old_multiplier_not_used_as_upper_bound: true });
  }
  const minimumMicros = rows.reduce((sum, row) => sum + row.conservative_held_micros, 0);
  must(Number.isSafeInteger(minimumMicros), 'root_minimum_overflow');
  return { minimumMicros, rows, accounting: 'Catalog/list-equivalent exposure and conservative holds; not invoice reconciliation or proof of incremental subscription funding' };
}

async function validatedHistoricalThoughtReserve(guard) {
  if (!guard.historicalThoughtsReserve) return 0;
  const extra = guard.historicalThoughtsReserve;
  must(Number.isSafeInteger(extra.micros) && extra.micros >= 0 && typeof extra.file === 'string', 'historical_thought_reserve_invalid');
  const file = path.resolve(BASE, extra.file);
  must(path.dirname(file) === path.join(BASE, 'evidence') && path.basename(file) === 'campaign-safety-reconciliation-before-traces.json', 'historical_thought_evidence_not_supported');
  const bytes = await readFile(file);
  must(sha(bytes) === extra.sha256, 'historical_thought_evidence_changed');
  const evidence = JSON.parse(bytes);
  must(evidence.kind === 'conservative_known_cost_and_reserve_reconciliation_not_invoice' && Array.isArray(evidence.thoughtAdjustments), 'historical_thought_reconciliation_missing');
  const sum = evidence.thoughtAdjustments.reduce((total, row) => {
    must(Number.isSafeInteger(row.extraMicros) && row.extraMicros >= 0 && Number.isSafeInteger(row.thoughtTokens) && row.thoughtTokens > 0, 'historical_thought_row_invalid');
    return total + row.extraMicros;
  }, 0);
  must(sum === extra.micros && evidence.geminiThoughtExtraMicros === extra.micros, 'historical_thought_reserve_does_not_match_evidence');
  return extra.micros;
}

export function validateRevisionCampaignGuardShape(guard, selectedSliceMicros) {
  must(guard && typeof guard === 'object' && !Array.isArray(guard), 'root_campaign_guard_shape_invalid');
  must(guard.campaignCapMicros === 30000000 && guard.plannedSubscriptionSliceMaxMicros === selectedSliceMicros && guard.unknownProviderCostFullyReserved === true, 'root_campaign_caps_or_unknown_reserves_invalid');
}

// Both release booleans must be present and equal; false blocks before any access.
export function validateRevisionExecutionRelease(contract, manifest) {
  must(typeof contract?.execution_release === 'boolean' && typeof manifest?.execution_release === 'boolean' && contract.execution_release === manifest.execution_release, 'cap30_execution_release_metadata_mismatch');
  must(contract.execution_release === true, 'final_artifact_pending');
}

export async function verifyRootCampaignGuard(config) {
  const file = path.resolve(config.campaignGuard ?? '');
  const selectedSliceMicros = selectedReferenceSliceUsd(config) * 1000000;
  must(path.dirname(file) === path.join(BASE, 'evidence') && /^approved-core3-.+\.json$/.test(path.basename(file)), 'parent_root_campaign_guard_path_required');
  const bytes = await readFile(file);
  must(sha(bytes) === config.authorization?.campaign_guard_sha256, 'parent_root_campaign_guard_sha_mismatch');
  const guard = JSON.parse(bytes);
  const age = Date.now() - Date.parse(guard.at);
  must(Number.isFinite(age) && age >= 0 && age <= 600000, 'fresh_parent_root_campaign_reconciliation_required');
  validateRevisionCampaignGuardShape(guard, selectedSliceMicros);
  for (const key of ['conservativePriorMicros', 'outstandingMaxOutboundMicros', 'totalReservedMicros']) must(Number.isSafeInteger(guard[key]) && guard[key] >= 0, 'root_campaign_cost_or_max_outbound_unknown');
  const live = await rootFinancialSnapshot();
  must(JSON.stringify(guard.source_files) === JSON.stringify(live), 'root_financial_artifacts_changed_reconcile_again');
  const minimum = await rootFinancialMinimum(live);
  const thoughts = await validatedHistoricalThoughtReserve(guard);
  must(guard.conservativePriorMicros === minimum.minimumMicros + thoughts, 'root_guard_prior_does_not_equal_recomputed_artifact_minimum_and_validated_thoughts');
  must(Array.isArray(guard.plannedOutbounds), 'explicit_other_planned_outbound_bounds_required');
  const ids = new Set();
  const outstanding = guard.plannedOutbounds.reduce((sum, item) => {
    const cap = item.slice_cap_micros ?? 3000000;
    must(typeof item.id === 'string' && /^[a-z0-9-]{1,96}$/.test(item.id) && !ids.has(item.id) && [3000000, 6000000].includes(cap) && item.max_micros === cap, 'other_pending_provider_commands_require_their_recorded_full_slice_hold');
    ids.add(item.id); return sum + item.max_micros;
  }, 0);
  must(guard.outstandingMaxOutboundMicros === outstanding, 'root_guard_outstanding_does_not_equal_planned_bounds');
  const maxOutbound = guard.conservativePriorMicros + outstanding + selectedSliceMicros;
  must(guard.totalReservedMicros === maxOutbound && maxOutbound <= guard.campaignCapMicros && guard.withinCap === true, 'root_campaign_max_outbound_exceeds_campaign_or_sum_disagrees');
  return { guard: path.relative(BASE, file), sha256: sha(bytes), conservativePriorMicros: guard.conservativePriorMicros, outstandingMaxOutboundMicros: outstanding, recomputed_minimum_micros: minimum.minimumMicros, validated_historical_thoughts_micros: thoughts, total_max_outbound_usd: maxOutbound / 1e6, snapshot_files: Object.keys(live).length, accounting: minimum.accounting, actual_incremental_paid_usd: null, invoice_or_credit_funding_verified: false };
}

async function reserveCampaign(plan, config) {
  const rootGuard = await verifyRootCampaignGuard(config);
  const budget = await subscriptionCampaignBudget(plan.epoch);
  must(!budget.slices.some((slice) => slice.unsettled), 'pending_campaign_command_requires_parent_reconciliation');
  must(budget.conservative_micros + plan.caps.slice_usd * 1000000 <= 30000000, 'whole_slice_conservative_reserve_exceeds_campaign');
  const file = path.join(plan.epoch, `approved-core3-subscription-${plan.name}-reservation.json`);
  const record = { slice: plan.name, case: plan.case, at: new Date().toISOString(), state: 'ACTIVE', charge_micros: plan.caps.slice_usd * 1000000, charge_usd: plan.caps.slice_usd, slice_cap_micros: plan.caps.slice_usd * 1000000, runtime_creation_attempted: false,
    reason: 'Selected reference slice ceiling retained until native provider forwards and committed costs reconcile; not subscription funding', campaign_before: budget.conservative_usd, root_guard: rootGuard };
  await writeFile(file, `${JSON.stringify(record, null, 2)}\n`, { mode: 0o600, flag: 'wx' });
  return { file, record };
}

function execMetadata(command, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { env: { PATH: process.env.PATH ?? '/usr/bin:/bin' }, stdio: ['ignore', 'pipe', 'pipe'] });
    const chunks = [];
    let bytes = 0;
    const timer = setTimeout(() => child.kill('SIGKILL'), 15000);
    child.stdout.on('data', (chunk) => { bytes += chunk.length; if (bytes <= 65536) chunks.push(chunk); else child.kill('SIGKILL'); });
    child.stderr.on('data', () => {});
    child.once('error', () => { clearTimeout(timer); reject(new Error('BLOCKED:local_readonly_metadata_command_unavailable')); });
    child.once('close', (code) => { clearTimeout(timer); if (code !== 0 || bytes > 65536) reject(new Error('BLOCKED:local_readonly_metadata_failed')); else resolve(Buffer.concat(chunks).toString('utf8')); });
  });
}

async function oauthStorageMetadata(ctx) {
  must(/^[a-f0-9-]{36}$/.test(ctx.upstreamId), 'local_imported_upstream_uuid_invalid');
  // Never select ciphertext or plaintext. -readonly prevents accidental DB mutations.
  const sql = `SELECT spec.id, spec.kind, token.never_refresh, token.oauth_token_generation, length(token.oauth_credentials_ciphertext) AS encrypted_bytes, (SELECT count(*) FROM upstream_api_key_secret_v1 WHERE upstream_id=spec.id AND api_key_ciphertext IS NOT NULL) AS api_key_secrets FROM upstream_spec_v1 spec JOIN upstream_oauth_token_v1 token ON token.upstream_id=spec.id WHERE spec.id='${ctx.upstreamId}' AND spec.deleted_at IS NULL;`;
  const rows = JSON.parse(await execMetadata('sqlite3', ['-readonly', '-json', ctx.storagePath, sql]));
  must(rows.length === 1 && rows[0].kind === ctx.upstreams[0]?.kind && rows[0].never_refresh === 1 && rows[0].encrypted_bytes > 16 && rows[0].api_key_secrets === 0, 'server_aead_access_only_metadata_not_persisted');
  return rows[0];
}

function list(body, key) { return Array.isArray(body) ? body : Array.isArray(body?.[key]) ? body[key] : []; }
async function oauthFence(ctx, runtime, kind) {
  const [upstreams, principal, keys] = await Promise.all([
    runtime.admin(ctx, '/admin/v1/upstreams'),
    runtime.admin(ctx, `/admin/v1/principals/${ctx.principalId}`),
    runtime.admin(ctx, `/admin/v1/principals/${ctx.principalId}/keys?status=active`),
  ]);
  must([upstreams, principal, keys].every((response) => response.status === 200), 'local_oauth_fence_metadata_unavailable');
  const rows = list(upstreams.body, 'upstreams');
  const eligible = rows.filter((upstream) => upstream.enabled !== false);
  must(rows.length === 1 && eligible.length === 1 && eligible[0].id === ctx.upstreamId && eligible[0].kind === kind, 'api_key_or_foreign_upstream_fallback_forbidden');
  must(JSON.stringify(principal.body.allowed_upstreams) === JSON.stringify([ctx.upstreamId]), 'principal_oauth_candidate_ids_not_exclusive');
  const expected = runtime.serverLimitSpec(ctx.limits.client);
  must(JSON.stringify(principal.body.default_limits) === JSON.stringify(expected), 'principal_request_or_usd_limit_changed');
  const active = list(keys.body, 'keys');
  must(active.length === 1 && active[0].key_id === ctx.keyId, 'fresh_local_client_key_owner_mismatch');
  return { only_eligible_upstream_id: ctx.upstreamId, only_eligible_upstream_kind: kind,
    principal_id: ctx.principalId, key_id: ctx.keyId, candidate_ids: principal.body.allowed_upstreams,
    limits: principal.body.default_limits, key_limit_policy: 'Admin issue-key accepts label only; limit_overrides=[] inherits this principal request/USD policy',
    provider_api_key_upstreams_present: 0, fallback_generation_allowed: false };
}

async function refreshSnapshot(ctx) {
  const response = await fetch(ctx.metricsUrl, { signal: AbortSignal.timeout(10000) });
  must(response.status === 200, 'local_refresh_metrics_unavailable');
  const text = await response.text();
  const samples = text.split('\n').filter((line) => /^cclb_oauth_refresh_total(?:\{|\s)/.test(line)).map((line) => {
    const value = Number(line.trim().split(/\s+/).at(-1));
    must(Number.isFinite(value) && value >= 0, 'invalid_refresh_metric_sample');
    return value;
  });
  return { total: samples.reduce((sum, value) => sum + value, 0), sample_count: samples.length,
    scope: 'All OAuth refresh outcomes in this fresh owned server; absent counter means no refresh outcome was recorded, not network interception proof' };
}

async function eventsForPrincipal(ctx, runtime) {
  let cursor = 0;
  const events = new Map();
  for (let page = 0; page < 100; page += 1) {
    const query = new URLSearchParams({ since_cursor: String(cursor), principal_id: ctx.principalId, limit: '500' });
    const response = await runtime.admin(ctx, `/admin/v1/events/delta?${query}`);
    must(response.status === 200 && Array.isArray(response.body.events), 'local_settlement_events_unavailable');
    for (const event of response.body.events) {
      must(typeof event.request_id === 'string' && !events.has(event.request_id), 'duplicate_or_missing_terminal_request_id');
      events.set(event.request_id, event);
    }
    if (response.body.exhausted || response.body.events.length === 0) return [...events.values()];
    must(Number.isSafeInteger(response.body.next_cursor) && response.body.next_cursor > cursor, 'nonadvancing_settlement_cursor');
    cursor = response.body.next_cursor;
  }
  throw new Error('BLOCKED:settlement_event_pages_exhausted');
}

function redactAll(value, privateValues) {
  let text = typeof value === 'string' ? value : JSON.stringify(value);
  for (const secret of privateValues) if (typeof secret === 'string' && secret.length) text = text.split(secret).join('[REDACTED]');
  return text.replace(/(Bearer\s+)[A-Za-z0-9._~-]+/gi, '$1[REDACTED]');
}

function bridgeModule(key, runtime) {
  const names = Object.keys(runtime).filter((name) => /^[A-Za-z_$][\w$]*$/.test(name));
  return `const p=globalThis[Symbol.for(${quote(key)})]; if(!p)throw Error('BLOCKED:private_subscription_context_missing');\n` + names.map((name) => {
    if (name === 'BINARY_PATH') return 'export const BINARY_PATH=p.ctx.binaryPath;';
    if (name === 'EXPECTED_BINARY_SHA256') return 'export const EXPECTED_BINARY_SHA256=p.ctx.expectedBinarySha256;';
    if (name === 'prepareSlice') return `export async function prepareSlice(opts){if(opts.provider!==p.ctx.provider||opts.model!==p.ctx.model)throw Error('BLOCKED:native_provider_model_drift');return p.ctx;}`;
    if (name === 'ensureGeminiCliQuotaReady') return 'export const ensureGeminiCliQuotaReady=(...args)=>p.ensureQuota(...args);';
    if (name === 'finishEvidence') return 'export const finishEvidence=(...args)=>p.finishEvidence(...args);';
    return `export const ${name}=p.runtime.${name};`;
  }).join('\n');
}

// Shared transport adapter for the three existing native runners. It only builds
// an import-safe module; the authorized parent explicitly invokes module.main().
// pinned.contract is the verified source contract (required by Codex and AGY).
// A private shallow clone selects the fresh cap without editing historical files.
// Money is reserved only at the actual provider boundary, including retries and
// automatic successors. Client observers retain their original local counters.
// API-key callers retain their real kind/root/profile and Platform-auto policy;
// ChatGPT subscription Standard must never be substituted for API-project auto.
export async function buildGuardedNativeModule(plan, config, pinned, prepared, privateValues, outboundGuard) {
  const { runtime, ctx, providerBoundary } = prepared;
  const artifact = pinned.artifact;
  must(artifact && Object.isFrozen(artifact) && typeof artifact.path === 'string' && path.isAbsolute(artifact.path) && /^[0-9a-f]{64}$/.test(artifact.expected_sha256 ?? ''), 'pinned_native_artifact_required');
  must(ctx.binaryPath === artifact.path && ctx.binarySha256 === artifact.expected_sha256 && ctx.expectedBinarySha256 === artifact.expected_sha256, 'prepared_native_artifact_mismatch');
  must(['codex', 'omp-gemini', 'agy'].includes(plan.native), 'unsupported_shared_native_transport_profile');
  must(plan.caps?.slice_usd === 6 && ctx.limits?.client?.sliceCostUsd === plan.caps.slice_usd && runtime.CLIENT_LIMITS?.sliceCostUsd === plan.caps.slice_usd, 'prepared_runtime_reference_cap_mismatch');
  must(outboundGuard && typeof outboundGuard.reserve === 'function' && isProviderBoundaryRelay(providerBoundary) && providerBoundary.guardIdentity() === outboundGuard, 'actual_provider_boundary_and_identical_guard_required');
  const kind = plan.kind ?? ctx.upstreams?.[0]?.kind;
  const allowedKinds = plan.native === 'codex' ? ['openai_oauth', 'openai_api_key'] : ['gemini_oauth', 'gemini_api_key'];
  must(allowedKinds.includes(kind) && ctx.upstreams?.length === 1 && ctx.upstreams[0].kind === kind && ctx.upstreams[0].id === ctx.upstreamId && Array.isArray(privateValues), 'single_actual_native_kind_and_private_redaction_values_required');
  must(new URL(ctx.upstreams[0].baseUrl).href === new URL(providerBoundary.upstreamBaseUrl).href, 'configured_upstream_not_the_owned_provider_boundary');
  let currentNativeEvidence = null;
  if (plan.native === 'codex') {
    must(!Object.hasOwn(config, 'model') && process.env.CODEX_QA_MODEL === undefined, 'codex_current_native_model_override_forbidden');
    currentNativeEvidence = await loadCurrentNativeModelEvidence(config.nativeModelEvidence ?? {});
    must(plan.models?.length === 1 && plan.models[0] === currentNativeEvidence.model && ctx.model === currentNativeEvidence.model, 'codex_current_native_observation_seed_mismatch');
    must(plan.native_model_evidence?.provenance?.evidence_sha256 === currentNativeEvidence.provenance.evidence_sha256, 'codex_current_native_evidence_plan_mismatch');
  }
  const originalSourceSliceUsd = pinned.contract?.paid_bounds?.usd_ceiling_per_slice;
  if (plan.native !== 'omp-gemini') must([3, 6].includes(originalSourceSliceUsd), 'pinned_native_contract_required');
  const selectedContract = pinned.contract ? { ...pinned.contract, paid_bounds: { ...pinned.contract.paid_bounds, usd_ceiling_per_slice: plan.caps.slice_usd } } : null;
  const key = `cc-lb-approved-native-${randomUUID()}`;
  const approvedClient = prepared.approvedClient;
  if (plan.native === 'omp-gemini') assertVerifiedOmpClient(approvedClient, pinned.contract);
  const bridge = {
    runtime, ctx, privateValues, kind, contract: selectedContract, providerBoundary, fixtureNode: pinnedFixtureNode, traceSummary: (runnerCtx) => otlpTraceSummary(runnerCtx, config),
    approvedClient,
    approvedLauncher: () => assertVerifiedOmpClient(approvedClient, pinned.contract),
    versionEnvironment: ompVersionProbeEnvironment,
    observedModel: currentNativeEvidence?.model ?? null,
    currentNativeEvidence,
    currentNativeProfile: currentNativeEvidence ? Object.freeze({ ...CURRENT_NATIVE_PROFILE, config_contract: CURRENT_NATIVE_CONFIG_CONTRACT }) : null,
    configuredAuthority: new URL(providerBoundary.upstreamBaseUrl).host,
    captureError: (value) => appendFileSync(path.join(ctx.evidenceDir, 'provider-errors-redacted.jsonl'), `${runtime.redactText(redactAll(value, privateValues))}\n`, { mode: 0o600 }),
    assertCurrentNativeCli: (cli) => must(cli?.path === currentNativeEvidence.cli.path && cli.version === currentNativeEvidence.cli.version && cli.sha256 === currentNativeEvidence.cli.sha256, 'codex_current_native_cli_identity_mismatch'),
    assertCurrentNativeLaunch: (configText, args, env) => {
      const modelConfigPresent = /^\s*model\s*=/m.test(configText);
      const argvModelPresent = args.some((arg) => arg === '-m' || arg.startsWith('-m=') || arg === '--model' || arg.startsWith('--model=') || /^model\s*=/.test(arg));
      const envModelPresent = Object.hasOwn(env, 'CODEX_QA_MODEL');
      const metadata = Object.freeze({ generated_config_model_present: modelConfigPresent, argv_model_flag_present: argvModelPresent, CODEX_QA_MODEL_present: envModelPresent });
      must(!modelConfigPresent && !argvModelPresent && !envModelPresent, 'codex_current_native_launch_model_override_forbidden');
      return metadata;
    },
    finishEvidence: (context, summary) => runtime.finishEvidence(context, {
      ...summary, provider_boundary: providerBoundary.metadata(),
      ...(currentNativeEvidence ? { current_native_model_evidence: currentNativeEvidence, current_native_profile: CURRENT_NATIVE_PROFILE } : {}),
      provider_host_evidence_scope: 'Legacy host fields describe the configured owned relay; actual TLS observations are separate in provider_boundary',
      default_url_coverage_claimed: false, native_direct_provider_bypass_coverage: 'NOT_CLAIMED',
    }),
  };
  bridge.ensureQuota = async (context, options) => {
    const original = "JSON.stringify({ max_tokens: maxOutputTokens, generationConfig: { maxOutputTokens: 8 }, contents: 'invalid-not-array' })";
    must(pinned.sources['qa-runtime.mjs'].includes(original), 'negative_probe_body_source_changed');
    must(typeof providerBoundary.beginPinnedNegativeProbe === 'function' && typeof providerBoundary.settlePinnedNegativeProbe === 'function', 'causal_provider_negative_probe_operation_required');
    const operation = providerBoundary.beginPinnedNegativeProbe({ model: options.model, maxOutputTokens: options.maxOutputTokens });
    let ended = false;
    try {
      const originalHost = providerBoundary.metadata()?.source_original_url?.host;
      const quotaContext = originalHost === 'generativelanguage.googleapis.com'
        ? Object.create(context, { upstreams: { value: context.upstreams.map((u) => u.id === context.upstreamId ? { ...u, host: originalHost } : u), enumerable: true } })
        : context;
      const result = await runtime.ensureGeminiCliQuotaReady(quotaContext, options);
      const health = result.checks?.health;
      const budget = result.checks?.budget;
      const proof = { kind: 'pinned_negative_quota_probe', providerAttempts: result.providerAttempts,
        status: result.providerResponse?.status, errorStatus: result.providerResponse?.errorStatus,
        providerEvents: result.checks?.events?.providerEvents, event: result.checks?.events?.event,
        costKnown: budget?.costKnown, attemptsDelta: budget ? budget.attemptsAfter - budget.attemptsBefore : null,
        costDeltaMicros: budget ? Math.round((budget.costAfter - budget.costBefore) * 1e6) : null,
        drops: health?.dropIncreases?.length, failoverDelta: health ? health.failoverAfter - health.failoverBefore : null };
      const settled = providerBoundary.settlePinnedNegativeProbe(operation, result.verdict === 'READY' ? proof : null);
      ended = true;
      must(settled.ok, 'negative_probe_actual_provider_hold_requires_reconciliation');
      return result;
    } catch (error) {
      if (!ended) providerBoundary.settlePinnedNegativeProbe(operation, null);
      throw error;
    }
  };
  runtime.setSubscriptionProbeObserver?.(bridge.captureError);
  globalThis[Symbol.for(key)] = bridge;
  const helperUrl = `data:text/javascript;base64,${Buffer.from(bridgeModule(key, runtime)).toString('base64')}`;
  const nativeSourcePath = path.join(plan.epoch, plan.native, 'run.mjs');
  let source = pinned.sources[`${plan.native}/run.mjs`];
  const addBridge = `const SUBSCRIPTION_PRIVATE=globalThis[Symbol.for(${quote(key)})];\n`;
  if (plan.native === 'agy') {
    source = once(source, "from '../qa-runtime.mjs'", `from ${quote(helperUrl)}`, 'agy_private_runtime');
    source = once(source, 'const HERE = path.dirname(fileURLToPath(import.meta.url));', `${addBridge}const HERE = ${quote(ctx.root)};`, 'agy_owned_root');
    source = once(source, "const CONTRACT = JSON.parse(fs.readFileSync(path.join(CAMPAIGN, 'contract.json'), 'utf8'));", 'const CONTRACT = SUBSCRIPTION_PRIVATE.contract;', 'agy_fresh_selected_reference_contract_only');
    source = once(source, "const FIXTURE = path.join(HERE, 'fixture');", `const FIXTURE = ${quote(path.join(plan.epoch, 'agy/fixture'))};`, 'agy_original_fixture');
    source = once(source, "    server.listen(0, '127.0.0.1', () => {", `    server.listen(${plan.observer_port}, '127.0.0.1', () => {`, 'agy_preledgered_observer_port');
    source = once(source, 'new URL(configuredBaseUrl).hostname', 'new URL(configuredBaseUrl).host', 'agy_owned_authority');
    source = once(source, String.raw`  if (!configuredHost || /^(127\.|localhost$|\[?::1\]?$)/.test(configuredHost)) {`, '  if (!configuredHost || configuredHost !== SUBSCRIPTION_PRIVATE.configuredAuthority) {', 'agy_owned_local_configuration_check');
    source = once(source, "summary.blocked.push('upstream host missing or local; not a real provider');", "summary.blocked.push('upstream host does not match the owned provider-boundary configuration');", 'agy_truthful_owned_host_failure');
    source = once(source, '    if (obj?.error) {', '    if (obj?.error) {\n      SUBSCRIPTION_PRIVATE.captureError({ provider: SUBSCRIPTION_PRIVATE.kind, error: obj.error });', 'agy_full_redacted_provider_error');
    source = once(source, "        message: String(obj.error.message ?? '').slice(0, 200),", "        message: rt.redactText(String(obj.error.message ?? '')).slice(0, 200),", 'agy_observer_error_identity_redaction');
    source = once(source, '    output_tokens: u.candidatesTokenCount ?? 0,', '    output_tokens: (u.candidatesTokenCount ?? 0) + (u.thoughtsTokenCount ?? 0),', 'agy_usage_includes_thoughts');
    const tail = "main().catch((error) => {\n  const message = typeof rt.redactText === 'function' ? rt.redactText(String(error?.message ?? error)) : String(error?.message ?? error);\n  const blocked = message.startsWith('BLOCKED') || (rt.CredentialError && error instanceof rt.CredentialError);\n  const verdicts = Object.fromEntries(SLICE.scenarios.map((s) => [s, blocked ? 'BLOCKED' : 'NOT_RUN']));\n  console.log(JSON.stringify({ slice: 'agy', verdicts, driverError: message }));\n  process.exitCode = blocked ? 2 : 1;\n});";
    source = once(source, tail, 'export { main };', 'agy_parent_owned_main');
  } else {
    source = once(source, 'from "../qa-runtime.mjs"', `from ${quote(helperUrl)}`, 'native_private_runtime');
    source = once(source, 'const SLICE_DIR = dirname(fileURLToPath(import.meta.url));', `${addBridge}const SLICE_DIR = ${quote(ctx.root)};`, 'native_owned_root');
    if (plan.native === 'codex') {
      must(typeof Bun !== 'undefined', 'codex_native_runner_requires_bun_no_http_substitute');
      source = once(source, 'const CONTRACT = JSON.parse(readFileSync(join(ROOT, "contract.json"), "utf8"));', 'const CONTRACT = SUBSCRIPTION_PRIVATE.contract;', 'codex_fresh_selected_reference_contract_only');
      source = once(source, 'const FIXTURE = join(SLICE_DIR, "fixture");', `const FIXTURE = ${quote(path.join(plan.epoch, 'codex/fixture'))};`, 'codex_original_fixture');
      source = once(source, 'const HIDDEN_CHECK = join(SLICE_DIR, "hidden-check.mjs");', `const HIDDEN_CHECK = ${quote(path.join(plan.epoch, 'codex/hidden-check.mjs'))};`, 'codex_original_hidden_check');
      source = once(source, '// Optional env:\n//   CODEX_QA_MODEL (default gpt-5.1-codex), CODEX_QA_SCENARIOS (comma list, default http,ws,ws_reconnect)', '// Optional env:\n//   CODEX_QA_SCENARIOS (comma list, default http,ws,ws_reconnect)', 'codex_no_model_override_documentation');
      source = once(source, '// gpt-5.1-codex was rejected by the provider (model_not_found) for this key; gpt-5.2 has real\n// generations through the same key/endpoint (omp-gpt slice). Pass CODEX_QA_MODEL explicitly.\nconst MODEL = process.env.CODEX_QA_MODEL || "gpt-5.1-codex";', '// Current-native metadata supplies MODEL solely as the observational expected catalog key.\nconst MODEL = SUBSCRIPTION_PRIVATE.observedModel;', 'codex_current_native_observed_model_only');
      source = once(source, '    `model = "${MODEL}"`,\n', '', 'codex_no_generated_config_model_override');
      source = once(source, '  resolveCodex();', '  resolveCodex();\n  SUBSCRIPTION_PRIVATE.assertCurrentNativeCli(CODEX);', 'codex_observed_cli_identity_before_launch');
      source = once(source, '    const child = spawn(CODEX.path, args, { cwd, env, stdio: ["ignore", "pipe", "pipe"] });', '    const currentNativeConfig = readFileSync(join(S.codexHome, "config.toml"), "utf8");\n    const noOverride = SUBSCRIPTION_PRIVATE.assertCurrentNativeLaunch(currentNativeConfig, args, env);\n    writePrivate(join(S.dir, `codex-${tag}-current-native-config-contract.json`), JSON.stringify(noOverride));\n    const child = spawn(CODEX.path, args, { cwd, env, stdio: ["ignore", "pipe", "pipe"] });', 'codex_actual_generated_config_env_argv_no_override');
      source = once(source, '    port: 0,', `    port: ${plan.observer_port},`, 'codex_preledgered_tee_port');
      source = once(source, '  SECRET_VALUES = [ctx.__clientKeyValue];', '  SECRET_VALUES = [...SUBSCRIPTION_PRIVATE.privateValues, ctx.__clientKeyValue];', 'codex_provider_projection_redaction');
      source = once(source, '    authorization_sha12: short(val("authorization")),', '', 'codex_no_credential_derived_hash_artifacts');
      source = once(source, '    out.store = r?.store ?? null;', '    out.store = r?.store ?? null;\n    out.service_tier = r?.service_tier ?? null;', 'codex_observed_response_tier_metadata');
      source = once(source, '  if (prewarmCompleted.some((x) => !(x.usage?.input_tokens > 0) || (x.output?.length ?? 0) > 0)) reasons.push("generate:false prewarm completion lacks input usage or produced output");', [
        '  const invalidObservedPrewarm = (u) => u && ["input_tokens", "cached_tokens", "output_tokens", "reasoning_tokens", "total_tokens"].some((key) => u[key] != null && (!Number.isSafeInteger(u[key]) || u[key] < 0 || ((key === "output_tokens" || key === "reasoning_tokens") && u[key] !== 0)));',
        '  if (prewarmCompleted.some((x) => (x.output?.length ?? 0) > 0 || invalidObservedPrewarm(x.usage))) reasons.push("generate:false prewarm produced output or invalid observed usage");',
        '  t.prewarm.usage_evidence = prewarmCompleted.every((x) => x.usage && Number.isSafeInteger(x.usage.input_tokens) && x.usage.output_tokens === 0) ? "OBSERVED_EXACT_VALUES" : "MISSING_OR_PARTIAL_FINANCE_UNPROVEN";',
      ].join('\n'), 'codex_documented_no_output_prewarm_not_positive_input_assumption');
      source = once(source, 'function summarizeProviderEvent(ev) {', 'function summarizeProviderEvent(ev) {\n  if (ev?.error || ev?.response?.error) SUBSCRIPTION_PRIVATE.captureError({ provider: SUBSCRIPTION_PRIVATE.kind, error: ev.error ?? ev.response.error });', 'codex_sse_ws_provider_error');
      source = once(source, '          const j = tryJson(t);', '          const j = tryJson(t);\n          if (j?.error) SUBSCRIPTION_PRIVATE.captureError({ provider: SUBSCRIPTION_PRIVATE.kind, status: res.status, error: j.error });', 'codex_http_provider_error');
      source = once(source, 'main().catch((e) => {\n  console.error(redact(String(e?.stack ?? e)));\n  process.exit(1);\n});', 'export { main };', 'codex_parent_owned_main');
      must(!source.includes('CODEX_QA_MODEL') && !source.includes('--model'), 'codex_generated_model_override_forbidden');
    } else {
      pinnedFixtureNode(); // Fail before the native runner starts if the pinned Node is absent or changed.
      source = pinOmpGeminiFixtureTests(source);
      source = pinOmpGeminiTraceSummary(source);
      source = once(source, 'const EXPECTED_OMP_VERSION = "18.12.2";', 'const EXPECTED_OMP_VERSION = SUBSCRIPTION_PRIVATE.approvedClient.version;', 'omp_approved_version_from_identity');
      source = once(source, '  const ompPath = String(spawnSync("/bin/sh", ["-c", "command -v omp"], { encoding: "utf8", env: { PATH: process.env.PATH ?? "" } }).stdout || "").trim();', '  const ompPath = SUBSCRIPTION_PRIVATE.approvedLauncher();', 'omp_approved_launcher');
      source = once(source, 'env: { PATH: process.env.PATH ?? "", HOME: ctx.clientHome }', 'env: SUBSCRIPTION_PRIVATE.versionEnvironment(ctx.clientHome)', 'omp_cloud_key_free_version_probe');
      source = once(source, '  if (!versionText.includes(EXPECTED_OMP_VERSION)) summary.notes.push(`omp_version_differs:${versionText}`);', '  if (ompVersion.error || ompVersion.status !== 0 || ompVersion.signal != null || versionText !== EXPECTED_OMP_VERSION) throw Error("BLOCKED:approved_omp_version_mismatch");', 'omp_exact_approved_version_gate');
      source = once(source, 'cmd: "omp", args, cwd: session.projectDir', 'cmd: SUBSCRIPTION_PRIVATE.approvedLauncher(), args, cwd: session.projectDir', 'omp_approved_identity_before_native_spawn');
      source = once(source, 'const FIXTURE_DIR = join(SLICE_DIR, "fixture");', `const FIXTURE_DIR = ${quote(path.join(plan.epoch, 'omp-gemini/fixture'))};`, 'omp_original_fixture');
      source = once(source, 'const CONTROL_MODEL = "gemini-3.5-flash-lite";', `const CONTROL_MODEL = ${quote(config.controlModel)};`, 'omp_confirmed_canonical_control');
      source = once(source, 'const EXPECTED_PROVIDER_HOST = "generativelanguage.googleapis.com";', `const EXPECTED_PROVIDER_HOST = ${quote(bridge.configuredAuthority)};`, 'omp_owned_local_configuration_receipt_separate');
      source = once(source, '    server.listen(0, "127.0.0.1", () => resolve(server));', `    server.listen(${plan.observer_port}, "127.0.0.1", () => resolve(server));`, 'omp_preledgered_tap_port');
      source = once(source, '  if (obj.error) {', '  if (obj.error) {\n    SUBSCRIPTION_PRIVATE.captureError({ provider: SUBSCRIPTION_PRIVATE.kind, error: obj.error });', 'omp_full_redacted_provider_error');
      const marker = '// Run only when executed directly; importing (offline adjudicator) reuses the predicate without side effects.';
      must(source.split(marker).length === 2, 'omp_direct_main_anchor_drift');
      source = source.slice(0, source.indexOf(marker)) + 'export { main };\n';
    }
  }
  // Codex removes only assistant-selected model overrides. All other native
  // CLI settings, tools, SDK warmup and unmodified request/response handling stay.
  // Original client counters remain additional early stops, not money reserves.
  must(!source.includes('fileURLToPath(import.meta.url)'), 'unmapped_data_module_path');
  try {
    const module = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
    return { module, bridgeKey: key, transformedSourceHash: sha(source), originalSource: nativeSourcePath, originalSourceSliceUsd, selectedReferenceSliceUsd: plan.caps.slice_usd, actualUpstreamKind: kind, providerBoundary, currentNativeModelEvidence: currentNativeEvidence, currentNativeProfile: bridge.currentNativeProfile, paidWsPolicy: currentNativeEvidence ? 'Actual provider-boundary relay reserves every create/steer write; native SDK tools and warmup are unmodified; Codex model selection has no assistant override' : 'Actual provider-boundary relay reserves every create/steer write; native SDK configuration and warmup are unmodified' };
  } catch {
    delete globalThis[Symbol.for(key)];
    throw new Error('BLOCKED:pinned_native_runner_load_failed_no_secret_details');
  }
}

function scenarioVerdict(summary) {
  if (typeof summary?.verdict === 'string') return summary.verdict;
  const values = Array.isArray(summary?.scenarios) ? summary.scenarios : Object.values(summary?.scenarios ?? {});
  if (!values.length) return 'NOT_RUN';
  return values.every((value) => value.verdict === 'PASS') ? 'PASS' : values.some((value) => value.verdict === 'FAIL') ? 'FAIL' : 'BLOCKED';
}
function forwardCount(summary, native) {
  return native === 'codex' ? summary?.budget?.attempts_used : native === 'omp-gemini' ? summary?.slice_forwarded_attempts : Number.isSafeInteger(summary?.generationRequestsObserved) ? summary.generationRequestsObserved + (summary.setupReadiness?.providerAttempts ?? 0) : null;
}

function trustedWsJoin(summary, events, capture, ctx) {
  const checks = [];
  const joined = [];
  const used = new Set();
  const spans = capture?.spans ?? [];
  const turns = (summary?.scenarios ?? []).flatMap((scenario) => scenario.turns ?? []);
  checks.push({ name: 'collector_decode_and_loss', pass: Boolean(capture?.stats?.acceptedSpans > 0 && capture.stats.decodeErrors === 0 && capture.stats.rejectedSpans === 0 && capture.stats.oversizedRequests === 0) });
  let creates = 0;
  for (const turn of turns) {
    const requests = (turn.requests ?? []).filter((request) => request.transport === 'ws');
    checks.push({ name: `${turn.label}:durable_event_loss_or_timeout`, pass: turn.cc_lb_events?.count === requests.length && turn.cc_lb_events?.invalidated === false && turn.cc_lb_events?.timed_out === false });
    for (const request of requests) {
      creates += 1;
      const terminals = (turn.responses ?? []).filter((response) => response.attempt === request.attempt && ['response.completed', 'response.incomplete', 'response.failed'].includes(response.type));
      const response = terminals[0];
      const matches = response?.response_id ? spans.filter((span) => span.name === 'proxy.responses_websocket.turn' && span.resource?.serviceName === 'cc-lb' && span.attributes?.['gen_ai.response.id'] === response.response_id) : [];
      const ids = [...new Set(matches.map((span) => span.attributes?.['cc_lb.request.id']).filter((id) => typeof id === 'string'))];
      const streams = [...new Set(matches.map((span) => span.attributes.stream_id ?? span.attributes['cc_lb.responses.stream_id'] ?? span.attributes['gen_ai.request.stream_id']).filter((id) => id !== undefined && id !== null && id !== ''))];
      const rows = ids.length === 1 ? events.filter((event) => event.request_id === ids[0]) : [];
      const event = rows[0];
      const usage = response?.usage;
      const expected = usage ? { input_tokens: Math.max(0, usage.input_tokens - (usage.cached_tokens ?? 0)), cache_read_input_tokens: usage.cached_tokens ?? 0, output_tokens: usage.output_tokens, ...(usage.reasoning_tokens != null ? { thinking_tokens: usage.reasoning_tokens } : {}) } : null;
      const pass = terminals.length === 1 && ids.length === 1 && streams.length <= 1 && rows.length === 1 && !used.has(ids[0]) && expected && Object.entries(expected).every(([key, value]) => Number.isSafeInteger(value) && value >= 0 && event[key] === value)
        && event.upstream_id === ctx.upstreamId && event.principal_id === ctx.principalId && (event.key_id ?? event.api_key_id ?? event.client_key_id) === ctx.keyId && event.status >= 200 && event.status < 300 && event.status !== 101 && Number.isSafeInteger(event.cost_usd_micros) && event.cost_usd_micros >= 0;
      checks.push({ name: request.attempt, pass: Boolean(pass), terminal_count: terminals.length, trusted_request_ids: ids, durable_events: rows.length, observed_stream_ids: streams });
      if (ids[0]) used.add(ids[0]);
      if (pass) joined.push({ request_id: ids[0], provider_response_id_sha256: sha(response.response_id), stream_id: streams[0] ?? null, default_lane: streams.length === 0, generate_false: request.generate === false, expected_usage: expected, cost_usd_micros: event.cost_usd_micros });
    }
  }
  checks.push({ name: 'native_ws_creates_exercised', pass: creates > 0 });
  return { verdict: checks.every((check) => check.pass) ? 'PASS' : 'INCOMPLETE', creates, joined_turns: joined, checks,
    joining: 'Observed provider response_id -> actual OTel gen_ai.response.id + cc_lb.request.id -> unique durable request_id; no sequence/time join or fabricated identity' };
}

async function waitTrustedWsEvidence(config, summary, events, ctx) {
  // Event-driven wait on the existing collector's atomic snapshot writes; no
  // sleep/retry loop, new collector, fake span or application hook is introduced.
  return new Promise((resolve, reject) => {
    let watcher;
    let finished = false;
    let reading = false;
    let dirty = false;
    let latest = { verdict: 'INCOMPLETE', reason: 'trusted collector snapshot not yet available' };
    const finish = (error) => { if (finished) return; finished = true; clearTimeout(timer); watcher?.close(); if (error) reject(error); else resolve(latest); };
    const inspect = async () => {
      if (finished) return;
      if (reading) { dirty = true; return; }
      reading = true;
      try { const capture = await json(config.capture); latest = trustedWsJoin(summary, events, capture, ctx); if (latest.verdict === 'PASS') finish(); }
      catch { finish(new Error('BLOCKED:trusted_collector_snapshot_invalid')); }
      finally {
        reading = false;
        if (dirty && !finished) { dirty = false; queueMicrotask(() => { void inspect(); }); }
      }
    };
    const timer = setTimeout(() => finish(), 30000);
    try {
      watcher = watch(path.dirname(config.capture), () => { void inspect(); });
      watcher.once('error', () => finish(new Error('BLOCKED:trusted_collector_snapshot_watch_failed')));
      void inspect();
    } catch { finish(new Error('BLOCKED:trusted_collector_snapshot_watch_unavailable')); }
  });
}

export async function runNativeSubscription(config) {
  must(typeof Bun !== 'undefined', 'use_bun_for_original_native_cli_runners');
  const plan = await nativeSubscriptionPlan(config);
  const pinned = await pinnedInputs(plan, config);
  const spec = NATIVE_SUBSCRIPTION_CASES[config.case];
  // Identity failure must precede the reservation and every broker/key/SUT read.
  const approvedClient = plan.native === 'omp-gemini' ? await preflightApprovedOmpClient(plan, pinned.contract) : null;
  const sourceOptions = { ...config.source, kind: spec.kind };
  must(BROKER_SOURCE_CONTRACT.sources[sourceOptions.sourceHash12]?.kind === spec.kind, 'approved_owner_source_required');
  const probe = await sourcePreflight(sourceOptions, config.authorization);
  must(probe.verdict === 'PARSER_AVAILABLE_NOT_READ', 'broker_has_no_safe_json_projection_parser');
  const reservation = await reserveCampaign(plan, config);
  let prepared;
  let access;
  let native;
  let outboundGuard;
  let providerBoundary;
  let nativeCommandFinished = false;
  let observedCostMicros = null;
  let runtimeCreationAttempted = false;
  let failure = null;
  const oldEnv = {};
  const evidence = { case: config.case, native: plan.native, kind: spec.kind, state: 'NOT_RUN', binary_sha256: config.expectedSha,
    caps: plan.caps, source: null, source_preflight: probe, import: null, native_scenarios: null,
    current_native: plan.native_model_evidence ? Object.freeze({
      observation: plan.native_model_evidence,
      provenance: plan.native_model_evidence.provenance,
      config_contract: Object.freeze({
        no_model_override: true,
        historical_harness_source: 'UNCHANGED_FIXED_RAW_EVIDENCE',
        authenticated_session_provider_qa_proven: false,
      }),
    }) : null,
    root_campaign_guard: reservation.record.root_guard,
    refresh: null, cost: null, provider_errors: 'evidence/provider-errors-redacted.jsonl', all_original_cases_required: plan.required_original_cases };
  try {
    access = await readBrokerAccess(sourceOptions, config.authorization, probe);
    evidence.source = access.evidence;
    runtimeCreationAttempted = true;
    reservation.record.runtime_creation_attempted = true;
    prepared = await prepareAccessOnlySubscriptionRuntime({ ...config, revisionManifestSha256: config.authorization?.revision_manifest_sha256, artifact: pinned.artifact, model: plan.models[0], sliceCostUsd: plan.caps.slice_usd, sliceName: plan.name, pricedModels: plan.models,
      providerBoundaryPort: plan.provider_boundary_port, getProviderGuard: () => outboundGuard,
      onProviderBoundary: (value) => { providerBoundary = value; },
      descriptor: descriptorForAccess(spec.kind, access.accessMaterial), accessMaterial: access.accessMaterial });
    const { ctx, runtime } = prepared;
    evidence.import = { ...prepared.evidence, fence: await oauthFence(ctx, runtime, spec.kind), storage_before: await oauthStorageMetadata(ctx) };
    must(ctx.blocked.length === 0 && ctx.costCatalog?.modelPriced === true && ctx.costCatalog?.installedByServer === true, 'installed_exact_model_catalog_required');
    for (const model of plan.models) must(ctx.costCatalog.prices?.[model] && Number.isFinite(ctx.costCatalog.prices[model].inputMax) && Number.isFinite(ctx.costCatalog.prices[model].outputMax), 'canonical_native_catalog_price_unavailable');
    const catalog = JSON.parse(await readFile(ctx.priceCatalogPath, 'utf8'));
    outboundGuard = createOutboundBudgetGuard({ ctx, plan, catalog, models: plan.models,
      campaignBudgetBefore: reservation.record.root_guard, pricingEligibility: config.pricingEligibility });
    must(isProviderBoundaryRelay(providerBoundary) && providerBoundary === prepared.providerBoundary && providerBoundary.guardIdentity() === outboundGuard, 'prepared_actual_provider_boundary_guard_mismatch');
    evidence.outbound_preflight = outboundGuard.preflight();
    must(evidence.outbound_preflight.ok, `outbound_guard_setup:${evidence.outbound_preflight.reason}`);
    const initialBudget = await runtime.assertBudget(ctx);
    must(initialBudget.attempts === 0 && initialBudget.costKnown === true, 'fresh_oauth_slice_budget_required');
    const refreshBefore = await refreshSnapshot(ctx);
    must(refreshBefore.total === 0, 'refresh_occurred_before_native_launch');
    for (const entry of [
      { kind: 'native_listener_reservation', id: `127.0.0.1:${plan.observer_port}`, port: plan.observer_port, delete: 'Ends with the recorded parent subscription native process; explicit cleanup only' },
      { kind: 'native_cli_process_reservation', id: `${plan.native} children under ${ctx.root}`, delete: 'Verify matching owned client root and recorded PID before explicit cleanup' },
    ]) await runtime.recordResource({ name: plan.name }, entry);
    const privateValues = [access.accessMaterial.access_token, access.accessMaterial.account_id, access.accessMaterial.project_id, ctx.clientKey, ctx._adminToken].filter(Boolean);
    const env = { QA_RESUME: undefined, ...(plan.native === 'codex' ? {} : { CODEX_QA_MODEL: config.model }), CODEX_QA_SCENARIOS: spec.scenario,
      CODEX_QA_ATTEMPT_CAP: undefined, QA_GEMINI_PRIMARY_MODEL: plan.native === 'codex' ? undefined : config.model, QA_OTLP_ENDPOINT: config.otlpEndpoint,
      AGY_WIRE_MODEL: config.model, AGY_MODEL_LABEL: config.agyModelLabel ?? 'gemini-3.1-pro-low',
      AGY_PRICED_MODELS: (config.auxiliaryModels ?? []).join(','), AGY_OBSERVER: '1' };
    for (const [key, value] of Object.entries(env)) { oldEnv[key] = process.env[key]; if (value === undefined) delete process.env[key]; else process.env[key] = value; }
    // Model/scenario constants are initialized at module evaluation, so set env before loading.
    native = await buildGuardedNativeModule(plan, config, pinned, { ...prepared, approvedClient }, privateValues, outboundGuard);
    try { await native.module.main(); } catch (error) {
      failure = 'native_cli_or_provider_rejected_original_wire';
      appendFileSync(path.join(ctx.evidenceDir, 'provider-errors-redacted.jsonl'), `${redactAll({ layer: 'native_driver', message: String(error?.message ?? error) }, privateValues)}\n`, { mode: 0o600 });
    } finally {
      nativeCommandFinished = true;
    }
    const summary = await json(path.join(ctx.root, 'summary.json'));
    const events = await eventsForPrincipal(ctx, runtime);
    const attempts = events.filter((event) => event.upstream_id && event.error_code !== 'limit_rejected');
    const count = forwardCount(summary, plan.native);
    const ownerOk = attempts.every((event) => event.upstream_id === ctx.upstreamId && event.principal_id === ctx.principalId && (event.api_key_id ?? event.key_id ?? event.client_key_id) === ctx.keyId);
    const costsKnown = attempts.every((event) => Number.isSafeInteger(event.cost_usd_micros) && event.cost_usd_micros >= 0);
    const countMatches = Number.isSafeInteger(count) && count > 0 && count === attempts.length && count <= 16;
    const costUsd = attempts.reduce((sum, event) => sum + (Number(event.cost_usd_micros) || 0), 0) / 1e6;
    observedCostMicros = costsKnown ? attempts.reduce((sum, event) => sum + event.cost_usd_micros, 0) : null;
    const storageAfter = await oauthStorageMetadata(ctx);
    const refreshAfter = await refreshSnapshot(ctx);
    const fenceAfter = await oauthFence(ctx, runtime, spec.kind);
    const unchangedGrant = storageAfter.oauth_token_generation === evidence.import.storage_before.oauth_token_generation && storageAfter.never_refresh === 1;
    const wsProof = ['codex-ws', 'codex-ws-reconnect'].includes(plan.case) ? await waitTrustedWsEvidence(config, summary, events, ctx) : { verdict: 'NOT_APPLICABLE', reason: 'Not a WebSocket case' };
    const reconciliation = reconcileReferenceReservation({ record: reservation.record, guard: outboundGuard, providerBoundaries: [providerBoundary], nativeCommandFinished, observedCostMicros });
    Object.assign(reservation.record, reconciliation.record);
    evidence.reference_closure = { reconciled: reconciliation.reconciled, proof: reconciliation.proof, reason: reconciliation.reason };
    evidence.provider_boundary = providerBoundary.metadata();
    const outbound = outboundGuard.snapshot();
    const modeledChargeMicros = Math.max(observedCostMicros ?? 0, outbound.total_held_micros);
    const providerCountMatches = reconciliation.reconciled && reconciliation.proof.guard_attempts === outbound.attempts;
    const tlsObserved = evidence.provider_boundary.tls_receipts?.some((entry) => entry.secure_connect_observed === true && entry.certificate_authorized === true && entry.hostname_verified === true) === true;
    const allBoundsSettled = providerCountMatches && tlsObserved && outbound.unresolved === false && outbound.bound_breach === false && outbound.unsafe_integer_cost_evidence === false && Number.isSafeInteger(modeledChargeMicros) && modeledChargeMicros <= plan.caps.slice_usd * 1000000;
    const settled = costsKnown && countMatches && ownerOk && costUsd <= plan.caps.slice_usd && unchangedGrant && refreshAfter.total === 0 && ['PASS', 'NOT_APPLICABLE'].includes(wsProof.verdict) && allBoundsSettled;
    const verdict = scenarioVerdict(summary);
    evidence.state = settled && verdict === 'PASS' && !failure ? 'PASS' : outbound.blocked ? 'BLOCKED' : verdict === 'FAIL' || failure ? 'FAIL' : 'BLOCKED';
    evidence.native_scenarios = { verdict, scenarios: summary?.scenarios ?? null, forwarded_count: count, committed_provider_attempts: attempts.length, count_matches: countMatches, event_owner_ids_match: ownerOk, native_client_uses_owned_local_key: true, credential_derived_hashes_serialized: false, ref: path.join(ctx.root, 'summary.json'), original_source: native.originalSource, original_source_slice_usd: native.originalSourceSliceUsd, selected_reference_slice_usd: native.selectedReferenceSliceUsd, actual_upstream_kind: native.actualUpstreamKind, legacy_driver_guard: 'Additional early stop only; not the financial worst-case bound', admission_observation_scope: 'client_to_cc_lb_original_frame_or_request', transformed_source_sha256: native.transformedSourceHash };
    if (plan.native_model_evidence) {
      evidence.native_scenarios.current_native_model_evidence = plan.native_model_evidence;
      evidence.native_scenarios.current_native_profile = plan.current_native_profile;
      evidence.native_scenarios.model_observation_key = plan.models[0];
      evidence.native_scenarios.actual_authenticated_session_provider_qa_proven = false;
    }
    evidence.native_scenarios.trusted_ws_turns = wsProof;
    evidence.native_scenarios.paid_ws_policy = native.paidWsPolicy;
    evidence.native_scenarios.admission_observation_scope = 'actual_provider_boundary_write';
    evidence.native_scenarios.client_request_count_is_not_provider_dispatch_count = true;
    evidence.native_scenarios.actual_provider_reservations = outbound.attempts;
    evidence.native_scenarios.actual_provider_counter_matches = providerCountMatches;
    evidence.native_scenarios.actual_tls_observed = tlsObserved;
    evidence.native_scenarios.default_url_coverage_claimed = false;
    evidence.refresh = { before: refreshBefore, after: refreshAfter, token_generation_unchanged: unchangedGrant,
      never_refresh_persisted: storageAfter.never_refresh === 1, adapter_token_endpoint_calls: 0,
      proof_scope: 'Supported never_refresh import + persisted token generation + fresh-server refresh metrics. No packet-level token-endpoint interception is claimed.' };
    evidence.cost = { settled, selected_reference_slice_usd: plan.caps.slice_usd, committed_catalog_list_cost_usd: costsKnown ? costUsd : null, conservative_list_charge_usd: modeledChargeMicros / 1e6, unknown_and_inflight_holds_preserved: outbound.unresolved, conservative_charge_usd: reservation.record.charge_usd, actual_provider_attempts: outbound.attempts, client_request_attempts: attempts.length, outbound_guard: outbound, actual_incremental_paid_usd: null, invoice_or_credit_funding_verified: false };
    evidence.import.fence_after = fenceAfter;
    const redactedEvents = events.map((event) => ({ request_id: event.request_id, upstream_id: event.upstream_id, principal_id: event.principal_id,
      key_id: event.api_key_id ?? event.key_id ?? event.client_key_id ?? null, status: event.status, error_code: event.error_code ?? null,
      model: event.model, input_tokens: event.input_tokens, output_tokens: event.output_tokens, thinking_tokens: event.thinking_tokens,
      cache_read_input_tokens: event.cache_read_input_tokens, cost_usd_micros: event.cost_usd_micros }));
    await writeFile(path.join(ctx.evidenceDir, 'subscription-settled-events.json'), `${JSON.stringify(redactedEvents, null, 2)}\n`, { mode: 0o600 });
  } catch (error) {
    failure = String(error?.message ?? '').startsWith('BLOCKED:') ? String(error.message) : 'BLOCKED:subscription_runner_failed_no_secret_details';
    if (outboundGuard) evidence.outbound_guard = outboundGuard.snapshot();
    evidence.state = 'BLOCKED';
    evidence.failure = failure;
    if (!runtimeCreationAttempted) {
      Object.assign(reservation.record, { state: 'CLOSED_BEFORE_RUNTIME_CREATION', charge_micros: 0, charge_usd: 0, runtime_creation_attempted: false });
    } else {
      const reconciliation = reconcileReferenceReservation({ record: reservation.record, guard: outboundGuard, providerBoundaries: providerBoundary ? [providerBoundary] : [], nativeCommandFinished, observedCostMicros });
      Object.assign(reservation.record, reconciliation.record);
      evidence.reference_closure = { reconciled: reconciliation.reconciled, proof: reconciliation.proof, reason: reconciliation.reason };
      if (providerBoundary) evidence.provider_boundary = providerBoundary.metadata();
    }
  } finally {
    for (const [key, value] of Object.entries(oldEnv)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
    if (prepared) prepared.runtime.setSubscriptionProbeObserver?.(null);
    if (native) delete globalThis[Symbol.for(native.bridgeKey)];
    if (access) { access.accessMaterial.access_token = ''; delete access.accessMaterial.account_id; delete access.accessMaterial.project_id; }
    await writeFile(reservation.file, `${JSON.stringify(reservation.record, null, 2)}\n`, { mode: 0o600 });
  }
  if (prepared) await writeFile(path.join(prepared.ctx.root, 'subscription-evidence.json'), `${JSON.stringify(evidence, null, 2)}\n`, { mode: 0o600 });
  else await writeFile(path.join(plan.epoch, `approved-core3-subscription-${plan.name}-blocked.json`), `${JSON.stringify(evidence, null, 2)}\n`, { mode: 0o600 });
  return evidence;
}

export async function subscriptionCampaignEvidence(config) {
  const plan = subscriptionRuntimePlan(config);
  const cases = [];
  for (const [id, spec] of Object.entries(NATIVE_SUBSCRIPTION_CASES)) {
    const file = path.join(plan.epoch, spec.slice, 'subscription-evidence.json');
    const evidence = await json(file);
    const valid = evidence?.case === id && evidence.kind === spec.kind && evidence.binary_sha256 === config.expectedSha;
    cases.push({ case: id, verdict: !evidence ? 'NOT_RUN' : valid ? evidence.state : 'INVALID', settled: valid && evidence.cost?.settled === true,
      upstream_id: valid ? evidence.import?.imported_upstream_id ?? null : null, upstream_kind: valid ? evidence.import?.fence?.only_eligible_upstream_kind ?? null : null, ref: path.relative(plan.epoch, file) });
  }
  const complete = cases.every((item) => item.verdict === 'PASS' && item.settled === true);
  return { kind: 'native_subscription_campaign_evidence', binary_sha256: config.expectedSha, mandatory_complete: complete,
    verdict: complete ? 'PASS' : cases.some((item) => item.verdict === 'FAIL' || item.verdict === 'INVALID') ? 'FAIL' : 'INCOMPLETE', cases,
    paid_calls_by_evidence_command: 0, provider_tokens_read_by_evidence_command: false, missing_cases_never_count_as_complete: true };
}

export function subscriptionConfigContract() {
  return { execution: 'NOT_RUN', default_mode: 'metadata_only', required_cases: Object.keys(NATIVE_SUBSCRIPTION_CASES),
    no_refresh_or_api_key_fallback: true, caps: { new_slice_usd: 6, allowed_selected_slice_usd: [6], historical_recorded_slice_usd: [3, 6], historical_missing_cap_usd: 3, campaign_usd: 30, slice_forwards: 16 },
    current_native_model_contract: CURRENT_NATIVE_CONFIG_CONTRACT,
    config: {
      case: 'codex-http|codex-ws|codex-ws-reconnect|omp-gemini|agy',
      epoch: 'new parent-ledger-owned fixed-<newsha12>-core3[-label]', expectedSha: 'new green binary SHA256',
      sliceCostUsd: 6,
      kind: 'openai_oauth|gemini_oauth',
      model: 'Gemini/AGY only; Codex derives its observational catalog key from nativeModelEvidence and rejects config.model',
      nativeModelEvidence: { path: 'Parent-pinned existing local metadata summary path', sha256: 'Parent-pinned exact SHA256' },
      controlModel: 'OMP Gemini only', auxiliaryModels: ['AGY native auxiliary model IDs'],
      campaignGuard: 'fresh parent reconciliation under existing evidence/approved-core3-*.json',
      pricingEligibility: { '<model>': { protocol: 'openai-responses|google-generate-content', eligible_tiers: ['documented protocol-eligible tier only'], omitted_tier: 'documented mapping; otherwise admission blocks', citations: ['exact official source/docs; parent review required'], responses_ws_prewarm: { eligible_models: ['exact documented legacy model IDs only'], generate_false_output_zero: true, cache_write: 'included_in_input', citations: ['official Responses WS guide plus earlier-model prompt-caching fee contract'], supported_prompt_cache_retention: ['in_memory', '24h'] } } },
      source: { pod: 'cliproxyapi-69d8bc8d88-mdzj5', podUid: '22039872-630b-4c56-bf61-d904cead0f47', arch: 'aarch64', sourceHash12: 'approved basename hash, never source file name or grant',
        projectionJq: 'omit for existing parser; otherwise exact separately user-approved /tmp tool path+official sha256' },
      authorization: {
        parent_new_binary_green: true, application_access_only_qa_passed: true, parent_security_review_passed: true,
        codex_default_url_qa_green: 'required for Codex', gateway_access_restored: true,
        allow_readonly_source_metadata: true, allow_readonly_access_projection: true,
        allow_local_oauth_import: true, allow_runtime_bootstrap_network: true, allow_native_subscription_generation: true,
        temporary_jq_user_approved: 'only after separate explicit approval for exact pod/tool path',
        serial_campaign_execution: true, confirmed_native_models: ['each selected native model'],
        parent_resources_notified: true, notified_root: 'exact plan.root', notified_ports: ['exact plan.ports'],
        notified_native_port: 'exact plan.observer_port', notified_provider_boundary_port: 'exact plan.provider_boundary_port (first runtime port +120)', allow_owned_provider_boundary: true, campaign_guard_sha256: 'fresh reconciled root guard SHA', revision_manifest_sha256: 'parent-supplied raw SHA256 of approved-core3-cap30-manifest.json; must equal the file bytes',
        parent_collector_ledgered: 'required for WS cases',
        runner_sha256: { 'approved-core3-subscription-contract-v30.mjs': 'reviewed SHA', 'approved-core3-subscription-source.mjs': 'reviewed SHA', 'approved-core3-subscription-native-v30.mjs': 'reviewed SHA', 'approved-core3-subscription-current-native.mjs': 'reviewed SHA', 'approved-core3-subscription-budget-v30.mjs': 'reviewed SHA', 'approved-core3-provider-boundary.mjs': 'reviewed SHA', 'approved-core3-api-key-controls-v30.mjs': 'reviewed SHA' },
      },
    },
    commands: [
      `bun ${BASE}/approved-core3-subscription-native-v30.mjs --plan --config <nonsecret-config.json>`,
      `bun ${BASE}/approved-core3-subscription-native-v30.mjs --source-preflight --config <nonsecret-config.json>`,
      `bun ${BASE}/approved-core3-subscription-native-v30.mjs --run --config <nonsecret-config.json>`,
      `bun ${BASE}/approved-core3-subscription-native-v30.mjs --evidence --config <nonsecret-config.json>`,
    ], source_blockers: BROKER_SOURCE_CONTRACT.current_blockers,
    functions: { source_preflight: 'sourcePreflight()', getaccess_memory_only: 'readBrokerAccess()', supported_import: 'prepareAccessOnlySubscriptionRuntime()', native_and_settlement: 'runNativeSubscription()', shared_native_adapter: 'buildGuardedNativeModule(plan, config, pinned, prepared, privateValues, outboundGuard)', root_financial_snapshot: 'rootFinancialSnapshot()', root_financial_minimum: 'rootFinancialMinimum(snapshot)', sealed_reference_reconciliation: 'reconcileReferenceReservation({record,guard,providerBoundaries,nativeCommandFinished,observedCostMicros})' },
    pinned_run_sealed_zero: { loader: 'loadPinnedRunSealedZeroEvidence(actualBinding)', validator: 'validatePinnedRunSealedZeroEvidence(record, loaded)', operator: 'applyPinnedRunSealedZeroClosure(binding, {reservationPath, expectedPreimageSha256, expectedSnapshot, ledgerPath})', execution: 'Offline explicit Main-authorized operator only; normal run/plan never invokes it', effect: 'NO_POSSIBLE_PROVIDER_DISPATCH reference exposure only; original INCOMPLETE, native-finished false and unknown usage/invoice remain unchanged' },
    root_campaign_guard_contract: {
      file: 'campaignGuard under existing evidence/approved-core3-*.json', at: 'fresh ISO timestamp <=10 minutes',
      campaignCapMicros: 30000000, plannedSubscriptionSliceMaxMicros: 6000000,
      planned_slice_field_scope: 'Legacy field name denotes the selected current slice amount only; it does not assert a credential kind',
      conservativePriorMicros: 'exact rootFinancialMinimum(source_files).minimumMicros + validated historicalThoughtsReserve.micros',
      outstandingMaxOutboundMicros: 'exact sum of other currently unsealed dispatch-capable command bounds only; queued serial commands are not pre-reserved; each entry matches its creation slice_cap_micros3m or6m',
      plannedOutbounds: [], historicalThoughtsReserve: 'optional exact sum from SHA-pinned original campaign-safety-reconciliation-before-traces.json',
      totalReservedMicros: 'exact prior + outstanding + selected slice 6m, at most 25m',
      unknownProviderCostFullyReserved: true, withinCap: true,
      source_files: 'exact sorted map from rootFinancialSnapshot(); subscription and api-key reservation namespaces are both included',
      historical_cap_rule: 'Stored3m stays3m; absent legacy creation cap defaults3m; new6m must be explicit and consistent',
      bounded_unknown_rule: 'Only finished native command + genuine same-guard provider boundaries + durable generation seals + exhaustive unique counter/amount joins may replace the planned whole-slice reserve with spent+UNKNOWN+IN_FLIGHT; never synthesize zero or stop retained resources',
      provider_observation_scope: 'Actual provider HTTPS/WSS write attempts, not client request count; TLS receipts and configured-local endpoint deviation are separate; direct/default-URL bypass coverage is not claimed',
      hosted_tool_fees: 'Actual hosted/built-in types require independently proven fee and call-count bounds; unknown fee is pricing BLOCKED, never tool removal or payload/config weakening',
      required_config_authorization: 'campaign_guard_sha256', list_cost_is_not_invoice_or_subscription_funding: true,
    },
    websocket_collector: { otlpEndpoint: 'existing epoch http://127.0.0.1:<18563+offset>', capture: 'epoch/approved-core3-spans.json', authorization: 'parent_collector_ledgered=true', no_new_collector_created: true },
    pricing_references: {
      codex_0163_native_default_wire: 'https://raw.githubusercontent.com/openai/codex/rust-v0.163.0/codex-rs/protocol/src/openai_models.rs',
      codex_0163_default_override_tests: 'https://raw.githubusercontent.com/openai/codex/rust-v0.163.0/codex-rs/core/tests/suite/model_switching.rs',
      subscription_standard_vs_opt_in_fast: 'https://learn.chatgpt.com/docs/agent-configuration/speed',
      public_responses_omission_is_project_auto_not_subscription_proof: 'https://developers.openai.com/api/reference/resources/responses/methods/create',
      google_subscription_not_developer_priority_tariff: 'https://geminicli.com/docs/resources/quota-and-pricing/',
      google_private_counttokens_and_invoice_rates: 'UNKNOWN; do not invent local byte/token formulas, tier eligibility, or paid funding',
    },
    retained_resources: 'No automatic server/root cleanup. Parent ledger reservations precede creation; existing native runners record actual process/listener ownership. User cleanup signal required.',
  };
}

const cliEntryPath = process.argv[1] ? await realpath(path.resolve(process.argv[1])).catch(() => null) : null;
if (import.meta.url.startsWith('file:') && cliEntryPath !== null && cliEntryPath === await realpath(fileURLToPath(import.meta.url)).catch(() => null)) {
  const args = process.argv.slice(2);
  if (!args.length) console.log(JSON.stringify(subscriptionConfigContract(), null, 2));
  else {
    const [mode, flag, file] = args;
    must(args.length === 3 && ['--plan', '--source-preflight', '--run', '--evidence'].includes(mode) && flag === '--config', 'usage_subscription_native_mode_config');
    const config = await json(path.resolve(file));
    must(config && !Object.keys(config).some((key) => /token|secret|password|credential/i.test(key)), 'nonsecret_config_required');
    let output;
    try {
      if (mode === '--plan') output = await nativeSubscriptionPlan(config);
      else if (mode === '--source-preflight') output = await sourcePreflight({ ...config.source, kind: config.kind }, config.authorization);
      else if (mode === '--evidence') output = await subscriptionCampaignEvidence(config);
      else output = await runNativeSubscription(config);
      console.log(JSON.stringify(output, null, 2));
      if (output.state && output.state !== 'PASS' && mode === '--run') process.exitCode = 2;
    } catch (error) {
      const message = String(error?.message ?? '');
      console.error(message.startsWith('BLOCKED:') ? message : 'BLOCKED:subscription_command_failed_no_secret_details');
      process.exitCode = 2;
    }
  }
}
