#!/usr/bin/env node
// Metadata workflow only. Default planning performs reads; explicit preparation
// creates a fresh epoch after validation and durable resource registration.
// No application/client/collector launch, credential getter, or provider call.
import { createHash } from 'node:crypto';
import { readFile, writeFile, mkdir, lstat, realpath } from 'node:fs/promises';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { isDeepStrictEqual } from 'node:util';
import { validateCompiledArtifact } from './approved-core3-oracle-v30.mjs';
import { apiKeyCaseResources, APPROVED_PORT_OFFSETS, API_KEY_CASE_NAMES } from './approved-core3-api-key-native-v30.mjs';
import { validateApprovedOmpIdentity } from './approved-core3-subscription-native-v30.mjs';

export const ROOT = path.dirname(fileURLToPath(import.meta.url));
export const LEDGER_PATH = '/tmp/cc-lb-actual-cli-e2e-20261020-ledger.json';
export const OLD_SOURCE_PATHS = Object.freeze([
  'fixed-epoch.mjs', 'qa-runtime.mjs', 'contract.json', 'codex/run.mjs', 'omp-gemini/run.mjs',
  'agy/run.mjs', 'faults/run.mjs', 'faults/relay.mjs', 'fixed-afb27555110c-trace-r5/omp-gpt/run.mjs',
  'fixed-afb27555110c-r3/native-http-cancel.mjs', 'fixed-afb27555110c-r2/anthropic-regression.mjs',
  'trace-proof/verify-application-traces.mjs', 'trace-proof/otlp-receiver.mjs',
  'approved-core3-controls.mjs', 'approved-core3-oracle.mjs', 'approved-core3-collector.mjs',
  'approved-core3-api-key-native.mjs', 'approved-core3-api-key-gpt.mjs', 'approved-core3-api-key-controls.mjs',
  'approved-core3-subscription-native.mjs', 'approved-core3-subscription-budget.mjs', 'approved-core3-ws-blackbox.mjs',
  'approved-core3-api-key-tests.mjs', 'approved-core3-provider-boundary.mjs', 'approved-core3-provider-boundary-tests.mjs',
  'approved-core3-subscription-current-native.mjs', 'approved-core3-subscription-current-native-tests.mjs',
  'approved-core3-ws-blackbox-current-wire.mjs', 'approved-core3-local-wire-prepare.mjs',
]);
export const V30_SOURCE_PATHS = Object.freeze([
  'approved-core3-api-key-native-v30.mjs', 'approved-core3-subscription-native-v30.mjs',
  'approved-core3-subscription-contract-v30.mjs', 'approved-core3-subscription-budget-v30.mjs',
  'approved-core3-api-key-gpt-v30.mjs', 'approved-core3-api-key-controls-v30.mjs',
  'approved-core3-cap30-contract.json', 'approved-core3-cap30-manifest.json', 'approved-core3-cap30-tests.mjs',
  'approved-core3-oracle-v30.mjs', 'approved-core3-campaign-v30.mjs', 'approved-core3-campaign-v30-tests.mjs',
  'approved-core3-subscription-source.mjs',
]);
// Case inputs the API/control adapters read from the epoch beyond the source
// sets above: the omp-gpt runner's transitive cache-proof module and the
// omp-gpt fixture tree (prefix below). The bytes equal the trace-r5 sibling.
export const CASE_SOURCE_PATHS = Object.freeze(['omp-gpt/cache-proof.mjs']);
const FIXTURE_PREFIXES = Object.freeze(['omp-gemini/fixture/', 'omp-gpt/fixture/']);
// The superseded cap19 manifest is pinned by its preserved raw blob, never by
// the mutable active cap19 file.
export const HISTORICAL_CAP19_MANIFEST = Object.freeze({
  path: 'evidence/approved-core3-before-fixture-node-fix-cap19-manifest-sha256-b68b809aff3e8dcc5de2a65fca7021cd9489021e0cec7d8a3d60620c1e7bc1e3.json',
  sha256: 'b68b809aff3e8dcc5de2a65fca7021cd9489021e0cec7d8a3d60620c1e7bc1e3',
});
const MANIFEST = 'approved-core3-cap30-manifest.json';
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const must = (ok, code) => { if (!ok) throw new Error(`BLOCKED:${code}`); };
const isHash = value => typeof value === 'string' && /^[0-9a-f]{64}$/.test(value);
const exists = async file => { try { await lstat(file); return true; } catch (error) { if (error.code === 'ENOENT') return false; throw error; } };
function once(text, from, to, label) {
  must(text.split(from).length === 2, `source_anchor_drift:${label}`);
  return text.replace(from, to);
}
async function readRegular(file) {
  const stat = await lstat(file);
  must(stat.isFile() && !stat.isSymbolicLink(), 'source_must_be_regular');
  return readFile(file);
}
// --case (an API CASES key) and --port-offset (an approved offset) are required:
// the epoch's selected target and its probed port bundle come only from them.
export function parseArgs(argv) {
  const options = { prepare: false };
  const flags = { '--expected-sha': 'expectedSha', '--attempt-label': 'label', '--paid-contract': 'paidContract', '--paid-contract-sha': 'paidContractSha', '--revision-manifest-sha': 'revisionManifestSha', '--campaign-guard': 'campaignGuard', '--campaign-guard-sha': 'campaignGuardSha', '--case': 'caseName', '--port-offset': 'portOffset' };
  const seen = new Set();
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--prepare') { options.prepare = true; continue; }
    const key = flags[argv[i]];
    must(key && typeof argv[i + 1] === 'string' && !argv[i + 1].startsWith('--') && !seen.has(key), 'invalid_argument');
    seen.add(key);
    options[key] = argv[++i];
  }
  must(isHash(options.expectedSha) && isHash(options.paidContractSha) && isHash(options.revisionManifestSha), 'explicit_input_sha_required');
  must(typeof options.paidContract === 'string' && path.isAbsolute(options.paidContract), 'explicit_paid_contract_required');
  must(/^[a-z0-9][a-z0-9-]{0,11}$/.test(options.label ?? ''), 'safe_epoch_label_required');
  must(options.portOffset === undefined || /^[0-9]{1,5}$/.test(options.portOffset), 'port_offset_not_approved');
  if (options.portOffset !== undefined) options.portOffset = Number(options.portOffset);
  selectedResources(options);
  return options;
}
// The selected case's resources at the selected offset, from the API plan's
// own resource helper (single source of the port formulas).
export function selectedResources(options) {
  must(options.caseName !== undefined, 'case_required');
  must(API_KEY_CASE_NAMES.includes(options.caseName), 'unknown_case');
  must(options.portOffset !== undefined, 'port_offset_required');
  must(APPROVED_PORT_OFFSETS.includes(options.portOffset), 'port_offset_not_approved');
  return apiKeyCaseResources(options.caseName, options.portOffset);
}
// Every listener the selected case needs, proxy first: SUT ports, its native
// observer/relay/tap and provider boundary, and the epoch collector pair.
export function selectedBundlePorts(resources) {
  return [...new Set([...resources.ports, ...resources.auxiliary_ports, resources.collector.port, resources.collector.control_port])];
}

async function loadInputs(options, deps) {
  const root = deps.sourceRoot ?? ROOT;
  const manifestBytes = await readRegular(path.join(root, MANIFEST));
  must(hash(manifestBytes) === options.revisionManifestSha, 'revision_manifest_hash_mismatch');
  const revision = JSON.parse(manifestBytes);
  must(revision.new_campaign_cap_micros === 30000000 && revision.previous_campaign_cap_micros === 25000000 && revision.historical_campaign_cap_micros === 15000000 && revision.selected_slice_cap_micros === 6000000 && revision.historical_unknown_holds_immutable === true && typeof revision.execution_release === 'boolean', 'revision_policy_mismatch');
  const previous = revision.historical_cap19_manifest;
  must(previous && previous.path === HISTORICAL_CAP19_MANIFEST.path && previous.sha256 === HISTORICAL_CAP19_MANIFEST.sha256 && hash(await readRegular(path.join(root, previous.path))) === previous.sha256, 'historical_cap19_manifest_pin_mismatch');
  const required = new Set([...OLD_SOURCE_PATHS, ...V30_SOURCE_PATHS, ...CASE_SOURCE_PATHS]);
  for (const rel of Object.keys(revision.source_sha256 ?? {})) {
    must(!path.isAbsolute(rel) && !rel.split('/').includes('..') && rel !== MANIFEST, 'invalid_manifest_source_path');
    // Extra source inputs are limited to the selected immutable fixture tree.
    must(required.has(rel) || FIXTURE_PREFIXES.some(prefix => rel.startsWith(prefix)), 'unapproved_source_input');
    required.add(rel);
  }
  const source = { [MANIFEST]: manifestBytes.toString('utf8') };
  for (const rel of required) {
    if (rel === MANIFEST) continue;
    const bytes = await readRegular(path.join(root, rel));
    must(isHash(revision.source_sha256?.[rel]) && hash(bytes) === revision.source_sha256[rel], `source_hash_mismatch:${rel}`);
    source[rel] = bytes.toString('utf8');
  }
  for (const prefix of FIXTURE_PREFIXES) must(Object.keys(source).some(rel => rel.startsWith(prefix)), `case_fixture_pins_missing:${prefix}`);
  return { root, source, revision, manifestSha: hash(manifestBytes) };
}
// Top-level keys a cap30 contract may add to the original contract. Every other
// addition is refused; every original field stays deepEqual (below).
const ADDED_CONTRACT_KEYS = Object.freeze(['revision_status', 'execution_release', 'historical_artifact_reference', 'compiled_artifact_binding', 'api_key_pricing_eligibility', 'approved_client_identity', 'credential_binding']);
const OPENAI_DOCS = /^https:\/\/developers\.openai\.com\/api\/(docs|reference)\//;
const exactKeys = (value, keys) => value && typeof value === 'object' && !Array.isArray(value) && isDeepStrictEqual(Object.keys(value).sort(), [...keys].sort());
// Sourced OMP-GPT gpt-5.2 eligibility for this project's API key only. The
// user confirmed the project is Standard (no Scale Tier/FedRAMP), so an omitted
// service_tier resolves to 'default' here. It is an account-scoped fact, never
// a universal API default. The models entry is exactly the budget guard's
// pricingEligibility[model] shape; the guard still checks catalog prices.
export function validateApiKeyPricingEligibility(value) {
  must(exactKeys(value, ['schema', 'account_scope', 'models']) && value.schema === 'approved-core3-api-key-pricing-eligibility-v1', 'api_key_pricing_eligibility_shape');
  const scope = value.account_scope;
  must(exactKeys(scope, ['credential_env', 'basis', 'omitted_service_tier_resolves_to', 'scope', 'universal_default_claimed', 'key_value_read', 'confirmed_at'])
    && scope.credential_env === 'OPENAI_BACKEND_API_KEY' && scope.basis === 'user_confirmed_project_standard_payg_scale_no_fedramp'
    && scope.omitted_service_tier_resolves_to === 'default' && scope.scope === 'this_project_only'
    && scope.universal_default_claimed === false && scope.key_value_read === false
    && typeof scope.confirmed_at === 'string' && Number.isFinite(Date.parse(scope.confirmed_at)), 'api_key_pricing_eligibility_account_scope');
  must(exactKeys(value.models, ['gpt-5.2']), 'api_key_pricing_eligibility_models_gpt52_only');
  const entry = value.models['gpt-5.2'];
  must(exactKeys(entry, ['protocol', 'eligible_tiers', 'omitted_tier', 'citations']) && entry.protocol === 'openai-responses'
    && isDeepStrictEqual(entry.eligible_tiers, ['default']) && entry.omitted_tier === 'default', 'api_key_pricing_eligibility_gpt52_standard_only');
  must(Array.isArray(entry.citations) && entry.citations.length > 0 && new Set(entry.citations).size === entry.citations.length
    && entry.citations.every((citation) => typeof citation === 'string' && citation.length > 0 && citation.length <= 4096)
    && ['models/gpt-5.2', 'pricing'].every((page) => entry.citations.includes(`https://developers.openai.com/api/docs/${page}`))
    && entry.citations.some((citation) => !OPENAI_DOCS.test(citation) && citation.startsWith('User-confirmed OPENAI_BACKEND_API_KEY')), 'api_key_pricing_eligibility_citations');
  return value;
}
async function loadSelected(options, inputs) {
  const contractBytes = await readRegular(options.paidContract);
  must(hash(contractBytes) === options.paidContractSha, 'paid_contract_hash_mismatch');
  const contract = JSON.parse(contractBytes);
  must(isDeepStrictEqual(contract, JSON.parse(inputs.source['approved-core3-cap30-contract.json'])), 'selected_contract_not_revision_contract');
  must(typeof contract.execution_release === 'boolean', 'execution_release_bit_missing_or_invalid');
  must(contract.execution_release === inputs.revision.execution_release, 'execution_release_mismatch');
  const old = JSON.parse(inputs.source['contract.json']);
  for (const [key, value] of Object.entries(old)) {
    if (['artifact', 'paid_bounds', 'paid_attempt_overrides'].includes(key)) continue;
    must(isDeepStrictEqual(contract[key], value), `original_contract_field_changed:${key}`);
  }
  for (const key of Object.keys(contract)) must(Object.hasOwn(old, key) || ADDED_CONTRACT_KEYS.includes(key), `unapproved_contract_field_added:${key}`);
  if (Object.hasOwn(contract, 'api_key_pricing_eligibility')) validateApiKeyPricingEligibility(contract.api_key_pricing_eligibility);
  if (Object.hasOwn(contract, 'approved_client_identity')) validateApprovedOmpIdentity(contract.approved_client_identity);
  if (Object.hasOwn(contract, 'credential_binding')) {
    const binding = contract.credential_binding;
    must(binding && binding.source_type === 'envFile' && binding.env_file === '/data/workspace/projects/runbear/plugbear/.env' && binding.env_key === 'OPENAI_BACKEND_API_KEY' && binding.value_read === false, 'credential_binding_invalid');
  }
  const newBounds = { ...contract.paid_bounds, usd_ceiling_campaign: old.paid_bounds?.usd_ceiling_campaign, enforcement: old.paid_bounds?.enforcement };
  must(isDeepStrictEqual(newBounds, old.paid_bounds) && contract.paid_bounds.usd_ceiling_campaign === 30, 'revision_bounds_changed');
  must(contract.paid_attempt_overrides?.codex?.max_generation_attempts === 32 && contract.paid_attempt_overrides.codex.usd_ceiling === 3, 'original_codex_caps_changed');
  const artifact = contract.artifact;
  must(artifact && typeof artifact.path === 'string' && path.isAbsolute(artifact.path) && isHash(artifact.expected_sha256), 'artifact_reference_invalid');
  let compiled = null;
  if (contract.compiled_artifact_binding) compiled = await validateCompiledArtifact(contract.compiled_artifact_binding, artifact, options.expectedSha);
  return { contract, artifact, compiled, artifactPending: !compiled, contractSha: hash(contractBytes) };
}

// Reuse the original fixed-epoch transform functions, not a second port/anchor algorithm.
export async function fixedTransforms(sourceText, root, offset) {
  must(APPROVED_PORT_OFFSETS.includes(offset), 'port_offset_not_approved');
  let text = once(sourceText, 'const ROOT = path.dirname(fileURLToPath(import.meta.url));', `const ROOT = ${JSON.stringify(root)};`, 'fixed_root');
  text = once(text, "main().catch((error) => {\n  console.error(`fixed-epoch: ${error.message}`);\n  process.exit(2);\n});", 'export { buildHelper, validateCredentialDescriptor };\nexport function setOffset(value) { PORT_OFFSET = value; }', 'fixed_exports');
  const module = await import(`data:text/javascript;base64,${Buffer.from(text).toString('base64')}`);
  module.setOffset(offset);
  return module;
}
// Case role files the API/control adapters read from the epoch. These are the
// original core3 relationships and transforms (approved-core3-campaign.mjs
// :136-149, :212-215), applied with this epoch's offset; the adapters'
// freshPortDeclarations and once() anchors expect exactly this shape.
// The trace-r5 omp-gpt runner pins its own historical binary, the original
// core3 OLD_BINARY (approved-core3-campaign.mjs:14), not contract.json's artifact.
export const TRACE_RUNNER_BINARY = 'afb27555110c2072a6997adc76901c9d3d7cb0db1265a02496f22621d0b420cb';
export function caseRoleOutputs(source, { historicalSha, expectedSha, offset, campaignGuard }) {
  must(isHash(historicalSha) && isHash(expectedSha) && Number.isSafeInteger(offset) && path.isAbsolute(campaignGuard ?? ''), 'case_role_inputs_required');
  const traceRunner = source['fixed-afb27555110c-trace-r5/omp-gpt/run.mjs'];
  must(traceRunner.includes(historicalSha), 'source_anchor_drift:trace_native_binary_anchor_missing');
  const faults = once(source['faults/run.mjs'],
    "  if (raw !== '0' && raw !== '100') throw new Error(`QA_FAULT_PORT_OFFSET must be 0 or 100 (got ${raw})`);",
    `  if (raw !== '${offset}') throw new Error('QA_FAULT_PORT_OFFSET must match approved core3 epoch');`, 'fresh_faults_port_offset');
  let cancel = source['fixed-afb27555110c-r3/native-http-cancel.mjs'];
  cancel = once(cancel, 'const DRIVER_PORTS = [18550, 18551, 18552];', 'const DRIVER_PORTS = CONCURRENT_MODE ? [18590, 18591, 18592] : [18550, 18551, 18552];', 'cancel_disjoint_modes');
  cancel = once(cancel, 'const RELAY_PORT = 18853;', `const RELAY_PORT = CONCURRENT_MODE ? ${18593 + offset} : ${18553 + offset};`, 'cancel_relay_port');
  cancel = once(cancel, 'const TAP_PORT = 18854;', `const TAP_PORT = CONCURRENT_MODE ? ${18594 + offset} : ${18554 + offset};`, 'cancel_tap_port');
  cancel = once(cancel, "const CAMPAIGN_GUARD = path.join(SOURCE_ROOT, 'evidence', 'campaign-safety-reconciliation-before-traces.json');", `const CAMPAIGN_GUARD = ${JSON.stringify(campaignGuard)};`, 'fresh_cancel_campaign_guard');
  const anthropic = once(source['fixed-afb27555110c-r2/anthropic-regression.mjs'], 'ports: [18750, 18751, 18752]', `ports: [${18570 + offset}, ${18571 + offset}, ${18572 + offset}]`, 'anthropic_ports');
  return {
    'omp-gpt/run.mjs': { text: traceRunner.split(historicalSha).join(expectedSha), sourceRel: 'fixed-afb27555110c-trace-r5/omp-gpt/run.mjs', transforms: ['new binary sha; preserve proven trace-aware native runner'] },
    'faults/run.mjs': { text: faults, sourceRel: 'faults/run.mjs', transforms: ['new owned epoch port offset; original 16-forward and USD guards unchanged'] },
    'approved-core3-native-http-cancel.mjs': { text: cancel, sourceRel: 'fixed-afb27555110c-r3/native-http-cancel.mjs', transforms: ['new owned relay/tap ports', 'fresh external campaign guard required'] },
    'approved-core3-anthropic-regression.mjs': { text: anthropic, sourceRel: 'fixed-afb27555110c-r2/anthropic-regression.mjs', transforms: ['new epoch ports; unchanged native Anthropic oracle'] },
  };
}
async function finance(options, inputs) {
  if (!options.campaignGuard) return null;
  must(isHash(options.campaignGuardSha), 'root_finance_guard_sha_required');
  const module = await import(pathToFileURL(path.join(inputs.root, 'approved-core3-subscription-native-v30.mjs')).href);
  return module.verifyRootCampaignGuard({ campaignGuard: options.campaignGuard, sliceCostUsd: 6, authorization: { campaign_guard_sha256: options.campaignGuardSha } });
}
export async function planCampaign(options, deps = {}) {
  must(isHash(options.expectedSha) && isHash(options.paidContractSha) && isHash(options.revisionManifestSha), 'explicit_input_sha_required');
  must(/^[a-z0-9][a-z0-9-]{0,11}$/.test(options.label ?? ''), 'safe_epoch_label_required');
  const resources = selectedResources(options);
  const inputs = await loadInputs(options, deps);
  const selected = await loadSelected(options, inputs);
  // setOffset and buildHelper run back to back (no await between them).
  const transforms = await fixedTransforms(inputs.source['fixed-epoch.mjs'], inputs.root, resources.port_offset);
  const original = JSON.parse(inputs.source['contract.json']);
  const helper = transforms.buildHelper(inputs.source['qa-runtime.mjs'], original.artifact.expected_sha256, options.expectedSha);
  const rootGuard = await finance(options, inputs);
  const roles = caseRoleOutputs(inputs.source, { historicalSha: TRACE_RUNNER_BINARY, expectedSha: options.expectedSha, offset: resources.port_offset, campaignGuard: options.campaignGuard ?? path.join(inputs.root, 'evidence', 'approved-core3-campaign-safety.json') });
  const outputBase = deps.outputRoot ?? ROOT;
  const collectorBase = deps.collectorRoot ?? deps.outputRoot ?? ROOT.replace(/^\/private\/tmp\//, '/tmp/');
  if (path.resolve(outputBase) !== path.resolve(collectorBase)) must(await realpath(outputBase) === await realpath(collectorBase), 'collector_root_identity_mismatch');
  const epochRoot = path.join(outputBase, `fixed-${options.expectedSha.slice(0, 12)}-core3-v30-${options.label}`);
  const collectorEpochRoot = path.join(collectorBase, path.basename(epochRoot));
  const sourceDescriptors = Object.fromEntries(Object.entries(inputs.source).sort(([a], [b]) => a.localeCompare(b)).map(([rel, text]) => [rel, { source: path.join(inputs.root, rel), source_sha256: hash(text) }]));
  const sourceHashes = Object.fromEntries(Object.entries(sourceDescriptors).filter(([rel]) => rel !== MANIFEST).map(([rel, descriptor]) => [rel, descriptor.source_sha256]));
  const plan = {
    kind: 'approved_core3_v30_sha_pinned_parent_campaign', state: 'NOT_RUN', mode: 'METADATA_ONLY', prepared: false,
    // selected_case is the preparation target whose bundle prepare probes. The
    // epoch is a common source/artifact snapshot with every role file; this field
    // is not a dispatch allowlist and grants no run permission.
    selected_case: resources.case, epoch_root: epochRoot, collector_epoch_root: collectorEpochRoot, port_offset: resources.port_offset,
    ports: { proxy: resources.ports[0], admin: resources.ports[1], metrics: resources.ports[2], observer: resources.observer_port, auxiliary: [...resources.auxiliary_ports], provider_boundary: resources.provider_boundary_port, collector: resources.collector.port, collector_control: resources.collector.control_port },
    probed_ports: selectedBundlePorts(resources),
    scope: { selected_cases: [resources.case], other_cases: 'PENDING; not executed by this metadata workflow' },
    binary: selected.artifactPending ? null : selected.artifact, compiled_artifact_binding: selected.contract.compiled_artifact_binding ?? null,
    historical_binary_reference: original.artifact, final_artifact_pending: selected.artifactPending,
    artifact_status: selected.artifactPending ? 'PENDING_FINAL_ARTIFACT' : selected.contract.execution_release ? 'FINAL_ARTIFACT_BOUND_POLICY_ELIGIBLE' : 'FINAL_ARTIFACT_BOUND_EXECUTION_PENDING',
    execution_release: selected.contract.execution_release,
    execution_release_semantics: 'Artifact/policy eligibility only; not a dispatch instruction, deployment release, performance result, or waiver of per-run authorization/finance/pricing/TLS gates.',
    source_sha256: sourceHashes, source_descriptors: sourceDescriptors, revision_manifest_sha256: inputs.manifestSha,
    selected_paid_contract: { path: options.paidContract, sha256: selected.contractSha },
    caps: { effective_campaign_usd: 30, selected_slice_usd: 6, previous_campaign_usd: 25, historical_source_campaign_usd: 15, historical_source_slice_usd: 3, common_generation_forwards: 16, turn_timeout_seconds: 120 },
    user_campaign_scope: { objective: original.objective, slices: original.slices, assertions: original.required_evidence_fields, completion_rule: original.completion_rule },
    required_selected_scenarios: [...resources.required_original_scenarios],
    pending_campaign_cases: [...API_KEY_CASE_NAMES.filter((name) => name !== resources.case), 'subscription baselines', 'other required cross-cutting assertions'],
    codex_ws_oracle_for_selected_case: resources.case === 'api-key-codex' ? 'REQUIRED_TRUSTED_SPANS' : 'NOT_APPLICABLE',
    financial_guard: rootGuard ?? { state: 'REQUIRED_BEFORE_PREPARE', historical_unknowns_immutable: true },
    commands: {
      plan: `bun ${ROOT}/approved-core3-api-key-native-v30.mjs --plan --config ${ROOT}/evidence/approved-core3-${resources.case}-v30-config.json`,
      run: `bun ${ROOT}/approved-core3-api-key-native-v30.mjs --run --config ${ROOT}/evidence/approved-core3-${resources.case}-v30-config.json`,
      collector: `node ${ROOT}/approved-core3-collector.mjs --epoch ${collectorEpochRoot} --port ${resources.collector.port} --control-port ${resources.collector.control_port} --output ${collectorEpochRoot}/approved-core3-spans.json`,
      oracle: `node ${ROOT}/approved-core3-oracle-v30.mjs --epoch ${epochRoot} --expected-sha ${options.expectedSha}`,
    },
    output_hashes: { 'qa-runtime.mjs': { source_sha256: hash(inputs.source['qa-runtime.mjs']), sha256: hash(helper.text), transforms: helper.transforms },
      ...Object.fromEntries(Object.entries(roles).map(([rel, role]) => [rel, { source: role.sourceRel, source_sha256: hash(inputs.source[role.sourceRel]), sha256: hash(role.text), transforms: role.transforms }])) },
    credentials_accessed: false, provider_calls: 0,
  };
  return { plan, inputs, selected, helper, transforms, roles };
}
export async function portFree(port) {
  return new Promise(resolve => {
    const socket = net.connect({ host: '127.0.0.1', port });
    socket.setTimeout(500);
    socket.once('connect', () => { socket.destroy(); resolve(false); });
    socket.once('error', error => { socket.destroy(); resolve(error.code === 'ECONNREFUSED'); });
    socket.once('timeout', () => { socket.destroy(); resolve(false); });
  });
}
async function ledgerRoot(root, ledgerPath) {
  const ledger = JSON.parse(await readFile(ledgerPath, 'utf8'));
  ledger.entries.push({ created_at: new Date().toISOString(), status: 'reserved', owner_slice: 'CoreQaHarnessOwner-v30-workflow', kind: 'scratch_epoch_root', id: root, delete: `After explicit cleanup approval and ownership verification: rm -rf -- '${root}'` });
  await writeFile(ledgerPath, `${JSON.stringify(ledger, null, 2)}\n`);
}
export async function prepareCampaign(options, deps = {}) {
  const result = await planCampaign(options, deps);
  const { plan, inputs, selected, helper, transforms, roles } = result;
  must(!plan.final_artifact_pending, 'v30_final_artifact_pending');
  must(plan.financial_guard.sha256 && options.campaignGuard, 'fresh_canonical_root_finance_required');
  must(!(await exists(plan.epoch_root)), 'epoch_already_exists');
  for (const port of plan.probed_ports) must(await (deps.portFree ?? portFree)(port), `port_in_use:${port}`);
  // Locator-only validation is reused from fixed-epoch, only on explicit prepare.
  const locator = await transforms.validateCredentialDescriptor();
  // Re-read every source and the fresh global financial guard before mutation.
  await loadInputs(options, deps);
  await finance(options, inputs);
  const root = plan.epoch_root;
  await ledgerRoot(root, deps.ledgerPath ?? LEDGER_PATH);
  await mkdir(root, { mode: 0o700 });
  const manifest = { epoch: path.basename(root), binary: selected.artifact, compiled_artifact_binding: selected.contract.compiled_artifact_binding, port_offset: plan.port_offset, launched: null, files: {}, approved_core3: plan };
  const roleTexts = Object.fromEntries(Object.entries(roles).map(([rel, role]) => [rel, role.text]));
  const output = { ...inputs.source, ...roleTexts, 'qa-runtime.mjs': helper.text, 'contract.json': JSON.stringify(selected.contract, null, 2) + '\n', 'credentials.json': locator };
  for (const [rel, text] of Object.entries(output)) {
    const file = path.join(root, rel);
    await mkdir(path.dirname(file), { recursive: true, mode: 0o700 });
    await writeFile(file, text, { mode: 0o600, flag: 'wx' });
    const sourceRel = rel === 'contract.json' ? 'approved-core3-cap30-contract.json' : roles[rel]?.sourceRel ?? rel;
    manifest.files[rel] = { source: path.join(inputs.root, sourceRel), source_sha256: hash(rel === 'credentials.json' ? locator : inputs.source[sourceRel]), sha256: hash(text), transforms: rel === 'qa-runtime.mjs' ? helper.transforms : roles[rel]?.transforms ?? [] };
  }
  const prior = plan.financial_guard;
  const baseline = { slice: 'prior-campaign-baseline', attempts: 0, costUsd: (prior.conservativePriorMicros + prior.outstandingMaxOutboundMicros) / 1000000, costKnown: false, unpricedRequestIds: [], basis: 'Canonical prior catalog exposure including fully reserved UNKNOWN/in-flight; not new spend or invoice proof', root_guard: prior, historical_aggregate_only: true };
  await mkdir(path.join(root, 'prior-campaign-baseline'), { mode: 0o700 });
  await writeFile(path.join(root, 'prior-campaign-baseline/budget.json'), JSON.stringify(baseline, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
  await writeFile(path.join(root, 'fixed-epoch.json'), JSON.stringify(manifest, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
  await writeFile(path.join(root, 'approved-core3-campaign-v30.json'), JSON.stringify(plan, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
  return { ...result, plan: { ...plan, prepared: true, state: 'PREPARED_METADATA_ONLY' }, prepared: { root, manifest } };
}
const cliEntryPath = process.argv[1] ? await realpath(path.resolve(process.argv[1])).catch(() => null) : null;
if (import.meta.url.startsWith('file:') && cliEntryPath !== null && cliEntryPath === await realpath(fileURLToPath(import.meta.url)).catch(() => null)) {
  try {
    const options = parseArgs(process.argv.slice(2));
    const { plan } = options.prepare ? await prepareCampaign(options) : await planCampaign(options);
    process.stdout.write(JSON.stringify(plan, null, 2) + '\n');
  } catch (error) {
    const code = typeof error?.message === 'string' && /^BLOCKED:[a-z0-9_:/.-]{1,256}$/i.test(error.message) ? error.message : 'BLOCKED:workflow_input_invalid';
    process.stdout.write(JSON.stringify({ kind: 'approved_core3_v30_sha_pinned_parent_campaign', state: 'BLOCKED', mode: 'METADATA_ONLY', prepared: false, code, credentials_accessed: false, provider_calls: 0 }) + '\n');
    process.exitCode = 2;
  }
}
