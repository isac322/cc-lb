#!/usr/bin/env node
// Offline selected-native-case verification only; never dispatches, mutates a
// hold, reads credentials/SQLite, or starts a native client.
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { readFile, lstat, realpath } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { reconcileGuardedEvents } from './approved-core3-api-key-native-v30.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');
const integer = value => Number.isSafeInteger(value) && value >= 0;
const sha = value => typeof value === 'string' && /^[0-9a-f]{64}$/.test(value);
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
class EvidenceError extends Error {
  constructor(code, verdict = 'INCOMPLETE') { super(code); this.verdict = verdict; }
}
const requireEvidence = (ok, code, verdict) => { if (!ok) throw new EvidenceError(code, verdict); };
const readJson = async file => {
  try { return JSON.parse(await readFile(file, 'utf8')); }
  catch (error) {
    if (error.code === 'ENOENT') return null;
    throw new EvidenceError('evidence_json_unreadable');
  }
};
async function regularBytes(file) {
  const stat = await lstat(file).catch(() => null);
  requireEvidence(stat?.isFile() && !stat.isSymbolicLink(), 'evidence_regular_file_required');
  return readFile(file);
}
async function sameFile(actual, expected) {
  requireEvidence(typeof actual === 'string' && path.isAbsolute(actual), 'evidence_path_missing');
  const [a, b] = await Promise.all([realpath(actual).catch(() => null), realpath(expected).catch(() => null)]);
  requireEvidence(a !== null && a === b, 'evidence_path_identity_mismatch', 'FAIL');
  return regularBytes(expected);
}
async function hashRegular(file) {
  const stat = await lstat(file).catch(() => null);
  requireEvidence(stat?.isFile() && !stat.isSymbolicLink(), 'artifact_regular_file_required');
  const digest = createHash('sha256');
  for await (const chunk of createReadStream(file)) digest.update(chunk);
  return digest.digest('hex');
}
function inside(root, relative) {
  requireEvidence(typeof relative === 'string' && !path.isAbsolute(relative), 'relative_evidence_reference_required');
  const file = path.resolve(root, relative);
  const rel = path.relative(root, file);
  requireEvidence(rel !== '' && rel !== '..' && !rel.startsWith(`..${path.sep}`), 'evidence_reference_escape', 'FAIL');
  return file;
}

// collectorRun maps raw candidates to output, thoughts to reasoning (runner:1029).
// Explicit projection zero is accepted as observed by the producer; missing/null
// fields are never converted into zero. This is usage arithmetic, not new pricing.
export function expectedNativeUsage(usage) {
  if (!object(usage) || ![usage.input, usage.cached, usage.output, usage.reasoning].every(integer)) return null;
  const totalOutput = usage.output + usage.reasoning;
  if (usage.cached > usage.input || !Number.isSafeInteger(totalOutput)) return null;
  return { input_tokens: usage.input - usage.cached, cache_read_input_tokens: usage.cached, output_tokens: totalOutput, thinking_tokens: usage.reasoning };
}
export async function validateCompiledArtifact(binding, artifact, expectedSha) {
  requireEvidence(sha(expectedSha) && artifact?.expected_sha256 === expectedSha, 'compiled_artifact_argument_mismatch', 'FAIL');
  requireEvidence(object(binding) && binding.scope === 'FULL_COMMITTED_GIT_ARCHIVE' && binding.tracked_blob_count === 2087 && /^[0-9a-f]{40}$/.test(binding.source_commit ?? '') && /^[0-9a-f]{40}$/.test(binding.source_tree ?? ''), 'full_compiled_source_binding_missing');
  for (const key of ['compiled_witness', 'compiled_pair', 'source_index_before', 'source_index_after', 'source_archive', 'overlay_equivalence']) {
    const ref = binding[key];
    requireEvidence(typeof ref?.path === 'string' && path.isAbsolute(ref.path) && sha(ref.sha256), 'compiled_source_reference_missing');
    requireEvidence(await hashRegular(ref.path) === ref.sha256, 'compiled_source_reference_hash_mismatch', 'FAIL');
  }
  requireEvidence(await hashRegular(artifact.path) === expectedSha, 'immutable_binary_hash_mismatch', 'FAIL');
  const witness = await readJson(binding.compiled_witness.path);
  const pair = await readJson(binding.compiled_pair.path);
  for (const record of [witness, pair?.after]) {
    requireEvidence(record?.binary?.path === artifact.path && record.binary.sha256 === expectedSha && record.source_commit === binding.source_commit && record.source_tree === binding.source_tree && record.source_before?.tracked_blob_count === binding.tracked_blob_count, 'compiled_witness_identity_mismatch', 'FAIL');
    for (const [field, key] of [['source_before', 'source_index_before'], ['source_after', 'source_index_after'], ['archive', 'source_archive']]) {
      requireEvidence(record[field]?.path === binding[key].path && record[field]?.sha256 === binding[key].sha256, 'compiled_source_reference_identity_mismatch', 'FAIL');
    }
  }
  requireEvidence(pair.after.compiled_witness?.path === binding.compiled_witness.path && pair.after.compiled_witness.sha256 === binding.compiled_witness.sha256, 'compiled_pair_witness_mismatch', 'FAIL');
  const before = await readJson(binding.source_index_before.path);
  const after = await readJson(binding.source_index_after.path);
  for (const index of [before, after]) {
    requireEvidence(index?.source_commit === binding.source_commit && index.source_tree === binding.source_tree && Array.isArray(index.files) && index.files.length === binding.tracked_blob_count, 'compiled_source_index_identity_mismatch', 'FAIL');
  }
  const beforeRows = new Map(before.files.map(row => [row.path, row]));
  const afterRows = new Map(after.files.map(row => [row.path, row]));
  requireEvidence(beforeRows.size === binding.tracked_blob_count && afterRows.size === beforeRows.size, 'compiled_source_path_set_mismatch', 'FAIL');
  for (const [name, row] of beforeRows) {
    const post = afterRows.get(name);
    requireEvidence(sha(row.sha256) && post?.sha256 === row.sha256 && row.expected_git_blob_oid === row.actual_archive_git_blob_oid && post.git_blob_oid === row.expected_git_blob_oid, 'compiled_source_blob_mismatch', 'FAIL');
  }
  return { source_commit: binding.source_commit, source_tree: binding.source_tree, tracked_blob_count: binding.tracked_blob_count, binary_sha256: expectedSha, compiled_witness_sha256: binding.compiled_witness.sha256 };
}

async function verifyIdentity(epoch, expectedSha, summary) {
  requireEvidence(sha(expectedSha), 'expected_sha_invalid');
  const manifest = await readJson(path.join(epoch, 'fixed-epoch.json'));
  requireEvidence(manifest && object(manifest.binary), 'epoch_manifest_binary_missing');
  const selected = manifest.binary;
  const contractBytes = await regularBytes(path.join(epoch, 'contract.json'));
  requireEvidence(hash(contractBytes) === manifest.files?.['contract.json']?.sha256, 'epoch_contract_pin_mismatch', 'FAIL');
  const contract = JSON.parse(contractBytes);
  requireEvidence(contract.artifact?.path === selected.path && contract.artifact?.expected_sha256 === expectedSha && JSON.stringify(contract.compiled_artifact_binding) === JSON.stringify(manifest.compiled_artifact_binding), 'contract_compiled_binding_mismatch', 'FAIL');
  requireEvidence(selected.expected_sha256 === expectedSha && typeof selected.path === 'string' && path.isAbsolute(selected.path), 'epoch_binary_argument_mismatch', 'FAIL');
  const compiled = await validateCompiledArtifact(manifest.compiled_artifact_binding, selected, expectedSha);
  requireEvidence(summary.binary?.sha256 === expectedSha && summary.binary?.expected === expectedSha && summary.binary?.match === true, 'summary_sut_binary_mismatch', 'FAIL');
  requireEvidence(await realpath(summary.binary.path).catch(() => null) === await realpath(selected.path).catch(() => null), 'summary_binary_path_mismatch', 'FAIL');
  const producerPin = manifest.files?.['omp-gemini/run.mjs'];
  requireEvidence(sha(producerPin?.sha256), 'native_producer_pin_missing');
  requireEvidence(hash(await regularBytes(path.join(epoch, 'omp-gemini/run.mjs'))) === producerPin.sha256, 'native_producer_pin_mismatch', 'FAIL');
  const kernelName = 'approved-core3-api-key-native-v30.mjs';
  const kernelPin = manifest.files?.[kernelName]?.sha256;
  requireEvidence(sha(kernelPin) && hash(await regularBytes(path.join(path.dirname(fileURLToPath(import.meta.url)), kernelName))) === kernelPin, 'reconciliation_kernel_pin_mismatch', 'FAIL');
  return { sha256: expectedSha, producer_sha256: producerPin.sha256, selected_path: selected.path, compiled };
}

async function verifyCatalog(epoch, summary, wires) {
  const expectedPath = path.join(epoch, 'omp-gemini/runtime/price-catalog.json');
  const provenancePath = path.join(epoch, 'omp-gemini/runtime/price-catalog.provenance.json');
  const catalog = summary.costCatalog;
  requireEvidence(object(catalog) && catalog.available === true && catalog.installedByServer === true && catalog.modelPriced === true, 'native_catalog_not_installed');
  const bytes = await sameFile(catalog.path, expectedPath);
  const provenance = await readJson(provenancePath);
  requireEvidence(provenance?.source === 'official_default_price_catalog_url' && provenance.httpStatus === 200 && provenance.url === 'https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json', 'catalog_provenance_missing');
  requireEvidence(sha(provenance.sha256) && hash(bytes) === provenance.sha256 && catalog.provenance?.sha256 === provenance.sha256, 'catalog_hash_mismatch', 'FAIL');
  await sameFile(provenance.path, expectedPath);
  await sameFile(catalog.provenance.path, expectedPath);
  let entries;
  try { entries = JSON.parse(bytes); } catch { throw new EvidenceError('catalog_json_unreadable'); }
  for (const model of new Set(wires.map(wire => wire.model))) {
    requireEvidence(typeof model === 'string', 'wire_model_missing');
    const key = `gemini/${model}`;
    requireEvidence(catalog.prices?.[model]?.key === key && entries[key]?.litellm_provider === 'gemini', 'catalog_exact_native_key_mismatch', 'FAIL');
    const row = entries[key];
    requireEvidence([row.input_cost_per_token, row.output_cost_per_token].every(value => typeof value === 'number' && Number.isFinite(value) && value >= 0) && Number.isSafeInteger(row.max_input_tokens) && row.max_input_tokens > 0 && Number.isSafeInteger(row.max_output_tokens) && row.max_output_tokens > 0, 'catalog_rates_or_hard_bounds_missing');
  }
  return { path: expectedPath, sha256: provenance.sha256, origin: provenance.source };
}

export async function geminiProof(epochInput, expectedSha) {
  const epoch = path.resolve(epochInput);
  const base = { oracle_scope: 'selected_native_omp_gemini', money_recomputed: false, hold_mutations: 0, provider_calls: 0 };
  try {
    requireEvidence(sha(expectedSha), 'expected_sha_invalid');
    const summary = await readJson(path.join(epoch, 'omp-gemini/summary.json'));
    if (!summary) return { ...base, verdict: 'NOT_RUN', reason: 'native_summary_absent' };
    const binary = await verifyIdentity(epoch, expectedSha, summary);
    requireEvidence(Array.isArray(summary.runs) && summary.runs.length > 0, 'native_runs_absent');
    const wires = [];
    const events = new Map();
    const rawUsage = new Map();
    for (const run of summary.runs) {
      requireEvidence(Array.isArray(run.wire), 'native_wire_array_missing');
      const ref = inside(path.join(epoch, 'omp-gemini'), run.ref);
      const runArtifact = await readJson(ref);
      requireEvidence(runArtifact && Array.isArray(runArtifact.events?.summaries), 'run_committed_events_missing');
      requireEvidence(runArtifact.events.complete === true && runArtifact.events.invalidated === false && Array.isArray(runArtifact.events.drop_increases) && runArtifact.events.drop_increases.length === 0, 'native_event_observation_incomplete');
      requireEvidence(Array.isArray(runArtifact.tap), 'native_raw_tap_missing');
      for (const tap of runArtifact.tap) {
        if (tap.forwarded !== true || tap.injected_locally === true) continue;
        const id = tap.response?.request_id;
        requireEvidence(typeof id === 'string' && !rawUsage.has(id), 'duplicate_or_missing_raw_response_id', 'FAIL');
        rawUsage.set(id, tap.response);
      }
      for (const event of runArtifact.events.summaries) {
        requireEvidence(typeof event.request_id === 'string' && event.request_id.length > 0 && !events.has(event.request_id), 'duplicate_or_missing_committed_request_id', 'FAIL');
        events.set(event.request_id, event);
      }
      for (const wire of run.wire) {
        if (wire.forwarded === true && wire.generation === true && wire.injected !== true) wires.push(wire);
      }
    }
    requireEvidence(wires.length > 0, 'native_generation_rows_absent');
    const catalog = await verifyCatalog(epoch, summary, wires);
    const api = await readJson(path.join(epoch, 'approved-core3-api-key-omp-gemini-evidence.json'));
    requireEvidence(api?.case === 'api-key-omp-gemini' && api.binary_sha256 === expectedSha, 'api_evidence_binary_or_case_missing');
    await sameFile(api.native_or_control?.summary, path.join(epoch, 'omp-gemini/summary.json'));
    const outbound = api.cost?.outbound;
    requireEvidence(outbound?.campaign_usd_cap_micros === 30000000 && outbound.slice_usd_cap_micros === 6000000, 'v30_outbound_caps_missing_or_wrong', 'FAIL');
    requireEvidence(outbound.schema === 'outbound-budget-v1' && outbound.case === 'api-key-omp-gemini' && outbound.case_forward_cap === 16 && outbound.attempts <= 16 && outbound.bound_breach === false && outbound.unsafe_integer_cost_evidence === false && outbound.unsupported_cost_dimension === false && api.cost.settled === true, 'native_guard_cost_safety_incomplete');
    requireEvidence(summary.principalId && summary.keyId && summary.upstream_id && Array.isArray(summary.upstreams), 'summary_principal_scope_missing');
    const eventRows = [...events.values()];
    const recomputed = reconcileGuardedEvents(outbound, eventRows, summary);
    requireEvidence(recomputed.matched === true, 'guard_event_reconciliation_incomplete');
    requireEvidence(api.cost.reconciliation?.matched === true && api.cost.committed_catalog_cost_micros === recomputed.committed_catalog_micros && api.cost.reconciliation.committed_catalog_micros === recomputed.committed_catalog_micros, 'stored_reconciliation_mismatch', 'FAIL');
    const ids = new Set();
    const joined = [];
    let positiveThoughts = false;
    for (const wire of wires) {
      requireEvidence(typeof wire.requestId === 'string' && !ids.has(wire.requestId), 'duplicate_or_missing_native_wire_id', 'FAIL');
      ids.add(wire.requestId);
      const expected = expectedNativeUsage(wire.providerUsage);
      requireEvidence(expected, 'wire_usage_missing_or_invalid');
      requireEvidence(sha(wire.providerResponseIdSha256) && wire.providerResponseIdPresent === true, 'wire_provider_response_id_missing');
      const raw = rawUsage.get(wire.requestId);
      requireEvidence(raw && integer(raw.usage?.thoughtsTokenCount), 'raw_thoughts_observation_missing');
      requireEvidence(wire.providerUsage.input === raw.usage.promptTokenCount && wire.providerUsage.output === raw.usage.candidatesTokenCount && wire.providerUsage.reasoning === raw.usage.thoughtsTokenCount && wire.providerUsage.cached === (raw.usage.cachedContentTokenCount ?? 0), 'native_usage_projection_mismatch', 'FAIL');
      requireEvidence(Array.isArray(raw.response_ids) && raw.response_ids.length === 1 && hash(raw.response_ids[0]) === wire.providerResponseIdSha256, 'native_provider_response_id_mismatch', 'FAIL');
      const event = events.get(wire.requestId);
      requireEvidence(event, 'wire_committed_event_missing');
      requireEvidence(event.model === wire.model && event.principal_id === summary.principalId && event.key_id === summary.keyId && event.upstream_id === wire.upstreamId && event.upstream_id === summary.upstream_id, 'wire_event_identity_or_owner_mismatch', 'FAIL');
      requireEvidence(event.status === wire.httpStatus && event.status >= 200 && event.status < 300, 'wire_event_status_not_success');
      requireEvidence(Object.entries(expected).every(([key, value]) => event[key] === value), 'wire_candidates_plus_thoughts_usage_mismatch', 'FAIL');
      requireEvidence(integer(event.cost_usd_micros), 'committed_event_cost_missing');
      positiveThoughts ||= expected.thinking_tokens > 0;
      joined.push({ request_id: wire.requestId, model: wire.model, expected_usage: expected, event_cost_usd_micros: event.cost_usd_micros });
    }
    requireEvidence(ids.size === eventRows.length, 'unmatched_committed_generation_rows');
    return { ...base, verdict: 'PASS', binary, catalog, joined, reconciliation: recomputed, thoughts_observation: positiveThoughts ? 'POSITIVE_OBSERVED' : 'NONE_OBSERVED', positive_thoughts_coverage: positiveThoughts, guard_join_scope: 'Real producer aggregate guard reconciliation plus independent native requestId/event join; reservation IDs are not fabricated into request IDs', rule: 'wire.output=candidates; event.output_tokens=wire.output+wire.reasoning exactly once' };
  } catch (error) {
    return { ...base, verdict: error instanceof EvidenceError ? error.verdict : 'INCOMPLETE', reason: error instanceof EvidenceError ? error.message : 'oracle_evidence_unreadable' };
  }
}

export async function oracleEntry(argv) {
  if (argv.length !== 4 || argv[0] !== '--epoch' || argv[2] !== '--expected-sha') return { output: { verdict: 'INCOMPLETE', reason: 'invalid_argument' }, exitCode: 2 };
  const output = await geminiProof(argv[1], argv[3]);
  return { output, exitCode: output.verdict === 'PASS' ? 0 : 1 };
}
const cliEntryPath = process.argv[1] ? await realpath(path.resolve(process.argv[1])).catch(() => null) : null;
if (import.meta.url.startsWith('file:') && cliEntryPath !== null && cliEntryPath === await realpath(fileURLToPath(import.meta.url)).catch(() => null)) {
  const { output, exitCode } = await oracleEntry(process.argv.slice(2));
  process.stdout.write(`${JSON.stringify(output, null, 2)}\n`);
  process.exitCode = exitCode;
}
