#!/usr/bin/env node
// Family boundaries only. Q-HARNESS owns canonical endpoint/transport coverage.
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ISSUE = /^https:\/\/github\.com\/isac322\/cc-lb\/issues\/[1-9]\d*$/;
const REFERENCE = /^(?:https:\/\/github\.com\/[\w.-]+\/[\w.-]+\/(?:blob|tree|issues|pull)\/[^\s?#]+(?:#[\w.=-]+)?|(?:docs|tests)\/[\w./-]+(?:#[\w.-]+)?)$/;
const PINS = {
  codex: { version: '0.169.0', revision: 'd32063f96c2dff1dc96af839b3952abb6acfeda4' },
  agy: { version: '1.5.9', sha256: 'fad3628ad91fe8613a72f074a1c5fde7cd816517788a5b9be96703f7d256741a' },
};
// Required families are not Q-HARNESS surface IDs. Splitting credential modes
// keeps an API-key control from discharging subscription acceptance.
export const REQUIRED_FAMILIES = Object.freeze({
  'codex-model': [1307, 'feature'],
  'codex-auxiliary': [1309, 'feature'],
  'codex-account-workspace': [1313, 'feature'],
  'codex-apps-hosted-mcp': [1314, 'feature'],
  'codex-authenticated-plugins': [1314, 'feature'],
  'codex-files': [1315, 'feature'],
  'codex-cloud-tasks': [1316, 'feature'],
  'codex-remote-enrollment': [1317, 'feature'],
  'codex-vendor-oauth': [1318, 'direct'],
  'codex-telemetry-feedback-statsig': [1318, 'direct'],
  'codex-update-install-pet-assets': [1318, 'direct'],
  'codex-github-content-archives': [1318, 'direct'],
  'codex-signed-storage': [1315, 'signed-storage'],
  'codex-internal-thread-service': [1318, 'internal'],
  'codex-cloud-model-preview': [1318, 'internal'],
  'codex-agent-identity': [1318, 'internal'],
  'codex-configured-remote-environment': [1318, 'configured'],
  'codex-message-board': [1318, 'configured'],
  'codex-external-mcp-oauth': [1318, 'configured'],
  'codex-configured-otel': [1318, 'configured'],
  'codex-subscription-voice': [1318, 'feature'],
  'agy-native-api-key': [1308, 'feature'],
  'agy-native-oauth': [1308, 'feature'],
  'agy-raw-code-assist': [1319, 'feature'],
  'agy-code-assist-controls': [1320, 'feature'],
  'agy-remote-agent': [1321, 'feature'],
  'agy-remote-control': [1323, 'feature'],
  'agy-subscription-voice': [1322, 'feature'],
  'agy-local-microphone': [1322, 'local'],
  'agy-configured-analytics': [1323, 'configured'],
  'agy-vendor-oauth': [1318, 'direct'],
});

const object = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const text = (value) => typeof value === 'string' && value.trim().length > 0;
const issue = (number) => `https://github.com/isac322/cc-lb/issues/${number}`;

// Do not echo offending values: a rejected input might itself contain a secret.
function scanPrivateData(value, errors, location = 'ledger') {
  if (typeof value === 'string') {
    // Credential-custody prose names bearer credentials/tokens without supplying
    // a value. Match actual scheme values, not those descriptive nouns.
    if (/(?:\bBearer\s+(?!(?:credentials?|tokens?)(?:\s|[.,;:]|$))[A-Za-z0-9._~-]{8,}|\bsk-[A-Za-z0-9_-]{12,}|\bya29\.[A-Za-z0-9_-]+|\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+|-----BEGIN [A-Z ]*PRIVATE KEY-----|[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}|\/(?:Users|home|data\/data)\/|[?&](?:key|token|access_token|refresh_token|signature|x-amz-signature|x-goog-signature)=)/i.test(value)) {
      errors.push(`${location}: credential or private data is forbidden`);
    }
  } else if (Array.isArray(value)) {
    value.forEach((entry, index) => scanPrivateData(entry, errors, `${location}[${index}]`));
  } else if (object(value)) {
    for (const [key, entry] of Object.entries(value)) {
      if (/^(?:token|access_token|refresh_token|api_key|password|private_key|client_secret|authorization|cookie|signed_url|email|account_id|workspace_id|principal_id|session_id|device_id)$/i.test(key)) {
        errors.push(`${location}: credential or private identifier field is forbidden`);
      }
      scanPrivateData(entry, errors, location);
    }
  }
}

export function validateClientCapabilities(ledger) {
  const errors = [];
  const fail = (label, message) => errors.push(`${label}: ${message}`);
  const fields = (value, keys, label) => {
    if (!object(value)) { fail(label, 'must be an object'); return false; }
    for (const key of Object.keys(value)) if (!keys.includes(key)) fail(label, 'unexpected field');
    for (const key of keys) if (!(key in value)) fail(label, `missing field ${key}`);
    return true;
  };
  const requiredText = (value, label) => { if (!text(value)) fail(label, 'must be non-empty text'); };
  const enumValue = (value, choices, label) => { if (!choices.includes(value)) fail(label, 'invalid classification'); };
  const reference = (value, label) => { if (!text(value) || !REFERENCE.test(value)) fail(label, 'requires a public evidence reference'); };
  const dependencies = (value, label) => {
    if (!Array.isArray(value) || !value.length || value.some((entry) => !ISSUE.test(entry)) || new Set(value).size !== value.length) {
      fail(label, 'requires unique owner/dependency issue URLs');
    }
  };
  scanPrivateData(ledger, errors);
  if (!fields(ledger, ['schemaVersion', 'metadata', 'clients', 'sources', 'families'], 'ledger')) return errors;
  if (ledger.schemaVersion !== 1) fail('ledger', 'unsupported schema version');
  if (fields(ledger.metadata, ['scope', 'issue', 'canonicalCoverage', 'acceptance', 'limitations'], 'metadata')) {
    if (ledger.metadata.scope !== 'family-boundaries-only' || ledger.metadata.issue !== issue(1318)) fail('metadata', 'requires D-PARITY family scope');
    if (fields(ledger.metadata.canonicalCoverage, ['ownerIssue', 'status', 'reason'], 'canonicalCoverage')) {
      if (ledger.metadata.canonicalCoverage.ownerIssue !== issue(1324) || ledger.metadata.canonicalCoverage.status !== 'pending-external-ledger') fail('canonicalCoverage', 'must preserve the pending Q-HARNESS dependency');
      requiredText(ledger.metadata.canonicalCoverage.reason, 'canonicalCoverage.reason');
    }
    if (fields(ledger.metadata.acceptance, ['ownerIssue', 'status', 'reason'], 'acceptance')) {
      if (ledger.metadata.acceptance.ownerIssue !== issue(1325) || !['pending-feature-evidence', 'feature-evidence-recorded'].includes(ledger.metadata.acceptance.status)) fail('acceptance', 'must preserve the pending Q-ACCEPT dependency or record complete feature evidence');
      requiredText(ledger.metadata.acceptance.reason, 'acceptance.reason');
    }
    requiredText(ledger.metadata.limitations, 'metadata.limitations');
  }
  if (fields(ledger.clients, ['codex', 'agy'], 'clients')) {
    for (const [client, pin] of Object.entries(PINS)) {
      if (fields(ledger.clients[client], Object.keys(pin), `clients.${client}`)) {
        for (const [key, value] of Object.entries(pin)) if (ledger.clients[client][key] !== value) fail(`clients.${client}`, 'client pin mismatch');
      }
    }
  }
  const sources = object(ledger.sources) ? ledger.sources : {};
  if (!object(ledger.sources) || !Object.keys(sources).length) fail('sources', 'requires pinned source evidence');
  for (const [index, source] of Object.values(sources).entries()) {
    const label = `source[${index}]`;
    if (!fields(source, ['kind', 'reference', 'scope'], label)) continue;
    enumValue(source.kind, ['pinned-source', 'runtime-documentation', 'issue-handoff'], `${label}.kind`);
    reference(source.reference, label);
    requiredText(source.scope, `${label}.scope`);
    if (source.kind === 'pinned-source' && !/\/(?:blob|tree)\/[a-f0-9]{40}(?:\/|$)/.test(source.reference ?? '')) fail(label, 'source revision must be pinned');
  }
  if (!Array.isArray(ledger.families)) { fail('families', 'must be an array'); return errors; }
  const seen = new Set();
  for (const [index, row] of ledger.families.entries()) {
    const label = `family[${index}]`;
    if (!fields(row, ['id', 'client', 'caller', 'reachability', 'authBase', 'clientMode', 'upstreamCredential', 'ownerIssue', 'dependencies', 'sourceEvidence', 'status', 'deferReason', 'acceptanceEvidence'], label)) continue;
    if (seen.has(row.id)) fail(label, 'duplicate family');
    seen.add(row.id);
    const requirement = REQUIRED_FAMILIES[row.id];
    if (!requirement) fail(label, 'unknown family; endpoint coverage belongs to Q-HARNESS');
    if (!Object.hasOwn(PINS, row.client) || !text(row.id) || !row.id.startsWith(`${row.client}-`)) fail(label, 'client/family mismatch');
    for (const key of ['caller', 'reachability', 'authBase', 'clientMode']) requiredText(row[key], `${label}.${key}`);
    enumValue(row.upstreamCredential, ['openai_oauth', 'openai_api_key', 'gemini_oauth', 'gemini_api_key', 'none', 'vendor-direct', 'configured-service', 'signed-url'], `${label}.upstreamCredential`);
    if (!ISSUE.test(row.ownerIssue) || (requirement && row.ownerIssue !== issue(requirement[0]))) fail(label, 'missing or incorrect family owner');
    dependencies(row.dependencies, `${label}.dependencies`);
    if (!Array.isArray(row.dependencies) || !row.dependencies.includes(issue(1324)) || !row.dependencies.includes(issue(1325))) fail(label, 'requires Q-HARNESS and Q-ACCEPT dependencies');
    if (!Array.isArray(row.sourceEvidence) || !row.sourceEvidence.length || row.sourceEvidence.some((id) => !Object.hasOwn(sources, id))) fail(label, 'missing or unknown source evidence');
    if (fields(row.status, ['route', 'boundary', 'clientAcceptance'], `${label}.status`)) {
      enumValue(row.status.route, ['shipped', 'partial', 'not-claimed'], `${label}.status.route`);
      enumValue(row.status.boundary, ['feature', 'direct', 'local', 'internal', 'configured', 'signed-storage'], `${label}.status.boundary`);
      enumValue(row.status.clientAcceptance, ['pending', 'supported'], `${label}.status.clientAcceptance`);
      if (requirement && row.status.boundary !== requirement[1]) fail(label, 'incorrect required boundary');
      if (row.status.route !== 'not-claimed' && !(Array.isArray(row.sourceEvidence) && row.sourceEvidence.some((id) => sources[id]?.kind === 'runtime-documentation'))) fail(label, 'shipped route needs separate runtime documentation');
      if (row.id === 'agy-native-oauth' && (!Array.isArray(row.dependencies) || !row.dependencies.includes(issue(1310)))) fail(label, 'native OAuth requires the G-DISC dependency without transferring G-NATIVE ownership');
      if (row.id === 'agy-raw-code-assist' && (!Array.isArray(row.dependencies) || ![1312, 1306, 1310].every((number) => row.dependencies.includes(issue(number))) || row.dependencies.includes(issue(1311)))) fail(label, 'raw Code Assist requires F-OWNER, G-HOST and G-DISC, not Codex C-AUTH');
    }
    if (row.id === 'agy-native-api-key' && row.upstreamCredential !== 'gemini_api_key') fail(label, 'API-key axis mismatch');
    if (['agy-native-oauth', 'agy-raw-code-assist', 'agy-code-assist-controls', 'agy-remote-agent', 'agy-remote-control', 'agy-subscription-voice'].includes(row.id) && row.upstreamCredential !== 'gemini_oauth') fail(label, 'OAuth axis mismatch');
    if (typeof row.id === 'string' && row.id.startsWith('codex-') && row.status?.boundary === 'feature' && row.upstreamCredential !== 'openai_oauth') fail(label, 'subscription axis mismatch');
    if (row.status?.boundary === 'signed-storage' && row.upstreamCredential !== 'signed-url') fail(label, 'signed storage must remain credential-free apart from its scoped signed URL');
    if (row.status?.boundary === 'local' && row.upstreamCredential !== 'none') fail(label, 'local boundary cannot acquire upstream credentials');
    if (row.status?.boundary === 'configured' && row.upstreamCredential !== 'configured-service') fail(label, 'configured service credential axis mismatch');
    if (row.status?.clientAcceptance === 'pending') {
      requiredText(row.deferReason, `${label}.deferReason`);
      if (row.acceptanceEvidence !== null) fail(label, 'pending rows must not imply acceptance evidence');
      continue;
    }
    if (row.status?.clientAcceptance !== 'supported') continue;
    if (row.deferReason !== null) fail(label, 'supported rows cannot retain a defer reason');
    const evidence = row.acceptanceEvidence;
    if (!fields(evidence, ['implementation', 'execution', 'wire', 'upstream', 'ownerAffinity', 'provider', 'usage', 'quota', 'cost'], `${label}.acceptanceEvidence`)) continue;
    if (fields(evidence.implementation, ['reference', 'mergedCommit'], `${label}.implementation`)) {
      if (!/^https:\/\/github\.com\/isac322\/cc-lb\/pull\/[1-9]\d*$/.test(evidence.implementation.reference ?? '') || !/^[a-f0-9]{40}$/.test(evidence.implementation.mergedCommit ?? '')) fail(label, 'requires merged implementation PR and commit');
    }
    if (fields(evidence.execution, ['kind', 'client', 'version', 'pin', 'command', 'outcome', 'reference'], `${label}.execution`)) {
      const pin = Object.hasOwn(PINS, row.client) ? PINS[row.client] : null;
      if (evidence.execution.kind !== 'actual-client' || evidence.execution.outcome !== 'passed') fail(label, 'requires passing actual-client execution, not descriptors, sources or replay');
      if (!pin || evidence.execution.client !== row.client || evidence.execution.version !== pin.version || evidence.execution.pin !== (pin.revision ?? pin.sha256)) fail(label, 'execution client pin mismatch');
      requiredText(evidence.execution.command, `${label}.command`);
      reference(evidence.execution.reference, `${label}.execution`);
    }
    if (fields(evidence.wire, ['method', 'path', 'transport', 'reference'], `${label}.wire`)) {
      enumValue(evidence.wire.method, ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'RPC', 'LOCAL'], `${label}.wire.method`);
      enumValue(evidence.wire.transport, ['http', 'sse', 'websocket', 'grpc', 'local'], `${label}.wire.transport`);
      if (!/^\/(?!\/)[^\s?#]*$/.test(evidence.wire.path ?? '')) fail(label, 'requires redacted wire path without query credentials');
      reference(evidence.wire.reference, `${label}.wire`);
    }
    if (fields(evidence.upstream, ['credentialKind', 'selection', 'reference'], `${label}.upstream`)) {
      if (evidence.upstream.credentialKind !== row.upstreamCredential) fail(label, 'upstream credential axis mismatch');
      requiredText(evidence.upstream.selection, `${label}.upstream.selection`);
      reference(evidence.upstream.reference, `${label}.upstream`);
    }
    if (fields(evidence.ownerAffinity, ['decision', 'reference'], `${label}.ownerAffinity`)) {
      requiredText(evidence.ownerAffinity.decision, `${label}.ownerAffinity.decision`);
      reference(evidence.ownerAffinity.reference, `${label}.ownerAffinity`);
    }
    if (fields(evidence.provider, ['outcome', 'entitlement', 'clientOutcome', 'reference'], `${label}.provider`)) {
      if (evidence.provider.outcome !== 'passed' || evidence.provider.clientOutcome !== 'passed') fail(label, 'provider and client outcomes must pass');
      if (evidence.provider.entitlement !== 'confirmed') fail(label, 'applicable entitlement must be confirmed');
      reference(evidence.provider.reference, `${label}.provider`);
    }
    for (const metric of ['usage', 'quota', 'cost']) {
      const observation = evidence[metric];
      if (!object(observation)) { fail(label, `${metric} evidence is required`); continue; }
      if (observation.state === 'absent') {
        if (!fields(observation, ['state', 'reason', 'reference'], `${label}.${metric}`)) continue;
        requiredText(observation.reason, `${label}.${metric}.reason`);
      } else {
        if (!fields(observation, ['state', 'origin', 'values', 'period', 'periodSource', 'reference'], `${label}.${metric}`)) continue;
        if (observation.state !== 'reported' || observation.origin !== 'provider') fail(label, `${metric} must be reported by the provider; fabricated/inferred values are forbidden`);
        if (!object(observation.values) || !Object.keys(observation.values).length || Object.values(observation.values).some((value) => typeof value !== 'number' || !Number.isFinite(value) || value < 0)) fail(label, `${metric} requires finite nonnegative reported numbers`);
        if (observation.period === null) {
          if (observation.periodSource !== null) fail(label, `${metric} absent period cannot have an inferred source`);
        } else if (!text(observation.period) || observation.periodSource !== 'provider') fail(label, `${metric} period must be explicit provider evidence, never inferred`);
      }
      reference(observation.reference, `${label}.${metric}`);
    }
  }
  for (const id of Object.keys(REQUIRED_FAMILIES)) if (!seen.has(id)) fail('families', `missing required family ${id}`);
  if (ledger.metadata?.acceptance?.status === 'feature-evidence-recorded' && ledger.families.some((row) => row?.status?.boundary === 'feature' && row.status.clientAcceptance !== 'supported')) fail('acceptance', 'feature evidence remains pending');
  return errors;
}

// Route support and client acceptance are separate claims. This guard checks
// the bounded caveat and broad client promotions, not the provider route table.
export function validatePublicClaims(content, label, ledger) {
  const errors = [];
  const paragraphs = content.split(/\n\s*\n/).map((part) => part.replace(/\s+/g, ' '));
  const caveat = paragraphs.some((part) => /(?:\b(?:shipped|native|existing)\b.{0,100}\broutes?\b|\broutes?\b.{0,100}\b(?:shipped|native|existing)\b)/i.test(part)
    && /(?:\b(?:clients?|CLI)\b.{0,100}\b(?:acceptance|verification|support|parity)\b|\b(?:acceptance|verification|support|parity)\b.{0,100}\b(?:clients?|CLI)\b)/i.test(part)
    && /\b(?:pending|unverified|not (?:yet )?(?:verified|established|proven)|does not (?:prove|establish|imply))\b/i.test(part));
  if (!caveat) errors.push(`${label}: missing route-versus-client-acceptance boundary`);
  for (const sentence of content.split(/(?<=[.!?])\s+|\n/)) {
    const positive = /\b(?:fully?\s+(?:supports?|supported|verified)|complete\s+(?:support|parity)|(?:full|complete)\s+(?:Codex|agy|Antigravity)\s+(?:CLI\s+)?(?:support|parity)|all\s+(?:(?:Codex|agy|Antigravity)\s+)?(?:features|capabilities)\s+(?:work|(?:are\s+)?supported)|(?:Codex|agy|Antigravity)\s+(?:CLI\s+)?(?:v?\d+(?:\.\d+){1,3}\s+)?(?:is\s+)?(?:fully\s+)?(?:supported|verified)|real[- ]client[- ]tested|subscription\s+acceptance\s+(?:has\s+)?passed)\b/i;
    if (!positive.test(sentence) || !/\b(?:Codex|agy|Antigravity|client|CLI|subscription\s+acceptance)\b/i.test(sentence)) continue;
    // A negated assertion is not a positive support claim. Require negation in
    // the same clause; a preceding unrelated caveat cannot license promotion.
    for (const clause of sentence.split(/[,;]|\b(?:but|while|whereas|although|and|yet|however)\b/i)) {
      const assertions = [...clause.matchAll(new RegExp(positive.source, 'gi'))];
      for (const [index, assertion] of assertions.entries()) {
        const before = clause.slice(index ? assertions[index - 1].index + assertions[index - 1][0].length : 0, assertion.index);
        const after = clause.slice(assertion.index + assertion[0].length, assertions[index + 1]?.index);
        const negatedBefore = /\b(?:no|not|never|cannot|without|unverified|pending)(?:\s+[a-z-]+){0,3}\s*$/i.test(before);
        const negatedAfter = /^\s+(?:(?:is|are|remains?|has|have|still)\s+){0,2}(?:not|unverified|pending)\b/i.test(after);
        if (negatedBefore || negatedAfter) continue;
        const client = /\b(?:agy|Antigravity)\b/i.test(clause) ? 'agy' : /\bCodex\b/i.test(clause) ? 'codex' : null;
        const rows = (Array.isArray(ledger?.families) ? ledger.families : []).filter((row) => row?.status?.boundary === 'feature' && (!client || row.client === client));
        if (ledger?.metadata?.canonicalCoverage?.status === 'pending-external-ledger' || !rows.length || rows.some((row) => row.status?.clientAcceptance !== 'supported') || validateClientCapabilities(ledger).length) errors.push(`${label}: broad client-support promotion lacks accepted feature evidence and canonical Q-HARNESS completeness`);
      }
    }
  }
  return errors;
}

export function checkClientCapabilities(root) {
  const ledgerPath = path.join(root, 'docs/client-capabilities.json');
  let ledger;
  try { ledger = JSON.parse(readFileSync(ledgerPath, 'utf8')); }
  catch { return ['docs/client-capabilities.json: cannot read valid JSON']; }
  const errors = validateClientCapabilities(ledger);
  for (const file of ['docs/runtime-management.md', 'site/src/content/docs/reference/source/runtime-management.md', 'README.md', 'positioning.yml']) {
    try { errors.push(...validatePublicClaims(readFileSync(path.join(root, file), 'utf8'), file, ledger)); }
    catch { errors.push(`${file}: cannot read public claims`); }
  }
  return errors;
}

function main(argv) {
  let root;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--root') {
      root = argv[++index];
      if (!root || root.startsWith('--')) return ['--root requires a directory'];
    } else if (arg.startsWith('--root=')) {
      root = arg.slice('--root='.length);
      if (!root) return ['--root requires a directory'];
    } else return ['unknown argument'];
  }
  return checkClientCapabilities(path.resolve(root ?? path.join(path.dirname(fileURLToPath(import.meta.url)), '..')));
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const errors = main(process.argv.slice(2));
  for (const error of errors) console.error(`FAIL ${error}`);
  if (errors.length) {
    console.error(`client-capabilities: ${errors.length} failure(s)`);
    process.exitCode = 1;
  } else console.log('client-capabilities: family boundaries valid; canonical coverage and client acceptance remain separate dependencies');
}
