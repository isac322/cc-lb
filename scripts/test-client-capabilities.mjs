import assert from 'node:assert/strict';
import test from 'node:test';
import { REQUIRED_FAMILIES, validateClientCapabilities, validatePublicClaims } from './check-client-capabilities.mjs';

// Entirely synthetic objects: these references and passing outcomes are test
// controls, not provider evidence, public ledger entries or a Q-HARNESS API.
const issue = (number) => `https://github.com/isac322/cc-lb/issues/${number}`;
const evidenceRef = 'tests/real-client/synthetic-control.json';
const codexPin = 'd32063f96c2dff1dc96af839b3952abb6acfeda4';
const agyPin = 'fad3628ad91fe8613a72f074a1c5fde7cd816517788a5b9be96703f7d256741a';
const caveat = 'Shipped native routes do not establish pinned client acceptance. Exact CLI verification remains pending.';

function pendingLedger() {
  return {
    schemaVersion: 1,
    metadata: {
      scope: 'family-boundaries-only', issue: issue(1318),
      canonicalCoverage: { ownerIssue: issue(1324), status: 'pending-external-ledger', reason: 'Canonical endpoint ledger is independently owned.' },
      acceptance: { ownerIssue: issue(1325), status: 'pending-feature-evidence', reason: 'Feature results are independently owned.' },
      limitations: 'Synthetic family schema control only; no canonical coverage or provider success claim.',
    },
    clients: { codex: { version: '0.169.0', revision: codexPin }, agy: { version: '1.5.9', sha256: agyPin } },
    sources: {
      source: { kind: 'pinned-source', reference: `https://github.com/openai/codex/blob/${codexPin}/codex-rs/features/src/lib.rs`, scope: 'Synthetic source provenance control.' },
      runtime: { kind: 'runtime-documentation', reference: 'docs/runtime-management.md#proxy-capability-boundary', scope: 'Route eligibility only.' },
    },
    families: Object.entries(REQUIRED_FAMILIES).map(([id, [owner, boundary]]) => ({
      id, client: id.startsWith('codex-') ? 'codex' : 'agy',
      caller: 'Synthetic caller', reachability: 'Not executed', authBase: 'Explicit configured origin', clientMode: 'Synthetic client mode',
      upstreamCredential: boundary === 'feature' ? (id.startsWith('codex-') ? 'openai_oauth' : id === 'agy-native-api-key' ? 'gemini_api_key' : 'gemini_oauth') : boundary === 'signed-storage' ? 'signed-url' : boundary === 'configured' ? 'configured-service' : 'none',
      ownerIssue: issue(owner), dependencies: [issue(1324), issue(1325), ...(id === 'agy-native-oauth' ? [issue(1310)] : [])], sourceEvidence: ['source'],
      status: { route: 'not-claimed', boundary, clientAcceptance: 'pending' },
      deferReason: 'Exact pinned-client execution and entitlement evidence remain pending.', acceptanceEvidence: null,
    })),
  };
}

function promote(ledger, id = 'agy-native-oauth') {
  const row = ledger.families.find((family) => family.id === id);
  row.status.clientAcceptance = 'supported';
  row.deferReason = null;
  const absent = () => ({ state: 'absent', reason: 'Synthetic provider response contains no observation.', reference: evidenceRef });
  row.acceptanceEvidence = {
    implementation: { reference: 'https://github.com/isac322/cc-lb/pull/999999', mergedCommit: 'a'.repeat(40) },
    execution: { kind: 'actual-client', client: row.client, version: ledger.clients[row.client].version, pin: row.client === 'codex' ? codexPin : agyPin, command: row.client === 'codex' ? 'codex exec synthetic-control' : 'agy synthetic-control', outcome: 'passed', reference: evidenceRef },
    wire: { method: 'POST', path: '/gemini/v1beta/models/synthetic:generateContent', transport: 'sse', reference: evidenceRef },
    upstream: { credentialKind: row.upstreamCredential, selection: 'Synthetic upstream selection, no real identifier', reference: evidenceRef },
    ownerAffinity: { decision: 'Synthetic owner retained without failover', reference: evidenceRef },
    provider: { outcome: 'passed', entitlement: 'confirmed', clientOutcome: 'passed', reference: evidenceRef },
    usage: absent(), quota: absent(), cost: absent(),
  };
  return row;
}

function rejects(change, pattern, { supported = false, id } = {}) {
  const ledger = pendingLedger();
  const row = supported ? promote(ledger, id) : ledger.families[0];
  change(ledger, row);
  const before = structuredClone(ledger);
  const errors = validateClientCapabilities(ledger);
  assert.ok(errors.some((error) => pattern.test(error)), `Expected ${pattern}; got ${errors.join('; ')}`);
  assert.deepEqual(ledger, before, 'validation must not mutate caller data');
}

test('pending family boundaries validate without implying canonical coverage or live success', () => {
  const ledger = pendingLedger();
  const before = structuredClone(ledger);
  assert.deepEqual(validateClientCapabilities(ledger), []);
  assert.deepEqual(ledger, before);
});

test('complete synthetic positive control permits future evidence-backed promotion', () => {
  for (const id of ['agy-native-api-key', 'agy-native-oauth', 'agy-subscription-voice', 'agy-remote-control', 'codex-model']) {
    const ledger = pendingLedger();
    promote(ledger, id);
    assert.deepEqual(validateClientCapabilities(ledger), [], id);
  }
});

test('complete synthetic family evidence cannot license exhaustive client claims while canonical coverage is pending', () => {
  const ledger = pendingLedger();
  for (const row of ledger.families) if (row.status.boundary === 'feature') promote(ledger, row.id);
  ledger.metadata.acceptance.status = 'feature-evidence-recorded';
  assert.deepEqual(validateClientCapabilities(ledger), []);
  assert.match(validatePublicClaims(`${caveat}\n\nCodex CLI is fully supported.`, 'synthetic aggregate control', ledger).join(' '), /canonical Q-HARNESS completeness/);
});

test('reported numeric zero stays distinct from absent or fabricated usage', () => {
  const ledger = pendingLedger();
  const row = promote(ledger);
  row.acceptanceEvidence.usage = { state: 'reported', origin: 'provider', values: { inputTokens: 0 }, period: null, periodSource: null, reference: evidenceRef };
  row.acceptanceEvidence.quota = { state: 'reported', origin: 'provider', values: { remaining: 0 }, period: 'provider-labelled-window', periodSource: 'provider', reference: evidenceRef };
  assert.deepEqual(validateClientCapabilities(ledger), []);
});

test('descriptor/source/binary/recorder/synthetic evidence never replaces actual execution', () => {
  for (const kind of ['descriptor-only', 'source-only', 'binary-only', 'recorder', 'synthetic', 'replay', 'live_client']) {
    rejects((_, row) => { row.acceptanceEvidence.execution.kind = kind; }, /passing actual-client/, { supported: true });
  }
  rejects((_, row) => { row.status.clientAcceptance = 'supported'; row.deferReason = null; }, /acceptanceEvidence: must be an object/);
});

test('an execution label alone cannot promote failed or unexecuted voice and remote actions', () => {
  for (const id of ['codex-subscription-voice', 'agy-subscription-voice', 'agy-remote-agent', 'agy-remote-control']) {
    for (const outcome of ['pending', 'not-executed', 'failed', null]) {
      rejects((_, row) => { row.acceptanceEvidence.execution.outcome = outcome; }, /passing actual-client/, { supported: true, id });
    }
    rejects((_, row) => { row.acceptanceEvidence.provider.clientOutcome = 'failed'; }, /client outcomes must pass/, { supported: true, id });
  }
});

test('API-key controls cannot close the independent native OAuth axis', () => {
  rejects((_, row) => { row.acceptanceEvidence.upstream.credentialKind = 'gemini_api_key'; }, /credential axis mismatch/, { supported: true });
  rejects((ledger) => { ledger.families.find((row) => row.id === 'agy-native-oauth').upstreamCredential = 'gemini_api_key'; }, /OAuth axis mismatch/);
  const ledger = pendingLedger();
  promote(ledger, 'agy-native-api-key');
  assert.equal(ledger.families.find((row) => row.id === 'agy-native-oauth').status.clientAcceptance, 'pending');
  assert.deepEqual(validateClientCapabilities(ledger), []);
});

test('provider failure, unavailable entitlement and missing execution proof remain blocking', () => {
  for (const entitlement of ['absent', 'denied', 'unknown', null]) {
    rejects((_, row) => { row.acceptanceEvidence.provider.entitlement = entitlement; }, /entitlement must be confirmed/, { supported: true });
  }
  rejects((_, row) => { row.acceptanceEvidence.provider.outcome = 'failed'; }, /provider and client outcomes/, { supported: true });
  for (const key of ['implementation', 'execution', 'wire', 'upstream', 'ownerAffinity', 'provider', 'usage', 'quota', 'cost']) {
    rejects((_, row) => { delete row.acceptanceEvidence[key]; }, /missing field/, { supported: true });
  }
});

test('implementation, client pins, command, wire and owner evidence must be exact', () => {
  const mutations = [
    [(_, row) => { row.acceptanceEvidence.implementation.mergedCommit = 'unmerged'; }, /merged implementation/],
    [(_, row) => { row.acceptanceEvidence.implementation.reference = issue(1310); }, /merged implementation/],
    [(ledger) => { ledger.clients.agy.version = '1.5.8'; }, /client pin mismatch/],
    [(_, row) => { row.acceptanceEvidence.execution.pin = 'b'.repeat(64); }, /execution client pin mismatch/],
    [(_, row) => { row.acceptanceEvidence.execution.version = '1.5.8'; }, /execution client pin mismatch/],
    [(_, row) => { row.acceptanceEvidence.execution.client = 'codex'; }, /execution client pin mismatch/],
    [(_, row) => { row.acceptanceEvidence.execution.command = ''; }, /command: must be non-empty/],
    [(_, row) => { row.acceptanceEvidence.wire.path = 'https://example.invalid/path'; }, /redacted wire path/],
    [(_, row) => { row.acceptanceEvidence.wire.transport = 'descriptor'; }, /invalid classification/],
    [(_, row) => { row.acceptanceEvidence.ownerAffinity.reference = ''; }, /public evidence reference/],
  ];
  for (const [change, pattern] of mutations) rejects(change, pattern, { supported: true });
});

test('inferred quota periods and invented usage/cost are rejected', () => {
  const reported = () => ({ state: 'reported', origin: 'provider', values: { count: 1 }, period: null, periodSource: null, reference: evidenceRef });
  for (const metric of ['usage', 'quota', 'cost']) {
    for (const origin of ['invented', 'estimated', 'inferred', 'source']) {
      rejects((_, row) => { row.acceptanceEvidence[metric] = { ...reported(), origin }; }, /fabricated\/inferred/, { supported: true });
    }
    rejects((_, row) => { row.acceptanceEvidence[metric] = { ...reported(), values: { count: -1 } }; }, /nonnegative reported numbers/, { supported: true });
    rejects((_, row) => { row.acceptanceEvidence[metric] = { ...reported(), period: 'five-hours', periodSource: 'inferred' }; }, /period must be explicit provider evidence/, { supported: true });
    rejects((_, row) => { row.acceptanceEvidence[metric].values = { count: 0 }; }, /unexpected field/, { supported: true });
  }
});

test('required family completeness, duplicates, owners, dependencies and defer reasons fail closed', () => {
  for (const id of Object.keys(REQUIRED_FAMILIES)) {
    rejects((ledger) => { ledger.families = ledger.families.filter((row) => row.id !== id); }, /missing required family/);
  }
  rejects((ledger, row) => { ledger.families.push(structuredClone(row)); }, /duplicate family/);
  rejects((_, row) => { row.ownerIssue = issue(1); }, /incorrect family owner/);
  rejects((_, row) => { row.deferReason = ''; }, /deferReason: must be non-empty/);
  rejects((_, row) => { row.dependencies = [issue(1324)]; }, /Q-HARNESS and Q-ACCEPT/);
  rejects((_, row) => { row.status.boundary = 'internal'; }, /incorrect required boundary/);
  rejects((_, row) => { row.status.route = 'shipped'; }, /separate runtime documentation/);
  rejects((ledger) => { ledger.metadata.canonicalCoverage.status = 'complete'; }, /pending Q-HARNESS/);
  rejects((ledger) => { ledger.metadata.acceptance.status = 'complete'; }, /pending Q-ACCEPT/);
  rejects((ledger) => { ledger.sources.source.reference = 'https://github.com/openai/codex/blob/main/file.rs'; }, /source revision must be pinned/);
  rejects((ledger) => { ledger.metadata.acceptance.status = 'feature-evidence-recorded'; }, /feature evidence remains pending/);
  rejects((ledger) => { ledger.families.find((row) => row.id === 'codex-signed-storage').upstreamCredential = 'openai_oauth'; }, /signed storage must remain credential-free/);
  rejects((ledger) => { ledger.families.find((row) => row.id === 'agy-local-microphone').upstreamCredential = 'gemini_oauth'; }, /local boundary cannot acquire/);
});

test('public credentials, signed queries, private identifiers and local paths are forbidden without echo', () => {
  for (const value of ['Bearer synthetic-secret-value', 'sk-syntheticsecret123456', 'https://example.invalid/file?X-Amz-Signature=synthetic', '/Users/synthetic/private', 'synthetic@example.invalid']) {
    const ledger = pendingLedger();
    ledger.families[0].deferReason = value;
    const errors = validateClientCapabilities(ledger);
    assert.ok(errors.some((error) => /credential or private data/.test(error)));
    assert.ok(errors.every((error) => !error.includes(value)));
  }
  rejects((_, row) => { row.access_token = 'synthetic'; }, /credential or private identifier/);
});

test('credential-custody descriptions are allowed but adjoining actual values are still rejected', () => {
  const ledger = pendingLedger();
  const row = ledger.families.find((family) => family.id === 'codex-signed-storage');
  row.authBase = 'exact signed storage destination without vendor bearer credentials';
  row.deferReason = 'Never forward bearer tokens to signed storage.';
  assert.deepEqual(validateClientCapabilities(ledger), []);
  for (const token of ['synthetic-secret-value', 'credentials-secret', 'tokens-secret']) {
    row.authBase = `Do not forward bearer credentials; captured Authorization: Bearer ${token}`;
    assert.ok(validateClientCapabilities(ledger).some((error) => /credential or private data/.test(error)));
  }
});

test('native OAuth acceptance belongs to G-NATIVE while G-DISC stays a dependency', () => {
  const ledger = pendingLedger();
  const row = ledger.families.find((family) => family.id === 'agy-native-oauth');
  assert.equal(row.ownerIssue, issue(1308));
  assert.ok(row.dependencies.includes(issue(1310)));
  row.ownerIssue = issue(1310);
  assert.match(validateClientCapabilities(ledger).join(' '), /incorrect family owner/);
  row.ownerIssue = issue(1308);
  row.dependencies = row.dependencies.filter((dependency) => dependency !== issue(1310));
  assert.match(validateClientCapabilities(ledger).join(' '), /G-DISC dependency/);
});

test('malformed JSON-shaped inputs return errors rather than throwing', () => {
  for (const input of [null, [], {}, { schemaVersion: 1 }]) assert.ok(validateClientCapabilities(input).length);
  for (const value of [null, 42, {}, 'invalid']) {
    rejects((ledger) => { ledger.families[0] = value; }, /must be an object|missing field|client\/family mismatch/);
    rejects((_, row) => { row.sourceEvidence = value; }, /source evidence/);
    rejects((_, row) => { row.dependencies = value; }, /dependencies|Q-HARNESS/);
  }
});

test('public caveat accepts route support and rejects missing acceptance distinction', () => {
  const ledger = pendingLedger();
  for (const text of [caveat, 'Existing route support is distinct from exact CLI acceptance, which remains unverified.', 'Shipped routes and credential support do not prove pinned client verification. Acceptance is pending.']) {
    assert.deepEqual(validatePublicClaims(text, 'control', ledger), []);
  }
  assert.match(validatePublicClaims('Provider-native routes are supported.', 'control', ledger).join(' '), /missing route-versus-client/);
});

test('YAML not-claimable boundary may put acceptance before plural clients', () => {
  const content = 'not_claimable:\n  - Full acceptance for the pinned Codex 0.169.0 or agy 1.5.9 clients remains unverified; shipped route support and credential eligibility do not establish client parity.';
  assert.deepEqual(validatePublicClaims(content, 'positioning control', pendingLedger()), []);
  assert.match(validatePublicClaims(`${content}\n  - Codex CLI 0.169.0 is supported.`, 'positioning control', pendingLedger()).join(' '), /promotion lacks accepted feature evidence/);
});

test('shipped API-key and OAuth route statements do not imply full-client support', () => {
  for (const claim of ['OpenAI API-key Realtime and Gemini Live routes are supported.', 'OAuth model routes retain their shipped support.', 'Codex uses a shipped Responses route; exact client acceptance remains pending.']) {
    assert.deepEqual(validatePublicClaims(`${caveat}\n\n${claim}`, 'route control', pendingLedger()), []);
  }
});

test('negated broad claims are not mistaken for positive promotions', () => {
  for (const claim of ['Codex CLI is not fully supported.', 'We do not claim complete support for the agy CLI.', 'Full Codex CLI support is not established.', 'There is no complete parity for the Antigravity client.', 'Codex CLI 0.169.0 is not supported.', 'This client is not real-client tested.', 'Subscription acceptance has not passed.']) {
    assert.deepEqual(validatePublicClaims(`${caveat}\n\n${claim}`, 'control', pendingLedger()), [], claim);
  }
});

test('an unrelated caveat cannot license broad or mixed positive client claims', () => {
  for (const claim of ['Codex CLI is fully supported.', 'Full Codex CLI support is available.', 'All agy features are supported.', 'Codex CLI is not fully supported, but agy CLI is fully supported.', 'Not complete parity for Codex; Antigravity is fully verified.', 'Codex CLI 0.169.0 is supported.', 'agy 1.5.9 is supported.', 'Codex CLI is real-client tested.', 'Subscription acceptance passed.']) {
    assert.match(validatePublicClaims(`${caveat}\n\n${claim}`, 'control', pendingLedger()).join(' '), /promotion lacks accepted feature evidence/, claim);
  }
});
