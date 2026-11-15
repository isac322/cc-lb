import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import test from 'node:test';
import { COVERAGE_STATES, DEFAULT_FIXTURE_MANIFEST, DEFAULT_SLUG_CROSSWALK, importCoverage, serializePortableCoverage, validateCoverage } from './coverage.mjs';
import { mutateCoverage } from './import-coverage.mjs';
import { fixtures as codexFixtures } from '../codex169/fixtures.mjs';
import { fixtures as agyFixtures } from '../agy159/fixtures.mjs';

const original = JSON.parse(await readFile(new URL('./coverage-ledger.json', import.meta.url), 'utf8'));
const clone = () => structuredClone(original);
const validate = (ledger, extra = {}) => validateCoverage({ ledger, ...extra });
function rejectsMutation(mutate, pattern = /Coverage:/) {
  const altered = clone();
  mutate(altered);
  assert.throws(() => validate(altered), pattern);
  assert.deepEqual(validate(original), { valid: true, surfaces: 371, actions: 154, livePass: 0 });
}

function attachResults(ledger, results) {
  for (const item of [...ledger.surfaces, ...ledger.actions]) {
    const surfaces = item.surface_id ? [item.id] : [...item.actualSurfaceIds, ...item.candidateSurfaceIds];
    item.test.resultIds = results.filter((result) => result.surfaceIds.some((id) => surfaces.includes(id))).map(({ id }) => id).sort();
  }
  return ledger;
}

// QA COV001: independent original commitments, not just agreement of two edited arrays.
test('COV001 retains all 371 unique surfaces and 154 actions; omission/duplicate controls fail', () => {
  assert.equal(new Set(original.surfaces.map(({ id }) => id)).size, 371);
  assert.equal(new Set(original.actions.map(({ id }) => id)).size, 154);
  assert.equal(original.surfaces.filter(({ cli }) => cli === 'codex').length, 146);
  assert.equal(original.surfaces.filter(({ cli }) => cli === 'agy').length, 225);
  for (const mutation of ['omission', 'duplicate']) {
    assert.throws(() => validateCoverage(mutateCoverage(original, mutation)), /Coverage:/);
    assert.equal(validate(original).valid, true);
  }
  rejectsMutation((ledger) => {
    const removed = ledger.actions.pop();
    for (const surface of ledger.surfaces) {
      surface.actualActionIds = surface.actualActionIds.filter((id) => id !== removed.id);
      surface.candidateActionIds = surface.candidateActionIds.filter((id) => id !== removed.id);
    }
  }, /original inventory count/);
});

// QA COV002: candidate reachability and actual references are different bidirectional edges.
test('COV002 requires exact actual and candidate reverse links, rejects unknown and duplicate references', () => {
  for (const [surfaceField, actionField] of [['actualActionIds', 'actualSurfaceIds'], ['candidateActionIds', 'candidateSurfaceIds']]) {
    for (const surface of original.surfaces) for (const id of surface[surfaceField]) {
      assert(original.actions.find((action) => action.id === id)[actionField].includes(surface.id));
    }
    rejectsMutation((ledger) => ledger.surfaces.find((surface) => surface[surfaceField].length)[surfaceField].pop(), /missing reverse/);
    rejectsMutation((ledger) => ledger.surfaces[0][surfaceField].push('unknown-action'), /orphan action/);
    rejectsMutation((ledger) => {
      const surface = ledger.surfaces.find((entry) => entry[surfaceField].length);
      surface[surfaceField].push(surface[surfaceField][0]);
    }, /duplicate/);
    rejectsMutation((ledger) => ledger.actions[0][actionField].push('codex:unknown-surface'), /manifest/);
  }
});

// QA COV003: removing both ends cannot erase an obligation; deferred work has an owner and next proof.
test('COV003 requires deferred owner/reason/nextProof and explicit assignment ownership', () => {
  for (const state of ['blocked-contract', 'blocked-account', 'deferred-reachability']) {
    for (const key of ['owner', 'reason', 'nextProof']) rejectsMutation((ledger) => {
      ledger.surfaces[0].disposition.state = state;
      delete ledger.surfaces[0].disposition[key];
    }, /deferred|owner/);
  }
  rejectsMutation((ledger) => {
    ledger.surfaces[0].featureIssue.assignment = 'UNKNOWN';
    ledger.surfaces[0].disposition.owner = 'UNKNOWN';
  }, /unknown assignment/);
  const accounts = original.surfaces.find(({ id }) => id === 'codex:accounts-check');
  assert.equal(accounts.featureIssue.assignment, 'C-ACCOUNT');
  assert.equal(original.surfaces.find(({ id }) => id === 'codex:workspace-switch-token').featureIssue.assignment, 'C-ACCOUNT');
  assert.equal(original.surfaces.find(({ id }) => id === 'codex:plugins-installed').featureIssue.assignment, 'C-APPS');
  assert.equal(DEFAULT_SLUG_CROSSWALK['codex-auth-base-contract'], 'C-AUTH');
  assert.equal(DEFAULT_SLUG_CROSSWALK['agy-direct-feature-contract'], 'G-NATIVE');
  const sourceOnly = original.surfaces.find(({ id }) => id === 'codex:responses-sse');
  assert.equal(sourceOnly.sourceEvidence.provenance, 'client-source-snapshot');
  assert.match(sourceOnly.sourceEvidence.evidence_level, /source_implemented/);
  assert.equal(sourceOnly.observation.state, 'not-observed');
  assert.equal(sourceOnly.observation.liveStatus, 'NOT-RUN');
  rejectsMutation((ledger) => { ledger.surfaces[0].dependencies.push('UNKNOWN'); }, /unknown dependency/);
  rejectsMutation((ledger) => { ledger.surfaces[0].prerequisites.push('unknown external condition'); }, /unknown external prerequisite/);
  rejectsMutation((ledger) => { ledger.actions[0].dependencies.push('UNKNOWN'); }, /unknown dependency/);
  rejectsMutation((ledger) => { ledger.actions[0].originalIssueRefs.pop(); }, /original action/);
});

// QA COV004: required metadata must survive independently of reference counts.
test('COV004 rejects missing source/version/hash/aliases/transport/evidence/action/test-result metadata', () => {
  const mutations = [
    (ledger) => { delete ledger.sourceInputs.codex; },
    (ledger) => { delete ledger.surfaces[0].client.version; },
    (ledger) => { delete ledger.surfaces[0].client.source_revision; },
    (ledger) => { delete ledger.surfaces[0].client.binary_sha256; },
    (ledger) => { ledger.surfaces[0].client.binary_sha256 = []; },
    (ledger) => { delete ledger.surfaces[0].aliases; },
    (ledger) => { delete ledger.surfaces[0].transport; },
    (ledger) => { delete ledger.surfaces[0].sourceEvidence; },
    (ledger) => { ledger.surfaces[0].sourceEvidence.references = []; },
    (ledger) => { delete ledger.surfaces[0].sourceEvidence.evidence_level; },
    (ledger) => { delete ledger.surfaces[0].sourceEvidence.provenance; },
    (ledger) => { delete ledger.actions[0].consumer_action; },
    (ledger) => { delete ledger.actions[0].test.status; },
    (ledger) => { delete ledger.actions[0].result; },
    (ledger) => { ledger.sourceInputs.qa.content_sha256 = 'f'.repeat(64); },
    (ledger) => { ledger.sourceInputs.qa.sha256 = 'not-a-hash'; },
  ];
  for (const mutation of mutations) rejectsMutation(mutation);
  assert.deepEqual(COVERAGE_STATES, ['captured', 'synthetic', 'related-source-only', 'descriptor-only', 'blocked-contract', 'blocked-account', 'deferred-reachability', 'verified-live']);
  for (const state of COVERAGE_STATES.filter((state) => state !== 'verified-live')) {
    const ledger = clone();
    ledger.surfaces[0].sourceEvidence.state = state;
    assert.equal(validate(ledger).livePass, 0);
  }
  rejectsMutation((ledger) => { ledger.surfaces[0].sourceEvidence.state = 'invented'; }, /unknown evidence/);
  rejectsMutation((ledger) => { ledger.surfaces[0].disposition.state = 'invented'; }, /unknown disposition/);
  rejectsMutation((ledger) => { ledger.surfaces[0].sourceEvidence.state = 'verified-live'; }, /cannot become live PASS/);
  rejectsMutation((ledger) => { ledger.surfaces[0].disposition.state = 'verified-live'; }, /cannot become live PASS/);
});

// QA COV005: serialization and input transforms are deterministic; exact input bytes remain separately hashed.
test('COV005 deterministic portable serialization refuses private paths and credentials', () => {
  const shuffled = clone();
  shuffled.surfaces.reverse(); shuffled.actions.reverse();
  assert.equal(serializePortableCoverage(shuffled), serializePortableCoverage(original));
  for (const value of ['/tmp/private-profile', '/Users/private/home', 'identity@example.invalid', 'Bearer SYNTHETIC-PRIVATE-CREDENTIAL']) {
    const ledger = clone();
    ledger.surfaces[0].disposition.reason = value;
    assert.throws(() => serializePortableCoverage(ledger), /nonportable/);
  }
  assert.equal(JSON.parse(serializePortableCoverage(original)).sourceInputs.qa.sha256, 'cbbccfeee0abb265fc619249acf21a8e3bb0bb9dfeadeae7439c2a9667b06c09');
});

test('COV005 malformed or changed input bytes fail closed before any ledger is returned', () => {
  const emptyQa = JSON.stringify({ rows: [], surface_coverage: [], extension_surface_inventory: [] });
  assert.throws(() => importCoverage({ qaBytes: '{', codexBytes: '[]', agyBytes: '[]', issueMap: {} }), /malformed JSON/);
  assert.throws(() => importCoverage({ qaBytes: emptyQa, codexBytes: '[]', agyBytes: '[]', issueMap: {} }), /source hash/);
});

// Supply approved external input paths explicitly; the repository does not carry private original QA prose.
const inputRoot = process.env.COVERAGE_INPUT_ROOT;
test('COV005 original import states, source hashes and reordered input content commitments', { skip: !inputRoot && 'Set COVERAGE_INPUT_ROOT to the approved external inventory root' }, async () => {
  const [qaBytes, codexBytes, agyBytes, issueMap] = await Promise.all([
    readFile(join(inputRoot, 'qa/qa-list.json')), readFile(join(inputRoot, 'research/CodexSurface/codex-surface.json')),
    readFile(join(inputRoot, 'research/AgySurface/agy-surface.json')), readFile(join(inputRoot, 'ISSUE-MAP.json'), 'utf8').then(JSON.parse),
  ]);
  const inputs = { qaBytes, codexBytes, agyBytes, issueMap };
  const imported = importCoverage(inputs);
  assert.equal(serializePortableCoverage(imported), serializePortableCoverage(original));
  const qa = JSON.parse(qaBytes);
  for (const row of qa.rows) {
    const action = imported.actions.find(({ id }) => id === row.id);
    assert.equal(action.status, row.status);
    assert.deepEqual(action.result, row.result);
    assert.deepEqual(action.originalIssueRefs, row.requires_issues);
  }
  qa.rows.reverse(); qa.surface_coverage.reverse(); qa.extension_surface_inventory.reverse();
  const reordered = importCoverage({ ...inputs, qaBytes: JSON.stringify(qa), codexBytes: JSON.stringify(JSON.parse(codexBytes).reverse()), agyBytes: JSON.stringify(JSON.parse(agyBytes).reverse()) });
  assert.deepEqual(reordered.surfaces, imported.surfaces);
  assert.deepEqual(reordered.actions, imported.actions);
  for (const name of ['qa', 'codex', 'agy']) {
    assert.equal(reordered.sourceInputs[name].content_sha256, imported.sourceInputs[name].content_sha256);
    assert.notEqual(reordered.sourceInputs[name].sha256, imported.sourceInputs[name].sha256);
  }
  assert.throws(() => importCoverage({ ...inputs, qaBytes: '{' }), /malformed JSON/);
  assert.throws(() => importCoverage({ ...inputs, codexBytes: '[]' }), /source hash/);
  assert.throws(() => importCoverage({ ...inputs, slugCrosswalk: {} }), /unknown dependency/);
  const unknownIssue = structuredClone(issueMap); delete unknownIssue['C-ACCOUNT'];
  assert.throws(() => importCoverage({ ...inputs, issueMap: unknownIssue }), /issue assignment/);
});

// QA COV006: fixture PASS and live consumer decode are independent observations.
test('COV006 synthetic or descriptor fixtures cannot promote original action states to live PASS', () => {
  assert.throws(() => validateCoverage(mutateCoverage(original, 'synthetic-live')), /synthetic or unproven live PASS/);
  for (const provenance of ['synthetic', 'descriptor-only', 'captured']) {
    const fixtureManifest = [{ id: 'fixture-negative', provenance, surfaceIds: [original.surfaces[0].id] }];
    const resultManifest = [{ id: 'result-negative', fixtureId: 'fixture-negative', provenance, status: 'PASS', state: 'verified-live', consumerDecoded: true, evidenceSha256: 'a'.repeat(64), client: { version: 'fixture', binary_sha256: 'b'.repeat(64) }, surfaceIds: fixtureManifest[0].surfaceIds }];
    assert.throws(() => validate(original, { fixtureManifest, resultManifest }), /synthetic or unproven live PASS/);
  }
  rejectsMutation((ledger) => { ledger.actions[0].status = 'PASS'; }, /original action/);
  rejectsMutation((ledger) => { ledger.surfaces[0].observation.liveStatus = 'PASS'; }, /cannot become live PASS/);
  const syntheticFixture = DEFAULT_FIXTURE_MANIFEST.find((fixture) => fixture.provenance === 'synthetic');
  const syntheticResult = { id: 'synthetic-pass-result', fixtureId: syntheticFixture.id, provenance: 'synthetic', state: 'synthetic', status: 'PASS', liveStatus: 'NOT-RUN', consumerDecoded: false, surfaceIds: syntheticFixture.surfaceIds };
  assert.equal(validate(attachResults(clone(), [syntheticResult]), { resultManifest: [syntheticResult] }).livePass, 0);
  assert.equal(original.actions.filter(({ status }) => status === 'PASS').length, 0);
});

// QA COV007: promised fixture families are present, while all runtime execution remains NOT-RUN.
test('COV007 every named acceptance family links existing fixture exports without deferring harness work', () => {
  const fixtureManifest = [...codexFixtures, ...agyFixtures];
  assert(fixtureManifest.length > 0);
  assert.equal(validate(original, { fixtureManifest }).valid, true);
  for (const family of original.fixtureFamilies) {
    const matches = fixtureManifest.filter((fixture) => fixture.surfaceIds.some((id) => family.surfaceIds.includes(id)));
    assert(matches.length > 0, `${family.id} must have reusable fixtures`);
    assert.deepEqual(family.fixtureIds, matches.map(({ id }) => id).sort());
    assert.equal(family.fixturePresence, 'present');
    assert.equal(family.status, 'NOT-RUN');
  }
  const gateway = fixtureManifest.find(({ id }) => id === 'agy159-gateway-bearer-sse');
  assert(gateway);
  assert.deepEqual(gateway.surfaceIds, ['agy:gemini-api-key-stream']);
  assert.equal(gateway.provenance, 'synthetic');
  assert.equal(gateway.native_only, true);
  assert.equal(gateway.expected.consumer.authMode, 'gateway-bearer');
  assert.equal(gateway.expected.consumer.modelSource, 'AGY_LLM_GATEWAY_MODELS');
  assert.equal(gateway.expected.provider_success, false);
  assert.equal(gateway.expected.subscription_success, false);
  assert(gateway.source_refs.some(({ qualification }) => qualification?.includes('not a captured successful turn')));
  const native = original.surfaces.find(({ id }) => id === 'agy:gemini-api-key-stream');
  assert(native.test.fixtureIds.includes(gateway.id));
  assert.equal(native.test.status, 'NOT-RUN');
  assert.equal(native.observation.liveStatus, 'NOT-RUN');
  assert.equal(validate(original, { fixtureManifest }).livePass, 0);
  assert.equal(serializePortableCoverage(original, { fixtureManifest }), serializePortableCoverage(original));
  rejectsMutation((ledger) => { ledger.fixtureFamilies.pop(); }, /required fixture families/);
  rejectsMutation((ledger) => { ledger.fixtureFamilies[0].fixtureIds = []; }, /missing required fixture family/);
  rejectsMutation((ledger) => { ledger.fixtureFamilies[0].fixturePresence = 'deferred'; }, /missing required fixture family/);
  rejectsMutation((ledger) => { ledger.surfaces[0].test.fixtureIds = ['fabricated-fixture']; }, /unknown fixture/);
  rejectsMutation((ledger) => { ledger.surfaces[0].test.resultIds = ['fabricated-result']; }, /unknown result/);
  assert.throws(() => validate(original, { fixtureManifest: [{ id: 'unknown', provenance: 'synthetic', surfaceIds: ['agy:unknown'] }] }), /unknown fixture surface/);
});

test('COV007 fixture linkage rejects omission and existing but unrelated references on surfaces/actions/families', () => {
  for (const collection of ['surfaces', 'actions']) {
    rejectsMutation((ledger) => {
      ledger[collection].find((item) => item.test.fixtureIds.length).test.fixtureIds = [];
    }, /fixture linkage/);
    rejectsMutation((ledger) => {
      const item = ledger[collection].find((entry) => entry.test.fixtureIds.length);
      const surfaces = item.surface_id ? [item.id] : [...item.actualSurfaceIds, ...item.candidateSurfaceIds];
      const unrelated = DEFAULT_FIXTURE_MANIFEST.find((fixture) => fixture.surfaceIds.every((id) => !surfaces.includes(id)));
      assert(unrelated);
      item.test.fixtureIds = [unrelated.id];
    }, /fixture linkage/);
  }
  rejectsMutation((ledger) => {
    ledger.fixtureFamilies.find((family) => family.fixtureIds.length > 1).fixtureIds.pop();
  }, /family fixture linkage/);
});

test('COV007 result linkage requires exact row mapping and consistent fixture/surface provenance', () => {
  const fixtures = ['codex:responses-sse', 'agy:gemini-api-key-stream'].map((surfaceId) =>
    DEFAULT_FIXTURE_MANIFEST.find((fixture) => fixture.provenance === 'synthetic' && fixture.surfaceIds.includes(surfaceId)));
  assert(fixtures.every(Boolean));
  const results = fixtures.map((fixture, index) => ({
    id: `synthetic-link-result-${index}`, fixtureId: fixture.id, provenance: 'synthetic',
    state: 'synthetic', status: 'PASS', liveStatus: 'NOT-RUN', consumerDecoded: false,
    surfaceIds: structuredClone(fixture.surfaceIds),
  }));
  const intact = attachResults(clone(), results);
  assert.equal(validate(intact, { resultManifest: results }).livePass, 0);
  for (const collection of ['surfaces', 'actions']) {
    for (const replacement of [[], [results[1].id]]) {
      const altered = structuredClone(intact);
      const item = altered[collection].find((entry) => entry.test.resultIds.includes(results[0].id) && !entry.test.resultIds.includes(results[1].id));
      assert(item);
      item.test.resultIds = replacement;
      assert.throws(() => validate(altered, { resultManifest: results }), /result linkage/);
      assert.equal(validate(intact, { resultManifest: results }).valid, true);
    }
  }
  const wrongSurfaces = structuredClone(results);
  wrongSurfaces[0].surfaceIds = results[1].surfaceIds;
  assert.throws(() => validate(intact, { resultManifest: wrongSurfaces }), /result fixture surface/);
  const wrongFixture = structuredClone(results);
  wrongFixture[0].fixtureId = fixtures[1].id;
  assert.throws(() => validate(intact, { resultManifest: wrongFixture }), /result fixture surface/);
  const wrongProvenance = structuredClone(results);
  wrongProvenance[0].provenance = 'descriptor-only';
  assert.throws(() => validate(intact, { resultManifest: wrongProvenance }), /result fixture provenance/);
});
