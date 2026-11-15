import { createHash } from 'node:crypto';
import { assertSafeArtifact } from './redaction.mjs';
import { fixtures as codexFixtures } from '../codex169/fixtures.mjs';
import { fixtures as agyFixtures } from '../agy159/fixtures.mjs';

export const DEFAULT_FIXTURE_MANIFEST = Object.freeze([...codexFixtures, ...agyFixtures]);
const HANDOFF_KEYS = new Set(['C-ACCOUNT', 'C-APPS', 'C-AUTH', 'C-AUX', 'C-CLOUD', 'C-FILES', 'C-MODEL', 'C-RC', 'D-PARITY', 'F-OWNER', 'G-AUTH', 'G-CTRL', 'G-DISC', 'G-HOST', 'G-NATIVE', 'G-RC', 'G-REMOTE', 'G-VOICE', 'Q-ACCEPT', 'Q-HARNESS']);
const EXTERNAL_PREREQUISITES = new Set([
  'C-MODEL owner-affinity and principal-auth contract',
  'direct OpenAI OAuth subscription upstream configuration',
  'existing principal-authenticated Responses and #1304 state contract',
  'no cliproxyapi backend or client vendor-token flow',
  'pending account-owner policy for history-notes state (must not be invented)',
]);

export const COVERAGE_STATES = Object.freeze(['captured', 'synthetic', 'related-source-only', 'descriptor-only', 'blocked-contract', 'blocked-account', 'deferred-reachability', 'verified-live']);
const SOURCE_FILES = Object.freeze({ qa: 'qa/qa-list.json', codex: 'research/CodexSurface/codex-surface.json', agy: 'research/AgySurface/agy-surface.json' });
// These commitments are generated once from the original approved inventory, not from a ledger under validation.
const ORIGINAL = Object.freeze({
  surfaces: 'e6a10f5aaec22b6a35b2c41ec4fcf5bd94a76586ff82062248f1d3c05b1a4ff2',
  actions: 'a3913a295a462dc828fc1362712733c302ce6c5e289d9026b50e001adfa8db29',
  qa: '25c5700734927e85df5ce98bb3345a1cec93c04bf55073bf8c570e7fde3dc77f',
  codex: 'c3a259895e23531190b4e7f4bcc77f02b6faf6a726e3e8ff7988ac6f274e6ec9',
  agy: '359ce4c7037b95887429551e069855735a371fa4a4c101f170676de33c4fcd1d',
});

// Fixture presence and fixture execution are separate: every promised family is present, execution remains NOT-RUN.
const FIXTURE_FAMILIES = Object.freeze({
  'codex-responses-sse': ['codex:responses-sse'],
  'codex-responses-ws': ['codex:responses-websocket'],
  'codex-compaction-v2': ['codex:responses-compaction-v2'],
  'codex-account-workspace': ['codex:accounts-check', 'codex:workspace-switch-token', 'codex:accounts-verified-access'],
  'codex-mcp-plugin': ['codex:apps-mcp-post', 'codex:plugins-install'],
  'codex-signed-file': ['codex:files-create', 'codex:files-upload-blob', 'codex:files-finalize'],
  'codex-cloud-task': ['codex:cloud-task-create', 'codex:cloud-task-detail'],
  'codex-remote-control': ['codex:remote-control-enroll', 'codex:remote-control-refresh', 'codex:remote-control-websocket'],
  'agy-native-sse': ['agy:gemini-api-key-stream'],
  'agy-raw-code-assist': ['agy:code_assist-predictionservice-generatecontent', 'agy:code_assist-predictionservice-streamgeneratecontent'],
  'agy-discovery-quota': ['agy:code_assist-predictionservice-fetchavailablemodels', 'agy:code_assist-predictionservice-retrieveuserquota'],
  'agy-control': ['agy:code_assist-cloudcode-fetchadmincontrols'],
  'agy-remote-agent': ['agy:remote_agent-jetskiremoteagentproxyservice-cancelcascade'],
  'agy-remote-control': ['agy:remote_control-apiservice-sendsignalingmessage'],
  'agy-voice': ['agy:remote_agent-jetskiremoteagentproxyservice-streamvoice'],
});

export const DEFAULT_SLUG_CROSSWALK = Object.freeze({
  'codex-auth-base-contract': 'C-AUTH',
  'codex-responses-stateful-transport': 'C-MODEL',
  'codex-model-auxiliary-apis': 'C-AUX',
  'codex-account-control-plane': 'C-ACCOUNT',
  'codex-apps-connectors-mcp': 'C-APPS',
  'codex-files-signed-upload': 'C-FILES',
  'codex-cloud-tasks': 'C-CLOUD',
  'codex-remote-control-and-realtime': 'C-RC',
  'codex-direct-auth-telemetry-updates-plugins': 'D-PARITY',
  'codex-optional-internal-transports': 'D-PARITY',
  'codex-qa-docs-parity': 'Q-HARNESS',
  'agy-direct-gemini-e2e': 'G-NATIVE',
  'agy-gemini-payload-fidelity': 'G-NATIVE',
  'agy-gemini-state-affinity': 'G-NATIVE',
  'agy-direct-feature-contract': 'G-NATIVE',
  'agy-codeassist-principal-gateway': 'G-AUTH',
  'agy-codeassist-generation-envelope': 'G-AUTH',
  'agy-codeassist-discovery-quota': 'G-DISC',
  'agy-codeassist-control-plane': 'G-CTRL',
  'agy-host-profile-and-scheduler': 'G-HOST',
  'agy-remote-agent-runtime': 'G-REMOTE',
  'agy-remote-control': 'G-RC',
  'agy-voice': 'G-VOICE',
  'agy-e2e-qa-harness': 'Q-HARNESS',
  'agy-docs-and-positioning': 'D-PARITY',
});
const CODEX_FAMILIES = Object.freeze({
  account_usage_settings_policy: 'C-ACCOUNT', apps_connectors_mcp_files: 'C-APPS',
  authentication_and_identity: 'D-PARITY', cloud_tasks: 'C-CLOUD', internal_cloud_preview: 'D-PARITY',
  model_inference_and_tools: 'C-AUX', optional_remote_control_and_execution: 'D-PARITY',
  plugins_skills_marketplaces: 'C-APPS', telemetry_feedback_optional_scorer: 'D-PARITY', updates_install_assets: 'D-PARITY',
});
const AGY_BOUNDARIES = new Set(['google-oauth-authorize', 'google-oauth-token', 'google-userinfo', 'google-userinfo-v3', 'google-tokeninfo-v3', 'google-operations', 'clearcut-telemetry', 'auto-update-check', 'changelog-fetch', 'plugin-marketplace-official', 'plugin-marketplace-partners', 'mcp-http', 'local-language-server', 'local-health', 'local-oauth-callback', 'external-web-and-tools', 'voice-local-microphone']);
const DISCOVERY = new Set(['code_assist-predictionservice-fetchavailablemodels', 'code_assist-predictionservice-retrieveuserquota', 'code_assist-predictionservice-retrieveuserquotasummary', 'code_assist-cloudcode-listmodelconfigs', 'code_assist-cloudcode-loadcodeassist', 'code_assist-cloudcode-onboarduser', 'code_assist-cloudcode-onboarduserbackgroundtasks', 'code_assist-cloudcode-listcloudaicompanionprojects']);
const sorted = (values) => [...values].sort();
const fail = (message) => { throw new Error(`Coverage: ${message}`); };
function object(value, label) { if (!value || typeof value !== 'object' || Array.isArray(value)) fail(`${label} must be an object`); return value; }
function array(value, label) { if (!Array.isArray(value)) fail(`${label} must be an array`); return value; }
function text(value, label) { if (typeof value !== 'string' || !value) fail(`${label} must be a nonempty string`); return value; }
function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map((key) => [key, stable(value[key])]));
  return value;
}
const json = (value) => JSON.stringify(stable(value));
const digest = (value) => createHash('sha256').update(value).digest('hex');
function unique(values, label) { const seen = new Set(); for (const value of values) { if (seen.has(value)) fail(`duplicate ${label}: ${value}`); seen.add(value); } return seen; }
function same(actual, expected, label) { if (json(actual) !== json(expected)) fail(`${label} mismatch`); }
function canonicalSource(name, value) {
  if (name !== 'qa') return [...array(value, name)].sort((a, b) => a.id.localeCompare(b.id));
  return { ...value, rows: [...array(value.rows, 'QA rows')].sort((a, b) => a.id.localeCompare(b.id)), surface_coverage: [...array(value.surface_coverage, 'surface coverage')].sort((a, b) => `${a.cli}:${a.surface_id}`.localeCompare(`${b.cli}:${b.surface_id}`)), extension_surface_inventory: [...array(value.extension_surface_inventory, 'extensions')].sort((a, b) => `${a.cli}:${a.surface_id}`.localeCompare(`${b.cli}:${b.surface_id}`)) };
}
function parse(bytes, name) {
  if (!(typeof bytes === 'string' || bytes instanceof Uint8Array)) fail(`${name} requires input bytes`);
  let value; try { value = JSON.parse(Buffer.from(bytes).toString('utf8')); } catch { fail(`${name} malformed JSON`); }
  return { value, source: { reference: SOURCE_FILES[name], sha256: digest(bytes), content_sha256: digest(json(canonicalSource(name, value))) } };
}
function ownerFor(cli, source) {
  const id = source.id ?? source.surface_id;
  if (cli === 'codex') {
    if (id === 'accounts-verified-access' || id === 'workspace-switch-token' || id === 'account-security-setup' || id === 'cloud-config-bundle') return 'C-ACCOUNT';
    if (id.startsWith('responses-')) return 'C-MODEL';
    if (id.startsWith('files-')) return 'C-FILES';
    if (id.startsWith('remote-control-')) return 'C-RC';
    if (id.startsWith('realtime-') || id === 'api-key-turn-costs' || id.startsWith('curated-') || id === 'binary-only-github-contents' || id.startsWith('external-mcp-')) return 'D-PARITY';
    return CODEX_FAMILIES[source.feature_group] ?? fail(`unknown Codex assignment ${id}`);
  }
  if (cli !== 'agy') fail(`unknown CLI ${cli}`);
  if (AGY_BOUNDARIES.has(id)) return 'D-PARITY';
  if (id === 'remote_agent-jetskiremoteagentproxyservice-streamvoice' || id === 'aicode_v1main-predictionservice-bidigeneratecontent' || id.startsWith('voice-')) return 'G-VOICE';
  if (id === 'gemini-api-key-stream' || id === 'generation-image-mode' || id === 'generation-websearch-mode') return 'G-NATIVE';
  if (DISCOVERY.has(id)) return 'G-DISC';
  if (id.startsWith('code_assist-predictionservice-')) return 'G-AUTH';
  if (id.startsWith('code_assist-cloudcode-') || id.startsWith('code_assist-jetskiservice-')) return 'G-CTRL';
  if (id.startsWith('remote_agent-') || id.startsWith('aicode_v1main-')) return 'G-REMOTE';
  if (id.startsWith('remote_control-')) return /-(recordclientevent|recordtrajectoryanalytics)$/.test(id) ? 'D-PARITY' : 'G-RC';
  fail(`unknown agy assignment ${id}`);
}
function issue(key, issueMap) {
  const entry = object(issueMap[key], `issue assignment ${key}`);
  if (!Number.isSafeInteger(entry.number) || !/^https:\/\/github\.com\/[^/?#]+\/[^/?#]+\/issues\/\d+$/.test(entry.url ?? '')) fail(`invalid public issue ${key}`);
  return { assignment: key, number: entry.number, url: entry.url };
}
function publicReferences(source, cli, id) {
  return (source.evidence ?? source.sources ?? source.source_refs ?? []).flatMap((entry) => {
    if (typeof entry?.url === 'string' && /^https:\/\/(github\.com|developers\.google\.com|cloud\.google\.com|ai\.google\.dev)\//.test(entry.url) && !/[?@]/.test(entry.url)) return [{ url: entry.url }];
    if (typeof entry?.source === 'string' && /^[a-zA-Z0-9_.-][a-zA-Z0-9_./:#-]*$/.test(entry.source) && !entry.source.includes('..')) return [{ relative_id: entry.source }];
    return [];
  }).concat([{ relative_id: `${SOURCE_FILES[cli]}#${id}` }]);
}
function evidenceState(source) {
  if (source.evidence_level === 'captured_local_request') return 'captured';
  if ((source.evidence ?? source.source_refs ?? []).some((entry) => entry.url?.startsWith('https://github.com/openai/codex/blob/'))) return 'captured';
  if (/related_source|related_executor_source|sibling_source_only/.test(source.evidence_level ?? '')) return 'related-source-only';
  return 'descriptor-only';
}
function actionCommitment(actions) {
  return actions.map(({ id, cli, version, scope, status, result, actualSurfaceIds, candidateSurfaceIds, originalIssueRefs }) => ({
    id, cli, version, scope, status, result, actualSurfaceIds, candidateSurfaceIds, originalIssueRefs,
  })).sort((a, b) => a.id.localeCompare(b.id));
}

/** Import only structural evidence. No request bodies, accounts, commands, or private source locations are copied. */
export function importCoverage({ qaBytes, codexBytes, agyBytes, issueMap, slugCrosswalk = DEFAULT_SLUG_CROSSWALK, fixtureManifest = DEFAULT_FIXTURE_MANIFEST }) {
  object(issueMap, 'issue map'); object(slugCrosswalk, 'slug crosswalk');
  const inputs = { qa: parse(qaBytes, 'qa'), codex: parse(codexBytes, 'codex'), agy: parse(agyBytes, 'agy') };
  for (const [name, input] of Object.entries(inputs)) same(input.source.content_sha256, ORIGINAL[name], `${name} source hash`);
  const qa = inputs.qa.value;
  const sources = new Map();
  const clientMetadata = Object.fromEntries(['codex', 'agy'].map((cli) => {
    const evidence = inputs[cli].value.flatMap((source) => source.evidence ?? source.sources ?? []);
    const binaryHashes = sorted(new Set(evidence.flatMap((entry) => [entry.sha256, entry.binary_sha256].filter((hash) => /^[a-f0-9]{64}$/.test(hash ?? '')))));
    const revision = cli === 'codex' ? evidence.map((entry) => entry.url?.match(/^https:\/\/github\.com\/openai\/codex\/blob\/([a-f0-9]{40})\//)?.[1]).find(Boolean) : null;
    return [cli, { binaryHashes, revision }];
  }));
  for (const cli of ['codex', 'agy']) for (const source of inputs[cli].value) {
    const id = `${cli}:${text(source.id, 'surface id')}`;
    if (sources.has(id)) fail(`duplicate surface ${id}`);
    sources.set(id, { source, cli });
  }
  for (const source of qa.extension_surface_inventory) {
    const id = `${source.cli}:${source.surface_id}`;
    if (sources.has(id)) fail(`duplicate extension ${id}`);
    sources.set(id, { source, cli: source.cli });
  }
  const actions = qa.rows.map((row) => {
    const cli = text(row.cli, 'action cli');
    const link = (ids) => sorted(array(ids, `${row.id} links`).map((id) => `${cli}:${id}`));
    const originalIssueRefs = structuredClone(array(row.requires_issues, 'original issue refs'));
    const dependencies = sorted(new Set(originalIssueRefs.map((slug) => {
      const key = slugCrosswalk[slug];
      if (!HANDOFF_KEYS.has(key)) fail(`unknown dependency slug ${slug}`);
      issue(key, issueMap);
      return key;
    })));
    return {
      id: text(row.id, 'action id'), cli, version: text(row.version, 'action version'), scope: row.scope,
      status: row.status, result: structuredClone(object(row.result, 'action result')),
      actualSurfaceIds: link(row.surface_ids), candidateSurfaceIds: link(row.candidate_surface_ids),
      originalIssueRefs, dependencies, consumer_action: `qa/qa-list.json#${row.id}`,
      test: { fixtureIds: [], resultIds: [], status: 'NOT-RUN' },
    };
  }).sort((a, b) => a.id.localeCompare(b.id));
  const surfaceCoverage = new Map();
  for (const item of qa.surface_coverage) { const id = `${item.cli}:${item.surface_id}`; if (surfaceCoverage.has(id)) fail(`duplicate coverage ${id}`); surfaceCoverage.set(id, item); }
  const surfaces = [...sources.entries()].map(([id, { source, cli }]) => {
    const coverage = surfaceCoverage.get(id) ?? fail(`missing surface coverage ${id}`);
    const owner = ownerFor(cli, source);
    const featureIssue = issue(owner, issueMap);
    const sourceDependencies = sorted(unique(array(issueMap[owner].dependencies, `${owner} dependencies`), 'issue dependency'));
    const dependencies = sourceDependencies.filter((key) => Object.hasOwn(issueMap, key));
    const prerequisites = sourceDependencies.filter((key) => !Object.hasOwn(issueMap, key));
    for (const key of prerequisites) if (!EXTERNAL_PREREQUISITES.has(key)) fail(`unknown external prerequisite ${key}`);
    const state = evidenceState(source);
    const disposition = owner === 'D-PARITY' ? { state: 'deferred-reachability', owner, reason: 'Direct, configured, local, internal, or optional boundary; inclusion in inventory does not claim proxy support.', nextProof: 'Verify the actual client boundary and credential isolation; publish the supported/excluded boundary in the owning issue.' } : { state: 'deferred-reachability', owner, reason: 'Source/descriptor or limited local capture is not successful provider or consumer evidence.', nextProof: 'Owning issue supplies authorized real-client reachability, entitlement, owner isolation, and consumer-decoded terminal evidence.' };
    const localHashes = (source.evidence ?? source.sources ?? source.source_refs ?? []).flatMap((entry) => [entry.sha256, entry.binary_sha256].filter((hash) => /^[a-f0-9]{64}$/.test(hash ?? '')));
    const binaryHashes = sorted(new Set(localHashes.length ? localHashes : clientMetadata[cli].binaryHashes));
    return {
      id, cli, surface_id: source.id ?? source.surface_id,
      client: {
        name: cli, version: cli === 'codex' ? '0.169.0' : '1.5.9',
        source_revision: clientMetadata[cli].revision,
        binary_sha256: binaryHashes,
      },
      aliases: structuredClone(source.path_aliases ?? []),
      transport: source.transport ?? coverage.method, method: coverage.method, path: coverage.path,
      sourceEvidence: {
        state, evidence_level: source.evidence_level ?? source.reachability ?? source.evidence_status,
        provenance: cli === 'codex' && state === 'captured' ? 'client-source-snapshot' : (source.evidence_level === 'captured_local_request' ? 'local-request-capture' : 'qualified-source-or-descriptor'),
        references: publicReferences(source, cli, source.id ?? source.surface_id),
      },
      observation: { state: source.evidence_level === 'captured_local_request' ? 'captured' : 'not-observed', liveStatus: 'NOT-RUN', consumerDecoded: false },
      disposition, featureIssue, dependencies, prerequisites,
      actualActionIds: sorted(array(coverage.action_ids, 'actual actions')),
      candidateActionIds: sorted(array(coverage.candidate_action_ids, 'candidate actions')),
      test: { fixtureIds: [], resultIds: [], status: 'NOT-RUN' },
    };
  }).sort((a, b) => a.id.localeCompare(b.id));
  for (const item of [...surfaces, ...actions]) {
    const ids = item.surface_id ? [item.id] : [...item.actualSurfaceIds, ...item.candidateSurfaceIds];
    item.test.fixtureIds = sorted(fixtureManifest.filter((fixture) => fixture.surfaceIds.some((id) => ids.includes(id))).map((fixture) => fixture.id));
  }
  const fixtureFamilies = Object.entries(FIXTURE_FAMILIES).map(([id, surfaceIds]) => ({
    id, surfaceIds,
    fixtureIds: sorted(fixtureManifest.filter((fixture) => fixture.surfaceIds.some((surfaceId) => surfaceIds.includes(surfaceId))).map((fixture) => fixture.id)),
    fixturePresence: 'present', status: 'NOT-RUN',
  }));
  const ledger = { schemaVersion: 1, inventory: { surfaces: 371, actions: 154, reference: 'qa/qa-list.json', ownershipReferences: ['ISSUE-MAP.json', 'plan/challenge/verdicts.md'] }, sourceInputs: Object.fromEntries(Object.entries(inputs).map(([name, input]) => [name, input.source])), fixtureFamilies, surfaces, actions };
  validateCoverage({ ledger, fixtureManifest });
  return ledger;
}

/** Validate both directions against a fixed original manifest, not mutually consistent edited arrays. */
export function validateCoverage({ ledger, fixtureManifest = DEFAULT_FIXTURE_MANIFEST, resultManifest = [] }) {
  object(ledger, 'ledger');
  if (ledger.schemaVersion !== 1) fail('unsupported schema');
  const surfaces = array(ledger.surfaces, 'surfaces'), actions = array(ledger.actions, 'actions');
  const surfaceIds = unique(surfaces.map((item) => text(item.id, 'surface id')), 'surface');
  const actionIds = unique(actions.map((item) => text(item.id, 'action id')), 'action');
  if (surfaces.length !== 371 || actions.length !== 154) fail('original inventory count mismatch');
  same(digest(json(sorted(surfaceIds))), ORIGINAL.surfaces, 'original surface manifest');
  same(digest(json(actionCommitment(actions))), ORIGINAL.actions, 'original action state/links manifest');
  for (const name of ['qa', 'codex', 'agy']) {
    const source = object(ledger.sourceInputs?.[name], `${name} provenance`);
    same(source.content_sha256, ORIGINAL[name], `${name} source hash`);
    if (!/^[a-f0-9]{64}$/.test(source.sha256 ?? '')) fail(`${name} byte hash malformed`);
    same(source.reference, SOURCE_FILES[name], `${name} source reference`);
  }
  const surfaceMap = new Map(surfaces.map((entry) => [entry.id, entry]));
  const actionMap = new Map(actions.map((entry) => [entry.id, entry]));
  const fixtures = array(fixtureManifest, 'fixture manifest'), results = array(resultManifest, 'result manifest');
  unique(fixtures.map((entry) => text(entry.id, 'fixture id')), 'fixture'); unique(results.map((entry) => text(entry.id, 'result id')), 'result');
  const fixtureMap = new Map(fixtures.map((entry) => [entry.id, entry]));
  const resultMap = new Map(results.map((entry) => [entry.id, entry]));
  for (const fixture of fixtures) {
    if (!['synthetic', 'descriptor-only', 'captured'].includes(fixture.provenance)) fail(`invalid fixture provenance ${fixture.id}`);
    const references = unique(array(fixture.surfaceIds, 'fixture surfaces'), 'fixture surface');
    if (!references.size) fail(`empty fixture surfaces ${fixture.id}`);
    for (const id of references) if (!surfaceIds.has(id)) fail(`unknown fixture surface ${id}`);
  }
  for (const result of results) {
    if (!COVERAGE_STATES.includes(result.state)) fail(`unknown result evidence state ${result.id}`);
    const fixture = result.fixtureId ? fixtureMap.get(result.fixtureId) : null;
    if (result.fixtureId && !fixture) fail(`unknown result fixture ${result.fixtureId}`);
    const references = unique(array(result.surfaceIds, 'result surfaces'), 'result surface');
    if (!references.size) fail(`empty result surfaces ${result.id}`);
    for (const id of references) {
      if (!surfaceIds.has(id)) fail(`unknown result surface ${id}`);
      if (fixture && !fixture.surfaceIds.includes(id)) fail(`result fixture surface mismatch ${result.id}/${id}`);
    }
    if (fixture) same(sorted(references), sorted(fixture.surfaceIds), `result fixture surfaces ${result.id}`);
    if (fixture && result.provenance !== fixture.provenance) fail(`result fixture provenance mismatch ${result.id}`);
    if (['synthetic', 'descriptor-only'].includes(result.state) && !fixture) fail(`result fixture binding missing ${result.id}`);
    if (result.state === 'verified-live' || result.liveStatus === 'PASS' || (result.status === 'PASS' && result.provenance === 'verified-live')) {
      if (fixture || result.provenance !== 'verified-live' || result.consumerDecoded !== true || !result.evidenceSha256?.match(/^[a-f0-9]{64}$/) || !result.client?.version || !result.client?.binary_sha256?.match(/^[a-f0-9]{64}$/)) fail(`synthetic or unproven live PASS ${result.id}`);
    }
  }
  const families = array(ledger.fixtureFamilies, 'fixture families');
  same(sorted(unique(families.map((family) => family.id), 'fixture family')), Object.keys(FIXTURE_FAMILIES).sort(), 'required fixture families');
  for (const family of families) {
    same(family.surfaceIds, FIXTURE_FAMILIES[family.id], 'fixture family surfaces');
    for (const id of family.surfaceIds) if (!surfaceIds.has(id)) fail(`unknown family surface ${id}`);
    if (family.status !== 'NOT-RUN') fail(`unexecuted family claim ${family.id}`);
    if (!array(family.fixtureIds, 'family fixture IDs').length || family.fixturePresence !== 'present') fail(`missing required fixture family ${family.id}`);
    for (const id of unique(array(family.fixtureIds, 'family fixture IDs'), 'family fixture ID')) {
      const fixture = fixtureMap.get(id) ?? fail(`unknown family fixture ${id}`);
      if (!fixture.surfaceIds.some((surfaceId) => family.surfaceIds.includes(surfaceId))) fail(`unrelated family fixture ${id}`);
    }
    const expectedFixtures = sorted(fixtures.filter((fixture) => fixture.surfaceIds.some((id) => family.surfaceIds.includes(id))).map((fixture) => fixture.id));
    same(family.fixtureIds, expectedFixtures, `family fixture linkage ${family.id}`);
  }
  for (const surface of surfaces) {
    same(surface.id, `${surface.cli}:${surface.surface_id}`, 'canonical surface id');
    if (!['codex', 'agy'].includes(surface.cli)) fail(`unknown CLI ${surface.cli}`);
    const client = object(surface.client, 'client metadata');
    text(client.name, 'client name'); text(client.version, 'client version');
    if (!Object.hasOwn(client, 'source_revision') || (client.source_revision !== null && !/^[a-f0-9]{40}$/.test(client.source_revision))) fail(`source revision missing or malformed ${surface.id}`);
    for (const hash of array(client.binary_sha256, 'binary hashes')) if (!/^[a-f0-9]{64}$/.test(hash)) fail(`binary hash malformed ${surface.id}`);
    if (!client.binary_sha256.length) fail(`binary hash missing ${surface.id}`);
    array(surface.aliases, 'aliases'); text(surface.transport, 'transport'); text(surface.method, 'method'); text(surface.path, 'path');
    const references = array(surface.sourceEvidence?.references, 'source references');
    if (!references.length) fail(`missing source evidence ${surface.id}`);
    for (const reference of references) if (!(typeof reference.url === 'string' || typeof reference.relative_id === 'string')) fail(`invalid source reference ${surface.id}`);
    text(surface.featureIssue?.assignment, 'feature assignment');
    if (!HANDOFF_KEYS.has(surface.featureIssue.assignment)) fail(`unknown assignment ${surface.id}`);
    if (!Number.isSafeInteger(surface.featureIssue?.number) || !/^https:\/\/github\.com\/[^/?#]+\/[^/?#]+\/issues\/\d+$/.test(surface.featureIssue?.url ?? '')) fail(`feature issue missing ${surface.id}`);
    for (const key of unique(array(surface.dependencies, 'dependencies'), 'dependency')) if (!HANDOFF_KEYS.has(key)) fail(`unknown dependency ${key}`);
    for (const value of unique(array(surface.prerequisites, 'prerequisites'), 'prerequisite')) if (!EXTERNAL_PREREQUISITES.has(value)) fail(`unknown external prerequisite ${value}`);
    text(surface.sourceEvidence.evidence_level, 'original evidence level');
    text(surface.sourceEvidence.provenance, 'source provenance');
    if (!COVERAGE_STATES.includes(surface.sourceEvidence?.state)) fail(`unknown evidence state ${surface.id}`);
    if (!COVERAGE_STATES.includes(surface.disposition?.state)) fail(`unknown disposition ${surface.id}`);
    if (['blocked-contract', 'blocked-account', 'deferred-reachability'].includes(surface.disposition.state)) for (const key of ['owner', 'reason', 'nextProof']) text(surface.disposition[key], `deferred ${key}`);
    if (surface.disposition.owner !== surface.featureIssue?.assignment) fail(`owner mismatch ${surface.id}`);
    if (surface.observation?.liveStatus !== 'NOT-RUN' || surface.observation?.consumerDecoded !== false || surface.disposition.state === 'verified-live' || surface.sourceEvidence.state === 'verified-live') fail(`imported evidence cannot become live PASS ${surface.id}`);
    for (const [forward, reverse] of [['actualActionIds', 'actualSurfaceIds'], ['candidateActionIds', 'candidateSurfaceIds']]) {
      unique(array(surface[forward], forward), forward);
      for (const actionId of surface[forward]) { const action = actionMap.get(actionId) ?? fail(`orphan action ${actionId}`); if (!action[reverse].includes(surface.id)) fail(`missing reverse ${actionId}/${surface.id}`); }
    }
  }
  for (const action of actions) for (const [forward, reverse] of [['actualSurfaceIds', 'actualActionIds'], ['candidateSurfaceIds', 'candidateActionIds']]) {
    unique(array(action[forward], forward), forward);
    for (const id of action[forward]) { const surface = surfaceMap.get(id) ?? fail(`unknown surface ${id}`); if (!surface[reverse].includes(action.id)) fail(`missing reverse ${id}/${action.id}`); }
  }
  for (const item of [...surfaces, ...actions]) {
    if (item.cli && actionIds.has(item.id)) text(item.consumer_action, 'consumer action');
    if (actionIds.has(item.id)) {
      for (const key of array(item.dependencies, 'action dependencies')) if (!HANDOFF_KEYS.has(key)) fail(`unknown dependency ${key}`);
      const expected = sorted(new Set(array(item.originalIssueRefs, 'original issue refs').map((slug) => DEFAULT_SLUG_CROSSWALK[slug] ?? fail(`unknown original issue ref ${slug}`))));
      same(item.dependencies, expected, 'action dependency crosswalk');
    }
    if (item.test?.status !== 'NOT-RUN') fail(`unexecuted fixture claim ${item.id}`);
    for (const id of unique(array(item.test?.fixtureIds, 'test fixtures'), 'fixture reference')) if (!fixtureMap.has(id)) fail(`unknown fixture reference ${id}`);
    for (const id of unique(array(item.test?.resultIds, 'test results'), 'result reference')) if (!resultMap.has(id)) fail(`unknown result reference ${id}`);
    const rowSurfaces = item.surface_id ? [item.id] : [...item.actualSurfaceIds, ...item.candidateSurfaceIds];
    const expectedFixtures = sorted(fixtures.filter((fixture) => fixture.surfaceIds.some((id) => rowSurfaces.includes(id))).map((fixture) => fixture.id));
    const expectedResults = sorted(results.filter((result) => result.surfaceIds.some((id) => rowSurfaces.includes(id))).map((result) => result.id));
    same(item.test.fixtureIds, expectedFixtures, `fixture linkage ${item.id}`);
    same(item.test.resultIds, expectedResults, `result linkage ${item.id}`);
  }
  return { valid: true, surfaces: surfaces.length, actions: actions.length, livePass: 0 };
}

/** Portable serialization refuses rather than silently rewriting private input. */
export function serializePortableCoverage(ledger, { fixtureManifest = DEFAULT_FIXTURE_MANIFEST, resultManifest = [] } = {}) {
  validateCoverage({ ledger, fixtureManifest, resultManifest });
  const portable = { ...ledger, surfaces: [...ledger.surfaces].sort((a, b) => a.id.localeCompare(b.id)), actions: [...ledger.actions].sort((a, b) => a.id.localeCompare(b.id)) };
  const output = `${JSON.stringify(stable(portable), null, 2)}\n`;
  if (/(?:\/tmp\/|\/data\/|\/Users\/|\/home\/|\/etc\/profiles\/|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}|Bearer\s+\S+|access_token(?!=\{token\})|refresh_token|private_key)/i.test(output)) fail('nonportable or credential-bearing content');
  assertSafeArtifact(output, { sentinels: [] });
  return output;
}
