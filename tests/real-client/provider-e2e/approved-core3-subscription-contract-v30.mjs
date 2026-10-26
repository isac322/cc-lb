#!/usr/bin/env node
// Default CLI is metadata-only: no credential sources, DBs, URLs or token endpoints are opened.
// Exported runtime preparer is FUTURE PARENT-ONLY opt-in; caller supplies access-only material in memory.
import { pathToFileURL, fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { readFile, realpath } from 'node:fs/promises';
import path from 'node:path';
import { createProviderBoundaryRelay, isProviderBoundaryRelay } from './approved-core3-provider-boundary.mjs';
const REVISION_ROOT = path.dirname(fileURLToPath(import.meta.url));
const CAP30_CONTRACT = path.join(REVISION_ROOT, 'approved-core3-cap30-contract.json');
const CAP30_MANIFEST = path.join(REVISION_ROOT, 'approved-core3-cap30-manifest.json');

export const CURRENT_NATIVE_MODEL_HELPER = Object.freeze({
  module: './approved-core3-subscription-current-native.mjs',
  exports: ['parseCurrentNativeModelEvidence', 'loadCurrentNativeModelEvidence'],
  mode: 'LOCAL_METADATA_ONLY',
  current_user_session_default_proven: false,
  provider_qa_verified: false,
  no_model_override: true,
});

export const SUBSCRIPTION_CONTRACT = Object.freeze({
  current_native_model_evidence: CURRENT_NATIVE_MODEL_HELPER,
  kind: 'access_only_never_refresh_metadata_contract', version: 1,
  providers: ['openai_oauth', 'gemini_oauth'],
  source_state: 'OWNER_METADATA_LOCATED_SOURCES_ACTUAL_ACCESS_NOT_READ_HERE', live_qa: 'NOT_RUN_PENDING_SECURITY_REVIEW_SAFE_PARSER_APPROVAL_AND_NEW_BINARY_GREEN',
  scope: 'Default descriptor contract only. Exported parent-only preparer uses the supported local admin import after explicit gates; it never reads broker/grant files itself.',
  required_metadata: ['kind', 'provenance', 'access_token_present', 'refresh_token_present', 'refresh_expiry_metadata_present', 'never_refresh', 'access_expires_in_seconds', 'account_id_present', 'project_id_present', 'no_refresh_endpoint_calls', 'source_grant_mutated', 'source_grant_copied'],
  wire_contract: {
    openai_oauth: { base_url: 'https://chatgpt.com/backend-api/codex', required_route: '/backend-api/codex/responses', status: 'Native execution blocked until approved default URL regression gate is GREEN', authorization: 'Bearer access-only; optional ChatGPT-Account-ID', body: 'unchanged bytes', account: 'optional; signer may derive from JWT in memory', project: 'forbidden' },
    gemini_oauth: { base_url: 'https://daily-cloudcode-pa.googleapis.com', routes: ['/v1internal:generateContent', '/v1internal:streamGenerateContent'], authorization: 'Bearer access-only; remove x-goog-api-key', body: 'existing provider signer envelope model/project/request; no speculative CLIProxy userAgent/requestType/requestId/sessionId/maxOutputTokens change', project: 'required nonempty; body/bundle mismatch rejected' },
  },
  future_import_shape: {
    note: 'Existing OAuthTokenBundle schema requires explicit refresh_token empty string, scopes empty array and expires_at_unix_secs. Only the gated future parent preparer constructs the access-only bundle in memory.',
    required: ['access_token', 'refresh_token', 'scopes', 'expires_at_unix_secs', 'never_refresh'],
    constraints: { refresh_token: 'empty string only', scopes: 'explicit array, no inferred permissions', never_refresh: true, expires_at_unix_secs: 'future', refresh_token_expires_at_unix_secs: 'absent', account_id: 'OpenAI optional metadata only', project_id: 'Gemini required, OpenAI forbidden' },
  },
  owner_evidence: 'CoreAccessOnlyOwner: supported POST /admin/v1/upstreams oauth_tokens import, server-owned AEAD(AAD=new UUID) and atomic storage. Frozen baseline rejected empty refresh/never_refresh; current implementation must pass parent GREEN gates before execution. SafeSubscriptionSourceScout located owner metadata, not current token values. Google observed expiry is stale and must be reread from owner only, never refreshed here.',
  prohibited: ['transferring/copying original grant files or locally reading raw grants', 'refresh/id-token/source ciphertext in transport or logs', 'OAuth refresh', 'token endpoint calls', 'shared OAuth grant update', 'product web or unapproved operational changes', 'generation calls by this preparer'],
});

export function validateSubscriptionDescriptor(value) {
  const allowed = new Set(SUBSCRIPTION_CONTRACT.required_metadata);
  const errors = [];
  if (!value || typeof value !== 'object' || Array.isArray(value)) return { verdict: 'INVALID', errors: ['metadata_object_required'] };
  if (Object.keys(value).some((k) => !allowed.has(k))) errors.push('unknown_field_no_values_or_locators_allowed');
  if (SUBSCRIPTION_CONTRACT.required_metadata.some((k) => !Object.hasOwn(value, k))) errors.push('required_metadata_missing');
  if (!SUBSCRIPTION_CONTRACT.providers.includes(value.kind)) errors.push('unsupported_kind');
  if (value.provenance !== 'existing_shared_grant_read_only_access') errors.push('read_only_provenance_required');
  for (const key of ['access_token_present', 'never_refresh']) if (value[key] !== true) errors.push(`${key}_must_be_true`);
  for (const key of ['refresh_token_present', 'refresh_expiry_metadata_present', 'source_grant_mutated', 'source_grant_copied']) if (value[key] !== false) errors.push(`${key}_must_be_false`);
  if (value.no_refresh_endpoint_calls !== 0) errors.push('refresh_endpoint_must_never_be_called');
  if (!Number.isSafeInteger(value.access_expires_in_seconds) || value.access_expires_in_seconds <= 0) errors.push('future_access_lifetime_required');
  if (typeof value.account_id_present !== 'boolean' || typeof value.project_id_present !== 'boolean') errors.push('identifier_presence_booleans_required');
  if (value.kind === 'gemini_oauth' && value.project_id_present !== true) errors.push('gemini_project_required');
  if (value.kind === 'openai_oauth' && value.project_id_present !== false) errors.push('openai_project_forbidden');
  // PASS means metadata shape only, NEVER live credential or provider functionality proof.
  return { verdict: errors.length ? 'INVALID' : 'PASS_METADATA_ONLY', errors, liveQa: 'NOT_RUN', secretsRead: false, networkCalls: 0 };
}

export const SUBSCRIPTION_RUNNER_CONTRACT = Object.freeze({
  kind: 'prepared_access_only_subscription_runtime', execution: 'NOT_RUN',
  current_native_model_observation: CURRENT_NATIVE_MODEL_HELPER,
  source_projection: {
    owner: 'Parent-approved broker/source adapter only; not this module',
    remote_allowlist: ['access_token', 'expires_at_unix_secs', 'account_id (OpenAI optional)', 'project_id (Gemini required)'],
    transport: 'Remote-select allowlisted fields BEFORE transport; pipe directly to parent memory. Never transfer the original JSON/grant/refresh/id-token, argv, shell tracing, log or temporary credential file.',
    selector_status: 'Implemented by approved-core3-subscription-source.mjs: owner-approved basename hashes, existing remote JSON parser only or separately user-approved official SHA-pinned /tmp jq. Missing parser fails closed; no whole-grant/regex/management-refresh fallback.',
  },
  local_import: { method: 'POST', route: '/admin/v1/upstreams', kind: 'actual openai_oauth or gemini_oauth', warmup_enabled: false, oauth_tokens: { access_token: 'memory only', refresh_token: '', expires_at_unix_secs: 'actual future expiry', scopes: [], never_refresh: true, account_id: 'OpenAI optional, omitted otherwise', project_id: 'Gemini required, omitted for OpenAI' }, forbidden: ['api_key_value', 'workspace_id', 'refresh-token expiry', 'direct DB/AEAD/ciphertext injection'] },
  runtime: 'Fresh separately ledger-owned subscription slice using pinned existing QA runtime lifecycle and safety guards, never an existing/shared principal or OAuth grant.',
  artifact_selection: 'Explicit frozen {path, expected_sha256} from the SHA-pinned paid contract only; exact manifest.binary path/SHA and caller expectedSha agreement plus actual file hash are required before access material. Legacy module constants never select the running binary.',
  generation: 'Pinned original native Codex/OMP Gemini/AGY runners; every paid provider write uses the branded provider-boundary relay and the identical durable guard. Client counters remain additional early stops only. No raw-HTTP native substitute, payload/response morph or extra vendor RPC metadata.',
  host_contract: 'The original real target origin/path/query is retained as the TLS target; only fresh QA upstream seeding uses an owned loopback origin with the same prefix/query. Configured local host and observed real TLS receipts are separate; default-URL/native-direct-bypass coverage is not claimed. Gemini keeps its existing model/project/request signer envelope unchanged.',
  required_gates: ['new binary and approved application access-only tests GREEN', 'new shared-source security review plus parent syntax/unit verification passed', 'safe remote JSON parser available; separately approved exact temporary jq scope is unchanged', 'parent notified exact runtime/observer/provider-boundary ports before creation', 'parent authorizes local scratch import and bootstrap/catalog network', 'safe access-only in-memory projection supplied', 'reference root $30 and explicitly selected fresh-slice $6; historical $15 and $19 evidence unchanged', 'real provider-boundary identity and causal negative-probe/sealed accounting evidence; no single-candidate max-dispatch assumption'],
  effects_when_later_authorized: ['Create one fresh ledger-owned QA runtime/server, SQLite, local keys/principal and actual OAuth upstream via supported server AEAD import', 'Create the separately ledgered owned provider-boundary listener at the exact notified port; getGuard unset must deny generation', 'Original helper may fetch the official price catalog; OpenAI/Gemini never_refresh imports seed no OAuth refresh/bootstrap metadata task, and warmup_enabled remains false', 'Unknown metadata POSTs, onboarding/project mutations, manual metadata job seeding, and token refresh are not permitted exceptions', 'This preparer itself launches no native generation; native protocol bytes/warmup remain unchanged and incompatibility is reported, not normalized', 'No resource deletion/stop is implied; command accounting uses future-generation seals, preserving all UNKNOWN/IN_FLIGHT holds and the retained ledger'],
});

const runnerMust = (ok, code) => { if (!ok) throw new Error(`BLOCKED:${code}`); };
const runnerHash = (text) => createHash('sha256').update(text).digest('hex');
function replaceRunnerAnchor(text, from, to, name) {
  runnerMust(text.split(from).length === 2, `subscription_helper_source_drift:${name}`);
  return text.replace(from, to);
}

export function subscriptionRuntimePlan({ epoch, expectedSha, kind, baseUrl }) {
  const root = path.resolve(epoch ?? '');
  runnerMust(path.dirname(root) === '/tmp/cc-lb-actual-cli-e2e-20261020' && /^fixed-[0-9a-f]{12}-core3(?:-[a-z0-9-]{1,16})?$/.test(path.basename(root)), 'fresh_core3_epoch_required');
  runnerMust(/^[0-9a-f]{64}$/.test(expectedSha ?? '') && expectedSha !== 'afb27555110c2072a6997adc76901c9d3d7cb0db1265a02496f22621d0b420cb' && path.basename(root).startsWith(`fixed-${expectedSha.slice(0, 12)}-`), 'new_epoch_sha_required');
  runnerMust(SUBSCRIPTION_CONTRACT.providers.includes(kind), 'actual_oauth_kind_required');
  const provider = kind === 'openai_oauth' ? 'openai' : 'gemini';
  const defaultUrl = provider === 'openai' ? 'https://chatgpt.com/backend-api/codex' : 'https://daily-cloudcode-pa.googleapis.com';
  const url = new URL(baseUrl ?? defaultUrl);
  runnerMust(url.protocol === 'https:' && !url.username && !url.password && !url.search && !url.hash, 'provider_url_shape_invalid');
  if (provider === 'openai') runnerMust(url.href.replace(/\/$/, '') === defaultUrl, 'openai_codex_origin_required');
  else runnerMust(['daily-cloudcode-pa.googleapis.com', 'cloudcode-pa.googleapis.com'].includes(url.hostname) && ['/', '/v1internal', '/v1internal/'].includes(url.pathname), 'approved_google_cloudcode_origin_required');
  return { state: 'NOT_RUN', epoch: root, expectedSha, kind, provider, name: `${provider}-subscription`, root: path.join(root, `${provider}-subscription`), baseUrl: url.href.replace(/\/$/, ''), uses_host_override: url.href.replace(/\/$/, '') !== defaultUrl, ports_rule: provider === 'openai' ? '[19500,19501,19502] + manifest.port_offset' : '[19510,19511,19512] + manifest.port_offset', source: 'in-memory access-only projection; original grant never transferred or copied', required_before_call: SUBSCRIPTION_RUNNER_CONTRACT.required_gates };
}

function projectSubscriptionRuntimeSource({ helperText, plan, kind, sliceCostUsd }) {
  const oldGetter = [
    'export async function getCredential(provider) {',
    '  if (credentialCache.has(provider)) return credentialCache.get(provider);',
    '  const descriptor = (await loadDescriptor())[provider];',
    "  if (!descriptor) throw new CredentialError('DESCRIPTOR_MISSING', `no credential descriptor for provider ${provider}`);",
    "  if (!/_api_key$/.test(descriptor.kind)) throw new CredentialError('NOT_API_KEY', `${provider} kind ${descriptor.kind} is not API-key only`);",
    '  let apiKey;',
    '  try {',
    '    apiKey = await retrieveSecret(descriptor.source);',
    '  } catch (error) {',
    '    if (error instanceof CredentialError) throw error;',
    "    throw new CredentialError('SOURCE_UNAVAILABLE', `${provider} credential retrieval failed: ${error.message}`);",
    '  }',
    "  apiKey = typeof apiKey === 'string' ? apiKey.trim() : apiKey;",
    "  if (!apiKey) throw new CredentialError('SOURCE_EMPTY', `${provider} credential source returned no value`);",
    '  remember(apiKey);',
    '  const cred = Object.freeze({ kind: descriptor.kind, baseUrl: descriptor.baseUrl, apiKey });',
    '  credentialCache.set(provider, cred);',
    '  return cred;',
    '}',
  ].join('\n');
  let source = replaceRunnerAnchor(helperText, 'export const ROOT = path.dirname(fileURLToPath(import.meta.url));', `export const ROOT = ${JSON.stringify(plan.epoch)};`, 'isolated_epoch_root');
  const helperSourceCaps = [3, 6].filter((cap) => source.includes(`  sliceCostUsd: ${cap},`));
  runnerMust(helperSourceCaps.length === 1, 'pinned_helper_source_slice_cap_ambiguous');
  const originalSourceSliceCostUsd = helperSourceCaps[0];
  source = replaceRunnerAnchor(source, `  sliceCostUsd: ${originalSourceSliceCostUsd},`, `  sliceCostUsd: ${sliceCostUsd},`, 'fresh_runtime_selected_reference_cap_only');
  source = replaceRunnerAnchor(source, '  campaignCostUsd: 15, // contract C655: 4 CLI slices + 1 fault slice, each $3 / 16 generations', '  campaignCostUsd: 30, // contract C655: 4 CLI slices + 1 fault slice, each $3 / 16 generations', 'fresh_runtime_campaign_cap_v30');
  source = replaceRunnerAnchor(source, oldGetter, [
    'let subscriptionMemoryCredential = null;',
    'export function setSubscriptionMemoryCredential(value) { subscriptionMemoryCredential = value; if (value) { remember(value.oauthTokens.access_token); remember(value.oauthTokens.account_id); remember(value.oauthTokens.project_id); } }',
    'export async function getCredential(provider) {',
    `  if (provider !== ${JSON.stringify(plan.provider)} || !subscriptionMemoryCredential) throw new CredentialError('SUBSCRIPTION_MEMORY_MISSING', 'approved access-only projection missing');`,
    '  return subscriptionMemoryCredential;',
    '}',
  ].join('\n'), 'access_only_memory_source_no_original_credential_reads');
  source = replaceRunnerAnchor(source, '        name: u.name, kind: cred.kind, api_key_value: cred.apiKey, base_url: baseUrl,',
    '        name: u.name, kind: cred.kind, oauth_tokens: cred.oauthTokens, base_url: baseUrl,', 'supported_oauth_admin_import');
  source = replaceRunnerAnchor(source, "note: 'Fresh QA SQLite; API-key upstream rows only, no OAuth.'", "note: 'Fresh QA SQLite; actual access-only OAuth upstream imported through supported admin AEAD API; no upstream API key.'", 'truthful_subscription_sqlite_ledger_note');
  source += '\nlet subscriptionProbeObserver = null;\nexport function setSubscriptionProbeObserver(value) { subscriptionProbeObserver = value; }\n';
  source += '\nlet subscriptionBaseUrlRewrite = null;\nexport function setSubscriptionBaseUrlRewrite(value) { subscriptionBaseUrlRewrite = value; }\n';
  source = replaceRunnerAnchor(source, '    const baseUrl = u.baseUrl ?? cred.baseUrl;', '    const originalBaseUrl = u.baseUrl ?? cred.baseUrl;\n    if (!subscriptionBaseUrlRewrite) throw new Error("BLOCKED:provider_boundary_rewrite_required");\n    const baseUrl = await subscriptionBaseUrlRewrite({ ctx, upstream: u, baseUrl: originalBaseUrl });', 'owned_provider_boundary_at_upstream_seed_only');
  source = replaceRunnerAnchor(source, '    const google = isGoogleEnvelope(res.body);', '    if (subscriptionProbeObserver && res.body?.error) subscriptionProbeObserver({ source: "original_negative_quota_probe", status: res.status, error: res.body.error });\n    const google = isGoogleEnvelope(res.body);', 'full_redacted_negative_probe_error_before_original_summary');
  if (kind === 'gemini_oauth') {
    // Preserve the original negative quota probe and cost/count guard; only its
    // scratch topology precondition changes to this real OAuth upstream.
    source = replaceRunnerAnchor(source, "const GOOGLE_API_HOST = 'generativelanguage.googleapis.com';", "const GOOGLE_API_HOST = '127.0.0.1';", 'owned_google_quota_local_host_receipt_separate');
    source = replaceRunnerAnchor(source, "upstream.kind !== 'gemini_api_key'", "upstream.kind !== 'gemini_oauth'", 'oauth_google_quota_kind');
    source = source.replace('probe requires exactly one real gemini_api_key upstream at generativelanguage.googleapis.com', 'probe requires exactly one actual gemini_oauth upstream at its approved cloudcode host');
  }
  return { source, originalSourceSliceCostUsd };
}

export async function prepareAccessOnlySubscriptionRuntime(options) {
  const { epoch, expectedSha, kind, model, pricedModels = [], baseUrl, artifact: providedArtifact, authorization = {}, sliceName, otlpEndpoint, sliceCostUsd = 6, providerBoundaryPort, getProviderGuard, onProviderBoundary, revisionManifestSha256 } = options;
  const plan = subscriptionRuntimePlan({ epoch, expectedSha, kind, baseUrl });
  runnerMust(sliceCostUsd === 6, 'approved_reference_slice_cap_required');
  if (sliceName !== undefined) {
    runnerMust(['codex-http-subscription', 'codex-ws-subscription', 'codex-ws-reconnect-subscription', 'omp-gemini-subscription', 'agy-subscription', 'omp-gpt-subscription'].includes(sliceName), 'approved_subscription_slice_name_required');
    runnerMust((kind === 'openai_oauth') === (sliceName.startsWith('codex-') || sliceName === 'omp-gpt-subscription'), 'native_slice_provider_mismatch');
    plan.name = sliceName;
    plan.root = path.join(plan.epoch, sliceName);
  }
  // These are explicit invocation gates, not authorization granted by shipping this module.
  runnerMust(authorization.application_access_only_qa_passed === true, 'access_only_application_gates_not_green');
  runnerMust(authorization.parent_new_binary_green === true && authorization.parent_security_review_passed === true, 'parent_new_binary_and_source_security_review_required');
  runnerMust(authorization.parent_resources_notified === true && authorization.allow_local_oauth_import === true && authorization.allow_runtime_bootstrap_network === true, 'parent_runtime_import_and_bootstrap_authorization_required');
  if (plan.uses_host_override) runnerMust(authorization.provider_host_override_approved === true, 'provider_host_override_not_approved');
  runnerMust(providedArtifact && typeof providedArtifact === 'object' && !Array.isArray(providedArtifact) && Object.keys(providedArtifact).length === 2 && Object.keys(providedArtifact).every((key) => key === 'path' || key === 'expected_sha256'), 'subscription_explicit_artifact_required');
  const requestedArtifact = { path: providedArtifact.path, expected_sha256: providedArtifact.expected_sha256 };
  runnerMust(typeof requestedArtifact.path === 'string' && path.isAbsolute(requestedArtifact.path) && requestedArtifact.expected_sha256 === expectedSha, 'subscription_explicit_artifact_invalid');
  const manifest = JSON.parse(await readFile(path.join(plan.epoch, 'fixed-epoch.json'), 'utf8'));
  const contractText = await readFile(CAP30_CONTRACT, 'utf8');
  const revisionManifestText = await readFile(CAP30_MANIFEST, 'utf8');
  runnerMust(/^[0-9a-f]{64}$/.test(revisionManifestSha256 ?? '') && runnerHash(revisionManifestText) === revisionManifestSha256, 'cap30_revision_manifest_sha_required');
  const revisionManifest = JSON.parse(revisionManifestText);
  runnerMust(revisionManifest.source_sha256?.['approved-core3-cap30-contract.json'] === runnerHash(contractText), 'cap30_contract_manifest_hash_mismatch');
  const sourceContract = JSON.parse(contractText);
  runnerMust(typeof sourceContract.execution_release === 'boolean' && typeof revisionManifest.execution_release === 'boolean' && sourceContract.execution_release === revisionManifest.execution_release, 'cap30_execution_release_metadata_mismatch');
  runnerMust(sourceContract.execution_release === true, 'final_artifact_pending');
  runnerMust(sourceContract.paid_bounds?.generation_attempts_per_slice === 16 && [3, 6].includes(sourceContract.paid_bounds.usd_ceiling_per_slice) && sourceContract.paid_bounds.usd_ceiling_campaign === 30, 'subscription_source_paid_contract_required');
  runnerMust(typeof sourceContract.artifact?.path === 'string' && path.isAbsolute(sourceContract.artifact.path) && sourceContract.artifact.expected_sha256 === expectedSha, 'subscription_source_artifact_invalid');
  const artifact = Object.freeze({ path: sourceContract.artifact.path, expected_sha256: sourceContract.artifact.expected_sha256 });
  runnerMust(requestedArtifact.path === artifact.path && requestedArtifact.expected_sha256 === artifact.expected_sha256, 'subscription_explicit_artifact_mismatch');
  runnerMust(manifest.binary?.path === artifact.path && manifest.binary.expected_sha256 === artifact.expected_sha256, 'subscription_manifest_artifact_mismatch');
  runnerMust(runnerHash(await readFile(artifact.path)) === artifact.expected_sha256, 'subscription_current_binary_mismatch');
  const helperText = await readFile(path.join(plan.epoch, 'qa-runtime.mjs'), 'utf8');
  runnerMust(runnerHash(helperText) === manifest.files?.['qa-runtime.mjs']?.sha256, 'subscription_helper_not_pinned');
  // Artifact and source identities are verified before any access-only material is read.
  const { descriptor } = options;
  runnerMust(validateSubscriptionDescriptor(descriptor).verdict === 'PASS_METADATA_ONLY' && descriptor.kind === kind, 'safe_subscription_descriptor_required');
  runnerMust(typeof model === 'string' && /^[a-zA-Z0-9._-]+$/.test(model), 'approved_model_required');
  const { accessMaterial } = options;
  runnerMust(accessMaterial && typeof accessMaterial === 'object' && !Array.isArray(accessMaterial), 'in_memory_access_projection_required');
  const allowed = new Set(['access_token', 'expires_at_unix_secs', 'account_id', 'project_id']);
  runnerMust(Object.keys(accessMaterial).every((key) => allowed.has(key)), 'refresh_idtoken_grant_or_unknown_projection_field_forbidden');
  runnerMust(typeof accessMaterial.access_token === 'string' && accessMaterial.access_token.trim().length > 0, 'access_projection_empty');
  runnerMust(Number.isSafeInteger(accessMaterial.expires_at_unix_secs) && accessMaterial.expires_at_unix_secs > Math.floor(Date.now() / 1000), 'access_projection_expired');
  if (plan.provider === 'openai') {
    runnerMust(accessMaterial.project_id === undefined, 'openai_project_forbidden');
    runnerMust(accessMaterial.account_id === undefined || (typeof accessMaterial.account_id === 'string' && accessMaterial.account_id.trim().length > 0), 'openai_account_metadata_invalid');
  } else {
    runnerMust(typeof accessMaterial.project_id === 'string' && accessMaterial.project_id.trim().length > 0, 'gemini_project_required');
    runnerMust(accessMaterial.account_id === undefined, 'gemini_account_projection_unnecessary');
  }
  const offset = manifest.port_offset;
  runnerMust([500, 600, 700, 800, 900, 1000].includes(offset), 'subscription_port_offset_invalid');
  if (otlpEndpoint !== undefined) runnerMust(otlpEndpoint === `http://127.0.0.1:${18563 + offset}` && authorization.parent_collector_ledgered === true, 'existing_owned_epoch_collector_required');
  const firstPort = ({ 'codex-http-subscription': 19500, 'codex-ws-subscription': 19503, 'codex-ws-reconnect-subscription': 19506, 'omp-gpt-subscription': 19520, 'omp-gemini-subscription': 19510, 'agy-subscription': 19530 }[plan.name] ?? (plan.provider === 'openai' ? 19500 : 19510)) + offset;
  const ports = [firstPort, firstPort + 1, firstPort + 2];
  runnerMust(authorization.notified_root === plan.root && JSON.stringify(authorization.notified_ports) === JSON.stringify(ports), 'exact_subscription_resources_not_notified');
  runnerMust(providerBoundaryPort === firstPort + 120 && authorization.notified_provider_boundary_port === providerBoundaryPort && authorization.allow_owned_provider_boundary === true && typeof getProviderGuard === 'function', 'exact_owned_provider_boundary_and_guard_required');
  if (onProviderBoundary !== undefined) runnerMust(typeof onProviderBoundary === 'function', 'private_provider_boundary_observer_invalid');
  const { source, originalSourceSliceCostUsd } = projectSubscriptionRuntimeSource({ helperText, plan, kind, sliceCostUsd });
  // No source/grant file is imported. Node builtins only; no process starts at module evaluation.
  const runtime = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
  runnerMust(await runtime.sha256File(artifact.path) === artifact.expected_sha256, 'subscription_current_binary_mismatch');
  const tokens = { access_token: accessMaterial.access_token, refresh_token: '', expires_at_unix_secs: accessMaterial.expires_at_unix_secs, scopes: [], never_refresh: true };
  if (plan.provider === 'openai' && accessMaterial.account_id !== undefined) tokens.account_id = accessMaterial.account_id;
  if (plan.provider === 'gemini') tokens.project_id = accessMaterial.project_id;
  const accessMatchesSourceInMemory = runnerHash(tokens.access_token) === runnerHash(accessMaterial.access_token);
  runnerMust(accessMatchesSourceInMemory, 'access_import_source_consistency_mismatch');
  runtime.setSubscriptionMemoryCredential({ kind, baseUrl: plan.baseUrl, oauthTokens: tokens });
  let providerBoundary = null;
  runtime.setSubscriptionBaseUrlRewrite(async ({ ctx, baseUrl: originalBaseUrl }) => {
    runnerMust(providerBoundary === null && new URL(originalBaseUrl).href === new URL(plan.baseUrl).href, 'original_subscription_target_changed_or_multiple_boundaries');
    providerBoundary = await createProviderBoundaryRelay({
      ctx, port: providerBoundaryPort, targetBaseUrl: originalBaseUrl,
      protocol: kind === 'openai_oauth' ? 'openai-responses' : 'google-generate-content',
      models: ctx.pricedModels, recordResource: runtime.recordResource, getGuard: getProviderGuard,
    });
    runnerMust(isProviderBoundaryRelay(providerBoundary), 'real_provider_boundary_brand_required');
    if (onProviderBoundary) onProviderBoundary(providerBoundary);
    return providerBoundary.upstreamBaseUrl;
  });
  try {
    // Reserve exact paths/listeners before any directory, SQLite or process creation.
    for (const entry of [
      { kind: 'scratch_directory_reservation', id: plan.root, delete: `After explicit cleanup signal and ownership verification: rm -rf ${plan.root}` },
      { kind: 'sqlite_database_reservation', id: path.join(plan.root, 'runtime/storage.sqlite'), delete: 'Contained in the owned subscription root; stop its recorded process before approved cleanup' },
      ...ports.map((port) => ({ kind: 'listener_reservation', id: `127.0.0.1:${port} ${plan.name}`, port, delete: 'Stop only this subscription root recorded PID after explicit cleanup signal' })),
      { kind: 'process_reservation', id: `${artifact.path} serve --config ${path.join(plan.root, 'runtime/cc-lb.toml')}`, delete: 'Stop only matching recorded PID after explicit cleanup signal' },
      { kind: 'provider_boundary_listener_reservation', id: `127.0.0.1:${providerBoundaryPort} ${plan.name}`, port: providerBoundaryPort, delete: 'Owned relay lifecycle only; retain ledger until explicit user cleanup signal' },
    ]) await runtime.recordResource({ name: plan.name }, entry);
    const ctx = await runtime.prepareSlice({ name: plan.name, artifact, ports, provider: plan.provider, model, pricedModels, ...(otlpEndpoint ? { otlpEndpoint } : {}), upstreams: [{ provider: plan.provider, name: `${plan.name}-access-only`, baseUrl: plan.baseUrl }], resume: false });
    runnerMust(ctx.binaryPath === artifact.path && ctx.binarySha256 === artifact.expected_sha256 && ctx.expectedBinarySha256 === artifact.expected_sha256, 'subscription_prepared_artifact_mismatch');
    runnerMust(ctx.upstreams.length === 1 && ctx.upstreams[0].kind === kind, 'subscription_kind_or_owner_mismatch');
    runnerMust(ctx.limits.client.maxGenerationAttempts === 16 && ctx.limits.client.sliceCostUsd === sliceCostUsd && ctx.limits.client.campaignCostUsd === 30, 'subscription_selected_reference_caps_changed');
    runnerMust(isProviderBoundaryRelay(providerBoundary) && new URL(ctx.upstreams[0].baseUrl).href === new URL(providerBoundary.upstreamBaseUrl).href, 'subscription_upstream_not_the_owned_provider_boundary');
    // Private return only. NEVER console.log/serialize ctx: it contains the local principal/admin keys.
    return { ctx, runtime, providerBoundary, providerTargetBaseUrl: plan.baseUrl, evidence: { verdict: ctx.blocked.length ? 'BLOCKED' : 'PREPARED_NOT_GENERATED', provider: plan.provider, kind, binary_sha256: expectedSha, imported_upstream_id: ctx.upstreamId, never_refresh_requested: true, refresh_secret_supplied: false, original_grant_transferred_or_copied: false, generation_calls: 0, provider_host_verified: false, access_matches_source_in_memory: accessMatchesSourceInMemory, credential_derived_hashes_serialized: false, original_source_slice_usd: originalSourceSliceCostUsd, selected_reference_slice_usd: sliceCostUsd, reference_campaign_usd: 30, subscription_credits_or_invoice_protection_claimed: false, provider_boundary_configured: true, default_url_coverage_claimed: false } };
  } catch {
    // Never echo errors/responses that might contain credentials or project/account identifiers.
    throw new Error('BLOCKED:subscription_runtime_import_or_bootstrap_failed_no_secret_details');
  } finally {
    runtime.setSubscriptionMemoryCredential(null);
    runtime.setSubscriptionBaseUrlRewrite(null);
    // JS strings cannot be securely zeroized; keep caller material in memory only until process exit.
    tokens.access_token = '';
    if ('account_id' in tokens) delete tokens.account_id;
    if ('project_id' in tokens) delete tokens.project_id;
  }
}
const cliEntryPath = process.argv[1] ? await realpath(path.resolve(process.argv[1])).catch(() => null) : null;
if (import.meta.url.startsWith('file:') && cliEntryPath !== null && cliEntryPath === await realpath(fileURLToPath(import.meta.url)).catch(() => null)) {
  if (process.argv.length === 2) console.log(JSON.stringify(SUBSCRIPTION_CONTRACT, null, 2));
  else if (process.argv.length === 3 && process.argv[2] === '--runner-contract') console.log(JSON.stringify(SUBSCRIPTION_RUNNER_CONTRACT, null, 2));
  else if (process.argv.length === 3 && process.argv[2] === '--validate-stdin') {
    let input = '';
    for await (const chunk of process.stdin) { input += chunk; if (input.length > 4096) throw new Error('metadata_input_too_large'); }
    let parsed; try { parsed = JSON.parse(input); } catch { throw new Error('metadata_input_invalid_json'); }
    const result = validateSubscriptionDescriptor(parsed);
    console.log(JSON.stringify(result)); if (result.verdict === 'INVALID') process.exitCode = 1;
  } else throw new Error('usage: approved-core3-subscription-contract.mjs [--validate-stdin|--runner-contract]');
}
