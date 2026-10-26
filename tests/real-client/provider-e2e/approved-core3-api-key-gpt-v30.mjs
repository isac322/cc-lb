// Parent-run source adapter only. No execution on import and no original-file mutation.
// The original HTTP and upgrade paths stay intact. A branded real provider-boundary
// relay, not this client tap, reserves and settles every actual upstream forward.
import { createHash, randomUUID } from 'node:crypto';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { isProviderBoundaryRelay } from './approved-core3-provider-boundary.mjs';
import { pinnedFixtureNode, assertVerifiedOmpClient, ompVersionProbeEnvironment } from './approved-core3-subscription-native-v30.mjs';

const must = (ok, code) => { if (!ok) throw new Error(`BLOCKED:${code}`); };
const sha = (text) => createHash('sha256').update(text).digest('hex');
const once = (text, from, to, name) => { must(text.split(from).length === 2, `gpt_source_drift:${name}`); return text.replace(from, to); };
const data = (text) => `data:text/javascript;base64,${Buffer.from(text).toString('base64')}`;

// The v30 controller is Bun, so the original process.execPath test spawn would
// run Bun instead of Node's test runner. Use the shared pinned Node and require
// real TAP counts; the behavior eval, prompts and request handling stay verbatim.
function pinGptFixtureTests(source) {
  const pinned = once(source, 'const tests = spawnSync(process.execPath, ["--test", "--test-reporter=tap"],', 'const tests = spawnSync(API_KEY_PRIVATE.fixtureNode(), ["--test", "--test-reporter=tap"],', 'gpt_fixture_tests_pinned_node');
  return once(pinned, '  out.ok = tests.status === 0 && out.node_test_counts?.fail === 0 && ', '  out.ok = tests.status === 0 && Number.isSafeInteger(out.node_test_counts?.tests) && out.node_test_counts.tests > 0 && out.node_test_counts.fail === 0 && ', 'gpt_fixture_tests_counted_pass');
}

// Two host legs. The real provider leg is the relay's original HTTPS target. The
// runner's host check compares the owned loopback relay authority (host:port, as
// qa-runtime hostOf() records upstream hosts), like the shared OMP runner.
function pinGptProviderHostLegs(source, providerBoundary) {
  const providerOrigin = providerBoundary.metadata().source_original_url;
  must(providerOrigin?.protocol === 'https:' && providerOrigin.host === 'api.openai.com' && providerOrigin.port === 443, 'gpt_original_provider_https_443_required');
  const ownedAuthority = new URL(providerBoundary.upstreamBaseUrl);
  must(ownedAuthority.protocol === 'http:' && ownedAuthority.hostname === '127.0.0.1' && ownedAuthority.port !== '', 'gpt_owned_loopback_authority_required');
  const pinned = once(source, 'const EXPECTED_PROVIDER_HOST = "api.openai.com";', `const EXPECTED_PROVIDER_HOST = ${JSON.stringify(ownedAuthority.host)};`, 'owned_loopback_authority_real_tls_receipts_separate');
  return once(pinned, String.raw`  const realHost = hostInfo.hosts.length > 0 && hostInfo.hosts.every((host) => !/^(127\.|localhost|\[::1\]|0\.0\.0\.0)/.test(host));`, '  const realHost = hostInfo.hosts.length > 0 && hostInfo.hosts.every((host) => host === EXPECTED_PROVIDER_HOST);', 'owned_loopback_authority_host_check');
}

function pinGptClientIdentity(source) {
  source = once(source, 'const EXPECTED_OMP_VERSION_PREFIX = "18.12.5";', 'const EXPECTED_OMP_VERSION_PREFIX = API_KEY_PRIVATE.approvedClient.version;', 'approved_omp_version_from_verified_identity');
  source = once(source, '  const ompPath = findExecutable("omp");', '  const ompPath = API_KEY_PRIVATE.approvedLauncher();', 'approved_omp_launcher');
  source = once(source, 'env: { HOME: helpHome, PATH: process.env.PATH ?? "", TERM: "dumb", NO_COLOR: "1" }', 'env: API_KEY_PRIVATE.versionEnvironment(helpHome)', 'cloud_key_free_omp_version_probe');
  source = once(source, '  if (!ompVersion.includes(EXPECTED_OMP_VERSION_PREFIX)) summary.blockers.push(`omp_version_unexpected:${ompVersion}`);', '  if (versionOut.error || versionOut.status !== 0 || versionOut.signal != null || ompVersion !== EXPECTED_OMP_VERSION_PREFIX) summary.blockers.push(`omp_version_unexpected:${ompVersion}`);', 'exact_approved_omp_version_gate');
  return once(source, '    const child = spawn(ompBin, args, { cwd, env, stdio: ["ignore", "pipe", "pipe"] });', '    if (ompBin !== API_KEY_PRIVATE.approvedLauncher()) throw Error("BLOCKED:approved_omp_launch_mismatch");\n    const child = spawn(ompBin, args, { cwd, env, stdio: ["ignore", "pipe", "pipe"] });', 'approved_omp_identity_before_native_spawn');
}

export async function buildGuardedApiKeyGptModule(plan, config, pinned, prepared, privateValues, outboundGuard) {
  must(plan.case === 'api-key-omp-gpt' && plan.kind === 'openai_api_key' && plan.native === 'omp-gpt', 'real_api_key_gpt_profile_required');
  must(plan.caps.slice_usd === 6 && plan.caps.case_forwards === 16 && plan.model === 'gpt-5.2', 'original_gpt_scope_and_selected_cap_required');
  // The selected cap30 contract keeps the historical $3 per-slice bound; the
  // historical $15 campaign stays pinned in the campaign manifest, not here.
  must(pinned.contract?.paid_bounds?.usd_ceiling_per_slice === 3 && pinned.contract.paid_bounds.usd_ceiling_campaign === 30, 'selected_contract_caps_required');
  const { runtime, ctx, providerBoundary } = prepared;
  const artifact = pinned.artifact;
  must(artifact && Object.isFrozen(artifact) && typeof artifact.path === 'string' && path.isAbsolute(artifact.path) && /^[0-9a-f]{64}$/.test(artifact.expected_sha256 ?? '') && artifact.expected_sha256 === plan.expectedSha, 'gpt_pinned_paid_artifact_required');
  must(pinned.contract.artifact?.path === artifact.path && pinned.contract.artifact.expected_sha256 === artifact.expected_sha256 && pinned.manifest?.binary?.path === artifact.path && pinned.manifest.binary.expected_sha256 === artifact.expected_sha256, 'gpt_pinned_artifact_pair_mismatch');
  must(ctx.binaryPath === artifact.path && ctx.binarySha256 === artifact.expected_sha256 && ctx.expectedBinarySha256 === artifact.expected_sha256, 'gpt_prepared_artifact_identity_mismatch');
  must(isProviderBoundaryRelay(providerBoundary) && providerBoundary.guardIdentity() === outboundGuard, 'actual_provider_boundary_guard_required_no_client_only_fallback');
  const approvedClient = prepared.approvedClient;
  assertVerifiedOmpClient(approvedClient, pinned.contract);
  const key = `cc-lb-approved-api-key-gpt-${randomUUID()}`;
  const bridge = { runtime, ctx, artifact, providerBoundary, privateValues, fixtureNode: pinnedFixtureNode, approvedClient,
    approvedLauncher: () => assertVerifiedOmpClient(approvedClient, pinned.contract), versionEnvironment: ompVersionProbeEnvironment };
  const helper = Object.keys(runtime).filter((name) => /^[A-Za-z_$][\w$]*$/.test(name)).map((name) => {
    if (name === 'prepareSlice') return `export async function prepareSlice(opts){if(opts.provider!=="openai"||opts.model!=="gpt-5.2")throw Error("BLOCKED:gpt_original_model_drift");return p.ctx;}`;
    if (name === 'BINARY_PATH') return 'export const BINARY_PATH=p.ctx.binaryPath;';
    if (name === 'EXPECTED_BINARY_SHA256') return 'export const EXPECTED_BINARY_SHA256=p.ctx.expectedBinarySha256;';
    return `export const ${name}=p.runtime.${name};`;
  }).join('\n');
  const helperUrl = data(`const p=globalThis[Symbol.for(${JSON.stringify(key)})];if(!p)throw Error("BLOCKED:gpt_private_bridge_missing");\n${helper}`);
  let source = pinned.sources['omp-gpt/run.mjs'];
  must(typeof source === 'string' && typeof pinned.sources['omp-gpt/cache-proof.mjs'] === 'string', 'pinned_gpt_sources_missing');
  source = once(source, 'from "../qa-runtime.mjs"', `from ${JSON.stringify(helperUrl)}`, 'private_guarded_runtime');
  source = once(source, 'from "./cache-proof.mjs"', `from ${JSON.stringify(pathToFileURL(path.join(plan.epoch, 'omp-gpt/cache-proof.mjs')).href)}`, 'pinned_original_cache_module');
  source = once(source, 'const SLICE_DIR = dirname(fileURLToPath(import.meta.url));', `const API_KEY_PRIVATE=globalThis[Symbol.for(${JSON.stringify(key)})];\nconst SLICE_DIR = ${JSON.stringify(ctx.root)};`, 'private_owned_root');
  const binaryPathDeclarations = source.match(/^const BINARY_PATH = "[^"\r\n]+";$/gm) ?? [];
  const binaryShaDeclarations = source.match(/^const EXPECTED_BINARY_SHA256 = "[0-9a-f]{64}";$/gm) ?? [];
  must(binaryPathDeclarations.length === 1 && binaryShaDeclarations.length === 1, 'gpt_source_drift:independent_binary_declarations');
  const privateBridgeOffset = source.indexOf('const API_KEY_PRIVATE=');
  must(privateBridgeOffset >= 0 && privateBridgeOffset < source.indexOf(binaryPathDeclarations[0]) && privateBridgeOffset < source.indexOf(binaryShaDeclarations[0]), 'gpt_private_bridge_must_precede_binary_declarations');
  source = once(source, binaryPathDeclarations[0], 'const BINARY_PATH = API_KEY_PRIVATE.ctx.binaryPath;', 'selected_context_binary_path');
  source = once(source, binaryShaDeclarations[0], 'const EXPECTED_BINARY_SHA256 = API_KEY_PRIVATE.ctx.expectedBinarySha256;', 'selected_context_binary_sha');
  source = pinGptClientIdentity(source);
  source = once(source, 'const FIXTURE_DIR = join(SLICE_DIR, "fixture");', `const FIXTURE_DIR = ${JSON.stringify(path.join(plan.epoch, 'omp-gpt/fixture'))};`, 'original_fixture');
  source = once(source, 'const USD_CEILING_SLICE = 3;', 'const USD_CEILING_SLICE = 6;', 'new_selected_six_only');
  source = pinGptProviderHostLegs(source, providerBoundary);
  source = once(source, '    server.listen(0, "127.0.0.1", () => resolve(server));', `    server.listen(${plan.observer_port}, "127.0.0.1", () => resolve(server));`, 'notified_observer_port');
  pinnedFixtureNode(); // Fail before the native runner starts if the pinned Node is absent or changed.
  source = pinGptFixtureTests(source);
  const tail = '\nmain()\n  .then((summary) => {';
  must(source.split(tail).length === 2, 'gpt_direct_invocation_tail_drift');
  source = source.slice(0, source.indexOf(tail)) + '\nexport { main };\n';
  must(!source.includes('fileURLToPath(import.meta.url)'), 'gpt_unmapped_data_module_path');
  globalThis[Symbol.for(key)] = bridge;
  try {
    const module = await import(data(source));
    return { module, bridgeKey: key, transformedSourceHash: sha(source), originalSource: path.join(plan.epoch, 'omp-gpt/run.mjs'), sourceMap: { 'omp-gpt/run.mjs': sha(source), 'omp-gpt/cache-proof.mjs': sha(pinned.sources['omp-gpt/cache-proof.mjs']) }, guardSourceScope: 'provider-boundary', clientTapBudgetRole: 'original_count_and_early_stop_only', originalSourceSliceUsd: 3, selectedReferenceSliceUsd: 6, defaultUrlCoverageClaimed: false };
  } catch {
    delete globalThis[Symbol.for(key)];
    throw new Error('BLOCKED:guarded_gpt_native_module_load_failed_no_secret_details');
  }
}
