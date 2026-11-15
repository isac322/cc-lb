import { createHash, randomUUID } from 'node:crypto';
import { createServer, connect } from 'node:net';
import { mkdir, mkdtemp, open, readFile, readdir, realpath, writeFile } from 'node:fs/promises';
import { constants as fsConstants } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { boundedProcessSucceeded, buildAgyLaunch, classifyConsumerFailure, findExecutable, observeDecodedConsumerOutput, runBoundedProcess, validateReplayFixture } from './recorder-replay.mjs';
import { assertSafeArtifact, createRedactionContext, sanitizeRecord } from './redaction.mjs';

export const LINUX_CLIENT = Object.freeze({ name: 'agy', version: '1.5.9', platform: 'linux', arch: 'arm64', libc: 'musl', binary_sha256: 'd81a2547df4d29e391725219c1908056885abc6c73fd23819553f622ac9b098d' });
export const LINUX_IMAGE = 'sha256:1018733457011bf88d81db98cf43aac6d5150afc18787dbe204771fedf80ff95';
export const LINUX_PROTOCOL = 'qharness-linux-replay/v1';
const RAW_CAP = 2097152;
const PROTOCOL_CAP = Math.ceil(RAW_CAP * 4 / 3) + 262144;
const error = (code) => Object.assign(new Error(code), { code });
const hash = (bytes) => createHash('sha256').update(bytes).digest('hex');

export function validateLinuxBinaryStat(stat) {
  if (!stat?.isFile?.() || !Number.isSafeInteger(stat.size) || stat.size < 1 || stat.size > 256 * 1024 * 1024) throw error('QHARNESS_CLIENT_ARTIFACT_REJECTED');
}

export async function hashLinuxBinary(filePath) {
  let file;
  try {
    // O_NONBLOCK prevents opening a FIFO from waiting for a writer. fstat is
    // checked on the opened descriptor before any bytes are consumed.
    file = await open(filePath, fsConstants.O_RDONLY | fsConstants.O_NONBLOCK);
    const before = await file.stat();
    validateLinuxBinaryStat(before);
    const digest = createHash('sha256');
    let bytes = 0;
    const stream = file.createReadStream({ autoClose: false, start: 0, end: 256 * 1024 * 1024 });
    for await (const chunk of stream) {
      bytes += chunk.length;
      if (bytes > 256 * 1024 * 1024) throw error('QHARNESS_CLIENT_ARTIFACT_REJECTED');
      digest.update(chunk);
    }
    const after = await file.stat();
    validateLinuxBinaryStat(after);
    if (bytes !== before.size || after.size !== before.size || after.mtimeMs !== before.mtimeMs || after.ctimeMs !== before.ctimeMs) throw error('QHARNESS_CLIENT_ARTIFACT_REJECTED');
    return digest.digest('hex');
  } catch {
    throw error('QHARNESS_CLIENT_ARTIFACT_REJECTED');
  } finally { await file?.close(); }
}
const quote = (value) => `'${String(value).replaceAll("'", "'\\''")}'`;
const HELPER_SOURCES = ['isolation.go', 'listeners.go', 'main.go'];
const HELPER_FAILURES = Object.freeze({
  config_rejected: 'QHARNESS_LINUX_HELPER_CONFIG_REJECTED',
  isolation_unverified: 'QHARNESS_LINUX_HELPER_ISOLATION_UNVERIFIED',
  client_pin_unverified: 'QHARNESS_LINUX_HELPER_CLIENT_PIN_UNVERIFIED',
  fixture_rejected: 'QHARNESS_LINUX_HELPER_FIXTURE_REJECTED',
  settings_failed: 'QHARNESS_LINUX_HELPER_SETTINGS_FAILED',
  listener_bind_failed: 'QHARNESS_LINUX_HELPER_LISTENER_BIND_FAILED',
  listener_evidence_failed: 'QHARNESS_LINUX_HELPER_LISTENER_EVIDENCE_FAILED',
  listener_initial_observation_failed: 'QHARNESS_LINUX_HELPER_LISTENER_INITIAL_OBSERVATION_FAILED',
  listener_late_observation_failed: 'QHARNESS_LINUX_HELPER_LISTENER_LATE_OBSERVATION_FAILED',
  server_shutdown_failed: 'QHARNESS_LINUX_HELPER_SERVER_SHUTDOWN_FAILED',
});

export function linuxHelperFailureReason(bytes) {
  if (!Buffer.isBuffer(bytes) || bytes.length > PROTOCOL_CAP) return null;
  let events;
  try { events = bytes.toString('utf8').trim().split('\n').map((line) => JSON.parse(line)); } catch { return null; }
  if (events.length > 35 || events.some((event) => !event || typeof event !== 'object' || Array.isArray(event) || !['ready', 'request', 'consumer', 'complete'].includes(event.type)) ||
      events.filter((event) => event.type === 'complete').length !== 1 || events.at(-1).type !== 'complete') return null;
  const complete = events.at(-1);
  if (Object.keys(complete).some((key) => !['type', 'requests', 'body_read_calls', 'auth_validated', 'provider_calls', 'listeners', 'listener_closure', 'error'].includes(key)) ||
      typeof complete.error !== 'string' || !Object.hasOwn(HELPER_FAILURES, complete.error)) return null;
  return HELPER_FAILURES[complete.error];
}

async function helperSourceDigest(directory) {
  if (await realpath(directory) !== directory) throw error('QHARNESS_HELPER_SOURCE_SCOPE_REJECTED');
  const entries = await readdir(directory, { withFileTypes: true });
  if (entries.some((entry) => !entry.isFile() || (!HELPER_SOURCES.includes(entry.name) && !/^[a-z_]+_test\.go$/.test(entry.name))) ||
      HELPER_SOURCES.some((name) => !entries.some((entry) => entry.name === name))) throw error('QHARNESS_HELPER_SOURCE_SCOPE_REJECTED');
  const sources = await Promise.all(HELPER_SOURCES.map(async (name) => [name, hash(await readFile(path.join(directory, name)))]));
  return hash(Buffer.from(JSON.stringify(sources)));
}

export function resolveLinuxDockerEndpoint({ inspectedEndpoint, dockerHost, dockerContext }) {
  // DOCKER_CONTEXT has Docker CLI precedence over DOCKER_HOST. Never replace
  // a rejected endpoint with another context or the default daemon.
  const endpoint = !dockerContext && dockerHost ? dockerHost : inspectedEndpoint;
  if (typeof endpoint !== 'string' || endpoint.length > 4096 || !/^unix:\/\/\/[^\x00-\x20\x7f?#%]+$/.test(endpoint)) throw error('QHARNESS_DOCKER_ENDPOINT_REJECTED');
  return endpoint;
}

export function validateLinuxInspect(value, { id, label, mounts, running = true }) {
  const c = Array.isArray(value) && value.length === 1 ? value[0] : null;
  const h = c?.HostConfig; const config = c?.Config;
  if (!c || c.Id !== id || config?.Labels?.['qharness.owner'] !== label || c.Image !== LINUX_IMAGE ||
      c.State?.Running !== running || config.User !== '65534:65534' || config.OpenStdin !== true || config.Tty !== false ||
      config.Entrypoint?.join('\0') !== '/opt/qharness/helper' || (config.Cmd?.length ?? 0) !== 0 ||
      h?.NetworkMode !== 'none' || h.ReadonlyRootfs !== true || h.Privileged !== false ||
      h.LogConfig?.Type !== 'none' || h.CapAdd?.length || h.CapDrop?.join(',').toUpperCase() !== 'ALL' ||
      h.SecurityOpt?.join(',') !== 'no-new-privileges' || !['', 'private'].includes(h.PidMode) || h.IpcMode !== 'private' ||
      !['', 'private'].includes(h.UTSMode ?? '') || h.Devices?.length || h.DeviceRequests?.length || h.DeviceCgroupRules?.length || h.VolumesFrom?.length ||
      Object.keys(h.PortBindings ?? {}).length ||
      Object.keys(c.NetworkSettings?.Networks ?? {}).some((name) => name !== 'none')) throw error('QHARNESS_LINUX_INSPECT_REJECTED');
  if (h.PidsLimit !== 128 || h.Memory !== 536870912 || h.NanoCpus !== 2000000000 || h.PublishAllPorts === true) throw error('QHARNESS_LINUX_RESOURCE_POLICY_REJECTED');
  const expectedTmpfs = ['/home/qharness', '/tmp', '/var/lib/postgresql'];
  if (Object.keys(h.Tmpfs ?? {}).sort().join() !== expectedTmpfs.sort().join() || expectedTmpfs.some((target) => !['rw', 'noexec', 'nosuid', 'nodev', 'size=67108864', 'uid=65534', 'gid=65534', 'mode=700'].every((option) => h.Tmpfs[target].split(',').includes(option)))) throw error('QHARNESS_LINUX_TMPFS_REJECTED');
  const binds = c.Mounts?.filter((mount) => mount.Type === 'bind') ?? [];
  if (binds.length !== mounts.length || c.Mounts.some((mount) => !['bind', 'tmpfs'].includes(mount.Type)) || binds.some((mount) => mount.RW !== false || !mounts.some((expected) => expected.source === mount.Source && expected.target === mount.Destination)) || c.Mounts.filter((mount) => mount.Type === 'tmpfs').some((mount) => !expectedTmpfs.includes(mount.Destination))) throw error('QHARNESS_LINUX_MOUNTS_REJECTED');
  return true;
}

function decodeBase64(value) {
  if (typeof value !== 'string' || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) throw error('QHARNESS_LINUX_PROTOCOL_REJECTED');
  const bytes = Buffer.from(value, 'base64');
  if (bytes.length > RAW_CAP) throw error('QHARNESS_LINUX_PROTOCOL_REJECTED');
  return bytes;
}

export function parseLinuxProtocol(bytes) {
  if (!Buffer.isBuffer(bytes) || bytes.length > PROTOCOL_CAP) throw error('QHARNESS_LINUX_PROTOCOL_REJECTED');
  let events;
  try { events = bytes.toString('utf8').trim().split('\n').map((line) => JSON.parse(line)); } catch { throw error('QHARNESS_LINUX_PROTOCOL_REJECTED'); }
  if (events.some((event) => !event || typeof event !== 'object' || Array.isArray(event))) throw error('QHARNESS_LINUX_PROTOCOL_REJECTED');
  const ready = events[0]; const complete = events.at(-1); const consumers = events.filter((event) => event.type === 'consumer');
  if (complete.error !== undefined) throw error(linuxHelperFailureReason(bytes) ?? 'QHARNESS_LINUX_PROTOCOL_REJECTED');
  if (ready?.type !== 'ready' || ready.protocol !== LINUX_PROTOCOL || complete?.type !== 'complete' || consumers.length !== 1 || events.length > 35 || events.slice(1, -2).some((event) => event.type !== 'request') || events.at(-2)?.type !== 'consumer') throw error('QHARNESS_LINUX_PROTOCOL_REJECTED');
  if (Object.keys(LINUX_CLIENT).some((key) => ready.client?.[key] !== LINUX_CLIENT[key])) throw error('QHARNESS_LINUX_PROTOCOL_PIN_REJECTED');
  const isolation = ready.isolation;
  if (isolation?.uid !== 65534 || ['capabilities_zero', 'no_new_privileges', 'loopback_only', 'no_routes', 'external_denied', 'host_canary_denied', 'mounts_verified', 'devices_verified'].some((key) => isolation[key] !== true) || ready.listening?.address !== '127.0.0.1' || ready.listening.port !== 18080) throw error('QHARNESS_LINUX_ISOLATION_REJECTED');
  const requests = events.filter((event) => event.type === 'request');
  if (!requests.length || requests.some((event) => event.auth_valid !== true || event.body_read !== true || !Number.isInteger(event.body_bytes) || event.body_bytes < 0 || event.body_bytes > RAW_CAP || typeof event.method !== 'string' || typeof event.path !== 'string' || event.status !== 200 || Object.keys(event).some((key) => !['type', 'method', 'path', 'status', 'auth_valid', 'body_read', 'body_bytes'].includes(key))) || complete.requests !== requests.length || complete.body_read_calls !== requests.length || complete.auth_validated !== true || complete.provider_calls !== 0 || !Array.isArray(complete.listeners) || !complete.listeners.length || complete.listener_closure !== true) throw error('QHARNESS_LINUX_EVIDENCE_REJECTED');
  if (complete.listeners.some((listener) => !Number.isInteger(listener.owner_pid) || listener.owner_pid < 2 || !Number.isInteger(listener.owner_process_group) || listener.owner_process_group < 2 || !['127.0.0.1', '::1'].includes(listener.address) || !Number.isInteger(listener.port) || listener.port < 1 || listener.port > 65535 || listener.port === 18080)) throw error('QHARNESS_LINUX_LISTENER_REJECTED');
  const consumer = consumers[0];
  if (!Number.isInteger(consumer.exit_code) || typeof consumer.timed_out !== 'boolean' || typeof consumer.overflow !== 'boolean' || !(consumer.signal === null || typeof consumer.signal === 'string')) throw error('QHARNESS_LINUX_PROTOCOL_REJECTED');
  const stdout = decodeBase64(consumer.stdout_b64); const stderr = decodeBase64(consumer.stderr_b64);
  if (stdout.length + stderr.length > RAW_CAP) throw error('QHARNESS_LINUX_PROTOCOL_REJECTED');
  return { ready, complete, requests, consumer: { stdout, stderr, exitCode: consumer.exit_code, signal: consumer.signal, timedOut: consumer.timed_out, overflow: consumer.overflow } };
}

function positiveCanary(port) {
  return new Promise((resolve, reject) => {
    const socket = connect({ host: '127.0.0.1', port });
    socket.setTimeout(1000);
    socket.once('connect', () => { socket.destroy(); resolve(); });
    socket.once('error', () => { socket.destroy(); reject(error('QHARNESS_HOST_CANARY_FAILED')); });
    socket.once('timeout', () => { socket.destroy(); reject(error('QHARNESS_HOST_CANARY_FAILED')); });
  });
}

export async function recoverOwnedLinuxContainer({ name, label, invoke }) {
  if (!/^qharness-1324-[a-f0-9-]{36}$/.test(name) || typeof label !== 'string' || !label) throw error('QHARNESS_LINUX_CLEANUP_OWNERSHIP_FAILED');
  const inspected = await invoke(['inspect', name]);
  if (!boundedProcessSucceeded(inspected)) throw error('QHARNESS_LINUX_CREATION_UNRESOLVED');
  let details;
  try { details = JSON.parse(inspected.stdout.toString()); } catch { throw error('QHARNESS_LINUX_CREATION_UNRESOLVED'); }
  if (!Array.isArray(details) || details.length !== 1 || !/^[a-f0-9]{64}$/.test(details[0].Id) || details[0].Name !== `/${name}` || details[0].Config?.Labels?.['qharness.owner'] !== label) throw error('QHARNESS_LINUX_CLEANUP_OWNERSHIP_FAILED');
  return details[0].Id;
}

export async function cleanupOwnedLinuxContainer({ id, label, invoke, ledger, scope = {} }) {
  if (!/^[a-f0-9]{64}$/.test(id) || typeof label !== 'string' || !label) throw error('QHARNESS_LINUX_CLEANUP_OWNERSHIP_FAILED');
  const owned = await invoke(['inspect', id]);
  if (!boundedProcessSucceeded(owned)) throw error('QHARNESS_LINUX_CLEANUP_FAILED');
  const details = JSON.parse(owned.stdout.toString());
  if (details.length !== 1 || details[0].Id !== id || details[0].Config?.Labels?.['qharness.owner'] !== label || (scope.name && details[0].Name !== `/${scope.name}`)) throw error('QHARNESS_LINUX_CLEANUP_OWNERSHIP_FAILED');
  if (!boundedProcessSucceeded(await invoke(['stop', '--time', '2', id])) || !boundedProcessSucceeded(await invoke(['wait', id])) || !boundedProcessSucceeded(await invoke(['rm', id]))) throw error('QHARNESS_LINUX_CLEANUP_FAILED');
  const absent = await invoke(['container', 'ls', '-a', '--no-trunc', '--filter', `id=${id}`, '--format', '{{.ID}}']);
  if (!boundedProcessSucceeded(absent) || absent.stdout.toString().trim()) throw error('QHARNESS_LINUX_CLEANUP_FAILED');
  await ledger({ ...scope, kind: 'docker-container', identifier: id, label, state: 'removed', delete_command: `docker rm ${id}` });
}

export async function runLinuxConsumer({ fixture, agyBinaryPath, scratchDir, ledger, deadlineMs = 30000 }) {
  const base = { fixtureId: fixture?.id, classification: 'synthetic-consumer-replay', client: LINUX_CLIENT, fixtureSourceClient: fixture?.client, providerAcceptance: 'NOT-RUN', runtimeAcceptance: 'NOT-RUN', fullClientAcceptance: false, macosAcceptance: 'BLOCKED', executionBackend: { kind: 'docker', imageDigest: LINUX_IMAGE, network: 'none' } };
  let summary = { ...base, status: 'BLOCKED', reason: 'QHARNESS_LINUX_SETUP_FAILED' };
  let id; let canary; let canaryEntry; let label; let docker; let invoke; let containerScope; let createAttempted = false;
  try {
    validateReplayFixture(fixture);
    if (typeof ledger !== 'function' || !Number.isInteger(deadlineMs) || deadlineMs < 1000 || deadlineMs > 30000) throw error('QHARNESS_LINUX_ARGUMENT_INVALID');
    if (fixture.client?.name !== 'agy' || fixture.client.version !== '1.5.9' || fixture.client.binary_sha256 !== 'fad3628ad91fe8613a72f074a1c5fde7cd816517788a5b9be96703f7d256741a') throw error('QHARNESS_CONSUMER_PIN_REQUIRED');
    const consumer = fixture.expected?.consumer;
    if (typeof consumer?.marker !== 'string' || !/^[A-Z0-9_]{8,100}$/.test(consumer.marker)) throw error('QHARNESS_CONSUMER_ACTION_UNSUPPORTED');
    const launch = buildAgyLaunch({ consumer, origin: 'http://127.0.0.1:18080' });
    if (!agyBinaryPath || await hashLinuxBinary(agyBinaryPath) !== LINUX_CLIENT.binary_sha256) throw error('QHARNESS_CLIENT_HASH_MISMATCH');
    docker = findExecutable('docker'); const go = findExecutable('go');
    if (!docker || !go) throw error('QHARNESS_LINUX_CLI_UNAVAILABLE');
    await mkdir(scratchDir, { recursive: true, mode: 0o700 }); scratchDir = await realpath(scratchDir);
    await ledger({ kind: 'scratch-directory', identifier: scratchDir, delete_command: `rm -rf -- ${quote(scratchDir)}`, state: 'retained' });
    const env = { PATH: '/usr/bin:/bin', HOME: scratchDir, LANG: 'C', TMPDIR: scratchDir };
    const metadataEnv = Object.fromEntries(['PATH', 'HOME', 'DOCKER_CONFIG', 'DOCKER_CONTEXT', 'DOCKER_HOST'].filter((key) => process.env[key] !== undefined).map((key) => [key, process.env[key]]));
    const endpointMetadata = await runBoundedProcess(docker, ['context', 'inspect', '--format', '{{json .Endpoints.docker.Host}}'], { cwd: scratchDir, scratchDir, ledger, env: metadataEnv, deadlineMs: 5000, maxBytes: 16384 });
    if (!boundedProcessSucceeded(endpointMetadata)) throw error('QHARNESS_DOCKER_ENDPOINT_UNAVAILABLE');
    let inspectedEndpoint;
    try { inspectedEndpoint = JSON.parse(endpointMetadata.stdout.toString()); } catch { throw error('QHARNESS_DOCKER_ENDPOINT_UNAVAILABLE'); }
    const endpoint = resolveLinuxDockerEndpoint({ inspectedEndpoint, dockerHost: metadataEnv.DOCKER_HOST, dockerContext: metadataEnv.DOCKER_CONTEXT });
    const dockerConfig = await mkdtemp(path.join(scratchDir, 'docker-config-'));
    const dockerEnv = { ...env, DOCKER_CONFIG: dockerConfig, DOCKER_HOST: endpoint };
    invoke = (args, options = {}) => runBoundedProcess(docker, args, { cwd: scratchDir, scratchDir, ledger, env: dockerEnv, deadlineMs: 5000, ...options });
    const source = fileURLToPath(new URL('./linux-helper/', import.meta.url)).replace(/\/$/, '');
    const sourceDigest = await helperSourceDigest(source);
    const helper = path.join(scratchDir, 'linux-helper');
    const compiler = await runBoundedProcess(go, ['version'], { cwd: scratchDir, env, scratchDir, ledger, deadlineMs: 5000 });
    if (!boundedProcessSucceeded(compiler) || !/^go version go\d+\.\d+(?:\.\d+)? [a-z0-9]+\/[a-z0-9]+\s*$/.test(compiler.stdout.toString())) throw error('QHARNESS_GO_VERSION_UNAVAILABLE');
    const built = await runBoundedProcess(go, ['build', '-trimpath', '-buildvcs=false', '-o', helper, '.'], { cwd: source, scratchDir, ledger, deadlineMs: 60000, env: { ...env, CGO_ENABLED: '0', GOOS: 'linux', GOARCH: 'arm64', GOENV: 'off', GOWORK: 'off', GO111MODULE: 'off', GOPROXY: 'off', GOSUMDB: 'off', GOTOOLCHAIN: 'local', GOCACHE: path.join(scratchDir, 'go-cache'), GOPATH: path.join(scratchDir, 'go-path') } });
    if (!boundedProcessSucceeded(built)) throw error('QHARNESS_GO_BUILD_FAILED');
    if (await helperSourceDigest(source) !== sourceDigest) throw error('QHARNESS_HELPER_SOURCE_CHANGED');
    base.executionBackend.helper = { source_sha256: sourceDigest, source_files: HELPER_SOURCES, source_hash_scheme: 'sha256-json-sorted-filename-sha256-v1', compiler: compiler.stdout.toString().trim(), compiler_sha256: hash(await readFile(go)), binary_sha256: hash(await readFile(helper)) };
    const context = createRedactionContext({ scenarioId: fixture.id, policyVersion: 'qharness-v1' });
    const safeFixture = Buffer.from(JSON.stringify(sanitizeRecord(fixture, context)));
    assertSafeArtifact(safeFixture, { sentinels: [], allowedSyntheticTokens: [] });
    const fixturePath = path.join(scratchDir, 'linux-fixture.json'); await writeFile(fixturePath, safeFixture, { mode: 0o644 });
    const mounts = [{ source: await realpath(agyBinaryPath), target: '/opt/agy' }, { source: helper, target: '/opt/qharness/helper' }, { source: fixturePath, target: '/opt/qharness/fixture.json' }];
    if (mounts.some((mount) => /[,\n\r]/.test(mount.source))) throw error('QHARNESS_LINUX_MOUNT_PATH_INVALID');
    label = randomUUID();
    const name = `qharness-1324-${label}`;
    containerScope = { name, label, owner_pid: process.pid, scratch_dir: scratchDir, fixture_id: fixture.id, created_utc: new Date().toISOString(), delete_command: `id=$(docker inspect --format '{{.Id}}' ${name}) && [ "$(docker inspect --format '{{ index .Config.Labels "qharness.owner" }}' "$id")" = ${quote(label)} ] && docker stop "$id" && docker wait "$id" && docker rm "$id"` };
    await ledger({ ...containerScope, kind: 'docker-container', identifier: name, state: 'planned' });
    const tmpfs = ['/home/qharness', '/tmp', '/var/lib/postgresql'].flatMap((target) => ['--tmpfs', `${target}:rw,noexec,nosuid,nodev,size=67108864,uid=65534,gid=65534,mode=700`]);
    createAttempted = true;
    const created = await invoke(['create', '-i', '--name', name, '--pull', 'never', '--platform', 'linux/arm64', '--label', `qharness.owner=${label}`, '--log-driver', 'none', '--network', 'none', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--user', '65534:65534', '--ipc', 'private', '--pids-limit', '128', '--memory', '512m', '--cpus', '2', '--entrypoint', '/opt/qharness/helper', ...tmpfs, ...mounts.flatMap((mount) => ['--mount', `type=bind,source=${mount.source},target=${mount.target},readonly`]), LINUX_IMAGE]);
    const candidate = created.stdout.toString().trim();
    if (!/^[a-f0-9]{64}$/.test(candidate)) throw error('QHARNESS_LINUX_CREATE_FAILED');
    id = candidate;
    await ledger({ ...containerScope, kind: 'docker-container', identifier: id, state: 'active', delete_command: `docker stop ${id}; docker wait ${id}; docker rm ${id}` });
    if (!boundedProcessSucceeded(created)) throw error('QHARNESS_LINUX_CREATE_FAILED');
    if (!boundedProcessSucceeded(await invoke(['start', id]))) throw error('QHARNESS_LINUX_START_FAILED');
    const inspected = await invoke(['inspect', id]);
    if (!boundedProcessSucceeded(inspected)) throw error('QHARNESS_LINUX_INSPECT_FAILED');
    validateLinuxInspect(JSON.parse(inspected.stdout.toString()), { id, label, mounts });
    canary = createServer((socket) => socket.destroy());
    await new Promise((resolve, reject) => { canary.once('error', reject); canary.listen(0, '127.0.0.1', resolve); });
    const port = canary.address().port;
    canaryEntry = { kind: 'listener', identifier: `127.0.0.1:${port}`, owner_pid: process.pid, close_identity: `linux-consumer:${label}`, delete_command: 'await canary.close()', cleanup_language: 'javascript-in-owning-process' };
    await ledger({ ...canaryEntry, state: 'active' }); await positiveCanary(port);
    const input = Buffer.from(JSON.stringify({ protocol: LINUX_PROTOCOL, fixture_path: '/opt/qharness/fixture.json', client: LINUX_CLIENT, launch: { argv: ['/opt/agy', ...launch.argv], env: { ...launch.environment, HOME: '/home/qharness', TMPDIR: '/tmp', PATH: '/usr/bin:/bin', LANG: 'C.UTF-8', TERM: 'dumb', NO_COLOR: '1' }, settings: launch.settings }, consumer: { auth_mode: launch.authMode }, host_canary_port: port, limits: { max_requests: 32, max_bytes: RAW_CAP, deadline_ms: deadlineMs } }));
    const attached = await invoke(['attach', '--sig-proxy=false', id], { input, deadlineMs: deadlineMs + 5000, maxBytes: PROTOCOL_CAP });
    await positiveCanary(port);
    const helperFailure = linuxHelperFailureReason(attached.stdout);
    if (helperFailure) throw error(helperFailure);
    if (!boundedProcessSucceeded(attached) || attached.stderr.length) throw error('QHARNESS_LINUX_ATTACH_FAILED');
    const proof = parseLinuxProtocol(attached.stdout);
    const decoded = observeDecodedConsumerOutput({ client: 'agy', stdout: proof.consumer.stdout, marker: consumer.marker });
    const reason = classifyConsumerFailure(proof.consumer) ?? (!decoded.markerObserved ? 'QHARNESS_CONSUMER_MARKER_NOT_OBSERVED' : !decoded.completed ? 'QHARNESS_CONSUMER_TURN_INCOMPLETE' : null);
    summary = { ...base, ...decoded, status: reason ? 'FAIL' : 'PASS', reason, requestsObserved: proof.requests.length, isolation: proof.ready.isolation, ownedListenerAudit: { sampled: true, continuousIngressEnforcement: false, listeners: proof.complete.listeners, closed: true }, exitCode: proof.consumer.exitCode, signal: proof.consumer.signal, timedOut: proof.consumer.timedOut, outputOverflow: proof.consumer.overflow };
  } catch (cause) { summary = { ...base, status: createAttempted ? 'FAIL' : 'BLOCKED', reason: cause.code?.startsWith('QHARNESS_') ? cause.code : 'QHARNESS_LINUX_OPERATION_FAILED' }; }
  finally {
    if (canary) {
      await new Promise((resolve) => canary.close(resolve));
      try { await ledger({ ...canaryEntry, state: 'removed' }); } catch { summary.status = 'FAIL'; summary.reason = 'QHARNESS_CLEANUP_LEDGER_FAILED'; }
    }
    if (createAttempted) {
      try {
        if (!id) {
          id = await recoverOwnedLinuxContainer({ name: containerScope.name, label, invoke });
          try { await ledger({ ...containerScope, kind: 'docker-container', identifier: id, state: 'recovered', delete_command: `docker stop ${id}; docker wait ${id}; docker rm ${id}` }); }
          catch { summary.status = 'FAIL'; summary.reason = 'QHARNESS_CLEANUP_LEDGER_FAILED'; }
        }
        await cleanupOwnedLinuxContainer({ id, label, invoke, ledger, scope: containerScope });
      } catch (cause) {
        summary.status = 'FAIL'; summary.reason = cause.code?.startsWith('QHARNESS_') ? cause.code : 'QHARNESS_LINUX_CLEANUP_FAILED';
        try { await ledger({ ...containerScope, kind: 'docker-container', identifier: id ?? containerScope.name, state: 'unresolved', cleanup_reason: summary.reason }); }
        catch { summary.reason = 'QHARNESS_CLEANUP_LEDGER_FAILED'; }
      }
    }
  }
  summary.executionBackend.containerId = id ?? null;
  return summary;
}
