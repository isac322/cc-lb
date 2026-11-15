import test from 'node:test';
import assert from 'node:assert/strict';
import { LINUX_CLIENT, LINUX_IMAGE, LINUX_PROTOCOL, cleanupOwnedLinuxContainer, hashLinuxBinary, linuxHelperFailureReason, parseLinuxProtocol, recoverOwnedLinuxContainer, resolveLinuxDockerEndpoint, runLinuxConsumer, validateLinuxBinaryStat, validateLinuxInspect } from './linux-consumer.mjs';
import { main } from './run.mjs';
import { realpathSync } from 'node:fs';
import path from 'node:path';
import { findExecutable } from './recorder-replay.mjs';

function protocol() {
  return [
    { type: 'ready', protocol: LINUX_PROTOCOL, client: LINUX_CLIENT, isolation: { uid: 65534, capabilities_zero: true, no_new_privileges: true, loopback_only: true, no_routes: true, external_denied: true, host_canary_denied: true, mounts_verified: true, devices_verified: true }, listening: { address: '127.0.0.1', port: 18080 } },
    { type: 'request', method: 'POST', path: '/synthetic', status: 200, auth_valid: true, body_read: true, body_bytes: 2 },
    { type: 'consumer', exit_code: 0, signal: null, timed_out: false, overflow: false, stdout_b64: Buffer.from('{"status":"SUCCESS","response":"SYNTHETIC_MARKER"}').toString('base64'), stderr_b64: '' },
    { type: 'complete', requests: 1, body_read_calls: 1, auth_validated: true, provider_calls: 0, listeners: [{ owner_pid: 2, owner_process_group: 2, address: '127.0.0.1', port: 12345 }], listener_closure: true },
  ];
}
const encode = (events) => Buffer.from(events.map((event) => JSON.stringify(event)).join('\n') + '\n');

test('Linux protocol decodes child output only in memory', () => {
  const result = parseLinuxProtocol(encode(protocol()));
  assert.match(result.consumer.stdout.toString(), /SYNTHETIC_MARKER/);
  assert.equal(result.complete.provider_calls, 0);
});

for (const [name, mutate] of [
  ['wrong binary pin', (events) => { events[0].client = { ...LINUX_CLIENT, binary_sha256: '0'.repeat(64) }; }],
  ['missing isolation observation', (events) => { delete events[0].isolation.no_routes; }],
  ['wrong listener', (events) => { events[0].listening.address = '0.0.0.0'; }],
  ['provider call', (events) => { events[3].provider_calls = 1; }],
  ['unknown event', (events) => { events.splice(1, 0, { type: 'debug', raw: 'forbidden' }); }],
  ['unauthenticated body', (events) => { events[1].auth_valid = false; }],
  ['header persistence', (events) => { events[1].headers = []; }],
  ['raw body persistence', (events) => { events[1].body = {}; }],
  ['unobserved child listener', (events) => { events[3].listeners = []; }],
  ['listener cleanup failure', (events) => { events[3].listener_closure = false; }],
  ['bad base64', (events) => { events[2].stdout_b64 = '!'; }],
  ['wrong completion count', (events) => { events[3].requests = 2; }],
]) test(`Linux protocol rejects ${name}`, () => {
  const events = protocol(); mutate(events);
  assert.throws(() => parseLinuxProtocol(encode(events)), /QHARNESS_LINUX_/);
});

test('Linux protocol rejects malformed and oversized messages', () => {
  assert.throws(() => parseLinuxProtocol(Buffer.from('not-json')), /QHARNESS_LINUX_/);
  assert.throws(() => parseLinuxProtocol(Buffer.alloc(4 * 1024 * 1024)), /QHARNESS_LINUX_/);
});

function inspection() {
  return [{ Id: 'a'.repeat(64), Image: LINUX_IMAGE, State: { Running: true }, Config: { Labels: { 'qharness.owner': 'owned' }, User: '65534:65534', OpenStdin: true, Tty: false, Entrypoint: ['/opt/qharness/helper'], Cmd: null }, HostConfig: { NetworkMode: 'none', ReadonlyRootfs: true, Privileged: false, LogConfig: { Type: 'none' }, CapAdd: null, CapDrop: ['ALL'], SecurityOpt: ['no-new-privileges'], PidMode: 'private', IpcMode: 'private', UTSMode: 'private', PidsLimit: 128, Memory: 536870912, NanoCpus: 2000000000, Tmpfs: Object.fromEntries(['/home/qharness', '/tmp', '/var/lib/postgresql'].map((target) => [target, 'rw,noexec,nosuid,nodev,size=67108864,uid=65534,gid=65534,mode=700'])) }, Mounts: [{ Type: 'bind', Source: '/owned/agy', Destination: '/opt/qharness/agy', RW: false }], NetworkSettings: { Networks: { none: {} } } }];
}
const expected = { id: 'a'.repeat(64), label: 'owned', mounts: [{ source: '/owned/agy', target: '/opt/qharness/agy' }] };

test('Linux inspect accepts only explicit owned policy', () => assert.equal(validateLinuxInspect(inspection(), expected), true));
for (const [name, mutate] of [
  ['owner', (c) => { c.Config.Labels['qharness.owner'] = 'foreign'; }],
  ['network', (c) => { c.HostConfig.NetworkMode = 'host'; }],
  ['logs', (c) => { c.HostConfig.LogConfig.Type = 'json-file'; }],
  ['writable mount', (c) => { c.Mounts[0].RW = true; }],
  ['additional mount', (c) => { c.Mounts.push({ Type: 'bind', Source: '/foreign', Destination: '/extra', RW: false }); }],
  ['volume', (c) => { c.Mounts.push({ Type: 'volume', Destination: '/var/lib/postgresql' }); }],
  ['missing tmpfs override', (c) => { delete c.HostConfig.Tmpfs['/var/lib/postgresql']; }],
  ['capability', (c) => { c.HostConfig.CapAdd = ['NET_ADMIN']; }],
  ['privilege', (c) => { c.HostConfig.Privileged = true; }],
  ['unknown policy', (c) => { c.HostConfig.SecurityOpt = ['no-new-privileges', 'seccomp=unconfined']; }],
]) test(`Linux inspect rejects ${name}`, () => {
  const value = inspection(); mutate(value[0]);
  assert.throws(() => validateLinuxInspect(value, expected), /QHARNESS_LINUX_/);
});

function successful(stdout = '') {
  return { exitCode: 0, timedOut: false, overflow: false, error: null, cleanupError: null, processGroupCleanup: { status: 'gone' }, stdout: Buffer.from(stdout), stderr: Buffer.alloc(0) };
}

test('Linux cleanup proves exact ownership, stop/wait/remove and absence', async () => {
  const calls = []; const entries = [];
  await cleanupOwnedLinuxContainer({
    id: expected.id, label: expected.label, ledger: async (entry) => entries.push(entry),
    invoke: async (args) => { calls.push(args); return successful(args[0] === 'inspect' ? JSON.stringify(inspection()) : ''); },
  });
  assert.deepEqual(calls.map((args) => args[0]), ['inspect', 'stop', 'wait', 'rm', 'container']);
  assert.equal(entries[0].state, 'removed');
});

test('Linux cleanup refuses foreign container without stop or remove', async () => {
  const calls = [];
  await assert.rejects(cleanupOwnedLinuxContainer({
    id: expected.id, label: 'foreign', ledger: async () => {},
    invoke: async (args) => { calls.push(args); return successful(JSON.stringify(inspection())); },
  }), /QHARNESS_LINUX_CLEANUP_OWNERSHIP_FAILED/);
  assert.equal(calls.length, 1);
});

for (const step of ['inspect', 'stop', 'wait', 'rm', 'container']) test(`Linux cleanup cannot pass failed ${step}`, async () => {
  await assert.rejects(cleanupOwnedLinuxContainer({
    id: expected.id, label: expected.label, ledger: async () => {},
    invoke: async (args) => args[0] === step ? { ...successful(), exitCode: 1 } : successful(args[0] === 'inspect' ? JSON.stringify(inspection()) : ''),
  }), /QHARNESS_LINUX_CLEANUP_FAILED/);
});

test('Linux cleanup cannot pass a remaining container or failed ledger', async () => {
  await assert.rejects(cleanupOwnedLinuxContainer({
    id: expected.id, label: expected.label, ledger: async () => {},
    invoke: async (args) => successful(args[0] === 'inspect' ? JSON.stringify(inspection()) : args[0] === 'container' ? expected.id : ''),
  }), /QHARNESS_LINUX_CLEANUP_FAILED/);
  await assert.rejects(cleanupOwnedLinuxContainer({
    id: expected.id, label: expected.label, ledger: async () => { throw new Error('ledger-failed'); },
    invoke: async (args) => successful(args[0] === 'inspect' ? JSON.stringify(inspection()) : ''),
  }), /ledger-failed/);
});

for (const argv of [
  ['--mode', 'fixtures', '--scratch', '/unused', '--client', 'agy', '--backend', 'linux-container', '--agy-binary', '/unused/agy'],
  ['--mode', 'consumers', '--scratch', '/unused', '--client', 'codex', '--backend', 'linux-container', '--agy-binary', '/unused/agy'],
  ['--mode', 'consumers', '--scratch', '/unused', '--client', 'agy', '--backend', 'automatic'],
  ['--mode', 'consumers', '--scratch', '/unused', '--client', 'agy', '--agy-binary', '/unused/agy'],
  ['--mode', 'consumers', '--scratch', '/unused', '--client', 'agy', '--backend', 'linux-container'],
]) test(`Linux backend rejects invalid explicit selection ${argv.join(' ')}`, async () => {
  await assert.rejects(main(argv), /QHARNESS_INVALID_ARGUMENT/);
});

test('Linux Docker endpoint preserves exact selected local Unix endpoint', () => {
  assert.equal(resolveLinuxDockerEndpoint({ inspectedEndpoint: 'unix:///owned/docker.sock' }), 'unix:///owned/docker.sock');
  assert.equal(resolveLinuxDockerEndpoint({ inspectedEndpoint: 'unix:///default/docker.sock', dockerHost: 'unix:///selected/docker.sock' }), 'unix:///selected/docker.sock');
  assert.equal(resolveLinuxDockerEndpoint({ inspectedEndpoint: 'unix:///context/docker.sock', dockerHost: 'tcp://remote:2375', dockerContext: 'selected-context' }), 'unix:///context/docker.sock');
});

for (const inspectedEndpoint of [undefined, null, {}, '', 'tcp://127.0.0.1:2375', 'ssh://local', 'npipe:////./pipe/docker_engine', 'unix://relative', 'unix://host/socket', 'unix:///socket?query', 'unix:///socket#fragment', 'unix:///%73ocket', 'unix:///socket\n', `unix:///${'a'.repeat(4097)}`]) test('Linux Docker endpoint rejects invalid or non-Unix transport without fallback', () => {
  assert.throws(() => resolveLinuxDockerEndpoint({ inspectedEndpoint }), /QHARNESS_DOCKER_ENDPOINT_REJECTED/);
});

test('Linux Docker endpoint does not fall back from selected remote host or context', () => {
  assert.throws(() => resolveLinuxDockerEndpoint({ inspectedEndpoint: 'unix:///local.sock', dockerHost: 'tcp://remote:2375' }), /QHARNESS_DOCKER_ENDPOINT_REJECTED/);
  assert.throws(() => resolveLinuxDockerEndpoint({ inspectedEndpoint: 'ssh://remote', dockerHost: 'unix:///local.sock', dockerContext: 'remote-context' }), /QHARNESS_DOCKER_ENDPOINT_REJECTED/);
});

for (const [code, reason] of [
  ['config_rejected', 'QHARNESS_LINUX_HELPER_CONFIG_REJECTED'],
  ['isolation_unverified', 'QHARNESS_LINUX_HELPER_ISOLATION_UNVERIFIED'],
  ['client_pin_unverified', 'QHARNESS_LINUX_HELPER_CLIENT_PIN_UNVERIFIED'],
  ['fixture_rejected', 'QHARNESS_LINUX_HELPER_FIXTURE_REJECTED'],
  ['settings_failed', 'QHARNESS_LINUX_HELPER_SETTINGS_FAILED'],
  ['listener_bind_failed', 'QHARNESS_LINUX_HELPER_LISTENER_BIND_FAILED'],
  ['listener_evidence_failed', 'QHARNESS_LINUX_HELPER_LISTENER_EVIDENCE_FAILED'],
  ['server_shutdown_failed', 'QHARNESS_LINUX_HELPER_SERVER_SHUTDOWN_FAILED'],
]) test(`Linux helper exposes only fixed diagnostic for ${code}`, () => {
  const earlyFailure = encode([{ type: 'complete', error: code }]);
  assert.equal(linuxHelperFailureReason(earlyFailure), reason);
  assert.throws(() => parseLinuxProtocol(earlyFailure), { code: reason });
  const events = protocol(); events.at(-1).error = code;
  assert.equal(linuxHelperFailureReason(encode(events)), reason);
  assert.throws(() => parseLinuxProtocol(encode(events)), { code: reason });
});

test('Linux helper unknown or malformed diagnostics retain generic failure without raw text', () => {
  for (const bytes of [
    Buffer.from('raw-secret-output'),
    encode([{ type: 'complete', error: 'config_rejected raw-secret' }]),
    encode([{ type: 'complete', error: 'QHARNESS_ARBITRARY_SECRET' }]),
    encode([{ type: 'complete', error: 'toString' }]),
    encode([{ type: 'complete', error: { message: 'config_rejected' } }]),
    encode([{ type: 'complete', error: 'config_rejected', raw: 'raw-secret' }]),
    encode([{ type: 'complete', error: 'config_rejected' }, { type: 'complete', error: 'config_rejected' }]),
    encode([{ type: 'debug', error: 'config_rejected' }, { type: 'complete', error: 'config_rejected' }]),
    Buffer.alloc(4 * 1024 * 1024),
  ]) {
    assert.equal(linuxHelperFailureReason(bytes), null);
    assert.throws(() => parseLinuxProtocol(bytes), /QHARNESS_LINUX_PROTOCOL_REJECTED/);
  }
  const events = protocol(); events.at(-1).error = 'unknown';
  assert.equal(linuxHelperFailureReason(encode(events)), null);
  assert.throws(() => parseLinuxProtocol(encode(events)), /QHARNESS_LINUX_PROTOCOL_REJECTED/);
});

test('shared executable lookup resolves Node and returns null for a missing CLI', () => {
  const directory = path.dirname(process.execPath);
  assert.equal(findExecutable(path.basename(process.execPath), directory), realpathSync(process.execPath));
  assert.equal(findExecutable('qharness-missing-cli-1324', directory), null);
});

test('Linux binary metadata requires a nonempty regular file within 256 MiB', () => {
  for (const stat of [
    { isFile: () => false, size: 1 },
    { isFile: () => true, size: 0 },
    { isFile: () => true, size: 256 * 1024 * 1024 + 1 },
    { isFile: () => true, size: Infinity },
  ]) assert.throws(() => validateLinuxBinaryStat(stat), /QHARNESS_CLIENT_ARTIFACT_REJECTED/);
  assert.doesNotThrow(() => validateLinuxBinaryStat({ isFile: () => true, size: 256 * 1024 * 1024 }));
});

test('Linux binary hashing rejects a device before consuming its bytes', async () => {
  await assert.rejects(hashLinuxBinary('/dev/zero'), /QHARNESS_CLIENT_ARTIFACT_REJECTED/);
});

const recoveredName = 'qharness-1324-12345678-1234-1234-1234-123456789abc';
const recoveryScope = { name: recoveredName, owner_pid: 123, scratch_dir: '/owned/scratch', fixture_id: 'synthetic-recovery', created_utc: '2026-11-16T00:00:00.000Z' };
function recoveryInspection() {
  const value = inspection();
  value[0].Name = `/${recoveredName}`;
  return value;
}

test('uncertain Docker creation recovers only exact name and label then cleans exact ID', async () => {
  const calls = []; const entries = [];
  const invoke = async (args) => {
    calls.push(args);
    return successful(args[0] === 'inspect' ? JSON.stringify(recoveryInspection()) : '');
  };
  const id = await recoverOwnedLinuxContainer({ name: recoveredName, label: expected.label, invoke });
  assert.equal(id, expected.id);
  await cleanupOwnedLinuxContainer({ id, label: expected.label, invoke, ledger: async (entry) => entries.push(entry), scope: recoveryScope });
  assert.deepEqual(calls[0], ['inspect', recoveredName]);
  assert.equal(calls.slice(1).some((args) => args.includes(recoveredName)), false);
  assert.equal(entries[0].name, recoveredName);
  assert.equal(entries[0].owner_pid, recoveryScope.owner_pid);
  assert.equal(entries[0].scratch_dir, recoveryScope.scratch_dir);
  assert.equal(entries[0].fixture_id, recoveryScope.fixture_id);
  assert.equal(entries[0].created_utc, recoveryScope.created_utc);
  assert.equal(entries[0].state, 'removed');
});

for (const [name, output] of [
  ['missing name', () => ({ ...successful(), exitCode: 1 })],
  ['missing ID', () => { const value = recoveryInspection(); delete value[0].Id; return successful(JSON.stringify(value)); }],
  ['wrong label', () => { const value = recoveryInspection(); value[0].Config.Labels['qharness.owner'] = 'foreign'; return successful(JSON.stringify(value)); }],
  ['wrong name', () => { const value = recoveryInspection(); value[0].Name = '/foreign'; return successful(JSON.stringify(value)); }],
  ['malformed metadata', () => successful('not-json')],
]) test(`uncertain Docker creation refuses ${name} without any deletion`, async () => {
  const calls = [];
  await assert.rejects(recoverOwnedLinuxContainer({
    name: recoveredName, label: expected.label,
    invoke: async (args) => { calls.push(args); return output(); },
  }), /QHARNESS_LINUX_(?:CREATION_UNRESOLVED|CLEANUP_OWNERSHIP_FAILED)/);
  assert.deepEqual(calls, [['inspect', recoveredName]]);
});

test('recovered Docker creation remains failed when exact-ID cleanup fails', async () => {
  const calls = [];
  const invoke = async (args) => {
    calls.push(args);
    if (args[0] === 'stop') return { ...successful(), exitCode: 1 };
    return successful(args[0] === 'inspect' ? JSON.stringify(recoveryInspection()) : '');
  };
  const id = await recoverOwnedLinuxContainer({ name: recoveredName, label: expected.label, invoke });
  await assert.rejects(cleanupOwnedLinuxContainer({ id, label: expected.label, invoke, ledger: async () => {}, scope: recoveryScope }), /QHARNESS_LINUX_CLEANUP_FAILED/);
  assert.equal(calls.some((args) => args[0] === 'rm'), false);
});

test('Linux result never invents a container identity before creation', async () => {
  const result = await runLinuxConsumer({ fixture: null });
  assert.equal(result.executionBackend.containerId, null);
  assert.notEqual(result.status, 'PASS');
  assert.equal(result.executionBackend.imageDigest, LINUX_IMAGE);
  assert.equal(result.client.platform, 'linux');
  assert.equal(result.macosAcceptance, 'BLOCKED');
  assert.equal(result.providerAcceptance, 'NOT-RUN');
  assert.equal(result.runtimeAcceptance, 'NOT-RUN');
  assert.equal(result.fullClientAcceptance, false);
});
