#!/usr/bin/env node
// Fixed-binary QA epoch preparer/launcher (parent-run tool only).
//
//   node fixed-epoch.mjs --expected-sha <64hex> [--client omp-gemini,agy,codex,omp-gpt,faults]
//   node fixed-epoch.mjs --expected-sha <64hex> --client codex --run [--resume] [--deadline-sec 1800]
//   Retry epoch, same binary: add --attempt-label r2 (root fixed-<sha12>-r2) --port-offset 200
//   (185xx -> 187xx; default 100 -> 186xx; faults accepts only 100). Before anything is ledgered or
//   written, a read-only TCP probe aborts if any target port already listens (skipped for --resume).
//   Prior-campaign baseline counts old root slices AND every other fixed-* epoch; USD3/USD15 unchanged.
//
// Default mode is PREPARE-ONLY: verifies the current target binary digest equals --expected-sha,
// then materialises NEW files under <root>/fixed-<sha12>/ (old QA root, ledger entries, servers,
// SQLite and evidence are never modified):
//   qa-runtime.mjs     generated from ../qa-runtime.mjs with exactly two anchored transforms:
//                      (1) old EXPECTED_BINARY_SHA256 literal -> new sha,
//                      (2) prepareSlice maps opts.ports 18500..18599 -> +100 (186xx); idempotent
//                          for ports already in 186xx (agy reads ports from the epoch contract).
//   contract.json      generated from ../contract.json: artifact.expected_sha256 = new sha,
//                      slice/server-contract ports remapped +100, added `fixed_epoch` block.
//   credentials.json   locator-only descriptor, validated (no secret-looking values) then copied.
//   <client>/run.mjs (+cache-proof.mjs for omp-gpt) and <client>/fixture/** copied; the ONLY
//                      text change is old sha literal -> new sha (omp-gpt pins it locally).
//   prior-campaign-baseline/budget.json
//                      prior real campaign spend (old root slices + other fixed epochs) so the
//                      epoch helper's campaign USD15 gate counts it; per-slice USD3 unchanged.
//   fixed-epoch.json   manifest: sources/outputs sha256, transforms, port map, baseline, runs.
// Every anchor must match exactly once or the tool aborts (source drift => regenerate design).
// Credential text is never copied: client/key, homes, runtime, evidence are NOT cloned.
//
// Ledger: every invocation (prepare-only included) first validates all source-drift anchors with
// zero side effects, then registers <root>/fixed-<sha12> as scratch_directory in the shared ledger
// (retained source helper's recordResource, idempotent by id) BEFORE creating the dir or any file.
// cc-lb PIDs are ledgered by the epoch helper's prepareSlice during --run.
// --run (explicit parent opt-in, paid), single controller per epoch (controller.lock.json):
// phase 1 preflights EVERY selected client (binary digest, pinned copy digests, forced env) with no
// spawn; phase 2 seals manifest.launched once; phase 3 launches all selected drivers CONCURRENTLY
// (Promise.allSettled) with native engine (shebang: bun|node), cwd = epoch slice dir, own process
// group, own deadline and launch-logs/<client>-<ts>.{stdout,stderr}; one failure never stops others.
// Codex is forced to CODEX_QA_MODEL=gpt-5.2 and CODEX_QA_ATTEMPT_CAP =
// contract.paid_attempt_overrides.codex.max_generation_attempts (32, USD3). The only kill this tool
// ever performs is the deadline SIGTERM/SIGKILL of the driver process group it spawned itself.

import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { closeSync, createReadStream, existsSync, openSync, writeFileSync } from 'node:fs';
import { appendFile, lstat, mkdir, readdir, readFile, rename, writeFile } from 'node:fs/promises';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const SRC_HELPER = path.join(ROOT, 'qa-runtime.mjs');
const SRC_CONTRACT = path.join(ROOT, 'contract.json');
const SRC_CREDENTIALS = path.join(ROOT, 'credentials.json');
const LEDGER_PATH = '/tmp/cc-lb-actual-cli-e2e-20261020-ledger.json';
// Port offset for driver-supplied 185xx ports: --port-offset (default 100 -> 186xx; 200 -> 187xx).
// Set once from argv in main() before any port mapping; recorded in manifest + contract.
let PORT_OFFSET = 100;
const ALLOWED_PORT_OFFSETS = new Set([100, 200, 300, 400]);
const OLD_PORT_LO = 18500;
const OLD_PORT_HI = 18600; // exclusive
const CODEX_MODEL = 'gpt-5.2';
const BASELINE_DIR = 'prior-campaign-baseline';
// fixture: copy <client>/fixture/** verbatim. env: fixed driver env forced at --run (an inherited
// differing value aborts). Ports: slices[].ports, or port_roles.server_contract_slice for faults.
const CLIENTS = Object.freeze({
  'omp-gemini': { files: ['run.mjs'], fixture: true },
  agy: { files: ['run.mjs'], fixture: true },
  // hidden-check.mjs is driver-side (copied into the slice dir, never into the agent project).
  codex: { files: ['run.mjs', 'hidden-check.mjs'], fixture: true },
  'omp-gpt': { files: ['run.mjs', 'cache-proof.mjs'], fixture: true },
  // Server-contract raw cases (not actual-CLI coverage). The faults driver itself shifts ALL its
  // server+relay ports by QA_FAULT_PORT_OFFSET (186xx); the epoch helper leaves 186xx unchanged.
  // QA_FAULT_REQUEST_CAP=24 is unpaid-probe headroom; real provider forwards stay 16 / USD3.
  faults: { files: ['run.mjs', 'relay.mjs'], fixture: false, env: { QA_FAULT_SLICE: 'faults', QA_FAULT_PORT_OFFSET: '100', QA_FAULT_REQUEST_CAP: '24' } },
});
const CRED_NAME_RE = /(^|[\\/])(auth\.json|\.env(\..*)?|key|.*credential.*|.*token.*|.*oauth.*|.*\.pem|.*\.key|.*keyring.*|.*secret.*)$/i;
const SECRET_VALUE_RE = /^(sk-|sk_|AIza|ya29\.|gh[pousr]_|xox[abpr]-|eyJ)/;

// ------------------------------------------------------------------ utils

const nowIso = () => new Date().toISOString();
const sha256Text = (s) => createHash('sha256').update(s).digest('hex');
function sha256File(file) {
  return new Promise((resolve, reject) => {
    const h = createHash('sha256');
    createReadStream(file).on('data', (c) => h.update(c)).on('end', () => resolve(h.digest('hex'))).on('error', reject);
  });
}
async function readJson(file) {
  try { return JSON.parse(await readFile(file, 'utf8')); } catch { return null; }
}
function countOf(text, needle) {
  let n = 0;
  for (let i = text.indexOf(needle); i !== -1; i = text.indexOf(needle, i + needle.length)) n += 1;
  return n;
}
function replaceExactlyOnce(text, needle, replacement, label) {
  const n = countOf(text, needle);
  if (n !== 1) throw new Error(`transform anchor "${label}" matched ${n} times (need exactly 1); source drifted, aborting`);
  return text.replace(needle, () => replacement);
}
const mapPort = (p) => (Number.isInteger(p) && p >= OLD_PORT_LO && p < OLD_PORT_HI ? p + PORT_OFFSET : p);
function mapPortsDeep(v) {
  if (Array.isArray(v)) return v.map(mapPortsDeep);
  if (v && typeof v === 'object') return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, mapPortsDeep(x)]));
  return mapPort(v);
}

function parseArgs(argv) {
  const out = { expectedSha: null, clients: null, run: false, resume: false, deadlineSec: 1800, attemptLabel: null, portOffset: 100 };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const val = () => { const v = argv[i + 1]; if (v === undefined) throw new Error(`${a} needs a value`); i += 1; return v; };
    if (a === '--expected-sha') out.expectedSha = val();
    else if (a === '--client') out.clients = val().split(',').map((s) => s.trim()).filter(Boolean);
    else if (a === '--run') out.run = true;
    else if (a === '--resume') out.resume = true;
    else if (a === '--deadline-sec') out.deadlineSec = Number(val());
    else if (a === '--attempt-label') out.attemptLabel = val();
    else if (a === '--port-offset') out.portOffset = Number(val());
    else if (/^[0-9a-f]{64}$/.test(a) && !out.expectedSha) out.expectedSha = a;
    else throw new Error(`unknown argument: ${a}`);
  }
  if (!/^[0-9a-f]{64}$/.test(out.expectedSha ?? '')) throw new Error('--expected-sha <64 lowercase hex> is required');
  if (out.run && !out.clients) throw new Error('--run requires an explicit --client list (paid execution is never defaulted)');
  out.clients ??= Object.keys(CLIENTS);
  for (const c of out.clients) if (!CLIENTS[c]) throw new Error(`unsupported client "${c}" (allowed: ${Object.keys(CLIENTS).join(', ')})`);
  if (out.resume && !out.run) throw new Error('--resume only applies with --run');
  if (!(Number.isInteger(out.deadlineSec) && out.deadlineSec >= 60 && out.deadlineSec <= 7200)) throw new Error('--deadline-sec must be an integer in 60..7200');
  if (out.attemptLabel !== null && !/^[a-z0-9-]{1,24}$/.test(out.attemptLabel)) throw new Error('--attempt-label must match [a-z0-9-]{1,24}');
  if (!ALLOWED_PORT_OFFSETS.has(out.portOffset)) throw new Error(`--port-offset must be one of ${[...ALLOWED_PORT_OFFSETS].join(', ')}`);
  // The faults driver applies its own QA_FAULT_PORT_OFFSET, which its owner supports only at 100.
  if (out.clients.includes('faults') && out.portOffset !== 100) throw new Error('faults supports only --port-offset 100 (QA_FAULT_PORT_OFFSET range owned by the faults driver)');
  return out;
}

let tmpSeq = 0;
// Atomic new-file writer. Existing identical content is a no-op; differing content is replaced
// only when `replaceable` (epoch/slice never launched), otherwise refused.
async function writePinned(file, content, { replaceable, mode = 0o600 }) {
  if (existsSync(file)) {
    const cur = await readFile(file);
    if (cur.equals(Buffer.from(content))) return 'unchanged';
    if (!replaceable) throw new Error(`refusing to change pinned file after launch: ${file}`);
  }
  await mkdir(path.dirname(file), { recursive: true, mode: 0o700 });
  const tmp = `${file}.tmp-${process.pid}-${(tmpSeq += 1)}`;
  await writeFile(tmp, content, { mode, flag: 'wx' });
  await rename(tmp, file);
  return 'written';
}

// ------------------------------------------------------------- sources

async function loadSources() {
  const helper = await readFile(SRC_HELPER, 'utf8');
  const contractText = await readFile(SRC_CONTRACT, 'utf8');
  const contract = JSON.parse(contractText);
  const oldSha = contract?.artifact?.expected_sha256;
  if (!/^[0-9a-f]{64}$/.test(oldSha ?? '')) throw new Error('source contract artifact.expected_sha256 missing');
  const binLine = helper.match(/^export const BINARY_PATH = '([^']+)';$/m);
  const shaLine = helper.match(/^export const EXPECTED_BINARY_SHA256 = '([0-9a-f]{64})';$/m);
  if (!binLine || !shaLine) throw new Error('source helper BINARY_PATH/EXPECTED_BINARY_SHA256 anchors missing');
  if (shaLine[1] !== oldSha) throw new Error('source helper EXPECTED_BINARY_SHA256 != source contract artifact sha');
  if (binLine[1] !== contract.artifact.path) throw new Error('source helper BINARY_PATH != source contract artifact.path');
  // Current USD bounds must remain exactly as approved (USD3 slice / USD15 campaign).
  if (contract.paid_bounds?.usd_ceiling_per_slice !== 3 || contract.paid_bounds?.usd_ceiling_campaign !== 15)
    throw new Error('source contract USD bounds are not USD3/USD15; refusing');
  if (countOf(helper, '  sliceCostUsd: 3,\n') !== 1 || countOf(helper, '  campaignCostUsd: 15,') !== 1)
    throw new Error('source helper CLIENT_LIMITS USD bounds are not USD3/USD15; refusing');
  return { helper, contractText, contract, oldSha, binaryPath: binLine[1] };
}

function buildHelper(src, oldSha, newSha) {
  if (/fixedEpochPort|FIXED_EPOCH_PORT_OFFSET/.test(src)) throw new Error('source helper already defines fixed-epoch symbols');
  const oldLine = `export const EXPECTED_BINARY_SHA256 = '${oldSha}';`;
  let out = replaceExactlyOnce(src, oldLine, [
    `export const EXPECTED_BINARY_SHA256 = '${newSha}';`,
    `// fixed-epoch transform (generated by ../fixed-epoch.mjs, not hand-edited): driver-supplied`,
    `// ${OLD_PORT_LO}..${OLD_PORT_HI - 1} ports are shifted +${PORT_OFFSET} so this epoch never collides with retained services.`,
    `export const FIXED_EPOCH_PORT_OFFSET = ${PORT_OFFSET};`,
    `function fixedEpochPort(port) {`,
    `  return Number.isInteger(port) && port >= ${OLD_PORT_LO} && port < ${OLD_PORT_HI} ? port + FIXED_EPOCH_PORT_OFFSET : port;`,
    `}`,
  ].join('\n'), 'EXPECTED_BINARY_SHA256');
  // Known prepareSlice destructure forms (exact text, no regex): the original, and the trace-harness
  // form adding optional otlpEndpoint. Exactly one occurrence of exactly one form must exist; the
  // rewrite only removes `ports` from the destructure (all other bindings preserved) and re-derives
  // it with the epoch port shift. Any other drift aborts before ledger/file side effects.
  const destructureForms = [
    ['  const { name, ports, provider, model } = opts;', '  const { name, provider, model } = opts;'],
    ['  const { name, ports, provider, model, otlpEndpoint } = opts;', '  const { name, provider, model, otlpEndpoint } = opts;'],
  ];
  const hits = destructureForms.map(([from]) => countOf(out, from));
  const total = hits.reduce((s, n) => s + n, 0);
  if (total !== 1) throw new Error(`transform anchor "prepareSlice ports destructuring" matched ${total} times across ${destructureForms.length} known forms (need exactly 1); source drifted, aborting`);
  const [from, to] = destructureForms[hits.indexOf(1)];
  out = replaceExactlyOnce(out, from,
    `${to}\n  const ports = Array.isArray(opts.ports) ? opts.ports.map(fixedEpochPort) : opts.ports;`,
    'prepareSlice ports destructuring');
  if (countOf(out, oldSha) !== 0) throw new Error('old sha still present in generated helper');
  if (countOf(out, newSha) !== 1) throw new Error('generated helper must contain the new sha exactly once');
  return { text: out, transforms: ['EXPECTED_BINARY_SHA256 old->new (1)', `prepareSlice opts.ports ${OLD_PORT_LO}-${OLD_PORT_HI - 1} +${PORT_OFFSET} (destructure form: ${from.trim()})`] };
}

function buildContract(contract, srcContractSha, oldSha, newSha, epochRel, priorUsd) {
  const c = structuredClone(contract);
  c.artifact = { ...c.artifact, expected_sha256: newSha };
  c.slices = c.slices.map((s) => ({ ...s, ports: s.ports.map(mapPort) }));
  if (c.port_roles?.server_contract_slice) c.port_roles.server_contract_slice = mapPortsDeep(c.port_roles.server_contract_slice);
  c.fixed_epoch = {
    generated_by: 'fixed-epoch.mjs',
    epoch_dir: epochRel,
    source_contract_sha256: srcContractSha,
    previous_artifact_sha256: oldSha,
    port_offset: PORT_OFFSET,
    port_rule: `driver ports ${OLD_PORT_LO}-${OLD_PORT_HI - 1} are shifted +${PORT_OFFSET} by the epoch helper's prepareSlice; authoritative ports are <slice>/prepare.json, not driver summary literals`,
    codex_model_forced: CODEX_MODEL,
    prior_campaign_cost_baseline_ref: `${BASELINE_DIR}/budget.json`,
    prior_campaign_cost_usd_at_prepare: priorUsd,
    retained_epochs: 'Old QA root slices, ledger entries, servers and SQLite are retained untouched.',
  };
  return `${JSON.stringify(c, null, 2)}\n`;
}

async function validateCredentialDescriptor() {
  const text = await readFile(SRC_CREDENTIALS, 'utf8');
  const d = JSON.parse(text);
  const allowedSource = new Set(['type', 'path', 'table', 'column', 'upstreamId', 'masterEnvFile', 'masterEnvKey', 'key']);
  for (const [prov, entry] of Object.entries(d)) {
    const keys = Object.keys(entry);
    if (keys.some((k) => !['kind', 'baseUrl', 'source', 'guard'].includes(k))) throw new Error(`credential descriptor ${prov}: unexpected field`);
    for (const [k, v] of Object.entries(entry.source ?? {})) {
      if (!allowedSource.has(k) || typeof v !== 'string') throw new Error(`credential descriptor ${prov}.source.${k}: not a locator field`);
    }
    const walk = (v) => (typeof v === 'string' ? [v] : v && typeof v === 'object' ? Object.values(v).flatMap(walk) : []);
    if (walk(entry).some((s) => SECRET_VALUE_RE.test(s))) throw new Error(`credential descriptor ${prov}: secret-looking value; refusing to copy`);
  }
  return text;
}

async function listFixtureFiles(dir, rel = '') {
  const out = [];
  for (const ent of await readdir(path.join(dir, rel), { withFileTypes: true })) {
    const r = path.join(rel, ent.name);
    const st = await lstat(path.join(dir, r));
    if (st.isSymbolicLink()) throw new Error(`fixture symlink refused: ${r}`);
    if (CRED_NAME_RE.test(r)) throw new Error(`fixture credential-like file name refused: ${r}`);
    if (st.isDirectory()) out.push(...await listFixtureFiles(dir, r));
    else if (st.isFile()) out.push({ rel: r, mode: st.mode & 0o777 });
    else throw new Error(`fixture non-regular entry refused: ${r}`);
  }
  return out;
}

// Prior real campaign spend: per prior slice max(cc-lb committed cost, driver gate usd).
async function computePriorBaseline(epochName) {
  const slices = [];
  const scan = async (dir, label) => {
    for (const ent of await readdir(dir, { withFileTypes: true })) {
      if (!ent.isDirectory()) continue;
      if (ent.name === BASELINE_DIR) continue; // never double-count another epoch's baseline
      const sliceDir = path.join(dir, ent.name);
      if (dir === ROOT && /^fixed-[0-9a-f]{12}(-[a-z0-9-]{1,24})?$/.test(ent.name)) {
        if (ent.name !== epochName) await scan(sliceDir, ent.name);
        continue;
      }
      const b = await readJson(path.join(sliceDir, 'budget.json'));
      const drv = await readJson(path.join(sliceDir, 'driver-budget.json'));
      if (!b && !drv) continue;
      const ccLbUsd = Number(b?.costUsd) || 0;
      const driverUsd = Number(drv?.usd) || 0;
      const ids = Array.isArray(b?.unpricedRequestIds) ? b.unpricedRequestIds : null;
      const unknown = ids ? ids.map((id) => `${label}/${ent.name}:${id}`)
        : (b && b.costKnown === false && Number(b.attempts) !== 0 ? [`${label}/${ent.name}:legacy_unknown`] : []);
      slices.push({
        slice: `${label}/${ent.name}`,
        cc_lb_committed_cost_usd: ccLbUsd,
        cc_lb_attempts: b?.attempts ?? null,
        driver_gate_usd: drv ? driverUsd : null,
        driver_attempts: drv?.attempts ?? null,
        counted_usd: Math.max(ccLbUsd, driverUsd),
        unpriced: unknown,
        budget_at: b?.at ?? drv?.updated_at ?? null,
      });
    }
  };
  await scan(ROOT, 'root');
  const costUsd = Number(slices.reduce((s, x) => s + x.counted_usd, 0).toFixed(6));
  const attempts = slices.reduce((s, x) => s + Math.max(Number(x.cc_lb_attempts) || 0, Number(x.driver_attempts) || 0), 0);
  const unpricedRequestIds = slices.flatMap((x) => x.unpriced);
  return {
    slice: BASELINE_DIR,
    at: nowIso(),
    attempts,
    costUsd,
    costKnown: unpricedRequestIds.length === 0,
    unpricedRequestIds,
    catalogPriced: true,
    basis: 'Prior campaign real spend counted against this epoch campaign USD15 gate: per prior slice max(cc-lb committed event cost_usd, driver-computed gate usd incl. conservative reserve). Per-slice USD3 caps apply unchanged to new epoch slices.',
    sources: slices,
  };
}

// ------------------------------------------------------------- prepare

async function sliceLaunched(sliceDir) {
  return ['runtime', 'prepare.json', 'resources.jsonl', 'budget.json'].some((f) => existsSync(path.join(sliceDir, f)));
}

// Every loopback port the selected clients' cc-lb instances will bind in this epoch.
function epochPorts(contract, clients) {
  const flat = (v) => (Array.isArray(v) ? v.flatMap(flat) : v && typeof v === 'object' ? Object.values(v).flatMap(flat) : Number.isInteger(v) ? [v] : []);
  const ports = clients.flatMap((c) => (c === 'faults'
    ? flat(mapPortsDeep(contract.port_roles?.server_contract_slice ?? {}))
    : (contract.slices.find((s) => s.id === c)?.ports ?? []).map(mapPort)));
  return [...new Set(ports)];
}

// Read-only listener probe: TCP connect to 127.0.0.1 and immediately close. Refused => free;
// connect or any other outcome (timeout, unexpected error) => treated as busy (fail closed).
function portListening(port) {
  return new Promise((resolve) => {
    const sock = net.connect({ host: '127.0.0.1', port });
    const done = (busy) => { sock.destroy(); resolve(busy); };
    sock.setTimeout(1000, () => done(true));
    sock.once('connect', () => done(true));
    sock.once('error', (e) => done(e.code !== 'ECONNREFUSED'));
  });
}

async function prepare(opts) {
  const src = await loadSources();
  const newSha = opts.expectedSha;
  if (newSha === src.oldSha) throw new Error('expected sha equals the previous epoch sha; a fixed epoch requires the rebuilt binary');
  const actual = await sha256File(src.binaryPath);
  if (actual !== newSha) throw new Error(`BLOCKED: current binary ${src.binaryPath} sha256 ${actual} != expected ${newSha}`);

  const epochName = `fixed-${newSha.slice(0, 12)}${opts.attemptLabel ? `-${opts.attemptLabel}` : ''}`;
  const epochRoot = path.join(ROOT, epochName);
  // An existing epoch keeps its recorded offset; a different --port-offset never re-targets it.
  const prior = await readJson(path.join(epochRoot, 'fixed-epoch.json'));
  if (prior && prior.port_offset !== PORT_OFFSET) throw new Error(`epoch ${epochName} was prepared with port offset ${prior.port_offset}, not ${PORT_OFFSET}`);
  if (prior && (prior.attempt_label ?? null) !== opts.attemptLabel) throw new Error(`epoch ${epochName} attempt label mismatch`);
  // Read-only port collision probe (TCP connect only; never stops or touches existing listeners).
  // Skipped only for --resume, where the epoch's own ledger-owned servers legitimately listen.
  if (!opts.resume) {
    const busy = [];
    for (const port of epochPorts(src.contract, opts.clients)) if (await portListening(port)) busy.push(port);
    if (busy.length) throw new Error(`BLOCKED: epoch ports already listening (read-only probe, nothing stopped): ${busy.join(',')}`);
  }
  // Source-drift anchor validation happens before anything is ledgered or written: an anchor
  // mismatch, non-locator credential descriptor or bad client source aborts with zero side effects.
  const helperBuilt = buildHelper(src.helper, src.oldSha, newSha);
  const credentialText = await validateCredentialDescriptor();
  for (const client of opts.clients) {
    for (const f of CLIENTS[client].files) if (!existsSync(path.join(ROOT, client, f))) throw new Error(`missing source ${client}/${f}`);
    if (CLIENTS[client].fixture) await listFixtureFiles(path.join(ROOT, client, 'fixture'));
  }
  // Ledger entry for the epoch dir is written before the dir or any file in it exists.
  const ledgered = await registerEpochDir(epochRoot, epochName, newSha);
  // --run: single controller per epoch before any epoch file is (re)written.
  if (opts.run) await acquireController(epochRoot);

  const manifestPath = path.join(epochRoot, 'fixed-epoch.json');
  const manifest = (await readJson(manifestPath)) ?? {
    epoch: epochName, created_at: nowIso(), source_root: ROOT, ledger_path: LEDGER_PATH,
    binary: { path: src.binaryPath, expected_sha256: newSha, previous_sha256: src.oldSha, verifications: [] },
    attempt_label: opts.attemptLabel, port_offset: PORT_OFFSET, port_offset_source: 'cli --port-offset (default 100)',
    files: {}, slices: {}, ledgered: null, launched: null,
  };
  if (manifest.binary.expected_sha256 !== newSha) throw new Error('manifest sha mismatch for epoch dir');
  manifest.ledgered ??= ledgered;
  manifest.binary.verifications.push({ at: nowIso(), phase: opts.run ? 'run-prepare' : 'prepare', sha256: actual });
  const sliceDirs = Object.keys(CLIENTS).map((c) => path.join(epochRoot, c));
  const anyLaunched = (manifest.launched ?? null) !== null || (await Promise.all(sliceDirs.map(sliceLaunched))).some(Boolean);

  const baseline = await computePriorBaseline(epochName);
  const record = async (rel, srcPath, content, transforms, replaceable, mode) => {
    const status = await writePinned(path.join(epochRoot, rel), content, { replaceable, mode });
    if (status === 'written' || !manifest.files[rel]) {
      manifest.files[rel] = { source: srcPath, source_sha256: srcPath ? sha256Text(await readFile(srcPath)) : null, sha256: sha256Text(content), transforms, at: nowIso() };
    }
  };

  // Shared epoch copies are regenerated only before the epoch's first launch; afterwards they stay
  // pinned (digest-verified before every run) even if the old-root sources keep evolving.
  if (!anyLaunched) {
    await record('qa-runtime.mjs', SRC_HELPER, helperBuilt.text, helperBuilt.transforms, true);
    await record('contract.json', SRC_CONTRACT,
      buildContract(src.contract, sha256Text(src.contractText), src.oldSha, newSha, epochName, baseline.costUsd),
      ['artifact.expected_sha256 old->new', `slices[].ports/port_roles.server_contract_slice +${PORT_OFFSET}`, 'fixed_epoch block added'], true);
    await record('credentials.json', SRC_CREDENTIALS, credentialText, ['verbatim (locator-only validated)'], true);
  } else {
    for (const rel of ['qa-runtime.mjs', 'contract.json', 'credentials.json'])
      if (!manifest.files[rel] || !existsSync(path.join(epochRoot, rel))) throw new Error(`launched epoch is missing pinned ${rel}`);
  }
  await record(`${BASELINE_DIR}/budget.json`, null, `${JSON.stringify(baseline, null, 2)}\n`, ['generated prior-campaign baseline'], true);

  for (const client of opts.clients) {
    const srcDir = path.join(ROOT, client);
    const dstDir = path.join(epochRoot, client);
    const launched = await sliceLaunched(dstDir);
    const entry = manifest.slices[client] ?? { files: [] };
    if (launched) { manifest.slices[client] = { ...entry, pinned_after_launch: true }; continue; }
    const files = [];
    for (const f of CLIENTS[client].files) {
      const text = await readFile(path.join(srcDir, f), 'utf8');
      const n = countOf(text, src.oldSha);
      const out = n ? text.split(src.oldSha).join(newSha) : text;
      const rel = path.join(client, f);
      await record(rel, path.join(srcDir, f), out, n ? [`old sha literal -> new sha (${n})`] : ['verbatim'], true, 0o700);
      files.push(rel);
    }
    for (const fx of CLIENTS[client].fixture ? await listFixtureFiles(path.join(srcDir, 'fixture')) : []) {
      const srcPath = path.join(srcDir, 'fixture', fx.rel);
      const rel = path.join(client, 'fixture', fx.rel);
      await record(rel, srcPath, await readFile(srcPath), ['verbatim fixture'], true, fx.mode | 0o600);
      files.push(rel);
    }
    const head = (await readFile(path.join(srcDir, 'run.mjs'), 'utf8')).split('\n', 1)[0];
    const engine = /\bbun\b/.test(head) ? 'bun' : /\bnode\b/.test(head) ? 'node' : null;
    if (!engine) throw new Error(`${client}/run.mjs has no bun/node shebang`);
    manifest.slices[client] = { prepared_at: nowIso(), engine, files };
  }
  manifest.prior_campaign_baseline = { ref: `${BASELINE_DIR}/budget.json`, cost_usd: baseline.costUsd, attempts: baseline.attempts, cost_known: baseline.costKnown };
  await writePinned(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, { replaceable: true });
  return { src, epochName, epochRoot, manifestPath, manifest, baseline };
}

// ------------------------------------------------------------------ run

async function verifyPinned(epochRoot, manifest, client) {
  const rels = ['qa-runtime.mjs', 'contract.json', 'credentials.json', ...(manifest.slices[client]?.files ?? [])];
  for (const rel of rels) {
    const want = manifest.files[rel]?.sha256;
    const got = await sha256File(path.join(epochRoot, rel));
    if (!want || want !== got) throw new Error(`pinned copy digest mismatch: ${rel}`);
  }
}

// Registers the epoch dir in the shared ledger BEFORE any file under it is created (prepare-only
// included). Uses the retained source helper's recordResource read-only via import (ledger lock +
// record format reused; no ctx.root so nothing is appended outside the ledger). Idempotent by id.
async function registerEpochDir(epochRoot, epochName, newSha) {
  const ledger = await readJson(LEDGER_PATH);
  if (!ledger || !Array.isArray(ledger.entries)) throw new Error(`ledger unreadable: ${LEDGER_PATH}`);
  const existing = ledger.entries.find((e) => e.id === epochRoot);
  if (existing) return { at: existing.created_at, id: epochRoot, ledger: LEDGER_PATH, preexisting_entry: true };
  if (existsSync(epochRoot)) throw new Error(`epoch dir exists without a ledger entry (ownership unproven): ${epochRoot}`);
  const helper = await import(pathToFileURL(SRC_HELPER).href);
  const rec = await helper.recordResource({ name: epochName }, {
    kind: 'scratch_directory',
    id: epochRoot,
    delete: `After explicit cleanup signal and ownership verification (no ledger-owned PID under it still alive): rm -rf ${epochRoot}`,
    note: `Fixed-binary QA epoch for cc-lb sha256 ${newSha}, created by fixed-epoch.mjs prepare; cc-lb processes are ledgered separately by the epoch helper prepareSlice.`,
  });
  return { at: rec?.created_at ?? nowIso(), id: epochRoot, ledger: LEDGER_PATH, preexisting_entry: false };
}

function resolveBin(name) {
  if (name === 'node' && !globalThis.Bun) return process.execPath;
  for (const dir of (process.env.PATH ?? '').split(':')) {
    const p = path.join(dir, name);
    if (dir && existsSync(p)) return p;
  }
  throw new Error(`engine ${name} not found on PATH`);
}

// Single-controller guard: one --run process per epoch. Liveness probe is signal 0 only (never a
// kill). The lock file is kept (marked released at exit), never deleted by this tool.
async function acquireController(epochRoot) {
  const lock = path.join(epochRoot, 'controller.lock.json');
  await mkdir(epochRoot, { recursive: true, mode: 0o700 });
  const mine = `${JSON.stringify({ pid: process.pid, started_at: nowIso(), released: false })}\n`;
  try {
    await writeFile(lock, mine, { mode: 0o600, flag: 'wx' });
  } catch (error) {
    if (error.code !== 'EEXIST') throw error;
    const cur = await readJson(lock);
    let alive = false;
    if (cur && Number.isInteger(cur.pid) && !cur.released) {
      try { process.kill(cur.pid, 0); alive = true; } catch (e) { alive = e.code === 'EPERM'; }
    }
    if (alive) throw new Error(`another fixed-epoch controller (pid ${cur.pid}) holds ${lock}`);
    const tmp = `${lock}.tmp-${process.pid}`;
    await writeFile(tmp, mine, { mode: 0o600, flag: 'wx' });
    await rename(tmp, lock);
  }
  const release = () => {
    try { writeFileSync(lock, `${JSON.stringify({ pid: process.pid, released: true, released_at: nowIso() })}\n`, { mode: 0o600 }); } catch {}
  };
  process.once('exit', release);
}

// All in-process manifest writes go through one queue so concurrent client completions never race.
let manifestQueue = Promise.resolve();
function saveManifest(ctx) {
  const run = manifestQueue.then(() => writePinned(ctx.manifestPath, `${JSON.stringify(ctx.manifest, null, 2)}\n`, { replaceable: true }));
  manifestQueue = run.catch(() => {});
  return run;
}

// Preflight for one client: binary digest, pinned copy digests, forced env. No spawn, no writes.
async function preflightClient(ctx, client, opts) {
  const { epochRoot, manifest, src } = ctx;
  const actual = await sha256File(src.binaryPath);
  if (actual !== opts.expectedSha) throw new Error(`BLOCKED: binary digest changed before ${client}: ${actual}`);
  await verifyPinned(epochRoot, manifest, client);
  const contract = JSON.parse(await readFile(path.join(epochRoot, 'contract.json'), 'utf8'));
  // Parent-controlled env (e.g. QA_GEMINI_PRIMARY_MODEL) is inherited unchanged; only QA_RESUME and
  // the explicitly forced keys below are overridden.
  const env = { ...process.env, QA_FIXED_EPOCH: ctx.epochName };
  if (opts.resume) env.QA_RESUME = '1'; else delete env.QA_RESUME;
  if (client === 'codex') {
    if (process.env.CODEX_QA_MODEL && process.env.CODEX_QA_MODEL !== CODEX_MODEL) throw new Error(`CODEX_QA_MODEL must be ${CODEX_MODEL} in fixed epochs`);
    const cap = contract.paid_attempt_overrides?.codex?.max_generation_attempts;
    if (!Number.isInteger(cap)) throw new Error('contract paid_attempt_overrides.codex.max_generation_attempts missing');
    if (process.env.CODEX_QA_ATTEMPT_CAP && Number(process.env.CODEX_QA_ATTEMPT_CAP) !== cap) throw new Error(`CODEX_QA_ATTEMPT_CAP must equal contract value ${cap}`);
    if (contract.paid_attempt_overrides.codex.usd_ceiling !== 3) throw new Error('codex usd_ceiling must stay 3');
    env.CODEX_QA_MODEL = CODEX_MODEL;
    env.CODEX_QA_ATTEMPT_CAP = String(cap);
  }
  for (const [k, v] of Object.entries(CLIENTS[client].env ?? {})) {
    if (process.env[k] !== undefined && process.env[k] !== v) throw new Error(`${k} must be ${v} for ${client} in fixed epochs (inherited ${process.env[k]})`);
    env[k] = v;
  }
  const engine = manifest.slices[client]?.engine;
  if (!engine) throw new Error(`${client} not prepared in this epoch`);
  return { client, env, engine, bin: resolveBin(engine), sliceDir: path.join(epochRoot, client), binarySha256: actual };
}

// Launches one prepared client in its own process group with its own deadline and log files.
async function launchClient(ctx, pre, opts) {
  const { epochRoot, manifest } = ctx;
  const { client, env, engine, bin, sliceDir, binarySha256 } = pre;
  const startedAt = nowIso();
  const tag = startedAt.replace(/[:.]/g, '-');
  const logDir = path.join(epochRoot, 'launch-logs');
  await mkdir(logDir, { recursive: true, mode: 0o700 });
  const stdoutPath = path.join(logDir, `${client}-${tag}.stdout`);
  const stderrPath = path.join(logDir, `${client}-${tag}.stderr`);
  const outFd = openSync(stdoutPath, 'wx', 0o600);
  const errFd = openSync(stderrPath, 'wx', 0o600);
  let child;
  try {
    child = spawn(bin, [path.join(sliceDir, 'run.mjs')], { cwd: sliceDir, env, stdio: ['ignore', outFd, errFd], detached: true });
  } finally {
    closeSync(outFd);
    closeSync(errFd);
  }
  const spawnError = new Promise((resolve) => child.once('error', resolve));
  const runRec = { client, engine, bin, pid: child.pid ?? null, cwd: sliceDir, started_at: startedAt, binary_sha256: binarySha256,
    codex_model: client === 'codex' ? CODEX_MODEL : undefined, resume: opts.resume, stdout: stdoutPath, stderr: stderrPath,
    // Non-secret QA reproduction knobs inherited from the parent env (recorded verbatim).
    qa_knobs: { QA_GEMINI_PRIMARY_MODEL: env.QA_GEMINI_PRIMARY_MODEL ?? null, QA_OTLP_ENDPOINT: env.QA_OTLP_ENDPOINT ?? null,
      QA_REQUEST_TRACE_PREFIX: env.QA_REQUEST_TRACE_PREFIX ?? null, attempt_label: opts.attemptLabel, port_offset: PORT_OFFSET } };
  await appendFile(path.join(epochRoot, 'runs.jsonl'), `${JSON.stringify({ ...runRec, event: 'start' })}\n`, { mode: 0o600 });
  let timedOut = false;
  // Approved kill scope: only this tool's own driver process group, only at the deadline.
  const timer = child.pid ? setTimeout(() => {
    timedOut = true;
    try { process.kill(-child.pid, 'SIGTERM'); } catch {}
    setTimeout(() => { try { process.kill(-child.pid, 'SIGKILL'); } catch {} }, 10_000).unref();
  }, opts.deadlineSec * 1000) : null;
  const [code, signal, error] = await Promise.race([
    new Promise((resolve) => child.once('exit', (c, s) => resolve([c, s, null]))),
    spawnError.then((e) => [null, null, e]),
  ]);
  clearTimeout(timer);
  const end = { ...runRec, event: 'exit', ended_at: nowIso(), exit_code: code, signal, timed_out: timedOut, spawn_error: error ? String(error.message) : null,
    evidence: { summary: path.join(sliceDir, 'summary.json'), prepare: path.join(sliceDir, 'prepare.json'), budget: path.join(sliceDir, 'budget.json'), evidence_dir: path.join(sliceDir, 'evidence') } };
  await appendFile(path.join(epochRoot, 'runs.jsonl'), `${JSON.stringify(end)}\n`, { mode: 0o600 });
  manifest.slices[client].runs = [...(manifest.slices[client].runs ?? []), { started_at: startedAt, exit_code: code, signal, timed_out: timedOut, stdout: stdoutPath, stderr: stderrPath }];
  await saveManifest(ctx);
  return end;
}

// ----------------------------------------------------------------- main

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  PORT_OFFSET = opts.portOffset;
  const ctx = await prepare(opts);
  const out = {
    mode: opts.run ? 'run' : 'prepare-only',
    epoch_root: ctx.epochRoot,
    manifest: ctx.manifestPath,
    binary_sha256_verified: opts.expectedSha,
    ports: Object.fromEntries(Object.keys(CLIENTS).map((c) => [c, c === 'faults'
      ? mapPortsDeep(ctx.src.contract.port_roles?.server_contract_slice ?? {})
      : (ctx.src.contract.slices.find((s) => s.id === c)?.ports ?? []).map(mapPort)])),
    prior_campaign_baseline: { file: path.join(ctx.epochRoot, BASELINE_DIR, 'budget.json'), cost_usd: ctx.baseline.costUsd, cost_known: ctx.baseline.costKnown },
    ledger_entry: ctx.manifest.ledgered,
    attempt_label: opts.attemptLabel,
    port_offset: PORT_OFFSET,
    run_command: `node ${fileURLToPath(import.meta.url)} --expected-sha ${opts.expectedSha}${opts.attemptLabel ? ` --attempt-label ${opts.attemptLabel}` : ''} --port-offset ${PORT_OFFSET} --client ${opts.clients.join(',')} --run`,
  };
  if (!opts.run) { console.log(JSON.stringify(out, null, 2)); return; }
  // Phase 1: preflight EVERY selected client (digests, pinned copies, env) before any launch;
  // any failure aborts the whole run with zero driver processes started.
  const pre = [];
  for (const client of opts.clients) pre.push(await preflightClient(ctx, client, opts));
  for (const p of pre) ctx.manifest.binary.verifications.push({ at: nowIso(), phase: `run:${p.client}`, sha256: p.binarySha256 });
  // Phase 2: seal once. Shared copies are never regenerated after this.
  ctx.manifest.launched ??= { at: nowIso(), clients: opts.clients };
  ctx.manifest.launches = [...(ctx.manifest.launches ?? []), { at: nowIso(), clients: opts.clients, resume: opts.resume, controller_pid: process.pid }];
  await saveManifest(ctx);
  // Phase 3: concurrent launch on disjoint ports / slice roots; one client's failure never stops
  // another (allSettled). Each has its own process group, deadline and stdout/stderr files.
  const settled = await Promise.allSettled(pre.map((p) => launchClient(ctx, p, opts)));
  out.runs = settled.map((s, i) => (s.status === 'fulfilled'
    ? { client: pre[i].client, exit_code: s.value.exit_code, signal: s.value.signal, timed_out: s.value.timed_out, spawn_error: s.value.spawn_error, summary: s.value.evidence.summary, stdout: s.value.stdout, stderr: s.value.stderr }
    : { client: pre[i].client, controller_error: String(s.reason?.message ?? s.reason) }));
  out.runs_log = path.join(ctx.epochRoot, 'runs.jsonl');
  console.log(JSON.stringify(out, null, 2));
  if (out.runs.some((r) => r.exit_code !== 0)) process.exitCode = 1;
}

main().catch((error) => {
  console.error(`fixed-epoch: ${error.message}`);
  process.exit(2);
});
