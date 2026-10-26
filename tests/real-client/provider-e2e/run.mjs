#!/usr/bin/env node
// Portable multi-provider real-CLI E2E QA runner for cc-lb.
// Supports:
//   --self-test                                   Run offline unit & OTLP lifecycle tests (0 provider calls)
//   --preflight [--case <name|all>] [--env-file]  Verify binaries, CLI versions, ports, and credential presence (0 provider calls)
//   --run --case <name|all> [--env-file]          Run real CLI E2E slice(s) through provider-boundary + budget guard + OTLP + oracle
import { spawn, spawnSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import {
  chmodSync,
  cpSync,
  createWriteStream,
  existsSync,
  mkdirSync,
  readFileSync,
  realpathSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseEnv } from 'node:util';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../../..');

const CASE_SPECS = Object.freeze({
  'omp-gpt': {
    id: 'omp-gpt',
    alias: 'api-key-omp-gpt',
    provider: 'openai',
    kind: 'openai_api_key',
    cli: 'omp',
    model: 'gpt-5.2',
    models: ['gpt-5.2'],
    forwards: 16,
    basePorts: [18510, 18511, 18512],
    observerPort: 18513,
    boundaryPort: 20510,
    targetBaseUrl: 'https://api.openai.com',
    protocol: 'openai-responses',
    envKeys: ['OPENAI_BACKEND_API_KEY', 'OPENAI_API_KEY', 'OPENAI_MANAGED_API_KEY'],
    scenarios: ['omp-gpt.stateless_tools', 'omp-gpt.stateful_previous_response_id', 'omp-gpt.session_continue'],
  },
  'omp-gemini': {
    id: 'omp-gemini',
    alias: 'api-key-omp-gemini',
    provider: 'gemini',
    kind: 'gemini_api_key',
    cli: 'omp',
    model: 'gemini-3.1-pro-preview',
    controlModel: 'gemini-3.5-flash-lite',
    models: ['gemini-3.1-pro-preview', 'gemini-3.5-flash-lite'],
    forwards: 16,
    basePorts: [18520, 18521, 18522],
    observerPort: 18523,
    boundaryPort: 20520,
    targetBaseUrl: 'https://generativelanguage.googleapis.com',
    protocol: 'google-generate-content',
    envKeys: ['GEMINI_MANAGED_API_KEY', 'GEMINI_API_KEY', 'GOOGLE_API_KEY'],
    scenarios: [
      'omp-gemini.gemini3_tools_thought_signature',
      'omp-gemini.session_continue',
      'omp-gemini.flash_lite_transport',
    ],
  },
  codex: {
    id: 'codex',
    alias: 'api-key-codex',
    provider: 'openai',
    kind: 'openai_api_key',
    cli: 'codex',
    model: process.env.CODEX_QA_MODEL || 'gpt-6.1-sol',
    models: [process.env.CODEX_QA_MODEL || 'gpt-6.1-sol'],
    forwards: 32,
    basePorts: [18500, 18501, 18502],
    observerPort: 18503,
    boundaryPort: 20500,
    targetBaseUrl: 'https://api.openai.com',
    protocol: 'openai-responses',
    envKeys: ['OPENAI_BACKEND_API_KEY', 'OPENAI_API_KEY', 'OPENAI_MANAGED_API_KEY'],
    scenarios: ['http', 'ws', 'ws_reconnect', 'exact_session_resume'],
  },
  agy: {
    id: 'agy',
    alias: 'api-key-agy',
    provider: 'gemini',
    kind: 'gemini_api_key',
    cli: 'agy',
    model: 'gemini-3.1-pro-preview',
    agyModelLabel: 'gemini-3.1-pro-low',
    models: ['gemini-3.1-pro-preview', 'gemini-3.1-flash-lite-preview'],
    forwards: 16,
    basePorts: [18530, 18531, 18532],
    observerPort: 18533,
    boundaryPort: 20530,
    targetBaseUrl: 'https://generativelanguage.googleapis.com',
    protocol: 'google-generate-content',
    envKeys: ['GEMINI_MANAGED_API_KEY', 'GEMINI_API_KEY', 'GOOGLE_API_KEY'],
    scenarios: ['Actual Antigravity coding tools', 'thoughtSignature continuity', 'Session continue'],
  },
});

const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');

function parseArgs(argv) {
  const opts = {
    mode: null,
    caseName: 'all',
    envFile: process.env.CC_LB_QA_ENV_FILE ?? null,
    workDir: process.env.CC_LB_QA_WORK_DIR ?? null,
    portOffset: process.env.CC_LB_QA_PORT_OFFSET ? Number(process.env.CC_LB_QA_PORT_OFFSET) : null,
    sliceUsd: process.env.CC_LB_QA_SLICE_USD ? Number(process.env.CC_LB_QA_SLICE_USD) : 6,
  };
  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--self-test') opts.mode = 'self-test';
    else if (arg === '--preflight' || arg === '--preflight-only') opts.mode = 'preflight';
    else if (arg === '--run') opts.mode = 'run';
    else if (arg === '--run-oauth') opts.mode = 'run-oauth';
    else if (arg === '--case' && argv[i + 1]) opts.caseName = argv[++i];
    else if (arg === '--env-file' && argv[i + 1]) opts.envFile = path.resolve(argv[++i]);
    else if (arg === '--work-dir' && argv[i + 1]) opts.workDir = path.resolve(argv[++i]);
    else if (arg === '--offset' && argv[i + 1]) opts.portOffset = Number(argv[++i]);
    else if (arg === '--slice-usd' && argv[i + 1]) opts.sliceUsd = Number(argv[++i]);
    else if (arg === '--help' || arg === '-h') opts.mode = 'help';
    else throw new Error(`Unknown or incomplete argument: ${arg}`);
  }
  return opts;
}

function normalizeCases(caseName) {
  if (!caseName || caseName === 'all') return Object.values(CASE_SPECS);
  const normalized = caseName.replace(/^api-key-/, '');
  const spec = CASE_SPECS[normalized];
  if (!spec) {
    throw new Error(
      `Unknown case '${caseName}'. Supported: ${Object.keys(CASE_SPECS)
        .map((k) => `${k} (or api-key-${k})`)
        .join(', ')}, all`,
    );
  }
  return [spec];
}

function findBinary(name) {
  if (name === 'agy' && process.env.AGY_BIN && existsSync(process.env.AGY_BIN)) {
    return realpathSync(process.env.AGY_BIN);
  }
  const searchDirs = (process.env.PATH ?? '').split(path.delimiter).filter(Boolean);
  if (name === 'agy') searchDirs.push('/tmp/cc-lb-antigravity-cli');
  for (const dir of searchDirs) {
    const candidate = path.join(dir, name);
    try {
      const st = statSync(candidate);
      if (st.isFile()) return realpathSync(candidate);
    } catch {}
  }
  return null;
}

function cliVersion(binPath) {
  if (!binPath) return null;
  const out = spawnSync(binPath, ['--version'], { encoding: 'utf8', timeout: 15_000 });
  if (out.status !== 0) return null;
  return String(out.stdout || out.stderr || '').trim().split(/\r?\n/)[0] || null;
}

function loadEnvMap(envFile) {
  const map = {};
  if (envFile && existsSync(envFile)) {
    const text = readFileSync(envFile, 'utf8');
    for (const line of text.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const cleaned = trimmed.replace(/^export\s+/, '');
      try {
        Object.assign(map, parseEnv(cleaned));
      } catch {}
    }
  }
  return map;
}

function resolveCredentialLocator(spec, envFile, fileEnv) {
  for (const key of spec.envKeys) {
    if (typeof fileEnv[key] === 'string' && fileEnv[key].trim().length > 0 && envFile) {
      return {
        found: true,
        source: { type: 'envfile', path: envFile, key },
        fingerprint12: sha256(fileEnv[key].trim()).slice(0, 12),
      };
    }
    if (typeof process.env[key] === 'string' && process.env[key].trim().length > 0) {
      return {
        found: true,
        source: { type: 'env', key },
        fingerprint12: sha256(process.env[key].trim()).slice(0, 12),
      };
    }
  }
  return { found: false, source: null, fingerprint12: null };
}

function portFree(port) {
  return new Promise((resolve) => {
    const socket = net.connect({ host: '127.0.0.1', port });
    let done = false;
    const finish = (value) => {
      if (done) return;
      done = true;
      socket.destroy();
      resolve(value);
    };
    socket.once('connect', () => finish(false));
    socket.once('error', (err) => finish(err?.code === 'ECONNREFUSED'));
    socket.setTimeout(300, () => finish(false));
  });
}

async function findFreeOffset(preferredOffset = null) {
  const candidates = preferredOffset !== null ? [preferredOffset] : [500, 600, 700, 800, 900, 1000, 1100, 1200];
  for (const offset of candidates) {
    const portsToCheck = [
      18500 + offset,
      18501 + offset,
      18502 + offset,
      18503 + offset,
      18510 + offset,
      18511 + offset,
      18512 + offset,
      18513 + offset,
      18520 + offset,
      18521 + offset,
      18522 + offset,
      18523 + offset,
      18530 + offset,
      18531 + offset,
      18532 + offset,
      18533 + offset,
      18563 + offset,
      18564 + offset,
      20500 + offset,
      20510 + offset,
      20520 + offset,
      20530 + offset,
    ];
    const checks = await Promise.all(portsToCheck.map(portFree));
    if (checks.every(Boolean)) return offset;
  }
  throw new Error('BLOCKED:no_free_port_offset_available');
}

function runSelfTests() {
  const testFiles = [
    path.join(HERE, 'self-test.mjs'),
    path.join(HERE, 'trace-proof/otlp-receiver.lifecycle.test.mjs'),
    path.join(HERE, 'approved-core3-subscription-current-native-tests.mjs'),
  ];
  const results = [];
  for (const file of testFiles) {
    const rel = path.relative(REPO_ROOT, file);
    const proc = spawnSync(process.execPath, ['--test', file], {
      cwd: HERE,
      stdio: 'inherit',
      env: process.env,
    });
    results.push({ file: rel, status: proc.status });
    if (proc.status !== 0) {
      process.exit(proc.status ?? 1);
    }
  }
  console.log(JSON.stringify({ status: 'PASS_SELF_TEST', suites: results.length, results }, null, 2));
}

async function runPreflight(opts) {
  const cases = normalizeCases(opts.caseName);
  const ccLbBin = process.env.CC_LB_BIN ?? path.join(REPO_ROOT, 'target/debug/cc-lb');
  const ccLbExists = existsSync(ccLbBin);
  const ccLbSha256 = ccLbExists ? sha256(readFileSync(ccLbBin)) : null;
  const fileEnv = loadEnvMap(opts.envFile);
  const offset = await findFreeOffset(opts.portOffset);

  const caseReports = cases.map((spec) => {
    const cliBin = findBinary(spec.cli);
    const version = cliVersion(cliBin);
    const cred = resolveCredentialLocator(spec, opts.envFile, fileEnv);
    return {
      case: spec.alias,
      id: spec.id,
      provider: spec.provider,
      model: spec.model,
      models: spec.models,
      scenarios: spec.scenarios,
      cli: { name: spec.cli, binary: cliBin, version, ready: Boolean(cliBin && version) },
      credential: {
        ready: cred.found,
        sourceType: cred.source?.type ?? null,
        keyName: cred.source?.key ?? null,
        fingerprint12: cred.fingerprint12,
      },
      ports: {
        proxy: spec.basePorts[0] + offset,
        admin: spec.basePorts[1] + offset,
        metrics: spec.basePorts[2] + offset,
        observer: spec.observerPort + offset,
        providerBoundary: spec.boundaryPort + offset,
        collector: 18563 + offset,
        collectorControl: 18564 + offset,
      },
    };
  });

  const allReady =
    ccLbExists && caseReports.every((c) => c.cli.ready && (opts.envFile || c.credential.ready || opts.mode === 'preflight'));

  const report = {
    status: allReady ? 'PASS_PREFLIGHT' : 'BLOCKED_PREFLIGHT',
    provider_calls: 0,
    cc_lb_binary: { path: ccLbBin, exists: ccLbExists, sha256: ccLbSha256 },
    selected_port_offset: offset,
    slice_cap_usd: opts.sliceUsd,
    cases: caseReports,
  };
  console.log(JSON.stringify(report, null, 2));
  if (!ccLbExists) process.exit(1);
}

async function startCollectorWithCorrelationIds({ port, controlPort, outputFile }) {
  const source = readFileSync(path.join(HERE, 'trace-proof/otlp-receiver.mjs'), 'utf8');
  const anchor = "  'cc_lb.request.id',";
  const mainAnchor = "\nif (path.resolve(process.argv[1] ?? '') === fileURLToPath(import.meta.url)) {";
  const moduleText = source
    .slice(0, source.indexOf(mainAnchor))
    .replace(
      anchor,
      `${anchor}\n  'event.id',\n  'stream_id',\n  'cc_lb.event.id',\n  'gen_ai.response.id',\n  'cc_lb.responses.connection_id',\n  'cc_lb.responses.stream_id',\n  'gen_ai.request.stream_id',`,
    );
  const mod = await import(`data:text/javascript;base64,${Buffer.from(moduleText).toString('base64')}`);
  return mod.startOtlpReceiver({ port, controlPort, outputFile });
}

function stageWorkDir(workDir, ccLbBin, ccLbSha, ledgerPath, credentialsObj, offset = 0) {
  mkdirSync(workDir, { recursive: true, mode: 0o700 });
  for (const rel of [
    'contract.json',
    'approved-core3-cap30-contract.json',
    'qa-runtime.mjs',
    'approved-core3-subscription-budget-v30.mjs',
    'approved-core3-provider-boundary.mjs',
    'approved-core3-oracle.mjs',
    'omp-gpt',
    'omp-gemini',
    'codex',
    'agy',
  ]) {
    cpSync(path.join(HERE, rel), path.join(workDir, rel), { recursive: true });
  }

  const contractPath = path.join(workDir, 'contract.json');
  const contract = JSON.parse(readFileSync(contractPath, 'utf8'));
  contract.artifact = { path: ccLbBin, expected_sha256: ccLbSha };
  contract.resource_ledger = ledgerPath;
  if (Array.isArray(contract.slices) && offset) {
    contract.slices = contract.slices.map((s) => ({
      ...s,
      ports: Array.isArray(s.ports) ? s.ports.map((p) => p + offset) : s.ports,
    }));
  }
  writeFileSync(contractPath, `${JSON.stringify(contract, null, 2)}\n`, { mode: 0o600 });

  writeFileSync(path.join(workDir, 'credentials.json'), `${JSON.stringify(credentialsObj, null, 2)}\n`, {
    mode: 0o600,
  });

  if (!existsSync(ledgerPath)) {
    writeFileSync(
      ledgerPath,
      `${JSON.stringify({ created_at: new Date().toISOString(), entries: [] }, null, 2)}\n`,
      { mode: 0o600 },
    );
  }

  // Update qa-runtime.mjs in the staged workDir with the current binary path, SHA-256, and ledger path
  const rtPath = path.join(workDir, 'qa-runtime.mjs');
  let rtSource = readFileSync(rtPath, 'utf8');
  rtSource = rtSource
    .replace(
      "export const LEDGER_PATH = '/tmp/cc-lb-actual-cli-e2e-20261020-ledger.json';",
      `export const LEDGER_PATH = ${JSON.stringify(ledgerPath)};`,
    )
    .replace(
      "export const BINARY_PATH = '/data/data/orca/workspaces/cc-lb/gemini-codex/target/debug/cc-lb';",
      `export const BINARY_PATH = ${JSON.stringify(ccLbBin)};`,
    )
    .replace(
      "export const EXPECTED_BINARY_SHA256 = '50d43f03ef36956abed6a1894f328c92c6e535263a5e4d29ae971daa8085f977';",
      `export const EXPECTED_BINARY_SHA256 = ${JSON.stringify(ccLbSha)};`,
    );
  writeFileSync(rtPath, rtSource, { mode: 0o600 });

  const ompBin = findBinary('omp');
  const ompVer = cliVersion(ompBin) ?? '18.12.6';
  const ompVerMatch = ompVer.match(/(\d+\.\d+\.\d+)/);
  const ompVerStr = ompVerMatch ? ompVerMatch[1] : '18.12.6';

  // Update omp-gpt/run.mjs in staged workDir
  const ompGptPath = path.join(workDir, 'omp-gpt/run.mjs');
  let ompGptSource = readFileSync(ompGptPath, 'utf8');
  ompGptSource = ompGptSource
    .replace(
      'const BINARY_PATH = "/data/data/orca/workspaces/cc-lb/gemini-codex/target/debug/cc-lb";',
      `const BINARY_PATH = ${JSON.stringify(ccLbBin)};`,
    )
    .replace(
      'const EXPECTED_BINARY_SHA256 = "dcde4343277cfc977bdfa77df768ea475d990d86058a879264fc58bb2a4e71a4";',
      `const EXPECTED_BINARY_SHA256 = ${JSON.stringify(ccLbSha)};`,
    )
    .replace(
      'const EXPECTED_OMP_VERSION_PREFIX = "18.12.5";',
      `const EXPECTED_OMP_VERSION_PREFIX = ${JSON.stringify(ompVerStr)};`,
    )
    .replace(
      'const PORTS = [18510, 18511, 18512];',
      `const PORTS = [${18510 + offset}, ${18511 + offset}, ${18512 + offset}];`,
    );
  writeFileSync(ompGptPath, ompGptSource, { mode: 0o755 });

  // Update omp-gemini/run.mjs in staged workDir
  const ompGemPath = path.join(workDir, 'omp-gemini/run.mjs');
  let ompGemSource = readFileSync(ompGemPath, 'utf8');
  ompGemSource = ompGemSource
    .replace(
      'const EXPECTED_OMP_VERSION = "18.12.2";',
      `const EXPECTED_OMP_VERSION = ${JSON.stringify(ompVerStr)};`,
    )
    .replace(
      'const PORTS = [18520, 18521, 18522];',
      `const PORTS = [${18520 + offset}, ${18521 + offset}, ${18522 + offset}];`,
    );
  writeFileSync(ompGemPath, ompGemSource, { mode: 0o755 });

  const codexPath = path.join(workDir, 'codex/run.mjs');
  let codexSource = readFileSync(codexPath, 'utf8');
  codexSource = codexSource.replace(
    'const PORTS = [18500, 18501, 18502];',
    `const PORTS = [${18500 + offset}, ${18501 + offset}, ${18502 + offset}];`,
  );
  writeFileSync(codexPath, codexSource, { mode: 0o755 });

  // Update agy/run.mjs in staged workDir with thoughtsTokenCount summation
  const agyPath = path.join(workDir, 'agy/run.mjs');
  let agySource = readFileSync(agyPath, 'utf8');
  agySource = agySource.replace(
    '    output_tokens: u.candidatesTokenCount ?? 0,',
    '    output_tokens: (u.candidatesTokenCount ?? 0) + (u.thoughtsTokenCount ?? 0),',
  );
  writeFileSync(agyPath, agySource, { mode: 0o755 });
  chmodSync(path.join(workDir, 'codex/run.mjs'), 0o755);
}

async function runLiveSuite(opts) {
  const cases = normalizeCases(opts.caseName);
  const ccLbBin = process.env.CC_LB_BIN ?? path.join(REPO_ROOT, 'target/debug/cc-lb');
  if (!existsSync(ccLbBin)) {
    throw new Error(`cc-lb binary not found at ${ccLbBin}. Run 'cargo build -p cc-lb-server' first.`);
  }
  const ccLbSha = sha256(readFileSync(ccLbBin));
  const fileEnv = loadEnvMap(opts.envFile);

  const credentialsObj = {};
  for (const spec of cases) {
    const cred = resolveCredentialLocator(spec, opts.envFile, fileEnv);
    if (!cred.found) {
      throw new Error(
        `Missing credential for ${spec.alias}. Provide one of [${spec.envKeys.join(', ')}] in environment or via --env-file.`,
      );
    }
    credentialsObj[spec.provider] = {
      kind: spec.kind,
      baseUrl: spec.targetBaseUrl,
      source: cred.source,
    };
  }

  const runId = new Date().toISOString().replace(/[:.]/g, '-');
  const workDir =
    opts.workDir ?? path.join(os.tmpdir(), `cc-lb-provider-e2e-${runId}-${randomUUID().slice(0, 8)}`);
  const ledgerPath =
    process.env.CC_LB_QA_LEDGER_PATH ?? path.join(workDir, 'resource-ledger.json');
  const offset = await findFreeOffset(opts.portOffset);
  stageWorkDir(workDir, ccLbBin, ccLbSha, ledgerPath, credentialsObj, offset);
  const collectorPort = 18563 + offset;
  const collectorControlPort = 18564 + offset;
  const spansFile = path.join(workDir, 'approved-core3-spans.json');
  const receiver = await startCollectorWithCorrelationIds({
    port: collectorPort,
    controlPort: collectorControlPort,
    outputFile: spansFile,
  });

  const results = [];
  try {
    for (const spec of cases) {
      const driverPath = path.join(workDir, spec.id, 'run.mjs');
      const runnerCmd = spec.id === 'codex' || spec.id === 'agy' ? findBinary('bun') ?? 'bun' : process.execPath;
      const env = {
        ...process.env,
        ...fileEnv,
        QA_GEMINI_PRIMARY_MODEL: 'gemini-3.1-pro-preview',
        CODEX_QA_MODEL: spec.id === 'codex' ? spec.model : undefined,
        CODEX_QA_ATTEMPT_CAP: '32',
        OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${collectorPort}`,
      };
      const logOut = path.join(workDir, `${spec.id}.stdout.log`);
      const logErr = path.join(workDir, `${spec.id}.stderr.log`);
      const outStream = createWriteStream(logOut, { mode: 0o600 });
      const errStream = createWriteStream(logErr, { mode: 0o600 });

      const exitCode = await new Promise((resolve) => {
        const child = spawn(runnerCmd, [driverPath], {
          cwd: workDir,
          env,
          stdio: ['ignore', 'pipe', 'pipe'],
        });
        child.stdout.pipe(outStream);
        child.stderr.pipe(errStream);
        child.once('close', (code) => {
          outStream.end();
          errStream.end();
          resolve(code);
        });
      });

      const summaryPath = path.join(workDir, spec.id, 'summary.json');
      const summary = existsSync(summaryPath) ? JSON.parse(readFileSync(summaryPath, 'utf8')) : null;
      const verdict =
        summary?.verdict ??
        (summary?.verdicts && Object.values(summary.verdicts).every((v) => v === 'PASS')
          ? 'PASS'
          : summary?.scenarios && Object.values(summary.scenarios).every((v) => v?.verdict === 'PASS')
            ? 'PASS'
            : 'FAIL');

      results.push({
        case: spec.alias,
        id: spec.id,
        exitCode,
        verdict,
        summaryPath,
      });
    }
  } finally {
    await receiver.close();
  }

  const allPassed = results.every((r) => r.verdict === 'PASS' && r.exitCode === 0);
  console.log(
    JSON.stringify(
      {
        status: allPassed ? 'PASS' : 'FAIL',
        work_dir: workDir,
        ledger_path: ledgerPath,
        spans_file: spansFile,
        results,
      },
      null,
      2,
    ),
  );
  if (!allPassed) process.exit(1);
}

function printHelp() {
  console.log(`Usage:
  node tests/real-client/provider-e2e/run.mjs --self-test
  node tests/real-client/provider-e2e/run.mjs --preflight [--case <omp-gpt|omp-gemini|codex|agy|all>] [--env-file <path>]
  node tests/real-client/provider-e2e/run.mjs --run --case <omp-gpt|omp-gemini|codex|agy|all> [--env-file <path>] [--work-dir <dir>]
  node tests/real-client/provider-e2e/run.mjs --run-oauth [--work-dir <dir>]
`);
}

const opts = parseArgs(process.argv);
if (!opts.mode || opts.mode === 'help') {
  printHelp();
  process.exit(opts.mode === 'help' ? 0 : 2);
}
if (opts.mode === 'self-test') {
  runSelfTests();
} else if (opts.mode === 'preflight') {
  await runPreflight(opts);
} else if (opts.mode === 'run') {
  await runLiveSuite(opts);
} else if (opts.mode === 'run-oauth') {
  const bunBin = findBinary('bun') ?? 'bun';
  const proc = spawnSync(bunBin, [path.join(HERE, 'run-oauth-shared-pool.mjs')], {
    cwd: HERE,
    stdio: 'inherit',
    env: { ...process.env, ...(opts.workDir ? { CC_LB_QA_WORK_DIR: opts.workDir } : {}) },
  });
  process.exit(proc.status ?? 1);
}
