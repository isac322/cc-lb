#!/usr/bin/env node
// Verifies via Cargo Resolver 2 compilation units (`target`, `host-build`,
// `host-proc-macro`) that every partition in `.github/ci/cov-partitions.json`
// resolves the same workspace crate feature sets as `seed-cov-cache`.
import { execFile as execFileCb } from 'node:child_process';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const execFile = promisify(execFileCb);
const HERE = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_ROOT = path.resolve(HERE, '..');
const UNIT_KINDS = ['target', 'host-build', 'host-proc-macro'];
const MAX_CARGO_CONCURRENCY = 4;

async function mapLimit(items, limit, fn) {
  const results = new Array(items.length);
  let nextIdx = 0;
  const workerCount = Math.min(limit, items.length);
  await Promise.all(
    Array.from({ length: workerCount }, async () => {
      while (true) {
        const idx = nextIdx;
        nextIdx += 1;
        if (idx >= items.length) break;
        results[idx] = await fn(items[idx], idx);
      }
    }),
  );
  return results;
}

function normalizeFeatureList(featStr) {
  const cleaned = featStr.replaceAll(' (*)', '');
  const feats = cleaned
    .split(',')
    .map((f) => f.trim())
    .filter(Boolean);
  return [...new Set(feats)].sort().join(',');
}

function recordUnitFeature(unitMap, pkgName, featKey) {
  let set = unitMap.get(pkgName);
  if (!set) {
    set = new Set();
    unitMap.set(pkgName, set);
  }
  set.add(featKey);
}

function createEmptyUnitBuckets() {
  return {
    target: new Map(),
    'host-build': new Map(),
    'host-proc-macro': new Map(),
  };
}

function parseCargoTreeDepthLine(line) {
  const match = /^(\d+)\s*(.+?)\|\|\|(.*)$/.exec(line.trimEnd());
  if (!match) return null;
  const depth = Number(match[1]);
  const pkgPart = match[2].trim();
  const pkgName = pkgPart.split(/\s+/)[0];
  if (!pkgName) return null;
  return {
    depth,
    pkgPart,
    pkgName,
    featKey: normalizeFeatureList(match[3]),
  };
}

function parseNormalDevUnits(stdout, workspaceNames, procMacroNames, buckets) {
  let procMacroDepth = null;
  for (const rawLine of stdout.split(/\r?\n/)) {
    const parsed = parseCargoTreeDepthLine(rawLine);
    if (!parsed) continue;
    const { depth, pkgPart, pkgName, featKey } = parsed;
    if (procMacroDepth !== null && depth <= procMacroDepth) {
      procMacroDepth = null;
    }
    const unitKind = procMacroDepth !== null ? 'host-proc-macro' : 'target';
    if (workspaceNames.has(pkgName)) {
      recordUnitFeature(buckets[unitKind], pkgName, featKey);
    }
    if (
      procMacroDepth === null &&
      (procMacroNames.has(pkgName) || /\(proc-macro\)/.test(pkgPart))
    ) {
      procMacroDepth = depth;
    }
  }
}

function parseBuildUnits(stdout, workspaceNames, buckets) {
  for (const rawLine of stdout.split(/\r?\n/)) {
    const parsed = parseCargoTreeDepthLine(rawLine);
    if (!parsed || parsed.depth === 0) continue;
    if (workspaceNames.has(parsed.pkgName)) {
      recordUnitFeature(buckets['host-build'], parsed.pkgName, parsed.featKey);
    }
  }
}

async function detectHostTriple(rootDir) {
  const { stdout } = await execFile('rustc', ['-vV'], { cwd: rootDir, encoding: 'utf8' });
  const match = /^host:\s*(\S+)/m.exec(stdout);
  if (!match) {
    throw new Error(`Unable to determine rustc host triple from:\n${stdout}`);
  }
  return match[1];
}

async function loadWorkspaceMeta(rootDir) {
  const { stdout } = await execFile('cargo', ['metadata', '--no-deps', '--format-version', '1'], {
    cwd: rootDir,
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
  });
  const metadata = JSON.parse(stdout);
  const workspaceIds = new Set(metadata.workspace_members ?? []);
  const workspaceNames = new Set();
  const procMacroNames = new Set();
  for (const pkg of metadata.packages ?? []) {
    if (workspaceIds.size > 0 && !workspaceIds.has(pkg.id)) continue;
    workspaceNames.add(pkg.name);
    if ((pkg.targets ?? []).some((t) => (t.kind ?? []).includes('proc-macro'))) {
      procMacroNames.add(pkg.name);
    }
  }
  return { workspaceNames, procMacroNames };
}

function formatCargoError(err) {
  const stderr = (err?.stderr ?? '').toString().trim();
  if (!stderr) return err?.message ?? String(err);
  const lines = stderr.split(/\r?\n/).filter(Boolean);
  return lines.slice(0, 6).join(' | ');
}

async function inspectCargoUnits({
  rootDir,
  target,
  locked,
  pkgArgs,
  workspaceNames,
  procMacroNames,
}) {
  const buckets = createEmptyUnitBuckets();
  const baseArgs = ['tree', '--offline', ...(locked ? ['--locked'] : []), '--target', target];

  const [normalRes, buildRes] = await Promise.all([
    execFile(
      'cargo',
      [...baseArgs, '-e', 'normal,dev', '--prefix', 'depth', '--format', '{p}|||{f}', ...pkgArgs],
      { cwd: rootDir, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 },
    ),
    execFile(
      'cargo',
      [...baseArgs, '-e', 'build', '--prefix', 'depth', '--format', '{p}|||{f}', ...pkgArgs],
      { cwd: rootDir, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 },
    ),
  ]);

  parseNormalDevUnits(normalRes.stdout, workspaceNames, procMacroNames, buckets);
  parseBuildUnits(buildRes.stdout, workspaceNames, buckets);
  return buckets;
}

function compareBuckets(partName, partBuckets, seedBuckets) {
  const errors = [];
  for (const unitKind of UNIT_KINDS) {
    const actualMap = partBuckets[unitKind];
    const seedMap = seedBuckets[unitKind];
    for (const pkgName of [...actualMap.keys()].sort()) {
      const actual = [...actualMap.get(pkgName)].sort();
      const expected = [...(seedMap.get(pkgName) ?? [])].sort();
      if (actual.join(';') !== expected.join(';')) {
        errors.push(
          `partition '${partName}' [${unitKind}] feature drift vs seed-cov-cache for '${pkgName}': resolved=[${actual.join(' | ')}], seed=[${expected.join(' | ')}]`,
        );
      }
    }
  }
  return errors;
}

/**
 * Checks every run in `config.partitions` against the `seed-cov-cache`
 * `--workspace --all-features` invocation across `target`, `host-build`, and
 * `host-proc-macro` Cargo Resolver 2 compilation units.
 */
export async function checkFeatureParity({ rootDir, config, target, locked = true }) {
  const resolvedTarget = target ?? (await detectHostTriple(rootDir));
  const { workspaceNames, procMacroNames } = await loadWorkspaceMeta(rootDir);

  const seedPkgArgs = ['--workspace', '--all-features'];
  for (const excludedPkg of config.excluded ?? []) {
    seedPkgArgs.push('--exclude', excludedPkg);
  }

  let seedBuckets;
  try {
    seedBuckets = await inspectCargoUnits({
      rootDir,
      target: resolvedTarget,
      locked,
      pkgArgs: seedPkgArgs,
      workspaceNames,
      procMacroNames,
    });
  } catch (err) {
    return {
      target: resolvedTarget,
      partitionRuns: 0,
      errors: [`seed-cov-cache cargo tree failed: ${formatCargoError(err)}`],
    };
  }

  const runTasks = [];
  for (const part of config.partitions ?? []) {
    for (const run of part.runs ?? []) {
      const pkgArgs = [];
      for (const pkg of run.packages ?? []) {
        pkgArgs.push('-p', pkg);
      }
      pkgArgs.push('--all-features');
      const extraFeatures = Array.isArray(run.extra_features)
        ? run.extra_features.filter(Boolean)
        : [];
      if (extraFeatures.length > 0) {
        pkgArgs.push('--features', extraFeatures.join(','));
      }
      runTasks.push({ partName: part.name, pkgArgs });
    }
  }

  const perRunResults = await mapLimit(runTasks, MAX_CARGO_CONCURRENCY, async (task) => {
    try {
      const buckets = await inspectCargoUnits({
        rootDir,
        target: resolvedTarget,
        locked,
        pkgArgs: task.pkgArgs,
        workspaceNames,
        procMacroNames,
      });
      return { partName: task.partName, buckets, error: null };
    } catch (err) {
      return {
        partName: task.partName,
        buckets: null,
        error: `partition '${task.partName}' cargo tree failed: ${formatCargoError(err)}`,
      };
    }
  });

  const errors = [];
  for (const res of perRunResults) {
    if (res.error) {
      errors.push(res.error);
    } else {
      errors.push(...compareBuckets(res.partName, res.buckets, seedBuckets));
    }
  }

  return {
    target: resolvedTarget,
    partitionRuns: runTasks.length,
    errors,
  };
}

function parseCliArgs(argv) {
  let rootDir = DEFAULT_ROOT;
  let configPath = null;
  let target = null;
  let locked = true;
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '--root' && argv[i + 1]) {
      rootDir = path.resolve(argv[++i]);
    } else if (argv[i] === '--config' && argv[i + 1]) {
      configPath = argv[++i];
    } else if (argv[i] === '--target' && argv[i + 1]) {
      target = argv[++i];
    } else if (argv[i] === '--no-locked') {
      locked = false;
    } else {
      console.error(
        'Usage: node scripts/check-cov-feature-parity.mjs [--root <dir>] [--config <path>] [--target <triple>] [--no-locked]',
      );
      process.exit(2);
    }
  }
  return {
    rootDir,
    configPath: configPath ?? path.join(rootDir, '.github/ci/cov-partitions.json'),
    target,
    locked,
  };
}

async function main() {
  const { rootDir, configPath, target, locked } = parseCliArgs(process.argv.slice(2));
  const config = JSON.parse(readFileSync(configPath, 'utf8'));
  const result = await checkFeatureParity({ rootDir, config, target, locked });
  if (result.errors.length > 0) {
    for (const err of result.errors) {
      console.error(`check-cov-feature-parity: ERROR: ${err}`);
    }
    process.exit(1);
  }
  console.log(
    `check-cov-feature-parity: PASS (${result.partitionRuns} partition runs, target=${result.target}, 0 unit feature mismatches)`,
  );
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await main();
}
