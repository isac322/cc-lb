#!/usr/bin/env node
// Verifies via Cargo's resolved `--unit-graph` (`target` and `host` compilation
// units) that every partition in `.github/ci/cov-partitions.json` resolves the
// same workspace crate feature sets as `seed-cov-cache`.
import { execFile as execFileCb } from 'node:child_process';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const execFile = promisify(execFileCb);
const HERE = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_ROOT = path.resolve(HERE, '..');
const MAX_CARGO_CONCURRENCY = 4;

async function mapLimit(items, limit, fn) {
  if (items.length === 0) return [];
  const results = new Array(items.length);
  let nextIdx = 0;
  const workerCount = Math.min(limit, items.length);
  const settled = await Promise.allSettled(
    Array.from({ length: workerCount }, async () => {
      while (true) {
        const idx = nextIdx;
        nextIdx += 1;
        if (idx >= items.length) break;
        results[idx] = await fn(items[idx], idx);
      }
    }),
  );
  const firstRejected = settled.find((s) => s.status === 'rejected');
  if (firstRejected) {
    throw firstRejected.reason;
  }
  return results;
}

function recordUnitFeature(unitMap, unitKey, featKey) {
  let set = unitMap.get(unitKey);
  if (!set) {
    set = new Set();
    unitMap.set(unitKey, set);
  }
  set.add(featKey);
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
  const workspaceIdToName = new Map();
  for (const pkg of metadata.packages ?? []) {
    if (workspaceIds.size > 0 && !workspaceIds.has(pkg.id)) continue;
    workspaceIdToName.set(pkg.id, pkg.name);
  }
  return { workspaceIdToName };
}

function formatCargoError(err) {
  const stderr = (err?.stderr ?? '').toString().trim();
  if (!stderr) return err?.message ?? String(err);
  const lines = stderr.split(/\r?\n/).filter(Boolean);
  return lines.slice(0, 6).join(' | ');
}

function parseUnitGraph(stdout, workspaceIdToName) {
  const graph = JSON.parse(stdout);
  if (graph?.version !== 1 || !Array.isArray(graph?.units)) {
    throw new Error(`Unexpected Cargo unit-graph payload (version=${graph?.version})`);
  }

  const units = new Map();
  for (const u of graph.units) {
    const pkgName = workspaceIdToName.get(u.pkg_id);
    if (!pkgName) continue;
    // `cargo nextest` runs compiled test binaries and never invokes rustdoc doctests.
    if (u.mode === 'doctest') continue;

    const unitKind = u.platform ? 'target' : 'host';
    const targetKinds = (u.target?.kind ?? []).slice().sort().join(',');
    const targetName = u.target?.name ?? '';
    const unitKey = `${unitKind}|${pkgName}|${targetKinds}|${targetName}|${u.mode}`;
    const featKey = [...new Set(u.features ?? [])].sort().join(',');
    recordUnitFeature(units, unitKey, featKey);
  }
  return units;
}

async function inspectCargoUnits({ rootDir, target, locked, pkgArgs, workspaceIdToName }) {
  const args = [
    'test',
    '-Z',
    'unstable-options',
    '--unit-graph',
    '--no-run',
    '--offline',
    ...(locked ? ['--locked'] : []),
    '--target',
    target,
    ...pkgArgs,
  ];
  const { stdout } = await execFile('cargo', args, {
    cwd: rootDir,
    encoding: 'utf8',
    maxBuffer: 128 * 1024 * 1024,
    env: { ...process.env, RUSTC_BOOTSTRAP: '1' },
  });
  return parseUnitGraph(stdout, workspaceIdToName);
}

function compareUnits(partName, partUnits, seedUnits) {
  const errors = [];
  for (const unitKey of [...partUnits.keys()].sort()) {
    const [unitKind, pkgName, targetKinds, targetName, mode] = unitKey.split('|');
    const actualFeats = [...partUnits.get(unitKey)].sort();
    const seedSet = seedUnits.get(unitKey);
    const seedFeats = [...(seedSet ?? [])].sort();
    for (const actualFeat of actualFeats) {
      if (!seedSet || !seedSet.has(actualFeat)) {
        errors.push(
          `partition '${partName}' [${unitKind}] feature drift vs seed-cov-cache for '${pkgName}' (${targetKinds}:${targetName}:${mode}): resolved=[${actualFeat}], seed=[${seedFeats.join(' | ')}]`,
        );
      }
    }
  }
  return errors;
}

/**
 * Checks every run in `config.partitions` against the `seed-cov-cache`
 * `--workspace --all-features` invocation across `target` and `host`
 * Cargo Resolver 2 compilation units via `cargo test --unit-graph`.
 */
export async function checkFeatureParity({ rootDir, config, target, locked = true }) {
  const resolvedTarget = target ?? (await detectHostTriple(rootDir));
  const { workspaceIdToName } = await loadWorkspaceMeta(rootDir);

  const seedExcluded = Array.isArray(config.seed_excluded)
    ? config.seed_excluded
    : (config.excluded ?? []);
  const seedPkgArgs = ['--workspace', '--all-features'];
  for (const excludedPkg of seedExcluded) {
    seedPkgArgs.push('--exclude', excludedPkg);
  }

  let seedUnits;
  try {
    seedUnits = await inspectCargoUnits({
      rootDir,
      target: resolvedTarget,
      locked,
      pkgArgs: seedPkgArgs,
      workspaceIdToName,
    });
  } catch (err) {
    return {
      target: resolvedTarget,
      partitionRuns: 0,
      errors: [`seed-cov-cache cargo test --unit-graph failed: ${formatCargoError(err)}`],
    };
  }

  const runTasks = [];
  for (const part of config.partitions ?? []) {
    for (const run of part.runs ?? []) {
      const pkgArgs = [];
      for (const pkg of run.packages ?? []) {
        pkgArgs.push('-p', pkg);
      }
      pkgArgs.push(...(run.cargo_args ?? []));
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
      const units = await inspectCargoUnits({
        rootDir,
        target: resolvedTarget,
        locked,
        pkgArgs: task.pkgArgs,
        workspaceIdToName,
      });
      return { partName: task.partName, units, error: null };
    } catch (err) {
      return {
        partName: task.partName,
        units: null,
        error: `partition '${task.partName}' cargo test --unit-graph failed: ${formatCargoError(err)}`,
      };
    }
  });

  const errors = [];
  for (const res of perRunResults) {
    if (res.error) {
      errors.push(res.error);
    } else {
      errors.push(...compareUnits(res.partName, res.units, seedUnits));
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
