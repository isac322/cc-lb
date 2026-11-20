#!/usr/bin/env node
// Validates .github/ci/cov-partitions.json against cargo metadata and coverage-gate.sh.
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');

export function extractGatedPackages(gateScriptText) {
  const gated = new Set();
  for (const match of gateScriptText.matchAll(/set\("([^"]+)",\s*\d+\)/g)) {
    gated.add(match[1]);
  }
  return gated;
}

function targetKindForTestSelection(target) {
  const kinds = new Set(target.kind ?? []);
  if (kinds.has('lib') || kinds.has('rlib') || kinds.has('cdylib') || kinds.has('proc-macro')) {
    return 'lib';
  }
  if (kinds.has('bin')) return 'bin';
  if (kinds.has('test')) return `test:${target.name}`;
  return null;
}

function runSelectsTarget(cargoArgs, targetKind) {
  if (!cargoArgs || cargoArgs.length === 0) return true;
  if (cargoArgs.includes('--all-targets')) return true;
  for (let i = 0; i < cargoArgs.length; i += 1) {
    const arg = cargoArgs[i];
    if (arg === '--lib' && targetKind === 'lib') return true;
    if (arg === '--bins' && targetKind === 'bin') return true;
    if (arg === '--bin' && cargoArgs[i + 1] && targetKind === 'bin') return true;
    if (arg === '--tests' && targetKind.startsWith('test:')) return true;
    if (arg === '--test' && cargoArgs[i + 1] && targetKind === `test:${cargoArgs[i + 1]}`) return true;
  }
  return false;
}

export function validatePartitions(config, metadata, gatedPackages) {
  const errors = [];
  const excluded = new Set(config.excluded ?? []);
  const partitions = config.partitions ?? [];
  if (partitions.length === 0) {
    errors.push('partitions list must not be empty');
    return errors;
  }

  const workspaceIds = new Set(metadata.workspace_members ?? []);
  const workspacePkgs = new Map();
  for (const pkg of metadata.packages ?? []) {
    if (workspaceIds.size === 0 || workspaceIds.has(pkg.id)) {
      workspacePkgs.set(pkg.name, pkg);
    }
  }

  for (const ex of excluded) {
    if (!workspacePkgs.has(ex)) {
      errors.push(`excluded package '${ex}' is not a workspace member`);
    }
    if (gatedPackages.has(ex)) {
      errors.push(`gated package '${ex}' must not be excluded`);
    }
  }

  const partitionNames = new Set();
  const seenTargetsByPkg = new Map();

  for (const part of partitions) {
    if (!part.name) errors.push('partition is missing a name');
    if (partitionNames.has(part.name)) errors.push(`duplicate partition name '${part.name}'`);
    partitionNames.add(part.name);

    const runs = part.runs ?? [];
    if (runs.length === 0) errors.push(`partition '${part.name}' has no runs`);
    const seenPkgInPart = new Set();
    for (const run of runs) {
      const pkgs = run.packages ?? [];
      if (pkgs.length === 0) errors.push(`partition '${part.name}' has a run with no packages`);
      const cargoArgs = run.cargo_args ?? [];
      for (const pkgName of pkgs) {
        if (excluded.has(pkgName)) {
          errors.push(`package '${pkgName}' in partition '${part.name}' is also listed in excluded`);
          continue;
        }
        const pkg = workspacePkgs.get(pkgName);
        if (!pkg) {
          errors.push(`package '${pkgName}' in partition '${part.name}' is not a workspace member`);
          continue;
        }
        if (seenPkgInPart.has(pkgName) && cargoArgs.length === 0) {
          errors.push(`package '${pkgName}' appears multiple times with default targets in '${part.name}'`);
        }
        seenPkgInPart.add(pkgName);

        const testableTargets = new Set();
        for (const t of pkg.targets ?? []) {
          const tk = targetKindForTestSelection(t);
          if (tk) testableTargets.add(tk);
        }
        const recorded = seenTargetsByPkg.get(pkgName) ?? new Set();
        for (const tk of testableTargets) {
          if (runSelectsTarget(cargoArgs, tk)) {
            if (recorded.has(tk)) {
              errors.push(`target '${pkgName}:${tk}' is covered by multiple runs (including partition '${part.name}')`);
            }
            recorded.add(tk);
          }
        }
        seenTargetsByPkg.set(pkgName, recorded);
      }
    }
  }

  for (const [pkgName, pkg] of workspacePkgs) {
    if (excluded.has(pkgName)) continue;
    const expectedTargets = new Set();
    for (const t of pkg.targets ?? []) {
      const tk = targetKindForTestSelection(t);
      if (tk) expectedTargets.add(tk);
    }
    const covered = seenTargetsByPkg.get(pkgName) ?? new Set();
    if (covered.size === 0) {
      errors.push(`workspace package '${pkgName}' is missing from all partitions`);
      continue;
    }
    for (const tk of expectedTargets) {
      if (!covered.has(tk)) {
        errors.push(`target '${pkgName}:${tk}' is not covered by any partition run`);
      }
    }
  }

  for (const gated of gatedPackages) {
    const covered = seenTargetsByPkg.get(gated);
    if (!covered || covered.size === 0) {
      errors.push(`coverage-gate package '${gated}' is not covered by any partition`);
    }
  }

  return errors;
}

function main() {
  const configPath = process.argv[2] ?? path.join(ROOT, '.github/ci/cov-partitions.json');
  const config = JSON.parse(readFileSync(configPath, 'utf8'));
  const gateText = readFileSync(path.join(ROOT, 'scripts/coverage-gate.sh'), 'utf8');
  const gatedPackages = extractGatedPackages(gateText);
  const rawMeta = execFileSync('cargo', ['metadata', '--no-deps', '--format-version', '1'], {
    cwd: ROOT,
    encoding: 'utf8',
  });
  const metadata = JSON.parse(rawMeta);
  const errors = validatePartitions(config, metadata, gatedPackages);
  const ciPath = path.join(ROOT, '.github/workflows/ci.yml');
  try {
    const ciText = readFileSync(ciPath, 'utf8');
    const matrixMatches = [...ciText.matchAll(/- partition:\s*([a-z0-9-]+)\s*\n\s*needs_postgres:\s*(true|false)\s*\n\s*needs_wasm:\s*(true|false)\s*\n\s*save_sccache:\s*"(true|false)"/g)];
    if (matrixMatches.length === 0) {
      errors.push('could not find nextest-cov-part matrix entries in .github/workflows/ci.yml');
    } else {
      const ciMatrix = matrixMatches.map((m) => ({
        name: m[1],
        needs_postgres: m[2] === 'true',
        needs_wasm: m[3] === 'true',
        save_sccache: m[4] === 'true',
      }));
      const expectedMatrix = (config.partitions ?? []).map((p) => ({
        name: p.name,
        needs_postgres: Boolean(p.needs_postgres),
        needs_wasm: Boolean(p.needs_wasm),
        save_sccache: Boolean(p.save_sccache),
      }));
      if (JSON.stringify(ciMatrix) !== JSON.stringify(expectedMatrix)) {
        errors.push('.github/workflows/ci.yml nextest-cov-part matrix does not match .github/ci/cov-partitions.json');
      }
    }
  } catch (err) {
    errors.push(`failed to read .github/workflows/ci.yml: ${err.message}`);
  }
  if (errors.length > 0) {
    for (const err of errors) console.error(`check-cov-partitions: ERROR: ${err}`);
    process.exit(1);
  }
  const totalCovered = (config.partitions ?? []).reduce(
    (set, p) => {
      for (const r of p.runs ?? []) for (const name of r.packages ?? []) set.add(name);
      return set;
    },
    new Set(),
  ).size;
  console.log(
    `check-cov-partitions: PASS (${config.partitions.length} partitions, ${totalCovered} covered packages, ${(config.excluded ?? []).length} excluded, ${gatedPackages.size} gated)`,
  );
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
