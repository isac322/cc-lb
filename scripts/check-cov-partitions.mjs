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
  if (target.test === false) return null;
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

function parsePartitionShard(nextestArgs = []) {
  for (let i = 0; i < nextestArgs.length; i += 1) {
    const arg = nextestArgs[i];
    let spec = null;
    if (arg === '--partition' && nextestArgs[i + 1]) {
      spec = nextestArgs[i + 1];
    } else if (arg.startsWith('--partition=')) {
      spec = arg.slice('--partition='.length);
    }
    if (!spec) continue;
    const match = /^(hash|count):(\d+)\/(\d+)$/.exec(spec);
    if (!match) {
      return { invalid: spec };
    }
    const index = Number(match[2]);
    const total = Number(match[3]);
    if (total < 2 || index < 1 || index > total) {
      return { invalid: spec };
    }
    return { kind: match[1], index, total };
  }
  return null;
}

function collectWorkspacePackages(metadata) {
  const workspaceIds = new Set(metadata.workspace_members ?? []);
  const workspacePkgs = new Map();
  for (const pkg of metadata.packages ?? []) {
    if (workspaceIds.size === 0 || workspaceIds.has(pkg.id)) {
      workspacePkgs.set(pkg.name, pkg);
    }
  }
  return workspacePkgs;
}

function isTargetActiveForCoverage(target) {
  if (!target) return true;
  const normalized = String(target).replace(/\s+/g, '');
  return normalized !== 'cfg(loom)' && normalized !== 'cfg(trybuild)';
}

function getRelevantDeps(pkg, rootSet, workspacePkgs, workspaceOnly = true) {
  if (!pkg) return [];
  const isRoot = rootSet.has(pkg.name);
  return (pkg.dependencies ?? []).filter(
    (dep) =>
      (!workspaceOnly || workspacePkgs.has(dep.name)) &&
      isTargetActiveForCoverage(dep.target) &&
      (dep.kind !== 'dev' || isRoot),
  );
}

function createResolutionState(workspacePkgs, rootPkgNames) {
  return {
    workspacePkgs,
    rootPkgNames,
    rootSet: new Set(rootPkgNames),
    activePkgs: new Set(),
    featuresByPkg: new Map(),
    activeDepKeysByPkg: new Map(),
    weakDepFeatsByPkg: new Map(),
    expandedByPkg: new Map(),
    extraErrors: [],
    changed: false,
  };
}

function ensurePkgState(state, pkgName) {
  if (!state.featuresByPkg.has(pkgName)) state.featuresByPkg.set(pkgName, new Set());
  if (!state.activeDepKeysByPkg.has(pkgName)) state.activeDepKeysByPkg.set(pkgName, new Set());
  if (!state.weakDepFeatsByPkg.has(pkgName)) state.weakDepFeatsByPkg.set(pkgName, new Map());
  if (!state.expandedByPkg.has(pkgName)) state.expandedByPkg.set(pkgName, new Set());
}

function activatePkg(state, pkgName) {
  if (!state.workspacePkgs.has(pkgName)) return;
  ensurePkgState(state, pkgName);
  if (!state.activePkgs.has(pkgName)) {
    state.activePkgs.add(pkgName);
    state.changed = true;
  }
}

function enablePkgFeature(state, pkgName, featName) {
  const pkg = state.workspacePkgs.get(pkgName);
  if (!pkg || !featName) return;
  if (pkg.features && Object.prototype.hasOwnProperty.call(pkg.features, featName)) {
    ensurePkgState(state, pkgName);
    const set = state.featuresByPkg.get(pkgName);
    if (!set.has(featName)) {
      set.add(featName);
      state.changed = true;
    }
    return;
  }
  for (const dep of getRelevantDeps(pkg, state.rootSet, state.workspacePkgs)) {
    if (dep.optional && (dep.rename ?? dep.name) === featName) {
      activateDepEdge(state, pkgName, dep);
    }
  }
}

function activateDepEdge(state, ownerName, dep) {
  const depKey = dep.rename ?? dep.name;
  ensurePkgState(state, ownerName);
  const activeKeys = state.activeDepKeysByPkg.get(ownerName);
  if (!activeKeys.has(depKey)) {
    activeKeys.add(depKey);
    state.changed = true;
  }
  activatePkg(state, dep.name);
  if (dep.uses_default_features !== false) {
    enablePkgFeature(state, dep.name, 'default');
  }
  for (const df of dep.features ?? []) {
    enablePkgFeature(state, dep.name, df);
  }
}

function activateDepByKey(state, ownerName, depKey, depFeat) {
  const owner = state.workspacePkgs.get(ownerName);
  for (const dep of getRelevantDeps(owner, state.rootSet, state.workspacePkgs)) {
    if ((dep.rename ?? dep.name) === depKey || dep.name === depKey) {
      activateDepEdge(state, ownerName, dep);
      if (depFeat) enablePkgFeature(state, dep.name, depFeat);
    }
  }
}

function addWeakDepFeature(state, ownerName, depKey, depFeat) {
  ensurePkgState(state, ownerName);
  const weakMap = state.weakDepFeatsByPkg.get(ownerName);
  let set = weakMap.get(depKey);
  if (!set) {
    set = new Set();
    weakMap.set(depKey, set);
  }
  if (!set.has(depFeat)) {
    set.add(depFeat);
    state.changed = true;
  }
}

function applyExtraFeatureSpecs(state, extraFeatures) {
  for (const rawSpec of extraFeatures) {
    const spec = rawSpec.trim();
    if (!spec) continue;
    const slashIdx = spec.indexOf('/');
    if (slashIdx === -1) {
      let matched = false;
      for (const rootName of state.rootPkgNames) {
        const pkg = state.workspacePkgs.get(rootName);
        if (pkg?.features && Object.prototype.hasOwnProperty.call(pkg.features, spec)) {
          enablePkgFeature(state, rootName, spec);
          matched = true;
        }
      }
      if (!matched) {
        state.extraErrors.push(
          `extra_features entry '${spec}' does not match any feature on [${state.rootPkgNames.join(', ')}]`,
        );
      }
      continue;
    }

    const depSpec = spec.slice(0, slashIdx);
    const depFeat = spec.slice(slashIdx + 1);
    let matched = false;
    if (state.rootSet.has(depSpec)) {
      const targetPkg = state.workspacePkgs.get(depSpec);
      if (targetPkg?.features && Object.prototype.hasOwnProperty.call(targetPkg.features, depFeat)) {
        enablePkgFeature(state, depSpec, depFeat);
        matched = true;
      }
    }
    for (const rootName of state.rootPkgNames) {
      const rootPkg = state.workspacePkgs.get(rootName);
      for (const dep of getRelevantDeps(rootPkg, state.rootSet, state.workspacePkgs, false)) {
        const depKey = dep.rename ?? dep.name;
        if (depKey !== depSpec && dep.name !== depSpec) continue;
        matched = true;
        if (state.workspacePkgs.has(dep.name)) {
          activateDepEdge(state, rootName, dep);
          enablePkgFeature(state, dep.name, depFeat);
        }
      }
    }
    if (!matched) {
      state.extraErrors.push(
        `extra_features entry '${spec}' does not match any root package or direct dependency of [${state.rootPkgNames.join(', ')}]`,
      );
    }
  }
}

function propagateActivePackageStep(state, pkgName) {
  const pkg = state.workspacePkgs.get(pkgName);
  const relevant = getRelevantDeps(pkg, state.rootSet, state.workspacePkgs);
  for (const dep of relevant) {
    if (!dep.optional) activateDepEdge(state, pkgName, dep);
  }

  const expanded = state.expandedByPkg.get(pkgName);
  for (const featName of [...(state.featuresByPkg.get(pkgName) ?? [])]) {
    if (expanded.has(featName)) continue;
    expanded.add(featName);
    state.changed = true;
    for (const item of pkg.features?.[featName] ?? []) {
      if (item.startsWith('dep:')) {
        activateDepByKey(state, pkgName, item.slice('dep:'.length), null);
      } else if (item.includes('?/')) {
        const qSlash = item.indexOf('?/');
        addWeakDepFeature(state, pkgName, item.slice(0, qSlash), item.slice(qSlash + 2));
      } else if (item.includes('/')) {
        const slash = item.indexOf('/');
        activateDepByKey(state, pkgName, item.slice(0, slash), item.slice(slash + 1));
      } else {
        enablePkgFeature(state, pkgName, item);
      }
    }
  }

  const activeKeys = state.activeDepKeysByPkg.get(pkgName);
  const weakMap = state.weakDepFeatsByPkg.get(pkgName);
  for (const dep of relevant) {
    const depKey = dep.rename ?? dep.name;
    if (!activeKeys.has(depKey)) continue;
    for (const df of weakMap.get(depKey) ?? []) {
      enablePkgFeature(state, dep.name, df);
    }
  }
}

// Offline workspace-internal feature resolver mirroring Cargo's `--all-features`
// and `--features` unification so `fmt` can catch cache-busting drift without
// restoring `~/.cargo/registry` for `cargo tree`.
export function resolveWorkspaceFeatures(workspacePkgs, rootPkgNames, extraFeatures = []) {
  const state = createResolutionState(workspacePkgs, rootPkgNames);

  for (const rootName of rootPkgNames) {
    const pkg = workspacePkgs.get(rootName);
    if (!pkg) continue;
    activatePkg(state, rootName);
    for (const featName of Object.keys(pkg.features ?? {})) {
      enablePkgFeature(state, rootName, featName);
    }
    for (const dep of getRelevantDeps(pkg, state.rootSet, workspacePkgs)) {
      if (dep.optional) activateDepEdge(state, rootName, dep);
    }
  }

  applyExtraFeatureSpecs(state, extraFeatures);

  do {
    state.changed = false;
    for (const pkgName of [...state.activePkgs]) {
      propagateActivePackageStep(state, pkgName);
    }
  } while (state.changed);

  return {
    featuresByPkg: state.featuresByPkg,
    activePkgs: state.activePkgs,
    extraErrors: state.extraErrors,
  };
}

export function validateFeatureParity(config, metadata) {
  const errors = [];
  const excluded = new Set(config.excluded ?? []);
  const workspacePkgs = collectWorkspacePackages(metadata);
  const seedRootPkgs = [...workspacePkgs.keys()].filter((name) => !excluded.has(name));
  const seedResolved = resolveWorkspaceFeatures(workspacePkgs, seedRootPkgs, []);

  for (const part of config.partitions ?? []) {
    for (const run of part.runs ?? []) {
      const partResolved = resolveWorkspaceFeatures(
        workspacePkgs,
        run.packages ?? [],
        run.extra_features ?? [],
      );
      for (const err of partResolved.extraErrors) {
        errors.push(`partition '${part.name}' ${err}`);
      }
      for (const pkgName of [...partResolved.activePkgs].sort()) {
        const actual = partResolved.featuresByPkg.get(pkgName) ?? new Set();
        const expected = seedResolved.featuresByPkg.get(pkgName) ?? new Set();
        const missing = [...expected].filter((f) => !actual.has(f)).sort();
        const extra = [...actual].filter((f) => !expected.has(f)).sort();
        if (missing.length > 0 || extra.length > 0) {
          const actualList = [...actual].sort().join(', ');
          const expectedList = [...expected].sort().join(', ');
          const details = [];
          if (missing.length > 0) details.push(`missing=[${missing.join(', ')}]`);
          if (extra.length > 0) details.push(`extra=[${extra.join(', ')}]`);
          errors.push(
            `partition '${part.name}' feature drift vs seed-cov-cache for '${pkgName}': resolved=[${actualList}], seed=[${expectedList}], ${details.join(', ')}`,
          );
        }
      }
    }
  }
  return errors;
}

export function validatePartitions(config, metadata, gatedPackages) {
  const errors = [];
  const excluded = new Set(config.excluded ?? []);
  const partitions = config.partitions ?? [];
  if (partitions.length === 0) {
    errors.push('partitions list must not be empty');
    return errors;
  }

  const workspacePkgs = collectWorkspacePackages(metadata);

  for (const ex of excluded) {
    if (!workspacePkgs.has(ex)) {
      errors.push(`excluded package '${ex}' is not a workspace member`);
    }
    if (gatedPackages.has(ex)) {
      errors.push(`gated package '${ex}' must not be excluded`);
    }
  }

  const partitionNames = new Set();
  const seenPackages = new Set();
  const seenTargetsByPkg = new Map();
  const shardGroups = new Map();

  for (const part of partitions) {
    if (!part.name) errors.push('partition is missing a name');
    if (partitionNames.has(part.name)) errors.push(`duplicate partition name '${part.name}'`);
    partitionNames.add(part.name);
    if (
      part.test_threads !== undefined &&
      (!Number.isInteger(part.test_threads) || part.test_threads < 1)
    ) {
      errors.push(`partition '${part.name}' has invalid test_threads '${part.test_threads}'`);
    }

    const runs = part.runs ?? [];
    if (runs.length === 0) errors.push(`partition '${part.name}' has no runs`);
    const seenPkgInPart = new Set();
    for (const run of runs) {
      const pkgs = run.packages ?? [];
      if (pkgs.length === 0) errors.push(`partition '${part.name}' has a run with no packages`);
      const cargoArgs = run.cargo_args ?? [];
      const extraFeatures = run.extra_features ?? [];
      if (
        !Array.isArray(extraFeatures) ||
        extraFeatures.some((f) => typeof f !== 'string' || f.length === 0)
      ) {
        errors.push(`partition '${part.name}' has invalid extra_features`);
      }
      const shard = parsePartitionShard(run.nextest_args ?? []);
      if (shard?.invalid) {
        errors.push(`partition '${part.name}' has invalid nextest shard '${shard.invalid}'`);
        continue;
      }

      if (shard) {
        const shardKey = JSON.stringify({
          kind: shard.kind,
          total: shard.total,
          packages: [...pkgs].sort(),
          cargoArgs,
          extraFeatures: Array.isArray(extraFeatures) ? [...extraFeatures].sort() : [],
          filterExpr: run.filter_expr ?? null,
        });
        let group = shardGroups.get(shardKey);
        if (!group) {
          group = {
            shardKind: shard.kind,
            total: shard.total,
            packages: [...pkgs],
            cargoArgs,
            indices: new Map(),
            hasObjectUploader: false,
          };
          shardGroups.set(shardKey, group);
        }
        if (group.indices.has(shard.index)) {
          errors.push(`duplicate shard ${shard.kind}:${shard.index}/${shard.total} in partition '${part.name}'`);
        } else {
          group.indices.set(shard.index, part.name);
        }
        if (part.upload_objects !== false) {
          group.hasObjectUploader = true;
        }
        for (const pkgName of pkgs) {
          if (excluded.has(pkgName)) {
            errors.push(`package '${pkgName}' in partition '${part.name}' is also listed in excluded`);
          } else if (!workspacePkgs.has(pkgName)) {
            errors.push(`package '${pkgName}' in partition '${part.name}' is not a workspace member`);
          } else {
            seenPackages.add(pkgName);
          }
        }
        // Only record targets from shard 1 to avoid flagging sibling shards as duplicate coverage.
        if (shard.index !== 1) continue;
      } else if (part.upload_objects === false) {
        errors.push(`unsharded partition '${part.name}' must not set upload_objects=false`);
      }

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
        seenPackages.add(pkgName);
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

  for (const [, group] of shardGroups) {
    for (let idx = 1; idx <= group.total; idx += 1) {
      if (!group.indices.has(idx)) {
        errors.push(
          `missing shard ${group.shardKind}:${idx}/${group.total} for [${group.packages.join(', ')}]`,
        );
      }
    }
    if (!group.hasObjectUploader) {
      errors.push(`shard group for [${group.packages.join(', ')}] has no partition with upload_objects=true`);
    }
  }

  for (const [pkgName, pkg] of workspacePkgs) {
    if (excluded.has(pkgName)) continue;
    if (!seenPackages.has(pkgName)) {
      errors.push(`workspace package '${pkgName}' is missing from all partitions`);
      continue;
    }
    const expectedTargets = new Set();
    for (const t of pkg.targets ?? []) {
      const tk = targetKindForTestSelection(t);
      if (tk) expectedTargets.add(tk);
    }
    if (expectedTargets.size === 0) continue;
    const covered = seenTargetsByPkg.get(pkgName) ?? new Set();
    if (covered.size === 0) {
      errors.push(`workspace package '${pkgName}' has no testable targets covered by any partition`);
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

  const hasFeatureMetadata =
    workspacePkgs.size > 0 &&
    [...workspacePkgs.values()].every(
      (pkg) => pkg.features && typeof pkg.features === 'object' && Array.isArray(pkg.dependencies),
    );
  if (hasFeatureMetadata) {
    errors.push(...validateFeatureParity(config, metadata));
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
    const matrixMatches = [
      ...ciText.matchAll(
        /- partition:\s*([a-z0-9-]+)\s*\n\s*needs_postgres:\s*(true|false)\s*\n\s*seed_sched_template:\s*(true|false)\s*\n\s*needs_wasm:\s*(true|false)/g,
      ),
    ];
    if (matrixMatches.length === 0) {
      errors.push('could not find nextest-cov-part matrix entries in .github/workflows/ci.yml');
    } else {
      const ciMatrix = matrixMatches.map((m) => ({
        name: m[1],
        needs_postgres: m[2] === 'true',
        seed_sched_template: m[3] === 'true',
        needs_wasm: m[4] === 'true',
      }));
      const expectedMatrix = (config.partitions ?? []).map((p) => ({
        name: p.name,
        needs_postgres: Boolean(p.needs_postgres),
        seed_sched_template: Boolean(p.seed_sched_template),
        needs_wasm: Boolean(p.needs_wasm),
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
