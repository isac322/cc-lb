#!/usr/bin/env node
// Prunes workspace build artifacts, matching fingerprints, incremental caches,
// and coverage raw/report files from a Cargo target directory before caching,
// preserving compiled third-party dependencies.
import { execFileSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync, rmSync, statSync } from 'node:fs';
import path from 'node:path';

export function collectWorkspacePrefixes(metadata) {
  const workspaceIds = new Set(metadata.workspace_members ?? []);
  const pkgPrefixes = new Set();
  const targetPrefixes = new Set();

  for (const pkg of metadata.packages ?? []) {
    if (workspaceIds.size > 0 && !workspaceIds.has(pkg.id)) continue;
    pkgPrefixes.add(pkg.name);
    pkgPrefixes.add(pkg.name.replaceAll('-', '_'));
    for (const target of pkg.targets ?? []) {
      targetPrefixes.add(target.name);
      targetPrefixes.add(target.name.replaceAll('-', '_'));
    }
  }
  return { pkgPrefixes, targetPrefixes };
}

function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function buildHashMatcher(names, allowLibPrefix) {
  if (names.size === 0) return null;
  const alts = [...names].sort((a, b) => b.length - a.length).map(escapeRegex).join('|');
  const prefix = allowLibPrefix ? '(?:lib)?' : '';
  return new RegExp(`^${prefix}(?:${alts})-[0-9a-f]{16}(?:$|\\.)`);
}

function removeEntry(targetPath) {
  rmSync(targetPath, { recursive: true, force: true });
}

function removeCoverageFilesInDir(dir) {
  if (!existsSync(dir)) return 0;
  let removed = 0;
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isSymbolicLink() || entry.isFile()) {
      if (/\.(?:profraw|profdata|lcov)$/i.test(entry.name)) {
        removeEntry(fullPath);
        removed += 1;
      }
    } else if (entry.isDirectory()) {
      removed += removeCoverageFilesInDir(fullPath);
    }
  }
  return removed;
}

function isKeepableWorkspaceLib(entryName) {
  return (
    entryName.endsWith('.rlib') ||
    entryName.endsWith('.rmeta') ||
    entryName.endsWith('.so') ||
    entryName.endsWith('.dylib') ||
    entryName.endsWith('.a') ||
    entryName.endsWith('.d')
  );
}

function getMtimeMs(entryPath) {
  try {
    return statSync(entryPath).mtimeMs;
  } catch {
    return 0;
  }
}

function pruneIncrementalCrateDir(crateDirPath) {
  let removed = 0;
  const sessions = [];
  for (const entry of readdirSync(crateDirPath, { withFileTypes: true })) {
    const fullPath = path.join(crateDirPath, entry.name);
    if (entry.name.endsWith('.lock') || entry.name.endsWith('-working')) {
      removeEntry(fullPath);
      removed += 1;
    } else if (entry.isDirectory() && entry.name.startsWith('s-')) {
      sessions.push({ name: entry.name, fullPath, mtimeMs: getMtimeMs(fullPath) });
    }
  }
  sessions.sort((a, b) => b.mtimeMs - a.mtimeMs || b.name.localeCompare(a.name));
  for (const stale of sessions.slice(1)) {
    removeEntry(stale.fullPath);
    removed += 1;
  }
  return removed;
}

function pruneIncrementalDir(incrementalDir, allowedNames) {
  if (!existsSync(incrementalDir)) return 0;
  let removed = 0;
  const byCrate = new Map();

  for (const entry of readdirSync(incrementalDir, { withFileTypes: true })) {
    const fullPath = path.join(incrementalDir, entry.name);
    if (!entry.isDirectory() || entry.isSymbolicLink()) {
      removeEntry(fullPath);
      removed += 1;
      continue;
    }
    const match = entry.name.match(/^(.+)-([0-9a-zA-Z]+)$/);
    const crateName = match?.[1];
    if (!crateName || !allowedNames.has(crateName)) {
      removeEntry(fullPath);
      removed += 1;
      continue;
    }
    if (!byCrate.has(crateName)) byCrate.set(crateName, []);
    byCrate.get(crateName).push({ name: entry.name, fullPath, mtimeMs: getMtimeMs(fullPath) });
  }

  for (const candidates of byCrate.values()) {
    candidates.sort((a, b) => b.mtimeMs - a.mtimeMs || b.name.localeCompare(a.name));
    for (const stale of candidates.slice(1)) {
      removeEntry(stale.fullPath);
      removed += 1;
    }
    if (candidates[0]) {
      removed += pruneIncrementalCrateDir(candidates[0].fullPath);
    }
  }
  return removed;
}

function pruneProfileDir(profileDir, pkgPrefixes, targetPrefixes, options = {}) {
  const { keepWorkspaceLibs = false, keepLatestIncremental = false } = options;
  const stats = { removedEntries: 0 };
  const allNames = new Set([...pkgPrefixes, ...targetPrefixes]);
  const depMatcher = buildHashMatcher(allNames, true);
  const metaMatcher = buildHashMatcher(allNames, false);

  const incrementalDir = path.join(profileDir, 'incremental');
  if (existsSync(incrementalDir)) {
    if (keepLatestIncremental) {
      stats.removedEntries += pruneIncrementalDir(incrementalDir, allNames);
    } else {
      removeEntry(incrementalDir);
      stats.removedEntries += 1;
    }
  }

  for (const subdir of ['deps', 'examples']) {
    const dir = path.join(profileDir, subdir);
    if (!existsSync(dir) || !depMatcher) continue;
    for (const entry of readdirSync(dir)) {
      if (!depMatcher.test(entry)) continue;
      if (keepWorkspaceLibs && subdir === 'deps' && isKeepableWorkspaceLib(entry)) {
        continue;
      }
      removeEntry(path.join(dir, entry));
      stats.removedEntries += 1;
    }
  }

  if (!keepWorkspaceLibs) {
    const fingerprintDir = path.join(profileDir, '.fingerprint');
    if (existsSync(fingerprintDir) && metaMatcher) {
      for (const entry of readdirSync(fingerprintDir)) {
        if (metaMatcher.test(entry)) {
          removeEntry(path.join(fingerprintDir, entry));
          stats.removedEntries += 1;
        }
      }
    }

    const buildDir = path.join(profileDir, 'build');
    if (existsSync(buildDir)) {
      for (const entry of readdirSync(buildDir, { withFileTypes: true })) {
        const fullPath = path.join(buildDir, entry.name);
        // Supports both legacy `build/<pkg>-<hash>` and new Cargo `build/<pkg>/<hash>` layouts.
        if (pkgPrefixes.has(entry.name) || targetPrefixes.has(entry.name)) {
          removeEntry(fullPath);
          stats.removedEntries += 1;
        } else if (metaMatcher && metaMatcher.test(entry.name)) {
          removeEntry(fullPath);
          stats.removedEntries += 1;
        }
      }
    }
  }

  // Prune root-level workspace executables and their sidecars inside profileDir.
  for (const entry of readdirSync(profileDir, { withFileTypes: true })) {
    if (entry.isDirectory()) continue;
    const stem = entry.name.replace(/\.(?:d|pdb|dwp|exe|dll|so|dylib|rlib|rmeta)$/, '').replace(/^lib/, '');
    if (pkgPrefixes.has(stem) || targetPrefixes.has(stem) || (depMatcher && depMatcher.test(entry.name))) {
      removeEntry(path.join(profileDir, entry.name));
      stats.removedEntries += 1;
    }
  }

  return stats;
}

function isProfileDir(dir) {
  return (
    existsSync(path.join(dir, 'deps')) ||
    existsSync(path.join(dir, '.fingerprint')) ||
    existsSync(path.join(dir, 'build')) ||
    existsSync(path.join(dir, 'incremental'))
  );
}

export function pruneTargetDir(targetDir, metadata, options = {}) {
  if (!existsSync(targetDir)) return { removedEntries: 0 };
  const { pkgPrefixes, targetPrefixes } = collectWorkspacePrefixes(metadata);
  let removedEntries = removeCoverageFilesInDir(targetDir);

  const visited = new Set();
  if (isProfileDir(targetDir)) {
    const res = pruneProfileDir(targetDir, pkgPrefixes, targetPrefixes, options);
    return { removedEntries: removedEntries + res.removedEntries };
  }
  const queue = [targetDir];
  while (queue.length > 0) {
    const dir = queue.shift();
    if (visited.has(dir) || !existsSync(dir)) continue;
    visited.add(dir);

    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      if (!entry.isDirectory() || entry.isSymbolicLink()) continue;
      if (entry.name === 'wasm32-unknown-unknown') continue;
      const fullPath = path.join(dir, entry.name);
      if (isProfileDir(fullPath)) {
        const res = pruneProfileDir(fullPath, pkgPrefixes, targetPrefixes, options);
        removedEntries += res.removedEntries;
      } else if (dir === targetDir && !entry.name.startsWith('.')) {
        // Also inspect target/<triple>/{debug,release,...} and nested target roots (e.g. semver-checks/target).
        const st = statSync(fullPath);
        if (st.isDirectory()) queue.push(fullPath);
      }
    }
  }

  return { removedEntries };
}

function main() {
  const args = process.argv.slice(2);
  let targetDir = 'target';
  let metadataFile = null;
  let keepWorkspaceLibs = false;
  let keepLatestIncremental = false;
  for (let i = 0; i < args.length; i += 1) {
    if (args[i] === '--target-dir' && args[i + 1]) {
      targetDir = args[++i];
    } else if (args[i] === '--metadata-file' && args[i + 1]) {
      metadataFile = args[++i];
    } else if (args[i] === '--keep-workspace-libs') {
      keepWorkspaceLibs = true;
    } else if (args[i] === '--keep-latest-incremental') {
      keepLatestIncremental = true;
    } else {
      console.error(
        'Usage: node scripts/prune-rust-target.mjs --target-dir <dir> [--metadata-file <json>] [--keep-workspace-libs] [--keep-latest-incremental]',
      );
      process.exit(2);
    }
  }

  const rawMeta = metadataFile
    ? readFileSync(metadataFile, 'utf8')
    : execFileSync('cargo', ['metadata', '--no-deps', '--format-version', '1'], { encoding: 'utf8' });
  const metadata = JSON.parse(rawMeta);
  const { removedEntries } = pruneTargetDir(path.resolve(targetDir), metadata, {
    keepWorkspaceLibs,
    keepLatestIncremental,
  });
  console.log(
    `prune-rust-target: pruned ${removedEntries} workspace/scratch entries from ${targetDir} (keepWorkspaceLibs=${keepWorkspaceLibs}, keepLatestIncremental=${keepLatestIncremental})`,
  );
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
