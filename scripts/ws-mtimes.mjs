#!/usr/bin/env node
// Records and restores content-verified file and directory mtimes with exact
// nanosecond precision so Cargo keeps unchanged workspace artifacts Fresh across
// fresh GitHub Actions checkouts.
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import path from 'node:path';

const SNAPSHOT_VERSION = 2;

const PYTHON_SET_MTIMES_NS = `
import json, os, sys
if sys.platform == "linux":
    import resource
    limit = 256 * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
for target_path, ns_str in json.load(sys.stdin):
    ns = int(ns_str)
    os.utime(target_path, ns=(ns, ns))
`;

function hashBuffer(buf) {
  return createHash('sha256').update(buf).digest('hex');
}

function hashFile(fullPath) {
  return hashBuffer(readFileSync(fullPath));
}

function listWorkspaceRelFiles(rootDir, extraDirs) {
  const relFiles = new Set();
  const out = execFileSync(
    'git',
    ['ls-files', '-z', '--cached', '--others', '--exclude-standard'],
    { cwd: rootDir, maxBuffer: 64 * 1024 * 1024 },
  );
  for (const raw of out.toString('utf8').split('\0')) {
    if (!raw) continue;
    relFiles.add(raw.split(path.sep).join('/'));
  }

  for (const extra of extraDirs) {
    const absExtra = path.resolve(rootDir, extra);
    if (!existsSync(absExtra)) continue;
    const walk = (absDir) => {
      for (const entry of readdirSync(absDir, { withFileTypes: true })) {
        const full = path.join(absDir, entry.name);
        if (entry.isSymbolicLink()) continue;
        if (entry.isDirectory()) {
          walk(full);
        } else if (entry.isFile()) {
          relFiles.add(path.relative(rootDir, full).split(path.sep).join('/'));
        }
      }
    };
    const st = lstatSync(absExtra);
    if (st.isDirectory()) {
      walk(absExtra);
    } else if (st.isFile()) {
      relFiles.add(path.relative(rootDir, absExtra).split(path.sep).join('/'));
    }
  }
  return relFiles;
}

function computeDirDigests(rootDir, relFiles, fileHashes) {
  const dirChildren = new Map();
  const ensureDir = (relDir) => {
    if (!dirChildren.has(relDir)) dirChildren.set(relDir, new Set());
    return dirChildren.get(relDir);
  };

  for (const relFile of relFiles) {
    let cur = path.posix.dirname(relFile);
    let childName = path.posix.basename(relFile);
    let childMarker = `f:${childName}:${fileHashes.get(relFile)}`;
    while (cur && cur !== '.') {
      ensureDir(cur).add(childMarker);
      childName = path.posix.basename(cur);
      cur = path.posix.dirname(cur);
      childMarker = `d:${childName}`;
    }
  }

  const dirs = new Map();
  for (const [relDir, markers] of dirChildren) {
    const absDir = path.join(rootDir, relDir);
    if (!existsSync(absDir)) continue;
    const st = lstatSync(absDir, { bigint: true });
    if (!st.isDirectory()) continue;
    const digest = hashBuffer(Buffer.from([...markers].sort().join('\n')));
    dirs.set(relDir, { digest, mtimeNs: st.mtimeNs.toString() });
  }
  return dirs;
}

function applyMtimesNs(utimeOps) {
  if (utimeOps.length === 0) return;
  try {
    execFileSync('python3', ['-c', PYTHON_SET_MTIMES_NS], {
      input: JSON.stringify(utimeOps),
      encoding: 'utf8',
      maxBuffer: 16 * 1024 * 1024,
      stdio: ['pipe', 'inherit', 'inherit'],
    });
    return;
  } catch (err) {
    if (err?.code !== 'ENOENT') {
      throw err;
    }
  }
  for (const [targetPath, nsStr] of utimeOps) {
    const ns = BigInt(nsStr);
    const sec = Number(ns / 1_000_000_000n) + Number(ns % 1_000_000_000n) / 1e9;
    utimesSync(targetPath, sec, sec);
  }
}

export function recordWorkspaceMtimes(rootDir, outPath, extraDirs = []) {
  const resolvedRoot = path.resolve(rootDir);
  const relExtraDirs = extraDirs
    .map((d) => path.relative(resolvedRoot, path.resolve(resolvedRoot, d)).split(path.sep).join('/'))
    .filter((d) => d && !d.startsWith('..'));

  const relFiles = listWorkspaceRelFiles(resolvedRoot, relExtraDirs);
  const files = {};
  const fileHashes = new Map();

  for (const relFile of [...relFiles].sort()) {
    const full = path.join(resolvedRoot, relFile);
    if (!existsSync(full)) continue;
    const st = lstatSync(full, { bigint: true });
    if (!st.isFile() || st.isSymbolicLink()) continue;
    const sha256 = hashFile(full);
    fileHashes.set(relFile, sha256);
    files[relFile] = { sha256, mtimeNs: st.mtimeNs.toString() };
  }

  const dirMap = computeDirDigests(resolvedRoot, new Set(fileHashes.keys()), fileHashes);
  const dirs = {};
  for (const relDir of [...dirMap.keys()].sort()) {
    dirs[relDir] = dirMap.get(relDir);
  }

  const snapshot = {
    version: SNAPSHOT_VERSION,
    extraDirs: relExtraDirs,
    files,
    dirs,
  };
  mkdirSync(path.dirname(path.resolve(outPath)), { recursive: true });
  writeFileSync(outPath, JSON.stringify(snapshot), 'utf8');
  return { fileCount: Object.keys(files).length, dirCount: Object.keys(dirs).length };
}

export function restoreWorkspaceMtimes(rootDir, inPath) {
  if (!existsSync(inPath)) {
    return { restoredFiles: 0, skippedFiles: 0, restoredDirs: 0, skippedDirs: 0, missing: true };
  }
  let snapshot;
  try {
    snapshot = JSON.parse(readFileSync(inPath, 'utf8'));
  } catch {
    return { restoredFiles: 0, skippedFiles: 0, restoredDirs: 0, skippedDirs: 0, corrupt: true };
  }
  if (!snapshot || snapshot.version !== SNAPSHOT_VERSION || typeof snapshot.files !== 'object') {
    return { restoredFiles: 0, skippedFiles: 0, restoredDirs: 0, skippedDirs: 0, corrupt: true };
  }

  const resolvedRoot = path.resolve(rootDir);
  const recordedFiles = snapshot.files ?? {};
  const hashedInPass1 = new Map();
  const utimeOps = [];
  let restoredFiles = 0;
  let skippedFiles = 0;

  for (const [relFile, meta] of Object.entries(recordedFiles)) {
    if (!meta || typeof meta.mtimeNs !== 'string' || !/^\d+$/.test(meta.mtimeNs)) {
      skippedFiles += 1;
      continue;
    }
    const full = path.join(resolvedRoot, relFile);
    if (!existsSync(full)) {
      skippedFiles += 1;
      continue;
    }
    const st = lstatSync(full);
    if (!st.isFile() || st.isSymbolicLink()) {
      skippedFiles += 1;
      continue;
    }
    const sha256 = hashFile(full);
    hashedInPass1.set(relFile, sha256);
    if (sha256 !== meta.sha256) {
      skippedFiles += 1;
      continue;
    }
    utimeOps.push([full, meta.mtimeNs]);
    restoredFiles += 1;
  }

  // Re-scan current non-ignored + extraDirs files using cached Pass 1 hashes
  // to avoid reading unchanged files twice while detecting added/deleted entries.
  const currentRelFiles = listWorkspaceRelFiles(resolvedRoot, snapshot.extraDirs ?? []);
  const currentHashes = new Map();
  for (const relFile of currentRelFiles) {
    const prevHash = hashedInPass1.get(relFile);
    if (prevHash !== undefined) {
      currentHashes.set(relFile, prevHash);
      continue;
    }
    const full = path.join(resolvedRoot, relFile);
    if (!existsSync(full)) continue;
    const st = lstatSync(full, { throwIfNoEntry: false });
    if (!st || !st.isFile() || st.isSymbolicLink()) continue;
    currentHashes.set(relFile, hashFile(full));
  }
  const currentDirDigests = computeDirDigests(resolvedRoot, new Set(currentHashes.keys()), currentHashes);

  const recordedDirs = snapshot.dirs ?? {};
  // Restore bottom-up (deepest path first) after all files so child touches
  // cannot reset parent dir mtimes.
  const sortedDirs = Object.keys(recordedDirs).sort((a, b) => {
    const depthDiff = b.split('/').length - a.split('/').length;
    return depthDiff !== 0 ? depthDiff : a.localeCompare(b);
  });

  let restoredDirs = 0;
  let skippedDirs = 0;
  for (const relDir of sortedDirs) {
    const rec = recordedDirs[relDir];
    const cur = currentDirDigests.get(relDir);
    const fullDir = path.join(resolvedRoot, relDir);
    if (
      !rec ||
      typeof rec.mtimeNs !== 'string' ||
      !/^\d+$/.test(rec.mtimeNs) ||
      !cur ||
      cur.digest !== rec.digest ||
      !existsSync(fullDir)
    ) {
      skippedDirs += 1;
      continue;
    }
    utimeOps.push([fullDir, rec.mtimeNs]);
    restoredDirs += 1;
  }

  applyMtimesNs(utimeOps);

  return { restoredFiles, skippedFiles, restoredDirs, skippedDirs };
}

function main() {
  const args = process.argv.slice(2);
  const cmd = args[0];
  let root = process.cwd();
  let filePath = null;
  const extraDirs = [];

  for (let i = 1; i < args.length; i += 1) {
    if ((args[i] === '--out' || args[i] === '--in') && args[i + 1]) {
      filePath = args[++i];
    } else if (args[i] === '--root' && args[i + 1]) {
      root = args[++i];
    } else if (args[i] === '--extra-dir' && args[i + 1]) {
      extraDirs.push(args[++i]);
    } else {
      console.error('Usage: node scripts/ws-mtimes.mjs <record --out file [--extra-dir d] | restore --in file>');
      process.exit(2);
    }
  }

  if (!filePath || (cmd !== 'record' && cmd !== 'restore')) {
    console.error('Usage: node scripts/ws-mtimes.mjs <record --out file [--extra-dir d] | restore --in file>');
    process.exit(2);
  }

  if (cmd === 'record') {
    const res = recordWorkspaceMtimes(root, filePath, extraDirs);
    console.log(`ws-mtimes: recorded ${res.fileCount} files and ${res.dirCount} dirs -> ${filePath}`);
  } else {
    const res = restoreWorkspaceMtimes(root, filePath);
    if (res.missing || res.corrupt) {
      console.log(`ws-mtimes: snapshot unavailable (${res.missing ? 'missing' : 'corrupt'}), skipped restore`);
    } else {
      console.log(
        `ws-mtimes: restored ${res.restoredFiles} files (${res.skippedFiles} changed/new), ${res.restoredDirs} dirs (${res.skippedDirs} changed/new)`,
      );
    }
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
