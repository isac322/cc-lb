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

const SNAPSHOT_VERSION = 3;

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

function hashBufferSha256(buf) {
  return createHash('sha256').update(buf).digest('hex');
}

function hashFileSha256(fullPath) {
  return hashBufferSha256(readFileSync(fullPath));
}

function hashGitBlobFile(fullPath) {
  const buf = readFileSync(fullPath);
  return createHash('sha1').update(`blob ${buf.length}\0`).update(buf).digest('hex');
}

function normalizeRel(rel) {
  return rel.split(path.sep).join('/');
}

function walkExtraDirs(rootDir, extraDirs, visitFile) {
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
          visitFile(normalizeRel(path.relative(rootDir, full)), full);
        }
      }
    };
    const st = lstatSync(absExtra, { throwIfNoEntry: false });
    if (!st || st.isSymbolicLink()) continue;
    if (st.isDirectory()) {
      walk(absExtra);
    } else if (st.isFile()) {
      visitFile(normalizeRel(path.relative(rootDir, absExtra)), absExtra);
    }
  }
}

function listWorkspaceRelFilesV2(rootDir, extraDirs) {
  const relFiles = new Set();
  const out = execFileSync(
    'git',
    ['ls-files', '-z', '--cached', '--others', '--exclude-standard'],
    { cwd: rootDir, maxBuffer: 64 * 1024 * 1024 },
  );
  for (const raw of out.toString('utf8').split('\0')) {
    if (!raw) continue;
    relFiles.add(normalizeRel(raw));
  }
  walkExtraDirs(rootDir, extraDirs, (rel) => {
    relFiles.add(rel);
  });
  return relFiles;
}

function parseGitIndexStageOids(rootDir) {
  const indexOids = new Map();
  const out = execFileSync('git', ['ls-files', '--stage', '-z'], {
    cwd: rootDir,
    maxBuffer: 64 * 1024 * 1024,
  });
  for (const token of out.toString('utf8').split('\0')) {
    if (!token) continue;
    const tabIdx = token.indexOf('\t');
    if (tabIdx <= 0) continue;
    const [mode, oid, stage] = token.slice(0, tabIdx).split(' ');
    const relPath = normalizeRel(token.slice(tabIdx + 1));
    if (!relPath) continue;
    if ((mode === '100644' || mode === '100755') && stage === '0' && /^[0-9a-f]{40}$/.test(oid)) {
      indexOids.set(relPath, oid);
    }
  }
  return indexOids;
}

function parseGitDirtyPaths(rootDir) {
  const dirtyPaths = new Set();
  const out = execFileSync(
    'git',
    ['--no-optional-locks', 'status', '--porcelain=v1', '-z', '--untracked-files=all'],
    { cwd: rootDir, maxBuffer: 64 * 1024 * 1024 },
  );
  const tokens = out.toString('utf8').split('\0');
  for (let i = 0; i < tokens.length; i += 1) {
    const token = tokens[i];
    if (!token) continue;
    const x = token[0];
    const y = token[1];
    const rel1 = normalizeRel(token.slice(3));
    if (rel1) dirtyPaths.add(rel1);
    if (x === 'R' || x === 'C' || y === 'R' || y === 'C') {
      const next = tokens[i + 1];
      if (next !== undefined) {
        i += 1;
        const rel2 = normalizeRel(next);
        if (rel2) dirtyPaths.add(rel2);
      }
    }
  }
  return dirtyPaths;
}

function collectWorkspaceBlobOids(rootDir, extraDirs) {
  const indexOids = parseGitIndexStageOids(rootDir);
  const dirtyPaths = parseGitDirtyPaths(rootDir);
  const fileOids = new Map();

  for (const [relFile, oid] of indexOids) {
    if (!dirtyPaths.has(relFile)) {
      fileOids.set(relFile, oid);
    }
  }

  for (const relFile of dirtyPaths) {
    const full = path.join(rootDir, relFile);
    const st = lstatSync(full, { throwIfNoEntry: false });
    if (!st || !st.isFile() || st.isSymbolicLink()) continue;
    fileOids.set(relFile, hashGitBlobFile(full));
  }

  walkExtraDirs(rootDir, extraDirs, (relFile, full) => {
    if (!fileOids.has(relFile)) {
      fileOids.set(relFile, hashGitBlobFile(full));
    }
  });

  return fileOids;
}

function computeDirDigests(rootDir, relFiles, fileDigests) {
  const dirChildren = new Map();
  const ensureDir = (relDir) => {
    if (!dirChildren.has(relDir)) dirChildren.set(relDir, new Set());
    return dirChildren.get(relDir);
  };

  for (const relFile of relFiles) {
    let cur = path.posix.dirname(relFile);
    let childName = path.posix.basename(relFile);
    let childMarker = `f:${childName}:${fileDigests.get(relFile)}`;
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
    const digest = hashBufferSha256(Buffer.from([...markers].sort().join('\n')));
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
    .map((d) => normalizeRel(path.relative(resolvedRoot, path.resolve(resolvedRoot, d))))
    .filter((d) => d && !d.startsWith('..'));

  const blobOids = collectWorkspaceBlobOids(resolvedRoot, relExtraDirs);
  const files = {};
  const verifiedOids = new Map();

  for (const relFile of [...blobOids.keys()].sort()) {
    const full = path.join(resolvedRoot, relFile);
    const st = lstatSync(full, { bigint: true, throwIfNoEntry: false });
    if (!st || !st.isFile() || st.isSymbolicLink()) continue;
    const oid = blobOids.get(relFile);
    verifiedOids.set(relFile, oid);
    files[relFile] = { oid, mtimeNs: st.mtimeNs.toString() };
  }

  const dirMap = computeDirDigests(resolvedRoot, new Set(verifiedOids.keys()), verifiedOids);
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

function restoreDirs(resolvedRoot, recordedDirs, currentDirDigests, utimeOps) {
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
  return { restoredDirs, skippedDirs };
}

function restoreWorkspaceMtimesV2(resolvedRoot, snapshot) {
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
    const st = lstatSync(full, { throwIfNoEntry: false });
    if (!st || !st.isFile() || st.isSymbolicLink()) {
      skippedFiles += 1;
      continue;
    }
    const sha256 = hashFileSha256(full);
    hashedInPass1.set(relFile, sha256);
    if (sha256 !== meta.sha256) {
      skippedFiles += 1;
      continue;
    }
    utimeOps.push([full, meta.mtimeNs]);
    restoredFiles += 1;
  }

  const currentRelFiles = listWorkspaceRelFilesV2(resolvedRoot, snapshot.extraDirs ?? []);
  const currentHashes = new Map();
  for (const relFile of currentRelFiles) {
    const prevHash = hashedInPass1.get(relFile);
    if (prevHash !== undefined) {
      currentHashes.set(relFile, prevHash);
      continue;
    }
    const full = path.join(resolvedRoot, relFile);
    const st = lstatSync(full, { throwIfNoEntry: false });
    if (!st || !st.isFile() || st.isSymbolicLink()) continue;
    currentHashes.set(relFile, hashFileSha256(full));
  }
  const currentDirDigests = computeDirDigests(resolvedRoot, new Set(currentHashes.keys()), currentHashes);
  const { restoredDirs, skippedDirs } = restoreDirs(
    resolvedRoot,
    snapshot.dirs ?? {},
    currentDirDigests,
    utimeOps,
  );
  applyMtimesNs(utimeOps);
  return { restoredFiles, skippedFiles, restoredDirs, skippedDirs };
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
  if (!snapshot || typeof snapshot.files !== 'object' || snapshot.files === null) {
    return { restoredFiles: 0, skippedFiles: 0, restoredDirs: 0, skippedDirs: 0, corrupt: true };
  }

  const resolvedRoot = path.resolve(rootDir);
  if (snapshot.version === 2) {
    return restoreWorkspaceMtimesV2(resolvedRoot, snapshot);
  }
  if (snapshot.version !== SNAPSHOT_VERSION) {
    return { restoredFiles: 0, skippedFiles: 0, restoredDirs: 0, skippedDirs: 0, corrupt: true };
  }

  const recordedFiles = snapshot.files ?? {};
  const currentOids = collectWorkspaceBlobOids(resolvedRoot, snapshot.extraDirs ?? []);
  const utimeOps = [];
  let restoredFiles = 0;
  let skippedFiles = 0;

  for (const [relFile, meta] of Object.entries(recordedFiles)) {
    if (
      !meta ||
      typeof meta.oid !== 'string' ||
      typeof meta.mtimeNs !== 'string' ||
      !/^\d+$/.test(meta.mtimeNs)
    ) {
      skippedFiles += 1;
      continue;
    }
    const curOid = currentOids.get(relFile);
    if (curOid === undefined || curOid !== meta.oid) {
      skippedFiles += 1;
      continue;
    }
    const full = path.join(resolvedRoot, relFile);
    const st = lstatSync(full, { throwIfNoEntry: false });
    if (!st || !st.isFile() || st.isSymbolicLink()) {
      skippedFiles += 1;
      continue;
    }
    utimeOps.push([full, meta.mtimeNs]);
    restoredFiles += 1;
  }

  const currentDirDigests = computeDirDigests(resolvedRoot, new Set(currentOids.keys()), currentOids);
  const { restoredDirs, skippedDirs } = restoreDirs(
    resolvedRoot,
    snapshot.dirs ?? {},
    currentDirDigests,
    utimeOps,
  );
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
