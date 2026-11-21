#!/usr/bin/env node
// Extracts partition coverage archives (part-*.profdata + stripped objects),
// merges profile counters once with llvm-profdata, and exports merged LCOV
// via llvm-cov export using cargo-llvm-cov's exact filename ignore regex.
import { execFileSync, spawn } from 'node:child_process';
import {
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readdirSync,
  readSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');

export function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function normalizePosix(p) {
  return p.split(path.sep).join('/');
}

export function buildIgnoreFilenameRegex(rootDir, cargoHome = '', rustupHome = '') {
  const rootEsc = escapeRegex(normalizePosix(path.resolve(rootDir)));
  const patterns = [
    '/rustc/([0-9a-f]+|[0-9]+\\.[0-9]+\\.[0-9]+)/',
    `^${rootEsc}(/.*)?/(tests|examples|benches)/`,
    `^${rootEsc}(/.*)?/(tests\\.rs|[0-9a-zA-Z_-]+[_-]tests\\.rs)$`,
    `^${rootEsc}/target($|/)`,
  ];
  if (cargoHome) {
    patterns.push(`^${escapeRegex(normalizePosix(path.resolve(cargoHome)))}/(registry|git)/`);
  }
  if (rustupHome) {
    patterns.push(`^${escapeRegex(normalizePosix(path.resolve(rustupHome)))}/toolchains($|/)`);
  }
  patterns.push(
    `^${rootEsc}/(tests/load|tests/stress-suite|tools/workspace-hack)($|/)`,
    `^${rootEsc}/(fuzz|third_party)/`,
    '^(tests/|fuzz/|examples/|third_party/|tests/fixtures/)',
  );
  return patterns.join('|');
}

function resolveLlvmTools() {
  const resolveExplicit = (envName) => {
    const candidate = process.env[envName];
    if (candidate && existsSync(candidate)) return candidate;
    return null;
  };
  const envProfdata = resolveExplicit('LLVM_PROFDATA');
  const envCov = resolveExplicit('LLVM_COV');
  if (envProfdata && envCov) {
    return { profdata: envProfdata, cov: envCov };
  }

  const sysroot = execFileSync('rustc', ['--print', 'sysroot'], { cwd: ROOT, encoding: 'utf8' }).trim();
  const vv = execFileSync('rustc', ['-vV'], { cwd: ROOT, encoding: 'utf8' });
  const hostMatch = /^host:\s*(\S+)/m.exec(vv);
  if (!hostMatch) {
    throw new Error(`Unable to determine rustc host triple from:\n${vv}`);
  }
  const llvmBinDir = path.join(sysroot, 'lib', 'rustlib', hostMatch[1], 'bin');
  const resolveTool = (envName, binName) => {
    const candidate = process.env[envName] || path.join(llvmBinDir, binName);
    if (!existsSync(candidate)) {
      throw new Error(`Required LLVM tool '${binName}' not found at '${candidate}' (install llvm-tools-preview)`);
    }
    return candidate;
  };
  return {
    profdata: resolveTool('LLVM_PROFDATA', 'llvm-profdata'),
    cov: resolveTool('LLVM_COV', 'llvm-cov'),
  };
}

function findArchives(partsDir) {
  const found = [];
  const walk = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
      } else if (entry.isFile() && entry.name.endsWith('.tar.zst')) {
        found.push(full);
      }
    }
  };
  if (existsSync(partsDir)) walk(partsDir);
  return found.sort();
}

function extractArchive(archivePath, destDir) {
  mkdirSync(destDir, { recursive: true });
  return new Promise((resolve, reject) => {
    const zstd = spawn('zstd', ['-dc', archivePath], {
      cwd: ROOT,
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    const sparseFlag = process.platform === 'linux' ? '--sparse' : '-S';
    const tar = spawn('tar', [sparseFlag, '-C', destDir, '-xf', '-'], {
      cwd: ROOT,
      stdio: ['pipe', 'inherit', 'inherit'],
    });
    zstd.stdout.pipe(tar.stdin);
    zstd.on('error', (err) => {
      tar.kill();
      reject(err);
    });
    tar.on('error', reject);
    let zstdCode = null;
    let tarCode = null;
    const done = () => {
      if (zstdCode === null || tarCode === null) return;
      if (zstdCode !== 0) return reject(new Error(`zstd -dc ${archivePath} exited with ${zstdCode}`));
      if (tarCode !== 0) return reject(new Error(`tar -xf for ${archivePath} exited with ${tarCode}`));
      resolve();
    };
    zstd.on('close', (code) => {
      zstdCode = code ?? 1;
      done();
    });
    tar.on('close', (code) => {
      tarCode = code ?? 1;
      done();
    });
  });
}

export function collectExtractedArtifacts(extractRoot) {
  const profdataFiles = [];
  const objsByName = new Map();
  if (!existsSync(extractRoot)) {
    return { profdataFiles, objectFiles: [] };
  }
  const partDirs = readdirSync(extractRoot, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => d.name)
    .sort();
  for (const partDirName of partDirs) {
    const partDir = path.join(extractRoot, partDirName);
    for (const entry of readdirSync(partDir, { withFileTypes: true })) {
      if (entry.isFile() && entry.name.endsWith('.profdata')) {
        profdataFiles.push(path.join(partDir, entry.name));
      }
    }
    const objsDir = path.join(partDir, 'objs');
    if (existsSync(objsDir)) {
      for (const entry of readdirSync(objsDir, { withFileTypes: true })) {
        if (entry.isFile() && !objsByName.has(entry.name)) {
          objsByName.set(entry.name, path.join(objsDir, entry.name));
        }
      }
    }
  }
  const objectFiles = [...objsByName.entries()]
    .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
    .map(([, full]) => full);
  return { profdataFiles: profdataFiles.sort(), objectFiles };
}

function fileHasSfRecord(filePath) {
  const fd = openSync(filePath, 'r');
  try {
    const buf = Buffer.alloc(65536);
    const bytesRead = readSync(fd, buf, 0, buf.length, 0);
    return buf.subarray(0, bytesRead).toString('utf8').includes('SF:');
  } finally {
    closeSync(fd);
  }
}

async function main() {
  const partsDir = process.argv[2];
  const outLcov = process.argv[3];
  if (!partsDir || !outLcov) {
    console.error('Usage: node scripts/export-merged-lcov.mjs <parts-dir> <out-lcov>');
    process.exit(2);
  }

  const archives = findArchives(path.resolve(partsDir));
  if (archives.length === 0) {
    console.error(`export-merged-lcov: no .tar.zst archives found under ${partsDir}`);
    process.exit(1);
  }

  const tools = resolveLlvmTools();
  const shmBase = existsSync('/dev/shm') ? '/dev/shm' : os.tmpdir();
  const extractRoot = path.join(shmBase, `cc-lb-cov-merge-${process.pid}`);
  rmSync(extractRoot, { recursive: true, force: true });
  mkdirSync(extractRoot, { recursive: true });

  try {
    await Promise.all(
      archives.map((archive, idx) =>
        extractArchive(archive, path.join(extractRoot, `${String(idx).padStart(2, '0')}-${path.basename(archive, '.tar.zst')}`)),
      ),
    );

    const { profdataFiles, objectFiles } = collectExtractedArtifacts(extractRoot);
    if (profdataFiles.length === 0) {
      throw new Error(`No .profdata files extracted from ${archives.length} archives`);
    }
    if (objectFiles.length === 0) {
      throw new Error(`No coverage object files extracted from ${archives.length} archives`);
    }

    const absOut = path.resolve(outLcov);
    mkdirSync(path.dirname(absOut), { recursive: true });
    const mergedProfdata = path.join(extractRoot, 'merged.profdata');
    const profListFile = path.join(extractRoot, 'profdata-list.txt');
    writeFileSync(profListFile, `${profdataFiles.join('\n')}\n`, 'utf8');
    execFileSync(tools.profdata, ['merge', '-sparse', '-f', profListFile, '-o', mergedProfdata], {
      cwd: ROOT,
      stdio: 'inherit',
    });

    const cargoHome = process.env.CARGO_HOME || path.join(os.homedir(), '.cargo');
    const rustupHome = process.env.RUSTUP_HOME || path.join(os.homedir(), '.rustup');
    const ignoreRegex = buildIgnoreFilenameRegex(ROOT, cargoHome, rustupHome);

    const covArgs = [
      'export',
      '-format=lcov',
      '-num-threads=4',
      `-instr-profile=${mergedProfdata}`,
      `-ignore-filename-regex=${ignoreRegex}`,
    ];
    for (const obj of objectFiles) {
      covArgs.push('-object', obj);
    }

    const outFd = openSync(absOut, 'w');
    try {
      execFileSync(tools.cov, covArgs, {
        cwd: ROOT,
        stdio: ['ignore', outFd, 'inherit'],
      });
    } finally {
      closeSync(outFd);
    }

    const st = statSync(absOut, { throwIfNoEntry: false });
    if (!st || st.size === 0 || !fileHasSfRecord(absOut)) {
      throw new Error(`Exported LCOV '${absOut}' is empty or missing SF records`);
    }
    console.log(
      `export-merged-lcov: merged ${profdataFiles.length} profdata + ${objectFiles.length} unique objects -> ${outLcov} (${(st.size / (1024 * 1024)).toFixed(2)} MiB)`,
    );
  } finally {
    rmSync(extractRoot, { recursive: true, force: true });
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await main();
}
