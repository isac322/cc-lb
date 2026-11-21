#!/usr/bin/env node
// Runs one coverage partition from .github/ci/cov-partitions.json, merges sparse
// profraw data into part-<name>.profdata, strips required coverage sections from
// built binaries, and archives them to target/cov-part-<name>.tar.zst.
import { execFile, execFileSync, spawn } from 'node:child_process';
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');

function resolveLlvmTools() {
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
    objcopy: resolveTool('LLVM_OBJCOPY', 'llvm-objcopy'),
    strip: resolveTool('LLVM_STRIP', 'llvm-strip'),
    cov: resolveTool('LLVM_COV', 'llvm-cov'),
  };
}

function collectProfrawFiles(dirs) {
  const found = new Set();
  for (const dir of dirs) {
    if (!existsSync(dir)) continue;
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isFile() && entry.name.endsWith('.profraw')) {
        const st = statSync(full);
        if (st.size > 0) found.add(full);
      }
    }
  }
  return [...found].sort();
}

function removeProfrawFiles(dirs) {
  for (const dir of dirs) {
    if (!existsSync(dir)) continue;
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      if ((entry.isFile() || entry.isSymbolicLink()) && entry.name.endsWith('.profraw')) {
        rmSync(path.join(dir, entry.name), { force: true });
      }
    }
  }
}

function parseShowEnv(stdout) {
  const parsed = {};
  for (const rawLine of stdout.split(/\r?\n/)) {
    let line = rawLine.trim();
    if (!line || line.startsWith('info:') || line.startsWith('warning:')) continue;
    if (line.startsWith('export ')) {
      line = line.slice('export '.length);
    }
    const eq = line.indexOf('=');
    if (eq === -1) continue;
    const key = line.slice(0, eq);
    if (!/^[A-Z0-9_]+$/.test(key)) continue;
    let val = line.slice(eq + 1);
    if (val.startsWith("'") && val.endsWith("'") && val.length >= 2) {
      val = val.slice(1, -1).replaceAll("'\"'\"'", "'");
    }
    parsed[key] = val;
  }
  return parsed;
}

function collectBinaryPaths(nextestJson) {
  const paths = new Set();
  for (const bin of Object.values(nextestJson['rust-binaries'] ?? {})) {
    if (bin && typeof bin['binary-path'] === 'string') {
      paths.add(bin['binary-path']);
    }
  }
  const meta = nextestJson['rust-build-meta'] ?? {};
  const targetDir = meta['target-directory'];
  if (typeof targetDir === 'string') {
    for (const list of Object.values(meta['non-test-binaries'] ?? {})) {
      if (!Array.isArray(list)) continue;
      for (const entry of list) {
        if (entry && typeof entry.path === 'string') {
          paths.add(path.join(targetDir, entry.path));
        }
      }
    }
  }
  return [...paths].filter((p) => {
    const st = statSync(p, { throwIfNoEntry: false });
    return st && st.isFile();
  });
}

async function mapLimit(items, limit, fn) {
  let idx = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (true) {
      const current = idx;
      idx += 1;
      if (current >= items.length) break;
      await fn(items[current], current);
    }
  });
  await Promise.all(workers);
}

function runPipeTarZstd(stageDir, archivePath) {
  return new Promise((resolve, reject) => {
    const sparseFlag = process.platform === 'linux' ? '--sparse' : '-S';
    const tar = spawn('tar', [sparseFlag, '-C', stageDir, '-cf', '-', '.'], {
      cwd: ROOT,
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    const zstd = spawn('zstd', ['-3', '-T0', '-f', '-o', archivePath], {
      cwd: ROOT,
      stdio: ['pipe', 'inherit', 'inherit'],
    });
    tar.stdout.pipe(zstd.stdin);
    let tarErr = null;
    tar.on('error', (err) => {
      tarErr = err;
      zstd.kill();
    });
    zstd.on('error', reject);
    let tarCode = null;
    let zstdCode = null;
    const finish = () => {
      if (tarCode === null || zstdCode === null) return;
      if (tarErr) return reject(tarErr);
      if (tarCode !== 0) return reject(new Error(`tar exited with code ${tarCode}`));
      if (zstdCode !== 0) return reject(new Error(`zstd exited with code ${zstdCode}`));
      resolve();
    };
    tar.on('close', (code) => {
      tarCode = code ?? 1;
      finish();
    });
    zstd.on('close', (code) => {
      zstdCode = code ?? 1;
      finish();
    });
  });
}

async function stripObjects(binPaths, objsDir, profdataPath, tools) {
  mkdirSync(objsDir, { recursive: true });
  const usedNames = new Map();
  const mapped = binPaths.map((binPath) => {
    const base = path.basename(binPath);
    const count = usedNames.get(base) ?? 0;
    usedNames.set(base, count + 1);
    const outName = count === 0 ? base : `${base}-${count}`;
    return { binPath, outPath: path.join(objsDir, outName) };
  });

  const stripWithObjcopy = async ({ binPath, outPath }) => {
    await execFileAsync(tools.objcopy, [
      '--strip-all',
      '--only-section=__llvm_covmap',
      '--only-section=__llvm_covfun',
      '--only-section=__llvm_prf_names',
      binPath,
      outPath,
    ]);
    chmodSync(outPath, 0o755);
  };

  const stripWithLlvmStrip = async ({ binPath, outPath }) => {
    await execFileAsync(tools.strip, ['--strip-all', binPath, '-o', outPath]);
    chmodSync(outPath, 0o755);
  };

  let stripFn = stripWithObjcopy;
  if (mapped.length > 0) {
    let smokeOk = false;
    try {
      await stripWithObjcopy(mapped[0]);
      await execFileAsync(tools.cov, [
        'export',
        '--summary-only',
        `-instr-profile=${profdataPath}`,
        '-object',
        mapped[0].outPath,
      ]);
      smokeOk = true;
    } catch (err) {
      console.warn(
        `run-cov-partition: llvm-objcopy --only-section failed llvm-cov validation (${err.message}); falling back to llvm-strip --strip-all`,
      );
      stripFn = stripWithLlvmStrip;
      await stripWithLlvmStrip(mapped[0]);
      await execFileAsync(tools.cov, [
        'export',
        '--summary-only',
        `-instr-profile=${profdataPath}`,
        '-object',
        mapped[0].outPath,
      ]);
      smokeOk = true;
    }
    if (!smokeOk) {
      throw new Error('Failed to validate stripped coverage object');
    }
  }

  if (mapped.length > 1) {
    await mapLimit(mapped.slice(1), 4, stripFn);
  }
  return mapped.length;
}

async function main() {
  const partName = process.argv[2];
  if (!partName) {
    console.error('Usage: node scripts/run-cov-partition.mjs <partition-name>');
    process.exit(2);
  }

  const cfgPath = path.join(ROOT, '.github/ci/cov-partitions.json');
  const cfg = JSON.parse(readFileSync(cfgPath, 'utf8'));
  const part = (cfg.partitions ?? []).find((p) => p.name === partName);
  if (!part) {
    console.error(`run-cov-partition: unknown partition '${partName}'`);
    process.exit(2);
  }
  if ((part.runs ?? []).length !== 1) {
    console.error(`run-cov-partition: partition '${partName}' must define exactly 1 run`);
    process.exit(2);
  }
  const run = part.runs[0];
  const tools = resolveLlvmTools();

  const shmBase = existsSync('/dev/shm') ? '/dev/shm' : os.tmpdir();
  const shmTmp = path.join(shmBase, 'cc-lb-tmp');
  const profrawDir = path.join(shmBase, `cc-lb-profraw-${partName}`);
  mkdirSync(shmTmp, { recursive: true });
  rmSync(profrawDir, { recursive: true, force: true });
  mkdirSync(profrawDir, { recursive: true });

  const targetDir = path.join(ROOT, 'target');
  const llvmTargetDir = path.join(targetDir, 'llvm-cov-target');
  mkdirSync(targetDir, { recursive: true });
  mkdirSync(llvmTargetDir, { recursive: true });

  const stageDir = path.join(shmBase, `cc-lb-cov-stage-${partName}`);
  rmSync(stageDir, { recursive: true, force: true });
  mkdirSync(stageDir, { recursive: true });

  const baseEnv = {
    ...process.env,
    TMPDIR: shmTmp,
    LLVM_COV: tools.cov,
    LLVM_PROFDATA: tools.profdata,
  };

  removeProfrawFiles([profrawDir, targetDir, llvmTargetDir]);

  const pkgArgs = [];
  for (const pkg of run.packages ?? []) {
    pkgArgs.push('--package', pkg);
  }
  const cargoArgs = run.cargo_args ?? [];
  const nextestArgs = [
    'nextest',
    'run',
    ...pkgArgs,
    ...cargoArgs,
    '--all-features',
    '--test-threads=4',
  ];
  if (run.filter_expr) {
    nextestArgs.push('-E', run.filter_expr);
  }
  for (const narg of run.nextest_args ?? []) {
    nextestArgs.push(narg);
  }

  try {
    const showEnvOut = execFileSync('cargo', ['llvm-cov', 'show-env', '--sh'], {
      cwd: ROOT,
      env: baseEnv,
      encoding: 'utf8',
    });
    const covEnv = {
      ...baseEnv,
      ...parseShowEnv(showEnvOut),
      CARGO_TARGET_DIR: llvmTargetDir,
      LLVM_PROFILE_FILE: path.join(profrawDir, `part-${partName}-%4m.profraw`),
    };

    console.log(`::group::cargo ${nextestArgs.join(' ')}`);
    try {
      execFileSync('cargo', nextestArgs, { cwd: ROOT, env: covEnv, stdio: 'inherit' });
    } finally {
      console.log('::endgroup::');
    }

    const profrawFiles = collectProfrawFiles([profrawDir, targetDir, llvmTargetDir]);
    if (profrawFiles.length === 0) {
      throw new Error(`No non-empty .profraw files produced for partition '${partName}'`);
    }

    const profdataPath = path.join(stageDir, `part-${partName}.profdata`);
    const profrawListPath = path.join(stageDir, 'profraw-files.txt');
    writeFileSync(profrawListPath, `${profrawFiles.join('\n')}\n`, 'utf8');
    execFileSync(tools.profdata, ['merge', '-sparse', '-f', profrawListPath, '-o', profdataPath], {
      cwd: ROOT,
      stdio: 'inherit',
    });
    rmSync(profrawListPath, { force: true });
    removeProfrawFiles([profrawDir, targetDir, llvmTargetDir]);

    let objCount = 0;
    if (part.upload_objects !== false) {
      const listArgs = ['nextest', 'list', '--list-type', 'binaries-only', '--message-format', 'json', ...pkgArgs, ...cargoArgs, '--all-features'];
      const listStdout = execFileSync('cargo', listArgs, {
        cwd: ROOT,
        env: covEnv,
        encoding: 'utf8',
        maxBuffer: 64 * 1024 * 1024,
      });
      const binariesJson = JSON.parse(listStdout);
      const binPaths = collectBinaryPaths(binariesJson);
      if (binPaths.length === 0) {
        throw new Error(`No built binaries discovered for partition '${partName}'`);
      }
      objCount = await stripObjects(binPaths, path.join(stageDir, 'objs'), profdataPath, tools);
    }

    const archivePath = path.join(targetDir, `cov-part-${partName}.tar.zst`);
    await runPipeTarZstd(stageDir, archivePath);
    const archiveSizeMb = (statSync(archivePath).size / (1024 * 1024)).toFixed(2);
    console.log(
      `run-cov-partition: wrote ${archivePath} (${archiveSizeMb} MiB, ${profrawFiles.length} profraw -> 1 profdata, ${objCount} stripped objects)`,
    );
  } finally {
    rmSync(stageDir, { recursive: true, force: true });
    rmSync(profrawDir, { recursive: true, force: true });
  }
}

await main();
