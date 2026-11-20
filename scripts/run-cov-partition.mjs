#!/usr/bin/env node
// Runs one coverage partition from .github/ci/cov-partitions.json and exports target/partition-<name>.lcov.
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { mergeLcovTexts } from './merge-lcov.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const IGNORE_REGEX = '^(tests/|fuzz/|examples/|third_party/|tests/fixtures/)';

function cleanProfileArtifacts(llvmTargetDir) {
  if (!existsSync(llvmTargetDir)) return;
  for (const name of readdirSync(llvmTargetDir)) {
    if (name.endsWith('.profraw') || name.endsWith('.profdata')) {
      rmSync(path.join(llvmTargetDir, name), { force: true });
    }
  }
}

function main() {
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

  const shmTmp = '/dev/shm/cc-lb-tmp';
  mkdirSync(shmTmp, { recursive: true });
  const targetDir = path.join(ROOT, 'target');
  const llvmTargetDir = path.join(targetDir, 'llvm-cov-target');
  mkdirSync(llvmTargetDir, { recursive: true });

  const env = {
    ...process.env,
    TMPDIR: shmTmp,
  };

  const partLcovs = [];
  for (let idx = 0; idx < part.runs.length; idx += 1) {
    const run = part.runs[idx];
    cleanProfileArtifacts(llvmTargetDir);

    const outLcov =
      part.runs.length === 1
        ? path.join(targetDir, `partition-${partName}.lcov`)
        : path.join(targetDir, `part-${partName}-${idx}.lcov`);

    const args = ['llvm-cov', 'nextest', '--no-clean'];
    for (const pkg of run.packages ?? []) {
      args.push('--package', pkg);
    }
    for (const carg of run.cargo_args ?? []) {
      args.push(carg);
    }
    args.push('--all-features', '--test-threads=4');
    if (run.filter_expr) {
      args.push('-E', run.filter_expr);
    }
    args.push('--lcov', '--output-path', outLcov, '--ignore-filename-regex', IGNORE_REGEX);

    console.log(`::group::cargo ${args.join(' ')}`);
    try {
      execFileSync('cargo', args, { cwd: ROOT, env, stdio: 'inherit' });
    } finally {
      console.log('::endgroup::');
    }
    partLcovs.push(readFileSync(outLcov, 'utf8'));
    cleanProfileArtifacts(llvmTargetDir);
  }

  const finalPath = path.join(targetDir, `partition-${partName}.lcov`);
  if (partLcovs.length > 1) {
    const merged = mergeLcovTexts(partLcovs);
    writeFileSync(finalPath, merged, 'utf8');
  }
  console.log(`run-cov-partition: wrote ${finalPath}`);
}

main();
