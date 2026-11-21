import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { buildIgnoreFilenameRegex, collectExtractedArtifacts } from './export-merged-lcov.mjs';

test('buildIgnoreFilenameRegex excludes test files, target, cargo/rustup, and _tests.rs modules', () => {
  const root = '/home/runner/work/cc-lb/cc-lb';
  const cargoHome = '/home/runner/.cargo';
  const rustupHome = '/home/runner/tmp/rustup';
  const re = new RegExp(buildIgnoreFilenameRegex(root, cargoHome, rustupHome));

  const shouldIgnore = [
    '/rustc/129f3b9964af4d4a709d1383930ade12dfe7c081/library/core/src/option.rs',
    `${root}/crates/cc-lb-server/tests/integration.rs`,
    `${root}/crates/cc-lb-engine/tests/typestate_contracts/main.rs`,
    `${root}/crates/cc-lb-server/src/http_tests.rs`,
    `${root}/crates/cc-lb-admin/src/api-tests.rs`,
    `${root}/crates/cc-lb-engine/src/tests.rs`,
    `${root}/target/llvm-cov-target/debug/build/foo/out/bar.rs`,
    `${cargoHome}/registry/src/index.crates.io/tokio-1.0.0/src/lib.rs`,
    `${cargoHome}/git/checkouts/foo/src/lib.rs`,
    `${rustupHome}/toolchains/1.99.1-x86_64-unknown-linux-gnu/lib/rustlib/src/rust/library/alloc/src/vec/mod.rs`,
    `${root}/tests/load/src/main.rs`,
    `${root}/tests/stress-suite/src/main.rs`,
    `${root}/tools/workspace-hack/src/lib.rs`,
    `${root}/fuzz/fuzz_targets/x.rs`,
    'tests/fixtures/fake-anthropic/src/lib.rs',
  ];
  for (const p of shouldIgnore) {
    assert.ok(re.test(p), `expected regex to ignore: ${p}`);
  }

  const shouldKeep = [
    `${root}/crates/cc-lb-server/src/main.rs`,
    `${root}/crates/cc-lb-server/src/proxy/handler.rs`,
    `${root}/crates/cc-lb-engine/src/pipeline.rs`,
    `${root}/crates/cc-lb-signer-anthropic-key/src/lib.rs`,
    `${root}/crates/cc-lb-storage-sqlite/src/store.rs`,
  ];
  for (const p of shouldKeep) {
    assert.ok(!re.test(p), `expected regex to keep: ${p}`);
  }
});

test('collectExtractedArtifacts deduplicates objects by basename and preserves sorted profdata', () => {
  const tmp = mkdtempSync(path.join(os.tmpdir(), 'cc-lb-export-test-'));
  try {
    mkdirSync(path.join(tmp, '01-server-unit', 'objs'), { recursive: true });
    mkdirSync(path.join(tmp, '02-server-integ-1', 'objs'), { recursive: true });
    mkdirSync(path.join(tmp, '03-server-integ-2'), { recursive: true });

    writeFileSync(path.join(tmp, '01-server-unit', 'part-server-unit.profdata'), 'p1');
    writeFileSync(path.join(tmp, '01-server-unit', 'objs', 'cc-lb'), 'bin1');
    writeFileSync(path.join(tmp, '01-server-unit', 'objs', 'cc_lb_server-aaaa'), 'unit');

    writeFileSync(path.join(tmp, '02-server-integ-1', 'part-server-integ-1.profdata'), 'p2');
    writeFileSync(path.join(tmp, '02-server-integ-1', 'objs', 'cc-lb'), 'bin-dup');
    writeFileSync(path.join(tmp, '02-server-integ-1', 'objs', 'integration-bbbb'), 'integ');

    writeFileSync(path.join(tmp, '03-server-integ-2', 'part-server-integ-2.profdata'), 'p3');

    const { profdataFiles, objectFiles } = collectExtractedArtifacts(tmp);
    assert.equal(profdataFiles.length, 3);
    assert.deepEqual(
      objectFiles.map((f) => path.basename(f)),
      ['cc-lb', 'cc_lb_server-aaaa', 'integration-bbbb'],
    );
    assert.ok(objectFiles.some((f) => f.includes('01-server-unit')));
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
});
