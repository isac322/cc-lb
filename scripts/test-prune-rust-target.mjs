import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { pruneTargetDir } from './prune-rust-target.mjs';

const SAMPLE_METADATA = {
  workspace_members: ['cc-lb-server 2.2.1', 'tests-integration 0.1.0'],
  packages: [
    {
      id: 'cc-lb-server 2.2.1',
      name: 'cc-lb-server',
      targets: [
        { name: 'cc_lb_server', kind: ['lib'] },
        { name: 'cc-lb', kind: ['bin'] },
        { name: 'integration', kind: ['test'] },
      ],
    },
    {
      id: 'tests-integration 0.1.0',
      name: 'tests-integration',
      targets: [{ name: 'integration', kind: ['test'] }],
    },
  ],
};

function touch(filePath) {
  mkdirSync(path.dirname(filePath), { recursive: true });
  writeFileSync(filePath, 'stub');
}

test('pruneTargetDir removes workspace artifacts paired with fingerprints and preserves third-party deps', () => {
  const root = mkdtempSync(path.join(os.tmpdir(), 'prune-rust-target-'));
  try {
    const debugDir = path.join(root, 'debug');
    touch(path.join(root, 'extra.profraw'));
    touch(path.join(debugDir, 'cc-lb'));
    touch(path.join(debugDir, 'cc-lb.d'));
    touch(path.join(debugDir, 'incremental', 'inc-1', 'work'));
    touch(path.join(debugDir, 'deps', 'libcc_lb_server-0123456789abcdef.rlib'));
    touch(path.join(debugDir, 'deps', 'integration-fedcba9876543210'));
    touch(path.join(debugDir, 'deps', 'libtokio-0123456789abcdef.rlib'));
    touch(path.join(debugDir, 'deps', 'libtokio_macros-0123456789abcdef.so'));
    touch(path.join(debugDir, '.fingerprint', 'cc-lb-server-0123456789abcdef', 'lib-cc_lb_server'));
    touch(path.join(debugDir, '.fingerprint', 'integration-fedcba9876543210', 'test-integration'));
    touch(path.join(debugDir, '.fingerprint', 'tokio-0123456789abcdef', 'lib-tokio'));
    touch(path.join(debugDir, 'build', 'cc-lb-server-0123456789abcdef', 'out', 'meta.rs'));
    touch(path.join(debugDir, 'build', 'ring-0123456789abcdef', 'out', 'libring.a'));
    // Also test new Cargo build-dir layout.
    touch(path.join(debugDir, 'build', 'cc-lb-server', '0123456789abcdef', 'fingerprint', 'invoked'));
    touch(path.join(debugDir, 'build', 'tokio', '0123456789abcdef', 'fingerprint', 'lib-tokio'));

    const res = pruneTargetDir(root, SAMPLE_METADATA);
    assert.ok(res.removedEntries >= 8);

    assert.equal(existsSync(path.join(root, 'extra.profraw')), false);
    assert.equal(existsSync(path.join(debugDir, 'cc-lb')), false);
    assert.equal(existsSync(path.join(debugDir, 'cc-lb.d')), false);
    assert.equal(existsSync(path.join(debugDir, 'incremental')), false);
    assert.equal(existsSync(path.join(debugDir, 'deps', 'libcc_lb_server-0123456789abcdef.rlib')), false);
    assert.equal(existsSync(path.join(debugDir, 'deps', 'integration-fedcba9876543210')), false);
    assert.equal(existsSync(path.join(debugDir, '.fingerprint', 'cc-lb-server-0123456789abcdef')), false);
    assert.equal(existsSync(path.join(debugDir, '.fingerprint', 'integration-fedcba9876543210')), false);
    assert.equal(existsSync(path.join(debugDir, 'build', 'cc-lb-server-0123456789abcdef')), false);
    assert.equal(existsSync(path.join(debugDir, 'build', 'cc-lb-server')), false);

    assert.equal(existsSync(path.join(debugDir, 'deps', 'libtokio-0123456789abcdef.rlib')), true);
    assert.equal(existsSync(path.join(debugDir, 'deps', 'libtokio_macros-0123456789abcdef.so')), true);
    assert.equal(existsSync(path.join(debugDir, '.fingerprint', 'tokio-0123456789abcdef', 'lib-tokio')), true);
    assert.equal(existsSync(path.join(debugDir, 'build', 'ring-0123456789abcdef', 'out', 'libring.a')), true);
    assert.equal(existsSync(path.join(debugDir, 'build', 'tokio', '0123456789abcdef', 'fingerprint', 'lib-tokio')), true);

    // Direct profile dir invocation (e.g. target/debug or target/llvm-cov-target/debug).
    touch(path.join(debugDir, 'deps', 'libcc_lb_server-aaaaaaaaaaaaaaaa.rlib'));
    touch(path.join(debugDir, '.fingerprint', 'cc-lb-server-aaaaaaaaaaaaaaaa', 'lib-cc_lb_server'));
    const directRes = pruneTargetDir(debugDir, SAMPLE_METADATA);
    assert.equal(directRes.removedEntries, 2);
    assert.equal(existsSync(path.join(debugDir, 'deps', 'libcc_lb_server-aaaaaaaaaaaaaaaa.rlib')), false);
    assert.equal(existsSync(path.join(debugDir, '.fingerprint', 'cc-lb-server-aaaaaaaaaaaaaaaa')), false);
    assert.equal(existsSync(path.join(debugDir, 'deps', 'libtokio-0123456789abcdef.rlib')), true);

    // keepWorkspaceLibs=true preserves .rlib/.rmeta/.d/.fingerprint/build while stripping executables & incremental.
    touch(path.join(debugDir, 'cc-lb'));
    touch(path.join(debugDir, 'incremental', 'inc-2', 'work'));
    touch(path.join(debugDir, 'deps', 'libcc_lb_server-1111111111111111.rlib'));
    touch(path.join(debugDir, 'deps', 'libcc_lb_server-1111111111111111.rmeta'));
    touch(path.join(debugDir, 'deps', 'cc_lb_server-1111111111111111.d'));
    touch(path.join(debugDir, 'deps', 'integration-2222222222222222'));
    touch(path.join(debugDir, '.fingerprint', 'cc-lb-server-1111111111111111', 'lib-cc_lb_server'));
    touch(path.join(debugDir, 'build', 'cc-lb-server-1111111111111111', 'out', 'meta.rs'));
    const keepRes = pruneTargetDir(debugDir, SAMPLE_METADATA, { keepWorkspaceLibs: true });
    assert.ok(keepRes.removedEntries >= 3);
    assert.equal(existsSync(path.join(debugDir, 'cc-lb')), false);
    assert.equal(existsSync(path.join(debugDir, 'incremental')), false);
    assert.equal(existsSync(path.join(debugDir, 'deps', 'integration-2222222222222222')), false);
    assert.equal(existsSync(path.join(debugDir, 'deps', 'libcc_lb_server-1111111111111111.rlib')), true);
    assert.equal(existsSync(path.join(debugDir, 'deps', 'libcc_lb_server-1111111111111111.rmeta')), true);
    assert.equal(existsSync(path.join(debugDir, 'deps', 'cc_lb_server-1111111111111111.d')), true);
    assert.equal(existsSync(path.join(debugDir, '.fingerprint', 'cc-lb-server-1111111111111111')), true);
    assert.equal(existsSync(path.join(debugDir, 'build', 'cc-lb-server-1111111111111111', 'out', 'meta.rs')), true);

    // Nested target/tests/trybuild/<triple>/debug layout with keepWorkspaceLibs=true.
    const trybuildRoot = path.join(root, 'tests', 'trybuild');
    const trybuildTripleDebug = path.join(trybuildRoot, 'x86_64-unknown-linux-gnu', 'debug');
    touch(path.join(trybuildTripleDebug, 'deps', 'libcc_lb_server-3333333333333333.rmeta'));
    touch(path.join(trybuildTripleDebug, 'deps', 'cc_lb_server-3333333333333333.d'));
    touch(path.join(trybuildTripleDebug, 'incremental', 'inc-3', 'work'));
    const trybuildRes = pruneTargetDir(trybuildRoot, SAMPLE_METADATA, { keepWorkspaceLibs: true });
    assert.ok(trybuildRes.removedEntries >= 1);
    assert.equal(existsSync(path.join(trybuildTripleDebug, 'incremental')), false);
    assert.equal(existsSync(path.join(trybuildTripleDebug, 'deps', 'libcc_lb_server-3333333333333333.rmeta')), true);
    assert.equal(existsSync(path.join(trybuildTripleDebug, 'deps', 'cc_lb_server-3333333333333333.d')), true);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('pruneTargetDir with keepLatestIncremental keeps only newest workspace hash dir and newest s-* session', () => {
  const root = mkdtempSync(path.join(os.tmpdir(), 'prune-rust-inc-'));
  try {
    const releaseDir = path.join(root, 'aarch64-unknown-linux-musl', 'release');
    const incDir = path.join(releaseDir, 'incremental');

    const oldCrateHash = path.join(incDir, 'cc_lb_server-1111aaaa');
    const newCrateHash = path.join(incDir, 'cc_lb_server-2222bbbb');
    const nonWsCrate = path.join(incDir, 'tokio-3333cccc');
    const strayFile = path.join(incDir, 'stray.lock');

    const oldSess = path.join(newCrateHash, 's-1000-01');
    const newSess = path.join(newCrateHash, 's-2000-02');
    const workingSess = path.join(newCrateHash, 's-3000-03-working');
    const crateLock = path.join(newCrateHash, 's-2000-02.lock');

    touch(path.join(oldCrateHash, 's-0900-00', 'work.bin'));
    touch(path.join(oldSess, 'dep-graph.bin'));
    touch(path.join(newSess, 'dep-graph.bin'));
    touch(path.join(workingSess, 'dep-graph.bin'));
    touch(crateLock);
    touch(path.join(nonWsCrate, 's-4000-01', 'work.bin'));
    touch(strayFile);

    utimesSync(oldSess, 1_700_000_100, 1_700_000_100);
    utimesSync(newSess, 1_700_000_200, 1_700_000_200);
    utimesSync(oldCrateHash, 1_700_000_100, 1_700_000_100);
    utimesSync(newCrateHash, 1_700_000_200, 1_700_000_200);

    const res = pruneTargetDir(root, SAMPLE_METADATA, {
      keepWorkspaceLibs: true,
      keepLatestIncremental: true,
    });
    assert.ok(res.removedEntries >= 5);
    assert.equal(existsSync(strayFile), false);
    assert.equal(existsSync(nonWsCrate), false);
    assert.equal(existsSync(oldCrateHash), false);
    assert.equal(existsSync(oldSess), false);
    assert.equal(existsSync(workingSess), false);
    assert.equal(existsSync(crateLock), false);
    assert.equal(existsSync(path.join(newSess, 'dep-graph.bin')), true);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
