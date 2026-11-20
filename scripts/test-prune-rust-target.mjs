import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
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
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
