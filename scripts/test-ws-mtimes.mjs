import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, utimesSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { recordWorkspaceMtimes, restoreWorkspaceMtimes } from './ws-mtimes.mjs';

function initRepo() {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'ws-mtimes-'));
  execFileSync('git', ['init', '-q'], { cwd: dir });
  execFileSync('git', ['config', 'user.email', 'test@example.com'], { cwd: dir });
  execFileSync('git', ['config', 'user.name', 'Test'], { cwd: dir });
  execFileSync('git', ['config', 'commit.gpgsign', 'false'], { cwd: dir });
  return dir;
}

test('ws-mtimes restores unchanged files and dirs while keeping modified/new entries at NOW', () => {
  const repo = initRepo();
  const scratch = mkdtempSync(path.join(os.tmpdir(), 'ws-mtimes-snap-'));
  try {
    mkdirSync(path.join(repo, 'crates/db/migrations'), { recursive: true });
    mkdirSync(path.join(repo, 'crates/admin/web/dist/assets'), { recursive: true });
    writeFileSync(path.join(repo, 'crates/db/lib.rs'), 'pub fn a() {}');
    writeFileSync(path.join(repo, 'crates/db/migrations/001.sql'), 'CREATE TABLE t (id INT);');
    writeFileSync(path.join(repo, '.gitignore'), 'crates/admin/web/dist/\n');
    writeFileSync(path.join(repo, 'crates/admin/web/dist/index.html'), '<html/>');
    writeFileSync(path.join(repo, 'crates/admin/web/dist/assets/index-skip.js'), '');
    execFileSync('git', ['add', '.gitignore', 'crates/db'], { cwd: repo });

    // Set deterministic old mtimes (year 2024).
    const oldSec = 1_700_000_000;
    for (const rel of [
      'crates/db/lib.rs',
      'crates/db/migrations/001.sql',
      'crates/admin/web/dist/index.html',
      'crates/admin/web/dist/assets/index-skip.js',
      'crates/admin/web/dist/assets',
      'crates/admin/web/dist',
      'crates/db/migrations',
    ]) {
      utimesSync(path.join(repo, rel), oldSec, oldSec);
    }

    const snap = path.join(scratch, 'mtimes.json');
    const rec = recordWorkspaceMtimes(repo, snap, ['crates/admin/web/dist']);
    assert.ok(rec.fileCount >= 5);

    // Simulate a fresh git checkout setting all tracked files & dirs to NOW.
    const nowSec = Math.floor(Date.now() / 1000);
    for (const rel of [
      'crates/db/lib.rs',
      'crates/db/migrations/001.sql',
      'crates/db/migrations',
      'crates/admin/web/dist/index.html',
      'crates/admin/web/dist/assets/index-skip.js',
      'crates/admin/web/dist/assets',
      'crates/admin/web/dist',
    ]) {
      utimesSync(path.join(repo, rel), nowSec, nowSec);
    }
    // Mutate one tracked file by 1 byte -> must stay at NOW!
    writeFileSync(path.join(repo, 'crates/db/lib.rs'), 'pub fn b() {}');
    utimesSync(path.join(repo, 'crates/db/lib.rs'), nowSec, nowSec);

    const res1 = restoreWorkspaceMtimes(repo, snap);
    assert.equal(res1.skippedFiles, 1);
    assert.ok(Math.abs(statSync(path.join(repo, 'crates/db/migrations/001.sql')).mtimeMs - oldSec * 1000) < 10);
    assert.ok(Math.abs(statSync(path.join(repo, 'crates/db/migrations')).mtimeMs - oldSec * 1000) < 10);
    assert.ok(Math.abs(statSync(path.join(repo, 'crates/admin/web/dist/index.html')).mtimeMs - oldSec * 1000) < 10);
    assert.ok(Math.abs(statSync(path.join(repo, 'crates/admin/web/dist')).mtimeMs - oldSec * 1000) < 10);
    assert.ok(statSync(path.join(repo, 'crates/db/lib.rs')).mtimeMs >= (nowSec - 2) * 1000);

    // Adding a new migration file must leave `migrations` dir at NOW while restoring 001.sql!
    utimesSync(path.join(repo, 'crates/db/migrations'), nowSec, nowSec);
    writeFileSync(path.join(repo, 'crates/db/migrations/002.sql'), 'CREATE TABLE u (id INT);');
    utimesSync(path.join(repo, 'crates/db/migrations/002.sql'), nowSec, nowSec);
    utimesSync(path.join(repo, 'crates/db/migrations'), nowSec, nowSec);

    const res2 = restoreWorkspaceMtimes(repo, snap);
    assert.ok(res2.skippedDirs >= 1);
    assert.ok(statSync(path.join(repo, 'crates/db/migrations')).mtimeMs >= (nowSec - 2) * 1000);

    // Missing snapshot safely no-ops.
    const missing = restoreWorkspaceMtimes(repo, path.join(scratch, 'does-not-exist.json'));
    assert.equal(missing.missing, true);
  } finally {
    rmSync(repo, { recursive: true, force: true });
    rmSync(scratch, { recursive: true, force: true });
  }
});

test('ws-mtimes preserves exact nanosecond timestamps and rejects v1 snapshots', () => {
  const repo = initRepo();
  const scratch = mkdtempSync(path.join(os.tmpdir(), 'ws-mtimes-ns-'));
  try {
    mkdirSync(path.join(repo, 'crates/hack'), { recursive: true });
    const libPath = path.join(repo, 'crates/hack/lib.rs');
    const dirPath = path.join(repo, 'crates/hack');
    writeFileSync(libPath, 'pub fn ok() {}');
    execFileSync('git', ['add', 'crates/hack'], { cwd: repo });

    const exactNs = '1700000000123456789';
    execFileSync(
      'python3',
      ['-c', 'import os, sys; ns = int(sys.argv[1]); [os.utime(p, ns=(ns, ns)) for p in sys.argv[2:]]', exactNs, libPath, dirPath],
      { cwd: repo },
    );
    const recordedFileNs = statSync(libPath, { bigint: true }).mtimeNs;
    const recordedDirNs = statSync(dirPath, { bigint: true }).mtimeNs;

    const snap = path.join(scratch, 'mtimes-v2.json');
    recordWorkspaceMtimes(repo, snap);

    const nowSec = Math.floor(Date.now() / 1000);
    utimesSync(libPath, nowSec, nowSec);
    utimesSync(dirPath, nowSec, nowSec);

    const res = restoreWorkspaceMtimes(repo, snap);
    assert.equal(res.restoredFiles, 1);
    assert.equal(statSync(libPath, { bigint: true }).mtimeNs, recordedFileNs);
    assert.equal(statSync(dirPath, { bigint: true }).mtimeNs, recordedDirNs);

    // v1 snapshot safely marks corrupt/incompatible.
    const v1Snap = path.join(scratch, 'mtimes-v1.json');
    writeFileSync(v1Snap, JSON.stringify({ version: 1, files: {}, dirs: {} }));
    assert.equal(restoreWorkspaceMtimes(repo, v1Snap).corrupt, true);
  } finally {
    rmSync(repo, { recursive: true, force: true });
    rmSync(scratch, { recursive: true, force: true });
  }
});

test('ws-mtimes v3 handles committed index OIDs, staged renames, dirty worktree edits, and v2 compat fallback', () => {
  const repo = initRepo();
  const scratch = mkdtempSync(path.join(os.tmpdir(), 'ws-mtimes-v3-'));
  try {
    mkdirSync(path.join(repo, 'src'), { recursive: true });
    const aPath = path.join(repo, 'src/a.rs');
    const bPath = path.join(repo, 'src/b.rs');
    const cPath = path.join(repo, 'src/c.rs');
    writeFileSync(aPath, 'pub fn a() {}');
    writeFileSync(bPath, 'pub fn b() {}');
    writeFileSync(cPath, 'pub fn c_v1() {}');
    execFileSync('git', ['add', 'src'], { cwd: repo });
    execFileSync('git', ['commit', '-qm', 'initial'], { cwd: repo });

    // Staged rename a.rs -> a_renamed.rs, staged+unstaged modification to c.rs, new untracked u.rs.
    const aRenamedPath = path.join(repo, 'src/a_renamed.rs');
    const uPath = path.join(repo, 'src/u.rs');
    execFileSync('git', ['mv', 'src/a.rs', 'src/a_renamed.rs'], { cwd: repo });
    writeFileSync(cPath, 'pub fn c_staged() {}');
    execFileSync('git', ['add', 'src/c.rs'], { cwd: repo });
    writeFileSync(cPath, 'pub fn c_worktree() {}');
    writeFileSync(uPath, 'pub fn u() {}');

    const oldSec = 1_700_000_100;
    for (const p of [aRenamedPath, bPath, cPath, uPath, path.join(repo, 'src')]) {
      utimesSync(p, oldSec, oldSec);
    }

    const v3SnapPath = path.join(scratch, 'mtimes-v3.json');
    const rec = recordWorkspaceMtimes(repo, v3SnapPath);
    assert.equal(rec.fileCount, 4);

    const v3Snap = JSON.parse(readFileSync(v3SnapPath, 'utf8'));
    assert.equal(v3Snap.version, 3);
    assert.ok(v3Snap.files['src/b.rs'].oid);
    assert.equal(v3Snap.files['src/b.rs'].sha256, undefined);
    // Verify dirty worktree content hashed accurately (not stale staged blob).
    const expectedCOid = createHash('sha1')
      .update(`blob ${Buffer.byteLength('pub fn c_worktree() {}')}\0pub fn c_worktree() {}`)
      .digest('hex');
    assert.equal(v3Snap.files['src/c.rs'].oid, expectedCOid);

    const nowSec = Math.floor(Date.now() / 1000);
    for (const p of [aRenamedPath, bPath, cPath, uPath, path.join(repo, 'src')]) {
      utimesSync(p, nowSec, nowSec);
    }

    const res3 = restoreWorkspaceMtimes(repo, v3SnapPath);
    assert.equal(res3.restoredFiles, 4);
    assert.equal(res3.skippedFiles, 0);
    assert.ok(Math.abs(statSync(aRenamedPath).mtimeMs - oldSec * 1000) < 10);
    assert.ok(Math.abs(statSync(bPath).mtimeMs - oldSec * 1000) < 10);
    assert.ok(Math.abs(statSync(cPath).mtimeMs - oldSec * 1000) < 10);
    assert.ok(Math.abs(statSync(uPath).mtimeMs - oldSec * 1000) < 10);
    assert.ok(Math.abs(statSync(path.join(repo, 'src')).mtimeMs - oldSec * 1000) < 10);

    // Mutate worktree c.rs after snapshot -> skipped on restore, leaving src dir at NOW.
    for (const p of [bPath, cPath, path.join(repo, 'src')]) {
      utimesSync(p, nowSec, nowSec);
    }
    writeFileSync(cPath, 'pub fn c_worktree_2() {}');
    utimesSync(cPath, nowSec, nowSec);
    const res3Dirty = restoreWorkspaceMtimes(repo, v3SnapPath);
    assert.equal(res3Dirty.skippedFiles, 1);
    assert.ok(Math.abs(statSync(bPath).mtimeMs - oldSec * 1000) < 10);
    assert.ok(statSync(cPath).mtimeMs >= (nowSec - 2) * 1000);
    assert.ok(statSync(path.join(repo, 'src')).mtimeMs >= (nowSec - 2) * 1000);

    // Legacy v2 snapshot compatibility restore fallback.
    const bSha256 = createHash('sha256').update(readFileSync(bPath)).digest('hex');
    const v2SnapPath = path.join(scratch, 'mtimes-v2-compat.json');
    writeFileSync(
      v2SnapPath,
      JSON.stringify({
        version: 2,
        extraDirs: [],
        files: {
          'src/b.rs': { sha256: bSha256, mtimeNs: String(BigInt(oldSec) * 1_000_000_000n) },
        },
        dirs: {},
      }),
    );
    utimesSync(bPath, nowSec, nowSec);
    const res2Compat = restoreWorkspaceMtimes(repo, v2SnapPath);
    assert.equal(res2Compat.restoredFiles, 1);
    assert.ok(Math.abs(statSync(bPath).mtimeMs - oldSec * 1000) < 10);
  } finally {
    rmSync(repo, { recursive: true, force: true });
    rmSync(scratch, { recursive: true, force: true });
  }
});
