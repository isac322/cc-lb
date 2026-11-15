import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { appendFile, mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

// Uses the actual built binary and WASM. Missing inputs are failures, never skips.
function required(name) {
  const value = process.env[name];
  assert.ok(value && path.isAbsolute(value), `${name} must be an explicit absolute path`);
  return value;
}
const bin = required('CC_LB_BIN');
const wasm = required('CC_LB_PLUGIN_WASM');
const root = required('CC_LB_QA_OUTPUT');
const ledger = required('CC_LB_QA_LEDGER');
const harness = fileURLToPath(new URL('./plugin-slots.mjs', import.meta.url));
const preload = fileURLToPath(new URL('./plugin-slots-failure-preload.mjs', import.meta.url));
const shellQuote = value => `'${value.replaceAll("'", "'\\''")}'`;
await mkdir(root, { mode: 0o700 }); // Refuse reuse and preserve every run's evidence.
await appendFile(ledger, `${JSON.stringify({ kind: 'scratch-directory', identifier: root,
  deleteCommand: `rm -r -- ${shellQuote(root)}`, createdAt: new Date().toISOString() })}\n`, { mode: 0o600 });

async function runHarness(t, mode) {
  const output = path.join(root, `${mode}-$capture's-evidence`);
  const metadata = path.join(root, `${mode}-child.json`);
  const runLedger = path.join(root, `${mode}.ledger`);
  const child = spawn(process.execPath, ['--import', preload, harness], {
    env: { ...process.env, CC_LB_BIN: bin, CC_LB_PLUGIN_WASM: wasm, CC_LB_QA_OUTPUT: output,
      CC_LB_QA_LEDGER: runLedger, CC_LB_QA_FAULT_MODE: mode, CC_LB_QA_FAULT_METADATA: metadata },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const closed = new Promise(resolve => child.once('close', resolve));
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', data => { stdout += data; });
  child.stderr.on('data', data => { stderr += data; });
  t.after(async () => {
    // Only stop subprocesses owned by this invocation, never pre-existing resources.
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM');
    await closed;
    let owned;
    try { owned = JSON.parse(await readFile(metadata, 'utf8')); } catch (error) {
      if (error.code === 'ENOENT') return;
      throw error;
    }
    assert.equal(owned.harnessPid, child.pid);
    if (!owned.exited && owned.childPid) {
      try { process.kill(owned.childPid, 'SIGTERM'); } catch (error) {
        if (error.code !== 'ESRCH') throw error;
      }
    }
  });
  const done = new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      child.kill('SIGTERM');
      reject(new Error(`Harness timed out (${mode})\n${stderr}`));
    }, 120000);
    child.once('error', error => { clearTimeout(timer); reject(error); });
    closed.then(() => { clearTimeout(timer); resolve({ code: child.exitCode, signal: child.signalCode }); });
  });
  await appendFile(ledger, `${JSON.stringify({ kind: 'process', identifier: `lifecycle-test harness pid=${child.pid}; output=${output}`,
    deleteCommand: `kill -TERM ${child.pid}`, createdAt: new Date().toISOString() })}\n`, { mode: 0o600 });
  const exit = await done;
  const owned = JSON.parse(await readFile(metadata, 'utf8'));
  assert.equal(owned.harnessPid, child.pid);
  assert.equal(owned.exited, true, `Owned cc-lb child must exit before harness (${mode})\n${stderr}`);
  assert.equal(exit.signal, null, stderr);
  return { ...exit, output, runLedger, stdout, stderr };
}

for (const [mode, marker] of [
  ['report', 'INJECTED_REPORT_WRITE_FAILURE'],
  ['ledger-child', 'INJECTED_CHILD_LEDGER_FAILURE'],
]) {
  test(`${mode} failure exits nonzero and closes owned child/provider`, async t => {
    const result = await runHarness(t, mode);
    assert.equal(result.code, 1, result.stderr);
    assert.ok(result.stderr.includes(marker), result.stderr);
    if (mode === 'report') {
      assert.ok(result.stdout.includes('PASS: plugin slot HTTP regression'), result.stderr);
    } else {
      const evidence = JSON.parse(await readFile(path.join(result.output, 'results.json'), 'utf8'));
      assert.equal(evidence.passed, false);
      assert.ok(evidence.errors.some(error => error.message === marker));
    }
  });
}

test('successful harness records literal shell-quoted cleanup paths', async t => {
  const result = await runHarness(t, 'normal');
  assert.equal(result.code, 0, result.stderr);
  const evidence = JSON.parse(await readFile(path.join(result.output, 'results.json'), 'utf8'));
  assert.equal(evidence.passed, true);
  const records = (await readFile(result.runLedger, 'utf8')).trim().split('\n').map(line => JSON.parse(line));
  const scratch = records.find(record => record.kind === 'scratch-directory' && record.identifier === result.output);
  const admin = records.filter(record => record.kind.startsWith('admin-') && record.identifier.endsWith(`config=${path.join(result.output, 'cc-lb.toml')}`));
  assert.ok(scratch, 'scratch directory must have a cleanup record');
  assert.ok(admin.length > 0, 'admin resources must have cleanup records');
  for (const record of [scratch, ...admin]) {
    const prefix = record.kind === 'scratch-directory' ? 'rm -r -- '
      : '# Stop recorded processes first; then remove isolated store: rm -r -- ';
    assert.ok(record.deleteCommand.startsWith(prefix));
    const encoded = record.deleteCommand.slice(prefix.length);
    assert.ok(encoded.startsWith("'") && encoded.endsWith("'"));
    // Decode only our single-quoted token grammar; never evaluate shell text or delete evidence.
    const decoded = encoded.slice(1, -1).replaceAll("'\\''", "'");
    assert.equal(encoded, shellQuote(decoded), 'cleanup path must be one POSIX single-quoted token');
    const argv = ['rm', '-r', '--', decoded];
    assert.deepEqual(argv, ['rm', '-r', '--', result.output]);
  }
});
