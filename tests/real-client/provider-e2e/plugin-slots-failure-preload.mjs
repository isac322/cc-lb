// Fault injection for plugin-slots.lifecycle.test.mjs; never used by normal harness runs.
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import cp from 'node:child_process';
import { syncBuiltinESMExports } from 'node:module';

const mode = process.env.CC_LB_QA_FAULT_MODE;
const metadata = process.env.CC_LB_QA_FAULT_METADATA;
const originalSpawn = cp.spawn;
cp.spawn = (...args) => {
  const child = originalSpawn(...args);
  const owned = { harnessPid: process.pid, childPid: child.pid, mode };
  fs.writeFileSync(metadata, JSON.stringify(owned), { mode: 0o600 });
  fs.appendFileSync(process.env.CC_LB_QA_LEDGER, `${JSON.stringify({
    kind: 'process', identifier: `fault-test cc-lb pid=${child.pid}; metadata=${metadata}`,
    deleteCommand: `kill -TERM ${child.pid}`, createdAt: new Date().toISOString(),
  })}\n`, { mode: 0o600 });
  child.once('exit', (code, signal) => {
    fs.writeFileSync(metadata, JSON.stringify({ ...owned, exited: true, code, signal }), { mode: 0o600 });
  });
  return child;
};
const originalWrite = fsp.writeFile;
fsp.writeFile = async (file, ...args) => {
  if (mode === 'report' && String(file).endsWith('/results.json')) throw new Error('INJECTED_REPORT_WRITE_FAILURE');
  return originalWrite(file, ...args);
};
const originalAppend = fsp.appendFile;
fsp.appendFile = async (file, data, ...args) => {
  if (mode === 'ledger-child' && String(data).includes('cc-lb pid=')) throw new Error('INJECTED_CHILD_LEDGER_FAILURE');
  return originalAppend(file, data, ...args);
};
syncBuiltinESMExports();
