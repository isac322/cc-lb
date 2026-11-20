import assert from 'node:assert/strict';
import test from 'node:test';
import { mergeLcovTexts } from './merge-lcov.mjs';

test('mergeLcovTexts sums hits across partitions and recomputes summaries cleanly', () => {
  const lcov1 = [
    'SF:/repo/crates/cc-lb-server/src/lib.rs',
    'FN:10,alpha',
    'FN:20,beta',
    'FNDA:0,alpha',
    'FNDA:2,beta',
    'FNF:2',
    'FNH:1',
    'BRDA:12,0,0,-',
    'BRDA:12,0,1,0',
    'DA:10,0',
    'DA:11,0',
    'DA:20,2',
    'LF:3',
    'LH:1',
    'end_of_record',
  ].join('\n');

  const lcov2 = [
    'SF:/repo/crates/cc-lb-server/src/lib.rs',
    'FN:10,alpha',
    'FNDA:3,alpha',
    'BRDA:12,0,0,1',
    'BRDA:12,0,1,-',
    'DA:10,3',
    'DA:11,0',
    'DA:25,4',
    'end_of_record',
    'SF:/repo/crates/cc-lb-admin/src/lib.rs',
    'DA:1,1',
    'LF:1',
    'LH:1',
    'end_of_record',
  ].join('\n');

  const merged = mergeLcovTexts([lcov1, lcov2]);
  assert.ok(merged.includes('SF:/repo/crates/cc-lb-admin/src/lib.rs'));
  assert.ok(merged.includes('SF:/repo/crates/cc-lb-server/src/lib.rs'));
  assert.ok(merged.includes('FNDA:3,alpha'));
  assert.ok(merged.includes('FNDA:2,beta'));
  assert.ok(merged.includes('FNF:2\nFNH:2'));
  assert.ok(merged.includes('BRDA:12,0,0,1'));
  assert.ok(merged.includes('BRDA:12,0,1,0'));
  assert.ok(merged.includes('BRF:2\nBRH:1'));
  assert.ok(merged.includes('DA:10,3\nDA:11,0\nDA:20,2\nDA:25,4'));
  assert.ok(merged.includes('LF:4\nLH:3'));
});
