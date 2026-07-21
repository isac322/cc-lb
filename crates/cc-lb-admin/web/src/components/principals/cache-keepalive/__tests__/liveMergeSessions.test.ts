import { describe, expect, it } from 'vitest';
import type { CacheKeepaliveRow } from '../../../../lib/cacheKeepaliveApi';
import { mergeLiveSessions } from '../liveMergeSessions';

describe('mergeLiveSessions', () => {
  const makeRow = (
    id: string,
    state: CacheKeepaliveRow['state'] = 'scheduled',
  ): CacheKeepaliveRow => ({
    id,
    state,
    last_message_at_ms: 0,
    ttl: null,
    attempts: null,
    max_attempts: 12,
    reason: 'test',
    generation: 1,
    upstream: null,
    error: null,
    net_pnl: 0,
    session_key_hash: 'hash',
  });

  it('Given empty input, When merged, Then returns empty array', () => {
    expect(mergeLiveSessions([], 10)).toEqual([]);
  });

  it('Given duplicate ids, When merged, Then keeps the first occurrence (newest-first)', () => {
    const rows = [
      makeRow('1', 'renewed'),
      makeRow('2', 'scheduled'),
      makeRow('1', 'scheduled'), // duplicate, older state
    ];
    const result = mergeLiveSessions(rows, 10);
    expect(result).toHaveLength(2);
    expect(result[0].id).toBe('1');
    expect(result[0].state).toBe('renewed');
    expect(result[1].id).toBe('2');
  });

  it('Given rows exceeding maxRows, When merged, Then trims to maxRows', () => {
    const rows = [makeRow('1'), makeRow('2'), makeRow('3'), makeRow('4')];
    const result = mergeLiveSessions(rows, 2);
    expect(result).toHaveLength(2);
    expect(result[0].id).toBe('1');
    expect(result[1].id).toBe('2');
  });

  it('Given in-place update, When merged, Then reflects the changed row object at same id', () => {
    const rows1 = [makeRow('1', 'scheduled')];
    const result1 = mergeLiveSessions(rows1, 10);
    expect(result1[0].state).toBe('scheduled');

    const rows2 = [makeRow('1', 'renewed')];
    const result2 = mergeLiveSessions(rows2, 10);
    expect(result2[0].state).toBe('renewed');
  });

  it('Given order, When merged, Then preserves newest-first order', () => {
    const rows = [makeRow('3'), makeRow('1'), makeRow('2')];
    const result = mergeLiveSessions(rows, 10);
    expect(result.map((r) => r.id)).toEqual(['3', '1', '2']);
  });
});
