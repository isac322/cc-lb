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
    is_active: state === 'renewed' || state === 'scheduled',
    last_message_at_ms: 0,
    ttl: null,
    attempts: null,
    max_attempts: 12,
    reason: 'test',
    generation: 1,
    upstream: null,
    error: null,
    net_pnl: 0,
    total_spent: 0,
    total_avoided: null,
    evidence_status: 'none',
    session_key_hash: 'hash',
  });

  it('Given empty input, When merged, Then returns empty array', () => {
    expect(mergeLiveSessions([])).toEqual([]);
  });

  it('Given a session on page 1 and a stale copy on page 2, When merged, Then the page-1 copy wins', () => {
    const pageOneCopy = makeRow('1', 'renewed');
    const second = makeRow('2', 'scheduled');
    const stalePageTwoCopy = makeRow('1', 'scheduled');

    const result = mergeLiveSessions([pageOneCopy, second, stalePageTwoCopy]);

    expect(result).toEqual([pageOneCopy, second]);
    expect(result[0]).toBe(pageOneCopy);
    expect(result[1]).toBe(second);
  });

  it('does not truncate rows after all pages are loaded', () => {
    const rows = [makeRow('1'), makeRow('2'), makeRow('3'), makeRow('4')];
    expect(mergeLiveSessions(rows)).toHaveLength(4);
  });

  it('Given order, When merged, Then preserves first-seen order', () => {
    const rows = [makeRow('3'), makeRow('1'), makeRow('2')];
    const result = mergeLiveSessions(rows);
    expect(result.map((r) => r.id)).toEqual(['3', '1', '2']);
  });
});
