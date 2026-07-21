import { describe, expect, it } from 'vitest';
import { cacheKeepaliveRows } from '../components/principals/cache-keepalive/__fixtures__/cacheKeepaliveFixtures';
import {
  CacheKeepaliveDetailSchema,
  CacheKeepaliveHorizonSchema,
  CacheKeepaliveListResponseSchema,
  CacheKeepaliveRowSchema,
  type CacheKeepaliveSessionsParams,
  CacheKeepaliveStatusFilterSchema,
  CacheKeepaliveSummarySchema,
  cacheKeepaliveSessionsPath,
} from './cacheKeepaliveApi';
import {
  apiRowFromFixture,
  cacheKeepaliveListResponseFixture,
  cacheKeepaliveNotTrackedDetailFixture,
  cacheKeepaliveRenewedDetailFixture,
  cacheKeepaliveSummaryFixture,
  cacheKeepaliveSummaryOnlyResponseFixture,
} from './test-utils/cacheKeepalive-fixtures';

describe('CacheKeepaliveSummarySchema', () => {
  it('decodes the four card metrics', () => {
    const parsed = CacheKeepaliveSummarySchema.parse(
      cacheKeepaliveSummaryFixture,
    );
    expect(parsed).toEqual({
      renewing_now: 2,
      sessions_last_5m: 5,
      renewals_fired: 128,
      cost_saved: 12.47,
    });
  });

  it('rejects a summary with a non-numeric metric', () => {
    const bad: unknown = {
      ...cacheKeepaliveSummaryFixture,
      cost_saved: '12.47',
    };
    expect(CacheKeepaliveSummarySchema.safeParse(bad).success).toBe(false);
  });
});

describe('CacheKeepaliveListResponseSchema', () => {
  it('decodes a list built from every frozen fixture row', () => {
    const parsed = CacheKeepaliveListResponseSchema.parse(
      cacheKeepaliveListResponseFixture,
    );
    expect(parsed.rows).toHaveLength(7);
    expect(parsed.rows.map((row) => row.state)).toEqual([
      'renewed',
      'scheduled',
      'capped',
      'capped',
      'expired',
      'not_tracked',
      'renewed',
    ]);
    expect(parsed.rows.map((row) => row.reason)).toEqual(
      cacheKeepaliveRows.map((row) => row.reason),
    );
    expect(parsed.next_cursor).toBe('cursor-page-2');
  });

  it('decodes the error-overlaps-reason row with error equal to reason', () => {
    const parsed = CacheKeepaliveListResponseSchema.parse(
      cacheKeepaliveListResponseFixture,
    );
    const errorRow = parsed.rows[6];
    expect(errorRow.error).toBe(errorRow.reason);
    expect(errorRow.error).toBe('renewal dispatch unavailable');
  });

  it('decodes the not_tracked decision row with null attempts and zero P&L', () => {
    const parsed = CacheKeepaliveListResponseSchema.parse(
      cacheKeepaliveListResponseFixture,
    );
    const notTracked = parsed.rows[5];
    expect(notTracked.state).toBe('not_tracked');
    expect(notTracked.attempts).toBeNull();
    expect(notTracked.net_pnl).toBe(0);
  });

  it('decodes the limit=0 summary-only shape with no rows', () => {
    const parsed = CacheKeepaliveListResponseSchema.parse(
      cacheKeepaliveSummaryOnlyResponseFixture,
    );
    expect(parsed.rows).toEqual([]);
    expect(parsed.next_cursor).toBeNull();
    expect(parsed.summary).toEqual(cacheKeepaliveSummaryFixture);
  });
});

describe('CacheKeepaliveRowSchema malformed input', () => {
  const validRow = apiRowFromFixture(cacheKeepaliveRows[0], 0);

  it('rejects an unknown capitalized state', () => {
    const bad: unknown = { ...validRow, state: 'Renewed' };
    expect(CacheKeepaliveRowSchema.safeParse(bad).success).toBe(false);
  });

  it('rejects a legacy warm-family status word', () => {
    const bad: unknown = { ...validRow, state: 'warmup' };
    expect(CacheKeepaliveRowSchema.safeParse(bad).success).toBe(false);
  });

  it('rejects a row missing the required net_pnl field', () => {
    const missingNetPnl: unknown = {
      id: 'x',
      last_message_at_ms: 1,
      state: 'renewed',
      ttl: '5m',
      attempts: 1,
      max_attempts: 12,
      reason: 'agent-in-turn (tool_use: `bash`)',
      generation: 7,
      upstream: null,
      error: null,
      session_key_hash: 'hash-x',
    };
    expect(CacheKeepaliveRowSchema.safeParse(missingNetPnl).success).toBe(
      false,
    );
  });

  it('rejects a row with an out-of-domain ttl', () => {
    const bad: unknown = { ...validRow, ttl: '30m' };
    expect(CacheKeepaliveRowSchema.safeParse(bad).success).toBe(false);
  });

  it('accepts a null ttl for a decision-backed row', () => {
    const nullTtl: unknown = { ...validRow, ttl: null };
    expect(CacheKeepaliveRowSchema.safeParse(nullTtl).success).toBe(true);
  });

  it('parses a numeric u64-like generation', () => {
    const numericGeneration: unknown = {
      ...validRow,
      generation: 4_294_967_297,
    };
    const parsed = CacheKeepaliveRowSchema.safeParse(numericGeneration);
    expect(parsed.success).toBe(true);
    if (parsed.success) {
      expect(parsed.data.generation).toBe(4_294_967_297);
    }
  });

  it('rejects a string generation', () => {
    const bad: unknown = { ...validRow, generation: 'gen-1' };
    expect(CacheKeepaliveRowSchema.safeParse(bad).success).toBe(false);
  });
});

describe('CacheKeepaliveDetailSchema', () => {
  it('decodes a renewed session detail with multi-turn P&L and config snapshot', () => {
    const parsed = CacheKeepaliveDetailSchema.parse(
      cacheKeepaliveRenewedDetailFixture,
    );
    expect(parsed.turns).toHaveLength(3);
    expect(parsed.turns.map((turn) => turn.turn_number)).toEqual([3, 2, 1]);
    expect(parsed.config_snapshot).toEqual({
      lead_5m: 270,
      lead_1h: 3570,
      max_renewals: 12,
      max_duration: 14_400,
      snapshot_bytes: 524_288,
    });
    expect(parsed.raw_record.principal_id).toBe('principal-1');
    expect(parsed.is_last_pending).toBe(true);
  });

  it('decodes a not_tracked detail with a null turn P&L', () => {
    const parsed = CacheKeepaliveDetailSchema.parse(
      cacheKeepaliveNotTrackedDetailFixture,
    );
    expect(parsed.state).toBe('not_tracked');
    expect(parsed.turns[0].pnl).toBeNull();
    expect(parsed.net_pnl).toBe(0);
  });

  it('rejects a detail whose state enum is malformed', () => {
    const bad: unknown = {
      ...cacheKeepaliveRenewedDetailFixture,
      state: 'refresh',
    };
    expect(CacheKeepaliveDetailSchema.safeParse(bad).success).toBe(false);
  });
});

describe('cacheKeepaliveSessionsPath', () => {
  it('emits limit=0 for the card summary call', () => {
    expect(cacheKeepaliveSessionsPath('p1', { limit: 0 })).toBe(
      '/admin/v1/principals/p1/cache-keepalive?limit=0',
    );
  });

  it('emits no query string when no params are given', () => {
    expect(cacheKeepaliveSessionsPath('p1', {})).toBe(
      '/admin/v1/principals/p1/cache-keepalive',
    );
  });

  it('encodes horizon, status, error, and cursor params', () => {
    expect(
      cacheKeepaliveSessionsPath('p1', {
        horizon: '7d',
        status: 'capped',
        error: true,
        cursor: 'cur-2',
      }),
    ).toBe(
      '/admin/v1/principals/p1/cache-keepalive?cursor=cur-2&horizon=7d&status=capped&error=true',
    );
  });

  it('throws at the request boundary on an invalid horizon', () => {
    const params: CacheKeepaliveSessionsParams = JSON.parse(
      '{"horizon":"banana"}',
    );
    expect(() => cacheKeepaliveSessionsPath('p1', params)).toThrow();
  });

  it('throws at the request boundary on a legacy warm-family status', () => {
    const params: CacheKeepaliveSessionsParams = JSON.parse(
      '{"status":"warmup"}',
    );
    expect(() => cacheKeepaliveSessionsPath('p1', params)).toThrow();
  });
});

describe('cache keepalive filter enums', () => {
  it('accepts every valid horizon value', () => {
    for (const horizon of ['24h', '7d', 'all']) {
      expect(CacheKeepaliveHorizonSchema.safeParse(horizon).success).toBe(true);
    }
  });

  it('rejects an out-of-domain horizon value', () => {
    expect(CacheKeepaliveHorizonSchema.safeParse('banana').success).toBe(false);
  });

  it('accepts all, every base state, and error as status filters', () => {
    for (const status of [
      'all',
      'renewed',
      'scheduled',
      'capped',
      'expired',
      'not_tracked',
      'error',
    ]) {
      expect(CacheKeepaliveStatusFilterSchema.safeParse(status).success).toBe(
        true,
      );
    }
  });

  it('rejects warm-family, capitalized, and arbitrary status filters', () => {
    for (const status of ['warmup', 'Renewed', 'banana', 'refresh']) {
      expect(CacheKeepaliveStatusFilterSchema.safeParse(status).success).toBe(
        false,
      );
    }
  });
});
