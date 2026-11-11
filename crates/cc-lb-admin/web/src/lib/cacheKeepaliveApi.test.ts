import { describe, expect, it } from 'vitest';
import { cacheKeepaliveRows } from '../components/principals/cache-keepalive/__fixtures__/cacheKeepaliveFixtures';
import vocabulary from './__contract__/cache_keepalive_vocabulary.json';
import {
  CACHE_KEEPALIVE_STATES,
  CACHE_KEEPALIVE_STATUS_FILTERS,
  CacheKeepaliveCancelReasonSchema,
  CacheKeepaliveDetailSchema,
  CacheKeepaliveGapSchema,
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

describe('Cache keepalive canonical wire vocabulary', () => {
  it('accepts exactly the canonical Rust cancellation reasons', () => {
    expect(CacheKeepaliveCancelReasonSchema.options).toEqual(
      vocabulary.cancel_reasons,
    );
  });

  it('keeps state and filter sets equal to the server contract', () => {
    expect([...CACHE_KEEPALIVE_STATES].sort()).toEqual(
      [...vocabulary.states].sort(),
    );
    expect([...CACHE_KEEPALIVE_STATUS_FILTERS].sort()).toEqual(
      [...vocabulary.status_filters].sort(),
    );
  });

  it.each(vocabulary.cancel_reasons)(
    'decodes %s in list and detail wire payloads',
    (cancelReason) => {
      const row = {
        ...apiRowFromFixture(cacheKeepaliveRows[0], 0),
        state: 'cancelled',
        is_active: false,
        cancel_reason: cancelReason,
        reason: `Cancelled: ${cancelReason.replaceAll('_', ' ')}`,
      };
      const listWire = JSON.parse(
        JSON.stringify({
          summary: null,
          groups: null,
          rows: [row],
          next_cursor: null,
        }),
      );
      const detailWire = JSON.parse(
        JSON.stringify({
          ...cacheKeepaliveRenewedDetailFixture,
          ...row,
          raw_record: {
            ...cacheKeepaliveRenewedDetailFixture.raw_record,
            status: 'terminal',
          },
        }),
      );

      expect(
        CacheKeepaliveListResponseSchema.parse(listWire).rows[0].cancel_reason,
      ).toBe(cancelReason);
      expect(CacheKeepaliveDetailSchema.parse(detailWire).cancel_reason).toBe(
        cancelReason,
      );
    },
  );

  it('still rejects an unknown cancellation reason on both endpoints', () => {
    const row = {
      ...apiRowFromFixture(cacheKeepaliveRows[0], 0),
      cancel_reason: 'unknown_reason',
    };
    expect(
      CacheKeepaliveListResponseSchema.safeParse({
        summary: null,
        groups: null,
        rows: [row],
        next_cursor: null,
      }).success,
    ).toBe(false);
    expect(
      CacheKeepaliveDetailSchema.safeParse({
        ...cacheKeepaliveRenewedDetailFixture,
        ...row,
      }).success,
    ).toBe(false);
  });
});

describe('CacheKeepaliveSummarySchema', () => {
  it('decodes the four card metrics', () => {
    const parsed = CacheKeepaliveSummarySchema.parse(
      cacheKeepaliveSummaryFixture,
    );
    expect(parsed).toEqual({
      renewing_now: 2,
      sessions_last_5m: 5,
      retained_successful_hits: 96,
      cost_saved: 12.47,
      total_spent: 4.18,
      total_avoided: 16.65,
      unlisted_sessions: 0,
      unlisted_renewals: 0,
      unlisted_spent: 0,
      unlisted_net: 0,
      evidence_status: 'observed',
      unknown_cycles: 0,
      retained_from_ms: 1_699_395_200_000,
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
  it('decodes nullable summary and groups keys on overview and cursor payloads', () => {
    const limitZeroPayload = {
      summary: cacheKeepaliveSummaryFixture,
      groups: null,
      rows: [],
      next_cursor: null,
    };
    const cursorPayload = {
      summary: null,
      groups: null,
      rows: [apiRowFromFixture(cacheKeepaliveRows[0], 0)],
      next_cursor: 'cursor-page-2',
    };

    expect(
      CacheKeepaliveListResponseSchema.parse(limitZeroPayload),
    ).toMatchObject({
      groups: null,
      rows: [],
      next_cursor: null,
    });
    expect(CacheKeepaliveListResponseSchema.parse(cursorPayload)).toMatchObject(
      {
        summary: null,
        groups: null,
        next_cursor: 'cursor-page-2',
      },
    );
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

  it('accepts OpenAI retention TTLs and rejects unknown values', () => {
    expect(
      CacheKeepaliveRowSchema.safeParse({ ...validRow, ttl: '30m' }).success,
    ).toBe(true);
    expect(
      CacheKeepaliveRowSchema.safeParse({ ...validRow, ttl: '12h' }).success,
    ).toBe(true);
    expect(
      CacheKeepaliveRowSchema.safeParse({ ...validRow, ttl: '24h' }).success,
    ).toBe(false);
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

  it('preserves known and unknown cycles across the selected-horizon renewal table', () => {
    const cycles = [
      '00000000-0000-0000-0000-000000000032',
      '00000000-0000-0000-0000-000000000031',
      null,
    ];
    const parsed = CacheKeepaliveDetailSchema.parse({
      ...cacheKeepaliveRenewedDetailFixture,
      total_renewals_current_cycle: 0,
      is_last_pending: false,
      turns: cacheKeepaliveRenewedDetailFixture.turns.map((turn, index) => ({
        ...turn,
        cycle_id: cycles[index],
        turn_number: 1,
        pending: false,
      })),
    });
    expect(parsed.turns.map((turn) => turn.cycle_id)).toEqual(cycles);
    expect(parsed.turns.map((turn) => turn.turn_number)).toEqual([1, 1, 1]);
    expect(parsed.total_renewals_current_cycle).toBe(0);
    expect(parsed.is_last_pending).toBe(false);
  });

  it('preserves mixed gap outcome counts without replacing the legacy evidence status', () => {
    const endings = [
      'followup_hit',
      'followup_hit',
      ...Array.from({ length: 8 }, () => 'ttl_expired_no_followup' as const),
      'followup_unknown',
    ] as const;
    const gaps = endings.map((ending_kind, index) => ({
      cycle_id: `00000000-0000-0000-0000-${(index + 1).toString(16).padStart(12, '0')}`,
      pause_at_ms: 1_700_000_000_000 + index * 300_000,
      cached_tokens: 20_000,
      pulses: [
        {
          pulse_number: 1,
          time_ms: 1_700_000_270_000 + index * 300_000,
          offset_sec: 270,
          since_prev_renewal_sec: null,
          spent_usd: 0.001,
          hit: true,
        },
      ],
      ending_kind,
      ending_at_ms: 1_700_000_300_000 + index * 300_000,
      total_duration_sec: 300,
      since_last_renewal_sec: 30,
      spent_usd: 0.001,
      avoided_usd: null,
      net_pnl_usd: null,
    }));
    const parsed = CacheKeepaliveDetailSchema.parse({
      ...cacheKeepaliveRenewedDetailFixture,
      state: 'cancelled',
      is_active: false,
      evidence_status: 'unknown',
      total_spent: 0.011,
      total_avoided: null,
      net_pnl: null,
      total_renewals_current_cycle: 0,
      is_last_pending: false,
      gaps,
      turns: gaps.map((gap) => ({
        cycle_id: gap.cycle_id,
        turn_number: 1,
        renewals: 1,
        followed_up:
          gap.ending_kind === 'followup_unknown'
            ? null
            : gap.ending_kind === 'followup_hit',
        label: 'agent-in-turn',
        time_ms: gap.pulses[0].time_ms,
        pnl: null,
        pending: false,
        evidence_status:
          gap.ending_kind === 'followup_unknown' ? 'unknown' : 'observed',
      })),
      follow_up_outcomes: {
        observed: 2,
        pending: 0,
        no_follow_up: 8,
        unknown: 1,
      },
    });
    expect(parsed.follow_up_outcomes).toEqual({
      observed: 2,
      pending: 0,
      no_follow_up: 8,
      unknown: 1,
    });
    expect(
      Object.values(parsed.follow_up_outcomes).reduce(
        (sum, count) => sum + count,
        0,
      ),
    ).toBe(parsed.gaps?.length);
    expect(parsed.evidence_status).toBe('unknown');
  });

  it('requires explicit detail outcome counts instead of inferring a header summary', () => {
    expect(
      CacheKeepaliveDetailSchema.safeParse({
        ...cacheKeepaliveRenewedDetailFixture,
        follow_up_outcomes: undefined,
      }).success,
    ).toBe(false);
  });

  it('decodes a not_tracked detail with a null turn P&L', () => {
    const parsed = CacheKeepaliveDetailSchema.parse(
      cacheKeepaliveNotTrackedDetailFixture,
    );
    expect(parsed.state).toBe('not_tracked');
    expect(parsed.turns[0].pnl).toBeNull();
    expect(parsed.net_pnl).toBe(0);
  });
  it.each([
    {
      name: 'terminal hit loss',
      evidence: 'observed',
      renewals: 1,
      spent: 0.006,
      avoided: 0,
      pnl: -0.006,
      followedUp: false,
      pending: false,
    },
    {
      name: 'terminal miss loss',
      evidence: 'observed',
      renewals: 0,
      spent: 0.015,
      avoided: 0,
      pnl: -0.015,
      followedUp: false,
      pending: false,
    },
    {
      name: 'persisted zero-cost follow-up',
      evidence: 'observed',
      renewals: 1,
      spent: 0,
      avoided: 0.075,
      pnl: 0.075,
      followedUp: true,
      pending: false,
    },
    {
      name: 'unknown evidence',
      evidence: 'unknown',
      renewals: 1,
      spent: 0.021,
      avoided: null,
      pnl: null,
      followedUp: null,
      pending: false,
    },
    {
      name: 'pending follow-up',
      evidence: 'pending',
      renewals: 1,
      spent: 0.006,
      avoided: null,
      pnl: null,
      followedUp: null,
      pending: true,
    },
  ])('preserves $name economics and renewal count', (scenario) => {
    const parsed = CacheKeepaliveDetailSchema.parse({
      ...cacheKeepaliveRenewedDetailFixture,
      state: scenario.pending ? 'renewed' : 'expired',
      is_active: scenario.pending,
      attempts: scenario.renewals,
      total_spent: scenario.spent,
      total_avoided: scenario.avoided,
      net_pnl: scenario.pnl,
      evidence_status: scenario.evidence,
      total_renewals_current_cycle: scenario.renewals,
      is_last_pending: scenario.pending,
      turns: [
        {
          cycle_id: null,
          turn_number: 1,
          renewals: scenario.renewals,
          followed_up: scenario.followedUp,
          label: 'agent-in-turn',
          time_ms: 1_700_000_000_000,
          pnl: scenario.pnl,
          pending: scenario.pending,
          evidence_status: scenario.evidence,
        },
      ],
      raw_record: {
        ...cacheKeepaliveRenewedDetailFixture.raw_record,
        renewal_count: scenario.renewals,
        status: scenario.pending ? 'renewed' : 'expired',
      },
    });

    expect(parsed.total_spent).toBe(scenario.spent);
    expect(parsed.total_avoided).toBe(scenario.avoided);
    expect(parsed.net_pnl).toBe(scenario.pnl);
    expect(parsed.evidence_status).toBe(scenario.evidence);
    expect(parsed.total_renewals_current_cycle).toBe(scenario.renewals);
    expect(parsed.turns[0]).toMatchObject({
      renewals: scenario.renewals,
      followed_up: scenario.followedUp,
      pnl: scenario.pnl,
      pending: scenario.pending,
      evidence_status: scenario.evidence,
    });
  });

  it('preserves retained economics when a replacement cycle has no turns', () => {
    const parsed = CacheKeepaliveDetailSchema.parse({
      ...cacheKeepaliveRenewedDetailFixture,
      state: 'scheduled',
      attempts: 0,
      total_spent: 0.021,
      total_avoided: 0.075,
      net_pnl: 0.054,
      evidence_status: 'observed',
      total_renewals_current_cycle: 0,
      is_last_pending: false,
      turns: [],
      raw_record: {
        ...cacheKeepaliveRenewedDetailFixture.raw_record,
        renewal_count: 0,
        status: 'scheduled',
      },
    });

    expect(parsed.total_spent).toBe(0.021);
    expect(parsed.total_avoided).toBe(0.075);
    expect(parsed.net_pnl).toBe(0.054);
    expect(parsed.evidence_status).toBe('observed');
    expect(parsed.is_active).toBe(true);
    expect(parsed.is_last_pending).toBe(false);
    expect(parsed.total_renewals_current_cycle).toBe(0);
    expect(parsed.turns).toEqual([]);
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

  it('encodes the in-request status filter without treating it as scheduled', () => {
    expect(cacheKeepaliveSessionsPath('p1', { status: 'in_request' })).toBe(
      '/admin/v1/principals/p1/cache-keepalive?status=in_request',
    );
  });

  it('encodes the tracked-only default status filter', () => {
    expect(
      cacheKeepaliveSessionsPath('p1', {
        horizon: '24h',
        status: 'tracked',
      }),
    ).toBe(
      '/admin/v1/principals/p1/cache-keepalive?horizon=24h&status=tracked',
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
    for (const horizon of ['1h', '6h', '24h', '7d', 'all']) {
      expect(CacheKeepaliveHorizonSchema.safeParse(horizon).success).toBe(true);
    }
  });

  it('rejects an out-of-domain horizon value', () => {
    expect(CacheKeepaliveHorizonSchema.safeParse('banana').success).toBe(false);
  });

  it('accepts all, visible, tracked, every base state, and error as status filters', () => {
    for (const status of [
      'all',
      'visible',
      'tracked',
      'renewed',
      'scheduled',
      'in_request',
      'capped',
      'expired',
      'cancelled',
      'failed',
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
describe('cache keepalive wire additions', () => {
  it('decodes an in-request row and detail independently of the stored terminal marker', () => {
    const detail = {
      ...cacheKeepaliveRenewedDetailFixture,
      state: 'in_request',
      is_active: false,
      cancel_reason: null,
      reason: 'A real request is in progress',
      error: null,
      raw_record: {
        ...cacheKeepaliveRenewedDetailFixture.raw_record,
        status: 'terminal',
      },
    };
    expect(CacheKeepaliveRowSchema.parse(detail)).toMatchObject({
      state: 'in_request',
      is_active: false,
      cancel_reason: null,
      error: null,
    });
    expect(CacheKeepaliveDetailSchema.parse(detail).raw_record?.status).toBe(
      'terminal',
    );
    expect(
      CacheKeepaliveListResponseSchema.parse({
        ...cacheKeepaliveListResponseFixture,
        rows: [detail],
      }).rows[0].state,
    ).toBe('in_request');
  });

  it('retains the neutral incomplete-request reason on an expired projection', () => {
    const row = CacheKeepaliveRowSchema.parse({
      ...apiRowFromFixture(cacheKeepaliveRows[0], 0),
      state: 'expired',
      is_active: false,
      cancel_reason: null,
      reason: 'Request did not complete before its deadline',
      error: null,
    });
    expect(row.state).toBe('expired');
    expect(row.reason).toBe('Request did not complete before its deadline');
    expect(row.error).toBeNull();
    expect(row.cancel_reason).toBeNull();
  });

  it('accepts cancelled and failed row states', () => {
    const row = apiRowFromFixture(cacheKeepaliveRows[0], 0);
    expect(
      CacheKeepaliveRowSchema.parse({ ...row, state: 'cancelled' }).state,
    ).toBe('cancelled');
    expect(
      CacheKeepaliveRowSchema.parse({ ...row, state: 'failed' }).state,
    ).toBe('failed');
  });

  it('accepts followup_unknown gap endings', () => {
    const gap = {
      cycle_id: null,
      pause_at_ms: 1,
      cached_tokens: 1,
      pulses: [],
      ending_kind: 'followup_unknown',
      ending_at_ms: 2,
      total_duration_sec: 1,
      since_last_renewal_sec: null,
      spent_usd: 0.01,
      avoided_usd: 0,
      net_pnl_usd: -0.01,
    };
    expect(CacheKeepaliveGapSchema.parse(gap).ending_kind).toBe(
      'followup_unknown',
    );
  });
});
