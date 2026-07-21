import type {
  CacheKeepaliveRowFixture,
  CacheKeepaliveTurnFixture,
} from '../../components/principals/cache-keepalive/__fixtures__/cacheKeepaliveFixtures';
import { cacheKeepaliveRows } from '../../components/principals/cache-keepalive/__fixtures__/cacheKeepaliveFixtures';
import type {
  CacheKeepaliveDetail,
  CacheKeepaliveListResponse,
  CacheKeepaliveRow,
  CacheKeepaliveSummary,
  CacheKeepaliveTurn,
} from '../cacheKeepaliveApi';

const BASE_MS = 1_700_000_000_000;

export const cacheKeepaliveSummaryFixture: CacheKeepaliveSummary = {
  renewing_now: 2,
  sessions_last_5m: 5,
  renewals_fired: 128,
  cost_saved: 12.47,
};

export function apiRowFromFixture(
  fixture: CacheKeepaliveRowFixture,
  index: number,
): CacheKeepaliveRow {
  return {
    id: fixture.id,
    last_message_at_ms: BASE_MS - index * 60_000,
    state: fixture.state,
    ttl: fixture.ttl,
    attempts: fixture.attempts,
    max_attempts: fixture.maxAttempts,
    reason: fixture.reason,
    generation: 7,
    upstream: 'upstream-a',
    error: fixture.error,
    net_pnl: fixture.netPnl,
    session_key_hash: `hash-${fixture.id}`,
  };
}

function apiTurnFromFixture(
  turn: CacheKeepaliveTurnFixture,
): CacheKeepaliveTurn {
  return {
    turn_number: turn.number,
    renewals: turn.renewals,
    followed_up: turn.followedUp,
    label: 'agent-in-turn',
    time_ms: BASE_MS,
    pnl: turn.pnl,
    pending: turn.pending,
  };
}

export const cacheKeepaliveApiRows: CacheKeepaliveRow[] =
  cacheKeepaliveRows.map((row, index) => apiRowFromFixture(row, index));

export const cacheKeepaliveListResponseFixture: CacheKeepaliveListResponse = {
  summary: cacheKeepaliveSummaryFixture,
  rows: cacheKeepaliveApiRows,
  next_cursor: 'cursor-page-2',
};

export const cacheKeepaliveSummaryOnlyResponseFixture: CacheKeepaliveListResponse =
  {
    summary: cacheKeepaliveSummaryFixture,
    rows: [],
    next_cursor: null,
  };

export function apiDetailFromFixture(
  fixture: CacheKeepaliveRowFixture,
  index: number,
): CacheKeepaliveDetail {
  // Fixture stores turns oldest-first; the detail endpoint emits newest-first.
  const newestFirstTurns = [...fixture.turns].reverse();
  const newestTurn = fixture.turns[fixture.turns.length - 1];
  return {
    ...apiRowFromFixture(fixture, index),
    renewal_tokens: 20_000,
    total_avoided: 0.12,
    total_spent: 0.018,
    total_renewals: fixture.attempts ?? 0,
    is_last_pending: newestTurn.pending,
    turns: newestFirstTurns.map(apiTurnFromFixture),
    config_snapshot: {
      lead_5m: 270,
      lead_1h: 3570,
      max_renewals: 12,
      max_duration: 14_400,
      snapshot_bytes: 524_288,
    },
    raw_record: {
      session_key_hash: `hash-${fixture.id}`,
      principal_id: 'principal-1',
      upstream_id: 'upstream-a',
      generation: 7,
      renewal_count: fixture.attempts ?? 0,
      ttl: fixture.ttl,
      status: fixture.state,
      enqueue_state: null,
    },
  };
}

export const cacheKeepaliveRenewedDetailFixture: CacheKeepaliveDetail =
  apiDetailFromFixture(cacheKeepaliveRows[0], 0);

export const cacheKeepaliveNotTrackedDetailFixture: CacheKeepaliveDetail =
  apiDetailFromFixture(cacheKeepaliveRows[5], 5);
