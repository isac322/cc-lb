import type { CacheKeepaliveState } from './cacheKeepaliveContract';

export type CacheKeepaliveTurnFixture = Readonly<{
  number: number;
  renewals: number;
  followedUp: boolean;
  pending: boolean;
  pnl: number | null;
}>;

export type CacheKeepaliveRowFixture = Readonly<{
  id: string;
  state: CacheKeepaliveState;
  reason: string;
  error: string | null;
  ttl: '5m' | '1h';
  attempts: number | null;
  maxAttempts: number;
  netPnl: number;
  turns: readonly CacheKeepaliveTurnFixture[];
}>;

export const cacheKeepaliveRows = [
  {
    id: 'a1f39c2b7e04',
    state: 'renewed',
    reason: 'agent-in-turn (tool_use: `bash`)',
    error: null,
    ttl: '5m',
    attempts: 8,
    maxAttempts: 12,
    netPnl: 0.102,
    turns: [
      { number: 1, renewals: 2, followedUp: true, pending: false, pnl: 0.063 },
      { number: 2, renewals: 3, followedUp: true, pending: false, pnl: 0.057 },
      { number: 3, renewals: 3, followedUp: false, pending: true, pnl: -0.018 },
    ],
  },
  {
    id: '7b204de1c83f',
    state: 'scheduled',
    reason: 'agent-in-turn (tool_use: `edit_file`) — first renewal in 4m 30s',
    error: null,
    ttl: '5m',
    attempts: 0,
    maxAttempts: 12,
    netPnl: 0,
    turns: [
      { number: 1, renewals: 0, followedUp: false, pending: true, pnl: 0 },
    ],
  },
  {
    id: 'f4b71a0c9d52',
    state: 'capped',
    reason: 'max renewals reached',
    error: null,
    ttl: '5m',
    attempts: 12,
    maxAttempts: 12,
    netPnl: 0.0936,
    turns: [
      { number: 1, renewals: 2, followedUp: true, pending: false, pnl: 0.076 },
      { number: 2, renewals: 4, followedUp: true, pending: false, pnl: 0.061 },
      {
        number: 3,
        renewals: 6,
        followedUp: false,
        pending: false,
        pnl: -0.043,
      },
    ],
  },
  {
    id: 'capped-max-duration-4h',
    state: 'capped',
    reason: 'max duration reached (4h)',
    error: null,
    ttl: '1h',
    attempts: 6,
    maxAttempts: 12,
    netPnl: -0.0576,
    turns: [
      {
        number: 1,
        renewals: 6,
        followedUp: false,
        pending: false,
        pnl: -0.0576,
      },
    ],
  },
  {
    id: '0b33e9f71a2c',
    state: 'expired',
    reason: 'TTL expired before follow-up',
    error: null,
    ttl: '1h',
    attempts: 8,
    maxAttempts: 12,
    netPnl: 0.1152,
    turns: [
      { number: 1, renewals: 3, followedUp: true, pending: false, pnl: 0.1632 },
      {
        number: 2,
        renewals: 5,
        followedUp: false,
        pending: false,
        pnl: -0.048,
      },
    ],
  },
  {
    id: 'decision-2d8077e9a1c4',
    state: 'not_tracked',
    reason: 'user turn (stop_reason=end_turn)',
    error: null,
    ttl: '5m',
    attempts: null,
    maxAttempts: 12,
    netPnl: 0,
    turns: [
      { number: 1, renewals: 0, followedUp: false, pending: false, pnl: null },
    ],
  },
  {
    id: 'error-overlaps-reason',
    state: 'renewed',
    reason: 'renewal dispatch unavailable',
    error: 'renewal dispatch unavailable',
    ttl: '5m',
    attempts: 1,
    maxAttempts: 12,
    netPnl: -0.006,
    turns: [
      {
        number: 1,
        renewals: 1,
        followedUp: false,
        pending: true,
        pnl: -0.006,
      },
    ],
  },
] as const satisfies readonly CacheKeepaliveRowFixture[];
