// Typed admin-web contract + client for the principal Cache keepalive
// summary/list/detail read endpoints. Schemas are the decode boundary
// (parse-don't-validate): every payload from the server is parsed into a
// typed value here, so hooks and components downstream never re-validate.
//
// State strings are the frozen wire values shared with the Rust view model
// and mirrored by the Todo 1 render contract
// (`components/principals/cache-keepalive/__fixtures__/cacheKeepaliveContract.ts`).
import * as z from 'zod';
import { getJson } from './api';

export const CACHE_KEEPALIVE_STATES = [
  'renewed',
  'scheduled',
  'capped',
  'expired',
  'not_tracked',
] as const;

export const CacheKeepaliveStateSchema = z.enum(CACHE_KEEPALIVE_STATES);
export type CacheKeepaliveState = z.infer<typeof CacheKeepaliveStateSchema>;

export const CacheKeepaliveTtlSchema = z.enum(['5m', '1h']);
export type CacheKeepaliveTtl = z.infer<typeof CacheKeepaliveTtlSchema>;

export const CacheKeepaliveHorizonSchema = z.enum(['24h', '7d', 'all']);
export type CacheKeepaliveHorizon = z.infer<typeof CacheKeepaliveHorizonSchema>;

export const CACHE_KEEPALIVE_STATUS_FILTERS = [
  'all',
  ...CACHE_KEEPALIVE_STATES,
  'error',
] as const;

export const CacheKeepaliveStatusFilterSchema = z.enum(
  CACHE_KEEPALIVE_STATUS_FILTERS,
);
export type CacheKeepaliveStatusFilter = z.infer<
  typeof CacheKeepaliveStatusFilterSchema
>;

// Card metric scope is fixed by the plan and does not track drawer horizon:
//   renewing_now      = current scheduled or mid-renewal sessions
//   sessions_last_5m  = sessions seen in the last 5 minutes only
//   renewals_fired    = all-time total renewals fired
//   cost_saved        = all-time net dollars saved after renewal spend
export const CacheKeepaliveSummarySchema = z.object({
  renewing_now: z.number(),
  sessions_last_5m: z.number(),
  renewals_fired: z.number(),
  cost_saved: z.number(),
});
export type CacheKeepaliveSummary = z.infer<typeof CacheKeepaliveSummarySchema>;

export const CacheKeepaliveRowSchema = z.object({
  id: z.string(),
  last_message_at_ms: z.number(),
  // Server provides `relative_time` only when it already has a shared
  // formatter; otherwise the frontend formats from `last_message_at_ms`.
  relative_time: z.string().optional(),
  state: CacheKeepaliveStateSchema,
  ttl: CacheKeepaliveTtlSchema.nullable(),
  attempts: z.number().nullable(),
  max_attempts: z.number(),
  reason: z.string(),
  // u64 keepalive generation from the storage record, not a display string.
  generation: z.number(),
  upstream: z.string().nullable(),
  error: z.string().nullable(),
  net_pnl: z.number(),
  // Opaque stable-sort tiebreak id (plan: `session_key_hash ASC`); the
  // cursor encodes it together with `last_message_at_ms`.
  session_key_hash: z.string(),
});
export type CacheKeepaliveRow = z.infer<typeof CacheKeepaliveRowSchema>;

export const CacheKeepaliveListResponseSchema = z.object({
  summary: CacheKeepaliveSummarySchema,
  rows: z.array(CacheKeepaliveRowSchema),
  next_cursor: z.string().nullable(),
});
export type CacheKeepaliveListResponse = z.infer<
  typeof CacheKeepaliveListResponseSchema
>;

export const CacheKeepaliveTurnSchema = z.object({
  turn_number: z.number(),
  renewals: z.number(),
  followed_up: z.boolean(),
  label: z.string(),
  time_ms: z.number(),
  // `not_tracked` turns carry no P&L, so `pnl` is null there.
  pnl: z.number().nullable(),
  pending: z.boolean(),
});
export type CacheKeepaliveTurn = z.infer<typeof CacheKeepaliveTurnSchema>;

export const CacheKeepaliveConfigSnapshotSchema = z.object({
  lead_5m: z.number(),
  lead_1h: z.number(),
  max_renewals: z.number(),
  max_duration: z.number(),
  snapshot_bytes: z.number(),
});
export type CacheKeepaliveConfigSnapshot = z.infer<
  typeof CacheKeepaliveConfigSnapshotSchema
>;

export const CacheKeepaliveRawRecordSchema = z.object({
  session_key_hash: z.string(),
  principal_id: z.string(),
  upstream_id: z.string().nullable(),
  generation: z.number(),
  renewal_count: z.number(),
  ttl: CacheKeepaliveTtlSchema.nullable(),
  status: z.string(),
  enqueue_state: z.string().nullable().optional(),
});
export type CacheKeepaliveRawRecord = z.infer<
  typeof CacheKeepaliveRawRecordSchema
>;

export const CacheKeepaliveDetailSchema = CacheKeepaliveRowSchema.extend({
  renewal_tokens: z.number(),
  total_avoided: z.number(),
  total_spent: z.number(),
  total_renewals: z.number(),
  is_last_pending: z.boolean(),
  turns: z.array(CacheKeepaliveTurnSchema),
  // Persisted or derived schedule-time config; historical rows may lack it.
  config_snapshot: CacheKeepaliveConfigSnapshotSchema.nullable(),
  raw_record: CacheKeepaliveRawRecordSchema,
});
export type CacheKeepaliveDetail = z.infer<typeof CacheKeepaliveDetailSchema>;

export interface CacheKeepaliveSessionsParams {
  // `limit = 0` fetches the four card metrics only, with an empty `rows`.
  limit?: number;
  cursor?: string;
  horizon?: CacheKeepaliveHorizon;
  status?: CacheKeepaliveStatusFilter;
  error?: boolean;
}

export type CacheKeepaliveSessionsFilters = Omit<
  CacheKeepaliveSessionsParams,
  'cursor'
>;

export function cacheKeepaliveSessionsPath(
  principalId: string,
  params: CacheKeepaliveSessionsParams,
): string {
  const searchParams = new URLSearchParams();
  if (params.limit !== undefined) {
    searchParams.set('limit', String(params.limit));
  }
  if (params.cursor) {
    searchParams.set('cursor', params.cursor);
  }
  if (params.horizon !== undefined) {
    searchParams.set(
      'horizon',
      CacheKeepaliveHorizonSchema.parse(params.horizon),
    );
  }
  if (params.status !== undefined) {
    searchParams.set(
      'status',
      CacheKeepaliveStatusFilterSchema.parse(params.status),
    );
  }
  if (params.error) {
    searchParams.set('error', 'true');
  }
  const qs = searchParams.toString();
  return `/admin/v1/principals/${principalId}/cache-keepalive${qs ? `?${qs}` : ''}`;
}

export async function getCacheKeepaliveSessions(
  principalId: string,
  params: CacheKeepaliveSessionsParams = {},
): Promise<CacheKeepaliveListResponse> {
  const res = await getJson<unknown>(
    cacheKeepaliveSessionsPath(principalId, params),
  );
  return CacheKeepaliveListResponseSchema.parse(res);
}

export async function getCacheKeepaliveSessionDetail(
  principalId: string,
  sessionId: string,
): Promise<CacheKeepaliveDetail> {
  const res = await getJson<unknown>(
    `/admin/v1/principals/${principalId}/cache-keepalive/${encodeURIComponent(sessionId)}`,
  );
  return CacheKeepaliveDetailSchema.parse(res);
}
