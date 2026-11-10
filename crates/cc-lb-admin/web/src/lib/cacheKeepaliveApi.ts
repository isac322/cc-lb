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
  'cancelled',
  'failed',
  'not_tracked',
] as const;

export const CacheKeepaliveStateSchema = z.enum(CACHE_KEEPALIVE_STATES);
export type CacheKeepaliveState = z.infer<typeof CacheKeepaliveStateSchema>;

export const CacheKeepaliveTtlSchema = z.enum(['5m', '1h', '30m', '12h']);
export type CacheKeepaliveTtl = z.infer<typeof CacheKeepaliveTtlSchema>;

export const CacheKeepaliveHorizonSchema = z.enum([
  '1h',
  '6h',
  '24h',
  '7d',
  'all',
]);
export type CacheKeepaliveHorizon = z.infer<typeof CacheKeepaliveHorizonSchema>;

export const CACHE_KEEPALIVE_STATUS_FILTERS = [
  'all',
  'visible',
  'tracked',
  'renewed',
  'scheduled',
  'capped',
  'expired',
  'cancelled',
  'failed',
  'not_tracked',
  'error',
] as const;

export const CacheKeepaliveStatusFilterSchema = z.enum(
  CACHE_KEEPALIVE_STATUS_FILTERS,
);
export type CacheKeepaliveStatusFilter = z.infer<
  typeof CacheKeepaliveStatusFilterSchema
>;

// Summary scope remains explicit: retained history is not the current cycle.
export const CacheKeepaliveEvidenceStatusSchema = z.enum([
  'observed',
  'none',
  'pending',
  'unknown',
] as const);
export type CacheKeepaliveEvidenceStatus = z.infer<
  typeof CacheKeepaliveEvidenceStatusSchema
>;

export const CacheKeepaliveSummarySchema = z.object({
  renewing_now: z.number(),
  sessions_last_5m: z.number(),
  retained_successful_hits: z.number(),
  cost_saved: z.number().nullable(),
  total_spent: z.number(),
  total_avoided: z.number().nullable(),
  unlisted_sessions: z.number(),
  unlisted_renewals: z.number(),
  unlisted_spent: z.number(),
  // Null when a deleted session's renewal has an unknown cost.
  unlisted_net: z.number().nullable(),
  evidence_status: CacheKeepaliveEvidenceStatusSchema,
  unknown_cycles: z.number(),
  retained_from_ms: z.number().nullable(),
});
export type CacheKeepaliveSummary = z.infer<typeof CacheKeepaliveSummarySchema>;

export const CacheKeepaliveGroupSchema = z.object({
  key: z.string(),
  label: z.string(),
  kind: z.enum(['tool', 'state']),
  sessions: z.number(),
  net_pnl: z.number().nullable(),
  total_spent: z.number(),
  total_avoided: z.number().nullable(),
});
export type CacheKeepaliveGroup = z.infer<typeof CacheKeepaliveGroupSchema>;

export const CacheKeepaliveRowSchema = z.object({
  id: z.string(),
  last_message_at_ms: z.number(),
  relative_time: z.string().optional(),
  state: CacheKeepaliveStateSchema,
  is_active: z.boolean(),
  ttl: CacheKeepaliveTtlSchema.nullable(),
  attempts: z.number().nullable(),
  max_attempts: z.number(),
  reason: z.string(),
  display_reason: z.string().nullable().optional(),
  generation: z.number(),
  upstream: z.string().nullable(),
  error: z.string().nullable(),
  net_pnl: z.number().nullable(),
  total_spent: z.number(),
  total_avoided: z.number().nullable(),
  evidence_status: CacheKeepaliveEvidenceStatusSchema,
  session_key_hash: z.string(),
  retained_renewals: z.number().optional(),
  group_key: z.string(),
  group_label: z.string(),
  group_kind: z.enum(['tool', 'state']),
});
export type CacheKeepaliveRow = z.infer<typeof CacheKeepaliveRowSchema>;
export const CacheKeepaliveListResponseSchema = z.object({
  summary: CacheKeepaliveSummarySchema.nullable(),
  groups: z.array(CacheKeepaliveGroupSchema).nullable(),
  rows: z.array(CacheKeepaliveRowSchema),
  next_cursor: z.string().nullable(),
});
export type CacheKeepaliveListResponse = z.infer<
  typeof CacheKeepaliveListResponseSchema
>;

export const CacheKeepaliveTurnSchema = z.object({
  turn_number: z.number(),
  renewals: z.number(),
  followed_up: z.boolean().nullable(),
  label: z.string(),
  time_ms: z.number(),
  pnl: z.number().nullable(),
  pending: z.boolean(),
  evidence_status: CacheKeepaliveEvidenceStatusSchema,
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

export const CacheKeepalivePulseSchema = z.object({
  pulse_number: z.number(),
  time_ms: z.number(),
  offset_sec: z.number(),
  since_prev_renewal_sec: z.number().nullable(),
  spent_usd: z.number().nullable(),
  hit: z.boolean(),
});
export type CacheKeepalivePulse = z.infer<typeof CacheKeepalivePulseSchema>;

export const CacheKeepaliveGapSchema = z.object({
  cycle_id: z.string().nullable(),
  pause_at_ms: z.number(),
  cached_tokens: z.number(),
  pulses: z.array(CacheKeepalivePulseSchema),
  ending_kind: z.enum([
    'followup_hit',
    'followup_unknown',
    'scheduled_waiting',
    'capped_no_followup',
    'ttl_expired_no_followup',
  ]),
  ending_at_ms: z.number(),
  total_duration_sec: z.number(),
  since_last_renewal_sec: z.number().nullable(),
  spent_usd: z.number(),
  avoided_usd: z.number().nullable(),
  net_pnl_usd: z.number().nullable(),
});
export type CacheKeepaliveGap = z.infer<typeof CacheKeepaliveGapSchema>;

export const CacheKeepaliveDetailSchema = CacheKeepaliveRowSchema.extend({
  renewal_tokens: z.number(),
  total_renewals_current_cycle: z.number(),
  is_last_pending: z.boolean(),
  turns: z.array(CacheKeepaliveTurnSchema),
  config_snapshot: CacheKeepaliveConfigSnapshotSchema.nullable(),
  raw_record: CacheKeepaliveRawRecordSchema,
  gaps: z.array(CacheKeepaliveGapSchema).optional(),
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
  horizon: CacheKeepaliveHorizon = 'all',
): Promise<CacheKeepaliveDetail> {
  const searchParams = new URLSearchParams({
    id: sessionId,
    horizon: CacheKeepaliveHorizonSchema.parse(horizon),
  });
  const path = `/admin/v1/principals/${principalId}/cache-keepalive/detail?${searchParams.toString()}`;
  const res = await getJson<unknown>(path);
  return CacheKeepaliveDetailSchema.parse(res);
}
