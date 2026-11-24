import type { CacheKeepaliveRow } from '../../../lib/cacheKeepaliveApi';

type SessionState = CacheKeepaliveRow['state'];
export type StateTone = 'live' | 'ok' | 'neutral' | 'danger';

// Describe the client-visible lifecycle rather than the scheduler's internal state.
const SESSION_STATE_DISPLAY: Record<
  SessionState,
  { label: string; tone: StateTone }
> = {
  scheduled: { label: 'Warm', tone: 'live' },
  renewed: { label: 'Warm', tone: 'live' },
  in_request: { label: 'In use', tone: 'neutral' },
  cancelled: { label: 'Returned', tone: 'ok' },
  capped: { label: 'At limit', tone: 'neutral' },
  expired: { label: 'Expired', tone: 'neutral' },
  failed: { label: 'Failed', tone: 'danger' },
  not_tracked: { label: 'Not tracked', tone: 'neutral' },
};

export function sessionStateDisplay(state: SessionState) {
  return SESSION_STATE_DISPLAY[state];
}
