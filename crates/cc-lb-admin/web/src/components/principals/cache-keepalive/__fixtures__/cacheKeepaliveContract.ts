export const CACHE_KEEPALIVE_STATES = [
  'renewed',
  'scheduled',
  'capped',
  'expired',
  'cancelled',
  'failed',
  'not_tracked',
] as const;

export type CacheKeepaliveState = (typeof CACHE_KEEPALIVE_STATES)[number];

export const cacheKeepaliveCardCopy = {
  title: 'Cache keepalive',
  tooltip:
    'OpenAI Responses keepalive is for GPT-5.6+ models: 30m sessions may renew during idle gaps, while 12h retention is recorded without read re-anchoring.',
  metrics: [
    ['Renewing now', 'current scheduled or mid-renewal'],
    ['Sessions (last 5m)', 'seen in last five minutes'],
    ['Successful renewals · 24h', 'renewals that hit the cache'],
    ['Cost saved · 24h', 'net, after renewal spend'],
  ],
  caption:
    'Keeps supported prompt caches warm during idle gaps. OpenAI Responses support is limited to GPT-5.6+ models: 30m sessions may renew, while 12h retention is recorded without read re-anchoring.',
  offLine:
    'Off. Turn on to renew supported prompt-cache sessions during idle gaps. OpenAI Responses 12h retention is recorded without read re-anchoring.',
  actions: ['Sessions', 'Settings'],
} as const;

export const cacheKeepaliveDrawerCopy = {
  sessionsTitle: 'Cache keepalive sessions',
  sessionDetailTitle: 'Session detail',
  closeDetail: 'Close',
  backDetail: 'Back',
  closeHistoryAriaLabel: 'Close history',
  closeSettingsAriaLabel: 'Close settings',
  horizons: ['1h', '6h', '24h', '7d', 'All'] as const,
  filters: [
    'All statuses',
    'Renewed',
    'Scheduled',
    'Capped',
    'Expired',
    'Cancelled',
    'Failed',
    'Not tracked',
    'Error',
  ],
  loading: 'Loading sessions…',
  allShown: 'All sessions shown',
  exhausted: '· No more sessions ·',
  overviewFields: [
    'Session ID',
    'Upstream',
    'TTL',
    'Generation',
    'First seen',
    'Total renewals',
  ],
  turnTimeline: [
    'Message-by-message',
    'Current turn · Live',
    'Final turn',
    'waiting for follow-up',
    'cache used by follow-up',
    'follow-up unknown',
    'no follow-up (loss)',
  ],
  configCollapsible: 'Config in effect at schedule time',
  rawCollapsible: 'Raw session record',
} as const;

export const cacheKeepaliveSettingsCopy = {
  title: 'Cache keepalive settings',
  labels: [
    'Enabled',
    'Renewal lead time · 5m TTL',
    'Renewal lead time · 1h TTL',
    'Max renewals per session',
    'Max total duration',
    'Snapshot max bytes',
    'extra_wait_for_user_tools',
    'treat end_turn as ambiguous',
    'LLM Judge',
  ],
  helperCopy: [
    '→ renews 30s before the 5m cache expires',
    '= 4h',
    '= 512 KiB',
    'Add tool...',
    'Reserved for a future release',
    'Reset',
    'Save changes',
  ],
} as const;

export const cacheKeepaliveAnimationContract = {
  metricFlash: '1s ease-out',
  rowTransition:
    'transform 400ms ease, background-color 150ms ease, border-color 150ms ease',
  riseTransition: 'transform 560ms cubic-bezier(0.22, 1, 0.36, 1)',
  newSessionTransition: 'opacity 400ms ease, transform 400ms ease',
  pauseAnimationsGlobal: 'window.PAUSE_ANIMATIONS',
} as const;

export const cacheKeepaliveMoneyExamples = {
  renewedNet: '+$0.102',
  cappedNet: '+$0.0936',
  expiredNet: '+$0.1152',
  pendingTurn: '−$0.018 pending',
  notTrackedRow: '$0.00',
  notTrackedTurn: '-',
} as const;
