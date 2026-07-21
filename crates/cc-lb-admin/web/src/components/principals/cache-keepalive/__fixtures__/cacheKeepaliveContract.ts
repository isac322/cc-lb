export const CACHE_KEEPALIVE_STATES = [
  'renewed',
  'scheduled',
  'capped',
  'expired',
  'not_tracked',
] as const;

export type CacheKeepaliveState = (typeof CACHE_KEEPALIVE_STATES)[number];

export const cacheKeepaliveCardCopy = {
  title: 'Cache keepalive',
  tooltip:
    'Keeps the Anthropic prompt cache warm by renewing its TTL — fires a tiny synthetic request just before the prompt cache expires so the next real request still hits a warm cache.',
  metrics: [
    ['Renewing now', 'scheduled or mid-renewal'],
    ['Sessions (last 5m)', 'seen in last 5 min'],
    ['Renewals fired', 'all-time'],
    ['Cost saved', 'net, after renewal spend'],
  ],
  caption: 'Renews the prompt-cache TTL during idle gaps.',
  actions: ['Sessions', 'Settings'],
} as const;

export const cacheKeepaliveDrawerCopy = {
  sessionsTitle: 'Cache keepalive sessions',
  sessionDetailTitle: 'Session detail',
  closeDetail: 'Close ▶',
  closeHistoryAriaLabel: 'Close history',
  closeSettingsAriaLabel: 'Close settings',
  horizons: ['24h', '7d', 'All'],
  filters: [
    'All',
    'Renewed',
    'Scheduled',
    'Capped',
    'Expired',
    'Not tracked',
    'Error',
  ],
  loadingOlder: 'Loading older sessions...',
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
