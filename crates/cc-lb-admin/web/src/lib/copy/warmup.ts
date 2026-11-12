export const COPY = {
  cardTitle: 'Warmup',
  cardSubtitle: 'Prepares the next quota window during idle gaps.',
  helpText:
    'Warm-up sends a tiny background request during idle time so the next quota window is ready when traffic resumes. It uses only windows the account reports. Gemini upstreams warm Gemini models and Claude and GPT-OSS models separately; each group follows its own reset.',
  geminiCardSubtitle:
    "Prepares each model group's next quota window during idle gaps.",
  /** Long lane names, shown only when an upstream has more than one lane. */
  laneLabels: {
    primary: 'Gemini models',
    third_party: 'Claude and GPT-OSS models',
  },
  /** Compact lane names for segmented controls and row captions. */
  laneShortLabels: {
    primary: 'Gemini',
    third_party: 'Claude·OSS',
  },
  laneFilterLabel: 'Model group',
  laneFireBothOption: 'Both',
  fireLaneUnavailableTemplate:
    '{lane} are no longer available for this account. Close this dialog and choose again.',
  laneHistoryAllOption: 'All',
  laneNotReported: 'Not reported by this account',
  lastCycleLabel: 'Last cycle',
  dialectPluginLabel: 'Shape plugin',
  defaultPluginOption: 'None (default request shape)',
  enableButtonLabel: 'Enable warmup',
  fireNowButtonLabel: 'Fire now',
  clearPluginButtonLabel: 'Clear plugin',
  confirmFireTitle: 'Fire warmup now?',
  confirmFireConfirmLabel: 'Fire now',
  confirmFireCancelLabel: 'Cancel',
  confirmClearPluginTitle: 'Clear shape plugin?',
  confirmClearPluginBody:
    'Future warmups will use the default request shape. You can re-attach a plugin any time.',
  confirmClearPluginConfirmLabel: 'Clear',
  toggleEnabledSuccess: 'Warmup enabled',
  toggleDisabledSuccess: 'Warmup disabled',
  dialectPluginSaveSuccess: 'Shape plugin updated',
  dialectPluginClearSuccess: 'Shape plugin cleared',
  disabledEmpty:
    'Warmup is off for this upstream. Enable to start idle subscription windows.',
  lastNull: 'Never warmed',
  confirmFireBody:
    'This sends a real request to {upstreamName} and consumes a small amount of quota. The next scheduled attempt follows the subscription window reset.',
  fireSuccess: 'Warmup request completed',
  fireSuccessLaneTemplate: 'Warmup request completed for {lane}',
  leaseHeldTemplate:
    'Another replica ({heldBy}) is currently priming this upstream. Try again in ~30 seconds.',
  staleRevisionHint: 'Settings changed elsewhere — re-apply your changes.',
  noShapePluginsAvailable:
    'No shape plugins installed. Install one from the Plugins page to customize the warmup request.',
  unknownPluginTemplate: 'Unknown plugin: {id}',
  fireErrorReasons: {
    auth_failed:
      'Warmup failed: upstream rejected the OAuth credentials. Refresh the OAuth subscription metadata.',
    forbidden:
      "Warmup failed: upstream returned 403. Check the account's API scope.",
    bad_request:
      'Warmup failed: upstream rejected the request body. Check the warmup model and any shape plugin configuration.',
    not_found: 'Warmup failed: upstream returned 404. Check the base URL.',
    dialect_plugin_failed:
      'Warmup failed: the dialect plugin raised an error. Check plugin logs.',
    oauth_credentials_missing:
      'Warmup cannot fire: this upstream has no OAuth credentials yet. Complete the OAuth flow first.',
    seven_day_quota_exhausted:
      'Warmup skipped: the 7-day subscription quota is exhausted. The scheduler will retry after the reset window.',
    upstream_disabled: 'Warmup skipped: upstream is disabled.',
    upstream_deleted: 'Warmup skipped: upstream was deleted.',
    quota_group_unavailable:
      'Warmup skipped: this account does not report quota for this model group.',
    rate_limited_cycle_key_missing:
      'Warmup failed: 429 without quota reset headers.',
    upstream_5xx: 'Warmup failed: upstream returned 5xx.',
    network_error: 'Warmup failed: network error.',
    request_timeout: 'Warmup failed: request timeout.',
    dialect_plugin_transient: 'Warmup failed: shape plugin transient error.',
    request_build_failed: 'Warmup failed: could not build request.',
    oauth_refresh_failed: 'Warmup failed: OAuth refresh failed.',
    credential_decrypt_failed: 'Warmup failed: credential decrypt failed.',
  },
} as const;

export const TOAST_DURATIONS = {
  success: 6000, // toast.success(msg, { duration: TOAST_DURATIONS.success })
  error: undefined, // sonner default — error toasts are NOT auto-dismissed; rely on inline error panel
} as const;
export const LEASE_PANEL_AUTO_DISMISS_MS = 10000; // setTimeout to clear leasePanel state
export const FIRE_NOW_COOLDOWN_MS = 1000; // post-200 cooldown before re-enable

export type FireErrorReason = keyof typeof COPY.fireErrorReasons;
