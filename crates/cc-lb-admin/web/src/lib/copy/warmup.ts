export const COPY = {
  cardTitle: 'Warmup',
  cardSubtitle:
    'Keep the 5h OAuth subscription window primed by sending a small request before quota expires.',
  lastCycleLabel: 'Last cycle',
  dialectPluginLabel: 'Shape plugin',
  defaultPluginOption: 'None (default request shape)',
  enableButtonLabel: 'Enable warmup',
  fireNowButtonLabel: 'Fire warmup now',
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
    'Warmup is off for this upstream. Enable to keep the 5h OAuth window primed.',
  upstreamPausedEmpty:
    'Upstream is disabled, so warmup is paused. Re-enable the upstream to resume scheduled warmups.',
  lastNull: 'Never warmed',
  confirmFireBody:
    'This sends a real /messages request to {upstreamName} and consumes a small token. The schedule for the current 5h window will be skipped.',
  fireSuccess: 'Warmup fired — 5h window primed',
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
      'Warmup failed: upstream rejected the request body. Check the dialect plugin configuration.',
    not_found: 'Warmup failed: upstream returned 404. Check the base URL.',
    dialect_plugin_failed:
      'Warmup failed: the dialect plugin raised an error. Check plugin logs.',
    oauth_credentials_missing:
      'Warmup cannot fire: this upstream has no OAuth credentials yet. Complete the OAuth flow first.',
    transient:
      'Warmup failed transiently. The background loop will retry automatically.',
  },
} as const;

export const TOAST_DURATIONS = {
  success: 6000, // toast.success(msg, { duration: TOAST_DURATIONS.success })
  error: undefined, // sonner default — error toasts are NOT auto-dismissed; rely on inline error panel
} as const;
export const LEASE_PANEL_AUTO_DISMISS_MS = 10000; // setTimeout to clear leasePanel state
export const FIRE_NOW_COOLDOWN_MS = 1000; // post-200 cooldown before re-enable

export type FireErrorReason = keyof typeof COPY.fireErrorReasons;
