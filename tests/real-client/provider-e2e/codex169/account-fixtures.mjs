import { clientPin } from './fixtures.mjs';

// Synthetic, source-shaped contracts only. These records are not captured traffic,
// provider evidence, authentication proof, or a claim that stock Codex consumes them.
const source = (path, start, end = start) => `https://github.com/openai/codex/blob/${clientPin.source_revision}/codex-rs/${path}#L${start}-L${end}`;
const refs = Object.freeze({
  accountsList: source('backend-client/src/client.rs', 525, 535),
  accountsTypes: source('backend-client/src/types.rs', 129, 155),
  accountsMap: source('backend-client/src/types.rs', 158, 239),
  profile: source('backend-client/src/client/profile.rs', 10, 40),
  profileStats: source('backend-client/src/types.rs', 548, 566),
  rateLimits: source('backend-client/src/client/rate_limit_resets.rs', 84, 158),
  rateLimitTypes: source('backend-client/src/types.rs', 22, 42),
  resetConsumeTypes: source('backend-client/src/types.rs', 104, 118),
  resetConsumeTests: source('app-server/tests/suite/v2/rate_limit_reset_credits.rs', 66, 104),
  nudge: source('backend-client/src/client.rs', 553, 564),
  config: source('backend-client/src/client.rs', 659, 680),
  configModel: source('codex-backend-openapi-models/src/models/config_bundle_response.rs', 15, 31),
  settings: source('backend-client/src/client.rs', 683, 701),
  messages: source('backend-client/src/client.rs', 703, 714),
  turnEstimates: source('backend-client/src/client/chatgpt_turn_cost.rs', 27, 75),
  threadUsage: source('backend-client/src/client/thread_usage.rs', 39, 125),
  taskUsage: source('backend-client/src/client/task_usage.rs', 12, 107),
  planHistory: source('backend-client/src/client/plan_history.rs', 9, 81),
  analytics: source('backend-client/src/client/analytics.rs', 9, 125),
  analyticsTests: source('backend-client/src/client/analytics_tests.rs', 16, 189),
  securitySetup: source('app-server/src/request_processors/account_processor/security_setup.rs', 47, 88),
  verifiedAccess: source('codex-mcp/src/trusted_access.rs', 71, 113),
  verifiedAccessRoute: source('app-server/tests/suite/v2/daybreak_access.rs', 125, 133),
  workspaceSwitch: source('login/src/auth/manager/workspace_switching.rs', 272, 319),
});

const headers = Object.freeze([['content-type', 'application/json']]);
const request = (method, path, body = null, extra = []) => ({ method, path, headers: [...headers, ...extra], body });
const response = (data, status = 200, extra = []) => ({ status, headers: [...headers, ...extra], frames: [{ kind: 'json', data }], termination: 'eof' });
const surfaceIds = {
  'accounts-check': 'codex:accounts-check',
  'usage-profile': 'codex:account-profile',
  'reset-credit-list': 'codex:reset-credits',
  'reset-credit-consume-idempotent': 'codex:reset-credit-consume',
  'reset-credit-nudge': 'codex:credit-nudge-email',
  'usage-rate-limit-wham': 'codex:usage',
  config: 'codex:cloud-config-bundle',
  settings: 'codex:user-settings',
  messages: 'codex:workspace-messages',
  security: 'codex:account-security-setup',
  'thread-estimates': 'codex:thread-cost-estimates',
  'thread-usage': 'codex:thread-usage',
  'thread-usage-v2': 'codex:task-usage-v2',
  'plan-history': 'codex:plan-limit-history',
  analytics: 'codex:analytics-tokens',
  'workspace-switch': 'codex:workspace-switch-token',
  'verified-access': 'codex:accounts-verified-access',
};
const canonicalSurface = (id, family) => {
  if (surfaceIds[id]) return surfaceIds[id];
  if (id === 'analytics-consumer') return 'codex:analytics-tokens';
  if (id === 'analytics-credits-wham') return 'codex:analytics-credits';
  if (id === 'analytics-business') return 'codex:analytics-tokens';
  if (id === 'analytics-enterprise-tokens') return 'codex:analytics-enterprise-tokens';
  if (id.startsWith('analytics-enterprise-credits') || id === 'analytics-enterprise-throttle') return 'codex:analytics-enterprise-credits';
  if (id === 'analytics-messages') return 'codex:analytics-messages';
  if (id === 'analytics-plugins') return 'codex:analytics-plugins';
  if (id === 'analytics-skills') return 'codex:analytics-skills';
  return surfaceIds[family] ?? family;
};
const deepFreeze = (value) => {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
};
const boundary = 'Fixture assertions only; actual consumer subprocess decode, routing ownership, proxy behavior and provider/account reachability remain unverified.';
const fixture = (id, family, scenario, req, res, expected, sourceRefs, options = {}) => deepFreeze({
  id, client: clientPin, surfaceIds: [canonicalSurface(id, family)], scenario,
  origin: 'deterministic-handwritten-source-shaped', request: req, response: res,
  expected: { evidence: 'synthetic', contractBoundary: boundary, ...expected },
  sourceRefs, provenance: options.provenance ?? 'synthetic',
  syntheticBoundary: 'Source-shaped replay only; consumer decode, proxy ownership, local session verification, provider reachability and real CLI acceptance remain unverified.',
  ...(options.notes ? { notes: options.notes } : {}),
});

const codex = (path) => `/api/codex${path}`;
const wham = (path) => `/wham${path}`;
const account = 'synthetic-account-a';

const fixtures = [
  fixture('account-check-sentinel', 'accounts-check', 'Accounts check list preserves routing descriptors and no selected/user inference',
    request('GET', codex('/accounts/check')),
    response({ accounts: [{ id: account, can_access_with_session: true, sso_connection_name: null, is_deactivated: false, is_fedramp_compliant_workspace: false, plan_type: 'team', workspace_backend_origin: 'NO_CONSTRAINT', account_routing_override: 'NO_CONSTRAINT', name: null, profile_picture_url: null, structure: 'workspace' }], account_ordering: [account], default_account_id: account }),
    { accountId: account, originSentinel: 'NO_CONSTRAINT', routingOverride: 'NO_CONSTRAINT', selectedInference: 'not-in-wire' }, [refs.accountsList, refs.accountsTypes]),
  fixture('account-check-map', 'accounts-check', 'Accounts check map form normalizes through account ordering without invented routing headers',
    request('GET', wham('/accounts/check')),
    response({ accounts: { [account]: { account: { account_id: account, is_deactivated: false, is_fedramp_compliant_workspace: false, plan_type: 'team', name: null, profile_picture_url: null, structure: 'workspace' }, can_access_with_session: true, sso_connection_name: null } }, account_ordering: [account], default_account_id: account }),
    { accountId: account, mapNormalization: true, routingFields: 'absent-in-map-form' }, [refs.accountsList, refs.accountsMap]),
  fixture('account-check-override', 'accounts-check', 'Accounts check keeps HTTPS backend origin distinct from region routing override',
    request('GET', codex('/accounts/check')),
    response({ accounts: [{ id: account, can_access_with_session: true, sso_connection_name: null, is_deactivated: false, is_fedramp_compliant_workspace: false, plan_type: 'team', workspace_backend_origin: 'https://proxy.synthetic.invalid', account_routing_override: 'us_cr', name: 'Synthetic workspace', profile_picture_url: null, structure: 'workspace' }], account_ordering: [account], default_account_id: account }),
    { accountId: account, workspaceBackendOrigin: 'https://proxy.synthetic.invalid', accountRoutingOverride: 'us_cr' }, [refs.accountsList, refs.accountsTypes]),
  fixture('usage-profile', 'usage-profile', 'Profile preserves optional identity, metadata, integer token stats and unknown extension data',
    request('GET', codex('/profiles/me')),
    response({ profile: { display_name: 'Synthetic user', username: null }, metadata: { stats_as_of: null, stats_error: null }, stats: { lifetime_tokens: 1200, peak_daily_tokens: null, longest_running_turn_sec: null, current_streak_days: 0, longest_streak_days: null, daily_usage_buckets: [{ start_date: '2026-11-16', tokens: 1200 }], fast_mode_usage_percentage: 12.5, most_used_reasoning_effort: null, most_used_reasoning_effort_percentage: null, unique_skills_used: 0, total_skills_used: null, total_threads: 1, top_invocations: null }, unknown_profile_extension: { preserved: true } }),
    { profileUsername: null, lifetimeTokens: 1200, decimalFastMode: 12.5, unknownExtension: true }, [refs.profile, refs.profileStats]),
  fixture('reset-credit-list', 'reset-credit', 'Reset credit list preserves required available count and no-data credits',
    request('GET', codex('/rate-limit-reset-credits')),
    response({ credits: [], available_count: 0, unknown_credit_field: 'preserved' }),
    { credits: [], availableCount: 0, noData: true }, [refs.rateLimits, refs.rateLimitTypes]),
  fixture('reset-credit-consume-idempotent', 'reset-credit', 'Reset credit consume binds redeem request and optional credit ID',
    request('POST', codex('/rate-limit-reset-credits/consume'), { redeem_request_id: 'redeem-1', credit_id: 'credit-1' }),
    response({ code: 'already_redeemed', windows_reset: 0 }),
    { code: 'already_redeemed', requestBodyFields: ['redeem_request_id', 'credit_id'] }, [refs.rateLimits, refs.resetConsumeTypes, refs.resetConsumeTests]),
  fixture('reset-credit-nudge', 'reset-nudge', 'Nudge POST carries source credit type and preserves a rate-limit response',
    request('POST', codex('/accounts/send_add_credits_nudge_email'), { credit_type: 'credits' }),
    response({ fixture_extension: { synthetic_error: 'nudge-cooldown' } }, 429, [['retry-after', '37']]),
    { status: 429, retryAfter: '37', errorBodySchema: 'synthetic transport probe, not a source-defined error model' }, [refs.nudge]),
  fixture('config-bundle', 'config', 'Config bundle preserves generated nullable TOML fields',
    request('GET', codex('/config/bundle')),
    response({ config_toml: null, requirements_toml: null, unknown_config_extension: { preserved: true } }),
    { configToml: null, requirementsToml: null, unknownExtension: true }, [refs.config, refs.configModel]),
  fixture('settings-user', 'settings', 'User settings preserve explicit policy value and absent compatibility fields',
    request('GET', codex('/settings/user'), null, [['cache-control', 'no-cache, no-store']]),
    response({ commit_attribution_enabled: true, unknown_setting: 'preserved' }),
    { commitAttributionEnabled: true, absentLegacyMemorySetting: true, consumer: 'git-attribution-policy' }, [refs.settings]),
  fixture('workspace-messages', 'messages', 'Workspace messages preserve nullable archive timestamp and unknown message type',
    request('GET', codex('/workspace-messages'), null, [['cache-control', 'no-store']]),
    response({ messages: [{ message_id: 'message-1', message_type: 'announcement', message_body: 'Synthetic notice', created_at: '2026-11-16T00:00:00Z', archived_at: null }, { message_id: 'message-2', message_type: 'future_type', message_body: 'Unknown', created_at: null }] }),
    { messageCount: 2, archivedAt: null, unknownType: 'future_type' }, [refs.messages]),
  fixture('security-setup-read', 'security', 'Security setup backend GET preserves nullable notice response',
    request('GET', wham('/security-setup'), null, [['chatgpt-account-id', account]]),
    response({ notice: null }),
    { notice: null, disposition: 'blocked-contract', consumerIdentityChecks: 'not executed' }, [refs.securitySetup]),
  fixture('turn-estimates', 'thread-estimates', 'Turn estimates preserve thread and turn association and settled response IDs',
    request('POST', codex('/usage/thread-estimates/query'), { threads: [{ thread_id: 'thread-1', turn_ids: ['turn-1', 'turn-2'] }], include_settled_response_ids: true }),
    response({ threads: [{ thread_id: 'thread-1', turns: [{ turn_id: 'turn-1', model: 'gpt-5.2-codex', estimated_usage_usd_micros: 1250000, settled_response_ids: ['resp-1'] }, { turn_id: 'turn-2', model: null, estimated_usage_usd_micros: null }] }] }),
    { threadId: 'thread-1', turnIds: ['turn-1', 'turn-2'], includeSettledResponseIds: true, decimalUnit: 'integer micros', nullModelAndAmount: true }, [refs.turnEstimates]),
  fixture('thread-usage-query', 'thread-usage', 'Thread usage omits unavailable rows rather than converting them to zero',
    request('POST', codex('/usage/thread_usage/query'), { thread_ids: ['thread-1', 'thread-2'] }),
    response({ threads: [{ thread_id: 'thread-1', estimated_usage_credits_micros: 250000, estimated_usage_usd_micros: null, native_usage_usd_micros: 175000, groups: [{ model: 'gpt-5.2-codex', reasoning_effort: null, speed: 'fast', estimated_usage_credits_micros: 250000, native_usage_usd_micros: null, input_tokens: 10, output_tokens: 5, total_tokens: 15 }] }] }),
    { requestedThreadIds: ['thread-1', 'thread-2'], returnedThreadIds: ['thread-1'], omittedMeans: 'unavailable-not-zero', nullEstimatedUsd: true }, [refs.threadUsage]),
  fixture('task-usage-query-v2', 'thread-usage-v2', 'Task usage query_v2 preserves created/descendant IDs, decimal percentages and absent balance',
    request('POST', codex('/usage/thread_usage/query_v2'), { threads: [{ thread_id: 'thread-1', created_at: null, descendant_thread_ids: ['thread-1-child'] }] }),
    response({ data_as_of: null, threads: [{ thread_id: 'thread-1', data_status: 'partial', usage_source: 'synthetic-backend', five_hour_limit_percent: 12.5, weekly_limit_percent: null, groups: [], unknown_usage_field: { preserved: true } }] }),
    { threadId: 'thread-1', createdAt: null, descendantThreadIds: ['thread-1-child'], status: 'partial', fiveHourPercent: 12.5, weeklyPercent: null, absentBalance: true }, [refs.taskUsage]),
  fixture('plan-history', 'plan-history', 'Plan history always requests seven days and preserves null coverage and basis points',
    request('GET', `${codex('/usage/plan_limit_history')}?days=7`),
    response({ data_as_of: null, coverage_start: null, coverage_complete: false, approximate: true, boundary_tolerance_seconds: null, periods: [{ id: 'period-1', window_minutes: 300, plan_type: 'unknown', starts_at: '2026-11-15T00:00:00Z', ends_at: '2026-11-15T05:00:00Z', accounting_complete: false, used_basis_points: 12.34, breakdowns: null }] }),
    { days: 7, missingCoverage: true, usedBasisPoints: 12.34, breakdowns: null }, [refs.planHistory]),
  fixture('analytics-consumer', 'analytics', 'Consumer usage analytics keeps inclusive dates and day grouping',
    request('GET', `${codex('/usage/daily-token-usage-breakdown')}?start_date=2026-11-01&end_date=2026-11-07&group_by=day`),
    response({ data: [{ date: '2026-11-01', product_surface_usage_values: { codex: 10.5 } }], units: 'tokens' }),
    { report: 'consumer-usage', groupBy: 'day', decimalUsage: 10.5 }, [refs.analytics, refs.analyticsTests]),
  fixture('usage-rate-limit-wham', 'reset-credit', 'ChatGPT backend usage route preserves ordinary usage permission shape',
    request('GET', wham('/usage')),
    response({ plan_type: 'plus', rate_limit: { allowed: false, limit_reached: true, primary_window: { used_percent: 100, limit_window_seconds: 300, reset_after_seconds: 60, reset_at: 2000000000 } } }),
    { pathStyle: 'chatgpt-api', ordinaryUsageAllowed: false }, [refs.rateLimits]),
  fixture('analytics-credits-wham', 'analytics', 'Credit events preserve signed amounts on the ChatGPT backend route without date query parameters',
    request('GET', wham('/usage/credit-usage-events')),
    response({ data: [{ date: '2026-11-01', product_surface: 'cli', credit_amount: -0.004 }] }),
    { report: 'credits', signedCreditAmount: -0.004, dateQuery: 'absent' }, [refs.analytics, refs.analyticsTests]),
  fixture('analytics-business', 'analytics', 'Business workspace token analytics uses its source route without enterprise-only selectors',
    request('GET', `${codex('/usage/daily-workspace-user-token-usage-breakdown')}?start_date=2026-11-01&end_date=2026-11-07&group_by=day`),
    response({ data: [] }),
    { report: 'workspace-credits', workspaceUserQuery: false, modesQuery: false }, [refs.analytics, refs.analyticsTests]),
  fixture('analytics-enterprise-tokens', 'analytics', 'Enterprise token analytics repeats source modes and model breakdown selectors',
    request('GET', `${codex('/usage/daily-workspace-user-token-usage-breakdown')}?start_date=2026-11-01&end_date=2026-11-07&group_by=day&breakdown_by=model&modes=codex&modes=work`),
    response({ data: [] }),
    { report: 'enterprise-tokens', repeatedModes: ['codex', 'work'], breakdownBy: 'model', workspaceUserQuery: false }, [refs.analytics]),
  ...['product', 'model', 'speed', 'reasoning_effort'].map((breakdown) => fixture(`analytics-enterprise-credits-${breakdown}`, 'analytics',
    `Enterprise credit analytics preserves supported ${breakdown} breakdown selector`,
    request('GET', `${codex('/usage/daily-workspace-user-credit-usage')}?start_date=2026-11-01&end_date=2026-11-07&breakdown=${breakdown}`),
    response({ data: [], series: [], breakdown }),
    { report: 'enterprise-credits', breakdown }, [refs.analytics, refs.analyticsTests])),
  fixture('analytics-messages', 'analytics', 'Workspace message analytics adds workspace_user and preserves empty data',
    request('GET', `${wham('/analytics/daily-workspace-usage-counts')}?start_date=2026-11-01&end_date=2026-11-07&group_by=day&workspace_user=true`),
    response({ data: [] }),
    { report: 'messages', workspaceUserQuery: true, data: [] }, [refs.analytics, refs.analyticsTests]),
  fixture('analytics-plugins', 'analytics', 'Plugin analytics adds workspace_user and top plugin limit',
    request('GET', `${codex('/analytics/daily-plugin-usage-metrics')}?start_date=2026-11-01&end_date=2026-11-07&group_by=day&workspace_user=true&top_plugin_limit=10`),
    response({ data: [{ date: '2026-11-01', plugin_usage_overviews: [{ plugin_id: null, plugin_name: 'example', marketplace: null, display_name: 'Example plugin', invocation_counts: 12 }] }] }),
    { report: 'plugins', workspaceUserQuery: true, topPluginLimit: 10 }, [refs.analytics, refs.analyticsTests]),
  fixture('analytics-skills', 'analytics', 'Skill analytics adds workspace_user and top skill limit',
    request('GET', `${wham('/analytics/daily-skill-usage-metrics')}?start_date=2026-11-01&end_date=2026-11-07&group_by=day&workspace_user=true&top_skill_limit=10`),
    response({ data: [] }),
    { report: 'skills', workspaceUserQuery: true, topSkillLimit: 10 }, [refs.analytics]),
  fixture('analytics-enterprise-throttle', 'analytics', 'Enterprise credit analytics preserves throttling status and Retry-After',
    request('GET', `${codex('/usage/daily-workspace-user-credit-usage')}?start_date=2026-11-01&end_date=2026-11-07&breakdown=model`),
    response({ error: 'rate_limited', data: null }, 429, [['retry-after', '61']]),
    { status: 429, retryAfter: '61', dataAbsent: true }, [refs.analytics]),
  fixture('verified-access-active', 'verified-access', 'Verified access active program shape is preserved without claiming tool delivery',
    request('GET', '/accounts/verified_access', null, [['authorization', 'Bearer synthetic-account-token'], ['chatgpt-account-id', account]]),
    response({ programs: [{ program: 'cyber', state: 'active', grants: [{ level: 'tac1', source: 'individual' }] }] }),
    { disposition: 'blocked-contract', state: 'active', noEntitlementClaim: true }, [refs.verifiedAccess, refs.verifiedAccessRoute], { provenance: 'descriptor-only' }),
  fixture('verified-access-inactive', 'verified-access', 'Verified access inactive program shape is preserved without claiming tool denial semantics',
    request('GET', '/accounts/verified_access', null, [['authorization', 'Bearer synthetic-account-token'], ['chatgpt-account-id', account]]),
    response({ programs: [{ program: 'cyber', state: 'inactive', grants: [] }] }),
    { disposition: 'blocked-contract', state: 'inactive', noEntitlementClaim: true }, [refs.verifiedAccess, refs.verifiedAccessRoute], { provenance: 'descriptor-only' }),
  fixture('workspace-switch', 'workspace-switch', 'Local-mode workspace exchange shape remains deferred until issuer and patched consumer verification',
    request('POST', '/accounts/switch-workspace-token', { workspace_id: 'synthetic-account-b' }, [['authorization', 'Bearer synthetic-local-access']]),
    response({ access_token: 'synthetic-local-access-b', workspace_generation: 2 }),
    { workspaceId: 'synthetic-account-b', disposition: 'blocked-contract', pendingContract: 'Approved local-mode extension: access_token plus workspace_generation from the same SessionGrant. Token codec/issuer/verifier, durable target binding and delayed-renewal generation rejection remain implementation/consumer seams; access_token is not validated as a credential.' }, [refs.workspaceSwitch]),
  fixture('workspace-switch-failure', 'workspace-switch', 'Local workspace selector conflict does not imply a vendor token exchange',
    request('POST', '/accounts/switch-workspace-token', { workspace_id: 'foreign-workspace' }, [['authorization', 'Bearer synthetic-local-access']]),
    response({ error: 'workspace_unavailable' }, 400, [['cc-lb-error-origin', 'proxy']]),
    { status: 400, disposition: 'blocked-contract', pendingContract: 'Synthetic local selector conflict; no issuer authorization or durable owner mapping is exercised.' }, [refs.workspaceSwitch]),
  fixture('account-usage-upstream-unauthorized', 'usage-rate-limit-wham', 'Generic account read preserves an upstream unauthorized response without local expiry provenance',
    request('GET', wham('/usage')),
    response({ error: 'upstream_unauthorized', fixture_extension: 'synthetic-upstream-body' }, 401, [['cc-lb-error-origin', 'upstream']]),
    { status: 401, errorOrigin: 'upstream', contractExtension: 'Approved local-mode error provenance; not a Codex source claim.' }, [refs.rateLimits]),
  fixture('verified-access', 'verified-access', 'Verified access is a read-only entitlement gate with unknown result on unavailable evidence',
    request('GET', '/accounts/verified_access', null, [['authorization', 'Bearer synthetic-account-token'], ['chatgpt-account-id', account]]),
    response({ programs: [{ program: 'cyber', state: 'unavailable', grants: [] }] }, 503),
    { disposition: 'blocked-contract', status: 503, grants: 'unknown', noEntitlementClaim: true }, [refs.verifiedAccess, refs.verifiedAccessRoute], { provenance: 'descriptor-only' }),
];

export { fixtures, refs };
export default Object.freeze(fixtures);
