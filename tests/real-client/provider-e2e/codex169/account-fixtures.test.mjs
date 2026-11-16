import assert from 'node:assert/strict';
import test from 'node:test';
import accountFixtures, { refs } from './account-fixtures.mjs';
import { clientPin } from './fixtures.mjs';
import { encodeReplayFrames, validateReplayFixture } from '../client-harness/recorder-replay.mjs';

// QA list:
// - shared replay validation and pinned GitHub source anchors;
// - exact encoded JSON bytes, query multiplicity, status and Retry-After;
// - independent source-shaped checks for null/absent/decimal/unknown fields;
// - no consumer/provider success claim for local credential or entitlement seams.
const get = (id) => {
  const fixture = accountFixtures.find((candidate) => candidate.id === id);
  assert.ok(fixture, `missing fixture ${id}`);
  return fixture;
};
const body = (fixture) => JSON.parse(encodeReplayFrames(fixture).toString('utf8'));
const params = (fixture) => new URL(fixture.request.path, 'https://fixture.invalid').searchParams;

test('account fixtures are replay-valid and source-pinned', () => {
  for (const fixture of accountFixtures) {
    assert.equal(validateReplayFixture(fixture), fixture);
    assert.match(fixture.sourceRefs[0], new RegExp(clientPin.source_revision));
    assert.match(fixture.sourceRefs[0], /#L\d+-L\d+$/);
    assert.match(fixture.syntheticBoundary, /consumer decode/);
    assert.equal(fixture.client, clientPin);
    assert.equal(fixture.expected.evidence, 'synthetic');
    assert.match(fixture.expected.contractBoundary, /remain unverified/);
    assert.ok(Object.isFrozen(fixture));
    assert.ok(Object.isFrozen(fixture.request));
    assert.ok(Object.isFrozen(fixture.response.frames));
  }
  assert.equal(get('account-check-sentinel').surfaceIds[0], 'codex:accounts-check');
  assert.equal(get('verified-access').surfaceIds[0], 'codex:accounts-verified-access');
  assert.equal(get('workspace-switch').surfaceIds[0], 'codex:workspace-switch-token');
  assert.notEqual(refs.accountsTypes, refs.accountsMap);
  for (const [id, surface] of [
    ['reset-credit-list', 'codex:reset-credits'],
    ['reset-credit-consume-idempotent', 'codex:reset-credit-consume'],
    ['reset-credit-nudge', 'codex:credit-nudge-email'],
    ['config-bundle', 'codex:cloud-config-bundle'],
    ['settings-user', 'codex:user-settings'],
    ['turn-estimates', 'codex:thread-cost-estimates'],
    ['thread-usage-query', 'codex:thread-usage'],
    ['task-usage-query-v2', 'codex:task-usage-v2'],
    ['plan-history', 'codex:plan-limit-history'],
  ]) assert.equal(get(id).surfaceIds[0], surface);
});

test('accounts-check preserves real list/map schemas and routing descriptors', () => {
  const list = body(get('account-check-sentinel'));
  assert.deepEqual(Object.keys(list.accounts[0]).sort(), [
    'account_routing_override', 'can_access_with_session', 'id', 'is_deactivated',
    'is_fedramp_compliant_workspace', 'name', 'plan_type', 'profile_picture_url',
    'sso_connection_name', 'structure', 'workspace_backend_origin',
  ].sort());
  assert.equal(list.accounts[0].workspace_backend_origin, 'NO_CONSTRAINT');
  assert.equal(list.accounts[0].account_routing_override, 'NO_CONSTRAINT');
  assert.equal(list.default_account_id, list.account_ordering[0]);

  const map = body(get('account-check-map'));
  assert.equal(map.accounts[map.account_ordering[0]].account.account_id, map.account_ordering[0]);
  assert.equal('workspace_backend_origin' in map.accounts[map.account_ordering[0]], false);

  const override = body(get('account-check-override'));
  assert.equal(override.accounts[0].workspace_backend_origin, 'https://proxy.synthetic.invalid');
  assert.equal(override.accounts[0].account_routing_override, 'us_cr');
});

test('profile and account reads preserve source fields without invented aggregation', () => {
  const profile = body(get('usage-profile'));
  assert.equal(profile.profile.username, null);
  assert.equal(profile.stats.lifetime_tokens, 1200);
  assert.equal(profile.stats.fast_mode_usage_percentage, 12.5);
  assert.equal(profile.stats.top_invocations, null);
  assert.deepEqual(profile.unknown_profile_extension, { preserved: true });

  const messages = body(get('workspace-messages'));
  assert.equal(messages.messages[0].archived_at, null);
  assert.equal(messages.messages[1].message_type, 'future_type');
  assert.equal(messages.messages[1].created_at, null);
});

test('reset list, consume request fields and nudge transport evidence are preserved', () => {
  const list = body(get('reset-credit-list'));
  assert.deepEqual(list.credits, []);
  assert.equal(list.available_count, 0);

  const consume = get('reset-credit-consume-idempotent');
  assert.deepEqual(consume.request.body, { redeem_request_id: 'redeem-1', credit_id: 'credit-1' });
  assert.equal(body(consume).code, 'already_redeemed');
  assert.equal(body(consume).windows_reset, 0);

  const nudge = get('reset-credit-nudge');
  assert.equal(nudge.response.status, 429);
  assert.equal(nudge.response.headers.find(([name]) => name === 'retry-after')[1], '37');
  assert.equal(encodeReplayFrames(nudge).toString('utf8'), '{"fixture_extension":{"synthetic_error":"nudge-cooldown"}}');
});

test('usage query fixtures preserve source request and response distinctions', () => {
  const estimates = get('turn-estimates');
  assert.equal(estimates.request.path, '/api/codex/usage/thread-estimates/query');
  assert.equal(estimates.request.body.include_settled_response_ids, true);
  assert.deepEqual(estimates.request.body.threads[0].turn_ids, ['turn-1', 'turn-2']);
  assert.equal(body(estimates).threads[0].turns[1].estimated_usage_usd_micros, null);

  const threadUsage = get('thread-usage-query');
  assert.deepEqual(threadUsage.request.body.thread_ids, ['thread-1', 'thread-2']);
  assert.deepEqual(body(threadUsage).threads.map(({ thread_id }) => thread_id), ['thread-1']);

  const v2 = get('task-usage-query-v2');
  assert.deepEqual(v2.request.body.threads[0].descendant_thread_ids, ['thread-1-child']);
  assert.equal(body(v2).threads[0].five_hour_limit_percent, 12.5);
  assert.equal(body(v2).threads[0].weekly_limit_percent, null);
  assert.equal('balance_usage_credits' in body(v2).threads[0], false);
});

test('plan and analytics routes preserve source query semantics and exact response bytes', () => {
  const plan = get('plan-history');
  assert.equal(params(plan).get('days'), '7');
  assert.equal(body(plan).periods[0].used_basis_points, 12.34);
  assert.equal(body(plan).periods[0].breakdowns, null);

  const consumer = get('analytics-consumer');
  assert.equal(params(consumer).get('group_by'), 'day');
  assert.equal(body(consumer).data[0].product_surface_usage_values.codex, 10.5);

  const business = get('analytics-business');
  assert.equal(params(business).get('group_by'), 'day');
  assert.equal(params(business).get('workspace_user'), null);
  assert.deepEqual(body(business).data, []);

  const enterpriseTokens = get('analytics-enterprise-tokens');
  assert.deepEqual(params(enterpriseTokens).getAll('modes'), ['codex', 'work']);
  assert.equal(params(enterpriseTokens).get('breakdown_by'), 'model');
  assert.equal(params(enterpriseTokens).get('workspace_user'), null);

  for (const breakdown of ['product', 'model', 'speed', 'reasoning_effort']) {
    const enterpriseCredits = get(`analytics-enterprise-credits-${breakdown}`);
    assert.equal(params(enterpriseCredits).get('breakdown'), breakdown);
    assert.equal(body(enterpriseCredits).breakdown, breakdown);
  }

  const messages = get('analytics-messages');
  assert.equal(params(messages).get('workspace_user'), 'true');
  assert.deepEqual(body(messages).data, []);

  const plugins = get('analytics-plugins');
  assert.equal(body(plugins).data[0].plugin_usage_overviews[0].invocation_counts, 12);
  assert.equal(params(plugins).get('top_plugin_limit'), '10');

  const skills = get('analytics-skills');
  assert.equal(params(skills).get('top_skill_limit'), '10');
  assert.equal(params(skills).get('workspace_user'), 'true');

  const credits = get('analytics-credits-wham');
  assert.equal(params(credits).toString(), '');
  assert.equal(body(credits).data[0].credit_amount, -0.004);
  const throttle = get('analytics-enterprise-throttle');
  assert.equal(throttle.response.status, 429);
  assert.equal(throttle.response.headers.find(([name]) => name === 'retry-after')[1], '61');
});

test('security, verified access and local workspace extension schema encoding stays explicit', () => {
  const security = get('security-setup-read');
  assert.equal(security.request.method, 'GET');
  assert.equal(security.request.path, '/wham/security-setup');
  assert.equal(body(security).notice, null);
  assert.equal(security.expected.disposition, 'blocked-contract');

  for (const [id, state] of [['verified-access-active', 'active'], ['verified-access-inactive', 'inactive'], ['verified-access', 'unavailable']]) {
    const verified = get(id);
    assert.equal(verified.request.method, 'GET');
    assert.equal(verified.request.path, '/accounts/verified_access');
    assert.equal(body(verified).programs[0].state, state);
    assert.equal(verified.expected.disposition, 'blocked-contract');
  }

  const exchange = get('workspace-switch');
  assert.equal(body(exchange).access_token, 'synthetic-local-access-b');
  assert.equal(body(exchange).workspace_generation, 2);
  assert.equal(exchange.expected.disposition, 'blocked-contract');
  assert.match(exchange.expected.pendingContract, /Approved local-mode extension/);
  assert.match(exchange.expected.pendingContract, /not validated as a credential/);

  const localConflict = get('workspace-switch-failure');
  assert.equal(localConflict.response.status, 400);
  assert.equal(localConflict.response.headers.find(([name]) => name === 'cc-lb-error-origin')[1], 'proxy');

  const upstream = get('account-usage-upstream-unauthorized');
  assert.equal(upstream.request.path, '/wham/usage');
  assert.equal(upstream.response.status, 401);
  assert.equal(upstream.response.headers.find(([name]) => name === 'cc-lb-error-origin')[1], 'upstream');
  assert.equal(upstream.response.headers.some(([name]) => name === 'cc-lb-auth-reason'), false);
});
