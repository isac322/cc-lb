import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, expect, test } from 'vitest';
import { CACHE_KEEPALIVE_STATES as API_STATES } from '../../../lib/cacheKeepaliveApi';
import * as queries from '../../../lib/queries';
import {
  CACHE_KEEPALIVE_STATES,
  cacheKeepaliveAnimationContract,
  cacheKeepaliveCardCopy,
  cacheKeepaliveDrawerCopy,
  cacheKeepaliveMoneyExamples,
  cacheKeepaliveSettingsCopy,
} from './__fixtures__/cacheKeepaliveContract';
import { cacheKeepaliveRows } from './__fixtures__/cacheKeepaliveFixtures';
import { CacheKeepaliveSettingsDrawer } from './CacheKeepaliveSettingsDrawer';

afterEach(cleanup);

test('freezes Cache keepalive card, drawer, settings, money, and animation contract', () => {
  expect(cacheKeepaliveCardCopy).toEqual({
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
  });
  expect(cacheKeepaliveDrawerCopy.closeHistoryAriaLabel).toBe('Close history');
  expect(cacheKeepaliveDrawerCopy.closeSettingsAriaLabel).toBe(
    'Close settings',
  );
  expect(cacheKeepaliveDrawerCopy.closeDetail).toBe('Close');
  expect(cacheKeepaliveDrawerCopy.backDetail).toBe('Back');
  expect(cacheKeepaliveDrawerCopy.turnTimeline).toEqual([
    'Message-by-message',
    'Current turn · Live',
    'Final turn',
    'waiting for follow-up',
    'cache used by follow-up',
    'follow-up unknown',
    'no follow-up (loss)',
  ]);
  expect(cacheKeepaliveSettingsCopy.labels).toEqual([
    'Cache keepalive enabled',
    'Renewal lead time · 5m / 30m',
    'Renewal lead time · 1h / 12h',
    'Max renewals per session',
    'Max total duration',
    'Snapshot max bytes',
    'Extra wait-for-user tools',
    'Treat end of turn as ambiguous',
    'OpenAI Responses GPT-5.6+ keepalive',
  ]);
  expect(cacheKeepaliveSettingsCopy.helperCopy).toEqual([
    'Seconds. Renews 10s before an Anthropic 5m or OpenAI 30m cache expires.',
    'Seconds. Renews 5m before an Anthropic 1h cache expires. OpenAI 12h retention is recorded, not renewed.',
    'Seconds. = 4h',
    '= 8 MiB',
  ]);
  expect(cacheKeepaliveMoneyExamples.cappedNet).toBe('+$0.0936');
  expect(cacheKeepaliveAnimationContract.riseTransition).toBe(
    'transform 560ms cubic-bezier(0.22, 1, 0.36, 1)',
  );
});

test('matches the settings copy contract to the rendered default drawer', () => {
  const principal: queries.Principal = {
    id: 'contract-principal',
    name: 'Contract principal',
    kind: 'machine',
    enabled: true,
    revision: 1,
    allowed_models: [],
    allowed_upstreams: [],
    default_limits: [],
    cache_keepalive: null,
  };
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });

  render(
    <QueryClientProvider client={queryClient}>
      <CacheKeepaliveSettingsDrawer
        open
        onOpenChange={() => {}}
        principal={principal}
      />
    </QueryClientProvider>,
  );

  expect(
    screen.getByRole('dialog', { name: cacheKeepaliveSettingsCopy.title }),
  ).toBeDefined();
  for (const label of cacheKeepaliveSettingsCopy.labels) {
    expect(screen.getByLabelText(label)).toBeDefined();
  }
  for (const hint of cacheKeepaliveSettingsCopy.helperCopy) {
    expect(screen.getByText(hint)).toBeDefined();
  }
});

test('keeps in-request state and client-facing lifecycle wording aligned with the API', () => {
  expect(CACHE_KEEPALIVE_STATES).toEqual(API_STATES);
  expect(API_STATES).toContain('in_request');
  expect(cacheKeepaliveDrawerCopy.stateLabels).toContain('In use');
});

test('freezes decision-backed, error-overlap, state, reason, and multi-turn fixtures', () => {
  expect(cacheKeepaliveRows.map((row) => row.state)).toEqual([
    'renewed',
    'scheduled',
    'capped',
    'capped',
    'expired',
    'not_tracked',
    'renewed',
  ]);
  expect(cacheKeepaliveRows.map((row) => row.reason)).toEqual([
    'agent-in-turn (tool_use: `bash`)',
    'agent-in-turn (tool_use: `edit_file`) — first renewal in 4m 30s',
    'max renewals reached',
    'max duration reached (4h)',
    'TTL expired before follow-up',
    'user turn (stop_reason=end_turn)',
    'renewal dispatch unavailable',
  ]);
  expect(cacheKeepaliveRows[0].turns).toHaveLength(3);
  expect(cacheKeepaliveRows[2].netPnl).toBe(0.0936);
  expect(cacheKeepaliveRows[5].id).toContain('decision-');
  expect(cacheKeepaliveRows[6].error).toBe(cacheKeepaliveRows[6].reason);
});

test('requires typed principal Cache keepalive list and detail hooks', () => {
  expect(Object.hasOwn(queries, 'useCacheKeepaliveSessions')).toBe(true);
  expect(Object.hasOwn(queries, 'useCacheKeepaliveSessionDetail')).toBe(true);
});
