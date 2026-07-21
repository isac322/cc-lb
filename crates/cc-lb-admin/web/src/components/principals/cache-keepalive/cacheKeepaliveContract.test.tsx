import { expect, test } from 'vitest';
import * as queries from '../../../lib/queries';
import {
  cacheKeepaliveAnimationContract,
  cacheKeepaliveCardCopy,
  cacheKeepaliveDrawerCopy,
  cacheKeepaliveMoneyExamples,
  cacheKeepaliveSettingsCopy,
} from './__fixtures__/cacheKeepaliveContract';
import { cacheKeepaliveRows } from './__fixtures__/cacheKeepaliveFixtures';

test('freezes Cache keepalive card, drawer, settings, money, and animation contract', () => {
  expect(cacheKeepaliveCardCopy).toEqual({
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
  });
  expect(cacheKeepaliveDrawerCopy.closeHistoryAriaLabel).toBe('Close history');
  expect(cacheKeepaliveDrawerCopy.closeSettingsAriaLabel).toBe(
    'Close settings',
  );
  expect(cacheKeepaliveDrawerCopy.closeDetail).toBe('Close ▶');
  expect(cacheKeepaliveDrawerCopy.turnTimeline).toEqual([
    'Message-by-message',
    'Current turn · Live',
    'Final turn',
    'waiting for follow-up',
    'cache used by follow-up',
    'no follow-up (loss)',
  ]);
  expect(cacheKeepaliveSettingsCopy.helperCopy).toEqual([
    '→ renews 30s before the 5m cache expires',
    '= 4h',
    '= 512 KiB',
    'Add tool...',
    'Reserved for a future release',
    'Reset',
    'Save changes',
  ]);
  expect(cacheKeepaliveMoneyExamples.cappedNet).toBe('+$0.0936');
  expect(cacheKeepaliveAnimationContract.riseTransition).toBe(
    'transform 560ms cubic-bezier(0.22, 1, 0.36, 1)',
  );
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
