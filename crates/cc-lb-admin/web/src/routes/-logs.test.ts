import { describe, expect, it } from 'vitest';
import { MAX_FORMATTABLE_UNIX_SECONDS } from '../lib/timezone';
import {
  buildHistoricalFilters,
  buildLiveFilters,
  getLogsRouteState,
  logsSearchSchema,
} from './logs';

describe('logsSearchSchema', () => {
  it('coerces string query params to integers', () => {
    const parsed = logsSearchSchema.parse({
      time_range: 'custom',
      since_unix_secs: '1700000000',
      until_unix_secs: '1700003600',
    });

    expect(parsed.time_range).toBe('custom');
    expect(parsed.since_unix_secs).toBe(1700000000);
    expect(parsed.until_unix_secs).toBe(1700003600);
  });

  it('handles missing optional params', () => {
    const parsed = logsSearchSchema.parse({});
    expect(parsed.time_range).toBeUndefined();
    expect(parsed.since_unix_secs).toBeUndefined();
    expect(parsed.until_unix_secs).toBeUndefined();
    expect(parsed.event_kind).toBeUndefined();
  });

  it('leaves the kind filter unset by default and keeps explicit kinds', () => {
    expect(logsSearchSchema.parse({}).event_kind).toBeUndefined();
    expect(logsSearchSchema.parse({ event_kind: 'renewal' }).event_kind).toBe(
      'renewal',
    );
    expect(
      logsSearchSchema.parse({ event_kind: 'unclassified' }).event_kind,
    ).toBe('unclassified');
  });

  it('ignores legacy source_kind params and invalid kind values', () => {
    const legacy = logsSearchSchema.parse({ source_kind: 'renewal' });
    expect(legacy.event_kind).toBeUndefined();
    expect(legacy).not.toHaveProperty('source_kind');

    const invalid = logsSearchSchema.parse({ event_kind: 'bogus' });
    expect(invalid.event_kind).toBeUndefined();
  });

  it('normalizes Unix bounds outside the JavaScript Date range', () => {
    const parsed = logsSearchSchema.parse({
      time_range: 'custom',
      since_unix_secs: '8640000000001',
      until_unix_secs: '9007199254740991',
    });

    expect(parsed.since_unix_secs).toBeUndefined();
    expect(parsed.until_unix_secs).toBeUndefined();
  });

  it('accepts the maximum four-digit-year boundary and rejects the next second', () => {
    const accepted = logsSearchSchema.parse({
      time_range: 'custom',
      since_unix_secs: String(MAX_FORMATTABLE_UNIX_SECONDS - 59),
      until_unix_secs: String(MAX_FORMATTABLE_UNIX_SECONDS),
    });
    expect(accepted.since_unix_secs).toBe(MAX_FORMATTABLE_UNIX_SECONDS - 59);
    expect(accepted.until_unix_secs).toBe(MAX_FORMATTABLE_UNIX_SECONDS);

    const rejected = logsSearchSchema.parse({
      time_range: 'custom',
      since_unix_secs: String(MAX_FORMATTABLE_UNIX_SECONDS + 1),
    });
    expect(rejected.since_unix_secs).toBeUndefined();
  });

  it('normalizes an inverted custom range', () => {
    const parsed = logsSearchSchema.parse({
      time_range: 'custom',
      since_unix_secs: '200',
      until_unix_secs: '100',
    });

    expect(parsed.since_unix_secs).toBeUndefined();
    expect(parsed.until_unix_secs).toBeUndefined();
  });
});

describe('getLogsRouteState', () => {
  it('disables effective tailing when the range has a fixed end', () => {
    expect(
      getLogsRouteState({
        userRequestedTailing: true,
        until_unix_secs: 1_700_000_000,
      }).effectiveTailing,
    ).toBe(false);
  });

  it('enables effective tailing when the right edge stays open', () => {
    expect(
      getLogsRouteState({
        userRequestedTailing: true,
      }).effectiveTailing,
    ).toBe(true);
  });
});

describe('buildHistoricalFilters', () => {
  it('sends both absolute bounds when the range has a fixed end', () => {
    expect(
      buildHistoricalFilters({
        session: 'session-1',
        event_kind: 'messages',
        since_unix_secs: 100,
        until_unix_secs: 200,
      }),
    ).toEqual({
      thread_id: 'session-1',
      event_kind: 'messages',
      since_unix_secs: '100',
      until_unix_secs: '200',
    });
  });

  it('omits until when the right edge stays pinned to now', () => {
    expect(
      buildHistoricalFilters({
        principal_id: 'principal-1',
        event_kind: 'messages',
        since_unix_secs: 100,
      }),
    ).toEqual({
      principal_id: 'principal-1',
      event_kind: 'messages',
      since_unix_secs: '100',
    });
  });

  it('sends no time keys when the range is unbounded', () => {
    const unbounded = buildHistoricalFilters({ event_kind: 'messages' });
    expect(unbounded).toEqual({ event_kind: 'messages' });
    expect(unbounded).not.toHaveProperty('since_unix_secs');
    expect(unbounded).not.toHaveProperty('until_unix_secs');
  });

  it('maps every route filter to the backend contract', () => {
    const filters = {
      principal_id: 'principal-1',
      upstream_id: '018f0000-0000-7000-8000-000000000001',
      session: 'session-1',
      model: 'claude-sonnet-4-5',
      status: '4xx' as const,
      event_kind: 'renewal' as const,
      time_range: 'custom' as const,
      since_unix_secs: 100,
      until_unix_secs: 200,
    };
    const live = {
      principal_id: 'principal-1',
      upstream_id: '018f0000-0000-7000-8000-000000000001',
      thread_id: 'session-1',
      model: 'claude-sonnet-4-5',
      status_class: '4xx',
      event_kind: 'renewal',
    };
    expect(buildHistoricalFilters(filters)).toEqual({
      ...live,
      since_unix_secs: '100',
      until_unix_secs: '200',
    });
    expect(buildLiveFilters(filters)).toEqual(live);
    expect(buildLiveFilters(filters)).not.toHaveProperty('status');
    expect(buildLiveFilters(filters)).not.toHaveProperty('session');
  });
  it('applies the errors filter before server pagination', () => {
    const filters = { status: 'errors' as const };
    expect(buildLiveFilters(filters)).toEqual({ status_class: 'errors' });
    expect(buildHistoricalFilters(filters)).toEqual({ status_class: 'errors' });
  });
});
