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
  it('disables sentinel when session filter is active', () => {
    const state = getLogsRouteState({
      sessionFilter: 'session_123',
      hasNextPage: true,
      userRequestedTailing: true,
      isLastClientPage: true,
    });
    expect(state.showSentinel).toBe(false);
  });

  it('enables sentinel when no session filter, hasNextPage is true, and isLastClientPage is true', () => {
    const state = getLogsRouteState({
      sessionFilter: undefined,
      hasNextPage: true,
      userRequestedTailing: true,
      isLastClientPage: true,
    });
    expect(state.showSentinel).toBe(true);
  });

  it('disables sentinel when isLastClientPage is false', () => {
    const state = getLogsRouteState({
      sessionFilter: undefined,
      hasNextPage: true,
      userRequestedTailing: true,
      isLastClientPage: false,
    });
    expect(state.showSentinel).toBe(false);
  });

  it('disables effective tailing when custom time range is set', () => {
    const state = getLogsRouteState({
      sessionFilter: undefined,
      hasNextPage: true,
      userRequestedTailing: true,
      time_range: 'custom',
      isLastClientPage: true,
    });
    expect(state.effectiveTailing).toBe(false);
  });

  it('enables effective tailing when user requested it and no custom time range', () => {
    const state = getLogsRouteState({
      sessionFilter: undefined,
      hasNextPage: true,
      userRequestedTailing: true,
      time_range: '1h',
      isLastClientPage: true,
    });
    expect(state.effectiveTailing).toBe(true);
  });
});

describe('buildHistoricalFilters', () => {
  it('sends both custom bounds only when the range is complete', () => {
    expect(
      buildHistoricalFilters({
        session: 'client-only',
        time_range: 'custom',
        since_unix_secs: 100,
        until_unix_secs: 200,
      }),
    ).toEqual({ since_unix_secs: '100', until_unix_secs: '200' });

    expect(
      buildHistoricalFilters({
        time_range: 'custom',
        since_unix_secs: 100,
      }),
    ).toEqual({});
  });

  it('sends only since for a preset and no time keys for All time', () => {
    expect(
      buildHistoricalFilters({
        principal_id: 'principal-1',
        time_range: '1h',
        since_unix_secs: 100,
      }),
    ).toEqual({ principal_id: 'principal-1', since_unix_secs: '100' });

    const all = buildHistoricalFilters({
      time_range: 'all',
      since_unix_secs: 100,
      until_unix_secs: 200,
    });
    expect(all).toEqual({});
    expect(all).not.toHaveProperty('since');
    expect(all).not.toHaveProperty('until');
  });

  it('maps the status filter to the backend status_class param', () => {
    const filters = {
      session: 'client-only',
      status: '4xx' as const,
      time_range: 'custom' as const,
      since_unix_secs: 100,
      until_unix_secs: 200,
    };
    expect(buildHistoricalFilters(filters)).toEqual({
      status_class: '4xx',
      since_unix_secs: '100',
      until_unix_secs: '200',
    });
    expect(buildLiveFilters(filters)).toEqual({ status_class: '4xx' });
    expect(buildLiveFilters(filters)).not.toHaveProperty('status');
  });
});
