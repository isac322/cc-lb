import { describe, expect, it } from 'vitest';
import {
  cacheHitRatio,
  cacheMissRatio,
  fmtChartTooltipTs,
  fmtMsCompact,
  fmtSetupMs,
  formatBigInteger,
  formatCostMicros,
  formatCount,
  formatRate,
  formatUsdAmount,
  getRequestOutcome,
} from './format';

describe('cache ratios', () => {
  it('returns null for a zero denominator', () => {
    const components = {};
    expect(cacheHitRatio(components)).toBeNull();
    expect(cacheMissRatio(components)).toBeNull();
  });

  it('reports a pure cache hit', () => {
    const components = { cache_read_input_tokens: 500 };
    expect(cacheHitRatio(components)).toBe(1);
    expect(cacheMissRatio(components)).toBe(0);
  });

  it('reports a pure cache creation miss', () => {
    const components = { cache_creation_input_tokens: 500 };
    expect(cacheHitRatio(components)).toBe(0);
    expect(cacheMissRatio(components)).toBe(1);
  });

  it('includes input, creation, and read tokens in the denominator', () => {
    const components = {
      input_tokens: 200,
      cache_creation_input_tokens: 300,
      cache_read_input_tokens: 500,
    };
    expect(cacheHitRatio(components)).toBe(0.5);
    expect(cacheMissRatio(components)).toBe(0.5);
  });
});

describe('getRequestOutcome', () => {
  it('returns partial for isPartial=true', () => {
    expect(getRequestOutcome(true, 200, null)).toEqual({ type: 'partial' });
  });
  it('returns client_disconnected for 499 + client_closed_request', () => {
    expect(getRequestOutcome(false, 499, 'client_closed_request')).toEqual({
      type: 'client_disconnected',
      status: 499,
      error_code: 'client_closed_request',
    });
  });
  it('returns completed for 499 with other error code', () => {
    expect(getRequestOutcome(false, 499, 'other_error')).toEqual({
      type: 'completed',
      status: 499,
      error_code: 'other_error',
    });
  });
  it('returns completed for 0/terminal_dropped', () => {
    expect(getRequestOutcome(false, 0, 'terminal_dropped')).toEqual({
      type: 'completed',
      status: 0,
      error_code: 'terminal_dropped',
    });
  });
  it('returns completed for 504/tower_timeout', () => {
    expect(getRequestOutcome(false, 504, 'tower_timeout')).toEqual({
      type: 'completed',
      status: 504,
      error_code: 'tower_timeout',
    });
  });
  it('returns completed for 200', () => {
    expect(getRequestOutcome(false, 200, null)).toEqual({
      type: 'completed',
      status: 200,
      error_code: null,
    });
  });
  it('returns semantic_error for a completed 2xx upstream stream error', () => {
    expect(
      getRequestOutcome(
        false,
        200,
        'upstream_stream_error',
        'overloaded_error',
      ),
    ).toEqual({
      type: 'semantic_error',
      status: 200,
      error_code: 'upstream_stream_error',
      upstream_error_type: 'overloaded_error',
      label: 'overloaded_error',
    });
  });
  it('falls back to the terminal error code when the upstream type is absent', () => {
    expect(
      getRequestOutcome(false, 200, 'upstream_stream_error', null),
    ).toEqual({
      type: 'semantic_error',
      status: 200,
      error_code: 'upstream_stream_error',
      upstream_error_type: null,
      label: 'upstream_stream_error',
    });
  });
  it('returns semantic_error for a 2xx upstream refusal', () => {
    expect(
      getRequestOutcome(false, 200, 'upstream_refusal', 'refusal'),
    ).toEqual({
      type: 'semantic_error',
      status: 200,
      error_code: 'upstream_refusal',
      upstream_error_type: 'refusal',
      label: 'refusal',
    });
  });
  it('returns semantic_error for any 2xx row carrying a terminal error code', () => {
    expect(getRequestOutcome(false, 200, 'terminal_without_partial')).toEqual({
      type: 'semantic_error',
      status: 200,
      error_code: 'terminal_without_partial',
      upstream_error_type: undefined,
      label: 'terminal_without_partial',
    });
  });
  it('keeps an HTTP error numeric even when structured upstream details exist', () => {
    expect(
      getRequestOutcome(false, 429, 'upstream_4xx', 'rate_limit_error'),
    ).toEqual({
      type: 'completed',
      status: 429,
      error_code: 'upstream_4xx',
    });
  });
});

describe('formatCount', () => {
  it('uses en-US grouping at count boundaries', () => {
    expect(formatCount(999)).toBe('999');
    expect(formatCount(1_000)).toBe('1,000');
    expect(formatCount(1_234_567)).toBe('1,234,567');
  });

  it('handles missing, invalid, and overflowing counts', () => {
    expect(formatCount(null)).toBe('—');
    expect(formatCount(undefined)).toBe('—');
    expect(formatCount(Number.NaN)).toBe('—');
    expect(formatCount(Number.POSITIVE_INFINITY)).toBe('—');
    expect(formatCount(-1)).toBe('—');
    expect(formatCount(Number.MAX_SAFE_INTEGER)).toBe('9,007,199,254,740,991');
    expect(formatCount(Number.MAX_SAFE_INTEGER + 1)).toBe('—');
  });
});

describe('formatRate', () => {
  it('uses adaptive significant precision for nonzero rates', () => {
    expect(formatRate(1 / 60)).toBe('0.0167');
    expect(formatRate(1 / 3_600)).toBe('0.000278');
    expect(formatRate(1_234.5)).toBe('1,230');
    expect(formatRate(Number.MIN_VALUE)).not.toBe('0');
  });

  it('handles zero, missing, invalid, negative, and overflowing rates', () => {
    expect(formatRate(0)).toBe('0');
    expect(formatRate(null)).toBe('—');
    expect(formatRate(undefined)).toBe('—');
    expect(formatRate(Number.NaN)).toBe('—');
    expect(formatRate(Number.POSITIVE_INFINITY)).toBe('—');
    expect(formatRate(-0.1)).toBe('—');
    expect(formatRate(Number.MAX_SAFE_INTEGER + 1)).toBe('—');
  });
});

describe('formatUsdAmount', () => {
  it('groups the integer portion of dollar values', () => {
    expect(formatUsdAmount(999.99)).toBe('$999.99');
    expect(formatUsdAmount(1_234.5)).toBe('$1,235');
  });

  it('handles missing, invalid, negative, and overflowing values', () => {
    expect(formatUsdAmount(null)).toBe('—');
    expect(formatUsdAmount(undefined)).toBe('—');
    expect(formatUsdAmount(Number.NaN)).toBe('—');
    expect(formatUsdAmount(Number.POSITIVE_INFINITY)).toBe('—');
    expect(formatUsdAmount(-1)).toBe('—');
    expect(formatUsdAmount(Number.MAX_SAFE_INTEGER + 1)).toBe('—');
  });
});

describe('formatBigInteger', () => {
  it('returns dash for null/undefined', () => {
    expect(formatBigInteger(null)).toBe('—');
    expect(formatBigInteger(undefined)).toBe('—');
  });
  it('returns dash for non-finite', () => {
    expect(formatBigInteger(Number.NaN)).toBe('—');
    expect(formatBigInteger(Number.POSITIVE_INFINITY)).toBe('—');
  });
  it('returns dash for overflow', () => {
    expect(formatBigInteger(2 ** 53)).toBe('—');
  });
  it('formats normal numbers', () => {
    expect(formatBigInteger(1234567)).toBe('1,234,567');
  });
});

describe('formatCostMicros', () => {
  it('returns dash for null/undefined', () => {
    expect(formatCostMicros(null)).toBe('—');
    expect(formatCostMicros(undefined)).toBe('—');
  });
  it('returns dash for negative', () => {
    expect(formatCostMicros(-12345)).toBe('—');
  });
  it('returns dash for non-finite', () => {
    expect(formatCostMicros(Number.NaN)).toBe('—');
    expect(formatCostMicros(Number.POSITIVE_INFINITY)).toBe('—');
  });
  it('returns dash for overflow', () => {
    expect(formatCostMicros(2 ** 53)).toBe('—');
  });
  it('formats positive values as USD', () => {
    expect(formatCostMicros(1234567)).toBe('$1.2346');
    expect(formatCostMicros(1_234_567_800)).toBe('$1,234.5678');
  });
});

describe('fmtSetupMs', () => {
  it('distinguishes exact zero from fractional setup timings', () => {
    expect(fmtSetupMs(0)).toBe('0 ms');
    expect(fmtSetupMs(0.125)).toBe('0.125 ms');
    expect(fmtSetupMs(0.0001)).toBe('<0.001 ms');
  });
});

describe('fmtMsCompact', () => {
  it('returns dash for null', () => {
    expect(fmtMsCompact(null)).toEqual({ value: '—', unit: '' });
  });
  it('returns dash for undefined', () => {
    expect(fmtMsCompact(undefined)).toEqual({ value: '—', unit: '' });
  });
  it('formats 0 as 0ms', () => {
    expect(fmtMsCompact(0)).toEqual({ value: '0', unit: 'ms' });
  });
  it('formats sub-second as ms', () => {
    expect(fmtMsCompact(850)).toEqual({ value: '850', unit: 'ms' });
  });
  it('rounds sub-second to integer ms', () => {
    expect(fmtMsCompact(123.7)).toEqual({ value: '124', unit: 'ms' });
  });
  it('formats 1000 as 1.0s', () => {
    expect(fmtMsCompact(1000)).toEqual({ value: '1.0', unit: 's' });
  });
  it('formats 1500 as 1.5s', () => {
    expect(fmtMsCompact(1500)).toEqual({ value: '1.5', unit: 's' });
  });
  it('formats 12345 as 12.3s', () => {
    expect(fmtMsCompact(12_345)).toEqual({ value: '12.3', unit: 's' });
  });
  it('formats 60000 as 1.0m', () => {
    expect(fmtMsCompact(60_000)).toEqual({ value: '1.0', unit: 'm' });
  });
  it('formats 72000 as 1.2m', () => {
    expect(fmtMsCompact(72_000)).toEqual({ value: '1.2', unit: 'm' });
  });
});

describe('fmtChartTooltipTs', () => {
  it('returns dash for null', () => {
    expect(fmtChartTooltipTs(null)).toBe('—');
  });
  it('returns dash for undefined', () => {
    expect(fmtChartTooltipTs(undefined)).toBe('—');
  });
  it('returns dash for NaN', () => {
    expect(fmtChartTooltipTs(Number.NaN)).toBe('—');
  });
  it('does not return the raw epoch number as a string', () => {
    const epoch = 1_718_553_120;
    expect(fmtChartTooltipTs(epoch)).not.toBe(String(epoch));
  });
  it('matches the M/D HH:MM pattern with padded hour and minute', () => {
    const epoch = 1_718_553_120;
    expect(fmtChartTooltipTs(epoch)).toMatch(/^\d{1,2}\/\d{1,2} \d{2}:\d{2}$/);
  });
  it('pads HH:MM to two digits for early-morning times', () => {
    const d = new Date();
    d.setHours(9, 5, 0, 0);
    const ts = Math.floor(d.getTime() / 1000);
    expect(fmtChartTooltipTs(ts)).toMatch(/ 09:05$/);
  });
  it('uses local month/day with a slash separator before the time', () => {
    const d = new Date();
    d.setHours(12, 30, 0, 0);
    const ts = Math.floor(d.getTime() / 1000);
    const expected = `${d.getMonth() + 1}/${d.getDate()} 12:30`;
    expect(fmtChartTooltipTs(ts)).toBe(expected);
  });
});
