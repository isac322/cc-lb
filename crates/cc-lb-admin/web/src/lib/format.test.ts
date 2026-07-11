import { describe, expect, it } from 'vitest';
import {
  fmtChartTooltipTs,
  fmtMsCompact,
  formatBigInteger,
  formatCostMicros,
  getRequestOutcome,
} from './format';

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
