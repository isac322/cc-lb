import type { RequestEvent } from './api';

const DASH = '—';

export function fmtMs(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v) || v < 0) return DASH;
  if (v > Number.MAX_SAFE_INTEGER) return DASH;
  return `${Math.round(v).toLocaleString()} ms`;
}

export function fmtN(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return DASH;
  if (v > Number.MAX_SAFE_INTEGER) return DASH;
  return v.toLocaleString();
}

export function fmtBytes(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v) || v < 0) return DASH;
  if (v > Number.MAX_SAFE_INTEGER) return DASH;
  if (v < 1024) return `${v} B`;
  if (v < 1024 * 1024) return `${(v / 1024).toFixed(1)} KB`;
  return `${(v / (1024 * 1024)).toFixed(2)} MB`;
}

export function fmtUsd(micros: number | null | undefined): string {
  if (micros == null || !Number.isFinite(micros) || micros < 0) return DASH;
  if (micros > Number.MAX_SAFE_INTEGER) return DASH;
  const usd = micros / 1_000_000;
  return `$${usd.toFixed(4)}`;
}

export function formatBigInteger(
  n: number | null | undefined,
  _maxSafeDigits = 15,
): string {
  if (n == null || !Number.isFinite(n)) return DASH;
  if (n > Number.MAX_SAFE_INTEGER) return DASH;
  return n.toLocaleString();
}

export function formatCostMicros(micros?: number | null): string {
  if (micros == null || !Number.isFinite(micros) || micros < 0) return DASH;
  if (micros > Number.MAX_SAFE_INTEGER) return DASH;
  const usd = micros / 1_000_000;
  return `$${usd.toFixed(4)}`;
}

export function statusTone(s: number): 'ok' | 'warn' | 'danger' | 'neutral' {
  if (s >= 500) return 'danger';
  if (s >= 400) return 'warn';
  if (s >= 200 && s < 300) return 'ok';
  return 'neutral';
}

export function cacheHitRatio(e: RequestEvent): number | null {
  const read = e.cache_read_input_tokens ?? 0;
  const input = e.input_tokens ?? 0;
  const denom = read + input;
  if (denom <= 0) return null;
  return read / denom;
}

export interface SplitNumber {
  value: string;
  unit: '' | 'k' | 'm' | 'b';
}

export function splitNum(v: number | null | undefined): SplitNumber {
  if (v == null || !Number.isFinite(v) || v <= 0) {
    return { value: '0', unit: '' };
  }
  if (v > Number.MAX_SAFE_INTEGER) {
    return { value: DASH, unit: '' };
  }
  if (v < 1_000) {
    return { value: Math.round(v).toString(), unit: '' };
  }
  if (v < 1_000_000) {
    return { value: roundCompact(v / 1_000), unit: 'k' };
  }
  if (v < 1_000_000_000) {
    return { value: roundCompact(v / 1_000_000), unit: 'm' };
  }
  return { value: roundCompact(v / 1_000_000_000), unit: 'b' };
}

function roundCompact(v: number): string {
  if (v >= 100) return Math.round(v).toString();
  if (v >= 10) return v.toFixed(1).replace(/\.0$/, '');
  return v.toFixed(1);
}

export function fmtUsdCompact(micros: number | null | undefined): string {
  if (micros == null || !Number.isFinite(micros) || micros < 0) return DASH;
  if (micros > Number.MAX_SAFE_INTEGER) return DASH;
  const usd = micros / 1_000_000;
  return `$${usd.toFixed(4)}`;
}

export function fmtMsCompact(ms: number | null | undefined): {
  value: string;
  unit: string;
} {
  if (ms == null || !Number.isFinite(ms) || ms < 0)
    return { value: DASH, unit: '' };
  if (ms > Number.MAX_SAFE_INTEGER) return { value: DASH, unit: '' };
  if (ms < 1000) return { value: String(Math.round(ms)), unit: 'ms' };
  if (ms < 60_000) return { value: (ms / 1000).toFixed(1), unit: 's' };
  return { value: (ms / 60_000).toFixed(1), unit: 'm' };
}

export function formatRelativeUnixSeconds(seconds: number): Date {
  return new Date(seconds * 1000);
}

export function fmtChartTooltipTs(unixSecs: number | null | undefined): string {
  if (unixSecs == null || !Number.isFinite(unixSecs)) return DASH;
  const d = new Date(unixSecs * 1000);
  const m = d.getMonth() + 1;
  const day = d.getDate();
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  return `${m}/${day} ${hh}:${mm}`;
}
