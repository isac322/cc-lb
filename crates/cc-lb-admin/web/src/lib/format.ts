import type { RequestEvent } from './api';

const DASH = '—';

export function fmtMs(v: number | null | undefined): string {
  return v == null ? DASH : `${Math.round(v).toLocaleString()}ms`;
}

export function fmtN(v: number | null | undefined): string {
  return v == null ? DASH : v.toLocaleString();
}

export function fmtBytes(v: number | null | undefined): string {
  if (v == null) return DASH;
  if (v < 1024) return `${v} B`;
  if (v < 1024 * 1024) return `${(v / 1024).toFixed(1)} KB`;
  return `${(v / (1024 * 1024)).toFixed(2)} MB`;
}

export function fmtUsd(micros: number | null | undefined): string {
  if (micros == null) return DASH;
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
  if (micros == null) return DASH;
  const usd = micros / 1_000_000;
  return `$${usd.toFixed(4)}`;
}

export function fmtMsCompact(ms: number | null | undefined): {
  value: string;
  unit: string;
} {
  if (ms == null) return { value: '—', unit: '' };
  if (ms < 1000) return { value: String(Math.round(ms)), unit: 'ms' };
  if (ms < 60_000) return { value: (ms / 1000).toFixed(1), unit: 's' };
  return { value: (ms / 60_000).toFixed(1), unit: 'm' };
}
