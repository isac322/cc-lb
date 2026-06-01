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
