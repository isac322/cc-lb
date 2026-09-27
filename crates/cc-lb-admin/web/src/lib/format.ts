const DASH = '—';
const EN_US_NUMBER = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 20,
});
const EN_US_RATE = new Intl.NumberFormat('en-US', {
  maximumSignificantDigits: 3,
});
const EN_US_USD_0 = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 0,
  minimumFractionDigits: 0,
});
const EN_US_USD_2 = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 2,
  minimumFractionDigits: 2,
});
const EN_US_USD_4 = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 4,
  minimumFractionDigits: 4,
});

function formatUsdValue(usd: number, fractionDigits: 0 | 2 | 4): string {
  const formatter =
    fractionDigits === 0
      ? EN_US_USD_0
      : fractionDigits === 2
        ? EN_US_USD_2
        : EN_US_USD_4;
  return `$${formatter.format(usd === 0 ? 0 : usd)}`;
}

/** Token components of a usage bucket or request event. */
export interface TokenComponents {
  input_tokens?: number | null;
  output_tokens?: number | null;
  cache_creation_input_tokens?: number | null;
  cache_read_input_tokens?: number | null;
}

/**
 * Total tokens the model processed.
 *
 * All four components must be summed: `input_tokens` counts only the tokens after the last cache
 * breakpoint, so on a cached workload the cache read and creation terms carry most of the volume
 * and an input+output sum understates throughput by an order of magnitude.
 */
export function sumTokens(b: TokenComponents): number {
  return (
    (b.input_tokens ?? 0) +
    (b.output_tokens ?? 0) +
    (b.cache_creation_input_tokens ?? 0) +
    (b.cache_read_input_tokens ?? 0)
  );
}

export function fmtMs(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v) || v < 0) return DASH;
  if (v > Number.MAX_SAFE_INTEGER) return DASH;
  return `${EN_US_NUMBER.format(Math.round(v))} ms`;
}

export function fmtSetupMs(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v) || v < 0) return DASH;
  if (v > Number.MAX_SAFE_INTEGER) return DASH;
  if (v === 0) return '0 ms';
  if (v < 0.001) return '<0.001 ms';
  if (v < 1) return `${EN_US_NUMBER.format(Number(v.toFixed(3)))} ms`;
  return fmtMs(v);
}

export function fmtN(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return DASH;
  if (v > Number.MAX_SAFE_INTEGER) return DASH;
  return EN_US_NUMBER.format(v === 0 ? 0 : v);
}

export function formatCount(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v) || v < 0) return DASH;
  if (v > Number.MAX_SAFE_INTEGER) return DASH;
  return EN_US_NUMBER.format(v === 0 ? 0 : v);
}

/**
 * Requests per second: three significant digits, and `<0.01` below one per
 * hundred seconds so a quiet window never reads as a long run of zeros.
 */
export function formatRate(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v) || v < 0) return DASH;
  if (v > Number.MAX_SAFE_INTEGER) return DASH;
  if (v > 0 && v < 0.01) return '<0.01';
  return EN_US_RATE.format(v === 0 ? 0 : v);
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
  return formatUsdValue(micros / 1_000_000, 4);
}

export function formatUsdAmount(usd: number | null | undefined): string {
  if (usd == null || !Number.isFinite(usd) || usd < 0) return DASH;
  if (usd > Number.MAX_SAFE_INTEGER) return DASH;
  return formatUsdValue(usd, usd >= 1_000 ? 0 : 2);
}

export function formatBigInteger(
  n: number | null | undefined,
  _maxSafeDigits = 15,
): string {
  if (n == null || !Number.isFinite(n)) return DASH;
  if (n > Number.MAX_SAFE_INTEGER) return DASH;
  return EN_US_NUMBER.format(n === 0 ? 0 : n);
}

export function formatCostMicros(micros?: number | null): string {
  if (micros == null || !Number.isFinite(micros) || micros < 0) return DASH;
  if (micros > Number.MAX_SAFE_INTEGER) return DASH;
  return formatUsdValue(micros / 1_000_000, 4);
}

export type RequestOutcome =
  | { type: 'partial' }
  | { type: 'client_disconnected'; status: number; error_code: string }
  | {
      type: 'semantic_error';
      status: number;
      error_code?: string | null;
      upstream_error_type?: string | null;
      label: string;
    }
  | { type: 'completed'; status: number; error_code?: string | null };

export function getRequestOutcome(
  isPartial: boolean,
  status: number,
  error_code?: string | null,
  upstream_error_type?: string | null,
): RequestOutcome {
  if (isPartial) return { type: 'partial' };
  if (status === 499 && error_code === 'client_closed_request') {
    return { type: 'client_disconnected', status, error_code };
  }
  // A recorded error_code on a 2xx means the response was delivered to the
  // client as a success but the request still ended abnormally (mid-stream
  // error, upstream refusal, context-window overflow, …). The wire status
  // stays 200 while the outcome is an error, so any terminal error code —
  // not a per-code allowlist — marks the row.
  if (status >= 200 && status < 300 && error_code != null) {
    return {
      type: 'semantic_error',
      status,
      error_code,
      upstream_error_type,
      label: upstream_error_type || error_code || 'Request failed',
    };
  }
  return { type: 'completed', status, error_code };
}

export function statusTone(s: number): 'ok' | 'warn' | 'danger' | 'neutral' {
  if (s >= 500) return 'danger';
  if (s >= 400) return 'warn';
  if (s >= 200 && s < 300) return 'ok';
  return 'neutral';
}

export function requestOutcomeTone(
  outcome: RequestOutcome,
): 'ok' | 'warn' | 'danger' | 'neutral' {
  if (outcome.type === 'partial') return 'neutral';
  if (outcome.type === 'client_disconnected') return 'warn';
  if (outcome.type === 'semantic_error') return 'danger';
  return statusTone(outcome.status);
}

export function cacheHitRatio(components: TokenComponents): number | null {
  const input = components.input_tokens ?? 0;
  const created = components.cache_creation_input_tokens ?? 0;
  const read = components.cache_read_input_tokens ?? 0;
  const denom = input + created + read;
  if (denom <= 0) return null;
  return read / denom;
}

export function cacheMissRatio(components: TokenComponents): number | null {
  const input = components.input_tokens ?? 0;
  const created = components.cache_creation_input_tokens ?? 0;
  const read = components.cache_read_input_tokens ?? 0;
  const denom = input + created + read;
  if (denom <= 0) return null;
  return (input + created) / denom;
}

export interface SplitNumber {
  value: string;
  /** SI prefix — 'k' stays lowercase; 'M'/'B' never read as minutes/milli. */
  unit: '' | 'k' | 'M' | 'B';
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
    return { value: roundCompact(v / 1_000_000), unit: 'M' };
  }
  return { value: roundCompact(v / 1_000_000_000), unit: 'B' };
}

function roundCompact(v: number): string {
  if (v >= 100) return Math.round(v).toString();
  if (v >= 10) return v.toFixed(1).replace(/\.0$/, '');
  return v.toFixed(1);
}

export function fmtUsdCompact(micros: number | null | undefined): string {
  if (micros == null || !Number.isFinite(micros) || micros < 0) return DASH;
  if (micros > Number.MAX_SAFE_INTEGER) return DASH;
  return formatUsdValue(micros / 1_000_000, 4);
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
