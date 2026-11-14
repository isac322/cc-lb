import type { PoolQuotaChartRow } from '../routes/-overviewPoolQuota';
import type {
  CreditHistoryPoint,
  CreditHistoryResponse,
  CreditPoolHistory,
  CreditUpstreamHistory,
} from './api';

const PLAIN_DECIMAL = /^[+-]?[0-9]+(?:\.[0-9]+)?$/;

/** Truncate the original decimal string once, without a floating-point conversion. */
export function formatCreditInteger(raw: string | null | undefined): string {
  if (raw == null || !PLAIN_DECIMAL.test(raw)) return '—';
  const unsigned = raw.replace(/^[+-]/, '');
  const integer = unsigned.split('.')[0].replace(/^0+(?=\d)/, '');
  const sign = raw.startsWith('-') && integer !== '0' ? '-' : '';
  return sign + integer.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

/** Numeric coordinates are disposable; original decimal strings remain the label. */
export function creditChartCoordinate(
  raw: string | null | undefined,
): number | null {
  if (raw == null || !PLAIN_DECIMAL.test(raw)) return null;
  const number = Number(raw);
  if (!Number.isFinite(number) || Math.abs(number) > Number.MAX_SAFE_INTEGER)
    return null;
  if (number === 0 && /[1-9]/.test(raw)) return null;
  return Object.is(number, -0) ? 0 : number;
}

export function formatCreditAxisTick(value: number): string {
  const normalized = Object.is(value, -0) ? 0 : value;
  if (Math.abs(normalized) < 10) {
    return normalized.toFixed(2).replace(/\.?0+$/, '');
  }
  if (Math.abs(normalized) < 1_000) {
    return Math.round(normalized).toString();
  }
  return new Intl.NumberFormat('en-US', {
    notation: 'compact',
    maximumFractionDigits: 1,
  }).format(normalized);
}

export interface CreditMiniData {
  history: CreditUpstreamHistory;
  start: number;
  end: number;
}

export function creditMiniData(
  response: CreditHistoryResponse | undefined,
): Map<string, CreditMiniData> {
  if (response == null) return new Map();
  const end =
    response.until_unix_secs >= response.now_unix_secs
      ? response.pool.latest.at_unix_millis / 1000
      : response.until_unix_secs;
  return new Map(
    response.upstreams.map((history) => [
      history.upstream_id,
      {
        history,
        start: response.since_unix_secs,
        end,
      },
    ]),
  );
}

export function hasCreditHistory(
  history: CreditPoolHistory | undefined,
): boolean {
  return (
    history != null &&
    (history.latest.balance != null ||
      history.series.some((point) => point.balance != null))
  );
}

export interface CreditPlotRow {
  unix: number;
  credits: number | null;
  credit_balance: string | null;
  credit_observed_at: number | null;
  credit_contributing: number;
  credit_eligible: number;
  [key: string]: number | string | null;
}

/** Merge only after quota-specific freshness and window projection. Each stream
 * retains its own explicit nulls and tail; credit keys never enter quota-only
 * heartbeat, latest-reading, or sparse-window transformations. */
export function mergeCreditChartRows(
  quota: readonly PoolQuotaChartRow[],
  credits: readonly CreditHistoryPoint[] | undefined,
): CreditPlotRow[] {
  const times = new Map<
    number,
    { quota?: PoolQuotaChartRow; credits: CreditHistoryPoint[] }
  >();
  for (const row of quota) {
    const entry = times.get(row.unix) ?? { credits: [] };
    entry.quota = row;
    times.set(row.unix, entry);
  }
  for (const point of credits ?? []) {
    const unix = point.at_unix_millis / 1000;
    const entry = times.get(unix) ?? { credits: [] };
    entry.credits.push(point);
    times.set(unix, entry);
  }
  const quotaKeys = [
    ...new Set(
      quota.flatMap((row) => Object.keys(row).filter((key) => key !== 'unix')),
    ),
  ];
  const quotaEnd = quota.at(-1)?.unix ?? Number.NEGATIVE_INFINITY;
  const creditEnd = credits?.at(-1)?.at_unix_millis ?? Number.NEGATIVE_INFINITY;
  const quotaState: Record<string, number | null> = Object.fromEntries(
    quotaKeys.map((key) => [key, null]),
  );
  let credit: CreditHistoryPoint | undefined;
  const rows: CreditPlotRow[] = [];
  const append = (unix: number) => {
    const point = unix * 1000 <= creditEnd ? credit : undefined;
    rows.push({
      ...Object.fromEntries(
        quotaKeys.map((key) => [
          key,
          unix <= quotaEnd ? quotaState[key] : null,
        ]),
      ),
      unix,
      credits: creditChartCoordinate(point?.balance),
      credit_balance: point?.balance ?? null,
      credit_observed_at: point?.observed_at_unix_millis ?? null,
      credit_contributing: point?.contributing_upstreams ?? 0,
      credit_eligible: point?.eligible_upstreams ?? 0,
    });
  };
  for (const [unix, entry] of [...times].sort(
    ([left], [right]) => left - right,
  )) {
    if (entry.quota != null) {
      for (const key of quotaKeys) {
        if (key in entry.quota) quotaState[key] = entry.quota[key];
      }
    }
    if (entry.credits.length === 0) append(unix);
    for (const point of entry.credits) {
      credit = point;
      // Keep same-time null separators: collapsing them would connect a pool
      // membership/coverage change as though an account spent its balance.
      append(unix);
    }
  }
  return rows;
}
