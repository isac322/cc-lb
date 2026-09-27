import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { BreakdownPopover, fmtTokens } from './BreakdownPopover';
import { cx, Hint } from './primitives';
import { SLICE_COLORS } from './usage/sliceColors';

type TokenBreakdown = {
  input: number;
  output: number;
  cc_5m: number;
  cc_1h: number;
  cr: number;
};

function tokenBreakdown(e: RequestEventWithPhase): TokenBreakdown {
  const input = e.input_tokens ?? 0;
  const output = e.output_tokens ?? 0;
  const cr = e.cache_read_input_tokens ?? 0;
  const cc_5m_split = e.cache_creation_input_tokens_5m;
  const cc_1h_split = e.cache_creation_input_tokens_1h;
  if (cc_5m_split != null || cc_1h_split != null) {
    return {
      input,
      output,
      cc_5m: cc_5m_split ?? 0,
      cc_1h: cc_1h_split ?? 0,
      cr,
    };
  }
  const legacy = e.cache_creation_input_tokens ?? 0;
  return { input, output, cc_5m: legacy, cc_1h: 0, cr };
}

function totalInputTokens(b: TokenBreakdown): number {
  return b.input + b.cc_5m + b.cc_1h + b.cr;
}

/**
 * Share of the prompt served from cache, as a whole percent, or null when the
 * request read nothing from cache (the table hides the column when every row
 * is null).
 */
export function cacheHitPercent(e: RequestEventWithPhase): number | null {
  const b = tokenBreakdown(e);
  const denom = totalInputTokens(b);
  if (denom <= 0 || b.cr <= 0) return null;
  return Math.round((b.cr / denom) * 100);
}

/** "2.8k → 8": prompt tokens in, output tokens out, one size, tabular. */
export function TokenCell({
  event,
  isPartial,
  className,
}: {
  event: RequestEventWithPhase;
  isPartial?: boolean;
  className?: string;
}) {
  const b = tokenBreakdown(event);

  const popover = (
    <BreakdownPopover
      title="Tokens"
      rows={[
        {
          label: 'Input',
          value: b.input,
          color: SLICE_COLORS.input,
          fmt: fmtTokens,
        },
        {
          label: 'Output',
          value: b.output,
          color: SLICE_COLORS.output,
          fmt: fmtTokens,
        },
        {
          label: 'Cache create 5m',
          value: b.cc_5m,
          color: SLICE_COLORS.cache_create_5m,
          fmt: fmtTokens,
        },
        {
          label: 'Cache create 1h',
          value: b.cc_1h,
          color: SLICE_COLORS.cache_create_1h,
          fmt: fmtTokens,
        },
        {
          label: 'Cache read',
          value: b.cr,
          color: SLICE_COLORS.cache_read,
          fmt: fmtTokens,
        },
      ]}
    />
  );

  return (
    <td
      className={cx(
        'px-3 py-2 text-right tabular-nums whitespace-nowrap',
        className,
      )}
      onClick={(e) => e.stopPropagation()}
    >
      <Hint label={popover}>
        <span className={cx('cursor-help', isPartial ? 'animate-pulse' : '')}>
          <span className="text-text">{fmtTokens(totalInputTokens(b))}</span>
          <span aria-hidden="true" className="px-1 text-text-faint">
            →
          </span>
          <span className="sr-only"> in, </span>
          <span className="text-text">{fmtTokens(b.output)}</span>
          <span className="sr-only"> out</span>
        </span>
      </Hint>
    </td>
  );
}
