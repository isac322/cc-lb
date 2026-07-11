import { splitNum } from '../../lib/format';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { BreakdownPopover, fmtTokens } from './BreakdownPopover';
import { cx, Hint } from './primitives';
import { Sparkline } from './Sparkline';
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

function hitRatioPercent(b: TokenBreakdown): number {
  const denom = totalInputTokens(b);
  if (denom <= 0) return 0;
  return Math.round((b.cr / denom) * 100);
}

export function TokenCell({
  event,
  isPartial,
}: {
  event: RequestEventWithPhase;
  isPartial?: boolean;
}) {
  const b = tokenBreakdown(event);
  const hit = hitRatioPercent(b);
  const inp = splitNum(totalInputTokens(b));
  const out = splitNum(b.output);

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
      className="p-0 text-right whitespace-nowrap"
      onClick={(e) => e.stopPropagation()}
    >
      <Hint label={popover}>
        <div
          className={cx(
            'px-3 py-2 cursor-help block',
            isPartial ? 'animate-pulse' : '',
          )}
        >
          <div className="flex items-baseline justify-end tabular-nums leading-tight">
            <span className="shrink-0 w-[4ch] text-right text-sky-400">
              {inp.value}
            </span>
            <span className="shrink-0 w-[1ch] text-left text-text-faint">
              {inp.unit}
            </span>
            <span className="shrink-0 w-[2ch] text-center text-text-faint">
              /
            </span>
            <span className="shrink-0 w-[4ch] text-right text-violet-400">
              {out.value}
            </span>
            <span className="shrink-0 w-[1ch] text-left text-text-faint">
              {out.unit}
            </span>
            <span className="shrink-0 w-[3ch] text-right text-[10px] text-text-faint ml-3">
              hit
            </span>
            <span
              className={cx(
                'shrink-0 w-[3ch] text-right text-[10px] tabular-nums ml-1',
                hit > 0 ? 'text-emerald-400' : 'text-text-faint',
              )}
            >
              {hit}
            </span>
            <span className="shrink-0 w-[1ch] text-left text-[10px] text-text-faint">
              %
            </span>
          </div>
          <Sparkline
            segments={[
              { value: b.input, color: SLICE_COLORS.input },
              { value: b.output, color: SLICE_COLORS.output },
              { value: b.cc_5m, color: SLICE_COLORS.cache_create_5m },
              { value: b.cc_1h, color: SLICE_COLORS.cache_create_1h },
              { value: b.cr, color: SLICE_COLORS.cache_read },
            ]}
          />
        </div>
      </Hint>
    </td>
  );
}
