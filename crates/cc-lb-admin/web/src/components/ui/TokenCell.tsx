import { splitNum } from '../../lib/format';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { BreakdownPopover, fmtTokens } from './BreakdownPopover';
import { MetricCell } from './MetricCell';
import { cx } from './primitives';
import { USAGE_CATEGORIES, type UsageCategoryKey } from './usage/sliceColors';

type TokenBreakdown = Record<UsageCategoryKey, number>;

function tokenBreakdown(e: RequestEventWithPhase): TokenBreakdown {
  return {
    cache_read: e.cache_read_input_tokens ?? 0,
    cache_create_5m: e.cache_creation_input_tokens_5m ?? 0,
    cache_create_1h: e.cache_creation_input_tokens_1h ?? 0,
    input: e.input_tokens ?? 0,
    output: e.output_tokens ?? 0,
  };
}

/**
 * Share of the prompt served from cache, in whole percent rounded down (so
 * 100% means fully cached and a partial hit never reads as full), or null
 * when the request has no prompt tokens yet.
 */
function cacheHitPercent(b: TokenBreakdown, promptTokens: number) {
  if (promptTokens <= 0) return null;
  return Math.floor((b.cache_read / promptTokens) * 100);
}

/**
 * One token count in a fixed-width slot: the figure right-aligned, its k / m
 * unit in a fixed trailing slot, so every row's digits line up whatever the
 * magnitude ("21", "2.8k", "120k", "1.2M").
 */
function TokenFigure({ tokens, slot }: { tokens: number; slot: string }) {
  const { value, unit } = splitNum(tokens);
  return (
    <span
      data-slot={slot}
      className="inline-flex w-[5.5ch] shrink-0 justify-end"
    >
      <span className="text-text">{value}</span>
      <span className="w-[1.5ch] shrink-0 text-left text-text-muted">
        {unit}
      </span>
    </span>
  );
}

/**
 * "2.8k → 812   94% hit" (prompt tokens in, output tokens out, the share of
 * the prompt served from cache) over a bar of the request's token
 * composition in `USAGE_CATEGORIES` order. Each figure sits in a fixed-width,
 * right-aligned slot so the columns of in, out and hit line up down the table.
 * The "hit" word stays in the cell rather than the header because the card
 * layout has no header.
 */
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
  const prompt = b.cache_read + b.cache_create_5m + b.cache_create_1h + b.input;
  const hit = cacheHitPercent(b, prompt);
  const rows = USAGE_CATEGORIES.map((category) => ({
    label: category.label,
    color: category.color,
    value: b[category.key],
  }));

  const popover = (
    <BreakdownPopover
      title="Tokens"
      rows={rows.map((row) => ({ ...row, fmt: fmtTokens }))}
      footer={
        hit != null
          ? { label: 'Cache hit of prompt', value: hit, fmt: (v) => `${v}%` }
          : null
      }
    />
  );

  return (
    <MetricCell
      className={className}
      label={`Tokens ${fmtTokens(prompt)} in, ${fmtTokens(b.output)} out${
        hit != null ? `, cache hit ${hit}%` : ''
      }, show breakdown`}
      popover={popover}
      segments={rows}
      pulse={isPartial}
    >
      <span className="inline-flex items-baseline">
        <TokenFigure tokens={prompt} slot="in" />
        <span
          aria-hidden="true"
          className="w-[2ch] shrink-0 text-center text-text-faint"
        >
          →
        </span>
        <TokenFigure tokens={b.output} slot="out" />
        <span
          data-slot="hit"
          className={cx(
            'ml-2 w-[4.5ch] shrink-0 text-right',
            hit == null ? 'text-text-faint' : 'text-text',
          )}
        >
          {hit == null ? '—' : `${hit}%`}
        </span>
        <span
          className={cx(
            'ml-1 text-caption text-text-faint',
            hit == null ? 'invisible' : '',
          )}
        >
          hit
        </span>
      </span>
    </MetricCell>
  );
}
