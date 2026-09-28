import { fnv1aHash } from './colors';

const REQUEST_KIND_BADGE_TEXT: Readonly<Record<string, string>> = {
  main: 'main',
  advisor: 'adv',
  subagent: 'sub',
  recap: 'recap',
  compaction: 'comp',
  notification: 'notif',
  session_title: 'title',
  auto_thinking: 'think',
  side: 'side',
  look_at: 'vision',
  unknown: 'unk',
};

export function requestKindBadgeText(
  requestKind: string | null | undefined,
): string | null {
  const normalized = requestKind?.trim();
  if (!normalized) return null;
  return REQUEST_KIND_BADGE_TEXT[normalized] ?? normalized;
}

/**
 * Fixed OKLCH hue per known request kind, ≥ 21° apart. The accent is
 * near-achromatic, so badges can use most of the wheel; every badge color
 * (`categoricalColor`, both themes) instead stays at least OKLab ΔE 0.08
 * from warn, danger and the cache-write series — the free arcs are 108–170
 * and 195–358 — so a kind badge never reads as a severity. `main` (the bulk
 * of traffic) and `unknown` have no hue: they render as the quiet neutral
 * badge and an outlined neutral badge.
 */
const REQUEST_KIND_HUE: Readonly<Record<string, number>> = {
  side: 112,
  session_title: 135,
  notification: 200,
  subagent: 235,
  look_at: 258,
  compaction: 281,
  auto_thinking: 304,
  advisor: 327,
  recap: 350,
};

/**
 * Hue arcs a custom kind hashes into, in order: 108–170 then 195–358 —
 * the same severity-free arcs the fixed kinds draw from.
 */
const CUSTOM_KIND_ARCS: readonly { from: number; span: number }[] = [
  { from: 108, span: 63 }, // 108..170 inclusive
  { from: 195, span: 164 }, // 195..358 inclusive
];
const CUSTOM_KIND_HUE_SPAN = CUSTOM_KIND_ARCS.reduce(
  (sum, a) => sum + a.span,
  0,
);

function customKindHue(requestKind: string): number {
  let offset = fnv1aHash(requestKind) % CUSTOM_KIND_HUE_SPAN;
  for (const arc of CUSTOM_KIND_ARCS) {
    if (offset < arc.span) return arc.from + offset;
    offset -= arc.span;
  }
  return CUSTOM_KIND_ARCS[0]!.from; // unreachable: offset < total span
}

export type RequestKindTone =
  | { kind: 'main' }
  | { kind: 'unknown' }
  | { kind: 'hue'; hue: number };

export function requestKindTone(requestKind: string): RequestKindTone {
  if (requestKind === 'main') return { kind: 'main' };
  if (requestKind === 'unknown') return { kind: 'unknown' };
  return {
    kind: 'hue',
    hue: REQUEST_KIND_HUE[requestKind] ?? customKindHue(requestKind),
  };
}
