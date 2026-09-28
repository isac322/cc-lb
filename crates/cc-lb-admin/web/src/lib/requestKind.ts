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
 * Fixed OKLCH hue per known request kind, 21° apart. Every badge color
 * (`categoricalColor`, both themes) stays at least OKLab ΔE 0.08 from warn,
 * danger and the cache-write series, and from the accent teal and
 * accent-text — the free arcs are 110–142 and 228–354 — so a kind badge
 * never reads as a severity or a brand mark. `main` (the bulk of traffic)
 * and `unknown` have no hue: they render as the quiet neutral badge and an
 * outlined neutral badge.
 */
const REQUEST_KIND_HUE: Readonly<Record<string, number>> = {
  side: 112,
  session_title: 133,
  notification: 228,
  subagent: 249,
  look_at: 270,
  compaction: 291,
  auto_thinking: 312,
  advisor: 333,
  recap: 354,
};

/** Hue arcs a custom kind hashes into: the same arcs, 110–142 then 228–354. */
const CUSTOM_KIND_LOW_FROM = 110;
const CUSTOM_KIND_LOW_SPAN = 33;
const CUSTOM_KIND_HIGH_FROM = 228;
const CUSTOM_KIND_HUE_SPAN = 160;

function customKindHue(requestKind: string): number {
  const offset = fnv1aHash(requestKind) % CUSTOM_KIND_HUE_SPAN;
  return offset < CUSTOM_KIND_LOW_SPAN
    ? CUSTOM_KIND_LOW_FROM + offset
    : CUSTOM_KIND_HIGH_FROM + offset - CUSTOM_KIND_LOW_SPAN;
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
