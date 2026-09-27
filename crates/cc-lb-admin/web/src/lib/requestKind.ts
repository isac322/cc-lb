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
 * Fixed OKLCH hue per known request kind. The hues stay clear of warn (amber,
 * ~75–85), danger (red, ~20–30) and the accent violet (~280–290), so a kind
 * badge never reads as a severity or a brand mark, and neighbours are at
 * least 22° apart. `main` (the bulk of traffic) and `unknown` have no hue:
 * they render as the quiet neutral badge and an outlined neutral badge.
 */
const REQUEST_KIND_HUE: Readonly<Record<string, number>> = {
  side: 115,
  session_title: 140,
  recap: 165,
  compaction: 190,
  notification: 215,
  subagent: 240,
  look_at: 262,
  auto_thinking: 318,
  advisor: 342,
};

/** Hue range a custom kind hashes into: the same status- and accent-free arc. */
const CUSTOM_KIND_HUE_FROM = 110;
const CUSTOM_KIND_HUE_SPAN = 155;

export type RequestKindTone =
  | { kind: 'main' }
  | { kind: 'unknown' }
  | { kind: 'hue'; hue: number };

export function requestKindTone(requestKind: string): RequestKindTone {
  if (requestKind === 'main') return { kind: 'main' };
  if (requestKind === 'unknown') return { kind: 'unknown' };
  return {
    kind: 'hue',
    hue:
      REQUEST_KIND_HUE[requestKind] ??
      CUSTOM_KIND_HUE_FROM + (fnv1aHash(requestKind) % CUSTOM_KIND_HUE_SPAN),
  };
}
