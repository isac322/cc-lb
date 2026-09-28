import { describe, expect, it } from 'vitest';
import { requestKindBadgeText, requestKindTone } from './requestKind';

describe('requestKindBadgeText', () => {
  it.each([
    ['main', 'main'],
    ['advisor', 'adv'],
    ['subagent', 'sub'],
    ['recap', 'recap'],
    ['compaction', 'comp'],
    ['notification', 'notif'],
    ['session_title', 'title'],
    ['auto_thinking', 'think'],
    ['side', 'side'],
    ['look_at', 'vision'],
    ['unknown', 'unk'],
  ])('renders %s as %s', (requestKind, expected) => {
    expect(requestKindBadgeText(requestKind)).toBe(expected);
  });

  it('preserves an unrecognized non-empty kind', () => {
    expect(requestKindBadgeText('future_kind')).toBe('future_kind');
  });

  it('hides missing or blank kinds', () => {
    expect(requestKindBadgeText(null)).toBeNull();
    expect(requestKindBadgeText('   ')).toBeNull();
  });
});

describe('requestKindTone', () => {
  const hued = [
    'advisor',
    'subagent',
    'recap',
    'compaction',
    'notification',
    'session_title',
    'auto_thinking',
    'side',
    'look_at',
  ];
  const hueOf = (kind: string) => {
    const tone = requestKindTone(kind);
    if (tone.kind !== 'hue') throw new Error(`${kind} has no hue`);
    return tone.hue;
  };
  const circularGap = (a: number, b: number) =>
    Math.min(Math.abs(a - b), 360 - Math.abs(a - b));

  it('keeps main and unknown neutral', () => {
    expect(requestKindTone('main').kind).toBe('main');
    expect(requestKindTone('unknown').kind).toBe('unknown');
  });

  it('gives every other known kind its own hue, clear of warn, danger and accent', () => {
    const hues = hued.map(hueOf);
    for (let i = 0; i < hues.length; i++) {
      for (let j = i + 1; j < hues.length; j++) {
        expect(circularGap(hues[i]!, hues[j]!)).toBeGreaterThanOrEqual(21);
      }
      // danger ~20–28, warn ~73–83: at least 20° away. Accent teal ~185–192:
      // at least 40° away (closer hues read teal next to accent elements).
      for (const status of [24, 78]) {
        expect(circularGap(hues[i]!, status)).toBeGreaterThanOrEqual(20);
      }
      expect(circularGap(hues[i]!, 188)).toBeGreaterThanOrEqual(40);
    }
  });

  it('hashes custom kinds to hues clear of warn, danger and accent', () => {
    for (const kind of ['future_kind', 'review', 'planner', 'x', 'tool_use']) {
      for (const status of [24, 78]) {
        expect(circularGap(hueOf(kind), status)).toBeGreaterThanOrEqual(20);
      }
      expect(circularGap(hueOf(kind), 188)).toBeGreaterThanOrEqual(40);
    }
  });
});
