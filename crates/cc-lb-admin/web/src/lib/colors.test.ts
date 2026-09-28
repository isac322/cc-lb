import { beforeEach, describe, expect, it, vi } from 'vitest';
import type * as Colors from './colors';

/** OKLCH hue of a chip text color (`light-dark(oklch(L C H), …)`). */
function hueOf(color: string): number {
  const match = color.match(/oklch\([\d.]+ [\d.]+ (\d+)\)/);
  if (!match) throw new Error(`no hue in ${color}`);
  return Number(match[1]);
}

describe('getSessionColor', () => {
  let colors: typeof Colors;

  // Slot assignment is module state, so each test re-imports a fresh module;
  // a static import would share one registry across tests.
  beforeEach(async () => {
    vi.resetModules();
    colors = await import('./colors');
  });

  it('gives the first nine sessions visibly different hues', () => {
    const hues = Array.from({ length: 9 }, (_, i) =>
      hueOf(colors.getSessionColor(`session-${i}`).text),
    );
    expect(new Set(hues).size).toBe(9);
    for (const [i, a] of hues.entries()) {
      for (const b of hues.slice(i + 1)) {
        const gap = Math.min(Math.abs(a - b), 360 - Math.abs(a - b));
        expect(gap).toBeGreaterThanOrEqual(21);
      }
    }
  });

  it('keeps a session on the same color however many sessions follow', () => {
    const first = colors.getSessionColor('session-a');
    for (let i = 0; i < 30; i++) colors.getSessionColor(`other-${i}`);
    expect(colors.getSessionColor('session-a')).toEqual(first);
  });

  it('stays clear of the warm severity band and of warn/danger', () => {
    const gap = (a: number, b: number) =>
      Math.min(Math.abs(a - b), 360 - Math.abs(a - b));
    for (let i = 0; i < 40; i++) {
      const hue = hueOf(colors.getSessionColor(`s-${i}`).text);
      expect(hue > 100 || hue < 5).toBe(true);
      expect(gap(hue, 24)).toBeGreaterThanOrEqual(20);
      expect(gap(hue, 78)).toBeGreaterThanOrEqual(20);
    }
  });
});
