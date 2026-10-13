// UI-UU-01 / UI-UU-02: the admin web is often served over plain http on a LAN,
// where `crypto.randomUUID` is undefined. Every client UUID goes through
// `newUuid`, whose fallback still yields RFC 4122 v4 ids.

import { readdirSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import { afterEach, describe, expect, test, vi } from 'vitest';
import { newUuid } from './limitResets';

const V4 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

const SRC_DIR = path.resolve(import.meta.dirname, '..');
/** The one sanctioned call site: `newUuid` itself. */
const ALLOWED = path.join(SRC_DIR, 'lib', 'limitResets.ts');
/** This file names the API in its own assertions. */
const SELF = path.join(SRC_DIR, 'lib', 'noRandomUuid.test.ts');

function sourceFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((entry) => {
    const full = path.join(dir, entry);
    if (statSync(full).isDirectory()) return sourceFiles(full);
    return /\.(ts|tsx)$/.test(entry) ? [full] : [];
  });
}

describe('newUuid on an insecure origin (UI-UU-01)', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  test('falls back to getRandomValues and stays RFC 4122 v4 and unique', () => {
    const real = globalThis.crypto;
    vi.stubGlobal('crypto', {
      getRandomValues: real.getRandomValues.bind(real),
    });
    expect(globalThis.crypto.randomUUID).toBeUndefined();

    const ids = Array.from({ length: 1000 }, () => newUuid());
    for (const id of ids) expect(id).toMatch(V4);
    expect(new Set(ids).size).toBe(1000);
  });
});

describe('no direct randomUUID (UI-UU-02)', () => {
  test('only lib/limitResets.ts calls crypto.randomUUID', () => {
    const offenders = sourceFiles(SRC_DIR)
      .filter((file) => file !== ALLOWED && file !== SELF)
      .filter((file) => readFileSync(file, 'utf8').includes('randomUUID'))
      .map((file) => path.relative(SRC_DIR, file));
    expect(offenders).toEqual([]);
  });
});
