// Client half of the key gate (GATE-04 web side, D-INP01 order) and the
// hidden-character paste rejection (R2-8, UI-D-31).

import { describe, expect, test } from 'vitest';
import { KEY_CHARACTERS_REASON, parseApiKeyPaste } from './parseApiKeyPaste';

describe('parseApiKeyPaste', () => {
  test.each([
    ['sk-ant-admin01-abc', 'key_admin'],
    ['sk-ant-admin02-abc', 'key_admin'],
    ['SK-ANT-ADMIN01-ABC', 'key_admin'],
    ['sk-ant-api01-abc', 'key_enterprise_scoped'],
    ['sk-ant-oat01-abc', 'key_oat'],
    ['sk-ant-api03-abc', null],
    ['sk-ant-api02-abc', null],
  ] as const)('%s gates as %s', (input, gate) => {
    expect(parseApiKeyPaste(input)).toEqual({ kind: 'key', key: input, gate });
  });

  test('outer whitespace and shell lines fall away', () => {
    expect(parseApiKeyPaste('  sk-ant-api03-abc\n')).toEqual({
      kind: 'key',
      key: 'sk-ant-api03-abc',
      gate: null,
    });
    expect(
      parseApiKeyPaste('export ANTHROPIC_API_KEY=sk-ant-api03-abc_d-e'),
    ).toEqual({ kind: 'key', key: 'sk-ant-api03-abc_d-e', gate: null });
  });

  test.each([
    ['double-quoted key', '"sk-ant-api03-abc_d-e"'],
    ['single-quoted key', "'sk-ant-api03-abc_d-e'"],
    ['double-quoted export', 'export ANTHROPIC_API_KEY="sk-ant-api03-abc_d-e"'],
    ['single-quoted export', "export ANTHROPIC_API_KEY='sk-ant-api03-abc_d-e'"],
  ])('a %s falls away to the key', (_, input) => {
    expect(parseApiKeyPaste(input)).toEqual({
      kind: 'key',
      key: 'sk-ant-api03-abc_d-e',
      gate: null,
    });
  });

  test('a pasted Bearer header still reaches the OAuth gate', () => {
    expect(parseApiKeyPaste('Bearer sk-ant-oat01-xyz')).toEqual({
      kind: 'key',
      key: 'sk-ant-oat01-xyz',
      gate: 'key_oat',
    });
  });

  test.each([
    ['zero-width space', 'sk-ant-api03-ab\u200Bcd'],
    ['NBSP', 'sk-ant-api03-ab\u00A0cd'],
    ['non-ASCII letter', 'sk-ant-api03-abé'],
    ['trailing zero-width space', 'sk-ant-api03-abcd\u200B'],
    ['quoted zero-width space', '"sk-ant-api03-ab\u200Bcd"'],
    [
      'quoted export NBSP',
      "export ANTHROPIC_API_KEY='sk-ant-api03-ab\u00A0cd'",
    ],
    ['mismatched quotes', '"sk-ant-api03-abcd\''],
  ])(
    'a %s inside the key is rejected, never truncated (UI-D-31)',
    (_, input) => {
      expect(parseApiKeyPaste(input)).toEqual({
        kind: 'invalid',
        reason: KEY_CHARACTERS_REASON,
      });
    },
  );

  test('the field error copy is frozen', () => {
    expect(KEY_CHARACTERS_REASON).toBe(
      'Keys contain only letters, digits, - and _. Paste it again.',
    );
  });

  test('empty and multi-token pastes', () => {
    expect(parseApiKeyPaste('   ')).toEqual({ kind: 'empty' });
    expect(parseApiKeyPaste('two words')).toEqual({
      kind: 'invalid',
      reason: 'Paste one key, without spaces.',
    });
  });
});
