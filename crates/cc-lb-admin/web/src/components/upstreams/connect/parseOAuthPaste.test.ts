import { describe, expect, test } from 'vitest';
import {
  matchPasteToSession,
  parseOAuthPaste,
  stateOfAuthorizeUrl,
} from './parseOAuthPaste';

// A well-formed code: URL-safe characters and comfortably above the 16-char
// minimum so only the aspect under test varies.
const CODE = 'aB3xK9_qR2-mN7pQwZ';

describe('parseOAuthPaste', () => {
  test('empty and whitespace-only input is empty', () => {
    expect(parseOAuthPaste('')).toEqual({ kind: 'empty' });
    expect(parseOAuthPaste('   \n\t ')).toEqual({ kind: 'empty' });
  });

  test('a bare code parses with a bare_code source and no state', () => {
    expect(parseOAuthPaste(CODE)).toEqual({
      kind: 'code',
      code: CODE,
      state: null,
      source: 'bare_code',
    });
  });

  test('leading/trailing whitespace is trimmed before parsing', () => {
    expect(parseOAuthPaste(`  ${CODE}  `)).toEqual({
      kind: 'code',
      code: CODE,
      state: null,
      source: 'bare_code',
    });
  });

  test('code#state keeps the echoed state', () => {
    expect(parseOAuthPaste(`${CODE}#state-token-1`)).toEqual({
      kind: 'code',
      code: CODE,
      state: 'state-token-1',
      source: 'code_with_state',
    });
  });

  test('a trailing # means no state was echoed', () => {
    expect(parseOAuthPaste(`${CODE}#`)).toEqual({
      kind: 'code',
      code: CODE,
      state: null,
      source: 'code_with_state',
    });
  });

  test('a full callback URL extracts code and state', () => {
    expect(
      parseOAuthPaste(
        `https://console.anthropic.com/oauth/code/callback?code=${CODE}&state=state-abc`,
      ),
    ).toEqual({
      kind: 'code',
      code: CODE,
      state: 'state-abc',
      source: 'callback_url',
    });
  });

  test('a callback URL without a state still yields the code', () => {
    expect(
      parseOAuthPaste(`http://localhost:3000/callback?code=${CODE}`),
    ).toEqual({
      kind: 'code',
      code: CODE,
      state: null,
      source: 'callback_url',
    });
  });

  test('a URL without a code parameter is rejected with guidance', () => {
    const parsed = parseOAuthPaste(
      'https://console.anthropic.com/oauth/callback?state=abc',
    );
    expect(parsed.kind).toBe('invalid');
    if (parsed.kind === 'invalid') {
      expect(parsed.reason).toContain('no authorization code');
    }
  });

  test('a URL-shaped string that cannot be parsed is rejected', () => {
    const parsed = parseOAuthPaste('http://[bad');
    expect(parsed).toEqual({
      kind: 'invalid',
      reason: 'This looks like a broken link.',
    });
  });

  test('a truncated code below the minimum is rejected', () => {
    const parsed = parseOAuthPaste('aB3xK9_qR2-mN7p');
    expect(parsed.kind).toBe('invalid');
    if (parsed.kind === 'invalid') {
      expect(parsed.reason).toContain('cut off');
    }
    // The minimum boundary itself is accepted.
    expect(parseOAuthPaste('aB3xK9_qR2-mN7pQ').kind).toBe('code');
  });

  test('a code containing whitespace or unsupported characters is rejected', () => {
    for (const input of [`${CODE} ${CODE}`, `${CODE}?foo`, 'not a code']) {
      const parsed = parseOAuthPaste(input);
      expect(parsed.kind).toBe('invalid');
      if (parsed.kind === 'invalid') {
        expect(parsed.reason).toContain('authorization code');
      }
    }
  });
});

describe('matchPasteToSession', () => {
  test('matching state tokens yield a match', () => {
    const parsed = parseOAuthPaste(`${CODE}#session-state`);
    expect(matchPasteToSession(parsed, 'session-state')).toBe('match');
  });

  test('a foreign state token reports the other session', () => {
    const parsed = parseOAuthPaste(`${CODE}#other-state`);
    expect(matchPasteToSession(parsed, 'session-state')).toBe('other_session');
  });

  test('a bare code can never be verified and stays unknown', () => {
    const parsed = parseOAuthPaste(CODE);
    expect(matchPasteToSession(parsed, 'session-state')).toBe('unknown');
  });

  test('without a session state nothing can be compared', () => {
    const parsed = parseOAuthPaste(`${CODE}#session-state`);
    expect(matchPasteToSession(parsed, null)).toBe('unknown');
  });

  test('non-code pastes never match', () => {
    expect(matchPasteToSession(parseOAuthPaste(''), 'session-state')).toBe(
      'unknown',
    );
    expect(matchPasteToSession(parseOAuthPaste('short'), 'session-state')).toBe(
      'unknown',
    );
  });
});

describe('stateOfAuthorizeUrl', () => {
  test('reads the state query parameter of a well-formed URL', () => {
    expect(
      stateOfAuthorizeUrl('https://claude.ai/oauth/authorize?state=abc&x=1'),
    ).toBe('abc');
  });

  test('returns null when the URL is missing state or unparseable', () => {
    expect(
      stateOfAuthorizeUrl('https://claude.ai/oauth/authorize?foo=1'),
    ).toBeNull();
    expect(stateOfAuthorizeUrl('not a url')).toBeNull();
  });
});
