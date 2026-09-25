// Client mirror of the backend's `normalize_oauth_code` (crates/cc-lb-admin/
// src/v1/oauth.rs). Anthropic's hosted callback page hands the operator a
// `code#state` string; operators also paste the whole callback URL or just the
// code. Parsing locally lets the UI give instant feedback — including "this
// code belongs to a different/older sign-in" — before any request is sent.

export type ParsedOAuthPaste =
  | { kind: 'empty' }
  | { kind: 'invalid'; reason: string }
  | {
      kind: 'code';
      code: string;
      /** The `state` echoed back by Anthropic, when the paste carried one. */
      state: string | null;
      source: 'callback_url' | 'code_with_state' | 'bare_code';
    };

// Authorization codes are URL-safe tokens; anything with whitespace inside or
// shorter than this is a mis-copy (a word, a truncated selection).
const MIN_CODE_LENGTH = 16;
const CODE_PATTERN = /^[A-Za-z0-9._~-]+$/;

export function parseOAuthPaste(input: string): ParsedOAuthPaste {
  const trimmed = input.trim();
  if (trimmed === '') return { kind: 'empty' };

  if (/^https?:\/\//i.test(trimmed)) {
    let url: URL;
    try {
      url = new URL(trimmed);
    } catch {
      return { kind: 'invalid', reason: 'This looks like a broken link.' };
    }
    const code = url.searchParams.get('code');
    if (!code) {
      return {
        kind: 'invalid',
        reason:
          'This link has no authorization code. Copy the code shown after you click Authorize.',
      };
    }
    return finish(code, url.searchParams.get('state'), 'callback_url');
  }

  const hashAt = trimmed.indexOf('#');
  if (hashAt >= 0) {
    return finish(
      trimmed.slice(0, hashAt),
      trimmed.slice(hashAt + 1) || null,
      'code_with_state',
    );
  }
  return finish(trimmed, null, 'bare_code');
}

function finish(
  code: string,
  state: string | null,
  source: 'callback_url' | 'code_with_state' | 'bare_code',
): ParsedOAuthPaste {
  if (/\s/.test(code) || !CODE_PATTERN.test(code)) {
    return {
      kind: 'invalid',
      reason: 'That does not look like an authorization code.',
    };
  }
  if (code.length < MIN_CODE_LENGTH) {
    return {
      kind: 'invalid',
      reason: 'The code looks cut off. Copy the whole value.',
    };
  }
  return { kind: 'code', code, state, source };
}

/** The `state` query parameter of an authorize URL (equals the state token). */
export function stateOfAuthorizeUrl(authorizeUrl: string): string | null {
  try {
    return new URL(authorizeUrl).searchParams.get('state');
  } catch {
    return null;
  }
}

/** How a paste relates to the sign-in session the UI is waiting on. */
export type PasteMatch = 'match' | 'unknown' | 'other_session';

export function matchPasteToSession(
  parsed: ParsedOAuthPaste,
  sessionState: string | null,
): PasteMatch {
  if (parsed.kind !== 'code' || !parsed.state || !sessionState) {
    return 'unknown';
  }
  return parsed.state === sessionState ? 'match' : 'other_session';
}
