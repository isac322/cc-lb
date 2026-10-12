// Client mirror of the server's API-key prefix gate. Operators paste the bare
// key or a whole shell line (`export ANTHROPIC_API_KEY=sk-ant-…`); normalizing
// locally lets the Key step reject keys that can never call models before
// anything is sent. The server classifies again and is the authority: custom
// base URLs only get its verdict.

/** The §5 remediation codes the prefix alone decides. */
export type ApiKeyPrefixGate =
  | 'key_admin'
  | 'key_enterprise_scoped'
  | 'key_oat';

export type ParsedApiKey =
  | { kind: 'empty' }
  | { kind: 'invalid'; reason: string }
  | { kind: 'key'; key: string; gate: ApiKeyPrefixGate | null };

/** Field error for a key carrying a hidden or foreign character. */
export const KEY_CHARACTERS_REASON =
  'Keys contain only letters, digits, - and _. Paste it again.';

/** ASCII whitespace only: NBSP and zero-width spaces are not separators. */
const ASCII_SPACE = /[ \t\n\r\f\v]/;

export function parseApiKeyPaste(input: string): ParsedApiKey {
  let text = input.trim();
  if (text === '') return { kind: 'empty' };

  // A shell assignment (`export ANTHROPIC_API_KEY=…`) and one pair of
  // matching outer quotes fall away before the key itself is checked.
  text = text.replace(/^export\s+/, '').replace(/^[A-Za-z_][A-Za-z0-9_]*=/, '');
  const quoted = /^(['"])([\s\S]*)\1$/.exec(text);
  if (quoted) text = (quoted[2] ?? '').trim();

  // The first `sk-ant-…` token wins, so a `Bearer ` prefix falls away. The
  // token must end at ASCII whitespace or the end of the paste: a zero-width
  // space, an NBSP or any other foreign character right after it means the
  // key was cut short, and sending the prefix would earn a misleading 401.
  const embedded = /sk-ant-[A-Za-z0-9_-]*/.exec(text);
  if (embedded) {
    const next = text.charAt(embedded.index + embedded[0].length);
    if (next !== '' && !ASCII_SPACE.test(next)) {
      return { kind: 'invalid', reason: KEY_CHARACTERS_REASON };
    }
    text = embedded[0];
  } else {
    if (/\s/.test(text)) {
      return { kind: 'invalid', reason: 'Paste one key, without spaces.' };
    }
    if (!/^[\x21-\x7e]+$/.test(text)) {
      return { kind: 'invalid', reason: KEY_CHARACTERS_REASON };
    }
  }

  // Case-insensitive, like the server's gate.
  const lower = text.toLowerCase();
  const gate: ApiKeyPrefixGate | null = lower.startsWith('sk-ant-admin')
    ? 'key_admin'
    : lower.startsWith('sk-ant-api01')
      ? 'key_enterprise_scoped'
      : lower.startsWith('sk-ant-oat01')
        ? 'key_oat'
        : null;
  return { kind: 'key', key: text, gate };
}
