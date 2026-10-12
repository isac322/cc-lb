// UI-CP-01 / UI-CP-02 table: the shared fix lines are frozen copy, and each
// is one "do X → get Y" nudge.

import { describe, expect, test } from 'vitest';
import type { ApiKeyClass } from '../../../lib/api';
import {
  isAnthropicBaseUrl,
  shortWorkspaceId,
  usageFixLine,
} from './apiKeyCopy';

describe('apiKeyCopy', () => {
  test.each([
    [
      'workspace_bound',
      'Replace it with an All workspaces key → get reported usage.',
    ],
    ['org_scoped', "Raise the linked account's org role → get reported usage."],
    ['unknown', "Raise the linked account's org role → get reported usage."],
  ] as ReadonlyArray<[ApiKeyClass, string]>)(
    'usageFixLine(%s)',
    (keyClass, line) => {
      expect(usageFixLine(keyClass)).toBe(line);
      expect(line.split(' → ')).toHaveLength(2);
    },
  );

  // The same table as ORG-HOST-01 (`anthropic_api_key/host.rs`).
  test.each([
    ['', true],
    ['https://api.anthropic.com', true],
    ['https://api.anthropic.com/', true],
    ['https://api.anthropic.com:443/', true],
    ['https://API.ANTHROPIC.COM/', true],
    ['http://api.anthropic.com', false],
    ['https://api.anthropic.com:8443', false],
    ['https://api.anthropic.com.evil.com/', false],
    ['https://api.anthropic.com@evil.com/', false],
    ['https://evil.com/?h=api.anthropic.com', false],
    ['https://evil.com/api.anthropic.com/', false],
    ['https://api.anthropic.com./', false],
    ['https://xn--api-anthropic-xyz.com/', false],
    ['https://api.anthropic.com/v1/', false],
    ['https://user:pass@api.anthropic.com/', false],
    ['https://api.anthropic.com/?x=1', false],
    ['https://api.anthropic.com/#frag', false],
    ['https://api.anthropic.com/?', false],
    ['https://api.anthropic.com/#', false],
    ['https://proxy.example.com', false],
  ] as const)('isAnthropicBaseUrl(%j) is %s, like the server', (url, ok) => {
    expect(isAnthropicBaseUrl(url)).toBe(ok);
  });

  test('short workspace ids keep the last four characters', () => {
    expect(shortWorkspaceId('wrkspc_01ProductionWorkspace4f2a')).toBe(
      'wrkspc_…4f2a',
    );
    expect(shortWorkspaceId('wrkspc_short')).toBe('wrkspc_short');
  });
});
