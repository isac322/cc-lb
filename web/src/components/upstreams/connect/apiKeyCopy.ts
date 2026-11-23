// Wording shared by every API-key surface (connect dialog, the pane's Access
// section, usage card) so a capability limit reads, and is fixed, the same
// way everywhere. The fix must match the key type: a workspace-bound key can
// never read usage reports, so "raise the role" would be a false promise.

import type { ApiKeyClass, ApiKeyType } from '../../../lib/api';
import { DEFAULT_ANTHROPIC_BASE_URL } from '../../../lib/constants';

/** Console's API keys page. There is no create-key URL with prefilled fields. */
export const CONSOLE_KEYS_URL = 'https://platform.claude.com/settings/keys';

/** Console's workspace list; each workspace's URL carries its `wrkspc_…` ID. */
export const CONSOLE_WORKSPACES_URL =
  'https://platform.claude.com/settings/workspaces';

/** Console's Billing page: the only place the credit balance is shown. */
export const CONSOLE_BILLING_URL =
  'https://platform.claude.com/settings/billing';

export const KEY_TYPE_LABEL: Record<ApiKeyType, string> = {
  personal: 'Personal',
  service_account: 'Service account',
  workspace: 'Workspace',
};

/**
 * Fix for a key that cannot read Anthropic's usage reports, split so Access
 * can make the action a link when cc-lb itself can do it (`inApp`: replacing
 * the key); raising an org role happens in Console.
 */
export function usageFix(keyClass: ApiKeyClass): {
  action: string;
  result: string;
  inApp: boolean;
} {
  return keyClass === 'workspace_bound'
    ? {
        action: 'Replace it with an All workspaces key',
        result: 'get reported usage.',
        inApp: true,
      }
    : {
        action: "Raise the linked account's org role",
        result: 'get reported usage.',
        inApp: false,
      };
}

/** The same fix as one sentence, for the stopped banner and the dialog. */
export function usageFixLine(keyClass: ApiKeyClass): string {
  const { action, result } = usageFix(keyClass);
  return `${action} → ${result}`;
}

/** `wrkspc_…4f2a`: enough to recognize an id without a full-width string. */
export function shortWorkspaceId(id: string): string {
  return id.length > 12 ? `wrkspc_…${id.slice(-4)}` : id;
}

/**
 * Whether a base URL is Anthropic's default endpoint — the client mirror of
 * `org_api_applies` (`anthropic_api_key/host.rs`): `https:`, Anthropic's
 * host on its default port, a root path, and no userinfo, query, or
 * fragment. Anything else (`/v1/`, `?x=1`, `:8443`) is a custom endpoint the
 * caller must keep and send, so the server validates it. Inference probes
 * run at the configured URL either way — the key travels there on every
 * proxied request — but the org Admin API probes (workspace list, usage
 * report, api_keys) only run here; elsewhere they answer `not_applicable`.
 */
export function isAnthropicBaseUrl(baseUrl: string): boolean {
  const trimmed = baseUrl.trim();
  if (trimmed === '') return true;
  // Scheme, host and port must still match; the rest must be the bare root.
  // WHATWG drops an empty `?`/`#` from `search`/`hash`, so check the raw text.
  if (trimmed.includes('?') || trimmed.includes('#')) return false;
  try {
    const url = new URL(trimmed);
    const anthropic = new URL(DEFAULT_ANTHROPIC_BASE_URL);
    return (
      url.protocol === anthropic.protocol &&
      url.username === '' &&
      url.password === '' &&
      url.hostname === anthropic.hostname &&
      // `port` is '' for the scheme's default (an explicit :443 included).
      url.port === anthropic.port &&
      url.pathname === '/'
    );
  } catch {
    return false;
  }
}
