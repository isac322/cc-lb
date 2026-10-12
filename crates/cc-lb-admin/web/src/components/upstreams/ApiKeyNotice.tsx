// The detail pane's one owner for a blocking API-key state: rejected, never
// checked, or last check failed. It sits in the pane `notices` where an OAuth
// upstream shows its reconnect notice, with the same shape (inline Notice,
// one secondary action). Access shows only the short badge for the same
// state and hands its matching action to this notice.

import { KeyRound } from 'lucide-react';
import type { ApiKeyAccess } from '../../lib/api';
import { Button, Notice } from '../ui/primitives';

export type ApiKeyNoticeKind = 'rejected' | 'never_checked' | 'check_failed';

/** First match wins: a rejection outranks an unverified snapshot. */
export function apiKeyNoticeOf(
  access: ApiKeyAccess | null,
): ApiKeyNoticeKind | null {
  if (!access) return null;
  if (access.inference === 'denied') return 'rejected';
  if (!access.verified && access.checked_at_unix_secs === null)
    return 'never_checked';
  if (!access.verified) return 'check_failed';
  return null;
}

/**
 * A key whose capabilities are worth listing: verified and not rejected.
 * Access shows Workspace and Usage data only then, and the usage card's
 * "Get reported usage" pointer targets that row.
 */
export function apiKeyUsable(access: ApiKeyAccess | null): boolean {
  return access?.verified === true && access.inference !== 'denied';
}

export function ApiKeyNotice({
  kind,
  anthropicHost,
  lastError,
  onReplace,
  onRecheck,
  rechecking,
  recheckError,
}: {
  kind: ApiKeyNoticeKind;
  /** The upstream calls api.anthropic.com; otherwise "your endpoint". */
  anthropicHost: boolean;
  lastError: string | null;
  onReplace: () => void;
  onRecheck: () => void;
  rechecking: boolean;
  /** Why the last Re-check from this notice failed. */
  recheckError: string | null;
}) {
  const failure = recheckError ? (
    <p role="alert" className="mt-1 text-danger-text">
      Re-check failed: {recheckError}
    </p>
  ) : null;
  const recheckButton = (
    <Button size="sm" loading={rechecking} onClick={onRecheck}>
      {rechecking ? 'Checking…' : 'Re-check'}
    </Button>
  );
  const withLastError = (fix: string) =>
    lastError ? `${lastError} ${fix}` : fix;

  if (kind === 'rejected') {
    return (
      <Notice
        tone="danger"
        title={
          anthropicHost
            ? 'Anthropic rejected this key'
            : 'Your endpoint rejected this key'
        }
        action={
          <Button size="sm" iconLeft={<KeyRound />} onClick={onReplace}>
            Replace key
          </Button>
        }
      >
        <p className="break-words">
          {withLastError(
            'Replace the key → requests through this upstream succeed again.',
          )}
        </p>
        {failure}
      </Notice>
    );
  }
  if (kind === 'never_checked') {
    return (
      <Notice tone="warning" title="Key not checked yet" action={recheckButton}>
        <p>Re-check → see what this key can do at Anthropic.</p>
        {failure}
      </Notice>
    );
  }
  return (
    <Notice tone="warning" title="Last check failed" action={recheckButton}>
      <p className="break-words">
        {withLastError('Re-check → confirm the key works.')}
      </p>
      {failure}
    </Notice>
  );
}
