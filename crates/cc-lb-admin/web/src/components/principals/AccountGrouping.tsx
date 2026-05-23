import type { PrincipalLimitIdentity } from '../../lib/api';
import { Card } from '../primitives/Card';
import { StatusChip } from '../primitives/StatusChip';
import { Tooltip } from '../primitives/Tooltip';
import { LimitWindowSection } from './LimitWindowSection';

interface AccountGroupingProps {
  identity: PrincipalLimitIdentity;
}

export function AccountGrouping({ identity }: AccountGroupingProps) {
  const isUnobserved = identity.identity_kind === 'unobserved';

  let badgeColor: 'info' | 'warn' | 'neutral' = 'neutral';
  if (identity.identity_kind === 'account') badgeColor = 'info';
  else if (identity.identity_kind === 'credential') badgeColor = 'warn';

  // Sort windows: 5h first, weekly second, then others
  const sortedWindows = [...identity.windows].sort((a, b) => {
    if (a.window === '5h') return -1;
    if (b.window === '5h') return 1;
    if (a.window === 'weekly') return -1;
    if (b.window === 'weekly') return 1;
    return a.window.localeCompare(b.window);
  });

  return (
    <Card className="p-6 mb-6 last:mb-0 border border-graphite-800">
      <div className="flex items-center justify-between mb-6 pb-4 border-b border-graphite-800">
        <div className="flex items-center space-x-3">
          <StatusChip variant={badgeColor}>{identity.identity_kind}</StatusChip>
          {!isUnobserved && identity.identity_value && (
            <Tooltip content={identity.identity_value}>
              <span className="font-mono text-sm text-graphite-200 max-w-[200px] truncate">
                {identity.identity_value}
              </span>
            </Tooltip>
          )}
        </div>
        <div className="flex items-center">
          <StatusChip variant={identity.account_observed ? 'ok' : 'neutral'}>
            {identity.account_observed
              ? 'account_observed: true'
              : 'account_observed: false'}
          </StatusChip>
        </div>
      </div>

      {isUnobserved && !identity.account_observed && (
        <div className="mb-6 p-4 bg-amber-900/20 border border-amber-900/50 rounded-md text-sm text-amber-200/80">
          <p className="mb-2 font-medium text-amber-400">
            No account-scoped headers observed
          </p>
          <p>
            cc-lb is using the credential reference as a proxy identity because
            the upstream did not return account-scoped rate limit headers.
            Anthropic limit headers populate this view as traffic flows.
          </p>
        </div>
      )}

      <div className="space-y-6">
        {sortedWindows.map((window) => (
          <LimitWindowSection key={window.window} window={window} />
        ))}
      </div>
    </Card>
  );
}
