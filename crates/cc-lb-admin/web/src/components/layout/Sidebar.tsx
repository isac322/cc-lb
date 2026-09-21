import { Link } from '@tanstack/react-router';
import {
  Blocks,
  FileClock,
  LayoutDashboard,
  ScrollText,
  Server,
  Settings as SettingsIcon,
  Users,
} from 'lucide-react';
import { useMemo } from 'react';
import { useOAuthReconnectNudges } from '../../lib/oauthReconnect';
import { useUpstreams } from '../../lib/queries';
import { Badge, cx } from '../ui/primitives';

const NAV = [
  { path: '/', label: 'Overview', Icon: LayoutDashboard },
  {
    path: '/upstreams',
    label: 'Upstreams',
    Icon: Server,
  },
  { path: '/principals', label: 'Principals', Icon: Users },
  { path: '/plugins', label: 'Plugins', Icon: Blocks },
  { path: '/logs', label: 'Logs', Icon: ScrollText },
  { path: '/audit', label: 'Audit', Icon: FileClock },
  { path: '/settings', label: 'Settings', Icon: SettingsIcon },
] as const;

/**
 * Count of enabled OAuth upstreams whose reconnect nudge is active. An empty
 * nudge set means healthy or unknown (pending/failed queries with no cached
 * rows): render nothing rather than fabricate a zero. Known nudges keep
 * their badge through a refetch or failed poll, with a stale-status note for
 * assistive tech. Collapsed sidebars get a corner dot instead of a pill.
 */
function UpstreamOAuthAttentionBadge({ collapsed }: { collapsed: boolean }) {
  const upstreams = useUpstreams();
  const oauthUpstreams = useMemo(
    () =>
      (upstreams.data?.upstreams ?? []).filter(
        (u) => u.enabled && u.kind === 'anthropic_oauth',
      ),
    [upstreams.data],
  );
  const { nudges, isPending, isError } =
    useOAuthReconnectNudges(oauthUpstreams);

  const count = nudges.size;
  if (count === 0) return null;

  const danger = Array.from(nudges.values()).some((n) => n.tone === 'danger');
  const stale =
    upstreams.isPending || upstreams.isError || isPending || isError;
  const a11y = `${count} OAuth account${count === 1 ? '' : 's'} need${count === 1 ? 's' : ''} attention${stale ? ' (status may be outdated)' : ''}`;

  if (collapsed) {
    return (
      <span
        aria-label={a11y}
        className={cx(
          'absolute right-1 top-1 h-1.5 w-1.5 rounded-full',
          danger
            ? 'bg-[color:var(--color-danger)]'
            : 'bg-[color:var(--color-warn)]',
        )}
        role="img"
      />
    );
  }
  return (
    <Badge className="ml-auto" tone={danger ? 'danger' : 'warn'}>
      <span aria-hidden="true">{count}</span>
      <span className="sr-only">{a11y}</span>
    </Badge>
  );
}

export function SidebarBrand({ collapsed }: { collapsed: boolean }) {
  return (
    <div
      className={cx(
        'flex items-center gap-2.5 px-3 h-12 border-b border-subtle shrink-0',
        collapsed ? 'justify-center' : '',
      )}
    >
      <div className="w-6 h-6 bg-accent text-[color:var(--color-bg)] flex items-center justify-center font-bold text-[11px] rounded-sm">
        CC
      </div>
      {!collapsed ? (
        <span className="font-medium text-sm tracking-wide">cc-lb</span>
      ) : null}
    </div>
  );
}

export function SidebarNav({
  collapsed,
  onNavigate,
}: {
  collapsed: boolean;
  onNavigate: () => void;
}) {
  return (
    <nav className="flex-1 pt-3 pb-8 px-2 space-y-0.5 overflow-y-auto">
      {NAV.map(({ path, label, Icon }) => (
        <Link
          key={path}
          to={path}
          onClick={onNavigate}
          activeOptions={{ exact: path === '/' }}
          className={cx(
            'relative flex items-center gap-2.5 px-2.5 h-9 rounded-sm text-sm transition-colors hover:bg-overlay-5 text-text-muted hover:text-text',
            collapsed ? 'justify-center' : '',
          )}
          activeProps={{ className: 'bg-overlay-6 text-text' }}
        >
          <Icon className="w-4 h-4 shrink-0" aria-hidden="true" />
          {!collapsed ? (
            <span className="truncate">{label}</span>
          ) : (
            <span className="sr-only">{label}</span>
          )}
          {path === '/upstreams' ? (
            <UpstreamOAuthAttentionBadge collapsed={collapsed} />
          ) : null}
        </Link>
      ))}
    </nav>
  );
}

export function SidebarFooter({
  collapsed,
  version,
}: {
  collapsed: boolean;
  version: string | null;
}) {
  const label = version ? `cc-lb · v${version}` : 'cc-lb';

  return (
    <div
      className={cx(
        'border-t border-subtle px-3 py-3',
        collapsed ? 'text-center' : '',
      )}
    >
      <div className="text-[10px] uppercase tracking-wider text-text-faint">
        {collapsed ? 'cc' : label}
      </div>
    </div>
  );
}
