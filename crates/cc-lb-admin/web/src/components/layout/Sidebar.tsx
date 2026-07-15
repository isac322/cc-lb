import { Link } from '@tanstack/react-router';
import {
  Activity,
  Blocks,
  FileClock,
  KeyRound,
  LayoutDashboard,
  ScrollText,
  Server,
  Settings as SettingsIcon,
  Users,
} from 'lucide-react';
import { cx } from '../ui/primitives';

const NAV = [
  { path: '/', label: 'Overview', Icon: LayoutDashboard },
  { path: '/upstreams', label: 'Upstreams', Icon: Server },
  { path: '/principals', label: 'Principals', Icon: Users },
  { path: '/plugins', label: 'Plugins', Icon: Blocks },
  { path: '/logs', label: 'Logs', Icon: ScrollText },
  { path: '/audit', label: 'Audit', Icon: FileClock },
  { path: '/credentials', label: 'Credentials', Icon: KeyRound },
  { path: '/status', label: 'Status', Icon: Activity },
  { path: '/settings', label: 'Settings', Icon: SettingsIcon },
] as const;

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
            'flex items-center gap-2.5 px-2.5 h-9 rounded-sm text-sm transition-colors hover:bg-overlay-5 text-text-muted hover:text-text',
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
