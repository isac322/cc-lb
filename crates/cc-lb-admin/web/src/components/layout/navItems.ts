import {
  Blocks,
  FileClock,
  LayoutDashboard,
  type LucideIcon,
  ScrollText,
  Server,
  Settings,
  Users,
} from 'lucide-react';

export interface NavItem {
  path:
    | '/'
    | '/logs'
    | '/audit'
    | '/upstreams'
    | '/principals'
    | '/plugins'
    | '/settings';
  label: string;
  Icon: LucideIcon;
}

export interface NavGroup {
  label: string;
  items: readonly NavItem[];
}

/** Every top-level page, grouped the way the sidebar shows them. */
export const NAV_GROUPS: readonly NavGroup[] = [
  {
    label: 'Monitor',
    items: [
      { path: '/', label: 'Overview', Icon: LayoutDashboard },
      { path: '/logs', label: 'Logs', Icon: ScrollText },
      { path: '/audit', label: 'Audit', Icon: FileClock },
    ],
  },
  {
    label: 'Configure',
    items: [
      { path: '/upstreams', label: 'Upstreams', Icon: Server },
      { path: '/principals', label: 'Principals', Icon: Users },
      { path: '/plugins', label: 'Plugins', Icon: Blocks },
      { path: '/settings', label: 'Settings', Icon: Settings },
    ],
  },
];

export const NAV_ITEMS: readonly NavItem[] = NAV_GROUPS.flatMap((g) => g.items);

/** The nav item whose page contains `pathname`, or null for unknown routes. */
export function navItemForPath(pathname: string): NavItem | null {
  return (
    NAV_ITEMS.find((item) =>
      item.path === '/'
        ? pathname === '/'
        : pathname === item.path || pathname.startsWith(`${item.path}/`),
    ) ?? null
  );
}
