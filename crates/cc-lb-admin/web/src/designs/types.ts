import type { LucideIcon } from 'lucide-react';
import type { FC } from 'react';
import type { Installation, ProviderFamily } from '../lib/providers';

export type { Installation, ProviderFamily } from '../lib/providers';

export interface NavItem {
  path:
    | '/'
    | '/upstreams'
    | '/principals'
    | '/logs'
    | '/plugins'
    | '/settings'
    | '/audit'
    | '/providers/$provider';
  label: string;
  Icon: LucideIcon;
  search?: { provider?: 'all' | ProviderFamily };
  params?: { provider: ProviderFamily };
  /** Provider identity for logo-bearing navigation items. */
  provider?: ProviderFamily;
  /** Child provider destinations used by sidebar variant A. */
  children?: readonly NavItem[];
  /** True puts this destination in the mobile bar; others remain in More. */
  mobile?: boolean;
  /** Optional phone-specific order; otherwise use navigation-tree order. */
  mobileOrder?: number;
  /** Stable key when multiple destinations share a pathname. */
  id?: string;
}

export interface NavGroup {
  label: string;
  items: readonly NavItem[];
}

/** Shared design hooks used by administration pages and the shell. */
export interface Design {
  id: 'default';
  label: string;
  navItems: (installation: Installation) => readonly NavGroup[];
  OverviewPage: FC;
  UpstreamsListAddon?: { groupBy: 'provider' | 'none'; Header?: FC };
  LogsScopeBar?: FC;
  PrincipalsActivity?: FC<{ principalId: string }>;
  SystemWideNotice?: FC;
}
