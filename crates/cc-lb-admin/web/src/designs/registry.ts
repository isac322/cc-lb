import { useRouterState } from '@tanstack/react-router';
import { CircleDot } from 'lucide-react';
import { NAV_GROUPS } from '../components/layout/navItems';
import { usePreviewInstallation } from '../lib/previewControls';
import {
  type Installation,
  PROVIDER_LABELS,
  type ProviderFamily,
} from '../lib/providers';
import defaultDesign from './default';
import type { Design, NavGroup, NavItem } from './types';

export { defaultDesign };
export const designs: Record<'default', Design> = { default: defaultDesign };

/** Shared administration pages always use the incumbent design. */
export function useDesign(): Design {
  return defaultDesign;
}

function providerItems(providers: readonly ProviderFamily[]): NavItem[] {
  return providers.map((provider) => ({
    id: `provider:${provider}`,
    path: '/providers/$provider',
    params: { provider },
    provider,
    label: PROVIDER_LABELS[provider],
    Icon: CircleDot,
  }));
}

function providerNavigation(
  installation: Installation,
  surface: 'desktop' | 'mobile',
): readonly NavGroup[] {
  const base = NAV_GROUPS.map((group) => ({
    ...group,
    items: group.items.map((item) => ({
      ...item,
      mobile: ['/', '/upstreams', '/principals', '/logs'].includes(item.path),
      mobileOrder: ['/', '/upstreams', '/principals', '/logs'].indexOf(
        item.path,
      ),
    })),
  }));
  const monitor = base[0];
  const overview = monitor?.items.find((item) => item.path === '/');
  if (!monitor || !overview) return base;
  const providers = providerItems(installation.providers);

  // Provider destinations live under Overview on both desktop and mobile.
  if (surface === 'mobile') {
    return base.map((group) => ({
      ...group,
      items:
        group === monitor
          ? group.items.map((item) =>
              item === overview ? { ...overview, children: providers } : item,
            )
          : group.items,
    }));
  }

  return [
    {
      ...monitor,
      items: monitor.items.map((item) =>
        item === overview ? { ...overview, children: providers } : item,
      ),
    },
    ...base.slice(1),
  ];
}

type NavigationSurface = 'desktop' | 'mobile';

function matchesItem(item: NavItem, pathname: string, search: object): boolean {
  const target =
    item.path === '/providers/$provider'
      ? `/providers/${item.params?.provider ?? ''}`
      : item.path;
  const pathMatches =
    target === '/'
      ? pathname === '/'
      : pathname === target || pathname.startsWith(`${target}/`);
  if (!pathMatches) return false;
  return Object.entries(item.search ?? {}).every(
    ([key, value]) => (search as Record<string, unknown>)[key] === value,
  );
}

/** One navigation model drives desktop, phone tabs, page labels and titles. */
export function useDesignNavigation(surface: NavigationSurface = 'desktop') {
  const installation = usePreviewInstallation();
  const location = useRouterState({ select: (state) => state.location });
  const groups =
    installation.mode === 'multi'
      ? providerNavigation(installation, surface)
      : defaultDesign.navItems(installation);
  const items = groups.flatMap((group) => [
    ...group.items,
    ...group.items.flatMap((item) => item.children ?? []),
  ]);
  const matched = items.find((item) =>
    matchesItem(item, location.pathname, location.search),
  );
  const overview = items.find((item) => item.path === '/');
  const current =
    surface === 'mobile' &&
    installation.mode === 'multi' &&
    location.pathname.startsWith('/providers/') &&
    overview !== undefined
      ? overview
      : matched;
  return { groups, items, current };
}
