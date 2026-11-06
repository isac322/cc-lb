import { Link } from '@tanstack/react-router';
import { ArrowUpRight, MoreHorizontal, PanelLeft, Star } from 'lucide-react';
import { useId, useMemo } from 'react';
import { useDesignNavigation } from '../../designs/registry';
import type { NavGroup, NavItem } from '../../designs/types';
import {
  type OAuthReconnectNudge,
  useOAuthReconnectNudges,
} from '../../lib/oauthReconnect';
import { isOAuthUpstreamKind } from '../../lib/providers';
import { useUpstreams } from '../../lib/queries';
import { ProviderLogo } from '../ui/ProviderLogo';
import { cx, IconButton } from '../ui/primitives';

/**
 * Reconnect nudges for all OAuth upstreams: the one source for the
 * sidebar badge and the document-title count. Shares query keys with every
 * other OAuth status consumer, so extra callers add no requests.
 */
export function useUpstreamOAuthAttention(): {
  nudges: ReadonlyMap<string, OAuthReconnectNudge>;
  stale: boolean;
} {
  const upstreams = useUpstreams();
  const oauthUpstreams = useMemo(
    () =>
      (upstreams.data?.upstreams ?? []).filter((u) =>
        isOAuthUpstreamKind(u.kind),
      ),
    [upstreams.data],
  );
  const { nudges, isPending, isError } =
    useOAuthReconnectNudges(oauthUpstreams);
  const stale =
    upstreams.isPending || upstreams.isError || isPending || isError;
  return { nudges, stale };
}

/**
 * Count of OAuth upstreams whose reconnect nudge is active. An empty
 * nudge set means healthy or unknown (pending/failed queries with no cached
 * rows): render nothing rather than fabricate a zero. Known nudges keep
 * their badge through a refetch or failed poll, with a stale-status note for
 * assistive tech. Collapsed sidebars get a corner dot instead of a pill.
 */
function UpstreamOAuthAttentionBadge({ collapsed }: { collapsed: boolean }) {
  const { nudges, stale } = useUpstreamOAuthAttention();

  const count = nudges.size;
  if (count === 0) return null;

  const danger = Array.from(nudges.values()).some((n) => n.tone === 'danger');
  const a11y = `${count} OAuth account${count === 1 ? '' : 's'} need${count === 1 ? 's' : ''} attention${stale ? ' (status may be outdated)' : ''}`;
  if (collapsed) {
    return (
      <span
        aria-label={a11y}
        className={cx(
          'absolute right-1 top-1 h-1.5 w-1.5 rounded-full',
          danger ? 'bg-danger' : 'bg-warn',
        )}
        role="img"
      />
    );
  }
  // The tint is mixed into the ground rather than layered over it, so the
  // badge keeps its contrast on the selected nav fill.
  return (
    <span
      className={cx(
        'ml-auto inline-flex h-5 min-w-5 items-center justify-center rounded-full px-1.5 text-caption font-medium tabular-nums',
        danger
          ? 'bg-[color-mix(in_oklab,var(--color-danger)_15%,var(--color-bg))] text-danger-text'
          : 'bg-[color-mix(in_oklab,var(--color-warn)_15%,var(--color-bg))] text-warn-text',
      )}
    >
      <span aria-hidden="true">{count}</span>
      <span className="sr-only">{a11y}</span>
    </span>
  );
}

/**
 * The mark: the let-gate — two squared bracket "c" shapes facing each other
 * so they form a gate, with one accent lane passing through it. Master
 * geometry from `assets/brand/tools/brand.py` on a 64-unit grid; brackets in
 * `text` ink, the lane in `accent`. `size` in px.
 */
export function BrandMark({ size = 24 }: { size?: number }) {
  return (
    <svg
      aria-hidden="true"
      className="shrink-0"
      height={size}
      viewBox="0 0 64 64"
      width={size}
    >
      <path d="M11 14H27V20.5H18V43.5H27V50H11Z" fill="var(--color-text)" />
      <path d="M53 14H37V20.5H46V43.5H37V50H53Z" fill="var(--color-text)" />
      <rect fill="var(--color-accent)" height={50} width={5} x={29.5} y={7} />
    </svg>
  );
}

/**
 * Brand row, 48px so it lines up with the top bar. On the desktop rail it also
 * carries the collapse/expand toggle: right-aligned beside the wordmark when
 * expanded, directly under the mark when collapsed. The phone sheet omits it
 * (`onToggleCollapsed` absent).
 */
export function SidebarBrand({
  collapsed,
  onToggleCollapsed,
}: {
  collapsed: boolean;
  onToggleCollapsed?: () => void;
}) {
  const brand = (
    <div
      className={cx(
        'flex items-center gap-2.5 h-12 shrink-0',
        collapsed ? 'justify-center px-2' : 'px-5',
      )}
    >
      <BrandMark />
      {!collapsed ? (
        <span className="text-[0.9375rem] font-semibold leading-none">
          cc-lb
        </span>
      ) : (
        <span className="sr-only">cc-lb</span>
      )}
    </div>
  );
  if (!onToggleCollapsed) return brand;

  // One tree for both states so the button keeps keyboard focus when it
  // flips the rail.
  return (
    <div
      className={cx(
        'flex shrink-0',
        collapsed
          ? 'flex-col items-center'
          : 'h-12 items-center justify-between pr-3',
      )}
    >
      {brand}
      <IconButton
        label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        aria-expanded={!collapsed}
        title={collapsed ? 'Expand sidebar (⌘B)' : 'Collapse sidebar (⌘B)'}
        onClick={onToggleCollapsed}
      >
        <PanelLeft strokeWidth={1.75} aria-hidden="true" />
      </IconButton>
    </div>
  );
}

/**
 * Labels fade in when the sidebar expands. The sidebar width itself snaps:
 * animating width reflows the whole main column on every frame.
 */
const LABEL_FADE =
  'transition-opacity duration-150 ease-out starting:opacity-0 motion-reduce:transition-none';

/**
 * Nav item chrome. TanStack `Link` marks the current route with
 * `data-status="active"` and `aria-current="page"`: the neutral selection
 * fill and full ink. No edge rule. Below `lg` the only nav list is the
 * "More" sheet, so rows are 44px touch targets there.
 */
const NAV_ITEM =
  'relative flex items-center gap-2.5 h-9 max-lg:h-11 rounded-sm text-sm font-medium text-text-muted transition-colors hover:bg-overlay-2 hover:text-text ' +
  'data-[status=active]:bg-selected data-[status=active]:text-text data-[status=active]:hover:bg-selected ' +
  'data-[current=true]:bg-selected data-[current=true]:text-text data-[current=true]:hover:bg-selected ' +
  'focus-visible:outline-2 focus-visible:outline-accent focus-visible:-outline-offset-2';

function navItemKey(item: NavItem) {
  return (
    item.id ??
    `${item.path}:${item.params?.provider ?? ''}:${JSON.stringify(item.search)}`
  );
}

function SidebarNavItem({
  item,
  collapsed,
  current,
  onNavigate,
  nested = false,
}: {
  item: NavItem;
  collapsed: boolean;
  current?: NavItem;
  onNavigate: () => void;
  nested?: boolean;
}) {
  const { path, label, Icon, search, params, provider } = item;
  const isCurrent =
    current !== undefined && navItemKey(current) === navItemKey(item);
  return (
    <li>
      <Link
        to={path}
        params={params}
        search={search}
        onClick={onNavigate}
        activeOptions={{
          exact: path === '/',
          includeSearch: search !== undefined,
          explicitUndefined: true,
        }}
        aria-current={isCurrent ? 'page' : undefined}
        data-current={isCurrent ? 'true' : undefined}
        title={collapsed ? label : undefined}
        className={cx(
          NAV_ITEM,
          collapsed ? 'justify-center' : 'px-2',
          nested && !collapsed ? 'ml-5' : undefined,
        )}
      >
        {provider ? (
          <ProviderLogo
            provider={provider}
            size={16}
            className="size-4 shrink-0"
          />
        ) : (
          <Icon
            className="size-4 shrink-0"
            strokeWidth={1.75}
            aria-hidden="true"
          />
        )}
        {!collapsed ? (
          <span className={cx('truncate', LABEL_FADE)}>{label}</span>
        ) : (
          <span className="sr-only">{label}</span>
        )}
        {path === '/upstreams' ? (
          <UpstreamOAuthAttentionBadge collapsed={collapsed} />
        ) : null}
      </Link>
      {item.children?.length ? (
        <ul className="mt-0.5 space-y-0.5">
          {item.children.map((child) => (
            <SidebarNavItem
              key={navItemKey(child)}
              item={child}
              collapsed={collapsed}
              current={current}
              onNavigate={onNavigate}
              nested
            />
          ))}
        </ul>
      ) : null}
    </li>
  );
}

function SidebarNavGroup({
  group,
  collapsed,
  current,
  onNavigate,
}: {
  group: NavGroup;
  collapsed: boolean;
  current?: NavItem;
  onNavigate: () => void;
}) {
  const headingId = useId();
  return (
    <div>
      <div
        id={headingId}
        className={cx(
          collapsed
            ? 'sr-only'
            : cx('px-2 pb-2 text-overline text-text-faint', LABEL_FADE),
        )}
      >
        {group.label}
      </div>
      <ul aria-labelledby={headingId} className="space-y-0.5">
        {group.items.map((item) => (
          <SidebarNavItem
            key={navItemKey(item)}
            item={item}
            collapsed={collapsed}
            current={current}
            onNavigate={onNavigate}
          />
        ))}
      </ul>
    </div>
  );
}

export function SidebarNav({
  collapsed,
  onNavigate,
  mobile = false,
}: {
  collapsed: boolean;
  onNavigate: () => void;
  mobile?: boolean;
}) {
  const { groups, current } = useDesignNavigation(
    mobile ? 'mobile' : 'desktop',
  );
  return (
    <nav
      aria-label="Main"
      className={cx(
        'flex-1 pt-4 pb-8 px-3 overflow-y-auto',
        collapsed ? 'space-y-3' : 'space-y-6',
      )}
    >
      {groups.map((group, index) => (
        <div
          key={group.label}
          className={
            collapsed && index > 0 ? 'pt-3 border-t border-subtle' : undefined
          }
        >
          <SidebarNavGroup
            group={group}
            collapsed={collapsed}
            current={current}
            onNavigate={onNavigate}
          />
        </div>
      ))}
    </nav>
  );
}

const REPO_URL = 'https://github.com/isac322/cc-lb';
const STAR_A11Y = 'Star cc-lb on GitHub (opens in a new tab)';

/**
 * One quiet external link above the version line: muted 12px ink, no fill,
 * no count, no dismiss. It stays weaker than every nav item, and only the
 * star glyph picks up `accent-text` on hover or focus. 44px tall at every
 * width; collapsed, the star alone in a 44px square with the same
 * accessible name and a tooltip.
 */
function StarOnGitHubLink({ collapsed }: { collapsed: boolean }) {
  return (
    <a
      href={REPO_URL}
      target="_blank"
      rel="noopener noreferrer"
      aria-label={collapsed ? STAR_A11Y : undefined}
      title={collapsed ? 'Star on GitHub' : undefined}
      className={cx(
        'group inline-flex h-11 shrink-0 items-center rounded-sm text-caption font-medium text-text-muted transition-colors hover:bg-overlay-2 hover:text-text',
        'focus-visible:outline-2 focus-visible:outline-accent focus-visible:-outline-offset-2',
        collapsed ? 'w-11 justify-center' : '-mx-2 gap-2 self-stretch px-2',
      )}
    >
      <Star
        className="size-3.5 shrink-0 text-text-faint transition-colors group-hover:text-accent-text group-focus-visible:text-accent-text"
        strokeWidth={1.75}
        aria-hidden="true"
      />
      {!collapsed ? (
        <>
          <span className={cx('min-w-0 truncate', LABEL_FADE)}>
            Star on GitHub
          </span>
          <span className="sr-only">(opens in a new tab)</span>
          <ArrowUpRight
            className="ml-auto size-3 shrink-0 text-text-faint"
            strokeWidth={1.75}
            aria-hidden="true"
          />
        </>
      ) : null}
    </a>
  );
}

/**
 * The GitHub star link and the version at the foot of the rail and the
 * phone sheet. Collapsed, the inset narrows to 6px so the 44px link fits
 * the 56px rail.
 */
export function SidebarFooter({
  collapsed,
  version,
}: {
  collapsed: boolean;
  version: string | null;
}) {
  const label = version ? `cc-lb v${version}` : 'cc-lb';
  return (
    <div
      className={cx(
        'pt-2 pb-4 flex min-w-0 flex-col gap-2',
        collapsed ? 'items-center px-1.5 text-center' : 'items-start px-5',
      )}
    >
      <StarOnGitHubLink collapsed={collapsed} />
      <div
        className="text-2xs max-md:text-caption text-text-faint truncate"
        title={collapsed ? label : undefined}
      >
        {collapsed ? 'cc' : label}
      </div>
    </div>
  );
}

const TAB_ITEM =
  'relative flex flex-col items-center justify-center gap-1 min-w-0 text-caption font-medium text-text-muted transition-colors hover:text-text ' +
  'focus-visible:outline-2 focus-visible:outline-accent focus-visible:-outline-offset-2';

/**
 * Bottom tab bar below `lg`: four primary pages plus "More", which opens
 * the full navigation sheet. The active tab gets full ink on the neutral
 * selection fill. Page titles stay in the top bar only.
 */
export function SidebarTabBar({
  onMore,
  moreOpen,
}: {
  onMore: () => void;
  moreOpen: boolean;
}) {
  const { items, current } = useDesignNavigation('mobile');
  const tabs = items
    .filter((item) => item.mobile)
    .sort(
      (a, b) =>
        (a.mobileOrder ?? items.indexOf(a)) -
        (b.mobileOrder ?? items.indexOf(b)),
    )
    .slice(0, 4);
  const moreActive = current !== undefined && !tabs.includes(current);
  return (
    <nav
      aria-label="Main"
      className="fixed inset-x-0 bottom-0 z-30 grid grid-cols-5 border-t border-subtle bg-bg pb-[env(safe-area-inset-bottom)] lg:hidden"
    >
      {tabs.map((item) => {
        const { path, label, Icon, search, id } = item;
        const isCurrent =
          current !== undefined && navItemKey(current) === navItemKey(item);
        return (
          <Link
            key={id ?? `${path}:${JSON.stringify(search)}`}
            to={path}
            search={search}
            activeOptions={{
              exact: path === '/',
              includeSearch: search !== undefined,
              explicitUndefined: true,
            }}
            aria-current={isCurrent && !moreOpen ? 'page' : undefined}
            data-current={isCurrent ? 'true' : undefined}
            className={cx(
              TAB_ITEM,
              'h-14 data-[status=active]:bg-selected data-[status=active]:text-text data-[current=true]:bg-selected data-[current=true]:text-text',
            )}
          >
            <Icon className="size-4" strokeWidth={1.75} aria-hidden="true" />
            <span className="max-w-full truncate">{label}</span>
            {path === '/upstreams' ? (
              <UpstreamOAuthAttentionBadge collapsed />
            ) : null}
          </Link>
        );
      })}
      <button
        type="button"
        aria-haspopup="dialog"
        aria-expanded={moreOpen}
        aria-current={moreActive && !moreOpen ? 'page' : undefined}
        onClick={onMore}
        className={cx(TAB_ITEM, 'h-14', moreActive && 'bg-selected text-text')}
      >
        <MoreHorizontal
          className="size-4"
          strokeWidth={1.75}
          aria-hidden="true"
        />
        <span>More</span>
      </button>
    </nav>
  );
}
