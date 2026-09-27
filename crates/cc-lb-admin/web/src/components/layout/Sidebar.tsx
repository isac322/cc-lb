import { Link } from '@tanstack/react-router';
import { BookOpen, MoreHorizontal } from 'lucide-react';
import { useId, useMemo } from 'react';
import {
  type OAuthReconnectNudge,
  useOAuthReconnectNudges,
} from '../../lib/oauthReconnect';
import { useUpstreams } from '../../lib/queries';
import { cx } from '../ui/primitives';
import {
  NAV_GROUPS,
  NAV_ITEMS,
  type NavGroup,
  navItemForPath,
} from './navItems';

const DOCS_URL = 'https://github.com/isac322/cc-lb#readme';

/**
 * Reconnect nudges for enabled OAuth upstreams: the one source for the
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
      (upstreams.data?.upstreams ?? []).filter(
        (u) => u.enabled && u.kind === 'anthropic_oauth',
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
 * Count of enabled OAuth upstreams whose reconnect nudge is active. An empty
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
 * The mark: an open 240° dial with a violet sweep. `size` in px.
 */
export function BrandMark({ size = 24 }: { size?: number }) {
  return (
    <svg
      aria-hidden="true"
      className="shrink-0"
      height={size}
      viewBox="0 0 24 24"
      width={size}
    >
      <path
        d="M4.21 16.5A9 9 0 1 1 19.79 16.5"
        fill="none"
        stroke="var(--color-border-strong)"
        strokeWidth={1}
        vectorEffect="non-scaling-stroke"
      />
      <path
        d="M4.21 16.5A9 9 0 0 1 16.34 4.11"
        fill="none"
        stroke="var(--color-accent)"
        strokeWidth={3}
      />
    </svg>
  );
}

export function SidebarBrand({ collapsed }: { collapsed: boolean }) {
  return (
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
 * fill and full ink. No edge rule.
 */
const NAV_ITEM =
  'relative flex items-center gap-2.5 h-9 rounded-sm text-sm font-medium text-text-muted transition-colors hover:bg-overlay-2 hover:text-text ' +
  'data-[status=active]:bg-selected data-[status=active]:text-text data-[status=active]:hover:bg-selected ' +
  'focus-visible:outline-2 focus-visible:outline-accent focus-visible:-outline-offset-2';

function SidebarNavGroup({
  group,
  collapsed,
  onNavigate,
}: {
  group: NavGroup;
  collapsed: boolean;
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
        {group.items.map(({ path, label, Icon }) => (
          <li key={path}>
            <Link
              to={path}
              onClick={onNavigate}
              activeOptions={{ exact: path === '/' }}
              title={collapsed ? label : undefined}
              className={cx(NAV_ITEM, collapsed ? 'justify-center' : 'px-2')}
            >
              <Icon
                className="w-4 h-4 shrink-0"
                strokeWidth={1.75}
                aria-hidden="true"
              />
              {!collapsed ? (
                <span className={cx('truncate', LABEL_FADE)}>{label}</span>
              ) : (
                <span className="sr-only">{label}</span>
              )}
              {path === '/upstreams' ? (
                <UpstreamOAuthAttentionBadge collapsed={collapsed} />
              ) : null}
            </Link>
          </li>
        ))}
      </ul>
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
    <nav
      aria-label="Main"
      className={cx(
        'flex-1 pt-4 pb-8 px-3 overflow-y-auto',
        collapsed ? 'space-y-3' : 'space-y-6',
      )}
    >
      {NAV_GROUPS.map((group, index) => (
        <div
          key={group.label}
          className={
            collapsed && index > 0 ? 'pt-3 border-t border-subtle' : undefined
          }
        >
          <SidebarNavGroup
            group={group}
            collapsed={collapsed}
            onNavigate={onNavigate}
          />
        </div>
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
  const label = version ? `cc-lb v${version}` : 'cc-lb';

  return (
    <div
      className={cx(
        'px-5 py-4 flex flex-col gap-2',
        collapsed ? 'items-center px-2 text-center' : '',
      )}
    >
      <a
        href={DOCS_URL}
        target="_blank"
        rel="noopener noreferrer"
        aria-label={collapsed ? 'Docs (opens in a new tab)' : undefined}
        title={collapsed ? 'Docs' : undefined}
        className="inline-flex items-center gap-1.5 rounded-sm text-caption text-text-muted transition-colors hover:text-text focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
      >
        <BookOpen
          size={14}
          strokeWidth={1.75}
          className="shrink-0"
          aria-hidden="true"
        />
        {collapsed ? null : (
          <>
            Docs<span className="sr-only"> (opens in a new tab)</span>
          </>
        )}
      </a>
      <div
        className="text-2xs text-text-faint truncate"
        title={collapsed ? label : undefined}
      >
        {collapsed ? 'cc' : label}
      </div>
    </div>
  );
}

/** Pages that get their own tab on phones; the rest live under "More". */
const TAB_BAR_PATHS = ['/', '/upstreams', '/principals', '/logs'] as const;

const TAB_ITEM =
  'relative flex flex-col items-center justify-center gap-1 min-w-0 text-caption font-medium text-text-muted transition-colors hover:text-text ' +
  'focus-visible:outline-2 focus-visible:outline-accent focus-visible:-outline-offset-2';

/**
 * Bottom tab bar below `lg`: four primary pages plus "More", which opens
 * the full navigation sheet. The active tab gets full ink on the neutral
 * selection fill. Page titles stay in the top bar only.
 */
export function SidebarTabBar({
  pathname,
  onMore,
  moreOpen,
}: {
  pathname: string;
  onMore: () => void;
  moreOpen: boolean;
}) {
  const current = navItemForPath(pathname)?.path ?? null;
  const moreActive =
    current !== null && !(TAB_BAR_PATHS as readonly string[]).includes(current);
  return (
    <nav
      aria-label="Main"
      className="fixed inset-x-0 bottom-0 z-30 grid grid-cols-5 border-t border-subtle bg-bg pb-[env(safe-area-inset-bottom)] lg:hidden"
    >
      {TAB_BAR_PATHS.map((path) => {
        const item = NAV_ITEMS.find((candidate) => candidate.path === path);
        if (!item) return null;
        const { label, Icon } = item;
        return (
          <Link
            key={path}
            to={path}
            activeOptions={{ exact: path === '/' }}
            className={cx(
              TAB_ITEM,
              'h-14 data-[status=active]:bg-selected data-[status=active]:text-text',
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
        aria-current={moreActive ? 'page' : undefined}
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
