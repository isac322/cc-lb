import { Link } from '@tanstack/react-router';
import { BookOpen } from 'lucide-react';
import { useId, useMemo } from 'react';
import {
  type OAuthReconnectNudge,
  useOAuthReconnectNudges,
} from '../../lib/oauthReconnect';
import { useUpstreams } from '../../lib/queries';
import { cx } from '../ui/primitives';
import { NAV_GROUPS, type NavGroup } from './navItems';

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
  return (
    <span
      className={cx(
        'ml-auto inline-flex h-4 min-w-4 items-center justify-center rounded-full px-1 text-2xs font-medium tabular-nums',
        danger ? 'bg-danger/15 text-danger-text' : 'bg-warn/15 text-warn-text',
      )}
    >
      <span aria-hidden="true">{count}</span>
      <span className="sr-only">{a11y}</span>
    </span>
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
      <div
        aria-hidden="true"
        className="w-6 h-6 shrink-0 bg-overlay-6 border border-subtle text-text flex items-center justify-center font-semibold text-2xs rounded-sm"
      >
        CC
      </div>
      {!collapsed ? (
        <span className="font-medium text-sm">cc-lb</span>
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
            : cx('px-2.5 pb-1 text-overline text-text-faint', LABEL_FADE),
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
              className={cx(
                'relative flex items-center gap-2.5 px-2.5 h-9 rounded-sm text-sm transition-colors hover:bg-overlay-5 text-text-muted hover:text-text',
                'focus-visible:outline-2 focus-visible:outline-accent focus-visible:-outline-offset-2',
                collapsed ? 'justify-center' : '',
              )}
              activeProps={{ className: 'bg-overlay-6 text-text' }}
            >
              <Icon className="w-4 h-4 shrink-0" aria-hidden="true" />
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
        'flex-1 pt-3 pb-8 px-2 overflow-y-auto',
        collapsed ? 'space-y-3' : 'space-y-5',
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
        'border-t border-subtle px-3 py-3 flex flex-col gap-2',
        collapsed ? 'items-center text-center' : '',
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
