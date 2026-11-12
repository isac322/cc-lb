import { Ticket } from 'lucide-react';
import type { QuotaSnapshot } from '../../lib/api';
import type { CouponInventory } from '../../lib/couponInventory';
import { type Upstream, useUpstreamOAuthStatus } from '../../lib/queries';
import { cx, Skeleton } from '../ui/primitives';
import { LimitResetAction } from './LimitResetAction';

interface Props {
  upstream: Upstream;
  inventory?: CouponInventory;
  loading?: boolean;
}

/** One table-level owner survives row reordering and collapsed-list changes. */
export function UpstreamCouponAction({
  upstream,
  windows,
  onOpenChange,
}: {
  upstream: Upstream;
  windows: readonly QuotaSnapshot[];
  onOpenChange: (open: boolean) => void;
}) {
  const credential = useUpstreamOAuthStatus(upstream.id);
  return (
    <LimitResetAction
      upstream={upstream}
      credentialsStored={credential.data?.has_credentials}
      quotaWindows={windows}
      defaultOpen
      onOpenChange={onOpenChange}
      renderTrigger={() => null}
    />
  );
}

export function UpstreamCouponTrigger({
  upstream,
  inventory,
  loading,
  onClick,
  disabled = false,
}: Props & { onClick: () => void; disabled?: boolean }) {
  if (upstream.kind !== 'anthropic_oauth' && upstream.kind !== 'openai_oauth') {
    return null;
  }
  const count = inventory?.total ?? null;
  const summary =
    loading && count === null
      ? 'loading'
      : count === null
        ? 'unknown'
        : `${count}`;
  const breakdown = inventory?.groups
    .map((group) => `${group.label}: ${group.resets}`)
    .join(' · ');
  return (
    <button
      aria-label={`Coupons for ${upstream.name}: ${summary}`}
      aria-haspopup="dialog"
      className={cx(
        'inline-flex min-h-10 w-full items-center justify-end gap-1 rounded-sm px-1 text-caption tabular-nums text-text-muted transition-colors hover:bg-overlay-5 hover:text-text focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 md:min-h-7',
        disabled && 'control-disabled',
      )}
      data-testid="upstream-coupons"
      disabled={disabled}
      onClick={onClick}
      title={
        breakdown ||
        (count === null ? 'Coupon inventory not reported' : 'No active coupons')
      }
      type="button"
    >
      <Ticket
        aria-hidden="true"
        className="size-3.5 shrink-0"
        strokeWidth={1.75}
      />
      {loading && count === null ? (
        <Skeleton className="h-3 w-5" />
      ) : (
        <span aria-hidden="true">{count === null ? '—' : `×${count}`}</span>
      )}
    </button>
  );
}
