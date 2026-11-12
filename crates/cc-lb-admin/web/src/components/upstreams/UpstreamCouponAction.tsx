import { Ticket } from 'lucide-react';
import { useState } from 'react';
import type { QuotaSnapshot } from '../../lib/api';
import type { CouponInventory } from '../../lib/couponInventory';
import { type Upstream, useUpstreamOAuthStatus } from '../../lib/queries';
import { cx, Skeleton } from '../ui/primitives';
import { LimitResetAction } from './LimitResetAction';

interface Props {
  upstream: Upstream;
  windows: readonly QuotaSnapshot[];
  inventory?: CouponInventory;
  loading?: boolean;
}

/** The inventory poll is fleet-wide; the individual action loads only on demand. */
export function UpstreamCouponAction(props: Props) {
  const [activated, setActivated] = useState(false);
  const supported =
    props.upstream.kind === 'anthropic_oauth' ||
    props.upstream.kind === 'openai_oauth';
  if (!supported) return null;
  if (activated) return <OpenedCouponAction {...props} />;
  return <CouponTrigger {...props} onClick={() => setActivated(true)} />;
}

function OpenedCouponAction(props: Props) {
  const credential = useUpstreamOAuthStatus(props.upstream.id);
  return (
    <LimitResetAction
      upstream={props.upstream}
      credentialsStored={credential.data?.has_credentials}
      quotaWindows={props.windows}
      defaultOpen
      renderTrigger={({ onClick, disabled }) => (
        <CouponTrigger {...props} onClick={onClick} disabled={disabled} />
      )}
    />
  );
}

function CouponTrigger({
  upstream,
  inventory,
  loading,
  onClick,
  disabled = false,
}: Props & { onClick: () => void; disabled?: boolean }) {
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
