import { Link } from '@tanstack/react-router';
import { Ticket } from 'lucide-react';
import type { CouponInventory } from '../../lib/couponInventory';
import { formatCount } from '../../lib/format';
import { cx, Hint } from '../ui/primitives';

/** A separate button and portal leave the principal navigation link intact. */
export function CouponInventoryBadge({
  inventory,
  loading = false,
  error = false,
}: {
  inventory: CouponInventory;
  loading?: boolean;
  error?: boolean;
}) {
  const count = inventory.total;
  const text = !inventory.supported
    ? 'Resets not reported'
    : loading && count === null
      ? 'Resets loading…'
      : count === null
        ? 'Resets unknown'
        : inventory.complete
          ? `${formatCount(count)} ${count === 1 ? 'reset' : 'resets'}`
          : `${formatCount(count)} known · incomplete`;
  const details = (
    <div
      className="flex flex-col gap-2 py-1"
      data-testid="coupon-inventory-details"
    >
      <p className="font-medium">Resets on allowed upstreams</p>
      <p className="text-text-muted">
        Active resets on enabled, credentialed Anthropic and OpenAI upstreams
        allowed for this principal in the current provider scope. Shared account
        inventory counts once. Resets are not owned by this principal and do not
        guarantee routing or current usability. Paused and cooldown grants, and
        coupons unsupported by the current plan, remain included.
      </p>
      {!inventory.supported ? (
        <p className="text-text-muted">
          Reset inventory is not reported for this provider.
        </p>
      ) : loading && count === null ? (
        <p className="text-text-muted">Loading reset inventory…</p>
      ) : !inventory.complete ? (
        <p className="text-text-muted">
          {error
            ? 'Inventory could not be refreshed. Any counts shown are last known, not a complete current total.'
            : 'Inventory is incomplete: an identity or fresh observation is missing, or duplicate account observations disagree.'}
        </p>
      ) : count === 0 ? (
        <p className="text-text-muted">No active resets on these upstreams.</p>
      ) : null}
      {inventory.groups.length > 0 ? (
        <ul className="flex flex-col gap-2">
          {inventory.groups.map((group) => (
            <li className="flex flex-col gap-1" key={group.key}>
              <div className="flex items-baseline justify-between gap-3">
                <span className="flex min-w-0 items-baseline gap-1.5 break-words">
                  <span
                    aria-hidden="true"
                    className={cx(
                      'inline-block size-2 shrink-0',
                      group.fable ? 'bg-series-fable' : 'bg-neutral',
                    )}
                  />
                  <span>{group.label}</span>
                </span>
                <span className="shrink-0 tabular-nums">
                  {group.resets} {group.resets === 1 ? 'reset' : 'resets'}
                </span>
              </div>
              <span className="text-text-muted">
                {group.provider === 'anthropic' ? 'Anthropic' : 'OpenAI'}
                {group.scope ? ` · Clears ${group.scope}` : null}
              </span>
              {group.planUnsupported ? (
                <span className="text-text-muted">
                  Included in inventory; not supported by the current plan.
                </span>
              ) : group.unknownType ? (
                <span className="text-text-muted">
                  The provider reports this count without matching coupon
                  details. Type and plan support are unknown.
                </span>
              ) : group.applicabilityUnknown ? (
                <span className="text-text-muted">
                  Plan support has not been reported.
                </span>
              ) : null}
              <div className="flex flex-wrap gap-x-2 gap-y-1">
                {group.upstreamIds.map((upstreamId) => (
                  <UpstreamLink id={upstreamId} key={upstreamId} />
                ))}
              </div>
            </li>
          ))}
        </ul>
      ) : null}
      {inventory.upstreamIds.length > 0 ? (
        <div className="flex flex-col gap-1 border-t border-row pt-2">
          <p className="text-text-muted">Allowed upstreams in this inventory</p>
          <div className="flex flex-wrap gap-x-2 gap-y-1">
            {inventory.upstreamIds.map((upstreamId) => (
              <span
                className="inline-flex items-baseline gap-1"
                key={upstreamId}
              >
                <UpstreamLink id={upstreamId} />
                {inventory.unknownUpstreamIds.includes(upstreamId) ? (
                  <span className="text-text-muted">(unknown)</span>
                ) : null}
              </span>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );

  return (
    <Hint label={details} openOnFocus={false} side="bottom">
      <button
        aria-label={`Resets on allowed upstreams: ${text}. Show types and source upstreams`}
        className="inline-flex min-w-0 max-w-full items-center gap-1 rounded-sm text-caption text-text-muted underline decoration-border-strong underline-offset-4 transition-colors hover:text-text focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 max-md:min-h-10"
        data-testid="coupon-inventory-badge"
        type="button"
      >
        <Ticket
          aria-hidden="true"
          className="size-3.5 shrink-0"
          strokeWidth={1.75}
        />
        <span className="min-w-0 whitespace-normal text-left tabular-nums">
          {text}
        </span>
      </button>
    </Hint>
  );
}

function UpstreamLink({ id }: { id: string }) {
  return (
    <Link
      className="break-all rounded-sm text-accent-text underline decoration-border-strong underline-offset-4 hover:decoration-accent focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 max-md:inline-flex max-md:min-h-10 max-md:items-center"
      onClick={(event) => event.stopPropagation()}
      search={{ selectedId: id }}
      to="/upstreams"
    >
      {id}
    </Link>
  );
}
