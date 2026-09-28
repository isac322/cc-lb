import { CheckCircle2, Circle } from 'lucide-react';
import { usePrincipals, useUpstreams } from '../../lib/queries';
import { cx, Section } from '../ui/primitives';
import { OnboardingLink, type OnboardingTarget } from './OnboardingLink';

interface Step {
  readonly id: string;
  readonly title: string;
  readonly detail: string;
  readonly done: boolean;
  readonly target: OnboardingTarget;
  readonly actionLabel: string;
}

/**
 * Whether the first-run checklist is still showing: `true` while any step is
 * open, `false` once all are done, `undefined` until every source answers.
 * Shares its queries with FirstRunChecklist, so both always agree.
 */
export function useFirstRunIncomplete(
  requestSeen: boolean | undefined,
): boolean | undefined {
  const upstreams = useUpstreams();
  const principals = usePrincipals();
  const upstreamCount = upstreams.data?.upstreams.length;
  const principalCount = principals.data?.principals.length;
  if (
    upstreamCount === undefined ||
    principalCount === undefined ||
    requestSeen === undefined
  ) {
    return undefined;
  }
  return !(upstreamCount > 0 && principalCount > 0 && requestSeen);
}

/**
 * First-run setup steps on the Overview, derived from live data: an upstream
 * exists, a principal exists, and at least one request has been seen. It
 * renders nothing until every source has answered and disappears once all
 * three steps are satisfied, so it never flashes for a configured pool.
 */
export function FirstRunChecklist({
  requestSeen,
}: {
  /** `undefined` while request data is still loading. */
  readonly requestSeen: boolean | undefined;
}) {
  const upstreams = useUpstreams();
  const principals = usePrincipals();
  const upstreamCount = upstreams.data?.upstreams.length;
  const principalCount = principals.data?.principals.length;
  if (
    upstreamCount === undefined ||
    principalCount === undefined ||
    requestSeen === undefined
  ) {
    return null;
  }

  const steps: readonly Step[] = [
    {
      id: 'upstream',
      title: 'Add an upstream',
      detail:
        'A Claude subscription or API key the pool routes requests through.',
      done: upstreamCount > 0,
      target: 'new-upstream',
      actionLabel: 'New upstream',
    },
    {
      id: 'principal',
      title: 'Create a principal',
      detail:
        'The client identity that sends requests. Its limits apply to every key it holds.',
      done: principalCount > 0,
      target: 'new-principal',
      actionLabel: 'New principal',
    },
    {
      id: 'request',
      title: 'Issue a proxy key and send a request',
      detail:
        "Issue a key from the principal's page and send a request through the proxy with it. It shows up in Logs.",
      done: requestSeen,
      target: 'principals',
      actionLabel: 'Open principals',
    },
  ];
  const doneCount = steps.filter((step) => step.done).length;
  if (doneCount === steps.length) return null;
  const nextId = steps.find((step) => !step.done)?.id;

  return (
    <Section
      title="Set up the pool"
      subtitle={`${doneCount} of ${steps.length} done. Quota, traffic and the latest requests show up here once the first request goes through.`}
    >
      <ol className="flex max-w-3xl flex-col">
        {steps.map((step) => {
          const isNext = step.id === nextId;
          return (
            <li
              key={step.id}
              className="flex items-start gap-3 border-t border-row py-4 first:border-t-0 first:pt-0"
              aria-current={isNext ? 'step' : undefined}
            >
              {step.done ? (
                <CheckCircle2
                  className="mt-0.5 w-4 h-4 shrink-0 text-text-muted"
                  aria-hidden="true"
                />
              ) : (
                <Circle
                  className={cx(
                    'mt-0.5 w-4 h-4 shrink-0',
                    isNext ? 'text-text' : 'text-text-faint',
                  )}
                  aria-hidden="true"
                />
              )}
              {/* Phones: the 44px link box already gives the step's action
                  its air, so no extra gap sits between copy and link. */}
              <div className="min-w-0 flex-1 flex flex-col sm:flex-row sm:items-start sm:justify-between sm:gap-4">
                <div className="min-w-0">
                  <p
                    className={cx(
                      'text-body',
                      step.done ? 'text-text-muted' : 'text-text font-medium',
                    )}
                  >
                    {step.title}
                    <span className="sr-only">
                      {step.done ? ' (done)' : ' (to do)'}
                    </span>
                  </p>
                  {step.done ? null : (
                    <p className="mt-0.5 max-w-prose text-body text-text-muted">
                      {step.detail}
                    </p>
                  )}
                </div>
                {step.done ? null : (
                  <div className="shrink-0 sm:pt-0.5">
                    <OnboardingLink target={step.target}>
                      {step.actionLabel}
                    </OnboardingLink>
                  </div>
                )}
              </div>
            </li>
          );
        })}
      </ol>
    </Section>
  );
}
