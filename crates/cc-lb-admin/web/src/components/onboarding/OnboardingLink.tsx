import { Link } from '@tanstack/react-router';
import { ArrowRight } from 'lucide-react';
import type { ReactNode } from 'react';

/** Where a first-run step sends the operator: the existing page or its create dialog. */
export type OnboardingTarget =
  | 'new-upstream'
  | 'new-principal'
  | 'principals'
  | 'logs';

const LINK_CLASS =
  'inline-flex items-center gap-1 min-h-11 md:min-h-0 rounded-sm text-label text-accent-text underline-offset-2 hover:underline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2';

/** Text link that sends a first-run operator to an existing flow. */
export function OnboardingLink({
  target,
  children,
}: {
  readonly target: OnboardingTarget;
  readonly children: ReactNode;
}) {
  const body = (
    <>
      {children}
      <ArrowRight className="w-3 h-3" aria-hidden="true" />
    </>
  );
  switch (target) {
    case 'new-upstream':
      return (
        <Link to="/upstreams" search={{ action: 'new' }} className={LINK_CLASS}>
          {body}
        </Link>
      );
    case 'new-principal':
      return (
        <Link
          to="/principals"
          search={{ action: 'new' }}
          className={LINK_CLASS}
        >
          {body}
        </Link>
      );
    case 'principals':
      return (
        <Link to="/principals" className={LINK_CLASS}>
          {body}
        </Link>
      );
    case 'logs':
      return (
        <Link to="/logs" className={LINK_CLASS}>
          {body}
        </Link>
      );
  }
}
