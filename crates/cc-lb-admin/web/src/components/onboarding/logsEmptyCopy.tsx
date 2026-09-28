import { OnboardingLink } from './OnboardingLink';

/**
 * Empty-table copy for Logs. The unfiltered case means no request has been
 * recorded at all, so it names the next step instead of blaming filters. The
 * action is a module-level element so the memoized table keeps a stable prop.
 */
export const LOGS_EMPTY_COPY = {
  filtered: {
    title: 'No matching requests',
    description:
      'Nothing matches the current filters. Clear them to see all requests.',
    action: undefined,
  },
  unfiltered: {
    title: 'No requests yet',
    description:
      "Requests appear here once a client sends traffic through the proxy with a principal's proxy key.",
    action: (
      <OnboardingLink target="principals">Issue a proxy key</OnboardingLink>
    ),
  },
} as const;
