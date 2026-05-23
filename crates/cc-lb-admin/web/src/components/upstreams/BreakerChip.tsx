import { StatusChip } from '../primitives/StatusChip';

interface BreakerChipProps {
  state: string;
  failureCount: number;
  halfOpenInFlight: number;
  observed: boolean;
}

export function BreakerChip({ state, failureCount, halfOpenInFlight, observed }: BreakerChipProps) {
  if (!observed) {
    return <StatusChip variant="neutral">unobserved</StatusChip>;
  }

  let status: 'ok' | 'warn' | 'danger' | 'neutral' = 'neutral';
  let label = state;

  if (state === 'closed') {
    status = 'ok';
  } else if (state === 'half_open') {
    status = 'warn';
    label = `half_open (${halfOpenInFlight} in flight)`;
  } else if (state === 'open') {
    status = 'danger';
    label = `open (${failureCount} fails)`;
  }

  return <StatusChip variant={status}>{label}</StatusChip>;
}
