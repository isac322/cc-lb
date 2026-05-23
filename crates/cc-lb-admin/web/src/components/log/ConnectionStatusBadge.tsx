import { StatusChip } from '../primitives/StatusChip';

export function ConnectionStatusBadge({ status }: { status: 'live' | 'reconnecting' | 'error' }) {
  if (status === 'live') {
    return <StatusChip variant="live">Live</StatusChip>;
  }
  if (status === 'reconnecting') {
    return <StatusChip variant="warn">Reconnecting</StatusChip>;
  }
  return <StatusChip variant="danger">Error</StatusChip>;
}
