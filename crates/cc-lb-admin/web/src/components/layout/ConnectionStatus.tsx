import { useDashboardConnection } from '../../lib/api';
import { StatusChip } from '../primitives/StatusChip';

export function ConnectionStatus() {
  const state = useDashboardConnection();

  if (state === 'live') {
    return <StatusChip variant="live">Live</StatusChip>;
  }
  if (state === 'reconnecting') {
    return <StatusChip variant="warn">Reconnecting</StatusChip>;
  }
  if (state === 'auth_required') {
    return <StatusChip variant="danger">Auth required</StatusChip>;
  }
  return <StatusChip variant="neutral">Disconnected</StatusChip>;
}
