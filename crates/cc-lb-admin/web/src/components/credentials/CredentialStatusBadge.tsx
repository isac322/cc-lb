import { StatusChip } from '../primitives/StatusChip';

interface CredentialStatusBadgeProps {
  status: string;
}

export function CredentialStatusBadge({ status }: CredentialStatusBadgeProps) {
  let color: 'ok' | 'warn' | 'danger' | 'neutral' = 'neutral';
  
  if (status === 'valid') color = 'ok';
  else if (status === 'expiring_soon') color = 'warn';
  else if (status === 'expired' || status === 'revoked') color = 'danger';
  
  return <StatusChip variant={color}>{status}</StatusChip>;
}
