import { useState } from 'react';
import { useDraftPrincipals } from '../../lib/hooks/useDraftPrincipals';
import type { PrincipalWithId } from '../../lib/hooks/usePrincipalsManagement';
import { Button } from '../primitives/Button';

export function EnableDisableSwitch({
  principal,
  onRefresh,
  mock,
}: {
  principal: PrincipalWithId;
  onRefresh: () => void;
  mock?: boolean;
}) {
  const { enablePrincipal, disablePrincipal } = useDraftPrincipals(mock);
  const [isPending, setIsPending] = useState(false);

  const handleToggle = async () => {
    setIsPending(true);
    try {
      if (principal.disabled) {
        await enablePrincipal(principal.id);
      } else {
        await disablePrincipal(principal.id);
      }
      onRefresh();
    } catch (err) {
      console.error('Failed to toggle principal state', err);
    } finally {
      setIsPending(false);
    }
  };

  return (
    <Button
      variant="secondary"
      onClick={handleToggle}
      disabled={isPending}
      className={principal.disabled ? 'text-cyan-400' : 'text-red-400'}
    >
      {principal.disabled ? 'Enable' : 'Disable'}
    </Button>
  );
}
