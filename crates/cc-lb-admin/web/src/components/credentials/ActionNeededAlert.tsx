import { AlertTriangle } from 'lucide-react';

interface ActionNeededAlertProps {
  status: string;
}

export function ActionNeededAlert({ status }: ActionNeededAlertProps) {
  if (status !== 'expired' && status !== 'expiring_soon' && status !== 'missing') {
    return null;
  }

  return (
    <div className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-red-100 text-red-800">
      <AlertTriangle className="w-3 h-3" />
      Action needed
    </div>
  );
}
