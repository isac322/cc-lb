import { Button } from './Button';

export function ErrorState({ title = 'Error', message, onRetry }: { title?: string; message?: string; onRetry?: () => void }) {
  return (
    <div className="flex flex-col items-center justify-center p-8 text-center">
      <div className="w-12 h-12 rounded-full bg-red-500/10 flex items-center justify-center mb-4">
        <svg className="w-6 h-6 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
        </svg>
      </div>
      <h3 className="text-sm font-medium text-graphite-200">{title}</h3>
      {message && <p className="text-sm text-graphite-400 mt-1 max-w-md">{message}</p>}
      {onRetry && (
        <Button variant="secondary" onClick={onRetry} className="mt-4">
          Retry
        </Button>
      )}
    </div>
  );
}
