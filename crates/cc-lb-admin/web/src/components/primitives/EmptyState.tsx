export function EmptyState({ title = 'No data', message }: { title?: string; message?: string }) {
  return (
    <div className="flex flex-col items-center justify-center p-8 text-center">
      <div className="w-12 h-12 rounded-full bg-graphite-800 flex items-center justify-center mb-4">
        <svg className="w-6 h-6 text-graphite-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
        </svg>
      </div>
      <h3 className="text-sm font-medium text-graphite-200">{title}</h3>
      {message && <p className="text-sm text-graphite-400 mt-1">{message}</p>}
    </div>
  );
}
