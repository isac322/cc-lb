export function LoadingState({ message = 'Loading...' }: { message?: string }) {
  return (
    <div className="flex flex-col items-center justify-center p-8 text-center">
      <div className="w-6 h-6 border-2 border-graphite-700 border-t-cyan-500 rounded-full animate-spin mb-4" />
      <p className="text-sm text-graphite-400">{message}</p>
    </div>
  );
}
