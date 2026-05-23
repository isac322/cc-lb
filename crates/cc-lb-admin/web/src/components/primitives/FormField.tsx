export function FormField({
  label,
  help,
  error,
  children,
}: {
  label: string;
  help?: string;
  error?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col gap-1.5">
      <label className="text-sm font-medium text-graphite-200">{label}</label>
      {children}
      {error ? (
        <p className="text-xs text-red-400">{error}</p>
      ) : help ? (
        <p className="text-xs text-graphite-400">{help}</p>
      ) : null}
    </div>
  );
}
