export function Table<T>({
  data,
  columns,
  keyExtractor,
}: {
  data: T[];
  columns: {
    header: string;
    render: (item: T) => React.ReactNode;
    className?: string;
  }[];
  keyExtractor: (item: T) => string;
}) {
  return (
    <div className="w-full overflow-x-auto border border-graphite-800 rounded-lg bg-graphite-850">
      <table className="w-full text-sm text-left">
        <thead className="text-xs text-graphite-400 uppercase bg-graphite-900 border-b border-graphite-800 sticky top-0">
          <tr>
            {columns.map((col, i) => (
              <th
                key={i}
                className={`px-4 py-3 font-medium ${col.className || ''}`}
              >
                {col.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-graphite-800">
          {data.map((item) => (
            <tr
              key={keyExtractor(item)}
              className="hover:bg-graphite-800/50 transition-colors"
            >
              {columns.map((col, i) => (
                <td key={i} className={`px-4 py-3 ${col.className || ''}`}>
                  {col.render(item)}
                </td>
              ))}
            </tr>
          ))}
          {data.length === 0 && (
            <tr>
              <td
                colSpan={columns.length}
                className="px-4 py-8 text-center text-graphite-500"
              >
                No data available
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
