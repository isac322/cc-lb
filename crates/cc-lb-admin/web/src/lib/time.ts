export function unixToDate(unixSeconds: number): Date {
  return new Date(unixSeconds * 1000);
}

export function getRangeLabel(range: string): string {
  const labels: Record<string, string> = {
    '15m': 'Last 15 minutes',
    '1h': 'Last 1 hour',
    '6h': 'Last 6 hours',
    '24h': 'Last 24 hours',
    '7d': 'Last 7 days',
  };
  return labels[range] || range;
}

export function formatRelativeTime(unixSeconds: number): string {
  const date = unixToDate(unixSeconds);
  const now = new Date();
  const diffInSeconds = Math.floor((now.getTime() - date.getTime()) / 1000);

  if (diffInSeconds < 60) return 'just now';
  if (diffInSeconds < 3600) return `${Math.floor(diffInSeconds / 60)}m ago`;
  if (diffInSeconds < 86400) return `${Math.floor(diffInSeconds / 3600)}h ago`;
  return `${Math.floor(diffInSeconds / 86400)}d ago`;
}
