const REQUEST_KIND_BADGE_TEXT: Readonly<Record<string, string>> = {
  main: 'main',
  advisor: 'adv',
  subagent: 'sub',
  recap: 'recap',
  compaction: 'comp',
  notification: 'notif',
  session_title: 'title',
  auto_thinking: 'think',
  side: 'side',
  unknown: 'unk',
};

export function requestKindBadgeText(
  requestKind: string | null | undefined,
): string | null {
  const normalized = requestKind?.trim();
  if (!normalized) return null;
  return REQUEST_KIND_BADGE_TEXT[normalized] ?? normalized;
}
