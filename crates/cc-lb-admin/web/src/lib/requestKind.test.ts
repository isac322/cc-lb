import { describe, expect, it } from 'vitest';
import { requestKindBadgeText } from './requestKind';

describe('requestKindBadgeText', () => {
  it.each([
    ['main', 'main'],
    ['advisor', 'adv'],
    ['subagent', 'sub'],
    ['recap', 'recap'],
    ['compaction', 'comp'],
    ['notification', 'notif'],
    ['session_title', 'title'],
    ['auto_thinking', 'think'],
    ['side', 'side'],
    ['look_at', 'vision'],
    ['unknown', 'unk'],
  ])('renders %s as %s', (requestKind, expected) => {
    expect(requestKindBadgeText(requestKind)).toBe(expected);
  });

  it('preserves an unrecognized non-empty kind', () => {
    expect(requestKindBadgeText('future_kind')).toBe('future_kind');
  });

  it('hides missing or blank kinds', () => {
    expect(requestKindBadgeText(null)).toBeNull();
    expect(requestKindBadgeText('   ')).toBeNull();
  });
});
