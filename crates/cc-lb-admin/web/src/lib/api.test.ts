import { describe, expect, it } from 'vitest';
import { RequestEventPartialSchema } from './api';

describe('RequestEventPartialSchema', () => {
  const baseFixture = {
    event_id: 'e',
    request_id: 'r',
    ts: null,
    ts_ms: null,
  };

  it('parses valid partial object with thinking_budget_tokens as number', () => {
    const result = RequestEventPartialSchema.safeParse({
      ...baseFixture,
      thinking_budget_tokens: 18000,
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.thinking_budget_tokens).toBe(18000);
    }
  });

  it('parses valid partial object with thinking_budget_tokens as null', () => {
    const result = RequestEventPartialSchema.safeParse({
      ...baseFixture,
      thinking_budget_tokens: null,
    });
    expect(result.success).toBe(true);
  });

  it('parses valid partial object with thinking_budget_tokens omitted', () => {
    const result = RequestEventPartialSchema.safeParse(baseFixture);
    expect(result.success).toBe(true);
  });

  it('rejects partial object with thinking_budget_tokens as string', () => {
    const result = RequestEventPartialSchema.safeParse({
      ...baseFixture,
      thinking_budget_tokens: '18000',
    });
    expect(result.success).toBe(false);
  });
});
