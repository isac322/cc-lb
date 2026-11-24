import { describe, expect, it } from 'vitest';
import preflightOkOrg from './__contract__/preflight_ok_org.json';
import { normalizePreflight, PreflightWireResponseSchema } from './api';

// PRB-06 (web side): count_tokens 429 reaches the wire as `ok_limited`; the
// dialog treats the key as working.
describe('normalizePreflight inference mapping', () => {
  const withInference = (inference: string) =>
    PreflightWireResponseSchema.parse({
      ...preflightOkOrg,
      capabilities: { ...preflightOkOrg.capabilities, inference },
    });

  it('maps ok_limited to ok', () => {
    const view = normalizePreflight(withInference('ok_limited'));
    expect(view.verdict).toBe('ok');
    expect(view.inference).toBe('ok');
  });

  it('keeps ok, denied, and unverified as they are', () => {
    for (const inference of ['ok', 'denied', 'unverified'] as const) {
      expect(normalizePreflight(withInference(inference)).inference).toBe(
        inference,
      );
    }
  });

  it('rejects an inference value outside the contract', () => {
    expect(
      PreflightWireResponseSchema.safeParse({
        ...preflightOkOrg,
        capabilities: { ...preflightOkOrg.capabilities, inference: 'limited' },
      }).success,
    ).toBe(false);
  });
});
