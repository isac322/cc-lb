import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { RequestEventDrawer } from './RequestEventDrawer';

describe('RequestEventDrawer responsive structure', () => {
  it('renders with min-w-0 guards to prevent horizontal clipping on 500 errors', () => {
    const longString = 'a'.repeat(200);
    const event = {
      _phase: 'final',
      request_id: `req_${longString}`,
      ts: Date.now(),
      principal_id: `user_${longString}`,
      principal_kind: 'user',
      key_id: `key_${longString}`,
      upstream: `up_${longString}`,
      upstream_name: `upname_${longString}`,
      thread_id: `thread_${longString}`,
      model: `model_${longString}`,
      status: 500,
      error_code: `err_${longString}`,
      upstream_error_type: `type_${longString}`,
      upstream_error_message: `msg_${longString}`,
      input_tokens: 100,
      output_tokens: 200,
      cache_creation_input_tokens: 50,
      cache_read_input_tokens: 10,
      body_bytes: 1024,
      duration_ms: 150,
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={`name_${longString}`}
        onClose={() => {}}
      />,
    );

    const dialog = screen.getByRole('dialog', { name: 'Request detail' });
    expect(dialog.className).toContain('w-full');
    expect(dialog.className).toContain('max-w-lg');
    expect(dialog.className).toContain('overflow-x-hidden');

    const modelLabel = screen.getByText('Model');
    const modelValue = modelLabel.nextElementSibling;
    expect(modelValue?.className).toContain('min-w-0');

    const errorLabel = screen.getByText('Error');
    const errorValue = errorLabel.nextElementSibling;
    expect(errorValue?.className).toContain('min-w-0');

    const upstreamLabel = screen.getByText('Upstream');
    const upstreamValue = upstreamLabel.nextElementSibling;
    expect(upstreamValue?.className).toContain('min-w-0');

    const tokensHeader = screen.getByText('Tokens');
    const tokensCell = tokensHeader.parentElement;
    expect(tokensCell?.className).toContain('min-w-0');

    const grid = tokensCell?.parentElement;
    expect(grid?.className).toContain('grid-cols-1');
    expect(grid?.className).toContain('min-[420px]:grid-cols-2');
    expect(grid?.className).toContain('min-w-0');

    const costHeader = screen.getByText('Cost');
    const costCell = costHeader.parentElement;
    expect(costCell?.className).toContain('min-w-0');

    const scrollBody = grid?.parentElement;
    expect(scrollBody?.className).toContain('overflow-x-hidden');
  });

  it('renders with min-w-0 guards to prevent horizontal clipping on 499 client disconnected', () => {
    const longString = 'a'.repeat(200);
    const event = {
      _phase: 'final',
      request_id: `req_${longString}`,
      ts: Date.now(),
      principal_id: `user_${longString}`,
      principal_kind: 'user',
      key_id: `key_${longString}`,
      upstream: `up_${longString}`,
      upstream_name: `upname_${longString}`,
      thread_id: `thread_${longString}`,
      model: `model_${longString}`,
      status: 499,
      error_code: 'client_closed_request',
      input_tokens: 100,
      output_tokens: 200,
      cache_creation_input_tokens: 50,
      cache_read_input_tokens: 10,
      body_bytes: 1024,
      duration_ms: 150,
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={`name_${longString}`}
        onClose={() => {}}
      />,
    );

    const badge = screen.getByText('Client disconnected');
    expect(badge.className).toContain('min-w-0');
    expect(badge.className).toContain('whitespace-normal');
    expect(badge.className).toContain('break-words');
    expect(badge.className).not.toContain('whitespace-nowrap');
  });
});
