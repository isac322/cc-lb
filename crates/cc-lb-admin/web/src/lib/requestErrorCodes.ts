// Plain-language explanations for request-log `error_code` values. The codes
// mirror the fixed catalog in cc-lb-engine `terminal_observer::error_codes`;
// anything outside it (future codes, plugin-provided values) falls back to a
// generic sentence and keeps the raw code visible.

/** Config surface that can resolve the failure, when there is one. */
export type ErrorCodeTarget =
  | 'principal-router'
  | 'principal-limits'
  | 'upstream';

/** DOM ids of the principal detail cards the explanations link to. */
export const PRINCIPAL_ROUTER_ANCHOR = 'principal-router';
export const PRINCIPAL_LIMITS_ANCHOR = 'principal-default-limits';

export interface ErrorCodeExplanation {
  summary: string;
  nextStep: string | null;
  target: ErrorCodeTarget | null;
  known: boolean;
}

const KNOWN: Record<string, Omit<ErrorCodeExplanation, 'known'>> = {
  body_too_large: {
    summary: 'The request body was larger than the proxy accepts.',
    nextStep: 'Send a smaller request.',
    target: null,
  },
  body_read_failed: {
    summary: 'The proxy could not read the request body from the client.',
    nextStep: 'Retry the request; check the client connection if it repeats.',
    target: null,
  },
  invalid_json: {
    summary: 'The request body was not valid JSON.',
    nextStep: 'Fix the request payload in the client.',
    target: null,
  },
  authentication_failed: {
    summary: 'The proxy could not authenticate the request’s API key.',
    nextStep: 'Check that the client sends a valid, unrevoked key.',
    target: null,
  },
  principal_missing: {
    summary: 'The key authenticated, but its principal could not be loaded.',
    nextStep: 'Check that the principal still exists.',
    target: null,
  },
  router_pipeline_unavailable: {
    summary:
      'The principal’s router plugin chain could not run for this request.',
    nextStep: 'Review the router chain on the principal.',
    target: 'principal-router',
  },
  route_no_upstream_after_filter: {
    summary:
      'The router’s filters removed every upstream, so none could serve the request.',
    nextStep:
      'Loosen the filter chain on the principal’s router, or enable an upstream it allows.',
    target: 'principal-router',
  },
  route_not_configured: {
    summary:
      'The router did not resolve to an available upstream for this request.',
    nextStep:
      'Review the principal’s router and check that the chosen upstream exists and is enabled.',
    target: 'principal-router',
  },
  route_not_found: {
    summary: 'No proxy endpoint matches the requested path.',
    nextStep: 'Check the URL the client calls.',
    target: null,
  },
  method_not_allowed: {
    summary: 'The endpoint exists but does not accept this HTTP method.',
    nextStep: 'Check the HTTP method the client uses.',
    target: null,
  },
  drain_rejected: {
    summary: 'The proxy was draining for shutdown and refused new requests.',
    nextStep: 'Retry once the proxy is back up.',
    target: null,
  },
  upstream_affinity_unavailable: {
    summary:
      'Session affinity storage failed, so the proxy could not pick the sticky upstream.',
    nextStep: 'Retry; if it repeats, check the proxy’s storage.',
    target: null,
  },
  limit_rejected: {
    summary: 'A rate limit on this principal or key rejected the request.',
    nextStep: 'Wait for the window to reset, or raise the principal’s limits.',
    target: 'principal-limits',
  },
  signer_failed: {
    summary:
      'The proxy could not prepare the upstream credentials for this request.',
    nextStep:
      'Check the upstream’s credentials; reconnect it if they are unreadable or expired.',
    target: 'upstream',
  },
  upstream_dispatch_failed: {
    summary: 'The request could not be delivered to the upstream.',
    nextStep: 'Check the upstream’s endpoint and network reachability.',
    target: 'upstream',
  },
  upstream_4xx: {
    summary: 'The upstream rejected the request with a client error.',
    nextStep: 'See the upstream error below for the reason.',
    target: 'upstream',
  },
  upstream_5xx: {
    summary: 'The upstream failed with a server error.',
    nextStep: 'Retry; if it repeats, check the upstream’s status.',
    target: 'upstream',
  },
  upstream_stream_error: {
    summary: 'The upstream reported an error while streaming the response.',
    nextStep: 'See the upstream error below; retry if it was transient.',
    target: 'upstream',
  },
  upstream_refusal: {
    summary: 'The model refused to complete the request.',
    nextStep: null,
    target: null,
  },
  upstream_context_window_exceeded: {
    summary: 'The request exceeded the model’s context window.',
    nextStep:
      'Shorten the conversation or switch to a model with a larger context.',
    target: null,
  },
  tower_timeout: {
    summary: 'The request took longer than the proxy’s timeout.',
    nextStep: 'Retry; check the upstream’s latency if it repeats.',
    target: null,
  },
  terminal_dropped: {
    summary: 'The request ended without a recorded outcome.',
    nextStep: null,
    target: null,
  },
  client_closed_request: {
    summary: 'The client closed the connection before the response finished.',
    nextStep: null,
    target: null,
  },
};

export function explainErrorCode(code: string): ErrorCodeExplanation {
  const known = KNOWN[code];
  if (known) return { ...known, known: true };
  return {
    summary:
      'The request ended with an error the dashboard has no description for.',
    nextStep: null,
    target: null,
    known: false,
  };
}
