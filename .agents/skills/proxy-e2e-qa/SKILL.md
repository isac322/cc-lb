---
name: proxy-e2e-qa
description: Use this skill as the proxy-path QA gate for any work that can affect a proxy, gateway, load balancer, routing rule, request/response transformation, header/auth handling, signing path, upstream selection, retries, timeouts, streaming, error propagation, cache/rate-limit behavior, remote access surface, or client-visible proxy semantics. This skill does not own implementation; invoke it before implementation to define acceptance criteria and after implementation to prove behavior with proportional proxy-path verification, cleanup, and honest PASS/FAIL/BLOCKED reporting. Do not use for admin/dashboard-only work, OAuth setup debugging, or generic service health checks unless the proxy request path or client-visible routing behavior is affected.
---

# Proxy path QA gate

This skill captures the proxy QA workflow extracted from session `ses_1817dabe1ffeXI7R0UTrzmckQ4`, where the user corrected an agent that verified build, deployment, and health endpoints but skipped the actual proxy behavior the user cared about.

This is a verification harness and QA gate, not the implementation owner. It defines what proxy behavior must be proven, then verifies the implementation through the client-visible request path. A full provider E2E request is only the largest case; smaller proxy changes still need requirement capture and proportional evidence.

## Role and call timing

Call this skill at two points:

1. **Before proxy-affecting implementation**: use it to record the requirement, identify affected proxy surfaces, and define PASS/FAIL/BLOCKED evidence. Then hand implementation to the appropriate normal coding/debugging workflow.
2. **After implementation**: use it again to prove the proxy-path behavior. If verification fails, classify the failure and produce fix targets, then return to implementation and re-run this skill.

This skill owns:

- proxy acceptance criteria;
- verification depth selection;
- proxy-path test planning/execution guidance;
- evidence classification;
- temporary credential cleanup requirements;
- honest PASS/FAIL/BLOCKED/PARTIAL reporting.

It does **not** own broad code implementation. It may identify the failing layer and recommend the next fix, but another implementation flow makes the code change.

## Core principle

A proxy change is not done when code compiles, a port listens, or an admin health endpoint returns 200. It is done when the requested client-visible behavior is recorded, the affected proxy surfaces are identified, the verification depth matches the risk, and evidence proves the request/response path behaves as intended.

For full routing/signing/upstream claims, evidence must include a request entering the proxy surface with client-shaped credentials/headers, routing to an eligible upstream, returning the expected downstream response or classified failure, and cleaning up any temporary credential.

## When this skill applies

Use this skill for changes or questions involving:

- route matching, path rewriting, base URLs, dev proxy config, remote/LAN/client-visible surfaces;
- header forwarding, header stripping/sanitization, auth header names, token/key propagation, hop-by-hop headers;
- request shaping, body transforms, provider API versions, signing, OAuth-bound upstream requests;
- upstream selection, health/credential readiness, failover, load-balancing weights, sticky routing;
- retries, timeouts, cancellation, streaming/SSE, backpressure, response flushing;
- error mapping, status propagation, provider-vs-proxy failure classification, redaction;
- cache, rate limit, quota, cost accounting, request logging, selected-upstream metrics;
- any user question like "did the proxy still work?", "does this route actually go through the proxy?", or "don't fake it with health checks".

Do not use this skill for pure admin UI/dashboard work, provider OAuth setup debugging, systemd status checks, or generic port health checks unless the change can affect what a client sends to the proxy, where traffic is routed, or what response/error the client observes.

## Start by recording requirements

Before editing or testing, create a requirement checklist scaled to the task size. Never skip it.

For small proxy changes, an inline checklist is enough. For multi-surface or risky work, save a durable checklist in the workspace.

Record:

- the exact user-facing proxy behavior requested;
- the client-visible URL, port, protocol, path, and credential/header shape;
- the affected routes, transforms, headers, auth/signing steps, upstream selection rules, retries/timeouts, streaming/error behavior;
- non-regression behavior that must keep working;
- whether testing may create temporary credentials, hit paid providers, or touch production-like data;
- what evidence will count as PASS, FAIL, or BLOCKED.

## Decide proportional verification depth

Choose the smallest verification that proves the actual risk. Escalate when evidence is insufficient.

### Small proxy-affecting change

Examples: strip one hop-by-hop header, adjust one path rewrite, change timeout mapping, fix one base URL.

Workflow:

1. Inventory the exact call path and all callsites/config entries that can use it.
2. Build a focused positive request and at least one negative/control request.
3. Verify the transformed outbound request, selected route/upstream, and client-visible response/error.
4. Inspect logs/metrics/traces when available to prove the proxy lifecycle, not just the client response.
5. Report what was and was not covered. Do not claim full E2E if only a focused slice was tested.

### Medium proxy behavior change

Examples: upstream selection, retry/failover, auth/header propagation, provider error mapping, streaming behavior.

Workflow:

1. Create a small matrix: success path, expected failure path, and edge/control path.
2. Use test upstreams, mocks, or deterministic local harnesses when they prove the behavior better than a paid provider call.
3. Verify both client-visible results and internal evidence: selected upstream, retry count, timeout class, redaction, metrics, or trace IDs.
4. Re-run after fixes until every matrix row is classified PASS/FAIL/BLOCKED with evidence.

### Full proxy E2E

Use when the user asks whether the proxy actually works end-to-end, when routing/signing/upstream forwarding changed, or before claiming the proxy path is safe after risky work.

Workflow:

1. Identify proxy URL/port and admin/status surfaces.
2. Confirm an upstream is actually usable; do not trust a UI green dot or `enabled=true` alone.
3. Issue or identify a client credential if the system requires one.
4. Send a minimal request through the proxy surface, not admin health, not a dev-server proxy unless that is the client surface, and not the upstream directly.
5. Inspect status, body shape/content, selected upstream/logs/metrics, and failure class.
6. Revoke/delete any temporary credential and verify cleanup.

### Production-like or paid-provider QA

Use the cheapest request that still proves the path. Avoid retries unless required. Redact secrets. Report cost/side effects. Treat cleanup failure as a blocker/security follow-up, not a footnote.

## cc-lb-style full E2E shape

When the target resembles cc-lb, this shape is a useful template, not the only workflow:

- proxy port: `52251`;
- admin port: `52252`;
- status: `/admin/v1/status`;
- principal API key issue/revoke: `/admin/v1/principals/<principal_id>/keys`;
- provider request path: `/v1/messages` with `x-api-key`, `anthropic-version`, and JSON body.

A minimal Anthropic-compatible proxy proof may look like:

```bash
curl -i -X POST \
  -H "x-api-key: $APIKEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "Content-Type: application/json" \
  -d '{"model":"claude-haiku-4-5-20251001","max_tokens":24,"messages":[{"role":"user","content":"Reply with exactly: pong"}]}' \
  http://127.0.0.1:52251/v1/messages
```

Adapt model, path, host, and credential shape to the project. If the user/client reaches the proxy through a LAN IP or public hostname, verify that surface or explicitly say it was not verified.

## Failure handling

- No usable upstream: report `BLOCKED`, explain the missing prerequisite, and do not fake proxy pass.
- Auth/key issue fails: classify admin auth, principal selection, key issuance, or client credential shape.
- Proxy returns 401/403: check credential, principal permissions, and header name.
- Upstream/provider returns 4xx: distinguish successful routing to provider from provider rejection.
- Proxy returns 5xx/timeout: inspect logs/metrics/traces and classify routing, signing, upstream, timeout, retry, or network failure.
- Cleanup fails: keep trying safe cleanup paths or ask for operator action; report as blocker/security follow-up.
- Remote/client-visible surface differs from localhost: test the client-visible surface or state the gap.

## Report format

Use a report scaled to the task:

```markdown
Proxy QA: PASS|FAIL|BLOCKED|PARTIAL

Requirement checklist:
- Requested behavior:
- Affected proxy surfaces:
- Evidence required:

Verification depth:
- Small focused slice | medium matrix | full E2E | production-like

Evidence:
- Client-visible URL/path tested:
- Request shape and credential/header source:
- Upstream/route selected:
- Response/status/body/error class:
- Logs/metrics/traces checked:
- Cleanup result, if credentials were created:

Limits:
- What was not verified:
- Cost/side effects:
```

## Things the source session taught the hard way

- Do not answer "proxy unchanged" or "health is 200" when the user asked whether proxy behavior works.
- Do not reduce every proxy task to full provider E2E; small routing/header changes need focused proof, not ceremony.
- Do not claim full E2E from localhost if the user needed the remote/client-visible surface.
- Do not rely on dashboard status badges for upstream credential readiness.
- Do not leave temporary test keys active.
- Do not expose plaintext client keys or provider tokens in the report.
- Do not confuse OAuth setup QA with proxy-path QA. OAuth can be a prerequisite, but this skill proves client-visible proxy behavior.

## Trigger examples

Use this skill for prompts like:

- "proxy 테스트했어? 실제 요청 처리되는지 확인해."
- "I changed header sanitization in the gateway; prove X-Secret is stripped and auth still reaches the signer."
- "This path rewrite should send `/api/messages` to `/v1/messages`; verify it without doing unnecessary paid calls."
- "Remote users hit 10.222.0.6, so don't test only localhost."
- "Timeouts from upstream should become 504 with the original request id in logs."
- "Before PR, run the minimal safe proxy E2E and clean up test credentials."

Do not use it for:

- "dashboard 버튼 눌러봐" unless the dashboard changes proxy request behavior.
- "OAuth code exchange fails" unless the next claim is about proxy requests after OAuth succeeds.
- "systemd status 확인해" unless service health is only a prerequisite to proxy-path verification.
- "rename an internal Rust variable" unless it changes routing/signing/header behavior.
