import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import { captureGoogleGenai, judgeReplayedTurns, summarizeRequest } from "./run.mjs";

// Run: node --test tests/real-client/provider-e2e/omp-gemini/capture.test.mjs
// Invokes uv + the actual pinned google-genai SDK, never Google or cc-lb.

test("observer preserves alias spelling, false/zero values and original request bytes", () => {
  const raw = Buffer.from('{ "serviceTier":"flex", "service_tier":"priority", "generationConfig":{"thinkingConfig":{"thinkingLevel":"LOW","thinkingBudget":0,"includeThoughts":false}}, "contents":[] }\n');
  const before = Buffer.from(raw);
  const summary = summarizeRequest({ method: "POST", url: "/v1beta/models/gemini-3.1-pro-preview:generateContent", headers: {} }, raw);
  assert.deepEqual(raw, before);
  assert.equal(summary.body_bytes, raw.length);
  assert.equal(summary.body_sha12, createHash("sha256").update(raw).digest("hex").slice(0, 12));
  assert.deepEqual(summary.service_tier_aliases, { service_tier: "priority", serviceTier: "flex" });
  assert.deepEqual(summary.thinking_config, { keys: ["includeThoughts", "thinkingBudget", "thinkingLevel"], thinkingLevel: "LOW", thinkingBudget: 0, includeThoughts: false });
});
test("observer normalizes native snake aliases with camelCase precedence", () => {
  const snake = Buffer.from('{"generationConfig":{"thinkingConfig":{"thinking_level":"LOW","thinking_budget":1024,"include_thoughts":true}}}');
  assert.deepEqual(summarizeRequest({ method: "POST", url: "/v1beta/models/gemini-3.1-pro-preview:generateContent", headers: {} }, snake).thinking_config, {
    keys: ["include_thoughts", "thinking_budget", "thinking_level"],
    thinkingLevel: "LOW",
    thinkingBudget: 1024,
    includeThoughts: true,
  });
  const conflicting = Buffer.from('{"generationConfig":{"thinkingConfig":{"thinkingLevel":null,"thinking_level":"LOW","thinkingBudget":0,"thinking_budget":1024,"includeThoughts":false,"include_thoughts":true}}}');
  assert.deepEqual(summarizeRequest({ method: "POST", url: "/v1beta/models/gemini-3.1-pro-preview:generateContent", headers: {} }, conflicting).thinking_config, {
    keys: ["includeThoughts", "include_thoughts", "thinkingBudget", "thinkingLevel", "thinking_budget", "thinking_level"],
    thinkingLevel: null,
    thinkingBudget: 0,
    includeThoughts: false,
  });
});



test("actual google-genai wire captures retain tier aliases, thinking settings and signed tool continuation", () => {
  const capture = captureGoogleGenai();
  assert.equal(capture.classification, "SUPPORTED_SDK_OFFLINE_WIRE_CAPTURE_NOT_PROVIDER_QA");
  assert.deepEqual(capture.client, { name: "google-genai", version: "3.0.0", actual_sdk_invoked: true });
  assert.equal(capture.provider_calls, 0);
  assert.equal(capture.transport, "httpx.MockTransport");
  assert.match(capture.request_events, /^NOT_EXERCISED:/);
  assert.equal(capture.captures.find((entry) => entry.case === "camel-level").service_tier_source, "GenerateContentConfig.service_tier");
  assert.equal(capture.captures.find((entry) => entry.case === "snake-budget").service_tier_source, "HttpOptions.extra_body");
  const cases = new Map(capture.captures.map((entry) => [entry.case, entry.requests]));
  const wireCases = new Map(capture.sdk_thinking_config_wire.map((entry) => [entry.case, entry.requests]));
  const expected = [
    ["camel-level", { serviceTier: "flex" }, { thinking_level: "LOW", include_thoughts: true }, { keys: ["include_thoughts", "thinking_level"], thinkingLevel: "LOW", thinkingBudget: null, includeThoughts: true }],
    ["snake-budget", { service_tier: "priority" }, { thinking_budget: 1024, include_thoughts: false }, { keys: ["include_thoughts", "thinking_budget"], thinkingLevel: null, thinkingBudget: 1024, includeThoughts: false }],
    ["automatic-budget", {}, { thinking_budget: -1, include_thoughts: true }, { keys: ["include_thoughts", "thinking_budget"], thinkingLevel: null, thinkingBudget: null, includeThoughts: true }],
    ["disabled-budget", {}, { thinking_budget: 0, include_thoughts: false }, { keys: ["include_thoughts", "thinking_budget"], thinkingLevel: null, thinkingBudget: 0, includeThoughts: false }],
    ["omitted-config", {}, null, null],
    ["extra-body-level", { serviceTier: "flex" }, { thinkingLevel: "LOW", includeThoughts: true }, { keys: ["includeThoughts", "thinkingLevel"], thinkingLevel: "LOW", thinkingBudget: null, includeThoughts: true }],
    ["extra-body-budget", { service_tier: "priority" }, { thinkingBudget: 1024, includeThoughts: false }, { keys: ["includeThoughts", "thinkingBudget"], thinkingLevel: null, thinkingBudget: 1024, includeThoughts: false }],
    ["extra-body-automatic-budget", {}, { thinkingBudget: -1, includeThoughts: true }, { keys: ["includeThoughts", "thinkingBudget"], thinkingLevel: null, thinkingBudget: null, includeThoughts: true }],
    ["extra-body-disabled-budget", {}, { thinkingBudget: 0, includeThoughts: false }, { keys: ["includeThoughts", "thinkingBudget"], thinkingLevel: null, thinkingBudget: 0, includeThoughts: false }],
  ];
  const expectedNames = expected.map(([name]) => name).sort();
  assert.deepEqual(capture.captures.map((entry) => entry.case).sort(), expectedNames);
  assert.deepEqual(capture.sdk_thinking_config_wire.map((entry) => entry.case).sort(), expectedNames);
  assert.equal(cases.size, expected.length);
  assert.equal(wireCases.size, expected.length);
  const signatureHash = createHash("sha256").update(Buffer.from("offline-google-genai-signature").toString("base64")).digest("hex").slice(0, 12);
  for (const [name, aliases, thinking, normalizedThinking] of expected) {
    const thinkingSource = name.startsWith("extra-body-") ? "HttpOptions.extra_body" : thinking === null ? "omitted" : "GenerateContentConfig.thinking_config";
    assert.equal(capture.thinking_config_sources[name], thinkingSource, `${name}: configuration source is explicit`);
    const requests = cases.get(name);
    assert.equal(requests.length, 3, name);
    const wireRequests = wireCases.get(name);
    assert.equal(wireRequests.length, 3, name);
    for (const [index, request] of requests.entries()) {
      const wire = wireRequests[index];
      assert.deepEqual(wire.thinking_config, thinking, `${name}: actual SDK wire casing and values`);
      assert.equal(request.body_bytes, wire.body_bytes, `${name}: exact captured byte length`);
      assert.equal(request.body_sha12, wire.body_sha12, `${name}: exact captured byte hash`);
      assert.equal(request.method_kind, "generateContent");
      assert.equal(request.user_agent_family, "google-genai-sdk");
      assert.deepEqual(request.service_tier_aliases, aliases, name);
      assert.deepEqual(request.thinking_config, normalizedThinking, `${name}: normalized observer thinking settings`);
    }
    assert.equal(requests[0].contents.function_call_parts, 0, name);
    for (const request of requests.slice(1)) {
      assert.equal(request.contents.function_call_parts, 1, name);
      assert.equal(request.contents.function_response_parts, 1, name);
      assert.equal(judgeReplayedTurns(request.contents.model_turn_calls, [signatureHash]).ok, true, name);
    }
    assert.ok(requests[2].contents.count > requests[1].contents.count, `${name}: exact signed history retained on continuation`);
  }
});
