import assert from "node:assert/strict";
import test from "node:test";
import fixtures, { clientPin, redactionSentinels } from "./fixtures.mjs";
import coverageLedger from "../client-harness/coverage-ledger.json" with { type: "json" };

import { createRedactionContext, sanitizeRecord, assertSafeArtifact } from "../client-harness/redaction.mjs";

const byId = (id) => {
  const found = fixtures.find((entry) => entry.id === `agy159-${id}`);
  assert.ok(found, `Missing fixture ${id}`);
  return found;
};
const events = (entry) => entry.response.frames.map((frame) => {
  assert.equal(frame.kind, "sse");
  assert.match(frame.data, /^data: /);
  assert.ok(frame.data.endsWith("\n\n"), "A complete SSE event needs a blank-line delimiter");
  return JSON.parse(frame.data.slice(6).trim());
});
const decodeGrpc = (frame) => {
  assert.equal(frame.kind, "grpc");
  assert.equal(frame.encoding, "base64");
  const bytes = Buffer.from(frame.data, "base64");
  assert.ok(bytes.length >= 5, "gRPC requires compression flag plus uint32 length");
  assert.equal(bytes[0], 0, "Synthetic messages use uncompressed framing");
  return { declared: bytes.readUInt32BE(1), payload: bytes.subarray(5) };
};
function stringsFromKnownProto(payload) {
  const result = [];
  let offset = 0;
  while (offset < payload.length) {
    const tag = payload[offset++];
    assert.equal(tag & 7, 2, "These synthetic protobuf fields are length-delimited");
    assert.ok(offset < payload.length);
    const length = payload[offset++];
    assert.ok(offset + length <= payload.length, "Synthetic protobuf field must be complete");
    result.push({ number: tag >> 3, value: payload.subarray(offset, offset + length).toString("utf8") });
    offset += length;
  }
  return result;
}

test("catalogue pins the installed binary and never upgrades synthetic evidence to live proof", () => {
  assert.deepEqual(clientPin, {
    name: "agy", version: "1.5.9", source_revision: null,
    binary_sha256: "fad3628ad91fe8613a72f074a1c5fde7cd816517788a5b9be96703f7d256741a",
  });
  assert.equal(new Set(fixtures.map((entry) => entry.id)).size, fixtures.length);
  const requiredFamilies = [
    ["native-sse-text", "agy:gemini-api-key-stream"],
    ["raw-envelope-sse", "agy:code_assist-predictionservice-streamgeneratecontent"],
    ["discovery", "agy:code_assist-predictionservice-fetchavailablemodels"],
    ["quota", "agy:code_assist-predictionservice-retrieveuserquota"],
    ["controls", "agy:code_assist-cloudcode-fetchadmincontrols"],
    ["remote-cancel", "agy:remote_agent-jetskiremoteagentproxyservice-cancelcascade"],
    ["signaling-grpc", "agy:remote_control-apiservice-sendsignalingmessage"],
    ["audio-framing-candidate", "agy:remote_agent-jetskiremoteagentproxyservice-streamvoice"],
  ];
  for (const [id, surfaceId] of requiredFamilies) {
    assert.ok(byId(id).surfaceIds.includes(surfaceId), `${id} must cover ${surfaceId}`);
  }
  for (const entry of fixtures) {
    assert.deepEqual(entry.client, clientPin);
    assert.ok(["synthetic", "descriptor-only"].includes(entry.provenance));
    assert.equal(entry.expected.proof, "fixture-contract-only");
    assert.equal(entry.expected.runtime_status, "NOT_RUN");
    assert.equal(entry.expected.consumer_decode, "unobserved");
    assert.equal(entry.expected.provider_success, false);
    assert.equal(entry.expected.subscription_success, false);
    assert.ok(entry.surfaceIds.length > 0);
    for (const id of entry.surfaceIds) {
      assert.ok(coverageLedger.surfaces.some((surface) => surface.cli === clientPin.name && surface.id === id),
        `${entry.id} references an unknown canonical agy surface: ${id}`);
    }
    assert.ok(entry.source_refs.length > 0);
    for (const source of entry.source_refs) {
      assert.ok(source.qualification);
      assert.ok(source.artifact?.startsWith("AgySurface/") || source.url?.startsWith("https://github.com/"));
      assert.ok(!JSON.stringify(source).includes("/tmp/"));
      if (source.kind === "binary-descriptor") assert.equal(source.binary_sha256, clientPin.binary_sha256);
      if (source.kind === "related-source-only") assert.match(source.source_revision, /^[a-f0-9]{40}$/);
    }
    assert.equal(entry.request.method, entry.id === "agy159-signaling-ws-candidate" ? "GET" : "POST");
    assert.ok(Array.isArray(entry.request.headers));
    assert.ok(Array.isArray(entry.response.headers));
    assert.ok(entry.response.frames.length > 0);
  }
});

test("fresh imports construct identical candidates without an installed CLI or environment values", async () => {
  const fresh = await import("./fixtures.mjs?deterministic-fixture-check");
  assert.deepEqual(fresh.fixtures, fixtures);
  assert.equal(fresh.default, fresh.fixtures);
});

test("actual-consumer selector targets only native Gemini SSE and its exact synthetic marker", () => {
  const selected = fixtures.filter((entry) => entry.expected.consumer);
  assert.equal(selected.length, 2);
  const entry = byId("native-sse-text");
  assert.equal(entry.id, "agy159-native-sse-text");
  assert.deepEqual(entry.surfaceIds, ["agy:gemini-api-key-stream"]);
  assert.deepEqual(entry.expected.consumer, {
    mode: "agy-print", marker: "AGY159_SYNTHETIC_CONSUMER_OK", transport: "sse",
  });
  assert.equal(entry.request.path, "/v1beta/models/gemini-3.1-pro-preview:streamGenerateContent?alt=sse");
  assert.deepEqual(entry.response.headers, [["content-type", "text/event-stream"]]);
  const [content, terminal] = events(entry);
  assert.equal(content.candidates[0].content.parts[0].text, entry.expected.consumer.marker);
  assert.equal(Object.hasOwn(content, "response"), false, "Native consumer must not get a subscription wrapper");
  assert.equal(Object.hasOwn(content, "usageMetadata"), false);
  assert.equal(terminal.candidates[0].finishReason, "STOP");
  assert.equal(terminal.usageMetadata.cachedContentTokenCount, 0);
  assert.equal(entry.response.termination, "eof");
  assert.equal(entry.source_refs[0].kind, "captured");
  assert.match(entry.source_refs[0].qualification, /failed the SSE consumer/);
});

test("gateway bearer candidate stays native and selects its model from explicit gateway models", () => {
  const entry = byId("gateway-bearer-sse");
  assert.deepEqual(entry.expected.consumer, {
    mode: "agy-print",
    marker: "AGY159_SYNTHETIC_GATEWAY_BEARER_OK",
    transport: "sse",
    authMode: "gateway-bearer",
    model: "gemini-2.0-flash",
    modelSource: "AGY_LLM_GATEWAY_MODELS",
  });
  assert.equal(entry.provenance, "synthetic");
  assert.equal(entry.native_only, true);
  assert.deepEqual(entry.surfaceIds, ["agy:gemini-api-key-stream"]);
  assert.equal(entry.request.path, `/v1beta/models/${entry.expected.consumer.model}:streamGenerateContent?alt=sse`);
  assert.deepEqual(entry.request.headers, [
    ["authorization", "Bearer synthetic-gateway-api-key"],
    ["content-type", "application/json"],
  ]);
  const native = byId("native-sse-text");
  assert.equal(Object.hasOwn(native.expected.consumer, "authMode"), false);
  assert.equal(native.request.headers.some(([name]) => name.toLowerCase() === "authorization"), false);
  assert.notEqual(native.request.path, entry.request.path);
  assert.equal(Object.hasOwn(entry.request.body, "request"), false);
  assert.equal(Object.hasOwn(entry.request.body, "project"), false);
  assert.deepEqual(entry.response.headers, [["content-type", "text/event-stream"]]);
  const [content, terminal] = events(entry);
  assert.equal(content.candidates[0].content.parts[0].text, entry.expected.consumer.marker);
  assert.equal(Object.hasOwn(content, "response"), false);
  assert.equal(terminal.candidates[0].finishReason, "STOP");
  assert.equal(entry.response.termination, "eof");
  assert.equal(entry.expected.runtime_status, "NOT_RUN");
  assert.equal(entry.expected.provider_success, false);
  assert.equal(entry.expected.subscription_success, false);
  assert.match(entry.source_refs[0].qualification, /Synthetic native-only gateway recipe/);
});

test("thought signatures and safe opaque fields retain part/event order and zero presence", () => {
  const [thinking, tool, terminal] = events(byId("native-sse-tools"));
  const thought = thinking.candidates[0].content.parts[0];
  assert.equal(thought.thought, true);
  assert.equal(thought.thoughtSignature, "c3ludGhldGljLXRoaW5raW5n");
  const part = tool.candidates[0].content.parts[0];
  assert.equal(part.thoughtSignature, "c3ludGhldGljLXNpZ25hdHVyZQ==");
  assert.deepEqual(part.futurePart, { safeUnknown: [false, 0, "preserve"] });
  assert.deepEqual(part.functionCall, { id: "synthetic-call-1", name: "fixture_lookup", args: { key: "safe", futureArg: { zero: 0 } } });
  assert.equal(Object.hasOwn(thinking, "usageMetadata"), false);
  assert.equal(Object.hasOwn(tool, "usageMetadata"), false);
  assert.deepEqual(terminal.usageMetadata.promptTokensDetails, [{ modality: "TEXT", tokenCount: 7 }]);
  assert.equal(Object.hasOwn(terminal.usageMetadata, "thoughtsTokenCount"), true);
  assert.equal(terminal.usageMetadata.thoughtsTokenCount, 0);
  assert.deepEqual(terminal.usageMetadata.futureUsage, { zero: 0 });
  assert.equal(terminal.candidates[0].finishReason, "STOP");
});

test("functionResponse correlates to its original call and preserves the thought/call history", () => {
  const entry = byId("native-function-response");
  const history = entry.request.body.contents;
  assert.deepEqual(history.map((message) => message.role), ["user", "model", "user"]);
  assert.equal(history[1].parts[0].thought, true);
  const call = history[1].parts[1];
  const reply = history[2].parts[0].functionResponse;
  assert.equal(reply.id, call.functionCall.id);
  assert.equal(reply.name, call.functionCall.name);
  assert.equal(reply.response.result, 0);
  assert.deepEqual(reply.response.futureResult, { preserved: true });
  assert.ok(call.thoughtSignature);
  assert.equal(events(entry)[0].candidates[0].finishReason, "STOP");
});

test("raw envelope keeps requestType/requestId and nested usage rather than promoting wrapper fields", () => {
  for (const id of ["raw-envelope-sse", "raw-envelope-json"]) {
    const entry = byId(id);
    assert.equal(entry.request.body.requestType, "agent");
    assert.equal(entry.request.body.requestId, "synthetic-request-159");
    assert.equal(entry.request.body.request.sessionId, "synthetic-session");
    assert.equal(Object.hasOwn(entry.expected, "consumer"), false);
    const payloads = id.endsWith("sse") ? events(entry) : entry.response.frames.map((frame) => frame.data);
    const terminal = payloads.at(-1);
    assert.equal(Object.hasOwn(terminal, "usageMetadata"), false);
    assert.equal(terminal.response.usageMetadata.cachedContentTokenCount, 0);
    assert.equal(terminal.response.candidates[0].finishReason, "STOP");
  }
  const terminal = events(byId("raw-envelope-sse")).at(-1);
  assert.deepEqual(terminal.consumedCredits, [{ creditType: "SYNTHETIC", creditAmount: "0" }]);
  assert.deepEqual(terminal.remainingCredits, []);
  assert.deepEqual(terminal.metadata.safeUnknown, { zero: 0 });
});

test("discovery, quota, and controls stay descriptor-only and preserve absence versus explicit false/zero", () => {
  for (const id of ["discovery", "quota", "controls"]) {
    const entry = byId(id);
    assert.equal(entry.provenance, "descriptor-only");
    assert.match(entry.wire_certainty, /not captured/);
    assert.equal(Object.hasOwn(entry.expected, "consumer"), false);
  }
  const models = byId("discovery").response.frames[0].data;
  assert.deepEqual(models.models, { "synthetic-model": {} });
  assert.equal(models.defaultAgentModelId, "synthetic-model");
  assert.deepEqual(models.safeUnknown, { preserve: true });
  const [zero, absent] = byId("quota").response.frames[0].data.buckets;
  assert.equal(zero.remainingAmount, "0", "Proto int64 uses a JSON string");
  assert.equal(zero.remainingFraction, 0);
  assert.equal(Object.hasOwn(absent, "remainingAmount"), false);
  assert.equal(Object.hasOwn(absent, "remainingFraction"), false);
  const controls = byId("controls").response.frames[0].data;
  assert.equal(controls.disableTelemetry, false);
  assert.equal(controls.secureModeEnabled, true);
  assert.equal(Object.hasOwn(controls, "disableFeedback"), false);
  assert.deepEqual(controls.safeUnknown, { enabled: false });
});

test("signaling gRPC framing matches all six known synthetic protobuf fields", () => {
  const entry = byId("signaling-grpc");
  const frame = { kind: "grpc", encoding: entry.request.body_encoding, data: entry.request.body };
  const decoded = decodeGrpc(frame);
  assert.equal(decoded.declared, decoded.payload.length);
  const fields = stringsFromKnownProto(decoded.payload);
  assert.deepEqual(fields.map((part) => part.number), entry.expected_protobuf.field_numbers);
  assert.deepEqual(fields.map((part) => part.value), entry.expected_protobuf.values);
  const reply = decodeGrpc(entry.response.frames[0]);
  assert.equal(reply.declared, 0);
  assert.equal(reply.payload.length, 0);
  assert.deepEqual(entry.response.trailers, [["grpc-status", "0"]]);
});

test("voice gRPC payload has exact known envelope length without inventing nested voice/audio schema", () => {
  const entry = byId("voice-grpc");
  const decoded = decodeGrpc(entry.response.frames[0]);
  assert.equal(decoded.declared, 2);
  assert.deepEqual([...decoded.payload], [0x0a, 0x00]);
  assert.deepEqual(stringsFromKnownProto(decoded.payload), [{ number: 1, value: "" }]);
  assert.equal(entry.descriptor_candidate.nested_descriptor_available, false);
  assert.equal(entry.descriptor_candidate.audio_codec_known, false);
  assert.equal(entry.descriptor_candidate.nested_type, ".exa.remote_agent_proxy_pb.VoiceServerMessage");
});

test("candidate WS and PCM data remain explicitly local rather than asserted installed routes", () => {
  const ws = byId("signaling-ws-candidate");
  assert.equal(ws.path_scope, "local-fixture-only");
  assert.equal(ws.request.method, "GET", "A local WebSocket candidate uses an HTTP Upgrade GET");
  assert.equal(ws.response.status, 101, "A local WebSocket candidate switches protocols");
  assert.equal(ws.provenance, "descriptor-only");
  assert.match(ws.transport_candidate, /does not establish transport/);
  assert.deepEqual(ws.response.frames.map((frame) => frame.kind), ["ws-text", "ws-binary"]);
  assert.equal(JSON.parse(ws.response.frames[0].data).safeUnknown.preserve, true);
  assert.equal(ws.response.close_code, 1000);
  const audio = byId("audio-framing-candidate");
  const frame = audio.response.frames[0];
  assert.equal(audio.path_scope, "local-fixture-only");
  assert.equal(audio.descriptor_candidate.installed_audio_encoding_known, false);
  assert.equal(frame.sample_format, "s16le");
  assert.equal(Buffer.from(frame.data, "base64").length, frame.sample_count * frame.channels * 2);
  assert.deepEqual([...Buffer.from(frame.data, "base64")], Array(8).fill(0));
});

test("terminal, cancellation, truncation, malformed JSON, and provider error are distinct contracts", () => {
  const cancel = byId("native-cancel");
  assert.equal(cancel.response.termination, "client-cancel");
  assert.equal(cancel.contract.cancel_after_frame, 1);
  assert.equal(cancel.contract.terminal, false);
  assert.equal(Object.hasOwn(events(cancel)[0].candidates[0], "finishReason"), false);
  const partial = byId("native-partial-json");
  assert.equal(partial.response.termination, "connection-reset");
  assert.equal(partial.response.frames[0].data.endsWith("\n\n"), false);
  assert.throws(() => JSON.parse(partial.response.frames[0].data.slice(6)), SyntaxError);
  const malformed = byId("native-malformed-json");
  assert.equal(malformed.response.frames[0].data.endsWith("\n\n"), true);
  assert.throws(() => JSON.parse(malformed.response.frames[0].data.slice(6)), SyntaxError);
  const grpc = byId("voice-malformed-length");
  const decoded = decodeGrpc(grpc.response.frames[0]);
  assert.equal(decoded.declared, grpc.contract.declared_length);
  assert.equal(decoded.payload.length, grpc.contract.available_length);
  assert.ok(decoded.declared > decoded.payload.length);
  const provider = byId("provider-error");
  assert.equal(provider.response.status, 429);
  assert.equal(provider.response.frames[0].data.error.status, "RESOURCE_EXHAUSTED");
  assert.equal(provider.response.frames[0].data.error.details[0].retryDelay, "1s");
  assert.equal(Object.hasOwn(provider.response.frames[0].data, "candidates"), false);
});

test("auth-before-body and owner negatives specify observation obligations without claiming enforcement", () => {
  const auth = byId("auth-before-body");
  assert.throws(() => JSON.parse(auth.request.body), SyntaxError);
  assert.equal(auth.contract.body_reads, 0);
  for (const operation of ["owner_lookups", "quota_lookups", "signing_calls", "provider_calls"]) {
    assert.equal(auth.contract[operation], 0, `${operation} is a zero-operation proof obligation, not runtime evidence`);
  }
  assert.equal(auth.contract.expected_status, 401);
  const owner = byId("owner-negative");
  assert.notEqual(owner.contract.session_owner, owner.contract.requesting_owner);
  assert.equal(owner.request.body.request.sessionId, "synthetic-session");
  assert.equal(owner.contract.expected_status, 403);
  for (const entry of [auth, owner]) {
    assert.equal(entry.contract.harness_negative, true);
    assert.equal(entry.contract.provider_calls, 0);
    assert.equal(entry.contract.runtime_assertion, "unobserved");
    assert.equal(entry.response.status, entry.contract.expected_status);
  }
  const cancel = byId("remote-cancel");
  assert.equal(cancel.provenance, "descriptor-only");
  assert.deepEqual(cancel.request.body.stepIndices, [0]);
  assert.equal(cancel.request.body.killBackgroundTasks, false);
  assert.match(cancel.path_template, /\{\$api_version\}/);
});

test("redaction fixture seeds every carrier while preserving duplicate headers/query and safe unknowns", () => {
  const entry = byId("redaction-carriers");
  assert.equal(entry.request.body.nested.access_token, redactionSentinels.json);
  assert.ok(entry.request.body.fileData.fileUri.endsWith(redactionSentinels.file));
  assert.deepEqual(entry.request.body.safeUnknown, { keep: [false, 0, "yes"] });
  assert.deepEqual(entry.request.headers.filter(([name]) => name === "x-safe"), [["x-safe", "one"], ["x-safe", "two"]]);
  const query = new URL(entry.request.path, "http://127.0.0.1").searchParams;
  assert.equal(query.get("access_token"), redactionSentinels.query);
  assert.deepEqual(query.getAll("tag"), ["one", "two"]);
  assert.equal(entry.request.headers.find(([name]) => name === "authorization")[1], `Bearer ${redactionSentinels.header}`);
  assert.equal(JSON.parse(entry.response.frames[1].data).authorization, redactionSentinels.ws);
  const grpc = decodeGrpc(entry.response.frames[2]);
  assert.equal(grpc.declared, grpc.payload.length);
  assert.deepEqual(stringsFromKnownProto(grpc.payload), [{ number: 1, value: redactionSentinels.grpc }]);
  assert.equal(Buffer.from(entry.response.frames[3].data, "base64").toString(), redactionSentinels.binary);
  assert.equal(Buffer.from(entry.response.frames[4].data, "base64").toString(), redactionSentinels.audio);
  assert.equal(entry.contract.persistence, "redact-or-reject-before-write");
  assert.equal(entry.contract.opaque_live_content, "metadata-only-or-schema-aware-reconstruction");
});

test("persistence sanitization preserves the native synthetic SSE consumer marker and terminal schema", () => {
  const original = byId("native-sse-text");
  const retained = sanitizeRecord({ provenance: "synthetic", fixture: original },
    createRedactionContext({ scenarioId: original.id })).fixture;
  const [content, terminal] = events(retained);
  assert.equal(content.candidates[0].content.role, "model");
  assert.equal(content.candidates[0].content.parts[0].text, original.expected.consumer.marker);
  assert.equal(terminal.candidates[0].finishReason, "STOP");
  assert.equal(terminal.usageMetadata.cachedContentTokenCount, 0);
  assert.equal(assertSafeArtifact(Buffer.from(JSON.stringify(retained)), {
    sentinels: Object.values(redactionSentinels),
    allowedSyntheticTokens: [original.expected.consumer.marker],
  }), true);
});

test("structured synthetic unknowns and duplicate safe fields survive the persistence boundary", () => {
  const original = {
    provenance: "synthetic",
    safeUnknown: { keep: [false, 0, "yes"] },
    nested: { access_token: redactionSentinels.json },
    headers: [["authorization", `Bearer ${redactionSentinels.header}`], ["x-safe", "one"], ["x-safe", "two"]],
    query: [["access_token", redactionSentinels.query], ["tag", "one"], ["tag", "two"]],
  };
  assert.throws(() => assertSafeArtifact(Buffer.from(JSON.stringify(original)), {
    sentinels: Object.values(redactionSentinels),
  }), { message: "unsafe artifact rejected" });
  const retained = sanitizeRecord(original, createRedactionContext({ scenarioId: "agy159-safe-unknowns" }));
  assert.deepEqual(retained.safeUnknown, original.safeUnknown);
  assert.notEqual(retained.nested.access_token, redactionSentinels.json);
  assert.deepEqual(retained.headers.filter(([name]) => name === "x-safe"), [["x-safe", "one"], ["x-safe", "two"]]);
  assert.deepEqual(retained.query.filter(([name]) => name === "tag"), [["tag", "one"], ["tag", "two"]]);
  assert.notEqual(retained.query[0][1], redactionSentinels.query);
  assert.equal(assertSafeArtifact(Buffer.from(JSON.stringify(retained)), {
    sentinels: Object.values(redactionSentinels),
  }), true);
});

test("encoded replay frames explicitly declare authored synthetic content", () => {
  const frames = fixtures.flatMap((entry) => entry.response.frames)
    .filter((frame) => frame.encoding === "base64");
  assert.ok(frames.length > 0);
  for (const frame of frames) {
    assert.equal(frame.syntheticContent, true);
    assert.equal(Buffer.from(frame.data, "base64").toString("base64"), frame.data);
  }
});
