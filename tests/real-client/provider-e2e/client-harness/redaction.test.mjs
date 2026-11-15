import assert from "node:assert/strict";
import test from "node:test";
import {
  REDACTED,
  assertSafeArtifact,
  createRedactionContext,
  reconstructLogicalMessages,
  sanitizeHeaders,
  sanitizeQuery,
  sanitizeRecord,
  sanitizeUrl,
} from "./redaction.mjs";

const context = (scenarioId = "redaction-contract") => createRedactionContext({ scenarioId, policyVersion: "test-v1" });
const artifact = (record) => Buffer.from(JSON.stringify(record));
const sentinel = (suffix) => `QH_PLANTED_PRIVATE_${suffix}_ONLY`;
const assertRejected = (bytes, sentinels = []) => {
  assert.throws(() => assertSafeArtifact(bytes, { sentinels }), (error) => {
    assert.equal(error.message, "unsafe artifact rejected");
    for (const value of sentinels) assert.equal(error.message.includes(value), false);
    return true;
  });
};

// These tests assert synthetic infrastructure safety, never provider success.
test("QH-RED-001 nested credentials and encoded aliases are removed before the sink", () => {
  const values = {
    token: sentinel("TOKEN"), cookie: sentinel("COOKIE"), oauth: sentinel("OAUTH"),
    enrollment: sentinel("ENROLLMENT"), mint: sentinel("MINT"), principal: sentinel("PRINCIPAL"),
  };
  const record = {
    provenance: "synthetic",
    safeUnknown: { keep: "SAFE_SYNTHETIC_EXTENSION", number: 7 },
    aliasBeforeCredential: values.token,
    base64Alias: Buffer.from(values.token).toString("base64"),
    escaped: JSON.stringify({ Api_Key: values.token, safe: "SAFE_JSON" }),
    urlEncoded: encodeURIComponent(JSON.stringify({ refreshToken: values.token, safe: "SAFE_URL_JSON" })),
    base64Json: Buffer.from(JSON.stringify({ access_token: values.token, safe: "SAFE_B64_JSON" })).toString("base64"),
    expected: { nested: { bearer: values.token, safe: "SAFE_EXPECTED" } },
    nested: { a: [{ Api_Key: values.token, SET_COOKIE: `sid=${values.cookie}`, OAuth: { code: values.oauth }, enrollmentCode: values.enrollment, mintToken: values.mint, principal_id: values.principal }] },
  };
  assertRejected(artifact(record), Object.values(values));
  const clean = sanitizeRecord(record, context());
  assert.deepEqual(clean.safeUnknown, record.safeUnknown);
  assert.equal(clean.aliasBeforeCredential, REDACTED);
  assert.equal(clean.expected.nested.bearer, REDACTED);
  assert.equal(JSON.parse(clean.escaped).Api_Key, REDACTED);
  assert.equal(JSON.parse(decodeURIComponent(clean.urlEncoded)).refreshToken, REDACTED);
  assert.equal(JSON.parse(Buffer.from(clean.base64Json, "base64")).access_token, REDACTED);
  assert.equal(clean.nested.a[0].OAuth, REDACTED);
  assert.equal(clean.nested.a[0].enrollmentCode, REDACTED);
  assert.equal(clean.nested.a[0].mintToken, REDACTED);
  assert.match(clean.nested.a[0].principal_id, /^syn_principal_/);
  assert.equal(assertSafeArtifact(artifact(clean), { sentinels: Object.values(values) }), true);
  assert.equal(record.aliasBeforeCredential, values.token, "sanitization must not mutate the source");
});

test("QH-RED-002 ordered duplicates, query capabilities and signed URL userinfo/path are sanitized", () => {
  const secret = sentinel("SIGNED_CAPABILITY");
  const user = sentinel("URL_USER");
  const password = sentinel("URL_PASSWORD");
  const account = sentinel("ACCOUNT");
  const ctx = context();
  const source = {
    provenance: "synthetic",
    headers: [["X-Order", "one"], ["Authorization", `Bearer ${secret}`], ["X-Order", "two"], ["Cookie", `sid=${secret}`]],
    query: [["same", "first"], ["Signature", encodeURIComponent(secret)], ["same", "second"], ["account_id", account]],
    signedURL: `https://${user}:${password}@private.invalid/accounts/${account}?same=first&X-Amz-Signature=${encodeURIComponent(secret)}&same=second#${secret}`,
    encodedCapability: encodeURIComponent(`https://private.invalid/projects/${account}?token=${secret}`),
  };
  const clean = sanitizeRecord(source, ctx);
  assert.deepEqual(clean.headers.map(([key]) => key), source.headers.map(([key]) => key));
  assert.deepEqual(clean.headers.filter(([key]) => key === "X-Order"), [["X-Order", "one"], ["X-Order", "two"]]);
  assert.deepEqual(clean.query.slice(0, 3), [["same", "first"], ["Signature", REDACTED], ["same", "second"]]);
  const url = new URL(clean.signedURL);
  assert.equal(url.username, "");
  assert.equal(url.password, "");
  assert.equal(url.hash, "");
  assert.deepEqual(url.searchParams.getAll("same"), ["first", "second"]);
  assert.equal(url.searchParams.get("X-Amz-Signature"), REDACTED);
  assert.equal(url.pathname.split("/").at(-1), clean.query[3][1]);
  assert.equal(assertSafeArtifact(artifact(clean), { sentinels: [secret, user, password, account] }), true);
  assert.equal(new URL(sanitizeUrl(source.signedURL, ctx)).host, "capture.invalid");
  assert.deepEqual(sanitizeHeaders([["x-test", "unknown-captured"]], ctx), [["x-test", REDACTED]]);
  assert.deepEqual(sanitizeQuery([["x-test", "unknown-captured"]], ctx), [["x-test", REDACTED]]);
  const privateName = sentinel("PRIVATE_CAPABILITY_NAME");
  const captured = sanitizeRecord({ headers: [[privateName, ""], ["x-order", "one"], [privateName, ""]], query: [[privateName, ""], ["same", "safe"], [privateName, ""]] }, ctx);
  assert.equal(JSON.stringify(captured).includes(privateName), false);
  assert.match(captured.headers[0][0], /^x-redacted-[a-f0-9]{16}$/);
  assert.equal(captured.headers[0][0], captured.headers[2][0]);
  assert.equal(captured.headers[1][0], "x-order");
  assert.match(captured.query[0][0], /^redacted_query_[a-f0-9]{16}$/);
  assert.equal(captured.query[0][0], captured.query[2][0]);
  assert.equal(assertSafeArtifact(artifact(captured)), true);
  assertRejected(artifact({ provenance: "captured", headers: [[privateName, ""]] }));
  assertRejected(artifact({ provenance: "captured", query: [[privateName, ""]] }));
});

test("QH-RED-004 authored synthetic canonical base64 metadata survives while captured metadata stays opaque", () => {
  const headers = [["x-private-bin", "AAEC/w=="], ["x-private-bin", "AgM="]];
  const authored = sanitizeRecord({ provenance: "synthetic", request: { headers } }, context());
  assert.deepEqual(authored.request.headers, headers);
  assert.equal(assertSafeArtifact(artifact(authored)), true);
  const captured = sanitizeRecord({ request: { headers } }, context());
  assert.deepEqual(captured.request.headers, headers.map(([name, value]) => [name, { kind: "grpc-metadata-bin", byteLength: Buffer.byteLength(value) }]));
  assert.equal(assertSafeArtifact(artifact(captured)), true);
});

test("QH-RED-004 authored binary metadata still honors secret discovery, canonical encoding and sentinel guards", () => {
  const secret = sentinel("AUTHORED_METADATA");
  const encodedSecret = Buffer.from(secret).toString("base64");
  const structuredSecret = Buffer.from(JSON.stringify({ password: secret })).toString("base64");
  const clean = sanitizeRecord({
    provenance: "synthetic",
    headers: [
      ["x-private-bin", encodedSecret],
      ["x-private-bin", structuredSecret],
      ["x-token-bin", "AAEC/w=="],
      ["authorization", `Bearer ${secret}`],
      ["x-private-bin", "AgM"],
      ["x-private-bin", "AgM=\n"],
      ["x-private-bin", "!!!!"],
    ],
  }, context());
  assert.equal(clean.headers[0][1].kind, "grpc-metadata-bin");
  assert.equal(clean.headers[1][1].kind, "grpc-metadata-bin");
  assert.equal(clean.headers[2][1], REDACTED, "secret-key headers stay redacted even when their value is safe base64");
  assert.equal(clean.headers[3][1], REDACTED);
  for (const [, value] of clean.headers.slice(4)) assert.equal(value.kind, "grpc-metadata-bin");
  assert.equal(assertSafeArtifact(artifact(clean), { sentinels: [secret] }), true);
  const unknown = sentinel("UNKNOWN_METADATA");
  const guarded = sanitizeRecord({ provenance: "synthetic", headers: [["x-private-bin", Buffer.from(unknown).toString("base64")]] }, context());
  assertRejected(artifact(guarded), [unknown]);
});

test("QH-RED-003 complete split SSE/WS logical messages are sanitized and remain consumer-decodable", () => {
  const secret = sentinel("SPLIT_SECRET");
  const first = `event: message\ndata: ${JSON.stringify({ type: "message", extension: "SAFE_SSE_EXTENSION", candidates: [{ content: { role: "model", parts: [{ text: "SAFE_CONSUMER_MARKER" }] } }], access_token: secret })}\n\n`;
  const ws = JSON.stringify({ type: "message", result: "SAFE_WS_MARKER", password: secret });
  const split = first.indexOf(secret) + 9;
  const frames = [
    { kind: "sse", messageId: "sse-1", final: false, data: first.slice(0, split) },
    { kind: "sse", messageId: "sse-1", final: true, data: first.slice(split) },
    { kind: "ws-text", final: false, data: ws.slice(0, 15) },
    { kind: "ws-text", final: true, data: ws.slice(15) },
    { kind: "sse", data: "data: [DONE]\n\n" },
  ];
  assert.equal(reconstructLogicalMessages(frames).length, 3);
  const clean = sanitizeRecord({ provenance: "synthetic", response: { frames, termination: "terminal" } }, context());
  const persisted = artifact(clean);
  assert.equal(assertSafeArtifact(persisted, { sentinels: [secret] }), true);
  const restored = JSON.parse(persisted);
  assert.equal(restored.response.frames.length, frames.length);
  assert.deepEqual(restored.response.frames.map((frame) => frame.final), frames.map((frame) => frame.final));
  const messages = reconstructLogicalMessages(restored.response.frames);
  assert.deepEqual(messages.map((frame) => frame.kind), ["sse", "ws-text", "sse"]);
  const event = JSON.parse(messages[0].data.split("\n").find((line) => line.startsWith("data: ")).slice(6));
  assert.equal(event.candidates[0].content.parts[0].text, "SAFE_CONSUMER_MARKER");
  assert.equal(event.extension, "SAFE_SSE_EXTENSION");
  assert.equal(event.access_token, REDACTED);
  assert.equal(JSON.parse(messages[1].data).result, "SAFE_WS_MARKER");
  assert.equal(JSON.parse(messages[1].data).password, REDACTED);
  assert.equal(messages[2].data, "data: [DONE]\n\n");
  for (const frame of restored.response.frames) assert.equal(frame.byteLength, Buffer.byteLength(frame.data));
  const partial = sanitizeRecord({ response: { frames: [{ kind: "ws-text", final: false, data: `{"unknown":"${secret}` }] } }, context());
  assert.equal(assertSafeArtifact(artifact(partial), { sentinels: [secret] }), true);
});

test("QH-RED-003 synthetic physical framing survives persistence separately from the logical view", () => {
  const body = JSON.stringify({ type: "message", result: "SAFE_PHYSICAL_MARKER" });
  const frames = [
    { kind: "ws-text", messageId: "physical-1", opcode: 1, final: false, chunkSizes: [1, 2], data: body.slice(0, 12) },
    { kind: "ws-ping", opcode: 9, final: true, data: "ping" },
    { kind: "ws-continuation", messageId: "physical-1", opcode: 0, continuation: true, final: true, chunkSizes: [3, 1], data: body.slice(12) },
  ];
  const clean = sanitizeRecord({ provenance: "synthetic", response: { frames } }, context());
  const restored = JSON.parse(artifact(clean));
  assert.equal(restored.response.frames.length, 3);
  restored.response.frames.forEach((frame, index) => {
    const { byteLength, ...physical } = frame;
    assert.deepEqual(physical, frames[index]);
    assert.equal(byteLength, Buffer.byteLength(frame.data));
  });
  assert.equal(JSON.parse(reconstructLogicalMessages(restored.response.frames)[0].data).result, "SAFE_PHYSICAL_MARKER");
  const secret = sentinel("PHYSICAL_SPLIT_SECRET");
  const unsafe = JSON.stringify({ password: secret, safe: "SAFE_REWRITTEN_MARKER" });
  const split = unsafe.indexOf(secret) + 8;
  const fragmented = [
    { kind: "ws-text", opcode: 1, final: false, data: unsafe.slice(0, split) },
    { kind: "ws-continuation", opcode: 0, final: true, data: unsafe.slice(split) },
  ];
  assertRejected(artifact({ provenance: "synthetic", response: { frames: fragmented } }), [secret]);
  const rewritten = sanitizeRecord({ provenance: "synthetic", response: { frames: fragmented } }, context());
  assert.equal(rewritten.response.frames.length, 2);
  assert.deepEqual(rewritten.response.frames.map((frame) => frame.opcode), [1, 0]);
  assert.deepEqual(rewritten.response.frames.map((frame) => frame.final), [false, true]);
  assert.equal(assertSafeArtifact(artifact(rewritten), { sentinels: [secret] }), true);
  const decoded = JSON.parse(reconstructLogicalMessages(rewritten.response.frames)[0].data);
  assert.deepEqual(decoded, { password: REDACTED, safe: "SAFE_REWRITTEN_MARKER" });
  fragmented[0].chunkSizes = [1, 2];
  assert.throws(() => sanitizeRecord({ provenance: "synthetic", response: { frames: fragmented } }, context()), { message: "record sanitization failed" });
});

test("QH-RED-004 gRPC ordered metadata and opaque framing never retain captured bytes", () => {
  const secret = sentinel("GRPC_BINARY");
  const bytes = Buffer.concat([Buffer.from([0, 0, 0, 0, Buffer.byteLength(secret)]), Buffer.from(secret)]);
  const clean = sanitizeRecord({ response: { frames: [{ kind: "grpc", metadata: [["x-order", "one"], ["authorization", secret], ["x-private-bin", bytes.toString("base64")], ["x-order", "two"]], data: bytes }] } }, context());
  const frame = clean.response.frames[0];
  assert.deepEqual(frame.metadata.map(([name]) => name), ["x-order", "authorization", "x-private-bin", "x-order"]);
  assert.equal(frame.metadata[1][1], REDACTED);
  assert.equal(frame.metadata[2][1].kind, "grpc-metadata-bin");
  assert.equal(frame.data.kind, "grpc");
  assert.equal(frame.data.byteLength, bytes.length);
  assert.equal(assertSafeArtifact(artifact(clean), { sentinels: [secret] }), true);
  const generated = Buffer.from([0, 0, 0, 0, 0]).toString("base64");
  const fixture = sanitizeRecord({ provenance: "synthetic", response: { frames: [{ kind: "grpc", encoding: "base64", syntheticContent: true, data: generated }] } }, context());
  assert.equal(fixture.response.frames[0].data, generated);
  const notDeclared = sanitizeRecord({ provenance: "synthetic", response: { frames: [{ kind: "grpc", encoding: "base64", data: generated }] } }, context());
  assert.equal(notDeclared.response.frames[0].data.kind, "grpc");
});

test("QH-RED-005 unknown captured payload and file/audio/binary content are metadata-only without content hashes", () => {
  const secret = sentinel("OPAQUE_CONTENT");
  for (const provenance of ["captured", "synthetic"]) {
    const clean = sanitizeRecord({
      provenance,
      file: { name: secret, content: secret, sha256: secret, contentType: "application/octet-stream" },
      audio: { data: Buffer.from(secret).toString("base64"), hash: secret, contentType: "audio/wav" },
      binary: Buffer.from(secret), rawContent: secret, content_hash: secret,
      response: { frames: [{ kind: "audio", data: secret }, { kind: "ws-binary", data: Buffer.from(secret).toString("base64") }] },
      maliciousUnknown: { innocentLabel: secret },
    }, context());
    assert.equal(clean.file.kind, "file");
    assert.equal(clean.audio.contentType, "audio/wav");
    assert.equal(clean.binary.byteLength, Buffer.byteLength(secret));
    assert.equal("sha256" in clean.file, false);
    assert.equal("hash" in clean.audio, false);
    if (provenance === "captured") {
      assert.equal("maliciousUnknown" in clean, false);
      assert.equal(assertSafeArtifact(artifact(clean), { sentinels: [secret] }), true);
    } else {
      assert.equal(clean.maliciousUnknown.innocentLabel, secret, "synthetic unknowns are retained, not silently claimed safe");
      assertRejected(artifact(clean), [secret]);
      delete clean.maliciousUnknown;
      assert.equal(assertSafeArtifact(artifact(clean), { sentinels: [secret] }), true);
    }
  }
  for (const key of ["rawContent", "content_hash", "filePath", "audioTranscript"]) {
    const clean = sanitizeRecord({ provenance: "captured", [key]: secret }, context());
    assert.deepEqual(clean[key], { kind: key, byteLength: Buffer.byteLength(secret) });
    assert.equal(assertSafeArtifact(artifact(clean), { sentinels: [secret] }), true);
    clean[key].unexpectedOpaqueField = "unregistered private content";
    assertRejected(artifact(clean));
  }
  const raw = { provenance: "captured", harmlessLookingUnknown: secret, body: secret, stdout: secret };
  assertRejected(artifact(raw), [secret]);
  const clean = sanitizeRecord(raw, context());
  assert.deepEqual(clean.body, { kind: "body", byteLength: Buffer.byteLength(secret) });
  assert.equal(assertSafeArtifact(artifact(clean), { sentinels: [secret] }), true);
});

test("QH-RED-005 captured request body containers share the sanitizer/persistence schema", () => {
  const secret = sentinel("CAPTURED_REQUEST_BODY");
  const headerOnly = sanitizeRecord({
    provenance: "captured", method: "POST", path: "/v1/responses",
    headers: [["content-type", "application/json"], ["x-order", "first"], ["x-order", "second"]],
  }, context());
  assert.equal("body" in headerOnly, false);
  assert.equal(headerOnly.method, "POST");
  assert.deepEqual(headerOnly.headers.map(([name]) => name), ["content-type", "x-order", "x-order"]);
  assert.equal(assertSafeArtifact(artifact(headerOnly)), true);
  const rootBody = sanitizeRecord({ provenance: "captured", method: "POST", path: "/v1/responses", headers: [], body: secret }, context());
  assert.deepEqual(rootBody.body, { kind: "body", byteLength: Buffer.byteLength(secret) });
  assert.equal(assertSafeArtifact(artifact(rootBody), { sentinels: [secret] }), true);
  const raw = {
    request: { method: "POST", path: "/v1/responses", body: secret },
    response: { status: 200, body: JSON.stringify({ type: "message", password: secret }), payload: secret, content: secret },
    arbitraryUnknown: "unregistered opaque input",
  };
  const clean = sanitizeRecord(raw, context());
  assert.equal(clean.provenance, "captured");
  assert.deepEqual(clean.request.body, { kind: "body", byteLength: Buffer.byteLength(secret) });
  assert.deepEqual(clean.response.body, { type: "message", password: REDACTED });
  assert.deepEqual(clean.response.payload, { kind: "payload", byteLength: Buffer.byteLength(secret) });
  assert.deepEqual(clean.response.content, { kind: "content", byteLength: Buffer.byteLength(secret) });
  assert.equal("arbitraryUnknown" in clean, false);
  assert.equal(assertSafeArtifact(artifact(clean), { sentinels: [secret] }), true);
  const injected = JSON.parse(artifact(clean));
  injected.request.arbitraryUnknown = "unregistered opaque input";
  assertRejected(artifact(injected));
});

test("QH-RED-006 identities correlate by namespace within epoch but never expose the memory-only map", () => {
  const value = sentinel("IDENTITY");
  const ctx = context("epoch-test");
  const source = { provenance: "synthetic", account_id: value, workspace_id: value, nested: { accountId: value }, headers: [["x-account-id", value]], query: [["account_id", value]], path: `/accounts/${value}` };
  const clean = sanitizeRecord(source, ctx);
  assert.equal(clean.account_id, clean.nested.accountId);
  assert.equal(clean.account_id, clean.headers[0][1]);
  assert.equal(clean.account_id, clean.query[0][1]);
  assert.equal(clean.account_id, clean.path.split("/").at(-1));
  assert.notEqual(clean.account_id, clean.workspace_id);
  assert.equal(sanitizeRecord(source, ctx).account_id, clean.account_id);
  assert.notEqual(sanitizeRecord(source, context("epoch-test")).account_id, clean.account_id);
  assert.notEqual(sanitizeRecord(source, context("different-scenario")).account_id, clean.account_id);
  assert.deepEqual(Object.keys(ctx).sort(), ["policyVersion", "scenarioId"]);
  assert.equal(JSON.stringify(ctx).includes(value), false);
  const alias = sanitizeRecord({ provenance: "synthetic", echoBeforeId: value, account_id: value }, ctx);
  assert.equal(alias.echoBeforeId, clean.account_id);
  assert.equal(sanitizeRecord(clean, ctx).account_id, clean.account_id);
  assert.equal(assertSafeArtifact(artifact(clean), { sentinels: [value] }), true);
});

test("QH-RED-007 persistence guard rejects raw/bypassed/encoded mutations with static errors and accepts sanitized control", () => {
  const secret = sentinel("MUTATION");
  const mutations = [
    secret,
    JSON.stringify({ unknown: secret }),
    JSON.stringify({ unknown: encodeURIComponent(encodeURIComponent(secret + "/?&")) }),
    JSON.stringify({ unknown: Buffer.from(`opaque-prefix:${secret}:suffix`).toString("base64") }),
    JSON.stringify({ unknown: Buffer.from(secret).toString("base64url") }),
    `{ "unknown": "${[...secret].map((char) => `\\u${char.charCodeAt(0).toString(16).padStart(4, "0")}`).join("")}" }`,
    JSON.stringify({ unknown: [...secret].map((char) => `%${char.charCodeAt(0).toString(16)}`).join("") }),
    JSON.stringify({ provenance: "captured", arbitraryUnknown: "unregistered private text" }),
    JSON.stringify({ authorization: "unregistered private credential" }),
    JSON.stringify({ file: "unregistered private file bytes" }),
  ];
  for (const bytes of mutations) assertRejected(bytes, [secret]);
  const clean = sanitizeRecord({ provenance: "synthetic", expected: { access_token: secret }, response: { body: JSON.stringify({ safe: "SAFE_JSON_MARKER", password: secret }), headers: [["content-length", "99999"]] } }, context());
  const restored = JSON.parse(artifact(clean));
  assert.equal(JSON.parse(restored.response.body).safe, "SAFE_JSON_MARKER");
  assert.equal(restored.response.headers[0][1], String(Buffer.byteLength(restored.response.body)));
  assert.equal(assertSafeArtifact(artifact(clean), { sentinels: [secret] }), true);
  assert.equal(assertSafeArtifact("SAFE_SYNTHETIC_MARKER", { sentinels: ["SAFE_SYNTHETIC_MARKER"], allowedSyntheticTokens: ["SAFE_SYNTHETIC_MARKER"] }), true);
  assert.equal(assertSafeArtifact(artifact({
    provenance: "synthetic",
    originalIssueRefs: ["codex-auth-base-contract", "codex-responses-stateful-transport"],
    surfaceIds: ["cli:codex-auth-base-contract", "cli:codex-response-stream"],
    requiredClaims: ["auth-before-body", "wire-decoder"],
  })), true);
  for (const key of ["headers", "query", "metadata"]) {
    assertRejected(artifact({ provenance: "synthetic", [key]: [["authorization", "unregistered credential"]] }));
    assertRejected(artifact({ provenance: "synthetic", [key]: [[[["authorization", "unregistered credential"]]]] }));
  }
  const tampered = JSON.parse(artifact(clean));
  tampered.unexpectedEcho = secret;
  assertRejected(artifact(tampered), [secret]);
  const cycle = { provenance: "synthetic" };
  cycle[secret] = cycle;
  assert.throws(() => sanitizeRecord(cycle, context()), { message: "record sanitization failed" });
  const hostile = { get provenance() { throw new Error(secret); } };
  assert.throws(() => sanitizeRecord(hostile, context()), { message: "record sanitization failed" });
});

test("QH-RED-007 qaccept-action-v1 preserves only exact execution metadata without promoting acceptance", () => {
  const record = {
    provenance: "captured", schema: "qaccept-action-v1", action: "C005", status: "PASS",
    consumer: { status: "PASS", reason: "QACCEPT_ACTION_OBSERVED", completed: true, tool_count: 1, structured_match: true, more: false, result_count: 2 },
    client: { name: "codex", version: "0.169.0", source_revision: "a".repeat(40), binary_sha256: "b".repeat(64) },
    expected_upstream_kind: "openai_oauth", observed_upstream_kind: null,
    wireEvidence: "NOT-OBSERVED", liveFeatureAcceptance: "BLOCKED",
    runtimeAcceptance: "NOT-RUN", providerAcceptance: "NOT-RUN", fullAcceptance: "NOT-RUN",
    fullClientAcceptance: false, closes_acceptance: false,
  };
  const clean = sanitizeRecord(record, context());
  assert.deepEqual(clean, record);
  assert.notEqual(clean.consumer, record.consumer);
  assert.notEqual(clean.client, record.client);
  assert.equal(assertSafeArtifact(artifact(clean)), true);
  assert.equal(clean.provenance, "captured");
  assert.equal(clean.fullClientAcceptance, false);
  assert.equal(clean.liveFeatureAcceptance, "BLOCKED");
  for (const action of ["C005", "C004", "C054", "A002", "A006", null]) {
    for (const status of ["PASS", "FAIL", "BLOCKED"]) {
      const candidate = { ...record, action, status, consumer: { status, reason: "QACCEPT_INVALID_ENDPOINT" } };
      delete candidate.client;
      candidate.expected_upstream_kind = null;
      assert.deepEqual(sanitizeRecord(candidate, context()), candidate);
      assert.equal(assertSafeArtifact(artifact(candidate)), true);
    }
  }
  const noRevision = structuredClone(record);
  delete noRevision.client.source_revision;
  noRevision.client.name = "agy";
  noRevision.client.version = "1.5.9";
  noRevision.expected_upstream_kind = "gemini_api_key";
  assert.deepEqual(sanitizeRecord(noRevision, context()), noRevision);
  assert.equal(assertSafeArtifact(artifact(noRevision)), true);
  for (const client_transport of ["gemini-api-key-native", "enterprise-gateway-bearer-native"]) {
    for (const suffix of ["ISOLATION_OS_SANDBOX_UNAVAILABLE", "ISOLATION_SANDBOX_PREFLIGHT_FAILED", "ISOLATION_LOOPBACK_SCOPE_NOT_ENFORCED", "ISOLATION_NOT_READY"]) {
      const candidate = {
        ...noRevision, action: "A006", client_transport, status: "BLOCKED",
        consumer: { status: "BLOCKED", reason: `QACCEPT_${suffix}` },
      };
      assert.deepEqual(sanitizeRecord(candidate, context()), candidate);
      assert.equal(assertSafeArtifact(artifact(candidate)), true);
      assert.equal(candidate.runtimeAcceptance, "NOT-RUN");
      assert.equal(candidate.fullClientAcceptance, false);
    }
  }
  const mutations = [
    (value) => { value.provenance = "synthetic"; },
    (value) => { value.prompt = "unregistered private prompt"; },
    (value) => { value.action = "C999"; },
    (value) => { value.status = "SUCCESS"; },
    (value) => { value.consumer.reason = "QACCEPT_PRIVATE_ARBITRARY_STRING"; },
    (value) => { value.consumer.reason = "QACCEPT_ISOLATION_OS_SANDBOX_UNAVAILABLE_EXTRA"; },
    (value) => { value.client_transport = "unknown-private-transport"; },
    (value) => { value.client_transport = null; },
    (value) => { value.client_transport = { redirect: "private-redirect-not-permitted" }; },
    (value) => { value.consumer.rawOutput = "unregistered private output"; },
    (value) => { value.consumer.status = "BLOCKED"; },
    (value) => { value.consumer.completed = "true"; },
    (value) => { value.consumer.tool_count = 10001; },
    (value) => { value.consumer.result_count = -1; },
    (value) => { value.consumer.result_count = 1.5; },
    (value) => { value.client.name = "unknown-client"; },
    (value) => { value.client.version = "0.169.0-private-suffix"; },
    (value) => { value.client.source_revision = null; },
    (value) => { value.client.binary_sha256 = "private raw file"; },
    (value) => { value.client.account_id = "unregistered private account"; },
    (value) => { value.expected_upstream_kind = "private-provider"; },
    (value) => { value.observed_upstream_kind = "openai_oauth"; },
    (value) => { value.wireEvidence = "PASS"; },
    (value) => { value.liveFeatureAcceptance = "PASS"; },
    (value) => { value.runtimeAcceptance = "PASS"; },
    (value) => { value.providerAcceptance = "PASS"; },
    (value) => { value.fullAcceptance = "PASS"; },
    (value) => { value.fullClientAcceptance = true; },
    (value) => { value.closes_acceptance = true; },
    (value) => { delete value.wireEvidence; },
  ];
  for (const mutate of mutations) {
    const candidate = structuredClone(record);
    mutate(candidate);
    assert.throws(() => sanitizeRecord(candidate, context()), { message: "record sanitization failed" });
    assertRejected(artifact(candidate));
  }
});
