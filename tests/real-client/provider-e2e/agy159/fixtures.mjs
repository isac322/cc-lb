// These candidates describe local fixture contracts, never successful provider turns.
export const clientPin = Object.freeze({
  name: "agy",
  version: "1.5.9",
  source_revision: null,
  binary_sha256: "fad3628ad91fe8613a72f074a1c5fde7cd816517788a5b9be96703f7d256741a",
});

const research = "AgySurface/";
const descriptor = (file, message) => ({
  kind: "binary-descriptor",
  artifact: `${research}binary-descriptors.json`,
  file,
  message,
  binary_sha256: clientPin.binary_sha256,
  qualification: "Installed descriptor; runtime reachability and wire encoding unobserved",
});
const prediction = (message) => descriptor("google/internal/cloud/code/v1internal/prediction_service.proto", message);
const geminiParts = [
  descriptor("google/cloud/aiplatform/master/content.proto", "Part.thought / Part.thoughtSignature / Part.functionCall / Part.functionResponse"),
  descriptor("google/cloud/aiplatform/master/tool.proto", "FunctionCall / FunctionResponse"),
];
const remote = (message) => descriptor("google/gca/aicode/v1main/remote_agent_proxy.proto", message);
const signaling = (message) => descriptor("devtools/jetski/boq/api/proto/api.proto", message);
const related = {
  kind: "related-source-only",
  url: "https://github.com/router-for-me/CLIProxyAPI/blob/43806c865da7d47f9252316e95e315ad9f885b84/internal/translator/antigravity/gemini/antigravity_gemini_response.go#L27-L99",
  source_revision: "43806c865da7d47f9252316e95e315ad9f885b84",
  qualification: "Compatible executor, not installed agy source or captured response",
};
const nativeCapture = {
  kind: "captured",
  artifact: `${research}local-capture-loopback.json`,
  selection: "requests[0].method/path and request JSON field names only",
  qualification: "Dummy-key local request captured; JSON response failed the SSE consumer; no successful turn",
};
const surface = (id) => `agy:${id}`;
const direct = surface("gemini-api-key-stream");
const stream = surface("code_assist-predictionservice-streamgeneratecontent");
const voice = surface("remote_agent-jetskiremoteagentproxyservice-streamvoice");
const signal = surface("remote_control-apiservice-sendsignalingmessage");
const headers = [["content-type", "application/json"]];
const json = (data) => ({ kind: "json", data });
const sse = (data) => ({ kind: "sse", data: `data: ${JSON.stringify(data)}\n\n` });
const response = (frames, extra = {}) => ({
  status: 200,
  headers: [["content-type", frames[0]?.kind === "sse" ? "text/event-stream" : "application/json"]],
  frames,
  termination: "eof",
  ...extra,
});
const nativeRequest = () => ({
  method: "POST",
  path: "/v1beta/models/gemini-3.1-pro-preview:streamGenerateContent?alt=sse",
  headers,
  body: { contents: [{ role: "user", parts: [{ text: "Reply with the local synthetic marker." }] }] },
});
const rawBody = () => ({
  project: "synthetic-project",
  model: "synthetic-model",
  requestId: "synthetic-request-159",
  requestType: "agent",
  request: {
    sessionId: "synthetic-session",
    contents: [{ role: "user", parts: [{ text: "Synthetic request only." }] }],
  },
});
const rawRequest = (path = "/v1internal:streamGenerateContent?alt=sse") => ({ method: "POST", path, headers, body: rawBody() });
const usage = { promptTokenCount: 7, candidatesTokenCount: 1, totalTokenCount: 8, cachedContentTokenCount: 0 };
const candidate = (parts, extra = {}) => ({ content: { role: "model", parts }, index: 0, ...extra });
function fixture(id, surfaceIds, scenario, request, reply, source_refs, extra = {}) {
  return {
    id: `agy159-${id}`,
    client: clientPin,
    provenance: "synthetic",
    surfaceIds,
    scenario,
    request,
    response: reply,
    source_refs,
    expected: {
      proof: "fixture-contract-only",
      runtime_status: "NOT_RUN",
      consumer_decode: "unobserved",
      provider_success: false,
      subscription_success: false,
    },
    ...extra,
  };
}
function descriptorFixture(id, surfaceId, scenario, path, body, reply, refs, extra = {}) {
  return fixture(id, [surfaceId], scenario, { method: "POST", path, headers, body }, reply, refs, {
    provenance: "descriptor-only",
    wire_certainty: "candidate; not captured",
    ...extra,
  });
}

// Base64 is synthetic framing, not captured protobuf or audio. Small messages use
// length-delimited fields only; no opaque live bytes are imported or persisted.
function field(number, text) {
  const bytes = Buffer.from(text, "utf8");
  if (bytes.length > 127 || number > 15) throw new RangeError("Synthetic field exceeds one-byte framing");
  return Buffer.concat([Buffer.from([(number << 3) | 2, bytes.length]), bytes]);
}
function grpcFrame(payload, advertisedLength = payload.length) {
  const prefix = Buffer.alloc(5);
  prefix.writeUInt32BE(advertisedLength, 1);
  return { kind: "grpc", encoding: "base64", syntheticContent: true, data: Buffer.concat([prefix, payload]).toString("base64"), framing: "grpc-uncompressed" };
}
const voicePayload = Buffer.from([0x0a, 0x00]);
const signalingPayload = Buffer.concat([
  field(1, "synthetic-session"), field(2, "synthetic-signal"),
  field(3, "synthetic-message"), field(4, "synthetic-sender"),
  field(5, "synthetic-recipient"), field(6, "synthetic-type"),
]);
const toolCall = {
  thoughtSignature: "c3ludGhldGljLXNpZ25hdHVyZQ==",
  functionCall: { id: "synthetic-call-1", name: "fixture_lookup", args: { key: "safe", futureArg: { zero: 0 } } },
  futurePart: { safeUnknown: [false, 0, "preserve"] },
};
const thoughts = { thought: true, text: "Synthetic reasoning placeholder.", thoughtSignature: "c3ludGhldGljLXRoaW5raW5n" };
const nativeSuccess = fixture(
  "native-sse-text", [direct], "native Gemini SSE local consumer success candidate",
  nativeRequest(),
  response([
    sse({ candidates: [candidate([{ text: "AGY159_SYNTHETIC_CONSUMER_OK" }])] }),
    sse({ candidates: [candidate([{ text: "" }], { finishReason: "STOP" })], usageMetadata: usage }),
  ]),
  [nativeCapture],
);
nativeSuccess.expected.consumer = { mode: "agy-print", marker: "AGY159_SYNTHETIC_CONSUMER_OK", transport: "sse" };
const gatewayBearerHeaders = [
  ["authorization", "Bearer synthetic-gateway-api-key"],
  ["content-type", "application/json"],
];
const gatewayBearer = fixture(
  "gateway-bearer-sse", [direct],
  "native Gemini SSE gateway API-key-as-Bearer candidate; model selected from AGY_LLM_GATEWAY_MODELS",
  {
    method: "POST",
    path: "/v1beta/models/gemini-2.0-flash:streamGenerateContent?alt=sse",
    headers: gatewayBearerHeaders,
    body: { contents: [{ role: "user", parts: [{ text: "Reply with the local gateway marker." }] }] },
  },
  response([
    sse({ candidates: [candidate([{ text: "AGY159_SYNTHETIC_GATEWAY_BEARER_OK" }])] }),
    sse({ candidates: [candidate([{ text: "" }], { finishReason: "STOP" })], usageMetadata: usage }),
  ]),
  [{
    ...nativeCapture,
    selection: "native streamGenerateContent path shape only",
    qualification: "Synthetic native-only gateway recipe; fake bearer and explicit gateway model are fixture values, not a captured successful turn",
  }],
  { provenance: "synthetic", native_only: true },
);
gatewayBearer.expected.consumer = {
  mode: "agy-print",
  marker: "AGY159_SYNTHETIC_GATEWAY_BEARER_OK",
  transport: "sse",
  authMode: "gateway-bearer",
  model: "gemini-2.0-flash",
  modelSource: "AGY_LLM_GATEWAY_MODELS",
};

export const redactionSentinels = Object.freeze({
  json: "QHARNESS_AGY159_SECRET_JSON",
  header: "QHARNESS_AGY159_SECRET_HEADER",
  query: "QHARNESS_AGY159_SECRET_QUERY",
  ws: "QHARNESS_AGY159_SECRET_WS",
  grpc: "QHARNESS_AGY159_SECRET_GRPC",
  binary: "QHARNESS_AGY159_SECRET_BINARY",
  file: "QHARNESS_AGY159_SECRET_FILE",
  audio: "QHARNESS_AGY159_SECRET_AUDIO",
});

export const fixtures = [
  nativeSuccess,
  gatewayBearer,
  fixture("native-sse-tools", [direct], "ordered thought, opaque signature, function call, terminal and nested usage",
    nativeRequest(), response([
      sse({ candidates: [candidate([thoughts])] }),
      sse({ candidates: [candidate([toolCall])] }),
      sse({ candidates: [candidate([], { finishReason: "STOP" })], usageMetadata: { ...usage, thoughtsTokenCount: 0, promptTokensDetails: [{ modality: "TEXT", tokenCount: 7 }], futureUsage: { zero: 0 } } }),
    ]), [nativeCapture, ...geminiParts, related]),
  fixture("native-function-response", [direct], "functionResponse follows its original functionCall without reordering",
    { ...nativeRequest(), body: { contents: [
      { role: "user", parts: [{ text: "Use the synthetic tool." }] },
      { role: "model", parts: [thoughts, toolCall] },
      { role: "user", parts: [{ functionResponse: { id: "synthetic-call-1", name: "fixture_lookup", response: { result: 0, futureResult: { preserved: true } } } }] },
    ] } },
    response([sse({ candidates: [candidate([{ text: "Synthetic tool acknowledged." }], { finishReason: "STOP" })], usageMetadata: usage })]),
    [nativeCapture, ...geminiParts, related]),
  fixture("raw-envelope-sse", [stream], "subscription-shaped wrapper candidate retains request identity and nested accounting",
    rawRequest(), response([
      sse({ response: { candidates: [candidate([toolCall])] }, traceId: "synthetic-trace" }),
      sse({ response: { candidates: [candidate([], { finishReason: "STOP" })], usageMetadata: { ...usage, thoughtsTokenCount: 0 } }, consumedCredits: [{ creditType: "SYNTHETIC", creditAmount: "0" }], remainingCredits: [], metadata: { safeUnknown: { zero: 0 } } }),
    ]), [prediction("GenerateContentRequest / GenerateContentResponse"), ...geminiParts, related]),
  fixture("raw-envelope-json", [surface("code_assist-predictionservice-generatecontent")], "unary raw envelope does not flatten nested usage",
    rawRequest("/v1internal:generateContent"), response([json({ response: { candidates: [candidate([{ text: "Synthetic unary." }], { finishReason: "STOP" })], usageMetadata: usage }, traceId: "synthetic-trace", metadata: { safeUnknown: true } })]),
    [prediction("GenerateContentResponse")]),
  descriptorFixture("discovery", surface("code_assist-predictionservice-fetchavailablemodels"), "model discovery map and safe future field candidate",
    "/v1internal:fetchAvailableModels", { project: "synthetic-project", requestId: "synthetic-discovery" },
    response([json({ models: { "synthetic-model": {} }, defaultAgentModelId: "synthetic-model", commandModelIds: [], safeUnknown: { preserve: true } })]),
    [descriptor("google/internal/cloud/code/v1internal/model_configs.proto", "FetchAvailableModelsResponse")]),
  descriptorFixture("quota", surface("code_assist-predictionservice-retrieveuserquota"), "quota distinguishes explicit zero from absent amount/fraction",
    "/v1internal:retrieveUserQuota", { project: "synthetic-project" },
    response([json({ buckets: [{ modelId: "synthetic-zero", remainingAmount: "0", remainingFraction: 0 }, { modelId: "synthetic-absent" }] })]),
    [prediction("RetrieveUserQuotaResponse.BucketInfo")]),
  descriptorFixture("controls", surface("code_assist-cloudcode-fetchadmincontrols"), "control false differs from omitted control",
    "/v1internal:fetchAdminControls", { project: "synthetic-project" },
    response([json({ disableTelemetry: false, secureModeEnabled: true, safeUnknown: { enabled: false } })]),
    [descriptor("google/internal/cloud/code/v1internal/onboarding.proto", "FetchAdminControlsResponse")]),
  descriptorFixture("remote-cancel", surface("remote_agent-jetskiremoteagentproxyservice-cancelcascade"), "remote cancellation request candidate; no cancellation completion proof",
    "/v1internal/remote:cancelCascade", { cascadeId: "synthetic-cascade", reason: "synthetic cancel", stepIndices: [0], killBackgroundTasks: false },
    response([json({})]), [remote("CancelCascadeRequest / CancelCascadeResponse")],
    { path_template: "/{$api_version}/remote:cancelCascade", chosen_api_version: "synthetic v1internal candidate" }),
  descriptorFixture("signaling-grpc", signal, "known protobuf string fields inside uncompressed gRPC envelope",
    "/devtools_jetski_boq_api_proto.ApiService/SendSignalingMessage", signalingPayload.toString("base64"),
    response([grpcFrame(Buffer.alloc(0))], { headers: [["content-type", "application/grpc"]], trailers: [["grpc-status", "0"]] }),
    [signaling("SendSignalingMessageRequest / SendSignalingMessageResponse")],
    { request: { method: "POST", path: "/devtools_jetski_boq_api_proto.ApiService/SendSignalingMessage", headers: [["content-type", "application/grpc"]], body: grpcFrame(signalingPayload).data, body_encoding: "base64" }, expected_protobuf: { field_numbers: [1, 2, 3, 4, 5, 6], values: ["synthetic-session", "synthetic-signal", "synthetic-message", "synthetic-sender", "synthetic-recipient", "synthetic-type"] } }),
  descriptorFixture("signaling-ws-candidate", signal, "WS framing candidate only; no installed websocket route asserted",
    "/__fixture__/signaling", null,
    response([{ kind: "ws-text", data: JSON.stringify({ sessionId: "synthetic-session", payload: "synthetic-signal", safeUnknown: { preserve: true } }) }, { kind: "ws-binary", encoding: "base64", syntheticContent: true, data: signalingPayload.toString("base64") }], { status: 101, headers: [], termination: "ws-close", close_code: 1000 }),
    [signaling("SendSignalingMessageRequest")], { request: { method: "GET", path: "/__fixture__/signaling", headers: [], body: null }, path_scope: "local-fixture-only", transport_candidate: "websocket; descriptor does not establish transport" }),
  descriptorFixture("voice-grpc", voice, "empty nested voice message has known protobuf length; audio schema unresolved",
    "/google.gca.aicode.v1main.JetskiRemoteAgentProxyService/StreamVoice", { message: {} },
    response([grpcFrame(voicePayload)], { headers: [["content-type", "application/grpc"]], trailers: [["grpc-status", "0"]] }),
    [remote("StreamVoiceResponse.message field 1")], { descriptor_candidate: { nested_type: ".exa.remote_agent_proxy_pb.VoiceServerMessage", nested_descriptor_available: false, payload_hex: "0a00", audio_codec_known: false } }),
  descriptorFixture("audio-framing-candidate", voice, "synthetic PCM framing candidate; not established installed voice encoding",
    "/__fixture__/audio", null,
    response([{ kind: "audio", encoding: "base64", syntheticContent: true, data: Buffer.alloc(8).toString("base64"), media_type: "audio/pcm", sample_format: "s16le", sample_rate_hz: 16000, channels: 1, sample_count: 4 }], { headers: [["content-type", "audio/pcm"]] }),
    [remote("StreamVoiceResponse")], { path_scope: "local-fixture-only", descriptor_candidate: { installed_audio_encoding_known: false } }),
  fixture("native-cancel", [direct], "consumer cancellation after partial SSE must not synthesize success",
    nativeRequest(), response([sse({ candidates: [candidate([{ text: "Synthetic partial." }])] })], { termination: "client-cancel" }), [nativeCapture],
    { contract: { terminal: false, cancel_after_frame: 1, provider_success: false } }),
  fixture("native-partial-json", [direct], "truncated SSE event at connection reset is incomplete",
    nativeRequest(), response([{ kind: "sse", data: 'data: {"candidates":[{"content":' }], { termination: "connection-reset" }), [nativeCapture],
    { contract: { terminal: false, parse_error: "incomplete-json", provider_success: false } }),
  fixture("native-malformed-json", [direct], "complete SSE boundary with malformed JSON is a parse failure",
    nativeRequest(), response([{ kind: "sse", data: 'data: {"candidates":]}\n\n' }]), [nativeCapture],
    { contract: { terminal: false, parse_error: "malformed-json", provider_success: false } }),
  descriptorFixture("voice-malformed-length", voice, "gRPC declared length exceeds available synthetic payload",
    "/google.gca.aicode.v1main.JetskiRemoteAgentProxyService/StreamVoice", { message: {} },
    response([grpcFrame(voicePayload, voicePayload.length + 1)], { headers: [["content-type", "application/grpc"]], termination: "connection-reset" }),
    [remote("StreamVoiceResponse.message field 1")], { contract: { parse_error: "truncated-grpc-payload", declared_length: 3, available_length: 2, terminal: false } }),
  fixture("provider-error", [direct], "synthetic provider error retains status and structured detail without a successful candidate",
    nativeRequest(), response([json({ error: { code: 429, status: "RESOURCE_EXHAUSTED", message: "Synthetic quota error", details: [{ "@type": "type.googleapis.com/google.rpc.RetryInfo", retryDelay: "1s" }] } })], { status: 429 }), [nativeCapture],
    { contract: { error_code: 429, provider_success: false, terminal: false } }),
  fixture("auth-before-body", [stream], "harness negative: reject invalid principal before parsing malformed body or dispatching",
    { method: "POST", path: "/v1internal:streamGenerateContent?alt=sse", headers: [...headers, ["authorization", "Bearer synthetic-invalid-principal"]], body: '{"request":' },
    response([json({ error: { code: 401, message: "Synthetic unauthorized" } })], { status: 401 }), [prediction("GenerateContentRequest")],
    { contract: { harness_negative: true, body_reads: 0, owner_lookups: 0, quota_lookups: 0, signing_calls: 0, provider_calls: 0, expected_status: 401, runtime_assertion: "unobserved" } }),
  fixture("owner-negative", [stream], "harness negative: a different owner cannot resume the synthetic session",
    { ...rawRequest(), headers: [...headers, ["authorization", "Bearer synthetic-owner-b"]] },
    response([json({ error: { code: 403, message: "Synthetic owner mismatch" } })], { status: 403 }), [prediction("GenerateContentRequest")],
    { contract: { harness_negative: true, session_owner: "synthetic-owner-a", requesting_owner: "synthetic-owner-b", expected_status: 403, provider_calls: 0, runtime_assertion: "unobserved" } }),
  fixture("redaction-carriers", [direct, signal, voice], "seeded sensitive carriers and safe unknown fields for persistence-guard negatives",
    { method: "POST", path: `/__fixture__/redaction?access_token=${redactionSentinels.query}&tag=one&tag=two`, headers: [...headers, ["authorization", `Bearer ${redactionSentinels.header}`], ["x-safe", "one"], ["x-safe", "two"]], body: { nested: { access_token: redactionSentinels.json }, fileData: { fileUri: `file:///synthetic/${redactionSentinels.file}` }, safeUnknown: { keep: [false, 0, "yes"] } } },
    response([
      json({ safeUnknown: { keep: true }, nested: { access_token: redactionSentinels.json } }),
      { kind: "ws-text", data: JSON.stringify({ authorization: redactionSentinels.ws }) },
      grpcFrame(field(1, redactionSentinels.grpc)),
      { kind: "ws-binary", encoding: "base64", syntheticContent: true, data: Buffer.from(redactionSentinels.binary).toString("base64") },
      { kind: "audio", encoding: "base64", syntheticContent: true, data: Buffer.from(redactionSentinels.audio).toString("base64"), media_type: "application/x-synthetic-audio" },
    ]), [nativeCapture, signaling("SendSignalingMessageRequest"), remote("StreamVoiceResponse")],
    { path_scope: "local-fixture-only", contract: { harness_negative: true, persistence: "redact-or-reject-before-write", opaque_live_content: "metadata-only-or-schema-aware-reconstruction", safe_unknown_fields: "preserve-for-synthetic-only" } }),
];

export default fixtures;
