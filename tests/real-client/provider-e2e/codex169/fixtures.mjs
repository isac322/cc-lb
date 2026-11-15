// Synthetic protocol contracts, not captured traffic or proof of client/provider success.
// Every source URL is pinned; importing this module performs no I/O or client execution.
export const clientPin = Object.freeze({
  name: 'codex',
  version: '0.169.0',
  source_revision: 'd32063f96c2dff1dc96af839b3952abb6acfeda4',
  binary_sha256: '434603625edf4fd28af9c123a8b43ed569ef8a3c6a771eed8b656c04ed230b84',
});
const source = (path, lines) => `https://github.com/openai/codex/blob/${clientPin.source_revision}/codex-rs/${path}#${lines}`;
const refs = {
  responses: source('codex-api/src/sse/responses.rs', 'L375-L560'),
  models: source('protocol/src/models.rs', 'L1096-L1367'),
  headers: source('codex-api/src/requests/headers.rs', 'L1-L14'),
  ws: source('codex-api/src/common.rs', 'L341-L403'),
  compact: source('core/src/compact_remote_v2.rs', 'L291-L490'),
  compactInput: source('core/src/compact_remote_v2_attempt.rs', 'L175-L208'),
  backend: source('backend-client/src/client.rs', 'L516-L747'),
  workspace: source('backend-client/src/client.rs', 'L865-L869'),
  backendModels: source('backend-client/src/types.rs', 'L77-L155'),
  taskModels: source('backend-client/src/types.rs', 'L243-L285'),
  siblings: source('backend-client/src/types.rs', 'L542-L546'),
  taskCreate: source('cloud-tasks-client/src/http.rs', 'L340-L380'),
  taskList: source('codex-backend-openapi-models/src/models/task_list_item.rs', 'L15-L40'),
  pagination: source('codex-backend-openapi-models/src/models/paginated_list_task_list_item_.rs', 'L15-L20'),
  mcp: source('rmcp-client/src/http_client_adapter.rs', 'L149-L620'),
  mcpExamples: source('rmcp-client/tests/streamable_http_oauth_tool_call.rs', 'L107-L130'),
  plugin: source('ext/mcp/src/cloud_plugin.rs', 'L216-L244'),
  hostedMcp: source('codex-mcp/src/mcp/mod.rs', 'L667-L721'),
  workspaceSwitch: source('login/src/auth/manager/workspace_switching.rs', 'L272-L319'),
  pluginModels: source('core-plugins/src/remote.rs', 'L695-L924'),
  pluginPolicies: source('app-server-protocol/src/protocol/v2/plugin.rs', 'L639-L687'),
  pluginCatalog: source('core-plugins/src/remote.rs', 'L2322-L2344'),
  pluginDetail: source('core-plugins/src/remote.rs', 'L2413-L2431'),
  pluginInstall: source('core-plugins/src/remote.rs', 'L1686-L1764'),
  pluginShareModels: source('core-plugins/src/remote/share.rs', 'L98-L129'),
  pluginShare: source('core-plugins/src/remote/share.rs', 'L398-L470'),
  files: source('codex-api/src/files.rs', 'L107-L410'),
  remote: source('app-server-transport/src/transport/remote_control/protocol.rs', 'L21-L145'),
  remoteRoutes: source('app-server-transport/src/transport/remote_control/protocol.rs', 'L228-L273'),
  remoteWs: source('app-server-transport/src/transport/remote_control/websocket.rs', 'L1340-L1395'),
};
const marker = 'QHARNESS_CODEX169_SYNTHETIC_OK';
const safeOpaque = { fixture_extension: { label: 'safe-unknown', nested: [7, { preserved: true }] } };
const routingHeaders = [
  ['session-id', '00000000-0000-4000-8000-000000000169'],
  ['thread-id', '00000000-0000-4000-8000-000000000170'],
  ['chatgpt-account-id', 'synthetic-account-a'],
  ['x-fixture-duplicate', 'first'], ['x-fixture-duplicate', 'second'],
];
const jsonHeaders = [['content-type', 'application/json']];
const sseHeaders = [['content-type', 'text/event-stream'], ['x-fixture-duplicate', 'first'], ['x-fixture-duplicate', 'second']];
const request = (method, path, body = null, headers = routingHeaders) => ({ method, path, headers, body });
const json = (data) => ({ kind: 'json', data });
const sse = (data) => ({ kind: 'sse', data: `event: ${data.type}\ndata: ${JSON.stringify(data)}\n\n` });
const response = (frames, termination = 'eof', status = 200, headers = jsonHeaders) => ({ status, headers, frames, termination });
const body = (input = [{ type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Return the synthetic marker.' }] }]) => ({
  model: 'gpt-5.2-codex', instructions: 'Synthetic offline fixture.', input,
  tools: [], tool_choice: 'auto', parallel_tool_calls: true, reasoning: null,
  store: false, stream: true, include: [], ...safeOpaque,
});
const created = (id) => ({ type: 'response.created', response: { id, status: 'in_progress' } });
const completed = (id, endTurn = true) => ({ type: 'response.completed', response: {
  id, status: 'completed', end_turn: endTurn,
  usage: { input_tokens: 8, output_tokens: 3, total_tokens: 11, input_tokens_details: { cached_tokens: 0 }, output_tokens_details: { reasoning_tokens: 0 } },
} });
const message = (content = []) => ({ id: 'msg_synthetic_169', type: 'message', role: 'assistant', content, phase: 'final_answer' });
const textEvents = [
  created('resp_synthetic_text'),
  { type: 'response.output_item.added', output_index: 0, item: message() },
  { type: 'response.output_text.delta', item_id: 'msg_synthetic_169', output_index: 0, content_index: 0, delta: marker, ...safeOpaque },
  { type: 'response.output_item.done', output_index: 0, item: message([{ type: 'output_text', text: marker }]) },
  completed('resp_synthetic_text'),
];
const boundary = 'Fixture assertions only; actual consumer subprocess decode, routing ownership, proxy behavior and provider/account reachability remain unverified.';
function fixture(id, surfaceIds, scenario, req, res, expected, sourceRefs) {
  return { id: `codex169-${id}`, client: clientPin, provenance: 'synthetic', surfaceIds,
    scenario, origin: 'deterministic-handwritten-source-shaped', sourceRefs,
    request: req, response: res,
    expected: { evidence: 'synthetic', contractBoundary: boundary, ...expected } };
}
const f = [];
f.push(fixture('sse-text', ['codex:responses-sse'], 'Ordered normal Responses SSE turn',
  request('POST', '/responses', body()), response(textEvents.map(sse), 'eof', 200, sseHeaders),
  { terminal: 'response.completed', eventOrder: textEvents.map((event) => event.type), text: marker,
    consumer: { mode: 'codex-exec', marker, transport: 'sse' } }, [refs.responses, refs.models, refs.headers]));
f.push(fixture('ws-text', ['codex:responses-websocket'], 'Responses websocket response.create and ordered text turn',
  request('GET', '/responses', null, [...routingHeaders, ['upgrade', 'websocket'], ['connection', 'Upgrade']]),
  response(textEvents.map((data) => ({ kind: 'ws-text', data })), 'close:1000', 101, [['upgrade', 'websocket'], ['connection', 'Upgrade']]),
  { terminal: 'response.completed', text: marker, consumer: { mode: 'codex-exec', marker, transport: 'ws' } }, [refs.ws, refs.responses]));
f.at(-1).clientFrames = [{ kind: 'ws-text', data: { type: 'response.create', ...body() } }];
const call = { type: 'function_call', id: 'fc_synthetic_169', call_id: 'call_synthetic_169', name: 'fixture_lookup', arguments: '{"key":"safe"}', status: 'completed', ...safeOpaque };
f.push(fixture('sse-tool', ['codex:responses-sse'], 'Tool ID and JSON-string arguments survive ordered continuation',
  request('POST', '/responses', { ...body(), tools: [{ type: 'function', name: 'fixture_lookup', description: 'Synthetic lookup', parameters: { type: 'object', properties: { key: { type: 'string' } }, required: ['key'], additionalProperties: false }, strict: true }] }),
  response([created('resp_synthetic_tool'), { type: 'response.output_item.added', item: { ...call, arguments: '', status: 'in_progress' } },
    { type: 'response.function_call_arguments.delta', item_id: call.id, output_index: 0, delta: '{"key":' },
    { type: 'response.function_call_arguments.delta', item_id: call.id, output_index: 0, delta: '"safe"}' },
    { type: 'response.function_call_arguments.done', item_id: call.id, arguments: call.arguments },
    { type: 'response.output_item.done', item: call }, completed('resp_synthetic_tool', false)].map(sse), 'eof', 200, sseHeaders),
  { terminal: 'response.completed', endTurn: false, callId: call.call_id, itemId: call.id,
    continuation: request('POST', '/responses', body([call, { type: 'function_call_output', call_id: call.call_id, output: 'synthetic tool result', ...safeOpaque }])) }, [refs.responses, refs.models]));
// V2 uses the normal streaming /responses transport, NOT a guessed /responses/compact endpoint.
const compactItem = { type: 'compaction', id: 'cmp_synthetic_169', encrypted_content: 'synthetic-opaque-checkpoint-not-a-secret', ...safeOpaque };
f.push(fixture('compaction-v2', ['codex:responses-compaction-v2'], 'Exactly one opaque compaction output before completed',
  request('POST', '/responses', body([{ type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Synthetic history' }] }, { type: 'compaction_trigger' }])),
  response([created('resp_synthetic_compact'), { type: 'response.output_item.done', item: compactItem }, completed('resp_synthetic_compact')].map(sse), 'eof', 200, sseHeaders),
  { terminal: 'response.completed', compactionCount: 1, opaqueCheckpoint: compactItem.encrypted_content }, [refs.compact, refs.compactInput, refs.models]));
for (const [id, items] of [['missing', []], ['duplicate', [compactItem, { ...compactItem, id: 'cmp_synthetic_duplicate' }]]]) {
  f.push(fixture(`compaction-v2-${id}`, ['codex:responses-compaction-v2'], 'Invalid compaction item count is a source contract negative',
    request('POST', '/responses', body([{ type: 'compaction_trigger' }])),
    response([...items.map((item) => ({ type: 'response.output_item.done', item })), completed('resp_bad_compact')].map(sse), 'eof', 200, sseHeaders),
    { outcome: 'reject-compaction-count', compactionCount: items.length }, [refs.compact]));
}
for (const [style, prefix] of [['codex-api', '/api/codex'], ['chatgpt-api', '/wham']]) {
  f.push(fixture(`accounts-${style}`, ['codex:accounts-check'], 'Accounts carry account/workspace routing descriptors, not inferred routing success',
    request('GET', `${prefix}/accounts/check`), response([json({ accounts: [{ id: 'synthetic-account-a', can_access_with_session: true,
      sso_connection_name: null, is_deactivated: false, is_fedramp_compliant_workspace: false, plan_type: 'team',
      workspace_backend_origin: 'https://workspace.synthetic.invalid', account_routing_override: 'synthetic-routing-a', structure: 'workspace', ...safeOpaque }],
    account_ordering: ['synthetic-account-a'], default_account_id: 'synthetic-account-a' })]),
    { pathStyle: style, selectedAccount: 'synthetic-account-a', routingDisposition: 'descriptor-only' }, [refs.backend, refs.backendModels]));
  f.push(fixture(`workspace-${style}`, ['codex:workspace-messages'], 'Workspace message enum and active account header',
    request('GET', `${prefix}/workspace-messages`, null, [...routingHeaders, ['cache-control', 'no-store']]),
    response([json({ messages: [{ message_id: 'wm_synthetic_169', message_type: 'announcement', message_body: 'Synthetic workspace announcement.', created_at: '2026-01-01T00:00:00Z', archived_at: null, ...safeOpaque }] })]),
    { pathStyle: style, selectedAccount: 'synthetic-account-a' }, [refs.workspace, refs.backendModels]));
  f.push(fixture(`cloud-list-${style}`, ['codex:cloud-tasks-list'], 'Task pagination and repeated query entries are preserved as wire data',
    request('GET', `${prefix}/tasks/list?limit=2&task_filter=all&cursor=synthetic%2Fcursor&environment_id=env_synthetic&fixture_tag=one&fixture_tag=two`),
    response([json({ items: [{ id: 'task_synthetic_169', title: 'Synthetic cloud task', archived: false, has_unread_turn: true, ...safeOpaque }], cursor: 'synthetic-next-cursor' })]),
    { pathStyle: style, taskId: 'task_synthetic_169', duplicateQuery: [['fixture_tag', 'one'], ['fixture_tag', 'two']],
      queryExtension: 'Synthetic transport-preservation probe; stock client does not emit fixture_tag.' }, [refs.backend, refs.taskList, refs.pagination]));
  f.push(fixture(`cloud-create-${style}`, ['codex:cloud-task-create'], 'Create task uses new_task and content_type text',
    request('POST', `${prefix}/tasks`, { new_task: { environment_id: 'env_synthetic', branch: 'synthetic-branch', run_environment_in_qa_mode: false },
      input_items: [{ type: 'message', role: 'user', content: [{ content_type: 'text', text: 'Synthetic task prompt' }] }], metadata: { best_of_n: 2, ...safeOpaque } }),
    response([json({ task: { id: 'task_synthetic_169', ...safeOpaque } })]), { pathStyle: style, taskId: 'task_synthetic_169' }, [refs.backend, refs.taskCreate]));
  f.push(fixture(`cloud-detail-${style}`, ['codex:cloud-task-detail'], 'Task turn uses turn_status and sibling_turn_ids',
    request('GET', `${prefix}/tasks/task_synthetic_169`), response([json({
      current_user_turn: { id: 'turn_user_synthetic', input_items: [{ type: 'message', role: 'user', content: [{ content_type: 'text', text: 'Synthetic task prompt' }] }] },
      current_assistant_turn: { id: 'turn_synthetic_169', turn_status: 'completed', attempt_placement: 1, sibling_turn_ids: ['turn_synthetic_sibling'],
        output_items: [{ type: 'message', role: 'assistant', content: [{ content_type: 'text', text: 'Synthetic cloud result' }] }], ...safeOpaque }, current_diff_task_turn: null,
    })]), { pathStyle: style, taskId: 'task_synthetic_169', turnId: 'turn_synthetic_169' }, [refs.backend, refs.taskModels]));
  f.push(fixture(`cloud-siblings-${style}`, ['codex:cloud-task-siblings'], 'Sibling path is sibling_turns, not siblings',
    request('GET', `${prefix}/tasks/task_synthetic_169/turns/turn_synthetic_169/sibling_turns`),
    response([json({ sibling_turns: [{ id: 'turn_synthetic_sibling', turn_status: 'completed', attempt_placement: 2, ...safeOpaque }] })]),
    { pathStyle: style, siblingIds: ['turn_synthetic_sibling'] }, [refs.backend, refs.siblings]));
}
f.push(fixture('workspace-switch', ['codex:workspace-switch-token'], 'Workspace token exchange must retain user and target workspace binding',
  request('POST', '/accounts/switch-workspace-token', { workspace_id: 'synthetic-account-b' }, [['authorization', 'Bearer synthetic-current-workspace-token']]),
  response([json({ access_token: 'synthetic-workspace-token-not-a-valid-jwt' })]),
  { disposition: 'blocked-contract', requiredClaims: { chatgpt_account_id: 'synthetic-account-b', chatgpt_user_id: 'synthetic-user-a' },
    pendingContract: 'Response token is deliberately non-credential synthetic data; JWT validation and user/workspace equality require separate consumer observations.' }, [refs.workspaceSwitch]));
const mcpHeaders = [['accept', 'text/event-stream, application/json'], ['content-type', 'application/json'], ['mcp-protocol-version', '2025-06-18']];
const rpc = (id, method, params = {}) => ({ jsonrpc: '2.0', id, method, params });
const result = (id, value) => ({ jsonrpc: '2.0', id, result: value });
const tool = { name: 'fixture_lookup', description: 'Synthetic fixture tool', inputSchema: { type: 'object', properties: { key: { type: 'string' } }, required: ['key'] }, ...safeOpaque };
for (const binding of ['mcp', 'plugin']) {
  const hosted = binding === 'plugin';
  const path = hosted ? '/ps/mcp' : '/mcp';
  const surface = (method) => hosted ? [`codex:apps-mcp-${method.toLowerCase()}`] : ['codex:external-mcp-transport'];
  const sourceRefs = hosted ? [refs.mcp, refs.mcpExamples, refs.hostedMcp, refs.plugin] : [refs.mcp, refs.mcpExamples];
  const baseHeaders = hosted ? [...mcpHeaders, ['x-openai-product-sku', 'codex'], ['chatgpt-account-id', 'synthetic-account-a']] : mcpHeaders;
  const activeHeaders = [...baseHeaders, ['mcp-session-id', 'mcp_synthetic_169']];
  const extra = hosted ? { binding: 'hosted-plugin-runtime', pendingContract: 'Hosted plugin runtime URI is source-shaped; catalog entitlement, plugin loading and server negotiation remain unobserved.' } : { binding: 'configured-external-server' };
  for (const [id, method, params, value] of [
    [1, 'initialize', { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'codex', version: clientPin.version } }, { protocolVersion: '2025-06-18', capabilities: { tools: {} }, serverInfo: { name: 'synthetic-mcp', version: '1' }, ...safeOpaque }],
    [2, 'tools/list', {}, { tools: [tool] }],
    [3, 'tools/call', { name: tool.name, arguments: { key: 'safe', ...safeOpaque } }, { content: [{ type: 'text', text: 'Synthetic tool result' }], isError: false, ...safeOpaque }],
  ]) f.push(fixture(`${binding}-${method.replace('/', '-')}`, surface('POST'), `${binding} streamable HTTP JSON-RPC ${method}`,
    request('POST', path, rpc(id, method, params), id === 1 ? baseHeaders : activeHeaders),
    response([json(result(id, value))], 'eof', 200, [...jsonHeaders, ['mcp-session-id', 'mcp_synthetic_169']]), { rpcId: id, method, ...extra }, sourceRefs));
  f.push(fixture(`${binding}-initialized`, surface('POST'), 'Initialized notification has no JSON-RPC request ID or response body',
    request('POST', path, { jsonrpc: '2.0', method: 'notifications/initialized' }, activeHeaders), response([], 'eof', 202), { method: 'notifications/initialized', ...extra }, sourceRefs));
  f.push(fixture(`${binding}-get`, surface('GET'), 'Resume event stream with last-event-id and MCP session',
    request('GET', path, null, [...activeHeaders, ['last-event-id', 'event_synthetic_1']]),
    response([{ kind: 'sse', data: 'id: event_synthetic_2\nevent: message\ndata: {"jsonrpc":"2.0","method":"notifications/tools/list_changed"}\n\n' }], 'client-cancel', 200, sseHeaders),
    { lastEventId: 'event_synthetic_1', terminal: 'client-cancel', ...extra }, sourceRefs));
  f.push(fixture(`${binding}-delete`, surface('DELETE'), 'DELETE terminates only the selected MCP session',
    request('DELETE', path, null, activeHeaders), response([], 'eof', 204), { terminal: 'session-deleted', ...extra }, sourceRefs));
}
const uploadUrl = 'https://blob.synthetic.invalid/upload?sig=synthetic-signature&part=one&part=two';
f.push(fixture('file-create', ['codex:files-create'], 'Create signed upload with hosted-upload context',
  request('POST', '/files', { file_name: 'synthetic.txt', file_size: 4, use_case: 'codex', codex_connector_id: 'connector_synthetic', codex_action_name: 'fixture_upload', codex_model: 'gpt-5.2-codex', codex_thread_id: routingHeaders[1][1] }),
  response([json({ file_id: 'file_synthetic_169', upload_url: uploadUrl, upload_headers: { 'x-ms-version': '2023-11-03' }, pdf_c2pa_reservation: false, ...safeOpaque })]),
  { fileId: 'file_synthetic_169', next: 'codex169-file-put' }, [refs.files]));
f.push(fixture('file-put', ['codex:files-upload-blob'], 'Signed blob PUT is distinct from authenticated create/finalize',
  request('PUT', uploadUrl, 'c2FmZQ==', [['x-ms-blob-type', 'BlockBlob'], ['x-ms-client-request-id', '00000000-0000-4000-8000-000000000171'], ['content-length', '4'], ['x-ms-version', '2023-11-03']]),
  response([], 'eof', 201, [['etag', 'synthetic-etag']]),
  { fileId: 'file_synthetic_169', bodyEncoding: 'base64', byteLength: 4, next: 'codex169-file-finalize', credentialBoundary: 'Do not forward account Authorization to the signed blob host.' }, [refs.files]));
f.push(fixture('file-finalize', ['codex:files-finalize'], 'Finalize uses /uploaded and download status schema',
  request('POST', '/files/file_synthetic_169/uploaded', {}), response([json({ status: 'ready', download_url: 'https://blob.synthetic.invalid/download?sig=synthetic-download', file_name: 'synthetic.txt', mime_type: 'text/plain', file_size_bytes: 4, error_message: null, ...safeOpaque })]),
  { fileId: 'file_synthetic_169', state: 'ready' }, [refs.files]));
const remotePrefix = '/wham/remote/control/server';
const remoteResponse = { server_id: 'server_synthetic_169', environment_id: 'env_synthetic', remote_control_token: 'synthetic-remote-token-not-valid', expires_at: '2030-01-01T00:00:00Z', ...safeOpaque };
f.push(fixture('remote-enroll', ['codex:remote-control-enroll'], 'Remote enrollment returns server identity and expiring token',
  request('POST', `${remotePrefix}/enroll`, { name: 'Synthetic fixture host', os: 'linux', arch: 'x86_64', app_server_version: clientPin.version, installation_id: 'installation_synthetic_169' }),
  response([json(remoteResponse)]), { serverId: remoteResponse.server_id, environmentId: remoteResponse.environment_id }, [refs.remote, refs.remoteRoutes]));
f.push(fixture('remote-refresh', ['codex:remote-control-refresh'], 'Refresh is scoped to the selected server and installation',
  request('POST', `${remotePrefix}/refresh`, { server_id: remoteResponse.server_id, installation_id: 'installation_synthetic_169' }),
  response([json({ ...remoteResponse, remote_control_token: 'synthetic-refreshed-token-not-valid' })]), { serverId: remoteResponse.server_id }, [refs.remote, refs.remoteRoutes]));
f.push(fixture('remote-ws', ['codex:remote-control-websocket'], 'Remote control v3 client envelopes are not Responses events',
  request('GET', remotePrefix, null, [['upgrade', 'websocket'], ['connection', 'Upgrade'], ['x-codex-server-id', remoteResponse.server_id],
    ['x-codex-name', 'U3ludGhldGljIGZpeHR1cmUgaG9zdA=='], ['x-codex-protocol-version', '3'], ['x-codex-installation-id', 'installation_synthetic_169'],
    ['x-codex-subscribe-cursor', 'cursor_synthetic_169'], ['authorization', `Bearer ${remoteResponse.remote_control_token}`]]),
  response([{ kind: 'ws-text', data: { type: 'ping', client_id: 'client_synthetic_169', stream_id: 'stream_synthetic_169' } },
    { kind: 'ws-text', data: { type: 'ack', client_id: 'client_synthetic_169', stream_id: 'stream_synthetic_169', seq_id: 1, cursor: 'cursor_synthetic_170' } },
    { kind: 'ws-text', data: { type: 'client_closed', client_id: 'client_synthetic_169', stream_id: 'stream_synthetic_169' } }], 'close:1000', 101, [['upgrade', 'websocket'], ['connection', 'Upgrade']]),
  { protocolVersion: '3', eventOrder: ['ping', 'ack', 'client_closed'], terminal: 'client_closed' }, [refs.remote, refs.remoteRoutes, refs.remoteWs]));
for (const [id, frames, termination, expected] of [
  ['malformed-sse', [{ kind: 'sse', data: 'event: response.created\ndata: {not-json}\n\n' }], 'eof', { outcome: 'malformed-event', malformedJson: true }],
  ['partial-sse', [sse(created('resp_partial')), { kind: 'sse', data: 'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"unterminated' }], 'abrupt-eof', { outcome: 'stream-incomplete', partialFrame: true }],
  ['cancel-sse', [sse(created('resp_cancel')), sse({ type: 'response.output_text.delta', delta: 'Synthetic partial text' })], 'client-cancel', { terminal: 'client-cancel', outcome: 'cancelled-not-completed' }],
  ['provider-error', [sse(created('resp_failed')), sse({ type: 'response.failed', response: { id: 'resp_failed', error: { type: 'server_error', code: 'server_error', message: 'Synthetic provider error' } } })], 'eof', { terminal: 'response.failed', outcome: 'provider-error' }],
  ['incomplete', [sse(created('resp_incomplete')), sse({ type: 'response.incomplete', response: { id: 'resp_incomplete', incomplete_details: { reason: 'max_output_tokens' } } })], 'eof', { terminal: 'response.incomplete', outcome: 'incomplete-not-completed' }],
]) f.push(fixture(id, ['codex:responses-sse'], 'Synthetic SSE semantic negative; no production verdict', request('POST', '/responses', body()), response(frames, termination, 200, sseHeaders), expected, [refs.responses]));
f.push(fixture('ws-partial', ['codex:responses-websocket'], 'WS disconnect before completed is not a successful turn',
  request('GET', '/responses'), response([{ kind: 'ws-text', data: created('resp_ws_partial') }, { kind: 'ws-text', data: '{"type":' }], 'close:1006', 101, [['upgrade', 'websocket']]),
  { outcome: 'stream-incomplete', malformedJson: true }, [refs.ws, refs.responses]));
// Auth-principal selection and provider-state ownership are separate contracts.
// These setup records are synthetic prerequisites, not an auth-store snapshot.
const syntheticPrincipal = { id: 'principal_synthetic_169', authorization: 'Bearer synthetic-principal-169' };
const stateInput = (value, id) => ({ type: 'reasoning', id, summary: [], encrypted_content: value });
for (const [id, status, input, bindings] of [
  ['conflicting-owner', 400,
    [stateInput('synthetic-state-a', 'rs_synthetic_a'), stateInput('synthetic-state-b', 'rs_synthetic_b')],
    [{ kind: 'openai-reasoning', value: 'synthetic-state-a', ownerId: 'upstream_synthetic_a' },
      { kind: 'openai-reasoning', value: 'synthetic-state-b', ownerId: 'upstream_synthetic_b' }]],
  ['unavailable-owner', 503, [stateInput('synthetic-state-absent', 'rs_synthetic_absent')], []],
]) {
  const conflict = id === 'conflicting-owner';
  const value = fixture(id, ['codex:responses-sse'], 'Authenticated continuation with conflicting or absent provider-owned state',
    request('POST', '/responses', body(input), [...routingHeaders, ['authorization', syntheticPrincipal.authorization]]),
    response([], 'eof', status, [['x-request-id', `synthetic-${id}`]]),
    { outcome: conflict ? 'conflicting_owner' : 'owner_unavailable', disposition: 'blocked-contract',
      responseObservation: 'metadata-only', requiredCounters: { providerRequests: 0 },
      requiredOrdering: ['authenticate', 'read-body', 'extract-state-references', 'lookup-state-owners',
        conflict ? 'reject-conflicting-owners' : 'reject-absent-state-owner'],
      bodyMustBeParsed: true, stateOwnerDisposition: conflict ? 'conflicting' : 'absent',
      contractRefs: ['repo:crates/cc-lb-engine/tests/upstream_affinity.rs#L755-L842',
        'repo:crates/cc-lb-engine/tests/upstream_affinity.rs#L960-L1022'],
      pendingContract: 'Synthetic provider-state bindings only; requires real proxy/consumer measurement. No header-principal conflict or production status is inferred.' },
    [refs.models, refs.responses]);
  value.setup = { authenticatedPrincipal: syntheticPrincipal, providerStateBindings: bindings,
    availableUpstreamIds: ['upstream_synthetic_a', 'upstream_synthetic_b'] };
  f.push(value);
}
f.push(fixture('auth-before-body', ['codex:responses-sse'], 'Unauthenticated malformed body is rejected before body and provider-state work',
  request('POST', '/responses', '{deliberately-unparsed-body', [['content-type', 'application/json']]),
  response([], 'eof', 401, [['www-authenticate', 'Bearer realm="synthetic"'], ['x-request-id', 'synthetic-auth-before-body']]),
  { outcome: 'unauthorized', disposition: 'blocked-contract', responseObservation: 'metadata-only',
    requiredCounters: { bodyReads: 0, ownerLookups: 0, quotaReservations: 0, signingCalls: 0, providerRequests: 0 },
    requiredOrdering: ['authenticate', 'reject-unauthorized'],
    forbiddenActions: ['read-body', 'lookup-state-owners', 'reserve-quota', 'sign-provider-request', 'dispatch-provider'],
    pendingContract: 'Expected auth-before-body negative only; requires real proxy/consumer measurement and cannot establish owner lookup ordering for authenticated requests.' }, [refs.headers]));

const catalogPlugin = {
  id: 'plugin_synthetic_169', name: 'synthetic-plugin', scope: 'WORKSPACE',
  discoverability: 'PRIVATE', installation_policy: 'AVAILABLE', installation_policy_source: null,
  authentication_policy: 'ON_USE', status: 'ENABLED',
  release: { version: '1.0.0', display_name: 'Synthetic Plugin', description: 'Synthetic catalog description',
    app_ids: ['app_synthetic_169'], keywords: ['synthetic'], interface: { capabilities: [], screenshot_urls: [] },
    skills: [], mcp_servers: [{ key: 'synthetic-mcp' }], scheduled_tasks: null, ...safeOpaque },
  ...safeOpaque,
};
const pluginCandidate = { candidate: true, disposition: 'blocked-contract',
  pendingContract: 'Source-shaped plugin-service candidate only; entitlement, catalog fetch, installation, archive upload, sharing and actual consumer decode remain unverified.' };
f.push(fixture('plugin-catalog', ['codex:plugins-list'], 'Workspace plugin catalog with source pagination and release metadata',
  request('GET', '/ps/plugins/list?scope=WORKSPACE&limit=200&pageToken=synthetic%2Fpage'),
  response([json({ plugins: [catalogPlugin], pagination: { next_page_token: 'synthetic-next-plugin-page' }, ...safeOpaque })]),
  { ...pluginCandidate, pluginId: catalogPlugin.id, nextPageToken: 'synthetic-next-plugin-page' },
  [refs.pluginCatalog, refs.pluginModels, refs.pluginPolicies]));
f.push(fixture('plugin-detail', ['codex:plugins-detail'], 'Detail is an unwrapped directory item with optional bundle URL',
  request('GET', `/ps/plugins/${catalogPlugin.id}?includeDownloadUrls=true`),
  response([json({ ...catalogPlugin, release: { ...catalogPlugin.release, bundle_download_url: 'https://blob.synthetic.invalid/plugin.tar.gz?sig=synthetic-plugin-download' } })]),
  { ...pluginCandidate, pluginId: catalogPlugin.id }, [refs.pluginDetail, refs.pluginModels, refs.pluginPolicies]));
f.push(fixture('plugin-install', ['codex:plugins-install'], 'Install mutation binds plugin ID and enabled state independently of MCP initialize',
  request('POST', `/ps/plugins/${catalogPlugin.id}/install?includeAppsNeedingAuth=true`, { install_attempt_id: 'install_synthetic_169' }),
  response([json({ id: catalogPlugin.id, enabled: true, app_ids_needing_auth: ['app_synthetic_169'], ...safeOpaque })]),
  { ...pluginCandidate, pluginId: catalogPlugin.id, requiredEnabled: true }, [refs.pluginInstall, refs.pluginModels]));
f.push(fixture('plugin-share-upload-url', ['codex:plugins-share-upload-url'], 'Workspace sharing starts with archive upload metadata',
  request('POST', '/public/plugins/workspace/upload-url', { filename: 'synthetic-plugin.tar.gz', mime_type: 'application/gzip', size_bytes: 128 }),
  response([json({ file_id: 'plugin_archive_synthetic_169', upload_url: 'https://blob.synthetic.invalid/plugin-upload?sig=synthetic-plugin-upload', etag: 'synthetic-plugin-etag', ...safeOpaque })]),
  { ...pluginCandidate, archiveUpload: 'not-executed', fileId: 'plugin_archive_synthetic_169' }, [refs.pluginShareModels, refs.pluginShare]));
f.push(fixture('plugin-share-create', ['codex:plugins-share-create'], 'Sharing finalize preserves uploaded file, etag and discoverability',
  request('POST', '/public/plugins/workspace', { file_id: 'plugin_archive_synthetic_169', etag: 'synthetic-plugin-etag', discoverability: 'PRIVATE',
    share_targets: [{ principal_type: 'user', principal_id: 'user_synthetic_reader', role: 'reader' }] }),
  response([json({ plugin_id: catalogPlugin.id, share_url: 'https://share.synthetic.invalid/plugin_synthetic_169', can_publish_to_workspace: false, ...safeOpaque })]),
  { ...pluginCandidate, pluginId: catalogPlugin.id, requires: ['codex169-plugin-share-upload-url', 'synthetic-archive-put-not-observed'] },
  [refs.pluginShareModels, refs.pluginShare]));
function freeze(value) {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
}
export const fixtures = freeze(f);
export default fixtures;
