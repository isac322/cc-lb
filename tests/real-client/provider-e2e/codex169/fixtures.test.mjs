import assert from 'node:assert/strict';
import test from 'node:test';
import fixtures, { fixtures as namedFixtures, clientPin } from './fixtures.mjs';

// QA scope: verify synthetic data framing, source-shaped fields, continuation IDs,
// terminal ordering, routing descriptors, upload phases and explicit negatives.
// This is not a client decoder: reading an authored frame's JSON below verifies
// the fixture's serialization only. Actual CLI/subprocess acceptance is separate.
const get = (suffix) => {
  const value = fixtures.find(({ id }) => id === `codex169-${suffix}`);
  assert.ok(value, `fixture ${suffix} must exist`);
  return value;
};
const headerValues = (headers, name) => headers.filter(([key]) => key.toLowerCase() === name).map(([, value]) => value);
const payload = (fixture) => {
  assert.equal(fixture.response.frames.length, 1);
  assert.equal(fixture.response.frames[0].kind, 'json');
  return fixture.response.frames[0].data;
};
function authoredSse(frame) {
  assert.equal(frame.kind, 'sse');
  assert.ok(frame.data.endsWith('\n\n'), 'complete authored SSE block has its blank delimiter');
  const lines = frame.data.slice(0, -2).split('\n');
  assert.equal(lines.length, 2, 'Responses fixtures deliberately use one event and one data line');
  assert.ok(lines[0].startsWith('event: '));
  assert.ok(lines[1].startsWith('data: '));
  const value = JSON.parse(lines[1].slice(6));
  assert.equal(lines[0], `event: ${value.type}`);
  return value;
}
const events = (fixture) => fixture.response.frames.map(authoredSse);
const safeExtension = (value) => assert.deepEqual(value.fixture_extension, { label: 'safe-unknown', nested: [7, { preserved: true }] });

test('fixture identity, provenance and import data remain explicit and immutable', () => {
  assert.equal(fixtures, namedFixtures);
  assert.deepEqual(clientPin, {
    name: 'codex', version: '0.169.0', source_revision: 'd32063f96c2dff1dc96af839b3952abb6acfeda4',
    binary_sha256: '434603625edf4fd28af9c123a8b43ed569ef8a3c6a771eed8b656c04ed230b84',
  });
  assert.equal(new Set(fixtures.map(({ id }) => id)).size, fixtures.length);
  assert.ok(Object.isFrozen(fixtures));
  for (const fixture of fixtures) {
    assert.equal(fixture.client, clientPin);
    assert.equal(fixture.provenance, 'synthetic');
    assert.equal(fixture.origin, 'deterministic-handwritten-source-shaped');
    assert.equal(fixture.expected.evidence, 'synthetic');
    assert.match(fixture.expected.contractBoundary, /actual consumer subprocess decode.*remain unverified/);
    assert.ok(Object.isFrozen(fixture));
    assert.ok(Object.isFrozen(fixture.request.headers));
    assert.ok(Object.isFrozen(fixture.response.frames));
    assert.ok(fixture.surfaceIds.length > 0);
    for (const id of fixture.surfaceIds) assert.match(id, /^codex:[a-z0-9-]+$/);
    for (const ref of fixture.sourceRefs) {
      const url = new URL(ref);
      assert.equal(url.origin, 'https://github.com');
      assert.ok(url.pathname.startsWith(`/openai/codex/blob/${clientPin.source_revision}/codex-rs/`));
      assert.match(url.hash, /^#L\d+-L\d+$/);
    }
    for (const headers of [fixture.request.headers, fixture.response.headers]) {
      for (const pair of headers) {
        assert.equal(pair.length, 2);
        for (const part of pair) assert.equal(typeof part, 'string');
      }
    }
    assert.ok(Number.isInteger(fixture.response.status));
    assert.equal(typeof fixture.response.termination, 'string');
    for (const frame of fixture.response.frames) assert.ok(['json', 'sse', 'ws-text'].includes(frame.kind));
  }
});

test('normal SSE fixture orders message, delta, item completion and terminal response', () => {
  const fixture = get('sse-text');
  const sequence = events(fixture);
  assert.deepEqual(sequence.map(({ type }) => type), fixture.expected.eventOrder);
  assert.deepEqual(sequence.map(({ type }) => type), [
    'response.created', 'response.output_item.added', 'response.output_text.delta', 'response.output_item.done', 'response.completed',
  ]);
  assert.equal(sequence[0].response.id, sequence.at(-1).response.id);
  assert.equal(sequence[1].item.id, sequence[2].item_id);
  assert.equal(sequence[1].item.id, sequence[3].item.id);
  assert.equal(sequence[1].item.role, 'assistant');
  assert.equal(sequence[3].item.phase, 'final_answer');
  assert.deepEqual(sequence[3].item.content, [{ type: 'output_text', text: fixture.expected.text }]);
  assert.equal(sequence[2].delta, fixture.expected.text);
  assert.equal(sequence.at(-1).response.end_turn, true);
  assert.equal(sequence.at(-1).response.usage.total_tokens, 11);
  safeExtension(sequence[2]);
  safeExtension(fixture.request.body);
  assert.deepEqual(headerValues(fixture.request.headers, 'session-id'), ['00000000-0000-4000-8000-000000000169']);
  assert.deepEqual(headerValues(fixture.request.headers, 'thread-id'), ['00000000-0000-4000-8000-000000000170']);
  for (const headers of [fixture.request.headers, fixture.response.headers]) {
    assert.deepEqual(headerValues(headers, 'x-fixture-duplicate'), ['first', 'second']);
  }
});

test('Responses WS has a source-shaped outbound create message and independent consumer candidate', () => {
  const fixture = get('ws-text');
  assert.equal(fixture.request.method, 'GET');
  assert.equal(fixture.request.path, '/responses');
  assert.equal(fixture.response.status, 101);
  assert.equal(fixture.clientFrames[0].kind, 'ws-text');
  assert.equal(fixture.clientFrames[0].data.type, 'response.create');
  assert.equal(fixture.clientFrames[0].data.stream, true);
  assert.equal(fixture.clientFrames[0].data.store, false);
  const sequence = fixture.response.frames.map(({ kind, data }) => {
    assert.equal(kind, 'ws-text');
    return data;
  });
  assert.deepEqual(sequence, events(get('sse-text')));
  assert.equal(fixture.response.termination, 'close:1000');
  const candidates = fixtures.filter(({ expected }) => expected.consumer);
  assert.deepEqual(candidates.map(({ id }) => id), ['codex169-sse-text', 'codex169-ws-text']);
  for (const candidate of candidates) {
    assert.equal(candidate.expected.consumer.mode, 'codex-exec');
    assert.equal(candidate.expected.consumer.marker, candidate.expected.text);
    assert.equal(candidate.expected.consumer.transport, candidate.id === 'codex169-sse-text' ? 'sse' : 'ws');
    assert.equal(candidate.expected.evidence, 'synthetic');
  }
});

test('tool arguments are JSON strings and call IDs bind the later function output', () => {
  const fixture = get('sse-tool');
  const sequence = events(fixture);
  const added = sequence.find(({ type }) => type === 'response.output_item.added').item;
  const done = sequence.find(({ type }) => type === 'response.output_item.done').item;
  const fragments = sequence.filter(({ type }) => type === 'response.function_call_arguments.delta');
  assert.equal(added.arguments, '');
  assert.equal(added.status, 'in_progress');
  assert.equal(done.status, 'completed');
  assert.equal(done.call_id, fixture.expected.callId);
  assert.equal(done.id, fixture.expected.itemId);
  assert.equal(typeof done.arguments, 'string');
  assert.deepEqual(JSON.parse(done.arguments), { key: 'safe' });
  assert.equal(fragments.map(({ delta }) => delta).join(''), done.arguments);
  assert.ok(fragments.every(({ item_id }) => item_id === done.id));
  assert.equal(sequence.at(-1).response.end_turn, false);
  const [previousCall, output] = fixture.expected.continuation.body.input;
  assert.deepEqual(previousCall, done);
  assert.equal(output.type, 'function_call_output');
  assert.equal(output.call_id, done.call_id);
  assert.equal(output.output, 'synthetic tool result');
  safeExtension(done);
  safeExtension(output);
});

test('compaction V2 requires one opaque output and a completed response on /responses', () => {
  for (const [suffix, count] of [['compaction-v2', 1], ['compaction-v2-missing', 0], ['compaction-v2-duplicate', 2]]) {
    const fixture = get(suffix);
    const sequence = events(fixture);
    assert.equal(fixture.request.path, '/responses');
    assert.equal(fixture.request.body.input.at(-1).type, 'compaction_trigger');
    const outputs = sequence.filter(({ type, item }) => type === 'response.output_item.done' && item.type === 'compaction');
    assert.equal(outputs.length, count);
    assert.equal(fixture.expected.compactionCount, count);
    assert.equal(sequence.at(-1).type, 'response.completed');
    if (count !== 1) assert.equal(fixture.expected.outcome, 'reject-compaction-count');
    else {
      assert.equal(outputs[0].item.encrypted_content, fixture.expected.opaqueCheckpoint);
      safeExtension(outputs[0].item);
    }
  }
});

test('accounts and workspace descriptors retain selected account without claiming exchange success', () => {
  for (const [style, prefix] of [['codex-api', '/api/codex'], ['chatgpt-api', '/wham']]) {
    const fixture = get(`accounts-${style}`);
    const accounts = payload(fixture);
    assert.equal(fixture.request.path, `${prefix}/accounts/check`);
    assert.equal(accounts.default_account_id, fixture.expected.selectedAccount);
    assert.deepEqual(accounts.account_ordering, [accounts.accounts[0].id]);
    assert.equal(accounts.accounts[0].workspace_backend_origin, 'https://workspace.synthetic.invalid');
    assert.equal(accounts.accounts[0].account_routing_override, 'synthetic-routing-a');
    assert.equal(fixture.expected.routingDisposition, 'descriptor-only');
    safeExtension(accounts.accounts[0]);
    const workspace = get(`workspace-${style}`);
    assert.equal(workspace.request.path, `${prefix}/workspace-messages`);
    assert.deepEqual(headerValues(workspace.request.headers, 'chatgpt-account-id'), [workspace.expected.selectedAccount]);
    assert.deepEqual(headerValues(workspace.request.headers, 'cache-control'), ['no-store']);
    assert.equal(payload(workspace).messages[0].message_type, 'announcement');
  }
  const exchange = get('workspace-switch');
  assert.equal(exchange.request.path, '/accounts/switch-workspace-token');
  assert.equal(exchange.request.body.workspace_id, exchange.expected.requiredClaims.chatgpt_account_id);
  assert.equal(exchange.expected.requiredClaims.chatgpt_user_id, 'synthetic-user-a');
  assert.equal(exchange.expected.disposition, 'blocked-contract');
  assert.equal(payload(exchange).access_token, 'synthetic-workspace-token-not-a-valid-jwt');
  assert.match(exchange.expected.pendingContract, /JWT validation/);
});

test('cloud tasks use source-shaped create/list/detail/sibling schemas for both route aliases', () => {
  for (const [style, prefix] of [['codex-api', '/api/codex'], ['chatgpt-api', '/wham']]) {
    const list = get(`cloud-list-${style}`);
    const url = new URL(list.request.path, 'https://synthetic.invalid');
    assert.equal(url.pathname, `${prefix}/tasks/list`);
    assert.equal(url.searchParams.get('limit'), '2');
    assert.equal(url.searchParams.get('cursor'), 'synthetic/cursor');
    assert.deepEqual(url.searchParams.getAll('fixture_tag'), ['one', 'two']);
    assert.match(list.expected.queryExtension, /stock client does not emit/);
    assert.equal(payload(list).items[0].id, list.expected.taskId);
    assert.equal(payload(list).items[0].archived, false);
    assert.equal(payload(list).items[0].has_unread_turn, true);
    assert.equal(payload(list).cursor, 'synthetic-next-cursor');
    const create = get(`cloud-create-${style}`);
    assert.equal(create.request.method, 'POST');
    assert.equal(create.request.path, `${prefix}/tasks`);
    assert.deepEqual(create.request.body.new_task, { environment_id: 'env_synthetic', branch: 'synthetic-branch', run_environment_in_qa_mode: false });
    assert.equal(create.request.body.input_items[0].content[0].content_type, 'text');
    assert.equal(create.request.body.metadata.best_of_n, 2);
    assert.equal(payload(create).task.id, list.expected.taskId);
    const detail = get(`cloud-detail-${style}`);
    assert.equal(detail.request.path, `${prefix}/tasks/${list.expected.taskId}`);
    const turn = payload(detail).current_assistant_turn;
    assert.equal(turn.id, detail.expected.turnId);
    assert.equal(turn.turn_status, 'completed');
    assert.equal(turn.output_items[0].content[0].content_type, 'text');
    const siblings = get(`cloud-siblings-${style}`);
    assert.equal(siblings.request.path, `${detail.request.path}/turns/${turn.id}/sibling_turns`);
    assert.deepEqual(payload(siblings).sibling_turns.map(({ id }) => id), turn.sibling_turn_ids);
    safeExtension(payload(siblings).sibling_turns[0]);
  }
});

for (const binding of ['mcp', 'plugin']) {
  test(`${binding} initialize/list/call preserve RPC IDs, negotiated session and lifecycle methods`, () => {
    const init = get(`${binding}-initialize`);
    const list = get(`${binding}-tools-list`);
    const call = get(`${binding}-tools-call`);
    for (const fixture of [init, list, call]) {
      assert.equal(fixture.request.path, binding === 'plugin' ? '/ps/mcp' : '/mcp');
      assert.equal(fixture.request.body.jsonrpc, '2.0');
      assert.equal(payload(fixture).jsonrpc, '2.0');
      assert.equal(payload(fixture).id, fixture.request.body.id);
      assert.equal(fixture.expected.rpcId, fixture.request.body.id);
      assert.deepEqual(headerValues(fixture.request.headers, 'accept'), ['text/event-stream, application/json']);
      assert.deepEqual(headerValues(fixture.response.headers, 'mcp-session-id'), ['mcp_synthetic_169']);
    }
    assert.equal(init.request.body.params.protocolVersion, payload(init).result.protocolVersion);
    assert.equal(init.request.body.params.clientInfo.version, clientPin.version);
    assert.deepEqual(headerValues(init.request.headers, 'mcp-session-id'), []);
    const tool = payload(list).result.tools[0];
    assert.equal(tool.inputSchema.type, 'object');
    assert.equal(call.request.body.params.name, tool.name);
    assert.equal(call.request.body.params.arguments.key, 'safe');
    assert.deepEqual(headerValues(call.request.headers, 'mcp-session-id'), ['mcp_synthetic_169']);
    assert.equal(payload(call).result.isError, false);
    assert.equal(payload(call).result.content[0].type, 'text');
    safeExtension(tool);
    safeExtension(payload(call).result);
    const initialized = get(`${binding}-initialized`);
    assert.equal(Object.hasOwn(initialized.request.body, 'id'), false);
    assert.equal(initialized.response.status, 202);
    assert.deepEqual(initialized.response.frames, []);
    const getStream = get(`${binding}-get`);
    assert.equal(getStream.request.method, 'GET');
    assert.equal(getStream.request.body, null);
    assert.deepEqual(headerValues(getStream.request.headers, 'last-event-id'), ['event_synthetic_1']);
    assert.match(getStream.response.frames[0].data, /^id: event_synthetic_2\nevent: message\ndata: /);
    const notification = JSON.parse(getStream.response.frames[0].data.split('\n')[2].slice(6));
    assert.equal(notification.method, 'notifications/tools/list_changed');
    assert.equal(getStream.response.termination, 'client-cancel');
    const deletion = get(`${binding}-delete`);
    assert.equal(deletion.request.method, 'DELETE');
    assert.equal(deletion.request.body, null);
    assert.equal(deletion.response.status, 204);
    assert.deepEqual(deletion.response.frames, []);
    assert.deepEqual(headerValues(deletion.request.headers, 'mcp-session-id'), ['mcp_synthetic_169']);
    if (binding === 'plugin') {
      assert.deepEqual(headerValues(init.request.headers, 'x-openai-product-sku'), ['codex']);
      assert.match(init.expected.pendingContract, /remain unobserved/);
    }
  });
}

test('signed file upload keeps create, binary PUT and finalize identities and credential domains separate', () => {
  const create = get('file-create');
  const put = get('file-put');
  const finalize = get('file-finalize');
  assert.equal(create.request.body.use_case, 'codex');
  assert.equal(create.request.body.codex_thread_id, headerValues(create.request.headers, 'thread-id')[0]);
  assert.equal(payload(create).file_id, finalize.expected.fileId);
  assert.equal(payload(create).upload_url, put.request.path);
  assert.equal(create.expected.next, put.id);
  assert.equal(put.expected.next, finalize.id);
  assert.equal(put.request.method, 'PUT');
  assert.equal(put.expected.bodyEncoding, 'base64');
  const bytes = Buffer.from(put.request.body, 'base64');
  assert.equal(bytes.toString(), 'safe');
  assert.equal(bytes.length, create.request.body.file_size);
  assert.equal(bytes.length, put.expected.byteLength);
  assert.deepEqual(headerValues(put.request.headers, 'content-length'), [String(bytes.length)]);
  assert.deepEqual(headerValues(put.request.headers, 'x-ms-blob-type'), ['BlockBlob']);
  assert.deepEqual(headerValues(put.request.headers, 'authorization'), []);
  assert.deepEqual(headerValues(put.request.headers, 'chatgpt-account-id'), []);
  assert.deepEqual(new URL(put.request.path).searchParams.getAll('part'), ['one', 'two']);
  assert.equal(finalize.request.path, `/files/${payload(create).file_id}/uploaded`);
  assert.deepEqual(finalize.request.body, {});
  assert.equal(payload(finalize).status, 'success');
  assert.equal(payload(finalize).file_size_bytes, bytes.length);
  safeExtension(payload(create));
});

test('remote enrollment, refresh and WS v3 preserve server, installation and stream cursor identities', () => {
  const enroll = get('remote-enroll');
  const refresh = get('remote-refresh');
  const ws = get('remote-ws');
  assert.equal(enroll.request.path, '/wham/remote/control/server/enroll');
  assert.equal(enroll.request.body.app_server_version, clientPin.version);
  assert.equal(refresh.request.path, '/wham/remote/control/server/refresh');
  assert.equal(refresh.request.body.installation_id, enroll.request.body.installation_id);
  assert.equal(refresh.request.body.server_id, payload(enroll).server_id);
  assert.equal(payload(refresh).server_id, payload(enroll).server_id);
  assert.notEqual(payload(refresh).remote_control_token, payload(enroll).remote_control_token);
  assert.equal(payload(enroll).expires_at, '2030-01-01T00:00:00Z');
  assert.deepEqual(headerValues(ws.request.headers, 'x-codex-server-id'), [payload(enroll).server_id]);
  assert.deepEqual(headerValues(ws.request.headers, 'x-codex-installation-id'), [enroll.request.body.installation_id]);
  assert.deepEqual(headerValues(ws.request.headers, 'x-codex-protocol-version'), ['3']);
  assert.equal(Buffer.from(headerValues(ws.request.headers, 'x-codex-name')[0], 'base64').toString(), enroll.request.body.name);
  assert.deepEqual(headerValues(ws.request.headers, 'authorization'), [`Bearer ${payload(enroll).remote_control_token}`]);
  assert.deepEqual(ws.response.frames.map(({ data }) => data.type), ['ping', 'ack', 'client_closed']);
  const [, ack, closed] = ws.response.frames.map(({ data }) => data);
  assert.equal(ack.seq_id, 1);
  assert.equal(ack.cursor, 'cursor_synthetic_170');
  assert.equal(ack.client_id, closed.client_id);
  assert.equal(ack.stream_id, closed.stream_id);
  assert.equal(ws.expected.consumer, undefined);
});

test('malformed, partial, cancelled, provider-error and incomplete fixtures cannot masquerade as completed turns', () => {
  const malformed = get('malformed-sse');
  assert.equal(malformed.expected.malformedJson, true);
  assert.throws(() => authoredSse(malformed.response.frames[0]), SyntaxError);
  const partial = get('partial-sse');
  assert.equal(partial.response.termination, 'abrupt-eof');
  assert.equal(partial.response.frames.at(-1).data.endsWith('\n\n'), false);
  assert.equal(partial.expected.partialFrame, true);
  const cancelled = get('cancel-sse');
  assert.equal(cancelled.response.termination, 'client-cancel');
  assert.deepEqual(events(cancelled).map(({ type }) => type), ['response.created', 'response.output_text.delta']);
  const failed = events(get('provider-error'));
  assert.equal(failed.at(-1).type, 'response.failed');
  assert.equal(failed.at(-1).response.error.code, 'server_error');
  const incomplete = events(get('incomplete'));
  assert.equal(incomplete.at(-1).type, 'response.incomplete');
  assert.equal(incomplete.at(-1).response.incomplete_details.reason, 'max_output_tokens');
  const wsPartial = get('ws-partial');
  assert.equal(wsPartial.response.termination, 'close:1006');
  assert.throws(() => JSON.parse(wsPartial.response.frames.at(-1).data), SyntaxError);
  for (const suffix of ['malformed-sse', 'partial-sse', 'cancel-sse', 'provider-error', 'incomplete', 'ws-partial']) {
    assert.equal(get(suffix).expected.consumer, undefined);
    assert.notEqual(get(suffix).expected.terminal, 'response.completed');
  }
});

test('auth-before-body alone requires zero body, state-owner, quota, signing and provider work', () => {
  const fixture = get('auth-before-body');
  assert.equal(fixture.response.status, 401);
  assert.equal(fixture.request.body, '{deliberately-unparsed-body');
  assert.equal(fixture.expected.outcome, 'unauthorized');
  assert.equal(fixture.expected.disposition, 'blocked-contract');
  assert.deepEqual(fixture.response.frames, []);
  assert.equal(fixture.expected.responseObservation, 'metadata-only');
  assert.deepEqual(fixture.expected.requiredCounters, {
    bodyReads: 0, ownerLookups: 0, quotaReservations: 0, signingCalls: 0, providerRequests: 0,
  });
  assert.deepEqual(fixture.expected.requiredOrdering, ['authenticate', 'reject-unauthorized']);
  assert.deepEqual(fixture.expected.forbiddenActions, [
    'read-body', 'lookup-state-owners', 'reserve-quota', 'sign-provider-request', 'dispatch-provider',
  ]);
  assert.deepEqual(headerValues(fixture.request.headers, 'authorization'), []);
  assert.equal(fixture.setup, undefined);
  assert.equal(fixture.expected.consumer, undefined);
});

test('400 and 503 are provider-state owner contracts under one valid synthetic principal, not competing headers', () => {
  for (const [suffix, status, outcome, disposition] of [
    ['conflicting-owner', 400, 'conflicting_owner', 'conflicting'],
    ['unavailable-owner', 503, 'owner_unavailable', 'absent'],
  ]) {
    const fixture = get(suffix);
    assert.equal(fixture.response.status, status);
    assert.equal(typeof fixture.request.body, 'object');
    assert.equal(fixture.request.body.stream, true);
    assert.equal(fixture.expected.bodyMustBeParsed, true);
    assert.equal(fixture.expected.outcome, outcome);
    assert.equal(fixture.expected.stateOwnerDisposition, disposition);
    assert.equal(fixture.expected.disposition, 'blocked-contract');
    assert.deepEqual(fixture.response.frames, []);
    assert.equal(fixture.expected.responseObservation, 'metadata-only');
    assert.deepEqual(fixture.expected.requiredCounters, { providerRequests: 0 });
    assert.deepEqual(fixture.expected.requiredOrdering.slice(0, 4), [
      'authenticate', 'read-body', 'extract-state-references', 'lookup-state-owners',
    ]);
    assert.deepEqual(headerValues(fixture.request.headers, 'authorization'), [fixture.setup.authenticatedPrincipal.authorization]);
    assert.deepEqual(headerValues(fixture.request.headers, 'x-api-key'), []);
    assert.equal(fixture.setup.authenticatedPrincipal.id, 'principal_synthetic_169');
    assert.equal(fixture.expected.contractRefs.length, 2);
    assert.match(fixture.expected.pendingContract, /requires real proxy\/consumer measurement/);
    assert.equal(fixture.expected.consumer, undefined);
    for (const item of fixture.request.body.input) {
      assert.equal(item.type, 'reasoning');
      assert.deepEqual(item.summary, []);
      assert.equal(typeof item.encrypted_content, 'string');
    }
  }
  const conflict = get('conflicting-owner');
  const states = conflict.request.body.input.map(({ encrypted_content }) => encrypted_content);
  assert.equal(states.length, 2);
  assert.deepEqual(conflict.setup.providerStateBindings.map(({ value }) => value), states);
  assert.equal(new Set(conflict.setup.providerStateBindings.map(({ ownerId }) => ownerId)).size, 2);
  for (const { ownerId } of conflict.setup.providerStateBindings) assert.ok(conflict.setup.availableUpstreamIds.includes(ownerId));
  assert.equal(conflict.expected.requiredOrdering.at(-1), 'reject-conflicting-owners');
  const absent = get('unavailable-owner');
  assert.equal(absent.request.body.input.length, 1);
  assert.equal(absent.request.body.input[0].encrypted_content, 'synthetic-state-absent');
  assert.deepEqual(absent.setup.providerStateBindings, []);
  assert.equal(absent.expected.requiredOrdering.at(-1), 'reject-absent-state-owner');
});

test('plugin catalog and detail candidates preserve service pagination, policy enums and release structure', () => {
  const catalog = get('plugin-catalog');
  const detail = get('plugin-detail');
  const url = new URL(catalog.request.path, 'https://synthetic.invalid');
  assert.equal(url.pathname, '/ps/plugins/list');
  assert.equal(url.searchParams.get('scope'), 'WORKSPACE');
  assert.equal(url.searchParams.get('limit'), '200');
  assert.equal(url.searchParams.get('pageToken'), 'synthetic/page');
  assert.equal(payload(catalog).pagination.next_page_token, catalog.expected.nextPageToken);
  const [plugin] = payload(catalog).plugins;
  assert.equal(plugin.id, catalog.expected.pluginId);
  assert.equal(plugin.scope, 'WORKSPACE');
  assert.equal(plugin.installation_policy, 'AVAILABLE');
  assert.equal(plugin.authentication_policy, 'ON_USE');
  assert.equal(plugin.status, 'ENABLED');
  assert.equal(plugin.discoverability, 'PRIVATE');
  assert.equal(plugin.release.display_name, 'Synthetic Plugin');
  assert.deepEqual(plugin.release.mcp_servers, [{ key: 'synthetic-mcp' }]);
  assert.deepEqual(plugin.release.interface.capabilities, []);
  safeExtension(plugin);
  safeExtension(plugin.release);
  assert.equal(detail.request.path, `/ps/plugins/${plugin.id}?includeDownloadUrls=true`);
  assert.equal(payload(detail).id, plugin.id);
  assert.equal(payload(detail).release.bundle_download_url, 'https://blob.synthetic.invalid/plugin.tar.gz?sig=synthetic-plugin-download');
  assert.equal(Object.hasOwn(payload(detail), 'plugins'), false, 'detail is not a catalog wrapper');
});

test('plugin install is a separate candidate mutation with ID/enabled state and app-auth requirements', () => {
  const fixture = get('plugin-install');
  const plugin = payload(get('plugin-detail'));
  assert.equal(fixture.request.method, 'POST');
  assert.equal(fixture.request.path, `/ps/plugins/${plugin.id}/install?includeAppsNeedingAuth=true`);
  assert.deepEqual(fixture.request.body, { install_attempt_id: 'install_synthetic_169' });
  assert.equal(payload(fixture).id, plugin.id);
  assert.equal(payload(fixture).enabled, fixture.expected.requiredEnabled);
  assert.deepEqual(payload(fixture).app_ids_needing_auth, plugin.release.app_ids);
  assert.equal(Object.hasOwn(payload(fixture), 'jsonrpc'), false);
});

test('plugin sharing candidates retain archive upload prerequisites without claiming upload or publication', () => {
  const upload = get('plugin-share-upload-url');
  const share = get('plugin-share-create');
  assert.equal(upload.request.path, '/public/plugins/workspace/upload-url');
  assert.deepEqual(upload.request.body, { filename: 'synthetic-plugin.tar.gz', mime_type: 'application/gzip', size_bytes: 128 });
  assert.equal(upload.expected.archiveUpload, 'not-executed');
  assert.equal(share.request.path, '/public/plugins/workspace');
  assert.equal(share.request.body.file_id, payload(upload).file_id);
  assert.equal(share.request.body.etag, payload(upload).etag);
  assert.equal(share.request.body.discoverability, 'PRIVATE');
  assert.deepEqual(share.request.body.share_targets, [{ principal_type: 'user', principal_id: 'user_synthetic_reader', role: 'reader' }]);
  assert.equal(payload(share).plugin_id, share.expected.pluginId);
  assert.equal(payload(share).can_publish_to_workspace, false);
  assert.deepEqual(share.expected.requires, [upload.id, 'synthetic-archive-put-not-observed']);
  for (const suffix of ['plugin-catalog', 'plugin-detail', 'plugin-install', 'plugin-share-upload-url', 'plugin-share-create']) {
    const fixture = get(suffix);
    assert.equal(fixture.expected.candidate, true);
    assert.equal(fixture.expected.disposition, 'blocked-contract');
    assert.equal(fixture.expected.evidence, 'synthetic');
    assert.equal(fixture.expected.consumer, undefined);
    assert.match(fixture.expected.pendingContract, /remain unverified/);
  }
});
