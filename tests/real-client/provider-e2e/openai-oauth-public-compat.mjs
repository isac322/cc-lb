#!/usr/bin/env node
/**
 * Public OpenAI SDK compatibility against an already-running cc-lb whose only
 * OpenAI upstreams are openai_oauth accounts. This intentionally does not
 * create or delete server resources; the supplied principal/key are reused.
 */
import fs from 'node:fs';
import https from 'node:https';
import { setTimeout as sleep } from 'node:timers/promises';

import OpenAI from 'openai';

const env = process.env;
const proxy = required('CC_LB_PROXY_URL').replace(/\/$/, '');
const apiKey = required('CC_LB_API_KEY');
const model = env.CC_LB_MODEL || 'gpt-6.2-sol';
const caCert = env.CC_LB_CA_CERT;
const adminBase = env.CC_LB_ADMIN_URL?.replace(/\/$/, '');
const adminToken = env.CC_LB_ADMIN_TOKEN;
const principalId = env.CC_LB_PRINCIPAL_ID;
const baseURL = `${proxy}/openai/v1`;
const results = [];
const OUTPUT_TOKENS = 256;

function required(name) {
  const value = env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function clientOptions() {
  const options = { apiKey, baseURL, maxRetries: 0, timeout: 90_000 };
  if (caCert) {
    const value = fs.existsSync(caCert) ? fs.readFileSync(caCert) : caCert;
    options.httpAgent = new https.Agent({ ca: value });
  }
  return options;
}

function responseText(response) {
  if (typeof response?.output_text === 'string') return response.output_text;
  return (response?.output ?? [])
    .flatMap((item) => item?.content ?? [])
    .filter((item) => item?.type === 'output_text')
    .map((item) => item.text ?? '')
    .join('');
}

function statusOf(error) {
  return error?.status ?? error?.statusCode ?? error?.response?.status ?? null;
}

function errorBody(error) {
  return error?.error ?? error?.body?.error ?? error?.body ?? {};
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function assertNonEmpty(value, message) {
  assert(typeof value === 'string' && value.trim().length > 0, message);
}

async function run(id, name, fn) {
  try {
    await fn();
    results.push({ id, name, status: 'PASS' });
    console.log(`PASS ${id} ${name}`);
  } catch (error) {
    results.push({ id, name, status: 'FAIL', error: error instanceof Error ? error.message : String(error) });
    console.log(`FAIL ${id} ${name}: ${error instanceof Error ? error.message : String(error)}`);
  }
}

async function skip(id, name, reason) {
  results.push({ id, name, status: 'SKIP', reason });
  console.log(`SKIP ${id} ${name}: ${reason}`);
}

async function adminRequest(method, path, body, headers = {}) {
  assert(adminBase && adminToken, 'CC_LB_ADMIN_URL and CC_LB_ADMIN_TOKEN are required for admin operations');
  const response = await fetch(`${adminBase}${path}`, {
    method,
    headers: { Authorization: `Bearer ${adminToken}`, ...(body === undefined ? {} : { 'content-type': 'application/json' }), ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  let json;
  try { json = JSON.parse(text); } catch { json = null; }
  return { response, json, text };
}

async function principalOverride(value) {
  assert(principalId, 'CC_LB_PRINCIPAL_ID is required for override testing');
  const current = await adminRequest('GET', `/admin/v1/principals/${principalId}`);
  assert(current.response.ok, `principal GET failed: ${current.response.status} ${current.text}`);
  const etag = current.response.headers.get('etag') || `"${current.json.revision}"`;
  const updated = await adminRequest('PATCH', `/admin/v1/principals/${principalId}`, value, { 'if-match': etag });
  assert(updated.response.ok, `principal PATCH failed: ${updated.response.status} ${updated.text}`);
  return updated.json;
}

async function overrideCase(client) {
  const current = await adminRequest('GET', `/admin/v1/principals/${principalId}`);
  assert(current.response.ok, `principal GET failed: ${current.response.status} ${current.text}`);
  const original = current.json.openai_oauth_compat ?? null;
  try {
    await principalOverride({ openai_oauth_compat: { store_true: 'reject' } });
    let caught = null;
    try {
      await client.responses.create({ model, input: 'override must reject store', store: true, max_output_tokens: OUTPUT_TOKENS });
    } catch (error) { caught = error; }
    assert(caught, 'store:true override unexpectedly succeeded');
    assert(statusOf(caught) === 400, `expected HTTP 400, got ${statusOf(caught)}`);
    const body = errorBody(caught);
    assert(body.param === 'store', `expected error.param=store, got ${JSON.stringify(body)}`);
  } finally {
    await principalOverride({ openai_oauth_compat: original });
  }
}

function pngBytes() {
  return Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=', 'base64');
}

async function collect(iterable) {
  const values = [];
  for await (const item of iterable) values.push(item);
  return values;
}

async function rawSdkPost(client, path, body) {
  assert(typeof client.post === 'function', `installed OpenAI SDK has no raw POST helper for ${path}`);
  return client.post(path, { body });
}

async function inputTokenCount(client) {
  const api = client.responses?.inputTokens ?? client.responses?.input_tokens;
  if (api && typeof api.count === 'function') return api.count({ model, input: 'Count these short input tokens.' });
  return rawSdkPost(client, '/responses/input_tokens', { model, input: 'Count these short input tokens.' });
}

async function compact(client) {
  const body = { model, input: [{ role: 'user', content: [{ type: 'input_text', text: 'Keep only this short fact: the color is blue.' }] }] };
  if (client.responses && typeof client.responses.compact === 'function') return client.responses.compact(body);
  return rawSdkPost(client, '/responses/compact', body);
}

async function websocketSdkAttempt(client) {
  const websocket = client.responses?.websocket;
  if (typeof websocket !== 'function') return false;
  let connection;
  try {
    connection = await websocket({ model });
    if (!connection || typeof connection.send !== 'function') return false;
    const events = [];
    let wake;
    const push = (value) => {
      let event = value;
      if (value?.data !== undefined) event = value.data;
      if (typeof event === 'string' || Buffer.isBuffer(event)) {
        try { event = JSON.parse(event.toString()); } catch { return; }
      }
      if (event && typeof event === 'object') {
        events.push(event);
        const resolver = wake;
        wake = null;
        resolver?.();
      }
    };
    if (connection[Symbol.asyncIterator]) {
      (async () => { for await (const event of connection) push(event); })();
    } else if (typeof connection.on === 'function') {
      connection.on('message', push);
    } else if (typeof connection.addEventListener === 'function') {
      connection.addEventListener('message', push);
    } else {
      return false;
    }
    const send = (value) => connection.send(JSON.stringify(value));
    const waitFor = async (predicate, label) => {
      const deadline = Date.now() + 60_000;
      while (Date.now() < deadline) {
        const found = events.find(predicate);
        if (found) return found;
        await Promise.race([new Promise((resolve) => { wake = resolve; }), sleep(Math.max(1, deadline - Date.now()))]);
      }
      throw new Error(`timed out waiting for ${label}`);
    };
    send({ type: 'response.create', model, stream_id: 'lane_a', input: 'Say exactly A', max_output_tokens: OUTPUT_TOKENS });
    send({ type: 'response.create', model, stream_id: 'lane_b', input: 'Say exactly B', max_output_tokens: OUTPUT_TOKENS });
    const firstA = await waitFor((event) => event.type === 'response.completed' && event.stream_id === 'lane_a', 'lane_a completion');
    const firstB = await waitFor((event) => event.type === 'response.completed' && event.stream_id === 'lane_b', 'lane_b completion');
    assert(firstA.response?.id && firstB.response?.id, 'SDK WebSocket lanes did not return response ids');
    send({ type: 'response.create', model, stream_id: 'lane_a', previous_response_id: firstA.response.id, input: 'Say exactly C', max_output_tokens: OUTPUT_TOKENS });
    await waitFor((event) => event.type === 'response.completed' && event.stream_id === 'lane_a' && event.response?.id !== firstA.response.id, 'same-lane continuation');
    assert(!events.some((event) => event.type === 'response.metadata'), 'SDK WebSocket emitted response.metadata');
    return true;
  } catch {
    return false;
  } finally {
    try { connection?.close?.(); } catch { /* best effort */ }
  }
}

async function websocketRaw() {
  const module = await import('ws').catch(() => null);
  const WebSocket = module?.WebSocket ?? module?.default ?? globalThis.WebSocket;
  assert(WebSocket, 'neither the SDK websocket mode nor the ws package is installed');
  const url = `${proxy.replace(/^http/, 'ws')}/openai/v1/responses`;
  const options = { headers: { Authorization: `Bearer ${apiKey}`, 'OpenAI-Beta': 'responses_websockets=2025-02-01' } };
  if (caCert) options.ca = fs.existsSync(caCert) ? fs.readFileSync(caCert) : caCert;
  const socket = new WebSocket(url, options);
  const events = [];
  let opened;
  let failed;
  const openPromise = new Promise((resolve, reject) => { opened = resolve; failed = reject; });
  socket.on('open', opened);
  socket.on('error', failed);
  socket.on('message', (data) => {
    try { events.push(JSON.parse(data.toString())); } catch { /* ignore provider ping/text noise */ }
  });
  await Promise.race([openPromise, sleep(30_000).then(() => { throw new Error('WebSocket open timed out'); })]);
  const send = (value) => socket.send(JSON.stringify(value));
  const waitFor = async (predicate, label) => {
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      const found = events.find(predicate);
      if (found) return found;
      await sleep(50);
    }
    throw new Error(`timed out waiting for ${label}; events=${events.map((event) => event.type).join(',')}`);
  };
  try {
    send({ type: 'response.create', model, stream_id: 'lane_a', input: 'Say exactly A', max_output_tokens: OUTPUT_TOKENS });
    send({ type: 'response.create', model, stream_id: 'lane_b', input: 'Say exactly B', max_output_tokens: OUTPUT_TOKENS });
    const firstA = await waitFor((event) => event.type === 'response.completed' && event.stream_id === 'lane_a', 'lane_a completion');
    const firstB = await waitFor((event) => event.type === 'response.completed' && event.stream_id === 'lane_b', 'lane_b completion');
    assert(firstA.response?.id && firstB.response?.id, 'WebSocket lanes did not return response ids');
    send({ type: 'response.create', model, stream_id: 'lane_a', previous_response_id: firstA.response.id, input: 'Say exactly C', max_output_tokens: OUTPUT_TOKENS });
    await waitFor((event) => event.type === 'response.completed' && event.stream_id === 'lane_a' && event.response?.id !== firstA.response.id, 'same-lane continuation');
    const laneTypes = events.map((event) => event.stream_id).filter(Boolean);
    assert(laneTypes.includes('lane_a') && laneTypes.includes('lane_b'), 'WebSocket events lost stream_id lane routing');
    assert(!events.some((event) => event.type === 'response.metadata'), 'public WebSocket emitted response.metadata');
  } finally {
    try { socket.close(); } catch { /* already closed */ }
  }
}

async function main() {
  const client = new OpenAI(clientOptions());
  await run('QA-C-143', 'responses non-stream string input returns output_text', async () => {
    const response = await client.responses.create({ model, input: 'Reply with one short word: ok', max_output_tokens: OUTPUT_TOKENS });
    assertNonEmpty(responseText(response), 'Responses non-stream output_text was empty');
  });
  await run('QA-C-42', 'responses stream event sequence has no response.metadata', async () => {
    const events = await collect(await client.responses.create({ model, input: 'Reply with one short word: stream', max_output_tokens: OUTPUT_TOKENS, stream: true }));
    const types = events.map((event) => event.type);
    assert(types.includes('response.created') && types.includes('response.in_progress') && types.includes('response.completed'), `unexpected event sequence: ${types.join(',')}`);
    assert(types.some((type) => type === 'response.output_text.delta'), 'stream had no output text delta');
    assert(!types.includes('response.metadata'), 'stream emitted response.metadata');
  });
  await run('QA-C-12', 'store:true is coerced and succeeds', async () => {
    const response = await client.responses.create({ model, input: 'store coercion control: say ok', store: true, max_output_tokens: OUTPUT_TOKENS });
    assertNonEmpty(responseText(response), 'store coercion response was empty');
  });
  if (adminBase && adminToken && principalId) await run('QA-C-04', 'store:true override rejects and restores', () => overrideCase(client));
  else await skip('QA-C-04', 'store:true override rejects and restores', 'admin override environment not supplied');
  await run('QA-C-13', 'temperature is dropped', async () => {
    const response = await client.responses.create({ model, input: 'temperature drop control: say ok', temperature: 0.2, max_output_tokens: OUTPUT_TOKENS });
    assertNonEmpty(responseText(response), 'temperature request failed');
  });
  await run('QA-C-17', 'metadata is echoed', async () => {
    const metadata = { qa_case: 'metadata_echo', short: '1' };
    const response = await client.responses.create({ model, input: 'Echo metadata control: say ok', metadata, max_output_tokens: OUTPUT_TOKENS });
    assert(JSON.stringify(response.metadata) === JSON.stringify(metadata), `metadata mismatch: ${JSON.stringify(response.metadata)}`);
  });
  await run('QA-C-16', 'web_search_preview tool works', async () => {
    const response = await client.responses.create({ model, input: 'Use web search to find the current weekday, then answer briefly.', tools: [{ type: 'web_search_preview' }], tool_choice: 'required', max_output_tokens: OUTPUT_TOKENS });
    assertNonEmpty(responseText(response), 'web search response was empty');
    assert((response.output ?? []).some((item) => String(item.type ?? '').includes('web_search')), 'response contained no web search call');
  });
  await run('QA-C-104', 'function tool calling loop accepts function_call_output', async () => {
    const first = await client.responses.create({ model, input: 'Call lookup with city=Paris, then wait for the result.', tools: [{ type: 'function', name: 'lookup', description: 'Look up a city.', parameters: { type: 'object', properties: { city: { type: 'string' } }, required: ['city'], additionalProperties: false } }], tool_choice: { type: 'function', name: 'lookup' }, max_output_tokens: OUTPUT_TOKENS });
    const call = (first.output ?? []).find((item) => item.type === 'function_call');
    assert(call?.call_id, 'model did not return a function call');
    const second = await client.responses.create({ model, input: [...(first.output ?? []), { type: 'function_call_output', call_id: call.call_id, output: JSON.stringify({ temperature: 18 }) }, { role: 'user', content: [{ type: 'input_text', text: 'Answer in one word.' }] }], max_output_tokens: OUTPUT_TOKENS });
    assertNonEmpty(responseText(second), 'function_call_output follow-up was empty');
  });
  await run('QA-C-100', 'chat.completions non-stream parses', async () => {
    const response = await client.chat.completions.create({ model, messages: [{ role: 'user', content: 'Reply with one short word: chat' }], max_tokens: OUTPUT_TOKENS });
    assertNonEmpty(response.choices?.[0]?.message?.content, 'chat non-stream content was empty');
  });
  await run('QA-C-101', 'chat.completions stream parses', async () => {
    const stream = await client.chat.completions.create({ model, messages: [{ role: 'user', content: 'Reply with one short word: stream' }], max_tokens: OUTPUT_TOKENS, stream: true });
    const chunks = await collect(stream);
    assert(chunks.length > 1 && chunks.some((chunk) => chunk.choices?.[0]?.delta?.content), 'chat stream had no content deltas');
    assert(chunks.at(-1)?.choices?.[0]?.finish_reason, 'chat stream had no terminal finish_reason');
  });
  await run('QA-C-104-chat', 'chat.completions tools round-trip', async () => {
    const response = await client.chat.completions.create({ model, messages: [{ role: 'user', content: 'Call lookup for Paris.' }], tools: [{ type: 'function', function: { name: 'lookup', description: 'Look up a city.', parameters: { type: 'object', properties: { city: { type: 'string' } }, required: ['city'] } } }], tool_choice: { type: 'function', function: { name: 'lookup' } }, max_tokens: OUTPUT_TOKENS });
    assert(response.choices?.[0]?.message?.tool_calls?.[0]?.function?.name === 'lookup', 'chat tool call was not returned');
  });
  await run('QA-C-109', 'chat.completions n=2 returns two choices', async () => {
    const response = await client.chat.completions.create({ model, messages: [{ role: 'user', content: 'Reply with one short word: choices' }], n: 2, max_tokens: OUTPUT_TOKENS });
    assert(response.choices?.length === 2, `expected two choices, got ${response.choices?.length}`);
  });
  await run('QA-C-112', 'chat.completions stop cuts output', async () => {
    const response = await client.chat.completions.create({ model, messages: [{ role: 'user', content: 'Reply exactly ALPHA-BETA.' }], stop: ['-'], max_tokens: OUTPUT_TOKENS });
    const content = response.choices?.[0]?.message?.content ?? '';
    assert(!content.includes('-'), `stop sequence was not cut: ${content}`);
  });
  await run('QA-C-102', 'chat.completions include_usage emits usage chunk', async () => {
    const stream = await client.chat.completions.create({ model, messages: [{ role: 'user', content: 'Reply with one short word: usage' }], max_tokens: OUTPUT_TOKENS, stream: true, stream_options: { include_usage: true } });
    const chunks = await collect(stream);
    assert(chunks.some((chunk) => chunk.usage && Number.isFinite(chunk.usage.total_tokens)), 'include_usage stream had no usage chunk');
  });
  await run('QA-C-68', 'models.list contains configured model', async () => {
    const page = await client.models.list();
    const models = await collect(page);
    assert(models.some((item) => item.id === model), `models.list did not include ${model}`);
  });
  await run('QA-C-69', 'models.retrieve works', async () => {
    const item = await client.models.retrieve(model);
    assert(item.id === model, `models.retrieve returned ${item.id}`);
  });
  await run('QA-C-66', 'responses.compact returns usable compaction output', async () => {
    const compacted = await compact(client);
    const item = (compacted.output ?? []).find((entry) => entry.type === 'compaction');
    assert(item?.encrypted_content, `compact response had no encrypted compaction item: ${JSON.stringify(compacted)}`);
    const followup = await client.responses.create({ model, input: [...(compacted.output ?? []), { role: 'user', content: [{ type: 'input_text', text: 'What color was the fact? One word.' }] }], max_output_tokens: OUTPUT_TOKENS });
    assertNonEmpty(responseText(followup), 'compaction follow-up was empty');
  });
  await run('QA-C-126', 'input_tokens.count returns a number', async () => {
    const count = await inputTokenCount(client);
    const value = count.input_tokens ?? count.inputTokens;
    assert(Number.isFinite(value) && value > 0, `invalid input token count: ${JSON.stringify(count)}`);
  });
  await run('QA-C-71', 'images.generate returns b64_json and stream completes', async () => {
    const response = await client.images.generate({ model, prompt: 'A tiny blue dot', n: 1, size: '1024x1024', quality: 'low', response_format: 'b64_json' });
    assert(typeof response.data?.[0]?.b64_json === 'string' && response.data[0].b64_json.length > 0, 'images.generate had no b64_json');
    const streamed = await client.images.generate({ model, prompt: 'A tiny red dot', n: 1, size: '1024x1024', quality: 'low', response_format: 'b64_json', stream: true });
    const events = streamed?.[Symbol.asyncIterator] ? await collect(streamed) : [streamed];
    // Public image stream events carry `b64_json` at the top level.
    assert(events.some((event) => event?.type === 'image_generation.completed' && event.b64_json), 'images stream had no b64_json event');
  });
  await run('QA-C-72', 'images.edit multipart returns b64_json', async () => {
    const image = new File([pngBytes()], 'pixel.png', { type: 'image/png' });
    const response = await client.images.edit({ model, image, prompt: 'Keep this single pixel blue', size: '1024x1024', quality: 'low', response_format: 'b64_json' });
    assert(typeof response.data?.[0]?.b64_json === 'string' && response.data[0].b64_json.length > 0, 'images.edit had no b64_json');
  });
  await run('QA-C-80/82/85', 'responses WebSocket lanes and same-lane continuation', async () => {
    const sdkSocket = await websocketSdkAttempt(client);
    if (sdkSocket) {
      assert(typeof sdkSocket.send === 'function', 'SDK websocket object has no send');
      try { sdkSocket.close?.(); } catch { /* best effort */ }
      return;
    }
    await websocketRaw();
  });
  await run('QA-R-42', 'transcription disabled returns a public 4xx/503', async () => {
    const wav = Buffer.from('UklGRiQAAABXQVZFZm10IBAAAAABAAEAgD4AAAB9AAACABAAZGFjdGEAAAAA', 'base64');
    const file = new File([wav], 'silence.wav', { type: 'audio/wav' });
    let caught = null;
    try { await client.audio.transcriptions.create({ model, file, response_format: 'json' }); } catch (error) { caught = error; }
    assert(caught, 'transcription unexpectedly succeeded while disabled');
    assert([400, 404, 503].includes(statusOf(caught)), `unexpected transcription status ${statusOf(caught)}`);
  });
  const failures = results.filter((result) => result.status === 'FAIL');
  console.log('\nQA ID | STATUS | CASE');
  for (const result of results) console.log(`${result.id} | ${result.status} | ${result.name}${result.error ? ` — ${result.error}` : ''}`);
  if (failures.length) throw new Error(`${failures.length} E2E case(s) failed`);
}

main().catch((error) => {
  console.error(`\nFAIL openai OAuth public compatibility: ${error instanceof Error ? error.stack : error}`);
  process.exitCode = 1;
});
