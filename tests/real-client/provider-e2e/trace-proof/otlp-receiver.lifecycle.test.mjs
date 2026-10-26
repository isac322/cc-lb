#!/usr/bin/env node

import assert from 'node:assert/strict';
import { once } from 'node:events';
import http2 from 'node:http2';
import { readFile, rm } from 'node:fs/promises';
import net from 'node:net';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';

// QA list:
// QA-01: delayed persistence + client HTTP/2 reset reproduces the pre-fix
//         ERR_HTTP2_INVALID_STREAM process failure.
// QA-02: the same reset after the lifecycle fix leaves the receiver alive and
//         durably records the canceled export.
// QA-03: a later normal export receives grpc-status 0 and /snapshot contains
//         both the canceled and normal spans.
// QA-04: malformed and oversized frame rejection remains unchanged.

const RECEIVER = path.join(path.dirname(fileURLToPath(import.meta.url)), 'otlp-receiver.mjs');
const OUTPUT = path.join(path.dirname(RECEIVER), 'otlp-receiver.lifecycle.output.json');
const FIXED_RESPONSE_BLOCK = [
  '      if (stream.destroyed) return;',
  '      const responseHeaders = {',
  "        ':status': 200,",
  "        'content-type': 'application/grpc',",
  "        'grpc-encoding': 'identity',",
  "        trailer: 'grpc-status',",
  '      };',
  "      stream.once('wantTrailers', () => {",
  '        if (stream.destroyed) return;',
  '        try {',
  "          stream.sendTrailers({ 'grpc-status': String(result.status) });",
  '        } catch (error) {',
  "          if (error?.code !== 'ERR_HTTP2_INVALID_STREAM') throw error;",
  '        }',
  '      });',
  '      try {',
  '        stream.respond(responseHeaders, { waitForTrailers: true });',
  '        stream.end(result.ok ? ACK_FRAME : undefined);',
  '      } catch (error) {',
  "        if (error?.code !== 'ERR_HTTP2_INVALID_STREAM') throw error;",
  '      }',
].join('\n');
const BASELINE_RESPONSE_BLOCK = [
  '      const responseHeaders = {',
  "        ':status': 200,",
  "        'content-type': 'application/grpc',",
  "        'grpc-encoding': 'identity',",
  "        trailer: 'grpc-status',",
  '      };',
  "      stream.once('wantTrailers', () => stream.sendTrailers({ 'grpc-status': String(result.status) }));",
  '      stream.respond(responseHeaders, { waitForTrailers: true });',
  '      stream.end(result.ok ? ACK_FRAME : undefined);',
].join('\n');

function varint(value) {
  const bytes = [];
  let remaining = value;
  while (remaining >= 0x80) {
    bytes.push((remaining & 0x7f) | 0x80);
    remaining >>>= 7;
  }
  bytes.push(remaining);
  return Buffer.from(bytes);
}

function lengthDelimited(field, value) {
  const bytes = Buffer.isBuffer(value) ? value : Buffer.from(value);
  return Buffer.concat([varint((field << 3) | 2), varint(bytes.length), bytes]);
}

function spanExport(spanIdHex) {
  const traceId = Buffer.from('00112233445566778899aabbccddeeff', 'hex');
  const spanId = Buffer.from(spanIdHex, 'hex');
  const span = Buffer.concat([
    lengthDelimited(1, traceId),
    lengthDelimited(2, spanId),
    lengthDelimited(5, 'collector-lifecycle-regression'),
  ]);
  const scopeSpans = lengthDelimited(2, span);
  const resourceSpans = lengthDelimited(2, scopeSpans);
  const request = lengthDelimited(1, resourceSpans);
  return Buffer.concat([Buffer.from([0]), Buffer.alloc(4), request]).map((value, index, bytes) => {
    if (index === 1) return (request.length >>> 24) & 0xff;
    if (index === 2) return (request.length >>> 16) & 0xff;
    if (index === 3) return (request.length >>> 8) & 0xff;
    if (index === 4) return request.length & 0xff;
    return bytes[index];
  });
}

async function freePort() {
  const server = net.createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert(address && typeof address === 'object' && address.port > 0);
  await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  return address.port;
}

function childCode(mode) {
  return `
    import { readFile } from 'node:fs/promises';
    const source = await readFile(${JSON.stringify(RECEIVER)}, 'utf8');
    const stripped = source.replace(/if \\(path\\.resolve\\(process\\.argv\\[1\\] \\?\\? ''\\) === fileURLToPath\\(import\\.meta\\.url\\)\\) \\{[\\s\\S]*$/u, '');
    const baseline = stripped.replace(${JSON.stringify(FIXED_RESPONSE_BLOCK)}, ${JSON.stringify(BASELINE_RESPONSE_BLOCK)});
    const moduleText = ((${JSON.stringify(mode)} === 'baseline') ? baseline : stripped)
      .replace('      await persist();\\n      if (stream.destroyed) return;', '      await new Promise((resolve) => setTimeout(resolve, 100));\\n      await persist();\\n      if (stream.destroyed) return;')
      .replace('      await persist();\\n      const responseHeaders', '      await new Promise((resolve) => setTimeout(resolve, 100));\\n      await persist();\\n      const responseHeaders');
    const module = await import('data:text/javascript;base64,' + Buffer.from(moduleText).toString('base64'));
    const port = Number(process.env.OTLP_PORT);
    const controlPort = Number(process.env.CONTROL_PORT);
    const receiver = await module.startOtlpReceiver({
      port,
      controlPort,
      outputFile: ${JSON.stringify(OUTPUT)},
    });
    console.log('READY ' + receiver.port + ' ' + receiver.controlPort);
    process.stdin.resume();
  `;
}

async function waitForLine(stream, prefix) {
  let buffer = '';
  for (;;) {
    const [chunk] = await once(stream, 'data');
    buffer += chunk.toString();
    const lineEnd = buffer.indexOf('\n');
    if (lineEnd >= 0) {
      const line = buffer.slice(0, lineEnd).trim();
      if (line.startsWith(prefix)) return line;
      buffer = buffer.slice(lineEnd + 1);
    }
  }
}

async function spawnReceiver(mode) {
  const port = await freePort();
  const controlPort = await freePort();
  const child = spawn(process.execPath, ['--input-type=module', '-e', childCode(mode)], {
    env: { ...process.env, OTLP_PORT: String(port), CONTROL_PORT: String(controlPort) },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  let stderr = '';
  child.stderr.on('data', (chunk) => { stderr += chunk.toString(); });
  const ready = await Promise.race([
    waitForLine(child.stdout, 'READY '),
    once(child, 'exit').then(([code, signal]) => {
      throw new Error(`receiver child exited before ready: code=${code} signal=${signal} stderr=${stderr}`);
    }),
  ]);
  assert.equal(ready, `READY ${port} ${controlPort}`);
  return { child, port, controlPort, stderr: () => stderr };
}

function sendExport(port, payload, cancel) {
  return new Promise((resolve, reject) => {
    const client = http2.connect(`http://127.0.0.1:${port}`);
    client.once('error', reject);
    const request = client.request({
      ':method': 'POST',
      ':path': '/opentelemetry.proto.collector.trace.v1.TraceService/Export',
      'content-type': 'application/grpc',
      te: 'trailers',
    });
    const chunks = [];
    let responseHeaders = {};
    let responseTrailers = {};
    let settled = false;
    const timer = setTimeout(() => reject(new Error(`export request timed out (cancel=${cancel})`)), 5_000);
    request.on('data', (chunk) => chunks.push(chunk));
    request.once('response', (headers) => { responseHeaders = headers; });
    const finish = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      client.close();
      resolve({ headers: responseHeaders, trailers: responseTrailers, body: Buffer.concat(chunks) });
    };
    request.once('error', (error) => {
      if (!cancel && !settled) reject(error);
    });
    request.once('trailers', (trailers) => {
      responseTrailers = trailers;
      if (!cancel) setImmediate(finish);
    });
    if (cancel) request.once('close', finish);
    request.once('finish', () => {
      if (cancel) request.close(http2.constants.NGHTTP2_CANCEL);
    });
    request.end(payload);
  });
}

async function snapshot(controlPort) {
  return new Promise((resolve, reject) => {
    const client = http2.connect(`http://127.0.0.1:${controlPort}`);
    client.once('error', reject);
    const request = client.request({ ':method': 'GET', ':path': '/snapshot' });
    const chunks = [];
    request.on('data', (chunk) => chunks.push(chunk));
    request.once('error', reject);
    request.once('end', () => {
      client.close();
      resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
    });
    request.end();
  });
}

async function waitForExit(child) {
  if (child.exitCode !== null) return child.exitCode;
  const [code, signal] = await once(child, 'exit');
  return code ?? 128 + (signal ? 1 : 0);
}

async function runScenario(mode) {
  await rm(OUTPUT, { force: true });
  const receiver = await spawnReceiver(mode);
  const canceled = await sendExport(receiver.port, spanExport(mode === 'baseline' ? '0102030405060708' : '1112131415161718'), true);
  if (mode === 'baseline') {
    const exitCode = await waitForExit(receiver.child);
    const durable = JSON.parse(await readFile(OUTPUT, 'utf8'));
    assert.equal(durable.stats.acceptedSpans, 1);
    assert.equal(durable.spans[0].spanId, '0102030405060708');
    assert.notEqual(exitCode, 0, 'pre-fix delayed cancellation must expose the invalid-stream failure');
    return { exitCode, canceled, durable, stderr: receiver.child.stderr.read()?.toString() ?? '' };
  }

  assert.equal(receiver.child.exitCode, null, 'fixed receiver must survive a canceled stream');
  const normal = await sendExport(receiver.port, spanExport('2122232425262728'), false);
  assert.equal(normal.headers[':status'], 200);
  assert.equal(normal.trailers['grpc-status'], '0');
  assert.deepEqual(normal.body, Buffer.from([0, 0, 0, 0, 0]));
  const control = await snapshot(receiver.controlPort);
  assert.equal(control.stats.acceptedSpans, 2);
  assert.deepEqual(control.spans.map((span) => span.spanId), [
    '1112131415161718',
    '2122232425262728',
  ]);
  receiver.child.kill('SIGTERM');
  await waitForExit(receiver.child);
  return { canceled, normal, control };
}

async function main() {
  const module = await import(pathToFileURL(RECEIVER).href);
  assert.throws(() => module.decodeGrpcFrames(Buffer.from([0, 0, 0, 0]), 8), /truncated gRPC frame header/);
  assert.throws(() => module.decodeGrpcFrames(Buffer.from([0, 0, 0, 0, 1]), 8), /invalid gRPC frame length/);
  assert.throws(() => module.decodeGrpcFrames(Buffer.alloc(9), 8), /request exceeds maxBytes/);

  const before = await runScenario('baseline');
  if (process.argv.includes('--before-only')) {
    assert(before);
    console.log(JSON.stringify({
      qa: 'otlp-receiver-lifecycle-before',
      preFixExitCode: before.exitCode,
      preFixAcceptedSpans: before.durable.stats.acceptedSpans,
    }));
    return;
  }
  const after = await runScenario('fixed');
  console.log(JSON.stringify({
    qa: 'otlp-receiver-lifecycle',
    preFixExitCode: before.exitCode,
    preFixAcceptedSpans: before.durable.stats.acceptedSpans,
    postFixAcceptedSpans: after.control.stats.acceptedSpans,
    postFixGrpcStatus: after.normal.trailers['grpc-status'],
  }));
}

if (process.argv[2] === '--child') {
  throw new Error('child mode is internal and must be launched with --input-type=module');
}

await main();
