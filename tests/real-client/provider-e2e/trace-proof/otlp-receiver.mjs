#!/usr/bin/env node

import http2 from 'node:http2';
import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const LOOPBACK = '127.0.0.1';
export const DEFAULT_MAX_BYTES = 8 * 1024 * 1024;
export const MAX_SPANS = 10_000;
const MAX_STRING_BYTES = 4 * 1024;
const MAX_ATTRIBUTE_KEY_BYTES = 256;
const ACK_FRAME = Buffer.alloc(5);

// Field numbers are from the canonical OpenTelemetry proto sources. These are
// intentionally kept here instead of relying on generated code/dependencies:
// https://github.com/open-telemetry/opentelemetry-proto/blob/v1.9.0/opentelemetry/proto/trace/v1/trace.proto
// https://github.com/open-telemetry/opentelemetry-proto/blob/v1.9.0/opentelemetry/proto/collector/trace/v1/trace_service.proto
export const OTLP_PROTO_FIELD_CONTRACT = Object.freeze({
  ExportTraceServiceRequest: Object.freeze({ resourceSpans: 1 }),
  ResourceSpans: Object.freeze({ resource: 1, scopeSpans: 2, schemaUrl: 3 }),
  ScopeSpans: Object.freeze({ scope: 1, spans: 2, schemaUrl: 3 }),
  Resource: Object.freeze({ attributes: 1, droppedAttributesCount: 2 }),
  InstrumentationScope: Object.freeze({ name: 1, version: 2, attributes: 3 }),
  Span: Object.freeze({
    traceId: 1,
    spanId: 2,
    traceState: 3,
    parentSpanId: 4,
    name: 5,
    kind: 6,
    startTimeUnixNano: 7,
    endTimeUnixNano: 8,
    attributes: 9,
    status: 15,
    flags: 16,
  }),
  Status: Object.freeze({ code: 1, message: 2 }),
  KeyValue: Object.freeze({ key: 1, value: 2 }),
  AnyValue: Object.freeze({
    stringValue: 1,
    boolValue: 2,
    intValue: 3,
    doubleValue: 4,
    bytesValue: 5,
    arrayValue: 6,
    kvlistValue: 7,
  }),
});

const TRACE_ATTRIBUTE_ALLOWLIST = new Set([
  'otel.name',
  'otel.kind',
  'otel.status_code',
  'http.request.method',
  'http.route',
  'http.response.status_code',
  'url.path',
  'cc_lb.request.id',
  'cc_lb.upstream.id',
  'cc_lb.upstream.bulkhead_wait_ms',
  'cc_lb.upstream.dns_ms',
  'cc_lb.upstream.connect_ms',
  'cc_lb.upstream.connection_reused',
  'cc_lb.upstream.shape_ms',
  'cc_lb.upstream.sign_ms',
  'cc_lb.upstream.ttfb_ms',
  'error.type',
]);

const RESOURCE_ALLOWLIST = new Set(['service.name', 'service.version']);

class ProtoError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ProtoError';
  }
}

function asBuffer(value) {
  return Buffer.isBuffer(value) ? value : Buffer.from(value);
}

function readVarint(buffer, offset) {
  let value = 0n;
  let shift = 0n;
  let cursor = offset;
  for (let count = 0; count < 10; count += 1) {
    if (cursor >= buffer.length) throw new ProtoError('truncated varint');
    const byte = buffer[cursor];
    cursor += 1;
    const payload = BigInt(byte & 0x7f);
    if (count === 9 && payload > 1n) throw new ProtoError('varint exceeds 64 bits');
    value |= payload << shift;
    if ((byte & 0x80) === 0) return { value, offset: cursor };
    shift += 7n;
  }
  throw new ProtoError('varint exceeds 64 bits');
}

function readLengthDelimited(buffer, offset) {
  const lengthResult = readVarint(buffer, offset);
  const length = Number(lengthResult.value);
  if (!Number.isSafeInteger(length) || length < 0) throw new ProtoError('invalid length');
  const start = lengthResult.offset;
  const end = start + length;
  if (end > buffer.length) throw new ProtoError('truncated length-delimited field');
  return { value: buffer.subarray(start, end), offset: end };
}

function readFixed64(buffer, offset) {
  if (offset + 8 > buffer.length) throw new ProtoError('truncated fixed64');
  let value = 0n;
  for (let index = 0; index < 8; index += 1) value |= BigInt(buffer[offset + index]) << BigInt(index * 8);
  return { value, offset: offset + 8 };
}

function skipField(buffer, offset, wireType) {
  if (wireType === 0) return readVarint(buffer, offset).offset;
  if (wireType === 1) return offset + 8 <= buffer.length ? offset + 8 : (() => { throw new ProtoError('truncated fixed64'); })();
  if (wireType === 2) return readLengthDelimited(buffer, offset).offset;
  if (wireType === 5) return offset + 4 <= buffer.length ? offset + 4 : (() => { throw new ProtoError('truncated fixed32'); })();
  throw new ProtoError(`unsupported wire type ${wireType}`);
}

function text(bytes, limit = MAX_STRING_BYTES) {
  if (bytes.length > limit) throw new ProtoError('string exceeds limit');
  return bytes.toString('utf8').replace(/[\u0000-\u001f\u007f]/g, '');
}

function signedInt64(value) {
  return value >= (1n << 63n) ? value - (1n << 64n) : value;
}

function integerValue(value) {
  const signed = signedInt64(value);
  return signed >= BigInt(Number.MIN_SAFE_INTEGER) && signed <= BigInt(Number.MAX_SAFE_INTEGER)
    ? Number(signed)
    : signed.toString();
}

function parseAnyValue(buffer) {
  let offset = 0;
  let result;
  while (offset < buffer.length) {
    const key = readVarint(buffer, offset);
    offset = key.offset;
    const field = Number(key.value >> 3n);
    const wireType = Number(key.value & 7n);
    if (field === 1 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      result = text(part.value);
      offset = part.offset;
    } else if (field === 2 && wireType === 0) {
      const value = readVarint(buffer, offset);
      result = value.value !== 0n;
      offset = value.offset;
    } else if (field === 3 && wireType === 0) {
      const value = readVarint(buffer, offset);
      result = integerValue(value.value);
      offset = value.offset;
    } else if (field === 4 && wireType === 1) {
      if (offset + 8 > buffer.length) throw new ProtoError('truncated fixed64');
      const number = new DataView(buffer.buffer, buffer.byteOffset + offset, 8).getFloat64(0, true);
      result = Number.isFinite(number) ? number : undefined;
      offset += 8;
    } else {
      offset = skipField(buffer, offset, wireType);
    }
  }
  return result;
}

function parseKeyValue(buffer) {
  let offset = 0;
  let key;
  let value;
  while (offset < buffer.length) {
    const fieldKey = readVarint(buffer, offset);
    offset = fieldKey.offset;
    const field = Number(fieldKey.value >> 3n);
    const wireType = Number(fieldKey.value & 7n);
    if (field === 1 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      key = text(part.value, MAX_ATTRIBUTE_KEY_BYTES);
      offset = part.offset;
    } else if (field === 2 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      value = parseAnyValue(part.value);
      offset = part.offset;
    } else {
      offset = skipField(buffer, offset, wireType);
    }
  }
  return { key, value };
}

function parseAttributes(buffer, allowlist) {
  const attributes = {};
  let offset = 0;
  while (offset < buffer.length) {
    const fieldKey = readVarint(buffer, offset);
    offset = fieldKey.offset;
    const field = Number(fieldKey.value >> 3n);
    const wireType = Number(fieldKey.value & 7n);
    if (field === 1 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      const item = parseKeyValue(part.value);
      if (item.key && allowlist.has(item.key) && item.value !== undefined) attributes[item.key] = item.value;
      offset = part.offset;
    } else {
      offset = skipField(buffer, offset, wireType);
    }
  }
  return attributes;
}

function parseResource(buffer) {
  const attrs = parseAttributes(buffer, RESOURCE_ALLOWLIST);
  const resource = {};
  if (typeof attrs['service.name'] === 'string') resource.serviceName = attrs['service.name'];
  if (typeof attrs['service.version'] === 'string') resource.serviceVersion = attrs['service.version'];
  return Object.keys(resource).length > 0 ? resource : undefined;
}

function parseStatus(buffer) {
  let offset = 0;
  let code;
  let message;
  while (offset < buffer.length) {
    const fieldKey = readVarint(buffer, offset);
    offset = fieldKey.offset;
    const field = Number(fieldKey.value >> 3n);
    const wireType = Number(fieldKey.value & 7n);
    if (field === 1 && wireType === 0) {
      const value = readVarint(buffer, offset);
      code = Number(value.value);
      offset = value.offset;
    } else if (field === 2 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      message = text(part.value);
      offset = part.offset;
    } else {
      offset = skipField(buffer, offset, wireType);
    }
  }
  if (code === undefined && message === undefined) return undefined;
  return message === undefined ? { code: code ?? 0 } : { code: code ?? 0, message };
}

function parseSpan(buffer, resource) {
  let offset = 0;
  const span = {};
  const attributes = {};
  while (offset < buffer.length) {
    const fieldKey = readVarint(buffer, offset);
    offset = fieldKey.offset;
    const field = Number(fieldKey.value >> 3n);
    const wireType = Number(fieldKey.value & 7n);
    if (field === 1 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      if (part.value.length === 16) span.traceId = part.value.toString('hex');
      offset = part.offset;
    } else if (field === 2 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      if (part.value.length === 8) span.spanId = part.value.toString('hex');
      offset = part.offset;
    } else if (field === 4 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      if (part.value.length === 8) span.parentSpanId = part.value.toString('hex');
      offset = part.offset;
    } else if (field === 5 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      span.name = text(part.value);
      offset = part.offset;
    } else if (field === 6 && wireType === 0) {
      const value = readVarint(buffer, offset);
      span.kind = Number(value.value);
      offset = value.offset;
    } else if (field === 7 && wireType === 1) {
      const value = readFixed64(buffer, offset);
      span.startTimeUnixNano = value.value.toString();
      offset = value.offset;
    } else if (field === 8 && wireType === 1) {
      const value = readFixed64(buffer, offset);
      span.endTimeUnixNano = value.value.toString();
      offset = value.offset;
    } else if (field === 9 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      const item = parseKeyValue(part.value);
      if (item.key && TRACE_ATTRIBUTE_ALLOWLIST.has(item.key) && item.value !== undefined) attributes[item.key] = item.value;
      offset = part.offset;
    } else if (field === 15 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      span.status = parseStatus(part.value);
      offset = part.offset;
    } else {
      offset = skipField(buffer, offset, wireType);
    }
  }
  if (!span.traceId || !span.spanId || !span.name) return undefined;
  span.attributes = attributes;
  if (resource) span.resource = resource;
  return span;
}

function parseScopeSpans(buffer, resource) {
  let offset = 0;
  const spans = [];
  while (offset < buffer.length) {
    const fieldKey = readVarint(buffer, offset);
    offset = fieldKey.offset;
    const field = Number(fieldKey.value >> 3n);
    const wireType = Number(fieldKey.value & 7n);
    if (field === 2 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      const span = parseSpan(part.value, resource);
      if (span) spans.push(span);
      offset = part.offset;
    } else {
      offset = skipField(buffer, offset, wireType);
    }
  }
  return spans;
}

function parseResourceSpans(buffer) {
  let offset = 0;
  let resource;
  const scopePayloads = [];
  while (offset < buffer.length) {
    const fieldKey = readVarint(buffer, offset);
    offset = fieldKey.offset;
    const field = Number(fieldKey.value >> 3n);
    const wireType = Number(fieldKey.value & 7n);
    if (field === 1 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      resource = parseResource(part.value);
      offset = part.offset;
    } else if (field === 2 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      scopePayloads.push(part.value);
      offset = part.offset;
    } else {
      offset = skipField(buffer, offset, wireType);
    }
  }
  return scopePayloads.flatMap((payload) => parseScopeSpans(payload, resource));
}

export function decodeExportTraceServiceRequest(payload) {
  const buffer = asBuffer(payload);
  const spans = [];
  let offset = 0;
  while (offset < buffer.length) {
    const fieldKey = readVarint(buffer, offset);
    offset = fieldKey.offset;
    const field = Number(fieldKey.value >> 3n);
    const wireType = Number(fieldKey.value & 7n);
    if (field === 1 && wireType === 2) {
      const part = readLengthDelimited(buffer, offset);
      spans.push(...parseResourceSpans(part.value));
      offset = part.offset;
    } else {
      offset = skipField(buffer, offset, wireType);
    }
  }
  return spans;
}

export function decodeGrpcFrames(payload, maxBytes = DEFAULT_MAX_BYTES) {
  const buffer = asBuffer(payload);
  if (buffer.length > maxBytes) throw new ProtoError('request exceeds maxBytes');
  const messages = [];
  let offset = 0;
  while (offset < buffer.length) {
    if (buffer.length - offset < 5) throw new ProtoError('truncated gRPC frame header');
    const compressed = buffer[offset];
    const length = buffer.readUInt32BE(offset + 1);
    offset += 5;
    if (compressed !== 0) throw new ProtoError('compressed gRPC frames are not supported');
    if (length > maxBytes || length > buffer.length - offset) throw new ProtoError('invalid gRPC frame length');
    messages.push(buffer.subarray(offset, offset + length));
    offset += length;
  }
  return messages;
}

function emptySnapshot() {
  return {
    version: 1,
    spans: [],
    stats: { exportRequests: 0, acceptedSpans: 0, rejectedSpans: 0, decodeErrors: 0, oversizedRequests: 0 },
  };
}

function cloneSnapshot(snapshot) {
  return JSON.parse(JSON.stringify(snapshot));
}

async function atomicWriteJson(filePath, value) {
  const directory = path.dirname(filePath);
  const temporary = path.join(directory, `.${path.basename(filePath)}.${process.pid}.${crypto.randomUUID()}.tmp`);
  const content = `${JSON.stringify(value)}\n`;
  const handle = await fs.open(temporary, 'wx', 0o600);
  try {
    await handle.writeFile(content, 'utf8');
    await handle.sync();
  } finally {
    await handle.close();
  }
  await fs.chmod(temporary, 0o600);
  await fs.rename(temporary, filePath);
}

function parsePort(value, flag) {
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error(`${flag} must be an integer from 1 to 65535`);
  return port;
}

function parseArgs(argv) {
  const args = { maxBytes: DEFAULT_MAX_BYTES };
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (flag === '--port') args.port = parsePort(value, flag);
    else if (flag === '--control-port') args.controlPort = parsePort(value, flag);
    else if (flag === '--output') args.outputFile = value;
    else if (flag === '--max-bytes') {
      const maxBytes = Number(value);
      if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) throw new Error('--max-bytes must be a positive safe integer');
      args.maxBytes = maxBytes;
    } else if (flag === '--help') {
      console.log('Usage: otlp-receiver.mjs --port PORT --control-port PORT --output FILE [--max-bytes BYTES]');
      process.exit(0);
    } else {
      throw new Error(`unknown argument: ${flag}`);
    }
    index += 1;
  }
  if (!args.port || !args.controlPort || !args.outputFile) throw new Error('--port, --control-port, and --output are required');
  return args;
}

export async function startOtlpReceiver({ port, controlPort, outputFile, maxBytes = DEFAULT_MAX_BYTES }) {
  const otlpPort = parsePort(port, 'port');
  const control = parsePort(controlPort, 'controlPort');
  if (otlpPort === control) throw new Error('port and controlPort must differ');
  if (typeof outputFile !== 'string' || outputFile.length === 0) throw new Error('outputFile is required');
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) throw new Error('maxBytes must be a positive safe integer');

  let snapshot = emptySnapshot();
  let writeChain = Promise.resolve();
  const persist = () => {
    const copy = cloneSnapshot(snapshot);
    writeChain = writeChain.then(() => atomicWriteJson(outputFile, copy));
    return writeChain;
  };
  await persist();

  const recordExport = (body) => {
    snapshot.stats.exportRequests += 1;
    let messages;
    try {
      messages = decodeGrpcFrames(body, maxBytes);
    } catch (error) {
      snapshot.stats.decodeErrors += 1;
      if (error?.message === 'request exceeds maxBytes') snapshot.stats.oversizedRequests += 1;
      return { ok: false, status: error?.message === 'request exceeds maxBytes' ? 8 : 13 };
    }
    const incoming = [];
    try {
      for (const message of messages) incoming.push(...decodeExportTraceServiceRequest(message));
    } catch {
      snapshot.stats.decodeErrors += 1;
      return { ok: false, status: 13 };
    }
    if (snapshot.spans.length + incoming.length > MAX_SPANS) {
      snapshot.stats.rejectedSpans += incoming.length;
      return { ok: false, status: 8 };
    }
    snapshot.spans.push(...incoming);
    snapshot.stats.acceptedSpans += incoming.length;
    return { ok: true, status: 0 };
  };

  const receiveServer = http2.createServer();
  receiveServer.on('stream', (stream, headers) => {
    const method = headers[':method'];
    const requestPath = headers[':path'];
    const contentType = String(headers['content-type'] ?? '');
    if (method !== 'POST' || requestPath !== '/opentelemetry.proto.collector.trace.v1.TraceService/Export' || !contentType.startsWith('application/grpc')) {
      stream.respond({ ':status': 404, 'content-type': 'application/grpc', 'grpc-status': '12' });
      stream.end(ACK_FRAME);
      return;
    }
    const chunks = [];
    let total = 0;
    let oversized = false;
    stream.on('data', (chunk) => {
      if (oversized) return;
      total += chunk.length;
      if (total > maxBytes) {
        oversized = true;
        return;
      }
      chunks.push(chunk);
    });
    stream.on('error', () => {});
    stream.on('end', async () => {
      const result = oversized ? (() => {
        snapshot.stats.exportRequests += 1;
        snapshot.stats.oversizedRequests += 1;
        snapshot.stats.decodeErrors += 1;
        return { ok: false, status: 8 };
      })() : recordExport(Buffer.concat(chunks, total));
      await persist();
      if (stream.destroyed) return;
      const responseHeaders = {
        ':status': 200,
        'content-type': 'application/grpc',
        'grpc-encoding': 'identity',
        trailer: 'grpc-status',
      };
      stream.once('wantTrailers', () => {
        if (stream.destroyed) return;
        try {
          stream.sendTrailers({ 'grpc-status': String(result.status) });
        } catch (error) {
          if (error?.code !== 'ERR_HTTP2_INVALID_STREAM') throw error;
        }
      });
      try {
        stream.respond(responseHeaders, { waitForTrailers: true });
        stream.end(result.ok ? ACK_FRAME : undefined);
      } catch (error) {
        if (error?.code !== 'ERR_HTTP2_INVALID_STREAM') throw error;
      }
    });
  });

  const controlServer = http2.createServer();
  controlServer.on('stream', (stream, headers) => {
    if (headers[':method'] !== 'GET' || headers[':path'] !== '/snapshot') {
      stream.respond({ ':status': 404, 'content-type': 'application/json' });
      stream.end('{}');
      return;
    }
    const body = Buffer.from(JSON.stringify(snapshot));
    stream.respond({ ':status': 200, 'content-type': 'application/json', 'content-length': String(body.length) });
    stream.end(body);
  });

  await Promise.all([
    new Promise((resolve, reject) => { receiveServer.once('error', reject); receiveServer.listen(otlpPort, LOOPBACK, resolve); }),
    new Promise((resolve, reject) => { controlServer.once('error', reject); controlServer.listen(control, LOOPBACK, resolve); }),
  ]);

  let closed = false;
  return {
    port: otlpPort,
    controlPort: control,
    outputFile,
    maxBytes,
    snapshot: () => cloneSnapshot(snapshot),
    close: async () => {
      if (closed) return;
      closed = true;
      await Promise.all([
        new Promise((resolve) => receiveServer.close(() => resolve())),
        new Promise((resolve) => controlServer.close(() => resolve())),
      ]);
      await writeChain;
    },
  };
}

if (path.resolve(process.argv[1] ?? '') === fileURLToPath(import.meta.url)) {
  try {
    const options = parseArgs(process.argv.slice(2));
    const receiver = await startOtlpReceiver(options);
    console.log(`OTLP_READY port=${receiver.port} controlPort=${receiver.controlPort} output=${receiver.outputFile} maxBytes=${receiver.maxBytes}`);
    const shutdown = async (signal) => {
      try { await receiver.close(); } finally { process.exit(signal === 'SIGINT' ? 130 : 143); }
    };
    process.once('SIGINT', () => { void shutdown('SIGINT'); });
    process.once('SIGTERM', () => { void shutdown('SIGTERM'); });
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
