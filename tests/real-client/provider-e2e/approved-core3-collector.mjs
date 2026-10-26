#!/usr/bin/env node
// Parent-run only. Reuse existing bounded loopback OTLP decoder; extend only nonsecret correlation IDs.
// Original receiver and old captures remain unchanged. No listener/process is started by the author.
import { createHash } from 'node:crypto';
import { readFile, lstat } from 'node:fs/promises';
import path from 'node:path';
const BASE = '/tmp/cc-lb-actual-cli-e2e-20261020/final-gpt-direct-base-gpt30w-20251025';
const must = (ok, code) => { if (!ok) throw new Error(`BLOCKED:${code}`); };
const hash = (v) => createHash('sha256').update(v).digest('hex');
const o = {};
for (let i = 2; i < process.argv.length; i += 2) {
  const flag = process.argv[i]; const value = process.argv[i + 1];
  must(['--epoch', '--port', '--control-port', '--output'].includes(flag) && value, 'invalid_collector_argument');
  o[flag] = value;
}
const epoch = path.resolve(o['--epoch'] ?? '');
must(path.dirname(epoch) === BASE && /^fixed-[0-9a-f]{12}-core3(?:-[a-z0-9-]{1,16})?$/.test(path.basename(epoch)), 'new_core3_epoch_required');
const manifest = JSON.parse(await readFile(path.join(epoch, 'fixed-epoch.json'), 'utf8'));
const source = await readFile(path.join(BASE, 'trace-proof/otlp-receiver.mjs'), 'utf8');
must(hash(source) === manifest.approved_core3?.source_sha256?.['trace-proof/otlp-receiver.mjs'], 'receiver_source_not_pinned');
const output = path.resolve(o['--output'] ?? path.join(epoch, 'approved-core3-spans.json'));
must(path.dirname(output) === epoch && /^approved-core3-[a-z0-9-]+\.json$/.test(path.basename(output)), 'fresh_capture_path_required');
let exists = false; try { await lstat(output); exists = true; } catch (e) { if (e.code !== 'ENOENT') throw e; }
must(!exists, 'capture_already_exists');
const port = Number(o['--port'] ?? 18563 + manifest.port_offset);
const controlPort = Number(o['--control-port'] ?? 18564 + manifest.port_offset);
must(port === 18563 + manifest.port_offset && controlPort === 18564 + manifest.port_offset, 'manifest_listener_ports_required');
const anchor = "  'cc_lb.request.id',";
must(source.split(anchor).length === 2, 'receiver_allowlist_source_drift');
const main = "\nif (path.resolve(process.argv[1] ?? '') === fileURLToPath(import.meta.url)) {";
must(source.split(main).length === 2, 'receiver_main_source_drift');
const moduleText = source.slice(0, source.indexOf(main)).replace(anchor,
  `${anchor}\n  'event.id',\n  'stream_id',\n  'cc_lb.event.id',\n  'gen_ai.response.id',\n  'cc_lb.responses.connection_id',\n  'cc_lb.responses.stream_id',\n  'gen_ai.request.stream_id',`);
const module = await import(`data:text/javascript;base64,${Buffer.from(moduleText).toString('base64')}`);
const receiver = await module.startOtlpReceiver({ port, controlPort, outputFile: output });
console.log(`OTLP_READY port=${receiver.port} controlPort=${receiver.controlPort} output=${receiver.outputFile} source_sha256=${hash(source)} correlation_ids_only=true`);
let stopping = false;
const stop = async (signal) => {
  if (stopping) return; stopping = true;
  try { await receiver.close(); } finally { process.exit(signal === 'SIGINT' ? 130 : 143); }
};
process.once('SIGINT', () => { void stop('SIGINT'); });
process.once('SIGTERM', () => { void stop('SIGTERM'); });
