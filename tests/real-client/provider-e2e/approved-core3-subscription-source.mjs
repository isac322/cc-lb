// Parent-only access projection. Importing this module performs no I/O.
// Never log the private return of readBrokerAccess(); it contains access-only material.
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';

const must = (ok, code) => { if (!ok) throw new Error(`BLOCKED:${code}`); };
const hash = (value) => createHash('sha256').update(value).digest('hex');
export const BROKER_SOURCE_CONTRACT = Object.freeze({
  context: 'runbear-local', namespace: 'llm-proxy', deployment: 'cliproxyapi', container: 'main',
  directory: '/root/.cli-proxy-api', metadata_observed_at: '2026-10-22T02:37:06Z',
  current_blockers: ['New binary GREEN + independent source-transport security review + explicit temporary-tool approval required before access retrieval', 'Last owner parser metadata lacked jq/python/node; current pod parser availability has not been probed', 'CLIProxyAPI v8.0.29 has no supported access-only projection endpoint or inspect command'],
  current_target_metadata_only: { pod: 'cliproxyapi-69d8bc8d88-mdzj5', pod_uid: '22039872-630b-4c56-bf61-d904cead0f47', container: 'main', arch: 'aarch64', source_or_secret_values_read: false },
  forbidden_routes: ['auth-files/download', 'credentials/download', 'api-call', 'auth-files/refresh', 'credentials/refresh', 'quota/fetch', 'login', 'token endpoint'],
  sources: {
    '07208b5b62c0': { kind: 'openai_oauth', type: 'codex', expiry_observed: '2026-10-25T12:46:55+08:00', account_hash12: '877e6240bb38' },
    '35da33587fc3': { kind: 'openai_oauth', type: 'codex', expiry_observed: '2026-10-27T14:24:10+08:00', account_hash12: '333503347af6' },
    'f1f6e505f9f2': { kind: 'openai_oauth', type: 'codex', expiry_observed: '2026-10-28T13:17:05+08:00', account_hash12: '434c618afb91' },
    efef7b1bc18c: { kind: 'gemini_oauth', type: 'antigravity', expiry_observed: '2026-10-22T11:18:12+08:00', project_present: true, base_url: 'https://daily-cloudcode-pa.googleapis.com' },
  },
  transport: 'Existing remote JSON parser selects access token, actual expiry, account/project before stdout; only parent memory receives stdout. No grant file or refresh/id-token crosses the container boundary.',
});

function route(options, authorization) {
  must(authorization?.parent_new_binary_green === true, 'parent_new_binary_green_required');
  must(authorization?.gateway_access_restored === true, 'broker_gateway_auth_blocked_no_retry');
  must(authorization?.allow_readonly_source_metadata === true, 'readonly_source_metadata_authorization_required');
  must(typeof options?.pod === 'string' && /^cliproxyapi-[a-z0-9]+-[a-z0-9]+$/.test(options.pod), 'current_owner_verified_broker_pod_required');
  must(typeof options.podUid === 'string' && /^[a-f0-9-]{36}$/.test(options.podUid), 'exact_owner_verified_broker_pod_uid_required');
  const target = BROKER_SOURCE_CONTRACT.current_target_metadata_only;
  must(options.pod === target.pod && options.podUid === target.pod_uid && options.arch === target.arch, 'broker_target_or_arch_differs_from_reviewed_handoff');
  const source = BROKER_SOURCE_CONTRACT.sources[options.sourceHash12];
  must(source && source.kind === options.kind, 'approved_source_hash_and_kind_required');
  return { source, prefix: ['--context', 'runbear-local', '-n', 'llm-proxy', 'exec', options.pod, '-c', 'main', '--'] };
}

// Error strings are fixed: kubectl stderr can contain credential-plugin output and is never propagated.
function captured(args, { input, limit = 131072 } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn('kubectl', args, { env: process.env, stdio: ['pipe', 'pipe', 'pipe'] });
    const stdout = [];
    let bytes = 0;
    let failed = false;
    const timer = setTimeout(() => { failed = true; child.kill('SIGKILL'); }, 20000);
    child.stdout.on('data', (chunk) => { bytes += chunk.length; if (bytes > limit) { failed = true; child.kill('SIGKILL'); } else stdout.push(chunk); });
    // Drain without storing or emitting plugin/broker diagnostics.
    child.stderr.on('data', () => {});
    child.once('error', () => { clearTimeout(timer); reject(new Error('BLOCKED:broker_readonly_command_unavailable')); });
    child.once('close', (code) => {
      clearTimeout(timer);
      if (failed || code !== 0) { stdout.forEach((chunk) => chunk.fill(0)); reject(new Error('BLOCKED:broker_readonly_command_failed_no_retry')); return; }
      const buffer = Buffer.concat(stdout);
      stdout.forEach((chunk) => chunk.fill(0));
      resolve(buffer);
    });
    child.stdin.on('error', () => {});
    child.stdin.end(input);
  });
}

async function verifyBrokerIdentity(options) {
  const shape = '{.metadata.uid}{"\\t"}{range .status.containerStatuses[?(@.name=="main")]}{.ready}{end}';
  const bytes = await captured(['--context', 'runbear-local', '-n', 'llm-proxy', 'get', 'pod', options.pod, `-o=jsonpath=${shape}`], { limit: 256 });
  const identity = bytes.toString('utf8'); bytes.fill(0);
  must(identity === `${options.podUid}\ttrue`, 'broker_pod_uid_or_ready_state_changed_new_target_approval_required');
  const archBytes = await captured(['--context', 'runbear-local', '-n', 'llm-proxy', 'exec', options.pod, '-c', 'main', '--', 'uname', '-m'], { limit: 128 });
  const arch = archBytes.toString('utf8').trim(); archBytes.fill(0);
  must(arch === options.arch && JQ_RELEASE.assets[arch], 'broker_arch_changed_or_official_asset_unavailable');
  if (options.projectionJq) must(options.projectionJq.sha256 === JQ_RELEASE.assets[arch].sha256, 'temporary_jq_sha_does_not_match_observed_arch');
}

export async function sourcePreflight(options, authorization) {
  const { prefix } = route(options, authorization);
  await verifyBrokerIdentity(options);
  if (options.projectionJq) {
    const tool = options.projectionJq;
    must(authorization.temporary_jq_user_approved === true, 'temporary_remote_jq_requires_separate_user_approval');
    must(/^\/tmp\/cc-lb-subscription-projection-[a-z0-9-]{8,64}\/jq$/.test(tool.path) && Object.values(JQ_RELEASE.assets).some((asset) => asset.sha256 === tool.sha256), 'temporary_jq_exact_owned_path_and_official_sha_required');
    const output = await captured([...prefix, '/bin/sh', '-c', `test -x '${tool.path}' && printf '%s  %s\\n' '${tool.sha256}' '${tool.path}' | sha256sum -c - >/dev/null && printf 'verified'`], { limit: 128 });
    const ok = output.toString('utf8') === 'verified'; output.fill(0);
    must(ok, 'temporary_jq_checksum_mismatch');
    return { source_hash12: options.sourceHash12, kind: options.kind, pod_uid: options.podUid, parser: 'jq', parser_path: tool.path, parser_sha256: tool.sha256, token_values_read: false, grant_copied: false, install_attempted: false, verdict: 'PARSER_AVAILABLE_NOT_READ' };
  }
  const command = 'for p in jq python3 python node; do if command -v "$p" >/dev/null 2>&1; then printf "%s\\n" "$p"; fi; done';
  const buffer = await captured([...prefix, '/bin/sh', '-c', command], { limit: 1024 });
  const names = buffer.toString('utf8').trim().split(/\r?\n/).filter(Boolean);
  buffer.fill(0);
  must(names.every((name) => ['jq', 'python3', 'python', 'node'].includes(name)), 'unexpected_parser_probe_output');
  return { source_hash12: options.sourceHash12, kind: options.kind, pod_uid: options.podUid, context: 'runbear-local', namespace: 'llm-proxy', container: 'main', available_parsers: names, parser: ['python3', 'python', 'node', 'jq'].find((name) => names.includes(name)) ?? null, token_values_read: false, grant_copied: false, install_attempted: false, verdict: names.length ? 'PARSER_AVAILABLE_NOT_READ' : 'BLOCKED_NO_EXISTING_JSON_PARSER' };
}

function pythonProjection(sourceHash12, type) {
  return [
    'import os,sys,json,hashlib,datetime',
    'try:',
    ` names=[n for n in os.listdir('/root/.cli-proxy-api') if n.endswith('.json') and hashlib.sha256(n.encode()).hexdigest()[:12]==${JSON.stringify(sourceHash12)}]`,
    ' if len(names)!=1: raise ValueError()',
    " p=os.path.join('/root/.cli-proxy-api',names[0])",
    ' if os.path.islink(p): raise ValueError()',
    " with open(p,'r') as f: d=json.load(f)",
    ` if d.get('type')!=${JSON.stringify(type)} or d.get('disabled') is not False: raise ValueError()`,
    " a=d.get('access_token'); e=d.get('expired')",
    ' if not isinstance(a,str) or not a.strip() or not isinstance(e,str): raise ValueError()',
    " dt=datetime.datetime.fromisoformat(e.replace('Z','+00:00'))",
    " if dt.tzinfo is None or dt.utcoffset() is None: raise ValueError()",
    " exp=int(dt.timestamp())",
    " out={'access_token':a,'expires_at_unix_secs':exp}",
    type === 'codex' ? " if not isinstance(d.get('account_id'),str) or not d['account_id']: raise ValueError()" : " if not isinstance(d.get('project_id'),str) or not d['project_id']: raise ValueError()",
    type === 'codex' ? " out['account_id']=d['account_id']" : " out['project_id']=d['project_id']",
    " sys.stdout.write(json.dumps(out,separators=(',',':')))",
    'except BaseException:',
    ' sys.exit(41)',
  ].join('\n');
}

function nodeProjection(sourceHash12, type) {
  return [
    'try {',
    " const fs=require('fs'),p=require('path'),c=require('crypto');",
    ` const names=fs.readdirSync('/root/.cli-proxy-api').filter(n=>n.endsWith('.json')&&c.createHash('sha256').update(n).digest('hex').slice(0,12)===${JSON.stringify(sourceHash12)});`,
    ' if(names.length!==1)throw 0;',
    " const file=p.join('/root/.cli-proxy-api',names[0]);if(fs.lstatSync(file).isSymbolicLink())throw 0;",
    " const d=JSON.parse(fs.readFileSync(file,'utf8'));",
    ` if(d.type!==${JSON.stringify(type)}||d.disabled!==false||typeof d.access_token!=='string'||!d.access_token.trim()||typeof d.expired!=='string')throw 0;`,
    " if(!d.expired.includes('T')||!/(?:Z|[+-]\\d{2}:\\d{2})$/.test(d.expired))throw 0;",
    ' const out={access_token:d.access_token,expires_at_unix_secs:Math.floor(Date.parse(d.expired)/1000)};',
    ` const id=${JSON.stringify(type === 'codex' ? 'account_id' : 'project_id')};`,
    " if(typeof d[id]!=='string'||!d[id])throw 0;out[id]=d[id];",
    ' if(!Number.isSafeInteger(out.expires_at_unix_secs))throw 0;',
    ' process.stdout.write(JSON.stringify(out));',
    '} catch { process.exit(41); }',
  ].join('\n');
}

function jqProjection(sourceHash12, type, executable = 'jq') {
  // Shell locates by basename hash only. jq, not regex/sed/grep, parses/selects JSON remotely.
  // jq retains the original ISO expiry; the parent normalizes it without altering its value.
  const field = type === 'codex' ? 'account_id' : 'project_id';
  const filter = `if .type == "${type}" and .disabled == false and (.access_token | type == "string") and (.expired | type == "string") and (.${field} | type == "string") then {access_token, actual_expiry_iso: .expired, ${field}} else error("invalid source") end`;
  return `selected=''; count=0; for f in /root/.cli-proxy-api/*.json; do [ -f "$f" ] && [ ! -L "$f" ] || continue; b=\${f##*/}; sum=$(printf '%s' "$b" | sha256sum) || exit 41; set -- $sum; case "$1" in ${sourceHash12}*) selected=$f; count=$((count + 1));; esac; done; [ "$count" -eq 1 ] || exit 41; '${executable}' -ce '${filter}' < "$selected" 2>/dev/null`;
}

export function validateAccessProjection(value, kind) {
  const allowed = new Set(['access_token', 'expires_at_unix_secs', 'account_id', 'project_id']);
  must(value && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).every((key) => allowed.has(key)), 'refresh_idtoken_or_unknown_projection_field');
  must(typeof value.access_token === 'string' && value.access_token.length >= 16 && value.access_token.trim() === value.access_token && !/[\r\n\0]/.test(value.access_token), 'invalid_access_projection');
  must(Number.isSafeInteger(value.expires_at_unix_secs) && value.expires_at_unix_secs > Math.floor(Date.now() / 1000) + 300, 'source_expired_or_insufficient_lifetime_reread_owner_only');
  must(['openai_oauth', 'gemini_oauth'].includes(kind), 'actual_oauth_kind_required');
  const id = kind === 'openai_oauth' ? 'account_id' : 'project_id';
  const forbidden = kind === 'openai_oauth' ? 'project_id' : 'account_id';
  must(typeof value[id] === 'string' && value[id].trim().length > 0 && value[forbidden] === undefined, 'source_identity_metadata_invalid');
  return value;
}

// Private access-only material is returned in memory, never printed or written to a file.
export async function readBrokerAccess(options, authorization, preflight) {
  const { source, prefix } = route(options, authorization);
  must(authorization?.allow_readonly_access_projection === true, 'parent_access_projection_authorization_required');
  must(authorization?.parent_security_review_passed === true, 'parent_source_transport_security_review_required');
  must(preflight?.source_hash12 === options.sourceHash12 && preflight.kind === options.kind && preflight.verdict === 'PARSER_AVAILABLE_NOT_READ', 'existing_remote_json_parser_required_no_download_fallback');
  must(preflight.pod_uid === options.podUid, 'broker_projection_target_changed_after_preflight');
  await verifyBrokerIdentity(options);
  const parser = preflight.parser;
  let args;
  if (parser === 'python3' || parser === 'python') args = [parser, '-c', pythonProjection(options.sourceHash12, source.type)];
  else if (parser === 'node') args = ['node', '-e', nodeProjection(options.sourceHash12, source.type)];
  else if (parser === 'jq') {
    const executable = preflight.parser_path ?? 'jq';
    must(executable === 'jq' || (authorization.temporary_jq_user_approved === true && options.projectionJq?.path === executable && options.projectionJq.sha256 === preflight.parser_sha256), 'temporary_jq_approval_or_path_changed');
    const verify = executable === 'jq' ? '' : `printf '%s  %s\\n' '${preflight.parser_sha256}' '${executable}' | sha256sum -c - >/dev/null || exit 41; `;
    args = ['/bin/sh', '-c', verify + jqProjection(options.sourceHash12, source.type, executable)];
  }
  else throw new Error('BLOCKED:no_existing_remote_json_parser_no_safe_supported_projection_route');
  const buffer = await captured([...prefix, ...args]);
  let value;
  try {
    value = JSON.parse(buffer.toString('utf8'));
    if (parser === 'jq') {
      const allowed = new Set(['access_token', 'actual_expiry_iso', 'account_id', 'project_id']);
      must(Object.keys(value).every((key) => allowed.has(key)) && typeof value.actual_expiry_iso === 'string', 'unexpected_remote_projection_field');
      must(value.actual_expiry_iso.includes('T') && /(?:Z|[+-]\d{2}:\d{2})$/.test(value.actual_expiry_iso), 'actual_expiry_timezone_required');
      value.expires_at_unix_secs = Math.floor(Date.parse(value.actual_expiry_iso) / 1000);
      delete value.actual_expiry_iso;
    }
    validateAccessProjection(value, options.kind);
    if (source.account_hash12) must(hash(value.account_id).slice(0, 12) === source.account_hash12, 'owner_account_hash_mismatch');
  } catch {
    throw new Error('BLOCKED:remote_access_projection_invalid_or_expired_no_secret_details');
  } finally {
    buffer.fill(0);
  }
  const evidence = {
    kind: options.kind, expires_at_unix_secs: value.expires_at_unix_secs,
    access_material_validated_in_memory: true, owner_identity_consistency_checked_in_memory: true,
    credential_derived_hashes_serialized: false,
    original_grant_copied: false, refresh_token_extracted: false, id_token_extracted: false,
    refresh_endpoint_calls_by_adapter: 0, provider_generation_calls_by_adapter: 0,
    signature_scope: 'Source/import intent only, not an observed cc-lb-to-provider wire header',
  };
  return { accessMaterial: value, evidence };
}

export function descriptorForAccess(kind, accessMaterial) {
  validateAccessProjection(accessMaterial, kind);
  return { kind, provenance: 'existing_shared_grant_read_only_access', access_token_present: true,
    refresh_token_present: false, refresh_expiry_metadata_present: false, never_refresh: true,
    access_expires_in_seconds: accessMaterial.expires_at_unix_secs - Math.floor(Date.now() / 1000),
    account_id_present: typeof accessMaterial.account_id === 'string', project_id_present: typeof accessMaterial.project_id === 'string',
    no_refresh_endpoint_calls: 0, source_grant_mutated: false, source_grant_copied: false };
}

// Design-only staging option. Returning this plan does not download, install, execute,
// create a pod, open a grant, or grant the additional operational write authorization.
export const JQ_RELEASE = Object.freeze({
  tag: 'jq-1.8.2', official_release: 'https://github.com/jqlang/jq/releases/tag/jq-1.8.2',
  checked_release_api: 'https://api.github.com/repos/jqlang/jq/releases/latest',
  checksums_url: 'https://github.com/jqlang/jq/releases/download/jq-1.8.2/sha256sum.txt',
  assets: {
    x86_64: { name: 'jq-linux-amd64', sha256: 'b1c22172dd303f3be49e935aa56aa48a8b7a46e0bc838b4997d3bb451495870f' },
    aarch64: { name: 'jq-linux-arm64', sha256: '8b85c817833814ddca00a144c33705546355afccf0cf39b188f3cdb48b852309' },
  },
});

export function safeProjectionOptionPlan({ pod, podUid, arch, runId }) {
  must(/^cliproxyapi-[a-z0-9]+-[a-z0-9]+$/.test(pod ?? ''), 'owner_verified_pod_required');
  must(/^[a-f0-9-]{36}$/.test(podUid ?? ''), 'owner_verified_pod_uid_required');
  must(/^[a-z0-9-]{8,64}$/.test(runId ?? ''), 'unique_parent_run_id_required');
  const asset = JQ_RELEASE.assets[arch];
  must(asset, 'observe_remote_uname_before_selecting_linux_asset');
  const target = BROKER_SOURCE_CONTRACT.current_target_metadata_only;
  must(pod === target.pod && podUid === target.pod_uid && arch === target.arch, 'staging_target_not_the_reviewed_uid_and_observed_arch');
  const directory = `/tmp/cc-lb-subscription-projection-${runId}`;
  const executable = `${directory}/jq`;
  const prefix = ['--context', 'runbear-local', '-n', 'llm-proxy', 'exec', '-i', pod, '-c', 'main', '--'];
  const shell = `umask 077; [ ! -L /tmp ] && [ -d /tmp ] && [ -w /tmp ] || exit 41; mkdir '${directory}' || exit 41; cat > '${executable}' || exit 41; printf '%s  %s\\n' '${asset.sha256}' '${executable}' | sha256sum -c - >/dev/null || exit 41; chmod 500 '${executable}' || exit 41`;
  const downloadUrl = `https://github.com/jqlang/jq/releases/download/${JQ_RELEASE.tag}/${asset.name}`;
  const script = [
    "// DESIGN ONLY. Parent must first obtain explicit approval for this exact pod/path/tool write.",
    "import {createHash} from 'node:crypto';",
    "import {spawn} from 'node:child_process';",
    "import {recordResource} from '/tmp/cc-lb-actual-cli-e2e-20261020/qa-runtime.mjs';",
    `const targetArgs=${JSON.stringify(['--context', 'runbear-local', '-n', 'llm-proxy', 'get', 'pod', pod, '-o=jsonpath={.metadata.uid}'])};`,
    "const uidChild=spawn('kubectl',targetArgs,{stdio:['ignore','pipe','pipe']});let uid='';uidChild.stdout.on('data',c=>{uid+=c;});uidChild.stderr.on('data',()=>{});",
    `await new Promise((resolve,reject)=>{uidChild.once('error',()=>reject(Error('target identity unavailable')));uidChild.once('close',code=>code===0&&uid===${JSON.stringify(podUid)}?resolve():reject(Error('approved pod UID changed')));});`,
    `const archChild=spawn('kubectl',${JSON.stringify([...prefix.filter((value) => value !== '-i'), 'uname', '-m'])},{stdio:['ignore','pipe','pipe']});let observedArch='';archChild.stdout.on('data',c=>{observedArch+=c;});archChild.stderr.on('data',()=>{});`,
    `await new Promise((resolve,reject)=>{archChild.once('error',()=>reject(Error('target arch unavailable')));archChild.once('close',code=>code===0&&observedArch.trim()===${JSON.stringify(arch)}?resolve():reject(Error('approved pod arch changed')));});`,
    `const mountArgs=${JSON.stringify(['--context', 'runbear-local', '-n', 'llm-proxy', 'get', 'pod', pod, '-o=jsonpath={range .spec.containers[?(@.name=="main")].volumeMounts[*]}{.mountPath}{"\\n"}{end}'])};`,
    "const mountsChild=spawn('kubectl',mountArgs,{stdio:['ignore','pipe','pipe']});let mounts='';mountsChild.stdout.on('data',c=>{mounts+=c;});mountsChild.stderr.on('data',()=>{});",
    "await new Promise((resolve,reject)=>{mountsChild.once('error',()=>reject(Error('mount metadata unavailable')));mountsChild.once('close',code=>code===0&&!mounts.split(/\\r?\\n/).some(p=>p==='/'||p==='/tmp'||p.startsWith('/tmp/'))?resolve():reject(Error('tmp is mounted or metadata unavailable; writable-layer-only staging blocked')));});",
    `const response=await fetch(${JSON.stringify(downloadUrl)});if(!response.ok)throw Error('download failed');`,
    "const bytes=Buffer.from(await response.arrayBuffer());",
    `if(createHash('sha256').update(bytes).digest('hex')!==${JSON.stringify(asset.sha256)})throw Error('official jq SHA mismatch');`,
    `await recordResource({name:'subscription-projection-tool'},{kind:'remote_scratch_directory',id:${JSON.stringify(`runbear-local/llm-proxy/${pod}/main/uid=${podUid}:${directory}`)},pod_uid:${JSON.stringify(podUid)},arch:${JSON.stringify(arch)},tool_sha256:${JSON.stringify(asset.sha256)},delete:${JSON.stringify(`After explicit cleanup signal and exact pod UID/ownership/checksum verification: kubectl --context runbear-local -n llm-proxy exec ${pod} -c main -- rm -rf ${directory}`)},note:'Only official pinned jq in writable /tmp; no grant/PVC/config/service mutation'});`,
    `const child=spawn('kubectl',${JSON.stringify([...prefix, '/bin/sh', '-c', shell])},{stdio:['pipe','pipe','pipe']});`,
    "child.stdout.on('data',()=>{});child.stderr.on('data',()=>{});child.stdin.end(bytes);",
    "await new Promise((resolve,reject)=>{child.once('error',()=>reject(Error('staging unavailable')));child.once('close',code=>code===0?resolve():reject(Error('staging failed; retain ledger entry')));});",
    "bytes.fill(0);console.log('Verified jq staging only; no credential extraction executed.');",
  ].join('\n');
  return { state: 'DESIGN_ONLY_NOT_AUTHORIZED_NOT_RUN', exact_additional_scope: { context: 'runbear-local', namespace: 'llm-proxy', pod, pod_uid: podUid, arch, container: 'main', directory, effects: ['create private directory in existing container writable layer', 'stream one pinned official static jq executable into it', 'read-only future source projection after separate parent green gates'], excluded: ['PVC/config writes', 'package installation', 'service restart', 'grant copy/refresh', 'helper pod creation'] },
    remote_arch_readonly_fact: 'kubectl --context runbear-local -n llm-proxy exec deploy/cliproxyapi -c main -- uname -m',
    additional_readonly_facts_before_approval: ['Inspect selected pod spec volumeMounts; reject /tmp or its ancestors/subpaths on shared/PVC/config mounts', 'test -d /tmp && test ! -L /tmp && test -w /tmp; no write test before approval', 'Owner verifies current pod identity and exact checksum; changed pod invalidates tool approval'],
    asset: { ...asset, download_url: downloadUrl, checksums_url: JQ_RELEASE.checksums_url }, projectionJq: { path: executable, sha256: asset.sha256 },
    exact_staging_script: script, projection_implementation: 'readBrokerAccess() chooses by basename hash and remote-selects access/actual expiry/account/project only; never run a projection command manually into a tool or terminal stdout',
    projection_scripts_design_only: Object.fromEntries(Object.entries(BROKER_SOURCE_CONTRACT.sources).map(([sourceHash12, source]) => [sourceHash12, jqProjection(sourceHash12, source.type, executable)])),
    safer_than_helper_pod: ['No new pod/PVC attachment, service account or helper image; uses already-authorized pods/exec only', 'Existing broker container still gets a temporary executable, so this is an operational write requiring separate explicit user approval'],
    readonly_pvc_helper_alternative: { authorization: 'Separate pods/create + pods/exec + PVC get/use, admission and image approval required; never an RBAC workaround', pvc: 'cliproxyapi', last_observed_access_mode: 'ReadWriteOnce', tradeoffs: ['RWO permits multiple pods only on the same node when supported; another node generally causes Multi-Attach failure', 'Read-only volumeMount does not by itself guarantee CSI can attach a second pod; do not assume RWX', 'Same-node affinity and readOnly PVC+volumeMount+mount propagation policy must be validated before considering a helper', 'Larger attack surface and operational scope: helper image, service account, pod, network policy and cleanup; would need explicit approval and ledger before creation'] } };
}
