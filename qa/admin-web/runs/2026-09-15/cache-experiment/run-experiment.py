#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRATCH = Path('/data/tmp/cc-lb-admin-web-qa-evidence/cache-experiment')
CREDENTIALS = SCRATCH / 'credentials'
PROOF_DIR = SCRATCH / 'condition-proof'
RAW = SCRATCH / 'raw.jsonl'
RESULT = SCRATCH / 'result-summary.json'
MANIFEST = SCRATCH / 'manifest.json'
APP_LOG = SCRATCH / 'app.log'
CATALOG = json.loads((SCRATCH / 'relation-catalog.json').read_text())
FIXTURE = json.loads((SCRATCH / 'measurement-fixture.json').read_text())
PROTOCOL = json.loads((SCRATCH / 'protocol.json').read_text())
BINARY = Path('/data/tmp/cc-lb-admin-web-qa-completion/target/debug/cc-lb')
HELPER = Path('/data/tmp/cc-lb-admin-web-qa-completion/crates/cc-lb-admin/web/qa/admin-web-cache-control.py')
PG_CONTAINER = 'cc-lb-cache-experiment-pg'
PG_VOLUME = 'cc-lb-cache-experiment-pgdata'
PG_IMAGE = 'postgres:16'
PY_IMAGE = 'python:3.12-slim'
ADMIN_HOST = '127.0.0.1'
ADMIN_PORT = 53272
DB_PORT = 53279
DB_NAME = 'admin_web_qa'
DB_USER = 'admin_web_qa'
APP_USER = 'admin_web_qa'
PRINCIPAL = '00000000-0000-4000-8000-000000000005'
BASE = f'/admin/v1/principals/{PRINCIPAL}/cache-keepalive'
ENDPOINTS = {
    'summary': BASE + '?limit=0&horizon=all&status=all',
    'list': BASE + '?limit=50&horizon=all&status=all',
    'detail': BASE + '/perf-detail-session',
}
COLD_SAMPLES_PER_PHASE = 8
WARM_SAMPLES = 30
TARGET_RELATIONS = {
    'cache_keepalive_sessions', 'cache_keepalive_decisions', 'cache_keepalive_turns',
    'principals_v1', 'upstream_spec_v1', 'upstream_api_key_secret_v1',
    'upstream_oauth_token_v1', 'upstream_status_v1', 'request_events_v1',
}
APP: subprocess.Popen[bytes] | None = None
APP_LOG_HANDLE = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def run(args: list[str], *, input_text: str | None = None, timeout: float = 120.0, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(args, input=input_text, text=True, capture_output=True, timeout=timeout)
    if check and result.returncode != 0:
        raise RuntimeError(f"command failed rc={result.returncode}: {args[0]} {args[1] if len(args)>1 else ''}; stderr_sha256={sha256_bytes(result.stderr.encode())}")
    return result


def docker_exists() -> bool:
    return run(['docker', 'container', 'inspect', PG_CONTAINER], check=False).returncode == 0


def wait_port(port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket() as sock:
            sock.settimeout(0.25)
            try:
                sock.connect(('127.0.0.1', port))
                return
            except OSError:
                time.sleep(0.05)
    raise RuntimeError(f'port {port} did not become ready')


def start_pg() -> None:
    if docker_exists():
        raise RuntimeError('owned PostgreSQL container unexpectedly already exists')
    args = [
        'docker', 'run', '-d', '--rm', '--name', PG_CONTAINER,
        '--label', 'cc-lb.qa.owner=cache-experiment',
        '--env-file', str(SCRATCH / 'pg.env'),
        '-p', f'127.0.0.1:{DB_PORT}:5432',
        '-v', f'{PG_VOLUME}:/var/lib/postgresql/data',
        PG_IMAGE, 'postgres',
        '-c', 'shared_preload_libraries=pg_stat_statements',
        '-c', 'track_io_timing=on',
        '-c', 'shared_buffers=128MB',
        '-c', 'max_connections=30',
    ]
    run(args)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        ready = run(['docker', 'exec', PG_CONTAINER, 'pg_isready', '-U', DB_USER, '-d', DB_NAME], check=False, timeout=5)
        if ready.returncode == 0:
            return
        time.sleep(0.1)
    raise RuntimeError('PostgreSQL did not become ready')


def stop_pg() -> None:
    if not docker_exists():
        return
    run(['docker', 'stop', '--time', '30', PG_CONTAINER], timeout=45)
    deadline = time.monotonic() + 15
    while docker_exists() and time.monotonic() < deadline:
        time.sleep(0.1)
    if docker_exists():
        raise RuntimeError('owned PostgreSQL container did not stop')


def start_app() -> None:
    global APP, APP_LOG_HANDLE
    if APP is not None:
        raise RuntimeError('app unexpectedly already running')
    APP_LOG_HANDLE = APP_LOG.open('ab')
    APP = subprocess.Popen([str(SCRATCH / 'start-app.sh')], stdout=APP_LOG_HANDLE, stderr=subprocess.STDOUT, start_new_session=True)
    wait_port(ADMIN_PORT, 60)
    status, _, _, _, _ = http_get('/admin/health')
    if status != 200:
        raise RuntimeError(f'app health returned {status}')


def stop_app() -> None:
    global APP, APP_LOG_HANDLE
    if APP is not None:
        if APP.poll() is None:
            os.killpg(APP.pid, signal.SIGTERM)
            try:
                APP.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(APP.pid, signal.SIGKILL)
                APP.wait(timeout=5)
        APP = None
    if APP_LOG_HANDLE is not None:
        APP_LOG_HANDLE.close()
        APP_LOG_HANDLE = None


def psql(sql: str) -> str:
    return run(['docker', 'exec', PG_CONTAINER, 'psql', '-U', DB_USER, '-d', DB_NAME, '-v', 'ON_ERROR_STOP=1', '-At', '-c', sql], timeout=120).stdout


def cache_helper(operation: str, proof_name: str) -> dict[str, Any]:
    command = (
        f'exec python /helper.py {operation} --root /cache-root '
        '--nonce "$(cat /secrets/cache_nonce)" --manifest cache-manifest.json '
        + ('--files-quiesced ' if operation in {'evict', 'warm'} else '')
        + '--output -'
    )
    result = run([
        'docker', 'run', '--rm', '--user', '999:999',
        '-v', f'{PG_VOLUME}:/cache-root',
        '-v', f'{HELPER}:/helper.py:ro',
        '-v', f'{CREDENTIALS}:/secrets:ro',
        PY_IMAGE, 'sh', '-c', command,
    ], timeout=180)
    payload = json.loads(result.stdout)
    atomic_json(PROOF_DIR / f'{proof_name}.json', payload)
    return payload


def path_group(path: str) -> str:
    for relation in CATALOG['relations']:
        base = relation['path']
        if path == base or re.fullmatch(re.escape(base) + r'(?:_(?:fsm|vm|init))?(?:\.\d+)?', path):
            return relation['group']
    return 'unmapped'


def mincore_summary(report: dict[str, Any], suffix: str = 'after') -> dict[str, Any]:
    groups: dict[str, dict[str, int]] = {}
    for item in report['files']:
        group = path_group(item['relative_path'])
        row = groups.setdefault(group, {'files': 0, 'pages': 0, 'resident_pages': 0})
        row['files'] += 1
        row['pages'] += int(item['pages'])
        resident = item.get(f'resident_pages_{suffix}')
        if resident is not None:
            row['resident_pages'] += int(resident)
    for row in groups.values():
        row['resident_ratio'] = (row['resident_pages'] / row['pages']) if row['pages'] else None
    return {
        'operation': report['operation'],
        'successful': report['successful'],
        'total_pages': report['totals']['pages'],
        'resident_pages': report['totals'][f'resident_pages_{suffix}'],
        'resident_ratio': report['totals'][f'resident_ratio_{suffix}'],
        'cache_state': report['totals'][f'cache_state_{suffix}'],
        'groups': groups,
        'report_sha256': sha256_file(PROOF_DIR / report['_proof_file']) if '_proof_file' in report else None,
    }


def pg_buffer_proof() -> dict[str, Any]:
    values = ','.join(f"({int(r['oid'])},'{r['group']}')" for r in CATALOG['relations'])
    sql = f"""
WITH target(oid,grp) AS (VALUES {values}),
buf AS (
 SELECT relfilenode, count(*)::bigint AS buffers
 FROM pg_buffercache
 WHERE reldatabase=(SELECT oid FROM pg_database WHERE datname='{DB_NAME}')
 GROUP BY relfilenode
)
SELECT json_build_object(
 'group',target.grp,
 'relations',count(*),
 'bytes',sum(pg_relation_size(target.oid)),
 'buffer_pages',coalesce(sum(buf.buffers),0)
)::text
FROM target
LEFT JOIN buf ON buf.relfilenode=pg_relation_filenode(target.oid)
GROUP BY target.grp
ORDER BY target.grp;
"""
    rows = [json.loads(line) for line in psql(sql).splitlines() if line.strip()]
    return {'groups': {row['group']: {k: row[k] for k in ('relations', 'bytes', 'buffer_pages')} for row in rows}, 'captured_at_utc': utc_now()}


def reset_statement_stats() -> None:
    psql('SELECT pg_stat_statements_reset();')


def statement_stats() -> dict[str, Any]:
    sql = f"""
SELECT json_build_object(
 'queryid',queryid,'calls',calls,'rows',rows,
 'shared_blks_hit',shared_blks_hit,'shared_blks_read',shared_blks_read,
 'shared_blks_dirtied',shared_blks_dirtied,'shared_blks_written',shared_blks_written,
 'temp_blks_read',temp_blks_read,'temp_blks_written',temp_blks_written,
 'blk_read_time',blk_read_time,'blk_write_time',blk_write_time,
 'total_exec_time',total_exec_time,'query',query
)::text
FROM pg_stat_statements
WHERE dbid=(SELECT oid FROM pg_database WHERE datname='{DB_NAME}')
  AND userid=(SELECT usesysid FROM pg_user WHERE usename='{APP_USER}')
  AND query NOT LIKE '%pg_stat_statements%'
ORDER BY total_exec_time DESC;
"""
    raw = [json.loads(line) for line in psql(sql).splitlines() if line.strip()]
    statements = []
    totals = {k: 0.0 for k in ('calls','rows','shared_blks_hit','shared_blks_read','shared_blks_dirtied','shared_blks_written','temp_blks_read','temp_blks_written','blk_read_time','blk_write_time','total_exec_time')}
    target_calls = 0.0
    for item in raw:
        query = item.pop('query')
        relations = sorted(set(re.findall(r'(?i)\b(?:FROM|JOIN|UPDATE|INTO)\s+([a-z_][a-z0-9_]*)', query)))
        is_target = bool(TARGET_RELATIONS.intersection(relations)) or any(name in query for name in TARGET_RELATIONS)
        if is_target:
            for key in totals:
                totals[key] += float(item[key])
            target_calls += float(item['calls'])
        statements.append({**item, 'query_sha256': sha256_bytes(query.encode()), 'relations': relations, 'target_read_path': is_target})
    return {
        'target_totals': totals,
        'target_sql_calls': target_calls,
        'all_app_statement_count': len(statements),
        'statements': statements,
        'pool_wait_ms': None,
        'pool_wait_observation': 'not observable from current app metrics/pg_stat_statements; intentionally omitted, never zero-filled',
    }


def http_get(path: str) -> tuple[int, dict[str, str], bytes, float, float]:
    token = (CREDENTIALS / 'admin_token').read_text().strip()
    connection = http.client.HTTPConnection(ADMIN_HOST, ADMIN_PORT, timeout=120)
    started = time.perf_counter_ns()
    connection.request('GET', path, headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/json'})
    response = connection.getresponse()
    ttfb_ms = (time.perf_counter_ns() - started) / 1_000_000
    body = response.read()
    wall_ms = (time.perf_counter_ns() - started) / 1_000_000
    headers = {k.lower(): v for k, v in response.getheaders()}
    status = response.status
    connection.close()
    return status, headers, body, ttfb_ms, wall_ms


def response_summary(body: bytes) -> tuple[str, dict[str, Any]]:
    value = json.loads(body)
    canonical = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
    summary: dict[str, Any] = {'canonical_sha256': sha256_bytes(canonical), 'body_sha256': sha256_bytes(body), 'body_bytes': len(body)}
    if isinstance(value, dict):
        if 'rows' in value:
            rows = value.get('rows') or []
            summary.update({
                'row_count': len(rows),
                'first_id': rows[0].get('id') if rows else None,
                'last_id': rows[-1].get('id') if rows else None,
                'next_cursor_sha256': sha256_bytes(value['next_cursor'].encode()) if value.get('next_cursor') else None,
                'summary': value.get('summary'),
            })
        else:
            summary.update({'id': value.get('id'), 'turn_count': len(value.get('turns') or []), 'state': value.get('state')})
    return summary['canonical_sha256'], summary


def append_record(record: dict[str, Any]) -> None:
    with RAW.open('ab') as target:
        target.write((json.dumps(record, sort_keys=True) + '\n').encode())
        target.flush()
        os.fsync(target.fileno())


def request_record(phase: str, endpoint: str, sample: int, condition: dict[str, Any] | None) -> dict[str, Any]:
    reset_statement_stats()
    started_at = utc_now()
    status, headers, body, ttfb_ms, wall_ms = http_get(ENDPOINTS[endpoint])
    ended_at = utc_now()
    canonical_hash, body_summary = response_summary(body)
    stats = statement_stats()
    record = {
        'schema': 'cc-lb-controlled-cache-request/v1',
        'run_id': f'{phase}-{endpoint}-{sample:03d}',
        'phase': phase,
        'endpoint': endpoint,
        'sample': sample,
        'concurrency': 1,
        'started_at_utc': started_at,
        'ended_at_utc': ended_at,
        'request': {'method': 'GET', 'path': ENDPOINTS[endpoint]},
        'response': {'status': status, 'ttfb_ms': ttfb_ms, 'wall_ms': wall_ms, 'headers': {'etag': headers.get('etag'), 'cache-control': headers.get('cache-control')}, **body_summary},
        'database': stats,
        'condition': condition,
        'app_cache_classification': 'db_read_observed' if stats['target_sql_calls'] > 0 else 'api_response_served_without_observed_target_db_read',
    }
    append_record(record)
    if status != 200:
        raise RuntimeError(f'{phase} {endpoint} sample {sample} returned {status}')
    print(f"{phase} {endpoint} {sample}: wall_ms={wall_ms:.3f} db_reads={stats['target_totals']['shared_blks_read']:.0f}", flush=True)
    return record


def cold_sample(phase: str, endpoint: str, sample: int) -> dict[str, Any]:
    stop_app()
    stop_pg()
    proof_prefix = f'{phase}-{endpoint}-{sample:03d}'
    evict = cache_helper('evict', proof_prefix + '-evict')
    evict['_proof_file'] = proof_prefix + '-evict.json'
    evict_summary = mincore_summary(evict)
    if not evict['successful'] or evict['totals']['resident_pages_after'] != 0:
        raise RuntimeError(f"cache eviction not proven for {proof_prefix}: {evict['status']} resident={evict['totals']['resident_pages_after']}")
    start_pg()
    pre_app_inspect = cache_helper('inspect', proof_prefix + '-pre-app-mincore')
    pre_app_inspect['_proof_file'] = proof_prefix + '-pre-app-mincore.json'
    pre_app_pg = pg_buffer_proof()
    start_app()
    pre_request_inspect = cache_helper('inspect', proof_prefix + '-pre-request-mincore')
    pre_request_inspect['_proof_file'] = proof_prefix + '-pre-request-mincore.json'
    pre_request_pg = pg_buffer_proof()
    condition = {
        'evict': evict_summary,
        'pre_app_mincore': mincore_summary(pre_app_inspect, 'before'),
        'pre_app_pg_buffers': pre_app_pg,
        'pre_request_mincore': mincore_summary(pre_request_inspect, 'before'),
        'pre_request_pg_buffers': pre_request_pg,
        'shared_buffers_control': 'PostgreSQL clean stop/restart before this single request',
        'app_state_control': 'fresh cc-lb process before this single request',
    }
    record = request_record(phase, endpoint, sample, condition)
    post_inspect = cache_helper('inspect', proof_prefix + '-post-request-mincore')
    post_inspect['_proof_file'] = proof_prefix + '-post-request-mincore.json'
    record['condition']['post_request_mincore'] = mincore_summary(post_inspect, 'before')
    record['condition']['post_request_pg_buffers'] = pg_buffer_proof()
    # Rewrite this line's final condition by replacing the just-appended record atomically at end of run.
    return record


def rewrite_raw(records: list[dict[str, Any]]) -> None:
    temporary = RAW.with_suffix('.jsonl.tmp')
    with temporary.open('w') as target:
        for record in records:
            target.write(json.dumps(record, sort_keys=True) + '\n')
    os.chmod(temporary, 0o600)
    temporary.replace(RAW)


def percentiles(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    def percentile(p: float) -> float:
        if len(ordered) == 1:
            return ordered[0]
        position = (len(ordered) - 1) * p
        low = int(position)
        high = min(low + 1, len(ordered) - 1)
        fraction = position - low
        return ordered[low] * (1 - fraction) + ordered[high] * fraction
    return {'min': min(ordered), 'median': statistics.median(ordered), 'p95': percentile(0.95), 'max': max(ordered), 'mean': statistics.fmean(ordered)}


def summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[str, Any] = {}
    unresolved = []
    for endpoint in ENDPOINTS:
        endpoint_records = [r for r in records if r['endpoint'] == endpoint]
        hashes = sorted(set(r['response']['canonical_sha256'] for r in endpoint_records))
        statuses = sorted(set(r['response']['status'] for r in endpoint_records))
        groups[endpoint] = {'semantic_hashes': hashes, 'statuses': statuses, 'phases': {}}
        if len(hashes) != 1:
            unresolved.append(f'{endpoint}: canonical response hash changed across controlled conditions')
        for phase in ('cold-A', 'warm', 'cold-B'):
            selected = [r for r in endpoint_records if r['phase'] == phase]
            groups[endpoint]['phases'][phase] = {
                'samples': len(selected),
                'wall_ms': percentiles([r['response']['wall_ms'] for r in selected]),
                'ttfb_ms': percentiles([r['response']['ttfb_ms'] for r in selected]),
                'db_shared_blks_read': percentiles([r['database']['target_totals']['shared_blks_read'] for r in selected]),
                'db_shared_blks_hit': percentiles([r['database']['target_totals']['shared_blks_hit'] for r in selected]),
                'db_exec_ms': percentiles([r['database']['target_totals']['total_exec_time'] for r in selected]),
                'target_sql_calls': percentiles([r['database']['target_sql_calls'] for r in selected]),
                'app_cache_classifications': sorted(set(r['app_cache_classification'] for r in selected)),
            }
    cold_proof = [r for r in records if r['phase'].startswith('cold')]
    evicted = all(r['condition']['evict']['resident_pages'] == 0 and r['condition']['evict']['successful'] for r in cold_proof)
    keepalive_pg_pre_app = [r['condition']['pre_app_pg_buffers']['groups'].get('keepalive', {}).get('buffer_pages') for r in cold_proof]
    keepalive_pg_pre_request = [r['condition']['pre_request_pg_buffers']['groups'].get('keepalive', {}).get('buffer_pages') for r in cold_proof]
    keepalive_mincore_pre_request = [r['condition']['pre_request_mincore']['groups'].get('keepalive', {}).get('resident_ratio') for r in cold_proof]
    if not evicted:
        unresolved.append('one or more cold cycles did not reach zero measured relation-file resident pages after fadvise')
    if any(v not in (0, None) for v in keepalive_pg_pre_app):
        unresolved.append('one or more PostgreSQL restarts had keepalive target buffers resident before app startup')
    return {
        'schema': 'cc-lb-controlled-cache-result/v1',
        'created_at_utc': utc_now(),
        'verdict': 'PASS' if not unresolved else 'FAIL',
        'claim_boundary': PROTOCOL['scope']['claim_boundary'],
        'sample_counts': {'cold_A_per_endpoint': COLD_SAMPLES_PER_PHASE, 'warm_per_endpoint': WARM_SAMPLES, 'cold_B_per_endpoint': COLD_SAMPLES_PER_PHASE, 'total_per_endpoint': 2 * COLD_SAMPLES_PER_PHASE + WARM_SAMPLES, 'total_requests': len(records), 'concurrency': 1},
        'endpoints': groups,
        'condition_proof': {
            'cold_cycles': len(cold_proof),
            'all_evictions_zero_resident_pages': evicted,
            'keepalive_pg_buffer_pages_pre_app': {'min': min(keepalive_pg_pre_app), 'max': max(keepalive_pg_pre_app)},
            'keepalive_pg_buffer_pages_pre_request': {'min': min(keepalive_pg_pre_request), 'max': max(keepalive_pg_pre_request)},
            'keepalive_mincore_ratio_pre_request': {'min': min(v for v in keepalive_mincore_pre_request if v is not None), 'max': max(v for v in keepalive_mincore_pre_request if v is not None)},
            'transition_order': ['cold-A', 'warm', 'cold-B'],
        },
        'key_boundaries': {
            'principal_id': PRINCIPAL,
            'summary': {'limit': 0, 'horizon': 'all', 'status': 'all'},
            'list': {'limit': 50, 'horizon': 'all', 'status': 'all'},
            'detail_id': 'perf-detail-session',
        },
        'pure_pool_wait': {'value': None, 'reason': 'not observable from current app metrics/pg_stat_statements; not zero-filled'},
        'limitations': PROTOCOL['limitations'],
        'unresolved': unresolved,
    }


def cleanup() -> None:
    try:
        stop_app()
    finally:
        stop_pg()
    for path in CREDENTIALS.iterdir():
        if path.is_file():
            path.unlink()
    if (SCRATCH / 'pg.env').exists():
        (SCRATCH / 'pg.env').unlink()
    if (SCRATCH / 'cc-lb.toml').exists():
        # Contains the disposable DB password and is not evidence.
        (SCRATCH / 'cc-lb.toml').unlink()
    if (SCRATCH / 'start-app.sh').exists():
        (SCRATCH / 'start-app.sh').unlink()
    try:
        CREDENTIALS.rmdir()
    except OSError:
        pass


def main() -> int:
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    for path in (RAW, RESULT, MANIFEST):
        path.unlink(missing_ok=True)
    if PROOF_DIR.exists():
        for path in PROOF_DIR.iterdir():
            if path.is_file():
                path.unlink()
    records: list[dict[str, Any]] = []
    failure: str | None = None
    started = utc_now()
    try:
        stop_app()
        stop_pg()
        for sample in range(1, COLD_SAMPLES_PER_PHASE + 1):
            for endpoint in ENDPOINTS:
                records.append(cold_sample('cold-A', endpoint, sample))
        # The final cold-A app/PG remain running. Prime each exact query path, then measure warm samples.
        for endpoint in ENDPOINTS:
            status, _, _, _, _ = http_get(ENDPOINTS[endpoint])
            if status != 200:
                raise RuntimeError(f'warm-up {endpoint} returned {status}')
        for sample in range(1, WARM_SAMPLES + 1):
            for endpoint in ENDPOINTS:
                records.append(request_record('warm', endpoint, sample, {'shared_buffers_control': 'same running PostgreSQL after explicit endpoint warm-up', 'app_state_control': 'same running cc-lb process after explicit endpoint warm-up'}))
        for sample in range(1, COLD_SAMPLES_PER_PHASE + 1):
            for endpoint in ENDPOINTS:
                records.append(cold_sample('cold-B', endpoint, sample))
        rewrite_raw(records)
        result = summarize(records)
        atomic_json(RESULT, result)
        evidence_files = [p for p in SCRATCH.rglob('*') if p.is_file() and 'credentials' not in p.parts and p.name not in {'pg.env', 'cc-lb.toml', 'start-app.sh'}]
        manifest = {
            'schema': 'cc-lb-controlled-cache-manifest/v1',
            'started_at_utc': started,
            'ended_at_utc': utc_now(),
            'binary': {'path': str(BINARY), 'version': 'cc-lb 0.4.9 (ef70b34)', 'sha256': sha256_file(BINARY), 'source_commit': 'ef70b347790c46fa9956435a820a51b60c547d04'},
            'source_fixture': PROTOCOL['source_fixture'],
            'measurement_fixture': FIXTURE['datasets']['postgres:candidate'],
            'images': {
                'postgres_16': run(['docker', 'image', 'inspect', PG_IMAGE, '--format', '{{.Id}}']).stdout.strip(),
                'python_3_12_slim': run(['docker', 'image', 'inspect', PY_IMAGE, '--format', '{{.Id}}']).stdout.strip(),
            },
            'owned_resources': {'volume': PG_VOLUME, 'container': PG_CONTAINER, 'ports': PROTOCOL['ports']},
            'artifacts': {str(p.relative_to(SCRATCH)): {'sha256': sha256_file(p), 'bytes': p.stat().st_size} for p in sorted(evidence_files)},
            'cleanup': {'app_stopped': False, 'postgres_stopped': False, 'credentials_removed': False, 'named_volume_retained_for_main_followup': PG_VOLUME},
            'unrelated_impact': 'none; only labelled owned volume/container and loopback ports were used; no application source, Git, existing containers, or global cache controls were changed',
        }
        atomic_json(MANIFEST, manifest)
    except Exception as error:
        failure = f'{type(error).__name__}: {error}'
        if records:
            rewrite_raw(records)
        atomic_json(SCRATCH / 'failure.json', {'failed_at_utc': utc_now(), 'error': failure, 'completed_records': len(records)})
    finally:
        cleanup()
        if MANIFEST.exists():
            manifest = json.loads(MANIFEST.read_text())
            manifest['cleanup'] = {'app_stopped': True, 'postgres_stopped': True, 'credentials_removed': True, 'named_volume_retained_for_main_followup': PG_VOLUME}
            atomic_json(MANIFEST, manifest)
    if failure:
        print('EXPERIMENT_FAILED', failure, flush=True)
        return 1
    print('EXPERIMENT_COMPLETE requests=' + str(len(records)), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
