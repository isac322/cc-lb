#!/usr/bin/env python3
import json
import os
import re
from pathlib import Path

root = Path('/cache-root')
catalog = json.loads(Path('/input/relation-catalog.json').read_text())
nonce = Path('/secrets/cache_nonce').read_text().strip()
if not (16 <= len(nonce) <= 256):
    raise SystemExit('invalid nonce length')
files = set()
for relation in catalog['relations']:
    relpath = relation['path']
    if not relpath or not relpath.startswith(f"base/{catalog['database_oid']}/"):
        continue
    base = root / relpath
    parent = base.parent
    prefix = re.escape(base.name)
    pattern = re.compile(rf'^{prefix}(?:_(?:fsm|vm|init))?(?:\.\d+)?$')
    for candidate in parent.iterdir():
        if candidate.is_file() and pattern.fullmatch(candidate.name):
            files.add(str(candidate.relative_to(root)))
marker = {
    'schema': 'cc-lb-admin-web-cache-control-root/v1',
    'nonce': nonce,
    'owner_uid': os.geteuid(),
}
manifest = {
    'schema': 'cc-lb-admin-web-cache-control-manifest/v1',
    'database_kind': 'postgres',
    'files': sorted(files),
}
(root / '.admin-web-cache-control-owner.json').write_text(json.dumps(marker, sort_keys=True) + '\n')
(root / 'cache-manifest.json').write_text(json.dumps(manifest, sort_keys=True) + '\n')
os.chmod(root / '.admin-web-cache-control-owner.json', 0o600)
os.chmod(root / 'cache-manifest.json', 0o600)
print(json.dumps({'file_count': len(files), 'database_oid': catalog['database_oid']}))
