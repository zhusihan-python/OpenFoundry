"""Exact147 opaque-DSN comparison; Root-gated22 values stay memory-only.

Diagnostic completion is not secret-clean. A hit never permits packaging.
No registry expansion, network, subprocess, package copy, or producer execution.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import types
from urllib.parse import urlsplit, unquote

sys.dont_write_bytecode = True
P26 = Path(__file__).absolute().parents[1]
ROOT = P26.parents[2]
PROVIDER_SHA = '1fa4f5f594e978664ca54bdad015ffeaec3523a2873ecfe41decbb291f7470bb'
REGISTRY_SHA = '6bd4e5e45a8d42c32d80bcb436feaf7857abb3441f7d8c1ea95fb0c37d63f7e5'
CANONICAL22 = '012f6b246d69520e700a4bce9fb9d05f96b9626fe2d71f08fd2585806eb8920a'
PATHS_SHA = '0e88a6412bf1f34f088de878731cb416622845fdadb6b81074262a112e436f80'
INVENTORY_SHA = '42bd171b599d44ecd82b4e89b1a88f3d219c2d7274d7ac5f6c9fee08bab3ff0d'
REGISTRY = 'tools/smoke/business-platform-p2-6/results/w9/2026-11-15-w9-attempt47/registry22.CANDIDATE.json'
PROVIDER = 'tools/smoke/business-platform-p2-6/archive/p26_secret_values_memory.py'
FIELDS = {'path', 'kind', 'count', 'input-id'}


def need(ok):
    if not ok: raise ValueError('memory diagnostic refused')


def parent_fd(root, rel):
    need(isinstance(rel, str) and re.fullmatch(r'[A-Za-z0-9._+-]+(?:/[A-Za-z0-9._+-]+)*', rel))
    parts = rel.split('/')
    need(not any(v in ('', '.', '..') or v.lower().startswith('.env')
        or v.lower().endswith('.env') or '.env.' in v.lower()
        or v.lower().endswith('.body') or '.body.' in v.lower() for v in parts))
    need(Path(root).is_absolute())
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in Path(root).parts[1:] + tuple(parts[:-1]):
            new = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd); fd = new
        return fd, parts[-1]
    except BaseException:
        os.close(fd); raise


def read(root, rel, pin=None):
    parent, leaf = parent_fd(root, rel)
    try: fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    finally: os.close(parent)
    try:
        before = os.fstat(fd); need(stat.S_ISREG(before.st_mode)); chunks = []
        while True:
            data = os.read(fd, 1048576)
            if not data: break
            chunks.append(data)
        raw = b''.join(chunks); after = os.fstat(fd)
        need((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)
            == (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns))
        need(len(raw) == before.st_size)
        if pin is not None:
            need(len(raw) == pin['size'] and hashlib.sha256(raw).hexdigest() == pin['sha256'])
        return raw
    finally: os.close(fd)


def canonical(entries):
    return hashlib.sha256(json.dumps(entries, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def scope(entries, digest=CANONICAL22):
    need(isinstance(entries, list) and len(entries) == 22 and canonical(entries) == digest)
    need(len({e['id'] for e in entries}) == 22)
    for e in entries:
        need(set(e) == {'id','path','key','role','projection'})
        need(all(isinstance(v,str) and v for v in e.values()))
        need(e['projection'] in ('exact','bearer-token','url-password'))


def compare(data, path, entries, values):
    """Pure in-memory comparison. Never return a value/hash/length/context."""
    need(len(values) == len(entries) and all(isinstance(v,str) and v for v in values))
    found = Counter(); passwords = []
    for entry, value in zip(entries, values):
        ident = entry['id']; count = data.count(value.encode())
        if count: found[('actual-registered-value-hit', ident)] += count
        parsed = urlsplit(value)
        if entry['projection'] == 'exact' and parsed.scheme in ('postgres','postgresql') and parsed.password is not None:
            passwords.append((ident, unquote(parsed.password)))
        elif entry['projection'] == 'url-password':
            passwords.append((ident, value))
    credentials = []
    for match in re.finditer(rb'(?i)postgres(?:ql)?://[A-Za-z0-9._-]+:([^\s@/]{4,})@', data):
        try: credentials.append(unquote(match.group(1).decode('utf-8')))
        except UnicodeError: raise ValueError('DSN decoding refused') from None
    for credential in credentials:
        for entry, value in zip(entries, values):
            if credential == value: found[('dsn-password-equals-registered-value', entry['id'])] += 1
        for ident, password in passwords:
            if credential == password: found[('dsn-password-equals-registered-url-password', ident)] += 1
    rows = [{'path':path,'kind':kind,'count':count,'input-id':ident}
            for (kind,ident),count in sorted(found.items())]
    rows.append({'path':path,'kind':'opaque-dsn-observation-count','count':len(credentials),'input-id':''})
    if not found: rows.append({'path':path,'kind':'no-registered-match-not-a-public-fixture-proof','count':0,'input-id':''})
    return rows


def render(rows, values):
    need(all(set(row) == FIELDS for row in rows))
    raw = (json.dumps(rows, indent=2) + '\n').encode()
    need(not any(v.encode() in raw for v in values))
    return raw


def load_private(provider, entries):
    try:
        values, metadata = provider.load_values(entries)
        return values, metadata, None
    except BaseException as exc:
        return None, None, {'path':'','kind':'loader-failed-'+type(exc).__name__,'count':1,'input-id':''}


def run(release_file):
    rf = Path(release_file).absolute(); original = read('/', str(rf).lstrip('/')); r = json.loads(original)
    need(r['schema'] == 'p26-opaque-dsn-memory-diagnostic-release-v1'
        and r['phase'] == 'opaque-dsn-memory-diagnostic' and r['root_accepted'] is True)
    need(r['workspace_root'] == str(ROOT) and r['provider_sha256'] == PROVIDER_SHA
        and r['registry']['path'] == REGISTRY and r['registry']['sha256'] == REGISTRY_SHA
        and r['registered_inputs_sha256'] == CANONICAL22 and r['path_manifest']['sha256'] == PATHS_SHA)
    def gate():
        need(read('/', str(rf).lstrip('/')) == original)
        need(datetime.now(timezone.utc) < datetime.fromisoformat(r['expires_utc'].replace('Z','+00:00')))
        need(hashlib.sha256(read(ROOT, str(Path(__file__).absolute().relative_to(ROOT)))).hexdigest() == r['tool_sha256'])
        need(hashlib.sha256(read(ROOT, PROVIDER)).hexdigest() == PROVIDER_SHA)
    gate()
    registry = json.loads(read(ROOT, REGISTRY, r['registry'])); entries = registry['registered_inputs']
    scope(entries); need(registry['registered_inputs_sha256'] == CANONICAL22)
    need(r['source_sha256'] == registry['source_sha256'] == 'ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e')
    need(r['output_path'].startswith('tools/smoke/business-platform-p2-6/results/w6/'))
    parent, leaf = parent_fd(ROOT,r['output_path'])
    try:
        try: os.stat(leaf,dir_fd=parent,follow_symlinks=False)
        except FileNotFoundError: pass
        else: need(False)
    finally: os.close(parent)
    manifest = json.loads(read(ROOT, r['path_manifest']['path'], r['path_manifest']))
    need(manifest['schema'] == 'p26-opaque-dsn-exact147-path-manifest-v1' and len(manifest['members']) == manifest['count'] == 147)
    need(manifest['inventory']['sha256'] == INVENTORY_SHA)
    inventory = json.loads(read(ROOT, manifest['inventory']['path'], manifest['inventory']))
    payloads = {}
    for path,pin in manifest['members'].items():
        gate(); need(pin == {k:inventory['members'][path][k] for k in ('sha256','size')})
        need(Path(path).name in ('01-go-test.raw','01-go-test-2.raw'))
        payloads[path] = read(ROOT, path, pin)
    gate()  # Final authorization/bytes/scope checks BEFORE importing/loading secrets.
    provider = types.ModuleType('fixed_current_secret_provider')
    provider.__file__ = str(ROOT / PROVIDER)
    exec(compile(read(ROOT, PROVIDER), str(ROOT / PROVIDER), 'exec'), provider.__dict__)
    gate(); scope(entries)
    values, metadata, failure = load_private(provider, entries)
    if failure is not None:
        print(json.dumps([failure]))
        return 1
    need(len(values) == len(metadata) == 22 and [m['id'] for m in metadata] == [e['id'] for e in entries])
    rows = []
    for path,data in payloads.items():
        gate(); rows.extend(compare(data, path, entries, values))
    rows.append({'path':'','kind':'loaded-registered-input-count','count':22,'input-id':''})
    rows.append({'path':'','kind':'verified-exact-file-count','count':147,'input-id':''})
    raw = render(rows, values)
    # Derived registered URL passwords receive the same output-leak guard.
    derived = [unquote(urlsplit(v).password) for e,v in zip(entries,values)
        if e['projection']=='exact' and urlsplit(v).scheme in ('postgres','postgresql') and urlsplit(v).password is not None]
    need(not any(v.encode() in raw for v in derived))
    gate(); scope(entries)
    need(r['output_path'].startswith('tools/smoke/business-platform-p2-6/results/w6/'))
    parent, leaf = parent_fd(ROOT,r['output_path'])
    try: fd = os.open(leaf,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=parent)
    finally: os.close(parent)
    with os.fdopen(fd,'wb') as stream: stream.write(raw)
    del values,derived,payloads,rows,raw,metadata
    print(json.dumps([{'path':r['output_path'],'kind':'memory-comparison-complete-not-secret-clean','count':147,'input-id':''}]))
    return 0


if __name__ == '__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--release-file',required=True);args=parser.parse_args()
    try: rc=run(args.release_file)
    except BaseException as exc:
        print(json.dumps([{'path':'','kind':'diagnostic-failed-'+type(exc).__name__,'count':1,'input-id':''}]),file=sys.stderr);rc=1
    sys.exit(rc)
