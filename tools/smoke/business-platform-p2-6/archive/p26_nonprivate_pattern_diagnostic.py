"""Root-gated nonprivate pattern diagnostic; never reads registry/actual values.

Only fixed inventory members and the pinned current45 helper plan are scanned.
All reports contain exactly kind/path/line/count/classification, never samples,
matched lengths, matched bytes, or hashes of matches. No dispositions applied.
"""
import argparse
import ast
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

sys.dont_write_bytecode = True
P26 = Path(__file__).absolute().parents[1]
ROOT = P26.parents[2]
INVENTORY = 'tools/smoke/business-platform-p2-6/results/w9/2026-11-15-w9-attempt48/final-history-inventory-actual-attempt1/stdout.json'
INVENTORY_SHA = '42bd171b599d44ecd82b4e89b1a88f3d219c2d7274d7ac5f6c9fee08bab3ff0d'
INVENTORY_SIZE = 53282052
FIELDS = {'kind', 'path', 'line', 'count', 'classification'}


def need(ok):
    if not ok: raise ValueError('safe diagnostic input refused')


def relative(value):
    need(isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9._+-]+(?:/[A-Za-z0-9._+-]+)*', value))
    parts = value.split('/')
    need(not any(p in ('', '.', '..') for p in parts))
    need(not any(p.lower().startswith('.env') or p.lower().endswith('.env')
                 or '.env.' in p.lower() or p.lower().endswith('.body')
                 or '.body.' in p.lower() for p in parts))
    return parts


def parent_fd(root, rel):
    parts = relative(rel)
    need(Path(root).is_absolute())
    # Walk EVERY parent with openat+O_NOFOLLOW; do not resolve away symlinks.
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in Path(root).parts[1:] + tuple(parts[:-1]):
            new = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd); fd = new
        return fd, parts[-1]
    except BaseException:
        os.close(fd); raise


def safe_read(root, rel):
    parent, leaf = parent_fd(root, rel)
    try: fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    finally: os.close(parent)
    try:
        before = os.fstat(fd); need(stat.S_ISREG(before.st_mode))
        chunks = []
        while True:
            chunk = os.read(fd, 1048576)
            if not chunk: break
            chunks.append(chunk)
        data = b''.join(chunks); after = os.fstat(fd)
        need((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
             == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns))
        need(len(data) == before.st_size)
        return data
    finally: os.close(fd)


def verified(root, rel, pin):
    need(type(pin['size']) is int and pin['size'] >= 0 and re.fullmatch('[0-9a-f]{64}', pin['sha256']))
    data = safe_read(root, rel)
    need(len(data) == pin['size'] and hashlib.sha256(data).hexdigest() == pin['sha256'])
    return data


def scanner_from_bytes(data):
    # Only the hash-verified NEW scanner module is loaded, with __main__ false.
    # Never call scan_package/selftest/actual-value mode or any old producer.
    module = types.ModuleType('p26_current_pattern_scanner')
    exec(compile(data, 'NEW-p26_secret_scan.py', 'exec'), module.__dict__)
    return module.scan_bytes


def scan_member(root, rel, pin, scanner, report_path):
    data = verified(root, rel, pin)
    grouped = Counter()
    for hit in scanner(data, report_path):
        need(hit.path == report_path and type(hit.line) is int and hit.line >= 1)
        need(hit.classification in ('suspect', 'test-or-fixture-literal'))
        grouped[(hit.kind, report_path, hit.line, hit.classification)] += 1
    return [{'kind': k, 'path': p, 'line': line, 'count': count, 'classification': cls}
            for (k, p, line, cls), count in sorted(grouped.items())]


def run(release_file):
    release_path = Path(release_file).absolute()
    original = safe_read('/', str(release_path).lstrip('/'))
    release = json.loads(original)
    need(release['schema'] == 'p26-nonprivate-pattern-diagnostic-release-v1'
         and release['root_accepted'] is True and release['phase'] == 'nonprivate-pattern-diagnostic')
    need(release['workspace_root'] == str(ROOT))
    def gate():
        need(safe_read('/', str(release_path).lstrip('/')) == original)
        need(datetime.now(timezone.utc) < datetime.fromisoformat(release['expires_utc'].replace('Z', '+00:00')))
    gate()
    tool_rel = str(Path(__file__).absolute().relative_to(ROOT))
    need(hashlib.sha256(safe_read(ROOT, tool_rel)).hexdigest() == release['tool_sha256'])
    need(release['inventory'] == {'path': INVENTORY, 'sha256': INVENTORY_SHA, 'size': INVENTORY_SIZE})
    inventory = json.loads(verified(ROOT, INVENTORY, release['inventory']))
    members = inventory['members']; need(len(members) == 137846)
    # No issues/private inventory records are opened, hashed, or scanned.
    plan = json.loads(verified(ROOT, release['helper_plan']['path'], release['helper_plan']))
    need(plan['schema'] == 'p26-nonprivate-pattern-helper-plan-v1')
    assembler_rel = 'tools/smoke/business-platform-p2-6/archive/p26_package_assembler.py'
    assembler_bytes = verified(ROOT, assembler_rel, plan['assembler'])
    nodes = [n for n in ast.parse(assembler_bytes).body if isinstance(n, ast.Assign)
             and any(isinstance(t, ast.Name) and t.id == 'TOOLS' for t in n.targets)]
    need(len(nodes) == 1)
    required = ast.literal_eval(nodes[0].value)
    need(isinstance(required, tuple) and len(required) == len(set(required)) == 45)
    helpers = plan['helpers']
    need(len(helpers) == 45 and {h['name'] for h in helpers} == set(required))
    for helper in helpers:
        need(helper['path'] == 'tools/smoke/business-platform-p2-6/archive/' + helper['name'])
        need(helper['report_path'] == 'tools/' + helper['name'])
    scanner_entry = next(h for h in helpers if h['name'] == 'p26_secret_scan.py')
    scanner = scanner_from_bytes(verified(ROOT, scanner_entry['path'], scanner_entry))
    findings = []; count = 0
    for path in sorted(members):
        gate(); relative(path)
        findings.extend(scan_member(ROOT, path, members[path], scanner, path)); count += 1
    for helper in sorted(helpers, key=lambda h: h['name']):
        gate()
        findings.extend(scan_member(ROOT, helper['path'], helper, scanner, helper['report_path'])); count += 1
    gate()
    need(hashlib.sha256(safe_read(ROOT, tool_rel)).hexdigest() == release['tool_sha256'])
    verified(ROOT, assembler_rel, plan['assembler'])
    for helper in helpers: verified(ROOT, helper['path'], helper)
    findings.append({'kind': 'verified-file-count', 'path': INVENTORY, 'line': 0,
                     'count': count, 'classification': 'pattern-diagnostic-only-not-secret-proof'})
    need(all(set(row) == FIELDS for row in findings))
    output_rel = release['output_path']
    need(output_rel.startswith('tools/smoke/business-platform-p2-6/results/w6/'))
    parent, leaf = parent_fd(ROOT, output_rel)
    try: fd = os.open(leaf, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
    finally: os.close(parent)
    with os.fdopen(fd, 'w') as stream:
        json.dump(findings, stream, indent=2); stream.write('\n')
    print(json.dumps([{'kind': 'diagnostic-report', 'path': output_rel, 'line': 0,
                       'count': len(findings) - 1, 'classification': 'pattern-diagnostic-only-not-secret-proof'}]))
    return 0  # Findings are diagnostic, never an automatic disposition/pass.


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--release-file', required=True)
    args = parser.parse_args()
    try: rc = run(args.release_file)
    except Exception:
        print(json.dumps([{'kind': 'diagnostic-refused', 'path': '', 'line': 0,
                           'count': 1, 'classification': 'safe-input-refusal'}]), file=sys.stderr)
        rc = 1
    sys.exit(rc)
