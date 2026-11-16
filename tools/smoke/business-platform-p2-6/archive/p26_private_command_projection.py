"""Root-released local-only projection of exactly26 private signed commands.

Command bytes are inspected only in memory. No requests, subprocesses, private
environment inputs, query values, query hashes or query lengths are emitted.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
from urllib.parse import urlsplit, urlunsplit

S = 'ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
MANIFEST_SHA = '83780215c3ebdfeaa676bfb835bafc12e9e770819454a242c87954f91db9785c'
INVENTORY_SHA = '42bd171b599d44ecd82b4e89b1a88f3d219c2d7274d7ac5f6c9fee08bab3ff0d'
MANIFEST_PATH = 'tools/smoke/business-platform-p2-6/results/w6/S2-readiness/pattern-public-literal-fix-code-pure-attempt1/EXACT-OPAQUE-SIGNED-CMD-PATHS.json'
INVENTORY_PATH = 'tools/smoke/business-platform-p2-6/results/w9/2026-11-15-w9-attempt48/final-history-inventory-actual-attempt1/stdout.json'
ORIGIN = 'http://127.0.0.1:17078'
SCHEMA = 'p26-private-command-projection-release-v1'
PHASE = 'p26-exact26-private-command-safe-projection'


def need(ok):
    if not ok:
        raise ValueError('private command projection refused')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def relative(value):
    need(isinstance(value, str) and value and not value.startswith('/')
         and '\\' not in value and all(x not in ('', '.', '..') for x in value.split('/')))
    need(value.startswith('tools/smoke/business-platform-p2-6/results/w7/')
         and value.endswith('.cmd') and 'file-bytes.cmd' in value)
    return value


def no_symlinks(path):
    path = Path(path)
    need(path.is_absolute())
    need(not any(p.is_symlink() for p in (path, *path.parents)))
    return path


def read_regular(path, pin):
    path = no_symlinks(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        need(stat.S_ISREG(before.st_mode))
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            raw = stream.read()
        after = os.fstat(fd)
        need((before.st_ino, before.st_size, before.st_mtime_ns)
             == (after.st_ino, after.st_size, after.st_mtime_ns))
        need(len(raw) == pin['size'] and sha(raw) == pin['sha256'])
        return raw
    finally:
        os.close(fd)


def project_command(raw):
    """Only command structure is attested, not unrecorded HTTP headers."""
    text = raw.decode('utf-8')
    need('\x00' not in text and '\r' not in text)
    lines = text.splitlines()
    need(len(lines) in (1, 2) and lines[0] == lines[0].strip())
    fields = lines[0].split(' ')
    need(len(fields) == 2 and fields[0] == 'GET' and fields[1])
    value = fields[1]
    need(not any(c.isspace() for c in value))
    base = None
    if len(lines) == 2:
        need(lines[1] == '# base=' + ORIGIN)
        base = ORIGIN
    parsed = urlsplit(value)
    if value.startswith('/'):
        need(not value.startswith('//') and base == ORIGIN)
        parsed = urlsplit(base + value)
    need(parsed.scheme in ('http', 'https') and parsed.username is None
         and parsed.password is None and parsed.scheme + '://' + parsed.netloc == ORIGIN)
    need(parsed.path.startswith('/v1/_internal/local-fs/')
         and re.fullmatch(r'/[A-Za-z0-9_./+-]+', parsed.path)
         and not any(p in ('.', '..') for p in parsed.path.split('/'))
         and bool(parsed.query))
    safe = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, '', ''))
    return {'method': 'GET', 'url_without_query_fragment': safe,
            'query_omitted': True, 'fragment_omitted': True,
            'anonymous': True,
            'scope': 'command-shape-only; unrecorded-request-headers-unclaimed'}


def inputs(release):
    need(release['schema'] == SCHEMA and release['phase'] == PHASE
         and release['root_accepted'] is True and release['source_sha256'] == S)
    now = datetime.datetime.now(datetime.timezone.utc)
    expiry = datetime.datetime.fromisoformat(release['expires_utc'].replace('Z', '+00:00'))
    need(expiry.tzinfo is not None and now < expiry)
    need(release['tool_sha256'] == sha(Path(__file__).read_bytes())
         and release['allowed_origins'] == [ORIGIN])
    root = no_symlinks(release['workspace_root'])
    manifest = release['paths_manifest']; inventory = release['inventory']
    need(manifest['path'] == MANIFEST_PATH and inventory['path'] == INVENTORY_PATH
         and manifest['sha256'] == MANIFEST_SHA and inventory['sha256'] == INVENTORY_SHA)
    m = json.loads(read_regular(root / manifest['path'], manifest))
    inv = json.loads(read_regular(root / inventory['path'], inventory))
    members = m['members']
    need(len(members) == 26)
    for path, pin in members.items():
        relative(path)
        need(set(pin) == {'sha256', 'size'}
             and pin == {k: inv['members'][path][k] for k in ('sha256', 'size')})
    output = no_symlinks(release['output_dir'])
    need(not output.exists() and output.is_relative_to(root / 'tools/smoke/business-platform-p2-6/results/w9')
         and not any(str(output) == str(root / p) for p in members))
    return root, output, members


def run(release_file):
    release_path = no_symlinks(release_file)
    release_bytes = release_path.read_bytes()
    release = json.loads(release_bytes)
    root, output, members = inputs(release)
    output.mkdir(parents=True, exist_ok=False)
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()
    rc = 1
    result = {'schema': 'p26-private-command-projection-result-v1', 'ok': False,
              'original_child_rc': 1, 'private_values_persisted': False,
              'services_called': 0, 'input_commands': 26}
    try:
        projected = {}
        for path, pin in members.items():
            need(release_path.read_bytes() == release_bytes)
            row = project_command(read_regular(root / path, pin))
            projected[path] = dict(row, original_file=pin)
        need(release_path.read_bytes() == release_bytes
             and sha(Path(__file__).read_bytes()) == release['tool_sha256'])
        doc = {'schema': 'p26-private-command-safe-projection-v1',
               'source_sha256': S, 'input_manifest': release['paths_manifest'],
               'inventory': release['inventory'], 'commands': projected,
               'original_private_commands_preserved': True,
               'query_values_hashes_lengths_persisted': False,
               'unrecorded_request_headers_claimed': False}
        (output / 'safe-command-projections.json').write_text(json.dumps(doc, indent=2) + '\n')
        rc = 0
        result.update(ok=True, original_child_rc=0, projected_commands=26)
    except BaseException:
        result['failure_kind'] = 'input-or-projection-refused'
    result.update(started_utc=started,
                  ended_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  tool_sha256=release['tool_sha256'])
    (output / 'RESULT.json').write_text(json.dumps(result, indent=2) + '\n')
    (output / 'rc').write_text(str(rc) + '\n')
    return rc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--release-file', required=True)
    args = parser.parse_args()
    try:
        return run(args.release_file)
    except BaseException:
        print('private command projection refused; no command contents reported', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.dont_write_bytecode = True
    sys.exit(main())
