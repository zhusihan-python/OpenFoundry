#!/usr/bin/env python3
"""Root-gated single-key private JWT renewal. No token/secret/hash output."""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
import uuid

sys.dont_write_bytecode = True
P26 = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(P26))
from common import p26_gates as G

SOURCE = 'ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
ENV = Path('/private/tmp/openfoundry-p26-http-smoke.env')
SECRET = Path('/private/tmp/openfoundry-platform-p1-q3v9/.env')
BACKUP = Path('/private/tmp/openfoundry-p26-http-writer-before-refresh.env')
KEY = b'P26_W7_JWT_WRITER'
SECRET_KEY = b'OF_P1_JWT_SECRET'
ACTOR = '1c0e238a-3657-4978-98dc-d71a342c3fa1'
ORG = 'a792b91e-dab5-4739-9344-2848c1e83487'
TTL = 10800


def tool_shas():
    return {str(Path(__file__).relative_to(P26)): G.sha256_file(__file__),
            'common/p26_gates.py': G.sha256_file(P26 / 'common/p26_gates.py')}


def validate_root(r, now, source, tools):
    if r.get('schema') != 'p26-http-writer-refresh-v1' or r.get('root_accepted') is not True or r.get('stage') != 'refresh-http-writer-only':
        raise ValueError('Root writer-only refresh release required')
    expiry = datetime.fromisoformat(r['expires_utc'].replace('Z', '+00:00'))
    if expiry.tzinfo is None or datetime.fromtimestamp(now, timezone.utc) >= expiry:
        raise ValueError('Root refresh phase expired')
    if r.get('candidate_source') != SOURCE or source != SOURCE or r.get('tool_shas') != tools:
        raise ValueError('Root source/tool pin mismatch')
    if r.get('private_paths') != {'smoke': str(ENV), 'secret': str(SECRET), 'backup': str(BACKUP)} or r.get('allowed_key') != KEY.decode() or type(r.get('duration_seconds')) is not int or r['duration_seconds'] != TTL:
        raise ValueError('Root exact private paths/key/duration required')
    out = Path(r['output']).resolve()
    if not out.is_relative_to((P26 / 'results/w10').resolve()):
        raise ValueError('Root exact NEW metadata output required')


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JWT JSON key')
        result[key] = value
    return result


def unb64(segment):
    if not re.fullmatch(rb'[A-Za-z0-9_-]+', segment):
        raise ValueError('invalid JWT encoding')
    return base64.b64decode(segment + b'=' * (-len(segment) % 4), altchars=b'-_', validate=True)


def b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b'=')


def renew_value(value, secret, now):
    if not value.startswith(b'Bearer '):
        raise ValueError('exact Bearer prefix required')
    parts = value[7:].split(b'.')
    if len(parts) != 3:
        raise ValueError('invalid JWT segments')
    head, payload, signature = parts
    header = json.loads(unb64(head), object_pairs_hook=unique_object)
    claims = json.loads(unb64(payload), object_pairs_hook=unique_object)
    if not isinstance(header, dict) or header.get('alg') != 'HS256' or not isinstance(claims, dict):
        raise ValueError('HS256 JWT required')
    expected = hmac.new(secret, head + b'.' + payload, hashlib.sha256).digest()
    if not hmac.compare_digest(unb64(signature), expected):
        raise ValueError('original JWT signature invalid')
    if claims.get('sub') != ACTOR or claims.get('org_id') != ORG:
        raise ValueError('fixed primary actor/org required')
    if type(claims.get('exp')) is not int or any(type(claims[k]) is not int for k in ('iat', 'nbf') if k in claims):
        raise ValueError('JWT time claims must be integers')
    roles = claims.get('roles', [])
    if not isinstance(roles, list) or any(not isinstance(x, str) for x in roles):
        raise ValueError('JWT roles shape invalid')
    refreshed = dict(claims, exp=now + TTL)
    for key in ('iat', 'nbf'):
        if key in refreshed:
            refreshed[key] = now
    unchanged = lambda obj: {k: v for k, v in obj.items() if k not in ('exp', 'iat', 'nbf')}
    if unchanged(refreshed) != unchanged(claims):
        raise ValueError('non-time JWT claims changed')
    message = head + b'.' + b64(json.dumps(refreshed, separators=(',', ':'), ensure_ascii=False).encode())
    signed = message + b'.' + b64(hmac.new(secret, message, hashlib.sha256).digest())
    if not hmac.compare_digest(unb64(signed.split(b'.')[2]), hmac.new(secret, message, hashlib.sha256).digest()):
        raise ValueError('renewed signature invalid')
    metadata = {'identity': {'actor': ACTOR, 'tenant': ORG}, 'roles': roles,
                'time_before': {k: claims[k] for k in ('exp', 'iat', 'nbf') if k in claims},
                'time_after': {k: refreshed[k] for k in ('exp', 'iat', 'nbf') if k in refreshed},
                'duration_seconds': TTL, 'original_signature_verified': True,
                'new_signature_verified': True, 'header_bytes_equal': True,
                'non_time_claims_equal': True, 'permissions_equal': refreshed.get('permissions') == claims.get('permissions')}
    return b'Bearer ' + signed, metadata


def renew_document(raw, secret_raw, now):
    lines = raw.splitlines(keepends=True)
    matching = [i for i, line in enumerate(lines) if line.startswith(KEY + b'=')]
    if len(matching) != 1:
        raise ValueError('exact single writer key required')
    secrets = [line.split(b'=', 1)[1].strip().strip(b'\"').strip(b"'") for line in secret_raw.splitlines()
               if line.startswith(SECRET_KEY + b'=')]
    if len(secrets) != 1 or not secrets[0]:
        raise ValueError('exact q3 signing key required')
    index = matching[0]
    line = lines[index]
    ending = b'\r\n' if line.endswith(b'\r\n') else b'\n' if line.endswith(b'\n') else b''
    old = line[len(KEY) + 1:len(line) - len(ending) if ending else None]
    value, metadata = renew_value(old, secrets[0], now)
    replacement = list(lines)
    replacement[index] = KEY + b'=' + value + ending
    if any(replacement[i] != lines[i] for i in range(len(lines)) if i != index):
        raise ValueError('other env bytes changed')
    metadata['other_env_bytes_equal'] = True
    return b''.join(replacement), KEY + b'=' + old + b'\n', metadata


class PrivateFS:
    def exists(self, path):
        return Path(path).exists() or Path(path).is_symlink()

    def read(self, path):
        # O_NOFOLLOW and fstat bind the actual opened inode, not a prior lstat.
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as f:
            info = os.fstat(f.fileno())
            if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600 or info.st_uid != os.geteuid():
                raise ValueError('private file mode/owner/type mismatch')
            return f.read()

    def backup(self, data):
        fd = os.open(BACKUP, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as f:
            os.fchmod(f.fileno(), 0o600)
            f.write(data); f.flush(); os.fsync(f.fileno())
        if self.read(BACKUP) != data:
            raise ValueError('exclusive backup verification failed')

    def replace(self, original, data):
        temporary = ENV.with_name(ENV.name + '.writer-refresh-' + uuid.uuid4().hex + '.tmp')
        try:
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'wb') as f:
                os.fchmod(f.fileno(), 0o600)
                f.write(data); f.flush(); os.fsync(f.fileno())
            if self.read(ENV) != original:
                raise ValueError('private env changed before atomic replace')
            os.replace(temporary, ENV)
            if self.read(ENV) != data:
                raise ValueError('atomic private env verification failed')
        finally:
            if temporary.exists():
                temporary.unlink()


def apply_refresh(fs, now, gate):
    if fs.exists(BACKUP):
        raise ValueError('exclusive old-writer backup already exists')
    original = fs.read(ENV)
    new, backup, metadata = renew_document(original, fs.read(SECRET), now)
    gate()
    if fs.read(ENV) != original:
        raise ValueError('private env changed before backup')
    fs.backup(backup)
    gate()
    fs.replace(original, new)
    metadata.update(private_mode_0600=True, backup_mode_0600=True, old_writer_backup_created=True, atomic_replace=True)
    return metadata


def run(release_file):
    path = Path(release_file).resolve()
    if path.stat().st_mode & 0o777 != 0o600 or path.is_symlink():
        raise ValueError('private Root file mode/type mismatch')
    release = json.loads(path.read_text())
    def gate():
        if json.loads(path.read_text()) != release:
            raise ValueError('Root release changed')
        validate_root(release, int(time.time()), G.candidate_hash(G.OPENFOUNDRY), tool_shas())
    gate()
    out = Path(release['output']).resolve()
    out.mkdir(parents=True, exist_ok=False)
    result = apply_refresh(PrivateFS(), int(time.time()), gate)
    # No hashes of individual private values/files, headers, JWTs or secrets.
    (out / 'RESULT.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'ok': True, 'metadata': str(out / 'RESULT.json')}))
    return 0


if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('--release-file', required=True); args = ap.parse_args()
    try: sys.exit(run(args.release_file))
    except Exception as exc:
        print('writer refresh refused/failed (' + type(exc).__name__ + ')', file=sys.stderr); sys.exit(1)
