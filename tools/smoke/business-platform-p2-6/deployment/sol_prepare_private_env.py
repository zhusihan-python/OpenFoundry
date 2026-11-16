#!/usr/bin/env python3
"""Mint NEW P26 private files only under explicit http-private-env phase.

No values are printed, put in argv, or written to evidence. Existing files
are never overwritten. The program deliberately does not start containers.
"""
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import sys
import time
import uuid
from urllib.parse import quote

sys.dont_write_bytecode = True
P26 = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(P26))
from common import p26_gates as G
from preflight import p26_current_preflight as W8

PATHS = {'admin': Path('/private/tmp/openfoundry-p26-http-admin.env'),
         'smoke': Path('/private/tmp/openfoundry-p26-http-smoke.env'),
         'aux': Path('/private/tmp/openfoundry-p26-aux.env')}


def mint(secret, sub, org, permissions):
    now = int(time.time())
    claims = {'sub': sub, 'org_id': org, 'jti': str(uuid.uuid4()),
              'iat': now, 'exp': now + 14400, 'roles': [],
              'permissions': permissions, 'name': 'P26 HTTP smoke',
              'email': 'p26-http@openfoundry.local'}
    b64 = lambda raw: base64.urlsafe_b64encode(raw).rstrip(b'=').decode()
    msg = b64(json.dumps({'alg': 'HS256', 'typ': 'JWT'}).encode()) + '.' + b64(json.dumps(claims).encode())
    return 'Bearer ' + msg + '.' + b64(hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest())


def prepare():
    release = G.load_release()
    assert release and 'http-private-env' in release['released_for'], 'Root http-private-env phase required'
    assert G.candidate_hash(G.ROOT_P26.parents[2]) == release['frozen_source_sha256']
    assert not any(p.exists() or p.is_symlink() for p in PATHS.values()), 'private path collision; preserve existing files'
    # Exact q3 parser matches accepted W8 simple dotenv recipe. No shell.
    values = {}
    for line in W8.Q3_ENV_FILE.read_text().splitlines():
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, val = line.split('=', 1)
        values[key] = val.strip().strip('\"').strip("'")
    required = ['OF_P1_JWT_SECRET', 'OF_P1_PG_PASSWORD', 'OF_P1_CREDENTIAL_KEY']
    assert all(values.get(k) for k in required), 'required q3 key absent (names only)'
    _, catalog, derived = W8.load_baseline(G.P25_FINAL_DIR)
    sub, org = W8.jwt_identity_from_baseline(derived, catalog)
    readonly_sub, foreign_sub, foreign_org = [str(uuid.uuid4()) for _ in range(3)]
    assert readonly_sub != sub and foreign_org != org
    writer = mint(values['OF_P1_JWT_SECRET'], sub, org,
                  ['dataset.write', 'dataset:write', 'connections:read', 'connections:write'])
    docs = {
        'admin': {'P26_HTTP_ADMIN_TOKEN': secrets.token_urlsafe(48)},
        'smoke': {'OPENFOUNDRY_SMOKE_JWT': writer,
                  'P26_W7_JWT_WRITER': writer,
                  'P26_W7_JWT_READONLY': mint(values['OF_P1_JWT_SECRET'], readonly_sub, org,
                                            ['connections:read', 'dataset:read', 'dataset.read']),
                  'P26_W7_JWT_FOREIGN': mint(values['OF_P1_JWT_SECRET'], foreign_sub, foreign_org,
                                           ['connections:read', 'connections:write', 'dataset.write', 'dataset:write'])},
        'aux': {'DATABASE_URL': 'postgres://openfoundry:' + quote(values['OF_P1_PG_PASSWORD'], safe='') + '@postgres:5432/connectors',
                'JWT_SECRET': values['OF_P1_JWT_SECRET'],
                'CREDENTIAL_ENCRYPTION_KEY': values['OF_P1_CREDENTIAL_KEY']}}
    # Refuse unsafe dotenv values without including them in errors.
    assert all('\n' not in v and '\r' not in v for d in docs.values() for v in d.values())
    assert G.load_release() == release, 'release changed during preparation'
    metadata = []
    for role, doc in docs.items():
        path = PATHS[role]
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as f:
            f.write(''.join(k + '=' + v + '\n' for k, v in doc.items()))
            f.flush()
            os.fsync(f.fileno())
        assert path.stat().st_mode & 0o777 == 0o600
        metadata.append({'path': str(path), 'mode': '0600', 'keys': sorted(doc)})
    return {'files': metadata, 'readonly_identity': 'same org, distinct nonowner sub; roles empty; read permissions only',
            'foreign_identity': 'distinct org; correctly HS256 signed', 'token_ttl_seconds': 14400,
            'restart_auth': 'JWT remains valid until expiry; boot change does not invalidate manual fresh auth'}


if __name__ == '__main__':
    try:
        print(json.dumps(prepare(), indent=2))
    except Exception as exc:
        # Exceptions from private I/O must never interpolate data values.
        print('private env preparation refused/failed: ' + type(exc).__name__, file=sys.stderr)
        sys.exit(1)
