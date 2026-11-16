#!/usr/bin/env python3
"""Six sealed old-artifact steps via NEW copied functions; explicit RO phase.

No historical source gate is executed. Source S and seal integrity are
independent checks. Downloads follow exactly one validated anonymous redirect.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
from types import SimpleNamespace

sys.dont_write_bytecode = True
P26 = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(P26))
from common import p26_gates as G
from preflight import p26_current_preflight as W
HERE = Path(__file__).resolve().parent
VENDOR = HERE / 'artifact_vendor'
STEP_NAMES = ['p21_bc_projection', 'p21_bc_chain', 'p21_bc_fixture_counts',
              'p21_bc_targets_live', 'p1_old_artifact_live', 'p1_old_buildjob_live']


def compare(expected, actual):
    def keyed(rows):
        out = {}
        for row in rows:
            if row['step'] in out:
                raise ValueError('duplicate artifact step')
            out[row['step']] = row
        return out
    before, after = keyed(expected), keyed(actual)
    problems = []
    if set(before) != set(STEP_NAMES) or set(after) != set(STEP_NAMES):
        problems.append('exact six-step set mismatch')
    for step in STEP_NAMES:
        if step not in before or step not in after:
            continue
        if not after[step]['ok'] or before[step] != after[step]:
            problems.append(step + ': full semantic detail differs or not ok')
    return {'ok': not problems, 'problems': problems}


def _text(value):
    return value.decode(errors='replace') if isinstance(value, bytes) else str(value or '')


def evidenced_http(att, url, token=None, transport=None):
    """HTTP .rc is transport outcome (0/1), never an HTTP status/child rc."""
    transport = transport or W.http_call
    before = att.step_begin('http-get', {'method': 'GET', **W.safe_url_projection(url), 'auth_attached': bool(token)})
    try:
        resp = transport(url, auth=token)
        outcome = 0 if resp.status is not None else 1
        meta = {'status': resp.status, 'transport_outcome': outcome, 'original_child_rc': None,
                'body_sha256': hashlib.sha256(resp.body).hexdigest(), 'bytes': len(resp.body), 'auth_attached': bool(token)}
        location = next((v for k, v in resp.headers.items() if k.lower() == 'location'), None)
        if location:
            meta['location'] = W.safe_url_projection(location)
        if resp.error:
            meta['transport_error'] = _text(resp.error)
        raw = resp.body
    except Exception as exc:
        att.step_finish('http-get', before, type(exc).__name__ + ': ' + str(exc), 1,
                        {'status': None, 'transport_outcome': 1, 'original_child_rc': None,
                         'failure_kind': type(exc).__name__}, kind='http')
        raise
    att.step_finish('http-get', before, raw, outcome, meta, kind='http')
    assert resp.status is not None, 'HTTP transport failed (original evidence retained)'
    return resp, location


def evidenced_pg(att, argv, runner=None):
    runner = runner or subprocess.run
    receipt = att.step_begin('pg-select', argv)
    try:
        cp = runner(argv, capture_output=True, text=True, timeout=30)
    except Exception as exc:
        partial = '\n'.join([type(exc).__name__ + ': ' + str(exc),
                             _text(getattr(exc, 'stdout', None)), _text(getattr(exc, 'stderr', None))])
        original_rc = getattr(exc, 'returncode', None)
        if type(original_rc) is not int:
            original_rc = None
        att.step_finish('pg-select', receipt, partial, 'unavailable' if original_rc is None else original_rc,
                        {'original_child_rc': original_rc, 'failure_kind': type(exc).__name__,
                         'stderr': _text(getattr(exc, 'stderr', None))}, kind='argv')
        raise
    att.step_finish('pg-select', receipt, cp.stdout, cp.returncode,
                    {'stderr': cp.stderr, 'original_child_rc': cp.returncode}, kind='argv')
    assert cp.returncode == 0, 'PG read failed (original rc retained)'
    return cp.stdout.strip()


def terminal_error(att, exc, steps):
    """Archive the scrubbed primary failure, never re-raise its raw message."""
    primary = type(exc).__name__
    try:
        att.finish({'ok': False, 'error': primary + ': ' + str(exc), 'steps': steps})
    except Exception:
        print('old artifact read failed (' + primary + '); terminal evidence write unavailable', file=sys.stderr)
        return 1
    print('old artifact read failed (' + primary + '); evidence=' + str(att.dir), file=sys.stderr)
    return 1


def run(release_file):
    release = G.load_release(release_file)
    assert release and 'final-old-artifact-readonly' in release['released_for']
    source = release['frozen_source_sha256']
    assert re.fullmatch('[a-f0-9]{64}', source) and G.candidate_hash(G.OPENFOUNDRY) == source
    assert release.get('candidate_source', source) == source
    for rel in ['deployment/final_old_artifact_readonly.py', 'deployment/artifact_vendor/copy-ledger.json', 'preflight/p26_current_preflight.py']:
        assert release['final_artifact_tool_shas'][rel] == G.sha256_file(P26 / rel), 'Root tool pin differs'
    for row in json.loads((VENDOR / 'copy-ledger.json').read_text())['files']:
        assert G.sha256_file(row['copy']) == row['copy_sha256'], 'NEW copy drift'
    # Full seal validation; the comparison manifest is itself a verified
    # seal member, not an independently selected replacement baseline.
    W.load_baseline(G.P25_FINAL_DIR)
    hm = json.loads((G.P25_FINAL_DIR / 'hash-manifest.json').read_text())['files']
    expected_path = VENDOR / 'sealed-comparison/12-reread-after-manifest.json'
    assert G.sha256_file(expected_path) == hm['12-reread-after-manifest.json']
    expected = [s for s in json.loads(expected_path.read_text())['steps'] if s['step'] in STEP_NAMES]
    att = W.Attempt(P26 / 'results/w6/deployment', 'old-artifact-readonly')
    secret = W.read_jwt_secret()
    att.add_secret(secret)
    def gate_release():
        assert G.load_release(release_file) == release, 'release changed or expired'
    def http(url, token=None):
        gate_release()
        # Fixed GET origins only; download query remains memory-only.
        origin = re.match(r'^https?://[^/]+', url)
        assert origin and origin.group() in {W.ORIGINS[k] for k in ['dataset', 'pipeline', 'fixture']}
        if token:
            assert origin.group() != W.ORIGINS['fixture'], 'fixture must be anonymous'
            att.add_secret(token)
        return evidenced_http(att, url, token)
    def http_json(url, token=None, timeout=10):
        resp, _ = http(url, token)
        assert resp.status not in (301, 302, 303, 307, 308), 'unexpected JSON redirect'
        return resp.status, json.loads(resp.body or b'null')
    pg = release['w7_pg_observer']
    assert re.fullmatch('[0-9a-f]{64}', pg['cid']) and pg['user'] == 'openfoundry' and pg['database'] == 'connectors'
    def psql(sql, database):
        gate_release()
        assert database == 'connectors'
        argv = ['docker', 'exec', pg['cid'], 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'openfoundry', '-d', database, '-c', 'BEGIN READ ONLY;', '-c', sql, '-c', 'COMMIT;']
        return evidenced_pg(att, argv)
    def mint_token(*, sub=None, org=None):
        token = W.sign_testing_jwt(secret, sub, org)
        att.add_secret(token)
        return token
    spec = importlib.util.spec_from_file_location('new_old_artifact_steps', VENDOR / 'readonly_baseline.py')
    base = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(base)  # NEW file only; its main is never invoked.
    base.M = SimpleNamespace(BASES=W.ORIGINS, mint_token=mint_token, scrub=att.redact)
    base.psql = psql
    base.http_json = http_json
    def live_target(token, dataset_id, label, out):
        status, _ = http_json(W.ORIGINS['dataset'] + '/v1/datasets/' + dataset_id, token)
        out[label]['dataset_get'] = {'status': status}
        status, files = http_json(W.ORIGINS['dataset'] + '/v1/datasets/' + dataset_id + '/files', token)
        assert status == 200
        file_list = files if isinstance(files, list) else files['files']
        out[label]['files_count'] = len(file_list)
        seen = set()
        for f in file_list:
            fid = f['id']
            assert fid not in seen, 'duplicate file id'
            seen.add(fid)
            response, location = http(W.ORIGINS['dataset'] + '/v1/datasets/' + dataset_id + '/files/' + fid + '/download', token)
            if response.status == 302:
                response, _ = http(W.validate_download_redirect(location), None)
            assert response.status == 200, 'download status not 200'
            entry = {'download_status': response.status, 'bytes': len(response.body), 'sha256': hashlib.sha256(response.body).hexdigest()}
            if f.get('transaction_id'):
                status, txn = http_json(W.ORIGINS['dataset'] + '/v1/datasets/' + dataset_id + '/branches/main/transactions/' + f['transaction_id'], token)
                assert status == 200
                entry['txn_status'] = txn.get('status')
                entry['txn_content_hash'] = (txn.get('metadata') or {}).get('content_hash')
            out[label]['file_' + fid] = entry
    base._live_target = live_target
    base.STEPS.clear()
    try:
        base.step_legacy_projections()
        chain = base.step_legacy_chain()
        base.step_fixture_counts(True)
        base.step_legacy_targets_live(chain, True)
        base.step_p1_old_artifact(True)
        base.step_p1_old_buildjob(True)
        result = compare(expected, base.STEPS)
        att.gate('sealed-old-artifacts', result)
        assert G.candidate_hash(G.OPENFOUNDRY) == source
        att.finish({'ok': result['ok'], 'source_S': source, 'baseline': str(expected_path), 'steps': base.STEPS, 'comparison': result, 'scope': 'six old artifact domains only; W8 whole-set guard separate'})
        print(att.dir)
        return 0 if result['ok'] else 1
    except Exception as exc:
        return terminal_error(att, exc, base.STEPS)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--release-file', required=True)
    args = ap.parse_args()
    try:
        rc = run(args.release_file)
    except Exception as exc:
        # Covers preflight/bootstrap failures before the six-step collector.
        # No unredacted exception message or Python traceback reaches stderr.
        print('old artifact preflight failed (' + type(exc).__name__ + ')', file=sys.stderr)
        rc = 1
    sys.exit(rc)
